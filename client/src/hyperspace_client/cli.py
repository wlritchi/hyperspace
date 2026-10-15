"""The `hyperspace` command."""

from __future__ import annotations

import argparse
import fnmatch
import getpass
import json
import logging
import os
import sys
import time
from collections.abc import Callable
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx

from hyperspace_client import config as config_mod
from hyperspace_client import credentials
from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api, ApiError
from hyperspace_client.checkpoint import checkpoint
from hyperspace_client.config import Config, ConfigError
from hyperspace_client.credentials import CredentialError
from hyperspace_client.hooks import handle
from hyperspace_client.notices import after_sync, refresh_states
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer, register_host
from hyperspace_client.teleport import TeleportError, fork, restore, send

log = logging.getLogger("hyperspace")
# Before logging is set up (for example, on a bad config file), a warning
# must not reach stderr: a hook prints nothing on an error.
log.addHandler(logging.NullHandler())

# Events where the hook's cwd is the session's workspace.
_START_EVENTS = ("SessionStart",)
# Errors that apply to the credential, not to one session, so that the next
# session of a sweep gets the same error.
CREDENTIAL_ERRORS = frozenset({"unauthorized", "forbidden", "rate_limited"})


class Exit(Exception):
    def __init__(self, code: int, message: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _setup_logging(cfg: Config, quiet: bool) -> None:
    cfg.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    handler = RotatingFileHandler(
        cfg.state_dir / "log", maxBytes=1_000_000, backupCount=2
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(process)d %(levelname)s %(message)s")
    )
    root = logging.getLogger()
    root.addHandler(handler)
    if not quiet:
        stream = logging.StreamHandler(sys.stderr)
        stream.setLevel(logging.WARNING)
        root.addHandler(stream)
    root.setLevel(logging.INFO)


def _context(cfg: Config, state: State) -> Context:
    url, token = cfg.require_server()
    return Context(
        api=Api(url, token), state=state, adapter=ClaudeCode(), host=cfg.host
    )


def _excluded(cfg: Config, workspace: str) -> bool:
    return any(fnmatch.fnmatch(workspace, pattern) for pattern in cfg.exclude_cwd)


def resolve_workspace(
    ctx: Context, session: LocalSession, cwd: str | None, event: str | None
) -> str:
    """The workspace of a session. See "Workspace" in the spec."""
    record = ctx.state.session(session.harness, session.session_id)
    moved = record.transcript is not None and record.transcript != str(
        session.transcript
    )
    if cwd and (event in _START_EVENTS or record.workspace is None or moved):
        return cwd
    if record.workspace:
        return record.workspace
    first = ctx.adapter.first_cwd(session)
    return first or cwd or str(session.transcript.parent)


def _sync_one(
    cfg: Config,
    ctx: Context,
    session: LocalSession,
    workspace: str,
) -> None:
    if _excluded(cfg, workspace):
        log.info("%s: workspace %s is excluded", session.session_id, workspace)
        return
    result = Syncer(ctx, session, workspace).run()
    if isinstance(ctx.adapter, ClaudeCode):
        after_sync(ctx, ctx.adapter, session)
    if result.uploaded:
        log.info(
            "%s: uploaded %d bytes in %d files",
            session.session_id,
            result.uploaded,
            result.files,
        )
    for error in result.errors:
        log.warning("%s: %s", session.session_id, error)


def cmd_sync(args: argparse.Namespace, cfg: Config) -> int:
    state = State(cfg.state_dir)
    ctx = _context(cfg, state)
    try:
        register_host(ctx, cfg.host_kind)
        if args.all:
            return _sweep(cfg, ctx, args.if_stale)
        if args.transcript is None:
            raise Exit(2, "sync needs --transcript or --all")
        session = ctx.adapter.session_for_transcript(Path(args.transcript))
        if session is None:
            log.info("not a session transcript: %s", args.transcript)
            return 0
        workspace = resolve_workspace(ctx, session, args.cwd, args.event)
        record = state.session(session.harness, session.session_id)
        record.workspace = workspace
        record.transcript = str(session.transcript)
        state.save_session(record)

        def run() -> None:
            _sync_one(cfg, ctx, session, workspace)

        if args.checkpoint:
            checkpoint(
                state,
                session.harness,
                session.session_id,
                run,
                cfg.checkpoint_interval,
                wait=not args.no_wait,
            )
        else:
            with state.try_lock(session.harness, session.session_id) as locked:
                if not locked:
                    log.info("%s: another sync is running", session.session_id)
                    return 0
                run()
        return 0
    finally:
        ctx.api.close()
        state.close()


def _sweep(cfg: Config, ctx: Context, if_stale: float | None) -> int:
    state = ctx.state
    if if_stale is not None:
        last = state.get("last_sweep")
        if last is not None and time.time() - float(last) < if_stale:
            return 0
    state.put("last_sweep", str(time.time()))
    count = 0
    for session in ctx.adapter.discover():
        with state.try_lock(session.harness, session.session_id) as locked:
            if not locked:
                continue
            try:
                workspace = resolve_workspace(ctx, session, None, None)
                _sync_one(cfg, ctx, session, workspace)
                count += 1
            except ApiError as exc:
                log.warning("%s: %s", session.session_id, exc)
                if exc.code in CREDENTIAL_ERRORS:
                    log.warning("sweep: stopped after %d sessions", count)
                    raise
            except httpx.HTTPError:
                # The server cannot be reached. The next session gets the
                # same error, so stop.
                raise
            except Exception as exc:  # noqa: BLE001
                # A local error (for example an unreadable file) applies
                # to this session only. Log it and continue the sweep.
                log.warning("%s: %s: %s", session.session_id, type(exc).__name__, exc)
    if isinstance(ctx.adapter, ClaudeCode):
        refresh_states(ctx, ctx.adapter)
    log.info("sweep: %d sessions", count)
    return 0


def cmd_whoami(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with Api(url, token) as api:
        print(json.dumps(api.whoami(), indent=2))
    print(f"token from: {cfg.token_source}", file=sys.stderr)
    return 0


def cmd_login(args: argparse.Namespace, cfg: Config) -> int:
    if not cfg.url:
        raise Exit(2, "no server URL: set HYPERSPACE_URL or `url` in config.toml")
    if args.stdin:
        token = sys.stdin.read().strip()
    elif sys.stdin.isatty():
        token = getpass.getpass("Hyperspace token: ").strip()
    else:
        raise Exit(2, "no terminal: give the token on stdin with --stdin")
    if not token:
        raise Exit(2, "no token given")
    with Api(cfg.url, token) as api:
        who = api.whoami()
    host = who.get("host")
    if host and host != cfg.host:
        print(
            f"hyperspace: this credential is for host {host!r}, but this host is "
            f"{cfg.host!r}; set `host = {json.dumps(host)}` in config.toml, or "
            "create a credential for this host",
            file=sys.stderr,
        )
    backend = credentials.store(cfg.url, token, cfg.state_dir, cfg.token_cache_ttl)
    print(f"stored the token for {host or cfg.host} in the keyring ({backend})")
    if cfg.token_source in ("environment", "file"):
        print(
            f"hyperspace: the token from the {cfg.token_source} comes first; "
            "remove it to use the keyring",
            file=sys.stderr,
        )
    return 0


def cmd_logout(args: argparse.Namespace, cfg: Config) -> int:
    if not cfg.url:
        raise Exit(2, "no server URL: set HYPERSPACE_URL or `url` in config.toml")
    in_keyring, in_cache = credentials.forget(cfg.url)
    places = [p for p, had in (("keyring", in_keyring), ("cache", in_cache)) if had]
    print(
        f"removed the token from the {' and the '.join(places)}"
        if places
        else "no token stored"
    )
    return 0


def cmd_hook(args: argparse.Namespace, cfg: Config) -> int:
    """The entry point for harness hooks: the hook's JSON is on stdin. A hook
    must never fail the session, so this always exits 0."""
    try:
        payload = json.load(sys.stdin)
        if isinstance(payload, dict):
            text = handle(payload, cfg)  # pyright: ignore[reportUnknownArgumentType]
            if text:
                print(text)
    except Exception as exc:  # noqa: BLE001
        log.warning("hook: %s: %s", type(exc).__name__, exc)
    return 0


def _print_table(rows: list[list[str]]) -> None:
    if not rows:
        return
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)).rstrip())


def cmd_ls(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with Api(url, token) as api:
        sessions = api.sessions(
            harness=args.harness or "",
            host=args.host or "",
            workspace=args.workspace or "",
            title=args.title or "",
            limit=str(args.limit),
        )
    if args.json:
        print(json.dumps(sessions, indent=2))
        return 0
    rows = [["SESSION", "LEASE", "STATE", "LAST ACTIVE", "WORKSPACE", "TITLE"]]
    for s in sessions:
        lease = s["state"]["lease"]
        last = s.get("last_placement")
        rows.append(
            [
                s["session_id"],
                f"{lease['state']}:{lease.get('host', '-')}",
                s["state"]["state"],
                (last["last_seen"][:16] if last else "-"),
                s["metadata"].get("workspace", "-"),
                (s["metadata"].get("title") or "")[:50],
            ]
        )
    _print_table(rows)
    return 0


def cmd_show(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with Api(url, token) as api:
        manifest = api.manifest(ClaudeCode.harness, args.session)
    if args.json:
        print(json.dumps(manifest, indent=2))
        return 0
    meta = manifest["metadata"]
    state = manifest["state"]
    lease = state["lease"]
    print(f"session   {manifest['session_id']}")
    print(f"title     {meta.get('title', '-')}")
    print(f"state     {state['state']}")
    print(f"lease     {lease['state']} {lease.get('host', '')} since {lease['since']}")
    print("placements:")
    for p in manifest["placements"]:
        print(f"  {p['first_seen'][:16]}  {p['cause']:<8} {p['host']}:{p['workspace']}")
    for f in manifest["files"]:
        if f["kind"] == "log" and f["path"] == "transcript.jsonl":
            print("transcript branches:")
            for g in f["generations"]:
                mark = "*" if g["id"] == f["active"] else " "
                parent = (
                    f" from {g['parent']}@{g.get('fork_offset')}"
                    if "parent" in g
                    else ""
                )
                print(
                    f" {mark} {g['id']}  {g['cause']:<8} {g['host']:<12} "
                    f"len {g['length']}  activity {g['activity']}{parent}"
                )
    branches = state.get("branches", [])
    if branches:
        print("conflicting branches:")
        for b in branches:
            print(f"  {b['generation']}  {b['host']}  activity {b['activity']}")
    return 0


def cmd_grant(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with Api(url, token) as api:
        api.grant(ClaudeCode.harness, args.session, args.to)
    print(f"granted {args.session} to {args.to}")
    return 0


def _with_context(cfg: Config, fn: Callable[[Context, ClaudeCode], int]) -> int:
    state = State(cfg.state_dir)
    ctx = _context(cfg, state)
    try:
        return fn(ctx, ClaudeCode())
    except TeleportError as exc:
        raise Exit(3, str(exc)) from None
    finally:
        ctx.api.close()
        state.close()


def cmd_teleport(args: argparse.Namespace, cfg: Config) -> int:
    def run(ctx: Context, adapter: ClaudeCode) -> int:
        print(send(ctx, adapter, args.session, args.to))
        return 0

    return _with_context(cfg, run)


def _exec_resume(session_id: str, workspace: str) -> None:
    os.chdir(workspace)
    os.execvp("claude", ["claude", "--resume", session_id])


def cmd_restore(args: argparse.Namespace, cfg: Config) -> int:
    workspace = str(Path(args.cwd or os.getcwd()).resolve())

    def run(ctx: Context, adapter: ClaudeCode) -> int:
        if args.held:
            return _restore_held(ctx, adapter, workspace)
        if args.session is None:
            raise Exit(2, "restore needs a session id, or --held")
        result = restore(
            ctx,
            adapter,
            args.session,
            workspace,
            steal=args.steal,
            force=args.force,
            tool_results=not args.no_tool_results,
        )
        print(
            f"restored {result.files} files of {result.session_id} into "
            f"{result.transcript.parent}"
        )
        if result.skipped:
            print(f"skipped: {', '.join(result.skipped)}")
        if args.resume:
            _exec_resume(result.session_id, workspace)
        print(f"next: cd {workspace} && claude --resume {result.session_id}")
        return 0

    return _with_context(cfg, run)


def _restore_held(ctx: Context, adapter: ClaudeCode, workspace: str) -> int:
    """Restore each session whose lease this host holds and that has no
    local copy, each in its own last workspace on this host."""
    restored = 0
    # A completed lease transfer can precede the destination's first append,
    # so placement-based host filtering would hide a pending restore. Ask
    # for the server's maximum. The list is not filtered by host.
    for summary in ctx.api.sessions(harness=adapter.harness, limit="1000"):
        lease = summary["state"]["lease"]
        sid = summary["session_id"]
        if lease["state"] != "held" or lease.get("host") != ctx.host:
            continue
        if adapter.find_transcripts(sid) and not ctx.state.get(
            f"restore_pending:{adapter.harness}:{sid}"
        ):
            continue
        target = lease.get("workspace") or workspace
        try:
            restore(ctx, adapter, sid, target, held_only=True)
            restored += 1
        except (TeleportError, ApiError) as exc:
            log.warning("%s: not restored: %s", sid, exc)
    print(f"restored {restored} sessions")
    return 0


def cmd_fork(args: argparse.Namespace, cfg: Config) -> int:
    workspace = str(Path(args.cwd or os.getcwd()).resolve())

    def run(ctx: Context, adapter: ClaudeCode) -> int:
        result = fork(ctx, adapter, args.session, workspace, args.branch)
        print(f"forked {result.old_id} ({result.generation}) as {result.new_id}")
        if args.resume:
            _exec_resume(result.new_id, workspace)
        print(f"next: cd {workspace} && claude --resume {result.new_id}")
        return 0

    return _with_context(cfg, run)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hyperspace", description=__doc__)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="hook mode: log errors to the state directory and exit 0",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("whoami", help="show the credential's account and host")
    p.set_defaults(func=cmd_whoami)

    p = sub.add_parser("sync", help="upload sessions to the server")
    p.add_argument("--transcript", help="the session's transcript file")
    p.add_argument("--cwd", help="the session's working directory (from the hook)")
    p.add_argument("--event", help="the hook event name")
    p.add_argument("--all", action="store_true", help="sync every local session")
    p.add_argument(
        "--if-stale",
        type=float,
        metavar="SECONDS",
        help="with --all: do nothing if the last sweep is newer than this",
    )
    p.add_argument(
        "--checkpoint",
        action="store_true",
        help="single-flight sync with a trailing run (for hooks)",
    )
    p.add_argument(
        "--no-wait",
        action="store_true",
        help="with --checkpoint: do not wait for the interval (session end)",
    )
    p.add_argument(
        "--unlock",
        action="store_true",
        help="with --quiet: the token can come from the keyring, which can ask "
        "for a passphrase (session start)",
    )
    p.set_defaults(func=cmd_sync)

    p = sub.add_parser("login", help="check a token and store it in the keyring")
    p.add_argument("--stdin", action="store_true", help="read the token from stdin")
    p.set_defaults(func=cmd_login)

    p = sub.add_parser("logout", help="remove the token from the keyring and cache")
    p.set_defaults(func=cmd_logout)

    p = sub.add_parser("ls", help="list sessions on the server")
    p.add_argument("--harness")
    p.add_argument("--host", help="sessions with a placement on this host")
    p.add_argument("--workspace")
    p.add_argument("--title", help="text in the title")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_ls)

    p = sub.add_parser("show", help="show a session: lease, placements, branches")
    p.add_argument("session")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("grant", help="let another host read a session")
    p.add_argument("session")
    p.add_argument("--to", required=True, metavar="HOST")
    p.set_defaults(func=cmd_grant)

    p = sub.add_parser(
        "teleport", help="send a session: surrender this host's lease (send side)"
    )
    p.add_argument("session")
    p.add_argument("--to", metavar="HOST", help="reserve the lease for this host")
    p.set_defaults(func=cmd_teleport)

    p = sub.add_parser(
        "restore", help="receive a session into the current directory (receive side)"
    )
    p.add_argument("session", nargs="?")
    p.add_argument("--cwd", help="the workspace (default: the current directory)")
    p.add_argument("--resume", action="store_true", help="then run claude --resume")
    p.add_argument(
        "--held",
        action="store_true",
        help="restore every session whose lease this host holds and that has no "
        "local copy (for a runner after a restart)",
    )
    p.add_argument(
        "--steal", action="store_true", help="take a lease that a gone host holds"
    )
    p.add_argument("--force", action="store_true", help="move other local copies aside")
    p.add_argument("--no-tool-results", action="store_true")
    p.set_defaults(func=cmd_restore)

    p = sub.add_parser("fork", help="continue a branch of a session under a new id")
    p.add_argument("session")
    p.add_argument("--branch", metavar="GENERATION")
    p.add_argument("--cwd", help="the workspace (default: the current directory)")
    p.add_argument("--resume", action="store_true", help="then run claude --resume")
    p.set_defaults(func=cmd_fork)

    p = sub.add_parser(
        "hook", help="harness hooks: read the hook's JSON on stdin and act on it"
    )
    p.set_defaults(func=cmd_hook)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func: Callable[[argparse.Namespace, Config], int] = args.func
    # A hook must never fail the session, whatever the configuration.
    quiet = args.quiet or args.command == "hook"
    try:
        cfg = config_mod.load()
        if not quiet:
            cfg.keyring_use = "interactive"
        elif getattr(args, "unlock", False):
            cfg.keyring_use = "once"
        _setup_logging(cfg, quiet)
        return func(args, cfg)
    except Exit as exc:
        if exc.message:
            print(f"hyperspace: {exc.message}", file=sys.stderr)
        return exc.code
    except (ConfigError, CredentialError, ApiError, httpx.HTTPError, OSError) as exc:
        if quiet:
            log.warning("%s: %s", type(exc).__name__, exc)
            return 0
        print(f"hyperspace: {exc}", file=sys.stderr)
        return 2 if isinstance(exc, ConfigError) else 1


if __name__ == "__main__":
    raise SystemExit(main())
