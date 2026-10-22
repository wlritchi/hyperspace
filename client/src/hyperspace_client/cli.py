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
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import EllipsisType
from typing import Any, cast

import httpx

from hyperspace_client import backfill, credentials, launcher
from hyperspace_client import config as config_mod
from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api, ApiError, NotFound
from hyperspace_client.checkpoint import checkpoint
from hyperspace_client.config import Config, ConfigError
from hyperspace_client.credentials import CredentialError
from hyperspace_client.hooks import handle, notices
from hyperspace_client.notices import after_sync, refresh_states
from hyperspace_client.state import State
from hyperspace_client.status import (
    clean_note,
    drop_queue,
    end_continued,
    printable,
    queue,
    seq,
    write,
)
from hyperspace_client.sync import Context, Syncer, register_host
from hyperspace_client.teleport import (
    TeleportError,
    fork,
    restore,
    restore_held,
    send,
)
from hyperspace_protocol import (
    CAP_LIVENESS,
    CAP_SESSION_STATUS,
    SessionStateBody,
    SessionSummary,
    StatusEvent,
    ValidationError,
    validate_session_id,
    validate_status_request,
)

log = logging.getLogger("hyperspace")
# Before logging is set up (for example, on a bad config file), a warning
# must not reach stderr: a hook prints nothing on an error.
log.addHandler(logging.NullHandler())

# Events where the hook's cwd is the session's workspace.
_START_EVENTS = ("SessionStart",)
# Events whose checkpoint also sends a heartbeat.
_HEARTBEAT_EVENTS = ("SessionStart", "SessionEnd")
# Errors that apply to the credential, not to one session, so that the next
# session of a sweep gets the same error.
CREDENTIAL_ERRORS = frozenset({"unauthorized", "forbidden", "rate_limited"})
# Claude Code sets this in the environment of the session's tools and hooks.
SESSION_ENV = "CLAUDE_CODE_SESSION_ID"
# The longest wait for the sync lock of the current session.
STATUS_LOCK_WAIT = 10.0
# `tail` reads the end of a transcript in windows that start at this size.
TAIL_WINDOW = 512 * 1024
# The order of `ls --open`: sessions that need the user first.
_RANK = {"ready": 0, "interrupted": 1, "stalled": 1, "working": 2, "waiting": 3}


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
    if ctx.state.cursor(
        session.transcript, ctx.api.cache_identity
    ) is None and ctx.adapter.is_stub(session):
        # For example, the id of a background job. Its transcript has no
        # conversation until Claude Code copies one into it. In an older job
        # shape, the conversation stays in the transcript of the session
        # that the job continues. The backup starts when the transcript gets
        # a conversation record.
        log.info("%s: no conversation; not backed up", session.session_id)
        return
    result = Syncer(ctx, session, workspace).run()
    if isinstance(ctx.adapter, ClaudeCode):
        after_sync(ctx, ctx.adapter, session)
        # The copy of a session in a background job can come after the sync
        # of the session's link to the job, or before it. Thus the syncs of
        # both check.
        end_continued(ctx, ctx.adapter, session.session_id)
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
        # Before the lock: another process can be syncing this session.
        state.save_session(record, ("workspace", "transcript"))

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
        if args.event in _HEARTBEAT_EVENTS:
            _hook_heartbeat(cfg, ctx, session.session_id, args.event)
        return 0
    finally:
        ctx.api.close()
        state.close()


def _hook_heartbeat(cfg: Config, ctx: Context, session_id: str, event: str) -> None:
    """Send a heartbeat at once, so that a start or an end shows with no
    wait for the launcher's next tick."""
    if launcher.off() or not isinstance(ctx.adapter, ClaudeCode):
        return
    try:
        if not ctx.api.has_capability(CAP_LIVENESS):
            return
        groups = ctx.adapter.live_processes(
            ctx.state.continued_from(ctx.adapter.harness)
        )
        if event == "SessionEnd":
            # The process can still be alive while its SessionEnd hook runs.
            # It is live for each id that it can write.
            groups = [ids for ids in groups if session_id not in ids]
        live = list(dict.fromkeys(sid for ids in groups for sid in ids))
        launcher.send_heartbeat(ctx, ctx.adapter, live, cfg.heartbeat_interval)
    except (ApiError, httpx.HTTPError) as exc:
        log.info("%s: no heartbeat: %s", session_id, exc)


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
            body = cast(dict[str, Any], payload)
            text = notices(body, cfg) if args.notices else handle(body, cfg)
            if text:
                print(text)
    except Exception as exc:  # noqa: BLE001
        log.warning("hook: %s: %s", type(exc).__name__, exc)
    return 0


def _print_table(rows: list[list[str]]) -> None:
    if not rows:
        return
    rows = [[printable(c) for c in r] for r in rows]
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)).rstrip())


def _time(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


def _age(seconds: float) -> str:
    for size, unit in ((86_400, "d"), (3600, "h"), (60, "m")):
        if seconds >= size:
            return f"{int(seconds // size)}{unit}"
    return f"{max(0, int(seconds))}s"


def _event_label(event: StatusEvent | None) -> str:
    if event is None:
        return "-"
    return ":".join(
        str(x) for x in (event["status"], event.get("reason"), event.get("wake")) if x
    )


def status_label(state: SessionStateBody, now: float, stalled_after: float) -> str:
    """The STATUS column: the status, or a state that combines status and
    liveness (see "Liveness" in the session status spec)."""
    event = state.get("status")
    live = state.get("liveness")
    label = _event_label(event)
    if live is not None and event is not None:
        blocked = live.get("close_blocked")
        if event["status"] == "done" and blocked:
            return f"done, not closed: {blocked}"
        if event["status"] == "working" and live["state"] == "stopped":
            return "interrupted"
        last = state.get("last_append")
        if (
            event["status"] == "working"
            and live["state"] == "live"
            and last is not None
            and now - _time(last) > stalled_after
        ):
            return "stalled"
    if live is not None and live["state"] == "unknown":
        return f"{label}, unknown since {live['since'][11:16]}"
    return label


def _rank(summary: SessionSummary, label: str) -> int:
    word = label.split(":", 1)[0].split(",", 1)[0]
    if word == "-":
        return 4
    return _RANK.get(word, 5)


def _note(summary: SessionSummary) -> str:
    event = summary["state"].get("status")
    note = (event or {}).get("note") or (summary.get("excerpt") or "")
    lines = note.strip().splitlines()
    return lines[0][:80] if lines else ""


def _print_status_table(sessions: list[SessionSummary], stalled_after: float) -> None:
    now = time.time()
    rows: list[tuple[int, float, list[str]]] = []
    for s in sessions:
        state = s["state"]
        label = status_label(state, now, stalled_after)
        live = state.get("liveness")
        lease = state["lease"]
        host = (
            live["host"]
            if live is not None and live["state"] == "live"
            else lease.get("host", "-")
        )
        last = state.get("last_append")
        when = _time(last) if last else 0.0
        rows.append(
            (
                _rank(s, label),
                -when,
                [
                    s["session_id"],
                    label,
                    f"{host}:{s['metadata'].get('workspace', '-')}",
                    _age(now - when) if last else "-",
                    (s["metadata"].get("title") or "")[:40],
                    _note(s),
                ],
            )
        )
    rows.sort(key=lambda r: (r[0], r[1]))
    header = ["SESSION", "STATUS", "WHERE", "AGE", "TITLE", "NOTE"]
    _print_table([header] + [r[2] for r in rows])


def cmd_ls(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with_status = args.open or bool(args.status)
    with Api(url, token) as api:
        if with_status:
            api.require_capability(CAP_SESSION_STATUS)
        sessions = api.sessions(
            harness=args.harness or "",
            host=args.host or "",
            workspace=args.workspace or "",
            title=args.title or "",
            limit=str(args.limit),
            open="1" if args.open else "",
            status=args.status or [],
        )
    if args.json:
        print(json.dumps(sessions, indent=2))
        return 0
    if with_status:
        _print_status_table(sessions, args.stalled_after * 60)
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
    print(f"title     {printable(meta.get('title', '-'))}")
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


def _current(state: State | None = None) -> str | None:
    """The session whose transcript the Claude Code process of this command
    writes. `CLAUDE_CODE_SESSION_ID` can hold the own id of a background job
    (see `ClaudeCode.resolve`). `state` gives the sessions that continued in
    an id. None if the variable is not set, or if the session of the job is
    not known."""
    sid = os.environ.get(SESSION_ENV)
    if not sid:
        return None
    try:
        validate_session_id(sid)
    except ValidationError as exc:
        raise Exit(2, f"{SESSION_ENV}: {exc}") from None
    continued = state.continued_from(ClaudeCode.harness) if state else None
    return ClaudeCode().resolve(sid, continued)


def _current_for(cfg: Config) -> str | None:
    """The current session. Opens the client state to find the sessions that
    continued in the id."""
    if not os.environ.get(SESSION_ENV):
        return None
    state = State(cfg.state_dir)
    try:
        return _current(state)
    finally:
        state.close()


def _target(session: str | None, current: str | None | EllipsisType = ...) -> str:
    """The session to act on: the argument, else the current session
    (`current`; `...` gets it from the environment)."""
    if not session:
        session = _current() if current is ... else current
    if not session and os.environ.get(SESSION_ENV):
        raise Exit(
            2,
            "give a session id; the session of this background job is not known",
        )
    if not session:
        raise Exit(2, f"give a session id; {SESSION_ENV} is not set")
    try:
        return validate_session_id(session)
    except ValidationError as exc:
        raise Exit(2, str(exc)) from None


def _targets(
    sessions: list[str], by_user: bool, current: str | None | EllipsisType = ...
) -> list[str]:
    """The sessions for a status command: the arguments, else the current
    session. In a backfill turn, only the current session, not as the user."""
    if current is ...:
        current = _current()
    sids = [_target(s) for s in sessions] or [_target(None, current)]
    if os.environ.get(backfill.SELF_ONLY_ENV) == "1" and (by_user or sids != [current]):
        raise Exit(
            2,
            "in a backfill turn, set only the status of this session, "
            "with no --by-user",
        )
    return sids


def _request(
    status: str,
    sid: str,
    *,
    reason: str | None,
    wake: str | None,
    note: str | None,
    by_user: bool,
    current: str | None | EllipsisType = ...,
) -> dict[str, Any]:
    """An explicit status write from the CLI. The source is the agent when
    the command runs in a Claude Code session, unless `by_user` is set.
    `current` is the current session; `...` gets it from the environment."""
    if current is ...:
        current = _current()
    if status == "ready" and reason is None:
        reason = "idle"
    agent = bool(os.environ.get(SESSION_ENV)) and not by_user
    body: dict[str, Any] = {
        "status": status,
        "source_kind": "agent" if agent else "user",
        "explicit": True,
        "base_seq": 0,
    }
    if reason is not None:
        body["reason"] = reason
    if wake is not None:
        body["wake"] = wake
    note = clean_note(note)
    if note is not None:
        body["note"] = note
    if current and current != sid:
        body["source_session"] = current
    try:
        validate_status_request(body)
    except ValidationError as exc:
        raise Exit(2, str(exc)) from None
    del body["base_seq"]
    return body


def set_status(ctx: Context, sid: str, request: dict[str, Any]) -> StatusEvent | None:
    """Write an explicit status. For the current session, hold its sync lock,
    replace any queued write, and save the write as the declaration of the
    turn. The Stop hook then keeps this status. If a sync holds the lock for
    longer than a short wait, queue the write for that sync. Return the event,
    or None if the write was dropped or queued."""
    harness = ClaudeCode.harness
    state = ctx.state
    if sid != _current(state):
        base = seq(ctx.api.state(harness, sid).get("status"))
        # A session with a copy on this host keeps its saved status current,
        # so that its hooks base their writes on this event. A later sync
        # does not see the event as new, so the notice is added here.
        local = state.session(harness, sid)
        return write(
            ctx,
            harness,
            sid,
            request,
            base,
            local if local.transcript else None,
            notify=True,
        )
    with state.lock(harness, sid, STATUS_LOCK_WAIT) as locked:
        # A sync that holds the lock can send the queued write now. Read the
        # queue before the record, as a hook does (see `status.on_hook`).
        queued = state.wanted_status(harness, sid)
        record = state.session(harness, sid)
        if not locked:
            want = {**request, "base_seq": seq(record.status)}
            queue(state, harness, sid, want, queued)
            state.set_declared(harness, sid, request)
            return None
        drop_queue(state, harness, sid)
        try:
            event = write(ctx, harness, sid, request, seq(record.status), record)
        except NotFound:
            # The server does not have the session yet. The next sync sends
            # the write.
            state.want_status(harness, sid, {**request, "base_seq": 0})
            event = None
        state.set_declared(harness, sid, request)
        return event


def _status_line(event: StatusEvent) -> str:
    source = event["source"]
    by = f"{source['kind']} on {source['host']}"
    if "session_id" in source:
        by += f" (session {source['session_id']})"
    note = event.get("note")
    note = f": {note}" if note else ""
    return printable(f"{_event_label(event)}{note} [{by}, {event['received'][:16]}]")


def _write_all(
    cfg: Config, sids: list[str], request: dict[str, Any], args: argparse.Namespace
) -> int:
    def run(ctx: Context, adapter: ClaudeCode) -> int:
        ctx.api.require_capability(CAP_SESSION_STATUS)
        failed = 0
        for sid in sids:
            event = set_status(ctx, sid, request)
            if event is None and ctx.state.wanted_status(adapter.harness, sid):
                print(f"{sid}: queued {request['status']} until the next sync")
            elif event is None:
                current = ctx.api.state(adapter.harness, sid).get("status")
                print(
                    f"{sid}: not changed; the status is "
                    f"{_status_line(current) if current else 'unset'}",
                    file=sys.stderr,
                )
                failed += 1
            elif getattr(args, "json", False):
                print(json.dumps(event))
            else:
                print(f"{sid}: {_status_line(event)}")
        return 1 if failed else 0

    return _with_context(cfg, run)


def cmd_status(args: argparse.Namespace, cfg: Config) -> int:
    current = _current_for(cfg)
    (sid,) = _targets([args.session] if args.session else [], args.by_user, current)
    if args.status is None:
        url, token = cfg.require_server()
        with Api(url, token) as api:
            api.require_capability(CAP_SESSION_STATUS)
            state = api.state(ClaudeCode.harness, sid)
        if args.json:
            print(json.dumps(state, indent=2))
            return 0
        event = state.get("status")
        print(_status_line(event) if event else "no status")
        live = state.get("liveness")
        if live is not None:
            print(f"liveness  {live['state']} on {live['host']} since {live['since']}")
            blocked = live.get("close_blocked")
            if blocked:
                print(f"not closed: {blocked}")
        return 0
    request = _request(
        args.status,
        sid,
        reason=args.reason,
        wake=args.wake,
        note=args.note,
        by_user=args.by_user,
        current=current,
    )
    return _write_all(cfg, [sid], request, args)


def cmd_done(args: argparse.Namespace, cfg: Config) -> int:
    current = _current_for(cfg)
    sids = _targets(args.sessions, args.by_user, current)
    code = 0
    for sid in sids:
        request = _request(
            "done",
            sid,
            reason=None,
            wake=None,
            note=args.note,
            by_user=args.by_user,
            current=current,
        )
        code = max(code, _write_all(cfg, [sid], request, args))
    return code


def cmd_reopen(args: argparse.Namespace, cfg: Config) -> int:
    current = _current_for(cfg)
    (sid,) = _targets([args.session], args.by_user, current)
    request = _request(
        "ready",
        sid,
        reason="idle",
        wake=None,
        note=args.note,
        by_user=args.by_user,
        current=current,
    )
    return _write_all(cfg, [sid], request, args)


def tail(api: Api, sid: str, turns: int) -> str:
    """The last `turns` turns of the active branch of a session's
    transcript. Read only the end of the transcript, in growing windows."""
    manifest = api.manifest(ClaudeCode.harness, sid)
    entry = next(
        (
            f
            for f in manifest["files"]
            if f["path"] == "transcript.jsonl" and f["kind"] == "log"
        ),
        None,
    )
    if entry is None or entry["kind"] != "log":
        raise Exit(1, f"{sid} has no transcript on the server")
    active = entry["active"]
    length = next(g["length"] for g in entry["generations"] if g["id"] == active)
    window = TAIL_WINDOW
    while True:
        start = max(0, length - window)
        data = api.read_log(ClaudeCode.harness, sid, active, start, length)
        # The first record of a window is incomplete, so a window must hold
        # one prompt more than the turns to show.
        if start == 0 or ClaudeCode.prompts(data) > turns:
            break
        window *= 4
    if start:
        data = data[data.find(b"\n") + 1 :]
    return ClaudeCode.format_tail(data, turns)


def cmd_tail(args: argparse.Namespace, cfg: Config) -> int:
    sid = _target(args.session) if args.session else _target(None, _current_for(cfg))
    url, token = cfg.require_server()
    with Api(url, token) as api:
        print(printable(tail(api, sid, args.n)))
    return 0


def cmd_backfill_status(args: argparse.Namespace, cfg: Config) -> int:
    try:
        since = backfill.parse_age(args.since)
    except ValueError as exc:
        raise Exit(2, str(exc)) from None
    if args.rate <= 0:
        raise Exit(2, "--rate must be more than 0")

    def run(ctx: Context, adapter: ClaudeCode) -> int:
        ctx.api.require_capability(CAP_SESSION_STATUS)
        now = time.time()
        selection = backfill.select(
            ctx,
            adapter,
            since,
            now,
            retry=args.retry,
            limit=args.limit,
            excluded=lambda ws: _excluded(cfg, ws),
        )
        skipped = ", ".join(
            f"{reason} {count}" for reason, count in sorted(selection.skipped.items())
        )
        if args.dry_run:
            rows = [["SESSION", "AGE", "WORKSPACE", "TITLE"]]
            for c in selection.candidates:
                rows.append(
                    [
                        c.session.session_id,
                        _age(now - c.modified),
                        c.workspace,
                        c.title[:50],
                    ]
                )
            _print_table(rows)
            print(f"{len(selection.candidates)} sessions to message")
            if skipped:
                print(f"skipped: {skipped}")
            return 0
        if skipped:
            print(f"skipped: {skipped}")
        results = backfill.run(
            ctx,
            adapter,
            selection.candidates,
            claude=cfg.claude_bin,
            rate=args.rate,
            timeout=args.timeout,
            report=lambda line: print(line, flush=True),
        )
        print(
            ", ".join(f"{k} {v}" for k, v in sorted(results.items())) or "nothing to do"
        )
        return 0

    return _with_context(cfg, run)


def cmd_grant(args: argparse.Namespace, cfg: Config) -> int:
    url, token = cfg.require_server()
    with Api(url, token) as api:
        api.grant(ClaudeCode.harness, args.session, args.to)
    print(f"granted {args.session} to {args.to}")
    return 0


def cmd_launcher(args: argparse.Namespace, cfg: Config) -> int:
    if launcher.off():
        return 0
    state = State(cfg.state_dir)
    adapter = ClaudeCode()
    try:
        if args.close_job:
            ctx = _context(cfg, state)
            try:
                launcher.close_after_exit(ctx, adapter, args.close_job, cfg.claude_bin)
            finally:
                ctx.api.close()
            return 0
        with launcher.host_lock(cfg.state_dir, cfg.host, adapter.config_dir) as held:
            if not held:
                if not args.if_absent:
                    raise Exit(1, "a launcher already runs for this host")
                return 0
            ctx = _context(cfg, state)
            try:
                # A runner's main process must not stop when the server is
                # not available for a short time at its start.
                if not launcher.connect(
                    ctx, cfg.host_kind, retry=args.stay or args.runner
                ):
                    log.warning("launcher: the server does not support liveness")
                    return 0
                return launcher.Launcher(
                    ctx,
                    adapter,
                    cfg.claude_bin,
                    cfg.heartbeat_interval,
                    None if args.stay or args.runner else cfg.launcher_idle_exit,
                    before_tick=lambda: _refresh_token(cfg, ctx),
                    runner=args.runner,
                    claude_args=cfg.launcher_claude_args,
                    workspace=str(Path.cwd().resolve()),
                ).run()
            finally:
                ctx.api.close()
    finally:
        state.close()


def _refresh_token(cfg: Config, ctx: Context) -> None:
    """Read the token cache again. Each read keeps the cache alive (see
    "Credentials" in the spec), and a new login takes effect."""
    if cfg.token_source not in ("cache", "keyring") or cfg.url is None:
        return
    try:
        found = credentials.find(
            cfg.url, cfg.state_dir, cfg.token_cache_ttl, "cache-only"
        )
    except CredentialError as exc:
        log.info("launcher: token cache: %s", exc)
        return
    if found is None or found[0] == cfg.token:
        return
    where = "the token cache" if found[1] == "cache" else "the keyring"
    try:
        config_mod.check_token(found[0], where)
    except ConfigError as exc:
        # Keep the current token. A bad token makes each request fail.
        log.warning("launcher: %s", exc)
        return
    cfg.token = found[0]
    ctx.api.close()
    ctx.api = Api(cfg.url, found[0])


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
    print(f"restored {restore_held(ctx, adapter, workspace).restored} sessions")
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
    p.add_argument(
        "--open", action="store_true", help="only sessions that are not done"
    )
    p.add_argument(
        "--status",
        action="append",
        choices=["working", "waiting", "ready", "done", "none"],
        help="only sessions with this status (repeatable)",
    )
    p.add_argument(
        "--stalled-after",
        type=float,
        default=30,
        metavar="MINUTES",
        help="show a live working session with no append for this long as stalled",
    )
    p.set_defaults(func=cmd_ls)

    p = sub.add_parser(
        "status", help="show or set the status of a session (default: this session)"
    )
    p.add_argument("status", nargs="?", choices=["working", "waiting", "ready", "done"])
    p.add_argument("--session", help=f"the session (default: ${SESSION_ENV})")
    p.add_argument("--reason", choices=["asked", "idle"], help="with ready")
    p.add_argument("--wake", choices=["local", "message"], help="with waiting")
    p.add_argument(
        "--note", help="one line: what the session waits for, or the outcome"
    )
    p.add_argument(
        "--by-user", action="store_true", help="the user sets the status, not the agent"
    )
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("done", help="mark sessions done (default: this session)")
    p.add_argument("sessions", nargs="*", metavar="SESSION")
    p.add_argument("--note", help="one line: the outcome")
    p.add_argument("--by-user", action="store_true")
    p.set_defaults(func=cmd_done)

    p = sub.add_parser("reopen", help="set a done session back to ready")
    p.add_argument("session")
    p.add_argument("--note", help="one line: why")
    p.add_argument("--by-user", action="store_true")
    p.set_defaults(func=cmd_reopen)

    p = sub.add_parser("tail", help="print the last turns of a session")
    p.add_argument("session", nargs="?")
    p.add_argument("-n", type=int, default=3, help="the number of turns (default 3)")
    p.set_defaults(func=cmd_tail)

    p = sub.add_parser(
        "backfill-status",
        help="ask each recent session on this host with no status to set one",
    )
    p.add_argument(
        "--since",
        default="30d",
        metavar="AGE",
        help="sessions whose transcript changed in this time (default 30d)",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=60,
        help="the most sessions to resume each hour (default 60)",
    )
    p.add_argument("--limit", type=int, help="the most sessions to message")
    p.add_argument(
        "--timeout",
        type=float,
        default=600,
        metavar="SECONDS",
        help="stop a resume after this time (default 600)",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="list the sessions and do nothing"
    )
    p.add_argument(
        "--retry",
        action="store_true",
        help="also message sessions that an earlier run messaged",
    )
    p.set_defaults(func=cmd_backfill_status)

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
    p.add_argument(
        "--force",
        action="store_true",
        help="move other local copies aside; restore a session with no conversation",
    )
    p.add_argument("--no-tool-results", action="store_true")
    p.set_defaults(func=cmd_restore)

    p = sub.add_parser("fork", help="continue a branch of a session under a new id")
    p.add_argument("session")
    p.add_argument("--branch", metavar="GENERATION")
    p.add_argument("--cwd", help="the workspace (default: the current directory)")
    p.add_argument("--resume", action="store_true", help="then run claude --resume")
    p.set_defaults(func=cmd_fork)

    p = sub.add_parser(
        "launcher",
        help="send this host's heartbeat and close done background jobs",
    )
    p.add_argument(
        "--stay",
        action="store_true",
        help="do not stop when no session runs (for a service manager)",
    )
    p.add_argument(
        "--runner",
        action="store_true",
        help="runner mode: never stop, restore the held sessions, and resume "
        "those that need a process; run it in the runner's workspace",
    )
    p.add_argument(
        "--if-absent",
        action="store_true",
        help="exit 0 with no message if a launcher already runs (for hooks)",
    )
    p.add_argument(
        "--close-job",
        metavar="JOB",
        help="wait for the job's session to stop, then close the job if its "
        "session is done (for the SessionEnd hook)",
    )
    p.add_argument(
        "--unlock",
        action="store_true",
        help="with --quiet: the token can come from the keyring, which can ask "
        "for a passphrase (session start)",
    )
    p.set_defaults(func=cmd_launcher)

    p = sub.add_parser(
        "hook", help="harness hooks: read the hook's JSON on stdin and act on it"
    )
    p.add_argument(
        "--notices",
        action="store_true",
        help="only print the session's pending notices for the agent",
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
