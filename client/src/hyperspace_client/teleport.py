"""Teleport: send a session from this host, and restore it on another.

See "Teleport" in the spec. `send` runs on the host that holds the lease:
it syncs the session, grants it to the target, surrenders the lease, and
moves the local transcript aside. `restore` runs on the target, in the
directory where the session must continue: it takes the lease, writes the
files with the server's bytes, and records an arrival for the notice.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import httpx

from hyperspace_client.adapters import LocalFile
from hyperspace_client.adapters.claude_code import (
    STUB_MAX,
    TRANSCRIPT,
    ClaudeCode,
    is_stub,
    sanitize_path,
)
from hyperspace_client.api import Api, ApiError
from hyperspace_client.state import Cursor, Segment, SessionRecord, State
from hyperspace_client.sync import CHUNK, Context, Syncer
from hyperspace_protocol import (
    CAP_RESTORE_PRECONDITION,
    BlobManifest,
    GenerationInfo,
    LogManifest,
    Manifest,
    Placement,
    SessionStateBody,
    TeleportResponse,
    validate_file_path,
    validate_metadata,
    validate_session_id,
)

log = logging.getLogger("hyperspace")

SURRENDERED = ".hyperspace-surrendered-"


class TeleportError(Exception):
    """A teleport or restore that must stop. The message tells the user what
    to do."""


@dataclass
class RestoreResult:
    session_id: str
    workspace: str
    transcript: Path
    files: int = 0
    skipped: list[str] = field(default_factory=list[str])
    fast_forward: bool = False


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".hyperspace-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def move_aside(transcript: Path, state: State) -> Path:
    """Rename a transcript so that `claude --resume` does not find it."""
    target = transcript.with_name(f"{transcript.name}{SURRENDERED}{time.time_ns()}")
    os.rename(transcript, target)
    state.forget_cursor(transcript)
    return target


def _log_bytes(api: Api, harness: str, sid: str, generation: str, length: int) -> bytes:
    parts: list[bytes] = []
    offset = 0
    while offset < length:
        end = min(offset + CHUNK, length)
        part = api.read_log(harness, sid, generation, offset, end)
        if len(part) != end - offset:
            raise TeleportError("incomplete log download")
        parts.append(part)
        offset = end
    return b"".join(parts)


def _active(entry: LogManifest) -> GenerationInfo:
    return next(g for g in entry["generations"] if g["id"] == entry["active"])


def _note_lease(record: SessionRecord, body: SessionStateBody, host: str) -> None:
    record.lease_state = body["lease"]["state"]
    record.lease_host = body["lease"].get("host")
    # A surrender ends this host's hold; a restore starts it.
    record.held_lease = record.lease_state == "held" and record.lease_host == host
    record.state = body["state"]
    record.state_updated = time.time()


# --- Send -----------------------------------------------------------------------


def send(
    ctx: Context,
    adapter: ClaudeCode,
    session_id: str,
    to_host: str | None,
) -> str:
    """Surrender this host's lease. Return a message for the user."""
    validate_session_id(session_id)
    with ctx.state.try_lock(adapter.harness, session_id) as locked:
        if not locked:
            raise TeleportError("another sync of this session is running")
        return _send(ctx, adapter, session_id, to_host)


def back_up(
    ctx: Context,
    adapter: ClaudeCode,
    session_id: str,
    transcripts: list[Path],
    purpose: str,
) -> None:
    """Sync the session and check that the server has every local byte of
    it, including an incomplete last record. Raise `TeleportError` if not.
    The caller holds the session's sync lock."""
    harness = adapter.harness
    record = ctx.state.session(harness, session_id)
    for transcript in transcripts:
        session = adapter.session_for_transcript(transcript)
        assert session is not None
        workspace = record.workspace or adapter.first_cwd(session) or str(Path.cwd())
        # The surrender needs the server copy to match the local file, also
        # when the file is now shorter with no new record.
        result = Syncer(ctx, session, workspace, empty_rewrites=True).run()
        if result.errors:
            raise TeleportError(f"cannot {purpose}: " + "; ".join(result.errors))
        metadata = ctx.state.session(harness, session_id).metadata
        for f in adapter.files(session, metadata):
            if not f.path.exists():
                continue
            data = f.path.read_bytes()
            cursor = ctx.state.cursor(f.path, ctx.api.cache_identity)
            if f.kind == "log" or cursor is not None:
                if (cursor is None and data) or (
                    cursor is not None
                    and (
                        cursor.length != len(data)
                        or any(
                            hashlib.sha256(
                                data[s.offset : s.offset + s.length]
                            ).hexdigest()
                            != s.sha256
                            for s in cursor.segments
                        )
                    )
                ):
                    raise TeleportError(
                        f"cannot {purpose}: {f.rel_path} has unuploaded bytes"
                    )
            else:
                known = ctx.state.blob(
                    f.path, ctx.api.cache_identity, harness, session_id, f.rel_path
                )
                if known is None or known[2] != hashlib.sha256(data).hexdigest():
                    raise TeleportError(
                        f"cannot {purpose}: {f.rel_path} has unuploaded bytes"
                    )


def _send(
    ctx: Context, adapter: ClaudeCode, session_id: str, to_host: str | None
) -> str:
    harness = adapter.harness
    if adapter.is_running(session_id):
        raise TeleportError(
            f"session {session_id} is running on this host. Stop it first."
        )
    local = adapter.find_transcripts(session_id)
    back_up(ctx, adapter, session_id, local, "surrender")
    if adapter.is_running(session_id):
        raise TeleportError("session started during sync; stop it before surrender")
    if to_host is not None and to_host != ctx.host:
        ctx.api.grant(harness, session_id, to_host)
    moved: list[Path] = []
    try:
        for transcript in local:
            moved.append(move_aside(transcript, ctx.state))
        state = ctx.api.surrender(harness, session_id, to_host)
    except BaseException:
        for transcript, backup in zip(local, moved, strict=False):
            os.replace(backup, transcript)
        raise
    record = ctx.state.session(harness, session_id)
    _note_lease(record, state, ctx.host)
    ctx.state.save_session(record)
    where = f"on {to_host}" if to_host else "on the target host"
    lines = [f"Surrendered the lease of {session_id}."]
    if moved:
        lines.append(f"Moved the local transcript aside: {moved[0]}")
    lines.append(
        f"Next, {where}, in the directory where the session must continue:\n"
        f"  hyperspace restore {session_id} --resume"
    )
    return "\n".join(lines)


# --- Receive ----------------------------------------------------------------------


class _UnconfirmedRestore(TeleportError):
    """The request may have committed. Do not remove its local files."""


def _confirm_teleport(
    api: Api,
    harness: str,
    sid: str,
    workspace: str,
    steal: bool,
    revision: str,
    teleport_id: str,
    already_sent: bool = False,
) -> TeleportResponse:
    uncertain = already_sent
    for attempt in range(2):
        try:
            response = api.teleport(
                harness, sid, workspace, steal, revision, teleport_id
            )
            if response["session"]["state"] == "conflicted":
                raise _UnconfirmedRestore(
                    "restore committed, but the session is conflicted; use hyperspace fork"
                )
            return response
        except (httpx.HTTPError, ApiError) as exc:
            committed = isinstance(exc, ApiError) and exc.code.startswith(
                "teleport_committed_"
            )
            ambiguous = isinstance(exc, httpx.HTTPError) or (
                isinstance(exc, ApiError) and (exc.status >= 500 or exc.status == 408)
            )
            if committed:
                assert isinstance(exc, ApiError)
                raise _UnconfirmedRestore(
                    f"restore committed but cannot resume: {exc.detail}; use hyperspace show or fork"
                ) from exc
            # These server-side transaction rejections establish that this
            # id did not commit. Authentication/rate-limit failures do not.
            definite = (
                isinstance(exc, ApiError)
                and exc.status in (400, 409)
                and exc.code
                in {
                    "stale_snapshot",
                    "lease_held",
                    "conflicted",
                    "bad_request",
                }
            )
            if definite or (not uncertain and not ambiguous):
                raise
            uncertain = True
            if attempt:
                raise _UnconfirmedRestore(
                    "restore files are complete, but lease confirmation is unavailable; retry restore before resume"
                ) from exc
    raise AssertionError("unreachable")


def _check_lease(manifest: Manifest, host: str, steal: bool) -> None:
    state = manifest["state"]
    if state["state"] == "conflicted":
        branches = ", ".join(
            f"{b['generation']} ({b['host']}, {b['activity']} activity)"
            for b in state.get("branches", [])
        )
        raise TeleportError(
            "the session is conflicted: two branches have activity. Fork one "
            f"with `hyperspace fork {manifest['session_id']} --branch <generation>`. "
            f"Branches: {branches}"
        )
    lease = state["lease"]
    holder = lease.get("host")
    if lease["state"] == "free" or holder == host or steal:
        return
    raise TeleportError(
        f"{holder} {'holds' if lease['state'] == 'held' else 'has a reservation for'} "
        f"the lease. Run `hyperspace teleport {manifest['session_id']} --to {host}` "
        f"on {holder}, or use --steal if {holder} is gone."
    )


def _source_tool_results(api: Api, manifest: Manifest) -> str | None:
    """The tool-results directory of the session at its last placement."""
    if not manifest["placements"]:
        return None
    source = manifest["placements"][-1]
    try:
        hosts = {h["name"]: h for h in api.hosts()}
    except (ApiError, httpx.HTTPError):
        return None
    config_dirs = hosts.get(source["host"], {}).get("config_dirs") or []
    if not config_dirs:
        return None
    project = f"{config_dirs[0]}/projects/{sanitize_path(source['workspace'])}"
    return f"{project}/{manifest['session_id']}/tool-results/"


def restore(
    ctx: Context,
    adapter: ClaudeCode,
    session_id: str,
    workspace: str,
    steal: bool = False,
    force: bool = False,
    tool_results: bool = True,
    held_only: bool = False,
) -> RestoreResult:
    validate_session_id(session_id)
    with ctx.state.try_lock(adapter.harness, session_id) as locked:
        if not locked:
            raise TeleportError("another sync or restore of this session is running")
        return _restore(
            ctx, adapter, session_id, workspace, steal, force, tool_results, held_only
        )


def _restore(
    ctx: Context,
    adapter: ClaudeCode,
    session_id: str,
    workspace: str,
    steal: bool,
    force: bool,
    tool_results: bool,
    held_only: bool,
) -> RestoreResult:
    harness = adapter.harness
    api = ctx.api
    api.require_capability(CAP_RESTORE_PRECONDITION)
    if adapter.is_running(session_id):
        raise TeleportError("session is running on this host; stop it before restore")
    # The id names local files, and can come from the server.
    validate_session_id(session_id)
    pending_key = f"restore_pending:{harness}:{session_id}"
    pending_text = ctx.state.get(pending_key)
    pending: dict[str, Any] = {}
    if pending_text and pending_text.startswith("{"):
        pending = json.loads(pending_text)
    replay = pending.get("phase") == "sent"
    if replay:
        # The server ties the teleport id to the target host and workspace,
        # not to the credential. Thus a new credential for the same host can
        # replay the request. An older record names the credential.
        same_server = (
            pending.get("server") == api.server_identity
            and pending.get("host") == ctx.host
        ) or pending.get("server") == api.cache_identity
        if not same_server or pending.get("workspace") != workspace:
            raise TeleportError(
                "retry the pending restore with its original server, host, and workspace"
            )
        manifest = cast(Manifest, pending["manifest"])
        steal = bool(pending["steal"])
        tool_results = bool(pending["tool_results"])
    else:
        manifest = api.manifest(harness, session_id)
    if held_only and not replay:
        lease = manifest["state"]["lease"]
        if lease["state"] != "held" or lease.get("host") != ctx.host:
            raise TeleportError("this host does not hold the lease")
    _check_lease(manifest, ctx.host, steal)

    project = adapter.project_dir(workspace)
    target = project / f"{session_id}.jsonl"
    others = [t for t in adapter.find_transcripts(session_id) if t != target]
    if others and not force:
        raise TeleportError(
            "a copy of this session is in another project directory: "
            f"{others[0]}. A second copy would make `claude --resume` fail. "
            "Use --force to move it aside."
        )

    revision = manifest.get("revision")
    if revision is None:
        raise TeleportError("server did not provide a restore revision")
    result = RestoreResult(session_id, workspace, target)
    session_dir = target.with_suffix("")
    prepared: list[tuple[LogManifest | BlobManifest, Path, bytes]] = []
    originals: dict[Path, bytes | None] = {}
    for entry in manifest["files"]:
        try:
            validate_file_path(entry["path"])
        except ValueError:
            result.skipped.append(entry["path"])
            continue
        path = adapter.local_path(session_dir, entry["path"])
        if path is None or (
            not tool_results and entry["path"].startswith("tool-results/")
        ):
            result.skipped.append(entry["path"])
            continue
        local = path.read_bytes() if path.exists() else None
        if entry["kind"] == "log":
            active = _active(entry)
            data = _log_bytes(api, harness, session_id, active["id"], active["length"])
            if local is not None and not data.startswith(local) and not force:
                raise TeleportError(
                    f"local {entry['path']} differs from the backup; use --force to preserve it and restore"
                )
        else:
            latest = entry["versions"][-1]
            if (
                local is not None
                and hashlib.sha256(local).hexdigest() != latest["sha256"]
            ):
                result.skipped.append(entry["path"])
                continue
            data = api.read_blob(harness, session_id, entry["path"], latest["version"])
            if (
                len(data) != latest["size"]
                or hashlib.sha256(data).hexdigest() != latest["sha256"]
            ):
                raise TeleportError(f"invalid download for {entry['path']}")
        originals[path] = local
        prepared.append((entry, path, data))
    if target not in originals:
        raise TeleportError("the session has no restorable transcript")
    transcript = next(data for _, path, data in prepared if path == target)
    if is_stub(transcript) and not force and not replay:
        raise TeleportError(_stub_message(ctx, adapter, session_id, transcript))

    # Download everything before touching local files. Keep rollback copies
    # until the conditional lease request succeeds. Cursors save afterward.
    changed: list[Path] = []
    moved: list[tuple[Path, Path]] = []
    teleport_id = str(pending["teleport_id"]) if replay else "t_" + uuid.uuid4().hex
    pending = {
        "server": api.server_identity,
        "host": ctx.host,
        "workspace": workspace,
        "steal": steal,
        "tool_results": tool_results,
        "teleport_id": teleport_id,
        "manifest": manifest,
        "phase": "sent" if replay else "prepared",
    }
    ctx.state.put(pending_key, json.dumps(pending))
    uncertain = replay
    try:
        if adapter.is_running(session_id):
            raise TeleportError("session started during restore")
        for path, local in originals.items():
            if (path.read_bytes() if path.exists() else None) != local:
                raise TeleportError(f"{path} changed during restore")
        for other in others:
            moved.append((other, move_aside(other, ctx.state)))
        for entry, path, data in prepared:
            local = originals[path]
            if local is not None and data != local and not data.startswith(local):
                moved.append((path, move_aside(path, ctx.state)))
            changed.append(path)
            if path.exists() and entry["kind"] == "log":
                with open(path, "ab") as f:
                    f.write(data[len(local or b"") :])
                    f.flush()
                    os.fsync(f.fileno())
                if path == target:
                    result.fast_forward = True
            elif local != data or not path.exists():
                _atomic_write(path, data)
        pending["phase"] = "sent"
        ctx.state.put(pending_key, json.dumps(pending))
        response = _confirm_teleport(
            api, harness, session_id, workspace, steal, revision, teleport_id, replay
        )
    except BaseException as exc:
        if isinstance(exc, _UnconfirmedRestore) or replay:
            uncertain = True
            changed.clear()
            moved.clear()
            if replay and isinstance(exc, ApiError) and exc.code == "stale_snapshot":
                # This id did not commit. Keep files blocked, but a later
                # retry can download a fresh snapshot and allocate a new id.
                pending["phase"] = "prepared"
                ctx.state.put(pending_key, json.dumps(pending))
        moved_paths = {old for old, _ in moved}
        for path in reversed(changed):
            if path in moved_paths or originals[path] is None:
                path.unlink(missing_ok=True)
            else:
                with open(path, "r+b") as f:
                    f.truncate(len(originals[path] or b""))
        for old, backup in reversed(moved):
            os.replace(backup, old)
        if not uncertain:
            ctx.state.put(pending_key, "")
        raise
    for entry, path, data in prepared:
        if entry["kind"] == "log":
            _save_log_cursor(ctx, harness, session_id, entry, path, data)
        else:
            st = path.stat()
            ctx.state.save_blob(
                path,
                st.st_size,
                st.st_mtime_ns,
                entry["versions"][-1]["sha256"],
                api.cache_identity,
                harness,
                session_id,
                entry["path"],
            )
        result.files += 1

    record = ctx.state.session(harness, session_id)
    record.workspace = workspace
    record.transcript = str(target)
    _note_lease(record, response["session"], ctx.host)
    # Metadata for later syncs comes from the restored transcript.
    record.metadata = adapter.update_metadata({}, TRANSCRIPT, target.read_bytes())
    record.sent_metadata = {}
    ctx.state.save_session(record)

    source: Placement | None = response["teleport"].get("source")
    arrival: dict[str, Any] = {
        "teleport_id": response["teleport"]["id"],
        "target_host": ctx.host,
        "target_workspace": workspace,
        "tool_results": tool_results,
        "target_tool_results": str(session_dir / "tool-results") + "/",
    }
    if source is not None and (
        source["host"] != ctx.host or source["workspace"] != workspace
    ):
        arrival["source"] = source
        old = _source_tool_results(api, manifest)
        if old is not None:
            arrival["source_tool_results"] = old
        ctx.state.add_arrival(harness, session_id, arrival)
    ctx.state.put(pending_key, "")
    return result


def _save_log_cursor(
    ctx: Context,
    harness: str,
    sid: str,
    entry: LogManifest,
    path: Path,
    data: bytes,
) -> None:
    active = _active(entry)
    st = path.stat()
    ctx.state.save_cursor(
        Cursor(
            local_path=str(path),
            server=ctx.api.cache_identity,
            harness=harness,
            session_id=sid,
            rel_path=entry["path"],
            generation=active["id"],
            length=len(data),
            dev=st.st_dev,
            ino=st.st_ino,
            segments=[Segment(0, len(data), hashlib.sha256(data).hexdigest())]
            if data
            else [],
        )
    )


def _stub_message(ctx: Context, adapter: ClaudeCode, sid: str, data: bytes) -> str:
    """Why a restore of a stub stops, and the sessions that can hold its
    conversation: a session that continued in it, or one with its title."""
    title = adapter.update_metadata({}, TRANSCRIPT, data).get("title")
    lines = [
        (
            f"the backup of {sid} holds no conversation, only records that name "
            "it. It is probably the id of a Claude Code background job, and the "
            "conversation is in the session that the job continued."
        )
    ]
    try:
        summaries = ctx.api.sessions(harness=adapter.harness, limit="1000")
    except (ApiError, httpx.HTTPError):
        summaries = []
    for summary in summaries:
        meta = summary["metadata"]
        if summary["session_id"] == sid:
            continue
        if meta.get("links", {}).get("continued_in") != sid and (
            not title or meta.get("title") != title
        ):
            continue
        where = summary.get("last_placement")
        place = f" on {where['host']}:{where['workspace']}" if where else ""
        lines.append(f"Possible session: {summary['session_id']}{place}")
    lines.append("Use --force to restore it anyway.")
    return "\n".join(lines)


# --- Fork ----------------------------------------------------------------------------


@dataclass
class ForkResult:
    old_id: str
    new_id: str
    generation: str
    transcript: Path


def fork(
    ctx: Context,
    adapter: ClaudeCode,
    session_id: str,
    workspace: str,
    branch: str | None = None,
) -> ForkResult:
    """Continue a branch of a session under a new id. The server selects the
    id; the adapter writes the copy in the harness's fork format; the new
    session's first sync uploads it and takes its lease."""
    harness = adapter.harness
    manifest = ctx.api.manifest(harness, session_id)
    entry = next(
        (
            f
            for f in manifest["files"]
            if f["path"] == TRANSCRIPT and f["kind"] == "log"
        ),
        None,
    )
    if entry is None or entry["kind"] != "log":
        raise TeleportError("the session has no transcript on the server")
    generations = {g["id"]: g for g in entry["generations"]}
    if branch is None:
        branches = manifest["state"].get("branches", [])
        if manifest["state"]["state"] == "conflicted" and len(branches) > 1:
            choices = ", ".join(f"{b['generation']} ({b['host']})" for b in branches)
            raise TeleportError(f"choose a branch with --branch: {choices}")
        branch = entry["active"]
    if branch not in generations:
        raise TeleportError(f"no generation {branch} in the transcript")
    length = generations[branch]["length"]
    if length <= STUB_MAX and is_stub(
        _log_bytes(ctx.api, harness, session_id, branch, length)
    ):
        raise TeleportError(
            f"{session_id} holds no conversation, only records that name it; "
            "there is nothing to fork"
        )
    event = ctx.api.fork(harness, session_id, branch, length)
    new_id = validate_session_id(event["session_id"])
    # Copy every remote file, not only files discoverable on this host.
    # Upload from a private staging directory so a shared local plan cannot
    # replace the source plan before its backup is complete.
    with tempfile.TemporaryDirectory(prefix="fork-", dir=ctx.state.directory) as temp:
        target = Path(temp) / f"{new_id}.jsonl"
        session = adapter.session_for_transcript(target)
        assert session is not None
        syncer = Syncer(ctx, session, workspace)
        metadata: dict[str, Any] = {}
        for index, item in enumerate(manifest["files"]):
            validate_file_path(item["path"])
            if item["kind"] == "log":
                gen = (
                    generations[branch] if item["path"] == TRANSCRIPT else _active(item)
                )
                data = _log_bytes(
                    ctx.api, harness, session_id, gen["id"], gen["length"]
                )
                if item["path"].endswith(".jsonl"):
                    data = adapter.fork_records(data, session_id, new_id)
            else:
                version = item["versions"][-1]
                data = ctx.api.read_blob(
                    harness, session_id, item["path"], version["version"]
                )
                if hashlib.sha256(data).hexdigest() != version["sha256"]:
                    raise TeleportError(f"invalid download for {item['path']}")
            path = Path(temp) / str(index)
            path.write_bytes(data)
            local = LocalFile(item["path"], item["kind"], path)
            if item["kind"] == "log" or len(data) > CHUNK * 2:
                metadata = syncer.sync_log(local, metadata)
            else:
                syncer.sync_blob(local)
        ctx.api.add_metadata(
            harness,
            new_id,
            workspace,
            validate_metadata(adapter.server_metadata(metadata)),
        )
    restored = restore(ctx, adapter, new_id, workspace)
    return ForkResult(session_id, new_id, branch, restored.transcript)


def restore_held(ctx: Context, adapter: ClaudeCode, workspace: str) -> int:
    """Restore each session whose lease this host holds and that has no
    local copy, each in its own last workspace on this host. Return the
    number of restored sessions."""
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
    return restored
