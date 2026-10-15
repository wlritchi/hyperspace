"""Teleport: send a session from this host, and restore it on another.

See "Teleport" in the spec. `send` runs on the host that holds the lease:
it syncs the session, grants it to the target, surrenders the lease, and
moves the local transcript aside. `restore` runs on the target, in the
directory where the session must continue: it takes the lease, writes the
files with the server's bytes, and records an arrival for the notice.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from hyperspace_client.adapters.claude_code import TRANSCRIPT, ClaudeCode, sanitize_path
from hyperspace_client.api import Api, ApiError
from hyperspace_client.state import Cursor, Segment, SessionRecord, State
from hyperspace_client.sync import CHUNK, Context, Syncer
from hyperspace_protocol import (
    BlobManifest,
    GenerationInfo,
    LogManifest,
    Manifest,
    Placement,
    SessionStateBody,
    validate_file_path,
    validate_session_id,
)

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
    target = transcript.with_name(f"{transcript.name}{SURRENDERED}{int(time.time())}")
    os.rename(transcript, target)
    state.forget_cursor(transcript)
    return target


def _log_bytes(api: Api, harness: str, sid: str, generation: str, length: int) -> bytes:
    parts: list[bytes] = []
    offset = 0
    while offset < length:
        end = min(offset + CHUNK, length)
        parts.append(api.read_log(harness, sid, generation, offset, end))
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
    harness = adapter.harness
    if adapter.is_running(session_id):
        raise TeleportError(
            f"session {session_id} is running on this host. Stop it first."
        )
    local = adapter.find_transcripts(session_id)
    record = ctx.state.session(harness, session_id)
    for transcript in local:
        session = adapter.session_for_transcript(transcript)
        assert session is not None
        workspace = record.workspace or adapter.first_cwd(session) or str(Path.cwd())
        with ctx.state.try_lock(harness, session_id) as locked:
            if not locked:
                raise TeleportError("another sync of this session is running")
            Syncer(ctx, session, workspace).run()
    if to_host is not None and to_host != ctx.host:
        ctx.api.grant(harness, session_id, to_host)
    state = ctx.api.surrender(harness, session_id, to_host)
    moved = [move_aside(t, ctx.state) for t in local]
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
    harness = adapter.harness
    api = ctx.api
    # The id names local files, and can come from the server.
    validate_session_id(session_id)
    manifest = api.manifest(harness, session_id)
    if held_only:
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

    response = api.teleport(harness, session_id, workspace, steal)
    # Read the manifest again: the source may have sent its last bytes
    # before it surrendered.
    manifest = api.manifest(harness, session_id)
    for other in others:
        move_aside(other, ctx.state)

    result = RestoreResult(session_id, workspace, target)
    session_dir = target.with_suffix("")
    for entry in manifest["files"]:
        try:
            validate_file_path(entry["path"])
        except ValueError:
            result.skipped.append(entry["path"])
            continue
        path = adapter.local_path(session_dir, entry["path"])
        if path is None:
            result.skipped.append(entry["path"])
            continue
        if not tool_results and entry["path"].startswith("tool-results/"):
            result.skipped.append(entry["path"])
            continue
        if entry["kind"] == "log":
            _restore_log(ctx, harness, session_id, entry, path, result)
        else:
            _restore_blob(ctx, harness, session_id, entry, path, result)

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
    return result


def _restore_log(
    ctx: Context,
    harness: str,
    sid: str,
    entry: LogManifest | BlobManifest,
    path: Path,
    result: RestoreResult,
) -> None:
    assert entry["kind"] == "log"
    active = _active(entry)
    data = _log_bytes(ctx.api, harness, sid, active["id"], active["length"])
    if path.exists():
        local = path.read_bytes()
        if data.startswith(local):
            # The session was here before: append what is missing.
            with open(path, "ab") as f:
                f.write(data[len(local) :])
                f.flush()
                os.fsync(f.fileno())
            if entry["path"] == TRANSCRIPT:
                result.fast_forward = True
        else:
            _atomic_write(path, data)
    else:
        _atomic_write(path, data)
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
    result.files += 1


def _restore_blob(
    ctx: Context,
    harness: str,
    sid: str,
    entry: LogManifest | BlobManifest,
    path: Path,
    result: RestoreResult,
) -> None:
    assert entry["kind"] == "blob"
    latest = entry["versions"][-1]
    if path.exists():
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != latest["sha256"]:
            # A local file with other content (for example, a plan file with
            # the same name). Keep it.
            result.skipped.append(entry["path"])
            return
    else:
        data = ctx.api.read_blob(harness, sid, entry["path"])
        _atomic_write(path, data)
    st = path.stat()
    ctx.state.save_blob(
        path,
        st.st_size,
        st.st_mtime_ns,
        latest["sha256"],
        ctx.api.cache_identity,
        harness,
        sid,
        entry["path"],
    )
    result.files += 1


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
    event = ctx.api.fork(harness, session_id, branch, length)
    new_id = validate_session_id(event["session_id"])
    data = _log_bytes(ctx.api, harness, session_id, branch, length)
    target = adapter.project_dir(workspace) / f"{new_id}.jsonl"
    _atomic_write(target, adapter.fork_records(data, session_id, new_id))
    session = adapter.session_for_transcript(target)
    assert session is not None
    record = ctx.state.session(harness, new_id)
    record.workspace = workspace
    record.transcript = str(target)
    ctx.state.save_session(record)
    with ctx.state.try_lock(harness, new_id) as locked:
        if locked:
            Syncer(ctx, session, workspace).run()
    return ForkResult(session_id, new_id, branch, target)
