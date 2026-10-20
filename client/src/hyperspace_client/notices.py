"""Notices for the plugin hooks, and the check at the first prompt after a
resume. See "The teleport notice" and "Prevention" in the spec.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from pathlib import Path

import httpx

from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import ApiError
from hyperspace_client.state import LEASE_FIELDS, SessionRecord, State
from hyperspace_client.sync import Context, _matching_prefix
from hyperspace_client.teleport import move_aside

log = logging.getLogger("hyperspace")


def _resumed_key(session: LocalSession) -> str:
    return f"resumed:{session.harness}:{session.session_id}"


def session_start_notice(
    state: State,
    adapter: ClaudeCode,
    session: LocalSession,
    host: str,
    cwd: str | None,
    source: str | None,
) -> str:
    """The text for the SessionStart hook to print. Local state only."""
    pending = pending_restore_decision(state, session)
    if pending is not None:
        return pending["reason"]
    parts: list[str] = []
    arrival = state.take_arrival(session.harness, session.session_id)
    record = state.session(session.harness, session.session_id)
    if arrival is not None:
        parts.append(adapter.arrival_notice(arrival))
    elif (
        source == "resume"
        and cwd
        and record.workspace
        and record.workspace != cwd
        and record.transcript != str(session.transcript)
    ):
        parts.append(adapter.move_notice(host, record.workspace, host, cwd))
    if source == "resume":
        state.put(_resumed_key(session), str(time.time()))
    parts.extend(state.take_notices(session.harness, session.session_id))
    return "\n\n".join(parts)


def _problem(record: SessionRecord, host: str) -> str | None:
    if record.state == "conflicted":
        return (
            "[hyperspace] This session is conflicted: two copies of it continued "
            "on different hosts. Continue this copy under a new id with "
            f"`hyperspace fork {record.session_id}`, or choose a branch with "
            f"`hyperspace show {record.session_id}`."
        )
    if (
        record.lease_host
        and record.lease_host != host
        and record.lease_state
        in (
            "held",
            "reserved",
        )
    ):
        return (
            f"[hyperspace] {record.lease_host} holds the lease of this session. "
            f"Bring it here with `hyperspace teleport {record.session_id} --to "
            f"{host}` on {record.lease_host} and `hyperspace restore "
            f"{record.session_id}` here, or continue this copy under a new id "
            f"with `hyperspace fork {record.session_id}`."
        )
    return None


def pending_restore_decision(
    state: State, session: LocalSession
) -> dict[str, str] | None:
    if not state.get(f"restore_pending:{session.harness}:{session.session_id}"):
        return None
    return {
        "decision": "block",
        "reason": (
            "[hyperspace] This session has an unfinished restore. Backup and resume "
            "are blocked until its lease and files are reconciled. Stop this session, "
            f"then run `hyperspace restore {session.session_id}` in the target workspace. "
            "If it reports a conflict, use `hyperspace fork` to recover a branch."
        ),
    }


def check_resume(ctx: Context, session: LocalSession) -> dict[str, str] | None:
    """For the UserPromptSubmit hook: at the first prompt after a resume,
    return a block decision if another host holds the lease or the session
    is conflicted. If the saved state is older than the resume, ask the
    server (with a short timeout). If the server cannot be reached, permit
    the prompt."""
    pending = pending_restore_decision(ctx.state, session)
    if pending is not None:
        return pending
    key = _resumed_key(session)
    resumed = ctx.state.get(key)
    if resumed is None:
        return None
    ctx.state.put(key, "")
    if not resumed:
        return None
    record = ctx.state.session(session.harness, session.session_id)
    if record.state_updated is None or record.state_updated < float(resumed):
        try:
            body = ctx.api.state(session.harness, session.session_id)
        except (ApiError, httpx.HTTPError) as exc:
            log.info("%s: no state check: %s", session.session_id, exc)
            return None
        record.lease_state = body["lease"]["state"]
        record.lease_host = body["lease"].get("host")
        record.state = body["state"]
        record.state_updated = time.time()
        # This hook does not hold the session lock.
        ctx.state.save_session(record, LEASE_FIELDS)
    problem = _problem(record, ctx.host)
    if problem is None:
        return None
    return {"decision": "block", "reason": problem}


def after_sync(ctx: Context, adapter: ClaudeCode, session: LocalSession) -> None:
    """Act on the lease state that the last sync saw, with the session lock held.

    A host that held the lease and lost it (another host now holds it)
    moves its local copy aside, so that `claude --resume` does not find a
    stale copy. It does so only if the session does not run here and every
    local byte is on the server. In all other cases (a host that never held
    the lease, a running session, a conflict, bytes not yet uploaded) it
    only adds a notice: the client never moves a file that it cannot prove
    is safe on the server.
    """
    record = ctx.state.session(session.harness, session.session_id)
    problem = _problem(record, ctx.host)
    if problem is None:
        return
    movable = (
        record.held_lease
        and record.state != "conflicted"
        and record.lease_state == "held"
        and not ctx.state.get(f"restore_pending:{session.harness}:{session.session_id}")
        and not adapter.is_running(session.session_id)
        and _fully_synced(ctx, adapter, session)
    )
    if not movable:
        ctx.state.add_notice(session.harness, session.session_id, "lease", problem)
        return
    if Path(session.transcript).exists():
        moved = move_aside(session.transcript, ctx.state)
        record.held_lease = False
        record.transcript = None
        ctx.state.save_session(record)
        log.info(
            "%s: %s holds the lease; moved the local copy aside to %s",
            session.session_id,
            record.lease_host,
            moved,
        )


def _identity(st: os.stat_result) -> tuple[int, ...]:
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _fully_synced(ctx: Context, adapter: ClaudeCode, session: LocalSession) -> bool:
    """Check every local file against the bytes acknowledged by this server."""
    state, server = ctx.state, ctx.api.cache_identity
    metadata = state.session(session.harness, session.session_id).metadata
    try:
        files = adapter.files(session, metadata)
        stats: dict[Path, tuple[int, ...]] = {}
        for f in files:
            before = f.path.stat()
            cursor = state.cursor(f.path, server)
            if f.kind == "log" or cursor is not None:
                if cursor is None or (
                    cursor.harness,
                    cursor.session_id,
                    cursor.rel_path,
                ) != (session.harness, session.session_id, f.rel_path):
                    return False
                end, _ = _matching_prefix(f.path, cursor.segments)
                if end != cursor.length or end != before.st_size:
                    return False
            else:
                known = state.blob(
                    f.path, server, session.harness, session.session_id, f.rel_path
                )
                if known is None:
                    return False
                with f.path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if digest != known[2]:
                    return False
            stats[f.path] = _identity(before)
        # Refuse a file change during validation, including new side files.
        return files == adapter.files(session, metadata) and all(
            _identity(path.stat()) == before for path, before in stats.items()
        )
    except OSError:
        return False


def refresh_states(ctx: Context, adapter: ClaudeCode) -> int:
    """Update the saved lease and state of this host's sessions with one
    request, and act on lost leases (see `after_sync`). A sync with no new
    bytes gets no session state, so the sweep calls this. Return the number
    of sessions whose saved state changed."""
    changed = 0
    index = adapter.transcript_index()
    for summary in ctx.api.sessions(
        harness=adapter.harness, host=ctx.host, limit="1000"
    ):
        sid = summary["session_id"]
        with ctx.state.try_lock(adapter.harness, sid) as locked:
            if not locked:
                continue
            record = ctx.state.session(adapter.harness, sid)
            lease = summary["state"]["lease"]
            new = (lease["state"], lease.get("host"), summary["state"]["state"])
            if new != (record.lease_state, record.lease_host, record.state):
                changed += 1
                record.lease_state, record.lease_host, record.state = new
                record.state_updated = time.time()
                ctx.state.save_session(record)
            if _problem(record, ctx.host) is None:
                continue
            # Also for a state that did not change: a session that ran during an
            # earlier refresh can be stopped now.
            for transcript in index.get(sid, []):
                session = adapter.session_for_transcript(transcript)
                if session is not None:
                    after_sync(ctx, adapter, session)
    return changed
