"""Notices for the plugin hooks, and the check at the first prompt after a
resume. See "The teleport notice" and "Prevention" in the spec.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import httpx

from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import ApiError
from hyperspace_client.state import SessionRecord, State
from hyperspace_client.sync import Context
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


def check_resume(ctx: Context, session: LocalSession) -> dict[str, str] | None:
    """For the UserPromptSubmit hook: at the first prompt after a resume,
    return a block decision if another host holds the lease or the session
    is conflicted. If the saved state is older than the resume, ask the
    server (with a short timeout). If the server cannot be reached, permit
    the prompt."""
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
        ctx.state.save_session(record)
    problem = _problem(record, ctx.host)
    if problem is None:
        return None
    return {"decision": "block", "reason": problem}


def after_sync(ctx: Context, adapter: ClaudeCode, session: LocalSession) -> None:
    """Act on the lease state that the last sync saw. If another host holds
    the lease and the session does not run here, move the local copy aside
    (its bytes are on the server). If it runs here, add a notice."""
    record = ctx.state.session(session.harness, session.session_id)
    problem = _problem(record, ctx.host)
    if problem is None:
        return
    if record.state == "conflicted" or adapter.is_running(session.session_id):
        ctx.state.add_notice(session.harness, session.session_id, "lease", problem)
        return
    if record.lease_state == "held" and Path(session.transcript).exists():
        moved = move_aside(session.transcript, ctx.state)
        log.info(
            "%s: %s holds the lease; moved the local copy aside to %s",
            session.session_id,
            record.lease_host,
            moved,
        )


def hook_json(decision: dict[str, str] | None) -> str:
    return json.dumps(decision) if decision else ""


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
