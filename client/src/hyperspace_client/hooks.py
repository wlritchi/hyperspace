"""`hyperspace hook`: the entry point for the harness hooks.

The plugin forwards every hook to this command, with the hook's JSON on
stdin, so all hook behavior is here and the plugin never changes with the
client. See "Plugin" in the spec.

| Event                      | Action                                        |
| -------------------------- | --------------------------------------------- |
| SessionStart               | Print notices (no network); start a checkpoint, the launcher if it does not run, and a sweep at most once an hour; at a start or resume, these can read the keyring |
| UserPromptSubmit           | Start a checkpoint; at the first prompt after a resume, print a block decision if the session lives elsewhere |
| SessionEnd                 | Start a checkpoint with no wait; for a `done` background job, start the close of the job |
| Other events in `CHECKPOINT_EVENTS` | Start a checkpoint                   |
| Any other event            | Nothing                                       |

Events in `STATUS_EVENTS` also queue a status write (see `status.py`), which
the checkpoint sends. With `--notices`, the hook only prints the pending
notices for the agent, for the plugin's synchronous PostToolUse hook.

Checkpoints and sweeps run in a detached process, so that the hook returns
at once and the work outlives the session.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

from hyperspace_client import launcher
from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api, ApiError
from hyperspace_client.config import Config, ConfigError
from hyperspace_client.notices import (
    check_resume,
    pending_restore_decision,
    session_start_notice,
)
from hyperspace_client.state import State
from hyperspace_client.status import STATUS_EVENTS, on_hook
from hyperspace_client.sync import Context

log = logging.getLogger("hyperspace")

CHECKPOINT_EVENTS = frozenset(
    {
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "SubagentStop",
        "PreCompact",
        "PostCompact",
        "Stop",
        "SessionEnd",
        "Notification",
    }
)
SWEEP_EVERY = "3600"
UNLOCK_SOURCES = frozenset({"startup", "resume"})


def _str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def handle(
    payload: dict[str, Any], cfg: Config, start: launcher.Detach = launcher.detach
) -> str:
    """Act on one hook event. Return the text for the hook's stdout."""
    event = _str(payload, "hook_event_name")
    transcript = _str(payload, "transcript_path")
    if event is None or transcript is None:
        return ""
    if event != "SessionStart" and event not in CHECKPOINT_EVENTS:
        return ""
    cwd = _str(payload, "cwd")
    adapter = ClaudeCode()
    session = adapter.session_for_transcript(Path(transcript))
    if session is None:
        return ""
    session = _resolve(cfg, adapter, session)
    transcript = str(session.transcript)
    if event in STATUS_EVENTS:
        try:
            state = State(cfg.state_dir)
            try:
                on_hook(
                    state, adapter, session.harness, session.session_id, event, payload
                )
            finally:
                state.close()
        except Exception as exc:  # noqa: BLE001
            # The status must not stop the backup.
            log.warning(
                "%s: status: %s: %s", session.session_id, type(exc).__name__, exc
            )

    # At a session start or resume, the user is there to answer a passphrase
    # prompt, so these syncs can read the keyring (see "Credentials").
    unlock = event == "SessionStart" and _str(payload, "source") in UNLOCK_SOURCES
    sync = ["--quiet", "sync", "--checkpoint", "--transcript", transcript]
    if cwd:
        sync += ["--cwd", cwd]
    sync += ["--event", event]
    if event == "SessionEnd":
        sync.append("--no-wait")
    if unlock:
        sync.append("--unlock")
    start(sync)

    if event == "SessionEnd":
        _close_if_done(cfg, adapter, session.session_id, start)

    if event == "SessionStart":
        sweep = ["--quiet", "sync", "--all", "--if-stale", SWEEP_EVERY]
        start(sweep + ["--unlock"] if unlock else sweep)
        if not launcher.off():
            run = ["--quiet", "launcher", "--if-absent"]
            start(run + ["--unlock"] if unlock else run)
        state = State(cfg.state_dir)
        try:
            return session_start_notice(
                state, adapter, session, cfg.host, cwd, _str(payload, "source")
            )
        finally:
            state.close()

    if event == "UserPromptSubmit":
        state = State(cfg.state_dir)
        try:
            pending = pending_restore_decision(state, session)
            if pending is not None:
                return json.dumps(pending)
            notices = state.take_notices(session.harness, session.session_id)
            decision = None
            try:
                url, token = cfg.require_server()
            except ConfigError as exc:
                log.info("%s: no resume check: %s", session.session_id, exc)
            else:
                api = Api(url, token, timeout=2.0)
                try:
                    ctx = Context(api=api, state=state, adapter=adapter, host=cfg.host)
                    decision = check_resume(ctx, session)
                except (ApiError, httpx.HTTPError) as exc:
                    log.info("%s: no resume check: %s", session.session_id, exc)
                finally:
                    api.close()
        finally:
            state.close()
        if decision is not None:
            return json.dumps(decision)
        return "\n\n".join(notices)
    return ""


def _resolve(cfg: Config, adapter: ClaudeCode, session: LocalSession) -> LocalSession:
    """The session whose transcript the hook's process writes. The hook of a
    background job can name the job's own id (see `ClaudeCode.resolve`).
    Only a job continues another session, so the hook of a process that is
    not a job keeps its session."""
    if not os.environ.get("CLAUDE_JOB_DIR"):
        return session
    state = State(cfg.state_dir)
    try:
        sid = adapter.resolve(session.session_id, state.continued_from(session.harness))
    finally:
        state.close()
    if sid == session.session_id:
        return session

    def mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    found = adapter.find_transcripts(sid)
    if not found:
        return session
    return LocalSession(session.harness, sid, max(found, key=mtime))


def _close_if_done(
    cfg: Config, adapter: ClaudeCode, session_id: str, start: launcher.Detach
) -> None:
    """At the end of a background job whose session is `done`, close the
    job when its process stops. Claude Code sets `CLAUDE_JOB_DIR` only in
    a background job. The job's session is the one whose transcript it
    writes."""
    job_dir = os.environ.get("CLAUDE_JOB_DIR")
    if not job_dir or launcher.off():
        return
    short = Path(job_dir).name
    harness = adapter.harness
    state = State(cfg.state_dir)
    try:
        jobs = adapter.jobs(state.continued_from(harness))
        job = next((j for j in jobs if j.short == short), None)
        sid = job.session_id if job is not None else session_id
        status = state.session(harness, sid).status
        reopened = launcher.queued_reopen(state, harness, sid)
    finally:
        state.close()
    if status is not None and status.get("status") == "done" and not reopened:
        start(["--quiet", "launcher", "--close-job", short])


def notices(payload: dict[str, Any], cfg: Config) -> str:
    """The pending notices of the session, as additional context for the
    agent, or "" if there are none."""
    if payload.get("agent_id"):
        # The context goes to the subagent. Keep the notices for the main
        # agent's next tool call or prompt.
        return ""
    transcript = _str(payload, "transcript_path")
    adapter = ClaudeCode()
    session = adapter.session_for_transcript(Path(transcript)) if transcript else None
    if session is None:
        return ""
    session = _resolve(cfg, adapter, session)
    state = State(cfg.state_dir)
    try:
        texts = state.take_notices(session.harness, session.session_id)
    finally:
        state.close()
    if not texts:
        return ""
    output = {
        "hookSpecificOutput": {
            "hookEventName": _str(payload, "hook_event_name") or "PostToolUse",
            "additionalContext": "\n\n".join(texts),
        }
    }
    return json.dumps(output)
