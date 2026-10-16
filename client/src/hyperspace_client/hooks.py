"""`hyperspace hook`: the entry point for the harness hooks.

The plugin forwards every hook to this command, with the hook's JSON on
stdin, so all hook behavior is here and the plugin never changes with the
client. See "Plugin" in the spec.

| Event                      | Action                                        |
| -------------------------- | --------------------------------------------- |
| SessionStart               | Print notices (no network); start a checkpoint, and a sweep at most once an hour; at a start or resume, these can read the keyring |
| UserPromptSubmit           | Start a checkpoint; at the first prompt after a resume, print a block decision if the session lives elsewhere |
| SessionEnd                 | Start a checkpoint with no wait               |
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
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

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

Detach = Callable[[list[str]], None]


def detach(args: list[str]) -> None:
    """Start the CLI with `args` in its own session, with no terminal or
    pipes, so that it outlives the hook and the harness session."""
    subprocess.Popen(
        [sys.executable, "-m", "hyperspace_client.cli", *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def _str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def handle(payload: dict[str, Any], cfg: Config, start: Detach = detach) -> str:
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
    if event in STATUS_EVENTS:
        state = State(cfg.state_dir)
        try:
            on_hook(state, adapter, session.harness, session.session_id, event, payload)
        except Exception as exc:  # noqa: BLE001
            # The status must not stop the backup.
            log.warning(
                "%s: status: %s: %s", session.session_id, type(exc).__name__, exc
            )
        finally:
            state.close()

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

    if event == "SessionStart":
        sweep = ["--quiet", "sync", "--all", "--if-stale", SWEEP_EVERY]
        start(sweep + ["--unlock"] if unlock else sweep)
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


def notices(payload: dict[str, Any], cfg: Config) -> str:
    """The pending notices of the session, as additional context for the
    agent, or "" if there are none."""
    transcript = _str(payload, "transcript_path")
    session = (
        ClaudeCode().session_for_transcript(Path(transcript)) if transcript else None
    )
    if session is None:
        return ""
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
