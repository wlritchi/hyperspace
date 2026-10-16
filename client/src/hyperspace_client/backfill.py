"""Backfill: ask each older session on this host to set its own status.

Only the agent can tell "the work is finished" from "the user did not act on
the plan yet", so the backfill does not guess. It resumes each session for one
headless turn with a message that asks the agent to set its status."""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import NotFound
from hyperspace_client.sync import Context

log = logging.getLogger(__name__)

MESSAGE = (
    "Hyperspace now records a status for each session: working, waiting, "
    "ready, or done. This session has no status yet. Read the end of this "
    "conversation and decide which status is correct. If the work is finished "
    "and the user probably does not need to return here, run "
    '`hyperspace done --note "<one-line outcome>"`. If you wait for an event '
    "such as a CI run or a review, run "
    '`hyperspace status waiting --wake message --note "<what you wait for>"`. '
    "If the user must decide or do something, run "
    '`hyperspace status ready --reason idle --note "<what the user must do>"`. '
    "Do not change files, run other commands, or continue the work. Reply with "
    "one line that says which status you set and why."
)
ALLOWED_TOOLS = ("Bash(hyperspace status:*)", "Bash(hyperspace done:*)")
# The variables of a Claude Code session that runs this command. The resumed
# session must not get them, or `hyperspace status` would act on the wrong
# session.
_SESSION_VARS = ("CLAUDE_CODE_SESSION_ID", "CLAUDE_JOB_DIR")
_AGE = re.compile(r"(\d+(?:\.\d+)?)([smhdw])")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86_400, "w": 604_800}
# After a timeout, the time that the process group gets to stop after SIGTERM.
_KILL_GRACE = 10.0


def parse_age(text: str) -> float:
    """Seconds from an age such as `30d`, `12h` or `90m`."""
    match = _AGE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"not an age: {text!r} (examples: 30d, 12h, 90m)")
    return float(match[1]) * _UNITS[match[2]]


def attempt_key(harness: str, session_id: str) -> str:
    return f"backfill:{harness}:{session_id}"


@dataclass(frozen=True)
class Candidate:
    session: LocalSession
    workspace: str
    modified: float
    title: str


@dataclass
class Selection:
    candidates: list[Candidate] = field(default_factory=list[Candidate])
    skipped: Counter[str] = field(default_factory=Counter[str])


@dataclass(frozen=True)
class RunResult:
    # None after a timeout.
    returncode: int | None
    output: str


Runner = Callable[[list[str], str, float], RunResult]


def run_claude(argv: list[str], cwd: str, timeout: float) -> RunResult:
    """Run a headless Claude Code turn. After `timeout` seconds, stop its
    whole process group, because Claude Code starts child processes."""
    env = {k: v for k, v in os.environ.items() if k not in _SESSION_VARS}
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        start_new_session=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
        return RunResult(proc.returncode, out)
    except subprocess.TimeoutExpired:
        _kill_group(proc.pid, signal.SIGTERM)
        try:
            out, _ = proc.communicate(timeout=_KILL_GRACE)
        except subprocess.TimeoutExpired:
            _kill_group(proc.pid, signal.SIGKILL)
            out, _ = proc.communicate()
        return RunResult(None, out)


def _kill_group(pid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _local_reason(
    ctx: Context,
    adapter: ClaudeCode,
    session: LocalSession,
    live: set[str],
    retry: bool,
    excluded: Callable[[str], bool],
) -> tuple[str | None, str]:
    """A reason to skip the session that the client can see without the
    server, and the session's workspace."""
    sid = session.session_id
    if not retry and ctx.state.get(attempt_key(adapter.harness, sid)) is not None:
        return "messaged before", ""
    if sid in live:
        return "live", ""
    record = ctx.state.session(adapter.harness, sid)
    workspace = record.workspace or adapter.first_cwd(session) or ""
    if not workspace or not Path(workspace).is_dir():
        return "no workspace", ""
    if excluded(workspace):
        return "excluded", ""
    return None, workspace


def _server_reason(ctx: Context, harness: str, sid: str) -> str | None:
    """A reason to skip the session from its state on the server."""
    try:
        state = ctx.api.state(harness, sid)
    except NotFound:
        return "not on server"
    if state.get("status") is not None:
        return "has status"
    lease = state["lease"]
    if lease["state"] != "held" or lease.get("host") != ctx.host:
        return "lease not here"
    return None


def select(
    ctx: Context,
    adapter: ClaudeCode,
    since: float,
    now: float,
    *,
    retry: bool = False,
    limit: int | None = None,
    excluded: Callable[[str], bool] = lambda _: False,
) -> Selection:
    """The sessions on this host, newest first, whose transcript changed in
    the last `since` seconds and that the backfill can message."""
    out = Selection()
    recent: list[tuple[float, LocalSession]] = []
    for session in adapter.discover():
        try:
            st = session.transcript.stat()
        except OSError:
            continue
        if st.st_size > 0 and st.st_mtime >= now - since:
            recent.append((st.st_mtime, session))
    recent.sort(key=lambda item: item[0], reverse=True)
    live = set(adapter.live_sessions())
    for modified, session in recent:
        if limit is not None and len(out.candidates) >= limit:
            break
        reason, workspace = _local_reason(ctx, adapter, session, live, retry, excluded)
        if reason is None:
            reason = _server_reason(ctx, adapter.harness, session.session_id)
        if reason is not None:
            out.skipped[reason] += 1
            continue
        record = ctx.state.session(adapter.harness, session.session_id)
        title = str(record.metadata.get("title") or "")
        out.candidates.append(Candidate(session, workspace, modified, title))
    return out


def command(claude: str, session_id: str) -> list[str]:
    return [
        claude,
        "--resume",
        session_id,
        "-p",
        MESSAGE,
        "--allowedTools",
        *ALLOWED_TOOLS,
    ]


def _outcome(ctx: Context, harness: str, sid: str, result: RunResult) -> str:
    event = ctx.api.state(harness, sid).get("status")
    # A hook can set `working` or `ready` during the turn. Only an explicit
    # write from the agent counts as the session's own decision.
    if event is not None and event["explicit"] and event["source"]["kind"] == "agent":
        return "set"
    if result.returncode is None:
        return "timeout"
    if result.returncode != 0:
        return "error"
    return "no status"


def run(
    ctx: Context,
    adapter: ClaudeCode,
    candidates: list[Candidate],
    *,
    claude: str,
    rate: float,
    timeout: float,
    report: Callable[[str], None],
    runner: Runner = run_claude,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> Counter[str]:
    """Message each candidate, with at most `rate` starts each hour. Record
    each attempt before the resume starts, so that a run that stops does not
    message a session again."""
    results: Counter[str] = Counter()
    spacing = 3600.0 / rate
    next_start: float | None = None
    for candidate in candidates:
        sid = candidate.session.session_id
        if next_start is not None:
            wait = next_start - clock()
            if wait > 0:
                sleep(wait)
        # The state can change while the run waits.
        reason = "live" if adapter.is_running(sid) else None
        reason = reason or _server_reason(ctx, adapter.harness, sid)
        if reason is not None:
            results[f"skipped: {reason}"] += 1
            report(f"{sid}: skipped ({reason})")
            continue
        started = clock()
        next_start = started + spacing
        key = attempt_key(adapter.harness, sid)
        ctx.state.put(key, json.dumps({"started": started}))
        result = runner(command(claude, sid), candidate.workspace, timeout)
        outcome = _outcome(ctx, adapter.harness, sid, result)
        ctx.state.put(key, json.dumps({"started": started, "result": outcome}))
        results[outcome] += 1
        last = result.output.strip().splitlines()[-1:] or [""]
        report(f"{sid}: {outcome}" + (f": {last[0][:200]}" if last[0] else ""))
        log.info("backfill %s: %s", sid, outcome)
    return results
