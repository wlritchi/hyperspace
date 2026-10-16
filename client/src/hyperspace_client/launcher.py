"""`hyperspace launcher`: one long-running process for each host.

See "Launcher" in the session status spec. In workstation mode, the
SessionStart hook starts the launcher, and the launcher stops when no session
has run for a time. At each tick it sends the host's heartbeat and closes the
background jobs of `done` sessions.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import subprocess
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import httpx

from hyperspace_client.adapters.claude_code import ClaudeCode, Job
from hyperspace_client.api import ApiError, NotFound
from hyperspace_client.state import State
from hyperspace_client.status import clean_note, printable
from hyperspace_client.sync import Context
from hyperspace_client.teleport import TeleportError, back_up
from hyperspace_protocol import (
    MAX_HEARTBEAT_SESSIONS,
    HeartbeatRequest,
    UnclosedSession,
)

log = logging.getLogger("hyperspace")

OFF_ENV = "HYPERSPACE_LAUNCHER"
# The state key of the jobs that `claude rm` refused, by job name.
UNCLOSED_KEY = "launcher_unclosed"
RM_TIMEOUT = 120.0
# A stopped job whose session is not `done` is checked again after this time,
# or at once when its state.json changes.
RECHECK = 900.0
# `--close-job` waits at most this long for the session's process to stop.
CLOSE_WAIT = 300.0
_POLL = 1.0

RunRm = Callable[[list[str]], tuple[int, str]]


def off() -> bool:
    """True if `HYPERSPACE_LAUNCHER=off`: no launcher, and no heartbeats."""
    return os.environ.get(OFF_ENV, "").strip().lower() in ("off", "0", "false", "no")


def run_rm(argv: list[str]) -> tuple[int, str]:
    """Run `claude rm`. Return its exit code and its output."""
    try:
        done = subprocess.run(
            argv,
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=RM_TIMEOUT,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, f"{type(exc).__name__}: {exc}"
    return done.returncode, done.stderr + done.stdout


@contextmanager
def host_lock(state_dir: Path, host: str, config_dir: Path) -> Iterator[bool]:
    """Hold the launcher lock of this host and harness config directory for
    the `with` body. The value is False if another launcher holds it."""
    digest = hashlib.sha256(str(config_dir).encode()).hexdigest()[:16]
    locks = state_dir / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    fd = os.open(locks / f"launcher--{host}--{digest}.lock", os.O_RDWR | os.O_CREAT)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        os.close(fd)


def load_unclosed(state: State) -> dict[str, dict[str, Any]]:
    """The jobs that `claude rm` refused: job name to session id, reason, and
    the mtime of the job's state.json at the refusal."""
    raw = state.get(UNCLOSED_KEY)
    if raw is None:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    return cast(dict[str, dict[str, Any]], value) if isinstance(value, dict) else {}


def save_unclosed(state: State, entries: dict[str, dict[str, Any]]) -> None:
    state.put(UNCLOSED_KEY, json.dumps(entries, sort_keys=True))


def send_heartbeat(
    ctx: Context,
    harness: str,
    live: list[str],
    interval: int,
    final: bool = False,
) -> None:
    """Send the host's heartbeat. It includes the refused jobs that the
    launcher recorded, because each heartbeat replaces the earlier list."""
    request: HeartbeatRequest = {
        "interval": interval,
        "sessions": [
            {"harness": harness, "session_id": sid}
            for sid in live[:MAX_HEARTBEAT_SESSIONS]
        ],
    }
    unclosed: list[UnclosedSession] = [
        {"harness": harness, "session_id": e["session_id"], "reason": e["reason"]}
        for e in load_unclosed(ctx.state).values()
    ]
    if unclosed:
        request["unclosed"] = unclosed[:MAX_HEARTBEAT_SESSIONS]
    if final:
        request["final"] = True
    ctx.api.heartbeat(request)


def _reason(code: int, output: str) -> str:
    for line in printable(output).splitlines():
        note = clean_note(line)
        if note:
            return note
    return f"claude rm exited with status {code}"


def close_job(
    ctx: Context, adapter: ClaudeCode, job: Job, claude: str, run: RunRm = run_rm
) -> str | None:
    """Close the background job of a `done` session: back up the session,
    then run `claude rm`. Return the reason if `claude rm` refuses, or None.

    The job is left as it is if its session runs, a sync of it runs, its
    status on the server is not `done`, or the server does not have every
    local byte. Raise `TeleportError` in the last case, and `NotFound` if
    the server does not have the session."""
    harness = adapter.harness
    sid = job.session_id
    with ctx.state.try_lock(harness, sid) as locked:
        if not locked or adapter.is_running(sid):
            return None
        event = ctx.api.state(harness, sid).get("status")
        if event is None or event["status"] != "done":
            return None
        back_up(ctx, adapter, sid, adapter.find_transcripts(sid), "close")
        if adapter.is_running(sid):
            return None
        # Never with --discard-unpushed or --force-remove-worktree: a refusal
        # keeps the work, and the user decides.
        code, output = run([claude, "rm", job.short])
    if code == 0:
        log.info("%s: closed the job %s", sid, job.short)
        return None
    reason = _reason(code, output)
    log.warning("%s: claude rm %s: %s", sid, job.short, reason)
    return reason


def record_refusal(state: State, job: Job, reason: str) -> None:
    entries = load_unclosed(state)
    entries[job.short] = {
        "session_id": job.session_id,
        "reason": reason,
        "mtime": job.mtime,
    }
    save_unclosed(state, entries)


def close_after_exit(
    ctx: Context,
    adapter: ClaudeCode,
    short: str,
    claude: str,
    run: RunRm = run_rm,
    wait: float = CLOSE_WAIT,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """For the SessionEnd hook of a `done` background job: wait for the
    session's process to stop, then close the job. The launcher's sweep
    covers a job that this misses."""
    job = next((j for j in adapter.jobs() if j.short == short), None)
    if job is None:
        return
    deadline = clock() + wait
    while adapter.is_running(job.session_id):
        if clock() >= deadline:
            log.info("%s: still runs; the sweep closes it later", job.session_id)
            return
        sleep(_POLL)
    try:
        reason = close_job(ctx, adapter, job, claude, run)
    except (NotFound, TeleportError) as exc:
        log.warning("%s: not closed: %s", job.session_id, exc)
        return
    if reason is not None:
        record_refusal(ctx.state, job, reason)


@dataclass
class Launcher:
    ctx: Context
    adapter: ClaudeCode
    claude: str
    interval: int
    # Stop after this many seconds with no live session; None: never.
    idle_exit: float | None
    run_rm: RunRm = run_rm
    # Called before each tick, for example to keep the token cache alive.
    before_tick: Callable[[], None] | None = None
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    # Job name to (state.json mtime, time of the last check).
    _checked: dict[str, tuple[float, float]] = field(
        default_factory=dict[str, tuple[float, float]]
    )

    def sweep(self, live: list[str]) -> None:
        """Close the stopped jobs of `done` sessions."""
        now = self.clock()
        jobs = self.adapter.jobs()
        by_name = {j.short: j for j in jobs}
        entries = load_unclosed(self.ctx.state)
        # A refusal holds until the job goes away or its state changes.
        kept = {
            short: e
            for short, e in entries.items()
            if short in by_name and by_name[short].mtime == e.get("mtime")
        }
        if kept != entries:
            save_unclosed(self.ctx.state, kept)
        self._checked = {k: v for k, v in self._checked.items() if k in by_name}
        for job in jobs:
            if job.session_id in live or job.short in kept:
                continue
            seen = self._checked.get(job.short)
            if seen is not None and seen[0] == job.mtime and now - seen[1] < RECHECK:
                continue
            self._checked[job.short] = (job.mtime, now)
            try:
                reason = close_job(
                    self.ctx, self.adapter, job, self.claude, self.run_rm
                )
            except (NotFound, TeleportError) as exc:
                log.info("%s: not closed: %s", job.session_id, exc)
                continue
            if reason is not None:
                record_refusal(self.ctx.state, job, reason)

    def tick(self) -> list[str]:
        """One heartbeat and one sweep. Return the live sessions."""
        live = self.adapter.live_sessions()
        try:
            if self.before_tick is not None:
                self.before_tick()
            send_heartbeat(self.ctx, self.adapter.harness, live, self.interval)
            self.sweep(live)
        except (ApiError, httpx.HTTPError, OSError) as exc:
            # The next tick tries again.
            log.warning("launcher: %s: %s", type(exc).__name__, exc)
        return live

    def run(self, max_ticks: int | None = None) -> int:
        last_live = self.clock()
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            ticks += 1
            if self.tick():
                last_live = self.clock()
            elif (
                self.idle_exit is not None
                and self.clock() - last_live >= self.idle_exit
            ):
                try:
                    send_heartbeat(
                        self.ctx, self.adapter.harness, [], self.interval, final=True
                    )
                except (ApiError, httpx.HTTPError) as exc:
                    log.warning("launcher: last heartbeat: %s", exc)
                log.info("launcher: no live session; stopping")
                return 0
            self.sleep(self.interval)
        return 0
