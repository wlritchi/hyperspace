"""`hyperspace launcher`: one long-running process for each host.

See "Launcher" in the session status spec. In workstation mode, the
SessionStart hook starts the launcher, and the launcher stops when no session
has run for a time. At each tick it sends the host's heartbeat and closes the
background jobs of `done` sessions. In runner mode, it also restores the
sessions whose lease the host holds, and resumes those that need a process.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import httpx

from hyperspace_client.adapters.claude_code import ClaudeCode, Job
from hyperspace_client.api import ApiError, NotFound
from hyperspace_client.backfill import Runner, RunResult, child_env
from hyperspace_client.state import State
from hyperspace_client.status import clean_note, printable
from hyperspace_client.sync import Context, register_host
from hyperspace_client.teleport import TeleportError, back_up, restore_held
from hyperspace_protocol import (
    CAP_LIVENESS,
    MAX_HEARTBEAT_SESSIONS,
    HeartbeatRequest,
    SessionSummary,
    UnclosedSession,
)

log = logging.getLogger("hyperspace")

OFF_ENV = "HYPERSPACE_LAUNCHER"
# The state key of the jobs that `claude rm` refused, by job name. Each
# harness config directory has its own key, with its source id at the end.
UNCLOSED_KEY = "launcher_unclosed"
RM_TIMEOUT = 120.0
# A stopped job whose session is not `done` is checked again after this time,
# or at once when its state.json changes.
RECHECK = 900.0
# `--close-job` waits at most this long for the session's process to stop.
CLOSE_WAIT = 300.0
_POLL = 1.0
# The first message of a session that a runner resumes, by its status.
RESUME_WORKING = (
    "[hyperspace] The runner restarted while you worked. Check the state of "
    "your work, then continue."
)
RESUME_WAITING = (
    "[hyperspace] The runner restarted. Your crons, Monitors and background "
    "tasks stopped. Set them up again if you still need them, then continue "
    "to wait."
)
# `claude --bg` returns when the daemon has the job.
RESUME_TIMEOUT = 120.0
# At most this many resumes of one session in RESUME_WINDOW seconds, so that a
# session that stops at once is not resumed in a loop.
MAX_RESUMES = 5
RESUME_WINDOW = 3600.0
# The longest wait between restores of held sessions after failures. The wait
# starts at one tick interval and doubles after each failure.
RESTORE_RETRY_MAX = 3600.0

# With --runner or --stay, the launcher waits this long after a start error
# that can be temporary, then tries again. The wait doubles after each error,
# to at most START_RETRY_MAX.
START_RETRY = 5.0
START_RETRY_MAX = 300.0
# httpcore's ProxyError for a refused HTTP CONNECT starts with the status, for
# example "407 Proxy Authentication Required".
_CONNECT_STATUS = re.compile(r"(\d{3})\b")
# A SOCKS ProxyError that refers to authentication: a refused authentication
# method ("Requested USERNAME/PASSWORD from proxy server, but got NO
# ACCEPTABLE METHODS.") or a refused credential ("Invalid username/password").
# A failed SOCKS connect ("Proxy Server could not connect: ...") does not
# refer to authentication.
_SOCKS_AUTH = re.compile(
    r"\b(?:authentication|username|password|methods?)\b", re.IGNORECASE
)

# The most output of `claude --bg` that the launcher reads.
_MAX_OUTPUT = 64 * 1024

RunRm = Callable[[list[str]], tuple[int, str]]
Detach = Callable[[list[str]], None]


def off() -> bool:
    """True if `HYPERSPACE_LAUNCHER=off`: no launcher, and no heartbeats."""
    return os.environ.get(OFF_ENV, "").strip().lower() in ("off", "0", "false", "no")


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


def source_id(config_dir: Path) -> str:
    """The id of a harness config directory: the launcher's lock name, the
    heartbeat's source, and the key of the launcher's state."""
    return hashlib.sha256(str(config_dir.resolve()).encode()).hexdigest()[:16]


def run_bg(argv: list[str], cwd: str, timeout: float) -> RunResult:
    """Run `claude --bg`. It can start the Claude Code daemon, which then
    keeps the open files of the command and runs every background job. So
    the output goes to a file, not a pipe that the daemon keeps open, and a
    timeout stops only the command, not its process group."""
    with tempfile.TemporaryFile("w+", errors="replace") as out:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=child_env(),
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        code: int | None
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            code = None
        out.seek(0)
        return RunResult(code, out.read(_MAX_OUTPUT))


@contextmanager
def host_lock(state_dir: Path, host: str, config_dir: Path) -> Iterator[bool]:
    """Hold the launcher lock of this host and harness config directory for
    the `with` body. The value is False if another launcher holds it."""
    locks = state_dir / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    name = f"launcher--{host}--{source_id(config_dir)}.lock"
    fd = os.open(locks / name, os.O_RDWR | os.O_CREAT)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        os.close(fd)


def load_unclosed(state: State, adapter: ClaudeCode) -> dict[str, dict[str, Any]]:
    """The jobs of `adapter`'s config directory that the launcher did not
    close: job name to session id, reason, and the mtime of the job's
    state.json at that time. `also` has more session ids for the reason.
    With `recheck`, a check refused the close, and the sweep checks the job
    again; else `claude rm` refused."""
    raw = state.get(_unclosed_key(adapter))
    if raw is None:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    return cast(dict[str, dict[str, Any]], value) if isinstance(value, dict) else {}


def save_unclosed(
    state: State, adapter: ClaudeCode, entries: dict[str, dict[str, Any]]
) -> None:
    state.put(_unclosed_key(adapter), json.dumps(entries, sort_keys=True))


def _unclosed_key(adapter: ClaudeCode) -> str:
    return f"{UNCLOSED_KEY}:{source_id(adapter.config_dir)}"


def send_heartbeat(
    ctx: Context,
    adapter: ClaudeCode,
    live: list[str],
    interval: int,
    final: bool = False,
) -> None:
    """Send the heartbeat of `adapter`'s config directory. It includes the
    refused jobs that the launcher recorded, because each heartbeat replaces
    the earlier list of its source."""
    harness = adapter.harness
    request: HeartbeatRequest = {
        "interval": interval,
        "source": source_id(adapter.config_dir),
        "sessions": [
            {"harness": harness, "session_id": sid}
            for sid in live[:MAX_HEARTBEAT_SESSIONS]
        ],
    }
    unclosed: list[UnclosedSession] = [
        {"harness": harness, "session_id": sid, "reason": e["reason"]}
        for e in load_unclosed(ctx.state, adapter).values()
        for sid in [e["session_id"], *e.get("also", [])]
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


def queued_reopen(state: State, harness: str, session_id: str) -> bool:
    """True if a status write that is not `done` waits in the queue."""
    queued = state.wanted_status(harness, session_id)
    return queued is not None and queued[0].get("status") != "done"


def _done(ctx: Context, harness: str, sid: str) -> bool:
    if queued_reopen(ctx.state, harness, sid):
        return False
    event = ctx.api.state(harness, sid).get("status")
    return event is not None and event["status"] == "done"


@dataclass(frozen=True)
class NotClosed:
    """Why `close_job` kept a job, for the heartbeat's `unclosed` list."""

    reason: str
    # True if `claude rm` refused: the refusal holds until the job's state
    # changes. Else the sweep checks the job again.
    refused: bool = True


def blocked_reason(job: Job) -> str:
    """Why a job whose session is not beyond doubt stays (see `Job`)."""
    if len(job.candidates) > 1:
        return f"job {job.short} can write more than one session; close it yourself"
    if job.candidates:
        return f"job {job.short} writes another session; close it yourself"
    return f"the session of job {job.short} is not known; close it yourself"


def close_job(
    ctx: Context, adapter: ClaudeCode, job: Job, claude: str, run: RunRm = run_rm
) -> NotClosed | None:
    """Close the background job of a `done` session: back up the session,
    then run `claude rm`. Return why the job stays, or None if it closed or
    if it is not ready to close.

    Only a job whose session is beyond doubt closes (see `Job`). The job is
    left as it is if its session runs, if a sync of its session runs, or if
    the server does not have every local byte. It is also left if the status
    of the session on the server or in the queue is not `done`. Raise
    `TeleportError` if the server does not have every local byte, and
    `NotFound` if the server does not have the session."""
    harness = adapter.harness
    sid = job.session_id
    if sid is None:
        return NotClosed(blocked_reason(job), refused=False)
    with ctx.state.try_lock(harness, sid) as locked:
        if not locked or adapter.is_running(sid) or not _done(ctx, harness, sid):
            return None
        back_up(ctx, adapter, sid, adapter.find_transcripts(sid), "close")
        # The backup sends a queued status write, which can reopen the
        # session, and a sync of another session can link it to the job's
        # id. Thus check again just before `claude rm`.
        if adapter.is_running(sid) or not _done(ctx, harness, sid):
            return None
        again = adapter.job(job.short, ctx.state.continued_from(harness))
        if again is None:
            return None
        if again.session_id != sid:
            return NotClosed(blocked_reason(again), refused=False)
        # Never with --discard-unpushed or --force-remove-worktree: a refusal
        # keeps the work, and the user decides.
        code, output = run([claude, "rm", job.short])
    if code == 0:
        log.info("%s: closed the job %s", sid, job.short)
        return None
    reason = _reason(code, output)
    log.warning("%s: claude rm %s: %s", sid, job.short, reason)
    return NotClosed(reason)


def record_refusal(
    state: State, adapter: ClaudeCode, job: Job, reason: str, recheck: bool = False
) -> None:
    """Record why the launcher did not close the job, for the heartbeat. The
    reason is on the job's own id and on each other session that the job can
    write."""
    entry: dict[str, Any] = {
        "session_id": job.own_id,
        "reason": reason,
        "mtime": job.mtime,
    }
    also = [sid for sid in job.candidates if sid != job.own_id]
    if also:
        entry["also"] = also
    if recheck:
        entry["recheck"] = True
    entries = load_unclosed(state, adapter)
    if entries.get(job.short) != entry:
        entries[job.short] = entry
        save_unclosed(state, adapter, entries)


def record(state: State, adapter: ClaudeCode, job: Job, kept: NotClosed) -> None:
    record_refusal(state, adapter, job, kept.reason, recheck=not kept.refused)


def forget_recheck(state: State, adapter: ClaudeCode, short: str) -> None:
    """Remove the entry of a job that a check, not `claude rm`, kept."""
    entries = load_unclosed(state, adapter)
    if entries.get(short, {}).get("recheck"):
        del entries[short]
        save_unclosed(state, adapter, entries)


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
    job = adapter.job(short, ctx.state.continued_from(adapter.harness))
    if job is None:
        return
    sid = job.session_id
    if sid is None:
        log.info("%s: not closed: %s", job.own_id, blocked_reason(job))
        return
    deadline = clock() + wait
    while adapter.is_running(sid):
        if clock() >= deadline:
            log.info("%s: still runs; the sweep closes it later", sid)
            return
        sleep(_POLL)
    try:
        kept = close_job(ctx, adapter, job, claude, run)
    except (NotFound, TeleportError) as exc:
        log.warning("%s: not closed: %s", sid, exc)
        return
    if kept is not None:
        record(ctx.state, adapter, job, kept)


def _proxy_auth_refused(message: str) -> bool:
    """True if a proxy refused the authentication: an HTTP CONNECT 407, or a
    SOCKS refusal of the authentication method or of the credential. The
    configuration must change before a retry can succeed."""
    status = _CONNECT_STATUS.match(message)
    if status is not None:
        return status.group(1) == "407"
    return _SOCKS_AUTH.search(message) is not None


def temporary(exc: Exception) -> bool:
    """True if a request can succeed later with no change: a network error, a
    timeout, an incorrect response from the server, a server error, a rate
    limit, or a proxy error other than a refused proxy authentication. A
    local error, for example a bad credential, an invalid token or URL, or a
    redirect loop, is not temporary."""
    if isinstance(exc, ApiError):
        return exc.status >= 500 or exc.status in (408, 429)
    if isinstance(exc, httpx.ProxyError):
        return not _proxy_auth_refused(str(exc))
    return isinstance(
        exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
    )


def connect(
    ctx: Context,
    kind: str,
    retry: bool,
    sleep: Callable[[float], None] | None = None,
) -> bool:
    """Register the host. Return True if the server supports liveness. With
    `retry`, wait and try again after a temporary error, for a runner that
    starts before the server is available."""
    delay = START_RETRY
    while True:
        try:
            register_host(ctx, kind)
            return ctx.api.has_capability(CAP_LIVENESS)
        except (ApiError, httpx.HTTPError, OSError) as exc:
            if not retry or not temporary(exc):
                raise
            log.warning(
                "launcher: start: %s: %s; again in %d s", type(exc).__name__, exc, delay
            )
        (sleep or time.sleep)(delay)
        delay = min(delay * 2, START_RETRY_MAX)


def needs_process(summary: SessionSummary, host: str, running: bool) -> str | None:
    """The first message for a resume of a held session on a runner, or None
    if the session needs no process here (R1 to R3 in the spec), or if the
    server shows it as `live` on any host."""
    state = summary["state"]
    lease = state["lease"]
    if running or lease["state"] != "held" or lease.get("host") != host:
        return None
    live = state.get("liveness")
    if live is not None and live["state"] == "live":
        # `running` does not show a copy on another host, or on this host in
        # another config directory. A resume would make a second copy.
        return None
    event = state.get("status")
    if event is None:
        # From before the status feature; the backfill gives it a status.
        return None
    if event["status"] == "working":
        return RESUME_WORKING
    if event["status"] == "waiting" and event.get("wake") == "local":
        return RESUME_WAITING
    return None


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
    # Runner mode: restore and resume the sessions whose lease the host holds.
    runner: bool = False
    # More Claude Code options for each resume, from the configuration.
    claude_args: list[str] = field(default_factory=list[str])
    # The workspace for a held session that has no workspace of its own.
    workspace: str = ""
    run_claude: Runner = run_bg
    # Starts a CLI command in the background, for a trailing checkpoint.
    start: Detach = detach
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    # Job name to (state.json mtime, time of the last check).
    _checked: dict[str, tuple[float, float]] = field(
        default_factory=dict[str, tuple[float, float]]
    )
    # Session id to the times of its recent resumes.
    _resumes: dict[str, deque[float]] = field(default_factory=dict[str, deque[float]])
    # The earliest time of the next restore of held sessions; None after a
    # restore with no failure.
    _restore_at: float | None = float("-inf")
    _restore_wait: float = 0.0

    def sweep(self, live: list[str]) -> None:
        """Close the stopped jobs of `done` sessions. A job whose session is
        not beyond doubt is never closed; the heartbeat says why."""
        state, adapter = self.ctx.state, self.adapter
        now = self.clock()
        jobs = adapter.jobs(state.continued_from(adapter.harness))
        by_name = {j.short: j for j in jobs}
        entries = load_unclosed(state, adapter)
        # A refusal of `claude rm` holds until the job goes away or its state
        # changes. The next check of the job replaces any other entry.
        kept = {
            short: e
            for short, e in entries.items()
            if short in by_name
            and (e.get("recheck") or by_name[short].mtime == e.get("mtime"))
        }
        if kept != entries:
            save_unclosed(state, adapter, kept)
        self._checked = {k: v for k, v in self._checked.items() if k in by_name}
        for job in jobs:
            if job.session_id is None:
                if job.own_id in live:
                    forget_recheck(state, adapter, job.short)
                else:
                    record_refusal(
                        state, adapter, job, blocked_reason(job), recheck=True
                    )
                continue
            if job.session_id in live:
                forget_recheck(state, adapter, job.short)
                continue
            if job.short in kept and not kept[job.short].get("recheck"):
                continue
            seen = self._checked.get(job.short)
            if seen is not None and seen[0] == job.mtime and now - seen[1] < RECHECK:
                continue
            self._checked[job.short] = (job.mtime, now)
            try:
                result = close_job(self.ctx, adapter, job, self.claude, self.run_rm)
            except (NotFound, TeleportError) as exc:
                log.info("%s: not closed: %s", job.session_id, exc)
                continue
            if result is None:
                forget_recheck(state, adapter, job.short)
            else:
                record(state, adapter, job, result)

    def resume_held(self, live: list[str]) -> None:
        """Resume the sessions whose lease this host holds and that need a
        process (runner mode)."""
        held = self.ctx.api.sessions(
            harness=self.adapter.harness,
            lease_host=self.ctx.host,
            open="1",
            limit="1000",
        )
        for summary in held:
            sid = summary["session_id"]
            prompt = needs_process(summary, self.ctx.host, sid in live)
            if prompt is not None:
                self.resume(sid, prompt)

    def resume(self, sid: str, prompt: str) -> bool:
        """Resume a session as a background job, in the workspace that this
        host recorded for it. Do not resume while its restore is pending, or
        while another process holds its sync lock. Return True if
        `claude --bg` succeeded."""
        now = self.clock()
        recent = self._resumes.setdefault(sid, deque[float]())
        while recent and now - recent[0] >= RESUME_WINDOW:
            recent.popleft()
        if len(recent) >= MAX_RESUMES:
            log.info("%s: resumed %d times in an hour; not again", sid, len(recent))
            return False
        harness = self.adapter.harness
        # Hold the session's sync lock from the checks until `claude --bg`
        # returns, so that no restore or sync starts between them. The new
        # process does not inherit the lock, and its hooks wait for the lock
        # only for a limited time.
        with self._hold(sid) as locked:
            if not locked:
                log.info("%s: not resumed: a sync or a restore of it runs", sid)
                return False
            if self.ctx.state.get(f"restore_pending:{harness}:{sid}"):
                # The restore retry must complete the restore first.
                log.info("%s: not resumed: its restore is not complete", sid)
                return False
            workspace = self.ctx.state.session(harness, sid).workspace
            # Only a session with a local transcript under this id, so that
            # the id is a valid file name and `claude --resume` finds it.
            if (
                not workspace
                or not os.path.isabs(workspace)
                or not os.path.isdir(workspace)
                or not self.adapter.find_transcripts(sid)
            ):
                log.warning("%s: not resumed: no local copy or no workspace", sid)
                return False
            # Claude Code starts a copy under a new id if the session runs.
            if self.adapter.is_running(sid):
                return False
            recent.append(now)
            argv = [self.claude, *self.claude_args, "--bg", "--resume", sid, prompt]
            result = self.run_claude(argv, workspace, RESUME_TIMEOUT)
        if result.returncode == 0:
            log.info("%s: resumed in %s", sid, workspace)
            return True
        first = next(iter(printable(result.output).splitlines()), "")
        log.warning("%s: resume failed (%s): %s", sid, result.returncode, first)
        return False

    @contextmanager
    def _hold(self, sid: str) -> Iterator[bool]:
        """Hold the session's sync lock as `try_lock` does. After the release,
        start a checkpoint if the session is dirty. A checkpoint that started
        during the hold did not get the lock, so it did not sync. No other
        process syncs for its mark (see "Checkpoint scheduling" in the
        spec)."""
        locked = False
        try:
            with self.ctx.state.try_lock(self.adapter.harness, sid) as locked:
                yield locked
        finally:
            if locked:
                self._checkpoint_if_dirty(sid)

    def _checkpoint_if_dirty(self, sid: str) -> None:
        harness = self.adapter.harness
        if self.ctx.state.dirty_since(harness, sid) is None:
            return
        # A dirty session has a transcript path, because the checkpoint sync
        # records the path before it marks the session.
        transcript = self.ctx.state.session(harness, sid).transcript
        if not transcript:
            return
        try:
            self.start(["--quiet", "sync", "--checkpoint", "--transcript", transcript])
        except OSError as exc:
            log.warning("%s: no checkpoint after the resume: %s", sid, exc)

    def tick(self) -> list[str]:
        """One heartbeat and one sweep, and in runner mode the restores and
        the resumes. Return the live sessions."""
        live = self.adapter.live_sessions(
            self.ctx.state.continued_from(self.adapter.harness)
        )
        try:
            if self.before_tick is not None:
                self.before_tick()
            # Send the heartbeat before the resumes, so that a stale `live`
            # from before a restart clears.
            send_heartbeat(self.ctx, self.adapter, live, self.interval)
            self.sweep(live)
            if self.runner:
                self.restore()
                self.resume_held(live)
        except (ApiError, httpx.HTTPError, OSError) as exc:
            # The next tick tries again.
            log.warning("launcher: %s: %s", type(exc).__name__, exc)
        return live

    def restore(self) -> None:
        """Runner mode: restore the held sessions at the first tick. After a
        failure, try again at later ticks with a doubling wait, until a
        restore has no failure."""
        if self._restore_at is None or self.clock() < self._restore_at:
            return
        try:
            result = restore_held(self.ctx, self.adapter, self.workspace)
        except (ApiError, httpx.HTTPError, OSError) as exc:
            log.warning("launcher: restore of held sessions: %s", exc)
        else:
            log.info("launcher: restored %d held sessions", result.restored)
            if not result.failed:
                self._restore_at = None
                return
        wait = max(self._restore_wait * 2, self.interval)
        self._restore_wait = min(wait, RESTORE_RETRY_MAX)
        self._restore_at = self.clock() + self._restore_wait
        log.info("launcher: restore again in %d s", self._restore_wait)

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
                        self.ctx, self.adapter, [], self.interval, final=True
                    )
                except (ApiError, httpx.HTTPError) as exc:
                    log.warning("launcher: last heartbeat: %s", exc)
                log.info("launcher: no live session; stopping")
                return 0
            self.sleep(self.interval)
        return 0
