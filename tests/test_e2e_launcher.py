# vim: filetype=python
"""The launcher: heartbeats, idle exit, the close of `done` background jobs,
and the resumes in runner mode, against an in-process server. A fake
`claude rm` and a fake `claude --bg` record their calls; the real `claude` and
the real ~/.claude are never used."""

from __future__ import annotations

import json
import os
import signal
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from _hs import FakeClaude, Server, make_server, prompt, text_reply
from hyperspace_client import launcher as launcher_mod
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api, ApiError
from hyperspace_client.backfill import RunResult
from hyperspace_client.cli import _hook_heartbeat, main, status_label
from hyperspace_client.config import Config, ConfigError, load
from hyperspace_client.launcher import (
    MAX_RESUMES,
    RESUME_WAITING,
    RESUME_WORKING,
    START_RETRY,
    Launcher,
    close_after_exit,
    close_job,
    host_lock,
    load_unclosed,
    needs_process,
    record_refusal,
    run_bg,
    run_rm,
)
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer
from hyperspace_client.teleport import HeldRestore, TeleportError, restore
from hyperspace_protocol import SessionStateBody, SessionSummary

WS = "/home/wlritchi/hyperspace"
HARNESS = "claude-code"
FAKE = str(Path(__file__).resolve().parent / "fake_claude.py")


class Host(ClaudeCode):
    """A Claude Code config directory whose live sessions the test sets."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.live: list[str] = []

    def live_sessions(self) -> list[str]:
        return list(self.live)


class Clock:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps = 0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self.t += seconds


class Rm:
    """A fake `claude rm` that records each call and the server's length of
    the session's transcript at that time."""

    def __init__(self, ctx: Context, code: int = 0, output: str = "") -> None:
        self.ctx = ctx
        self.code = code
        self.output = output
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> tuple[int, str]:
        self.calls.append(argv)
        return self.code, self.output


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


@pytest.fixture
def fake(tmp_path: Path) -> FakeClaude:
    return FakeClaude(tmp_path / "claude")


@pytest.fixture
def host(fake: FakeClaude) -> Host:
    return Host(fake.root)


@pytest.fixture
def ctx(server: Server, host: Host, tmp_path: Path) -> Iterator[Context]:
    c = Context(
        api=server.api("laptop"),
        state=State(tmp_path / "state"),
        adapter=host,
        host="laptop",
    )
    yield c
    c.state.close()


def session(ctx: Context, fake: FakeClaude, *records: dict[str, Any]) -> str:
    """A session with `records`, synced to the server. Return its id."""
    path = fake.transcript(WS)
    fake.write(path, *(records or (prompt("hi"), text_reply())))
    found = ctx.adapter.session_for_transcript(path)
    assert found is not None
    Syncer(ctx, found, WS).run()
    return found.session_id


def mark(ctx: Context, sid: str, status: str) -> None:
    current = ctx.api.state(HARNESS, sid).get("status")
    body: dict[str, Any] = {
        "status": status,
        "source_kind": "user",
        "explicit": True,
        "base_seq": current["seq"] if current else 0,
    }
    if status == "ready":
        body["reason"] = "idle"
    ctx.api.add_status(HARNESS, sid, body)  # pyright: ignore[reportArgumentType]


def job(fake: FakeClaude, short: str, sid: str, **extra: Any) -> Path:
    directory = fake.root / "jobs" / short
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "state.json"
    path.write_text(json.dumps({"sessionId": sid, **extra}))
    return path


def state(ctx: Context, sid: str) -> SessionStateBody:
    return ctx.api.state(HARNESS, sid)


def launcher(ctx: Context, host: Host, rm: Rm, clock: Clock, **kw: Any) -> Launcher:
    return Launcher(
        ctx,
        host,
        "claude",
        60,
        kw.pop("idle_exit", None),
        run_rm=rm,
        sleep=clock.sleep,
        clock=clock,
        **kw,
    )


def test_heartbeat_makes_session_live_then_stopped(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    clock = Clock()
    run = launcher(ctx, host, Rm(ctx), clock)
    host.live = [sid]
    run.tick()
    live = state(ctx, sid)["liveness"]
    assert (live["state"], live["host"]) == ("live", "laptop")
    host.live = []
    run.tick()
    assert state(ctx, sid)["liveness"]["state"] == "stopped"


def test_config_dirs_of_one_host_keep_their_own_liveness(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    first = session(ctx, fake)
    fake_b = FakeClaude(tmp_path / "claude-b")
    host_b = Host(fake_b.root)
    ctx_b = Context(api=ctx.api, state=ctx.state, adapter=host_b, host="laptop")
    second = session(ctx_b, fake_b)
    host.live, host_b.live = [first], [second]
    run_a = launcher(ctx, host, Rm(ctx), Clock())
    clock_b = Clock()
    run_b = launcher(ctx_b, host_b, Rm(ctx_b), clock_b, idle_exit=60.0)
    run_a.tick()
    run_b.tick()
    for sid in (first, second):
        assert state(ctx, sid)["liveness"]["state"] == "live"
    # A refusal in one config directory stays when the other one sweeps.
    record_refusal(ctx.state, host, job_for(host, fake, "aa", first), "unpushed")
    run_b.tick()
    assert set(load_unclosed(ctx.state, host)) == {"aa"}
    # The second launcher stops for idle; its last heartbeat ends only its
    # own sessions.
    host_b.live = []
    assert run_b.run(max_ticks=5) == 0
    assert state(ctx, second)["liveness"]["state"] == "stopped"
    assert state(ctx, first)["liveness"]["state"] == "live"


def test_idle_exit_sends_final_heartbeat(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    clock = Clock()
    host.live = [sid]
    run = launcher(ctx, host, Rm(ctx), clock, idle_exit=120.0)
    run.tick()
    host.live = []
    assert run.run(max_ticks=10) == 0
    assert clock.sleeps == 2
    assert state(ctx, sid)["liveness"]["state"] == "stopped"


def test_stay_never_exits(ctx: Context, host: Host) -> None:
    clock = Clock()
    run = launcher(ctx, host, Rm(ctx), clock, idle_exit=None)
    assert run.run(max_ticks=30) == 0
    assert clock.sleeps == 30


def test_close_sweep_skips_live_and_not_done(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    live, ready, done, unset = (session(ctx, fake) for _ in range(4))
    for sid in (live, done):
        mark(ctx, sid, "done")
    mark(ctx, ready, "ready")
    for short, sid in (("aa", live), ("bb", ready), ("cc", done), ("dd", unset)):
        job(fake, short, sid)
    job(fake, "ee", "99999999-9999-9999-9999-999999999999")
    host.live = [live]
    rm = Rm(ctx)
    launcher(ctx, host, rm, Clock()).tick()
    assert rm.calls == [["claude", "rm", "cc"]]


def test_close_syncs_before_rm(ctx: Context, fake: FakeClaude, host: Host) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    (path,) = host.find_transcripts(sid)
    fake.write(path, text_reply("the last words"))
    size = path.stat().st_size
    lengths: list[int] = []

    def rm(argv: list[str]) -> tuple[int, str]:
        manifest = ctx.api.manifest(HARNESS, sid)
        (entry,) = [f for f in manifest["files"] if f["path"] == "transcript.jsonl"]
        assert entry["kind"] == "log"
        active = next(g for g in entry["generations"] if g["id"] == entry["active"])
        lengths.append(active["length"])
        return 0, ""

    close_job(ctx, host, job_for(host, fake, "aa", sid), "claude", rm)
    assert lengths == [size]


def test_close_skips_a_queued_reopen(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    done_seq = state(ctx, sid)["status"]["seq"]
    # The user prompted while the server was down: the reopen waits in the
    # queue, and the server still says `done`.
    ctx.state.want_status(
        HARNESS,
        sid,
        {
            "status": "working",
            "source_kind": "hook",
            "explicit": True,
            "base_seq": done_seq,
        },
    )
    job(fake, "aa", sid)
    rm = Rm(ctx)
    launcher(ctx, host, rm, Clock()).tick()
    assert rm.calls == []


def test_close_asks_again_after_the_backup(
    ctx: Context, fake: FakeClaude, host: Host, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    original = launcher_mod.back_up

    def back_up(*args: Any) -> None:
        original(*args)
        mark(ctx, sid, "ready")

    monkeypatch.setattr(launcher_mod, "back_up", back_up)
    rm = Rm(ctx)
    close_job(ctx, host, job_for(host, fake, "aa", sid), "claude", rm)
    assert rm.calls == []


def job_for(host: Host, fake: FakeClaude, short: str, sid: str) -> Any:
    job(fake, short, sid)
    return next(j for j in host.jobs() if j.short == short)


def test_rm_refusal_is_reported_once(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    path = job(fake, "aa", sid)
    rm = Rm(ctx, 1, "\x1b[31merror:\x1b[0m worktree has unpushed commits\nmore\n")
    clock = Clock()
    run = launcher(ctx, host, rm, clock)
    run.tick()
    run.tick()
    clock.t += 10_000
    run.tick()
    assert len(rm.calls) == 1
    blocked = state(ctx, sid)["liveness"]["close_blocked"]
    assert blocked == "error: worktree has unpushed commits"
    assert status_label(state(ctx, sid), 0.0, 1e9) == f"done, not closed: {blocked}"
    # A change of the job's state lets the sweep try again.
    path.write_text(json.dumps({"sessionId": sid, "state": "stopped"}))
    rm.code = 0
    run.tick()
    assert len(rm.calls) == 2
    assert load_unclosed(ctx.state, host) == {}


def test_close_job_waits_for_the_process(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    job(fake, "aa", sid)
    host.live = [sid]
    clock = Clock()

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        if clock.sleeps == 3:
            host.live = []

    rm = Rm(ctx)
    close_after_exit(ctx, host, "aa", "claude", rm, sleep=sleep, clock=clock)
    assert rm.calls == [["claude", "rm", "aa"]]


def test_close_job_gives_up_on_a_running_session(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    job(fake, "aa", sid)
    host.live = [sid]
    clock = Clock()
    rm = Rm(ctx)
    close_after_exit(
        ctx, host, "aa", "claude", rm, wait=30, sleep=clock.sleep, clock=clock
    )
    assert rm.calls == []


def test_hook_heartbeat_leaves_out_the_ending_session(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    ending, other = session(ctx, fake), session(ctx, fake)
    host.live = [ending, other]
    launcher(ctx, host, Rm(ctx), Clock()).tick()
    cfg = Config(host="laptop")
    _hook_heartbeat(cfg, ctx, ending, "SessionEnd")
    assert state(ctx, ending)["liveness"]["state"] == "stopped"
    assert state(ctx, other)["liveness"]["state"] == "live"


def test_jobs_skips_unsafe_names(fake: FakeClaude, host: Host) -> None:
    sid = "11111111-1111-1111-1111-111111111111"
    job(fake, "1111aaaa", sid)
    job(fake, "--discard-unpushed", sid)
    job(fake, "bb", "../x")
    (fake.root / "jobs" / "cc").mkdir()
    (fake.root / "jobs" / "cc" / "state.json").write_text("{}")
    assert [(j.short, j.session_id) for j in host.jobs()] == [("1111aaaa", sid)]


def test_single_instance_lock(tmp_path: Path) -> None:
    with host_lock(tmp_path, "laptop", tmp_path / "a") as first:
        assert first
        with host_lock(tmp_path, "laptop", tmp_path / "a") as second:
            assert not second
        with host_lock(tmp_path, "laptop", tmp_path / "b") as other:
            assert other
    with host_lock(tmp_path, "laptop", tmp_path / "a") as again:
        assert again


def test_off_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HYPERSPACE_LAUNCHER", "off")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("HYPERSPACE_CONFIG", str(tmp_path / "none.toml"))
    # With no server URL, a launcher that did not stop at once would fail.
    assert main(["launcher", "--stay"]) == 0
    assert not (tmp_path / "state" / "hyperspace" / "locks").exists()


class Flaky:
    """Make the first calls of a method of `api` raise `errors`, in order."""

    def __init__(self, api: Api, name: str, *errors: Exception) -> None:
        self.real = getattr(api, name)
        self.errors = list(errors)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.errors:
            raise self.errors.pop(0)
        return self.real(*args, **kwargs)


def start_launcher(
    monkeypatch: pytest.MonkeyPatch,
    server: Server,
    tmp_path: Path,
    argv: list[str],
    **errors: list[Exception],
) -> tuple[int, list[float], list[str]]:
    """Run `hyperspace launcher` with the in-process server, where the API
    methods in `errors` fail first. Return the exit code, the sleeps, and
    the modes of the launcher that ran."""
    for name, value in {
        "XDG_STATE_HOME": str(tmp_path / "xdg"),
        "HYPERSPACE_CONFIG": str(tmp_path / "none.toml"),
        "HYPERSPACE_URL": "http://127.0.0.1:9",
        "HYPERSPACE_TOKEN": "unused",
        "HYPERSPACE_HOST": "laptop",
        "HYPERSPACE_HOST_KIND": "runner",
        "CLAUDE_CONFIG_DIR": str(tmp_path / "claude"),
    }.items():
        monkeypatch.setenv(name, value)
    api = server.api("laptop")
    for name, failures in errors.items():
        monkeypatch.setattr(api, name, Flaky(api, name, *failures))

    def context(cfg: Config, state: State) -> Context:
        return Context(api=api, state=state, adapter=ClaudeCode(), host=cfg.host)

    sleeps: list[float] = []
    ran: list[str] = []

    def run(self: Launcher, max_ticks: int | None = None) -> int:
        ran.append("runner" if self.runner else "workstation")
        return 0

    monkeypatch.setattr("hyperspace_client.cli._context", context)
    monkeypatch.setattr(Launcher, "run", run)
    monkeypatch.setattr("time.sleep", sleeps.append)
    return main(["launcher", *argv]), sleeps, ran


@pytest.mark.parametrize("argv", [["--runner"], ["--stay"]])
def test_launcher_waits_for_the_server_at_start(
    monkeypatch: pytest.MonkeyPatch, server: Server, tmp_path: Path, argv: list[str]
) -> None:
    code, sleeps, ran = start_launcher(
        monkeypatch,
        server,
        tmp_path,
        argv,
        register_host=[
            httpx.ConnectError("refused"),
            # The HTTPS proxy is not available.
            httpx.ProxyError("503 Service Unavailable"),
            ApiError(502, "bad_gateway"),
        ],
        whoami=[httpx.ReadTimeout("timeout")],
    )
    assert code == 0
    assert ran == ["runner" if argv == ["--runner"] else "workstation"]
    assert sleeps == [START_RETRY, 2 * START_RETRY, 4 * START_RETRY, 8 * START_RETRY]


def test_launcher_start_fails_at_once_for_a_bad_credential(
    monkeypatch: pytest.MonkeyPatch, server: Server, tmp_path: Path
) -> None:
    code, sleeps, ran = start_launcher(
        monkeypatch,
        server,
        tmp_path,
        ["--runner"],
        register_host=[ApiError(401, "unauthorized")],
    )
    assert (code, sleeps, ran) == (1, [], [])


@pytest.mark.parametrize(
    "error",
    [
        # For example, a token with a newline is not a valid header value.
        httpx.LocalProtocolError("Illegal header value"),
        httpx.UnsupportedProtocol("Request URL has an unsupported protocol"),
        httpx.TooManyRedirects("Exceeded maximum allowed redirects."),
        httpx.ProxyError("407 Proxy Authentication Required"),
        httpx.ProxyError("Invalid username/password"),
        httpx.DecodingError("Error -3 while decompressing data"),
        PermissionError("permission denied"),
    ],
)
def test_launcher_start_fails_at_once_for_a_local_error(
    monkeypatch: pytest.MonkeyPatch, server: Server, tmp_path: Path, error: Exception
) -> None:
    code, sleeps, ran = start_launcher(
        monkeypatch, server, tmp_path, ["--runner"], register_host=[error]
    )
    assert (code, sleeps, ran) == (1, [], [])


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ConnectError("refused"), True),
        (httpx.ConnectTimeout("timeout"), True),
        (httpx.ReadTimeout("timeout"), True),
        (httpx.WriteTimeout("timeout"), True),
        (httpx.PoolTimeout("timeout"), True),
        (httpx.ReadError("reset"), True),
        (httpx.WriteError("broken pipe"), True),
        (httpx.RemoteProtocolError("Server disconnected"), True),
        (httpx.ProxyError("502 Bad Gateway"), True),
        (httpx.ProxyError("503 Service Unavailable"), True),
        (httpx.ProxyError("504 Gateway Timeout"), True),
        (httpx.ProxyError("Proxy Server could not connect: Connection refused."), True),
        (httpx.ProxyError("407 Proxy Authentication Required"), False),
        (httpx.ProxyError("407"), False),
        (httpx.ProxyError("Invalid username/password"), False),
        (ApiError(500, "error"), True),
        (ApiError(503, "unavailable"), True),
        (ApiError(408, "timeout"), True),
        (ApiError(429, "rate_limited"), True),
        (ApiError(400, "bad_request"), False),
        (ApiError(401, "unauthorized"), False),
        (ApiError(403, "forbidden"), False),
        (ApiError(404, "not_found"), False),
        (httpx.LocalProtocolError("Illegal header value"), False),
        (httpx.UnsupportedProtocol("unsupported"), False),
        (httpx.TooManyRedirects("redirects"), False),
        (httpx.InvalidURL("invalid"), False),
        (OSError("no space left on device"), False),
    ],
)
def test_temporary_errors(error: Exception, expected: bool) -> None:
    assert launcher_mod.temporary(error) is expected


def test_workstation_launcher_start_does_not_wait(
    monkeypatch: pytest.MonkeyPatch, server: Server, tmp_path: Path
) -> None:
    code, sleeps, ran = start_launcher(
        monkeypatch,
        server,
        tmp_path,
        [],
        register_host=[httpx.ConnectError("refused")],
    )
    assert (code, sleeps, ran) == (1, [], [])


def test_launcher_start_needs_a_server_url(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HYPERSPACE_CONFIG", str(tmp_path / "none.toml"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    monkeypatch.delenv("HYPERSPACE_URL", raising=False)
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    assert main(["launcher", "--runner"]) == 2
    assert sleeps == []


def test_run_rm_reports_the_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "fail")
    code, output = run_rm([sys.executable, FAKE, "rm", "aa"])
    assert code == 1
    assert "worktree has unpushed commits" in output


# --- Runner mode -------------------------------------------------------------


class Bg:
    """A fake `claude --bg --resume` that records each call and its cwd."""

    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.calls: list[tuple[list[str], str]] = []

    def __call__(self, argv: list[str], cwd: str, timeout: float) -> RunResult:
        self.calls.append((argv, cwd))
        return RunResult(self.code, "" if self.code == 0 else "no such session")


def set_status(ctx: Context, sid: str, status: str, **extra: str) -> None:
    current = ctx.api.state(HARNESS, sid).get("status")
    body: dict[str, Any] = {
        "status": status,
        "source_kind": "hook",
        "explicit": False,
        "base_seq": current["seq"] if current else 0,
        **extra,
    }
    ctx.api.add_status(HARNESS, sid, body)  # pyright: ignore[reportArgumentType]


def held_session(ctx: Context, fake: FakeClaude, workspace: Path) -> str:
    """A session in `workspace`, synced, so that this host holds its lease."""
    workspace.mkdir(parents=True, exist_ok=True)
    path = fake.transcript(str(workspace))
    fake.write(path, prompt("hi"), text_reply())
    found = ctx.adapter.session_for_transcript(path)
    assert found is not None
    Syncer(ctx, found, str(workspace)).run()
    return found.session_id


def runner(ctx: Context, host: Host, bg: Bg, clock: Clock, **kw: Any) -> Launcher:
    return launcher(
        ctx,
        host,
        Rm(ctx),
        clock,
        runner=True,
        claude_args=kw.pop("claude_args", ["--model", "opus"]),
        workspace=kw.pop("workspace", "/nonexistent"),
        run_claude=bg,
        **kw,
    )


def _summary(
    status: dict[str, Any] | None,
    lease: dict[str, Any] | None = None,
    liveness: dict[str, Any] | None = None,
) -> SessionSummary:
    state: dict[str, Any] = {
        "state": "normal",
        "lease": lease or {"state": "held", "host": "agent-1", "since": "t"},
    }
    if status is not None:
        state["status"] = {
            "seq": 1,
            "source": {"kind": "hook", "host": "agent-1"},
            "explicit": False,
            "received": "t",
            **status,
        }
    if liveness is not None:
        state["liveness"] = liveness
    return {  # pyright: ignore[reportReturnType]
        "harness": HARNESS,
        "session_id": "s1",
        "metadata": {},
        "state": state,
    }


@pytest.mark.parametrize(
    ("summary", "running", "expected"),
    [
        (_summary({"status": "working"}), False, RESUME_WORKING),
        (_summary({"status": "working"}), True, None),
        (_summary({"status": "waiting", "wake": "local"}), False, RESUME_WAITING),
        (_summary({"status": "waiting", "wake": "message"}), False, None),
        (_summary({"status": "ready", "reason": "asked"}), False, None),
        (_summary({"status": "ready", "reason": "idle"}), False, None),
        (_summary({"status": "done"}), False, None),
        (_summary(None), False, None),
        (
            _summary(
                {"status": "working"},
                lease={"state": "reserved", "host": "agent-1", "since": "t"},
            ),
            False,
            None,
        ),
        (
            _summary(
                {"status": "working"},
                lease={"state": "held", "host": "laptop", "since": "t"},
            ),
            False,
            None,
        ),
        (
            _summary(
                {"status": "working"},
                liveness={"state": "live", "host": "laptop", "since": "t"},
            ),
            False,
            None,
        ),
        (
            _summary(
                {"status": "working"},
                liveness={"state": "live", "host": "agent-1", "since": "t"},
            ),
            False,
            None,
        ),
        (
            _summary(
                {"status": "working"},
                liveness={"state": "stopped", "host": "laptop", "since": "t"},
            ),
            False,
            RESUME_WORKING,
        ),
    ],
)
def test_needs_process_table(
    summary: SessionSummary, running: bool, expected: str | None
) -> None:
    assert needs_process(summary, "agent-1", running) == expected


def test_boot_resumes_interrupted_only(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    ws = tmp_path / "ws"
    working, local, message, ready, done, unset = (
        held_session(ctx, fake, ws) for _ in range(6)
    )
    set_status(ctx, working, "working")
    set_status(ctx, local, "waiting", wake="local")
    set_status(ctx, message, "waiting", wake="message")
    set_status(ctx, ready, "ready", reason="asked")
    set_status(ctx, done, "done")
    bg = Bg()
    assert runner(ctx, host, bg, Clock()).run(max_ticks=1) == 0
    calls = {argv[5]: (argv, cwd) for argv, cwd in bg.calls}
    assert set(calls) == {working, local}
    argv, cwd = calls[working]
    assert argv == [
        "claude",
        "--model",
        "opus",
        "--bg",
        "--resume",
        working,
        RESUME_WORKING,
    ]
    assert calls[local][0][-1] == RESUME_WAITING
    assert cwd == str(ws)
    assert unset not in calls


def test_running_session_is_not_resumed(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    host.live = [sid]
    bg = Bg()
    run = runner(ctx, host, bg, Clock())
    run.tick()
    assert bg.calls == []
    # The process stops: the next tick resumes the session.
    host.live = []
    run.tick()
    assert [argv[5] for argv, _ in bg.calls] == [sid]


def test_session_live_from_another_config_dir_is_not_resumed(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    # A second config directory on this host runs the session. The
    # launcher's own `is_running` cannot see that process.
    other = Host(tmp_path / "other-claude")
    launcher_mod.send_heartbeat(ctx, other, [sid], 60)
    assert state(ctx, sid)["liveness"]["host"] == "laptop"  # pyright: ignore[reportTypedDictNotRequiredAccess]
    bg = Bg()
    run = runner(ctx, host, bg, Clock())
    run.tick()
    assert bg.calls == []
    # The other process stops: the next tick resumes the session.
    launcher_mod.send_heartbeat(ctx, other, [], 60)
    run.tick()
    assert [argv[5] for argv, _ in bg.calls] == [sid]


def test_own_live_from_before_a_restart_does_not_block_the_resume(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    # The heartbeat of this config directory before the runner restarted.
    launcher_mod.send_heartbeat(ctx, host, [sid], 60)
    bg = Bg()
    runner(ctx, host, bg, Clock()).tick()
    assert [argv[5] for argv, _ in bg.calls] == [sid]


def test_resume_limit(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    bg = Bg(code=1)
    clock = Clock()
    run = runner(ctx, host, bg, clock)
    for _ in range(MAX_RESUMES + 3):
        run.tick()
        clock.t += 60
    assert len(bg.calls) == MAX_RESUMES
    clock.t += 3600
    run.tick()
    assert len(bg.calls) == MAX_RESUMES + 1


def test_resume_needs_a_local_copy_and_workspace(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    ws = tmp_path / "ws"
    sid = held_session(ctx, fake, ws)
    set_status(ctx, sid, "working")
    bg = Bg()
    run = runner(ctx, host, bg, Clock())
    ws.rmdir()
    run.tick()
    assert bg.calls == []
    ws.mkdir()
    for path in host.find_transcripts(sid):
        path.unlink()
    run.tick()
    assert bg.calls == []


def test_boot_restores_held_sessions(
    ctx: Context, host: Host, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def restore_held(c: Context, adapter: ClaudeCode, workspace: str) -> HeldRestore:
        calls.append(workspace)
        return HeldRestore(0, [])

    monkeypatch.setattr("hyperspace_client.launcher.restore_held", restore_held)
    runner(ctx, host, Bg(), Clock(), workspace="/w").run(max_ticks=3)
    launcher(ctx, host, Rm(ctx), Clock()).run(max_ticks=1)
    assert calls == ["/w"]


def fresh_runner(
    ctx: Context, tmp_path: Path, bg: Bg, clock: Clock
) -> tuple[Launcher, Host]:
    """A runner on the same host whose config and state directories are new
    and empty, as after a pod restart."""
    fresh = Host(tmp_path / "claude-2")
    c = Context(
        api=ctx.api, state=State(tmp_path / "state-2"), adapter=fresh, host=ctx.host
    )
    return runner(c, fresh, bg, clock), fresh


@pytest.mark.parametrize("failure", ["list", "session"])
def test_failed_boot_restore_is_retried(
    ctx: Context,
    fake: FakeClaude,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    bg = Bg()
    clock = Clock()
    run, fresh = fresh_runner(ctx, tmp_path, bg, clock)
    api = run.ctx.api
    sessions, manifest = api.sessions, api.manifest
    fails = [1]

    def flaky_sessions(**filters: Any) -> Any:
        # Only the unfiltered list of `restore_held`.
        if failure == "list" and "lease_host" not in filters and fails:
            fails.pop()
            raise httpx.ConnectError("server not available")
        return sessions(**filters)

    def flaky_manifest(*args: Any) -> Any:
        if failure == "session" and fails:
            fails.pop()
            raise ApiError(503, "unavailable")
        return manifest(*args)

    monkeypatch.setattr(api, "sessions", flaky_sessions)
    monkeypatch.setattr(api, "manifest", flaky_manifest)
    run.run(max_ticks=1)
    assert not fails
    assert fresh.find_transcripts(sid) == []
    assert bg.calls == []
    # The server is available again. A later tick restores and resumes the
    # session, with no restart of the launcher.
    run.tick()
    assert fresh.find_transcripts(sid) == [
        fresh.project_dir(str(tmp_path / "ws")) / f"{sid}.jsonl"
    ]
    assert [(argv[5], cwd) for argv, cwd in bg.calls] == [(sid, str(tmp_path / "ws"))]
    run.ctx.state.close()


def test_restore_retry_backs_off(
    ctx: Context, host: Host, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[float] = []
    clock = Clock()

    def restore_held(c: Context, adapter: ClaudeCode, workspace: str) -> HeldRestore:
        calls.append(clock.t)
        if len(calls) < 4:
            return HeldRestore(0, ["s1"])
        return HeldRestore(1, [])

    monkeypatch.setattr("hyperspace_client.launcher.restore_held", restore_held)
    run = runner(ctx, host, Bg(), clock)
    run.run(max_ticks=20)
    # One tick each 60 s; the waits are 60, 120 and 240 s, then no more
    # restores after one with no failure.
    assert calls == [0, 60, 180, 420]


def test_launcher_claude_args_config(tmp_path: Path) -> None:
    none = tmp_path / "none.toml"
    env = {"HYPERSPACE_LAUNCHER_CLAUDE_ARGS": '["--model", "opus", "--agent", "a"]'}
    assert load(env, none).launcher_claude_args == ["--model", "opus", "--agent", "a"]
    assert load({}, none).launcher_claude_args == []
    toml = tmp_path / "config.toml"
    toml.write_text('launcher_claude_args = ["--model", "sonnet"]\n')
    assert load({}, toml).launcher_claude_args == ["--model", "sonnet"]
    for bad in (
        "not json",
        '"--model"',
        "[1]",
        '[""]',
        '["--resume", "x"]',
        '["--resume=x"]',
        '["--fork-session"]',
        '["-p"]',
        '["--bg"]',
        '["--", "x"]',
        '["-pc"]',
        '["-rabc"]',
        '["-p=x"]',
    ):
        with pytest.raises(ConfigError):
            load({"HYPERSPACE_LAUNCHER_CLAUDE_ARGS": bad}, none)


def test_pending_restore_blocks_the_resume(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ws = tmp_path / "ws"
    sid = held_session(ctx, fake, ws)
    set_status(ctx, sid, "working")
    teleport = ctx.api.teleport

    def lost(*args: Any) -> Any:
        raise httpx.ReadTimeout("timeout")

    # A restore writes the files, but cannot confirm its teleport request.
    monkeypatch.setattr(ctx.api, "teleport", lost)
    with pytest.raises(TeleportError, match="confirmation"):
        restore(ctx, host, sid, str(ws))
    assert ctx.state.get(f"restore_pending:{HARNESS}:{sid}")
    assert host.find_transcripts(sid)
    bg = Bg()
    clock = Clock()
    run = runner(ctx, host, bg, clock)
    # The restore retry fails again. The runner does not resume.
    run.tick()
    assert ctx.state.get(f"restore_pending:{HARNESS}:{sid}")
    assert bg.calls == []
    # The restore retry confirms the restore. The runner resumes.
    monkeypatch.setattr(ctx.api, "teleport", teleport)
    clock.t += 60
    run.tick()
    assert not ctx.state.get(f"restore_pending:{HARNESS}:{sid}")
    assert [(argv[5], cwd) for argv, cwd in bg.calls] == [(sid, str(ws))]


def test_no_resume_while_another_process_holds_the_session_lock(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    bg = Bg()
    run = runner(ctx, host, bg, Clock())
    # For example, a restore or a sync of the session.
    other = State(tmp_path / "state")
    try:
        with other.try_lock(HARNESS, sid) as locked:
            assert locked
            run.tick()
            assert bg.calls == []
    finally:
        other.close()
    run.tick()
    assert [argv[5] for argv, _ in bg.calls] == [sid]


def test_resume_holds_the_session_lock_only_through_the_launch(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    pidfile = tmp_path / "daemon.pid"
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "daemon")
    monkeypatch.setenv("FAKE_CLAUDE_PIDFILE", str(pidfile))
    held: list[bool] = []

    def bg(argv: list[str], cwd: str, timeout: float) -> RunResult:
        with ctx.state.try_lock(HARNESS, sid) as free:
            held.append(not free)
        # The fake starts a daemon that continues to run, as `claude --bg` can.
        return run_bg([sys.executable, FAKE, *argv[1:]], cwd, timeout)

    run = runner(ctx, host, Bg(), Clock())
    run.run_claude = bg
    run.tick()
    daemon = int(pidfile.read_text())
    try:
        assert held == [True]
        # The daemon must not keep the lock, because the hooks of the session
        # need it.
        with ctx.state.try_lock(HARNESS, sid) as free:
            assert free
        os.kill(daemon, 0)
    finally:
        os.kill(daemon, signal.SIGKILL)
