# vim: filetype=python
"""The launcher: heartbeats, idle exit, the close of `done` background jobs,
and the resumes in runner mode, against an in-process server. A fake
`claude rm` and a fake `claude --bg` record their calls; the real `claude` and
the real ~/.claude are never used."""

from __future__ import annotations

import json
import logging
import os
import shlex
import signal
import socket
import sys
import threading
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from _hs import FakeClaude, Server, make_server, prompt, text_reply
from hyperspace_client import launcher as launcher_mod
from hyperspace_client.adapters import claude_code as claude_code_mod
from hyperspace_client.adapters.claude_code import ClaudeCode, ContinuedFrom
from hyperspace_client.api import Api, ApiError
from hyperspace_client.backfill import Runner, RunResult
from hyperspace_client.cli import (
    _hook_heartbeat,
    _print_status_table,
    main,
    status_label,
)
from hyperspace_client.config import Config, ConfigError, check_claude_args, load
from hyperspace_client.hooks import handle
from hyperspace_client.launcher import (
    IDLE_STOP,
    MAX_RESUMES,
    RECHECK,
    RESUME_WAITING,
    RESUME_WAKE,
    RESUME_WORKING,
    START_CHECK,
    START_RETRY,
    Launcher,
    close_after_exit,
    close_job,
    host_lock,
    launch_argv,
    launch_job_id,
    launch_session,
    load_unclosed,
    needs_process,
    record_refusal,
    run_bg,
    run_rm,
    save_launch_links,
)
from hyperspace_client.notices import session_start_notice
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer, register_host
from hyperspace_client.teleport import HeldRestore, TeleportError, restore
from hyperspace_protocol import Launch, SessionStateBody, SessionSummary
from store import SessionKey

WS = "/home/wlritchi/hyperspace"
HARNESS = "claude-code"
FAKE = str(Path(__file__).resolve().parent / "fake_claude.py")


class Host(ClaudeCode):
    """A Claude Code config directory whose live sessions the test sets."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.live: list[str] = []
        # Session id to (job name, end of its turn) of an idle process.
        self.idle: dict[str, tuple[str, float]] = {}

    def idle_since(self, session_id: str) -> tuple[str, float] | None:
        return self.idle.get(session_id)

    def live_processes(
        self, continued_from: ContinuedFrom | None = None
    ) -> list[list[str]]:
        return [[sid] for sid in self.live]

    def is_running(self, session_id: str) -> bool:
        return session_id in self.live


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
    s = make_server(
        tmp_path / "server",
        hosts={
            "laptop": ("laptop", {"append", "read", "grant", "launch"}, "all"),
            "agent": ("agent-1", {"append", "read", "grant"}, "granted"),
            "sender": ("sender", {"wake"}, "own"),
        },
    )
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


def no_start(args: list[str]) -> None:
    raise AssertionError(f"the launcher started {args}")


def launcher(ctx: Context, host: Host, rm: Rm, clock: Clock, **kw: Any) -> Launcher:
    kw.setdefault("start", no_start)
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


def test_the_sweep_stops_the_idle_job_of_a_done_session(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    job(fake, "aa", sid)
    host.live = [sid]
    host.idle[sid] = ("aa", 1000.0)
    now = [1000.0 + IDLE_STOP - 1]
    rm = Rm(ctx)
    run = launcher(ctx, host, rm, Clock(), runner=True, wall=lambda: now[0])
    # The turn ended too recently.
    run.sweep(host.live)
    assert rm.calls == []
    now[0] += 1
    run.sweep(host.live)
    assert rm.calls == [["claude", "stop", "aa"]]
    # The process stopped: the sweep closes the job as usual.
    host.live, host.idle = [], {}
    run.sweep(host.live)
    assert rm.calls[1:] == [["claude", "rm", "aa"]]


def test_a_workstation_does_not_stop_an_idle_done_job(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    job(fake, "aa", sid)
    host.live = [sid]
    host.idle[sid] = ("aa", 0.0)
    rm = Rm(ctx)
    launcher(ctx, host, rm, Clock(), wall=lambda: IDLE_STOP).sweep(host.live)
    assert rm.calls == []


def test_the_sweep_does_not_stop_an_open_or_busy_session(
    ctx: Context, fake: FakeClaude, host: Host
) -> None:
    ready, busy, other = (session(ctx, fake) for _ in range(3))
    mark(ctx, ready, "ready")
    for sid in (busy, other):
        mark(ctx, sid, "done")
    for short, sid in (("aa", ready), ("bb", busy), ("cc", other)):
        job(fake, short, sid)
    host.live = [ready, busy, other]
    # The idle process of `other` is not the job of the session.
    host.idle = {ready: ("aa", 0.0), other: ("dd", 0.0)}
    clock = Clock()
    rm = Rm(ctx)
    run = launcher(ctx, host, rm, clock, runner=True, wall=lambda: IDLE_STOP)
    run.sweep(host.live)
    assert rm.calls == []
    # A `done` from another host: the next check is after RECHECK.
    mark(ctx, ready, "done")
    clock.t += RECHECK - 1
    run.sweep(host.live)
    assert rm.calls == []
    clock.t += 1
    run.sweep(host.live)
    assert rm.calls == [["claude", "stop", "aa"]]


def test_the_stop_of_an_idle_job_logs_a_failure(
    ctx: Context, fake: FakeClaude, host: Host, caplog: pytest.LogCaptureFixture
) -> None:
    sid = session(ctx, fake)
    mark(ctx, sid, "done")
    job(fake, "aa", sid)
    host.live = [sid]
    host.idle[sid] = ("aa", 0.0)
    rm = Rm(ctx, code=1, output="no such job")
    run = launcher(ctx, host, rm, Clock(), runner=True, wall=lambda: IDLE_STOP)
    run.sweep(host.live)
    assert rm.calls == [["claude", "stop", "aa"]]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == [f"{sid}: claude stop aa exited with status 1: no such job"]


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
    # With no local transcript, the session of the job is not known.
    assert [(j.short, j.own_id, j.session_id) for j in host.jobs()] == [
        ("1111aaaa", sid, None)
    ]


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
    config: str = "",
    **errors: list[Exception],
) -> tuple[int, list[float], list[str]]:
    """Run `hyperspace launcher` with the in-process server and the
    config.toml text `config`, where the API methods in `errors` fail
    first. Return the exit code, the sleeps, and the modes of the launcher
    that ran."""
    config_path = tmp_path / "config.toml"
    if config:
        config_path.write_text(config)
    for name, value in {
        "XDG_STATE_HOME": str(tmp_path / "xdg"),
        "HYPERSPACE_CONFIG": str(config_path),
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
        (
            httpx.ProxyError(
                "Requested USERNAME/PASSWORD from proxy server, "
                "but got NO ACCEPTABLE METHODS."
            ),
            False,
        ),
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


_SOCKS_FAIL = b"\x05%c\x00\x01\x00\x00\x00\x00\x00\x00"


def proxy_error(proxy: str, url: str, replies: list[bytes]) -> httpx.ProxyError:
    """Return the httpx.ProxyError for a request through a fake proxy on
    loopback. For each reply, the proxy first reads one message from the
    client. After the last reply, it closes the connection."""
    listener = socket.create_server(("127.0.0.1", 0))

    def serve() -> None:
        connection, _ = listener.accept()
        with connection:
            for reply in replies:
                connection.recv(4096)
                connection.sendall(reply)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    port = listener.getsockname()[1]
    try:
        with (
            httpx.Client(proxy=proxy.format(port=port), timeout=10) as client,
            pytest.raises(httpx.ProxyError) as caught,
        ):
            client.get(url)
    finally:
        thread.join(timeout=10)
        listener.close()
    return caught.value


@pytest.mark.parametrize(
    ("proxy", "replies", "message", "expected"),
    [
        # The SOCKS proxy refuses the authentication method.
        (
            "socks5://user:secret@127.0.0.1:{port}",
            [b"\x05\xff"],
            (
                "Requested USERNAME/PASSWORD from proxy server, "
                "but got NO ACCEPTABLE METHODS."
            ),
            False,
        ),
        (
            "socks5://127.0.0.1:{port}",
            [b"\x05\xff"],
            (
                "Requested NO AUTHENTICATION REQUIRED from proxy server, "
                "but got NO ACCEPTABLE METHODS."
            ),
            False,
        ),
        # The SOCKS proxy requires a credential that the client does not send.
        (
            "socks5://127.0.0.1:{port}",
            [b"\x05\x02"],
            (
                "Requested NO AUTHENTICATION REQUIRED from proxy server, "
                "but got USERNAME/PASSWORD."
            ),
            False,
        ),
        # The SOCKS proxy refuses the credential.
        (
            "socks5://user:wrong@127.0.0.1:{port}",
            [b"\x05\x02", b"\x01\x01"],
            "Invalid username/password",
            False,
        ),
        # The SOCKS proxy cannot connect to the server.
        (
            "socks5://127.0.0.1:{port}",
            [b"\x05\x00", _SOCKS_FAIL % 1],
            "Proxy Server could not connect: General SOCKS server failure.",
            True,
        ),
        (
            "socks5://user:secret@127.0.0.1:{port}",
            [b"\x05\x02", b"\x01\x00", _SOCKS_FAIL % 5],
            "Proxy Server could not connect: Connection refused.",
            True,
        ),
        # The HTTP proxy refuses the CONNECT.
        (
            "http://127.0.0.1:{port}",
            [
                (
                    b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                    b'Proxy-Authenticate: Basic realm="proxy"\r\n'
                    b"Content-Length: 0\r\n\r\n"
                )
            ],
            "407 Proxy Authentication Required",
            False,
        ),
        *(
            (
                "http://127.0.0.1:{port}",
                [b"HTTP/1.1 %s\r\nContent-Length: 0\r\n\r\n" % status],
                status.decode(),
                True,
            )
            for status in (b"502 Bad Gateway", b"503 Service Unavailable")
        ),
    ],
)
def test_temporary_proxy_errors_from_the_transport(
    proxy: str, replies: list[bytes], message: str, expected: bool
) -> None:
    error = proxy_error(proxy, "https://127.0.0.1:1/", replies)
    assert str(error) == message
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

    def __call__(
        self, argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
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


def test_the_resume_keeps_the_sessions_effort(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    fake.write(host.find_transcripts(sid)[0], {**text_reply(), "effort": "low"})
    bg = Bg()
    args = ["--effort", "high", "--model", "opus", "--effort=max"]
    run = runner(ctx, host, bg, Clock(), claude_args=args)
    assert run.resume(sid, RESUME_WORKING) is True
    assert bg.calls[0][0] == [
        "claude",
        "--model",
        "opus",
        "--effort",
        "low",
        "--bg",
        "--resume",
        sid,
        RESUME_WORKING,
    ]


def test_a_plain_resume_leaves_the_model_to_the_transcript(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    fake.write(host.find_transcripts(sid)[0], {**text_reply(), "requestedModel": "x"})
    bg = Bg()
    run = runner(ctx, host, bg, Clock(), claude_args=["--agent", "a"])
    assert run.resume(sid, RESUME_WORKING) is True
    assert "--model" not in bg.calls[-1][0]


def test_a_resume_starts_in_the_lease_workspace(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    launch = tmp_path / "ws"
    sid = held_session(ctx, fake, launch)
    set_status(ctx, sid, "working")
    # The session was last resumed in a worktree that it entered by path,
    # which the runner does not trust.
    worktree = launch / "repo" / ".claude" / "worktrees" / "w"
    worktree.mkdir(parents=True)
    record = ctx.state.session(HARNESS, sid)
    record.workspace = str(worktree)
    ctx.state.save_session(record)
    bg = Bg()
    clock = Clock()
    run = runner(ctx, host, bg, clock)
    run.tick()
    assert [cwd for _, cwd in bg.calls] == [str(launch)]
    # Without the lease workspace on this host, the recorded workspace.
    assert run.resume(sid, RESUME_WORKING, str(tmp_path / "gone")) is True
    assert bg.calls[-1][1] == str(worktree)


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
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
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
    # One error at the limit, not one at each tick.
    limit = [r for r in caplog.records if "times in an hour" in r.getMessage()]
    assert [r.levelno for r in limit] == [logging.ERROR]
    assert f"{sid}: resumed {MAX_RESUMES} times in an hour" in limit[0].getMessage()
    clock.t += 3600
    run.tick()
    assert len(bg.calls) == MAX_RESUMES + 1
    assert sid not in run._limited  # pyright: ignore[reportPrivateUsage]


def test_resumes_wait_while_the_host_runs_max_sessions(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    for name in ("a", "b", "c"):
        set_status(ctx, held_session(ctx, fake, tmp_path / name), "working")
    bg = Bg()
    run = runner(ctx, host, bg, Clock(), max_sessions=3)
    # One process runs already, for example the runner's resident session.
    monkeypatch.setattr(run.adapter, "live_processes", lambda *_: [["resident"]])
    run.tick()
    assert len(bg.calls) == 2
    run.tick()
    warnings = [r for r in caplog.records if "resumes wait" in r.getMessage()]
    assert len(warnings) == 1
    assert "3 Claude Code processes run (at most 3); 1 resumes" in (
        warnings[0].getMessage()
    )
    # The resumes that wait do not count toward the resume limit.
    assert sum(len(d) for d in run._resumes.values()) == 4  # pyright: ignore[reportPrivateUsage]


def test_launcher_max_sessions_config(tmp_path: Path) -> None:
    none = tmp_path / "none.toml"
    assert load({}, none).launcher_max_sessions == 10
    assert (
        load({"HYPERSPACE_LAUNCHER_MAX_SESSIONS": "4"}, none).launcher_max_sessions == 4
    )
    toml = tmp_path / "config.toml"
    toml.write_text("launcher_max_sessions = 2\n")
    assert load({}, toml).launcher_max_sessions == 2
    for bad in ("launcher_max_sessions = 0\n", 'launcher_max_sessions = "x"\n'):
        toml.write_text(bad)
        with pytest.raises(ConfigError, match="launcher_max_sessions"):
            load({}, toml)
    with pytest.raises(ConfigError, match="launcher_max_sessions"):
        load({"HYPERSPACE_LAUNCHER_MAX_SESSIONS": "-1"}, none)


def test_run_bg_adds_the_environment(tmp_path: Path) -> None:
    out = tmp_path / "env"
    argv = [
        sys.executable,
        "-c",
        f"import os; open({str(out)!r}, 'w').write(os.environ['X_TEST'])",
    ]
    assert run_bg(argv, str(tmp_path), 30, {"X_TEST": "v"}).returncode == 0
    assert out.read_text() == "v"


@pytest.mark.parametrize(
    ("config", "names"),
    [
        ("", None),
        ('launcher_secret_env = ["RELAY_TOKEN"]\n', "launcher_secret_env"),
        (
            "launcher_secret_env = 1\n[launcher_env]\nRELAY_TOKEN = 2\n",
            "launcher_env and launcher_secret_env",
        ),
    ],
)
def test_launcher_warns_at_start_about_removed_options(
    monkeypatch: pytest.MonkeyPatch,
    server: Server,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    config: str,
    names: str | None,
) -> None:
    # A removed option must not stop the load of config.toml, even with a
    # value of the wrong type, because a hook must not fail.
    code, _, ran = start_launcher(monkeypatch, server, tmp_path, ["--stay"], config)
    assert (code, ran) == (0, ["workstation"])
    warnings = [r.getMessage() for r in caplog.records if "removed" in r.getMessage()]
    if names is None:
        assert warnings == []
    else:
        assert len(warnings) == 1
        pronoun = "them" if " and " in names else "it"
        assert warnings[0] == (
            f"launcher: config.toml has {names}, which a release removed; the "
            f"launcher ignores {pronoun}. A plugin that needs a value must read it "
            "from its own configuration"
        )


def job_bg(
    fake: FakeClaude, monkeypatch: pytest.MonkeyPatch, job_state: object
) -> Runner:
    """A `claude --bg` that runs the fake, which makes a new job with
    `job_state` in its state.json."""
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "job")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    raw = job_state if isinstance(job_state, str) else json.dumps(job_state)
    monkeypatch.setenv("FAKE_CLAUDE_JOB_STATE", raw)

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        return run_bg([sys.executable, FAKE, *argv[1:]], cwd, timeout, env)

    return bg


# The detail that Claude Code 2.1.301 wrote when a channel option took the
# first message as a channel spec, with an escape sequence and more text.
CRASH = "exit 1 before init \u2014 \x1b[31mError:\x1b[0m bad channel spec " + "x" * 600


def test_a_job_that_fails_at_its_start_is_a_failed_resume(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="hyperspace")
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    bg = job_bg(fake, monkeypatch, {"state": "failed", "detail": CRASH})
    clock = Clock()
    run = runner(ctx, host, Bg(), clock)
    run.run_claude = bg
    assert run.resume(sid, RESUME_WORKING) is False
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "failed at its start: exit 1 before init" in warnings[0]
    assert "bad channel spec" in warnings[0]
    assert "\x1b" not in warnings[0]
    assert len(warnings[0]) < 400
    assert "resumed in" not in caplog.text
    # The failed start counts toward the limit of resumes.
    for _ in range(MAX_RESUMES + 2):
        run.tick()
        clock.t += 60
    assert len(list((fake.root / "jobs").iterdir())) == MAX_RESUMES


def test_a_job_that_fails_after_some_polls_is_a_failed_resume(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    monkeypatch.setenv("FAKE_CLAUDE_JOB", "j1")
    bg = job_bg(fake, monkeypatch, {"state": "working", "detail": "starting\u2026"})
    clock = Clock()
    path = fake.root / "jobs" / "j1" / "state.json"
    tick = clock.sleep

    def sleep(seconds: float) -> None:
        tick(seconds)
        if clock.sleeps == 3:
            path.write_text(json.dumps({"state": "failed", "detail": "exit 1"}))

    run = runner(ctx, host, Bg(), clock)
    run.run_claude = bg
    run.sleep = sleep
    assert run.resume(sid, RESUME_WORKING) is False
    assert clock.sleeps == 3


@pytest.mark.parametrize(
    ("job_state", "waits"),
    [
        # The process started.
        ({"state": "working", "detail": "Reading files"}, False),
        ({"state": "blocked", "detail": "starting\u2026"}, False),
        # The process does not start in the time of the check.
        ({"state": "working", "detail": "starting\u2026"}, True),
        # The launcher cannot read the job's state.
        ("not json", True),
        ({"detail": "exit 1"}, True),
    ],
)
def test_a_job_that_does_not_fail_at_its_start_is_resumed(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    job_state: object,
    waits: bool,
) -> None:
    caplog.set_level(logging.INFO, logger="hyperspace")
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    clock = Clock()
    run = runner(ctx, host, Bg(), clock)
    run.run_claude = job_bg(fake, monkeypatch, job_state)
    assert run.resume(sid, RESUME_WORKING) is True
    assert f"{sid}: resumed in" in caplog.text
    assert (clock.t >= START_CHECK) == waits
    assert clock.t < START_CHECK + 1


def test_new_job_finds_the_job_of_the_resume(fake: FakeClaude) -> None:
    sid, other = str(uuid.uuid4()), str(uuid.uuid4())
    job(fake, "old", sid)
    before = fake.adapter.job_names()
    assert fake.adapter.new_job(before, sid) is None
    # One new job: its state does not have to name the session.
    job(fake, "a1", other)
    assert fake.adapter.new_job(before, sid) == "a1"
    # Two new jobs: only the one whose state names the session.
    job(fake, "b2", other, resumeSessionId=sid)
    assert fake.adapter.new_job(before, sid) == "b2"
    job(fake, "c3", sid)
    assert fake.adapter.new_job(before, sid) is None


def fake_jobs(fake: FakeClaude, monkeypatch: pytest.MonkeyPatch, run: Launcher) -> None:
    """Make `run` use the fake for `claude --bg` and `claude rm`, with the
    job records of Claude Code 2.1.301."""
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "resume")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        return run_bg([sys.executable, FAKE, *argv[1:]], cwd, timeout, env)

    def rm(argv: list[str]) -> tuple[int, str]:
        return run_rm([sys.executable, FAKE, *argv[1:]])

    run.run_claude = bg
    run.run_rm = rm


def job_ids(fake: FakeClaude) -> dict[str, str]:
    """Job name to the job's own id."""
    return {j.short: j.own_id for j in fake.adapter.jobs()}


def test_a_stopped_job_is_removed_so_the_resume_keeps_the_id(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="hyperspace")
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    # The job record from before the runner restarted.
    job(fake, sid[:8], sid, state="stopped")
    transcripts = host.find_transcripts(sid)
    run = runner(ctx, host, Bg(), Clock())
    fake_jobs(fake, monkeypatch, run)
    assert run.resume(sid, RESUME_WORKING) is True
    assert job_ids(fake) == {sid[:8]: sid}
    assert host.find_transcripts(sid) == transcripts
    assert len(list(transcripts[0].parent.glob("*.jsonl"))) == 1
    assert f"removed the job {sid[:8]}" in caplog.text
    assert "copied" not in caplog.text


def test_the_fake_copies_a_session_that_has_a_job_record_for_options(
    ctx: Context, fake: FakeClaude, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    job(fake, sid[:8], sid, state="stopped")
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "resume")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    argv = [sys.executable, FAKE, "--bg", "--resume", sid, "hi"]
    assert run_bg(argv, str(tmp_path), 30).returncode == 0
    assert job_ids(fake) == {sid[:8]: sid}
    argv[2:2] = ["--model", "opus"]
    result = run_bg(argv, str(tmp_path), 30)
    assert result.returncode == 0
    copy = next(own for short, own in job_ids(fake).items() if short != sid[:8])
    assert f"started a copy as {copy[:8]}" in result.output
    assert fake.adapter.find_transcripts(copy)


def test_a_refused_rm_blocks_the_resume(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    job(fake, sid[:8], sid, state="stopped")
    clock = Clock()
    run = runner(ctx, host, Bg(), clock)
    fake_jobs(fake, monkeypatch, run)
    monkeypatch.setenv("FAKE_CLAUDE_RM", "refuse")
    calls: list[list[str]] = []
    rm = run.run_rm

    def counted(argv: list[str]) -> tuple[int, str]:
        calls.append(argv)
        return rm(argv)

    run.run_rm = counted
    assert run.resume(sid, RESUME_WORKING) is False
    assert job_ids(fake) == {sid[:8]: sid}
    assert len(list(host.find_transcripts(sid)[0].parent.glob("*.jsonl"))) == 1
    assert f"claude rm {sid[:8]}: error: job has unpushed commits" in caplog.text
    # The limit of resumes also applies to the failed removals.
    for _ in range(MAX_RESUMES + 2):
        run.tick()
        clock.t += 60
    assert len(calls) == MAX_RESUMES
    assert calls[0] == ["claude", "rm", sid[:8]]


def test_no_resume_if_the_transcript_goes_with_the_job(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    job(fake, sid[:8], sid, state="stopped")

    def rm(argv: list[str]) -> tuple[int, str]:
        for path in host.find_transcripts(sid):
            path.unlink()
        return 0, ""

    bg = Bg()
    run = runner(ctx, host, bg, Clock())
    run.run_rm = rm
    assert run.resume(sid, RESUME_WORKING) is False
    assert bg.calls == []
    assert "no transcript after claude rm" in caplog.text


def test_a_job_with_a_worktree_resumes_with_its_saved_options(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="hyperspace")
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    # A job with a worktree resumes with no options, because options with its
    # record make a copy. Thus the launcher puts the model and the effort
    # level of the last turn into the saved options.
    last_turn = {
        **text_reply(),
        "requestedModel": "openai:gpt-6.1-sol",
        "effort": "low",
    }
    fake.write(host.find_transcripts(sid)[0], last_turn)
    channels = "--dangerously-load-development-channels"
    saved = [
        *("--name", "w", channels, "plugin:s@old", "plugin:n@m"),
        *("--effort", "high", "--model=opus", "--agent", "a"),
    ]
    path = job(
        fake,
        sid[:8],
        sid,
        state="stopped",
        worktreePath=str(tmp_path / "ws"),
        respawnFlags=saved,
    )
    # The launcher's own options replace the saved ones too, for example a
    # channel that changed since the job started.
    args = [channels, "plugin:s@new", "--agent", "a"]
    run = runner(ctx, host, Bg(), Clock(), claude_args=args)
    fake_jobs(fake, monkeypatch, run)
    calls: list[list[str]] = []
    bg, rm = run.run_claude, Rm(ctx)

    def counted(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        calls.append(argv)
        return bg(argv, cwd, timeout, env)

    run.run_claude, run.run_rm = counted, rm
    assert run.resume(sid, RESUME_WORKING) is True
    # `claude rm` can remove the worktree, so the job stays.
    assert rm.calls == []
    assert calls == [["claude", "--bg", "--resume", sid, RESUME_WORKING]]
    assert job_ids(fake) == {sid[:8]: sid}
    assert f"the job {sid[:8]} has a worktree" in caplog.text
    assert "copied" not in caplog.text
    record = json.loads(path.read_text())
    turn = ["--model", "openai:gpt-6.1-sol", "--effort", "low"]
    assert record["respawnFlags"] == ["--name", "w", *args, *turn]
    assert record["worktreePath"] == str(tmp_path / "ws")
    assert f"changed to {shlex.join([*args, *turn])}" in caplog.text


def test_a_resume_back_into_the_jobs_worktree_is_not_a_move(
    ctx: Context, fake: FakeClaude, tmp_path: Path
) -> None:
    launch = tmp_path / "ws"
    sid = held_session(ctx, fake, launch)
    worktree = launch / ".claude" / "worktrees" / "w"
    # The session last ran in the worktree of its job, from another
    # transcript path.
    record = ctx.state.session(HARNESS, sid)
    record.workspace, record.transcript = str(worktree), str(tmp_path / "old.jsonl")
    ctx.state.save_session(record)
    session = ctx.adapter.session_for_transcript(ctx.adapter.find_transcripts(sid)[0])
    assert session is not None

    def notice() -> str:
        return session_start_notice(
            ctx.state, ctx.adapter, session, "agent-1", str(launch), "resume"
        )

    # A job that owns no worktree: the hooks see a move.
    job(fake, sid[:8], sid, worktreePath=None)
    assert "resumed in a different place" in notice()
    # Claude Code starts the job in its first directory, then goes back to
    # the worktree.
    job(fake, sid[:8], sid, worktreePath=str(worktree))
    assert "different place" not in notice()
    job(fake, sid[:8], sid, worktreePath=str(worktree / "sub"))
    assert "resumed in a different place" in notice()
    # A worktree that the session entered by path: its job owns none, but
    # Claude Code goes back to it while it exists below the start directory.
    worktree.mkdir(parents=True)
    assert "different place" not in notice()
    elsewhere = tmp_path / "other"
    elsewhere.mkdir()
    record.workspace = str(elsewhere)
    ctx.state.save_session(record)
    assert "resumed in a different place" in notice()


def test_saved_options_change_only_for_a_new_value(fake: FakeClaude) -> None:
    flags = ["--effort", "low", "--model", "sonnet"]
    path = job(fake, "j1", "s", respawnFlags=flags)
    before = path.stat().st_mtime_ns
    set_saved = fake.adapter.set_saved_options
    assert set_saved("j1", ["--model", "sonnet", "--effort", "low"]) is False
    assert set_saved("j1", []) is False
    assert path.stat().st_mtime_ns == before
    assert set_saved("j1", ["--effort", "high"]) is True
    assert json.loads(path.read_text())["respawnFlags"] == [
        "--model",
        "sonnet",
        "--effort",
        "high",
    ]
    # Saved options that are not a list of strings stay as they are.
    for odd in (None, "--model x", ["--model", 1]):
        job(fake, "j2", "s", respawnFlags=odd)
        assert set_saved("j2", ["--model", "haiku"]) is False
    assert set_saved("../j1", ["--model", "haiku"]) is False


def test_saved_options_keep_a_state_that_changed_after_the_read(
    fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = job(fake, "j1", "s", state="stopped", respawnFlags=["--model", "x"])
    read = claude_code_mod._read_state
    running = {"sessionId": "s", "state": "working", "respawnFlags": ["--model", "x"]}

    def read_then_start(p: Path) -> dict[str, Any] | None:
        # Claude Code starts the job between the read and the write.
        record = read(p)
        p.write_text(json.dumps(running))
        os.utime(p, ns=(1, 1))
        return record

    monkeypatch.setattr(claude_code_mod, "_read_state", read_then_start)
    assert fake.adapter.set_saved_options("j1", ["--model", "haiku"]) is False
    assert json.loads(path.read_text()) == running
    assert [p.name for p in path.parent.iterdir()] == ["state.json"]


def test_a_job_with_a_worktree_that_fails_at_its_start_is_a_failed_resume(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    path = job(fake, sid[:8], sid, state="stopped", worktreePath=str(tmp_path))
    run = runner(ctx, host, Bg(), Clock())
    fake_jobs(fake, monkeypatch, run)
    bg = run.run_claude

    def fails(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        result = bg(argv, cwd, timeout, env)
        state = json.loads(path.read_text())
        path.write_text(json.dumps({**state, "state": "failed", "detail": "exit 1"}))
        return result

    run.run_claude = fails
    assert run.resume(sid, RESUME_WORKING) is False
    assert f"the job {sid[:8]} failed at its start: exit 1" in caplog.text


def test_the_state_of_a_kept_job_from_before_the_resume_does_not_count(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    job(fake, sid[:8], sid, state="failed", detail="old", worktreePath=str(tmp_path))
    clock = Clock()
    # A `claude --bg` that does not change the job's state.
    run = runner(ctx, host, Bg(), clock)
    assert run.resume(sid, RESUME_WORKING) is True
    assert clock.t >= START_CHECK


def test_the_resume_logs_a_copy(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    run = runner(ctx, host, Bg(), Clock())
    fake_jobs(fake, monkeypatch, run)
    monkeypatch.setenv("FAKE_CLAUDE_COPY", "1")
    assert run.resume(sid, RESUME_WORKING) is True
    (copy,) = job_ids(fake).values()
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors == [
        f"{sid}: the resume copied the session under the new id {copy[:8]}",
        (
            f"{sid}: claude --bg: note: session {sid[:8]} is already running in the"
            f" background, so this started a copy as {copy[:8]}."
        ),
    ]
    # No more resumes: each would start another copy.
    calls = len(job_ids(fake))
    assert run.resume(sid, RESUME_WORKING) is False
    assert run.resume(sid, RESUME_WORKING) is False
    assert len(job_ids(fake)) == calls
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors[2:] == [
        (
            f"{sid}: not resumed: an earlier resume copied it as {copy[:8]}; stop"
            " the copies, then resume it by hand"
        )
    ]
    # Once the session runs again, the launcher can resume it later.
    run.resume_held([sid])
    assert ctx.state.get(f"resume_copied:claude-code:{sid}") is None


def registration(
    fake: FakeClaude, pid: int, sid: str, domain: str | None, kind: str = "bg"
) -> Path:
    """Write a live-session registry record of Claude Code."""
    record: dict[str, Any] = {"pid": pid, "kind": kind, "sessionId": sid}
    if domain is not None:
        record["pidDomain"] = domain
    path = fake.root / "sessions" / f"{pid}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))
    return path


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_idle_since_reads_the_idle_bg_record_of_a_live_process(
    fake: FakeClaude,
) -> None:
    sid, other = str(uuid.uuid4()), str(uuid.uuid4())
    stat = Path("/proc/self/stat").read_text()
    start = stat[stat.rfind(")") + 2 :].split()[19]
    sessions = fake.root / "sessions"
    sessions.mkdir(parents=True)

    def write(pid: int, **fields: Any) -> None:
        record = {
            "pid": pid,
            "procStart": start,
            "kind": "bg",
            "sessionId": sid,
            "jobId": "aa11",
            "status": "idle",
            "statusUpdatedAt": 1_793_954_905_482,
            **fields,
        }
        (sessions / f"{pid}.json").write_text(json.dumps(record))

    adapter = ClaudeCode(fake.root)
    write(os.getpid())
    assert adapter.idle_since(sid) == ("aa11", 1_793_954_905.482)
    assert adapter.idle_since(other) is None
    for fields in (
        {"status": "busy"},
        {"kind": "interactive"},
        {"procStart": "1"},
        {"jobId": "-x"},
        {"statusUpdatedAt": "soon"},
    ):
        write(os.getpid(), **fields)
        assert adapter.idle_since(sid) is None, fields


def test_foreign_registrations_are_bg_records_from_another_pid_domain(
    fake: FakeClaude,
) -> None:
    sid, other = str(uuid.uuid4()), str(uuid.uuid4())
    here = claude_code_mod._pid_domain()  # pyright: ignore[reportPrivateUsage]
    foreign = registration(fake, 261, sid, "linux::pid:[6]")
    registration(fake, 262, sid, "linux::pid:[6]", kind="interactive")
    registration(fake, 263, sid, here)
    registration(fake, 264, sid, None)
    registration(fake, 265, other, "linux::pid:[6]")
    assert ClaudeCode(fake.root).foreign_registrations(sid) == [foreign]


def test_the_resume_skips_a_session_registered_in_another_pid_namespace(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    run = runner(ctx, host, Bg(), Clock())
    fake_jobs(fake, monkeypatch, run)
    # From the process of an earlier container: Claude Code would copy it.
    stale = registration(fake, 261, sid, "linux::pid:[6]")
    assert run.resume(sid, RESUME_WORKING) is False
    assert run.resume(sid, RESUME_WORKING) is False
    assert job_ids(fake) == {}
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors == [
        (
            f"{sid}: not resumed: Claude Code counts it as running in another"
            f" pid domain ({stale}); if no process there runs it, remove these"
            " files"
        )
    ]
    stale.unlink()
    assert run.resume(sid, RESUME_WORKING) is True
    assert list(job_ids(fake).values()) == [sid]


def test_the_resume_skips_a_session_with_an_unreadable_job_state(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    run = runner(ctx, host, Bg(), Clock())
    fake_jobs(fake, monkeypatch, run)
    # A write that a crash stopped: the launcher cannot remove the job, and
    # Claude Code would copy the session.
    job = fake.root / "jobs" / sid[:8]
    job.mkdir(parents=True)
    state = job / "state.json"
    state.write_bytes(b"\0" * 64)
    assert run.resume(sid, RESUME_WORKING) is False
    assert run.resume(sid, RESUME_WORKING) is False
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors == [
        (
            f"{sid}: not resumed: the job state {state} has no valid session id,"
            " so a resume would start a copy; if no process runs the job, remove"
            " its directory"
        )
    ]
    state.unlink()
    job.rmdir()
    assert run.resume(sid, RESUME_WORKING) is True
    assert list(job_ids(fake).values()) == [sid]


def test_the_resume_finds_a_copy_from_the_job_state(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "ws")
    set_status(ctx, sid, "working")
    copy = str(uuid.uuid4())
    run = runner(ctx, host, Bg(), Clock())
    # No note in the output: the state of the new job has the copy's id.
    run.run_claude = job_bg(
        fake, monkeypatch, {"sessionId": copy, "state": "working", "detail": "x"}
    )
    assert run.resume(sid, RESUME_WORKING) is True
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert errors == [f"{sid}: the resume copied the session under the new id {copy}"]


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
    session_page, manifest = api.session_page, api.manifest
    fails = [1]

    def flaky_sessions(**filters: Any) -> Any:
        # Only the list of `restore_held`, which has no `open` filter.
        if failure == "list" and "open" not in filters and fails:
            fails.pop()
            raise httpx.ConnectError("server not available")
        return session_page(**filters)

    def flaky_manifest(*args: Any) -> Any:
        if failure == "session" and fails:
            fails.pop()
            raise ApiError(503, "unavailable")
        return manifest(*args)

    monkeypatch.setattr(api, "session_page", flaky_sessions)
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
    env = {
        "HYPERSPACE_LAUNCHER_CLAUDE_ARGS": '["--agent", "opus", "--permission-mode", "default"]'
    }
    assert load(env, none).launcher_claude_args == [
        "--agent",
        "opus",
        "--permission-mode",
        "default",
    ]
    assert load({}, none).launcher_claude_args == []
    toml = tmp_path / "config.toml"
    toml.write_text('launcher_claude_args = ["--agent", "sonnet"]\n')
    assert load({}, toml).launcher_claude_args == ["--agent", "sonnet"]
    for bad in (
        "not json",
        '"--agent"',
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


@pytest.mark.parametrize(
    "args",
    [
        ["--dangerously-load-development-channels", "plugin:a@b"],
        ["--agent", "opus", "--add-dir", "/a", "/b"],
        ["--allowedTools", "Bash"],
        ["--allowed-tools", "Bash"],
        ["--disallowedTools", "Bash"],
        ["--disallowed-tools", "Bash"],
        ["--betas", "x"],
        ["--channels", "plugin:a@b"],
        ["--file", "spec"],
        ["--mcp-config", "a.json"],
        ["--tools", "Bash"],
        ["--plugin-dir", "/opt/plugins"],
        ["--agent", "opus", "--plugin-dir=/opt/plugins"],
        # With no value, the option takes the message.
        ["--agent", "opus", "--tools"],
        # The `=` form also takes the arguments after it.
        ["--agent", "opus", "--add-dir=/a"],
        ["--agent", "opus", "--add-dir=/a", "/b"],
        ["--dangerously-load-development-channels=plugin:a@b"],
    ],
)
def test_launcher_claude_args_cannot_end_with_a_list_option(args: list[str]) -> None:
    last = [a for a in args if a.startswith("-")][-1].split("=", 1)[0]
    with pytest.raises(ConfigError, match="put an option that") as exc:
        check_claude_args(args)
    assert f"cannot be {last}," in str(exc.value)


@pytest.mark.parametrize(
    "args",
    [
        [
            "--dangerously-load-development-channels",
            "plugin:a@b",
            "--dangerously-skip-permissions",
        ],
        ["--add-dir", "/a", "/b", "--agent", "opus"],
        ["--add-dir=/a", "--agent", "opus"],
        ["--agent", "opus"],
        ["--dangerously-skip-permissions"],
        [],
    ],
)
def test_launcher_claude_args_can_end_with_other_options(args: list[str]) -> None:
    assert check_claude_args(args) == args


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

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        with ctx.state.try_lock(HARNESS, sid) as free:
            held.append(not free)
        # The fake starts a daemon that continues to run, as `claude --bg` can.
        return run_bg([sys.executable, FAKE, *argv[1:]], cwd, timeout, env)

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


@pytest.mark.parametrize(
    "outcome",
    [
        RunResult(0, ""),
        RunResult(1, "error"),
        # `claude --bg` timed out.
        RunResult(None, ""),
        OSError("claude: not found"),
    ],
)
def test_a_checkpoint_that_the_launch_blocks_runs_after_it(
    monkeypatch: pytest.MonkeyPatch,
    server: Server,
    fake: FakeClaude,
    tmp_path: Path,
    outcome: RunResult | OSError,
) -> None:
    # The CLI and the launcher use the same state directory and server.
    for name, value in {
        "XDG_STATE_HOME": str(tmp_path / "xdg"),
        "HYPERSPACE_CONFIG": str(tmp_path / "none.toml"),
        "HYPERSPACE_URL": "http://127.0.0.1:9",
        "HYPERSPACE_TOKEN": "unused",
        "HYPERSPACE_HOST": "laptop",
        "CLAUDE_CONFIG_DIR": str(fake.root),
    }.items():
        monkeypatch.setenv(name, value)

    def context(cfg: Config, state: State) -> Context:
        return Context(
            api=server.api("laptop"), state=state, adapter=ClaudeCode(), host=cfg.host
        )

    monkeypatch.setattr("hyperspace_client.cli._context", context)
    host = Host(fake.root)
    ctx = Context(
        api=server.api("laptop"),
        state=State(tmp_path / "xdg" / "hyperspace"),
        adapter=host,
        host="laptop",
    )
    try:
        ws = tmp_path / "ws"
        sid = held_session(ctx, fake, ws)
        set_status(ctx, sid, "working")
        transcript = host.find_transcripts(sid)[0]
        cfg = load()
        hooked: list[list[str]] = []

        def checkpoint(args: list[str]) -> None:
            hooked.append(args)
            assert main(args) == 0

        def bg(
            argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
        ) -> RunResult:
            # Before `claude --bg` returns (or times out), the session runs a
            # short turn. Its Stop hook starts a checkpoint, which does not
            # get the session's lock.
            fake.write(transcript, prompt("go on"), text_reply())
            handle(
                {
                    "hook_event_name": "Stop",
                    "transcript_path": str(transcript),
                    "cwd": str(ws),
                },
                cfg,
                start=checkpoint,
            )
            assert hooked
            assert ctx.state.dirty_since(HARNESS, sid) is not None
            if isinstance(outcome, OSError):
                raise outcome
            return outcome

        started: list[list[str]] = []
        run = runner(ctx, host, Bg(), Clock(), start=started.append)
        run.run_claude = bg
        run.tick()
        assert started == [
            ["--quiet", "sync", "--checkpoint", "--transcript", str(transcript)]
        ]
        assert main(started[0]) == 0
        assert ctx.state.dirty_since(HARNESS, sid) is None
        assert state(ctx, sid).get("status", {}).get("status") == "ready"
        files = server.store.manifest(SessionKey("default", HARNESS, sid))["files"]
        lengths = [
            g["length"]
            for f in files
            if f["kind"] == "log"
            for g in f["generations"]
            if g["id"] == f["active"]
        ]
        assert transcript.stat().st_size in lengths
    finally:
        ctx.state.close()


@pytest.mark.parametrize("status", [None, "ready", "waiting", "working"])
def test_runner_resumes_pending_wake(
    ctx: Context,
    server: Server,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    status: str | None,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "workspace")
    if status == "ready":
        set_status(ctx, sid, status, reason="idle")
    elif status == "waiting":
        set_status(ctx, sid, status, wake="message")
    elif status is not None:
        set_status(ctx, sid, status)
    sender = server.api("sender")
    assert sender.wake(HARNESS, {"session": sid})["verdict"] == "queued"
    bg, clock = Bg(), Clock()
    run = runner(ctx, host, bg, clock)
    run.tick()
    assert sender.wake(HARNESS, {"session": sid})["verdict"] == "waking"
    expected = (
        f"{RESUME_WORKING}\n{RESUME_WAKE}" if status == "working" else RESUME_WAKE
    )
    assert [argv[-1] for argv, _ in bg.calls] == [expected]
    assert state(ctx, sid).get("wake_request") is not None
    host.live = [sid]
    run.tick()
    assert "wake_request" not in state(ctx, sid)
    assert len(bg.calls) == 1
    sender.close()


@pytest.mark.parametrize("stay", [False, True])
def test_workstation_wakes(
    ctx: Context,
    server: Server,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
    stay: bool,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "workspace")
    set_status(ctx, sid, "working")
    bg, clock = Bg(), Clock()
    run = launcher(ctx, host, Rm(ctx), clock, stay=stay, run_claude=bg)
    requests: list[httpx.Request] = []
    server.clients[0].event_hooks["request"].append(requests.append)
    run.tick()
    assert not bg.calls
    heartbeats = [
        json.loads(r.content) for r in requests if r.url.path.endswith("heartbeat")
    ]
    assert bool(heartbeats[-1].get("wakes")) == stay
    host.live = [sid]
    run.tick()
    sender = server.api("sender")
    assert sender.wake(HARNESS, {"session": sid})["verdict"] == "live"
    run.tick()
    assert not bg.calls
    host.live = []
    run.tick()
    assert [argv[-1] for argv, _ in bg.calls] == ([RESUME_WAKE] if stay else [])
    sender.close()


def test_ls_shows_pending_wake(
    ctx: Context,
    server: Server,
    fake: FakeClaude,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sid = held_session(ctx, fake, tmp_path / "workspace")
    sender = server.api("sender")
    sender.wake(HARNESS, {"session": sid, "pending": 2})
    _print_status_table(ctx.api.sessions(), 3600)
    assert "wake pending" in capsys.readouterr().out
    assert [s["session_id"] for s in ctx.api.all_sessions(wake="pending")] == [sid]
    sender.close()


@pytest.mark.parametrize("runner_mode", [False, True])
def test_done_session_is_not_resumed_for_a_wake(runner_mode: bool) -> None:
    summary = _summary({"status": "done"})
    summary["state"]["wake_request"] = {"received": "t", "count": 1}
    assert needs_process(summary, "agent-1", False, runner_mode) is None


def test_workstation_without_wake_capability(
    ctx: Context,
    server: Server,
    fake: FakeClaude,
    host: Host,
    tmp_path: Path,
) -> None:
    sid = held_session(ctx, fake, tmp_path / "workspace")
    sender = server.api("sender")
    sender.wake(HARNESS, {"session": sid})
    ctx.api._capabilities = {"liveness"}  # pyright: ignore[reportPrivateUsage]
    bg, clock = Bg(), Clock()
    run = launcher(ctx, host, Rm(ctx), clock, stay=True, run_claude=bg)
    run.tick()
    assert not bg.calls
    assert sender.wake(HARNESS, {"session": sid})["verdict"] == "queued"
    sender.close()


@pytest.mark.parametrize(
    "mode", ["started", "error", "missing", "cwd", "workspace", "off"]
)
def test_launch_pass(
    ctx: Context, host: Host, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:

    register_host(ctx, "workstation")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    sid = str(uuid.uuid4())
    item = ctx.api.launch(
        HARNESS,
        {
            "host": ctx.host,
            "prompt": "--dangerously-skip-permissions",
            "workspace": str(workspace),
        },
    )
    calls: list[list[str]] = []

    def start(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        calls.append(argv)
        assert argv[-3:] == ["--bg", "--", "--dangerously-skip-permissions"]
        assert "HYPERSPACE_LAUNCH" not in env
        marker = json.loads(argv[argv.index("--settings") + 1])["env"]
        assert marker["HYPERSPACE_LAUNCH"] == item["launch"]
        assert "HYPERSPACE_LAUNCH_PARENT" not in env
        if mode == "error":
            return RunResult(1, "start failed\nmore detail")
        if mode != "missing":
            path = host.config_dir / "jobs" / "job1" / "state.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {"sessionId": sid, "cwd": "/wrong" if mode == "cwd" else cwd}
                )
            )
        return RunResult(0, "backgrounded · job1")

    if mode == "workspace":
        workspace.rmdir()
    run = Launcher(
        ctx,
        host,
        FAKE,
        60,
        None,
        workspace=str(workspace),
        run_claude=start,
        launches=mode != "off",
    )
    run.launch_pending()
    run.launch_pending()
    result = ctx.api.get_launch(HARNESS, item["launch"])
    expected = {
        "started": "started",
        "error": "failed",
        "missing": "claimed",
        "cwd": "claimed",
        "workspace": "failed",
        "off": "pending",
    }
    assert result["state"] == expected[mode]
    assert len(calls) == (0 if mode in ("workspace", "off") else 1)
    if mode == "started":
        assert result["session"] == sid
        assert (
            ctx.state.session(HARNESS, sid).metadata["links"]["launch"]
            == item["launch"]
        )
        assert launch_session(host.config_dir, "../job1", str(workspace)) is None
    assert launch_job_id("backgrounded · job1") == "job1"
    assert launch_job_id("job started") is None


@pytest.mark.parametrize("option", ["--name"])
def test_launch_options_owned(option: str) -> None:
    with pytest.raises(ConfigError):
        check_claude_args([option, "value"])


def test_launch_argv_values() -> None:

    item = cast(
        Launch,
        {
            "prompt": "--dangerously-skip-permissions",
            "model": "--bad",
            "effort": "high",
            "name": "a b",
        },
    )
    assert launch_argv(FAKE, [], item) == [
        FAKE,
        "--model=--bad",
        "--effort=high",
        "--name=a b",
        "--bg",
        "--",
        "--dangerously-skip-permissions",
    ]


@pytest.mark.parametrize("source", ["startup", "resume", "clear", "compact"])
def test_launch_hook_marker(source: str) -> None:
    env = {
        "HYPERSPACE_LAUNCH": "l_one",
        "HYPERSPACE_LAUNCH_PARENT": "parent",
        "CLAUDE_JOB_DIR": "/jobs/12345678",
    }
    assert ClaudeCode.launch_marker(source, env, "12345678-session") == (
        ("l_one", "parent") if source == "startup" else None
    )


def test_launch_hook_sync_links(ctx: Context, fake: FakeClaude, host: Host) -> None:

    register_host(ctx, "workstation")
    parent = session(ctx, fake)
    response = ctx.api.launch(
        HARNESS, {"host": ctx.host, "prompt": "hi", "parent": parent}
    )
    lid = response["launch"]
    ctx.api.claim_launch(HARNESS, lid)
    path = fake.transcript(WS)
    fake.write(path, prompt("hi"), text_reply())
    found = host.session_for_transcript(path)
    assert found is not None
    save_launch_links(ctx.state, HARNESS, found.session_id, lid, parent)
    ctx.api.link_launch(HARNESS, lid, found.session_id)
    Syncer(ctx, found, WS).run()
    fake.write(path, prompt("again"), text_reply())
    Syncer(ctx, found, WS).run()
    assert ctx.api.get_launch(HARNESS, lid)["session"] == found.session_id
    assert (
        ctx.api.manifest(HARNESS, found.session_id)["metadata"]["links"]["parent"]
        == parent
    )


def test_launch_cli_local(
    server: Server,
    fake: FakeClaude,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:

    values = {
        "XDG_STATE_HOME": str(tmp_path / "xdg"),
        "HYPERSPACE_CONFIG": str(tmp_path / "none.toml"),
        "HYPERSPACE_URL": "http://127.0.0.1:9",
        "HYPERSPACE_TOKEN": "unused",
        "HYPERSPACE_HOST": "laptop",
        "CLAUDE_CONFIG_DIR": str(fake.root),
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    def context(cfg: Config, state: State) -> Context:
        return Context(
            api=server.api("laptop"), state=state, adapter=ClaudeCode(), host=cfg.host
        )

    monkeypatch.setattr("hyperspace_client.cli._context", context)
    sid = str(uuid.uuid4())
    original = Launcher.launch_pending

    def start(run: Launcher, launch_id: str | None = None) -> None:
        def bg(
            argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
        ) -> RunResult:
            path = fake.root / "jobs" / "localjob" / "state.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"sessionId": sid, "cwd": cwd}))
            return RunResult(0, "backgrounded · localjob")

        run.run_claude = bg
        original(run, launch_id)

    monkeypatch.setattr(Launcher, "launch_pending", start)
    local = State(tmp_path / "register")
    ctx = context(load(), local)
    register_host(ctx, "workstation")
    other = ctx.api.launch(HARNESS, {"host": ctx.host, "prompt": "not this command"})
    ctx.api.close()
    local.close()
    assert main(["launch", "--wait", "--json", "hello"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["session"] == sid
    api = server.api("laptop")
    assert api.get_launch(HARNESS, other["launch"])["state"] == "pending"
    queued = api.launch(HARNESS, {"host": "laptop", "prompt": "later"})
    api.close()
    assert main(["launch", "--cancel", queued["launch"], "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["state"] == "cancelled"


def test_launch_claim_race(
    ctx: Context, host: Host, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:

    register_host(ctx, "workstation")
    item = ctx.api.launch(
        HARNESS, {"host": ctx.host, "prompt": "hi", "workspace": str(tmp_path)}
    )
    pending = ctx.api.pending_launches(HARNESS)
    monkeypatch.setattr(ctx.api, "pending_launches", lambda harness: pending)
    calls: list[list[str]] = []

    def start(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        calls.append(argv)
        return RunResult(0, "no job id")

    first = Launcher(ctx, host, FAKE, 60, None, run_claude=start)
    second = Launcher(ctx, host, FAKE, 60, None, run_claude=start)
    first.launch_pending()
    second.launch_pending()
    assert len(calls) == 1
    assert ctx.api.get_launch(HARNESS, item["launch"])["state"] == "claimed"


def test_launch_heartbeat_off(
    ctx: Context, host: Host, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[dict[str, Any]] = []
    monkeypatch.setattr(ctx.api, "heartbeat", requests.append)
    run = Launcher(ctx, host, FAKE, 60, None, stay=True, launches=False)
    run.tick()
    assert requests and "launches" not in requests[0]


def test_launch_defaults() -> None:
    options = ["--model", "host", "--effort=low"]
    assert check_claude_args(options) == options
    assert launch_argv(FAKE, options, cast(Launch, {"prompt": "hi"})) == [
        FAKE,
        *options,
        "--bg",
        "--",
        "hi",
    ]
    assert launch_argv(
        FAKE,
        options,
        cast(Launch, {"prompt": "hi", "model": "request", "effort": "high"}),
    ) == [FAKE, "--model=request", "--effort=high", "--bg", "--", "hi"]


@pytest.mark.parametrize("outcome", [None, 0])
def test_launch_timeout_and_cap(
    ctx: Context, host: Host, tmp_path: Path, outcome: int | None
) -> None:
    register_host(ctx, "workstation")
    item = ctx.api.launch(
        HARNESS, {"host": ctx.host, "prompt": "hi", "workspace": str(tmp_path)}
    )
    calls: list[list[str]] = []

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        calls.append(argv)
        return RunResult(outcome, "")

    run = Launcher(ctx, host, FAKE, 60, None, run_claude=bg, max_sessions=0)
    run.launch_pending()
    assert not calls
    run.max_sessions = 1
    run.launch_pending()
    run.launch_pending()
    assert len(calls) == 1
    assert ctx.api.get_launch(HARNESS, item["launch"])["state"] == "claimed"


def test_launch_marker_child_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYPERSPACE_LAUNCH", "l_one")
    monkeypatch.setenv("HYPERSPACE_LAUNCH_PARENT", "parent")
    assert "HYPERSPACE_LAUNCH" not in launcher_mod.child_env()
    assert "HYPERSPACE_LAUNCH_PARENT" not in launcher_mod.child_env()
    assert (
        ClaudeCode.launch_marker(
            "startup",
            {"HYPERSPACE_LAUNCH": "l_one", "CLAUDE_JOB_DIR": "/jobs/parent12"},
            "child123-session",
        )
        is None
    )


@pytest.mark.parametrize("rejection", [None, 409, 404])
def test_sync_first_launch_link(
    ctx: Context,
    fake: FakeClaude,
    host: Host,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    rejection: int | None,
) -> None:
    register_host(ctx, "workstation")
    parent = session(ctx, fake)
    item = ctx.api.launch(HARNESS, {"host": ctx.host, "prompt": "hi", "parent": parent})
    ctx.api.claim_launch(HARNESS, item["launch"])
    path = fake.transcript(WS)
    fake.write(path, prompt("hi"))
    found = host.session_for_transcript(path)
    assert found is not None
    save_launch_links(ctx.state, HARNESS, found.session_id, item["launch"], parent)
    calls: list[str] = []
    original = ctx.api.link_launch

    def link(harness: str, lid: str, sid: str) -> None:
        calls.append(sid)
        if rejection:
            raise ApiError(rejection, "rejected")
        original(harness, lid, sid)

    monkeypatch.setattr(ctx.api, "link_launch", link)
    Syncer(ctx, found, WS).run()
    fake.write(path, text_reply())
    Syncer(ctx, found, WS).run()
    assert calls == [found.session_id]
    links = ctx.state.session(HARNESS, found.session_id).metadata.get("links", {})
    if rejection == 409:
        assert "launch" not in links and "parent" not in links
        assert ctx.state.get(f"launch_links:{HARNESS}:{found.session_id}") is None
    if rejection:
        assert (
            sum("session link rejected" in r.getMessage() for r in caplog.records) == 1
        )
    else:
        assert (
            ctx.api.get_launch(HARNESS, item["launch"])["session"] == found.session_id
        )


@pytest.mark.parametrize("source", ["startup", "resume"])
def test_handle_launch_marker(
    fake: FakeClaude, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    path = fake.transcript(WS)
    fake.write(path, prompt("hi"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(fake.root / "jobs" / path.stem[:8]))
    monkeypatch.setenv("HYPERSPACE_LAUNCH", "l_marker")
    monkeypatch.setenv("HYPERSPACE_LAUNCH_PARENT", "parent")
    cfg = Config(state_dir=tmp_path / "marker-state")
    handle(
        {
            "hook_event_name": "SessionStart",
            "transcript_path": str(path),
            "cwd": WS,
            "source": source,
        },
        cfg,
        start=lambda args: None,
    )
    state = State(cfg.state_dir)
    try:
        links = state.session(HARNESS, path.stem).metadata.get("links", {})
        assert links == (
            {"launch": "l_marker", "parent": "parent"} if source == "startup" else {}
        )
    finally:
        state.close()


def test_launch_parent_env(
    ctx: Context, fake: FakeClaude, host: Host, tmp_path: Path
) -> None:
    register_host(ctx, "workstation")
    parent = session(ctx, fake)
    ctx.api.launch(
        HARNESS,
        {
            "host": ctx.host,
            "prompt": "hi",
            "parent": parent,
            "workspace": str(tmp_path),
        },
    )

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        assert "HYPERSPACE_LAUNCH_PARENT" not in env
        assert (
            json.loads(argv[argv.index("--settings") + 1])["env"][
                "HYPERSPACE_LAUNCH_PARENT"
            ]
            == parent
        )
        return RunResult(None, "")

    Launcher(ctx, host, FAKE, 60, None, run_claude=bg).launch_pending()


def test_launch_workspace_realpath(tmp_path: Path) -> None:
    ws = tmp_path / "workspace"
    ws.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(ws)
    state = tmp_path / "jobs" / "job" / "state.json"
    state.parent.mkdir(parents=True)
    sid = str(uuid.uuid4())
    state.write_text(json.dumps({"sessionId": sid, "cwd": str(alias) + "/../alias/"}))
    assert launch_session(tmp_path, "job", str(ws)) == sid
    assert launch_job_id("note backgrounded · job") is None
    assert launch_job_id("backgrounded · job extra") is None
    assert launch_job_id("\x1b[32mbackgrounded · job\x1b[0m") == "job"


@pytest.mark.parametrize("terminal", ["failed", "cancelled"])
def test_launch_cli_wait_failure(
    ctx: Context,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("hyperspace_client.cli._context", lambda cfg, state: ctx)
    monkeypatch.setattr(ctx.api, "require_capability", lambda capability: None)
    requests: list[dict[str, Any]] = []

    def post(harness: str, body: dict[str, Any]) -> dict[str, str]:
        requests.append(dict(body))
        if len(requests) == 1:
            raise httpx.ConnectError("lost response")
        return {"launch": "l_retry", "verdict": "queued"}

    monkeypatch.setattr(ctx.api, "launch", post)
    monkeypatch.setattr(
        ctx.api,
        "get_launch",
        lambda harness, lid: {
            "launch": lid,
            "state": terminal,
            "reason": "not started",
        },
    )
    assert main(["launch", "--host", "other-host", "--wait", "hi"]) == 1
    captured = capsys.readouterr()
    assert "l_retry" in captured.err and "queued" in captured.err
    assert requests[0] == requests[1]


@pytest.mark.parametrize("capped", [False, True])
def test_launch_cli_local_error(
    ctx: Context,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    capped: bool,
) -> None:
    register_host(ctx, "workstation")
    monkeypatch.setattr("hyperspace_client.cli._context", lambda cfg, state: ctx)

    original = Launcher.launch_pending

    def broken(run: Launcher, launch_id: str | None = None) -> None:
        if capped:
            run.max_sessions = 0
            original(run, launch_id)
            return
        raise RuntimeError("local pass failed")

    monkeypatch.setattr(Launcher, "launch_pending", broken)
    monkeypatch.setenv("HYPERSPACE_HOST", ctx.host)
    assert main(["launch", "--json", "hi"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["state"] == "cancelled"
    assert "not started; launch was cancelled" in captured.err


def test_local_launch_does_not_list(
    ctx: Context, host: Host, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    register_host(ctx, "workstation")
    item = ctx.api.launch(
        HARNESS, {"host": ctx.host, "prompt": "hi", "workspace": str(tmp_path)}
    )

    def no_list(harness: str) -> list[Launch]:
        raise AssertionError("a local launch must not list pending requests")

    monkeypatch.setattr(ctx.api, "pending_launches", no_list)

    def bg(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        return RunResult(None, "")

    run = Launcher(ctx, host, FAKE, 60, None, run_claude=bg)
    run.launch_pending(item["launch"])
    assert ctx.api.get_launch(HARNESS, item["launch"])["state"] == "claimed"


@pytest.mark.parametrize("named", [False, True])
def test_launch_real_announcement(named: bool) -> None:
    output = (
        "backgrounded · 1c5c3d7c"
        + (" · env-probe" if named else "")
        + "\n  claude attach 1c5c3d7c    open in this terminal\n"
    )
    assert launch_job_id(output) == "1c5c3d7c"


@pytest.mark.parametrize("settings", [["--settings", "{}"], ["--settings={}"]])
def test_host_settings_disable_marker(settings: list[str]) -> None:
    argv = launch_argv(
        FAKE, settings, cast(Launch, {"launch": "l_one", "prompt": "hi"})
    )
    assert argv == [FAKE, *settings, "--bg", "--", "hi"]


def test_fake_launch_settings_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid = str(uuid.uuid4())
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "job")
    monkeypatch.setenv("FAKE_CLAUDE_JOB", sid[:8])
    monkeypatch.setenv(
        "FAKE_CLAUDE_JOB_STATE", json.dumps({"sessionId": sid, "cwd": str(tmp_path)})
    )
    monkeypatch.setenv("HYPERSPACE_LAUNCH", "l_wrong_daemon_marker")
    argv = launch_argv(
        FAKE,
        [],
        cast(Launch, {"launch": "l_right", "parent": "parent", "prompt": "hi"}),
    )
    result = run_bg(argv, str(tmp_path), 10, {})
    assert result.returncode == 0
    env = json.loads((tmp_path / "jobs" / sid[:8] / "environment.json").read_text())
    env["CLAUDE_JOB_DIR"] = str(tmp_path / "jobs" / sid[:8])
    assert ClaudeCode.launch_marker("startup", env, sid) == ("l_right", "parent")
