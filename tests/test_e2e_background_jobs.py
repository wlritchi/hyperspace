# vim: filetype=python
"""Claude Code background jobs, stub transcripts, and damaged transcript
lines.

A background job gets its own id J. Claude Code writes a `continued-in`
record into the transcript of the session S that the job continues, writes
J.jsonl with only title records, and the job writes its conversation into
S.jsonl.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, tool_call
from hyperspace_client import backfill
from hyperspace_client.adapters.claude_code import (
    STUB_MAX,
    TRANSCRIPT,
    ClaudeCode,
    is_stub,
)
from hyperspace_client.backfill import RunResult
from hyperspace_client.cli import (
    SESSION_ENV,
    Exit,
    _current,
    _hook_heartbeat,
    _request,
    _sweep,
    _target,
    _targets,
    set_status,
)
from hyperspace_client.config import Config
from hyperspace_client.hooks import handle
from hyperspace_client.launcher import Launcher
from hyperspace_client.state import LEASE_FIELDS, SessionRecord, State
from hyperspace_client.sync import REWRITE_SETTLE, Context, Syncer
from hyperspace_client.teleport import TeleportError, fork, restore, send
from store import SessionKey

H = "claude-code"
WS = "/work/project"
TITLE = "Nodes NotReady investigation"


def stub_records(sid: str) -> list[dict[str, Any]]:
    return [
        {"type": "ai-title", "aiTitle": TITLE, "sessionId": sid},
        {"type": "agent-name", "agentName": TITLE, "sessionId": sid},
    ]


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


@pytest.fixture
def fake(tmp_path: Path) -> FakeClaude:
    return FakeClaude(tmp_path / "claude")


@pytest.fixture
def ctx(server: Server, fake: FakeClaude, tmp_path: Path) -> Iterator[Context]:
    c = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    yield c
    c.state.close()


def sync(ctx: Context, transcript: Path, **kwargs: Any) -> Any:
    session = ctx.adapter.session_for_transcript(transcript)
    assert session is not None
    return Syncer(ctx, session, WS, **kwargs).run()


def server_ids(server: Server) -> set[str]:
    return {
        s["session_id"] for s in server.store.list_sessions("default", "laptop", "all")
    }


def transcript_entry(server: Server, sid: str) -> Any:
    manifest = server.store.manifest(SessionKey("default", H, sid))
    return next(f for f in manifest["files"] if f["path"] == TRANSCRIPT)


def active_bytes(server: Server, sid: str) -> bytes:
    entry = transcript_entry(server, sid)
    return server.store.read_log(
        SessionKey("default", H, sid), entry["active"], 0, None
    )


def background_job(fake: FakeClaude) -> tuple[Path, Path]:
    """A session S that continued in a background job J. Return the two
    transcripts."""
    s = fake.transcript(WS)
    j = fake.transcript(WS)
    fake.write(
        s,
        prompt("investigate", cwd=WS),
        {"type": "ai-title", "aiTitle": TITLE, "sessionId": s.stem},
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, *stub_records(j.stem))
    return s, j


# --- Stubs -------------------------------------------------------------------------


def test_stub_detection() -> None:
    sid = str(uuid.uuid4())
    stub = b"".join(json.dumps(r).encode() + b"\n" for r in stub_records(sid))
    assert is_stub(stub)
    assert is_stub(stub + b'{"type": "custom-title", "customTitle": "x"}\n')
    assert not is_stub(b"")
    assert not is_stub(b"\n\n")
    assert not is_stub(stub + json.dumps(prompt("hi")).encode() + b"\n")
    # An unknown record type, or a line that is not a record, can be
    # conversation.
    assert not is_stub(stub + b'{"type": "some-new-type"}\n')
    assert not is_stub(stub + b"not json\n")
    assert not is_stub(stub + b"[1, 2]\n")
    assert not is_stub(stub + b'{"type": []}\n')
    assert not is_stub(stub + b'{"type": {}}\n')
    assert not is_stub(stub * (STUB_MAX // len(stub) + 1))


def test_sweep_does_not_back_up_a_stub_until_it_has_a_conversation(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = background_job(fake)
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    _sweep(cfg, ctx, None)
    assert server_ids(server) == {s.stem}
    links = server.store.manifest(SessionKey("default", H, s.stem))["metadata"]
    assert links["links"] == {"continued_in": j.stem}
    # When the job writes its own conversation, J becomes a session, with
    # every byte.
    fake.write(j, prompt("now in the job", cwd=WS))
    _sweep(cfg, ctx, None)
    assert server_ids(server) == {s.stem, j.stem}
    assert active_bytes(server, j.stem) == j.read_bytes()


def test_a_stub_that_is_already_backed_up_keeps_syncing(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    # Earlier clients backed up stubs. Their sessions stay up to date.
    _, j = background_job(fake)
    sync(ctx, j)
    fake.write(j, {"type": "ai-title", "aiTitle": "Renamed", "sessionId": j.stem})
    _sweep(Config(host="laptop", state_dir=tmp_path / "state"), ctx, None)
    assert active_bytes(server, j.stem) == j.read_bytes()


def test_a_large_file_is_not_a_stub(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    title = {"type": "ai-title", "aiTitle": "x" * 1000, "sessionId": t.stem}
    fake.write(t, *([title] * 100))
    session = fake.adapter.session_for_transcript(t)
    assert session is not None
    assert not fake.adapter.is_stub(session)
    # Sync, restore and fork agree.
    sync(ctx, t)
    t.unlink()
    restore(ctx, fake.adapter, t.stem, WS)
    forked = fork(ctx, fake.adapter, t.stem, WS)
    assert forked.transcript.exists()


def test_restore_and_fork_refuse_a_stub(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    # An older client backed up the stub.
    sync(ctx, j)
    with pytest.raises(TeleportError) as info:
        restore(ctx, fake.adapter, j.stem, WS)
    message = str(info.value)
    assert "holds no conversation" in message
    assert f"Possible session: {s.stem}" in message
    assert "--force" in message
    with pytest.raises(TeleportError, match="nothing to fork"):
        fork(ctx, fake.adapter, j.stem, WS)
    restore(ctx, fake.adapter, j.stem, WS, force=True)
    assert is_stub(j.read_bytes())


# --- Running jobs ----------------------------------------------------------------


def register(fake: FakeClaude, **record: Any) -> None:
    """Register this test process as a live Claude Code process."""
    sessions = fake.root / "sessions"
    sessions.mkdir(exist_ok=True)
    (sessions / f"{os.getpid()}.json").write_text(
        json.dumps({"pid": os.getpid(), **record})
    )


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_a_job_under_its_own_id_runs_the_session_it_continues(
    fake: FakeClaude,
) -> None:
    s, j = background_job(fake)
    other = str(uuid.uuid4())
    register(fake, sessionId=other)
    assert not fake.adapter.is_running(s.stem)
    register(fake, sessionId=j.stem, jobId=j.stem[:8])
    assert fake.adapter.is_running(s.stem)
    assert fake.adapter.is_running(j.stem)
    assert not fake.adapter.is_running(other)


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_a_job_whose_state_resumes_the_session_runs_it(fake: FakeClaude) -> None:
    s = fake.transcript(WS)
    fake.write(s, prompt("hi", cwd=WS))
    job = fake.root / "jobs" / "abcd1234"
    job.mkdir(parents=True)
    (job / "state.json").write_text(
        json.dumps({"sessionId": str(uuid.uuid4()), "resumeSessionId": s.stem})
    )
    register(fake, sessionId=str(uuid.uuid4()), jobId="abcd1234")
    assert fake.adapter.is_running(s.stem)
    # A job id must name a directory in jobs/.
    register(fake, sessionId=str(uuid.uuid4()), jobId="../jobs/abcd1234")
    assert not fake.adapter.is_running(s.stem)


def test_a_stopped_job_does_not_run_the_session(fake: FakeClaude) -> None:
    s, j = background_job(fake)
    sessions = fake.root / "sessions"
    sessions.mkdir()
    # No process has this pid: pids are below 2**22 on Linux.
    (sessions / "1.json").write_text(
        json.dumps({"pid": 2**30, "sessionId": j.stem, "jobId": j.stem[:8]})
    )
    assert not fake.adapter.is_running(s.stem)


# --- NUL bytes before a record ----------------------------------------------------


def test_nul_bytes_before_a_record(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    record = json.dumps(prompt("after the hole", cwd=WS)).encode()
    title = json.dumps({"type": "ai-title", "aiTitle": "Torn"}).encode()
    data = b"\x00" * 1240 + record + b"\n" + b"\x00" * 3 + title + b"\n"
    t.write_bytes(data)
    session = fake.adapter.session_for_transcript(t)
    assert session is not None
    assert fake.adapter.first_cwd(session) == WS
    assert fake.adapter.activity(TRANSCRIPT, data) == 1
    sync(ctx, t)
    assert active_bytes(server, t.stem) == data
    manifest = server.store.manifest(SessionKey("default", H, t.stem))
    assert manifest["metadata"]["title"] == "Torn"
    assert manifest["metadata"]["record_count"] == 2

    new = str(uuid.uuid4())
    lines = ClaudeCode.fork_records(data, t.stem, new).splitlines(keepends=True)
    assert lines[0].startswith(b"\x00" * 1240)
    assert json.loads(lines[0].lstrip(b"\x00"))["cwd"] == WS
    assert lines[1].startswith(b"\x00" * 3)


# --- Rewrites --------------------------------------------------------------------


def test_a_recreated_empty_file_does_not_replace_the_backup(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one", cwd=WS), tool_call())
    sync(ctx, t)
    backup = t.read_bytes()
    # Claude Code sets the file aside and starts it again.
    os.rename(t, t.with_name(f"{t.stem}.orphaned-1-x.jsonl"))
    t.write_bytes(b'{"type": "ai-')
    sync(ctx, t)
    entry = transcript_entry(server, t.stem)
    assert [g["cause"] for g in entry["generations"]] == ["root"]
    assert active_bytes(server, t.stem) == backup
    # A complete record makes the fork.
    t.write_bytes(json.dumps(prompt("again", cwd=WS)).encode() + b"\n")
    sync(ctx, t)
    entry = transcript_entry(server, t.stem)
    rewrite = next(g for g in entry["generations"] if g["cause"] == "rewrite")
    assert rewrite["fork_offset"] == 0
    assert active_bytes(server, t.stem) == t.read_bytes()


def test_a_settled_shorter_file_gets_its_fork(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one", cwd=WS))
    sync(ctx, t)
    t.write_bytes(b"")
    sync(ctx, t)
    assert active_bytes(server, t.stem) != b""
    old = time.time() - REWRITE_SETTLE - 1
    os.utime(t, (old, old))
    sync(ctx, t)
    assert active_bytes(server, t.stem) == b""


def test_a_shorter_file_with_a_future_mtime_does_not_wait(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one", cwd=WS))
    sync(ctx, t)
    t.write_bytes(b"")
    future = time.time() + 86_400
    os.utime(t, (future, future))
    sync(ctx, t)
    assert active_bytes(server, t.stem) == b""


def test_a_same_size_replacement_with_the_same_mtime_is_uploaded(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("aaaa", cwd=WS))
    sync(ctx, t)
    st = t.stat()
    tmp = t.with_suffix(".tmp")
    tmp.write_bytes(t.read_bytes().replace(b"aaaa", b"bbbb"))
    os.utime(tmp, ns=(st.st_atime_ns, st.st_mtime_ns))
    os.replace(tmp, t)
    sync(ctx, t)
    assert active_bytes(server, t.stem) == t.read_bytes()
    assert sync(ctx, t).uploaded == 0


def test_send_forks_a_shorter_file_with_no_new_record(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one", cwd=WS))
    sync(ctx, t)
    fake.write(t, prompt("two", cwd=WS))
    sync(ctx, t)
    first = t.read_bytes().split(b"\n")[0] + b"\n"
    with open(t, "r+b") as f:
        f.truncate(len(first))
    sync(ctx, t)
    assert active_bytes(server, t.stem) != first
    send(ctx, fake.adapter, t.stem, None)
    assert active_bytes(server, t.stem) == first


# --- Session state -----------------------------------------------------------------


def test_saving_some_fields_keeps_the_others(tmp_path: Path) -> None:
    state = State(tmp_path / "state")
    try:
        record = SessionRecord(H, "s1", workspace=WS, metadata={"title": "x"})
        state.save_session(record)
        # A hook read the record before a sync saved new metadata.
        stale = state.session(H, "s1")
        record.metadata = {"title": "y"}
        state.save_session(record)
        stale.lease_state, stale.lease_host, stale.state = "held", "laptop", "normal"
        state.save_session(stale, LEASE_FIELDS)
        saved = state.session(H, "s1")
        assert saved.metadata == {"title": "y"}
        assert (saved.lease_state, saved.lease_host) == ("held", "laptop")
        # A new row gets defaults for the other columns.
        state.save_session(SessionRecord(H, "s2", workspace=WS), ("workspace",))
        assert state.session(H, "s2") == SessionRecord(H, "s2", workspace=WS)
        with pytest.raises(ValueError, match="unknown"):
            state.save_session(record, ("workspace; DROP TABLE sessions",))
    finally:
        state.close()


# --- Status and liveness of a job that writes another session ---------------------

needs_proc = pytest.mark.skipif(
    not Path("/proc/self/stat").exists(), reason="needs /proc"
)


def job_dir(fake: FakeClaude, short: str, **state: Any) -> Path:
    directory = fake.root / "jobs" / short
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "state.json").write_text(json.dumps(state, indent=2))
    return directory


def add_status(ctx: Context, sid: str, status: str, kind: str = "user") -> Any:
    current = ctx.api.state(H, sid).get("status")
    body: dict[str, Any] = {
        "status": status,
        "source_kind": kind,
        "explicit": kind != "hook",
        "base_seq": current["seq"] if current else 0,
    }
    return ctx.api.add_status(H, sid, body)  # pyright: ignore[reportArgumentType]


def no_start(args: list[str]) -> None:
    raise AssertionError(f"the launcher started {args}")


class Rm:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> tuple[int, str]:
        self.calls.append(argv)
        return 0, ""


def launcher(ctx: Context, fake: FakeClaude, **kw: Any) -> Launcher:
    kw.setdefault("start", no_start)
    kw.setdefault("run_rm", Rm())
    return Launcher(ctx, fake.adapter, "claude", 60, None, **kw)


def liveness(ctx: Context, sid: str) -> str:
    return ctx.api.state(H, sid)["liveness"]["state"]  # pyright: ignore[reportTypedDictNotRequiredAccess]


@needs_proc
def test_a_job_under_its_own_id_makes_its_session_live(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    continued = ctx.state.continued_from(H)
    assert continued == {j.stem: [s.stem]}
    # The job's state names only its own id: the continued-in record in S's
    # synced metadata links the two.
    job_dir(fake, j.stem[:8], sessionId=j.stem, resumeSessionId=j.stem)
    register(fake, sessionId=j.stem, jobId=j.stem[:8])
    assert fake.adapter.live_sessions(continued) == [j.stem, s.stem]
    launcher(ctx, fake).tick()
    assert liveness(ctx, s.stem) == "live"
    # The SessionEnd heartbeat of the job leaves out each id of the job.
    _hook_heartbeat(Config(host="laptop"), ctx, s.stem, "SessionEnd")
    assert liveness(ctx, s.stem) == "stopped"


@needs_proc
def test_a_job_makes_the_session_in_its_state_live(fake: FakeClaude) -> None:
    s = fake.transcript(WS)
    fake.write(s, prompt("hi", cwd=WS))
    own = str(uuid.uuid4())
    job_dir(fake, "abcd1234", sessionId=own, resumeSessionId=s.stem)
    register(fake, sessionId=own, jobId="abcd1234")
    assert fake.adapter.live_processes() == [[own, s.stem]]
    assert fake.adapter.live_sessions() == [own, s.stem]


@needs_proc
def test_the_runner_does_not_resume_a_session_that_a_job_writes(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    s, j = fake.transcript(str(ws)), fake.transcript(str(ws))
    fake.write(
        s,
        prompt("investigate", cwd=str(ws)),
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, *stub_records(j.stem))
    session = fake.adapter.session_for_transcript(s)
    assert session is not None
    Syncer(ctx, session, str(ws)).run()
    add_status(ctx, s.stem, "working", kind="hook")
    register(fake, sessionId=j.stem, jobId=j.stem[:8])
    calls: list[list[str]] = []

    def bg(argv: list[str], cwd: str, timeout: float) -> RunResult:
        calls.append(argv)
        return RunResult(0, "")

    run = launcher(ctx, fake, runner=True, workspace=str(ws), run_claude=bg)
    run.tick()
    assert calls == []
    assert liveness(ctx, s.stem) == "live"
    # The job stops: the next tick resumes the session.
    for path in (fake.root / "sessions").iterdir():
        path.unlink()
    run.tick()
    assert [argv[-2] for argv in calls] == [s.stem]


def test_a_job_closes_on_the_status_of_the_session_that_it_writes(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    # A job whose state names the session that it resumes.
    s, j = background_job(fake)
    # A job whose state names only its own id.
    s2, j2 = background_job(fake)
    # A job that has a conversation under its own id writes its own
    # transcript.
    j3 = fake.transcript(WS)
    fake.write(j3, prompt("own work", cwd=WS))
    for t in (s, s2, j3):
        sync(ctx, t)
        add_status(ctx, t.stem, "done")
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    job_dir(fake, "bb", sessionId=j2.stem, resumeSessionId=j2.stem)
    job_dir(fake, "cc", sessionId=j3.stem, resumeSessionId=s.stem)
    jobs = fake.adapter.jobs(ctx.state.continued_from(H))
    assert [(x.short, x.session_id, x.own_id) for x in jobs] == [
        ("aa", s.stem, j.stem),
        ("bb", s2.stem, j2.stem),
        ("cc", j3.stem, j3.stem),
    ]
    rm = Rm()
    launcher(ctx, fake, run_rm=rm).tick()
    assert rm.calls == [["claude", "rm", short] for short in ("aa", "bb", "cc")]


def hook_env(
    monkeypatch: pytest.MonkeyPatch, fake: FakeClaude, directory: Path
) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(directory))


def test_the_hooks_of_a_job_under_its_own_id_act_on_its_session(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    event = add_status(ctx, s.stem, "working", kind="hook")
    ctx.state.save_status(H, s.stem, event, None)
    directory = job_dir(
        fake,
        "aa",
        sessionId=j.stem,
        resumeSessionId=s.stem,
        inFlight={"tasks": 2},
    )
    hook_env(monkeypatch, fake, directory)
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    # The hooks name the job's own id.
    payload = {"transcript_path": str(j), "cwd": WS}
    started: list[list[str]] = []
    # The in-flight tasks of the job are for S.
    handle({**payload, "hook_event_name": "Stop"}, cfg, start=started.append)
    assert started[0][:5] == ["--quiet", "sync", "--checkpoint", "--transcript", str(s)]
    queued = ctx.state.wanted_status(H, s.stem)
    assert queued is not None
    assert (queued[0]["status"], queued[0]["wake"]) == ("waiting", "local")
    assert ctx.state.wanted_status(H, j.stem) is None
    # At the end of the job, the close depends on the status of S.
    ctx.state.drop_wanted_status(H, s.stem)
    event = add_status(ctx, s.stem, "done")
    ctx.state.save_status(H, s.stem, event, None)
    started.clear()
    handle({**payload, "hook_event_name": "SessionEnd"}, cfg, start=started.append)
    assert ["--quiet", "launcher", "--close-job", "aa"] in started


@needs_proc
def test_backfill_skips_stubs_and_the_sessions_of_live_jobs(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    s, j, idle = (fake.transcript(str(ws)) for _ in range(3))
    fake.write(
        s,
        prompt("investigate", cwd=str(ws)),
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, *stub_records(j.stem))
    fake.write(idle, prompt("idle", cwd=str(ws)))
    # Also a stub that an older client already backed up.
    for t in (s, j, idle):
        session = fake.adapter.session_for_transcript(t)
        assert session is not None
        Syncer(ctx, session, str(ws)).run()
    register(fake, sessionId=j.stem, jobId=j.stem[:8])
    selection = backfill.select(ctx, fake.adapter, 3600, time.time())
    assert [c.session.session_id for c in selection.candidates] == [idle.stem]
    assert selection.skipped == {"live": 2}
    # When the job stops, the stub is still not messaged.
    for path in (fake.root / "sessions").iterdir():
        path.unlink()
    selection = backfill.select(ctx, fake.adapter, 3600, time.time())
    assert {c.session.session_id for c in selection.candidates} == {s.stem, idle.stem}
    assert selection.skipped == {"no conversation": 1}


def test_the_cli_acts_on_the_session_that_a_job_writes(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _target(None) == s.stem
    assert _current(ctx.state) == s.stem
    # With no job state, the continued-in record of S gives S.
    monkeypatch.delenv("CLAUDE_JOB_DIR")
    assert _current() == j.stem
    assert _current(ctx.state) == s.stem
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(directory))
    # A write for the current session is the agent's declaration for S.
    request = _request("done", s.stem, reason=None, wake=None, note="x", by_user=False)
    assert request["source_kind"] == "agent" and "source_session" not in request
    assert set_status(ctx, s.stem, request) is not None
    assert ctx.state.session(H, s.stem).declared == request
    # A backfill turn accepts exactly the resolved session.
    monkeypatch.setenv(backfill.SELF_ONLY_ENV, "1")
    assert _targets([], by_user=False) == [s.stem]
    assert _targets([s.stem], by_user=False) == [s.stem]
    with pytest.raises(Exit):
        _targets([j.stem], by_user=False)


def test_a_notice_for_a_session_flags_the_id_that_it_continued_in(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    ctx.state.add_notice(H, s.stem, "status", "[hyperspace] marked done")
    assert ctx.state.notice_flag(s.stem).exists()
    assert ctx.state.notice_flag(j.stem).exists()
    assert ctx.state.take_notices(H, s.stem) == ["[hyperspace] marked done"]
    assert not ctx.state.notice_flag(s.stem).exists()
    assert not ctx.state.notice_flag(j.stem).exists()
