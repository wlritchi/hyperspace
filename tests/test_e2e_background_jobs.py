# vim: filetype=python
"""Claude Code background jobs, stub transcripts, and damaged transcript
lines.

A background job gets its own id J. Claude Code writes a `continued-in`
record into the transcript of the session S that the job continues, and
writes J.jsonl with only title records. In the older shape, the job writes
its conversation into S.jsonl (`background_job`). In the shape of
`/background` in Claude Code 2.1.301, J.jsonl gets a copy of S's conversation
and the job continues there (`copied_job`).
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, tool_call
from hyperspace_client import backfill
from hyperspace_client import launcher as launcher_module
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
    _sync_one,
    _target,
    _targets,
    set_status,
)
from hyperspace_client.config import Config
from hyperspace_client.hooks import handle, notices
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


def proc_start(pid: int) -> str:
    """Field 22 of /proc/<pid>/stat, as Claude Code records it."""
    stat = Path(f"/proc/{pid}/stat").read_text()
    return stat[stat.rfind(")") + 2 :].split()[19]


def register(fake: FakeClaude, **record: Any) -> None:
    """Register this test process as a live Claude Code process, with its
    start time unless `record` gives one."""
    sessions = fake.root / "sessions"
    sessions.mkdir(exist_ok=True)
    record.setdefault("procStart", proc_start(os.getpid()))
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


def add_status(
    ctx: Context, sid: str, status: str, kind: str = "user", **more: Any
) -> Any:
    current = ctx.api.state(H, sid).get("status")
    body: dict[str, Any] = {
        "status": status,
        "source_kind": kind,
        "explicit": kind != "hook",
        "base_seq": current["seq"] if current else 0,
        **more,
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


def test_only_a_job_that_writes_its_own_id_closes(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    # A job whose state names the session that it resumes.
    s, j = background_job(fake)
    # A job whose state names only its own id.
    s2, j2 = background_job(fake)
    # Jobs whose own id has a conversation: with no other session (dd and
    # ee), and with a state that resumes another session (cc).
    j3, j4, j5 = (fake.transcript(WS) for _ in range(3))
    for t in (j3, j4, j5):
        fake.write(t, prompt("own work", cwd=WS))
    for t in (s, s2, j3, j4, j5):
        sync(ctx, t)
        add_status(ctx, t.stem, "done")
    # An older client backed up the stub of J, and a user marked J done.
    sync(ctx, j)
    add_status(ctx, j.stem, "done")
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    job_dir(fake, "bb", sessionId=j2.stem, resumeSessionId=j2.stem)
    job_dir(fake, "cc", sessionId=j3.stem, resumeSessionId=s.stem)
    job_dir(fake, "dd", sessionId=j4.stem)
    job_dir(fake, "ee", sessionId=j5.stem, resumeSessionId=j5.stem)
    jobs = fake.adapter.jobs(ctx.state.continued_from(H))
    assert [(x.short, x.session_id, x.own_id, x.candidates) for x in jobs] == [
        ("aa", None, j.stem, (s.stem,)),
        ("bb", None, j2.stem, (s2.stem,)),
        ("cc", None, j3.stem, (j3.stem, s.stem)),
        ("dd", j4.stem, j4.stem, (j4.stem,)),
        ("ee", j5.stem, j5.stem, (j5.stem,)),
    ]
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == [["claude", "rm", "dd"], ["claude", "rm", "ee"]]
    # A job that writes another session stays, also when each of its
    # sessions is done.
    another = "writes another session; close it yourself"
    assert close_blocked(ctx, j.stem) == f"job aa {another}"
    assert close_blocked(ctx, s2.stem) == f"job bb {another}"
    reason = "job cc can write more than one session; close it yourself"
    assert close_blocked(ctx, j3.stem) == reason


def test_a_job_whose_own_id_has_a_competing_conversation_is_not_closed(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    # `claude --resume J` in a terminal gave J a conversation, while the job
    # wrote S. The job's state names S.
    s, j = background_job(fake)
    fake.write(j, prompt("in a terminal", cwd=WS))
    sync(ctx, s)
    sync(ctx, j)
    add_status(ctx, j.stem, "done")
    add_status(ctx, s.stem, "working", kind="hook")
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == []
    for sid in (s, j):
        assert close_blocked(ctx, sid.stem) == (
            "job aa can write more than one session; close it yourself"
        )


def test_a_change_of_the_job_state_during_the_close_keeps_the_job(
    server: Server, fake: FakeClaude, ctx: Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    j = fake.transcript(WS)
    fake.write(j, prompt("own work", cwd=WS))
    sync(ctx, j)
    add_status(ctx, j.stem, "done")
    directory = job_dir(fake, "aa", sessionId=j.stem)
    other = str(uuid.uuid4())
    real = launcher_module.back_up

    def back_up(*args: Any) -> None:
        real(*args)
        # The job's state now names another session.
        (directory / "state.json").write_text(
            json.dumps({"sessionId": j.stem, "resumeSessionId": other})
        )

    monkeypatch.setattr(launcher_module, "back_up", back_up)
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == []
    reason = "job aa can write more than one session; close it yourself"
    assert close_blocked(ctx, j.stem) == reason


def hook_env(
    monkeypatch: pytest.MonkeyPatch, fake: FakeClaude, directory: Path
) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(directory))


@needs_proc
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
    register(fake, sessionId=j.stem, jobId="aa")
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
    # The job writes another session, so its end does not close it.
    ctx.state.drop_wanted_status(H, s.stem)
    event = add_status(ctx, s.stem, "done")
    ctx.state.save_status(H, s.stem, event, None)
    started.clear()
    handle({**payload, "hook_event_name": "SessionEnd"}, cfg, start=started.append)
    assert not any("--close-job" in args for args in started)


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


@needs_proc
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
    register(fake, sessionId=j.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _target(None) == s.stem
    assert _current(ctx.state) == s.stem
    # A job state that names only J: the continued-in record of S gives S.
    (directory / "state.json").write_text(
        json.dumps({"sessionId": j.stem, "resumeSessionId": j.stem})
    )
    # With no synced metadata, the session of the job is not known.
    assert _current() is None
    assert _current(ctx.state) == s.stem
    # Outside a job, the id does not change.
    monkeypatch.delenv("CLAUDE_JOB_DIR")
    assert _current(ctx.state) == j.stem
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(directory))
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
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


# --- Jobs that copy their session ---------------------------------------------------


def copied_job(fake: FakeClaude) -> tuple[Path, Path]:
    """`/background` in a session S, as Claude Code 2.1.301 does it: the job J
    gets a copy of S's conversation in J.jsonl, with J as the `sessionId` of
    each record, S's transcript gets a `continued-in` record with J, and S
    ends. Return the two transcripts."""
    s, j = fake.transcript(WS), fake.transcript(WS)
    first = prompt("investigate", cwd=WS)
    fake.write(
        s,
        {**first, "sessionId": s.stem},
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, {**first, "sessionId": j.stem})
    return s, j


@needs_proc
def test_a_job_that_copied_its_session_acts_on_its_own_id(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = copied_job(fake)
    sync(ctx, s)
    continued = ctx.state.continued_from(H)
    assert continued == {j.stem: [s.stem]}
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _current(ctx.state) == j.stem
    (job,) = fake.adapter.jobs(continued)
    assert (job.session_id, job.candidates) == (j.stem, (j.stem,))
    # S stays live while the job runs, so that nothing else resumes it.
    assert fake.adapter.live_sessions(continued) == [j.stem, s.stem]
    assert fake.adapter.is_running(s.stem)
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    payload = {"transcript_path": str(j), "cwd": WS}
    started: list[list[str]] = []
    handle({**payload, "hook_event_name": "Stop"}, cfg, start=started.append)
    assert started[0][4] == str(j)
    queued = ctx.state.wanted_status(H, j.stem)
    assert queued is not None and queued[0]["status"] == "ready"
    assert ctx.state.wanted_status(H, s.stem) is None
    # The end of the job starts its close when J is done.
    sync(ctx, j)
    event = add_status(ctx, j.stem, "done")
    ctx.state.save_status(H, j.stem, event, None)
    started.clear()
    handle({**payload, "hook_event_name": "SessionEnd"}, cfg, start=started.append)
    assert ["--quiet", "launcher", "--close-job", "aa"] in started


def test_a_done_job_that_copied_its_session_closes(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = copied_job(fake)
    for t in (s, j):
        sync(ctx, t)
    add_status(ctx, s.stem, "working", kind="hook")
    add_status(ctx, j.stem, "done")
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == [["claude", "rm", "aa"]]
    assert "close_blocked" not in ctx.api.state(H, j.stem).get("liveness", {})


@needs_proc
def test_a_job_before_the_copy_of_its_session_acts_on_none(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The job starts with only title records in J.jsonl, and the continued-in
    # record of S is not synced yet.
    s, j = fake.transcript(WS), fake.transcript(WS)
    fake.write(
        s,
        prompt("investigate", cwd=WS),
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, *stub_records(j.stem))
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _current(ctx.state) is None
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    acts_on_none(ctx, cfg, {"transcript_path": str(j), "cwd": WS}, (s.stem, j.stem))
    # The copy arrives, and then S's link is synced.
    fake.write(j, prompt("investigate", cwd=WS))
    assert _current(ctx.state) == j.stem
    sync(ctx, s)
    assert _current(ctx.state) == j.stem


# --- Sessions that continued in a job that copied them ------------------------------


def sync_one(ctx: Context, transcript: Path, tmp_path: Path) -> None:
    """A checkpoint or sweep sync of one session, with its checks after the
    sync."""
    session = ctx.adapter.session_for_transcript(transcript)
    assert session is not None
    with ctx.state.try_lock(H, session.session_id) as locked:
        assert locked
        _sync_one(Config(host="laptop", state_dir=tmp_path / "state"), ctx, session, WS)


def status_of(ctx: Context, sid: str) -> Any:
    return ctx.api.state(H, sid).get("status")


def background_before_copy(
    ctx: Context, fake: FakeClaude, tmp_path: Path
) -> tuple[Path, Path]:
    """`/background` in a session S that is `ready`: S gets a `continued-in`
    record with J and ends, and the job J starts with no conversation in
    J.jsonl. The sync at the end of S sends the link."""
    s, j = fake.transcript(WS), fake.transcript(WS)
    fake.write(s, {**prompt("investigate", cwd=WS), "sessionId": s.stem})
    sync(ctx, s)
    event = add_status(ctx, s.stem, "ready", kind="hook", reason="idle")
    ctx.state.save_status(H, s.stem, event, None)
    fake.write(
        s,
        {"type": "continued-in", "continuedInSessionId": j.stem, "sessionId": s.stem},
    )
    fake.write(j, *stub_records(j.stem))
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    sync_one(ctx, s, tmp_path)
    return s, j


def copy(fake: FakeClaude, j: Path) -> None:
    fake.write(j, {**prompt("investigate", cwd=WS), "sessionId": j.stem})


def test_a_session_that_a_job_copied_is_done_after_the_copy(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = background_before_copy(ctx, fake, tmp_path)
    before = status_of(ctx, s.stem)
    launcher(ctx, fake).tick()
    sync_one(ctx, s, tmp_path)
    assert status_of(ctx, s.stem) == before
    # Before the copy, the hooks of the job can act on S. The sync did not
    # send this write yet.
    want = {"status": "working", "source_kind": "hook", "explicit": False}
    ctx.state.want_status(H, s.stem, want)
    copy(fake, j)
    sync_one(ctx, j, tmp_path)
    event = status_of(ctx, s.stem)
    assert (event["status"], event["note"]) == ("done", f"continued in {j.stem}")
    assert (event["source"]["kind"], event["explicit"]) == ("hook", False)
    assert event["seq"] == before["seq"] + 1
    # The done replaced the queued write of the job from before the copy.
    assert ctx.state.wanted_status(H, s.stem) is None
    assert ctx.state.session(H, s.stem).status == event
    # No agent runs in S, so the done gives no notice.
    assert not ctx.state.has_notice(H, s.stem, "status")
    for sid in (s.stem, j.stem):
        assert not ctx.state.notice_flag(sid).exists()
    # The client writes the done once. A reopen of S stays.
    add_status(ctx, s.stem, "ready", reason="idle")
    reopened = status_of(ctx, s.stem)
    for t in (s, j):
        sync_one(ctx, t, tmp_path)
    launcher(ctx, fake).tick()
    assert status_of(ctx, s.stem) == reopened


def test_a_link_that_is_synced_after_the_copy_also_marks_the_session_done(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = copied_job(fake)
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    # The job syncs before the link of S is synced.
    sync_one(ctx, j, tmp_path)
    assert ctx.state.wanted_status(H, s.stem) is None
    sync_one(ctx, s, tmp_path)
    assert status_of(ctx, s.stem)["status"] == "done"


def test_the_launcher_marks_the_session_done_after_the_copy(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    # The job is idle after the copy, so that no sync of it comes.
    s, j = background_before_copy(ctx, fake, tmp_path)
    copy(fake, j)
    sync(ctx, j)
    launcher(ctx, fake).tick()
    assert status_of(ctx, s.stem)["status"] == "done"


def test_a_session_that_a_job_of_the_older_shape_writes_stays_open(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = background_job(fake)
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    sync_one(ctx, s, tmp_path)
    add_status(ctx, s.stem, "working", kind="hook")
    for t in (s, j):
        sync_one(ctx, t, tmp_path)
    launcher(ctx, fake).tick()
    assert status_of(ctx, s.stem)["status"] == "working"


def test_a_session_that_is_done_gets_no_second_done(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = background_before_copy(ctx, fake, tmp_path)
    add_status(ctx, s.stem, "done")
    done = status_of(ctx, s.stem)
    copy(fake, j)
    sync_one(ctx, j, tmp_path)
    assert status_of(ctx, s.stem) == done


@pytest.mark.parametrize("has_state", [True, False])
def test_a_job_that_resumes_another_session_or_has_no_state_leaves_it_open(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path, has_state: bool
) -> None:
    s, j = background_before_copy(ctx, fake, tmp_path)
    before = status_of(ctx, s.stem)
    if has_state:
        other = str(uuid.uuid4())
        job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=other)
    else:
        (fake.root / "jobs" / "aa" / "state.json").unlink()
    copy(fake, j)
    for t in (j, s):
        sync_one(ctx, t, tmp_path)
    launcher(ctx, fake).tick()
    assert status_of(ctx, s.stem) == before


@needs_proc
def test_a_session_whose_own_process_runs_stays_open(
    server: Server, fake: FakeClaude, ctx: Context, tmp_path: Path
) -> None:
    s, j = background_before_copy(ctx, fake, tmp_path)
    copy(fake, j)
    # A process under the id of S, for example `claude --resume S`.
    register(fake, sessionId=s.stem)
    sync_one(ctx, j, tmp_path)
    assert status_of(ctx, s.stem)["status"] == "ready"
    # The process of the job does not make S run for this check.
    register(fake, sessionId=j.stem, jobId="aa")
    assert fake.adapter.is_running(s.stem)
    sync_one(ctx, j, tmp_path)
    assert status_of(ctx, s.stem)["status"] == "done"


# --- Jobs whose session is not known ------------------------------------------------


def continued(fake: FakeClaude, j: Path, *sources: Path) -> None:
    for s in sources:
        fake.write(
            s,
            {
                "type": "continued-in",
                "continuedInSessionId": j.stem,
                "sessionId": s.stem,
            },
        )


def old_done_stub(ctx: Context, j: Path) -> None:
    """An older client backed up the stub of J, and a user marked J done. Then
    the stub went away."""
    sync(ctx, j)
    add_status(ctx, j.stem, "done")
    j.unlink()


def close_blocked(ctx: Context, sid: str) -> str | None:
    return ctx.api.state(H, sid)["liveness"].get("close_blocked")  # pyright: ignore[reportTypedDictNotRequiredAccess]


def test_a_job_that_more_than_one_session_continued_in_is_not_closed(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = background_job(fake)
    t = fake.transcript(WS)
    fake.write(t, prompt("other", cwd=WS))
    continued(fake, j, t)
    for x in (s, t):
        sync(ctx, x)
    assert ctx.state.continued_from(H) == {j.stem: sorted([s.stem, t.stem])}
    add_status(ctx, s.stem, "working", kind="hook")
    add_status(ctx, t.stem, "done")
    old_done_stub(ctx, j)
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    (job,) = fake.adapter.jobs(ctx.state.continued_from(H))
    assert job.session_id is None
    assert set(job.candidates) == {s.stem, t.stem}
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == []
    # The heartbeat says why, for each session that the job can write.
    reason = "job aa can write more than one session; close it yourself"
    for sid in (j.stem, s.stem, t.stem):
        assert close_blocked(ctx, sid) == reason
    # With one other session that the job can write, the job stays, and the
    # reason changes.
    (fake.root / "jobs" / "aa" / "state.json").write_text(
        json.dumps({"sessionId": j.stem, "resumeSessionId": t.stem})
    )
    ctx.state.save_session(SessionRecord(H, s.stem, workspace=WS), ("metadata",))
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == []
    reason = "job aa writes another session; close it yourself"
    assert close_blocked(ctx, j.stem) == close_blocked(ctx, t.stem) == reason
    assert close_blocked(ctx, s.stem) is None


def test_a_job_with_no_mapping_to_its_session_is_not_closed(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    # The backup of S is from before the continued-in record.
    s = fake.transcript(WS)
    fake.write(s, prompt("investigate", cwd=WS))
    sync(ctx, s)
    add_status(ctx, s.stem, "working", kind="hook")
    j = fake.transcript(WS)
    fake.write(j, *stub_records(j.stem))
    continued(fake, j, s)
    old_done_stub(ctx, j)
    assert ctx.state.continued_from(H) == {}
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    assert rm.calls == []
    reason = "the session of job aa is not known; close it yourself"
    assert close_blocked(ctx, j.stem) == reason


def test_a_job_with_a_stale_mapping_to_its_session_is_not_closed(
    server: Server, fake: FakeClaude, ctx: Context
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    add_status(ctx, s.stem, "working", kind="hook")
    # S continued again in a second job. The synced metadata keeps only the
    # last continued-in record.
    j2 = fake.transcript(WS)
    fake.write(j2, *stub_records(j2.stem))
    continued(fake, j2, s)
    sync(ctx, s)
    assert ctx.state.continued_from(H) == {j2.stem: [s.stem]}
    old_done_stub(ctx, j)
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    job_dir(fake, "bb", sessionId=j2.stem, resumeSessionId=j2.stem)
    rm = Rm()
    launcher(ctx, fake, run_rm=rm).tick()
    assert rm.calls == []


# --- The current session -----------------------------------------------------------


def test_a_foreground_resume_of_a_stub_is_its_own_session(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    assert ctx.state.continued_from(H) == {j.stem: [s.stem]}
    # `claude --resume J` in a terminal: no job.
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(fake.root))
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _current(ctx.state) == j.stem
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    started: list[list[str]] = []
    handle(
        {"transcript_path": str(j), "cwd": WS, "hook_event_name": "Stop"},
        cfg,
        start=started.append,
    )
    assert started[0][4] == str(j)
    assert ctx.state.wanted_status(H, s.stem) is None


@needs_proc
def test_a_stale_job_dir_does_not_change_the_session(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    monkeypatch.setenv(SESSION_ENV, j.stem)
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    payload = {"transcript_path": str(j), "cwd": WS, "hook_event_name": "Stop"}
    # No process runs the job.
    assert _current(ctx.state) == j.stem
    # A different process runs the job.
    with subprocess.Popen(["sleep", "30"]) as other:
        try:
            sessions = fake.root / "sessions"
            sessions.mkdir(exist_ok=True)
            (sessions / f"{other.pid}.json").write_text(
                json.dumps({"pid": other.pid, "sessionId": j.stem, "jobId": "aa"})
            )
            assert fake.adapter.is_running(s.stem)
            assert _current(ctx.state) == j.stem
            started: list[list[str]] = []
            handle(payload, cfg, start=started.append)
            assert started[0][4] == str(j)
            assert ctx.state.wanted_status(H, s.stem) is None
        finally:
            other.kill()
    # This process runs the job.
    register(fake, sessionId=j.stem, jobId="aa")
    assert _current(ctx.state) == s.stem


@needs_proc
def test_a_job_that_can_write_more_than_one_session_acts_on_none(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, j = background_job(fake)
    t = fake.transcript(WS)
    fake.write(t, prompt("other", cwd=WS))
    continued(fake, j, t)
    sync(ctx, t)
    # The job's state names S; the synced metadata says that T continued in J.
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _current(ctx.state) is None
    with pytest.raises(Exit, match="the session of this background job is not known"):
        _target(None, _current(ctx.state))
    # An explicit session is not the current session.
    request = _request(
        "done", t.stem, reason=None, wake=None, note=None, by_user=False, current=None
    )
    assert request["source_kind"] == "agent" and "source_session" not in request
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    started: list[list[str]] = []
    handle(
        {"transcript_path": str(j), "cwd": WS, "hook_event_name": "Stop"},
        cfg,
        start=started.append,
    )
    assert started[0][4] == str(j)
    for sid in (s.stem, t.stem, j.stem):
        assert ctx.state.wanted_status(H, sid) is None


@needs_proc
def test_a_job_whose_session_is_not_known_acts_on_none(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # J is a stub, and no synced metadata links a session to J.
    j = fake.transcript(WS)
    fake.write(j, *stub_records(j.stem))
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=j.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, j.stem)
    assert _current(ctx.state) is None
    with pytest.raises(Exit, match="the session of this background job is not known"):
        _target(None, _current(ctx.state))
    with pytest.raises(Exit, match="give a session id"):
        _targets([], by_user=False)
    # An explicit session id still works.
    assert _target(j.stem) == j.stem
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    started: list[list[str]] = []
    handle(
        {"transcript_path": str(j), "cwd": WS, "hook_event_name": "Stop"},
        cfg,
        start=started.append,
    )
    assert started[0][4] == str(j)
    assert ctx.state.wanted_status(H, j.stem) is None


@needs_proc
@pytest.mark.parametrize("shape", ["not known", "ambiguous"])
def test_a_job_whose_session_is_not_known_takes_no_status_notices(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    j = fake.transcript(WS)
    fake.write(j, *stub_records(j.stem))
    resumes = j.stem
    if shape == "ambiguous":
        s, t = fake.transcript(WS), fake.transcript(WS)
        fake.write(s, prompt("one", cwd=WS))
        fake.write(t, prompt("two", cwd=WS))
        continued(fake, j, t)
        sync(ctx, t)
        resumes = s.stem
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=resumes)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    payload = {"transcript_path": str(j), "cwd": WS}

    def hook(event: str, **more: Any) -> str:
        started: list[list[str]] = []
        return handle(
            {**payload, "hook_event_name": event, **more}, cfg, start=started.append
        )

    # An older client backed up the stub, and a user marked J done.
    sync(ctx, j)
    event = add_status(ctx, j.stem, "done")
    ctx.state.save_status(H, j.stem, event, None)
    marked = "[hyperspace] marked done"
    ctx.state.add_notice(H, j.stem, "status", marked)
    ctx.state.add_notice(H, j.stem, "lease", "[hyperspace] lease 1")
    monkeypatch.setenv("HYPERSPACE_LAUNCHER", "off")
    # No status notice at the start, but the lease notice of the payload.
    assert hook("SessionStart", source="startup") == "[hyperspace] lease 1"
    ctx.state.add_notice(H, j.stem, "lease", "[hyperspace] lease 2")
    assert hook("UserPromptSubmit") == "[hyperspace] lease 2"
    ctx.state.add_notice(H, j.stem, "lease", "[hyperspace] lease 3")
    tool = {**payload, "hook_event_name": "PostToolUse"}
    out = json.loads(notices(tool, cfg))
    assert out["hookSpecificOutput"]["additionalContext"] == "[hyperspace] lease 3"
    assert notices(tool, cfg) == ""
    assert ctx.state.has_notice(H, j.stem, "status")
    # The prevention checks still apply to the session of the payload.
    ctx.state.put(f"restore_pending:{H}:{j.stem}", "1")
    assert "unfinished restore" in hook("UserPromptSubmit")


def acts_on_none(
    ctx: Context,
    cfg: Config,
    payload: dict[str, Any],
    sessions: tuple[str, ...],
) -> None:
    """The hooks with the payload write no status for any of `sessions`, and
    take only the lease notices of the payload's session."""
    sid = Path(payload["transcript_path"]).stem
    started: list[list[str]] = []
    handle({**payload, "hook_event_name": "Stop"}, cfg, start=started.append)
    assert started[0][4] == payload["transcript_path"]
    for x in sessions:
        assert ctx.state.wanted_status(H, x) is None
    ctx.state.add_notice(H, sid, "status", "[hyperspace] marked done")
    ctx.state.add_notice(H, sid, "lease", "[hyperspace] lease 1")
    prompted = {**payload, "hook_event_name": "UserPromptSubmit"}
    out = handle(prompted, cfg, start=started.append)
    assert out == "[hyperspace] lease 1"
    ctx.state.add_notice(H, sid, "lease", "[hyperspace] lease 2")
    tool = {**payload, "hook_event_name": "PostToolUse"}
    out = json.loads(notices(tool, cfg))
    assert out["hookSpecificOutput"]["additionalContext"] == "[hyperspace] lease 2"
    assert ctx.state.has_notice(H, sid, "status")


@needs_proc
@pytest.mark.parametrize("incoming", ["own", "resumed"])
@pytest.mark.parametrize("competitor", ["own conversation", "continued link"])
def test_a_job_with_a_competing_session_acts_on_none_under_each_of_its_ids(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    incoming: str,
    competitor: str,
) -> None:
    # The job's state names J and S. J also has a conversation, or the
    # synced metadata says that T continued in J. The process and the hooks
    # can name J or S.
    s, j = background_job(fake)
    sessions = (s.stem, j.stem)
    if competitor == "own conversation":
        fake.write(j, prompt("in a terminal", cwd=WS))
    else:
        t = fake.transcript(WS)
        fake.write(t, prompt("other", cwd=WS))
        continued(fake, j, t)
        sync(ctx, t)
        sessions += (t.stem,)
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    named = j if incoming == "own" else s
    monkeypatch.setenv(SESSION_ENV, named.stem)
    assert _current(ctx.state) is None
    with pytest.raises(Exit, match="the session of this background job is not known"):
        _target(None, _current(ctx.state))
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    payload = {"transcript_path": str(named), "cwd": WS}
    acts_on_none(ctx, cfg, payload, sessions)


@needs_proc
def test_a_job_whose_state_has_no_valid_own_id_acts_on_none(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s, _ = background_job(fake)
    directory = job_dir(fake, "aa", sessionId="not a session", resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=s.stem, jobId="aa")
    monkeypatch.setenv(SESSION_ENV, s.stem)
    assert _current(ctx.state) is None


@needs_proc
@pytest.mark.parametrize("when", ["missing", "gone"])
def test_the_hooks_act_on_none_if_the_transcript_of_the_job_session_is_not_found(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    when: str,
) -> None:
    # The job resumes S, but this host has no transcript of S, or S's
    # transcript goes away after the hook resolves the session.
    s, j = background_job(fake)
    if when == "missing":
        s.unlink()
    else:
        real = ClaudeCode.resolve

        def resolve(self: ClaudeCode, *args: Any) -> str | None:
            sid = real(self, *args)
            s.unlink(missing_ok=True)
            return sid

        monkeypatch.setattr(ClaudeCode, "resolve", resolve)
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    register(fake, sessionId=j.stem, jobId="aa")
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    payload = {"transcript_path": str(j), "cwd": WS}
    acts_on_none(ctx, cfg, payload, (s.stem, j.stem))


@needs_proc
@pytest.mark.parametrize("start", ["missing", None, "1"])
def test_a_registry_record_with_no_matching_start_time_does_not_change_the_session(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    start: str | None,
) -> None:
    s, j = background_job(fake)
    sync(ctx, s)
    directory = job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    hook_env(monkeypatch, fake, directory)
    monkeypatch.setenv(SESSION_ENV, j.stem)
    # A stale record whose pid is now this process's pid.
    register(fake, sessionId=j.stem, jobId="aa", procStart=start)
    if start == "missing":
        record = fake.root / "sessions" / f"{os.getpid()}.json"
        fields = json.loads(record.read_text())
        del fields["procStart"]
        record.write_text(json.dumps(fields))
    assert _current(ctx.state) == j.stem
    cfg = Config(host="laptop", state_dir=tmp_path / "state")
    started: list[list[str]] = []
    payload = {"transcript_path": str(j), "cwd": WS, "hook_event_name": "Stop"}
    handle(payload, cfg, start=started.append)
    assert started[0][4] == str(j)
    assert ctx.state.wanted_status(H, s.stem) is None
    # The liveness check still counts a record with no start time.
    assert fake.adapter.is_running(s.stem) == (start != "1")
    # With the start time of this process, the id changes.
    register(fake, sessionId=j.stem, jobId="aa")
    assert _current(ctx.state) == s.stem


@pytest.mark.parametrize("failure", ["backup", "not found"])
def test_a_failed_close_replaces_the_old_reason(
    server: Server,
    fake: FakeClaude,
    ctx: Context,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    j, s, t = (fake.transcript(WS) for _ in range(3))
    for x in (j, s, t):
        fake.write(x, prompt("work", cwd=WS))
    continued(fake, j, t)
    for x in (s, t) if failure == "not found" else (j, s, t):
        sync(ctx, x)
    job_dir(fake, "aa", sessionId=j.stem, resumeSessionId=s.stem)
    rm = Rm()
    run = launcher(ctx, fake, run_rm=rm)
    run.tick()
    run.tick()
    old = "job aa can write more than one session; close it yourself"
    for x in (s, t) if failure == "not found" else (j, s, t):
        assert close_blocked(ctx, x.stem) == old
    # Now the job writes only J, but its close fails.
    job_dir(fake, "aa", sessionId=j.stem)
    ctx.state.save_session(SessionRecord(H, t.stem, workspace=WS), ("metadata",))
    if failure == "backup":
        add_status(ctx, j.stem, "done")

        def back_up(*args: Any) -> None:
            raise TeleportError("cannot close: test")

        monkeypatch.setattr(launcher_module, "back_up", back_up)
    run.tick()
    run.tick()
    assert rm.calls == []
    assert close_blocked(ctx, s.stem) is None
    assert close_blocked(ctx, t.stem) is None
    if failure == "backup":
        assert close_blocked(ctx, j.stem) == "cannot close: test"
