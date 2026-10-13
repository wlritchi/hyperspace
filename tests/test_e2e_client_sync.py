# vim: filetype=python
"""The client sync engine against an in-process server."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from _hs import (
    FakeClaude,
    Server,
    context,
    make_server,
    prompt,
    text_reply,
    tool_call,
    tool_result,
)
from hyperspace_client.adapters import LocalSession
from hyperspace_client.api import Api
from hyperspace_client.checkpoint import checkpoint
from hyperspace_client.cli import _sweep
from hyperspace_client.config import Config
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer
from store import SessionKey

WS = "/home/wlritchi/hyperspace"


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


@pytest.fixture
def fake(tmp_path: Path) -> FakeClaude:
    return FakeClaude(tmp_path / "claude")


def sync(ctx: Context, transcript: Path, workspace: str = WS) -> Any:
    session = ctx.adapter.session_for_transcript(transcript)
    assert session is not None
    return Syncer(ctx, session, workspace).run()


def log_entry(server: Server, sid: str, path: str = "transcript.jsonl") -> Any:

    manifest = server.store.manifest(SessionKey("default", "claude-code", sid))
    return next(f for f in manifest["files"] if f["path"] == path), manifest


def active_bytes(server: Server, sid: str, path: str = "transcript.jsonl") -> bytes:

    entry, _ = log_entry(server, sid, path)
    return server.store.read_log(
        SessionKey("default", "claude-code", sid), entry["active"], 0, None
    )


def test_growth_and_partial_lines(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    sid = t.stem
    fake.write(t, prompt("hello"), tool_call(), tool_result(), partial='{"type": "us')
    r = sync(ctx, t)
    full = t.read_bytes()
    boundary = full.rfind(b"\n") + 1
    assert active_bytes(server, sid) == full[:boundary]
    assert r.session["lease"]["host"] == "laptop"
    # Finish the partial line and add more.
    with open(t, "a") as f:
        f.write('er", "message": {"content": "more"}}\n')
    fake.write(t, text_reply())
    sync(ctx, t)
    assert active_bytes(server, sid) == t.read_bytes()
    entry, manifest = log_entry(server, sid)
    assert len(entry["generations"]) == 1
    # Activity: one prompt, one tool call, and the completed prompt.
    assert entry["generations"][0]["activity"] == 3
    assert manifest["metadata"]["workspace"] == WS
    assert manifest["metadata"]["cwd"] == WS


def test_nothing_to_send_is_a_no_op(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("hi"))
    sync(ctx, t)
    r = sync(ctx, t)
    assert r.uploaded == 0 and r.files == 0


def test_titles(server: Server, fake: FakeClaude, tmp_path: Path) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("hi"), {"type": "ai-title", "aiTitle": "AI title"})
    sync(ctx, t)
    _, manifest = log_entry(server, t.stem)
    assert manifest["metadata"]["title"] == "AI title"
    fake.write(
        t,
        {"type": "custom-title", "customTitle": "Mine"},
        {"type": "ai-title", "aiTitle": "Later AI title"},
    )
    sync(ctx, t)
    _, manifest = log_entry(server, t.stem)
    assert manifest["metadata"]["title"] == "Mine"


def test_tail_truncation_forks_a_rewrite(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("one"))
    sync(ctx, t)
    fake.write(t, tool_call())
    sync(ctx, t)
    # Claude Code removes the last record and writes a different one.
    keep = t.read_bytes().split(b"\n")[0] + b"\n"
    with open(t, "r+b") as f:
        f.truncate(len(keep))
    fake.write(t, text_reply("instead"))
    r = sync(ctx, t)
    assert active_bytes(server, t.stem) == t.read_bytes()
    entry, manifest = log_entry(server, t.stem)
    causes = sorted(g["cause"] for g in entry["generations"])
    assert causes == ["rewrite", "root"]
    assert manifest["state"]["state"] == "normal"
    assert not r.diverged


def test_full_rewrite_with_new_inode(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("one"))
    sync(ctx, t)
    fake.write(t, prompt("two"))
    sync(ctx, t)
    first = t.read_bytes().split(b"\n")[0] + b"\n"
    tmp = t.with_suffix(".tmp")
    tmp.write_bytes(first + b'{"type": "user", "message": {"content": "other"}}\n')
    os.replace(tmp, t)
    sync(ctx, t)
    assert active_bytes(server, t.stem) == t.read_bytes()
    entry, _ = log_entry(server, t.stem)
    rewrite = next(g for g in entry["generations"] if g["cause"] == "rewrite")
    assert rewrite["fork_offset"] == len(first)


def test_unreachable_server_then_recovery(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    state_dir = tmp_path / "state"
    t = fake.transcript(WS)
    fake.write(t, prompt("one"))
    down = Context(
        api=Api("http://127.0.0.1:9", "hsp_x", timeout=0.5),
        state=State(state_dir),
        adapter=fake.adapter,
        host="laptop",
    )
    with pytest.raises(httpx.HTTPError):
        sync(down, t)
    assert down.state.cursor(t) is None
    fake.write(t, prompt("two"))
    ctx = context(server.api("laptop"), state_dir, fake, "laptop")
    sync(ctx, t)
    assert active_bytes(server, t.stem) == t.read_bytes()


def test_lost_state_adopts_the_server_copy(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one"), tool_call())
    sync(context(server.api("laptop"), tmp_path / "s1", fake, "laptop"), t)
    fake.write(t, prompt("two"))
    # A new state directory: the client does not know what it sent.
    sync(context(server.api("laptop"), tmp_path / "s2", fake, "laptop"), t)
    entry, _ = log_entry(server, t.stem)
    assert len(entry["generations"]) == 1
    assert active_bytes(server, t.stem) == t.read_bytes()


def test_lost_cursor_save_is_adopted(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("one"))
    sync(ctx, t)
    saved = ctx.state.cursor(t)
    assert saved is not None
    fake.write(t, prompt("two"), prompt("three"))
    sync(ctx, t)
    # The append committed, but the cursor save was lost.
    ctx.state.save_cursor(saved)
    fake.write(t, prompt("four"))
    r = sync(ctx, t)
    assert not r.diverged
    entry, _ = log_entry(server, t.stem)
    assert len(entry["generations"]) == 1
    assert active_bytes(server, t.stem) == t.read_bytes()


def test_two_hosts_diverge(server: Server, fake: FakeClaude, tmp_path: Path) -> None:
    t = fake.transcript(WS)
    fake.write(t, prompt("one"))
    laptop = context(server.api("laptop"), tmp_path / "s1", fake, "laptop")
    sync(laptop, t)
    laptop.api.grant("claude-code", t.stem, "agent-1")
    # The agent has its own copy of the file, with different new content.
    other = FakeClaude(tmp_path / "claude2")
    t2 = other.transcript("/home/agent/workspace", t.stem)
    t2.write_bytes(t.read_bytes())
    agent = context(server.api("agent"), tmp_path / "s2", other, "agent-1")
    sync(agent, t2, "/home/agent/workspace")  # adopts the server copy
    other.write(t2, prompt("agent work"))
    sync(agent, t2, "/home/agent/workspace")
    fake.write(t, prompt("laptop work"))
    r = sync(laptop, t)
    assert r.diverged
    entry, manifest = log_entry(server, t.stem)
    assert sorted(g["cause"] for g in entry["generations"]) == ["diverge", "root"]
    assert manifest["state"]["state"] == "conflicted"


def test_side_files(server: Server, fake: FakeClaude, tmp_path: Path) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, {**prompt("hi"), "slug": "my-plan"})
    side = t.with_suffix("")
    (side / "subagents").mkdir(parents=True)
    (side / "subagents" / "agent-a1.jsonl").write_text('{"type": "user"}\n')
    (side / "subagents" / "agent-a1.meta.json").write_text('{"agentType": "x"}')
    (side / "tool-results").mkdir()
    (side / "tool-results" / "toolu 01.txt").write_text("big output")
    (fake.root / "plans").mkdir()
    (fake.root / "plans" / "my-plan.md").write_text("# Plan")
    history = fake.root / "file-history" / t.stem
    history.mkdir(parents=True)
    (history / "abc123@v1").write_text("old file")
    sync(ctx, t)
    _, manifest = log_entry(server, t.stem)
    paths = {f["path"]: f["kind"] for f in manifest["files"]}
    assert paths == {
        "transcript.jsonl": "log",
        "subagents/agent-a1.jsonl": "log",
        "subagents/agent-a1.meta.json": "blob",
        "tool-results/toolu%2001.txt": "blob",
        "plans/my-plan.md": "blob",
        "file-history/abc123@v1": "blob",
    }
    # Unchanged blobs are not sent again.
    assert sync(ctx, t).uploaded == 0


def test_set_aside_files_are_not_sessions(fake: FakeClaude) -> None:
    t = fake.transcript(WS)
    t.with_name(t.name + ".hyperspace-surrendered-1").write_text("x\n")
    t.with_name(f"{t.stem}.orphaned-1-x.jsonl").write_text("x\n")
    t.with_name("agent-abc.jsonl").write_text("x\n")
    assert [s.session_id for s in fake.adapter.discover()] == [t.stem]


def test_backfill_many_sessions(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:

    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    for i in range(20):
        t = fake.transcript(f"/home/wlritchi/p{i % 4}")
        fake.write(t, prompt(f"session {i}", cwd=f"/home/wlritchi/p{i % 4}"))
    cfg = Config(
        host="laptop", exclude_cwd=["/home/wlritchi/p3"], state_dir=tmp_path / "state"
    )
    _sweep(cfg, ctx, None)
    summaries = server.store.list_sessions("default", "laptop", "all")
    assert len(summaries) == 15
    assert {s["metadata"]["workspace"] for s in summaries} == {
        "/home/wlritchi/p0",
        "/home/wlritchi/p1",
        "/home/wlritchi/p2",
    }


# --- Checkpoint scheduling ----------------------------------------------------


def test_checkpoint_skips_while_another_process_syncs(tmp_path: Path) -> None:
    state = State(tmp_path / "state")
    calls: list[int] = []
    with state.try_lock("h", "s") as locked:
        assert locked
        runs = checkpoint(state, "h", "s", lambda: calls.append(1), 10, wait=False)
    assert runs == 0 and calls == []
    # The mark stays for the process that holds the lock.
    assert state.dirty_since("h", "s") is not None


def test_checkpoint_runs_again_for_marks_made_during_a_sync(tmp_path: Path) -> None:
    state = State(tmp_path / "state")
    sleeps: list[float] = []
    clock = iter(float(i) for i in range(100))
    calls = 0

    def work() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            # A hook fires during the first sync; its process cannot get the
            # lock and exits.
            other = State(tmp_path / "state")
            runs = checkpoint(other, "h", "s", lambda: None, 10, wait=False)
            assert runs == 0

    runs = checkpoint(
        state, "h", "s", work, 10, sleep=sleeps.append, clock=lambda: next(clock)
    )
    assert runs == 2
    assert len(sleeps) == 1 and sleeps[0] > 0
    assert state.dirty_since("h", "s") is None


def test_many_concurrent_checkpoints_give_one_sync_at_a_time(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    State(state_dir).close()
    active = 0
    peak = 0
    total = 0
    guard = threading.Lock()

    def work() -> None:
        nonlocal active, peak, total
        with guard:
            active += 1
            peak = max(peak, active)
            total += 1
        threading.Event().wait(0.02)
        with guard:
            active -= 1

    def trigger() -> None:
        state = State(state_dir)
        checkpoint(state, "h", "s", work, 0.01)
        state.close()

    threads = [threading.Thread(target=trigger) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak == 1
    assert 1 <= total < 20
    assert State(state_dir).dirty_since("h", "s") is None


def test_local_session_type() -> None:
    assert LocalSession("claude-code", "x", Path("/x.jsonl")).harness == "claude-code"


def test_oversized_side_files_go_up_as_logs(
    server: Server, fake: FakeClaude, tmp_path: Path
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("hi"))
    side = t.with_suffix("") / "tool-results"
    side.mkdir(parents=True)
    big = os.urandom(9 * 1024 * 1024)
    (side / "big.bin").write_bytes(big)
    sync(ctx, t)
    entry, _ = log_entry(server, t.stem, "tool-results/big.bin")
    assert entry["kind"] == "log"
    assert active_bytes(server, t.stem, "tool-results/big.bin") == big


def test_unchanged_files_are_not_hashed(
    server: Server, fake: FakeClaude, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    t = fake.transcript(WS)
    fake.write(t, prompt("hi"))
    sync(ctx, t)

    def fail(*args: object) -> bool:
        raise AssertionError("an unchanged file was hashed")

    monkeypatch.setattr("hyperspace_client.sync._last_segment_ok", fail)
    assert sync(ctx, t).uploaded == 0
    monkeypatch.undo()
    fake.write(t, prompt("more"))
    assert sync(ctx, t).uploaded > 0
