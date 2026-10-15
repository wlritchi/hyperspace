# vim: filetype=python
"""Teleport, restore, fork, notices, and lease loss, against an in-process
server, with two fake hosts."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, tool_call
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api
from hyperspace_client.notices import (
    after_sync,
    check_resume,
    refresh_states,
    session_start_notice,
)
from hyperspace_client.sync import Context, Syncer, register_host
from hyperspace_client.teleport import (
    SURRENDERED,
    TeleportError,
    fork,
    restore,
    send,
)
from store import SessionKey

LAPTOP_WS = "/home/wlritchi/hyperspace"
AGENT_WS = "/home/agent/workspace"


@dataclass
class Host:
    name: str
    fake: FakeClaude
    ctx: Context

    @property
    def adapter(self) -> ClaudeCode:
        return self.fake.adapter


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


@pytest.fixture
def laptop(server: Server, tmp_path: Path) -> Host:
    fake = FakeClaude(tmp_path / "laptop-claude")
    ctx = context(server.api("laptop"), tmp_path / "laptop-state", fake, "laptop")
    register_host(ctx, "workstation", force=True)
    return Host("laptop", fake, ctx)


@pytest.fixture
def agent(server: Server, tmp_path: Path) -> Host:
    fake = FakeClaude(tmp_path / "agent-claude")
    ctx = context(server.api("agent"), tmp_path / "agent-state", fake, "agent-1")
    register_host(ctx, "runner", force=True)
    return Host("agent-1", fake, ctx)


def start_session(host: Host, workspace: str) -> Path:
    t = host.fake.transcript(workspace)
    host.fake.write(t, prompt("hello", cwd=workspace), tool_call())
    (t.with_suffix("") / "tool-results").mkdir(parents=True)
    (t.with_suffix("") / "tool-results" / "out.txt").write_text("saved output")
    sync(host, t, workspace)
    return t


def sync(host: Host, transcript: Path, workspace: str) -> None:
    session = host.adapter.session_for_transcript(transcript)
    assert session is not None
    Syncer(host.ctx, session, workspace).run()
    after_sync(host.ctx, host.adapter, session)


def manifest(server: Server, sid: str) -> dict:  # type: ignore[type-arg]
    return server.store.manifest(SessionKey("default", "claude-code", sid))  # type: ignore[return-value]


def test_teleport_and_restore(server: Server, laptop: Host, agent: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    message = send(laptop.ctx, laptop.adapter, sid, "agent-1")
    assert "hyperspace restore" in message
    # The local copy is no longer resumable on the laptop.
    assert laptop.adapter.find_transcripts(sid) == []
    assert list(t.parent.glob(f"{t.name}{SURRENDERED}*"))

    result = restore(agent.ctx, agent.adapter, sid, AGENT_WS)
    restored = agent.adapter.project_dir(AGENT_WS) / f"{sid}.jsonl"
    assert result.transcript == restored
    # The bytes are the server's bytes, with no change.
    surrendered = next(t.parent.glob(f"{t.name}{SURRENDERED}*"))
    assert restored.read_bytes() == surrendered.read_bytes()
    assert (restored.with_suffix("") / "tool-results" / "out.txt").read_text() == (
        "saved output"
    )
    m = manifest(server, sid)
    assert m["state"]["lease"]["host"] == "agent-1"

    # The SessionStart notice explains the move once.
    session = agent.adapter.session_for_transcript(restored)
    assert session is not None
    text = session_start_notice(
        agent.ctx.state, agent.adapter, session, "agent-1", AGENT_WS, "resume"
    )
    assert "teleported" in text
    assert LAPTOP_WS in text and AGENT_WS in text
    assert "tool-results" in text
    assert (
        session_start_notice(
            agent.ctx.state, agent.adapter, session, "agent-1", AGENT_WS, "startup"
        )
        == ""
    )

    # The agent continues the same generation.
    agent.fake.write(restored, prompt("agent work", cwd=AGENT_WS))
    sync(agent, restored, AGENT_WS)
    m = manifest(server, sid)
    entry = next(f for f in m["files"] if f["path"] == "transcript.jsonl")
    assert len(entry["generations"]) == 1
    assert [p["cause"] for p in m["placements"]] == ["start", "teleport"]


def test_restore_refuses_when_another_host_holds_the_lease(
    laptop: Host, agent: Host
) -> None:
    t = start_session(laptop, LAPTOP_WS)
    laptop.ctx.api.grant("claude-code", t.stem, "agent-1")
    with pytest.raises(TeleportError, match="laptop holds the lease"):
        restore(agent.ctx, agent.adapter, t.stem, AGENT_WS)


def test_send_refuses_a_running_session(laptop: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sessions = laptop.fake.root / "sessions"
    sessions.mkdir()
    pid = os.getpid()
    stat = Path(f"/proc/{pid}/stat").read_text()
    start = stat[stat.rfind(")") + 2 :].split()[19]
    (sessions / f"{pid}.json").write_text(
        json.dumps({"pid": pid, "sessionId": t.stem, "procStart": start})
    )
    with pytest.raises(TeleportError, match="running"):
        send(laptop.ctx, laptop.adapter, t.stem, "agent-1")
    # A record whose process started at another time does not count.
    (sessions / f"{pid}.json").write_text(
        json.dumps({"pid": pid, "sessionId": t.stem, "procStart": "1"})
    )
    send(laptop.ctx, laptop.adapter, t.stem, "agent-1")


def test_steal_then_old_host_moves_its_copy_aside(
    server: Server, laptop: Host, agent: Host
) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS, steal=True)
    assert [p["cause"] for p in manifest(server, sid)["placements"]][-1:] == ["start"]
    # A sync with no new bytes learns nothing; the sweep's refresh sees the
    # lease elsewhere, and moves the laptop's copy aside.
    sync(laptop, t, LAPTOP_WS)
    assert laptop.adapter.find_transcripts(sid) == [t]
    assert refresh_states(laptop.ctx, laptop.adapter) == 1
    assert laptop.adapter.find_transcripts(sid) == []


def test_lost_lease_on_a_running_session_gives_a_notice(
    laptop: Host, agent: Host
) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS, steal=True)
    laptop.fake.write(t, prompt("still working here"))
    original = laptop.adapter.is_running
    laptop.adapter.is_running = lambda session_id: True  # type: ignore[method-assign]
    try:
        sync(laptop, t, LAPTOP_WS)
    finally:
        laptop.adapter.is_running = original  # type: ignore[method-assign]
    assert laptop.adapter.find_transcripts(sid) == [t]
    notices = laptop.ctx.state.take_notices("claude-code", sid)
    assert notices and "agent-1 holds the lease" in notices[0]


def test_prevention_blocks_the_first_prompt_after_a_stale_resume(
    laptop: Host, agent: Host
) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS, steal=True)
    session = laptop.adapter.session_for_transcript(t)
    assert session is not None
    # The laptop was off: it resumes its old copy before any sync.
    time.sleep(0.01)
    session_start_notice(
        laptop.ctx.state, laptop.adapter, session, "laptop", LAPTOP_WS, "resume"
    )
    decision = check_resume(laptop.ctx, session)
    assert decision is not None and decision["decision"] == "block"
    assert "agent-1 holds the lease" in decision["reason"]
    # Only the first prompt is checked.
    assert check_resume(laptop.ctx, session) is None


def test_prevention_permits_when_the_server_is_down(laptop: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    session = laptop.adapter.session_for_transcript(t)
    assert session is not None
    session_start_notice(
        laptop.ctx.state, laptop.adapter, session, "laptop", LAPTOP_WS, "resume"
    )

    down = Context(
        api=Api("http://127.0.0.1:9", "hsp_x", timeout=0.2),
        state=laptop.ctx.state,
        adapter=laptop.adapter,
        host="laptop",
    )
    record = laptop.ctx.state.session("claude-code", t.stem)
    record.state_updated = 0
    laptop.ctx.state.save_session(record)
    assert check_resume(down, session) is None


def test_teleport_back_fast_forwards(server: Server, laptop: Host, agent: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    send(laptop.ctx, laptop.adapter, sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS)
    restored = agent.adapter.project_dir(AGENT_WS) / f"{sid}.jsonl"
    agent.fake.write(restored, prompt("agent work", cwd=AGENT_WS))
    sync(agent, restored, AGENT_WS)
    send(agent.ctx, agent.adapter, sid, "laptop")
    # Put the laptop's old copy back in place, as if it was never moved.
    surrendered = next(t.parent.glob(f"{t.name}{SURRENDERED}*"))
    os.rename(surrendered, t)
    result = restore(laptop.ctx, laptop.adapter, sid, LAPTOP_WS)
    assert result.fast_forward
    assert b"agent work" in t.read_bytes()
    entry = next(
        f for f in manifest(server, sid)["files"] if f["path"] == "transcript.jsonl"
    )
    assert len(entry["generations"]) == 1


def test_restore_refuses_a_second_copy(laptop: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    with pytest.raises(TeleportError, match="another project directory"):
        restore(laptop.ctx, laptop.adapter, t.stem, "/somewhere/else")
    restore(laptop.ctx, laptop.adapter, t.stem, "/somewhere/else", force=True)
    assert laptop.adapter.find_transcripts(t.stem) == [
        laptop.adapter.project_dir("/somewhere/else") / f"{t.stem}.jsonl"
    ]


@pytest.mark.parametrize(
    "text", ["laptop work", "high surrogate \ud83d", "low surrogate \ude00"]
)
def test_conflict_then_fork(
    server: Server, laptop: Host, agent: Host, text: str
) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS, steal=True)
    restored = agent.adapter.project_dir(AGENT_WS) / f"{sid}.jsonl"
    agent.fake.write(restored, prompt("agent work", cwd=AGENT_WS))
    sync(agent, restored, AGENT_WS)
    # The laptop was off during the steal, and its user kept working.
    laptop.fake.write(t, prompt(text))
    session = laptop.adapter.session_for_transcript(t)
    assert session is not None
    Syncer(laptop.ctx, session, LAPTOP_WS).run()
    assert manifest(server, sid)["state"]["state"] == "conflicted"
    with pytest.raises(TeleportError, match="conflicted"):
        restore(agent.ctx, agent.adapter, sid, "/tmp/elsewhere", force=True)
    with pytest.raises(TeleportError, match="choose a branch"):
        fork(laptop.ctx, laptop.adapter, sid, LAPTOP_WS)
    branches = manifest(server, sid)["state"]["branches"]
    mine = next(b for b in branches if b["host"] == "laptop")
    result = fork(
        laptop.ctx, laptop.adapter, sid, "/home/wlritchi/fork", mine["generation"]
    )
    copy = result.transcript.read_text().splitlines()
    original = t.read_text().splitlines()
    assert len(copy) == len(original)
    for a, b in zip(original, copy, strict=True):
        ra, rb = json.loads(a), json.loads(b)
        if "sessionId" in ra:
            assert rb["sessionId"] == result.new_id
        if "uuid" in ra:
            assert rb["forkedFrom"] == {"sessionId": sid, "messageUuid": ra["uuid"]}
            assert rb["uuid"] == ra["uuid"]
        ra.pop("sessionId", None)
        rb.pop("sessionId", None)
        rb.pop("forkedFrom", None)
        assert ra == rb
    m = manifest(server, result.new_id)
    assert m["forked_from"]["session_id"] == sid
    assert m["state"]["lease"]["host"] == "laptop"


def test_fork_matches_claude_code_format() -> None:
    """The adapter's fork output has the shape that Claude Code writes for
    --fork-session (seen in a real forked transcript): every record gets the
    new sessionId; records with a uuid keep it and get forkedFrom."""
    old, new = (
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
    )
    records = [
        {"type": "user", "uuid": "u1", "sessionId": old, "message": {"content": "hi"}},
        {"type": "custom-title", "customTitle": "t", "sessionId": old},
        {"type": "file-history-snapshot", "snapshot": {}},
    ]
    data = "".join(json.dumps(r) + "\n" for r in records).encode() + b"not json\n"
    out = ClaudeCode.fork_records(data, old, new).decode().splitlines()
    first, second, third = (json.loads(x) for x in out[:3])
    assert first["sessionId"] == new and first["uuid"] == "u1"
    assert first["forkedFrom"] == {"sessionId": old, "messageUuid": "u1"}
    assert second["sessionId"] == new and "forkedFrom" not in second
    assert third == records[2]
    assert out[3] == "not json"


def test_restore_held_after_config_loss(
    server: Server, agent: Host, tmp_path: Path
) -> None:
    t = start_session(agent, AGENT_WS)
    sid = t.stem
    # The pod restarts: the config directory is new and empty, the state
    # directory too.
    fresh = FakeClaude(tmp_path / "agent-claude-2")
    ctx = context(agent.ctx.api, tmp_path / "agent-state-2", fresh, "agent-1")
    restore(ctx, fresh.adapter, sid, AGENT_WS, held_only=True)
    assert fresh.adapter.find_transcripts(sid) == [
        fresh.adapter.project_dir(AGENT_WS) / f"{sid}.jsonl"
    ]


def test_plan_slug_cannot_escape(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    secret = tmp_path / "secret.md"
    secret.write_text("private")
    (fake.root / "plans").mkdir()
    t = fake.transcript(LAPTOP_WS)
    session = fake.adapter.session_for_transcript(t)
    assert session is not None
    for slug in ("../../secret", "../secret", "/etc/passwd", "a/b"):
        paths = [f.path for f in fake.adapter.files(session, {"_slug": slug})]
        assert all("secret" not in str(p) for p in paths), slug


def test_side_dir_symlinks_are_not_followed(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "private.txt").write_text("private")
    t = fake.transcript(LAPTOP_WS)
    side = t.with_suffix("") / "tool-results"
    side.mkdir(parents=True)
    (side / "link").symlink_to(outside)
    (side / "file-link").symlink_to(outside / "private.txt")
    session = fake.adapter.session_for_transcript(t)
    assert session is not None
    assert [f.rel_path for f in fake.adapter.files(session, {})] == ["transcript.jsonl"]


def test_cli_entry_point_runs() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "hyperspace_client.cli", "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    for command in ("sync", "teleport", "restore", "fork", "hook"):
        assert command in out.stdout


def test_restore_refuses_traversal_session_ids(laptop: Host) -> None:
    for bad in ("../../escape", "/abs", ".hidden", "a/b"):
        with pytest.raises(ValueError):
            restore(laptop.ctx, laptop.adapter, bad, LAPTOP_WS)


def test_local_paths_stay_in_their_directories(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    session_dir = (
        fake.adapter.project_dir(LAPTOP_WS) / "11111111-1111-1111-1111-111111111111"
    )
    session_dir.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (session_dir / "tool-results").symlink_to(outside)
    assert fake.adapter.local_path(session_dir, "tool-results/x.txt") is None
    for bad in (
        "../x",
        "plans/..%2Fx",
        "plans/a%2Fb",
        "file-history/%2E%2E",
        "other/x",
    ):
        assert fake.adapter.local_path(session_dir, bad) is None, bad
    assert fake.adapter.local_path(session_dir, "subagents/agent-1.jsonl") == (
        session_dir / "subagents" / "agent-1.jsonl"
    )


def test_a_host_that_never_held_the_lease_never_moves_files(
    laptop: Host, agent: Host
) -> None:
    """Two host identities can see the same transcript (a shared config
    directory, or a copied file). Only the host that held the lease and lost
    it may move its copy aside; any other host only gets a notice."""
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    copy = agent.fake.transcript(AGENT_WS, sid)
    copy.write_bytes(t.read_bytes())
    sync(agent, copy, AGENT_WS)
    assert agent.adapter.find_transcripts(sid) == [copy]
    refresh_states(agent.ctx, agent.adapter)
    assert agent.adapter.find_transcripts(sid) == [copy]
    # Even with the lease state in its record, it only gets a notice.
    record = agent.ctx.state.session("claude-code", sid)
    record.lease_state, record.lease_host = "held", "laptop"
    agent.ctx.state.save_session(record)
    session = agent.adapter.session_for_transcript(copy)
    assert session is not None
    after_sync(agent.ctx, agent.adapter, session)
    assert agent.adapter.find_transcripts(sid) == [copy]
    notices = agent.ctx.state.take_notices("claude-code", sid)
    assert notices and "laptop holds the lease" in notices[0]


def test_unsynced_bytes_keep_the_file_in_place(laptop: Host, agent: Host) -> None:
    t = start_session(laptop, LAPTOP_WS)
    sid = t.stem
    laptop.ctx.api.grant("claude-code", sid, "agent-1")
    restore(agent.ctx, agent.adapter, sid, AGENT_WS, steal=True)
    # A partial last line: it cannot go up yet, so the server lacks it.
    with open(t, "a") as f:
        f.write('{"type": "user", "message": {"content": "half')
    refresh_states(laptop.ctx, laptop.adapter)
    assert laptop.adapter.find_transcripts(sid) == [t]
    notices = laptop.ctx.state.take_notices("claude-code", sid)
    assert notices and "agent-1 holds the lease" in notices[0]
