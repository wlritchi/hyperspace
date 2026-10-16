# vim: filetype=python
"""The backfill against an in-process server, with a runner in place of
the `claude` binary."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, text_reply
from hyperspace_client.backfill import (
    RunResult,
    attempt_key,
    command,
    run,
    select,
)
from hyperspace_client.cli import SESSION_ENV, _request, set_status
from hyperspace_client.sync import Context, Syncer

HARNESS = "claude-code"
MONTH = 30 * 86_400


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(
        tmp_path / "server",
        hosts={
            "laptop": ("laptop", {"append", "read", "grant"}, "all"),
            "desk": ("desk", {"append", "read", "grant"}, "all"),
        },
    )
    yield s
    s.close()


@pytest.fixture
def fake(tmp_path: Path) -> FakeClaude:
    return FakeClaude(tmp_path / "claude")


@pytest.fixture
def ws(tmp_path: Path) -> str:
    path = tmp_path / "ws"
    path.mkdir()
    return str(path)


@pytest.fixture
def ctx(server: Server, fake: FakeClaude, tmp_path: Path) -> Iterator[Context]:
    c = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    yield c
    c.state.close()


def new_session(fake: FakeClaude, ws: str) -> tuple[str, Path]:
    path = fake.transcript(ws)
    fake.write(path, prompt("plan the work", cwd=ws), text_reply("Here is a plan."))
    return path.stem, path


def sync(ctx: Context, path: Path, ws: str) -> None:
    session = ctx.adapter.session_for_transcript(path)
    assert session is not None
    Syncer(ctx, session, ws).run()


def synced_session(ctx: Context, fake: FakeClaude, ws: str) -> str:
    sid, path = new_session(fake, ws)
    sync(ctx, path, ws)
    return sid


def agent_marks_done(ctx: Context, sid: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """What `hyperspace done --note ...` does inside the resumed session."""
    monkeypatch.setenv(SESSION_ENV, sid)
    request = _request(
        "done", sid, reason=None, wake=None, note="nothing left", by_user=False
    )
    set_status(ctx, sid, request)
    monkeypatch.delenv(SESSION_ENV)


def test_select_skips_status_live_lease_and_old(
    server: Server, ctx: Context, fake: FakeClaude, ws: str, tmp_path: Path
) -> None:
    eligible = synced_session(ctx, fake, ws)
    with_status = synced_session(ctx, fake, ws)
    ctx.api.add_status(
        HARNESS,
        with_status,
        {"status": "done", "source_kind": "user", "explicit": True, "base_seq": 0},
    )
    live = synced_session(ctx, fake, ws)
    (fake.root / "sessions").mkdir()
    (fake.root / "sessions" / "1.json").write_text(
        json.dumps({"sessionId": live, "pid": os.getpid()})
    )
    desk = context(server.api("desk"), tmp_path / "desk-state", fake, "desk")
    elsewhere, path = new_session(fake, ws)
    sync(desk, path, ws)
    desk.state.close()
    new_session(fake, ws)  # Not on the server.
    old, path = new_session(fake, ws)
    sync(ctx, path, ws)
    stale = time.time() - 2 * MONTH
    os.utime(path, (stale, stale))

    selection = select(ctx, fake.adapter, MONTH, time.time())
    assert [c.session.session_id for c in selection.candidates] == [eligible]
    assert selection.candidates[0].workspace == ws
    assert selection.skipped == {
        "has status": 1,
        "live": 1,
        "lease not here": 1,
        "not on server": 1,
    }
    assert old not in {c.session.session_id for c in selection.candidates}
    assert elsewhere != eligible


def test_backfill_with_fake_claude(
    ctx: Context, fake: FakeClaude, ws: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid = synced_session(ctx, fake, ws)
    calls: list[tuple[list[str], str]] = []

    def runner(argv: list[str], cwd: str, timeout: float) -> RunResult:
        calls.append((argv, cwd))
        agent_marks_done(ctx, sid, monkeypatch)
        return RunResult(0, "Set done: the plan was delivered.\n")

    selection = select(ctx, fake.adapter, MONTH, time.time())
    lines: list[str] = []
    results = run(
        ctx,
        fake.adapter,
        selection.candidates,
        claude="claude",
        rate=3600,
        timeout=60,
        report=lines.append,
        runner=runner,
    )
    assert calls == [(command("claude", sid), ws)]
    assert results == {"set": 1}
    assert lines == [f"{sid}: set: Set done: the plan was delivered."]
    event = ctx.api.state(HARNESS, sid).get("status")
    assert event is not None
    assert (event["status"], event.get("note")) == ("done", "nothing left")
    again = select(ctx, fake.adapter, MONTH, time.time(), retry=True)
    assert again.skipped == {"has status": 1}


def test_backfill_timeout_is_reported(ctx: Context, fake: FakeClaude, ws: str) -> None:
    sid = synced_session(ctx, fake, ws)
    selection = select(ctx, fake.adapter, MONTH, time.time())
    results = run(
        ctx,
        fake.adapter,
        selection.candidates,
        claude="claude",
        rate=3600,
        timeout=1,
        report=lambda _: None,
        runner=lambda argv, cwd, timeout: RunResult(None, ""),
    )
    assert results == {"timeout": 1}
    record = json.loads(ctx.state.get(attempt_key(HARNESS, sid)) or "{}")
    assert record["result"] == "timeout"
    assert select(ctx, fake.adapter, MONTH, time.time()).skipped == {
        "messaged before": 1
    }
    retry = select(ctx, fake.adapter, MONTH, time.time(), retry=True)
    assert [c.session.session_id for c in retry.candidates] == [sid]


def test_hook_status_is_not_the_agents_decision(
    ctx: Context, fake: FakeClaude, ws: str
) -> None:
    sid = synced_session(ctx, fake, ws)

    def runner(argv: list[str], cwd: str, timeout: float) -> RunResult:
        # The Stop hook writes `ready` when the agent sets nothing.
        ctx.api.add_status(
            HARNESS,
            sid,
            {
                "status": "ready",
                "reason": "idle",
                "source_kind": "hook",
                "explicit": False,
                "base_seq": 0,
            },
        )
        return RunResult(0, "I am not sure.\n")

    selection = select(ctx, fake.adapter, MONTH, time.time())
    results = run(
        ctx,
        fake.adapter,
        selection.candidates,
        claude="claude",
        rate=3600,
        timeout=60,
        report=lambda _: None,
        runner=runner,
    )
    assert results == {"no status": 1}


def test_interrupted_run_does_not_message_again(
    ctx: Context, fake: FakeClaude, ws: str
) -> None:
    first = synced_session(ctx, fake, ws)
    time.sleep(0.01)
    second = synced_session(ctx, fake, ws)

    def runner(argv: list[str], cwd: str, timeout: float) -> RunResult:
        raise KeyboardInterrupt

    selection = select(ctx, fake.adapter, MONTH, time.time())
    # Newest first.
    assert [c.session.session_id for c in selection.candidates] == [second, first]
    with pytest.raises(KeyboardInterrupt):
        run(
            ctx,
            fake.adapter,
            selection.candidates,
            claude="claude",
            rate=3600,
            timeout=60,
            report=lambda _: None,
            runner=runner,
        )
    after = select(ctx, fake.adapter, MONTH, time.time())
    assert [c.session.session_id for c in after.candidates] == [first]
    assert after.skipped == {"messaged before": 1}


def test_limit_counts_candidates(ctx: Context, fake: FakeClaude, ws: str) -> None:
    for _ in range(3):
        synced_session(ctx, fake, ws)
    selection = select(ctx, fake.adapter, MONTH, time.time(), limit=2)
    assert len(selection.candidates) == 2
