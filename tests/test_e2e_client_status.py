# vim: filetype=python
"""Session status from the client's hooks and CLI, against an in-process
server."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from _hs import (
    FakeClaude,
    Server,
    context,
    make_server,
    prompt,
    text_reply,
    tool_call,
)
from hyperspace_client import backfill
from hyperspace_client import cli as cli_mod
from hyperspace_client.api import Api
from hyperspace_client.cli import (
    SESSION_ENV,
    Exit,
    _print_status_table,
    _request,
    _target,
    _targets,
    set_status,
    status_label,
    tail,
)
from hyperspace_client.config import Config
from hyperspace_client.hooks import handle, notices
from hyperspace_client.notices import refresh_states
from hyperspace_client.state import State
from hyperspace_client.status import on_hook, send_wanted
from hyperspace_client.sync import Context, Syncer
from hyperspace_protocol import (
    CAP_RESTORE_PRECONDITION,
    CAP_WORKSPACE_UTF8,
    StatusEvent,
)

WS = "/home/wlritchi/hyperspace"
HARNESS = "claude-code"


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
def ctx(server: Server, fake: FakeClaude, tmp_path: Path) -> Iterator[Context]:
    c = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    yield c
    c.state.close()


def sync(ctx: Context, transcript: Path) -> None:
    session = ctx.adapter.session_for_transcript(transcript)
    assert session is not None
    Syncer(ctx, session, WS).run()


def hook(ctx: Context, fake: FakeClaude, sid: str, event: str, **payload: Any) -> None:
    on_hook(ctx.state, fake.adapter, HARNESS, sid, event, payload)


def server_status(api: Api, sid: str) -> StatusEvent:
    event = api.state(HARNESS, sid).get("status")
    assert event is not None
    return event


def started_session(ctx: Context, fake: FakeClaude) -> tuple[str, Path]:
    """A session with one turn that ended: the server has `ready`."""
    path = fake.transcript(WS)
    sid = path.stem
    fake.write(path, prompt("hello"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    fake.write(path, tool_call(), text_reply("All set. Shall I push?"))
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    return sid, path


def test_hook_sequence_sets_status(ctx: Context, fake: FakeClaude) -> None:
    path = fake.transcript(WS)
    sid = path.stem
    fake.write(path, prompt("hello"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event["source"]["kind"], event["explicit"]) == (
        "working",
        "hook",
        False,
    )
    hook(ctx, fake, sid, "PreToolUse", tool_name="AskUserQuestion")
    fake.write(path, tool_call("AskUserQuestion"))
    sync(ctx, path)
    assert server_status(ctx.api, sid).get("reason") == "asked"
    hook(ctx, fake, sid, "PostToolUse", tool_name="AskUserQuestion")
    fake.write(path, text_reply("Done. Shall I push?"))
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event.get("reason")) == ("ready", "idle")
    # The list entry carries the excerpt; the state body does not.
    assert "excerpt" not in event
    (summary,) = ctx.api.sessions(open="1")
    assert summary.get("excerpt") == "Done. Shall I push?"
    assert ctx.state.wanted_status(HARNESS, sid) is None
    assert ctx.state.session(HARNESS, sid).status == event


def test_remote_done_is_not_overwritten(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    desk = server.api("desk")
    base = server_status(desk, sid)["seq"]
    desk.add_status(
        HARNESS,
        sid,
        {
            "status": "done",
            "note": "merged",
            "source_kind": "user",
            "explicit": True,
            "base_seq": base,
        },
    )
    # The user prompts here before the client knows of the mark.
    fake.write(path, prompt("one more thing"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event["source"]["host"]) == ("done", "desk")
    assert ctx.state.wanted_status(HARNESS, sid) is None
    assert ctx.state.notice_flag(sid).exists()
    (notice,) = ctx.state.take_notices(HARNESS, sid)
    assert "desk marked this session done (by user)." in notice
    assert 'not an instruction): "merged"' in notice
    record = ctx.state.session(HARNESS, sid)
    assert record.done_seen is not None
    # Tool calls and the end of the turn do not change a done session.
    hook(ctx, fake, sid, "PreToolUse", tool_name="Bash")
    hook(ctx, fake, sid, "Stop")
    assert ctx.state.wanted_status(HARNESS, sid) is None


def test_done_then_prompt_reopens(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    request = {**_request("done", sid, reason=None, wake=None, note="x", by_user=True)}
    assert set_status(ctx, sid, request) is not None
    assert server_status(ctx.api, sid)["status"] == "done"
    assert ctx.state.notice_flag(sid).exists()
    # The user's next prompt reopens the session.
    fake.write(path, prompt("actually, also do Y"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event["explicit"], event["source"]["kind"]) == (
        "working",
        True,
        "user",
    )
    assert event.get("note") == "reopened by a prompt"
    # The reopen drops the done notice that the agent did not get.
    assert ctx.state.take_notices(HARNESS, sid) == []


def test_prompt_that_reopens_drops_the_done_notice(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    desk = server.api("desk")
    desk.add_status(
        HARNESS,
        sid,
        {
            "status": "done",
            "source_kind": "user",
            "explicit": True,
            "base_seq": server_status(desk, sid)["seq"],
        },
    )
    # The user resumes the session here. The sweep at the start learns of
    # the mark and queues the notice.
    refresh_states(ctx, fake.adapter)
    assert ctx.state.notice_flag(sid).exists()
    # The first prompt reopens the session, so the notice is not true now.
    fake.write(path, prompt("continue"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    assert not ctx.state.notice_flag(sid).exists()
    assert ctx.state.take_notices(HARNESS, sid) == []
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event["explicit"]) == ("working", True)


def run_hook(ctx: Context, path: Path, event: str, **payload: Any) -> str:
    """Run the hook command for `event` as the plugin does, with no server
    URL (so no resume check) and no detached processes."""
    cfg = Config(host=ctx.host, state_dir=ctx.state.directory)
    body = {"hook_event_name": event, "transcript_path": str(path), "cwd": WS}
    started: list[list[str]] = []
    return handle({**body, **payload}, cfg, start=started.append)


def post_tool_use(ctx: Context, path: Path) -> str:
    """The plugin's synchronous PostToolUse notice hook."""
    cfg = Config(host=ctx.host, state_dir=ctx.state.directory)
    return notices(
        {"hook_event_name": "PostToolUse", "transcript_path": str(path)}, cfg
    )


def mark_done_from_desk(server: Server, sid: str, note: str) -> None:
    desk = server.api("desk")
    desk.add_status(
        HARNESS,
        sid,
        {
            "status": "done",
            "source_kind": "user",
            "explicit": True,
            "note": note,
            "base_seq": server_status(desk, sid)["seq"],
        },
    )


def test_resume_does_not_tell_the_agent_to_stop(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    mark_done_from_desk(server, sid, "merged")
    # The sweep sees the mark while the session is stopped.
    refresh_states(ctx, fake.adapter)
    out = run_hook(ctx, path, "SessionStart", source="resume")
    assert "desk marked this session done" in out and "merged" in out
    assert "Finish the current step" not in out
    # The user's prompt reopens the session, so no done notice follows.
    fake.write(path, prompt("continue"))
    assert run_hook(ctx, path, "UserPromptSubmit") == ""
    assert post_tool_use(ctx, path) == ""
    assert not ctx.state.notice_flag(sid).exists()


def test_compaction_keeps_the_done_notice_for_the_turn(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    fake.write(path, prompt("next"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    mark_done_from_desk(server, sid, "merged")
    refresh_states(ctx, fake.adapter)
    # A compaction in the turn starts the session again. The turn continues,
    # so its next tool call gets the done notice.
    out = run_hook(ctx, path, "SessionStart", source="compact")
    assert "Finish the current step" not in out
    assert "Finish the current step and stop" in post_tool_use(ctx, path)


def test_live_session_gets_an_outside_done_at_its_next_tool_call(
    server: Server, ctx: Context, fake: FakeClaude
) -> None:
    sid, path = started_session(ctx, fake)
    fake.write(path, prompt("next"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    sync(ctx, path)
    assert post_tool_use(ctx, path) == ""
    mark_done_from_desk(server, sid, "merged")
    # The append response of the next checkpoint tells the client of the mark.
    fake.write(path, tool_call())
    hook(ctx, fake, sid, "PreToolUse", tool_name="Bash")
    sync(ctx, path)
    out = json.loads(post_tool_use(ctx, path))
    context = out["hookSpecificOutput"]["additionalContext"]
    assert "desk marked this session done" in context
    assert "Finish the current step and stop" in context
    assert post_tool_use(ctx, path) == ""


def test_agent_gets_no_notice_for_its_own_done(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, path = started_session(ctx, fake)
    monkeypatch.setenv(SESSION_ENV, sid)
    request = _request("done", sid, reason=None, wake=None, note="all", by_user=False)
    event = set_status(ctx, sid, request)
    assert event is not None and event["status"] == "done"
    sync(ctx, path)
    refresh_states(ctx, fake.adapter)
    assert post_tool_use(ctx, path) == ""
    assert not ctx.state.notice_flag(sid).exists()


def test_status_cli_defaults_to_env_session(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, path = started_session(ctx, fake)
    monkeypatch.delenv(SESSION_ENV, raising=False)
    with pytest.raises(Exit):
        _target(None)
    monkeypatch.setenv(SESSION_ENV, sid)
    assert _target(None) == sid
    request = _request(
        "waiting", sid, reason=None, wake="message", note="CI\non PR 41", by_user=False
    )
    assert request["source_kind"] == "agent" and "source_session" not in request
    assert request["note"] == "CI on PR 41"
    # A queued automatic write is replaced by the explicit one.
    hook(ctx, fake, sid, "PreToolUse", tool_name="AskUserQuestion")
    event = set_status(ctx, sid, request)
    assert event is not None and event["wake"] == "message"
    assert ctx.state.wanted_status(HARNESS, sid) is None
    # The declaration holds over the Stop hook of this turn.
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    assert server_status(ctx.api, sid)["status"] == "waiting"
    assert ctx.state.session(HARNESS, sid).declared is None


@pytest.mark.parametrize("sync_each", [True, False])
def test_declaration_holds_over_a_prompt_in_the_turn(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch, sync_each: bool
) -> None:
    sid, path = started_session(ctx, fake)
    monkeypatch.setenv(SESSION_ENV, sid)
    request = _request(
        "waiting", sid, reason=None, wake="local", note=None, by_user=False
    )
    declared = set_status(ctx, sid, request)
    assert declared is not None
    # After the declaration, a tool call needs permission. While the prompt
    # shows, the session needs the user.
    hook(ctx, fake, sid, "PreToolUse", tool_name="Bash")
    hook(ctx, fake, sid, "Notification", notification_type="permission_prompt")
    if sync_each:
        sync(ctx, path)
        assert server_status(ctx.api, sid)["reason"] == "asked"
    hook(ctx, fake, sid, "PostToolUse", tool_name="Bash")
    if sync_each:
        sync(ctx, path)
        assert server_status(ctx.api, sid)["status"] == "working"
    # The Stop hook writes the declaration again.
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    event = server_status(ctx.api, sid)
    assert (event["status"], event.get("wake"), event["explicit"]) == (
        "waiting",
        "local",
        True,
    )
    assert event["source"]["kind"] == "agent"
    # The declaration applies to one turn.
    hook(ctx, fake, sid, "UserPromptSubmit")
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    assert server_status(ctx.api, sid)["reason"] == "idle"


def test_agent_does_not_write_over_an_outside_done(
    server: Server, ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, _ = started_session(ctx, fake)
    desk = server.api("desk")
    desk.add_status(
        HARNESS,
        sid,
        {
            "status": "done",
            "source_kind": "user",
            "explicit": True,
            "base_seq": server_status(desk, sid)["seq"],
        },
    )
    monkeypatch.setenv(SESSION_ENV, sid)
    agent = _request("ready", sid, reason=None, wake=None, note=None, by_user=False)
    assert set_status(ctx, sid, agent) is None
    assert server_status(ctx.api, sid)["source"]["host"] == "desk"
    # The user, on the other hand, can reopen it with one retry.
    user = _request("ready", sid, reason=None, wake=None, note="not yet", by_user=True)
    event = set_status(ctx, sid, user)
    assert event is not None and event["status"] == "ready"


def test_write_for_another_session(
    server: Server, ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, _ = started_session(ctx, fake)
    other = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setenv(SESSION_ENV, other)
    request = _request("done", sid, reason=None, wake=None, note="dup", by_user=False)
    assert request["source_session"] == other
    desk = context(
        server.api("desk"), ctx.state.directory.parent / "desk", fake, "desk"
    )
    try:
        event = set_status(desk, sid, request)
    finally:
        desk.state.close()
    assert event is not None
    assert event["source"] == {"kind": "agent", "host": "desk", "session_id": other}


@pytest.mark.parametrize("by_user", [False, True])
def test_write_for_another_session_on_this_host(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch, by_user: bool
) -> None:
    sid, _ = started_session(ctx, fake)
    # A different session on this host, or the user in a different terminal.
    if by_user:
        monkeypatch.delenv(SESSION_ENV, raising=False)
    else:
        monkeypatch.setenv(SESSION_ENV, "22222222-2222-2222-2222-222222222222")
    request = _request("done", sid, reason=None, wake=None, note="dup", by_user=by_user)
    event = set_status(ctx, sid, request)
    assert event is not None and event["source"]["host"] == "laptop"
    assert ctx.state.session(HARNESS, sid).status == event
    (notice,) = ctx.state.take_notices(HARNESS, sid)
    assert "laptop marked this session done" in notice


def test_backfill_turn_sets_only_its_own_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    own = "11111111-1111-1111-1111-111111111111"
    other = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setenv(SESSION_ENV, own)
    assert _targets([other], by_user=True) == [other]
    monkeypatch.setenv(backfill.SELF_ONLY_ENV, "1")
    assert _targets([], by_user=False) == [own]
    assert _targets([own], by_user=False) == [own]
    for sessions, by_user in (
        ([other], False),
        ([own, other], False),
        ([own, own], False),
        ([], True),
    ):
        with pytest.raises(Exit):
            _targets(sessions, by_user)


def test_write_during_a_long_sync_is_queued(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, path = started_session(ctx, fake)
    monkeypatch.setenv(SESSION_ENV, sid)
    monkeypatch.setattr(cli_mod, "STATUS_LOCK_WAIT", 0.2)
    before = server_status(ctx.api, sid)
    request = _request(
        "waiting", sid, reason=None, wake="message", note="CI", by_user=False
    )
    # A sync holds the session's lock for longer than the CLI waits.
    with ctx.state.try_lock(HARNESS, sid) as locked:
        assert locked
        assert set_status(ctx, sid, request) is None
    assert server_status(ctx.api, sid) == before
    queued = ctx.state.wanted_status(HARNESS, sid)
    assert queued is not None and queued[0]["wake"] == "message"
    assert ctx.state.session(HARNESS, sid).declared == request
    # The Stop hook keeps the declaration, and the next sync sends it.
    hook(ctx, fake, sid, "Stop")
    sync(ctx, path)
    assert server_status(ctx.api, sid)["wake"] == "message"


def test_write_before_the_first_sync_is_queued(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = fake.transcript(WS)
    sid = path.stem
    monkeypatch.setenv(SESSION_ENV, sid)
    request = _request("done", sid, reason=None, wake=None, note="quick", by_user=False)
    assert set_status(ctx, sid, request) is None
    assert ctx.state.wanted_status(HARNESS, sid) is not None
    fake.write(path, prompt("hi"), text_reply("hello"))
    sync(ctx, path)
    assert server_status(ctx.api, sid)["status"] == "done"


def test_old_server_without_capability(ctx: Context, fake: FakeClaude) -> None:
    path = fake.transcript(WS)
    sid = path.stem
    fake.write(path, prompt("hello"))
    hook(ctx, fake, sid, "UserPromptSubmit")
    ctx.api._capabilities = {CAP_WORKSPACE_UTF8, CAP_RESTORE_PRECONDITION}  # pyright: ignore[reportPrivateUsage]
    sync(ctx, path)
    assert "status" not in ctx.api.state(HARNESS, sid)
    # The write stays queued for a server that has the capability.
    assert ctx.state.wanted_status(HARNESS, sid) is not None
    ctx.api._capabilities = None  # pyright: ignore[reportPrivateUsage]
    session = ctx.adapter.session_for_transcript(path)
    assert session is not None
    send_wanted(ctx, session)
    assert server_status(ctx.api, sid)["status"] == "working"


def test_ls_open_columns(
    server: Server, ctx: Context, fake: FakeClaude, capsys: pytest.CaptureFixture[str]
) -> None:
    sid, _ = started_session(ctx, fake)
    path2 = fake.transcript(WS)
    fake.write(path2, prompt("build it"))
    hook(ctx, fake, path2.stem, "UserPromptSubmit")
    sync(ctx, path2)
    # Both run on laptop. Then the second session's process stops during
    # its turn.
    both = [
        {"harness": HARNESS, "session_id": sid},
        {"harness": HARNESS, "session_id": path2.stem},
    ]
    ctx.api.heartbeat({"interval": 60, "sessions": both})
    ctx.api.heartbeat({"interval": 60, "sessions": both[:1]})
    sessions = ctx.api.sessions(open="1")
    _print_status_table(sessions, 30 * 60)
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["SESSION", "STATUS", "WHERE", "AGE", "TITLE", "NOTE"]
    # `ready` comes before `interrupted`.
    assert lines[1].startswith(sid) and "ready:idle" in lines[1]
    assert "laptop:" + WS in lines[1] and "All set. Shall I push?" in lines[1]
    assert lines[2].startswith(path2.stem) and "interrupted" in lines[2]


def test_status_label() -> None:
    now = time.time()
    base: dict[str, Any] = {
        "state": "normal",
        "lease": {"state": "held", "host": "laptop", "since": "x"},
    }
    working: dict[str, Any] = {
        "seq": 1,
        "status": "working",
        "source": {"kind": "hook", "host": "laptop"},
        "explicit": False,
        "received": "2026-10-16T00:00:00Z",
    }
    live = {"state": "live", "host": "laptop", "since": "2026-10-16T00:00:00Z"}
    old = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 3600))
    assert status_label({**base}, now, 1800) == "-"  # type: ignore[arg-type]
    state: Any = {**base, "status": working, "liveness": live, "last_append": old}
    assert status_label(state, now, 1800) == "stalled"
    assert status_label(state, now, 7200) == "working"
    state["liveness"] = {**live, "state": "unknown", "since": "2026-10-16T09:41:00Z"}
    assert status_label(state, now, 1800) == "working, unknown since 09:41"
    done = {**working, "status": "done", "explicit": True}
    state = {
        **base,
        "status": done,
        "liveness": {**live, "state": "stopped", "close_blocked": "unpushed work"},
    }
    assert status_label(state, now, 1800) == "done, not closed: unpushed work"


def test_tail_reads_active_branch(ctx: Context, fake: FakeClaude) -> None:
    path = fake.transcript(WS)
    sid = path.stem
    fake.write(
        path,
        prompt("first"),
        text_reply("answer one"),
        prompt("second"),
        tool_call("Read"),
        text_reply("answer two"),
    )
    sync(ctx, path)
    out = tail(ctx.api, sid, 1)
    assert "second" in out and "[tool] Read" in out and "answer two" in out
    assert "answer one" not in out
    # A rewrite of the local file makes a new active branch.
    path.write_text("")
    fake.write(path, prompt("rewritten"), text_reply("new answer"))
    sync(ctx, path)
    assert "new answer" in tail(ctx.api, sid, 5)
    assert "answer two" not in tail(ctx.api, sid, 5)


def test_state_dir_is_shared_with_the_notice_hook(tmp_path: Path) -> None:
    # The plugin's notice.sh looks for flags in $XDG_STATE_HOME/hyperspace.
    state = State(tmp_path / "hyperspace")
    assert state.notice_flag("x") == tmp_path / "hyperspace" / "notify" / "x"
