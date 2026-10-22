# vim: filetype=python
"""The client's session lists: pages with the cursor, servers without the
cursor, list timeouts, and `ls --status`."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, text_reply
from hyperspace_client import api as api_mod
from hyperspace_client import cli as cli_mod
from hyperspace_client.api import LIST_TIMEOUT, Api
from hyperspace_client.notices import refresh_states
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer
from hyperspace_client.teleport import restore_held
from hyperspace_protocol import (
    CAP_LIST_CURSOR,
    CAP_RESTORE_PRECONDITION,
    CAP_SESSION_STATUS,
    CAP_WORKSPACE_UTF8,
)

HARNESS = "claude-code"
WS = "/home/wlritchi/hyperspace"
AGENT_WS = "/home/agent/workspace"
# The capabilities of a server from before `list-cursor`.
OLD_CAPABILITIES = {CAP_WORKSPACE_UTF8, CAP_RESTORE_PRECONDITION, CAP_SESSION_STATUS}


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


def list_requests(server: Server) -> list[httpx.Request]:
    """Record the list requests of the last API client of `server`."""
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> None:
        if request.url.path.endswith("/sessions"):
            seen.append(request)

    server.clients[-1].event_hooks["request"].append(record)
    return seen


def add_sessions(api: Api, count: int) -> list[str]:
    """Sessions with metadata only, newest last."""
    sids = [f"s{n}" for n in range(count)]
    for sid in sids:
        api.add_metadata(HARNESS, sid, WS, {"title": sid})
    return sids


def test_all_sessions_reads_every_page(server: Server) -> None:
    api = server.api("laptop")
    sids = add_sessions(api, 5)
    requests = list_requests(server)
    found = [s["session_id"] for s in api.all_sessions(2, harness=HARNESS)]
    assert found == sids[::-1]
    assert len(requests) == 3
    params = [r.url.params for r in requests]
    assert [p.get("limit") for p in params] == ["2", "2", "2"]
    assert params[0].get("cursor") is None
    assert all(p.get("cursor") for p in params[1:])
    assert all(p.get("harness") == HARNESS for p in params)
    # The default page is the server's maximum.
    requests.clear()
    assert len(list(api.all_sessions(harness=HARNESS))) == 5
    assert [r.url.params.get("limit") for r in requests] == ["1000"]


def test_all_sessions_without_the_capability_sends_one_request(
    server: Server,
) -> None:
    api = server.api("laptop")
    sids = add_sessions(api, 5)
    api._capabilities = OLD_CAPABILITIES  # pyright: ignore[reportPrivateUsage]
    requests = list_requests(server)
    found = [s["session_id"] for s in api.all_sessions(2, harness=HARNESS)]
    assert found == sids[::-1]
    assert len(requests) == 1
    assert requests[0].url.params.get("limit") == "1000"
    assert "cursor" not in requests[0].url.params


def mock_api(
    handler: Callable[[httpx.Request], httpx.Response], capabilities: list[str]
) -> Api:
    """An API client for a server that `handler` simulates."""

    def route(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/whoami":
            return httpx.Response(200, json={"capabilities": capabilities})
        return handler(request)

    client = httpx.Client(
        transport=httpx.MockTransport(route), base_url="http://server/v1"
    )
    return Api("http://server", "token", client=client)


def summary(sid: str) -> dict[str, Any]:
    return {"harness": HARNESS, "session_id": sid}


def test_old_server_that_ignores_the_cursor_gets_one_request() -> None:
    calls: list[httpx.Request] = []

    def old_server(request: httpx.Request) -> httpx.Response:
        # The first page, whatever the parameters.
        calls.append(request)
        return httpx.Response(200, json={"sessions": [summary("a"), summary("b")]})

    api = mock_api(old_server, sorted(OLD_CAPABILITIES))
    assert [s["session_id"] for s in api.all_sessions(1)] == ["a", "b"]
    assert len(calls) == 1


def test_a_repeated_cursor_stops_the_pass() -> None:
    calls: list[httpx.Request] = []

    def broken(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"sessions": [summary("a")], "next": "same"})

    api = mock_api(broken, [CAP_LIST_CURSOR])
    assert len(list(api.all_sessions())) == 2
    assert len(calls) == 2
    assert calls[1].url.params.get("cursor") == "same"


def test_an_empty_page_with_a_cursor_does_not_stop_the_pass() -> None:
    # With the title filter, a page can be empty and the list not at its end.
    pages = {
        None: {"sessions": [summary("a")], "next": "1"},
        "1": {"sessions": [], "next": "2"},
        "2": {"sessions": [summary("b")]},
    }

    def paged(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    api = mock_api(paged, [CAP_LIST_CURSOR])
    assert [s["session_id"] for s in api.all_sessions(title="x")] == ["a", "b"]


def test_a_list_has_the_long_read_timeout_and_is_not_sent_again() -> None:
    calls: list[httpx.Request] = []

    def slow(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ReadTimeout("timed out", request=request)

    api = mock_api(slow, [CAP_LIST_CURSOR])
    with pytest.raises(httpx.ReadTimeout):
        list(api.all_sessions())
    assert len(calls) == 1
    timeout = calls[0].extensions["timeout"]
    assert timeout["read"] == LIST_TIMEOUT
    assert timeout["connect"] == 30.0


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HYPERSPACE_CONFIG", str(tmp_path / "none.toml"))
    monkeypatch.setenv("HYPERSPACE_URL", "http://127.0.0.1")
    monkeypatch.setenv("HYPERSPACE_TOKEN", "token")


def use_server(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
    capabilities: list[str],
) -> None:
    """Make the CLI talk to the server that `handler` simulates."""
    monkeypatch.setattr(
        cli_mod, "Api", lambda *args, **kwargs: mock_api(handler, capabilities)
    )


@pytest.mark.usefixtures("cli_env")
def test_cli_says_why_a_list_timed_out(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[httpx.Request] = []

    def slow(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ReadTimeout("timed out", request=request)

    use_server(monkeypatch, slow, [CAP_LIST_CURSOR, CAP_SESSION_STATUS])
    assert cli_mod.main(["ls", "--open"]) == 1
    assert capsys.readouterr().err == (
        "hyperspace: the server did not answer in time; "
        "it may be busy or its storage slow\n"
    )
    assert len(calls) == 1


@pytest.mark.usefixtures("cli_env")
def test_ls_status_takes_several_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[httpx.Request] = []

    def server(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"sessions": []})

    use_server(monkeypatch, server, [CAP_SESSION_STATUS])
    argv = ["ls", "--json", "--status", "working,waiting", "--status", "ready,working"]
    assert cli_mod.main(argv) == 0
    assert json.loads(capsys.readouterr().out) == []
    params = calls[0].url.params
    # Repeated parameters, which a server before comma-separated values
    # also accepts.
    assert params.get_list("status") == ["working", "waiting", "ready"]
    assert params.get("limit") == "50"
    assert "open" not in params
    calls.clear()
    assert cli_mod.main(["ls", "--json", "--open"]) == 0
    assert calls[0].url.params.get("open") == "1"
    assert "status" not in calls[0].url.params


@pytest.mark.parametrize("value", ["busy", "working,busy", ",", ""])
def test_ls_status_refuses_an_unknown_status(value: str) -> None:
    with pytest.raises(SystemExit):
        cli_mod.build_parser().parse_args(["ls", "--status", value])


@pytest.mark.usefixtures("cli_env")
def test_ls_all_pages(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    pages = {
        None: {"sessions": [summary("a")], "next": "1"},
        "1": {"sessions": [summary("b")]},
    }
    calls: list[httpx.Request] = []

    def paged(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    use_server(monkeypatch, paged, [CAP_LIST_CURSOR])
    assert cli_mod.main(["ls", "--json", "--all-pages", "--limit", "1"]) == 0
    found = [s["session_id"] for s in json.loads(capsys.readouterr().out)]
    assert found == ["a", "b"]
    assert [c.url.params.get("limit") for c in calls] == ["1", "1"]


def synced_session(ctx: Context, fake: FakeClaude, workspace: str) -> str:
    path = fake.transcript(workspace)
    fake.write(path, prompt("hello", cwd=workspace), text_reply())
    session = fake.adapter.session_for_transcript(path)
    assert session is not None
    Syncer(ctx, session, workspace).run()
    return path.stem


def test_refresh_states_reads_every_page(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeClaude(tmp_path / "claude")
    ctx = context(server.api("laptop"), tmp_path / "state", fake, "laptop")
    sids = [synced_session(ctx, fake, WS) for _ in range(3)]
    # Forget the saved lease of each session.
    for sid in sids:
        record = ctx.state.session(HARNESS, sid)
        record.lease_state = record.lease_host = record.state = None
        ctx.state.save_session(record)
    monkeypatch.setattr(api_mod, "MAX_PAGE", 1)
    requests = list_requests(server)
    assert refresh_states(ctx, fake.adapter) == 3
    assert len(requests) == 3
    for sid in sids:
        assert ctx.state.session(HARNESS, sid).lease_host == "laptop"


def test_restore_held_reads_every_page(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeClaude(tmp_path / "agent-claude")
    ctx = context(server.api("agent"), tmp_path / "agent-state", fake, "agent-1")
    sids = [synced_session(ctx, fake, AGENT_WS) for _ in range(3)]
    # The runner restarts with a new config directory and state directory.
    fresh = FakeClaude(tmp_path / "agent-claude-2")
    ctx2 = Context(
        api=ctx.api,
        state=State(tmp_path / "agent-state-2"),
        adapter=fresh.adapter,
        host="agent-1",
    )
    monkeypatch.setattr(api_mod, "MAX_PAGE", 1)
    requests = list_requests(server)
    result = restore_held(ctx2, fresh.adapter, AGENT_WS)
    assert (result.restored, result.failed) == (3, [])
    for sid in sids:
        assert fresh.adapter.find_transcripts(sid)
    # The list asks only for the sessions whose lease this host holds.
    assert {r.url.params.get("lease_host") for r in requests} == {"agent-1"}
    assert len(requests) == 3
