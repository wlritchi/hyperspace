# vim: filetype=python
"""Regression tests for the repository-wide review, with isolated fake hosts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading

import httpx
import pytest

from _hs import FakeClaude, context, make_server, prompt
from hyperspace_client.adapters.claude_code import sanitize_path
from hyperspace_client.api import ApiError
from hyperspace_client.sync import Syncer
from hyperspace_client.teleport import restore
from hyperspace_protocol import (
    HEADER_WORKSPACE_UTF8,
    decode_workspace,
    encode_workspace,
)
from store import SessionKey, Store, Writer

H = "claude-code"
WS = "/work/project"


@pytest.fixture
def hosts(tmp_path):
    server = make_server(tmp_path / "server")
    laptop = FakeClaude(tmp_path / "laptop")
    agent = FakeClaude(tmp_path / "agent")
    a = context(server.api("laptop"), tmp_path / "a-state", laptop, "laptop")
    b = context(server.api("agent"), tmp_path / "b-state", agent, "agent-1")
    t = laptop.transcript(WS)
    laptop.write(t, prompt("backed up", cwd=WS))
    session = laptop.adapter.session_for_transcript(t)
    Syncer(a, session, WS).run()
    a.api.grant(H, t.stem, "agent-1")
    yield server, laptop, agent, a, b, t
    a.state.close()
    b.state.close()
    server.close()


def test_unicode_workspace_round_trip(hosts):
    _, laptop, _, a, _, _ = hosts
    workspace = "/work/projet-é/项目/😀"
    t = laptop.transcript(workspace)
    laptop.write(t, prompt("unicode", cwd=workspace))
    Syncer(a, laptop.adapter.session_for_transcript(t), workspace).run()
    assert a.api.manifest(H, t.stem)["placements"][-1]["workspace"] == workspace
    assert decode_workspace(encode_workspace(workspace)) == workspace
    a.api._capabilities = set()
    with pytest.raises(ApiError, match="upgrade_required"):
        a.api.add_metadata(H, t.stem, workspace, {})


def test_invalid_workspace_encoding_is_rejected(hosts):
    _, _, _, a, _, t = hosts
    response = a.api._client.post(
        f"/sessions/{H}/{t.stem}/meta", json={}, headers={HEADER_WORKSPACE_UTF8: "%%%"}
    )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "metadata", [{"title": 123}, {"cwd": {}}, {"record_count": True}, {"links": []}]
)
def test_metadata_validation(hosts, metadata):
    _, _, _, a, _, t = hosts
    with pytest.raises(ApiError) as error:
        a.api.add_metadata(H, t.stem, WS, metadata)
    assert error.value.status == 400
    assert a.api.sessions(title="backed") == []


def test_old_invalid_metadata_is_safe_after_upgrade(hosts):
    server, _, _, a, _, t = hosts
    key = SessionKey("default", H, t.stem)
    with server.store._tx():
        sid = server.store._require_session(key)["id"]
        server.store._db.execute(
            "INSERT INTO metadata_events(session, host, received, body) VALUES (?, ?, ?, ?)",
            (
                sid,
                "laptop",
                "now",
                json.dumps({"title": 123, "cwd": {}, "custom": "keep"}),
            ),
        )
    assert a.api.sessions(title="anything") == []
    g = a.api.manifest(H, t.stem)["files"][0]["generations"][0]
    a.api.append(H, t.stem, "/new", g["id"], g["length"], b"new\n", 0)
    assert a.api.manifest(H, t.stem)["metadata"] == {"custom": "keep"}


def test_generation_authorization_is_rechecked_after_body(hosts):
    server, _, _, _, _, _ = hosts

    async def exercise():
        ready, finish = asyncio.Event(), asyncio.Event()
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as c:
            path = f"/v1/sessions/{H}/new-raced-session"

            def headers(who):
                return {
                    "Authorization": f"Bearer {server.tokens[who]}",
                    "Hyperspace-Workspace": WS,
                }

            async def body():
                ready.set()
                await finish.wait()
                yield b'{"path":"runner.jsonl","cause":"root"}'

            pending = asyncio.create_task(
                c.post(path + "/generations", headers=headers("agent"), content=body())
            )
            await ready.wait()
            response = await c.post(
                path + "/generations",
                headers=headers("laptop"),
                json={"path": "transcript.jsonl", "cause": "root"},
            )
            assert response.status_code == 200
            finish.set()
            assert (await pending).status_code == 404
            assert (await c.get(path, headers=headers("agent"))).status_code == 404

    asyncio.run(exercise())


def test_purge_rechecks_objects_after_commit(tmp_path, monkeypatch):
    one, two = Store(tmp_path / "store"), Store(tmp_path / "store")
    key = SessionKey("a", H, "one")
    other = SessionKey("a", H, "two")
    writer = Writer("a", "laptop", WS)
    data = b"shared"
    digest = hashlib.sha256(data).hexdigest()
    one.put_blob(key, "tool-results/x", writer, data, digest)
    original = one._remove_files

    def interleave(generations, digests):
        two.put_blob(other, "tool-results/x", writer, data, digest)
        original(generations, digests)

    monkeypatch.setattr(one, "_remove_files", interleave)
    one.purge_blob_version(key, "tool-results/x", 1, "test")
    assert two.read_blob(other, "tool-results/x", None) == data
    one.close()
    two.close()


def test_failed_transaction_entry_releases_store_lock(tmp_path):
    one, two = Store(tmp_path / "store"), Store(tmp_path / "store")
    one._db.execute("PRAGMA busy_timeout = 1")
    with two._tx(), pytest.raises(sqlite3.OperationalError), one._tx():
        pass
    acquired = []

    def acquire():
        ok = one._lock.acquire(timeout=1)
        acquired.append(ok)
        if ok:
            one._lock.release()

    thread = threading.Thread(target=acquire)
    thread.start()
    thread.join(2)
    assert acquired == [True]
    one.close()
    two.close()


def test_macos_running_process_is_not_treated_as_stopped(hosts, monkeypatch):
    import hyperspace_client.adapters.claude_code as adapter_module

    _, laptop, _, _, _, t = hosts
    sessions = laptop.root / "sessions"
    sessions.mkdir()
    (sessions / "123.json").write_text(
        json.dumps({"sessionId": t.stem, "pid": 123, "procStart": "platform-specific"})
    )
    monkeypatch.setattr(adapter_module.sys, "platform", "darwin")
    calls = []
    monkeypatch.setattr(
        adapter_module.os, "kill", lambda pid, sig: calls.append((pid, sig))
    )
    assert laptop.adapter.is_running(t.stem)
    assert calls == [(123, 0)]


def test_long_workspace_matches_harness_encoding():
    # Expected values from Claude Code 2.1.288's JavaScript implementation,
    # not from the Python encoder used by FakeClaude.
    assert sanitize_path("/a" * 110) == ("-a" * 100) + "-816x1g"
    assert sanitize_path("/😀") == "---"


def test_restore_into_long_workspace(hosts):
    _, _, agent, _, b, t = hosts
    workspace = "/a" * 160
    result = restore(b, agent.adapter, t.stem, workspace, steal=True)
    assert result.transcript.read_bytes() == t.read_bytes()
    assert len(result.transcript.parent.name) <= 208


def test_fork_creator_with_own_read_scope_can_upload(tmp_path):
    server = make_server(
        tmp_path / "server", {"own": ("runner", {"read", "append"}, "own")}
    )
    try:
        api = server.api("own")
        g = api.create_generation(H, "source", WS, "transcript.jsonl", "root")
        api.append(H, "source", WS, g["id"], 0, b"record\n", 1)
        event = api.fork(H, "source", g["id"], 7)
        api.create_generation(H, event["session_id"], WS, "transcript.jsonl", "root")
    finally:
        server.close()
