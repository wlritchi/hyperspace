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
from hyperspace_client.adapters import LocalFile
from hyperspace_client.adapters.claude_code import sanitize_path
from hyperspace_client.api import ApiError
from hyperspace_client.sync import Syncer
from hyperspace_client.teleport import SURRENDERED, TeleportError, fork, restore, send
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


def test_restore_preserves_unsynced_content_unless_forced(hosts):
    _, laptop, _, a, _, t = hosts
    backup = t.read_bytes()
    laptop.write(t, prompt("not uploaded"))
    local = t.read_bytes()
    before = a.api.manifest(H, t.stem)
    with pytest.raises(TeleportError, match="--force"):
        restore(a, laptop.adapter, t.stem, WS)
    assert t.read_bytes() == local
    assert a.api.manifest(H, t.stem)["teleports"] == before["teleports"]
    restore(a, laptop.adapter, t.stem, WS, force=True)
    assert t.read_bytes() == backup
    assert next(t.parent.glob(t.name + SURRENDERED + "*")).read_bytes() == local


def test_restore_refuses_live_target(hosts, monkeypatch):
    _, laptop, _, a, _, t = hosts
    monkeypatch.setattr(laptop.adapter, "is_running", lambda sid: True)
    with pytest.raises(TeleportError, match="running"):
        restore(a, laptop.adapter, t.stem, WS, force=True)


def test_restore_download_failure_does_not_take_lease(hosts, monkeypatch):
    _, _, agent, a, b, t = hosts

    def fail(*args):
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(b.api, "read_log", fail)
    with pytest.raises(httpx.ConnectError):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    assert a.api.state(H, t.stem)["lease"]["host"] == "laptop"
    assert agent.adapter.find_transcripts(t.stem) == []


def test_restore_disk_failure_rolls_back_and_keeps_lease(hosts, monkeypatch):
    from hyperspace_client import teleport

    _, _, agent, a, b, t = hosts
    original = teleport._atomic_write

    def fail(path, data):
        original(path, data)
        raise OSError("disk full")

    monkeypatch.setattr(teleport, "_atomic_write", fail)
    with pytest.raises(OSError, match="disk full"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    assert not (agent.adapter.project_dir(WS) / t.name).exists()
    assert a.api.state(H, t.stem)["lease"]["host"] == "laptop"


def test_restore_changed_snapshot_rolls_back(hosts, monkeypatch):
    _, _, agent, a, b, t = hosts
    original = b.api.teleport

    def changed(*args, **kwargs):
        a.api.add_metadata(H, t.stem, WS, {"title": "changed during download"})
        return original(*args, **kwargs)

    monkeypatch.setattr(b.api, "teleport", changed)
    with pytest.raises(ApiError, match="stale_snapshot"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    assert not (agent.adapter.project_dir(WS) / t.name).exists()
    assert a.api.state(H, t.stem)["lease"]["host"] == "laptop"
    assert (
        b.state.cursor(agent.adapter.project_dir(WS) / t.name, b.api.cache_identity)
        is None
    )


def test_restore_lost_response_retries_same_teleport(hosts, monkeypatch):
    _, _, agent, a, b, t = hosts
    original = b.api.teleport
    calls = []

    def lost(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(args[-1])
        if len(calls) == 1:
            raise httpx.ReadError("lost response")
        return result

    monkeypatch.setattr(b.api, "teleport", lost)
    result = restore(b, agent.adapter, t.stem, WS, steal=True)
    assert result.transcript.read_bytes() == t.read_bytes()
    assert len(calls) == 2 and calls[0] == calls[1]
    assert len(a.api.manifest(H, t.stem)["teleports"]) == 1


def test_restore_old_server_is_refused_before_local_changes(hosts):
    _, _, agent, _, b, t = hosts
    b.api._capabilities = set()
    with pytest.raises(ApiError, match="upgrade_required"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    assert agent.adapter.find_transcripts(t.stem) == []


def test_legacy_teleport_and_workspace_remain_supported(hosts):
    _, _, _, a, b, t = hosts
    b.api.teleport(H, t.stem, WS, True)
    a.api._capabilities = set()
    a.api.add_metadata(H, t.stem, WS, {"title": "legacy"})
    assert a.api.manifest(H, t.stem)["metadata"]["title"] == "legacy"


def test_send_refuses_partial_tail(hosts):
    _, laptop, _, a, _, t = hosts
    with t.open("ab") as f:
        f.write(b'{"partial":')
    local = t.read_bytes()
    with pytest.raises(TeleportError, match="unuploaded bytes"):
        send(a, laptop.adapter, t.stem, "agent-1")
    assert t.read_bytes() == local
    assert a.api.state(H, t.stem)["lease"]["host"] == "laptop"


def test_runner_fork_copies_all_side_files(hosts):
    _, _, agent, a, b, t = hosts
    side_log = json.dumps({"sessionId": t.stem, "type": "user"}).encode() + b"\n"
    g = a.api.create_generation(H, t.stem, WS, "subagents/agent-a.jsonl", "root")
    a.api.append(H, t.stem, WS, g["id"], 0, side_log, 0)
    files = {
        "tool-results/out.txt": b"tool result",
        "file-history/snapshot": b"old file",
        "workflows/run.json": b"{}",
        "plans/shared.md": b"source plan",
        "subagents/agent-a.meta.json": b"{}",
    }
    for name, data in files.items():
        a.api.put_blob(H, t.stem, WS, name, data)
    result = fork(b, agent.adapter, t.stem, WS)
    m = b.api.manifest(H, result.new_id)
    assert {f["path"] for f in m["files"]} == set(files) | {
        "transcript.jsonl",
        "subagents/agent-a.jsonl",
    }
    for name, data in files.items():
        assert b.api.read_blob(H, result.new_id, name) == data
        assert (
            agent.adapter.local_path(
                result.transcript.with_suffix(""), name
            ).read_bytes()
            == data
        )
    assert (
        result.new_id.encode()
        in (result.transcript.with_suffix("") / "subagents/agent-a.jsonl").read_bytes()
    )


def test_blob_cache_is_scoped_by_session(hosts):
    _, laptop, _, a, _, t = hosts
    plan = laptop.root / "plans" / "shared.md"
    plan.parent.mkdir()
    plan.write_text("plan")
    f = LocalFile("plans/shared.md", "blob", plan)
    first = laptop.adapter.session_for_transcript(t)
    Syncer(a, first, WS).sync_blob(f)
    second_path = laptop.transcript(WS)
    laptop.write(second_path, prompt("second"))
    second = laptop.adapter.session_for_transcript(second_path)
    syncer = Syncer(a, second, WS)
    syncer.run()
    syncer.sync_blob(f)
    assert a.api.read_blob(H, t.stem, f.rel_path) == b"plan"
    assert a.api.read_blob(H, second_path.stem, f.rel_path) == b"plan"


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


def test_pending_restore_blocks_sync_and_held_retries(hosts, monkeypatch):
    from hyperspace_client.cli import _restore_held

    _, _, agent, _, b, t = hosts
    original = b.api.teleport

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise httpx.ReadError("lost response")

    monkeypatch.setattr(b.api, "teleport", lost)
    with pytest.raises(TeleportError, match="confirmation"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    local = agent.adapter.project_dir(WS) / t.name
    assert local.read_bytes() == t.read_bytes()
    with pytest.raises(ApiError, match="restore_pending"):
        Syncer(b, agent.adapter.session_for_transcript(local), WS).run()
    monkeypatch.setattr(b.api, "teleport", original)
    _restore_held(b, agent.adapter, WS)
    assert not b.state.get(f"restore_pending:{H}:{t.stem}")
    assert b.state.cursor(local, b.api.cache_identity) is not None


def test_stale_lease_rejects_restore_even_with_steal(hosts):
    _, _, _, a, b, t = hosts
    revision = b.api.manifest(H, t.stem)["revision"]
    a.api.surrender(H, t.stem, "agent-1")
    with pytest.raises(ApiError, match="stale_snapshot"):
        b.api.teleport(H, t.stem, WS, True, revision, "t_test")
    assert a.api.state(H, t.stem)["lease"]["state"] == "reserved"


def test_force_restore_rejection_restores_divergent_local(hosts, monkeypatch):
    _, laptop, _, a, _, t = hosts
    laptop.write(t, prompt("local unsynced"))
    before = t.read_bytes()
    original = a.api.teleport

    def changed(*args, **kwargs):
        a.api.add_metadata(H, t.stem, WS, {"title": "changed"})
        return original(*args, **kwargs)

    monkeypatch.setattr(a.api, "teleport", changed)
    with pytest.raises(ApiError, match="stale_snapshot"):
        restore(a, laptop.adapter, t.stem, WS, force=True)
    assert t.read_bytes() == before


@pytest.mark.parametrize("retry_status", [401, 429, 504])
def test_ambiguous_restore_preserves_files_and_reconciles_same_id(
    hosts, monkeypatch, retry_status
):
    _, _, agent, a, b, t = hosts
    original = b.api.teleport
    calls = []

    def failures(*args, **kwargs):
        calls.append(args[-1])
        if len(calls) == 1:
            original(*args, **kwargs)
            raise ApiError(504, "error", "gateway timeout after commit")
        raise ApiError(retry_status, "rate_limited" if retry_status == 429 else "error")

    monkeypatch.setattr(b.api, "teleport", failures)
    with pytest.raises(TeleportError, match="confirmation"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    path = agent.adapter.project_dir(WS) / t.name
    assert path.read_bytes() == t.read_bytes()
    pending = json.loads(b.state.get(f"restore_pending:{H}:{t.stem}"))
    assert pending["teleport_id"] == calls[0] == calls[1]

    def recovered(*args, **kwargs):
        calls.append(args[-1])
        return original(*args, **kwargs)

    monkeypatch.setattr(b.api, "teleport", recovered)
    restore(b, agent.adapter, t.stem, WS)
    assert calls[-1] == calls[0]
    assert len(a.api.manifest(H, t.stem)["teleports"]) == 1
    assert not b.state.get(f"restore_pending:{H}:{t.stem}")


def test_pending_restore_blocks_every_prompt_without_credentials(hosts):
    from hyperspace_client.config import Config
    from hyperspace_client.hooks import handle

    _, _, agent, _, b, t = hosts
    b.state.put(f"restore_pending:{H}:{t.stem}", WS)
    cfg = Config(host="agent-1", state_dir=b.state.directory)
    payload = {
        "hook_event_name": "UserPromptSubmit",
        "transcript_path": str(agent.adapter.project_dir(WS) / t.name),
    }
    for _ in range(2):
        response = json.loads(handle(payload, cfg, start=lambda args: None))
        assert response["decision"] == "block"
        assert "unfinished restore" in response["reason"]
        assert "hyperspace restore" in response["reason"]


def test_log_cursors_do_not_cross_servers(hosts, tmp_path):
    _, laptop, _, a, _, t = hosts
    other = make_server(tmp_path / "other-server")
    try:
        api = other.api("laptop")
        from hyperspace_client.sync import Context

        ctx = Context(api, a.state, laptop.adapter, "laptop")
        session = laptop.adapter.session_for_transcript(t)
        Syncer(ctx, session, WS).run()
        m = api.manifest(H, t.stem)
        g = next(f for f in m["files"] if f["path"] == "transcript.jsonl")[
            "generations"
        ][0]
        assert api.read_log(H, t.stem, g["id"], 0, g["length"]) == t.read_bytes()
        laptop.write(t, prompt("new bytes"))
        Syncer(ctx, session, WS).run()
        Syncer(a, session, WS).run()
        assert (
            a.state.cursor(t, a.api.cache_identity).generation
            != a.state.cursor(t, api.cache_identity).generation
        )
    finally:
        other.close()


def test_legacy_cursor_is_ignored_and_remote_copy_adopted(hosts):
    _, laptop, _, a, _, t = hosts
    a.state._db.execute(
        "CREATE TABLE cursors (local_path TEXT PRIMARY KEY, generation TEXT)"
    )
    a.state._db.execute(
        "INSERT INTO cursors VALUES (?, ?)", (str(t), "wrong-server-generation")
    )
    a.state._db.execute("DELETE FROM remote_cursors")
    a.state._db.commit()
    Syncer(a, laptop.adapter.session_for_transcript(t), WS).run()
    assert (
        a.state.cursor(t, a.api.cache_identity).generation != "wrong-server-generation"
    )
    entry = next(
        f for f in a.api.manifest(H, t.stem)["files"] if f["path"] == "transcript.jsonl"
    )
    assert len(entry["generations"]) == 1


def test_conflict_after_commit_blocks_replay_and_preserves_files(hosts, monkeypatch):
    _, _, agent, a, b, t = hosts
    original = b.api.teleport
    calls = []

    def conflict(*args, **kwargs):
        calls.append(args[-1])
        result = original(*args, **kwargs)
        if len(calls) == 1:
            entry = next(
                f
                for f in a.api.manifest(H, t.stem)["files"]
                if f["path"] == "transcript.jsonl"
            )
            root = entry["generations"][0]
            a.api.append(
                H, t.stem, WS, root["id"], root["length"], b"activity one\n", 1
            )
            branch = a.api.create_generation(
                H, t.stem, WS, "transcript.jsonl", "diverge", root["id"], root["length"]
            )
            a.api.append(
                H, t.stem, WS, branch["id"], root["length"], b"activity two\n", 1
            )
            raise httpx.ReadError("lost first response")
        return result

    monkeypatch.setattr(b.api, "teleport", conflict)
    with pytest.raises(TeleportError, match="conflicted"):
        restore(b, agent.adapter, t.stem, WS, steal=True)
    assert calls[0] == calls[1]
    assert (agent.adapter.project_dir(WS) / t.name).read_bytes() == t.read_bytes()
    assert b.state.get(f"restore_pending:{H}:{t.stem}")
    assert a.api.state(H, t.stem)["state"] == "conflicted"


@pytest.mark.parametrize(
    "endpoint,body",
    [
        ("grants", {"host": "agent-1"}),
        ("teleports", {"workspace": WS, "steal": True}),
        ("lease/surrender", {}),
        ("forks", {"generation": "g_removed", "offset": 0}),
    ],
)
def test_existing_mutations_reauthorize_after_delayed_body(hosts, endpoint, body):
    server, _, _, a, _, t = hosts
    admin = Store(server.store.data_dir)
    key = SessionKey("default", H, t.stem)

    async def exercise():
        ready, finish = asyncio.Event(), asyncio.Event()

        async def delayed():
            ready.set()
            await finish.wait()
            yield json.dumps(body).encode()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.app), base_url="http://testserver"
        ) as client:
            path = f"/v1/sessions/{H}/{t.stem}"
            pending = asyncio.create_task(
                client.post(
                    path + "/" + endpoint,
                    headers={"Authorization": f"Bearer {server.tokens['agent']}"},
                    content=delayed(),
                )
            )
            await ready.wait()
            admin.purge_session(key, "test replacement")
            writer = Writer("default", "laptop", WS)
            g = admin.create_generation(key, "transcript.jsonl", writer, "root")
            data = b"private replacement\n"
            admin.append(
                key, g["id"], writer, 0, data, hashlib.sha256(data).hexdigest(), 1
            )
            finish.set()
            response = await pending
            assert response.status_code == 404
            assert response.json() == {"error": "not_found", "detail": "session"}
            response = await client.get(
                path, headers={"Authorization": f"Bearer {server.tokens['agent']}"}
            )
            assert response.status_code == 404

    try:
        asyncio.run(exercise())
        assert a.api.manifest(H, t.stem)["state"]["lease"]["host"] == "laptop"
    finally:
        admin.close()


@pytest.mark.parametrize(
    "suffix", ["", "/state", "/blobs/tool-results/private", "/generations/g_removed"]
)
def test_existing_reads_reauthorize_at_transaction_entry(hosts, monkeypatch, suffix):
    server, _, _, _, b, t = hosts
    admin = Store(server.store.data_dir)
    key = SessionKey("default", H, t.stem)
    original = server.store.existing_authorized

    def interleave(*args, **kwargs):
        admin.purge_session(key, "test replacement")
        writer = Writer("default", "laptop", WS)
        g = admin.create_generation(key, "transcript.jsonl", writer, "root")
        data = b"private replacement\n"
        admin.append(key, g["id"], writer, 0, data, hashlib.sha256(data).hexdigest(), 1)
        admin.put_blob(
            key, "tool-results/private", writer, data, hashlib.sha256(data).hexdigest()
        )
        return original(*args, **kwargs)

    monkeypatch.setattr(server.store, "existing_authorized", interleave)
    try:
        response = b.api._client.get(f"/sessions/{H}/{t.stem}" + suffix)
        assert response.status_code == 404
        assert response.json() == {"error": "not_found", "detail": "session"}
        assert (
            admin._db.execute(
                "SELECT COUNT(*) FROM read_audit WHERE what = 'manifest' AND host = 'agent-1'"
            ).fetchone()[0]
            == 0
        )
    finally:
        admin.close()


def test_existing_authorization_holds_cross_process_lock_through_use(hosts):
    server, _, _, _, _, t = hosts
    admin = Store(server.store.data_dir)
    key = SessionKey("default", H, t.stem)
    admin._db.execute("PRAGMA busy_timeout = 1")

    def use():
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            admin.purge_session(key, "must not interleave")
        return server.store.manifest(key)

    try:
        result = server.store.existing_authorized(
            key, "agent-1", "granted", use, "manifest"
        )
        assert result["session_id"] == t.stem
        assert admin.session_exists(key)
    finally:
        admin.close()


def test_conflict_response_uses_authorized_snapshot(hosts, monkeypatch):
    from store import Conflict

    server, _, _, a, b, t = hosts
    key = SessionKey("default", H, t.stem)
    admin = Store(server.store.data_dir)
    entry = next(
        f for f in a.api.manifest(H, t.stem)["files"] if f["path"] == "transcript.jsonl"
    )
    original = server.store.existing_authorized

    def interleave(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except Conflict:
            admin.purge_session(key, "test replacement after conflict")
            writer = Writer("default", "private-host", "/private/workspace")
            g = admin.create_generation(key, "transcript.jsonl", writer, "root")
            data = b"private\n"
            admin.append(
                key, g["id"], writer, 0, data, hashlib.sha256(data).hexdigest(), 1
            )
            raise

    monkeypatch.setattr(server.store, "existing_authorized", interleave)
    try:
        data = b"conflicting\n"
        response = b.api._client.post(
            f"/sessions/{H}/{t.stem}/generations/{entry['active']}/append",
            headers={
                "Hyperspace-Workspace": WS,
                "Hyperspace-Base-Length": "0",
                "Hyperspace-Sha256": hashlib.sha256(data).hexdigest(),
            },
            content=data,
        )
        assert response.status_code == 409
        assert response.json()["session"]["lease"]["host"] == "laptop"
        assert "private-host" not in response.text
        assert "/private/workspace" not in response.text
    finally:
        admin.close()


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


def test_pending_restore_replays_after_credential_rotation(tmp_path, monkeypatch):
    from hyperspace_client.sync import Context

    server = make_server(
        tmp_path / "server",
        {
            "laptop": ("laptop", {"append", "read", "grant"}, "all"),
            "agent": ("agent-1", {"append", "read", "grant"}, "granted"),
            "rotated": ("agent-1", {"append", "read", "grant"}, "granted"),
        },
    )
    laptop = FakeClaude(tmp_path / "laptop")
    agent = FakeClaude(tmp_path / "agent")
    a = context(server.api("laptop"), tmp_path / "a-state", laptop, "laptop")
    b = context(server.api("agent"), tmp_path / "b-state", agent, "agent-1")
    try:
        t = laptop.transcript(WS)
        laptop.write(t, prompt("backed up", cwd=WS))
        Syncer(a, laptop.adapter.session_for_transcript(t), WS).run()
        a.api.grant(H, t.stem, "agent-1")
        original = b.api.teleport
        calls: list[str] = []

        def lost(*args, **kwargs):
            calls.append(args[-1])
            if len(calls) == 1:
                original(*args, **kwargs)
            raise ApiError(504, "error", "gateway timeout")

        monkeypatch.setattr(b.api, "teleport", lost)
        with pytest.raises(TeleportError, match="confirmation"):
            restore(b, agent.adapter, t.stem, WS, steal=True)
        assert b.state.get(f"restore_pending:{H}:{t.stem}")

        # A pending request stays bound to its host and workspace.
        elsewhere = Context(server.api("rotated"), b.state, agent.adapter, "agent-2")
        with pytest.raises(TeleportError, match="original server"):
            restore(elsewhere, agent.adapter, t.stem, WS)
        with pytest.raises(TeleportError, match="original server"):
            restore(b, agent.adapter, t.stem, "/work/other")

        rotated = Context(server.api("rotated"), b.state, agent.adapter, "agent-1")
        restore(rotated, agent.adapter, t.stem, WS)
        assert not b.state.get(f"restore_pending:{H}:{t.stem}")
        teleports = a.api.manifest(H, t.stem)["teleports"]
        assert [e["id"] for e in teleports] == [calls[0]]
    finally:
        a.state.close()
        b.state.close()
        server.close()
