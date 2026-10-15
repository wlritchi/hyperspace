# vim: filetype=python
"""Regression tests for the repository-wide review, with isolated fake hosts."""

from __future__ import annotations

import hashlib
import sqlite3
import threading

import pytest

from _hs import FakeClaude, context, make_server, prompt
from hyperspace_client.sync import Syncer
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
