# vim: filetype=python
"""The storage core: generations, blobs, placements, leases, and conflicts."""

import hashlib
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import store as store_module
from hyperspace_protocol import (
    HeartbeatRequest,
    ReadScope,
    Status,
    StatusEvent,
    StatusRequest,
)
from store import (
    _MIGRATIONS,
    Conflict,
    NotFound,
    SessionKey,
    StaleStatus,
    Store,
    StoreError,
    Writer,
)

KEY = SessionKey("acct", "claude-code", "s1")
LAPTOP = Writer("acct", "laptop", "/home/wlritchi/hyperspace")
AGENT = Writer("acct", "agent-1", "/home/agent/workspace")
OTHER = Writer("acct", "agent-2", "/home/agent/workspace")
PATH = "transcript.jsonl"


class Clock:
    """A clock that moves one second forward at each call."""

    def __init__(self) -> None:
        self.t = datetime(2026, 10, 13, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


@pytest.fixture
def store(tmp_path: Path) -> Iterator[Store]:
    s = Store(tmp_path / "data", clock=Clock())
    yield s
    s.close()


def put(
    store: Store,
    gen: str,
    base: int,
    data: bytes,
    writer: Writer = LAPTOP,
    activity: int = 0,
    key: SessionKey = KEY,
) -> int:
    info, _ = store.append(
        key,
        gen,
        writer,
        base,
        data,
        hashlib.sha256(data).hexdigest(),
        activity,
    )
    return info["length"]


def root(store: Store, writer: Writer = LAPTOP, key: SessionKey = KEY) -> str:
    return store.create_generation(key, PATH, writer, "root")["id"]


def test_append_and_read(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n")
    n = put(store, g, n, b"two\n")
    assert n == 8
    assert store.read_log(KEY, g, 0, None) == b"one\ntwo\n"
    assert store.read_log(KEY, g, 2, 6) == b"e\ntw"


def test_retry_is_idempotent(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert put(store, g, 0, b"one\n") == 4
    assert store.read_log(KEY, g, 0, None) == b"one\n"


def test_wrong_base_length_is_a_conflict(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    with pytest.raises(Conflict) as err:
        put(store, g, 0, b"two\n")
    assert err.value.length == 4
    with pytest.raises(Conflict):
        put(store, g, 99, b"two\n")


def test_bad_digest(store: Store) -> None:
    g = root(store)
    with pytest.raises(StoreError) as err:
        store.append(KEY, g, LAPTOP, 0, b"x", "0" * 64, 0)
    assert err.value.code == "bad_digest"


def test_fork_reads_through_parent(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    put(store, g, 4, b"bbbb")
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc")
    assert store.read_log(KEY, r["id"], 0, None) == b"aaaacccc"
    assert store.read_log(KEY, r["id"], 2, 6) == b"aacc"
    assert store.read_log(KEY, g, 0, None) == b"aaaabbbb"


def test_fork_offset_must_be_in_range(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    with pytest.raises(StoreError):
        store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=5)


def test_second_root_is_refused(store: Store) -> None:
    root(store)
    with pytest.raises(StoreError) as err:
        root(store)
    assert err.value.code == "exists"


def test_recovery_cuts_uncommitted_frames(tmp_path: Path) -> None:
    data = tmp_path / "data"
    s = Store(data, clock=Clock())
    g = root(s)
    put(s, g, 0, b"one\n")
    s.close()
    log = data / "logs" / f"{g}.zst"
    size = log.stat().st_size
    with open(log, "ab") as f:
        f.write(b"garbage from a write that did not commit")
    s = Store(data, clock=Clock())
    assert s.recover() == [g]
    assert log.stat().st_size == size
    put(s, g, 4, b"two\n")
    assert s.read_log(KEY, g, 0, None) == b"one\ntwo\n"
    s.close()


def test_blob_versions_deduplicate(store: Store) -> None:
    def blob(data: bytes) -> int:
        sha = hashlib.sha256(data).hexdigest()
        return store.put_blob(KEY, "plans/p.md", LAPTOP, data, sha)["version"]

    assert blob(b"v1") == 1
    assert blob(b"v1") == 1
    assert blob(b"v2") == 2
    assert store.read_blob(KEY, "plans/p.md", None) == b"v2"
    assert store.read_blob(KEY, "plans/p.md", 1) == b"v1"


def test_blob_and_log_kinds_do_not_mix(store: Store) -> None:
    root(store)
    with pytest.raises(StoreError) as err:
        store.put_blob(KEY, PATH, LAPTOP, b"x", hashlib.sha256(b"x").hexdigest())
    assert err.value.code == "wrong_kind"


def test_first_append_acquires_the_lease(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    lease = store.session_state(KEY)["lease"]
    assert lease["state"] == "held"
    assert lease["host"] == "laptop"


def test_lease_transitions(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)

    with pytest.raises(StoreError) as err:
        store.surrender(KEY, "agent-1", None)
    assert err.value.code == "not_holder"

    state = store.surrender(KEY, "laptop", "agent-1")
    assert state["lease"]["state"] == "reserved"
    assert state["lease"]["host"] == "agent-1"

    with pytest.raises(StoreError) as err:
        store.teleport(KEY, OTHER, steal=False)
    assert err.value.code == "lease_held"

    event, state = store.teleport(KEY, AGENT, steal=False)
    assert not event["steal"]
    assert state["lease"] == {
        "state": "held",
        "host": "agent-1",
        "workspace": "/home/agent/workspace",
        "since": state["lease"]["since"],
    }

    # A non-holder can still append; the lease does not change.
    n = put(store, g, n, b"two\n", writer=LAPTOP)
    assert store.session_state(KEY)["lease"]["host"] == "agent-1"

    event, state = store.teleport(KEY, LAPTOP, steal=True)
    assert event["steal"]
    assert state["lease"]["host"] == "laptop"


def test_surrender_free_lets_any_host_take_it(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert store.surrender(KEY, "laptop", None)["lease"]["state"] == "free"
    _, state = store.teleport(KEY, OTHER, steal=False)
    assert state["lease"]["host"] == "agent-2"


def test_placements_record_causes(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)
    store.surrender(KEY, "laptop", "agent-1")
    store.teleport(KEY, AGENT, steal=False)
    n = put(store, g, n, b"two\n", writer=AGENT, activity=1)
    # The laptop resumes a stale copy: its append conflicts, and it forks.
    with pytest.raises(Conflict):
        put(store, g, 4, b"three\n")
    d = store.create_generation(KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=4)
    put(store, d["id"], 4, b"three\n")
    causes = [(p["host"], p["cause"]) for p in store.manifest(KEY)["placements"]]
    assert causes == [
        ("laptop", "start"),
        ("agent-1", "teleport"),
        ("laptop", "diverge"),
    ]


def test_move_without_teleport(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n")
    put(store, g, n, b"two\n", writer=Writer("acct", "laptop", "/elsewhere"))
    causes = [p["cause"] for p in store.manifest(KEY)["placements"]]
    assert causes == ["start", "move"]


def test_rewrite_never_conflicts(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa", activity=1)
    put(store, g, 4, b"bbbb", activity=1)
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc", activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log" and active["active"] == r["id"]


@pytest.mark.parametrize("inherited", [False, True])
def test_stale_rewrite_keeps_other_hosts_tail(store: Store, inherited: bool) -> None:
    g = root(store)
    n = put(store, g, 0, b"base\n", activity=1)
    store.teleport(KEY, AGENT, steal=True)
    end = put(store, g, n, b"agent work\n", writer=AGENT, activity=3)
    if inherited:
        g = store.create_generation(
            KEY, PATH, AGENT, "rewrite", parent=g, fork_offset=end
        )["id"]
        put(store, g, end, b"more work\n", writer=AGENT, activity=1)
    # The laptop did not see the agent's appends. Its rewrite forks from the
    # last offset that it uploaded.
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=n)
    put(store, r["id"], n, b"laptop work\n", activity=2)

    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert state["lease"]["host"] == "agent-1"
    assert {b["generation"] for b in state.get("branches", [])} == {g, r["id"]}
    with pytest.raises(StoreError) as err:
        store.teleport(KEY, OTHER, steal=True)
    assert err.value.code == "conflicted"


def test_rewrite_after_teleport_replaces_the_teleported_tail(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)
    put(store, g, n, b"failed\n", activity=1)
    store.surrender(KEY, "laptop", "agent-1")
    store.teleport(KEY, AGENT, steal=False)
    # The restored file is one segment in the agent's cursor, so a change to
    # the restored bytes forks at offset 0.
    r = store.create_generation(KEY, PATH, AGENT, "rewrite", parent=g, fork_offset=0)
    n = put(store, r["id"], 0, b"one\n", writer=AGENT, activity=1)
    put(store, r["id"], n, b"two\n", writer=AGENT, activity=1)
    assert store.session_state(KEY)["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log" and active["active"] == r["id"]


@pytest.mark.parametrize("leftover_first", [True, False])
def test_rewrite_after_steal_ignores_leftover_with_no_activity(
    store: Store, leftover_first: bool
) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)
    n = put(store, g, n, b"failed\n", activity=1)
    store.teleport(KEY, AGENT, steal=True)

    def leftover() -> None:
        # The laptop uploads bytes that it wrote before the steal. They have
        # no activity, and the agent never saw them.
        put(store, g, n, b"meta\n", activity=0)

    if leftover_first:
        leftover()
    # The agent restored the file as one cursor segment, so its change to the
    # restored bytes forks at offset 0.
    r = store.create_generation(KEY, PATH, AGENT, "rewrite", parent=g, fork_offset=0)
    m = put(store, r["id"], 0, b"one\n", writer=AGENT, activity=1)
    put(store, r["id"], m, b"two\n", writer=AGENT, activity=1)
    if not leftover_first:
        leftover()
    assert store.session_state(KEY)["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log" and active["active"] == r["id"]


def test_rewrite_after_steal_keeps_leftover_with_activity(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)
    store.teleport(KEY, AGENT, steal=True)
    put(store, g, n, b"laptop work\n", activity=1)
    r = store.create_generation(KEY, PATH, AGENT, "rewrite", parent=g, fork_offset=0)
    put(store, r["id"], 0, b"agent work\n", writer=AGENT, activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert {b["generation"] for b in state.get("branches", [])} == {g, r["id"]}


def test_teleport_before_other_appends_does_not_cover_them(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"base\n", activity=1)
    store.teleport(KEY, AGENT, steal=True)
    store.teleport(KEY, LAPTOP, steal=True)
    put(store, g, n, b"agent work\n", writer=AGENT, activity=1)
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=n)
    put(store, r["id"], n, b"laptop work\n", activity=1)
    assert store.session_state(KEY)["state"] == "conflicted"


def test_rewrite_can_replace_own_tail_after_teleport(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"base\n", activity=1)
    store.teleport(KEY, AGENT, steal=True)
    put(store, g, n, b"agent work\n", writer=AGENT, activity=1)
    r = store.create_generation(KEY, PATH, AGENT, "rewrite", parent=g, fork_offset=n)
    put(store, r["id"], n, b"new work\n", writer=AGENT, activity=1)
    assert store.session_state(KEY)["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log" and active["active"] == r["id"]


def _diverge(store: Store, laptop_activity: int, agent_activity: int) -> str:
    g = root(store)
    n = put(store, g, 0, b"base\n", activity=1)
    put(store, g, n, b"agent\n", writer=AGENT, activity=agent_activity)
    d = store.create_generation(KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=n)
    put(store, d["id"], n, b"laptop\n", activity=laptop_activity)
    return d["id"]


def test_two_active_branches_conflict(store: Store) -> None:
    _diverge(store, laptop_activity=1, agent_activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert {b["host"] for b in state.get("branches", [])} == {"laptop", "agent-1"}


def test_fork_inside_a_segment_counts_that_segment(store: Store) -> None:
    # One append holds the shared prefix and the agent's new prompt. The
    # laptop forks at a byte inside that segment (as `_adopt_remote` does at
    # the first differing byte) and adds its own prompt. The server cannot
    # tell where in the segment the agent's activity is, so it must count
    # the full segment, and the session is conflicted.
    g = root(store, writer=AGENT)
    put(store, g, 0, b"common\nprompt A\n", writer=AGENT, activity=1)
    fork = len(b"common\n")
    d = store.create_generation(
        KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=fork
    )
    put(store, d["id"], fork, b"prompt B\n", activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert {b["host"] for b in state.get("branches", [])} == {"laptop", "agent-1"}


def test_branch_with_only_metadata_does_not_conflict(store: Store) -> None:
    _diverge(store, laptop_activity=0, agent_activity=1)
    assert store.session_state(KEY)["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log"
    gens = {g["id"]: g for g in active["generations"]}
    # The active branch is the one with activity: the root generation, which
    # the agent continued. The laptop's diverge branch has none.
    assert gens[active["active"]]["cause"] == "root"


def test_conflict_is_sticky(store: Store) -> None:
    d = _diverge(store, laptop_activity=1, agent_activity=1)
    put(store, d, len(b"base\nlaptop\n"), b"more\n", activity=0)
    assert store.session_state(KEY)["state"] == "conflicted"


def test_conflicted_session_refuses_teleport(store: Store) -> None:
    _diverge(store, laptop_activity=1, agent_activity=1)
    with pytest.raises(StoreError) as err:
        store.teleport(KEY, OTHER, steal=True)
    assert err.value.code == "conflicted"


def test_recovery_fork(store: Store) -> None:
    d = _diverge(store, laptop_activity=1, agent_activity=1)
    fork = store.create_fork(KEY, d, len(b"base\nlaptop\n"))
    new_key = SessionKey("acct", "claude-code", fork["session_id"])
    assert store.session_exists(new_key)
    g = store.create_generation(new_key, PATH, LAPTOP, "fork")["id"]
    put(store, g, 0, b"forked copy\n", key=new_key, activity=1)
    manifest = store.manifest(new_key)
    assert manifest["forked_from"]["session_id"] == "s1"
    assert manifest["state"]["lease"]["host"] == "laptop"


def test_read_scopes(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert store.can_read(KEY, "laptop", "own")
    assert not store.can_read(KEY, "agent-1", "own")
    assert not store.can_read(KEY, "agent-1", "granted")
    assert store.can_read(KEY, "agent-1", "all")
    store.grant(KEY, "agent-1", "laptop")
    assert store.can_read(KEY, "agent-1", "granted")
    assert not store.can_read(KEY, "agent-1", "own")
    assert not store.can_read(SessionKey("acct", "claude-code", "nope"), "x", "all")


@pytest.mark.parametrize("first", ["metadata", "blob"])
@pytest.mark.parametrize("scope", ["own", "granted"])
def test_restricted_host_can_continue_new_session(
    tmp_path: Path, first: str, scope: ReadScope
) -> None:
    data_dir = tmp_path / "data"

    def upload(store: Store) -> None:
        if first == "metadata":
            store.add_metadata(KEY, AGENT, {"title": "New session"})
        else:
            store.put_blob(
                KEY,
                "result.txt",
                AGENT,
                b"result",
                hashlib.sha256(b"result").hexdigest(),
            )

    with closing(Store(data_dir)) as store:
        store.write_authorized(KEY, AGENT.host, scope, lambda: upload(store))

    # Access must survive a restart before the first transcript upload.
    with closing(Store(data_dir)) as store:
        store.write_authorized(KEY, AGENT.host, scope, lambda: upload(store))
        assert store.can_read(KEY, AGENT.host, scope)
        assert store.session_state(KEY)["lease"]["state"] == "free"
        assert not store.can_read(KEY, OTHER.host, scope)
        with pytest.raises(StoreError, match="session"):
            store.write_authorized(KEY, OTHER.host, scope, lambda: root(store, OTHER))
        gen = store.write_authorized(KEY, AGENT.host, scope, lambda: root(store, AGENT))
        store.existing_authorized(
            KEY, AGENT.host, scope, lambda: put(store, gen, 0, b"prompt\n", AGENT)
        )
        assert store.session_state(KEY)["lease"]["host"] == AGENT.host


def test_list_sessions_filters(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    store.add_metadata(KEY, LAPTOP, {"title": "Hyperspace design", "cwd": "/x"})
    other = SessionKey("acct", "claude-code", "s2")
    g2 = root(store, writer=AGENT, key=other)
    put(store, g2, 0, b"one\n", writer=AGENT, key=other)

    def ids(**kwargs: str) -> list[str]:
        return [
            s["session_id"]
            for s in store.list_sessions("acct", "laptop", "all", **kwargs)
        ]

    assert ids() == ["s2", "s1"]
    assert ids(title="design") == ["s1"]
    assert ids(on_host="agent-1") == ["s2"]
    assert [s["session_id"] for s in store.list_sessions("acct", "laptop", "own")] == [
        "s1"
    ]


def test_purge_session(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"secret\n")
    sha = hashlib.sha256(b"plan").hexdigest()
    store.put_blob(KEY, "plans/p.md", LAPTOP, b"plan", sha)
    store.add_metadata(KEY, LAPTOP, {"title": "has a secret"})
    result = store.purge_session(KEY, "leaked token")
    assert result["generations"] == 1 and result["objects"] == 1
    assert not store.session_exists(KEY)
    assert not (store.logs_dir / f"{g}.zst").exists()
    assert not store.objects.has(sha)
    # The id is free again.
    g2 = root(store)
    put(store, g2, 0, b"clean\n")
    assert store.read_log(KEY, g2, 0, None) == b"clean\n"
    rows = store._db.execute("SELECT target, reason FROM purges").fetchall()
    assert [tuple(r) for r in rows] == [("session acct/claude-code/s1", "leaked token")]


def test_read_audit_keeps_the_session_key_after_a_purge(store: Store) -> None:
    # SQLite gives the next session the rowid of the purged session, because
    # the purged session had the highest rowid.
    root(store)
    store.audit_read(KEY, "laptop", "manifest")
    store.purge_session(KEY, "leaked token")
    other = SessionKey("acct", "claude-code", "s2")
    root(store, key=other)
    rows = store._db.execute(
        "SELECT account, harness, session_id, what FROM read_audit"
    ).fetchall()
    assert [tuple(r) for r in rows] == [("acct", "claude-code", "s1", "manifest")]


def version_1_index(data: Path) -> sqlite3.Connection:
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    db.executescript(_MIGRATIONS[0])
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (1)")
    return db


def test_migration_copies_the_session_key_into_read_audit(tmp_path: Path) -> None:
    data = tmp_path / "data"
    db = version_1_index(data)
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'acct', 'claude-code', 's1', 't1')"
    )
    db.executemany(
        "INSERT INTO read_audit (ts, account, host, session, what) "
        "VALUES (?, 'acct', 'laptop', ?, 'manifest')",
        # A later read, a read at the time of creation, and a read of a
        # session that is gone.
        [("t2", 1), ("t1", 1), ("t2", 2)],
    )
    db.commit()
    db.close()
    store = Store(data)
    rows = store._db.execute(
        "SELECT session, harness, session_id FROM read_audit ORDER BY id"
    ).fetchall()
    store.close()
    assert [tuple(r) for r in rows] == [
        (1, "claude-code", "s1"),
        (1, "claude-code", "s1"),
        (2, None, None),
    ]


def test_migration_does_not_give_old_reads_to_a_session_that_took_the_rowid(
    tmp_path: Path,
) -> None:
    # Before the upgrade: a read of "secret", a purge of "secret", and then a
    # new session "fresh" with the same rowid, which also has a read.
    data = tmp_path / "data"
    db = version_1_index(data)
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'acct', 'claude-code', 'fresh', 't4')"
    )
    db.executemany(
        "INSERT INTO read_audit (ts, account, host, session, what) "
        "VALUES (?, 'acct', 'laptop', 1, 'manifest')",
        [("t2",), ("t5",)],
    )
    db.execute(
        "INSERT INTO purges (ts, target, reason) "
        "VALUES ('t3', 'session acct/claude-code/secret', 'leaked token')"
    )
    db.commit()
    db.close()
    store = Store(data)
    rows = store._db.execute(
        "SELECT ts, harness, session_id FROM read_audit ORDER BY id"
    ).fetchall()
    store.close()
    assert [tuple(r) for r in rows] == [
        ("t2", None, None),
        ("t5", "claude-code", "fresh"),
    ]


def test_migration_does_not_guess_when_a_purge_has_the_time_of_the_read(
    tmp_path: Path,
) -> None:
    # The read, the purge, and the new session all have the same time. Thus
    # the read can be of the purged session.
    data = tmp_path / "data"
    db = version_1_index(data)
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'acct', 'claude-code', 'fresh', 't')"
    )
    db.execute(
        "INSERT INTO read_audit (ts, account, host, session, what) "
        "VALUES ('t', 'acct', 'laptop', 1, 'manifest')"
    )
    db.execute(
        "INSERT INTO purges (ts, target, reason) "
        "VALUES ('t', 'session acct/claude-code/secret', 'leaked token')"
    )
    db.commit()
    db.close()
    store = Store(data)
    rows = store._db.execute("SELECT harness, session_id FROM read_audit").fetchall()
    store.close()
    assert [tuple(r) for r in rows] == [(None, None)]


def test_purge_generation_takes_its_forks(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    put(store, g, 4, b"bbbb")
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc")
    other = store.create_generation(KEY, "subagents/a.jsonl", LAPTOP, "root")
    info, _ = store.append(
        KEY, other["id"], LAPTOP, 0, b"x\n", hashlib.sha256(b"x\n").hexdigest(), 0
    )
    assert info["length"] == 2
    result = store.purge_generation(KEY, g, "secret in the root")
    assert result["generations"] == 2
    assert [f["path"] for f in store.manifest(KEY)["files"]] == ["subagents/a.jsonl"]
    with pytest.raises(StoreError):
        store.read_log(KEY, r["id"], 0, None)


def test_purge_blob_version_keeps_shared_objects(store: Store) -> None:
    sha = hashlib.sha256(b"same").hexdigest()
    store.put_blob(KEY, "tool-results/a.txt", LAPTOP, b"same", sha)
    store.put_blob(KEY, "tool-results/b.txt", LAPTOP, b"same", sha)
    result = store.purge_blob_version(KEY, "tool-results/a.txt", 1, "test")
    assert result["objects"] == 0
    assert store.objects.has(sha)
    assert store.read_blob(KEY, "tool-results/b.txt", None) == b"same"
    store.purge_blob_version(KEY, "tool-results/b.txt", 1, "test")
    assert not store.objects.has(sha)


def _record_syncs(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Record each fsync (as the (st_dev, st_ino) of its target) and each
    index commit, in order."""
    events: list[object] = []
    real_fsync = os.fsync
    real_exit = store_module._Transaction.__exit__

    def fsync(fd: int) -> None:
        st = os.fstat(fd)
        events.append((st.st_dev, st.st_ino))
        real_fsync(fd)

    def exit_(
        self: store_module._Transaction, exc_type: object, exc: object, tb: object
    ) -> None:
        if self._depth_owner and exc_type is None:
            events.append("commit")
        real_exit(self, exc_type, exc, tb)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(store_module._Transaction, "__exit__", exit_)
    return events


def _ident(path: Path) -> tuple[int, int]:
    st = path.stat()
    return (st.st_dev, st.st_ino)


def _synced_before_commit(events: list[object], path: Path) -> bool:
    commit = events.index("commit")
    return _ident(path) in events[:commit]


def test_first_append_syncs_directories_before_commit(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    g = root(store)
    events = _record_syncs(monkeypatch)
    put(store, g, 0, b"first\n")
    log = store.logs_dir / f"{g}.zst"
    # The file, its new directory, and the entry of that directory must be
    # durable before the index refers to the file.
    for path in (log, store.logs_dir, store.data_dir):
        assert _synced_before_commit(events, path), path


def test_first_append_to_fork_syncs_directories(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    fork = store.create_generation(
        KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4
    )
    events = _record_syncs(monkeypatch)
    put(store, fork["id"], 4, b"bbbb")
    assert _synced_before_commit(events, store.logs_dir)


def test_new_object_syncs_directories_before_commit(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = _record_syncs(monkeypatch)
    sha = hashlib.sha256(b"plan").hexdigest()
    store.put_blob(KEY, "plans/p.md", LAPTOP, b"plan", sha)
    obj = store.objects._path(sha)
    for path in (obj, obj.parent, store.objects.root, store.data_dir):
        assert _synced_before_commit(events, path), path


def test_existing_object_syncs_directories_before_commit(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A crash after the rename and before the directory fsync leaves the
    # object name in the page cache only. The next put must make it durable.
    sha = hashlib.sha256(b"same").hexdigest()
    store.objects.put(b"same")
    events = _record_syncs(monkeypatch)
    store.put_blob(KEY, "tool-results/a.txt", LAPTOP, b"same", sha)
    obj = store.objects._path(sha)
    for path in (obj.parent, store.objects.root, store.data_dir):
        assert _synced_before_commit(events, path), path


def test_blob_promotion_keeps_latest_content(store: Store) -> None:
    for data in (b"first", b"latest"):
        store.put_blob(KEY, PATH, LAPTOP, data, hashlib.sha256(data).hexdigest())
    gen = store.create_generation(KEY, PATH, LAPTOP, "root")
    assert gen["length"] == len(b"latest")
    assert store.read_log(KEY, gen["id"], 0, None) == b"latest"
    assert store.read_blob(KEY, PATH, 1) == b"first"
    assert store.read_blob(KEY, PATH, 2) == b"latest"
    with pytest.raises(StoreError) as err:
        store.create_generation(KEY, PATH, LAPTOP, "root")
    assert err.value.code == "exists"
    store.purge_generation(KEY, gen["id"], "test")
    assert store.read_blob(KEY, PATH, 2) == b"latest"
    assert store.manifest(KEY)["files"][0]["kind"] == "blob"
    assert store.purge_session(KEY, "test")["blob_versions"] == 2


# --- Status and liveness ------------------------------------------------


def set_status(
    store: Store,
    status: Status = "working",
    base: int = 0,
    key: SessionKey = KEY,
    host: str = "laptop",
    **extra: str,
) -> StatusEvent:
    request: StatusRequest = {
        "status": status,
        "source_kind": "hook",
        "explicit": False,
        "base_seq": base,
    }
    if status == "ready":
        request["reason"] = "idle"
    if status == "waiting":
        request["wake"] = "local"
    request.update(extra)  # type: ignore[typeddict-item]
    return store.add_status(key, host, request)


def beat(
    store: Store,
    host: str,
    *keys: SessionKey,
    interval: int = 60,
    scope: ReadScope = "all",
    **extra: object,
) -> int:
    request: HeartbeatRequest = {
        "interval": interval,
        "sessions": [{"harness": k.harness, "session_id": k.session_id} for k in keys],
    }
    request.update(extra)  # type: ignore[typeddict-item]
    return store.heartbeat("acct", host, scope, request)["recorded"]


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def timed(tmp_path: Path, clock: Clock) -> Iterator[Store]:
    s = Store(tmp_path / "data", clock=clock)
    yield s
    s.close()


def test_status_first_write_needs_base_zero(store: Store) -> None:
    with pytest.raises(NotFound):
        set_status(store)
    root(store)
    with pytest.raises(StaleStatus) as err:
        set_status(store, base=1)
    assert err.value.code == "stale_status" and err.value.current is None
    event = set_status(store, note="first")
    assert event["seq"] == 1 and event["note"] == "first"
    assert event["source"] == {"kind": "hook", "host": "laptop"}
    assert event["explicit"] is False


def test_status_cas_rejects_stale_base(store: Store) -> None:
    root(store)
    first = set_status(store, "ready")
    with pytest.raises(StaleStatus) as err:
        set_status(store, "working", base=0)
    assert err.value.current == first
    assert set_status(store, "working", base=1)["seq"] == 2


def test_status_aba_is_rejected(store: Store) -> None:
    root(store)
    set_status(store, "done")
    set_status(store, "ready", base=1)
    marked = set_status(store, "done", base=2, host="agent-1")
    # A writer that saw only the first "done" cannot replace the second one.
    with pytest.raises(StaleStatus) as err:
        set_status(store, "working", base=1)
    assert err.value.current == marked
    assert marked["source"]["host"] == "agent-1"


def test_status_in_state_body_without_excerpt(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    event = set_status(
        store,
        "waiting",
        note="CI on PR #41",
        excerpt="I pushed the fix.",
        source_session="s9",
    )
    state = store.session_state(KEY)
    assert state["status"] == event
    assert "excerpt" not in state["status"]
    assert event["wake"] == "local"
    assert event["source"]["session_id"] == "s9"
    assert (
        state["last_append"]
        == store.manifest(KEY)["files"][0]["generations"][0]["last_append"]
    )


def test_list_has_excerpt(store: Store) -> None:
    root(store)
    assert "excerpt" not in store.list_sessions("acct", "laptop", "all")[0]
    set_status(store, "ready", excerpt="Which backend?")
    assert store.list_sessions("acct", "laptop", "all")[0]["excerpt"] == (
        "Which backend?"
    )


def test_liveness_live_stopped_unknown(timed: Store, clock: Clock) -> None:
    root(timed)
    assert "liveness" not in timed.session_state(KEY)
    beat(timed, "laptop", KEY)
    live = timed.session_state(KEY)["liveness"]
    assert live["state"] == "live" and live["host"] == "laptop"
    since = live["since"]
    clock.t += timedelta(seconds=60)
    beat(timed, "laptop", KEY)
    # The run continues, so the start time stays.
    assert timed.session_state(KEY)["liveness"] == {
        "state": "live",
        "host": "laptop",
        "since": since,
    }
    clock.t += timedelta(seconds=200)
    unknown = timed.session_state(KEY)["liveness"]
    assert unknown["state"] == "unknown"
    # After a gap, a new heartbeat starts a new run.
    beat(timed, "laptop", KEY)
    again = timed.session_state(KEY)["liveness"]
    assert again["state"] == "live" and again["since"] > since
    last = again["since"]
    beat(timed, "laptop")
    stopped = timed.session_state(KEY)["liveness"]
    assert stopped["state"] == "stopped" and stopped["since"] >= last
    # A heartbeat after a stop also starts a new run.
    beat(timed, "laptop", KEY)
    assert timed.session_state(KEY)["liveness"]["since"] > stopped["since"]


def test_liveness_follows_the_host_that_listed_last(store: Store) -> None:
    root(store)
    beat(store, "laptop", KEY)
    beat(store, "agent-1", KEY)
    assert store.session_state(KEY)["liveness"]["host"] == "agent-1"


def test_stopped_copy_does_not_hide_a_live_copy(timed: Store, clock: Clock) -> None:
    root(timed)
    beat(timed, "laptop", KEY)
    since = timed.session_state(KEY)["liveness"]["since"]
    beat(timed, "agent-1", KEY)
    # The copy on agent-1 stops. The heartbeat of the laptop has not expired
    # and lists the session.
    beat(timed, "agent-1")
    assert timed.session_state(KEY)["liveness"] == {
        "state": "live",
        "host": "laptop",
        "since": since,
    }
    # The same rule applies to two sources of one host.
    beat(timed, "laptop", KEY, source="b")
    beat(timed, "laptop", source="b")
    live = timed.session_state(KEY)["liveness"]
    assert live["state"] == "live" and live["since"] == since
    # With no live copy, the host that listed last gives the state.
    clock.t += timedelta(hours=1)
    stopped = timed.session_state(KEY)["liveness"]
    assert stopped["state"] == "stopped" and stopped["host"] == "laptop"
    assert stopped["since"] > since


def test_final_heartbeat_stops_sessions(timed: Store, clock: Clock) -> None:
    root(timed)
    beat(timed, "laptop", KEY)
    beat(timed, "laptop", final=True)
    clock.t += timedelta(hours=1)
    # It stays stopped, not unknown, after the heartbeat expires.
    assert timed.session_state(KEY)["liveness"]["state"] == "stopped"


def test_heartbeat_close_blocked(store: Store) -> None:
    root(store)
    beat(store, "laptop", KEY)
    reason = "unpushed work in worktree"
    beat(
        store,
        "laptop",
        unclosed=[
            {"harness": KEY.harness, "session_id": KEY.session_id, "reason": reason}
        ],
    )
    liveness = store.session_state(KEY)["liveness"]
    assert liveness["state"] == "stopped" and liveness["close_blocked"] == reason


def test_close_blocked_for_a_session_never_listed(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    last = store.session_state(KEY)["last_append"]
    unclosed = [{"harness": KEY.harness, "session_id": KEY.session_id, "reason": "x"}]
    beat(store, "laptop", unclosed=unclosed)
    liveness = store.session_state(KEY)["liveness"]
    assert liveness == {
        "state": "stopped",
        "host": "laptop",
        "since": last,
        "close_blocked": "x",
    }


def test_unclosed_needs_read_access(store: Store) -> None:
    root(store)
    unclosed = [{"harness": KEY.harness, "session_id": KEY.session_id, "reason": "x"}]
    beat(store, "agent-1", scope="granted", unclosed=unclosed)
    assert "liveness" not in store.session_state(KEY)
    beat(store, "laptop", KEY)
    beat(store, "agent-1", scope="granted", unclosed=unclosed)
    assert "close_blocked" not in store.session_state(KEY)["liveness"]


def test_heartbeat_skips_unknown_and_unreadable_sessions(store: Store) -> None:
    root(store)
    missing = SessionKey("acct", "claude-code", "missing")
    assert beat(store, "agent-1", KEY, missing, scope="granted") == 0
    assert "liveness" not in store.session_state(KEY)
    assert beat(store, "laptop", KEY, missing, scope="own") == 1
    assert store.session_state(KEY)["liveness"]["host"] == "laptop"


def test_revision_ignores_status_and_heartbeat(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    revision = store.manifest(KEY)["revision"]
    set_status(store, "ready")
    beat(store, "laptop", KEY)
    assert store.manifest(KEY)["revision"] == revision
    put(store, g, 4, b"two\n")
    assert store.manifest(KEY)["revision"] != revision


def test_list_filters_status_open_lease_host_liveness(store: Store) -> None:
    keys = [SessionKey("acct", "claude-code", f"s{n}") for n in range(1, 5)]
    for key in keys[:3]:
        put(store, root(store, key=key), 0, b"one\n", key=key)
    g = root(store, writer=AGENT, key=keys[3])
    put(store, g, 0, b"one\n", writer=AGENT, key=keys[3])
    set_status(store, "ready", key=keys[0])
    set_status(store, "done", key=keys[1])
    set_status(store, "working", key=keys[3])
    beat(store, "agent-1", keys[3])

    def ids(**kwargs: object) -> list[str]:
        return sorted(
            s["session_id"]
            for s in store.list_sessions("acct", "laptop", "all", **kwargs)  # type: ignore[arg-type]
        )

    assert ids(statuses={"ready"}) == ["s1"]
    assert ids(statuses={"ready", "none"}) == ["s1", "s3"]
    assert ids(open_only=True) == ["s1", "s3", "s4"]
    assert ids(lease_host="agent-1") == ["s4"]
    assert ids(lease_host="laptop", open_only=True) == ["s1", "s3"]
    assert ids(liveness={"live"}) == ["s4"]
    assert ids(liveness={"none"}) == ["s1", "s2", "s3"]


def test_purge_session_removes_status_and_liveness(store: Store) -> None:
    root(store)
    set_status(store, "ready")
    set_status(store, "done", base=1)
    beat(store, "laptop", KEY)
    assert store.status_counts(KEY) == {"status_events": 2, "liveness": 1}
    result = store.purge_session(KEY, "test")
    assert result["status_events"] == 2 and result["liveness"] == 1
    # A new session with the same id starts with no status.
    root(store)
    state = store.session_state(KEY)
    assert "status" not in state and "liveness" not in state
    assert set_status(store)["seq"] == 1


def test_migration_3_on_a_version_2_index(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    for migration in _MIGRATIONS[:2]:
        db.executescript(migration)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (2)")
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'acct', 'claude-code', 's1', 't1')"
    )
    db.commit()
    db.close()
    with closing(Store(data)) as migrated:
        version = migrated._db.execute("SELECT version FROM schema_version").fetchone()
        assert version["version"] == len(_MIGRATIONS)
        assert "status" not in migrated.session_state(KEY)
        assert set_status(migrated)["seq"] == 1
        assert beat(migrated, "laptop", KEY) == 1
