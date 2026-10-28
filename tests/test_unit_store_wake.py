# vim: filetype=python
"""Wake requests: the verdicts, when a request is pending, runs, the wake
mark of a heartbeat, the list filter, and the migration."""

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hyperspace_protocol import HeartbeatRequest, Status, StatusRequest
from store import _MIGRATIONS, NotFound, SessionKey, Store, StoreError, Writer

KEY = SessionKey("acct", "claude-code", "s1")
LAPTOP = Writer("acct", "laptop", "/home/user/project")


class Clock:
    """A clock that moves one second forward at each call."""

    def __init__(self) -> None:
        self.t = datetime(2026, 10, 28, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> Iterator[Store]:
    s = Store(tmp_path / "data", clock=clock)
    yield s
    s.close()


def start(store: Store, key: SessionKey = KEY, writer: Writer = LAPTOP) -> None:
    """Make a session whose lease `writer` holds."""
    gen = store.create_generation(key, "transcript.jsonl", writer, "root")["id"]
    data = b"one\n"
    store.append(key, gen, writer, 0, data, hashlib.sha256(data).hexdigest(), 1)


def beat(
    store: Store, host: str, *keys: SessionKey, interval: int = 60, **extra: object
) -> None:
    request: HeartbeatRequest = {
        "interval": interval,
        "sessions": [{"harness": k.harness, "session_id": k.session_id} for k in keys],
    }
    request.update(extra)  # type: ignore[typeddict-item]
    store.heartbeat("acct", host, "all", request)


def set_status(
    store: Store, status: Status, base: int, key: SessionKey = KEY, **extra: str
) -> None:
    request: StatusRequest = {
        "status": status,
        "source_kind": "agent",
        "explicit": True,
        "base_seq": base,
    }
    if status == "ready":
        request["reason"] = "asked"
    if status == "waiting":
        request["wake"] = "message"
    request.update(extra)  # type: ignore[typeddict-item]
    store.add_status(key, "laptop", request)


def pending(store: Store, key: SessionKey = KEY) -> bool:
    return "wake_request" in store.session_state(key)


def test_wake_is_pending_until_a_new_run(store: Store) -> None:
    start(store)
    assert not pending(store)
    assert store.wake(KEY, 2) == "queued"
    request = store.session_state(KEY)["wake_request"]
    assert request["count"] == 2
    # A new wake replaces the request.
    assert store.wake(KEY, 3) == "queued"
    newer = store.session_state(KEY)["wake_request"]
    assert newer["count"] == 3 and newer["received"] > request["received"]
    # The first heartbeat that lists the session starts a run and ends the
    # request. Status events do not change it.
    set_status(store, "waiting", 0)
    assert pending(store)
    beat(store, "laptop", KEY)
    assert not pending(store)


def test_each_status_but_done_gets_a_wake(store: Store) -> None:
    start(store)
    statuses: list[Status] = ["working", "waiting", "ready"]
    for base, status in enumerate(statuses):
        set_status(store, status, base)
        assert store.wake(KEY, 1) == "queued"
        assert pending(store)


def test_done_ends_a_pending_request_and_refuses_wakes(store: Store) -> None:
    start(store)
    store.wake(KEY, 1)
    set_status(store, "done", 0, note="merged")
    assert not pending(store)
    with pytest.raises(StoreError) as err:
        store.wake(KEY, 1)
    assert err.value.code == "session_done"
    assert err.value.detail == "session done: merged"
    set_status(store, "done", 1)
    with pytest.raises(StoreError) as err:
        store.wake(KEY, 1)
    assert err.value.detail == "session done"
    # A later status opens the session again, and its request is pending
    # again: no run started after it.
    set_status(store, "ready", 2)
    assert pending(store)


def test_wake_refusals(store: Store) -> None:
    with pytest.raises(NotFound):
        store.wake(KEY, 1)
    start(store)
    store.surrender(KEY, "laptop", None)
    with pytest.raises(StoreError) as err:
        store.wake(KEY, 1)
    assert err.value.code == "lease_free"
    assert err.value.detail == "no host holds the session"
    assert not pending(store)


def test_reserved_lease_is_queued(store: Store) -> None:
    start(store)
    store.surrender(KEY, "laptop", "agent-1")
    # A launcher on the target host does not take a reserved lease.
    beat(store, "agent-1", wakes=True)
    assert store.wake(KEY, 1) == "queued"
    assert pending(store)


def test_a_heartbeat_gap_is_not_a_new_run(store: Store, clock: Clock) -> None:
    start(store)
    beat(store, "laptop", KEY)
    since = store.session_state(KEY)["liveness"]["since"]
    assert store.wake(KEY, 1) == "live"
    clock.t += timedelta(hours=1)
    assert store.session_state(KEY)["liveness"]["state"] == "unknown"
    # The host lost contact with the server, and the run continues. The
    # `since` of the liveness starts again after the gap.
    beat(store, "laptop", KEY)
    state = store.session_state(KEY)
    assert state["liveness"]["since"] > since
    assert "wake_request" in state
    # A stop and a new listing are a new run.
    beat(store, "laptop")
    assert pending(store)
    beat(store, "laptop", KEY)
    assert not pending(store)


def test_a_run_from_before_the_wake_does_not_end_it(store: Store) -> None:
    start(store)
    beat(store, "laptop", KEY)
    assert store.wake(KEY, 1) == "live"
    beat(store, "laptop", KEY)
    assert pending(store)
    # A run on another source of the host is a new run.
    beat(store, "laptop", KEY, source="other")
    assert not pending(store)


def test_unclosed_job_is_not_a_run(store: Store) -> None:
    start(store)
    store.wake(KEY, 1)
    # An append that arrives after the wake (a delayed sync of the job) moves
    # the last activity after the wake. The job then shows only as unclosed.
    data = b"two\n"
    gen = store.manifest(KEY)["files"][0]["generations"][0]["id"]
    store.append(KEY, gen, LAPTOP, 4, data, hashlib.sha256(data).hexdigest(), 1)
    unclosed = [{"harness": KEY.harness, "session_id": KEY.session_id, "reason": "x"}]
    beat(store, "laptop", unclosed=unclosed)
    with store._read():
        row = store._db.execute("SELECT * FROM session_liveness").fetchone()
    assert row["last_listed"] > store.session_state(KEY)["wake_request"]["received"]
    assert row["run_started"] is None
    # No heartbeat listed a run, so the request stays.
    assert pending(store)
    assert store.list_sessions("acct", "laptop", "all", wake_pending=True)
    # The first listing starts a run.
    beat(store, "laptop", KEY)
    assert not pending(store)


def test_wake_mark_gives_waking(store: Store, clock: Clock) -> None:
    start(store)
    beat(store, "laptop", wakes=True)
    assert store.wake(KEY, 1) == "waking"
    # The mark of another host does not count.
    other = SessionKey("acct", "claude-code", "s2")
    start(store, other, Writer("acct", "agent-1", "/home/agent/workspace"))
    assert store.wake(other, 1) == "queued"
    # A live session is `live`, also with the mark.
    beat(store, "laptop", KEY, wakes=True)
    assert store.wake(KEY, 1) == "live"


def test_wake_mark_expires(store: Store, clock: Clock) -> None:
    start(store)
    beat(store, "laptop", interval=60, wakes=True)
    clock.t += timedelta(seconds=170)
    assert store.wake(KEY, 1) == "waking"
    clock.t += timedelta(seconds=20)
    assert store.wake(KEY, 1) == "queued"


def test_hook_heartbeat_keeps_the_mark(store: Store, clock: Clock) -> None:
    start(store)
    beat(store, "laptop", interval=60, wakes=True)
    # A hook heartbeat of the same source, with a short interval. The mark
    # keeps the time and the interval of the launcher heartbeat.
    beat(store, "laptop", interval=10)
    clock.t += timedelta(seconds=100)
    assert store.wake(KEY, 1) == "waking"
    # The mark of any source of the lease host counts.
    beat(store, "laptop", interval=10)
    beat(store, "laptop", interval=60, source="other", wakes=True)
    clock.t += timedelta(seconds=100)
    assert store.wake(KEY, 1) == "waking"


def test_final_heartbeat_clears_the_mark(store: Store) -> None:
    start(store)
    beat(store, "laptop", wakes=True)
    beat(store, "laptop", final=True, wakes=True)
    assert store.wake(KEY, 1) == "queued"


def test_list_filter_wake_pending(store: Store) -> None:
    keys = [SessionKey("acct", "claude-code", f"s{n}") for n in range(1, 5)]
    for key in keys:
        start(store, key)
    store.wake(keys[0], 1)
    store.wake(keys[1], 1)
    store.wake(keys[2], 1)
    set_status(store, "done", 0, key=keys[1])
    beat(store, "laptop", keys[2])

    def ids(**kwargs: object) -> list[str]:
        return sorted(
            s["session_id"]
            for s in store.list_sessions("acct", "laptop", "all", **kwargs)  # type: ignore[arg-type]
        )

    assert ids(wake_pending=True) == ["s1"]
    assert ids() == ["s1", "s2", "s3", "s4"]
    listed = store.list_sessions("acct", "laptop", "all", wake_pending=True)
    assert listed[0]["state"]["wake_request"]["count"] == 1


def test_wake_does_not_make_a_restore_stale(store: Store) -> None:
    start(store)
    revision = store.manifest(KEY)["revision"]
    store.wake(KEY, 1)
    assert store.manifest(KEY)["revision"] == revision


def test_purge_removes_the_wake_request(store: Store) -> None:
    start(store)
    store.wake(KEY, 1)
    store.purge_session(KEY, "test")
    with store._read():
        assert store._db.execute("SELECT * FROM wake_requests").fetchall() == []
    start(store)
    assert not pending(store)


def test_migration_copies_the_run_start(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    for migration in _MIGRATIONS[:-1]:
        db.executescript(migration)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (?)", (len(_MIGRATIONS) - 1,))
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'acct', 'claude-code', 's1', 't1')"
    )
    db.execute("INSERT INTO session_liveness VALUES (1, 'laptop', '', 't2', 't3')")
    db.execute(
        "INSERT INTO host_heartbeats VALUES ('acct', 'laptop', '', 't3', 60, 0, '[]')"
    )
    db.commit()
    db.close()
    with closing(Store(data)) as migrated:
        with migrated._read():
            row = migrated._db.execute("SELECT * FROM session_liveness").fetchone()
            beat_row = migrated._db.execute("SELECT * FROM host_heartbeats").fetchone()
        assert row["run_started"] == "t2"
        assert beat_row["wakes_received"] is None
