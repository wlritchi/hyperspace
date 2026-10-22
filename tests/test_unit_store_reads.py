# vim: filetype=python
"""Reads on the read connections: they do not stop writes, they see only
committed data, and they cannot write."""

import sqlite3
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app import Settings
from store import SessionKey, Store, Writer, track_waits

KEY = SessionKey("acct", "claude-code", "s1")
LAPTOP = Writer("acct", "laptop", "/home/wlritchi/hyperspace")
# The maximum time in seconds for a step that must not block.
WAIT = 10
# The maximum time that a thread holds a transaction open. It is longer than
# WAIT, so that a step that blocks fails before the hold ends.
HOLD = 3 * WAIT


@pytest.fixture
def store(tmp_path: Path) -> Iterator[Store]:
    s = Store(tmp_path / "data")
    s.add_metadata(KEY, LAPTOP, {"title": "first"})
    yield s
    s.close()


def events(store: Store) -> int:
    return store._db.execute("SELECT COUNT(*) FROM metadata_events").fetchone()[0]


def title(store: Store) -> str:
    return store.list_sessions("acct", "laptop", "all")[0]["metadata"]["title"]


def test_a_slow_read_does_not_stop_a_write(store: Store) -> None:
    reading, release = threading.Event(), threading.Event()
    seen: list[int] = []

    def slow_read() -> None:
        seen.append(events(store))
        reading.set()
        release.wait(HOLD)
        seen.append(events(store))

    with ThreadPoolExecutor(2) as pool:
        reader = pool.submit(store.snapshot, slow_read)
        try:
            assert reading.wait(WAIT)
            write = pool.submit(store.add_metadata, KEY, LAPTOP, {"title": "second"})
            write.result(WAIT)
        finally:
            release.set()
        reader.result(WAIT)
    # The read kept its snapshot, and the next read sees the write.
    assert seen == [1, 1]
    assert store.snapshot(lambda: events(store)) == 2
    assert title(store) == "second"


def test_a_read_sees_only_committed_writes_and_does_not_wait(store: Store) -> None:
    written, commit = threading.Event(), threading.Event()

    def slow_write() -> None:
        with store._tx():
            store.add_metadata(KEY, LAPTOP, {"title": "second"})
            written.set()
            commit.wait(HOLD)

    with ThreadPoolExecutor(3) as pool:
        writer = pool.submit(slow_write)
        try:
            assert written.wait(WAIT)
            # The writer holds the store lock and the database write lock.
            count = pool.submit(store.snapshot, lambda: events(store))
            assert count.result(WAIT) == 1
            assert pool.submit(title, store).result(WAIT) == "first"
        finally:
            commit.set()
        writer.result(WAIT)
    assert store.snapshot(lambda: events(store)) == 2
    assert title(store) == "second"


def test_a_read_transaction_cannot_write(store: Store) -> None:
    with pytest.raises(RuntimeError, match="inside a read transaction"):
        store.snapshot(lambda: store.add_metadata(KEY, LAPTOP, {"title": "x"}))
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        store.snapshot(lambda: store._db.execute("DELETE FROM metadata_events"))
    assert store.snapshot(lambda: events(store)) == 1


def test_a_read_in_a_write_transaction_sees_its_writes(store: Store) -> None:
    with store._tx():
        store.add_metadata(KEY, LAPTOP, {"title": "second"})
        assert store.manifest(KEY)["metadata"].get("title") == "second"
        assert title(store) == "second"


def test_a_read_waits_for_a_free_read_connection(tmp_path: Path) -> None:
    store = Store(tmp_path / "data", read_connections=1)
    holding, release = threading.Event(), threading.Event()

    def hold() -> None:
        holding.set()
        release.wait(HOLD)

    try:
        with ThreadPoolExecutor(1) as pool:
            held = pool.submit(store.snapshot, hold)
            assert holding.wait(WAIT)
            timer = threading.Timer(0.3, release.set)
            timer.start()
            with track_waits() as waits:
                assert store.list_hosts("acct") == []
            timer.join()
            held.result(WAIT)
        assert waits.reader >= 0.2
        assert waits.writer == 0
    finally:
        release.set()
        store.close()


def test_a_write_counts_its_wait_for_the_writer(store: Store) -> None:
    holding = threading.Event()

    def hold() -> None:
        with store._tx():
            holding.set()
            time.sleep(0.3)

    with ThreadPoolExecutor(1) as pool:
        held = pool.submit(hold)
        assert holding.wait(WAIT)
        with track_waits() as waits:
            store.add_metadata(KEY, LAPTOP, {"title": "second"})
        held.result(WAIT)
    assert waits.writer >= 0.2
    assert waits.reader == 0


def test_page_cache_sizes(tmp_path: Path) -> None:
    store = Store(tmp_path / "data", read_cache_bytes=4 * 1024**2)
    try:
        writer = store._writer.execute("PRAGMA cache_size").fetchone()[0]
        reader = store.snapshot(
            lambda: store._db.execute("PRAGMA cache_size").fetchone()[0]
        )
        # Negative sizes are in KiB.
        assert (writer, reader) == (-128 * 1024, -4 * 1024)
    finally:
        store.close()


def test_read_settings_from_the_environment() -> None:
    settings = Settings.from_env(
        {
            "HYPERSPACE_DATA_DIR": "/data",
            "HYPERSPACE_READ_CONNECTIONS": "3",
            "HYPERSPACE_READ_CACHE_BYTES": "1048576",
            "HYPERSPACE_SLOW_REQUEST_SECONDS": "2.5",
        }
    )
    assert settings.read_connections == 3
    assert settings.read_cache_bytes == 1024**2
    assert settings.slow_request_seconds == 2.5
    defaults = Settings.from_env({"HYPERSPACE_DATA_DIR": "/data"})
    assert defaults.read_connections == 8
    assert defaults.read_cache_bytes == 16 * 1024**2
    assert defaults.slow_request_seconds == 1.0
