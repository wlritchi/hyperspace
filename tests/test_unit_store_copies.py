# vim: filetype=python
"""The copies of the latest status and metadata in the session rows after a
rollback: an older server writes events and does not update the copies, and
the next start of this server repairs them."""

import json
import logging
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from hyperspace_protocol import StatusRequest
from store import SessionKey, Store, Writer

LAPTOP = Writer("acct", "laptop", "/home/user/project")


def key(sid: str) -> SessionKey:
    return SessionKey("acct", "claude-code", sid)


def status(store: Store, sid: str, value: str) -> None:
    seq = store.session_state(key(sid)).get("status", {"seq": 0})["seq"]
    request: StatusRequest = {
        "status": value,  # type: ignore[typeddict-item]
        "source_kind": "hook",
        "explicit": False,
        "base_seq": seq,
    }
    store.add_status(key(sid), "laptop", request)


def ids(store: Store, **filters: object) -> list[str]:
    return sorted(
        s["session_id"]
        for s in store.list_sessions("acct", "laptop", "all", **filters)  # type: ignore[arg-type]
    )


def old_server(data: Path) -> sqlite3.Connection:
    """The index, as an older server writes it: with no copy updates."""
    return sqlite3.connect(data / "index.sqlite3", isolation_level=None)


def session_id(db: sqlite3.Connection, sid: str) -> int:
    return db.execute(
        "SELECT id FROM sessions WHERE session_id = ?", (sid,)
    ).fetchone()[0]


def old_status(db: sqlite3.Connection, sid: str, seq: int, value: str) -> None:
    db.execute(
        "INSERT INTO status_events (session, seq, status, source_kind, "
        "source_host, explicit, received) VALUES (?, ?, ?, 'hook', 'laptop', 0, ?)",
        (session_id(db, sid), seq, value, f"2026-10-23T00:00:0{seq}.000000Z"),
    )


def old_metadata(db: sqlite3.Connection, sid: str, body: dict[str, object]) -> None:
    db.execute(
        "INSERT INTO metadata_events (session, host, received, body) "
        "VALUES (?, 'laptop', '2026-10-23T00:00:00.000000Z', ?)",
        (session_id(db, sid), json.dumps(body)),
    )


def test_a_restart_repairs_copies_after_a_rollback(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    data = tmp_path / "data"
    with closing(Store(data)) as store:
        store.add_metadata(key("s1"), LAPTOP, {"title": "one"})
        status(store, "s1", "working")
        store.add_metadata(key("s2"), LAPTOP, {"title": "two"})
        store.add_metadata(key("s3"), LAPTOP, {"title": "three"})
    # The rollback: an older server writes events.
    with closing(old_server(data)) as db:
        old_status(db, "s1", 2, "done")
        old_metadata(
            db, "s2", {"automation": "security-review", "links": {"parent": "s1"}}
        )
        db.execute(
            "INSERT INTO sessions (account, harness, session_id, created) "
            "VALUES ('acct', 'claude-code', 's4', '2026-10-23T00:00:00.000000Z')"
        )
        old_metadata(db, "s4", {"links": {"parent": "s3"}})
    # The upgrade again.
    with caplog.at_level(logging.INFO, "hyperspace.store"):
        store = Store(data)
    with closing(store):
        assert ids(store, statuses={"done"}) == ["s1"]
        assert ids(store, top_level=True) == ["s1", "s3"]
        assert ids(store, parent="s1") == ["s2"]
        assert ids(store, parent="s3") == ["s4"]
        assert "metadata_events: new copies for 2 sessions" in caplog.text
        assert "status_events: new copies for 1 sessions" in caplog.text
    # A start with nothing new changes nothing.
    caplog.clear()
    with (
        caplog.at_level(logging.INFO, "hyperspace.store"),
        closing(Store(data)) as store,
    ):
        assert ids(store, top_level=True) == ["s1", "s3"]
    assert "new copies" not in caplog.text


def test_a_mark_whose_event_changed_repairs_all(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    data = tmp_path / "data"
    with closing(Store(data)) as store:
        store.add_metadata(key("s1"), LAPTOP, {"title": "one"})
        store.add_metadata(key("s2"), LAPTOP, {"title": "two"})
    # An older server deletes the newest event, and SQLite gives its id to
    # the next event.
    with closing(old_server(data)) as db:
        newest = db.execute("SELECT MAX(id) FROM metadata_events").fetchone()[0]
        db.execute("DELETE FROM metadata_events WHERE id = ?", (newest,))
        old_metadata(db, "s1", {"automation": "security-review"})
        assert db.execute("SELECT MAX(id) FROM metadata_events").fetchone()[0] == newest
    with caplog.at_level(logging.INFO, "hyperspace.store"):
        store = Store(data)
    with closing(store):
        assert ids(store, top_level=True) == ["s2"]
        assert "the mark is not valid" in caplog.text
        assert (
            store.list_sessions("acct", "laptop", "all", top_level=True)[0]["metadata"]
            == {}
        )


def test_a_purge_keeps_the_mark_valid(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    data = tmp_path / "data"
    with closing(Store(data)) as store:
        store.add_metadata(key("s1"), LAPTOP, {"title": "one"})
        status(store, "s1", "working")
        store.add_metadata(key("s2"), LAPTOP, {"title": "two"})
        status(store, "s2", "working")
        # The purge deletes the newest events.
        store.purge_session(key("s2"), "test")
    with (
        caplog.at_level(logging.INFO, "hyperspace.store"),
        closing(Store(data)) as store,
    ):
        assert ids(store) == ["s1"]
    assert "new copies" not in caplog.text


def test_a_fresh_index_has_valid_marks(tmp_path: Path) -> None:
    data = tmp_path / "data"
    with closing(Store(data)) as store:
        marks = {
            r["events"]: (r["last_id"], r["last_key"])
            for r in store._db.execute("SELECT * FROM copy_marks")  # pyright: ignore[reportPrivateUsage]
        }
        assert marks == {"status_events": (0, ""), "metadata_events": (0, "")}
        store.add_metadata(key("s1"), LAPTOP, {"title": "one"})
        row = store._db.execute(  # pyright: ignore[reportPrivateUsage]
            "SELECT last_id FROM copy_marks WHERE events = 'metadata_events'"
        ).fetchone()
        assert row["last_id"] == 1
