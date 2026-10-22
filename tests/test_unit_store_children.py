# vim: filetype=python
"""Child sessions in the index: the copies of `links.parent` and of the
top-level state in the session row, the list filters, and the migration."""

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from store import _MIGRATIONS, _RECENT, _SORT_KEY, SessionKey, Store, Writer

HARNESS = "claude-code"
LAPTOP = Writer("acct", "laptop", "/home/user/project")


class Clock:
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


def key(sid: str) -> SessionKey:
    return SessionKey("acct", HARNESS, sid)


def ids(store: Store, **filters: object) -> list[str]:
    return [
        s["session_id"]
        for s in store.list_sessions("acct", "laptop", "all", **filters)  # type: ignore[arg-type]
    ]


def test_list_filters_children(store: Store) -> None:
    store.add_metadata(key("parent"), LAPTOP, {"title": "work"})
    store.add_metadata(
        key("review-1"),
        LAPTOP,
        {"automation": "security-review", "links": {"parent": "parent"}},
    )
    store.add_metadata(key("review-2"), LAPTOP, {"automation": "security-review"})
    store.add_metadata(key("child"), LAPTOP, {"links": {"parent": "parent"}})
    store.add_metadata(key("fork"), LAPTOP, {"links": {"forked_from": "parent"}})
    # Without the filters, the list has every session, as for old clients.
    assert ids(store) == ["fork", "child", "review-2", "review-1", "parent"]
    assert ids(store, top_level=True) == ["fork", "parent"]
    assert ids(store, parent="parent") == ["child", "review-1"]
    assert ids(store, parent="parent", top_level=True) == []
    assert ids(store, parent="nobody") == []


def test_new_metadata_changes_the_copies(store: Store) -> None:
    store.add_metadata(key("s1"), LAPTOP, {"links": {"parent": "p1"}})
    assert ids(store, parent="p1") == ["s1"]
    store.add_metadata(key("s1"), LAPTOP, {"links": {"parent": "p2"}})
    assert ids(store, parent="p1") == []
    assert ids(store, parent="p2") == ["s1"]
    assert ids(store, top_level=True) == []
    store.add_metadata(key("s1"), LAPTOP, {"title": "top-level again"})
    assert ids(store, top_level=True) == ["s1"]
    assert ids(store, parent="p2") == []


def test_children_filters_use_indexes(store: Store) -> None:
    # The query of `list_sessions_page`.
    def plan(where: str) -> str:
        rows = store._db.execute(  # pyright: ignore[reportPrivateUsage]
            f"EXPLAIN QUERY PLAN SELECT s.*, {_SORT_KEY} AS sort_key FROM sessions s "
            f"WHERE s.account = :account AND {where} ORDER BY {_RECENT} "
            "LIMIT :limit + 1",
            {"account": "acct", "parent": "p", "limit": 50},
        ).fetchall()
        return " ".join(str(r["detail"]) for r in rows)

    assert "sessions_parent" in plan("s.parent = :parent")
    assert "sessions_recent" in plan("s.top_level = 1")


def test_migration_5_fills_the_copies(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    for migration in _MIGRATIONS[:4]:
        db.executescript(migration)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (4)")
    bodies = {
        "plain": {"title": "x"},
        "child": {"links": {"parent": "plain", "forked_from": "other"}},
        "review": {"automation": "security-review"},
        # An old server accepted links that are not strings.
        "bad-links": {"links": {"parent": "plain", "n": 7}},
        "bad-automation": {"automation": 7},
        "none": None,
    }
    for n, (sid, body) in enumerate(bodies.items(), start=1):
        db.execute(
            "INSERT INTO sessions (id, account, harness, session_id, created, "
            "metadata) VALUES (?, 'acct', 'claude-code', ?, ?, ?)",
            (
                n,
                sid,
                f"2026-10-1{n}T00:00:00.000000Z",
                None if body is None else json.dumps(body),
            ),
        )
    db.commit()
    db.close()
    with closing(Store(data)) as migrated:
        rows = {
            r["session_id"]: (r["parent"], r["top_level"])
            for r in migrated._db.execute("SELECT * FROM sessions")  # pyright: ignore[reportPrivateUsage]
        }
        assert rows == {
            "plain": (None, 1),
            "child": ("plain", 0),
            "review": (None, 0),
            "bad-links": (None, 1),
            "bad-automation": (None, 1),
            "none": (None, 1),
        }
        assert ids(migrated, top_level=True) == [
            "none",
            "bad-automation",
            "bad-links",
            "plain",
        ]
        assert ids(migrated, parent="plain") == ["child"]
