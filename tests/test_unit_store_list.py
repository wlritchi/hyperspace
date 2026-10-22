# vim: filetype=python
"""The session and workspace lists: SQL filters, the copies of the latest
status and metadata in the session row, and the migration that fills them."""

import hashlib
import json
import random
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hyperspace_protocol import ReadScope, SessionSummary, Status, StatusRequest
from store import _MIGRATIONS, SessionKey, Store, Writer

HARNESS = "claude-code"
WRITERS = [
    Writer("acct", "laptop", "/home/user/project"),
    Writer("acct", "laptop", "/home/user/other"),
    Writer("acct", "agent-1", "/home/agent/workspace"),
    Writer("acct", "agent-2", "/home/agent/workspace"),
]
HOSTS = ["laptop", "agent-1", "agent-2", "agent-3"]
STATUSES: list[Status] = ["working", "waiting", "ready", "done"]


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


def key(n: int) -> SessionKey:
    return SessionKey("acct", HARNESS if n % 7 else "codex", f"s{n:03d}")


def append(store: Store, k: SessionKey, writer: Writer, data: bytes) -> None:
    manifest = store.manifest(k) if store.session_exists(k) else None
    logs = [f for f in (manifest or {"files": []})["files"] if f["kind"] == "log"]
    if logs:
        gen = logs[0]["active"]  # type: ignore[typeddict-item]
        base = next(
            g["length"]
            for g in logs[0]["generations"]  # type: ignore[typeddict-item]
            if g["id"] == gen
        )
    else:
        gen = store.create_generation(k, "transcript.jsonl", writer, "root")["id"]
        base = 0
    store.append(k, gen, writer, base, data, hashlib.sha256(data).hexdigest(), 1)


def status(store: Store, k: SessionKey, value: Status, host: str) -> None:
    seq = store.session_state(k).get("status", {"seq": 0})["seq"]
    request: StatusRequest = {
        "status": value,
        "source_kind": "hook",
        "explicit": False,
        "base_seq": seq,
        "excerpt": f"excerpt of {k.session_id}",
    }
    store.add_status(k, host, request)


def build(store: Store, seed: int, count: int = 40) -> None:
    rng = random.Random(seed)
    for n in range(count):
        k = key(n)
        first = rng.choice(WRITERS)
        kind = rng.random()
        if kind < 0.1:
            # Only metadata: the author can read the session.
            store.add_metadata(k, first, {"title": f"Meta only {n}"})
        elif kind < 0.2:
            data = b"blob"
            store.put_blob(
                k, "tool-results/x", first, data, hashlib.sha256(data).hexdigest()
            )
        else:
            append(store, k, first, b"one\n")
            if rng.random() < 0.3:
                append(store, k, rng.choice(WRITERS), b"two\n")
        for _ in range(rng.randint(0, 3)):
            title: Any = rng.choice([f"Fix the list {n}", f"Ünïcode TITLE {n}", None])
            metadata: dict[str, Any] = {"workspace": first.workspace}
            if title is not None:
                metadata["title"] = title
            store.add_metadata(k, rng.choice(WRITERS), metadata)  # type: ignore[arg-type]
        if store.session_exists(k):
            for _ in range(rng.randint(0, 3)):
                status(store, k, rng.choice(STATUSES), first.host)
            if rng.random() < 0.2:
                store.grant(k, "agent-2", first.host)
            if rng.random() < 0.15 and store.session_state(k)["lease"].get("host"):
                holder = store.session_state(k)["lease"]["host"]  # type: ignore[typeddict-item]
                store.surrender(k, holder, rng.choice([None, "agent-3"]))
    for host in ("laptop", "agent-1"):
        listed = [key(n) for n in range(count) if rng.random() < 0.2]
        listed = [k for k in listed if store.session_exists(k)]
        store.heartbeat(
            "acct",
            host,
            "all",
            {
                "interval": 60,
                "sessions": [
                    {"harness": k.harness, "session_id": k.session_id} for k in listed
                ],
            },
        )


def reference_can_read(
    store: Store, row: sqlite3.Row, host: str, scope: ReadScope
) -> bool:
    """Read access as the rules in the spec state it, one query at a time."""
    if scope == "all":
        return True
    if row["lease_state"] == "held" and row["lease_host"] == host:
        return True
    checks = [
        "SELECT 1 FROM placements WHERE session = ? AND host = ?",
        (
            "SELECT 1 FROM generations g JOIN files f ON f.id = g.file "
            "WHERE f.session = ? AND g.host = ?"
        ),
        "SELECT 1 FROM metadata_events WHERE session = ? AND host = ?",
        (
            "SELECT 1 FROM blob_versions b JOIN files f ON f.id = b.file "
            "WHERE f.session = ? AND b.host = ?"
        ),
    ]
    if scope == "granted":
        checks.append("SELECT 1 FROM grants WHERE session = ? AND host = ?")
    return any(
        store._db.execute(sql, (row["id"], host)).fetchone() is not None
        for sql in checks
    )


def reference_list(
    store: Store,
    host: str,
    scope: ReadScope,
    harness: str | None = None,
    on_host: str | None = None,
    workspace: str | None = None,
    since: str | None = None,
    title: str | None = None,
    statuses: set[str] | None = None,
    open_only: bool = False,
    lease_host: str | None = None,
    liveness: set[str] | None = None,
    limit: int = 200,
) -> list[SessionSummary]:
    """The list as the store made it before the SQL filters: every session,
    with all its data from the event tables, filtered in Python."""
    rows = store._db.execute(
        "SELECT * FROM sessions WHERE account = 'acct' "
        "ORDER BY COALESCE(last_activity, created) DESC, id DESC"
    ).fetchall()
    out: list[SessionSummary] = []
    for row in rows:
        if harness is not None and row["harness"] != harness:
            continue
        if since is not None and (row["last_activity"] or row["created"]) < since:
            continue
        if not reference_can_read(store, row, host, scope):
            continue
        if lease_host is not None and (
            row["lease_state"] != "held" or row["lease_host"] != lease_host
        ):
            continue
        state = store._session_state(row["id"])
        latest = store._latest_status(row["id"])
        name = latest[0]["status"] if latest is not None else "none"
        if statuses is not None and name not in statuses:
            continue
        if open_only and name == "done":
            continue
        live = state.get("liveness")
        if liveness is not None and (live["state"] if live else "none") not in liveness:
            continue
        placements = store._placements(row["id"])
        if on_host is not None and not any(p["host"] == on_host for p in placements):
            continue
        if workspace is not None and not any(
            p["workspace"] == workspace for p in placements
        ):
            continue
        metadata = store._latest_metadata(row["id"])
        if (
            title is not None
            and title.lower() not in (metadata.get("title") or "").lower()
        ):
            continue
        summary: SessionSummary = {
            "harness": row["harness"],
            "session_id": row["session_id"],
            "metadata": metadata,
            "state": state,
        }
        if placements:
            summary["last_placement"] = placements[-1]
        if latest is not None and latest[1] is not None:
            summary["excerpt"] = latest[1]
        out.append(summary)
        if len(out) >= limit:
            break
    return out


def reference_workspaces(
    store: Store, host: str, scope: ReadScope
) -> list[dict[str, Any]]:
    found: dict[tuple[str, str], dict[str, Any]] = {}
    for row in store._db.execute("SELECT * FROM sessions WHERE account = 'acct'"):
        if not reference_can_read(store, row, host, scope):
            continue
        seen: set[tuple[str, str]] = set()
        for p in store._placements(row["id"]):
            place = (p["host"], p["workspace"])
            entry = found.setdefault(
                place,
                {
                    "host": p["host"],
                    "workspace": p["workspace"],
                    "last_seen": "",
                    "sessions": 0,
                },
            )
            if place not in seen:
                seen.add(place)
                entry["sessions"] += 1
            entry["last_seen"] = max(entry["last_seen"], p["last_seen"])
    return sorted(
        found.values(),
        key=lambda e: (e["last_seen"], e["host"], e["workspace"]),
        reverse=True,
    )


FILTERS: list[dict[str, Any]] = [
    {},
    {"limit": 3},
    {"harness": "codex"},
    {"since": "2026-10-13T00:01:00.000000Z"},
    {"on_host": "agent-1"},
    {"on_host": "agent-1", "limit": 2},
    {"workspace": "/home/user/other"},
    {"title": "fix THE"},
    {"title": "ünïcode", "limit": 2},
    {"statuses": {"done"}},
    {"statuses": {"working", "waiting", "ready"}},
    {"statuses": {"none"}},
    {"statuses": {"none", "ready"}, "limit": 4},
    {"open_only": True},
    {"open_only": True, "statuses": {"done", "ready"}},
    {"lease_host": "laptop"},
    {"lease_host": "agent-2", "open_only": True},
    {"liveness": {"live"}},
    {"liveness": {"none"}, "limit": 5},
    {"on_host": "laptop", "title": "list", "statuses": {"ready", "none"}},
]


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_lists_match_the_reference(store: Store, seed: int) -> None:
    build(store, seed)
    # Liveness depends on the time. Stop the clock, so that the two lists
    # see the same time.
    now = store._clock()
    store._clock = lambda: now
    for host in HOSTS:
        for scope in ("all", "own", "granted"):
            for filters in FILTERS:
                expected = reference_list(store, host, scope, **filters)  # type: ignore[arg-type]
                got = store.list_sessions("acct", host, scope, **filters)  # type: ignore[arg-type]
                assert got == expected, (host, scope, filters)
            assert store.list_workspaces("acct", host, scope) == reference_workspaces(
                store, host, scope
            ), (host, scope)
            for n in range(40):
                row = store._session_row(key(n))
                if row is not None:
                    assert store._can_read_row(row, host, scope) == reference_can_read(
                        store, row, host, scope
                    ), (host, scope, n)


def test_status_and_metadata_copies_follow_their_events(store: Store) -> None:
    k = key(1)
    writer = WRITERS[0]
    store.add_metadata(k, writer, {"title": "first"})
    status(store, k, "working", "laptop")
    status(store, k, "done", "laptop")
    store.add_metadata(k, writer, {"workspace": "/w"})
    row = store._session_row(k)
    assert row is not None
    assert row["status"] == "done"
    assert json.loads(row["metadata"]) == {"workspace": "/w"}
    [summary] = store.list_sessions("acct", "laptop", "all")
    assert summary["metadata"] == {"workspace": "/w"}
    assert summary["state"]["status"]["status"] == "done"  # type: ignore[typeddict-item]


def test_ties_in_activity_list_the_newer_row_first(store: Store) -> None:
    for n in (1, 2, 3):
        store.add_metadata(key(n), WRITERS[0], {"title": str(n)})
    with store._tx():
        store._db.execute("UPDATE sessions SET last_activity = 'same'")
    assert [s["session_id"] for s in store.list_sessions("acct", "laptop", "all")] == [
        "s003",
        "s002",
        "s001",
    ]


def test_a_session_that_comes_back_to_a_workspace_counts_once(store: Store) -> None:
    home, away = WRITERS[0], WRITERS[1]
    append(store, key(1), home, b"one\n")
    append(store, key(1), away, b"two\n")
    append(store, key(1), home, b"three\n")
    append(store, key(2), home, b"one\n")
    assert len(store._placements(store._session_row(key(1))["id"])) == 3  # type: ignore[index]
    counts = {
        e["workspace"]: e["sessions"]
        for e in store.list_workspaces("acct", "laptop", "all")
    }
    assert counts == {home.workspace: 2, away.workspace: 1}


def test_an_empty_status_set_matches_nothing(store: Store) -> None:
    store.add_metadata(key(1), WRITERS[0], {"title": "x"})
    assert store.list_sessions("acct", "laptop", "all", statuses=set()) == []


def test_migration_4_copies_the_latest_events(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    for migration in _MIGRATIONS[:3]:
        db.executescript(migration)
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (3)")
    for n in (1, 2, 3):
        db.execute(
            "INSERT INTO sessions (id, account, harness, session_id, created) "
            "VALUES (?, 'acct', 'claude-code', ?, ?)",
            (n, f"s{n}", f"2026-10-1{n}T00:00:00.000000Z"),
        )
    for seq, value in ((1, "working"), (3, "done"), (2, "ready")):
        db.execute(
            "INSERT INTO status_events (session, seq, status, source_kind, "
            "source_host, explicit, received) VALUES (1, ?, ?, 'hook', 'laptop', 0, 't')",
            (seq, value),
        )
    # An old server accepted a title that is not a string.
    for body in ({"title": "old"}, {"title": 7, "cwd": "/w"}):
        db.execute(
            "INSERT INTO metadata_events (session, host, received, body) "
            "VALUES (1, 'laptop', 't', ?)",
            (json.dumps(body),),
        )
    db.execute(
        "INSERT INTO metadata_events (session, host, received, body) "
        "VALUES (2, 'laptop', 't', ?)",
        (json.dumps({"title": "two"}),),
    )
    db.commit()
    db.close()
    with closing(Store(data)) as migrated:
        rows = {
            r["session_id"]: (r["status"], r["metadata"])
            for r in migrated._db.execute("SELECT * FROM sessions")
        }
        assert rows == {
            "s1": ("done", json.dumps({"title": 7, "cwd": "/w"})),
            "s2": (None, json.dumps({"title": "two"})),
            "s3": (None, None),
        }
        listed = {
            s["session_id"]: s["metadata"]
            for s in migrated.list_sessions("acct", "laptop", "all")
        }
        assert listed == {"s1": {"cwd": "/w"}, "s2": {"title": "two"}, "s3": {}}
        done = migrated.list_sessions("acct", "laptop", "all", statuses={"done"})
        assert [s["session_id"] for s in done] == ["s1"]
        names = {
            r["name"]
            for r in migrated._db.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index'"
            )
        }
        assert {"sessions_recent", "placements_host", "placements_workspace"} <= names
