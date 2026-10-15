"""The client's local state: `$XDG_STATE_HOME/hyperspace/state.sqlite3`.

Many short-lived processes (one for each hook) use this file at the same
time, so it uses WAL mode and a busy timeout. A file lock for each session
makes sure that only one process syncs a session at a time.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cursors (
    local_path TEXT PRIMARY KEY,
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    generation TEXT NOT NULL,
    length INTEGER NOT NULL,
    dev INTEGER NOT NULL,
    ino INTEGER NOT NULL,
    segments TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS session_blobs (
    server TEXT NOT NULL,
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    local_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    PRIMARY KEY (server, harness, session_id, rel_path, local_path)
);
CREATE TABLE IF NOT EXISTS sessions (
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    workspace TEXT,
    transcript TEXT,
    lease_state TEXT,
    lease_host TEXT,
    state TEXT,
    state_updated REAL,
    metadata TEXT NOT NULL DEFAULT '{}',
    sent_metadata TEXT NOT NULL DEFAULT '{}',
    last_placement TEXT,
    PRIMARY KEY (harness, session_id)
);
CREATE TABLE IF NOT EXISTS dirty (
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    marked REAL NOT NULL,
    PRIMARY KEY (harness, session_id)
);
CREATE TABLE IF NOT EXISTS arrivals (
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    body TEXT NOT NULL,
    created REAL NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS notices (
    id INTEGER PRIMARY KEY,
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    text TEXT NOT NULL,
    created REAL NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


@dataclass
class Segment:
    offset: int
    length: int
    sha256: str


@dataclass
class Cursor:
    local_path: str
    harness: str
    session_id: str
    rel_path: str
    generation: str
    length: int
    dev: int
    ino: int
    segments: list[Segment] = field(default_factory=list[Segment])
    # The file's size and modification time when the client last read
    # it to the end. If they and the inode are the same, nothing changed.
    size: int = -1
    mtime_ns: int = -1


@dataclass
class SessionRecord:
    harness: str
    session_id: str
    workspace: str | None = None
    transcript: str | None = None
    lease_state: str | None = None
    lease_host: str | None = None
    state: str | None = None
    state_updated: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    sent_metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    last_placement: str | None = None
    # True after the client saw this host hold the lease. Only a host
    # that held the lease and lost it moves its local copy aside.
    held_lease: bool = False


class State:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._db = sqlite3.connect(directory / "state.sqlite3", timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA busy_timeout = 30000")
        # In WAL mode, NORMAL keeps the file consistent after a crash. A
        # power loss can lose the last cursor saves; the next sync then
        # adopts the server copy.
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._db.executescript(_SCHEMA)
        columns = {r[1] for r in self._db.execute("PRAGMA table_info(cursors)")}
        for column in ("size", "mtime_ns"):
            if column not in columns:
                self._db.execute(
                    f"ALTER TABLE cursors ADD COLUMN {column} INTEGER NOT NULL DEFAULT -1"
                )
        columns = {r[1] for r in self._db.execute("PRAGMA table_info(sessions)")}
        if "held_lease" not in columns:
            self._db.execute(
                "ALTER TABLE sessions ADD COLUMN held_lease INTEGER NOT NULL DEFAULT 0"
            )
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    # --- Locks ---------------------------------------------------------------

    @contextmanager
    def try_lock(self, harness: str, session_id: str) -> Iterator[bool]:
        """Hold the session's sync lock for the `with` body. The value is
        False if another process holds it; then nothing is locked."""
        locks = self.directory / "locks"
        locks.mkdir(exist_ok=True)
        fd = os.open(locks / f"{harness}--{session_id}.lock", os.O_RDWR | os.O_CREAT)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    # --- Cursors -------------------------------------------------------------

    def cursor(self, local_path: Path) -> Cursor | None:
        row = self._db.execute(
            "SELECT * FROM cursors WHERE local_path = ?", (str(local_path),)
        ).fetchone()
        if row is None:
            return None
        return Cursor(
            local_path=row["local_path"],
            harness=row["harness"],
            session_id=row["session_id"],
            rel_path=row["rel_path"],
            generation=row["generation"],
            length=row["length"],
            dev=row["dev"],
            ino=row["ino"],
            segments=[Segment(*s) for s in json.loads(row["segments"])],
            size=row["size"],
            mtime_ns=row["mtime_ns"],
        )

    def save_cursor(self, cursor: Cursor) -> None:
        segments = json.dumps([[s.offset, s.length, s.sha256] for s in cursor.segments])
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO cursors (local_path, harness, session_id, "
                "rel_path, generation, length, dev, ino, segments, size, mtime_ns) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cursor.local_path,
                    cursor.harness,
                    cursor.session_id,
                    cursor.rel_path,
                    cursor.generation,
                    cursor.length,
                    cursor.dev,
                    cursor.ino,
                    segments,
                    cursor.size,
                    cursor.mtime_ns,
                ),
            )

    def forget_cursor(self, local_path: Path) -> None:
        with self._db:
            self._db.execute(
                "DELETE FROM cursors WHERE local_path = ?", (str(local_path),)
            )

    def move_cursor(self, old: Path, new: Path) -> None:
        with self._db:
            self._db.execute(
                "UPDATE cursors SET local_path = ? WHERE local_path = ?",
                (str(new), str(old)),
            )

    # --- Blobs ---------------------------------------------------------------

    def blob(
        self, local_path: Path, server: str, harness: str, sid: str, rel_path: str
    ) -> tuple[int, int, str] | None:
        row = self._db.execute(
            "SELECT size, mtime_ns, sha256 FROM session_blobs WHERE "
            "server = ? AND harness = ? AND session_id = ? AND rel_path = ? AND local_path = ?",
            (server, harness, sid, rel_path, str(local_path)),
        ).fetchone()
        return (row[0], row[1], row[2]) if row else None

    def save_blob(
        self,
        local_path: Path,
        size: int,
        mtime_ns: int,
        sha256: str,
        server: str,
        harness: str,
        sid: str,
        rel_path: str,
    ) -> None:
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO session_blobs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    server,
                    harness,
                    sid,
                    rel_path,
                    str(local_path),
                    size,
                    mtime_ns,
                    sha256,
                ),
            )

    # --- Sessions ------------------------------------------------------------

    def session(self, harness: str, session_id: str) -> SessionRecord:
        row = self._db.execute(
            "SELECT * FROM sessions WHERE harness = ? AND session_id = ?",
            (harness, session_id),
        ).fetchone()
        if row is None:
            return SessionRecord(harness, session_id)
        return SessionRecord(
            harness=harness,
            session_id=session_id,
            workspace=row["workspace"],
            transcript=row["transcript"],
            lease_state=row["lease_state"],
            lease_host=row["lease_host"],
            state=row["state"],
            state_updated=row["state_updated"],
            metadata=json.loads(row["metadata"]),
            sent_metadata=json.loads(row["sent_metadata"]),
            last_placement=row["last_placement"],
            held_lease=bool(row["held_lease"]),
        )

    def save_session(self, record: SessionRecord) -> None:
        if record == self.session(record.harness, record.session_id):
            return
        with self._db:
            self._db.execute(
                """
                INSERT OR REPLACE INTO sessions
                (harness, session_id, workspace, transcript, lease_state,
                 lease_host, state, state_updated, metadata, sent_metadata,
                 last_placement, held_lease)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.harness,
                    record.session_id,
                    record.workspace,
                    record.transcript,
                    record.lease_state,
                    record.lease_host,
                    record.state,
                    record.state_updated,
                    json.dumps(record.metadata),
                    json.dumps(record.sent_metadata),
                    record.last_placement,
                    int(record.held_lease),
                ),
            )

    def known_sessions(self) -> list[SessionRecord]:
        rows = self._db.execute("SELECT harness, session_id FROM sessions").fetchall()
        return [self.session(r[0], r[1]) for r in rows]

    # --- Dirty marks -----------------------------------------------------------

    def mark_dirty(self, harness: str, session_id: str) -> float:
        now = time.time()
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO dirty VALUES (?, ?, ?)",
                (harness, session_id, now),
            )
        return now

    def dirty_since(self, harness: str, session_id: str) -> float | None:
        row = self._db.execute(
            "SELECT marked FROM dirty WHERE harness = ? AND session_id = ?",
            (harness, session_id),
        ).fetchone()
        return float(row[0]) if row else None

    def clear_dirty(self, harness: str, session_id: str, upto: float) -> None:
        """Clear the mark if it is not newer than `upto`."""
        with self._db:
            self._db.execute(
                "DELETE FROM dirty WHERE harness = ? AND session_id = ? AND marked <= ?",
                (harness, session_id, upto),
            )

    # --- Arrivals and notices ----------------------------------------------------

    def add_arrival(self, harness: str, session_id: str, body: dict[str, Any]) -> None:
        with self._db:
            self._db.execute(
                "INSERT INTO arrivals (harness, session_id, body, created) "
                "VALUES (?, ?, ?, ?)",
                (harness, session_id, json.dumps(body), time.time()),
            )

    def take_arrival(self, harness: str, session_id: str) -> dict[str, Any] | None:
        with self._db:
            row = self._db.execute(
                "SELECT rowid, body FROM arrivals WHERE harness = ? AND session_id = ? "
                "AND delivered = 0 ORDER BY created DESC LIMIT 1",
                (harness, session_id),
            ).fetchone()
            if row is None:
                return None
            self._db.execute(
                "UPDATE arrivals SET delivered = 1 WHERE harness = ? AND session_id = ?",
                (harness, session_id),
            )
        body: dict[str, Any] = json.loads(row[1])
        return body

    def add_notice(self, harness: str, session_id: str, kind: str, text: str) -> None:
        with self._db:
            exists = self._db.execute(
                "SELECT 1 FROM notices WHERE harness = ? AND session_id = ? "
                "AND kind = ? AND text = ?",
                (harness, session_id, kind, text),
            ).fetchone()
            if exists is None:
                self._db.execute(
                    "INSERT INTO notices (harness, session_id, kind, text, created) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (harness, session_id, kind, text, time.time()),
                )

    def take_notices(self, harness: str, session_id: str) -> list[str]:
        with self._db:
            rows = self._db.execute(
                "SELECT id, text FROM notices WHERE harness = ? AND session_id = ? "
                "AND delivered = 0 ORDER BY id",
                (harness, session_id),
            ).fetchall()
            for r in rows:
                self._db.execute(
                    "UPDATE notices SET delivered = 1 WHERE id = ?", (r[0],)
                )
        return [str(r[1]) for r in rows]

    # --- Key-value ---------------------------------------------------------------

    def get(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return str(row[0]) if row else None

    def put(self, key: str, value: str) -> None:
        with self._db:
            self._db.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (key, value))
