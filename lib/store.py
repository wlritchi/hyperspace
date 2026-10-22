# vim: filetype=python
"""The Hyperspace store: the SQLite index, generation files, and blobs.

The store implements the model in the spec: sessions, log files as trees of
generations, blob versions, placements, leases, conflicts, recovery forks,
grants, metadata events, and the read audit. It never parses file content.

All public methods are safe to call from more than one thread. Writes use
one connection behind one lock, so there is one writer at a time. Reads that
do not write use a pool of read connections and do not take that lock: in WAL
mode, each read transaction sees the last committed snapshot of the index
while the writer writes.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import secrets
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Concatenate

from framelog import FrameLog
from hyperspace_protocol import (
    MAX_BODY,
    BlobManifest,
    BlobVersion,
    Branch,
    ForkEvent,
    GenerationCause,
    GenerationInfo,
    HeartbeatRequest,
    HeartbeatResponse,
    HostInfo,
    HostRecord,
    Lease,
    Liveness,
    LogManifest,
    Manifest,
    Placement,
    PlacementCause,
    ReadScope,
    SessionMetadata,
    SessionStateBody,
    SessionSummary,
    StatusEvent,
    StatusRequest,
    TeleportEvent,
    UnclosedSession,
    ValidationError,
    validate_metadata,
)
from objects import ObjectStore

# One activity record after the branch point makes a branch meaningful.
MEANINGFUL_ACTIVITY = 1

_MIGRATIONS: list[str] = [
    """
    CREATE TABLE hosts (
        account TEXT NOT NULL,
        name TEXT NOT NULL,
        info TEXT NOT NULL,
        registered_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (account, name)
    );
    CREATE TABLE sessions (
        id INTEGER PRIMARY KEY,
        account TEXT NOT NULL,
        harness TEXT NOT NULL,
        session_id TEXT NOT NULL,
        created TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'normal',
        conflicted_at TEXT,
        lease_state TEXT,
        lease_host TEXT,
        lease_workspace TEXT,
        lease_since TEXT,
        forked_from_session TEXT,
        forked_from_generation TEXT,
        forked_from_offset INTEGER,
        last_activity TEXT,
        UNIQUE (account, harness, session_id)
    );
    CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        session INTEGER NOT NULL REFERENCES sessions (id),
        path TEXT NOT NULL,
        kind TEXT NOT NULL,
        UNIQUE (session, path)
    );
    CREATE TABLE generations (
        id TEXT PRIMARY KEY,
        file INTEGER NOT NULL REFERENCES files (id),
        parent TEXT REFERENCES generations (id),
        fork_offset INTEGER NOT NULL DEFAULT 0,
        cause TEXT NOT NULL,
        length INTEGER NOT NULL,
        activity INTEGER NOT NULL DEFAULT 0,
        host TEXT NOT NULL,
        created TEXT NOT NULL,
        last_append TEXT NOT NULL
    );
    CREATE INDEX generations_file ON generations (file);
    CREATE TABLE segments (
        generation TEXT NOT NULL REFERENCES generations (id),
        offset INTEGER NOT NULL,
        length INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        activity INTEGER NOT NULL,
        frame_offset INTEGER NOT NULL,
        frame_length INTEGER NOT NULL,
        host TEXT NOT NULL,
        workspace TEXT NOT NULL,
        received TEXT NOT NULL,
        PRIMARY KEY (generation, offset)
    );
    CREATE TABLE blob_versions (
        file INTEGER NOT NULL REFERENCES files (id),
        version INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        size INTEGER NOT NULL,
        host TEXT NOT NULL,
        created TEXT NOT NULL,
        PRIMARY KEY (file, version)
    );
    CREATE TABLE metadata_events (
        id INTEGER PRIMARY KEY,
        session INTEGER NOT NULL REFERENCES sessions (id),
        host TEXT NOT NULL,
        received TEXT NOT NULL,
        body TEXT NOT NULL
    );
    CREATE INDEX metadata_events_session ON metadata_events (session);
    CREATE TABLE placements (
        id INTEGER PRIMARY KEY,
        session INTEGER NOT NULL REFERENCES sessions (id),
        host TEXT NOT NULL,
        workspace TEXT NOT NULL,
        cwd TEXT,
        cause TEXT NOT NULL,
        first_seen TEXT NOT NULL,
        last_seen TEXT NOT NULL
    );
    CREATE INDEX placements_session ON placements (session);
    CREATE TABLE teleports (
        id TEXT PRIMARY KEY,
        session INTEGER NOT NULL REFERENCES sessions (id),
        source TEXT,
        target_host TEXT NOT NULL,
        target_workspace TEXT NOT NULL,
        steal INTEGER NOT NULL,
        created TEXT NOT NULL
    );
    CREATE TABLE grants (
        session INTEGER NOT NULL REFERENCES sessions (id),
        host TEXT NOT NULL,
        granted_by TEXT NOT NULL,
        created TEXT NOT NULL,
        PRIMARY KEY (session, host)
    );
    CREATE TABLE read_audit (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        account TEXT NOT NULL,
        host TEXT NOT NULL,
        session INTEGER NOT NULL,
        what TEXT NOT NULL
    );
    CREATE TABLE purges (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL,
        target TEXT NOT NULL,
        reason TEXT NOT NULL
    );
    """,
    # SQLite can give a new session the rowid of a purged session. Thus each
    # read audit row also keeps the external key of the session that it
    # records. The updates copy the key into the rows that exist.
    #
    # An old row has only the rowid, and the session that has that rowid now
    # can be a new session that took the rowid after a purge. That session
    # was created after the purge, and the purge was after the read. Thus the
    # updates copy the key only when the session was created before the read.
    # When the two times are equal, the updates copy the key only if no
    # session purge has the same time. All other rows keep a NULL key, which
    # means "unknown". The updates do not guess a key for these rows.
    """
    ALTER TABLE read_audit ADD COLUMN harness TEXT;
    ALTER TABLE read_audit ADD COLUMN session_id TEXT;
    UPDATE read_audit SET harness = (
        SELECT s.harness FROM sessions s
        WHERE s.id = read_audit.session AND s.account = read_audit.account
        AND (
            s.created < read_audit.ts
            OR (
                s.created = read_audit.ts
                AND NOT EXISTS (
                    SELECT 1 FROM purges p
                    WHERE p.ts = read_audit.ts AND p.target LIKE 'session %'
                )
            )
        )
    );
    UPDATE read_audit SET session_id = (
        SELECT s.session_id FROM sessions s
        WHERE s.id = read_audit.session AND s.account = read_audit.account
        AND s.harness = read_audit.harness
    )
    WHERE harness IS NOT NULL;
    """,
    # Status events are session data, so they are append-only. Heartbeats
    # and liveness are current state, and the store overwrites their rows.
    """
    CREATE TABLE status_events (
        session INTEGER NOT NULL REFERENCES sessions (id),
        seq INTEGER NOT NULL,
        status TEXT NOT NULL,
        reason TEXT,
        wake TEXT,
        note TEXT,
        excerpt TEXT,
        source_kind TEXT NOT NULL,
        source_host TEXT NOT NULL,
        source_session TEXT,
        explicit INTEGER NOT NULL,
        received TEXT NOT NULL,
        PRIMARY KEY (session, seq)
    );
    CREATE TABLE host_heartbeats (
        account TEXT NOT NULL,
        host TEXT NOT NULL,
        source TEXT NOT NULL,
        received TEXT NOT NULL,
        interval INTEGER NOT NULL,
        final INTEGER NOT NULL,
        unclosed TEXT NOT NULL,
        PRIMARY KEY (account, host, source)
    );
    CREATE TABLE session_liveness (
        session INTEGER NOT NULL REFERENCES sessions (id),
        host TEXT NOT NULL,
        source TEXT NOT NULL,
        first_listed TEXT NOT NULL,
        last_listed TEXT NOT NULL,
        PRIMARY KEY (session, host, source)
    );
    """,
    # The session list sorts and filters in SQL. `status` is a copy of the
    # status of the latest status event, and `metadata` is a copy of the body
    # of the latest metadata event. Each write of such an event updates its
    # copy in the same transaction. The event tables stay the source of
    # truth.
    """
    ALTER TABLE sessions ADD COLUMN status TEXT;
    ALTER TABLE sessions ADD COLUMN metadata TEXT;
    UPDATE sessions SET status = (
        SELECT e.status FROM status_events e WHERE e.session = sessions.id
        ORDER BY e.seq DESC LIMIT 1
    );
    UPDATE sessions SET metadata = (
        SELECT m.body FROM metadata_events m WHERE m.session = sessions.id
        ORDER BY m.id DESC LIMIT 1
    );
    CREATE INDEX sessions_recent ON sessions (
        account, COALESCE(last_activity, created),
        status, lease_state, lease_host, harness
    );
    CREATE INDEX placements_host ON placements (host, session);
    CREATE INDEX placements_workspace ON placements (workspace, session);
    CREATE INDEX metadata_events_author ON metadata_events (session, host);
    """,
    # Child sessions. `parent` is a copy of `links.parent` of the latest
    # metadata, and `top_level` is 0 for a session with a parent or with
    # `automation` metadata. `add_metadata` updates them with `metadata`.
    # `top_level` is the last column of `sessions_recent`, so that the
    # filter `top_level=1` does not read the table row of a child session.
    """
    ALTER TABLE sessions ADD COLUMN parent TEXT;
    ALTER TABLE sessions ADD COLUMN top_level INTEGER NOT NULL DEFAULT 1;
    UPDATE sessions SET parent = json_extract(metadata, '$.links.parent')
    WHERE json_valid(metadata)
        AND json_type(metadata, '$.links.parent') = 'text'
        AND NOT EXISTS (
            SELECT 1 FROM json_each(metadata, '$.links') WHERE type != 'text'
        );
    UPDATE sessions SET top_level = 0
    WHERE parent IS NOT NULL
        OR (json_valid(metadata) AND json_type(metadata, '$.automation') = 'text');
    DROP INDEX sessions_recent;
    CREATE INDEX sessions_recent ON sessions (
        account, COALESCE(last_activity, created),
        status, lease_state, lease_host, harness, top_level
    );
    CREATE INDEX sessions_parent ON sessions (
        account, parent, COALESCE(last_activity, created)
    ) WHERE parent IS NOT NULL;
    """,
    # The newest event of each event table that has a copy in the session
    # row (see `_repair_copies`). A mark of 0 makes the next start fill the
    # copies of every session.
    """
    CREATE TABLE copy_marks (
        events TEXT PRIMARY KEY,
        last_id INTEGER NOT NULL,
        last_key TEXT NOT NULL
    );
    INSERT INTO copy_marks VALUES ('status_events', 0, ''), ('metadata_events', 0, '');
    """,
]

# The event tables whose latest event has a copy in the session row, with an
# expression that identifies an event. A server before migration 4 writes
# events and does not update the copies. Each write of this server sets the
# mark to its event in the same transaction. Thus an event above the mark,
# or a mark whose event is gone or is a different event (a deleted row whose
# id SQLite gave again), shows a write that did not update the copies.
_COPY_MARKS = {
    "status_events": "session || ':' || seq || ':' || received",
    "metadata_events": "session || ':' || received",
}
# The statements that fill the copies of the sessions that `{where}` selects,
# from their events. The same rules as migrations 4 and 5.
_FILL_COPIES = {
    "status_events": [
        """
        UPDATE sessions SET status = (
            SELECT e.status FROM status_events e WHERE e.session = sessions.id
            ORDER BY e.seq DESC LIMIT 1
        ) WHERE {where}
        """
    ],
    "metadata_events": [
        """
        UPDATE sessions SET metadata = (
            SELECT m.body FROM metadata_events m WHERE m.session = sessions.id
            ORDER BY m.id DESC LIMIT 1
        ) WHERE {where}
        """,
        """
        UPDATE sessions SET parent = CASE
            WHEN json_valid(metadata)
                AND json_type(metadata, '$.links.parent') = 'text'
                AND NOT EXISTS (
                    SELECT 1 FROM json_each(metadata, '$.links')
                    WHERE type != 'text'
                )
            THEN json_extract(metadata, '$.links.parent')
        END WHERE {where}
        """,
        """
        UPDATE sessions SET top_level = CASE
            WHEN parent IS NOT NULL
                OR (json_valid(metadata)
                    AND json_type(metadata, '$.automation') = 'text')
            THEN 0 ELSE 1
        END WHERE {where}
        """,
    ],
}

log = logging.getLogger("hyperspace.store")

# Read access to the session `s` for the host `:host`, for scopes other than
# `all`. `:granted` is 1 for the `granted` scope.
#
# Only a held lease gives read access. A lease reserved for a host does not:
# a surrender needs only the append scope, so a reserve must not work as a
# grant. Metadata and blobs can arrive before the first generation. Their
# stored authors give access without a placement or an early lease.
_READABLE = """(
    (s.lease_state = 'held' AND s.lease_host = :host)
    OR EXISTS (
        SELECT 1 FROM placements p WHERE p.session = s.id AND p.host = :host
    )
    OR EXISTS (
        SELECT 1 FROM files f JOIN generations g ON g.file = f.id
        WHERE f.session = s.id AND g.host = :host
    )
    OR EXISTS (
        SELECT 1 FROM metadata_events m WHERE m.session = s.id AND m.host = :host
    )
    OR EXISTS (
        SELECT 1 FROM files f JOIN blob_versions b ON b.file = f.id
        WHERE f.session = s.id AND b.host = :host
    )
    OR (:granted AND EXISTS (
        SELECT 1 FROM grants gr WHERE gr.session = s.id AND gr.host = :host
    ))
)"""

# The sort key of the session list, and its order. The public key of the
# session breaks ties, so that the order is total and a cursor does not show
# row ids. The index `sessions_recent` starts with the same expression, so
# SQLite reads the rows in this order and sorts only the rows that have the
# same time. The other columns of the index let SQLite refuse a row for the
# status, lease, and harness filters, and for read access, without a read of
# the table row.
_SORT_KEY = "COALESCE(s.last_activity, s.created)"
_RECENT = f"{_SORT_KEY} DESC, s.harness DESC, s.session_id DESC"


@dataclass(frozen=True)
class ListPosition:
    """The position of a session in the session list."""

    sort_key: str
    harness: str
    session_id: str


# A heartbeat expires after this many of its intervals.
HEARTBEAT_EXPIRY = 3

# The page cache of the writer connection.
WRITE_CACHE_BYTES = 128 * 1024**2
# The defaults for the pool of read connections. The server gets its values
# from its settings. Each read connection has its own page cache, so the
# caches of the pool use at most READ_CONNECTIONS * READ_CACHE_BYTES.
READ_CONNECTIONS = 8
READ_CACHE_BYTES = 16 * 1024**2
# The admin scripts open the same index while the server runs, so a
# connection waits for their locks.
_BUSY_TIMEOUT_MS = 10000


@dataclass
class Waits:
    """The time in seconds that the store calls of one context waited for
    the writer connection and for a read connection."""

    writer: float = 0.0
    reader: float = 0.0


_waits: ContextVar[Waits | None] = ContextVar("hyperspace_store_waits", default=None)


@contextmanager
def track_waits() -> Iterator[Waits]:
    """Add the waits of the store calls in this context to one `Waits`.

    A worker thread of `run_in_threadpool` runs in a copy of the context. The
    copy refers to the same `Waits`, so the waits in that thread count too.
    """
    waits = Waits()
    token = _waits.set(waits)
    try:
        yield waits
    finally:
        _waits.reset(token)


def _add_wait(writer: bool, seconds: float) -> None:
    waits = _waits.get()
    if waits is None:
        return
    if writer:
        waits.writer += seconds
    else:
        waits.reader += seconds


class StoreError(Exception):
    """A request that the store refuses. `code` is a short machine name."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(detail or code)
        self.code = code
        self.detail = detail


class NotFound(StoreError):
    def __init__(self, detail: str = "") -> None:
        super().__init__("not_found", detail)


class Conflict(StoreError):
    """An append whose base length is not the generation length."""

    def __init__(self, generation: str, length: int) -> None:
        super().__init__("conflict", f"generation {generation} has length {length}")
        self.generation = generation
        self.length = length
        self.session: SessionStateBody | None = None


class StaleStatus(StoreError):
    """A status write whose base is not the latest status event."""

    def __init__(self, current: StatusEvent | None) -> None:
        seq = current["seq"] if current is not None else 0
        super().__init__("stale_status", f"the latest status event is {seq}")
        self.current = current


@dataclass(frozen=True)
class SessionKey:
    account: str
    harness: str
    session_id: str


@dataclass(frozen=True)
class Writer:
    """The host and workspace that send a write."""

    account: str
    host: str
    workspace: str


@dataclass
class _Gen:
    id: str
    file: int
    parent: str | None
    fork_offset: int
    cause: str
    length: int
    activity: int
    host: str
    created: str
    last_append: str


@dataclass(frozen=True)
class _Step:
    gen: _Gen
    exit: int | None


def _gen_from_row(row: sqlite3.Row) -> _Gen:
    return _Gen(
        id=row["id"],
        file=row["file"],
        parent=row["parent"],
        fork_offset=row["fork_offset"],
        cause=row["cause"],
        length=row["length"],
        activity=row["activity"],
        host=row["host"],
        created=row["created"],
        last_append=row["last_append"],
    )


_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(_ISO_FORMAT)


def _parse_iso(value: str) -> datetime:
    return datetime.strptime(value, _ISO_FORMAT).replace(tzinfo=UTC)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(12)}"


def _reading[**P, R](
    method: Callable[Concatenate[Store, P], R],
) -> Callable[Concatenate[Store, P], R]:
    """Run a read method in one read transaction (see `Store._read`)."""

    @functools.wraps(method)
    def wrapper(self: Store, *args: P.args, **kwargs: P.kwargs) -> R:
        with self._read():
            return method(self, *args, **kwargs)

    return wrapper


class _ReadPool:
    """Read-only connections to the index, for reads that do not write.

    The pool opens a connection when a read needs one, and keeps at most
    `size` connections. When all of them are in use, a read waits for one.
    Thus the page caches of the pool use at most `size` * `cache_bytes`.
    """

    def __init__(self, path: Path, size: int, cache_bytes: int) -> None:
        if size < 1:
            raise ValueError("the read pool needs at least one connection")
        if cache_bytes < 0:
            raise ValueError("the read cache size must not be negative")
        self._path = path
        self._size = size
        self._cache_kib = cache_bytes // 1024
        self._idle: list[sqlite3.Connection] = []
        self._open = 0
        self._closed = False
        self._cond = threading.Condition()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        try:
            db.row_factory = sqlite3.Row
            db.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            # A negative size is in KiB.
            db.execute(f"PRAGMA cache_size = {-self._cache_kib}")
            # A read path must never write. With this setting, a write fails.
            db.execute("PRAGMA query_only = ON")
        except BaseException:
            db.close()
            raise
        return db

    def acquire(self) -> sqlite3.Connection:
        with self._cond:
            while True:
                if self._closed:
                    raise sqlite3.ProgrammingError("the store is closed")
                if self._idle:
                    # The connection that was used last has the warmest cache.
                    return self._idle.pop()
                if self._open < self._size:
                    self._open += 1
                    break
                self._cond.wait()
        try:
            return self._connect()
        except BaseException:
            with self._cond:
                self._open -= 1
                self._cond.notify()
            raise

    def release(self, db: sqlite3.Connection, reuse: bool) -> None:
        with self._cond:
            if reuse and not self._closed:
                self._idle.append(db)
                self._cond.notify()
                return
            self._open -= 1
            self._cond.notify()
        db.close()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            idle, self._idle = self._idle, []
            self._open -= len(idle)
            self._cond.notify_all()
        for db in idle:
            db.close()


class Store:
    def __init__(
        self,
        data_dir: Path,
        clock: Callable[[], datetime] | None = None,
        read_connections: int = READ_CONNECTIONS,
        read_cache_bytes: int = READ_CACHE_BYTES,
    ) -> None:
        self.data_dir = data_dir
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        # `db` is the connection of the current transaction of the thread.
        self._local = threading.local()
        data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = data_dir / "logs"
        self.objects = ObjectStore(data_dir / "objects")
        path = data_dir / "index.sqlite3"
        self._writer = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._writer.row_factory = sqlite3.Row
        self._writer.execute("PRAGMA journal_mode = WAL")
        self._writer.execute("PRAGMA foreign_keys = ON")
        self._writer.execute("PRAGMA synchronous = FULL")
        self._writer.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        # A negative size is in KiB.
        self._writer.execute(f"PRAGMA cache_size = {-(WRITE_CACHE_BYTES // 1024)}")
        self._migrate()
        self._repair_copies()
        self._readers = _ReadPool(path, read_connections, read_cache_bytes)
        # Recovery (see `recover`) is not done here: only the server calls it,
        # at startup. An admin script that runs beside the server must not cut
        # a frame that the server is writing.

    # --- Infrastructure -------------------------------------------------

    @property
    def _db(self) -> sqlite3.Connection:
        """The connection of the current transaction of this thread: a read
        connection in `_read`, else the writer connection."""
        db: sqlite3.Connection | None = getattr(self._local, "db", None)
        return self._writer if db is None else db

    def close(self) -> None:
        self._readers.close()
        self._writer.close()

    def now(self) -> str:
        return _iso(self._clock())

    def _migrate(self) -> None:
        with self._lock:
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
            )
            row = self._db.execute("SELECT version FROM schema_version").fetchone()
            version = row["version"] if row else 0
            if row is None:
                self._db.execute("INSERT INTO schema_version VALUES (0)")
            for index in range(version, len(_MIGRATIONS)):
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _MIGRATIONS[index].split(";"):
                        if statement.strip():
                            self._db.execute(statement)
                    self._db.execute(
                        "UPDATE schema_version SET version = ?", (index + 1,)
                    )
                    self._db.execute("COMMIT")
                except BaseException:
                    self._db.execute("ROLLBACK")
                    raise

    def _repair_copies(self) -> None:
        """Fill the copies of the latest status and metadata in the session
        rows again where a server that does not keep them wrote events, for
        example after a rollback to an older server and a new upgrade. Only
        the sessions with events above the mark get new copies, unless the
        mark cannot be trusted; then all sessions do."""
        with self._tx():
            for table, key in _COPY_MARKS.items():
                mark = self._db.execute(
                    "SELECT last_id, last_key FROM copy_marks WHERE events = ?",
                    (table,),
                ).fetchone()
                last_id = mark["last_id"] if mark is not None else 0
                trusted = mark is not None
                if trusted and last_id:
                    row = self._db.execute(
                        f"SELECT {key} AS k FROM {table} WHERE rowid = ?", (last_id,)
                    ).fetchone()
                    trusted = row is not None and row["k"] == mark["last_key"]
                if trusted:
                    newer = self._db.execute(
                        f"SELECT COUNT(DISTINCT session) AS n FROM {table} "
                        "WHERE rowid > ?",
                        (last_id,),
                    ).fetchone()["n"]
                    if not newer:
                        continue
                    where = f"id IN (SELECT session FROM {table} WHERE rowid > :mark)"
                    log.info("%s: new copies for %d sessions", table, newer)
                else:
                    where = "1"
                    log.warning("%s: the mark is not valid; new copies for all", table)
                for statement in _FILL_COPIES[table]:
                    self._db.execute(statement.format(where=where), {"mark": last_id})
                self._mark_copy(table)

    def _mark_copy(self, table: str) -> None:
        """Set the mark of an event table to its newest event, in the
        transaction that wrote or deleted events (see `_COPY_MARKS`)."""
        self._db.execute(
            "INSERT OR REPLACE INTO copy_marks (events, last_id, last_key) "
            f"SELECT ?, m.id, COALESCE((SELECT {_COPY_MARKS[table]} FROM {table} "
            f"WHERE rowid = m.id), '') "
            f"FROM (SELECT COALESCE(MAX(rowid), 0) AS id FROM {table}) m",
            (table,),
        )

    def _tx(self) -> _Transaction:
        return _Transaction(self)

    @contextmanager
    def _read(self) -> Iterator[None]:
        """Run the body in one deferred read transaction on a read
        connection, without the store lock. The body sees one committed
        snapshot of the index, and a write in another thread or process does
        not wait for it.

        In a transaction of this thread, the body uses that transaction, so
        that it sees the uncommitted writes of that transaction.
        """
        if getattr(self._local, "db", None) is not None:
            yield
            return
        start = time.monotonic()
        db = self._readers.acquire()
        _add_wait(False, time.monotonic() - start)
        self._local.db = db
        try:
            db.execute("BEGIN")
            yield
        finally:
            self._local.db = None
            reuse = True
            try:
                if db.in_transaction:
                    db.execute("ROLLBACK")
            except sqlite3.Error:
                reuse = False
            self._readers.release(db, reuse)

    def recover(self) -> list[str]:
        """Cut generation files to the end of their last committed frame.

        Return the ids of the generations whose files were longer.
        """
        cut: list[str] = []
        with self._lock:
            rows = self._db.execute(
                """
                SELECT g.id AS id,
                       COALESCE(MAX(s.frame_offset + s.frame_length), 0) AS end_
                FROM generations g LEFT JOIN segments s ON s.generation = g.id
                GROUP BY g.id
                """
            ).fetchall()
            for row in rows:
                if self._framelog(row["id"]).truncate(row["end_"]):
                    cut.append(row["id"])
            self.objects.clean_temporary()
        return cut

    def _framelog(self, generation: str) -> FrameLog:
        return FrameLog(self.logs_dir / f"{generation}.zst")

    # --- Hosts ----------------------------------------------------------

    def register_host(self, account: str, name: str, info: HostInfo) -> HostRecord:
        now = self.now()
        with self._tx():
            self._db.execute(
                """
                INSERT INTO hosts (account, name, info, registered_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (account, name)
                DO UPDATE SET info = excluded.info, updated_at = excluded.updated_at
                """,
                (account, name, json.dumps(info), now, now),
            )
            record = self.get_host(account, name)
        assert record is not None
        return record

    @_reading
    def get_host(self, account: str, name: str) -> HostRecord | None:
        row = self._db.execute(
            "SELECT * FROM hosts WHERE account = ? AND name = ?", (account, name)
        ).fetchone()
        if row is None:
            return None
        return self._host_record(row)

    @_reading
    def list_hosts(self, account: str) -> list[HostRecord]:
        rows = self._db.execute(
            "SELECT * FROM hosts WHERE account = ? ORDER BY name", (account,)
        ).fetchall()
        return [self._host_record(r) for r in rows]

    @staticmethod
    def _host_record(row: sqlite3.Row) -> HostRecord:
        info: dict[str, Any] = json.loads(row["info"])
        record: dict[str, Any] = {
            **info,
            "name": row["name"],
            "registered_at": row["registered_at"],
            "updated_at": row["updated_at"],
        }
        return record  # type: ignore[return-value]

    # --- Sessions -------------------------------------------------------

    def _session_row(self, key: SessionKey) -> sqlite3.Row | None:
        return self._db.execute(
            "SELECT * FROM sessions WHERE account = ? AND harness = ? AND session_id = ?",
            (key.account, key.harness, key.session_id),
        ).fetchone()

    def _require_session(self, key: SessionKey) -> sqlite3.Row:
        row = self._session_row(key)
        if row is None:
            raise NotFound("session")
        return row

    def _ensure_session(self, key: SessionKey) -> sqlite3.Row:
        row = self._session_row(key)
        if row is not None:
            return row
        self._db.execute(
            "INSERT INTO sessions (account, harness, session_id, created) "
            "VALUES (?, ?, ?, ?)",
            (key.account, key.harness, key.session_id, self.now()),
        )
        row = self._session_row(key)
        assert row is not None
        return row

    @_reading
    def session_exists(self, key: SessionKey) -> bool:
        return self._session_row(key) is not None

    def write_authorized[T](
        self, key: SessionKey, host: str, scope: ReadScope, operation: Callable[[], T]
    ) -> T:
        """Check access and write in the same database transaction."""
        with self._tx():
            session = self._session_row(key)
            if session is not None and not self._can_read_row(session, host, scope):
                raise NotFound("session")
            return operation()

    def existing_authorized[T](
        self,
        key: SessionKey,
        host: str,
        scope: ReadScope,
        operation: Callable[[], T],
        audit: str | None = None,
    ) -> T:
        """Authorize and use an existing session without a purge/recreate gap.

        This runs in a write transaction. Use it for a write, and for a read
        that writes an audit row. The database write lock also excludes admin
        processes. The process RLock alone cannot protect the identity of a
        session row.
        """
        with self._tx():
            session = self._require_session(key)
            if not self._can_read_row(session, host, scope):
                raise NotFound("session")
            try:
                result = operation()
            except Conflict as exc:
                # Capture the response state before releasing authorization.
                exc.session = self._session_state(session["id"])
                raise
            if audit is not None:
                self.audit_read(key, host, audit)
            return result

    def read_authorized[T](
        self,
        key: SessionKey,
        host: str,
        scope: ReadScope,
        operation: Callable[[], T],
    ) -> T:
        """Authorize and read an existing session in one read transaction.

        `operation` must not write. The authorization and the read see the
        same snapshot, so a purge and a new session with the same key cannot
        come between them.
        """
        with self._read():
            session = self._require_session(key)
            if not self._can_read_row(session, host, scope):
                raise NotFound("session")
            return operation()

    def snapshot[T](self, operation: Callable[[], T]) -> T:
        """Keep filtered collection reads in one cross-process snapshot.

        `operation` must not write. It runs in a read transaction, so it does
        not stop writes.
        """
        with self._read():
            return operation()

    # --- Files ----------------------------------------------------------

    def _file_row(self, session: int, path: str) -> sqlite3.Row | None:
        return self._db.execute(
            "SELECT * FROM files WHERE session = ? AND path = ?", (session, path)
        ).fetchone()

    def _ensure_file(self, session: int, path: str, kind: str) -> int:
        row = self._file_row(session, path)
        if row is not None:
            if row["kind"] != kind:
                raise StoreError("wrong_kind", f"{path} is a {row['kind']}")
            return int(row["id"])
        cur = self._db.execute(
            "INSERT INTO files (session, path, kind) VALUES (?, ?, ?)",
            (session, path, kind),
        )
        assert cur.lastrowid is not None
        return cur.lastrowid

    # --- Generations ----------------------------------------------------

    def _gen(self, generation: str) -> _Gen:
        row = self._db.execute(
            "SELECT * FROM generations WHERE id = ?", (generation,)
        ).fetchone()
        if row is None:
            raise NotFound("generation")
        return _gen_from_row(row)

    def _gens_of_file(self, file: int) -> list[_Gen]:
        rows = self._db.execute(
            "SELECT * FROM generations WHERE file = ? ORDER BY created, id", (file,)
        ).fetchall()
        return [_gen_from_row(r) for r in rows]

    def _gen_in_session(self, key: SessionKey, generation: str) -> _Gen:
        session = self._require_session(key)
        gen = self._gen(generation)
        owner = self._db.execute(
            "SELECT session FROM files WHERE id = ?", (gen.file,)
        ).fetchone()
        if owner is None or owner["session"] != session["id"]:
            raise NotFound("generation")
        return gen

    @_reading
    def generation_path(self, key: SessionKey, generation: str) -> str:
        gen = self._gen_in_session(key, generation)
        row = self._db.execute(
            "SELECT path FROM files WHERE id = ?", (gen.file,)
        ).fetchone()
        return str(row["path"])

    def create_generation(
        self,
        key: SessionKey,
        path: str,
        writer: Writer,
        cause: GenerationCause,
        parent: str | None = None,
        fork_offset: int = 0,
    ) -> GenerationInfo:
        """Create a generation of the log at `path`.

        A file's first generation has no parent and the cause `root` (or
        `fork` for the first file of a recovery fork). A later generation
        needs a parent in the same file and a fork offset that is not more
        than the parent's length. A first generation of a blob path changes
        the path to a log, and starts with the latest blob version.
        """
        with self._tx():
            session = self._ensure_session(key)
            previous = self._file_row(session["id"], path)
            seed: bytes | None = None
            if previous is not None and previous["kind"] == "blob":
                if parent is not None or cause not in ("root", "fork"):
                    raise StoreError("bad_request", "promotion needs a root")
                seed = self.read_blob(key, path, None)
                self._db.execute(
                    "UPDATE files SET kind = 'log' WHERE id = ?", (previous["id"],)
                )
            file = self._ensure_file(session["id"], path, "log")
            existing = self._gens_of_file(file)
            if parent is None:
                if existing:
                    raise StoreError("exists", "the file already has generations")
                if cause not in ("root", "fork"):
                    raise StoreError("bad_request", "a first generation is a root")
                fork_offset = 0
            else:
                if cause not in ("rewrite", "diverge"):
                    raise StoreError("bad_request", "a fork is a rewrite or diverge")
                parent_gen = self._gen(parent)
                if parent_gen.file != file:
                    raise NotFound("parent generation")
                if not 0 <= fork_offset <= parent_gen.length:
                    raise StoreError("bad_request", "fork offset out of range")
            now = self.now()
            gen = _Gen(
                id=_new_id("g"),
                file=file,
                parent=parent,
                fork_offset=fork_offset,
                cause=cause,
                length=fork_offset,
                activity=0,
                host=writer.host,
                created=now,
                last_append=now,
            )
            self._db.execute(
                """
                INSERT INTO generations
                (id, file, parent, fork_offset, cause, length, activity, host,
                 created, last_append)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    gen.id,
                    gen.file,
                    gen.parent,
                    gen.fork_offset,
                    gen.cause,
                    gen.length,
                    gen.activity,
                    gen.host,
                    gen.created,
                    gen.last_append,
                ),
            )
            if seed:
                # Keep the latest complete version active if the client stops
                # before it sends the larger file. Older versions stay blobs.
                self.append(
                    key,
                    gen.id,
                    writer,
                    0,
                    seed,
                    hashlib.sha256(seed).hexdigest(),
                    0,
                )
                gen = self._gen(gen.id)
        return self._gen_info(gen)

    def append(
        self,
        key: SessionKey,
        generation: str,
        writer: Writer,
        base_length: int,
        data: bytes,
        sha256: str,
        activity: int,
    ) -> tuple[GenerationInfo, SessionStateBody]:
        """Append `data` to a generation. See "Append protocol" in the spec.

        Raise `Conflict` if `base_length` is not the generation length and
        the request is not a retry of a stored segment.
        """
        if hashlib.sha256(data).hexdigest() != sha256:
            raise StoreError("bad_digest", "the sha256 does not match the data")
        if activity < 0:
            raise StoreError("bad_request", "activity must not be negative")
        if not data:
            raise StoreError("bad_request", "empty append")
        with self._tx():
            gen = self._gen_in_session(key, generation)
            session = self._require_session(key)
            if base_length != gen.length:
                retry = self._db.execute(
                    "SELECT 1 FROM segments WHERE generation = ? AND offset = ? "
                    "AND sha256 = ? AND length = ?",
                    (generation, base_length, sha256, len(data)),
                ).fetchone()
                if retry is not None:
                    return self._gen_info(gen), self._session_state(session["id"])
                raise Conflict(generation, gen.length)
            frame_offset, frame_length = self._framelog(generation).append(
                data, first_frame=gen.length == gen.fork_offset
            )
            now = self.now()
            self._db.execute(
                """
                INSERT INTO segments
                (generation, offset, length, sha256, activity, frame_offset,
                 frame_length, host, workspace, received)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generation,
                    base_length,
                    len(data),
                    sha256,
                    activity,
                    frame_offset,
                    frame_length,
                    writer.host,
                    writer.workspace,
                    now,
                ),
            )
            self._db.execute(
                "UPDATE generations SET length = ?, activity = activity + ?, "
                "last_append = ? WHERE id = ?",
                (base_length + len(data), activity, now, generation),
            )
            self._db.execute(
                "UPDATE sessions SET last_activity = ? WHERE id = ?",
                (now, session["id"]),
            )
            self._note_placement(session["id"], writer, gen.cause == "diverge")
            self._acquire_new_lease(session["id"], writer)
            self._update_conflict(session["id"], gen.file)
            gen = self._gen(generation)
            state = self._session_state(session["id"])
        return self._gen_info(gen), state

    @_reading
    def read_log(
        self, key: SessionKey, generation: str, start: int, end: int | None
    ) -> bytes:
        """Return bytes [start, end) of a generation, including the prefix
        that it inherits from its parents."""
        gen = self._gen_in_session(key, generation)
        if end is None or end > gen.length:
            end = gen.length
        if start < 0 or start > end:
            raise StoreError("bad_range", "invalid range")
        return b"".join(self._read_range(gen, start, end))

    def _read_range(self, gen: _Gen, start: int, end: int) -> Iterator[bytes]:
        if start >= end:
            return
        if gen.parent is not None and start < gen.fork_offset:
            parent = self._gen(gen.parent)
            yield from self._read_range(parent, start, min(end, gen.fork_offset))
            start = gen.fork_offset
            if start >= end:
                return
        rows = self._db.execute(
            """
            SELECT offset, length, frame_offset, frame_length FROM segments
            WHERE generation = ? AND offset < ? AND offset + length > ?
            ORDER BY offset
            """,
            (gen.id, end, start),
        ).fetchall()
        log = self._framelog(gen.id)
        for row in rows:
            data = log.read_frame(row["frame_offset"], row["frame_length"], MAX_BODY)
            lo = max(start, row["offset"]) - row["offset"]
            hi = min(end, row["offset"] + row["length"]) - row["offset"]
            yield data[lo:hi]

    # --- Blobs ----------------------------------------------------------

    def put_blob(
        self, key: SessionKey, path: str, writer: Writer, data: bytes, sha256: str
    ) -> BlobVersion:
        """Add a blob version. If the latest version has the same content,
        return it and store nothing."""
        if hashlib.sha256(data).hexdigest() != sha256:
            raise StoreError("bad_digest", "the sha256 does not match the data")
        with self._tx():
            session = self._ensure_session(key)
            file = self._ensure_file(session["id"], path, "blob")
            latest = self._db.execute(
                "SELECT * FROM blob_versions WHERE file = ? "
                "ORDER BY version DESC LIMIT 1",
                (file,),
            ).fetchone()
            if latest is not None and latest["sha256"] == sha256:
                return self._blob_version(latest)
            self.objects.put(data)
            version = (latest["version"] + 1) if latest is not None else 1
            self._db.execute(
                "INSERT INTO blob_versions (file, version, sha256, size, host, created) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (file, version, sha256, len(data), writer.host, self.now()),
            )
            row = self._db.execute(
                "SELECT * FROM blob_versions WHERE file = ? AND version = ?",
                (file, version),
            ).fetchone()
        return self._blob_version(row)

    @_reading
    def read_blob(self, key: SessionKey, path: str, version: int | None) -> bytes:
        session = self._require_session(key)
        file = self._file_row(session["id"], path)
        if file is None:
            raise NotFound("file")
        if version is None:
            row = self._db.execute(
                "SELECT sha256 FROM blob_versions WHERE file = ? "
                "ORDER BY version DESC LIMIT 1",
                (file["id"],),
            ).fetchone()
        else:
            row = self._db.execute(
                "SELECT sha256 FROM blob_versions WHERE file = ? AND version = ?",
                (file["id"], version),
            ).fetchone()
        if row is None:
            raise NotFound("version")
        return self.objects.get(row["sha256"], 1 << 31)

    @staticmethod
    def _blob_version(row: sqlite3.Row) -> BlobVersion:
        return {
            "version": row["version"],
            "sha256": row["sha256"],
            "size": row["size"],
            "host": row["host"],
            "created": row["created"],
        }

    # --- Metadata -------------------------------------------------------

    def add_metadata(
        self, key: SessionKey, writer: Writer, metadata: SessionMetadata
    ) -> None:
        validate_metadata(dict(metadata))
        body = json.dumps(metadata)
        parent = metadata.get("links", {}).get("parent")
        top_level = parent is None and "automation" not in metadata
        with self._tx():
            session = self._ensure_session(key)
            self._db.execute(
                "INSERT INTO metadata_events (session, host, received, body) "
                "VALUES (?, ?, ?, ?)",
                (session["id"], writer.host, self.now(), body),
            )
            self._db.execute(
                "UPDATE sessions SET metadata = ?, parent = ?, top_level = ? "
                "WHERE id = ?",
                (body, parent, int(top_level), session["id"]),
            )
            self._mark_copy("metadata_events")

    def _latest_metadata(self, session: int) -> SessionMetadata:
        row = self._db.execute(
            "SELECT body FROM metadata_events WHERE session = ? "
            "ORDER BY id DESC LIMIT 1",
            (session,),
        ).fetchone()
        return self._clean_metadata(row["body"] if row is not None else None)

    @staticmethod
    def _clean_metadata(body: str | None) -> SessionMetadata:
        """The metadata in the body of a metadata event, with no fields that
        are not valid."""
        if body is None:
            return {}
        metadata: dict[str, Any] = json.loads(body)
        # Old servers accepted malformed fields. Keep their events, but do
        # not let them break readers or new placements after an upgrade.
        clean: dict[str, Any] = {}
        for name, value in metadata.items():
            try:
                validate_metadata({name: value})
            except ValidationError:
                continue
            clean[name] = value
        return validate_metadata(clean)

    # --- Placements -----------------------------------------------------

    def _note_placement(self, session: int, writer: Writer, diverge: bool) -> None:
        now = self.now()
        latest = self._db.execute(
            "SELECT * FROM placements WHERE session = ? ORDER BY id DESC LIMIT 1",
            (session,),
        ).fetchone()
        if (
            latest is not None
            and latest["host"] == writer.host
            and latest["workspace"] == writer.workspace
        ):
            self._db.execute(
                "UPDATE placements SET last_seen = ? WHERE id = ?",
                (now, latest["id"]),
            )
            return
        cause: PlacementCause
        if diverge:
            cause = "diverge"
        elif latest is None:
            cause = "start"
        else:
            teleport = self._db.execute(
                "SELECT steal FROM teleports WHERE session = ? AND target_host = ? "
                "AND target_workspace = ? AND created >= ? "
                "ORDER BY created DESC LIMIT 1",
                (session, writer.host, writer.workspace, latest["first_seen"]),
            ).fetchone()
            if teleport is None:
                cause = "move"
            else:
                cause = "steal" if teleport["steal"] else "teleport"
        cwd = self._latest_metadata(session).get("cwd")
        self._db.execute(
            "INSERT INTO placements "
            "(session, host, workspace, cwd, cause, first_seen, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (session, writer.host, writer.workspace, cwd, cause, now, now),
        )

    def _placements(self, session: int) -> list[Placement]:
        rows = self._db.execute(
            "SELECT * FROM placements WHERE session = ? ORDER BY id", (session,)
        ).fetchall()
        return [self._placement(r) for r in rows]

    def _last_placement(self, session: int) -> Placement | None:
        row = self._db.execute(
            "SELECT * FROM placements WHERE session = ? ORDER BY id DESC LIMIT 1",
            (session,),
        ).fetchone()
        return self._placement(row) if row is not None else None

    @staticmethod
    def _placement(row: sqlite3.Row) -> Placement:
        p: Placement = {
            "host": row["host"],
            "workspace": row["workspace"],
            "cause": row["cause"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
        }
        if row["cwd"] is not None:
            p["cwd"] = row["cwd"]
        return p

    # --- Leases ---------------------------------------------------------

    def _acquire_new_lease(self, session: int, writer: Writer) -> None:
        """The first append of a new session acquires its lease."""
        self._db.execute(
            "UPDATE sessions SET lease_state = 'held', lease_host = ?, "
            "lease_workspace = ?, lease_since = ? "
            "WHERE id = ? AND lease_state IS NULL",
            (writer.host, writer.workspace, self.now(), session),
        )

    def surrender(
        self, key: SessionKey, host: str, reserved_for: str | None
    ) -> SessionStateBody:
        with self._tx():
            session = self._require_session(key)
            if session["lease_state"] != "held" or session["lease_host"] != host:
                raise StoreError("not_holder", "this host does not hold the lease")
            if reserved_for is None:
                self._db.execute(
                    "UPDATE sessions SET lease_state = 'free', lease_host = NULL, "
                    "lease_workspace = NULL, lease_since = ? WHERE id = ?",
                    (self.now(), session["id"]),
                )
            else:
                self._db.execute(
                    "UPDATE sessions SET lease_state = 'reserved', lease_host = ?, "
                    "lease_workspace = NULL, lease_since = ? WHERE id = ?",
                    (reserved_for, self.now(), session["id"]),
                )
            return self._session_state(session["id"])

    def teleport(
        self,
        key: SessionKey,
        writer: Writer,
        steal: bool,
        expected_revision: str | None = None,
        teleport_id: str | None = None,
    ) -> tuple[TeleportEvent, SessionStateBody]:
        """Take the lease only if the downloaded snapshot is still current.

        Legacy callers can omit the precondition during the transition.
        A supplied teleport id makes a retry after a lost response safe.
        """
        with self._tx():
            session = self._require_session(key)
            if teleport_id is not None:
                previous = self._db.execute(
                    "SELECT * FROM teleports WHERE id = ?", (teleport_id,)
                ).fetchone()
                if previous is not None:
                    if (
                        previous["session"] != session["id"]
                        or previous["target_host"] != writer.host
                        or previous["target_workspace"] != writer.workspace
                    ):
                        raise StoreError("bad_request", "teleport id is already in use")
                    if session["state"] == "conflicted":
                        raise StoreError(
                            "teleport_committed_conflict",
                            "this teleport committed, but the session is now conflicted; fork it",
                        )
                    if (
                        session["lease_state"] != "held"
                        or session["lease_host"] != writer.host
                        or session["lease_workspace"] != writer.workspace
                        or session["lease_since"] != previous["created"]
                    ):
                        raise StoreError(
                            "teleport_committed_moved",
                            "the lease changed after this teleport",
                        )
                    event: TeleportEvent = {
                        "id": teleport_id,
                        "target_host": writer.host,
                        "target_workspace": writer.workspace,
                        "steal": bool(previous["steal"]),
                        "created": previous["created"],
                    }
                    if previous["source"] is not None:
                        event["source"] = json.loads(previous["source"])
                    return event, self._session_state(session["id"])
            if (
                expected_revision is not None
                and self.manifest(key)["revision"] != expected_revision
            ):
                raise StoreError(
                    "stale_snapshot", "the session changed during restore; retry"
                )
            if session["state"] == "conflicted":
                raise StoreError("conflicted", "the session is conflicted; fork it")
            lease_state = session["lease_state"]
            lease_host = session["lease_host"]
            allowed = (
                lease_state in (None, "free")
                or (lease_state in ("reserved", "held") and lease_host == writer.host)
                or steal
            )
            if not allowed:
                raise StoreError("lease_held", f"{lease_host} holds the lease")
            stolen = steal and not (
                lease_state in (None, "free") or lease_host == writer.host
            )
            now = self.now()
            latest = self._db.execute(
                "SELECT * FROM placements WHERE session = ? ORDER BY id DESC LIMIT 1",
                (session["id"],),
            ).fetchone()
            source: Placement | None = None
            if latest is not None:
                source = self._placements(session["id"])[-1]
            event: TeleportEvent = {
                "id": teleport_id or _new_id("t"),
                "target_host": writer.host,
                "target_workspace": writer.workspace,
                "steal": stolen,
                "created": now,
            }
            if source is not None:
                event["source"] = source
            self._db.execute(
                "INSERT INTO teleports "
                "(id, session, source, target_host, target_workspace, steal, created) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event["id"],
                    session["id"],
                    json.dumps(source) if source is not None else None,
                    writer.host,
                    writer.workspace,
                    int(stolen),
                    now,
                ),
            )
            self._db.execute(
                "UPDATE sessions SET lease_state = 'held', lease_host = ?, "
                "lease_workspace = ?, lease_since = ? WHERE id = ?",
                (writer.host, writer.workspace, now, session["id"]),
            )
            return event, self._session_state(session["id"])

    def _lease(self, session: sqlite3.Row) -> Lease:
        state = session["lease_state"] or "free"
        lease: Lease = {
            "state": state,
            "since": session["lease_since"] or session["created"],
        }
        if session["lease_host"] is not None:
            lease["host"] = session["lease_host"]
        if session["lease_workspace"] is not None:
            lease["workspace"] = session["lease_workspace"]
        return lease

    # --- Branches and conflicts -----------------------------------------

    def _activity_after(self, gen: _Gen, offset: int, upto: int | None) -> int:
        """Activity of the segments of `gen` that have bytes in the range
        from `offset` to `upto`. A fork offset can be inside a segment, and
        the server cannot tell where in a segment the activity is. Thus a
        segment that crosses a limit of the range counts in full."""
        row = self._db.execute(
            "SELECT COALESCE(SUM(activity), 0) AS a FROM segments "
            "WHERE generation = ? AND offset + length > ? "
            "AND (? IS NULL OR offset < ?)",
            (gen.id, offset, upto, upto),
        ).fetchone()
        return int(row["a"])

    def _lineage(self, leaf: _Gen, by_id: dict[str, _Gen]) -> list[_Step]:
        """Return the path from the root to `leaf`. Each step holds a
        generation and the offset where the path leaves it (None for the
        leaf itself)."""
        path: list[_Step] = [_Step(leaf, None)]
        current = leaf
        while current.parent is not None:
            parent = by_id[current.parent]
            path.append(_Step(parent, current.fork_offset))
            current = parent
        path.reverse()
        return path

    def _branch_activity(
        self, lineage: list[_Step], after: int, seen: tuple[str, int] | None = None
    ) -> int:
        """Activity in a lineage at offsets at or after `after`. If `seen`
        is (generation, offset), the activity of that generation counts only
        at offsets at or after the larger of `after` and `offset`."""
        total = 0
        for step in lineage:
            start = after
            if seen is not None and step.gen.id == seen[0]:
                start = max(after, seen[1])
            if step.exit is not None and step.exit <= start:
                continue
            total += self._activity_after(step.gen, start, step.exit)
        return total

    @staticmethod
    def _shared_steps(a: list[_Step], b: list[_Step]) -> int:
        """The number of generations at the start of two lineages that are
        the same."""
        i = 0
        while i < min(len(a), len(b)) and a[i].gen.id == b[i].gen.id:
            i += 1
        return i

    @classmethod
    def _divergence(cls, a: list[_Step], b: list[_Step]) -> int:
        """The offset where two lineages stop sharing bytes."""
        i = cls._shared_steps(a, b)
        last_a, last_b = a[i - 1], b[i - 1]
        exits = [e for e in (last_a.exit, last_b.exit) if e is not None]
        return min(exits) if exits else last_a.gen.length

    def _rewrite_saw_parent(self, parent: _Gen, rewrite: _Gen) -> bool:
        """Return True if the host of `rewrite` had all bytes of `parent`.

        The host had them if it made the last append to `parent`, or if a
        teleport to the host came after that append. A teleport confirms a
        restore of a current snapshot.
        """
        if self._last_writer(parent) == rewrite.host:
            return True
        row = self._db.execute(
            "SELECT 1 FROM teleports t JOIN files f ON f.session = t.session "
            "WHERE f.id = ? AND t.target_host = ? AND t.created >= ? "
            "AND t.created <= ? LIMIT 1",
            (parent.file, rewrite.host, parent.last_append, rewrite.created),
        ).fetchone()
        return row is not None

    def _rewrite_seen_length(self, parent: _Gen, rewrite: _Gen) -> int:
        """Return the length of `parent` that the host of `rewrite` is known
        to have had when it created `rewrite`.

        The host had the bytes up to the end of its last append to `parent`.
        It also had all bytes of `parent` that the server received before
        the latest teleport to the host, because a teleport confirms a
        restore of a current snapshot.
        """
        teleport = self._db.execute(
            "SELECT MAX(t.created) AS created FROM teleports t "
            "JOIN files f ON f.session = t.session "
            "WHERE f.id = ? AND t.target_host = ? AND t.created <= ?",
            (parent.file, rewrite.host, rewrite.created),
        ).fetchone()["created"]
        row = self._db.execute(
            "SELECT COALESCE(MAX(offset + length), 0) AS seen FROM segments "
            "WHERE generation = ? AND received <= ? "
            "AND (host = ? OR received <= ?)",
            (parent.id, rewrite.created, rewrite.host, teleport),
        ).fetchone()
        return max(int(row["seen"]), rewrite.fork_offset)

    def _pair_activity(self, a: list[_Step], b: list[_Step]) -> int:
        """Activity of lineage `a` after it separates from lineage `b`.

        If `b` leaves the last shared generation through a `rewrite` child,
        the bytes of that generation that the host of the rewrite had do not
        count. That host saw these bytes and replaced them. Only the bytes
        that it did not see can hold work that the rewrite hides.
        """
        i = self._shared_steps(a, b)
        seen: tuple[str, int] | None = None
        if i < len(b) and b[i].gen.cause == "rewrite":
            parent = b[i - 1].gen
            seen = (parent.id, self._rewrite_seen_length(parent, b[i].gen))
        return self._branch_activity(a, self._divergence(a, b), seen)

    def _candidates(self, file: int) -> tuple[list[_Gen], dict[str, _Gen]]:
        """Return the branch tips of a log.

        A generation is a tip if it has bytes that no child inherits (or no
        children). A `rewrite` child retires a tip only if its host had all
        bytes of the tip and no append followed the child's creation.
        """
        gens = self._gens_of_file(file)
        by_id = {g.id: g for g in gens}
        children: dict[str, list[_Gen]] = {}
        for g in gens:
            if g.parent is not None:
                children.setdefault(g.parent, []).append(g)
        tips: list[_Gen] = []
        for g in gens:
            kids = children.get(g.id, [])
            if kids and g.length <= max(k.fork_offset for k in kids):
                continue
            retired = any(
                k.cause == "rewrite"
                and k.created >= g.last_append
                and self._rewrite_saw_parent(g, k)
                for k in kids
            )
            if not retired:
                tips.append(g)
        return tips, by_id

    def _last_writer(self, gen: _Gen) -> str:
        """The host of the last append to a generation. After a teleport, a
        generation can continue on a host other than the one that created
        it."""
        row = self._db.execute(
            "SELECT host FROM segments WHERE generation = ? "
            "ORDER BY offset DESC LIMIT 1",
            (gen.id,),
        ).fetchone()
        return row["host"] if row is not None else gen.host

    def _file_branches(self, file: int) -> tuple[_Gen | None, list[Branch], bool]:
        """Return (active tip, meaningful branches, conflicted) for a log."""
        tips, by_id = self._candidates(file)
        if not tips:
            return None, [], False
        lineages = {t.id: self._lineage(t, by_id) for t in tips}
        meaningful: list[tuple[_Gen, int]] = []
        scored: list[tuple[int, str, _Gen]] = []
        for t in tips:
            activity = max(
                (
                    self._pair_activity(lineages[t.id], lineages[o.id])
                    for o in tips
                    if o.id != t.id
                ),
                default=0,
            )
            is_meaningful = activity >= MEANINGFUL_ACTIVITY
            if is_meaningful:
                meaningful.append((t, activity))
            scored.append((int(is_meaningful), t.last_append, t))
        scored.sort(key=lambda s: (s[0], s[1]))
        active = scored[-1][2]
        conflicted = False
        for i, (a, _) in enumerate(meaningful):
            for b, _ in meaningful[i + 1 :]:
                la, lb = lineages[a.id], lineages[b.id]
                if (
                    self._pair_activity(la, lb) >= MEANINGFUL_ACTIVITY
                    and self._pair_activity(lb, la) >= MEANINGFUL_ACTIVITY
                ):
                    conflicted = True
        branches: list[Branch] = [
            {
                "generation": g.id,
                "host": self._last_writer(g),
                "activity": activity,
                "last_append": g.last_append,
            }
            for g, activity in meaningful
        ]
        return active, branches, conflicted

    def _update_conflict(self, session: int, file: int) -> None:
        """Mark the session conflicted if the log `file` now has two branches
        with meaningful activity. An append changes only its own file, and a
        file with one generation has one branch, so most appends skip the
        branch computation."""
        row = self._db.execute(
            "SELECT state FROM sessions WHERE id = ?", (session,)
        ).fetchone()
        if row["state"] == "conflicted":
            return
        count = self._db.execute(
            "SELECT COUNT(*) FROM generations WHERE file = ?", (file,)
        ).fetchone()[0]
        if count < 2:
            return
        _, _, conflicted = self._file_branches(file)
        if conflicted:
            self._db.execute(
                "UPDATE sessions SET state = 'conflicted', conflicted_at = ? "
                "WHERE id = ?",
                (self.now(), session),
            )

    def _session_state(self, session: int) -> SessionStateBody:
        row = self._db.execute(
            "SELECT * FROM sessions WHERE id = ?", (session,)
        ).fetchone()
        return self._session_state_and_excerpt(row)[0]

    def _session_state_and_excerpt(
        self, row: sqlite3.Row
    ) -> tuple[SessionStateBody, str | None]:
        """The state body of the session `row`, and the excerpt of its latest
        status event."""
        session = int(row["id"])
        body: SessionStateBody = {"state": row["state"], "lease": self._lease(row)}
        if row["state"] == "conflicted":
            branches: list[Branch] = []
            for f in self._db.execute(
                "SELECT id FROM files WHERE session = ? AND kind = 'log'", (session,)
            ).fetchall():
                branches.extend(self._file_branches(f["id"])[1])
            body["branches"] = branches
        latest = self._latest_status(session)
        if latest is not None:
            body["status"] = latest[0]
        liveness = self._liveness(row)
        if liveness is not None:
            body["liveness"] = liveness
        if row["last_activity"] is not None:
            body["last_append"] = row["last_activity"]
        return body, latest[1] if latest is not None else None

    @_reading
    def session_state(self, key: SessionKey) -> SessionStateBody:
        return self._session_state(self._require_session(key)["id"])

    # --- Status and liveness --------------------------------------------

    def _latest_status(self, session: int) -> tuple[StatusEvent, str | None] | None:
        """The latest status event of a session, and its excerpt."""
        row = self._db.execute(
            "SELECT * FROM status_events WHERE session = ? ORDER BY seq DESC LIMIT 1",
            (session,),
        ).fetchone()
        if row is None:
            return None
        event: StatusEvent = {
            "seq": row["seq"],
            "status": row["status"],
            "source": {"kind": row["source_kind"], "host": row["source_host"]},
            "explicit": bool(row["explicit"]),
            "received": row["received"],
        }
        if row["reason"] is not None:
            event["reason"] = row["reason"]
        if row["wake"] is not None:
            event["wake"] = row["wake"]
        if row["note"] is not None:
            event["note"] = row["note"]
        if row["source_session"] is not None:
            event["source"]["session_id"] = row["source_session"]
        return event, row["excerpt"]

    def add_status(
        self, key: SessionKey, host: str, request: StatusRequest
    ) -> StatusEvent:
        """Add a status event if `base_seq` is the latest seq.

        The caller checks read access. A sequence number, not the status
        value, is the base, so that a writer that saw an earlier `done` cannot
        replace a later `done`.
        """
        with self._tx():
            session = self._require_session(key)["id"]
            latest = self._latest_status(session)
            current = latest[0]["seq"] if latest is not None else 0
            if request["base_seq"] != current:
                raise StaleStatus(latest[0] if latest is not None else None)
            self._db.execute(
                """
                INSERT INTO status_events
                (session, seq, status, reason, wake, note, excerpt, source_kind,
                 source_host, source_session, explicit, received)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session,
                    current + 1,
                    request["status"],
                    request.get("reason"),
                    request.get("wake"),
                    request.get("note"),
                    request.get("excerpt"),
                    request["source_kind"],
                    host,
                    request.get("source_session"),
                    int(request["explicit"]),
                    self.now(),
                ),
            )
            self._db.execute(
                "UPDATE sessions SET status = ? WHERE id = ?",
                (request["status"], session),
            )
            self._mark_copy("status_events")
            added = self._latest_status(session)
            assert added is not None
            return added[0]

    def heartbeat(
        self, account: str, host: str, scope: ReadScope, request: HeartbeatRequest
    ) -> HeartbeatResponse:
        """Record the sessions that run on `host` now, for the heartbeat's
        source (one harness config directory of the host).

        The store skips a listed session that does not exist or that the host
        cannot read, with no error, so that a heartbeat is not an existence
        oracle.
        """
        source = request.get("source", "")
        with self._tx():
            now = self.now()
            previous = self._db.execute(
                "SELECT * FROM host_heartbeats "
                "WHERE account = ? AND host = ? AND source = ?",
                (account, host, source),
            ).fetchone()
            recorded = 0
            for item in request["sessions"]:
                session = self._session_row(
                    SessionKey(account, item["harness"], item["session_id"])
                )
                if session is None or not self._can_read_row(session, host, scope):
                    continue
                row = self._db.execute(
                    "SELECT * FROM session_liveness "
                    "WHERE session = ? AND host = ? AND source = ?",
                    (session["id"], host, source),
                ).fetchone()
                # `first_listed` is the start of the current run on this host.
                # It stays only if the previous heartbeat listed the session
                # and had not expired.
                first = now
                if (
                    row is not None
                    and previous is not None
                    and row["last_listed"] >= previous["received"]
                    and not self._expired(previous, now)
                ):
                    first = row["first_listed"]
                self._db.execute(
                    """
                    INSERT INTO session_liveness
                    (session, host, source, first_listed, last_listed)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (session, host, source) DO UPDATE SET
                        first_listed = excluded.first_listed,
                        last_listed = excluded.last_listed
                    """,
                    (session["id"], host, source, first, now),
                )
                recorded += 1
            unclosed: list[UnclosedSession] = []
            for item in request.get("unclosed", []):
                session = self._session_row(
                    SessionKey(account, item["harness"], item["session_id"])
                )
                if session is None or not self._can_read_row(session, host, scope):
                    continue
                unclosed.append(item)
                # A job can stop before any heartbeat lists it. Then the host
                # has no liveness row for it, and `close_blocked` would not
                # show. Record it as stopped at its last activity.
                since = session["last_activity"] or session["created"]
                if since < now:
                    self._db.execute(
                        """
                        INSERT INTO session_liveness
                        (session, host, source, first_listed, last_listed)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (session, host, source) DO NOTHING
                        """,
                        (session["id"], host, source, since, since),
                    )
            self._db.execute(
                """
                INSERT INTO host_heartbeats
                (account, host, source, received, interval, final, unclosed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (account, host, source) DO UPDATE SET
                    received = excluded.received,
                    interval = excluded.interval,
                    final = excluded.final,
                    unclosed = excluded.unclosed
                """,
                (
                    account,
                    host,
                    source,
                    now,
                    request["interval"],
                    int(request.get("final", False)),
                    json.dumps(unclosed),
                ),
            )
            return {"received": now, "recorded": recorded}

    @staticmethod
    def _expired(heartbeat: sqlite3.Row, now: str) -> bool:
        age = _parse_iso(now) - _parse_iso(heartbeat["received"])
        return age > timedelta(seconds=HEARTBEAT_EXPIRY * heartbeat["interval"])

    def _liveness(self, session: sqlite3.Row) -> Liveness | None:
        # Each row has the heartbeat of its host and source (config
        # directory). Only that heartbeat can say that the copy stopped,
        # because the other sources of the host do not list it.
        rows = self._db.execute(
            "SELECT l.host, l.first_listed, l.last_listed, "
            "h.received, h.interval, h.unclosed "
            "FROM session_liveness AS l LEFT JOIN host_heartbeats AS h "
            "ON h.account = ? AND h.host = l.host AND h.source = l.source "
            "WHERE l.session = ? ORDER BY l.last_listed DESC",
            (session["account"], session["id"]),
        ).fetchall()
        if not rows:
            return None
        now = self.now()
        liveness: Liveness
        # A copy can stop on one host while a copy on a different host still
        # runs. Thus a heartbeat that lists the session and has not expired
        # makes it live, whichever copy listed it last.
        for row in rows:
            if (
                row["received"] is not None
                and row["received"] <= row["last_listed"]
                and not self._expired(row, now)
            ):
                liveness = {
                    "state": "live",
                    "host": row["host"],
                    "since": row["first_listed"],
                }
                return self._close_blocked(session, row, liveness)
        row = rows[0]
        if row["received"] is None:
            return None
        if row["received"] > row["last_listed"]:
            # A newer heartbeat of the source did not list the session.
            liveness = {
                "state": "stopped",
                "host": row["host"],
                "since": row["last_listed"],
            }
        else:
            liveness = {
                "state": "unknown",
                "host": row["host"],
                "since": row["received"],
            }
        return self._close_blocked(session, row, liveness)

    @staticmethod
    def _close_blocked(
        session: sqlite3.Row, heartbeat: sqlite3.Row, liveness: Liveness
    ) -> Liveness:
        for item in json.loads(heartbeat["unclosed"]):
            if (
                item["harness"] == session["harness"]
                and item["session_id"] == session["session_id"]
            ):
                liveness["close_blocked"] = item["reason"]
        return liveness

    @_reading
    def status_counts(self, key: SessionKey) -> dict[str, int]:
        """The numbers of status events and liveness rows of a session."""
        session = self._require_session(key)["id"]
        return {
            name: self._db.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE session = ?", (session,)
            ).fetchone()["n"]
            for name, table in (
                ("status_events", "status_events"),
                ("liveness", "session_liveness"),
            )
        }

    # --- Forks ----------------------------------------------------------

    def create_fork(
        self, key: SessionKey, generation: str, offset: int, host: str | None = None
    ) -> ForkEvent:
        """Record a recovery fork of `generation` up to `offset`.

        The new session has no files yet. The client uploads the forked copy
        as the first generation of each file, with the cause `fork`.
        """
        with self._tx():
            session = self._require_session(key)
            gen = self._gen(generation)
            file = self._db.execute(
                "SELECT session FROM files WHERE id = ?", (gen.file,)
            ).fetchone()
            if file is None or file["session"] != session["id"]:
                raise NotFound("generation")
            if not 0 <= offset <= gen.length:
                raise StoreError("bad_request", "offset out of range")
            new_id = str(uuid.uuid4())
            now = self.now()
            self._db.execute(
                "INSERT INTO sessions (account, harness, session_id, created, "
                "forked_from_session, forked_from_generation, forked_from_offset) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    key.account,
                    key.harness,
                    new_id,
                    now,
                    key.session_id,
                    generation,
                    offset,
                ),
            )
            if host is not None:
                # A fork belongs to its creator even before its first upload.
                self._db.execute(
                    "UPDATE sessions SET lease_state = 'held', lease_host = ?, "
                    "lease_since = ? WHERE account = ? AND harness = ? AND session_id = ?",
                    (host, now, key.account, key.harness, new_id),
                )
        return {
            "session_id": new_id,
            "source_generation": generation,
            "source_offset": offset,
            "created": now,
        }

    # --- Grants and read access -----------------------------------------

    def grant(self, key: SessionKey, host: str, granted_by: str) -> None:
        with self._tx():
            session = self._require_session(key)
            self._db.execute(
                "INSERT INTO grants (session, host, granted_by, created) "
                "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (session["id"], host, granted_by, self.now()),
            )

    @_reading
    def can_read(self, key: SessionKey, host: str, scope: ReadScope) -> bool:
        session = self._session_row(key)
        if session is None:
            return False
        return self._can_read_row(session, host, scope)

    def _can_read_row(self, session: sqlite3.Row, host: str, scope: ReadScope) -> bool:
        """See "Scopes" in the spec. `_READABLE` has the rules, so that the
        session list applies the same rules in SQL."""
        if scope == "all":
            return True
        row = self._db.execute(
            f"SELECT EXISTS (SELECT 1 FROM sessions s WHERE s.id = :id AND {_READABLE})",
            {"id": session["id"], "host": host, "granted": int(scope == "granted")},
        ).fetchone()
        return bool(row[0])

    def audit_read(self, key: SessionKey, host: str, what: str) -> None:
        with self._tx():
            session = self._session_row(key)
            if session is None:
                return
            self._db.execute(
                "INSERT INTO read_audit "
                "(ts, account, host, session, harness, session_id, what) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    self.now(),
                    key.account,
                    host,
                    session["id"],
                    key.harness,
                    key.session_id,
                    what,
                ),
            )

    # --- Purge (admin) -----------------------------------------------------

    def _descendants(self, generation: str) -> list[str]:
        """`generation` and every generation forked from it, children first."""
        order: list[str] = []
        stack = [generation]
        while stack:
            current = stack.pop()
            order.append(current)
            stack.extend(
                r["id"]
                for r in self._db.execute(
                    "SELECT id FROM generations WHERE parent = ?", (current,)
                ).fetchall()
            )
        order.reverse()
        return order

    def _drop_generations(self, ids: list[str]) -> int:
        """Delete index rows of generations, children first. Return the
        number of segments removed."""
        segments = 0
        for gid in ids:
            segments += self._db.execute(
                "DELETE FROM segments WHERE generation = ?", (gid,)
            ).rowcount
            self._db.execute("DELETE FROM generations WHERE id = ?", (gid,))
        return segments

    def _unreferenced(self, digests: set[str]) -> list[str]:
        return [
            d
            for d in digests
            if self._db.execute(
                "SELECT 1 FROM blob_versions WHERE sha256 = ? LIMIT 1", (d,)
            ).fetchone()
            is None
        ]

    def _record_purge(self, target: str, reason: str) -> None:
        self._db.execute(
            "INSERT INTO purges (ts, target, reason) VALUES (?, ?, ?)",
            (self.now(), target, reason),
        )

    def _remove_files(self, generations: list[str], digests: list[str]) -> None:
        # After the commit: the index no longer refers to these files, so a
        # reader that gets here finds no row first.
        for gid in generations:
            self._framelog(gid).path.unlink(missing_ok=True)
        # Another process can add a reference after the purge commits.
        # Recheck under a database writer lock, and keep it through unlink.
        with self._tx():
            for digest in self._unreferenced(set(digests)):
                self.objects.remove(digest)

    def purge_session(self, key: SessionKey, reason: str) -> dict[str, int]:
        """Remove a session and all its data. Its id is free again: a host
        that still has the session and syncs it starts a new copy."""
        with self._tx():
            session = self._require_session(key)
            sid = session["id"]
            files = self._db.execute(
                "SELECT id FROM files WHERE session = ?", (sid,)
            ).fetchall()
            generations: list[str] = []
            for f in files:
                roots = [g.id for g in self._gens_of_file(f["id"]) if g.parent is None]
                for root in roots:
                    generations.extend(self._descendants(root))
            segments = self._drop_generations(generations)
            digests = {
                r["sha256"]
                for r in self._db.execute(
                    "SELECT v.sha256 FROM blob_versions v JOIN files f ON f.id = v.file "
                    "WHERE f.session = ?",
                    (sid,),
                ).fetchall()
            }
            versions = 0
            for f in files:
                versions += self._db.execute(
                    "DELETE FROM blob_versions WHERE file = ?", (f["id"],)
                ).rowcount
            for table in ("metadata_events", "placements", "teleports", "grants"):
                self._db.execute(f"DELETE FROM {table} WHERE session = ?", (sid,))
            status_events = self._db.execute(
                "DELETE FROM status_events WHERE session = ?", (sid,)
            ).rowcount
            liveness = self._db.execute(
                "DELETE FROM session_liveness WHERE session = ?", (sid,)
            ).rowcount
            self._db.execute("DELETE FROM files WHERE session = ?", (sid,))
            self._db.execute("DELETE FROM sessions WHERE id = ?", (sid,))
            for table in _COPY_MARKS:
                self._mark_copy(table)
            orphans = self._unreferenced(digests)
            self._record_purge(
                f"session {key.account}/{key.harness}/{key.session_id}", reason
            )
        self._remove_files(generations, orphans)
        return {
            "generations": len(generations),
            "segments": segments,
            "blob_versions": versions,
            "objects": len(orphans),
            "status_events": status_events,
            "liveness": liveness,
        }

    def purge_generation(
        self, key: SessionKey, generation: str, reason: str
    ) -> dict[str, int]:
        """Remove a generation and every generation forked from it (they
        inherit its bytes)."""
        with self._tx():
            gen = self._gen_in_session(key, generation)
            generations = self._descendants(generation)
            segments = self._drop_generations(generations)
            left = self._db.execute(
                "SELECT COUNT(*) FROM generations WHERE file = ?", (gen.file,)
            ).fetchone()[0]
            if left == 0:
                versions = self._db.execute(
                    "SELECT COUNT(*) FROM blob_versions WHERE file = ?", (gen.file,)
                ).fetchone()[0]
                if versions:
                    self._db.execute(
                        "UPDATE files SET kind = 'blob' WHERE id = ?", (gen.file,)
                    )
                else:
                    self._db.execute("DELETE FROM files WHERE id = ?", (gen.file,))
            self._record_purge(
                f"generation {key.account}/{key.harness}/{key.session_id}/"
                f"{generation} (+{len(generations) - 1} forked)",
                reason,
            )
        self._remove_files(generations, [])
        return {"generations": len(generations), "segments": segments}

    def purge_blob_version(
        self, key: SessionKey, path: str, version: int, reason: str
    ) -> dict[str, int]:
        with self._tx():
            session = self._require_session(key)
            file = self._file_row(session["id"], path)
            if file is None:
                raise NotFound("file")
            row = self._db.execute(
                "SELECT sha256 FROM blob_versions WHERE file = ? AND version = ?",
                (file["id"], version),
            ).fetchone()
            if row is None:
                raise NotFound("version")
            self._db.execute(
                "DELETE FROM blob_versions WHERE file = ? AND version = ?",
                (file["id"], version),
            )
            left = self._db.execute(
                "SELECT COUNT(*) FROM blob_versions WHERE file = ?", (file["id"],)
            ).fetchone()[0]
            if left == 0 and file["kind"] == "blob":
                self._db.execute("DELETE FROM files WHERE id = ?", (file["id"],))
            orphans = self._unreferenced({row["sha256"]})
            self._record_purge(
                f"blob {key.account}/{key.harness}/{key.session_id}/{path}@{version}",
                reason,
            )
        self._remove_files([], orphans)
        return {"blob_versions": 1, "objects": len(orphans)}

    # --- Manifest and lists ---------------------------------------------

    @_reading
    def manifest(self, key: SessionKey) -> Manifest:
        session = self._require_session(key)
        files: list[LogManifest | BlobManifest] = []
        for f in self._db.execute(
            "SELECT * FROM files WHERE session = ? ORDER BY path", (session["id"],)
        ).fetchall():
            if f["kind"] == "log":
                gens = self._gens_of_file(f["id"])
                active, _, _ = self._file_branches(f["id"])
                if active is None:
                    continue
                files.append(
                    {
                        "path": f["path"],
                        "kind": "log",
                        "generations": [self._gen_info(g) for g in gens],
                        "active": active.id,
                        "versions": [
                            self._blob_version(v)
                            for v in self._db.execute(
                                "SELECT * FROM blob_versions WHERE file = ? "
                                "ORDER BY version",
                                (f["id"],),
                            ).fetchall()
                        ],
                    }
                )
            else:
                versions = self._db.execute(
                    "SELECT * FROM blob_versions WHERE file = ? ORDER BY version",
                    (f["id"],),
                ).fetchall()
                files.append(
                    {
                        "path": f["path"],
                        "kind": "blob",
                        "versions": [self._blob_version(v) for v in versions],
                    }
                )
        teleports: list[TeleportEvent] = []
        for t in self._db.execute(
            "SELECT * FROM teleports WHERE session = ? ORDER BY created",
            (session["id"],),
        ).fetchall():
            event: TeleportEvent = {
                "id": t["id"],
                "target_host": t["target_host"],
                "target_workspace": t["target_workspace"],
                "steal": bool(t["steal"]),
                "created": t["created"],
            }
            if t["source"] is not None:
                event["source"] = json.loads(t["source"])
            teleports.append(event)
        manifest: Manifest = {
            "harness": key.harness,
            "session_id": key.session_id,
            "metadata": self._latest_metadata(session["id"]),
            "state": self._session_state(session["id"]),
            "placements": self._placements(session["id"]),
            "teleports": teleports,
            "files": files,
        }
        if session["forked_from_session"] is not None:
            manifest["forked_from"] = {
                "session_id": session["forked_from_session"],
                "source_generation": session["forked_from_generation"],
                "source_offset": session["forked_from_offset"],
                "created": session["created"],
            }
        # A heartbeat or a status write must not make a restore stale. An
        # append changes `last_append`, but the files show the append too.
        state = {
            name: value
            for name, value in manifest["state"].items()
            if name not in ("status", "liveness", "last_append")
        }
        hashed = {**manifest, "state": state}
        manifest["revision"] = hashlib.sha256(
            json.dumps(hashed, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return manifest

    @_reading
    def list_sessions(
        self,
        account: str,
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
        before: ListPosition | None = None,
        top_level: bool = False,
        parent: str | None = None,
    ) -> list[SessionSummary]:
        """List readable sessions, newest first. See `list_sessions_page`."""
        return self.list_sessions_page(
            account,
            host,
            scope,
            harness=harness,
            on_host=on_host,
            workspace=workspace,
            since=since,
            title=title,
            statuses=statuses,
            open_only=open_only,
            lease_host=lease_host,
            liveness=liveness,
            limit=limit,
            before=before,
            top_level=top_level,
            parent=parent,
        )[0]

    @_reading
    def list_sessions_page(
        self,
        account: str,
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
        before: ListPosition | None = None,
        top_level: bool = False,
        parent: str | None = None,
    ) -> tuple[list[SessionSummary], ListPosition | None]:
        """List readable sessions, newest first, after the position `before`.

        Return the sessions and the position of the last one, or None for the
        position if the list has no more sessions. Without the `title` and
        `liveness` filters, the position is None exactly at the end of the
        list. With them, the next page can be empty.

        `statuses` and `liveness` can contain `none`, which matches a session
        with no status event or with no liveness. `open_only` keeps the
        sessions whose latest status is not `done`.

        `top_level` keeps the sessions with no parent and no `automation`
        metadata. `parent` keeps the children of the session with that id.

        SQL applies all filters except `title` and `liveness`, and sorts with
        the index `sessions_recent`. Without these two filters, SQL also
        applies the limit. With them, the store reads the rows in batches
        until it has `limit` sessions. Only the sessions that the list
        returns get the queries for their state, metadata, and placement.
        """
        where = ["s.account = :account"]
        params: dict[str, Any] = {"account": account, "host": host}
        if before is not None:
            # The first term lets SQLite start the index scan at the position.
            where.append(
                f"{_SORT_KEY} <= :before_key AND ({_SORT_KEY}, s.harness, "
                "s.session_id) < (:before_key, :before_harness, :before_session)"
            )
            params["before_key"] = before.sort_key
            params["before_harness"] = before.harness
            params["before_session"] = before.session_id
        if harness is not None:
            where.append("s.harness = :harness")
            params["harness"] = harness
        if top_level:
            where.append("s.top_level = 1")
        if parent is not None:
            where.append("s.parent = :parent")
            params["parent"] = parent
        if since is not None:
            where.append(f"{_SORT_KEY} >= :since")
            params["since"] = since
        if lease_host is not None:
            where.append("s.lease_state = 'held' AND s.lease_host = :lease_host")
            params["lease_host"] = lease_host
        if on_host is not None:
            where.append(
                "EXISTS (SELECT 1 FROM placements p "
                "WHERE p.session = s.id AND p.host = :on_host)"
            )
            params["on_host"] = on_host
        if workspace is not None:
            where.append(
                "EXISTS (SELECT 1 FROM placements p "
                "WHERE p.session = s.id AND p.workspace = :workspace)"
            )
            params["workspace"] = workspace
        if statuses is not None:
            names = sorted(statuses - {"none"})
            matches = ["0"]
            for i, name in enumerate(names):
                params[f"status{i}"] = name
            if names:
                listed = ", ".join(f":status{i}" for i in range(len(names)))
                matches.append(f"s.status IN ({listed})")
            if "none" in statuses:
                matches.append("s.status IS NULL")
            where.append(f"({' OR '.join(matches)})")
        if open_only:
            where.append("(s.status IS NULL OR s.status != 'done')")
        if scope != "all":
            # Last, so that the simple filters skip its subqueries.
            where.append(_READABLE)
            params["granted"] = int(scope == "granted")
        sql = (
            f"SELECT s.*, {_SORT_KEY} AS sort_key FROM sessions s "
            f"WHERE {' AND '.join(where)} ORDER BY {_RECENT}"
        )
        in_python = title is not None or liveness is not None
        if not in_python:
            # One more row than the limit tells if the list has more rows.
            sql += " LIMIT :limit + 1"
            params["limit"] = limit
        cursor = self._db.execute(sql, params)

        def candidates() -> Iterator[sqlite3.Row]:
            while rows := cursor.fetchmany(max(limit, 50)):
                yield from rows

        out: list[SessionSummary] = []
        last: sqlite3.Row | None = None
        try:
            rows = candidates()
            for row in rows:
                if len(out) >= limit:
                    break
                metadata = self._clean_metadata(row["metadata"])
                if title is not None and (
                    title.lower() not in (metadata.get("title") or "").lower()
                ):
                    continue
                state, excerpt = self._session_state_and_excerpt(row)
                live = state.get("liveness")
                live_name = live["state"] if live is not None else "none"
                if liveness is not None and live_name not in liveness:
                    continue
                summary: SessionSummary = {
                    "harness": row["harness"],
                    "session_id": row["session_id"],
                    "metadata": metadata,
                    "state": state,
                }
                placement = self._last_placement(row["id"])
                if placement is not None:
                    summary["last_placement"] = placement
                if excerpt is not None:
                    summary["excerpt"] = excerpt
                out.append(summary)
                last = row
            else:
                # The rows ended before the list was full.
                last = None
        finally:
            cursor.close()
        if last is None:
            return out, None
        return out, ListPosition(last["sort_key"], last["harness"], last["session_id"])

    @_reading
    def list_workspaces(
        self, account: str, host: str, scope: ReadScope
    ) -> list[dict[str, str | int]]:
        """Workspaces of the sessions that `host` can read, latest first.

        `sessions` is the number of sessions with a placement in the
        workspace. A session that came back to a workspace counts once.
        """
        readable = "" if scope == "all" else f"AND {_READABLE}"
        rows = self._db.execute(
            f"""
            SELECT p.host AS host, p.workspace AS workspace,
                   MAX(p.last_seen) AS last_seen,
                   COUNT(DISTINCT p.session) AS sessions
            FROM sessions s JOIN placements p ON p.session = s.id
            WHERE s.account = :account {readable}
            GROUP BY p.host, p.workspace
            ORDER BY last_seen DESC, p.host, p.workspace
            """,
            {"account": account, "host": host, "granted": int(scope == "granted")},
        ).fetchall()
        return [
            {
                "host": r["host"],
                "workspace": r["workspace"],
                "last_seen": r["last_seen"],
                "sessions": r["sessions"],
            }
            for r in rows
        ]

    @staticmethod
    def _gen_info(gen: _Gen) -> GenerationInfo:
        info: GenerationInfo = {
            "id": gen.id,
            "cause": gen.cause,  # type: ignore[typeddict-item]
            "length": gen.length,
            "activity": gen.activity,
            "host": gen.host,
            "created": gen.created,
            "last_append": gen.last_append,
        }
        if gen.parent is not None:
            info["parent"] = gen.parent
            info["fork_offset"] = gen.fork_offset
        return info


class _Transaction:
    """`with store._tx():` runs the body in one write transaction on the
    writer connection, under the store lock."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._depth_owner = False
        self._outer: sqlite3.Connection | None = None

    def __enter__(self) -> None:
        store = self._store
        writer = store._writer
        self._outer = getattr(store._local, "db", None)
        if self._outer is not None and self._outer is not writer:
            # The write would not be in the snapshot of the read. A read path
            # that writes must use a write transaction from its start.
            raise RuntimeError("a write transaction inside a read transaction")
        start = time.monotonic()
        store._lock.acquire()
        try:
            if not writer.in_transaction:
                writer.execute("BEGIN IMMEDIATE")
                self._depth_owner = True
        except BaseException:
            store._lock.release()
            raise
        finally:
            _add_wait(True, time.monotonic() - start)
        store._local.db = writer

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        store = self._store
        try:
            if self._depth_owner:
                if exc_type is None:
                    store._writer.execute("COMMIT")
                else:
                    store._writer.execute("ROLLBACK")
        finally:
            store._local.db = self._outer
            store._lock.release()
