# vim: filetype=python
"""The Hyperspace store: the SQLite index, generation files, and blobs.

The store implements the model in the spec: sessions, log files as trees of
generations, blob versions, placements, leases, conflicts, recovery forks,
grants, metadata events, and the read audit. It never parses file content.

All public methods are safe to call from more than one thread. Writes take
one lock, so there is one writer at a time.
"""

from __future__ import annotations

import functools
import hashlib
import json
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
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
    HostInfo,
    HostRecord,
    Lease,
    LogManifest,
    Manifest,
    Placement,
    PlacementCause,
    ReadScope,
    SessionMetadata,
    SessionStateBody,
    SessionSummary,
    TeleportEvent,
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
]


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


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_urlsafe(12)}"


def _locked[**P, R](
    method: Callable[Concatenate[Store, P], R],
) -> Callable[Concatenate[Store, P], R]:
    """Run a read method under the store lock. All threads share one SQLite
    connection, so a read must not run in the middle of a write."""

    @functools.wraps(method)
    def wrapper(self: Store, *args: P.args, **kwargs: P.kwargs) -> R:
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class Store:
    def __init__(
        self,
        data_dir: Path,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.data_dir = data_dir
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = data_dir / "logs"
        self.objects = ObjectStore(data_dir / "objects")
        self._db = sqlite3.connect(
            data_dir / "index.sqlite3",
            check_same_thread=False,
            isolation_level=None,
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA synchronous = FULL")
        # The admin scripts open the same index while the server runs.
        self._db.execute("PRAGMA busy_timeout = 10000")
        self._migrate()
        # Recovery (see `recover`) is not done here: only the server calls it,
        # at startup. An admin script that runs beside the server must not cut
        # a frame that the server is writing.

    # --- Infrastructure -------------------------------------------------

    def close(self) -> None:
        self._db.close()

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

    def _tx(self) -> _Transaction:
        return _Transaction(self)

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

    @_locked
    def get_host(self, account: str, name: str) -> HostRecord | None:
        row = self._db.execute(
            "SELECT * FROM hosts WHERE account = ? AND name = ?", (account, name)
        ).fetchone()
        if row is None:
            return None
        return self._host_record(row)

    @_locked
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

    @_locked
    def session_exists(self, key: SessionKey) -> bool:
        return self._session_row(key) is not None

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
        than the parent's length.
        """
        with self._tx():
            session = self._ensure_session(key)
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
            frame_offset, frame_length = self._framelog(generation).append(data)
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

    @_locked
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

    @_locked
    def read_blob(self, key: SessionKey, path: str, version: int | None) -> bytes:
        session = self._require_session(key)
        file = self._file_row(session["id"], path)
        if file is None or file["kind"] != "blob":
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
        with self._tx():
            session = self._ensure_session(key)
            self._db.execute(
                "INSERT INTO metadata_events (session, host, received, body) "
                "VALUES (?, ?, ?, ?)",
                (session["id"], writer.host, self.now(), json.dumps(metadata)),
            )

    def _latest_metadata(self, session: int) -> SessionMetadata:
        row = self._db.execute(
            "SELECT body FROM metadata_events WHERE session = ? "
            "ORDER BY id DESC LIMIT 1",
            (session,),
        ).fetchone()
        if row is None:
            return {}
        metadata: SessionMetadata = json.loads(row["body"])
        return metadata

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
        out: list[Placement] = []
        for r in rows:
            p: Placement = {
                "host": r["host"],
                "workspace": r["workspace"],
                "cause": r["cause"],
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
            }
            if r["cwd"] is not None:
                p["cwd"] = r["cwd"]
            out.append(p)
        return out

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
        self, key: SessionKey, writer: Writer, steal: bool
    ) -> tuple[TeleportEvent, SessionStateBody]:
        """Take the lease for the writer's host and record a teleport."""
        with self._tx():
            session = self._require_session(key)
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
                "id": _new_id("t"),
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
        row = self._db.execute(
            "SELECT COALESCE(SUM(activity), 0) AS a FROM segments "
            "WHERE generation = ? AND offset >= ? AND (? IS NULL OR offset < ?)",
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

    def _branch_activity(self, lineage: list[_Step], after: int) -> int:
        """Activity in a lineage at offsets at or after `after`."""
        total = 0
        for step in lineage:
            if step.exit is not None and step.exit <= after:
                continue
            total += self._activity_after(step.gen, after, step.exit)
        return total

    @staticmethod
    def _divergence(a: list[_Step], b: list[_Step]) -> int:
        """The offset where two lineages stop sharing bytes."""
        i = 0
        while i < min(len(a), len(b)) and a[i].gen.id == b[i].gen.id:
            i += 1
        last_a, last_b = a[i - 1], b[i - 1]
        exits = [e for e in (last_a.exit, last_b.exit) if e is not None]
        return min(exits) if exits else last_a.gen.length

    def _candidates(self, file: int) -> tuple[list[_Gen], dict[str, _Gen]]:
        """Return the branch tips of a log.

        A generation is a tip if it has bytes that no child inherits (or no
        children). A tip whose tail a `rewrite` child replaced, with no
        append after that child was created, is retired and is not a tip.
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
                k.cause == "rewrite" and k.created >= g.last_append for k in kids
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
            others = [o for o in tips if o.id != t.id]
            activity = 0
            if others:
                after = min(
                    self._divergence(lineages[t.id], lineages[o.id]) for o in others
                )
                activity = self._branch_activity(lineages[t.id], after)
            is_meaningful = activity >= MEANINGFUL_ACTIVITY
            if is_meaningful:
                meaningful.append((t, activity))
            scored.append((int(is_meaningful), t.last_append, t))
        scored.sort(key=lambda s: (s[0], s[1]))
        active = scored[-1][2]
        conflicted = False
        for i, (a, _) in enumerate(meaningful):
            for b, _ in meaningful[i + 1 :]:
                d = self._divergence(lineages[a.id], lineages[b.id])
                if (
                    self._branch_activity(lineages[a.id], d) >= MEANINGFUL_ACTIVITY
                    and self._branch_activity(lineages[b.id], d) >= MEANINGFUL_ACTIVITY
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
        body: SessionStateBody = {"state": row["state"], "lease": self._lease(row)}
        if row["state"] == "conflicted":
            branches: list[Branch] = []
            for f in self._db.execute(
                "SELECT id FROM files WHERE session = ? AND kind = 'log'", (session,)
            ).fetchall():
                branches.extend(self._file_branches(f["id"])[1])
            body["branches"] = branches
        return body

    @_locked
    def session_state(self, key: SessionKey) -> SessionStateBody:
        return self._session_state(self._require_session(key)["id"])

    # --- Forks ----------------------------------------------------------

    def create_fork(self, key: SessionKey, generation: str, offset: int) -> ForkEvent:
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

    @_locked
    def can_read(self, key: SessionKey, host: str, scope: ReadScope) -> bool:
        session = self._session_row(key)
        if session is None:
            return False
        return self._can_read_row(session, host, scope)

    def _can_read_row(self, session: sqlite3.Row, host: str, scope: ReadScope) -> bool:
        if scope == "all":
            return True
        # Only a held lease gives read access. A lease reserved for a host
        # does not: a surrender needs only the append scope, so a reserve
        # must not work as a grant.
        if session["lease_state"] == "held" and session["lease_host"] == host:
            return True
        placed = self._db.execute(
            "SELECT 1 FROM placements WHERE session = ? AND host = ? LIMIT 1",
            (session["id"], host),
        ).fetchone()
        if placed is not None:
            return True
        wrote = self._db.execute(
            "SELECT 1 FROM generations g JOIN files f ON f.id = g.file "
            "WHERE f.session = ? AND g.host = ? LIMIT 1",
            (session["id"], host),
        ).fetchone()
        if wrote is not None:
            return True
        if scope == "granted":
            granted = self._db.execute(
                "SELECT 1 FROM grants WHERE session = ? AND host = ?",
                (session["id"], host),
            ).fetchone()
            return granted is not None
        return False

    def audit_read(self, key: SessionKey, host: str, what: str) -> None:
        session = self._session_row(key)
        if session is None:
            return
        with self._tx():
            self._db.execute(
                "INSERT INTO read_audit (ts, account, host, session, what) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.now(), key.account, host, session["id"], what),
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
        for digest in digests:
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
            self._db.execute("DELETE FROM files WHERE session = ?", (sid,))
            self._db.execute("DELETE FROM sessions WHERE id = ?", (sid,))
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
            if file is None or file["kind"] != "blob":
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
            if left == 0:
                self._db.execute("DELETE FROM files WHERE id = ?", (file["id"],))
            orphans = self._unreferenced({row["sha256"]})
            self._record_purge(
                f"blob {key.account}/{key.harness}/{key.session_id}/{path}@{version}",
                reason,
            )
        self._remove_files([], orphans)
        return {"blob_versions": 1, "objects": len(orphans)}

    # --- Manifest and lists ---------------------------------------------

    @_locked
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
        return manifest

    @_locked
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
        limit: int = 200,
    ) -> list[SessionSummary]:
        rows = self._db.execute(
            "SELECT * FROM sessions WHERE account = ? "
            "AND (? IS NULL OR harness = ?) "
            "AND (? IS NULL OR COALESCE(last_activity, created) >= ?) "
            "ORDER BY COALESCE(last_activity, created) DESC",
            (account, harness, harness, since, since),
        ).fetchall()
        out: list[SessionSummary] = []
        for row in rows:
            if not self._can_read_row(row, host, scope):
                continue
            placements = self._placements(row["id"])
            if on_host is not None and not any(
                p["host"] == on_host for p in placements
            ):
                continue
            if workspace is not None and not any(
                p["workspace"] == workspace for p in placements
            ):
                continue
            metadata = self._latest_metadata(row["id"])
            if (
                title is not None
                and title.lower() not in (metadata.get("title") or "").lower()
            ):
                continue
            summary: SessionSummary = {
                "harness": row["harness"],
                "session_id": row["session_id"],
                "metadata": metadata,
                "state": self._session_state(row["id"]),
            }
            if placements:
                summary["last_placement"] = placements[-1]
            out.append(summary)
            if len(out) >= limit:
                break
        return out

    @_locked
    def list_workspaces(
        self, account: str, host: str, scope: ReadScope
    ) -> list[dict[str, str | int]]:
        """Workspaces of the sessions that `host` can read."""
        readable = [
            row["id"]
            for row in self._db.execute(
                "SELECT * FROM sessions WHERE account = ?", (account,)
            ).fetchall()
            if self._can_read_row(row, host, scope)
        ]
        found: dict[tuple[str, str], dict[str, str | int]] = {}
        for session in readable:
            for p in self._placements(session):
                entry = found.setdefault(
                    (p["host"], p["workspace"]),
                    {
                        "host": p["host"],
                        "workspace": p["workspace"],
                        "last_seen": p["last_seen"],
                        "sessions": 0,
                    },
                )
                entry["sessions"] = int(entry["sessions"]) + 1
                entry["last_seen"] = max(str(entry["last_seen"]), p["last_seen"])
        return sorted(found.values(), key=lambda e: str(e["last_seen"]), reverse=True)

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
    """`with store._tx():` runs the body in one write transaction."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._depth_owner = False

    def __enter__(self) -> None:
        self._store._lock.acquire()
        try:
            if not self._store._db.in_transaction:
                self._store._db.execute("BEGIN IMMEDIATE")
                self._depth_owner = True
        except BaseException:
            self._store._lock.release()
            raise

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if self._depth_owner:
                if exc_type is None:
                    self._store._db.execute("COMMIT")
                else:
                    self._store._db.execute("ROLLBACK")
        finally:
            self._store._lock.release()
