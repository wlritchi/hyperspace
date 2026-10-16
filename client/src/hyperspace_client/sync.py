"""The sync engine: upload the local files of a session.

See "Append protocol" in the spec. For a log, the client keeps a cursor: the
generation that it appends to, the length that it uploaded, the file's
device and inode, and the segments that it uploaded. Before an append it
checks that the local file still starts with those segments. If it does not
(Claude Code shortened or rewrote the file), it forks a `rewrite` generation
at the end of the last segment that still matches.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from hyperspace_client import status
from hyperspace_client.adapters import Adapter, LocalFile, LocalSession
from hyperspace_client.api import Api, ApiError, AppendConflict
from hyperspace_client.state import (
    LEASE_FIELDS,
    Cursor,
    Segment,
    SessionRecord,
    State,
)
from hyperspace_protocol import MAX_BODY, SessionMetadata, SessionStateBody

log = logging.getLogger("hyperspace")

# Upload at most this much in one append. A smaller size than the server
# limit leaves room for headers and keeps each request short.
CHUNK = MAX_BODY // 2
# A record that is larger than CHUNK goes up in one append if it is at most
# this size. The margin keeps the compressed body below the server limit,
# also for data that does not compress.
RECORD_MAX = MAX_BODY - MAX_BODY // 128
# A rewritten file with no new complete record gets its fork only after it
# has not changed for this many seconds (see `Syncer.sync_log`).
REWRITE_SETTLE = 60.0


@dataclass
class Context:
    api: Api
    state: State
    adapter: Adapter
    host: str


@dataclass
class SyncResult:
    uploaded: int = 0
    files: int = 0
    session: SessionStateBody | None = None
    diverged: bool = False
    errors: list[str] = field(default_factory=list[str])


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(path: Path, start: int, end: int | None = None) -> bytes:
    with open(path, "rb") as f:
        f.seek(start)
        return f.read() if end is None else f.read(end - start)


def _chunk_size(data: bytes, size: int, limit: int) -> int:
    """The length of the next piece of `data` to upload. The piece ends at
    the last newline in the first `size` bytes. If there is no such newline,
    the piece is the first record, when that record is at most `limit`
    bytes. Otherwise the piece is `size` bytes of a record that is too large
    for one request."""
    if len(data) <= size:
        return len(data)
    cut = data.rfind(b"\n", 0, size)
    if cut >= 0:
        return cut + 1
    cut = data.find(b"\n", size, limit)
    return cut + 1 if cut >= 0 else size


def _record_start(path: Path, offset: int) -> int:
    """The offset where the record that holds byte `offset` starts: just
    after the last newline before `offset`, or 0."""
    pos = offset
    with open(path, "rb") as f:
        while pos > 0:
            start = max(0, pos - CHUNK)
            f.seek(start)
            cut = f.read(pos - start).rfind(b"\n")
            if cut >= 0:
                return start + cut + 1
            pos = start
    return 0


def _records_ending(
    path: Path, record: int, offset: int, piece: bytes
) -> tuple[bytes, int]:
    """The complete records that end in `piece`, which is at `offset` in
    the file at `path`, and the start of the record after them. `record` is
    the start of the record that holds `offset`. It can be before `offset`,
    when an earlier piece holds the start of that record."""
    cut = piece.rfind(b"\n")
    if cut < 0:
        return b"", record
    stop = offset + cut + 1
    if record == offset:
        return piece[: cut + 1], stop
    return _read(path, record, stop), stop


def _matching_prefix(path: Path, segments: list[Segment]) -> tuple[int, list[Segment]]:
    """The end of the longest run of uploaded segments, from offset 0, that
    the local file still holds. Return (end, the matching segments)."""
    kept: list[Segment] = []
    end = 0
    with open(path, "rb") as f:
        for seg in segments:
            if seg.offset != end:
                break
            f.seek(seg.offset)
            if _sha(f.read(seg.length)) != seg.sha256:
                break
            kept.append(seg)
            end = seg.offset + seg.length
    return end, kept


def _unchanged(cursor: Cursor, st: os.stat_result) -> bool:
    return (cursor.dev, cursor.ino, cursor.size, cursor.mtime_ns) == (
        st.st_dev,
        st.st_ino,
        st.st_size,
        st.st_mtime_ns,
    )


def _mark_read(cursor: Cursor, st: os.stat_result) -> None:
    """Record the stat from before the read, so that a change during the
    read shows as a difference at the next sync."""
    cursor.size, cursor.mtime_ns = st.st_size, st.st_mtime_ns


def _last_segment_ok(path: Path, cursor: Cursor) -> bool:
    if not cursor.segments:
        return True
    seg = cursor.segments[-1]
    return _sha(_read(path, seg.offset, seg.offset + seg.length)) == seg.sha256


class Syncer:
    def __init__(
        self,
        ctx: Context,
        session: LocalSession,
        workspace: str,
        empty_rewrites: bool = False,
    ) -> None:
        """`empty_rewrites`: fork a `rewrite` generation at once also when
        the rewritten file has no new complete record. Without it, the fork
        waits for such a record or for the file to settle (see
        `sync_log`)."""
        self.ctx = ctx
        self.session = session
        self.workspace = workspace
        self.empty_rewrites = empty_rewrites
        self.result = SyncResult()

    @property
    def _h(self) -> str:
        return self.session.harness

    @property
    def _sid(self) -> str:
        return self.session.session_id

    # --- Logs ------------------------------------------------------------------

    def _new_cursor(self, f: LocalFile, generation: str, st: os.stat_result) -> Cursor:
        return Cursor(
            local_path=str(f.path),
            server=self.ctx.api.cache_identity,
            harness=self._h,
            session_id=self._sid,
            rel_path=f.rel_path,
            generation=generation,
            length=0,
            dev=st.st_dev,
            ino=st.st_ino,
        )

    def _adopt_remote(
        self, f: LocalFile, st: os.stat_result, local_size: int
    ) -> Cursor:
        """The server already has this file, but the client has no cursor
        (for example, its state was lost). Find how much of the server's
        active branch the local file holds, and continue from there."""
        manifest = self.ctx.api.manifest(self._h, self._sid)
        entry = next(
            (
                e
                for e in manifest["files"]
                if e["path"] == f.rel_path and e["kind"] == "log"
            ),
            None,
        )
        if entry is None or entry["kind"] != "log":
            raise ApiError(
                409, "exists", "the server has the file but no active branch"
            )
        active = entry["active"]
        length = next(g["length"] for g in entry["generations"] if g["id"] == active)
        common = 0
        limit = min(length, local_size)
        while common < limit:
            end = min(common + CHUNK, limit)
            remote = self.ctx.api.read_log(self._h, self._sid, active, common, end)
            local = _read(f.path, common, end)
            if remote == local:
                common = end
                continue
            for i, (a, b) in enumerate(zip(remote, local, strict=False)):
                if a != b:
                    common += i
                    break
            else:
                common += min(len(remote), len(local))
            break
        cursor = self._new_cursor(f, active, st)
        if common == length:
            cursor.length = length
        else:
            gen = self.ctx.api.create_generation(
                self._h,
                self._sid,
                self.workspace,
                f.rel_path,
                "diverge",
                active,
                common,
            )
            cursor.generation = gen["id"]
            cursor.length = common
            self.result.diverged = True
        if cursor.length:
            cursor.segments = [
                Segment(0, cursor.length, _sha(_read(f.path, 0, cursor.length)))
            ]
        return cursor

    def _prepare_cursor(
        self, f: LocalFile, st: os.stat_result
    ) -> tuple[Cursor | None, bool]:
        """The cursor of `f`, and True if the file was rewritten. For a
        rewrite, the cursor moves back to the end of the segments that still
        match, and its generation is the one to fork from. It is not saved:
        `_fork_rewrite` saves it."""
        state = self.ctx.state
        cursor = state.cursor(f.path, self.ctx.api.cache_identity)
        if cursor is None or (cursor.harness, cursor.session_id, cursor.rel_path) != (
            self._h,
            self._sid,
            f.rel_path,
        ):
            return None, False
        if _unchanged(cursor, st):
            return cursor, False
        rewritten = (
            (st.st_dev, st.st_ino) != (cursor.dev, cursor.ino)
            or st.st_size < cursor.length
            or not _last_segment_ok(f.path, cursor)
        )
        if not rewritten:
            return cursor, False
        end, kept = _matching_prefix(f.path, cursor.segments)
        cursor.length = end
        cursor.segments = kept
        cursor.dev, cursor.ino = st.st_dev, st.st_ino
        return cursor, True

    def _fork_rewrite(self, f: LocalFile, cursor: Cursor) -> None:
        log.info("%s changed on disk; fork at %d", f.path, cursor.length)
        gen = self.ctx.api.create_generation(
            self._h,
            self._sid,
            self.workspace,
            f.rel_path,
            "rewrite",
            cursor.generation,
            cursor.length,
        )
        cursor.generation = gen["id"]
        self.ctx.state.save_cursor(cursor)

    def sync_log(self, f: LocalFile, metadata: dict[str, Any]) -> dict[str, Any]:
        try:
            st = f.path.stat()
        except FileNotFoundError:
            return metadata
        cursor, rewritten = self._prepare_cursor(f, st)
        if cursor is not None and not rewritten and _unchanged(cursor, st):
            return metadata
        start = cursor.length if cursor is not None else 0
        pending = _read(f.path, start)
        end = start + self.ctx.adapter.commit_boundary(f.rel_path, pending)
        if end <= start:
            if (
                rewritten
                and not self.empty_rewrites
                and 0 <= time.time() - st.st_mtime < REWRITE_SETTLE
            ):
                # A file that Claude Code is writing again (for example, set
                # aside and started again) can be empty or shorter for a
                # moment. Its fork would become the active branch, which a
                # restore then gives. The saved cursor does not change, so
                # the next sync finds the rewrite again. A file with an mtime
                # in the future did not come from such a write, and does not
                # wait.
                return metadata
            if cursor is not None:
                if rewritten:
                    self._fork_rewrite(f, cursor)
                _mark_read(cursor, st)
                self.ctx.state.save_cursor(cursor)
            return metadata
        if cursor is not None and rewritten:
            self._fork_rewrite(f, cursor)
        if cursor is None:
            try:
                gen = self.ctx.api.create_generation(
                    self._h, self._sid, self.workspace, f.rel_path, "root"
                )
                cursor = self._new_cursor(f, gen["id"], st)
            except ApiError as exc:
                if exc.code != "exists":
                    raise
                cursor = self._adopt_remote(f, st, end)
            self.ctx.state.save_cursor(cursor)
            if cursor.length:
                # Bytes that the server had before: they count for metadata.
                metadata = self.ctx.adapter.update_metadata(
                    metadata, f.rel_path, _read(f.path, 0, cursor.length)
                )
        # Each record goes with the append that holds its end. That append
        # carries the activity of the record, and the metadata includes the
        # record after that append. A record that is too large for one
        # request goes up in pieces, and two hosts can fork between two
        # pieces of a record. The last piece is after such a fork, so each
        # branch keeps the activity of the record that it does not share.
        record = _record_start(f.path, cursor.length)
        while cursor.length < end:
            before = cursor.length
            data = _read(f.path, before, min(end, before + RECORD_MAX))
            piece = data[: _chunk_size(data, CHUNK, RECORD_MAX)]
            records, _ = _records_ending(f.path, record, before, piece)
            self._append(
                f, cursor, piece, self.ctx.adapter.activity(f.rel_path, records)
            )
            if cursor.length > before:
                if cursor.length != before + len(piece):
                    piece = _read(f.path, before, cursor.length)
                records, record = _records_ending(f.path, record, before, piece)
                metadata = self.ctx.adapter.update_metadata(
                    metadata, f.rel_path, records
                )
        _mark_read(cursor, st)
        self.ctx.state.save_cursor(cursor)
        self.result.files += 1
        return metadata

    def _append(
        self, f: LocalFile, cursor: Cursor, chunk: bytes, activity: int
    ) -> None:
        """Append `chunk`, with `activity` activity records, at the cursor,
        or move the cursor past bytes that the server already has. On a
        conflict with other bytes, fork a `diverge` generation and append
        there."""
        api = self.ctx.api
        try:
            response = api.append(
                self._h,
                self._sid,
                self.workspace,
                cursor.generation,
                cursor.length,
                chunk,
                activity,
            )
        except AppendConflict as conflict:
            if conflict.session is not None:
                self.result.session = conflict.session
            if self._adopt_conflict(f, cursor, conflict.length):
                return
            gen = api.create_generation(
                self._h,
                self._sid,
                self.workspace,
                f.rel_path,
                "diverge",
                cursor.generation,
                cursor.length,
            )
            log.warning(
                "%s: another host appended to %s; new branch %s",
                self._sid,
                cursor.generation,
                gen["id"],
            )
            cursor.generation = gen["id"]
            self.ctx.state.save_cursor(cursor)
            self.result.diverged = True
            return
        # For a retry, the response holds the current generation length,
        # which can be larger than the end of `chunk`. The server checked
        # only the bytes of `chunk`, so the cursor moves past only those.
        # The next append finds any other bytes as a conflict.
        cursor.segments.append(Segment(cursor.length, len(chunk), _sha(chunk)))
        cursor.length += len(chunk)
        self.ctx.state.save_cursor(cursor)
        self.result.uploaded += len(chunk)
        self.result.session = response["session"]

    def _adopt_conflict(self, f: LocalFile, cursor: Cursor, length: int) -> bool:
        """After a 409: if the server's bytes from the cursor to `length` are
        the same as the local bytes (an earlier append committed, but the
        cursor did not save), move the cursor to `length` and return True."""
        if length <= cursor.length:
            return False
        try:
            local_size = f.path.stat().st_size
        except FileNotFoundError:
            return False
        if length > local_size:
            return False
        offset = cursor.length
        digest = hashlib.sha256()
        while offset < length:
            end = min(offset + CHUNK, length)
            remote = self.ctx.api.read_log(
                self._h, self._sid, cursor.generation, offset, end
            )
            local = _read(f.path, offset, end)
            if remote != local:
                return False
            digest.update(local)
            offset = end
        cursor.segments.append(
            Segment(cursor.length, length - cursor.length, digest.hexdigest())
        )
        cursor.length = length
        self.ctx.state.save_cursor(cursor)
        return True

    # --- Blobs ---------------------------------------------------------------------

    def _as_log(self, f: LocalFile) -> bool:
        """A file of the blob kind that is too large for one request goes
        up as a log, in chunks. The server then keeps the path as a log, so
        a file with a log cursor stays a log."""
        if self.ctx.state.cursor(f.path, self.ctx.api.cache_identity) is not None:
            return True
        try:
            return f.path.stat().st_size > MAX_BODY
        except FileNotFoundError:
            return False

    def sync_blob(self, f: LocalFile) -> bool:
        """Upload a changed blob. Return False if the server keeps the path
        as a log, for example after a lost log cursor."""
        try:
            st = f.path.stat()
        except FileNotFoundError:
            return True
        known = self.ctx.state.blob(
            f.path, self.ctx.api.cache_identity, self._h, self._sid, f.rel_path
        )
        if known is not None and known[:2] == (st.st_size, st.st_mtime_ns):
            return True
        if st.st_size > MAX_BODY:
            self.result.errors.append(f"{f.rel_path}: larger than {MAX_BODY} bytes")
            return True
        data = f.path.read_bytes()
        digest = _sha(data)
        if known is None or known[2] != digest:
            try:
                self.ctx.api.put_blob(
                    self._h, self._sid, self.workspace, f.rel_path, data
                )
            except ApiError as exc:
                if exc.code != "wrong_kind":
                    raise
                return False
            self.result.uploaded += len(data)
            self.result.files += 1
        self.ctx.state.save_blob(
            f.path,
            st.st_size,
            st.st_mtime_ns,
            digest,
            self.ctx.api.cache_identity,
            self._h,
            self._sid,
            f.rel_path,
        )
        return True

    # --- Session -------------------------------------------------------------------

    def run(self) -> SyncResult:
        state = self.ctx.state
        if state.get(f"restore_pending:{self._h}:{self._sid}"):
            raise ApiError(
                409, "restore_pending", "retry restore before syncing this session"
            )
        record = state.session(self._h, self._sid)
        record.workspace = self.workspace
        record.transcript = str(self.session.transcript)
        metadata = dict(record.metadata)
        files = self.ctx.adapter.files(self.session, metadata)
        # The transcript first: its records give the plan slug, and the
        # server creates the session and its lease from the first append.
        for f in files:
            if f.kind == "log":
                metadata = self.sync_log(f, metadata)
        for f in self.ctx.adapter.files(self.session, metadata):
            if f.kind != "blob":
                continue
            if self._as_log(f) or not self.sync_blob(f):
                metadata = self.sync_log(LocalFile(f.rel_path, "log", f.path), metadata)
        server_meta = {"workspace": self.workspace, **_server_metadata(metadata)}
        if server_meta != record.sent_metadata and self.result.files:
            self.ctx.api.add_metadata(
                self._h, self._sid, self.workspace, cast(SessionMetadata, server_meta)
            )
            record.sent_metadata = server_meta
        record.metadata = metadata
        fields = ("workspace", "transcript", "metadata", "sent_metadata")
        if self.result.session is not None:
            _note_session_state(record, self.result.session, self.ctx.host)
            status.observe(
                state, record, self.result.session.get("status"), self.ctx.host
            )
            fields += (*LEASE_FIELDS, "held_lease")
        # Only the columns that this sync sets: a hook that does not hold the
        # lock can save a newer lease state during the sync.
        state.save_session(record, fields)
        status.send_wanted(self.ctx, self.session)
        return self.result


def _server_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in metadata.items() if not k.startswith("_")}


def _note_session_state(
    record: SessionRecord, body: SessionStateBody, host: str
) -> None:
    lease = body["lease"]
    record.lease_state = lease["state"]
    record.lease_host = lease.get("host")
    if record.lease_state == "held" and record.lease_host == host:
        record.held_lease = True
    record.state = body["state"]
    record.state_updated = time.time()


def register_host(ctx: Context, kind: str, force: bool = False) -> None:
    """Register this host with the server, at most once a day."""
    info: dict[str, Any] = {
        "kind": kind,
        "home": str(Path.home()),
        "os": os.uname().sysname.lower(),
        "config_dirs": [str(getattr(ctx.adapter, "config_dir", ""))],
    }
    stamp = json.dumps(info, sort_keys=True)
    last = ctx.state.get("host_registered")
    if not force and last is not None:
        when, _, previous = last.partition(" ")
        if previous == stamp and time.time() - float(when) < 86_400:
            return
    ctx.api.register_host(info)  # type: ignore[arg-type]
    ctx.state.put("host_registered", f"{time.time()} {stamp}")
