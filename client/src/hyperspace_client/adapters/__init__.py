"""Harness adapters. Each adapter holds all knowledge of one harness's files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import quote, unquote

FileKind = Literal["log", "blob"]

# Characters that a session file path segment can hold without encoding. See
# validate_file_path in hyperspace-protocol.
_SAFE = "._@+=,~-"


def encode_segment(segment: str) -> str:
    """Percent-encode a local file name for use in a session file path."""
    return quote(segment, safe=_SAFE)


def decode_segment(segment: str) -> str:
    return unquote(segment)


@dataclass(frozen=True)
class LocalSession:
    harness: str
    session_id: str
    transcript: Path


@dataclass(frozen=True)
class LocalFile:
    rel_path: str
    kind: FileKind
    path: Path


class Adapter(Protocol):
    harness: str

    def discover(self) -> list[LocalSession]:
        """The sessions on this host."""
        ...

    def session_for_transcript(self, path: Path) -> LocalSession | None: ...

    def files(self, session: LocalSession, metadata: dict[str, Any]) -> list[LocalFile]:
        """The local files of a session, with their session file paths."""
        ...

    def commit_boundary(self, rel_path: str, data: bytes) -> int:
        """The number of leading bytes of `data` that are safe to upload: for
        a log of records, up to the end of the last complete record."""
        ...

    def activity(self, rel_path: str, data: bytes) -> int:
        """The number of activity records in `data`."""
        ...

    def update_metadata(
        self, metadata: dict[str, Any], rel_path: str, data: bytes
    ) -> dict[str, Any]:
        """Return `metadata` updated with the records in `data`. Keys that
        start with `_` are for the adapter and are not sent to the server."""
        ...

    def first_cwd(self, session: LocalSession) -> str | None:
        """The working directory of the session's first record, if known."""
        ...
