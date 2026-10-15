"""Wire types, constants, and validation for the Hyperspace HTTP API.

The server and the client both import this module. It has no runtime
dependencies. Request and response bodies are JSON objects; the `TypedDict`
classes here give their shapes. Times are ISO 8601 strings in UTC with a `Z`
suffix.
"""

from __future__ import annotations

import base64
import binascii
import re
from typing import Any, Literal, NotRequired, TypedDict, cast

API_PREFIX = "/v1"

# Headers on file-data requests.
HEADER_BASE_LENGTH = "Hyperspace-Base-Length"
HEADER_SHA256 = "Hyperspace-Sha256"
HEADER_ACTIVITY = "Hyperspace-Activity"
HEADER_WORKSPACE = "Hyperspace-Workspace"
HEADER_WORKSPACE_UTF8 = "Hyperspace-Workspace-Utf8"
CAP_WORKSPACE_UTF8 = "workspace-utf8"
CAPABILITIES = [CAP_WORKSPACE_UTF8]

# One request carries at most this much uncompressed file data. The client
# splits larger appends.
MAX_BODY = 8 * 1024 * 1024

MAX_SESSION_ID_LENGTH = 128
MAX_HOST_NAME_LENGTH = 64
MAX_HARNESS_LENGTH = 32
MAX_FILE_PATH_LENGTH = 512
MAX_FILE_PATH_SEGMENTS = 32
MAX_WORKSPACE_LENGTH = 4096

FileKind = Literal["log", "blob"]
HostKind = Literal["workstation", "runner"]
ReadScope = Literal["own", "granted", "all"]
Scope = Literal["append", "read", "grant"]
GenerationCause = Literal["root", "rewrite", "diverge", "fork"]
PlacementCause = Literal["start", "move", "teleport", "steal", "diverge"]
SessionState = Literal["normal", "conflicted"]
LeaseState = Literal["held", "reserved", "free"]

FILE_KINDS: tuple[FileKind, ...] = ("log", "blob")
HOST_KINDS: tuple[HostKind, ...] = ("workstation", "runner")
READ_SCOPES: tuple[ReadScope, ...] = ("own", "granted", "all")


class ValidationError(ValueError):
    """A name or a path that the protocol does not permit."""


_SESSION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_HOST_NAME_RE = re.compile(r"[a-z0-9][a-z0-9._-]*")
_HARNESS_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
# Characters that a session file path segment can contain. Adapters
# percent-encode other characters.
_SEGMENT_RE = re.compile(r"[A-Za-z0-9._@+=,~%-]+")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def validate_session_id(value: str) -> str:
    """Return `value` if it is a valid session id, else raise."""
    if not 1 <= len(value) <= MAX_SESSION_ID_LENGTH:
        raise ValidationError("session id length must be 1 to 128")
    if not _SESSION_ID_RE.fullmatch(value):
        raise ValidationError(f"invalid session id: {value!r}")
    return value


def validate_host_name(value: str) -> str:
    """Return `value` if it is a valid host name, else raise.

    Host names are lowercase. The client lowercases the system hostname.
    """
    if not 1 <= len(value) <= MAX_HOST_NAME_LENGTH:
        raise ValidationError("host name length must be 1 to 64")
    if not _HOST_NAME_RE.fullmatch(value):
        raise ValidationError(f"invalid host name: {value!r}")
    return value


def validate_harness(value: str) -> str:
    """Return `value` if it is a valid harness name, else raise."""
    if not 1 <= len(value) <= MAX_HARNESS_LENGTH:
        raise ValidationError("harness name length must be 1 to 32")
    if not _HARNESS_RE.fullmatch(value):
        raise ValidationError(f"invalid harness name: {value!r}")
    return value


def validate_id(value: str) -> str:
    """Return `value` if it is a valid server id (generation, teleport)."""
    if not _ID_RE.fullmatch(value):
        raise ValidationError(f"invalid id: {value!r}")
    return value


def validate_file_path(value: str) -> str:
    """Return `value` if it is a valid session file path, else raise.

    A session file path is relative, uses `/` as the separator, and has no
    empty, `.`, or `..` segments. The server uses these paths only as keys,
    never as file system paths, but the client writes files from them, so
    both sides check them.
    """
    if not 1 <= len(value) <= MAX_FILE_PATH_LENGTH:
        raise ValidationError("file path length must be 1 to 512")
    segments = value.split("/")
    if len(segments) > MAX_FILE_PATH_SEGMENTS:
        raise ValidationError("file path has too many segments")
    for segment in segments:
        if segment in ("", ".", "..") or not _SEGMENT_RE.fullmatch(segment):
            raise ValidationError(f"invalid file path: {value!r}")
    return value


def validate_sha256(value: str) -> str:
    """Return `value` if it is a lowercase hex sha256 digest, else raise."""
    if not _SHA256_RE.fullmatch(value):
        raise ValidationError(f"invalid sha256: {value!r}")
    return value


def validate_workspace(value: str) -> str:
    """Return `value` if it is a valid workspace path, else raise.

    A workspace is an absolute path on its host. The server stores it as an
    opaque string.
    """
    if not 1 <= len(value) <= MAX_WORKSPACE_LENGTH:
        raise ValidationError("workspace length must be 1 to 4096")
    if not value.startswith("/") or "\x00" in value:
        raise ValidationError(f"invalid workspace: {value!r}")
    return value


def encode_workspace(value: str) -> str:
    return base64.b64encode(validate_workspace(value).encode("utf-8")).decode("ascii")


def decode_workspace(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except (ValueError, UnicodeError, binascii.Error):
        raise ValidationError("invalid UTF-8 workspace header") from None
    return validate_workspace(decoded)


def validate_metadata(value: dict[str, Any]) -> SessionMetadata:
    """Check known fields. Unknown fields remain available for extensions."""
    for key in (
        "workspace",
        "cwd",
        "title",
        "first_timestamp",
        "last_timestamp",
        "harness_version",
    ):
        if key in value and not isinstance(value[key], str):
            raise ValidationError(f"metadata {key} must be a string")
    if "record_count" in value and (
        type(value["record_count"]) is not int or value["record_count"] < 0
    ):
        raise ValidationError("metadata record_count must be a nonnegative integer")
    if "links" in value and (
        not isinstance(value["links"], dict)
        or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in value["links"].items()
        )
    ):
        raise ValidationError("metadata links must map strings to strings")
    return cast(SessionMetadata, value)


# --- Bodies -----------------------------------------------------------------


class ErrorBody(TypedDict):
    error: str
    detail: NotRequired[str]


class WhoAmI(TypedDict):
    account: str
    host: str
    scopes: list[Scope]
    read_scope: NotRequired[ReadScope]
    capabilities: NotRequired[list[str]]


class HostInfo(TypedDict):
    kind: HostKind
    home: NotRequired[str]
    config_dirs: NotRequired[list[str]]
    os: NotRequired[str]


class HostRecord(HostInfo):
    name: str
    registered_at: str
    updated_at: str


class Placement(TypedDict):
    host: str
    workspace: str
    cwd: NotRequired[str]
    cause: PlacementCause
    first_seen: str
    last_seen: str


class Lease(TypedDict):
    state: LeaseState
    # The holder for "held"; the target host for "reserved".
    host: NotRequired[str]
    workspace: NotRequired[str]
    since: str


class Branch(TypedDict):
    generation: str
    host: str
    activity: int
    last_append: str


class SessionStateBody(TypedDict):
    state: SessionState
    lease: Lease
    # Present for a conflicted session: the branches with activity.
    branches: NotRequired[list[Branch]]


class AppendResponse(TypedDict):
    generation: str
    length: int
    session: SessionStateBody


class AppendConflict(TypedDict):
    error: Literal["conflict"]
    generation: str
    length: int
    session: SessionStateBody


class CreateGenerationRequest(TypedDict):
    # Absent for the first generation of a file.
    parent: NotRequired[str]
    fork_offset: NotRequired[int]
    cause: GenerationCause


class GenerationInfo(TypedDict):
    id: str
    parent: NotRequired[str]
    fork_offset: NotRequired[int]
    cause: GenerationCause
    length: int
    activity: int
    host: str
    created: str
    last_append: str


class BlobVersion(TypedDict):
    version: int
    sha256: str
    size: int
    host: str
    created: str


class LogManifest(TypedDict):
    path: str
    kind: Literal["log"]
    generations: list[GenerationInfo]
    # The leaf of the active branch.
    active: str


class BlobManifest(TypedDict):
    path: str
    kind: Literal["blob"]
    versions: list[BlobVersion]


class SessionMetadata(TypedDict, total=False):
    workspace: str
    cwd: str
    title: str
    first_timestamp: str
    last_timestamp: str
    harness_version: str
    record_count: int
    # Links to other sessions, for example {"forked_from": "<sid>"}.
    links: dict[str, str]


class TeleportEvent(TypedDict):
    id: str
    source: NotRequired[Placement]
    target_host: str
    target_workspace: str
    steal: bool
    created: str


class ForkEvent(TypedDict):
    session_id: str
    source_generation: str
    source_offset: int
    created: str


class Manifest(TypedDict):
    harness: str
    session_id: str
    metadata: SessionMetadata
    state: SessionStateBody
    placements: list[Placement]
    teleports: list[TeleportEvent]
    files: list[LogManifest | BlobManifest]
    forked_from: NotRequired[ForkEvent]


class SessionSummary(TypedDict):
    harness: str
    session_id: str
    metadata: SessionMetadata
    state: SessionStateBody
    last_placement: NotRequired[Placement]


class SessionList(TypedDict):
    sessions: list[SessionSummary]


class SurrenderRequest(TypedDict):
    reserved_for: NotRequired[str]


class TeleportRequest(TypedDict):
    workspace: str
    steal: NotRequired[bool]


class TeleportResponse(TypedDict):
    teleport: TeleportEvent
    session: SessionStateBody


class ForkRequest(TypedDict):
    generation: str
    offset: int


class ForkResponse(TypedDict):
    session_id: str


class GrantRequest(TypedDict):
    host: str
