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
CAP_RESTORE_PRECONDITION = "restore-precondition"
CAP_SESSION_STATUS = "session-status"
CAP_LIVENESS = "liveness"
# `GET /sessions` takes `cursor=` and returns `next` (see `SessionList`).
CAP_LIST_CURSOR = "list-cursor"
CAPABILITIES = [
    CAP_WORKSPACE_UTF8,
    CAP_RESTORE_PRECONDITION,
    CAP_SESSION_STATUS,
    CAP_LIVENESS,
    CAP_LIST_CURSOR,
]

# One request carries at most this much uncompressed file data. The client
# splits larger appends.
MAX_BODY = 8 * 1024 * 1024

MAX_SESSION_ID_LENGTH = 128
MAX_HOST_NAME_LENGTH = 64
MAX_HARNESS_LENGTH = 32
MAX_FILE_PATH_LENGTH = 512
MAX_FILE_PATH_SEGMENTS = 32
MAX_WORKSPACE_LENGTH = 4096
# Status notes are one line of characters; excerpts can have more lines.
MAX_STATUS_NOTE = 300
MAX_STATUS_EXCERPT = 1000
MAX_HEARTBEAT_SESSIONS = 1000
# Heartbeat intervals, in seconds.
MIN_HEARTBEAT_INTERVAL = 10
MAX_HEARTBEAT_INTERVAL = 3600

FileKind = Literal["log", "blob"]
HostKind = Literal["workstation", "runner"]
ReadScope = Literal["own", "granted", "all"]
Scope = Literal["append", "read", "grant"]
GenerationCause = Literal["root", "rewrite", "diverge", "fork"]
PlacementCause = Literal["start", "move", "teleport", "steal", "diverge"]
SessionState = Literal["normal", "conflicted"]
LeaseState = Literal["held", "reserved", "free"]
Status = Literal["working", "waiting", "ready", "done"]
ReadyReason = Literal["asked", "idle"]
WakeKind = Literal["local", "message"]
StatusSourceKind = Literal["user", "agent", "hook"]
LivenessState = Literal["live", "stopped", "unknown"]

FILE_KINDS: tuple[FileKind, ...] = ("log", "blob")
HOST_KINDS: tuple[HostKind, ...] = ("workstation", "runner")
READ_SCOPES: tuple[ReadScope, ...] = ("own", "granted", "all")
STATUSES: tuple[Status, ...] = ("working", "waiting", "ready", "done")
READY_REASONS: tuple[ReadyReason, ...] = ("asked", "idle")
WAKE_KINDS: tuple[WakeKind, ...] = ("local", "message")
STATUS_SOURCE_KINDS: tuple[StatusSourceKind, ...] = ("user", "agent", "hook")
LIVENESS_STATES: tuple[LivenessState, ...] = ("live", "stopped", "unknown")


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


def _note(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a string")
    if len(value) > MAX_STATUS_NOTE:
        raise ValidationError(f"{name} is longer than {MAX_STATUS_NOTE} characters")
    if any(_is_control(c) for c in value):
        raise ValidationError(f"{name} must be one line with no control characters")
    return value


def _is_control(char: str) -> bool:
    """True for a C0 or C1 control character, or DEL."""
    code = ord(char)
    return code < 0x20 or 0x7F <= code < 0xA0


def _nonnegative_int(value: object, name: str) -> int:
    # `bool` is a subclass of `int`, but a JSON `true` is not a number.
    if type(value) is not int or value < 0:
        raise ValidationError(f"{name} must be a nonnegative integer")
    return value


def validate_status_request(value: dict[str, Any]) -> StatusRequest:
    """Check a status write, and return it with only the known fields."""
    status = value.get("status")
    if status not in STATUSES:
        raise ValidationError(f"status must be one of {list(STATUSES)}")
    request: dict[str, Any] = {"status": status}
    reason = value.get("reason")
    if status == "ready":
        if reason not in READY_REASONS:
            raise ValidationError(f"reason must be one of {list(READY_REASONS)}")
        request["reason"] = reason
    elif reason is not None:
        raise ValidationError("reason is only for the status ready")
    wake = value.get("wake")
    if status == "waiting":
        if wake not in WAKE_KINDS:
            raise ValidationError(f"wake must be one of {list(WAKE_KINDS)}")
        request["wake"] = wake
    elif wake is not None:
        raise ValidationError("wake is only for the status waiting")
    if value.get("note") is not None:
        request["note"] = _note(value["note"], "note")
    excerpt = value.get("excerpt")
    if excerpt is not None:
        if not isinstance(excerpt, str) or len(excerpt) > MAX_STATUS_EXCERPT:
            raise ValidationError(
                f"excerpt must be a string of at most {MAX_STATUS_EXCERPT} characters"
            )
        request["excerpt"] = excerpt
    source_kind = value.get("source_kind")
    if source_kind not in STATUS_SOURCE_KINDS:
        raise ValidationError(f"source_kind must be one of {list(STATUS_SOURCE_KINDS)}")
    request["source_kind"] = source_kind
    source_session = value.get("source_session")
    if source_session is not None:
        if not isinstance(source_session, str):
            raise ValidationError("source_session must be a string")
        request["source_session"] = validate_session_id(source_session)
    explicit = value.get("explicit")
    if not isinstance(explicit, bool):
        raise ValidationError("explicit must be a boolean")
    request["explicit"] = explicit
    request["base_seq"] = _nonnegative_int(value.get("base_seq"), "base_seq")
    return cast(StatusRequest, request)


def _session_ref(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError("each session must be an object")
    item = cast(dict[str, Any], value)
    harness, sid = item.get("harness"), item.get("session_id")
    if not isinstance(harness, str) or not isinstance(sid, str):
        raise ValidationError("harness and session_id must be strings")
    return {
        "harness": validate_harness(harness),
        "session_id": validate_session_id(sid),
    }


_HEARTBEAT_SOURCE = re.compile(r"[A-Za-z0-9_-]{0,64}")


def validate_heartbeat(value: dict[str, Any]) -> HeartbeatRequest:
    """Check a heartbeat, and return it with only the known fields."""
    interval = _nonnegative_int(value.get("interval"), "interval")
    if not MIN_HEARTBEAT_INTERVAL <= interval <= MAX_HEARTBEAT_INTERVAL:
        raise ValidationError(
            f"interval must be {MIN_HEARTBEAT_INTERVAL} to "
            f"{MAX_HEARTBEAT_INTERVAL} seconds"
        )
    request: dict[str, Any] = {"interval": interval}
    source = value.get("source", "")
    if not isinstance(source, str) or not _HEARTBEAT_SOURCE.fullmatch(source):
        raise ValidationError("source must be 0 to 64 letters, digits, - or _")
    if source:
        request["source"] = source
    for name in ("sessions", "unclosed"):
        items = value.get(name, [])
        if not isinstance(items, list):
            raise ValidationError(f"{name} must be a list")
        if len(cast(list[Any], items)) > MAX_HEARTBEAT_SESSIONS:
            raise ValidationError(
                f"{name} has more than {MAX_HEARTBEAT_SESSIONS} entries"
            )
        refs: list[dict[str, Any]] = []
        for item in cast(list[Any], items):
            ref = _session_ref(item)
            if name == "unclosed":
                ref["reason"] = _note(
                    cast(dict[str, Any], item).get("reason"), "reason"
                )
            refs.append(ref)
        if name == "sessions" or refs:
            request[name] = refs
    final = value.get("final", False)
    if not isinstance(final, bool):
        raise ValidationError("final must be a boolean")
    if final:
        request["final"] = True
    return cast(HeartbeatRequest, request)


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


class StatusSource(TypedDict):
    kind: StatusSourceKind
    # The server sets the host from the credential.
    host: str
    # An agent in a different session wrote the event.
    session_id: NotRequired[str]


class StatusEvent(TypedDict):
    seq: int
    status: Status
    # Only with "ready", and required there.
    reason: NotRequired[ReadyReason]
    # Only with "waiting", and required there.
    wake: NotRequired[WakeKind]
    note: NotRequired[str]
    source: StatusSource
    explicit: bool
    received: str


class Liveness(TypedDict):
    state: LivenessState
    # The host of a live copy, or else the host that last listed the session.
    host: str
    since: str
    # Why the host's launcher did not close the session, if it did not.
    close_blocked: NotRequired[str]


class SessionStateBody(TypedDict):
    state: SessionState
    lease: Lease
    # Present for a conflicted session: the branches with activity.
    branches: NotRequired[list[Branch]]
    # Absent until the first status event. It has no excerpt.
    status: NotRequired[StatusEvent]
    # Absent until a host lists the session in a heartbeat.
    liveness: NotRequired[Liveness]
    last_append: NotRequired[str]


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
    versions: NotRequired[list[BlobVersion]]


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
    # Opaque snapshot token for conditional restore finalization.
    revision: NotRequired[str]


class SessionSummary(TypedDict):
    harness: str
    session_id: str
    metadata: SessionMetadata
    state: SessionStateBody
    last_placement: NotRequired[Placement]
    # The excerpt of the latest status event.
    excerpt: NotRequired[str]


class SessionList(TypedDict):
    sessions: list[SessionSummary]
    # An opaque cursor for the next page: send it back as `cursor=`, with the
    # same filters. Absent at the end of the list. Servers without the
    # `list-cursor` capability never send it.
    next: NotRequired[str]


class SurrenderRequest(TypedDict):
    reserved_for: NotRequired[str]


class TeleportRequest(TypedDict):
    workspace: str
    steal: NotRequired[bool]
    expected_revision: NotRequired[str]
    teleport_id: NotRequired[str]


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


class StatusRequest(TypedDict):
    status: Status
    reason: NotRequired[ReadyReason]
    wake: NotRequired[WakeKind]
    note: NotRequired[str]
    excerpt: NotRequired[str]
    source_kind: StatusSourceKind
    source_session: NotRequired[str]
    explicit: bool
    # The seq of the latest event that the writer saw, or 0 if it saw none.
    base_seq: int


class StatusResponse(TypedDict):
    status: StatusEvent


class StaleStatus(TypedDict):
    """The body of `409 stale_status`."""

    error: Literal["stale_status"]
    detail: str
    # The current event, if the session has one.
    status: NotRequired[StatusEvent]


class HeartbeatSession(TypedDict):
    harness: str
    session_id: str


class UnclosedSession(TypedDict):
    harness: str
    session_id: str
    reason: str


class HeartbeatRequest(TypedDict):
    interval: int
    # The client's id for the harness config directory. A host with more than
    # one config directory sends one heartbeat for each; each replaces only
    # the earlier heartbeat with the same source. Absent: "".
    source: NotRequired[str]
    sessions: list[HeartbeatSession]
    unclosed: NotRequired[list[UnclosedSession]]
    # The launcher stops after this heartbeat.
    final: NotRequired[bool]


class HeartbeatResponse(TypedDict):
    received: str
    # The listed sessions that the server recorded.
    recorded: int
