"""The Claude Code adapter.

Claude Code keeps a session in `<config dir>/projects/<sanitized cwd>/`:
`<sid>.jsonl` is the transcript, and `<sid>/` holds subagent transcripts,
spilled tool results, and workflow files. File-history snapshots are in
`<config dir>/file-history/<sid>/`, and plan files in
`<config dir>/plans/<slug>.md`. The JSONL format is internal to Claude Code,
so this module reads only a few fields, and passes all records through
without change.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from hyperspace_client.adapters import (
    LocalFile,
    LocalSession,
    decode_segment,
    encode_segment,
)

HARNESS = "claude-code"
TRANSCRIPT = "transcript.jsonl"

# Claude Code session ids are UUIDs. Other .jsonl files in a project
# directory (legacy agent-*.jsonl files, copies that Claude Code or
# Hyperspace set aside) do not match.
_SESSION_FILE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.jsonl"
)
_SIDE_DIRS = ("subagents", "tool-results", "workflows")
# The metadata keys that the server gets. Others are for the adapter.
_SERVER_KEYS = (
    "cwd",
    "title",
    "first_timestamp",
    "last_timestamp",
    "harness_version",
    "record_count",
    "links",
)


_SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}")

# Record types that name a session but hold no conversation. Claude Code
# writes a transcript with only these records for the id of a background
# job, while the job writes its conversation into the transcript of the
# session that it continues. Any other record type, also an unknown one,
# makes a transcript a real session.
_NAME_RECORDS = frozenset({"ai-title", "agent-name", "custom-title"})
# A transcript larger than this is never a stub, and is not read to check.
STUB_MAX = 64 * 1024
# A job id names a directory in `<config dir>/jobs/`.
_JOB_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _walk(root: Path) -> list[Path]:
    """The regular files below `root`, sorted. Symbolic links (to files or
    directories) are not followed, and every result is checked to be inside
    `root`."""
    real_root = root.resolve()
    found: list[Path] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        here = Path(directory)
        dirnames[:] = sorted(d for d in dirnames if not (here / d).is_symlink())
        for name in filenames:
            path = here / name
            if path.is_symlink() or not path.is_file():
                continue
            if not path.resolve().is_relative_to(real_root):
                continue
            found.append(path)
    return sorted(found)


def default_config_dir() -> Path:
    value = os.environ.get("CLAUDE_CONFIG_DIR")
    return Path(value) if value else Path.home() / ".claude"


def sanitize_path(path: str) -> str:
    """Match Claude Code 2.1.288, including its UTF-16 hash and 200-unit limit."""
    raw = path.encode("utf-16-le", errors="surrogatepass")
    units = [int.from_bytes(raw[i : i + 2], "little") for i in range(0, len(raw), 2)]
    encoded = "".join(chr(c) if c < 128 and chr(c).isalnum() else "-" for c in units)
    if len(encoded) <= 200:
        return encoded
    digest = 0
    for c in units:
        digest = (31 * digest + c) & 0xFFFFFFFF
    if digest >= 0x80000000:
        digest -= 0x100000000
    number = abs(digest)
    suffix = ""
    while number:
        number, digit = divmod(number, 36)
        suffix = "0123456789abcdefghijklmnopqrstuvwxyz"[digit] + suffix
    return encoded[:200] + "-" + (suffix or "0")


def _parse(line: bytes) -> dict[str, Any] | None:
    """The record on one transcript line, or None. An interrupted write can
    leave NUL bytes before a record on the same line, so leading NULs are
    ignored. (Python's JSON decoder reads leading NULs as UTF-32.)"""
    try:
        value = json.loads(line.lstrip(b"\x00"))
    except ValueError:
        return None
    return value if isinstance(value, dict) else None  # pyright: ignore[reportUnknownVariableType]


def _records(data: bytes) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in data.splitlines():
        if not line.strip():
            continue
        record = _parse(line)
        if record is not None:
            out.append(record)
    return out


def is_stub(data: bytes) -> bool:
    """True if `data` is at most `STUB_MAX` bytes, has records, and every
    line is a record that only names the session (see `_NAME_RECORDS`). A
    line that is not a record, or a record of another type, makes this
    False."""
    if len(data) > STUB_MAX:
        return False
    found = False
    for line in data.splitlines():
        if not line.strip():
            continue
        record = _parse(line)
        kind = record.get("type") if record is not None else None
        if not isinstance(kind, str) or kind not in _NAME_RECORDS:
            return False
        found = True
    return found


def _alive(record: dict[str, Any]) -> bool:
    """True if the process of a live-session registry record runs."""
    pid = record.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return False
    if sys.platform != "linux":
        # The registry's procStart format is platform-specific.
        # Until it can be compared, a live/reused pid must block a
        # destructive move rather than be treated as stopped.
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except OSError:
            return True
        return True
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    # The process name (field 2) can contain spaces; the fields after
    # its closing parenthesis are fixed.
    fields = stat[stat.rfind(")") + 2 :].split()
    start = fields[19] if len(fields) > 19 else None
    return record.get("procStart") in (None, start)


def is_prompt(record: dict[str, Any]) -> bool:
    """True for a user record that is a prompt: not a tool result, not a
    meta record, not a compaction summary."""
    if record.get("type") != "user" or record.get("isSidechain"):
        return False
    if record.get("isMeta") or record.get("isCompactSummary"):
        return False
    if "toolUseResult" in record:
        return False
    message = record.get("message")
    if not isinstance(message, dict):
        return False
    content = message.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    if isinstance(content, str):
        return bool(content.strip())
    if isinstance(content, list):
        items: list[Any] = content  # pyright: ignore[reportUnknownVariableType]
        return bool(items) and not any(
            isinstance(i, dict) and i.get("type") == "tool_result"  # pyright: ignore[reportUnknownMemberType]
            for i in items
        )
    return False


def tool_calls(record: dict[str, Any]) -> int:
    """The number of tool_use blocks in an assistant record."""
    if record.get("type") != "assistant" or record.get("isSidechain"):
        return 0
    message = record.get("message")
    if not isinstance(message, dict):
        return 0
    content = message.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    if not isinstance(content, list):
        return 0
    items: list[Any] = content  # pyright: ignore[reportUnknownVariableType]
    return sum(
        1
        for i in items
        if isinstance(i, dict) and i.get("type") == "tool_use"  # pyright: ignore[reportUnknownMemberType]
    )


class ClaudeCode:
    harness = HARNESS

    def __init__(self, config_dir: Path | None = None) -> None:
        self.config_dir = config_dir or default_config_dir()

    # --- Discovery -----------------------------------------------------------

    def discover(self) -> list[LocalSession]:
        projects = self.config_dir / "projects"
        if not projects.is_dir():
            return []
        out: list[LocalSession] = []
        for project in sorted(projects.iterdir()):
            if not project.is_dir():
                continue
            for path in sorted(project.iterdir()):
                if _SESSION_FILE.fullmatch(path.name) and path.is_file():
                    out.append(LocalSession(HARNESS, path.stem, path))
        return out

    def session_for_transcript(self, path: Path) -> LocalSession | None:
        if not _SESSION_FILE.fullmatch(path.name):
            return None
        return LocalSession(HARNESS, path.stem, path)

    def project_dir(self, workspace: str) -> Path:
        return self.config_dir / "projects" / sanitize_path(workspace)

    # --- Files ---------------------------------------------------------------

    def files(self, session: LocalSession, metadata: dict[str, Any]) -> list[LocalFile]:
        """The files of a session. Transcript content can name files (the
        plan slug), so every path is checked to stay in its directory, and
        symbolic links are not followed."""
        out = [LocalFile(TRANSCRIPT, "log", session.transcript)]
        side = session.transcript.with_suffix("")
        for sub in _SIDE_DIRS:
            root = side / sub
            if not root.is_dir() or root.is_symlink():
                continue
            for path in _walk(root):
                parts = path.relative_to(side).parts
                rel = "/".join(encode_segment(p) for p in parts)
                kind = (
                    "log" if sub == "subagents" and path.suffix == ".jsonl" else "blob"
                )
                out.append(LocalFile(rel, kind, path))
        history = self.config_dir / "file-history" / session.session_id
        if history.is_dir() and not history.is_symlink():
            for path in sorted(history.iterdir()):
                if path.is_file() and not path.is_symlink():
                    rel = f"file-history/{encode_segment(path.name)}"
                    out.append(LocalFile(rel, "blob", path))
        plan = self.plan_file(metadata.get("_slug"))
        if plan is not None:
            out.append(LocalFile(f"plans/{encode_segment(plan.name)}", "blob", plan))
        return out

    def plan_file(self, slug: object) -> Path | None:
        """The plan file for a slug from the transcript, or None. The slug
        must be a plain file name: a slug such as `../x` must not name a
        file outside the plans directory."""
        if not isinstance(slug, str) or not _SLUG.fullmatch(slug):
            return None
        plans = self.config_dir / "plans"
        plan = plans / f"{slug}.md"
        if not plan.is_file() or plan.is_symlink():
            return None
        if plan.resolve().parent != plans.resolve():
            return None
        return plan

    def local_path(self, session_dir: Path, rel_path: str) -> Path | None:
        """The local path for a session file path, for a session whose
        transcript is `session_dir.with_suffix('.jsonl')`. None for a file
        that this adapter does not restore.

        The path comes from the server, so it is checked: no empty, `.`, or
        `..` segments, no `/` in a decoded segment, and the result (after
        symbolic links) must be inside the directory for its kind."""
        parts = [decode_segment(p) for p in rel_path.split("/")]
        if any(p in ("", ".", "..") or "/" in p or "\x00" in p for p in parts):
            return None
        if rel_path == TRANSCRIPT:
            base, path = session_dir.parent, session_dir.with_suffix(".jsonl")
        elif parts[0] in _SIDE_DIRS:
            base, path = session_dir, session_dir.joinpath(*parts)
        elif parts[0] == "file-history" and len(parts) == 2:
            base = self.config_dir / "file-history" / session_dir.name
            path = base / parts[1]
        elif parts[0] == "plans" and len(parts) == 2:
            base = self.config_dir / "plans"
            path = base / parts[1]
        else:
            return None
        if not path.resolve().is_relative_to(base.resolve()):
            return None
        return path

    # --- Content ---------------------------------------------------------------

    def commit_boundary(self, rel_path: str, data: bytes) -> int:
        if rel_path.endswith(".jsonl"):
            return data.rfind(b"\n") + 1
        return len(data)

    def activity(self, rel_path: str, data: bytes) -> int:
        if rel_path != TRANSCRIPT:
            return 0
        total = 0
        for record in _records(data):
            if is_prompt(record):
                total += 1
            total += tool_calls(record)
        return total

    def update_metadata(
        self, metadata: dict[str, Any], rel_path: str, data: bytes
    ) -> dict[str, Any]:
        if rel_path != TRANSCRIPT:
            return metadata
        meta = dict(metadata)
        links: dict[str, str] = dict(meta.get("links") or {})
        for r in _records(data):
            meta["record_count"] = int(meta.get("record_count", 0)) + 1
            ts = r.get("timestamp")
            if isinstance(ts, str):
                meta.setdefault("first_timestamp", ts)
                meta["last_timestamp"] = ts
            if isinstance(r.get("cwd"), str):
                meta["cwd"] = r["cwd"]
            if isinstance(r.get("version"), str):
                meta["harness_version"] = r["version"]
            if isinstance(r.get("slug"), str):
                meta["_slug"] = r["slug"]
            kind = r.get("type")
            if kind == "custom-title" and isinstance(r.get("customTitle"), str):
                meta["_custom_title"] = r["customTitle"]
            elif kind == "ai-title" and isinstance(r.get("aiTitle"), str):
                meta["_ai_title"] = r["aiTitle"]
            elif kind == "continued-in" and isinstance(
                r.get("continuedInSessionId"), str
            ):
                links["continued_in"] = r["continuedInSessionId"]
            forked = r.get("forkedFrom")
            if isinstance(forked, dict) and isinstance(forked.get("sessionId"), str):  # pyright: ignore[reportUnknownMemberType]
                links["forked_from"] = str(forked["sessionId"])  # pyright: ignore[reportUnknownArgumentType]
        title = meta.get("_custom_title") or meta.get("_ai_title")
        if title:
            meta["title"] = str(title)[:500]
        if links:
            meta["links"] = links
        return meta

    @staticmethod
    def server_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
        return {k: metadata[k] for k in _SERVER_KEYS if k in metadata}

    def first_cwd(self, session: LocalSession) -> str | None:
        try:
            with open(session.transcript, "rb") as f:
                for _, line in zip(range(200), f, strict=False):
                    record = _parse(line)
                    if record is not None and isinstance(record.get("cwd"), str):
                        return str(record["cwd"])
        except OSError:
            return None
        return None

    def is_stub(self, session: LocalSession) -> bool:
        """True if the transcript only names the session (see `is_stub`).
        False if the file is large or cannot be read."""
        try:
            with open(session.transcript, "rb") as f:
                data = f.read(STUB_MAX + 1)
        except OSError:
            return False
        return is_stub(data)

    # --- Teleport support -------------------------------------------------------

    def find_transcripts(self, session_id: str) -> list[Path]:
        """Every resumable copy of a session on this host."""
        return self.transcript_index().get(session_id, [])

    def transcript_index(self) -> dict[str, list[Path]]:
        """Every resumable transcript on this host, by session id, from one
        scan of the project directories."""
        index: dict[str, list[Path]] = {}
        for session in self.discover():
            try:
                if session.transcript.stat().st_size == 0:
                    continue
            except OSError:
                continue
            index.setdefault(session.session_id, []).append(session.transcript)
        return index

    def is_running(self, session_id: str) -> bool:
        """True if a live Claude Code process on this host can write the
        session's transcript.

        Claude Code writes `<config dir>/sessions/<pid>.json` for each
        process, with the session id and the process start time (field 22 of
        /proc/<pid>/stat), so a reused pid does not count. A background job
        can register under its own id and write the transcript of the
        session that it continues. Thus a process also counts if its job's
        state names the session, or if the session's transcript records
        that it continued in the process's session id."""
        sessions = self.config_dir / "sessions"
        if not sessions.is_dir():
            return False
        continued: set[str] | None = None
        for path in sessions.glob("*.json"):
            try:
                record: dict[str, Any] = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if not isinstance(record, dict) or not _alive(record):  # pyright: ignore[reportUnnecessaryIsInstance]
                continue
            if record.get("sessionId") == session_id:
                return True
            if session_id in self._job_sessions(record.get("jobId")):
                return True
            if continued is None:
                continued = self._continued_in(session_id)
            if record.get("sessionId") in continued:
                return True
        return False

    def _job_sessions(self, job_id: object) -> set[str]:
        """The session ids in the state of a background job: its own id, and
        the id of the session that it resumes."""
        if not isinstance(job_id, str) or not _JOB_ID.fullmatch(job_id):
            return set()
        try:
            state = json.loads(
                (self.config_dir / "jobs" / job_id / "state.json").read_text()
            )
        except (OSError, ValueError):
            return set()
        if not isinstance(state, dict):
            return set()
        fields: dict[str, Any] = state  # pyright: ignore[reportUnknownVariableType]
        return {
            v
            for k in ("sessionId", "resumeSessionId")
            if isinstance(v := fields.get(k), str)
        }

    def _continued_in(self, session_id: str) -> set[str]:
        """The ids in the `continued-in` records of the session's local
        transcripts."""
        found: set[str] = set()
        for transcript in self.find_transcripts(session_id):
            try:
                with open(transcript, "rb") as f:
                    for line in f:
                        if b'"continued-in"' not in line:
                            continue
                        record = _parse(line)
                        if record is None or record.get("type") != "continued-in":
                            continue
                        target = record.get("continuedInSessionId")
                        if isinstance(target, str):
                            found.add(target)
            except OSError:
                continue
        return found

    @staticmethod
    def fork_records(data: bytes, old_id: str, new_id: str) -> bytes:
        """Copy a transcript under a new session id, the way Claude Code does
        for `--fork-session`: each record gets the new `sessionId`, and each
        record with a `uuid` also gets `forkedFrom` with the old session id
        and that uuid. Other fields do not change. Lines that are not JSON
        objects are copied as they are. NUL bytes before a record stay."""
        out: list[bytes] = []
        for line in data.splitlines(keepends=True):
            fields = _parse(line)
            if fields is None:
                out.append(line)
                continue
            prefix = line[: len(line) - len(line.lstrip(b"\x00"))]
            if "sessionId" in fields:
                fields["sessionId"] = new_id
            if isinstance(fields.get("uuid"), str):
                fields["forkedFrom"] = {
                    "sessionId": old_id,
                    "messageUuid": fields["uuid"],
                }
            newline = b"\n" if line.endswith(b"\n") else b""
            try:
                encoded = json.dumps(
                    fields, ensure_ascii=False, separators=(",", ":")
                ).encode()
            except UnicodeEncodeError:
                # JSON can contain escaped lone surrogates, but UTF-8 cannot.
                encoded = json.dumps(
                    fields, ensure_ascii=True, separators=(",", ":")
                ).encode()
            out.append(prefix + encoded + newline)
        return b"".join(out)

    @staticmethod
    def arrival_notice(arrival: dict[str, Any]) -> str:
        """The text that the SessionStart hook prints after a teleport."""
        source: dict[str, Any] = arrival.get("source") or {}
        lines = [
            f"[hyperspace] This session was teleported (teleport {arrival['teleport_id']})."
        ]
        if source:
            lines.append(
                f"Before: host {source.get('host')}, workspace {source.get('workspace')}, "
                f"last active {source.get('last_seen')}."
            )
        lines.append(
            f"Now:    host {arrival['target_host']}, workspace {arrival['target_workspace']}."
        )
        lines.append("Earlier messages refer to paths at the old location.")
        old = arrival.get("source_tool_results")
        new = arrival.get("target_tool_results")
        if not arrival.get("tool_results", True):
            lines.append(
                "Saved tool outputs from before the teleport are not on this host."
            )
        elif old and new and old != new:
            lines.append(
                f"Saved tool outputs that earlier messages cite under {old} "
                f"are now under {new}"
            )
        lines.append(
            "Only the conversation moved. Files, uncommitted changes, and running "
            "processes did not move."
        )
        return "\n".join(lines)

    @staticmethod
    def move_notice(
        old_host: str, old_workspace: str, host: str, workspace: str
    ) -> str:
        """The text for a resume in a different place, with no teleport."""
        return (
            "[hyperspace] This session was resumed in a different place. "
            f"Before: host {old_host}, workspace {old_workspace}. "
            f"Now: host {host}, workspace {workspace}. Earlier messages refer "
            "to paths at the old location."
        )
