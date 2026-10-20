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
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hyperspace_client.adapters import (
    LocalFile,
    LocalSession,
    decode_segment,
    encode_segment,
)
from hyperspace_protocol import MAX_STATUS_EXCERPT, validate_session_id

HARNESS = "claude-code"
TRANSCRIPT = "transcript.jsonl"


@dataclass(frozen=True)
class Job:
    """A Claude Code background job: `claude rm <short>` removes it."""

    short: str
    # The modification time of the job's state.json.
    mtime: float
    # The job's own id (`sessionId` in its state).
    own_id: str
    # The sessions whose transcripts the job can write (see
    # `ClaudeCode.writes`).
    candidates: tuple[str, ...] = ()

    @property
    def session_id(self) -> str | None:
        """The session that the job writes, if that is beyond doubt: its own
        id, when that is its only candidate. Else None, and the job is never
        closed automatically."""
        return self.own_id if self.candidates == (self.own_id,) else None


# A job directory name that cannot be read as an option.
_JOB_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")

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
# The excerpt comes from the end of the transcript only.
_EXCERPT_WINDOW = 256 * 1024
# One line of `format_tail` for a tool call shows at most this much input.
_TOOL_SUMMARY = 100

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
# Session id to the sessions whose transcripts have a `continued-in` record
# with that id (from `State.continued_from`).
ContinuedFrom = Mapping[str, Collection[str]]


def _session_id(value: object) -> str | None:
    """`value` if it is a valid session id, else None."""
    if not isinstance(value, str):
        return None
    try:
        return validate_session_id(value)
    except ValueError:
        return None


def _read_state(path: Path) -> dict[str, Any] | None:
    """The object in a job's state.json, or None."""
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return state if isinstance(state, dict) else None  # pyright: ignore[reportUnknownVariableType]


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


def _ancestors() -> set[int]:
    """The pids of this process and of its parent processes."""
    if sys.platform != "linux":
        return {os.getpid(), os.getppid()}
    out: set[int] = set()
    pid = os.getpid()
    while pid > 0 and pid not in out:
        out.add(pid)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            pid = int(stat[stat.rfind(")") + 2 :].split()[1])
        except (OSError, ValueError, IndexError):
            break
    return out


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


def _blocks(record: dict[str, Any]) -> list[dict[str, Any]]:
    """The content blocks of a record's message. A string content is one
    text block."""
    message = record.get("message")
    if not isinstance(message, dict):
        return []
    content = message.get("content")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if not isinstance(content, list):
        return []
    items: list[Any] = content  # pyright: ignore[reportUnknownVariableType]
    return [i for i in items if isinstance(i, dict)]


def _text(record: dict[str, Any]) -> str:
    """The text blocks of a record, without thinking blocks."""
    return "\n".join(
        str(b["text"])
        for b in _blocks(record)
        if b.get("type") == "text" and isinstance(b.get("text"), str)
    ).strip()


def _is_assistant(record: dict[str, Any]) -> bool:
    return record.get("type") == "assistant" and not record.get("isSidechain")


def _tool_line(block: dict[str, Any]) -> str:
    raw: Any = block.get("input")
    summary = ""
    if isinstance(raw, dict):
        fields: dict[str, Any] = raw  # pyright: ignore[reportUnknownVariableType]
        values = [str(v) for v in fields.values() if isinstance(v, str | int)]
        summary = " ".join(" ".join(values).split())
    if len(summary) > _TOOL_SUMMARY:
        summary = summary[: _TOOL_SUMMARY - 1] + "…"
    return f"  [tool] {block.get('name', '?')} {summary}".rstrip()


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

    def live_processes(
        self, continued_from: ContinuedFrom | None = None
    ) -> list[list[str]]:
        """For each live Claude Code process on this host, the ids of the
        sessions whose transcripts it can write. This is the rule of
        `is_running`: the process's registry id, the ids in its job's state,
        and the sessions that continued in its registry id (from
        `continued_from`). Invalid ids are left out, because the server
        refuses a heartbeat with one."""
        sessions = self.config_dir / "sessions"
        if not sessions.is_dir():
            return []
        out: list[list[str]] = []
        for path in sorted(sessions.glob("*.json")):
            try:
                record: dict[str, Any] = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if not isinstance(record, dict) or not _alive(record):  # pyright: ignore[reportUnnecessaryIsInstance]
                continue
            own = record.get("sessionId")
            candidates: list[object] = [own]
            candidates += sorted(self._job_sessions(record.get("jobId")))
            if isinstance(own, str):
                candidates += sorted((continued_from or {}).get(own, ()))
            ids: list[str] = []
            for candidate in candidates:
                sid = _session_id(candidate)
                if sid is not None and sid not in ids:
                    ids.append(sid)
            if ids:
                out.append(ids)
        return out

    def live_sessions(self, continued_from: ContinuedFrom | None = None) -> list[str]:
        """The ids of the sessions that a live Claude Code process on this
        host can write (see `live_processes`)."""
        live: list[str] = []
        for ids in self.live_processes(continued_from):
            live += [sid for sid in ids if sid not in live]
        return live

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

    def jobs(self, continued_from: ContinuedFrom | None = None) -> list[Job]:
        """The background jobs on this host, from
        `<config dir>/jobs/<short>/state.json`. Claude Code says that this file
        is not a stable interface, so a job with an unexpected name or no
        valid session id is left out."""
        jobs = self.config_dir / "jobs"
        if not jobs.is_dir():
            return []
        out: list[Job] = []
        for path in sorted(jobs.glob("*/state.json")):
            job = self.job(path.parent.name, continued_from)
            if job is not None:
                out.append(job)
        return out

    def job(
        self, short: str, continued_from: ContinuedFrom | None = None
    ) -> Job | None:
        """The background job `short` (see `jobs`), or None."""
        # The name goes to `claude rm` as an argument.
        if not _JOB_NAME.fullmatch(short):
            return None
        path = self.config_dir / "jobs" / short / "state.json"
        record = _read_state(path)
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return None
        own = _session_id(record.get("sessionId")) if record else None
        if record is None or own is None:
            return None
        candidates = self.writes(own, record.get("resumeSessionId"), continued_from)
        return Job(short, mtime, own, candidates)

    def writes(
        self,
        own: str,
        resumes: object = None,
        continued_from: ContinuedFrom | None = None,
    ) -> tuple[str, ...]:
        """The sessions whose transcripts a process or a job with the id `own`
        can write: `own` if it has a local transcript with a conversation,
        `resumes` (the `resumeSessionId` of the job's state), and the sessions
        that continued in `own`. A background job can write the transcript of
        the session that it continues. With exactly one candidate, that is
        the session. With none or more than one, the session is not known: a
        stale or missing `continued_from` cannot show which one it is. A
        `resumes` that is not a session id gives no candidates."""
        if resumes is not None and _session_id(resumes) is None:
            return ()
        candidates: list[str] = [own] if self._has_conversation(own) else []
        for value in (resumes, *sorted((continued_from or {}).get(own, ()))):
            sid = _session_id(value)
            if sid is not None and sid != own and sid not in candidates:
                candidates.append(sid)
        return tuple(candidates)

    def resolve(
        self, session_id: str, continued_from: ContinuedFrom | None = None
    ) -> str | None:
        """The session whose transcript the Claude Code process of this
        environment writes, from its session id (`CLAUDE_CODE_SESSION_ID`, or
        the session of a hook). In a background job, that id can be the job's
        own id. Only in the job that runs this process (`current_job`) does
        the id change, to the one session from `writes`. None if that job has
        no candidate or more than one. Else the id does not change."""
        state = self.current_job()
        if state is None or session_id not in (
            state.get("sessionId"),
            state.get("resumeSessionId"),
        ):
            return session_id
        candidates = self.writes(
            session_id, state.get("resumeSessionId"), continued_from
        )
        return candidates[0] if len(candidates) == 1 else None

    def current_job(self) -> dict[str, Any] | None:
        """The state of the background job that runs this process, or None.
        `CLAUDE_JOB_DIR` names the job. Its value can be stale, for example in
        a process that a job started and that outlives the job. Thus a live
        registry record of this process or of a parent process must also
        name the job."""
        job_dir = os.environ.get("CLAUDE_JOB_DIR")
        sessions = self.config_dir / "sessions"
        if not job_dir or not sessions.is_dir():
            return None
        short = Path(job_dir).name
        if not _JOB_ID.fullmatch(short):
            return None
        ancestors = _ancestors()
        for path in sessions.glob("*.json"):
            try:
                record: dict[str, Any] = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if (
                isinstance(record, dict)  # pyright: ignore[reportUnnecessaryIsInstance]
                and record.get("jobId") == short
                and record.get("pid") in ancestors
                and _alive(record)
            ):
                return _read_state(self.config_dir / "jobs" / short / "state.json")
        return None

    def _has_conversation(self, session_id: str) -> bool:
        """True if a local transcript of the session is not empty and not a
        stub."""
        name = f"{session_id}.jsonl"
        projects = self.config_dir / "projects"
        if not _SESSION_FILE.fullmatch(name) or not projects.is_dir():
            return False
        for project in projects.iterdir():
            transcript = project / name
            try:
                if transcript.stat().st_size == 0:
                    continue
            except OSError:
                continue
            if not self.is_stub(LocalSession(HARNESS, session_id, transcript)):
                return True
        return False

    # --- Status ------------------------------------------------------------------

    @staticmethod
    def in_flight() -> int:
        """The number of tasks that run in this background job: shells,
        subagents, Monitors and crons. A hook process has the job's
        environment. Claude Code says that the job state is not a stable
        interface, so a missing variable, file or field gives 0."""
        job_dir = os.environ.get("CLAUDE_JOB_DIR")
        if not job_dir:
            return 0
        try:
            state: Any = json.loads((Path(job_dir) / "state.json").read_text())
        except (OSError, ValueError):
            return 0
        if not isinstance(state, dict):
            return 0
        flight: Any = state.get("inFlight")  # pyright: ignore[reportUnknownMemberType]
        if not isinstance(flight, dict):
            return 0
        tasks: Any = flight.get("tasks")  # pyright: ignore[reportUnknownMemberType]
        return tasks if type(tasks) is int and tasks > 0 else 0

    @staticmethod
    def excerpt(transcript: Path) -> str | None:
        """The text of the agent's last message: the text blocks of the
        main-chain assistant records after the last prompt, read from the end
        of the transcript."""
        try:
            with open(transcript, "rb") as f:
                start = max(0, f.seek(0, os.SEEK_END) - _EXCERPT_WINDOW)
                f.seek(start)
                data = f.read()
        except OSError:
            return None
        if start:
            data = data[data.find(b"\n") + 1 :]
        # Claude Code writes each content block of a message as a record.
        # A tool call or a tool result is between two messages.
        parts: list[str] = []
        for record in reversed(_records(data)):
            if is_prompt(record):
                break
            if record.get("type") == "user" and parts:
                break
            if not _is_assistant(record):
                continue
            if parts and tool_calls(record):
                break
            text = _text(record)
            if text:
                parts.append(text)
        if not parts:
            return None
        text = "\n\n".join(reversed(parts))
        if len(text) > MAX_STATUS_EXCERPT:
            text = text[: MAX_STATUS_EXCERPT - 1] + "…"
        return text

    @staticmethod
    def format_tail(data: bytes, turns: int) -> str:
        """The last `turns` turns of a transcript, as text: each prompt and
        each message of the agent in full, and one line for each tool call.
        `data` can start in the middle of a record."""
        records = _records(data)
        starts = [i for i, r in enumerate(records) if is_prompt(r)]
        first = starts[-turns] if 0 < turns <= len(starts) else 0
        lines: list[str] = []
        for record in records[first:]:
            if is_prompt(record):
                stamp = record.get("timestamp")
                head = f"> user ({stamp})" if isinstance(stamp, str) else "> user"
                lines += ["", head, _text(record)]
            elif _is_assistant(record):
                text = _text(record)
                if text:
                    lines += ["", text]
                lines += [
                    _tool_line(b)
                    for b in _blocks(record)
                    if b.get("type") == "tool_use"
                ]
        return "\n".join(lines).strip("\n")

    @staticmethod
    def prompts(data: bytes) -> int:
        """The number of prompts in `data`."""
        return sum(1 for r in _records(data) if is_prompt(r))

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
