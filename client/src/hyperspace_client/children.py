"""Child sessions: sessions that a tool starts for the work of another
session, for example a security review of its change.

The adapter finds such a session from its first prompt (`classify`), and
gives it the metadata `automation`. The parent is the session on this host
that changed the files of the review with edit tools in a window before the
review started. The best match edited the most of these files, and of those,
it edited one of them last. If there is exactly one best match, the review
gets `links.parent`. Else it has no parent, and its `automation` metadata
still hides it from the lists of top-level sessions. See "Child sessions" in
the spec.

The sweep runs `link_children` before it syncs the sessions. The search for
a parent reads only the end of the transcripts that changed in the window,
and the client searches for the parent of each review only once.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import httpx

from hyperspace_client.adapters import LocalSession
from hyperspace_client.adapters.claude_code import ClaudeCode, Edit
from hyperspace_client.api import ApiError
from hyperspace_client.sync import Context, server_metadata
from hyperspace_protocol import SessionMetadata

log = logging.getLogger("hyperspace")

# The parent changed the files of the review in this time before the review
# started.
PARENT_WINDOW = 12 * 3600.0
# The most metadata writes of one sweep. The first sweep after an upgrade
# can find many reviews that the server already has; the next sweeps send
# the others, so that the sweep stays well under the server's rate limit.
MAX_SENDS = 200
# Metadata keys of the client: the first prompt was read, and the search for
# a parent was done.
CLASSIFIED = "_classified"
PARENT_CHECKED = "_parent_checked"


@dataclass
class LinkResult:
    classified: int = 0
    linked: int = 0
    unlinked: int = 0
    sent: int = 0


def find_parent(
    session_id: str,
    start: float,
    paths: Iterable[str],
    edits: dict[str, list[Edit]],
) -> str | None:
    """The parent of a review that started at `start` and that names
    `paths`, from the edits on this host by path, or None if no session or
    more than one session is the best match."""
    matched: dict[str, set[str]] = defaultdict(set)
    last: dict[str, float] = {}
    for path in paths:
        for edit in edits.get(path, ()):
            if edit.session_id == session_id:
                continue
            if not start - PARENT_WINDOW <= edit.time <= start:
                continue
            matched[edit.session_id].add(path)
            last[edit.session_id] = max(last.get(edit.session_id, 0.0), edit.time)
    ranked = sorted(
        ((len(found), last[sid], sid) for sid, found in matched.items()),
        reverse=True,
    )
    if not ranked or (len(ranked) > 1 and ranked[0][:2] == ranked[1][:2]):
        return None
    return ranked[0][2]


def link_children(ctx: Context, adapter: ClaudeCode) -> LinkResult:
    """Find the child sessions on this host and their parents, save the
    result in their metadata, and send the changed metadata of child
    sessions that the server already has."""
    result = LinkResult()
    state, harness = ctx.state, adapter.harness
    sessions = adapter.discover()
    pending: list[tuple[LocalSession, float, list[str]]] = []
    for session in sessions:
        with state.try_lock(harness, session.session_id) as locked:
            if not locked:
                continue
            record = state.session(harness, session.session_id)
            meta = record.metadata
            if CLASSIFIED not in meta:
                found = adapter.classify(session)
                if found is None:
                    continue
                meta = {**meta, **found, CLASSIFIED: True}
                record.metadata = meta
                state.save_session(record, ("metadata",))
                result.classified += 1
            start = _review_start(meta)
            if start is not None and PARENT_CHECKED not in meta:
                pending.append((session, start, list(meta.get("_review_paths") or [])))
    if pending:
        starts = [start for _, start, _ in pending]
        by_path: dict[str, list[Edit]] = defaultdict(list)
        for edit in adapter.edits(min(starts) - PARENT_WINDOW, max(starts)):
            by_path[edit.path].append(edit)
        for session, start, paths in pending:
            parent = find_parent(session.session_id, start, paths, by_path)
            with state.try_lock(harness, session.session_id) as locked:
                if not locked:
                    continue
                record = state.session(harness, session.session_id)
                meta = dict(record.metadata)
                if parent is not None:
                    meta["links"] = {**(meta.get("links") or {}), "parent": parent}
                    result.linked += 1
                else:
                    result.unlinked += 1
                meta[PARENT_CHECKED] = True
                record.metadata = meta
                state.save_session(record, ("metadata",))
    result.sent = _send(ctx, adapter, sessions)
    return result


def _review_start(meta: dict[str, Any]) -> float | None:
    value = meta.get("_review_start")
    return float(value) if isinstance(value, int | float) else None


def _send(ctx: Context, adapter: ClaudeCode, sessions: list[LocalSession]) -> int:
    """Send the metadata of child sessions that changed after the server got
    it. A sync sends the metadata only with new bytes, and a review that the
    server has gets no new bytes. A session that the server does not have
    gets its metadata with its first sync."""
    sent = 0
    for session in sessions:
        if sent >= MAX_SENDS:
            log.info("children: more metadata to send at the next sweep")
            break
        with ctx.state.try_lock(adapter.harness, session.session_id) as locked:
            if not locked:
                continue
            record = ctx.state.session(adapter.harness, session.session_id)
            if "automation" not in record.metadata or not record.sent_metadata:
                continue
            if record.workspace is None:
                continue
            body = server_metadata(record.workspace, record.metadata)
            if body == record.sent_metadata:
                continue
            try:
                ctx.api.add_metadata(
                    adapter.harness,
                    session.session_id,
                    record.workspace,
                    cast(SessionMetadata, body),
                )
            except ApiError as exc:
                log.warning("%s: metadata: %s", session.session_id, exc)
                if exc.status in (401, 403, 429):
                    break
                continue
            except httpx.HTTPError as exc:
                log.warning("children: metadata not sent: %s", exc)
                break
            record.sent_metadata = body
            ctx.state.save_session(record, ("sent_metadata",))
            sent += 1
    return sent
