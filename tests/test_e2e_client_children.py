# vim: filetype=python
"""Child sessions on the client: the security reviews that the
security-guidance plugin starts, their parents, the links on the server, and
the CLI. The transcripts are synthetic: they have the shape of the records
that Claude Code writes, but no real content."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from _hs import FakeClaude, Server, context, make_server, prompt, text_reply
from hyperspace_client import children as children_mod
from hyperspace_client import cli as cli_mod
from hyperspace_client.adapters import claude_code
from hyperspace_client.adapters.claude_code import (
    SECURITY_REVIEW,
    ClaudeCode,
    Edit,
    child_metadata,
    review_paths,
)
from hyperspace_client.api import Api
from hyperspace_client.children import PARENT_WINDOW, find_parent, link_children
from hyperspace_client.sync import Context, Syncer
from hyperspace_protocol import (
    CAP_CHILD_SESSIONS,
    CAP_LIST_CURSOR,
    CAP_SESSION_STATUS,
)

HARNESS = "claude-code"
REPO = "/home/user/project"
WORKTREE = "/home/user/project/.claude/worktrees/feature"
T0 = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def stamp(minutes: float) -> str:
    """A record timestamp, `minutes` after T0."""
    when = T0 + timedelta(minutes=minutes)
    return when.strftime("%Y-%m-%dT%H:%M:%S.") + f"{when.microsecond // 1000:03d}Z"


def epoch(minutes: float) -> float:
    return (T0 + timedelta(minutes=minutes)).timestamp()


def first_pass(paths: list[str]) -> str:
    """A prompt with the shape of the first pass of a review."""
    listed = "\n".join(f"  - {p}" for p in paths)
    diffs = "\n\n".join(f"=== DIFF: {p} ===\n@@ -1 +1 @@\n-a\n+b" for p in paths)
    return (
        "Review this change for security vulnerabilities.\n\n"
        "Changed files (you may Read these and any other file in the repo):\n"
        f"{listed}\n\nUnified diff (only + lines are new):\n\n{diffs}\n\n"
        "Investigate per the method in your instructions, then return the "
        "findings list."
    )


def second_pass(paths: list[str]) -> str:
    """A prompt with the shape of the second pass of a review."""
    candidates = [{"filePath": p, "category": "x"} for p in paths]
    diffs = "\n\n".join(f"=== DIFF: {p} ===\n@@ -1 +1 @@\n-a\n+b" for p in paths)
    return (
        "You previously flagged these candidate vulnerabilities:\n\n"
        + json.dumps(candidates, indent=2)
        + f"\n\nDIFF:\n{diffs}\n\nNow adversarially try to DISPROVE each one."
    )


def review_records(
    text: str, minutes: float, cwd: str = WORKTREE, entrypoint: str = "sdk-py"
) -> list[dict[str, Any]]:
    sid = str(uuid.uuid4())
    return [
        {
            "type": "queue-operation",
            "operation": "enqueue",
            "timestamp": stamp(minutes),
        },
        {
            "type": "user",
            "entrypoint": entrypoint,
            "cwd": cwd,
            "sessionId": sid,
            "timestamp": stamp(minutes),
            "message": {"role": "user", "content": text},
        },
        {
            "type": "assistant",
            "entrypoint": entrypoint,
            "cwd": cwd,
            "timestamp": stamp(minutes + 0.1),
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "t", "name": "Read", "input": {}}
                ],
            },
        },
    ]


# Tool results with the shapes that Claude Code writes for an edit tool call.
RESULTS: dict[str, dict[str, Any]] = {
    "ok": {"content": "The file has been updated successfully."},
    "failed": {
        "content": "<tool_use_error>String to replace not found in file.",
        "is_error": True,
    },
    "rejected": {
        "content": "The user doesn't want to proceed with this tool use.",
        "is_error": True,
    },
    "denied": {
        "content": "Permission for this action was denied.",
        "is_error": True,
    },
}


def edit(
    path: str, minutes: float, tool: str = "Edit", result: str | None = "ok"
) -> list[dict[str, Any]]:
    """An edit tool call, and its tool result unless `result` is None (an
    interrupted call)."""
    field = "notebook_path" if tool == "NotebookEdit" else "file_path"
    call_id = f"toolu_{uuid.uuid4().hex[:12]}"
    records: list[dict[str, Any]] = [
        {
            "type": "assistant",
            "cwd": REPO,
            "timestamp": stamp(minutes),
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "editing"},
                    {
                        "type": "tool_use",
                        "id": call_id,
                        "name": tool,
                        "input": {field: path},
                    },
                ],
            },
        }
    ]
    if result is not None:
        block = {"type": "tool_result", "tool_use_id": call_id, **RESULTS[result]}
        records.append(
            {
                "type": "user",
                "cwd": REPO,
                "timestamp": stamp(minutes + 0.01),
                "message": {"role": "user", "content": [block]},
            }
        )
    return records


def user_prompt(minutes: float) -> dict[str, Any]:
    return {**prompt("change the files", cwd=REPO), "timestamp": stamp(minutes)}


# --- Detection -----------------------------------------------------------------


def test_review_paths_are_absolute_and_unique() -> None:
    text = first_pass(["a.py", "lib/b.py"])
    assert review_paths(text, WORKTREE) == [f"{WORKTREE}/a.py", f"{WORKTREE}/lib/b.py"]
    # The second pass has only the diff headers.
    assert review_paths(second_pass(["lib/b.py"]), WORKTREE) == [f"{WORKTREE}/lib/b.py"]
    # A list item after the end of the "Changed files" list is not a path.
    assert review_paths("Changed files:\n  - a.py\n\n  - not-a-path\n", "/w") == [
        "/w/a.py"
    ]


@pytest.mark.parametrize("make", [first_pass, second_pass])
def test_child_metadata_of_a_review(make: Callable[[list[str]], str]) -> None:
    record = review_records(make(["a.py"]), 0)[1]
    assert child_metadata(record) == {
        "automation": SECURITY_REVIEW,
        "_review_start": epoch(0),
        "_review_paths": [f"{WORKTREE}/a.py"],
    }


def test_child_metadata_needs_the_sdk_and_the_prompt() -> None:
    # The same prompt from a user is not a review.
    record = review_records(first_pass(["a.py"]), 0, entrypoint="cli")[1]
    assert child_metadata(record) == {}
    record = review_records("Review my change, please.", 0)[1]
    assert child_metadata(record) == {}
    # With no working directory, a review has no paths to match.
    record = {**review_records(first_pass(["a.py"]), 0)[1], "cwd": None}
    assert child_metadata(record) == {"automation": SECURITY_REVIEW}


def test_classify_reads_the_first_prompt(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    path = fake.transcript(WORKTREE)
    session = fake.adapter.session_for_transcript(path)
    assert session is not None
    # No prompt yet.
    fake.write(path, {"type": "queue-operation", "timestamp": stamp(0)})
    assert fake.adapter.classify(session) is None
    fake.write(path, *review_records(first_pass(["a.py"]), 0)[1:])
    found = fake.adapter.classify(session)
    assert found is not None and found["automation"] == SECURITY_REVIEW
    # A session of the user: only its first prompt counts.
    path = fake.transcript(REPO)
    fake.write(path, user_prompt(0), *review_records(first_pass(["a.py"]), 1)[1:])
    session = fake.adapter.session_for_transcript(path)
    assert session is not None
    assert fake.adapter.classify(session) == {}


def test_classify_stops_after_the_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(claude_code, "_HEAD_MAX", 1000)
    fake = FakeClaude(tmp_path / "claude")
    path = fake.transcript(WORKTREE)
    big = {"type": "summary", "summary": "x" * 2000}
    fake.write(path, big, *review_records(first_pass(["a.py"]), 0)[1:])
    session = fake.adapter.session_for_transcript(path)
    assert session is not None
    assert fake.adapter.classify(session) == {}


# --- Edits -------------------------------------------------------------------------


def write_parent(fake: FakeClaude, *records: dict[str, Any]) -> Path:
    path = fake.transcript(REPO)
    fake.write(path, user_prompt(-600), *records)
    return path


def test_edits_of_main_agents_and_subagents(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    path = write_parent(
        fake,
        *edit(f"{WORKTREE}/a.py", -10),
        *edit(f"{WORKTREE}/./b.py", -9, "Write"),
        *edit(f"{WORKTREE}/c.py", -8, "MultiEdit"),
        *edit(f"{WORKTREE}/d.ipynb", -7, "NotebookEdit"),
        *edit("relative.py", -6),
        *edit(f"{WORKTREE}/old.py", -2000),
    )
    subagents = path.with_suffix("") / "subagents"
    subagents.mkdir(parents=True)
    fake.write(
        subagents / "agent-1.jsonl",
        *({**r, "isSidechain": True} for r in edit(f"{WORKTREE}/e.py", -5)),
    )
    edits = fake.adapter.edits(epoch(-60), epoch(0))
    assert sorted((e.path, e.session_id) for e in edits) == [
        (f"{WORKTREE}/{name}", path.stem)
        for name in ("a.py", "b.py", "c.py", "d.ipynb", "e.py")
    ]


def test_edits_read_only_the_end_of_a_long_transcript(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(claude_code, "_TAIL_STEP", 4096)
    fake = FakeClaude(tmp_path / "claude")
    old = [r for n in range(200) for r in edit(f"{WORKTREE}/old{n}.py", -3000 + n)]
    path = write_parent(fake, *old, *edit(f"{WORKTREE}/new.py", -10))
    edits = fake.adapter.edits(epoch(-60), epoch(0))
    assert [e.path for e in edits] == [f"{WORKTREE}/new.py"]
    assert path.stat().st_size > 8 * 4096
    # Only the end of the file is read.
    lines = list(claude_code._lines_since(path, epoch(-60)))  # pyright: ignore[reportPrivateUsage]
    assert len(lines) < 20


@pytest.mark.parametrize("result", ["failed", "rejected", "denied", None])
def test_edits_need_a_successful_result(tmp_path: Path, result: str | None) -> None:
    fake = FakeClaude(tmp_path / "claude")
    write_parent(
        fake,
        *edit(f"{WORKTREE}/a.py", -10, result=result),
        *edit(f"{WORKTREE}/b.py", -9),
    )
    edits = fake.adapter.edits(epoch(-60), epoch(0))
    assert [e.path for e in edits] == [f"{WORKTREE}/b.py"]


def test_a_result_with_no_call_in_the_lines_does_not_count() -> None:
    # The tail read can start between a call and its result.
    call, result = edit(f"{WORKTREE}/a.py", -10)
    lines = [(json.dumps(r) + "\n").encode() for r in (result,)]
    assert claude_code._edits_in(lines, epoch(-60), epoch(0), "s") == []  # pyright: ignore[reportPrivateUsage]
    lines = [(json.dumps(r) + "\n").encode() for r in (call, result)]
    found = claude_code._edits_in(lines, epoch(-60), epoch(0), "s")  # pyright: ignore[reportPrivateUsage]
    assert found == [Edit(f"{WORKTREE}/a.py", epoch(-10), "s")]


def test_a_failed_later_edit_does_not_take_the_parent(tmp_path: Path) -> None:
    # Session A changes the file; later, session B tries an edit of the same
    # file that fails. A review of A's change starts after both.
    fake = FakeClaude(tmp_path / "claude")
    a = write_parent(fake, *edit(f"{WORKTREE}/a.py", -30))
    write_parent(fake, *edit(f"{WORKTREE}/a.py", -5, result="failed"))
    edits = fake.adapter.edits(epoch(-60), epoch(0))
    by_path: dict[str, list[Edit]] = {}
    for e in edits:
        by_path.setdefault(e.path, []).append(e)
    assert find_parent("review", epoch(0), [f"{WORKTREE}/a.py"], by_path) == a.stem


def test_edits_skip_a_transcript_older_than_the_window(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    path = write_parent(fake, *edit(f"{WORKTREE}/a.py", -10))
    os.utime(path, (epoch(-120), epoch(-120)))
    assert fake.adapter.edits(epoch(-60), epoch(0)) == []


# --- Parent matching -----------------------------------------------------------------


def edits_by_path(*edits: Edit) -> dict[str, list[Edit]]:
    out: dict[str, list[Edit]] = {}
    for e in edits:
        out.setdefault(e.path, []).append(e)
    return out


def test_find_parent() -> None:
    start = epoch(0)
    a, b = "/w/a.py", "/w/b.py"
    one = edits_by_path(Edit(a, epoch(-5), "p1"), Edit(b, epoch(-4), "p1"))
    assert find_parent("r", start, [a, b], one) == "p1"
    # No edit of the files.
    assert find_parent("r", start, [a, b], {}) is None
    # Only edits outside the window: too early, or after the review started.
    window = PARENT_WINDOW / 60
    late = edits_by_path(Edit(a, epoch(-window - 1), "p1"), Edit(a, epoch(1), "p2"))
    assert find_parent("r", start, [a], late) is None
    # The session that edited more of the files wins.
    more = edits_by_path(
        Edit(a, epoch(-5), "p1"), Edit(b, epoch(-4), "p1"), Edit(a, epoch(-1), "p2")
    )
    assert find_parent("r", start, [a, b], more) == "p1"
    # With as many files, the session with the last edit wins.
    later = edits_by_path(Edit(a, epoch(-5), "p1"), Edit(a, epoch(-1), "p2"))
    assert find_parent("r", start, [a], later) == "p2"
    # A copy of the same records (a fork) is a tie: no parent.
    tie = edits_by_path(Edit(a, epoch(-5), "p1"), Edit(a, epoch(-5), "p2"))
    assert find_parent("r", start, [a], tie) is None
    # The review is not its own parent.
    assert find_parent("p1", start, [a], one) is None


# --- The link pass, with the server -----------------------------------------------


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "server")
    yield s
    s.close()


@pytest.fixture
def fake(tmp_path: Path) -> FakeClaude:
    return FakeClaude(tmp_path / "claude")


@pytest.fixture
def ctx(server: Server, fake: FakeClaude, tmp_path: Path) -> Context:
    return context(server.api("laptop"), tmp_path / "state", fake, "laptop")


def sync(ctx: Context, path: Path, workspace: str) -> None:
    session = ctx.adapter.session_for_transcript(path)
    assert session is not None
    Syncer(ctx, session, workspace).run()


def server_list(ctx: Context, **filters: str) -> list[str]:
    return [s["session_id"] for s in ctx.api.all_sessions(harness=HARNESS, **filters)]


def make_review(fake: FakeClaude, text: str, minutes: float) -> Path:
    path = fake.transcript(WORKTREE)
    fake.write(path, *review_records(text, minutes), text_reply())
    return path


def test_link_children_for_new_and_old_reviews(ctx: Context, fake: FakeClaude) -> None:
    parent = write_parent(
        fake,
        *edit(f"{WORKTREE}/a.py", -10),
        *edit(f"{WORKTREE}/lib/b.py", -9),
    )
    sync(ctx, parent, REPO)
    # A review that an older client backed up, with no links.
    old = make_review(fake, first_pass(["a.py", "lib/b.py"]), 0)
    sync(ctx, old, WORKTREE)
    assert set(server_list(ctx, top_level="1")) == {parent.stem, old.stem}
    # A new second pass, and a review of files that no session edited with
    # an edit tool.
    second = make_review(fake, second_pass(["lib/b.py"]), 2)
    orphan = make_review(fake, first_pass(["made-by-bash.py"]), 3)
    result = link_children(ctx, ClaudeCode(fake.root))
    assert (result.linked, result.unlinked) == (2, 1)
    # The old review gets its links now; the others with their first sync.
    assert result.sent == 1
    for path in (second, orphan):
        sync(ctx, path, WORKTREE)
    assert server_list(ctx, top_level="1") == [parent.stem]
    assert sorted(server_list(ctx, parent=parent.stem)) == sorted(
        [old.stem, second.stem]
    )
    manifest = ctx.api.manifest(HARNESS, orphan.stem)
    assert manifest["metadata"].get("automation") == SECURITY_REVIEW
    assert "links" not in manifest["metadata"]
    # A second pass changes nothing, and reads no transcript for edits.
    calls: list[int] = []
    adapter = ClaudeCode(fake.root)
    real = adapter.edits
    adapter.edits = lambda *a: calls.append(1) or real(*a)  # type: ignore[method-assign]
    again = link_children(ctx, adapter)
    assert (again.classified, again.linked, again.unlinked, again.sent) == (0, 0, 0, 0)
    assert calls == []


def test_link_children_sends_at_most_max_sends(
    ctx: Context, fake: FakeClaude, monkeypatch: pytest.MonkeyPatch
) -> None:
    reviews = [make_review(fake, first_pass([f"f{n}.py"]), n) for n in range(3)]
    for path in reviews:
        sync(ctx, path, WORKTREE)
    monkeypatch.setattr(children_mod, "MAX_SENDS", 2)
    assert link_children(ctx, ClaudeCode(fake.root)).sent == 2
    assert link_children(ctx, ClaudeCode(fake.root)).sent == 1
    assert server_list(ctx, top_level="1") == []


def test_sweep_links_children(
    ctx: Context, fake: FakeClaude, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = write_parent(fake, *edit(f"{WORKTREE}/a.py", -10))
    review = make_review(fake, first_pass(["a.py"]), 0)
    cfg = cli_mod.config_mod.Config(host="laptop", state_dir=tmp_path / "state")
    monkeypatch.setattr(cli_mod, "refresh_states", lambda *_: 0)
    ctx.adapter = ClaudeCode(fake.root)
    assert cli_mod._sweep(cfg, ctx, None) == 0  # pyright: ignore[reportPrivateUsage]
    assert server_list(ctx, parent=parent.stem) == [review.stem]
    # The internal lists still see every session.
    assert set(server_list(ctx)) == {parent.stem, review.stem}


# --- The CLI ---------------------------------------------------------------------


def mock_api(
    handler: Callable[[httpx.Request], httpx.Response], capabilities: list[str]
) -> Api:
    def route(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/whoami":
            return httpx.Response(200, json={"capabilities": capabilities})
        return handler(request)

    client = httpx.Client(
        transport=httpx.MockTransport(route), base_url="http://server/v1"
    )
    return Api("http://server", "token", client=client)


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HYPERSPACE_CONFIG", str(tmp_path / "none.toml"))
    monkeypatch.setenv("HYPERSPACE_URL", "http://127.0.0.1")
    monkeypatch.setenv("HYPERSPACE_TOKEN", "token")


@pytest.mark.usefixtures("cli_env")
@pytest.mark.parametrize(
    ("capabilities", "argv", "top_level"),
    [
        ([CAP_CHILD_SESSIONS], [], "1"),
        ([CAP_CHILD_SESSIONS], ["--all"], None),
        ([CAP_SESSION_STATUS, CAP_LIST_CURSOR], [], None),
        ([CAP_CHILD_SESSIONS, CAP_SESSION_STATUS], ["--open"], "1"),
    ],
)
def test_ls_hides_children_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    capabilities: list[str],
    argv: list[str],
    top_level: str | None,
) -> None:
    calls: list[httpx.Request] = []

    def server(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"sessions": []})

    monkeypatch.setattr(
        cli_mod, "Api", lambda *args, **kwargs: mock_api(server, capabilities)
    )
    assert cli_mod.main(["ls", "--json", *argv]) == 0
    capsys.readouterr()
    assert calls[0].url.params.get("top_level") == top_level


def test_show_lists_children(
    server: Server,
    ctx: Context,
    fake: FakeClaude,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    parent = write_parent(fake, *edit(f"{WORKTREE}/a.py", -10))
    sync(ctx, parent, REPO)
    review = make_review(fake, first_pass(["a.py"]), 0)
    link_children(ctx, ClaudeCode(fake.root))
    sync(ctx, review, WORKTREE)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HYPERSPACE_CONFIG", str(tmp_path / "none.toml"))
    monkeypatch.setenv("HYPERSPACE_URL", "http://127.0.0.1")
    monkeypatch.setenv("HYPERSPACE_TOKEN", "token")
    # Each command closes its client.
    monkeypatch.setattr(cli_mod, "Api", lambda *args, **kwargs: server.api("laptop"))
    assert cli_mod.main(["show", parent.stem]) == 0
    out = capsys.readouterr().out
    assert "child sessions:" in out
    assert f"{review.stem}  {SECURITY_REVIEW}" in out
    assert cli_mod.main(["show", review.stem]) == 0
    out = capsys.readouterr().out
    assert f"parent    {parent.stem}" in out
    assert f"automation {SECURITY_REVIEW}" in out
