# vim: filetype=python
"""Validation functions in hyperspace-protocol."""

from typing import Any

import pytest

from hyperspace_protocol import (
    MAX_HEARTBEAT_SESSIONS,
    MAX_LAUNCH_PROMPT,
    MAX_STATUS_EXCERPT,
    MAX_STATUS_NOTE,
    MAX_WAKE_HANDLE,
    MAX_WAKE_PENDING,
    REOPEN_NOTE_PREFIX,
    ValidationError,
    validate_file_path,
    validate_harness,
    validate_heartbeat,
    validate_host_name,
    validate_id,
    validate_launch_failure,
    validate_launch_request,
    validate_launch_session,
    validate_session_id,
    validate_sha256,
    validate_status_request,
    validate_wake_request,
    validate_workspace,
)

STATUS: dict[str, Any] = {
    "status": "working",
    "source_kind": "hook",
    "explicit": False,
    "base_seq": 0,
}


@pytest.mark.parametrize(
    "value",
    [
        "20e924a9-68b5-48d3-b1a4-15aa285980fd",
        "rollout-2026-10-13T12-00-00",
        "a",
        "A" * 128,
    ],
)
def test_session_id_valid(value: str) -> None:
    assert validate_session_id(value) == value


@pytest.mark.parametrize(
    "value",
    ["", "A" * 129, ".hidden", "-flag", "a/b", "a b", "a\x00", "../x", "ü"],
)
def test_session_id_invalid(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_session_id(value)


@pytest.mark.parametrize("value", ["laptop", "agent-1", "host.example", "a" * 64])
def test_host_name_valid(value: str) -> None:
    assert validate_host_name(value) == value


@pytest.mark.parametrize("value", ["", "Laptop", "-x", "a" * 65, "a/b", "a b"])
def test_host_name_invalid(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_host_name(value)


@pytest.mark.parametrize("value", ["claude-code", "codex", "x"])
def test_harness_valid(value: str) -> None:
    assert validate_harness(value) == value


@pytest.mark.parametrize("value", ["", "Claude", "a_b", "a" * 33, "-x"])
def test_harness_invalid(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_harness(value)


@pytest.mark.parametrize(
    "value",
    [
        "transcript.jsonl",
        "subagents/agent-a1b2.jsonl",
        "subagents/workflows/wf_1/agent-x.meta.json",
        "tool-results/toolu_01.txt",
        "tool-results/pdf-1/page-1.png",
        "file-history/0123abcd@v2",
        "plans/some-plan.md",
        "a%20b",
    ],
)
def test_file_path_valid(value: str) -> None:
    assert validate_file_path(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "/etc/passwd",
        "../x",
        "a/../b",
        "a/./b",
        "a//b",
        "a/",
        ".",
        "a\\b",
        "a b",
        "a\x00b",
        "x" * 513,
        "/".join(["a"] * 33),
    ],
)
def test_file_path_invalid(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_file_path(value)


def test_id() -> None:
    assert validate_id("g_01JA6ABC") == "g_01JA6ABC"
    for bad in ("", "_x", "a" * 65, "a.b", "a/b"):
        with pytest.raises(ValidationError):
            validate_id(bad)


def test_sha256() -> None:
    digest = "0" * 64
    assert validate_sha256(digest) == digest
    for bad in ("0" * 63, "A" * 64, "g" * 64):
        with pytest.raises(ValidationError):
            validate_sha256(bad)


def test_workspace() -> None:
    assert validate_workspace("/home/wlritchi/hyperspace")
    for bad in ("", "relative/path", "/a\x00b", "/" + "a" * 4096):
        with pytest.raises(ValidationError):
            validate_workspace(bad)


@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"status": "ready", "reason": "asked"},
        {"status": "waiting", "wake": "message", "note": "CI on PR #41"},
        {"status": "done", "source_kind": "user", "explicit": True, "base_seq": 7},
        {"source_kind": "agent", "source_session": "s2", "excerpt": "x" * 1000},
        {"note": "n" * MAX_STATUS_NOTE},
    ],
)
def test_status_request_valid(extra: dict[str, Any]) -> None:
    body = {**STATUS, **extra}
    assert validate_status_request({**body, "unknown": 1}) == body


@pytest.mark.parametrize(
    "extra",
    [
        {"status": "busy"},
        {"status": "ready"},
        {"status": "ready", "reason": "bored"},
        {"reason": "idle"},
        {"status": "waiting"},
        {"wake": "local"},
        {"note": "two\nlines"},
        {"note": "bell\x07"},
        {"note": "\x1b[2Jclear"},
        {"note": "c1\x9b"},
        {"note": "n" * (MAX_STATUS_NOTE + 1)},
        {"note": 3},
        {"excerpt": "x" * (MAX_STATUS_EXCERPT + 1)},
        {"source_kind": "robot"},
        {"source_session": "../x"},
        {"explicit": 1},
        {"base_seq": -1},
        {"base_seq": True},
        {"base_seq": "0"},
        {"base_seq": None},
    ],
)
def test_status_request_invalid(extra: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        validate_status_request({**STATUS, **extra})


def test_heartbeat_validation() -> None:
    ref = {"harness": "claude-code", "session_id": "s1"}
    assert validate_heartbeat({"interval": 60, "sessions": [ref]}) == {
        "interval": 60,
        "sessions": [ref],
    }
    full = {
        "interval": 10,
        "sessions": [],
        "unclosed": [{**ref, "reason": "unpushed work"}],
        "final": True,
        "wakes": True,
        "launches": True,
    }
    assert validate_heartbeat(full) == full
    assert "wakes" not in validate_heartbeat({"interval": 60, "wakes": False})
    assert "launches" not in validate_heartbeat({"interval": 60, "launches": False})
    assert validate_heartbeat({"interval": 3600}) == {"interval": 3600, "sessions": []}
    assert validate_heartbeat({"interval": 60, "source": "ab_12-"}) == {
        "interval": 60,
        "source": "ab_12-",
        "sessions": [],
    }
    assert "source" not in validate_heartbeat({"interval": 60, "source": ""})
    for bad in (
        {"interval": 9},
        {"interval": 3601},
        {"interval": True},
        {"interval": 60, "sessions": {}},
        {"interval": 60, "sessions": [{"harness": "claude-code"}]},
        {"interval": 60, "sessions": [{**ref, "session_id": "../x"}]},
        {"interval": 60, "sessions": [ref] * (MAX_HEARTBEAT_SESSIONS + 1)},
        {"interval": 60, "unclosed": [ref]},
        {"interval": 60, "unclosed": [{**ref, "reason": "a\nb"}]},
        {"interval": 60, "final": "yes"},
        {"interval": 60, "wakes": 1},
        {"interval": 60, "launches": "yes"},
        {"interval": 60, "source": "../x"},
        {"interval": 60, "source": "x" * 65},
        {"interval": 60, "source": 3},
    ):
        with pytest.raises(ValidationError):
            validate_heartbeat(bad)


def test_wake_request_validation() -> None:
    body = {
        "event": "wake",
        "account": "default",
        "session": "s1",
        "handle": "reviewer",
        "pending": 2,
        "items": [{"text": "not read"}],
        "later": "field",
    }
    assert validate_wake_request(body) == {
        "session": "s1",
        "handle": "reviewer",
        "pending": 2,
    }
    assert validate_wake_request({"session": "s1"}) == {"session": "s1", "pending": 0}
    # `pending` and `handle` are for information only. A bad value is cut or
    # dropped, so that the wake is not lost.
    assert validate_wake_request({"session": "s1", "pending": 2**70}) == {
        "session": "s1",
        "pending": MAX_WAKE_PENDING,
    }
    for handle, kept in (
        (3, None),
        ("", None),
        ("a\x1b[2J", "a?[2J"),
        ("x" * (MAX_WAKE_HANDLE + 1), "x" * MAX_WAKE_HANDLE),
    ):
        checked = validate_wake_request({"session": "s1", "handle": handle})
        assert checked.get("handle") == kept
    # The reason of a reopen is cleaned and cut to fit the note of the reopen.
    by = "7d0c4e57-2b8e-4c43-9a51-1f0e8c3c2a10"
    for reason, kept in (
        ("fix the bug", "fix the bug"),
        (" a\nb\x1b ", "a b"),
        ("x" * 400, "x" * (MAX_STATUS_NOTE - len(REOPEN_NOTE_PREFIX))),
    ):
        checked = validate_wake_request(
            {"session": "s1", "reopen": {"reason": reason, "by": by}}
        )
        assert checked.get("reopen") == {"reason": kept, "by": by}
    for reopen in (
        "fix it",
        {"by": by},
        {"reason": " \n", "by": by},
        {"reason": 7, "by": by},
        {"reason": "fix it"},
        {"reason": "fix it", "by": "../x"},
    ):
        with pytest.raises(ValidationError):
            validate_wake_request({"session": "s1", "reopen": reopen})
    for bad in (
        {},
        {"session": 7},
        {"session": "../x"},
        {"session": "s1", "pending": -1},
        {"session": "s1", "pending": "2"},
        {"session": "s1", "pending": False},
    ):
        with pytest.raises(ValidationError):
            validate_wake_request(bad)


LAUNCH: dict[str, Any] = {"host": "agent-1", "prompt": "review the change"}


def test_launch_request_validation() -> None:
    full = {
        **LAUNCH,
        "request": "3f1c2a9e-7d4b-4c1e-9a2f-0b8d6e5c4a31",
        "workspace": "/home/claude/workspace",
        "model": "claude-opus-5-5",
        "effort": "high",
        "name": "reviewer",
        "parent": "20e924a9-68b5-48d3-b1a4-15aa285980fd",
    }
    assert validate_launch_request(full) == full
    # Unknown fields are dropped, and null is the same as absent.
    assert validate_launch_request({**LAUNCH, "later": 1, "model": None}) == LAUNCH
    for model in ("opus", "sonnet[1m]", "us.anthropic.claude-x:0", "a/b@c"):
        assert validate_launch_request({**LAUNCH, "model": model})["model"] == model
    largest = "é" * (MAX_LAUNCH_PROMPT // 2)
    assert validate_launch_request({**LAUNCH, "prompt": largest})["prompt"] == largest


@pytest.mark.parametrize(
    "extra",
    [
        {"host": None},
        {"host": "Laptop"},
        {"prompt": None},
        {"prompt": ""},
        {"prompt": 7},
        {"prompt": "a\x00b"},
        {"prompt": "x" * (MAX_LAUNCH_PROMPT + 1)},
        {"prompt": "é" * (MAX_LAUNCH_PROMPT // 2 + 1)},
        {"prompt": "\ud800"},
        {"request": ""},
        {"request": "a b"},
        {"request": "x" * 65},
        {"request": 7},
        {"workspace": "relative/path"},
        {"workspace": ""},
        {"workspace": 7},
        {"model": "--dangerously-skip-permissions"},
        {"model": ""},
        {"model": "a b"},
        {"model": 7},
        {"effort": "-high"},
        {"effort": "HIGH"},
        {"effort": ""},
        {"name": "-x"},
        {"name": ""},
        {"name": "a\nb"},
        {"name": "x" * 129},
        {"parent": "../x"},
        {"parent": 7},
    ],
)
def test_launch_request_invalid(extra: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        validate_launch_request({**LAUNCH, **extra})


def test_launch_link_and_failure_bodies() -> None:
    assert validate_launch_session({"session": "s1"}) == "s1"
    for bad in ({}, {"session": 7}, {"session": "../x"}):
        with pytest.raises(ValidationError):
            validate_launch_session(bad)
    assert validate_launch_failure({"reason": " claude:\n  bad\toption "}) == (
        "claude: bad option"
    )
    long = validate_launch_failure({"reason": "x" * 1000})
    assert long == "x" * MAX_STATUS_NOTE
    for bad in ({}, {"reason": 7}, {"reason": " \n "}):
        with pytest.raises(ValidationError):
            validate_launch_failure(bad)
