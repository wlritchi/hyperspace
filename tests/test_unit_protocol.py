# vim: filetype=python
"""Validation functions in hyperspace-protocol."""

import pytest

from hyperspace_protocol import (
    ValidationError,
    validate_file_path,
    validate_harness,
    validate_host_name,
    validate_id,
    validate_session_id,
    validate_sha256,
    validate_workspace,
)


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
