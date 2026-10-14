# vim: filetype=python
"""`hyperspace hook`: what each hook event does."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from hyperspace_client.config import Config
from hyperspace_client.hooks import handle
from hyperspace_client.state import State

SID = "11111111-1111-1111-1111-111111111111"
TRANSCRIPT = f"/home/u/.claude/projects/-w/{SID}.jsonl"


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(host="laptop", state_dir=tmp_path / "state")


def run(cfg: Config, **payload: Any) -> tuple[str, list[list[str]]]:
    started: list[list[str]] = []
    payload.setdefault("transcript_path", TRANSCRIPT)
    payload.setdefault("cwd", "/w")
    out = handle(payload, cfg, start=started.append)
    return out, started


@pytest.mark.parametrize(
    "event",
    ["PreToolUse", "PostToolUse", "PostToolUseFailure", "SubagentStop", "Stop"],
)
def test_checkpoint_events(cfg: Config, event: str) -> None:
    out, started = run(cfg, hook_event_name=event)
    assert out == ""
    assert started == [
        [
            "--quiet",
            "sync",
            "--checkpoint",
            "--transcript",
            TRANSCRIPT,
            "--cwd",
            "/w",
            "--event",
            event,
        ]
    ]


def test_session_end_does_not_wait(cfg: Config) -> None:
    _, started = run(cfg, hook_event_name="SessionEnd")
    assert started[0][-1] == "--no-wait"


def test_session_start_sweeps_and_prints_the_arrival(cfg: Config) -> None:
    state = State(cfg.state_dir)
    state.add_arrival(
        "claude-code",
        SID,
        {
            "teleport_id": "t_x",
            "target_host": "laptop",
            "target_workspace": "/w",
            "source": {"host": "agent-1", "workspace": "/a", "last_seen": "now"},
        },
    )
    state.close()
    out, started = run(cfg, hook_event_name="SessionStart", source="resume")
    assert "This session was teleported" in out
    assert ["--quiet", "sync", "--all", "--if-stale", "3600", "--unlock"] in started
    # The arrival is shown once.
    out, _ = run(cfg, hook_event_name="SessionStart", source="resume")
    assert "teleported" not in out


@pytest.mark.parametrize(
    ("source", "unlock"),
    [("startup", True), ("resume", True), ("clear", False), ("compact", False)],
)
def test_only_a_start_or_resume_reads_the_keyring(
    cfg: Config, source: str, unlock: bool
) -> None:
    _, started = run(cfg, hook_event_name="SessionStart", source=source)
    assert len(started) == 2
    assert all(("--unlock" in args) == unlock for args in started)
    _, started = run(cfg, hook_event_name="UserPromptSubmit")
    assert "--unlock" not in started[0]


def test_user_prompt_prints_notices(cfg: Config) -> None:
    state = State(cfg.state_dir)
    state.add_notice("claude-code", SID, "lease", "[hyperspace] a notice")
    state.close()
    out, started = run(cfg, hook_event_name="UserPromptSubmit")
    assert out == "[hyperspace] a notice"
    assert started[0][:3] == ["--quiet", "sync", "--checkpoint"]


@pytest.mark.parametrize(
    "payload",
    [
        {"hook_event_name": "Notification"},
        {"hook_event_name": "SomeFutureEvent"},
        {"hook_event_name": "Stop", "transcript_path": "/not-a-session.txt"},
        {"hook_event_name": "Stop", "transcript_path": ""},
        {},
    ],
)
def test_other_input_does_nothing(cfg: Config, payload: dict[str, Any]) -> None:
    started: list[list[str]] = []
    payload.setdefault("transcript_path", TRANSCRIPT)
    assert handle(payload, cfg, start=started.append) == ""
    assert started == []


def test_hook_command_never_fails(tmp_path: Path) -> None:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(tmp_path),
        "XDG_STATE_HOME": str(tmp_path / "state"),
        "XDG_CONFIG_HOME": str(tmp_path / "config"),
        "HYPERSPACE_CONFIG": str(tmp_path / "broken.toml"),
    }
    (tmp_path / "broken.toml").write_text("not = [valid toml")
    for stdin in ("not json", "[]", json.dumps({"hook_event_name": "Stop"})):
        done = subprocess.run(
            [sys.executable, "-m", "hyperspace_client.cli", "hook"],
            input=stdin,
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=60,
        )
        assert done.returncode == 0, done.stderr
        assert done.stdout == "" and done.stderr == ""
