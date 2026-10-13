# vim: filetype=python
"""The plugin's hook script, with a fake `hyperspace` CLI."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
HOOK = ROOT / "plugin" / "hyperspace" / "hooks" / "hyperspace-hook.py"

FAKE_CLI = """#!{python}
import json, os, sys, time
with open(os.environ["FAKE_LOG"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
mode = os.environ.get("FAKE_MODE", "")
if mode == "slow":
    time.sleep(3)
if mode == "fail":
    print("must not be shown")
    sys.exit(2)
if "notice" in sys.argv:
    print("[hyperspace] notice text")
if "check-resume" in sys.argv:
    print(json.dumps({{"decision": "block", "reason": "moved"}}))
"""


@pytest.fixture
def fake(tmp_path: Path) -> dict[str, str]:
    cli = tmp_path / "fake-hyperspace"
    cli.write_text(FAKE_CLI.format(python=sys.executable))
    cli.chmod(0o755)
    return {
        "HYPERSPACE_CLI": str(cli),
        "FAKE_LOG": str(tmp_path / "calls.log"),
        "PATH": os.environ.get("PATH", ""),
    }


def hook(
    mode: str, payload: object, env: dict[str, str]
) -> tuple[subprocess.CompletedProcess[str], float]:
    start = time.monotonic()
    done = subprocess.run(
        [sys.executable, str(HOOK), mode],
        input=payload if isinstance(payload, str) else json.dumps(payload),
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=30,
    )
    return done, time.monotonic() - start


def calls(env: dict[str, str], wait: float = 0) -> list[list[str]]:
    deadline = time.monotonic() + wait
    path = Path(env["FAKE_LOG"])
    while True:
        lines = path.read_text().splitlines() if path.exists() else []
        if lines or time.monotonic() >= deadline:
            return [json.loads(x) for x in lines]
        time.sleep(0.05)


INPUT = {
    "session_id": "11111111-1111-1111-1111-111111111111",
    "transcript_path": "/home/u/.claude/projects/-w/11111111-1111-1111-1111-111111111111.jsonl",
    "cwd": "/w",
    "hook_event_name": "SessionStart",
    "source": "resume",
}


def test_notice_passes_output_through(fake: dict[str, str]) -> None:
    done, _ = hook("notice", INPUT, fake)
    assert done.returncode == 0
    assert done.stdout.strip() == "[hyperspace] notice text"
    assert calls(fake)[0] == [
        "--quiet",
        "notice",
        "--transcript",
        INPUT["transcript_path"],
        "--cwd",
        "/w",
        "--source",
        "resume",
    ]


def test_check_resume_prints_the_decision(fake: dict[str, str]) -> None:
    done, _ = hook(
        "check-resume", {**INPUT, "hook_event_name": "UserPromptSubmit"}, fake
    )
    assert json.loads(done.stdout) == {"decision": "block", "reason": "moved"}


def test_checkpoint_returns_at_once_and_runs_detached(fake: dict[str, str]) -> None:
    env = {**fake, "FAKE_MODE": "slow"}
    done, took = hook("checkpoint", {**INPUT, "hook_event_name": "PostToolUse"}, env)
    assert done.returncode == 0 and done.stdout == ""
    assert took < 2
    recorded = calls(env, wait=5)
    assert recorded[0][:3] == ["--quiet", "sync", "--checkpoint"]
    assert recorded[0][-2:] == ["--event", "PostToolUse"]


def test_start_also_sweeps_and_end_does_not_wait(fake: dict[str, str]) -> None:
    hook("start", INPUT, fake)
    recorded = calls(fake, wait=5)
    deadline = time.monotonic() + 5
    while len(recorded) < 2 and time.monotonic() < deadline:
        time.sleep(0.05)
        recorded = calls(fake)
    assert ["--quiet", "sync", "--all", "--if-stale", "3600"] in recorded
    Path(fake["FAKE_LOG"]).unlink()
    hook("end", {**INPUT, "hook_event_name": "SessionEnd"}, fake)
    assert calls(fake, wait=5)[0][-1] == "--no-wait"


@pytest.mark.parametrize(
    "payload",
    ["not json", "[]", {"session_id": "x"}, {**INPUT, "transcript_path": ""}],
)
def test_bad_input_does_nothing(fake: dict[str, str], payload: object) -> None:
    done, _ = hook("notice", payload, fake)
    assert done.returncode == 0 and done.stdout == "" and done.stderr == ""
    assert calls(fake) == []


def test_failing_cli_shows_nothing(fake: dict[str, str]) -> None:
    done, _ = hook("notice", INPUT, {**fake, "FAKE_MODE": "fail"})
    assert done.returncode == 0 and done.stdout == ""


def test_missing_cli_does_nothing(tmp_path: Path) -> None:
    env = {"PATH": str(tmp_path), "HYPERSPACE_CLI": str(tmp_path / "missing")}
    done, _ = hook("notice", INPUT, env)
    assert done.returncode == 0 and done.stdout == "" and done.stderr == ""
    done, _ = hook("checkpoint", INPUT, {"PATH": str(tmp_path)})
    assert done.returncode == 0 and done.stdout == ""


def test_hooks_json_lists_the_spec_events() -> None:
    config = json.loads((HOOK.parent / "hooks.json").read_text())["hooks"]
    assert set(config) == {
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "SubagentStop",
        "PreCompact",
        "PostCompact",
        "Stop",
        "SessionEnd",
    }
    for event, groups in config.items():
        for group in groups:
            for entry in group["hooks"]:
                mode = entry["command"].rsplit(" ", 1)[-1]
                sync = mode in ("notice", "check-resume")
                assert entry.get("async", False) is not sync, (event, mode)
