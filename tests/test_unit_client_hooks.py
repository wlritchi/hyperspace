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
from hyperspace_client.hooks import handle, notices
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
    assert len(started) == 3
    assert started[2][:3] == ["--quiet", "launcher", "--if-absent"]
    assert all(("--unlock" in args) == unlock for args in started)
    _, started = run(cfg, hook_event_name="UserPromptSubmit")
    assert "--unlock" not in started[0]


def test_launcher_off_switch(cfg: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYPERSPACE_LAUNCHER", "off")
    _, started = run(cfg, hook_event_name="SessionStart", source="startup")
    assert not any("launcher" in args for args in started)


def test_a_runner_hook_does_not_start_a_launcher(
    cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The runner's launcher is the entrypoint's main process. A hook in a
    # process that runs before it (for example a probe with `claude -p`) must
    # not take the host's launcher lock.
    monkeypatch.setenv("HYPERSPACE_LAUNCHER", "runner")
    cfg.host_kind = "runner"
    _, started = run(cfg, hook_event_name="SessionStart", source="startup")
    assert not any("launcher" in args for args in started)
    assert ["--quiet", "sync", "--all", "--if-stale", "3600", "--unlock"] in started


def _seen_status(cfg: Config, status: str) -> None:
    state = State(cfg.state_dir)
    try:
        record = state.session("claude-code", SID)
        record.status = {"seq": 1, "status": status}
        state.save_session(record)
    finally:
        state.close()


def _job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run in the background job 1111aaaa, whose own id SID has a
    conversation."""
    root = tmp_path / "claude"
    project = root / "projects" / "-w"
    project.mkdir(parents=True)
    record = {"type": "user", "message": {"role": "user", "content": "hi"}}
    (project / f"{SID}.jsonl").write_text(json.dumps(record) + "\n")
    job = root / "jobs" / "1111aaaa"
    job.mkdir(parents=True)
    (job / "state.json").write_text(json.dumps({"sessionId": SID}))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(root))
    monkeypatch.setenv("CLAUDE_JOB_DIR", str(job))


@pytest.mark.parametrize(
    ("in_job", "status", "closes"),
    [(True, "done", True), (True, "ready", False), (False, "done", False)],
)
def test_session_end_closes_a_done_job(
    cfg: Config,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    in_job: bool,
    status: str,
    closes: bool,
) -> None:
    if in_job:
        _job(tmp_path, monkeypatch)
    _seen_status(cfg, status)
    _, started = run(cfg, hook_event_name="SessionEnd")
    close = ["--quiet", "launcher", "--close-job", "1111aaaa"]
    assert (close in started) == closes


def test_session_end_keeps_a_job_with_a_queued_reopen(
    cfg: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _job(tmp_path, monkeypatch)
    _seen_status(cfg, "done")
    state = State(cfg.state_dir)
    try:
        state.want_status(
            "claude-code",
            SID,
            {"status": "working", "source_kind": "hook", "explicit": True},
        )
    finally:
        state.close()
    _, started = run(cfg, hook_event_name="SessionEnd")
    assert ["--quiet", "launcher", "--close-job", "1111aaaa"] not in started


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
        {"hook_event_name": "SubagentStart"},
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


def test_hook_records_wanted_status(cfg: Config) -> None:
    _, started = run(cfg, hook_event_name="UserPromptSubmit")
    assert started[0][:3] == ["--quiet", "sync", "--checkpoint"]
    state = State(cfg.state_dir)
    queued = state.wanted_status("claude-code", SID)
    assert queued is not None
    assert queued[0]["status"] == "working" and queued[0]["base_seq"] == 0
    state.close()
    run(cfg, hook_event_name="Stop")
    state = State(cfg.state_dir)
    queued = state.wanted_status("claude-code", SID)
    assert queued is not None and queued[0]["reason"] == "idle"
    state.close()


def test_notification_event_is_handled(cfg: Config) -> None:
    out, started = run(
        cfg, hook_event_name="Notification", notification_type="permission_prompt"
    )
    assert out == ""
    assert started[0][-2:] == ["--event", "Notification"]
    state = State(cfg.state_dir)
    queued = state.wanted_status("claude-code", SID)
    assert queued is not None and queued[0]["reason"] == "asked"
    state.close()


def test_notices_mode(cfg: Config) -> None:
    payload = {"hook_event_name": "PostToolUse", "transcript_path": TRANSCRIPT}
    assert notices(payload, cfg) == ""
    state = State(cfg.state_dir)
    state.add_notice("claude-code", SID, "status", "[hyperspace] marked done")
    state.close()
    out = json.loads(notices(payload, cfg))
    assert out == {
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": "[hyperspace] marked done",
        }
    }
    assert notices(payload, cfg) == ""
    assert not (cfg.state_dir / "notify" / SID).exists()


def test_notices_skip_a_subagent(cfg: Config) -> None:
    state = State(cfg.state_dir)
    state.add_notice("claude-code", SID, "status", "[hyperspace] marked done")
    state.close()
    sub = {
        "hook_event_name": "PostToolUse",
        "transcript_path": TRANSCRIPT,
        "agent_id": "a1b2c3",
    }
    # The notice is for the main agent. A subagent's tool call leaves it.
    assert notices(sub, cfg) == ""
    assert (cfg.state_dir / "notify" / SID).exists()
    main = {"hook_event_name": "PostToolUse", "transcript_path": TRANSCRIPT}
    out = json.loads(notices(main, cfg))
    assert out["hookSpecificOutput"]["additionalContext"] == "[hyperspace] marked done"
