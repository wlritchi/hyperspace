# vim: filetype=python
"""The plugin's hook forwarder, with a fake `hyperspace` CLI."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PLUGIN = ROOT / "plugin" / "hyperspace"
FORWARD = PLUGIN / "hooks" / "forward.sh"
NOTICE = PLUGIN / "hooks" / "notice.sh"

FAKE_CLI = """#!{python}
import json, os, sys
stdin = sys.stdin.read()
with open(os.environ["FAKE_LOG"], "a") as f:
    f.write(json.dumps({{"argv": sys.argv[1:], "stdin": stdin}}) + "\\n")
print("output from the CLI")
"""


@pytest.fixture
def fake(tmp_path: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cli = bin_dir / "hyperspace"
    cli.write_text(FAKE_CLI.format(python=sys.executable))
    cli.chmod(0o755)
    return {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "FAKE_LOG": str(tmp_path / "calls.log"),
    }


def forward(payload: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(FORWARD)],
        input=payload,
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=30,
    )


def calls(env: dict[str, str]) -> list[dict[str, object]]:
    path = Path(env["FAKE_LOG"])
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines()]


def test_forwards_stdin_to_hyperspace_hook(fake: dict[str, str]) -> None:
    payload = json.dumps({"hook_event_name": "Stop", "transcript_path": "/x.jsonl"})
    done = forward(payload, fake)
    assert done.returncode == 0
    assert done.stdout.strip() == "output from the CLI"
    assert calls(fake) == [{"argv": ["hook"], "stdin": payload}]


def test_hyperspace_cli_can_hold_arguments(fake: dict[str, str]) -> None:
    cli = Path(fake["PATH"].split(":")[0]) / "hyperspace"
    env = {**fake, "PATH": "/usr/bin:/bin", "HYPERSPACE_CLI": f"{sys.executable} {cli}"}
    done = forward("{}", env)
    assert done.returncode == 0
    assert calls(env)[0]["argv"] == ["hook"]


def test_no_cli_does_nothing(tmp_path: Path) -> None:
    done = forward("{}", {"PATH": str(tmp_path)})
    assert done.returncode == 0
    assert done.stdout == "" and done.stderr == ""


def test_manifest_has_no_version() -> None:
    # The marketplace entry's git ref selects the version (its commit), so
    # no release has to edit this file.
    manifest = json.loads((PLUGIN / ".claude-plugin" / "plugin.json").read_text())
    assert "version" not in manifest
    assert manifest["name"] == "hyperspace"


def test_hooks_json_forwards_every_event() -> None:
    config = json.loads((PLUGIN / "hooks" / "hooks.json").read_text())["hooks"]
    sync = {"SessionStart", "UserPromptSubmit"}
    assert sync | {"Notification"} <= set(config)
    notice = []
    for event, groups in config.items():
        for group in groups:
            for entry in group["hooks"]:
                if entry["command"] == '"${CLAUDE_PLUGIN_ROOT}/hooks/notice.sh"':
                    notice.append((event, entry.get("async", False)))
                    continue
                assert entry["command"] == '"${CLAUDE_PLUGIN_ROOT}/hooks/forward.sh"'
                assert entry.get("async", False) is (event not in sync), event
    # The notice hook is synchronous, so that its output reaches the agent.
    assert notice == [("PostToolUse", False)]


def test_forwarder_is_executable() -> None:
    assert os.access(FORWARD, os.X_OK)


def notice_env(fake: dict[str, str], tmp_path: Path, **extra: str) -> dict[str, str]:
    return {
        **fake,
        "HOME": str(tmp_path),
        "XDG_STATE_HOME": str(tmp_path / "state"),
        **extra,
    }


def notice(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    payload = json.dumps({"hook_event_name": "PostToolUse"})
    return subprocess.run(
        [str(NOTICE)],
        input=payload,
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=30,
    )


def test_notice_sh_fast_path(fake: dict[str, str], tmp_path: Path) -> None:
    env = notice_env(fake, tmp_path, CLAUDE_CODE_SESSION_ID="s1")
    flags = tmp_path / "state" / "hyperspace" / "notify"
    flags.mkdir(parents=True)
    (flags / "other-session").touch()
    done = notice(env)
    assert (done.returncode, done.stdout, done.stderr) == (0, "", "")
    assert calls(env) == []


def test_notice_sh_prints_notice(fake: dict[str, str], tmp_path: Path) -> None:
    env = notice_env(fake, tmp_path, CLAUDE_CODE_SESSION_ID="s1")
    flags = tmp_path / "state" / "hyperspace" / "notify"
    flags.mkdir(parents=True)
    (flags / "s1").touch()
    done = notice(env)
    assert done.returncode == 0
    assert done.stdout.strip() == "output from the CLI"
    assert [c["argv"] for c in calls(env)] == [["hook", "--notices"]]


def test_notice_sh_without_session_id(fake: dict[str, str], tmp_path: Path) -> None:
    env = notice_env(fake, tmp_path)
    assert notice(env).returncode == 0
    assert calls(env) == []
    flags = tmp_path / "state" / "hyperspace" / "notify"
    flags.mkdir(parents=True)
    (flags / "s1").touch()
    assert notice(env).returncode == 0
    assert [c["argv"] for c in calls(env)] == [["hook", "--notices"]]


@pytest.mark.parametrize(
    ("state", "runs"),
    [
        ('{\n  "sessionId": "j1",\n  "resumeSessionId": "s1"\n}\n', True),
        ('{"resumeSessionId":"s1","sessionId":"j1"}', True),
        ('{"sessionId": "j1", "resumeSessionId": "j1"}', False),
        ('{"sessionId": "j1", "resumeSessionId": null, "x": "s1"}', False),
        ('{"sessionId": "j1", "resumeSessionId": "../s1"}', False),
        ('{"sessionId": "j1"}', False),
        ("", False),
        (None, False),
    ],
)
def test_notice_sh_in_a_job_under_its_own_id(
    fake: dict[str, str], tmp_path: Path, state: str | None, runs: bool
) -> None:
    # The job has its own id j1 and writes the transcript of s1.
    job = tmp_path / "jobs" / "aa"
    job.mkdir(parents=True)
    if state is not None:
        (job / "state.json").write_text(state)
    env = notice_env(
        fake, tmp_path, CLAUDE_CODE_SESSION_ID="j1", CLAUDE_JOB_DIR=str(job)
    )
    flags = tmp_path / "state" / "hyperspace" / "notify"
    flags.mkdir(parents=True)
    # A path outside the flag directory must not count.
    (flags.parent / "s1").touch()
    (flags / "s1").touch()
    done = notice(env)
    assert (done.returncode, done.stderr) == (0, "")
    assert len(calls(env)) == (1 if runs else 0)


def test_notice_sh_hides_an_old_cli(tmp_path: Path) -> None:
    # A client without --notices fails; the hook still exits 0 quietly.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cli = bin_dir / "hyperspace"
    cli.write_text("#!/bin/sh\necho 'error: unknown option' >&2\nexit 2\n")
    cli.chmod(0o755)
    env = notice_env(
        {"PATH": f"{bin_dir}:/usr/bin:/bin"}, tmp_path, CLAUDE_CODE_SESSION_ID="s1"
    )
    flags = tmp_path / "state" / "hyperspace" / "notify"
    flags.mkdir(parents=True)
    (flags / "s1").touch()
    done = notice(env)
    assert (done.returncode, done.stdout, done.stderr) == (0, "", "")


def test_plugin_commands_and_skills_only_call_the_cli() -> None:
    texts = [
        (PLUGIN / "commands" / "done.md").read_text(),
        (PLUGIN / "skills" / "session-status" / "SKILL.md").read_text(),
        (PLUGIN / "skills" / "session-overview" / "SKILL.md").read_text(),
    ]
    for text in texts:
        assert text.startswith("---\n")
        assert "hyperspace " in text
    assert "hyperspace done --by-user" in texts[0]
