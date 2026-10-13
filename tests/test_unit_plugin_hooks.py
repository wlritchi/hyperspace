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
    assert sync <= set(config)
    for event, groups in config.items():
        for group in groups:
            for entry in group["hooks"]:
                assert entry["command"] == '"${CLAUDE_PLUGIN_ROOT}/hooks/forward.sh"'
                assert entry.get("async", False) is (event not in sync), event


def test_forwarder_is_executable() -> None:
    assert os.access(FORWARD, os.X_OK)
