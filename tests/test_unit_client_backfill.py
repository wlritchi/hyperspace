# vim: filetype=python
"""The backfill's argument handling, throttle, and headless runner."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from _hs import FakeClaude
from hyperspace_client.adapters import LocalSession
from hyperspace_client.api import Api
from hyperspace_client.backfill import (
    ALLOWED_TOOLS,
    MESSAGE,
    RESTRICTIONS,
    Candidate,
    RunResult,
    attempt_key,
    command,
    parse_age,
    run,
    run_claude,
)
from hyperspace_client.cli import build_parser
from hyperspace_client.state import State
from hyperspace_client.sync import Context

FAKE = str(Path(__file__).resolve().parent / "fake_claude.py")
HARNESS = "claude-code"


class StubApi:
    """The one call that `run` makes: the session state."""

    def __init__(self) -> None:
        self.events: dict[str, dict[str, Any]] = {}

    def state(self, harness: str, sid: str) -> dict[str, Any]:
        body: dict[str, Any] = {
            "state": "normal",
            "lease": {
                "state": "held",
                "host": "laptop",
                "since": "2026-10-16T00:00:00Z",
            },
        }
        if sid in self.events:
            body["status"] = self.events[sid]
        return body


def agent_done() -> dict[str, Any]:
    return {
        "seq": 1,
        "status": "done",
        "explicit": True,
        "source": {"kind": "agent", "host": "laptop"},
        "received": "2026-10-16T00:00:00Z",
    }


def test_parse_age() -> None:
    assert parse_age("30d") == 30 * 86_400
    assert parse_age("12h") == 12 * 3600
    assert parse_age("1.5w") == 1.5 * 604_800
    with pytest.raises(ValueError):
        parse_age("30 days")


def test_command_allows_only_status_and_done() -> None:
    argv = command("claude", "s1")
    assert argv[:5] == ["claude", "--resume", "s1", "-p", MESSAGE]
    assert argv[5:] == [*RESTRICTIONS, "--allowedTools", *ALLOWED_TOOLS]
    assert set(RESTRICTIONS) >= {"--restricted", "--strict-mcp-config", "dontAsk"}
    assert argv[argv.index("--tools") + 1] == "Bash"
    assert set(ALLOWED_TOOLS) == {
        "Bash(hyperspace status:*)",
        "Bash(hyperspace done:*)",
    }


def test_parser_accepts_backfill_options() -> None:
    args = build_parser().parse_args(
        [
            "backfill-status",
            "--since",
            "7d",
            "--rate",
            "10",
            "--limit",
            "3",
            "--dry-run",
        ]
    )
    assert (args.since, args.rate, args.limit, args.dry_run, args.retry) == (
        "7d",
        10.0,
        3,
        True,
        False,
    )


def test_run_claude_drops_session_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "outer")
    monkeypatch.setenv("CLAUDE_JOB_DIR", "/tmp/job")
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "echo")
    result = run_claude([sys.executable, FAKE, "--resume", "s1"], str(tmp_path), 30)
    assert result.returncode == 0
    report = json.loads(result.output.splitlines()[-1])
    assert report["argv"] == ["--resume", "s1"]
    assert Path(report["cwd"]).resolve() == tmp_path.resolve()
    assert report["env"] == {"CLAUDE_CODE_SESSION_ID": None, "CLAUDE_JOB_DIR": None}


def test_run_claude_timeout_stops_the_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pidfile = tmp_path / "child.pid"
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "hang")
    monkeypatch.setenv("FAKE_CLAUDE_PIDFILE", str(pidfile))
    start = time.monotonic()
    result = run_claude([sys.executable, FAKE], str(tmp_path), 2)
    assert result.returncode is None
    assert time.monotonic() - start < 20
    child = int(pidfile.read_text())
    # The child got the signal too. It can be a zombie for a short time.
    for _ in range(50):
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            break
        if Path(f"/proc/{child}/stat").read_text().split()[2] == "Z":
            break
        time.sleep(0.1)
    else:
        pytest.fail("the child of the timed-out process still runs")


def test_rate_limit_spacing(tmp_path: Path) -> None:
    fake = FakeClaude(tmp_path / "claude")
    api = StubApi()
    state = State(tmp_path / "state")
    ctx = Context(api=cast(Api, api), state=state, adapter=fake.adapter, host="laptop")
    now = [1000.0]
    starts: list[float] = []

    def runner(argv: list[str], cwd: str, timeout: float) -> RunResult:
        starts.append(now[0])
        now[0] += 10  # The resume takes 10 seconds.
        api.events[argv[2]] = agent_done()
        return RunResult(0, "set done\n")

    def sleep(seconds: float) -> None:
        now[0] += seconds

    candidates = [
        Candidate(LocalSession(HARNESS, f"s{i}", tmp_path / f"s{i}.jsonl"), "/w", 0, "")
        for i in range(3)
    ]
    lines: list[str] = []
    results = run(
        ctx,
        fake.adapter,
        candidates,
        claude="claude",
        rate=60,
        timeout=600,
        report=lines.append,
        runner=runner,
        clock=lambda: now[0],
        sleep=sleep,
    )
    assert starts == [1000.0, 1060.0, 1120.0]
    assert results == {"set": 3}
    assert lines[0] == "s0: set: set done"
    record = json.loads(state.get(attempt_key(HARNESS, "s2")) or "{}")
    assert record == {"started": 1120.0, "result": "set"}
    state.close()
