# vim: filetype=python
"""The admin scripts, run as an operator would run them."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from store import SessionKey, Store, Writer

ROOT = Path(__file__).resolve().parent.parent
KEY = SessionKey("default", "claude-code", "11111111-1111-1111-1111-111111111111")
W = Writer("default", "laptop", "/w")


def run(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "-qs", str(ROOT / "scripts" / script), *args],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.slow
def test_purge_and_stats_beside_a_live_store(tmp_path: Path) -> None:
    data = tmp_path / "data"
    live = Store(data)  # stands for the running server
    g = live.create_generation(KEY, "transcript.jsonl", W, "root")["id"]
    live.append(KEY, g, W, 0, b"secret\n", hashlib.sha256(b"secret\n").hexdigest(), 1)

    stats = run("stats.py", "--data-dir", str(data))
    assert stats.returncode == 0, stats.stderr
    assert "sessions 1" in stats.stdout

    args = ["--data-dir", str(data), "--reason", "test", "session", "claude-code"]
    dry = run("purge.py", *args, KEY.session_id)
    assert dry.returncode == 1 and "would remove" in dry.stdout
    assert live.session_exists(KEY)

    done = run("purge.py", "--yes", *args, KEY.session_id)
    assert done.returncode == 0, done.stderr
    assert not live.session_exists(KEY)
    # The live store keeps working after the purge.
    g2 = live.create_generation(KEY, "transcript.jsonl", W, "root")["id"]
    live.append(KEY, g2, W, 0, b"new\n", hashlib.sha256(b"new\n").hexdigest(), 1)
    assert "purges" in run("stats.py", "--data-dir", str(data)).stdout
    live.close()
