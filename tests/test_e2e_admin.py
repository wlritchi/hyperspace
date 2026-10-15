# vim: filetype=python
"""The admin scripts, run as an operator would run them."""

import hashlib
import sqlite3
import subprocess
from pathlib import Path

import pytest

from store import _MIGRATIONS, SessionKey, Store, Writer

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


@pytest.mark.slow
def test_stats_keeps_reads_of_a_purged_session_apart_from_a_new_session(
    tmp_path: Path,
) -> None:
    # SQLite gives a new session the rowid of a purged session that had the
    # highest rowid. The audit rows of the purged session must not then show
    # as reads of the new session.
    data = tmp_path / "data"
    store = Store(data)
    secret = SessionKey("default", "claude-code", "secret-session")
    fresh = SessionKey("default", "claude-code", "fresh-session")
    store.create_generation(secret, "transcript.jsonl", W, "root")
    store.audit_read(secret, "laptop", "manifest")
    store.purge_session(secret, "leaked token")
    store.create_generation(fresh, "transcript.jsonl", W, "root")
    store.close()

    stats = run("stats.py", "--data-dir", str(data))
    assert stats.returncode == 0, stats.stderr
    reads = stats.stdout.split("recent reads", 1)[1]
    assert "fresh-session" not in reads
    assert "claude-code/secret-session (purged)" in reads


@pytest.mark.slow
def test_stats_does_not_name_a_session_for_an_old_read_of_unknown_session(
    tmp_path: Path,
) -> None:
    # A version 1 index: a read of a purged session, and then a new session
    # with the same rowid, which also has a read.
    data = tmp_path / "data"
    data.mkdir()
    db = sqlite3.connect(data / "index.sqlite3")
    db.executescript(_MIGRATIONS[0])
    db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    db.execute("INSERT INTO schema_version VALUES (1)")
    db.execute(
        "INSERT INTO sessions (id, account, harness, session_id, created) "
        "VALUES (1, 'default', 'claude-code', 'fresh-session', 't4')"
    )
    db.executemany(
        "INSERT INTO read_audit (ts, account, host, session, what) "
        "VALUES (?, 'default', ?, 1, 'manifest')",
        [("t2", "old-host"), ("t5", "new-host")],
    )
    db.execute(
        "INSERT INTO purges (ts, target, reason) "
        "VALUES ('t3', 'session default/claude-code/secret-session', 'leak')"
    )
    db.commit()
    db.close()
    Store(data).close()  # migrates the index

    stats = run("stats.py", "--data-dir", str(data))
    assert stats.returncode == 0, stats.stderr
    reads = stats.stdout.split("recent reads", 1)[1].splitlines()
    old = next(line for line in reads if "old-host" in line)
    new = next(line for line in reads if "new-host" in line)
    assert "(unknown)" in old and "fresh-session" not in old
    assert "claude-code/fresh-session" in new and "(purged)" not in new
