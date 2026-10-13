#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Show what the store holds: sizes per host, workspace, and harness, the
largest sessions, conflicts, purges, and recent reads.

    scripts/stats.py [--top N] [--reads N]

It opens the index read-only, so it is safe while the server runs.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse
import os
import sqlite3
from pathlib import Path


def data_dir() -> Path:
    if os.environ.get("HYPERSPACE_DATA_DIR"):
        return Path(os.environ["HYPERSPACE_DATA_DIR"])
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local/share")
    return Path(base) / "hyperspace"


def size(n: int | None) -> str:
    value = float(n or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return str(n)


def table(title: str, header: list[str], rows: list[list[str]]) -> None:
    print(f"\n{title}")
    if not rows:
        print("  (none)")
        return
    all_rows = [header, *rows]
    widths = [max(len(r[i]) for r in all_rows) for i in range(len(header))]
    for r in all_rows:
        print("  " + "  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)))


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--reads", type=int, default=10)
    args = parser.parse_args()
    path = (args.data_dir or data_dir()) / "index.sqlite3"
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def one(sql: str) -> int:
        return int(db.execute(sql).fetchone()[0] or 0)

    print(f"store: {path.parent}")
    sessions = one("SELECT COUNT(*) FROM sessions")
    conflicted = one("SELECT COUNT(*) FROM sessions WHERE state = 'conflicted'")
    log_bytes = one("SELECT SUM(length) FROM segments")
    stored = one("SELECT SUM(frame_length) FROM segments")
    blob_bytes = one("SELECT SUM(size) FROM blob_versions")
    print(
        f"sessions {sessions}, conflicted {conflicted}, log bytes "
        f"{size(log_bytes)} (stored {size(stored)}), blob bytes {size(blob_bytes)}"
    )

    rows = db.execute(
        """
        SELECT host, SUM(n) FROM (
            SELECT host, SUM(length) AS n FROM segments GROUP BY host
            UNION ALL
            SELECT host, SUM(size) AS n FROM blob_versions GROUP BY host
        ) GROUP BY host ORDER BY 2 DESC
        """
    ).fetchall()
    table("bytes by host", ["HOST", "BYTES"], [[r[0], size(r[1])] for r in rows])

    rows = db.execute(
        "SELECT host, workspace, SUM(length) FROM segments "
        "GROUP BY host, workspace ORDER BY 3 DESC LIMIT ?",
        (args.top,),
    ).fetchall()
    table(
        "log bytes by workspace",
        ["HOST", "WORKSPACE", "BYTES"],
        [[r[0], r[1], size(r[2])] for r in rows],
    )

    rows = db.execute(
        "SELECT harness, COUNT(*) FROM sessions GROUP BY harness ORDER BY 2 DESC"
    ).fetchall()
    table(
        "sessions by harness",
        ["HARNESS", "SESSIONS"],
        [[r[0], str(r[1])] for r in rows],
    )

    rows = db.execute(
        """
        SELECT s.harness, s.session_id, s.state, SUM(seg.length)
        FROM sessions s JOIN files f ON f.session = s.id
        JOIN generations g ON g.file = f.id JOIN segments seg ON seg.generation = g.id
        GROUP BY s.id ORDER BY 4 DESC LIMIT ?
        """,
        (args.top,),
    ).fetchall()
    table(
        "largest sessions (log bytes)",
        ["HARNESS", "SESSION", "STATE", "BYTES"],
        [[r[0], r[1], r[2], size(r[3])] for r in rows],
    )

    rows = db.execute(
        "SELECT ts, target, reason FROM purges ORDER BY id DESC"
    ).fetchall()
    table("purges", ["TIME", "TARGET", "REASON"], [[r[0], r[1], r[2]] for r in rows])

    rows = db.execute(
        """
        SELECT a.ts, a.host, COALESCE(s.session_id, '(purged)'), a.what
        FROM read_audit a LEFT JOIN sessions s ON s.id = a.session
        ORDER BY a.id DESC LIMIT ?
        """,
        (args.reads,),
    ).fetchall()
    table(
        "recent reads",
        ["TIME", "HOST", "SESSION", "WHAT"],
        [list(map(str, r)) for r in rows],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
