#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Check a conflicted session again, and clear a false conflict.

    scripts/recheck-conflict.py claude-code <session id> [--write]

A conflicted session stays conflicted. The conflict is false if, in each log
with a conflict, the bytes of each other branch tip are a prefix of the bytes
of the active tip: then no branch holds work that the active branch does not
have. For such a session, --write records the contained tips and sets the
session back to normal. Without --write, the script shows the result and
changes nothing. It is safe while the server runs.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "zstandard>=0.23",
#     "hyperspace-protocol",
# ]
#
# [tool.uv.sources]
# hyperspace-protocol = { path = "../protocol" }
# ///

import argparse
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from store import SessionKey, Store, StoreError


def data_dir() -> Path:
    if os.environ.get("HYPERSPACE_DATA_DIR"):
        return Path(os.environ["HYPERSPACE_DATA_DIR"])
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local/share")
    return Path(base) / "hyperspace"


def describe(state: dict[str, Any]) -> str:
    if state["conflicted_at"] is None:
        return state["state"]
    return f"{state['state']} since {state['conflicted_at']}"


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("harness")
    parser.add_argument("session_id")
    parser.add_argument("--account", default="default")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--write", action="store_true", help="write the new state")
    args = parser.parse_args()

    store = Store(args.data_dir or data_dir())
    key = SessionKey(args.account, args.harness, args.session_id)
    try:
        result = store.recheck_conflict(key, args.write)
    except StoreError as exc:
        print(f"error: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1
    finally:
        store.close()
    print(f"before: {describe(result['before'])}")
    for log in result["logs"]:
        print(f"{log['path']}: active tip {log['active']}")
        for gen in log["contained"]:
            print(f"  {gen}: contained in the active tip")
        for gen in log["not_contained"]:
            print(f"  {gen}: NOT contained in the active tip")
    if result["written"]:
        print(f"after: {describe(result['after'])} (written)")
    elif result["after"] != result["before"]:
        print(
            f"after: {describe(result['after'])}. Run again with --write to write it."
        )
    else:
        print("after: no change")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
