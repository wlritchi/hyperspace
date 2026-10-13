#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Remove data from the store, in an emergency (for example, a secret in a
transcript). This is the only way to delete data; the API cannot.

    scripts/purge.py session claude-code <session id> --reason "..."
    scripts/purge.py generation claude-code <session id> <generation> --reason "..."
    scripts/purge.py blob claude-code <session id> <file path> --version N --reason "..."

Without --yes, the script shows what it would remove and changes nothing.
It is safe while the server runs. Each purge is recorded in the index. A
host that still has the session uploads it again at its next sync, so remove
the local copies too.
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
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from store import SessionKey, Store, StoreError


def data_dir() -> Path:
    if os.environ.get("HYPERSPACE_DATA_DIR"):
        return Path(os.environ["HYPERSPACE_DATA_DIR"])
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local/share")
    return Path(base) / "hyperspace"


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--account", default="default")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--reason", required=True, help="recorded with the purge")
    parser.add_argument("--yes", action="store_true", help="really remove the data")
    sub = parser.add_subparsers(dest="kind", required=True)
    for kind in ("session", "generation", "blob"):
        p = sub.add_parser(kind)
        p.add_argument("harness")
        p.add_argument("session_id")
        if kind == "generation":
            p.add_argument("generation")
        if kind == "blob":
            p.add_argument("path")
            p.add_argument("--version", type=int, required=True)
    args = parser.parse_args()

    store = Store(args.data_dir or data_dir())
    key = SessionKey(args.account, args.harness, args.session_id)
    try:
        manifest = store.manifest(key)
        if args.kind == "session":
            what = f"session {args.session_id}: {len(manifest['files'])} files"
        elif args.kind == "generation":
            what = f"generation {args.generation} of {args.session_id} and its forks"
        else:
            what = f"{args.path} version {args.version} of {args.session_id}"
        if not args.yes:
            print(f"would remove {what}. Run again with --yes to remove it.")
            return 1
        if args.kind == "session":
            result = store.purge_session(key, args.reason)
        elif args.kind == "generation":
            result = store.purge_generation(key, args.generation, args.reason)
        else:
            result = store.purge_blob_version(key, args.path, args.version, args.reason)
    except StoreError as exc:
        print(f"error: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1
    finally:
        store.close()
    print(f"removed {what}: {json.dumps(result)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
