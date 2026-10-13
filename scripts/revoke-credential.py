#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Remove a credential from the accounts file.

    scripts/revoke-credential.py abcdefghijkl

The server reloads the accounts file when it changes, so the credential stops
working at once.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse
import importlib.util
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from accounts import AccountsError

# The writer lives in new-credential.py; its name has a hyphen.
_spec = importlib.util.spec_from_file_location(
    "new_credential", Path(__file__).resolve().parent / "new-credential.py"
)
assert _spec is not None and _spec.loader is not None
_new = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_new)


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("credential_id")
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args()
    path = (args.data_dir or _new.data_dir()) / "accounts.toml"
    try:
        accounts = _new.load_accounts(path)
        found = False
        for account_id, account in list(accounts.accounts.items()):
            kept = tuple(c for c in account.credentials if c.id != args.credential_id)
            if len(kept) != len(account.credentials):
                found = True
                accounts.accounts[account_id] = replace(account, credentials=kept)
        if not found:
            print(f"error: no credential {args.credential_id}", file=sys.stderr)
            return 1
        _new.write_accounts(path, accounts)
    except AccountsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"revoked {args.credential_id}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
