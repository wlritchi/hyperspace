#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Remove a credential from the accounts file.

    scripts/revoke-credential.py abcdefghijkl

The server reloads the accounts file when it changes, so the credential stops
working at once. A credential in the accounts directory cannot be revoked
here: remove it where that file comes from.
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
    data = args.data_dir or _new.data_dir()
    path = data / "accounts.toml"
    unchecked: list[tuple[Path, str]] = []
    try:
        with _new.locked_accounts(path):
            accounts = _new.load_accounts(path)
            found = False
            for account_id, account in list(accounts.accounts.items()):
                kept = tuple(
                    c for c in account.credentials if c.id != args.credential_id
                )
                if len(kept) != len(account.credentials):
                    found = True
                    accounts.accounts[account_id] = replace(account, credentials=kept)
            # Check the directory also when accounts.toml has the id. An id in
            # two files works in neither, so to remove only the copy in
            # accounts.toml would make the copy in the directory work.
            in_directory, unchecked = _new.scan_directory(_new.accounts_dir(data))
            # A file that is not valid gives no credentials now, but it can
            # have this credential and become valid again. Do not refuse: an
            # emergency revocation must not wait for an unrelated broken file.
            for unchecked_path, reason in unchecked:
                print(
                    f"warning: could not check {unchecked_path} for credential "
                    f"{args.credential_id}: {reason}",
                    file=sys.stderr,
                )
            source = in_directory.get(args.credential_id)
            if source is not None:
                print(
                    f"error: credential {args.credential_id} is in {source}, "
                    "which the server only reads. Remove it where that file "
                    "comes from.",
                    file=sys.stderr,
                )
                if found:
                    print(
                        f"note: {path} also has it, and this script did not "
                        "change that file. Run this script again after the "
                        "credential is gone from the directory.",
                        file=sys.stderr,
                    )
                return 1
            if not found:
                where = " in the files that could be checked" if unchecked else ""
                print(
                    f"error: no credential {args.credential_id}{where}",
                    file=sys.stderr,
                )
                return 1
            _new.write_accounts(path, accounts)
    except AccountsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if unchecked:
        print(
            f"warning: this script changed only {path}. Make sure that the "
            "source of the accounts directory does not have credential "
            f"{args.credential_id}.",
            file=sys.stderr,
        )
    print(f"revoked {args.credential_id}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
