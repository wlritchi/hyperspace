#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Create a credential for one host and print its token once.

    scripts/new-credential.py --host laptop --scopes append,read,grant --read all
    scripts/new-credential.py --host agent-1 --scopes append,read,grant --read granted
    scripts/new-credential.py --host subspace --scopes wake

A `wake` credential can only send wakes, so it has no other scope. Its host
is a name for logs.

Run it where the server's data directory is (inside the server container, for
example with `kubectl exec`). The server reloads the accounts file when it
changes. Only the sha256 of the secret is stored.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse
import fcntl
import os
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

from accounts import (
    READ_SCOPES,
    SCOPES,
    Account,
    Accounts,
    AccountsError,
    Credential,
    account_files,
    dump_accounts,
    new_token,
    parse_accounts,
)


def data_dir() -> Path:
    if os.environ.get("HYPERSPACE_DATA_DIR"):
        return Path(os.environ["HYPERSPACE_DATA_DIR"])
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local/share")
    return Path(base) / "hyperspace"


def accounts_dir(data: Path) -> Path:
    """The directory of read-only accounts files, with the server's default."""
    if os.environ.get("HYPERSPACE_ACCOUNTS_DIR"):
        return Path(os.environ["HYPERSPACE_ACCOUNTS_DIR"])
    return data / "accounts.d"


def scan_directory(
    directory: Path,
) -> tuple[dict[str, Path], list[tuple[Path, str]]]:
    """Credential id -> file, for the valid files in the accounts directory,
    and (path, reason) for each file or directory that could not be checked.
    A directory that does not exist is not an error. These scripts never
    write those files."""
    out: dict[str, Path] = {}
    unchecked: list[tuple[Path, str]] = []
    try:
        paths = account_files(directory)
    except FileNotFoundError:
        return out, unchecked
    except OSError as exc:
        unchecked.append((directory, str(exc)))
        return out, unchecked
    for path in paths:
        try:
            accounts = parse_accounts(path.read_text())
        except (AccountsError, OSError, UnicodeDecodeError) as exc:
            unchecked.append((path, str(exc)))
            continue
        for cid in accounts.by_credential():
            out[cid] = path
    return out, unchecked


def directory_credentials(directory: Path) -> dict[str, Path]:
    """Credential id -> file, for the valid files in the accounts directory.
    Files that could not be checked are not included: a random new id is
    unlikely to match an id in them."""
    return scan_directory(directory)[0]


@contextmanager
def locked_accounts(path: Path) -> Iterator[None]:
    """Lock a stable sidecar: the accounts file itself is replaced on write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def write_accounts(path: Path, accounts: Accounts) -> None:
    text = dump_accounts(accounts)
    parse_accounts(text)  # Check that the server can read what we write.
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".accounts-")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def load_accounts(path: Path) -> Accounts:
    if not path.exists():
        return Accounts()
    return parse_accounts(path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--account", default="default")
    parser.add_argument("--host", required=True, help="the host name it acts as")
    parser.add_argument(
        "--scopes", default="append,read,grant", help=f"from {','.join(SCOPES)}"
    )
    parser.add_argument("--read", default="own", choices=READ_SCOPES)
    parser.add_argument("--expires", help="expiry, ISO 8601 (for example 2027-01-01)")
    parser.add_argument("--label", default="")
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args()

    data = args.data_dir or data_dir()
    path = data / "accounts.toml"
    scopes = frozenset(s.strip() for s in args.scopes.split(",") if s.strip())
    expires = None
    if args.expires:
        expires = datetime.fromisoformat(args.expires)
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
    try:
        with locked_accounts(path):
            accounts = load_accounts(path)
            # A credential id in two files works in neither.
            taken = (
                accounts.by_credential().keys()
                | directory_credentials(accounts_dir(data)).keys()
            )
            token, credential_id, digest = new_token()
            while credential_id in taken:
                token, credential_id, digest = new_token()
            credential = Credential(
                id=credential_id,
                secret_sha256=digest,
                host=args.host,
                scopes=scopes,
                read=args.read,
                expires_at=expires,
                label=args.label,
            )
            account = accounts.accounts.get(args.account, Account(id=args.account))
            accounts.accounts[args.account] = replace(
                account, credentials=(*account.credentials, credential)
            )
            write_accounts(path, accounts)
    except AccountsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"credential {credential_id} for host {args.host} in {path}", file=sys.stderr)
    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
