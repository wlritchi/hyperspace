# vim: filetype=python
"""Accounts and credentials: the accounts file, tokens, and authentication.

Standard library only, so that the admin scripts can import it with no
dependencies.

The accounts file is TOML:

    [accounts.default]
    label = "personal"

    [[accounts.default.credentials]]
    id = "abcdefghijkl"            # 12 base32 characters
    secret_sha256 = "..."          # sha256 of the secret, hex
    host = "laptop"                # the one host that this credential acts as
    scopes = ["append", "read", "grant"]   # or only ["wake"]
    read = "all"                   # own | granted | all
    expires_at = 2027-01-01T00:00:00Z   # optional
    label = "laptop, made 2026-10-13"   # optional

A token is `hsp_<credential id>_<secret>`. The server stores only the sha256
of the secret.

The server reads the main accounts file and the `*.toml` files in an accounts
directory, and merges them (see `AccountSources`). The admin scripts write
only the main file. Another system, for example a provisioner, can supply the
files in the directory.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import secrets
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger("hyperspace.accounts")

SCOPES = ("append", "read", "grant", "wake")
READ_SCOPES = ("own", "granted", "all")

_ACCOUNT_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
_CREDENTIAL_ID_RE = re.compile(r"[a-z2-7]{12}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_HOST_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
# hsp_<12 base32 chars>_<base64url secret>. The secret can contain "_", so the
# fixed-length id separates the parts.
_TOKEN_RE = re.compile(r"hsp_([a-z2-7]{12})_([A-Za-z0-9_-]{40,})")
_BASE32 = "abcdefghijklmnopqrstuvwxyz234567"


class AccountsError(ValueError):
    """The accounts file is not valid."""


@dataclass(frozen=True)
class Credential:
    id: str
    secret_sha256: str
    host: str
    scopes: frozenset[str]
    read: str = "own"
    expires_at: datetime | None = None
    label: str = ""

    def expired(self, now: datetime) -> bool:
        return self.expires_at is not None and now >= self.expires_at


@dataclass(frozen=True)
class Account:
    id: str
    label: str = ""
    credentials: tuple[Credential, ...] = ()


@dataclass(frozen=True)
class Principal:
    """The result of an authentication: who sends the request."""

    account: str
    host: str
    scopes: frozenset[str]
    read: str
    credential: str


@dataclass
class Accounts:
    accounts: dict[str, Account] = field(default_factory=dict[str, Account])

    def by_credential(self) -> dict[str, tuple[Account, Credential]]:
        out: dict[str, tuple[Account, Credential]] = {}
        for account in self.accounts.values():
            for cred in account.credentials:
                out[cred.id] = (account, cred)
        return out


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def new_token() -> tuple[str, str, str]:
    """Make a new credential: (token, credential id, sha256 of the secret)."""
    credential_id = "".join(secrets.choice(_BASE32) for _ in range(12))
    secret = secrets.token_urlsafe(32)
    return f"hsp_{credential_id}_{secret}", credential_id, hash_secret(secret)


def parse_token(token: str) -> tuple[str, str] | None:
    """Split a token into (credential id, secret), or return None."""
    m = _TOKEN_RE.fullmatch(token)
    return (m.group(1), m.group(2)) if m else None


def _str(table: dict[str, object], key: str, where: str, default: str = "") -> str:
    value = table.get(key, default)
    if not isinstance(value, str):
        raise AccountsError(f"{where}: {key} must be a string")
    return value


def _credential(raw: object, where: str) -> Credential:
    if not isinstance(raw, dict):
        raise AccountsError(f"{where}: a credential must be a table")
    table: dict[str, object] = raw
    cid = _str(table, "id", where)
    if not _CREDENTIAL_ID_RE.fullmatch(cid):
        raise AccountsError(f"{where}: invalid credential id {cid!r}")
    digest = _str(table, "secret_sha256", where)
    if not _SHA256_RE.fullmatch(digest):
        raise AccountsError(f"{where}: secret_sha256 must be 64 hex characters")
    host = _str(table, "host", where)
    if not _HOST_RE.fullmatch(host):
        raise AccountsError(f"{where}: invalid host {host!r}")
    scopes_raw = table.get("scopes", [])
    if not isinstance(scopes_raw, list) or not all(
        isinstance(s, str) and s in SCOPES for s in scopes_raw
    ):
        raise AccountsError(f"{where}: scopes must be a subset of {list(SCOPES)}")
    scopes = frozenset(str(s) for s in scopes_raw)
    # A wake credential goes to a message service. It must not also be able
    # to read transcripts or act as a host.
    if "wake" in scopes and len(scopes) > 1:
        raise AccountsError(f"{where}: the wake scope must be the only scope")
    read = _str(table, "read", where, "own")
    if read not in READ_SCOPES:
        raise AccountsError(f"{where}: read must be one of {list(READ_SCOPES)}")
    expires = table.get("expires_at")
    if expires is not None:
        if not isinstance(expires, datetime):
            raise AccountsError(f"{where}: expires_at must be a TOML date-time")
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=UTC)
    return Credential(
        id=cid,
        secret_sha256=digest,
        host=host,
        scopes=scopes,
        read=read,
        expires_at=expires,
        label=_str(table, "label", where),
    )


def parse_accounts(text: str) -> Accounts:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise AccountsError(f"invalid TOML: {exc}") from None
    accounts_raw = data.get("accounts", {})
    if not isinstance(accounts_raw, dict):
        raise AccountsError("accounts must be a table")
    result = Accounts()
    seen: set[str] = set()
    for account_id, raw in accounts_raw.items():
        if not isinstance(account_id, str) or not _ACCOUNT_RE.fullmatch(account_id):
            raise AccountsError(f"invalid account id {account_id!r}")
        if not isinstance(raw, dict):
            raise AccountsError(f"accounts.{account_id} must be a table")
        table: dict[str, object] = raw
        creds_raw = table.get("credentials", [])
        if not isinstance(creds_raw, list):
            raise AccountsError(f"accounts.{account_id}.credentials must be a list")
        creds: list[Credential] = []
        for i, c in enumerate(creds_raw):
            cred = _credential(c, f"accounts.{account_id}.credentials[{i}]")
            if cred.id in seen:
                raise AccountsError(f"duplicate credential id {cred.id}")
            seen.add(cred.id)
            creds.append(cred)
        result.accounts[account_id] = Account(
            id=account_id,
            label=_str(table, "label", f"accounts.{account_id}"),
            credentials=tuple(creds),
        )
    return result


def _toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def dump_accounts(accounts: Accounts) -> str:
    """Write `accounts` as TOML. Comments in the old file are not kept."""
    lines: list[str] = []
    for account in accounts.accounts.values():
        lines.append(f"[accounts.{account.id}]")
        if account.label:
            lines.append(f"label = {_toml_string(account.label)}")
        lines.append("")
        for c in account.credentials:
            lines.append(f"[[accounts.{account.id}.credentials]]")
            lines.append(f"id = {_toml_string(c.id)}")
            lines.append(f"secret_sha256 = {_toml_string(c.secret_sha256)}")
            lines.append(f"host = {_toml_string(c.host)}")
            scopes = ", ".join(_toml_string(s) for s in SCOPES if s in c.scopes)
            lines.append(f"scopes = [{scopes}]")
            lines.append(f"read = {_toml_string(c.read)}")
            if c.expires_at is not None:
                stamp = c.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                lines.append(f"expires_at = {stamp}")
            if c.label:
                lines.append(f"label = {_toml_string(c.label)}")
            lines.append("")
    return "\n".join(lines)


# Credential id -> (account id, credential).
Index = dict[str, tuple[str, Credential]]


def _check(index: Index, token: str, now: datetime) -> Principal | None:
    parsed = parse_token(token)
    if parsed is None:
        return None
    credential_id, secret = parsed
    found = index.get(credential_id)
    # Hash even when the id is unknown, so that the time does not show
    # whether the id exists.
    digest = hash_secret(secret)
    if found is None:
        hmac.compare_digest(digest, "0" * 64)
        return None
    account_id, cred = found
    if not hmac.compare_digest(digest, cred.secret_sha256):
        return None
    if cred.expired(now):
        return None
    return Principal(
        account=account_id,
        host=cred.host,
        scopes=cred.scopes,
        read=cred.read,
        credential=cred.id,
    )


def _file_stamp(stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)


class AccountsFile:
    """One accounts file, reloaded when it changes on disk."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stamp: tuple[int, int, int, int, int] | None = None
        self.index: Index = {}
        self.error: str | None = None
        self.reload()

    def reload(self) -> bool:
        """Read the file if it changed, and return True if it did. On an
        error, keep no credentials: a broken file must not leave revoked
        credentials in use. A file that does not exist has no credentials
        and is not an error."""
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            changed = self._stamp is not None or self.error is not None
            self._stamp = None
            self.index = {}
            self.error = None
            return changed
        except OSError as exc:
            error = f"{self.path}: {exc}"
            changed = self._stamp is not None or self.error != error
            self._stamp = None
            self.index = {}
            self.error = error
            return changed
        # An atomic replace (by the admin scripts, or in a Kubernetes Secret
        # volume) can keep the size and the modification time. The stamp
        # therefore also has the device, the inode and the change time.
        stamp = _file_stamp(stat)
        if stamp == self._stamp:
            return False
        self._stamp = stamp
        try:
            accounts = parse_accounts(self.path.read_text())
            self.error = None
        except (AccountsError, OSError, UnicodeDecodeError) as exc:
            accounts = Accounts()
            self.error = f"{self.path}: {exc}"
        self.index = {
            cid: (account.id, cred)
            for cid, (account, cred) in accounts.by_credential().items()
        }
        return True

    def unchanged(self) -> bool:
        """True for the same stamp, or still missing or unreadable with the same error."""
        try:
            return _file_stamp(self.path.stat()) == self._stamp
        except FileNotFoundError:
            return self._stamp is None and self.error is None
        except OSError as exc:
            return self._stamp is None and self.error == f"{self.path}: {exc}"

    def authenticate(self, token: str, now: datetime) -> Principal | None:
        self.reload()
        return _check(self.index, token, now)


def account_files(directory: Path) -> list[Path]:
    """The accounts files in an accounts directory, sorted by name: the
    regular files whose names end in `.toml`. Names that start with a dot are
    not included; a Kubernetes Secret volume keeps its internal data there.

    Raise `OSError` if the directory cannot be listed."""
    return sorted(
        p
        for p in directory.iterdir()
        if p.name.endswith(".toml") and not p.name.startswith(".") and p.is_file()
    )


class AccountSources:
    """The main accounts file and the files in the accounts directory, as one
    set of credentials. Each check reloads the files that changed and lists
    the directory again, so a change takes effect at once.

    - An account that is in more than one file has the credentials of all of
      them. One account can thus have credentials from the admin scripts and
      from a provisioner.
    - A file that is not valid gives no credentials, but the other files still
      work. A directory that cannot be listed gives no credentials.
    - A credential id that is in more than one file works in none of them.
    """

    def __init__(self, main: Path, directory: Path | None = None) -> None:
        self.main = AccountsFile(main)
        self.directory = directory
        self._files: dict[Path, AccountsFile] = {}
        self._directory_error: str | None = None
        self._index: Index = {}
        self.errors: tuple[str, ...] = ()
        self._snapshot_error: str | None = None
        self.reload()
        self._rebuild()

    @property
    def error(self) -> str | None:
        return "; ".join(self.errors) or None

    def reload(self) -> None:
        changed = self._snapshot_error is not None
        for _ in range(3):
            changed = self.main.reload() or changed
            changed = self._reload_directory() or changed
            # A volume update can remove a duplicate between file reads.
            # Do not use the merged credentials until a second check finds
            # the same directory listing and the same file stamps.
            paths, error = self._directory_listing()
            if (
                paths == list(self._files)
                and error == self._directory_error
                and all(f.unchanged() for f in [self.main, *self._files.values()])
            ):
                if self._snapshot_error:
                    log.info(
                        "accounts: the accounts files are stable again; credentials work"
                    )
                self._snapshot_error = None
                if changed:
                    self._rebuild()
                return
        self._snapshot_error = (
            "accounts files changed during reload; no credentials work"
        )
        self._rebuild()

    def _directory_listing(self) -> tuple[list[Path], str | None]:
        if self.directory is None:
            return [], None
        error: str | None = None
        try:
            paths = account_files(self.directory)
        except FileNotFoundError:
            paths = []
        except OSError as exc:
            paths = []
            error = f"{self.directory}: {exc}"
        return paths, error

    def _reload_directory(self) -> bool:
        paths, error = self._directory_listing()
        changed = error != self._directory_error or paths != list(self._files)
        self._directory_error = error
        files: dict[Path, AccountsFile] = {}
        for path in paths:
            old = self._files.get(path)
            if old is None:
                files[path] = AccountsFile(path)
            else:
                files[path] = old
                if old.reload():
                    changed = True
        self._files = files
        return changed

    def _rebuild(self) -> None:
        sources = [self.main, *self._files.values()]
        errors = [f.error for f in sources if f.error]
        if self._directory_error:
            errors.append(self._directory_error)
        index: Index = {}
        places: dict[str, list[Path]] = {}
        for source in sources:
            for cid, entry in source.index.items():
                index[cid] = entry
                places.setdefault(cid, []).append(source.path)
        for cid, paths in places.items():
            if len(paths) > 1:
                del index[cid]
                errors.append(
                    f"credential {cid} is in more than one file "
                    f"({', '.join(str(p) for p in paths)}); it works in none of them"
                )
        if self._snapshot_error:
            index = {}
            # An unverified attempt can contain duplicates that never existed.
            # Keep only the errors from the last verified sources.
            errors = [
                *(e for e in self.errors if e != self._snapshot_error),
                self._snapshot_error,
            ]
        self._index = index
        for message in errors:
            if message not in self.errors:
                log.error("accounts: %s", message)
        if self.errors and not errors:
            log.info("accounts: all accounts files are valid again")
        self.errors = tuple(errors)

    def authenticate(self, token: str, now: datetime) -> Principal | None:
        self.reload()
        return _check(self._index, token, now)
