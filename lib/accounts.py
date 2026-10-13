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
    scopes = ["append", "read", "grant"]
    read = "all"                   # own | granted | all
    expires_at = 2027-01-01T00:00:00Z   # optional
    label = "laptop, made 2026-10-13"   # optional

A token is `hsp_<credential id>_<secret>`. The server stores only the sha256
of the secret.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

SCOPES = ("append", "read", "grant")
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


class AccountsFile:
    """The accounts file, reloaded when it changes on disk."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stamp: tuple[int, int] | None = None
        self._accounts = Accounts()
        self._index: dict[str, tuple[Account, Credential]] = {}
        self.error: str | None = None
        self.reload()

    def reload(self) -> None:
        """Read the file if it changed. On an error, keep no credentials:
        a broken file must not leave revoked credentials in use."""
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._stamp = None
            self._accounts = Accounts()
            self._index = {}
            self.error = None
            return
        stamp = (stat.st_mtime_ns, stat.st_size)
        if stamp == self._stamp:
            return
        self._stamp = stamp
        try:
            self._accounts = parse_accounts(self.path.read_text())
            self.error = None
        except (AccountsError, OSError, UnicodeDecodeError) as exc:
            self._accounts = Accounts()
            self.error = str(exc)
        self._index = self._accounts.by_credential()

    def authenticate(self, token: str, now: datetime) -> Principal | None:
        self.reload()
        parsed = parse_token(token)
        if parsed is None:
            return None
        credential_id, secret = parsed
        found = self._index.get(credential_id)
        # Hash even when the id is unknown, so that the time does not show
        # whether the id exists.
        digest = hash_secret(secret)
        if found is None:
            hmac.compare_digest(digest, "0" * 64)
            return None
        account, cred = found
        if not hmac.compare_digest(digest, cred.secret_sha256):
            return None
        if cred.expired(now):
            return None
        return Principal(
            account=account.id,
            host=cred.host,
            scopes=cred.scopes,
            read=cred.read,
            credential=cred.id,
        )
