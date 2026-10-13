# vim: filetype=python
"""The accounts file and authentication."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from accounts import (
    Account,
    Accounts,
    AccountsError,
    AccountsFile,
    Credential,
    dump_accounts,
    new_token,
    parse_accounts,
    parse_token,
)

NOW = datetime(2026, 10, 13, tzinfo=UTC)


def make(
    host: str = "laptop",
    scopes: frozenset[str] = frozenset({"append", "read"}),
    read: str = "own",
    expires_at: datetime | None = None,
) -> tuple[str, Credential]:
    token, cid, digest = new_token()
    return token, Credential(
        id=cid,
        secret_sha256=digest,
        host=host,
        scopes=scopes,
        read=read,
        expires_at=expires_at,
    )


def write(path: Path, *creds: Credential) -> None:
    accounts = Accounts({"default": Account(id="default", credentials=creds)})
    path.write_text(dump_accounts(accounts))


def test_token_round_trip() -> None:
    token, cid, _ = new_token()
    parsed = parse_token(token)
    assert parsed is not None and parsed[0] == cid
    assert parse_token("ssp_abcdefghijkl_" + "x" * 43) is None
    assert parse_token("hsp_short_secret") is None


def test_dump_and_parse(tmp_path: Path) -> None:
    _, c = make(read="all", expires_at=NOW + timedelta(days=1))
    write(tmp_path / "a.toml", c)
    parsed = parse_accounts((tmp_path / "a.toml").read_text())
    assert parsed.accounts["default"].credentials == (c,)


@pytest.mark.parametrize(
    "text",
    [
        "accounts = 1",
        '[accounts."Bad Id"]',
        '[[accounts.default.credentials]]\nid = "short"',
        '[[accounts.default.credentials]]\nid = "abcdefghijkl"\n'
        'secret_sha256 = "' + "0" * 64 + '"\nhost = "Laptop"',
        '[[accounts.default.credentials]]\nid = "abcdefghijkl"\n'
        'secret_sha256 = "' + "0" * 64 + '"\nhost = "laptop"\nscopes = ["admin"]',
        "not toml [",
    ],
)
def test_invalid_files(text: str) -> None:
    with pytest.raises(AccountsError):
        parse_accounts(text)


def test_authenticate(tmp_path: Path) -> None:
    token, c = make(host="agent-1", read="granted")
    write(tmp_path / "accounts.toml", c)
    file = AccountsFile(tmp_path / "accounts.toml")
    principal = file.authenticate(token, NOW)
    assert principal is not None
    assert (principal.account, principal.host, principal.read) == (
        "default",
        "agent-1",
        "granted",
    )
    wrong = token[:-1] + ("B" if token.endswith("A") else "A")
    assert file.authenticate(wrong, NOW) is None
    assert file.authenticate("garbage", NOW) is None


def test_expiry(tmp_path: Path) -> None:
    token, c = make(expires_at=NOW + timedelta(hours=1))
    write(tmp_path / "accounts.toml", c)
    file = AccountsFile(tmp_path / "accounts.toml")
    assert file.authenticate(token, NOW) is not None
    assert file.authenticate(token, NOW + timedelta(hours=2)) is None


def test_reload_on_change(tmp_path: Path) -> None:
    path = tmp_path / "accounts.toml"
    token, c = make()
    write(path, c)
    file = AccountsFile(path)
    assert file.authenticate(token, NOW) is not None
    write(path)
    os.utime(path, ns=(1, 1))
    assert file.authenticate(token, NOW) is None


def test_broken_file_disables_all_credentials(tmp_path: Path) -> None:
    path = tmp_path / "accounts.toml"
    token, c = make()
    write(path, c)
    file = AccountsFile(path)
    assert file.authenticate(token, NOW) is not None
    path.write_text(path.read_text() + "\nbroken [")
    assert file.authenticate(token, NOW) is None
    assert file.error
