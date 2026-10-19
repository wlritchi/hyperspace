# vim: filetype=python
"""The accounts file and authentication."""

import importlib.util
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from accounts import (
    Account,
    Accounts,
    AccountsError,
    AccountsFile,
    AccountSources,
    Credential,
    dump_accounts,
    new_token,
    parse_accounts,
    parse_token,
)
from app import Settings

NOW = datetime(2026, 10, 13, tzinfo=UTC)
ROOT = Path(__file__).resolve().parent.parent


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


def write_account(path: Path, account: str, *creds: Credential) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    accounts = Accounts({account: Account(id=account, credentials=creds)})
    path.write_text(dump_accounts(accounts))


def test_sources_merge_an_account_across_files(tmp_path: Path) -> None:
    main_token, main_cred = make(host="laptop", read="all")
    dir_token, dir_cred = make(host="agent-1", read="granted")
    write_account(tmp_path / "accounts.toml", "default", main_cred)
    write_account(tmp_path / "accounts.d" / "runners.toml", "default", dir_cred)
    sources = AccountSources(tmp_path / "accounts.toml", tmp_path / "accounts.d")
    laptop = sources.authenticate(main_token, NOW)
    agent = sources.authenticate(dir_token, NOW)
    assert laptop is not None and agent is not None
    assert (laptop.account, laptop.host) == ("default", "laptop")
    assert (agent.account, agent.host, agent.read) == ("default", "agent-1", "granted")
    assert sources.errors == ()


def test_sources_without_a_directory(tmp_path: Path) -> None:
    token, c = make()
    write(tmp_path / "accounts.toml", c)
    for directory in (None, tmp_path / "missing"):
        sources = AccountSources(tmp_path / "accounts.toml", directory)
        assert sources.authenticate(token, NOW) is not None
        assert sources.error is None


def test_sources_broken_file_disables_only_its_credentials(tmp_path: Path) -> None:
    main_token, main_cred = make(host="laptop")
    dir_token, dir_cred = make(host="agent-1")
    other_token, other_cred = make(host="agent-2")
    directory = tmp_path / "accounts.d"
    write_account(tmp_path / "accounts.toml", "default", main_cred)
    write_account(directory / "a.toml", "default", dir_cred)
    write_account(directory / "b.toml", "default", other_cred)
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    (directory / "a.toml").write_text("broken [")
    assert sources.authenticate(dir_token, NOW) is None
    assert sources.authenticate(main_token, NOW) is not None
    assert sources.authenticate(other_token, NOW) is not None
    assert len(sources.errors) == 1 and "a.toml" in sources.errors[0]
    # A broken main file also disables only its own credentials.
    (tmp_path / "accounts.toml").write_text("broken [")
    write_account(directory / "a.toml", "default", dir_cred)
    assert sources.authenticate(main_token, NOW) is None
    assert sources.authenticate(dir_token, NOW) is not None
    assert len(sources.errors) == 1 and "accounts.toml" in sources.errors[0]


def test_sources_duplicate_credential_id_works_nowhere(tmp_path: Path) -> None:
    token, c = make(host="agent-1")
    other_token, other = make(host="agent-2")
    directory = tmp_path / "accounts.d"
    write_account(tmp_path / "accounts.toml", "default", c)
    write_account(directory / "a.toml", "default", c, other)
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(token, NOW) is None
    assert sources.authenticate(other_token, NOW) is not None
    assert any(c.id in e for e in sources.errors)
    # The same id in two accounts is also a duplicate.
    write_account(directory / "a.toml", "other", c)
    assert sources.authenticate(token, NOW) is None
    (directory / "a.toml").unlink()
    assert sources.authenticate(token, NOW) is not None
    assert sources.errors == ()


def test_sources_list_the_directory_at_each_check(tmp_path: Path) -> None:
    token, c = make(host="agent-1")
    directory = tmp_path / "accounts.d"
    directory.mkdir()
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(token, NOW) is None
    write_account(directory / "runners.toml", "default", c)
    assert sources.authenticate(token, NOW) is not None
    (directory / "runners.toml").unlink()
    assert sources.authenticate(token, NOW) is None


def test_sources_skip_other_names_and_dangling_links(tmp_path: Path) -> None:
    token, c = make()
    directory = tmp_path / "accounts.d"
    write_account(directory / ".hidden.toml", "default", c)
    write_account(directory / "notes.txt", "default", c)
    (directory / "sub.toml").mkdir()
    (directory / "gone.toml").symlink_to(directory / "nothing")
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(token, NOW) is None
    assert sources.errors == ()


def test_sources_follow_a_secret_volume_update(tmp_path: Path) -> None:
    """A Kubernetes Secret volume: `key -> ..data/key`, and an update swaps
    the `..data` link to a new directory. The new file can have the same
    size and time as the old one."""
    old_token, old = make(host="agent-1")
    new_token_, new = make(host="agent-1")
    directory = tmp_path / "accounts.d"
    write_account(directory / "..v1" / "runners.toml", "default", old)
    write_account(directory / "..v2" / "runners.toml", "default", new)
    stat = (directory / "..v1" / "runners.toml").stat()
    os.utime(
        directory / "..v2" / "runners.toml", ns=(stat.st_atime_ns, stat.st_mtime_ns)
    )
    (directory / "..data").symlink_to("..v1")
    (directory / "runners.toml").symlink_to("..data/runners.toml")
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(old_token, NOW) is not None
    (directory / "..data_tmp").symlink_to("..v2")
    os.replace(directory / "..data_tmp", directory / "..data")
    assert sources.authenticate(old_token, NOW) is None
    assert sources.authenticate(new_token_, NOW) is not None


def test_sources_reject_duplicate_during_volume_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token, cred = make()
    directory = tmp_path / "accounts.d"
    for name in ("a.toml", "b.toml"):
        write_account(directory / "..v1" / name, "default", cred)
        write_account(directory / "..v2" / name, "default")
        (directory / name).symlink_to(f"..data/{name}")
    (directory / "..data").symlink_to("..v1")
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(token, NOW) is None
    original = AccountsFile.reload
    swapped = False

    def reload(file: AccountsFile) -> bool:
        nonlocal swapped
        changed = original(file)
        if file.path == directory / "a.toml" and not swapped:
            (directory / "..next").symlink_to("..v2")
            os.replace(directory / "..next", directory / "..data")
            swapped = True
        return changed

    monkeypatch.setattr(AccountsFile, "reload", reload)
    assert sources.authenticate(token, NOW) is None
    assert swapped
    assert sources.authenticate(token, NOW) is None
    assert sources.errors == ()


def test_sources_exhaustion_does_not_log_unverified_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    token, cred = make()
    directory = tmp_path / "accounts.d"
    for generation in range(4):
        for position, name in enumerate(("a.toml", "b.toml")):
            creds = (cred,) if generation % 2 == position else ()
            write_account(directory / f"..v{generation}" / name, "default", *creds)
    for name in ("a.toml", "b.toml"):
        (directory / name).symlink_to(f"..data/{name}")
    (directory / "..data").symlink_to("..v0")
    sources = AccountSources(tmp_path / "accounts.toml", directory)
    assert sources.authenticate(token, NOW) is not None
    original = AccountsFile.reload
    swaps = 0

    def reload(file: AccountsFile) -> bool:
        nonlocal swaps
        changed = original(file)
        if file.path == directory / "a.toml" and swaps < 3:
            swaps += 1
            (directory / "..next").symlink_to(f"..v{swaps}")
            os.replace(directory / "..next", directory / "..data")
        return changed

    monkeypatch.setattr(AccountsFile, "reload", reload)
    assert sources.authenticate(token, NOW) is None
    assert swaps == 3
    assert len(sources.errors) == 1
    assert "changed during reload" in sources.errors[0]
    assert sources.authenticate(token, NOW) is not None
    assert sources.errors == ()
    assert not any("more than one file" in r.message for r in caplog.records)
    assert len([r for r in caplog.records if r.levelname == "ERROR"]) == 1


@pytest.mark.parametrize("broken_file", [False, True])
def test_sources_churn_fails_closed_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    broken_file: bool,
) -> None:
    token, cred = make()
    path = tmp_path / "accounts.toml"
    write(path, cred)
    directory = tmp_path / "accounts.d"
    directory.mkdir()
    if broken_file:
        (directory / "broken.toml").write_text("broken [")
    caplog.set_level("INFO", logger="hyperspace.accounts")
    sources = AccountSources(path, directory)
    original = AccountsFile.reload
    calls = 0

    def reload(file: AccountsFile) -> bool:
        nonlocal calls
        changed = original(file)
        if file.path == path:
            calls += 1
            os.utime(path, ns=(calls, calls))
        return changed

    with monkeypatch.context() as patch:
        patch.setattr(AccountsFile, "reload", reload)
        for _ in range(2):
            assert sources.authenticate(token, NOW) is None
        assert calls == 6
        assert "changed during reload" in (sources.error or "")
        assert len([r for r in caplog.records if r.levelname == "ERROR"]) == (
            1 + broken_file
        )
    assert sources.authenticate(token, NOW) is not None
    assert len(sources.errors) == int(broken_file)
    assert len([r for r in caplog.records if r.levelname == "ERROR"]) == 1 + broken_file
    assert sum("stable again" in r.message for r in caplog.records) == 1
    assert sources.authenticate(token, NOW) is not None
    assert sum("stable again" in r.message for r in caplog.records) == 1


def test_sources_verify_main_and_directory_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token, cred = make()
    path = tmp_path / "accounts.toml"
    directory = tmp_path / "accounts.d"
    write(path, cred)
    directory.mkdir()
    sources = AccountSources(path, directory)
    original = sources._reload_directory
    changed = False

    def reload() -> bool:
        nonlocal changed
        result = original()
        if not changed:
            write(path)
            write_account(directory / "a.toml", "default", cred)
            write_account(directory / "b.toml", "default", cred)
            changed = True
        return result

    monkeypatch.setattr(sources, "_reload_directory", reload)
    assert sources.authenticate(token, NOW) is None


def test_sources_unreadable_directory_is_an_error(tmp_path: Path) -> None:
    token, c = make()
    directory = tmp_path / "accounts.d"
    write_account(directory / "a.toml", "default", c)
    directory.chmod(0)
    try:
        if os.access(directory, os.R_OK):
            pytest.skip("running as a user that ignores permissions")
        sources = AccountSources(tmp_path / "accounts.toml", directory)
        assert sources.authenticate(token, NOW) is None
        assert sources.errors and str(directory) in sources.errors[0]
    finally:
        directory.chmod(0o700)


def test_settings_accounts_dir(tmp_path: Path) -> None:
    settings = Settings.from_env({"HYPERSPACE_DATA_DIR": str(tmp_path)})
    assert settings.accounts_dir == tmp_path / "accounts.d"
    settings = Settings.from_env(
        {"HYPERSPACE_DATA_DIR": str(tmp_path), "HYPERSPACE_ACCOUNTS_DIR": "/etc/x"}
    )
    assert settings.accounts_dir == Path("/etc/x")


def _script(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        name.replace("-", "_"), ROOT / "scripts" / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_revoke_refuses_a_directory_credential(tmp_path: Path) -> None:
    _, c = make(host="agent-1")
    write_account(tmp_path / "accounts.d" / "runners.toml", "default", c)
    env = {k: v for k, v in os.environ.items() if k != "HYPERSPACE_ACCOUNTS_DIR"}
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/revoke-credential.py"),
            c.id,
            "--data-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 1
    assert "runners.toml" in result.stderr


def test_new_credential_avoids_directory_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _script("new-credential")
    _, taken = make(host="agent-1")
    write_account(tmp_path / "accounts.d" / "runners.toml", "default", taken)
    fresh = new_token()
    tokens = iter([("hsp_x", taken.id, taken.secret_sha256), fresh])
    monkeypatch.setattr(module, "new_token", lambda: next(tokens))
    monkeypatch.delenv("HYPERSPACE_ACCOUNTS_DIR", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["new-credential", "--host", "laptop", "--data-dir", str(tmp_path)]
    )
    assert module.main() == 0
    written = parse_accounts((tmp_path / "accounts.toml").read_text())
    assert [c.id for c in written.accounts["default"].credentials] == [fresh[1]]
