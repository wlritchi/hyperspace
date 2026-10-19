# vim: filetype=python
"""Release and credential regression tests with synthetic data only."""

import base64
import importlib.util
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from accounts import (
    Account,
    Accounts,
    AccountSources,
    Credential,
    dump_accounts,
    new_token,
    parse_accounts,
)

ROOT = Path(__file__).resolve().parent.parent


def test_concurrent_credential_changes_keep_revocation(tmp_path):
    new = ROOT / "scripts/new-credential.py"
    revoke = ROOT / "scripts/revoke-credential.py"

    def create(index):
        return subprocess.run(
            [
                sys.executable,
                str(new),
                "--host",
                f"host-{index}",
                "--data-dir",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    old = create(0)
    old_id = old.split("_")[1]
    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(create, i) for i in range(1, 9)]
        removal = pool.submit(
            subprocess.run,
            [sys.executable, str(revoke), old_id, "--data-dir", str(tmp_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        tokens = [f.result() for f in futures]
        removal.result()
    accounts = parse_accounts((tmp_path / "accounts.toml").read_text())
    ids = {c.id for c in accounts.accounts["default"].credentials}
    assert ids == {t.split("_")[1] for t in tokens}
    assert old_id not in ids


def test_accounts_lock_covers_stable_sidecar(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "credential_writer", ROOT / "scripts/new-credential.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    path = tmp_path / "accounts.toml"
    code = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit(7)
"""
    with module.locked_accounts(path):
        result = subprocess.run(
            [sys.executable, "-c", code, str(path.with_suffix(".lock"))], check=False
        )
        assert result.returncode == 7
    result = subprocess.run(
        [sys.executable, "-c", code, str(path.with_suffix(".lock"))], check=False
    )
    assert result.returncode == 0


def test_revoke_refuses_id_also_in_accounts_directory(tmp_path):
    token, cid, digest = new_token()
    credential = Credential(
        id=cid, secret_sha256=digest, host="runner", scopes=frozenset({"read"})
    )
    text = dump_accounts(
        Accounts({"default": Account(id="default", credentials=(credential,))})
    )
    main = tmp_path / "accounts.toml"
    directory = tmp_path / "accounts.d"
    directory.mkdir()
    main.write_text(text)
    (directory / "runner.toml").write_text(text)
    sources = AccountSources(main, directory)
    assert sources.authenticate(token, datetime.now(UTC)) is None

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/revoke-credential.py"),
            cid,
            "--data-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "HYPERSPACE_ACCOUNTS_DIR": str(directory)},
    )

    assert result.returncode != 0
    assert "runner.toml" in result.stderr
    assert main.read_text() == text
    assert sources.authenticate(token, datetime.now(UTC)) is None


def test_revoke_warns_about_unreadable_directory_file(tmp_path):
    _, cid, digest = new_token()
    credential = Credential(
        id=cid, secret_sha256=digest, host="runner", scopes=frozenset({"read"})
    )
    text = dump_accounts(
        Accounts({"default": Account(id="default", credentials=(credential,))})
    )
    main = tmp_path / "accounts.toml"
    directory = tmp_path / "accounts.d"
    directory.mkdir()
    main.write_text(text)
    broken = directory / "runner.toml"
    broken.write_text(text + "[not valid toml\n")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/revoke-credential.py"),
            cid,
            "--data-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "HYPERSPACE_ACCOUNTS_DIR": str(directory)},
    )

    assert result.returncode == 0, result.stderr
    assert cid not in parse_accounts(main.read_text()).by_credential()
    assert f"could not check {broken}" in result.stderr
    assert f"changed only {main}" in result.stderr


def test_revoke_does_not_warn_without_accounts_directory(tmp_path):
    _, cid, digest = new_token()
    credential = Credential(
        id=cid, secret_sha256=digest, host="runner", scopes=frozenset({"read"})
    )
    main = tmp_path / "accounts.toml"
    main.write_text(
        dump_accounts(
            Accounts({"default": Account(id="default", credentials=(credential,))})
        )
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/revoke-credential.py"),
            cid,
            "--data-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "HYPERSPACE_ACCOUNTS_DIR": str(tmp_path / "missing")},
    )

    assert result.returncode == 0, result.stderr
    assert "warning" not in result.stderr


def test_registry_auth_preserves_arbitrary_password():
    text = (ROOT / ".forgejo/workflows/build.yaml").read_text()
    start = text.index('          AUTH="$(printf', text.index("- name: Registry login"))
    end = text.index("\n\n", start)
    script = "\n".join(line[10:] for line in text[start:end].splitlines())
    # Capture the generated config on stdout, rather than using a real registry.
    script = script.replace(' > "$DOCKER_CONFIG/config.json"', "")
    password = 'quotes" slash\\n actual\nnewline'
    out = subprocess.run(
        ["sh", "-c", script],
        env={
            **os.environ,
            "REGISTRY": "registry.example.com",
            "REGISTRY_USERNAME": "example",
            "REGISTRY_PASSWORD": password,
        },
        capture_output=True,
        text=True,
        check=True,
    )
    config = json.loads(out.stdout)
    auth = config["auths"]["registry.example.com"]["auth"]
    assert base64.b64decode(auth).decode() == "example:" + password


def test_release_uses_event_sha_and_serializes():
    for filename in ("build.yaml", "packages.yaml"):
        text = (ROOT / ".forgejo/workflows" / filename).read_text()
        assert 'git checkout --detach "${GITHUB_SHA}"' in text
        assert 'git checkout "${GITHUB_REF}"' not in text
        assert "cancel-in-progress: false" in text
        assert "concurrency:" in text


def test_version_skips_commit_already_in_newer_release(tmp_path):
    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, capture_output=True, text=True, check=True
        ).stdout.strip()

    git("init")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    git("config", "commit.gpgsign", "false")
    git("commit", "--allow-empty", "-m", "feat: first")
    first = git("rev-parse", "HEAD")
    git("commit", "--allow-empty", "-m", "fix: second")
    git("tag", "0.1.0")
    git("checkout", "--detach", first)
    result = subprocess.run(
        ["sh", str(ROOT / ".forgejo/scripts/next-version.sh"), ""],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == ""


def test_nginx_example_allows_supported_uploads():
    text = (ROOT / "docs/deploy.md").read_text()
    assert 'nginx.ingress.kubernetes.io/proxy-body-size: "9m"' in text
