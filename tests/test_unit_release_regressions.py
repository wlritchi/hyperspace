# vim: filetype=python
"""Release and credential regression tests with synthetic data only."""

import importlib.util
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from accounts import parse_accounts

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
