# vim: filetype=python
"""Pytest configuration: put lib/ and tests/ on the path, register markers,
and keep the tests away from the user's keyring, token cache, and Claude Code
session."""

import os
import sys
from pathlib import Path

import keyring.core
import pytest

_TESTS_DIR = Path(__file__).resolve().parent
_LIB = _TESTS_DIR.parent / "lib"
for _p in (str(_LIB), str(_TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _hs import MemoryCache, MemoryKeyring
from hyperspace_client import keycache

# For CLI subprocesses: a keyring that holds nothing.
os.environ["PYTHON_KEYRING_BACKEND"] = "keyring.backends.null.Keyring"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "slow: end-to-end tests that start the server")


@pytest.fixture(autouse=True)
def fake_keyring(monkeypatch: pytest.MonkeyPatch) -> MemoryKeyring:
    fake = MemoryKeyring()
    monkeypatch.setattr(keyring.core, "_keyring_backend", fake)
    return fake


@pytest.fixture(autouse=True)
def no_claude_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tests can run inside a Claude Code session, whose variables
    change the client's behavior."""
    for name in ("CLAUDE_CODE_SESSION_ID", "CLAUDE_JOB_DIR"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def token_cache(monkeypatch: pytest.MonkeyPatch) -> MemoryCache:
    cache = MemoryCache()
    monkeypatch.setattr(keycache, "CACHE", cache)
    return cache
