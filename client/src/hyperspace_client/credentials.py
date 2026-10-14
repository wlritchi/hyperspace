"""The token in the token cache and the system keyring (see "Credentials" in
the spec). `config` reads `HYPERSPACE_TOKEN` and the token file first.

A keyring can ask the user for a passphrase, so the caller says when the
keyring may be read:

- `cache-only`: hooks. Only the cache, and on macOS an unlocked Keychain.
- `once`: the syncs that a hook starts at a session start or resume. One
  process reads the keyring and fills the cache; others wait for it. After a
  failed or dismissed read, no hook reads the keyring for `RETRY_AFTER`.
- `interactive`: commands that the user runs. Read the keyring if needed.
"""

from __future__ import annotations

import contextlib
import ctypes
import fcntl
import logging
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from hyperspace_client import keycache

if TYPE_CHECKING:
    from keyring.backend import KeyringBackend

log = logging.getLogger("hyperspace")

KeyringUse = Literal["cache-only", "once", "interactive"]
TokenSource = Literal["environment", "file", "cache", "keyring"]

SERVICE = "hyperspace"
RETRY_AFTER = 600.0
# How long a process waits for another process's keyring read.
LOCK_WAIT = 300.0


class CredentialError(Exception):
    """The keyring cannot be read or written."""


class NotNow(CredentialError):
    """The keyring may not be read now, so the token is not known."""


def account(url: str) -> str:
    """The keyring user name for a server: the URL's host and port. A
    path-based backend (for example pass) makes a clean entry name of it."""
    return urlsplit(url).netloc


def _cache_name(url: str) -> str:
    return f"token:{account(url)}"


def _backend() -> KeyringBackend:
    # keyring adds about 50 ms to the start of every process that imports
    # it, and most hook processes never read the keyring.
    import keyring

    try:
        return keyring.get_keyring()
    except Exception as exc:  # noqa: BLE001 - a configured backend can raise anything
        raise CredentialError(_describe(exc)) from None


def _keyring_get(url: str) -> str | None:
    backend = _backend()
    try:
        return backend.get_password(SERVICE, account(url))
    except Exception as exc:  # noqa: BLE001
        raise CredentialError(_describe(exc)) from None


def _describe(exc: Exception) -> str:
    text = f"keyring: {type(exc).__name__}: {exc}"
    if isinstance(exc, ModuleNotFoundError):
        # A keyringrc.cfg names a backend that is not installed with the
        # client.
        text += (
            f"; install the package that provides {exc.name or 'the backend'} "
            "with the client: `uv tool install --force --with <package> "
            "hyperspace-client@...`"
        )
    return text


def _macos_keychain_unlocked() -> bool:
    """True if the default Keychain is unlocked. A read from a locked
    Keychain shows an unlock dialog."""
    try:
        security = ctypes.CDLL("/System/Library/Frameworks/Security.framework/Security")
        status = ctypes.c_uint32()
        if security.SecKeychainGetStatus(None, ctypes.byref(status)) != 0:
            return False
    except (OSError, AttributeError):
        return False
    return bool(status.value & 1)  # kSecUnlockStateStatus


def _read_unprompted(url: str) -> str | None:
    """Read a keyring that cannot prompt: only an unlocked macOS Keychain."""
    if sys.platform != "darwin":
        return None
    try:
        backend = _backend()
    except CredentialError:
        return None
    if not type(backend).__module__.startswith("keyring.backends.macOS"):
        return None
    if not _macos_keychain_unlocked():
        return None
    try:
        return backend.get_password(SERVICE, account(url))
    except Exception:  # noqa: BLE001
        return None


@contextlib.contextmanager
def _locked(path: Path, wait: float) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        deadline = time.monotonic() + wait
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise NotNow("another process is reading the keyring") from None
                time.sleep(0.2)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def find(
    url: str, state_dir: Path, ttl: int, use: KeyringUse
) -> tuple[str, TokenSource] | None:
    """Return (token, source), or None if the keyring has no token for `url`.
    Raise `NotNow` if `use` does not permit a keyring read now, and
    `CredentialError` if the keyring fails."""
    cache = keycache.CACHE
    name = _cache_name(url)
    token = cache.get(name, ttl)
    if token:
        return token, "cache"
    if use == "cache-only":
        token = _read_unprompted(url)
        if token:
            return token, "keyring"
        raise NotNow(
            "the token is not in the cache; hooks read the keyring only when a "
            "session starts or resumes. Run any hyperspace command to read it now"
        )
    marker = state_dir / "keyring-tried"
    with _locked(state_dir / "keyring.lock", LOCK_WAIT):
        # Another process can have filled the cache while this one waited.
        token = cache.get(name, ttl)
        if token:
            return token, "cache"
        if use == "once":
            with contextlib.suppress(OSError):
                if time.time() - marker.stat().st_mtime < RETRY_AFTER:
                    raise NotNow(
                        "a keyring read failed or was dismissed less than "
                        f"{RETRY_AFTER / 60:.0f} minutes ago"
                    )
        marker.touch(mode=0o600)
        token = _keyring_get(url)
        if token is None:
            return None
        cache.put(name, token, ttl)
        marker.unlink(missing_ok=True)
        return token, "keyring"


def store(url: str, token: str, state_dir: Path, ttl: int) -> str:
    """Save the token in the keyring and the cache. Return the keyring
    backend's name."""
    backend = _backend()
    try:
        backend.set_password(SERVICE, account(url), token)
    except Exception as exc:  # noqa: BLE001
        raise CredentialError(_describe(exc)) from None
    keycache.CACHE.put(_cache_name(url), token, ttl)
    (state_dir / "keyring-tried").unlink(missing_ok=True)
    return backend.name


def forget(url: str) -> tuple[bool, bool]:
    """Remove the token from the keyring and the cache. Return whether each
    one had it."""
    backend = _backend()
    from keyring.errors import PasswordDeleteError  # see _backend

    try:
        backend.delete_password(SERVICE, account(url))
        in_keyring = True
    except PasswordDeleteError:
        in_keyring = False
    except Exception as exc:  # noqa: BLE001
        raise CredentialError(_describe(exc)) from None
    return in_keyring, keycache.CACHE.delete(_cache_name(url))
