"""A token cache in the Linux kernel's user keyring (see "Credentials" in the
spec).

The cache keeps the token in kernel memory, never on disk. All processes of
the user can read it. A key goes away at reboot, or after `ttl` seconds with
no read. On other systems and CPU architectures, and in containers that block
the kernel keyring, the cache is always empty.
"""

from __future__ import annotations

import ctypes
import errno
import logging
import os
import platform
import sys
from typing import Protocol

log = logging.getLogger("hyperspace")

# (add_key, keyctl) system call numbers.
_SYSCALLS = {"x86_64": (248, 250), "aarch64": (217, 219)}
_KEY_SPEC_USER_KEYRING = -4
_KEYCTL_GET_KEYRING_ID = 0
_KEYCTL_SETPERM = 5
_KEYCTL_SEARCH = 10
_KEYCTL_READ = 11
_KEYCTL_SET_TIMEOUT = 15
_KEYCTL_INVALIDATE = 21
# All permissions for the possessor and the user. The hooks do not always
# possess a key that a terminal added, so the user permission must allow the
# read.
_PERM = 0x3F3F0000
# A missing, expired, or revoked key is a cache miss, not an error.
_MISS = {errno.ENOKEY, errno.EKEYEXPIRED, errno.EKEYREVOKED}


class TokenCache(Protocol):
    def get(self, name: str, ttl: int) -> str | None:
        """Return the value, and restart its timeout. None if absent."""
        ...

    def put(self, name: str, value: str, ttl: int) -> None: ...

    def delete(self, name: str) -> bool:
        """Remove the value. Return False if it was not there."""
        ...


class KernelCache:
    """Keys of type "user" in the user keyring, named `hyperspace:<name>`."""

    def __init__(self) -> None:
        numbers = _SYSCALLS.get(platform.machine())
        if sys.platform != "linux" or numbers is None:
            raise OSError(errno.ENOSYS, "no kernel keyring on this system")
        self._add_key, self._keyctl = numbers
        self._libc = ctypes.CDLL(None, use_errno=True)
        self._libc.syscall.restype = ctypes.c_long
        # The default seccomp profile of container runtimes blocks these
        # system calls.
        self._call(self._keyctl, _KEYCTL_GET_KEYRING_ID, _KEY_SPEC_USER_KEYRING, 1)

    def _call(self, *args: object) -> int:
        result = int(self._libc.syscall(*args))
        if result < 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
        return result

    def _find(self, name: str) -> int:
        return self._call(
            self._keyctl,
            _KEYCTL_SEARCH,
            _KEY_SPEC_USER_KEYRING,
            b"user",
            f"hyperspace:{name}".encode(),
            0,
        )

    def get(self, name: str, ttl: int) -> str | None:
        try:
            key = self._find(name)
            size = 4096
            while True:
                buf = ctypes.create_string_buffer(size)
                length = self._call(
                    self._keyctl, _KEYCTL_READ, key, buf, ctypes.c_size_t(size)
                )
                if length <= size:
                    break
                size = length
            self._call(self._keyctl, _KEYCTL_SET_TIMEOUT, key, ctypes.c_uint(ttl))
        except OSError as exc:
            if exc.errno not in _MISS:
                log.info("token cache: %s", exc)
            return None
        return buf.raw[:length].decode()

    def put(self, name: str, value: str, ttl: int) -> None:
        data = value.encode()
        try:
            key = self._call(
                self._add_key,
                b"user",
                f"hyperspace:{name}".encode(),
                data,
                ctypes.c_size_t(len(data)),
                _KEY_SPEC_USER_KEYRING,
            )
            self._call(self._keyctl, _KEYCTL_SETPERM, key, ctypes.c_ulong(_PERM))
            self._call(self._keyctl, _KEYCTL_SET_TIMEOUT, key, ctypes.c_uint(ttl))
        except OSError as exc:
            log.info("token cache: %s", exc)

    def delete(self, name: str) -> bool:
        try:
            self._call(self._keyctl, _KEYCTL_INVALIDATE, self._find(name))
        except OSError as exc:
            if exc.errno not in _MISS:
                log.info("token cache: %s", exc)
            return False
        return True


class NoCache:
    """The cache on systems with no kernel keyring."""

    def get(self, name: str, ttl: int) -> str | None:
        return None

    def put(self, name: str, value: str, ttl: int) -> None:
        pass

    def delete(self, name: str) -> bool:
        return False


def _default() -> TokenCache:
    try:
        return KernelCache()
    except OSError:
        return NoCache()


CACHE: TokenCache = _default()
