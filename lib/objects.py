# vim: filetype=python
"""Content-addressed blob objects: `<dir>/<sha256[:2]>/<sha256>.zst`."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from framelog import compress, decompress


class ObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, sha256: str) -> Path:
        return self.root / sha256[:2] / f"{sha256}.zst"

    def has(self, sha256: str) -> bool:
        return self._path(sha256).exists()

    def put(self, data: bytes) -> str:
        """Store `data` if it is not stored yet. Return its sha256."""
        sha256 = hashlib.sha256(data).hexdigest()
        path = self._path(sha256)
        if path.exists():
            return sha256
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(compress(data))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return sha256

    def get(self, sha256: str, max_size: int) -> bytes:
        return decompress(self._path(sha256).read_bytes(), max_size)

    def remove(self, sha256: str) -> None:
        self._path(sha256).unlink(missing_ok=True)

    def clean_temporary(self) -> int:
        """Remove temporary files that a crash left. Return the count."""
        count = 0
        if not self.root.exists():
            return 0
        for tmp in self.root.glob("*/.tmp-*"):
            tmp.unlink(missing_ok=True)
            count += 1
        return count
