# vim: filetype=python
"""Append-only generation files made of zstd frames.

Each segment of a generation is one zstd frame. The frames are concatenated
in one file, so the file is a valid zstd stream and only grows. The index
stores each frame's position, so a reader can decompress only the frames that
it needs.
"""

from __future__ import annotations

import os
from pathlib import Path

import zstandard

_LEVEL = 3


def compress(data: bytes) -> bytes:
    """Return one zstd frame that holds `data`, with the content size set."""
    return zstandard.ZstdCompressor(level=_LEVEL, write_content_size=True).compress(
        data
    )


def decompress(frame: bytes, max_size: int) -> bytes:
    """Decompress one frame. Raise if the content is larger than `max_size`."""
    params = zstandard.get_frame_parameters(frame)
    if params.content_size > max_size:
        raise ValueError("frame content is larger than the limit")
    return zstandard.ZstdDecompressor().decompress(frame, max_output_size=max_size)


def sync_dir(path: Path) -> None:
    """Fsync the directory `path`. This makes its entries durable: a file
    that was created in it, or renamed into it, then survives power loss."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FrameLog:
    """One generation file: `<dir>/<generation id>.zst`."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, data: bytes, first_frame: bool) -> tuple[int, int]:
        """Append `data` as one frame and fsync. Return (offset, length) of
        the frame in the file.

        Set `first_frame` when no committed index row refers to this file
        yet. The file and its directory can then be new, so this also fsyncs
        the directory and its parent. Without this step, power loss can
        remove the file name although the index commit refers to it.
        """
        frame = compress(data)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        try:
            offset = os.fstat(fd).st_size
            written = 0
            while written < len(frame):
                written += os.write(fd, frame[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
        if first_frame:
            sync_dir(self.path.parent)
            sync_dir(self.path.parent.parent)
        return offset, len(frame)

    def read_frame(self, offset: int, length: int, max_size: int) -> bytes:
        with open(self.path, "rb") as f:
            f.seek(offset)
            frame = f.read(length)
        if len(frame) != length:
            raise OSError(f"short read in {self.path}")
        return decompress(frame, max_size)

    def truncate(self, length: int) -> bool:
        """Cut the file to `length` bytes. Return True if it was longer.

        The store calls this at startup with the end of the last frame that
        the index refers to. Bytes after that point are from a write that
        did not commit.
        """
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return False
        if size <= length:
            return False
        with open(self.path, "r+b") as f:
            f.truncate(length)
            f.flush()
            os.fsync(f.fileno())
        return True
