"""Checkpoint scheduling: a single-flight sync with a trailing run.

See "Checkpoint scheduling" in the spec. A hook marks the session dirty and
starts a checkpoint process. The process syncs only if it gets the session's
lock; otherwise the process that holds the lock sees the mark and syncs
again. A session therefore uploads at most once each `interval` seconds while
it is busy, and the last change is always uploaded.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from hyperspace_client.state import State


def checkpoint(
    state: State,
    harness: str,
    session_id: str,
    sync: Callable[[], None],
    interval: float,
    wait: bool = True,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> int:
    """Mark the session dirty, then sync it while it stays dirty. Return the
    number of syncs that this process ran (0 if another process holds the
    lock)."""
    state.mark_dirty(harness, session_id)
    runs = 0
    last_start: float | None = None
    while True:
        with state.try_lock(harness, session_id) as locked:
            if not locked:
                return runs
            marked = state.dirty_since(harness, session_id)
            if marked is None:
                return runs
            if wait and last_start is not None:
                remaining = interval - (clock() - last_start)
                if remaining > 0:
                    sleep(remaining)
                    # Take the newest mark, so that marks made during the
                    # wait are cleared by this run.
                    marked = state.dirty_since(harness, session_id) or marked
            last_start = clock()
            state.clear_dirty(harness, session_id, marked)
            sync()
            runs += 1
        # The lock is free now. A mark made during the sync is still in the
        # table, and the loop runs again for it. A mark made after the check
        # below starts a process that gets the lock.
        if state.dirty_since(harness, session_id) is None:
            return runs
