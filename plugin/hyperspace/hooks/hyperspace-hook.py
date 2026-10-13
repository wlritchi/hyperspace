#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Hooks for the hyperspace plugin. Standard library only, so that they start
fast; the work is in the `hyperspace` CLI (hyperspace-client).

    hyperspace-hook.py notice        SessionStart: print notices (no network)
    hyperspace-hook.py check-resume  UserPromptSubmit: check the first prompt
                                     after a resume
    hyperspace-hook.py start         SessionStart: checkpoint, and a sweep at
                                     most once an hour
    hyperspace-hook.py checkpoint    other events: checkpoint
    hyperspace-hook.py end           SessionEnd: checkpoint with no wait

Checkpoints and sweeps run in a detached process, so that they outlive the
session and the hook exits at once. A hook never fails: on any error it
prints nothing and exits 0, and the CLI logs to its state directory.

The CLI is, in order: $HYPERSPACE_CLI (a command line), `hyperspace` on
PATH, or `uvx --from $HYPERSPACE_CLIENT_SPEC hyperspace` with a pinned
client release.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import json
import os
import shlex
import shutil
import subprocess
import sys

CLIENT_SPEC = (
    "hyperspace-client @ git+https://github.com/wlritchi/hyperspace"
    "@client-v0.1.0#subdirectory=client"
)
SYNC_TIMEOUT = 8.0
SWEEP_EVERY = "3600"


def cli() -> list[str] | None:
    override = os.environ.get("HYPERSPACE_CLI")
    if override:
        return shlex.split(override)
    found = shutil.which("hyperspace")
    if found:
        return [found]
    uvx = shutil.which("uvx")
    if uvx:
        spec = os.environ.get("HYPERSPACE_CLIENT_SPEC") or CLIENT_SPEC
        return [uvx, "-q", "--from", spec, "hyperspace"]
    return None


def detach(command: list[str]) -> None:
    """Start `command` in its own session, with no terminal or pipes, so it
    outlives the hook and the Claude Code session."""
    subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )


def run(command: list[str]) -> str:
    """Run a quick CLI command and return its output, or "" on failure."""
    try:
        done = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=SYNC_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return done.stdout if done.returncode == 0 else ""


def main(mode: str, payload: dict[str, object]) -> None:
    transcript = payload.get("transcript_path")
    if not isinstance(transcript, str) or not transcript:
        return
    base = cli()
    if base is None:
        return
    cwd = payload.get("cwd")
    event = payload.get("hook_event_name")
    common = ["--transcript", transcript]
    if isinstance(cwd, str) and cwd:
        common += ["--cwd", cwd]
    if mode == "notice":
        source = payload.get("source")
        extra = ["--source", source] if isinstance(source, str) else []
        sys.stdout.write(run([*base, "--quiet", "notice", *common, *extra]))
    elif mode == "check-resume":
        sys.stdout.write(
            run([*base, "--quiet", "check-resume", "--transcript", transcript])
        )
    elif mode in ("start", "checkpoint", "end"):
        sync = [*base, "--quiet", "sync", "--checkpoint", *common]
        if isinstance(event, str):
            sync += ["--event", event]
        if mode == "end":
            sync.append("--no-wait")
        detach(sync)
        if mode == "start":
            detach([*base, "--quiet", "sync", "--all", "--if-stale", SWEEP_EVERY])


if __name__ == "__main__":
    try:
        data = json.load(sys.stdin)
        if isinstance(data, dict) and len(sys.argv) > 1:
            main(sys.argv[1], data)
    except Exception:  # noqa: BLE001, S110
        # A hook must never break the session.
        pass
