#!/usr/bin/env python3
# vim: filetype=python
"""A stand-in for the `claude` binary in backfill and launcher tests.

`FAKE_CLAUDE_MODE=echo` prints its arguments, working directory, and the
session variables of its environment as one JSON line. `FAKE_CLAUDE_MODE=fail`
prints an error with color codes and exits 1. `FAKE_CLAUDE_MODE=hang`
starts a child process that writes its pid to `FAKE_CLAUDE_PIDFILE`, then
both wait for ever."""

import json
import os
import subprocess
import sys
import time


def main() -> None:
    mode = os.environ.get("FAKE_CLAUDE_MODE", "echo")
    if mode == "echo":
        env = {
            k: os.environ.get(k) for k in ("CLAUDE_CODE_SESSION_ID", "CLAUDE_JOB_DIR")
        }
        print("thinking")
        print(json.dumps({"argv": sys.argv[1:], "cwd": os.getcwd(), "env": env}))
        return
    if mode == "fail":
        print("\x1b[31merror:\x1b[0m worktree has unpushed commits", file=sys.stderr)
        sys.exit(1)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    with open(os.environ["FAKE_CLAUDE_PIDFILE"], "w") as f:
        f.write(str(child.pid))
    time.sleep(600)


if __name__ == "__main__":
    main()
