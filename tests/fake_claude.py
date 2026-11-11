#!/usr/bin/env python3
# vim: filetype=python
"""A stand-in for the `claude` binary in backfill and launcher tests.

`FAKE_CLAUDE_MODE=echo` prints its arguments, working directory, and the
session variables of its environment as one JSON line. `FAKE_CLAUDE_MODE=fail`
prints an error with color codes and exits 1. `FAKE_CLAUDE_MODE=hang`
starts a child process that writes its pid to `FAKE_CLAUDE_PIDFILE`, then
both wait for ever. `FAKE_CLAUDE_MODE=daemon` does the same as `hang`, but
exits at once, as `claude --bg` does when it starts the Claude Code daemon;
the child keeps the standard output open. `FAKE_CLAUDE_MODE=job` writes
`FAKE_CLAUDE_JOB_STATE` (JSON) as the state.json of a new job in
`CLAUDE_CONFIG_DIR`, with the name `FAKE_CLAUDE_JOB` or a random name, then
exits 0, as `claude --bg` does after it gives the job to the daemon.

`FAKE_CLAUDE_MODE=resume` copies the job records of Claude Code 2.1.301 in
`CLAUDE_CONFIG_DIR`. `--bg --resume <sid>` continues the session in place,
in its job record, or with no record in a new job `<sid[:8]>`. If options
come before `--bg` and a job record has `sessionId` <sid>, or if
`FAKE_CLAUDE_COPY` is set, it starts a copy under a new id: a new job, and a
new transcript with the records of the session under the new id.
`rm <short>` removes the job's directory and keeps the transcripts. With
`FAKE_CLAUDE_RM=refuse`, `rm` refuses and exits 1."""

import glob
import json
import os
import shutil
import subprocess
import sys
import time
import uuid


def record_of(jobs: str, sid: str) -> str | None:
    """The state.json of the job record with `sessionId` <sid>, or None."""
    for path in glob.glob(os.path.join(jobs, "*", "state.json")):
        with open(path) as f:
            if json.load(f).get("sessionId") == sid:
                return path
    return None


def copy_transcripts(config: str, sid: str, own: str) -> None:
    for path in glob.glob(os.path.join(config, "projects", "*", f"{sid}.jsonl")):
        with open(path) as src:
            lines = [json.loads(line) for line in src if line.strip()]
        with open(os.path.join(os.path.dirname(path), f"{own}.jsonl"), "w") as dst:
            for record in lines:
                if "sessionId" in record:
                    record["sessionId"] = own
                dst.write(json.dumps(record) + "\n")


def resume(config: str, sid: str, options: bool) -> None:
    jobs = os.path.join(config, "jobs")
    path = record_of(jobs, sid)
    state: dict[str, object] = {}
    if os.environ.get("FAKE_CLAUDE_COPY") or (path and options):
        own = str(uuid.uuid4())
        copy_transcripts(config, sid, own)
        if path:
            print(
                f"note: background session {sid[:8]} keeps its own saved options, "
                f"so the flags you passed started a copy as {own[:8]}. Without "
                f"flags, the same command continues {sid[:8]} itself."
            )
        else:
            print(
                f"note: session {sid[:8]} is already running in the background, "
                f"so this started a copy as {own[:8]}."
            )
        path = os.path.join(jobs, own[:8], "state.json")
    elif path:
        own = sid
        with open(path) as f:
            state = json.load(f)
    else:
        own = sid
        path = os.path.join(jobs, own[:8], "state.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    state.update(
        sessionId=own, resumeSessionId=own, state="working", detail="Reading files"
    )
    with open(path, "w") as f:
        json.dump(state, f)
    print("job started")


def rm(config: str, short: str) -> None:
    if os.environ.get("FAKE_CLAUDE_RM") == "refuse":
        print("error: job has unpushed commits", file=sys.stderr)
        sys.exit(1)
    shutil.rmtree(os.path.join(config, "jobs", short))


def main() -> None:
    mode = os.environ.get("FAKE_CLAUDE_MODE", "echo")
    if mode == "echo":
        env = {
            k: os.environ.get(k)
            for k in (
                "CLAUDE_CODE_SESSION_ID",
                "CLAUDE_JOB_DIR",
                "HYPERSPACE_STATUS_SELF_ONLY",
            )
        }
        print("thinking")
        print(json.dumps({"argv": sys.argv[1:], "cwd": os.getcwd(), "env": env}))
        return
    if mode == "job":
        job = os.path.join(
            os.environ["CLAUDE_CONFIG_DIR"],
            "jobs",
            os.environ.get("FAKE_CLAUDE_JOB") or uuid.uuid4().hex[:8],
        )
        os.makedirs(job)
        with open(os.path.join(job, "state.json"), "w") as f:
            f.write(os.environ["FAKE_CLAUDE_JOB_STATE"])
        print("backgrounded · " + os.path.basename(job))
        return
    if mode == "resume":
        args = sys.argv[1:]
        config = os.environ["CLAUDE_CONFIG_DIR"]
        if args[0] == "rm":
            rm(config, args[1])
        else:
            sid = args[args.index("--resume") + 1]
            resume(config, sid, options=args.index("--bg") > 0)
        return
    if mode == "fail":
        print("\x1b[31merror:\x1b[0m worktree has unpushed commits", file=sys.stderr)
        sys.exit(1)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    with open(os.environ["FAKE_CLAUDE_PIDFILE"], "w") as f:
        f.write(str(child.pid))
    if mode == "daemon":
        print("job started", flush=True)
        return
    time.sleep(600)


if __name__ == "__main__":
    main()
