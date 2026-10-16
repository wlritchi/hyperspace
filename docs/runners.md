# Hyperspace on container runners

A runner is a container (for example, a Kubernetes Deployment) where an agent
runs, with a stable name and a workspace on a persistent volume. See "Hosts"
in the [spec](specs/2026-10-12-hyperspace.md). This guide assumes that the
server runs (see [deploy.md](deploy.md)).

## Requirements

- The workspace (the agent's working directory) is on a persistent volume.
- The runner has a stable name. Pod names change at each restart, so set it
  with `HYPERSPACE_HOST`.
- The harness config directory (`~/.claude`) can be on a persistent volume,
  but it does not have to be (see "At boot").

## The image

Add the client and the plugin to the runner image, from one checkout of a
release, so that the plugin's hooks match the client that they call:

```dockerfile
RUN git clone --filter=blob:none https://github.com/wlritchi/hyperspace /opt/hyperspace \
    && git -C /opt/hyperspace checkout --detach X.Y.Z \
    && uv tool install /opt/hyperspace/client \
    && claude plugin marketplace add /opt/hyperspace \
    && claude plugin install hyperspace@hyperspace
```

Keep the checkout in the image: the marketplace registration and the client's
`hyperspace-protocol` dependency refer to it. The clone needs its history and
tags, because the client takes its version from the latest `client-v` tag.

## Credential and environment

Create a credential for the runner (see deploy.md), with `--read granted`, and
put the token in a Secret:

```sh
kubectl -n agents create secret generic agent-1-hyperspace --from-literal=token='<token>'
```

```yaml
env:
  - name: HYPERSPACE_URL
    value: https://hyperspace.example.com
  - name: HYPERSPACE_TOKEN
    valueFrom:
      secretKeyRef:
        name: agent-1-hyperspace
        key: token
  - name: HYPERSPACE_HOST
    value: agent-1
  - name: HYPERSPACE_HOST_KIND
    value: runner
```

## At boot: the launcher

The entrypoint runs `hyperspace launcher --runner` as its main process, in the
runner's workspace, in place of its own supervisor. See "Launcher" in the
[session status spec](specs/2026-10-15-session-status.md). The launcher:

1. Registers the host. If the server or the proxy is not available, it waits
   and tries again, from 5 seconds to at most 5 minutes between tries. A
   permanent error stops it at once with an error: a bad credential
   (`unauthorized` or `forbidden`), a proxy that refuses its credential
   (`407`), no server URL, a token with a space or a control character, or a
   local error in the request, for example a redirect loop.
2. Restores each session whose lease the runner holds and that has no local
   copy (the same as `hyperspace restore --held`). If the server is not
   available, or a restore fails, the launcher tries again at later ticks,
   after 1, 2, 4 minutes and so on, to at most one hour between tries, until a
   restore has no failure. A session resumes after its restore. A session with
   an unfinished restore (its teleport request is not confirmed) does not
   resume until a retry completes the restore.
3. Resumes each held session that needs a process: a session that was
   `working` when the runner stopped, and a session that is `waiting` for an
   event that only its process can receive (`wake: local`). It runs
   `claude <args> --bg --resume <session id> <message>` in the session's
   workspace, after it checks that the session does not run. It holds the
   session's sync lock from these checks until `claude --bg` returns, and it
   does not resume a session while a sync or a restore of it runs. If the
   session changed during the hold, the launcher then starts a checkpoint. It
   does not resume a session that the server shows as `live` on any host, also
   on the runner itself (for example from a different harness config
   directory). The message tells the agent that the runner restarted, and that
   its crons, Monitors and background tasks stopped.
4. Does not resume a session that is `ready`, `done`, `waiting` with
   `wake: message`, or that has no status. The user, or a message, wakes these
   sessions. Sessions from before the session status feature have no status
   until the backfill gives them one (`hyperspace backfill-status`).
5. Each minute: sends the heartbeat, closes the background jobs of `done`
   sessions with `claude rm`, and resumes again a held session that needs a
   process and does not run. It resumes one session at most 5 times in an
   hour.

The launcher never stops when no session runs. A runner with no session that
needs a process runs only the launcher.

The image must provide:

- The client and the plugin (see "The image"), and `claude` on the `PATH` (or
  `HYPERSPACE_CLAUDE`).
- The environment of "Credential and environment", and the Claude Code options
  for each resume as a JSON list in `HYPERSPACE_LAUNCHER_CLAUDE_ARGS`. Do not
  put `--bg`, `--resume`, `--continue`, `--fork-session`, `--session-id` or
  `--print` in it; the launcher refuses them. For example:

  ```yaml
  - name: HYPERSPACE_LAUNCHER_CLAUDE_ARGS
    value: >-
      ["--model", "opus", "--agent", "claude",
       "--dangerously-load-development-channels",
       "plugin:notifications@wlr-cc-plugins"]
  ```

- An entrypoint that starts in the workspace and then runs the launcher:

  ```sh
  cd "$WORKSPACE_DIR"
  exec hyperspace launcher --runner
  ```

The launcher does not yet receive wake requests from subspace. Until it does,
a message to a stopped session waits until a user or the runner resumes it.

## At boot, with your own supervisor

If the entrypoint keeps its own supervisor, two rules keep leases correct
across restarts:

1. If the harness config directory does not persist, restore the sessions
   whose lease the runner holds before the agent starts:

   ```sh
   hyperspace restore --held
   ```

   Each session goes back to the workspace where it ran.

2. If the entrypoint resumes background sessions (for example, from
   `~/.claude/jobs/*/state.json`), resume only sessions whose lease this
   runner holds. A session that was teleported away must not start again:

   ```sh
   holder="$(hyperspace show "$session_id" --json | jq -r '.state.lease.host // ""')"
   [ "$holder" = "$HYPERSPACE_HOST" ] && claude --resume "$session_id"
   ```

## Teleports to and from a runner

A workstation or another runner sends a session with
`hyperspace teleport <session id> --to agent-1`, which also grants it to the
runner. On the runner, `hyperspace restore <session id>` in the target
workspace takes the lease. To send a session from a runner, run
`hyperspace teleport` there after the session stops.
