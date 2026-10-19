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

## At boot

Two rules keep leases correct across restarts:

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
