# Deploying Hyperspace

This guide installs the server with the Helm chart, gives each host a
credential, and sets up the client and the Claude Code plugin on a
workstation. For container runners, see [runners.md](runners.md).

## 1. Install the server

The release workflow pushes the image to `<registry>/<owner>/hyperspace` and
the chart to `oci://<registry>/<owner>/charts/hyperspace`, with the same
version. You can also install the chart from a checkout (`charts/hyperspace`).

```yaml
# values.yaml
image:
  repository: registry.example.com/wlritchi/hyperspace
  # tag: defaults to the chart's appVersion
persistence:
  size: 20Gi
ingress:
  enabled: true
  className: nginx
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "9m"
  host: hyperspace.example.com
  tls:
    secretName: hyperspace-tls
server:
  # The ingress controller's addresses, so that rate limits see the real
  # client addresses.
  trustedProxies:
    - 10.0.0.0/8
```

```sh
helm install hyperspace oci://registry.example.com/wlritchi/charts/hyperspace \
  --version X.Y.Z -n hyperspace --create-namespace -f values.yaml
```

The chart runs one replica (the server has one writer), keeps the data PVC
when you uninstall, and makes no credentials. Clients must reach the server
over HTTPS: the client refuses plain HTTP except to a loopback address.

Set the ingress request body limit above 8 MiB, with room for compression
frame overhead. The nginx example uses 9 MiB; nginx's default 1 MiB rejects
some supported uploads. Other ingress controllers need their equivalent
setting.

About 2,000 sessions take about 500 MB on the server. The data directory only
grows, so file-level backups of the PVC (for example with restic) are
incremental.

The SQLite page caches of the server use at most 128 MiB for the writer plus
`server.readConnections` times `server.readCacheBytes` for the reads (256 MiB
in total with the defaults). Keep this total well under the memory limit of
the pod, with room for the request bodies. A request that takes longer than
`server.slowRequestSeconds` (default 1) goes to the log at INFO, with its time
and its waits for the database connections.

## 2. Create credentials

Each host gets its own credential, bound to its host name. Create them inside
the pod; the script prints each token once.

```sh
kubectl -n hyperspace exec deploy/hyperspace -- \
  python /app/hyperspace/scripts/new-credential.py \
  --host laptop --scopes append,read,grant --read all
```

| Host kind   | Scopes                    | Read      |
| ----------- | ------------------------- | --------- |
| Workstation | `append`, `read`, `grant` | `all`     |
| Runner      | `append`, `read`, `grant` | `granted` |

A runner with `--read granted` can read only its own sessions and the sessions
granted to it. Revoke a credential with
`scripts/revoke-credential.py <credential id>`; it stops working at once.

### A credential for subspace

Subspace wakes a stopped session through `POST /v1/wake/{harness}` (see the
[wake spec](specs/2026-10-28-wake.md)). Give it a credential with the `wake`
scope and no other scope. Its host name is only a name for logs:

```sh
kubectl -n hyperspace exec deploy/hyperspace -- \
  python /app/hyperspace/scripts/new-credential.py \
  --host subspace --scopes wake
```

Put the token and the URL `https://hyperspace.example.com/v1/wake/claude-code`
in the wake configuration of subspace. A `wake` credential can call only the
wake endpoint, so a leak of it exposes no transcript.

### Credentials from a provisioner

A provisioner (for example, OpenTofu) can make credentials with no step in the
pod. It makes each token itself, gives the token to the host (for example, in
a Secret for a runner), and gives the server an accounts file with the sha256
of each secret:

- Token: `hsp_<id>_<secret>`. The id is 12 characters from `a`-`z` and
  `2`-`7`. The secret is at least 40 characters from `A`-`Z`, `a`-`z`,
  `0`-`9`, `-` and `_`; use at least 256 random bits.
- Accounts file: the format in `lib/accounts.py`, with `secret_sha256` = the
  hex sha256 of the secret (the part after the id).

```toml
[[accounts.default.credentials]]
id = "abcdefghijkl"
secret_sha256 = "<hex sha256 of the secret>"
host = "agent-1"
scopes = ["append", "read", "grant"]
read = "granted"
```

Put the file in a Secret, under a key that ends in `.toml`, and name the
Secret in the chart:

```yaml
accountsSecrets:
  - hyperspace-runner-accounts
```

Use the account id of your other credentials, so that sessions can teleport
between the hosts. To revoke a credential, remove it at the provisioner. See
"Accounts files" in the spec for how the server merges the files.

## 3. Set up a workstation

Install the client:

```sh
uv tool install 'hyperspace-client @ git+https://github.com/wlritchi/hyperspace@client-vX.Y.Z#subdirectory=client'
```

Write the client configuration, then store the token in the system keyring.
`hyperspace login` checks the token with the server first.

```sh
install -d -m 700 ~/.config/hyperspace
cat > ~/.config/hyperspace/config.toml <<'EOF'
url = "https://hyperspace.example.com"
# host = "laptop"           # default: the system hostname
# exclude_cwd = ["/tmp/**"] # workspaces never to upload
EOF
hyperspace login
hyperspace whoami
```

The client uses your default `keyring` backend. If your `keyringrc.cfg` names
a backend from another package, install that package with the client:
`uv tool install --force --with <package> ...`, where `<package>` is the
package name or a local path. On Linux, the client also keeps the token in a
cache in kernel memory, so that hooks do not read the keyring during a turn.
If the keyring asks for a passphrase, it asks when a session starts or
resumes, at most once for many sessions (see "Credentials" in the spec).

A token file also works, with no keyring:
`token_file = "~/.config/hyperspace/token"` (mode 0600).

Install the plugin, so that the hooks back up sessions as they run. This
repository is its marketplace:

```sh
claude plugin marketplace add wlritchi/hyperspace
claude plugin install hyperspace@hyperspace
```

The plugin only forwards each hook to `hyperspace hook`, so it needs the
client on PATH (or `HYPERSPACE_CLI` set to a command line that runs it).
Without the client, the hooks do nothing. Upgrade the client with
`uv tool upgrade hyperspace-client`; the plugin does not change with it.

No file in the repository has a plugin version: Claude Code uses the commit,
so each new commit on the branch that the marketplace follows is an update. To
stay on a release, add the marketplace at its tag
(`claude plugin marketplace add wlritchi/hyperspace#X.Y.Z`).

Then back up the sessions that are already on the workstation. The first run
uploads everything (about 20 minutes for 2,000 sessions); later runs take
seconds. The plugin also runs this sweep at most once an hour.

```sh
hyperspace sync --all
```

### The launcher

`hyperspace launcher` sends the host's heartbeat each minute, so that
`hyperspace ls` shows which sessions run. It also closes the background jobs
of `done` sessions: after the job's process stops, it backs up the session and
runs `claude rm <job>`, which removes the job from `claude agents` and deletes
the job's worktree. If `claude rm` refuses, for example because the worktree
has commits that are not pushed, the job stays, and `hyperspace ls` shows
`done, not closed: <reason>`. The launcher never forces the removal. A job
that has its own id and writes the transcript of another session (an older
shape of a session that you sent to the background) also stays, with the
reason `job <short> writes another session; close it yourself`. Close such a
job with `claude rm <short>`.

The SessionStart hook starts the launcher if it does not run. It stops after
10 minutes with no live session. To run it all the time instead, use a service
manager with `--stay`. Under systemd:

```ini
# ~/.config/systemd/user/hyperspace-launcher.service
[Unit]
Description=Hyperspace launcher

[Service]
ExecStart=%h/.local/bin/hyperspace --quiet launcher --stay
# With no token in the cache yet, the launcher stops; try again later.
Restart=always
RestartSec=60

[Install]
WantedBy=default.target
```

```sh
systemctl --user enable --now hyperspace-launcher.service
```

Under launchd, on macOS:

```xml
<!-- ~/Library/LaunchAgents/hyperspace-launcher.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>hyperspace-launcher</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/you/.local/bin/hyperspace</string>
    <string>--quiet</string>
    <string>launcher</string>
    <string>--stay</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>60</integer>
</dict>
</plist>
```

```sh
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/hyperspace-launcher.plist
```

With `--stay`, the launcher also acts on wakes: when a message arrives for a
stopped session that this host holds, it resumes the session as a background
job (see the [wake spec](specs/2026-10-28-wake.md)). The resumed session gets
its messages only if the channel that relays subspace messages loads, so put
the Claude Code options for a resume in `config.toml`:

```toml
launcher_claude_args = [
  "--dangerously-load-development-channels", "plugin:subspace@subspace",
  "--model", "opus",
]
```

The launcher refuses some options in this list, and the last option must not
take a list of values; see "At boot: the launcher" in
[runners.md](runners.md). A launcher without `--stay` does not act on wakes:
the wake request then stays pending until you resume the session, or until a
launcher with `--stay` runs.

Only one launcher runs for each host and harness config directory, so a
launcher from a hook and one from a service manager do not conflict.
`HYPERSPACE_LAUNCHER=off` turns off the launcher and the heartbeats; set it
for tests on a real `~/.claude`. The settings `heartbeat_interval` (default
60), `launcher_idle_exit` (default 600), and `claude_bin` (or
`HYPERSPACE_CLAUDE`, default `claude`) go in `config.toml`. The client works
on Linux and macOS.

### Upgrade order

Upgrade the server before the client. The new server accepts old clients while
advertising `workspace-utf8` and `restore-precondition` capabilities. The new
client requires restore preconditions before it changes local files or takes a
lease. Unicode workspace uploads require encoded-header support. A subsequent
release will remove the legacy request forms.

### Rollback

The server's index migrations only add to the schema, and an older server
ignores the migrations that it does not know. Thus a rollback to an older
server image works with the same data directory. An older server does not
update the copies of the latest status and metadata in the session rows, which
the session list uses. When the newer server starts again, it finds the events
that the older server wrote, fills the copies of these sessions again, and
logs `new copies for N sessions`. No admin step is necessary.

## 4. Teleport a session

On the host that has the session (the lease holder), stop the session, then:

```sh
hyperspace teleport <session id> --to agent-1
```

On the target host, in the directory where the session must continue:

```sh
hyperspace restore <session id> --resume
```

The resumed session starts with a notice that says where it was before. Use
`hyperspace ls` and `hyperspace show <session id>` to see sessions, their
leases, and their branches. `hyperspace ls --open` lists the sessions that are
not done, with their status, and `hyperspace done <session id>` marks a
session done. If a host is gone and cannot surrender its lease,
`hyperspace restore --steal` takes it.

Sessions from before the session status have no status. To ask each one to set
its own status, run the backfill on each host that has the sessions:

```sh
hyperspace backfill-status --since 30d --dry-run   # list the sessions
hyperspace backfill-status --since 30d --rate 30   # message them
```

The backfill resumes each session for one headless turn, so each session costs
a full context load. It records each session that it messaged, and a second
run skips them unless you give `--retry`.

## 5. Admin work

Admin scripts run inside the pod, on the data directory, and are safe while
the server runs:

```sh
kubectl -n hyperspace exec deploy/hyperspace -- python /app/hyperspace/scripts/stats.py
kubectl -n hyperspace exec deploy/hyperspace -- python /app/hyperspace/scripts/purge.py \
  --reason "token in a transcript" session claude-code <session id>
```

`purge.py` shows what it would remove, and removes it only with `--yes`. A
host that still has the session uploads it again at its next sync, so remove
the local copies too.
