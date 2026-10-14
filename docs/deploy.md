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

About 2,000 sessions take about 500 MB on the server. The data directory only
grows, so file-level backups of the PVC (for example with restic) are
incremental.

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

Install the plugin from the marketplace, so that the hooks back up sessions as
they run:

```sh
claude plugin marketplace add wlritchi/wlr-cc-plugins
claude plugin install hyperspace@wlr-cc-plugins
```

The plugin only forwards each hook to `hyperspace hook`, so it needs the
client on PATH (or `HYPERSPACE_CLI` set to a command line that runs it).
Without the client, the hooks do nothing. Upgrade the client with
`uv tool upgrade hyperspace-client`; the plugin does not change with it.

A marketplace lists the plugin with a `git-subdir` source; the `ref` selects
the version:

```json
{
  "name": "hyperspace",
  "source": {
    "source": "git-subdir",
    "url": "https://github.com/wlritchi/hyperspace.git",
    "path": "plugin/hyperspace",
    "ref": "main"
  },
  "description": "Back up Claude Code transcripts to Hyperspace and teleport sessions between hosts"
}
```

Then back up the sessions that are already on the workstation. The first run
uploads everything (about 20 minutes for 2,000 sessions); later runs take
seconds. The plugin also runs this sweep at most once an hour.

```sh
hyperspace sync --all
```

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
leases, and their branches. If a host is gone and cannot surrender its lease,
`hyperspace restore --steal` takes it.

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
