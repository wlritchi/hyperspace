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

Store the token in a file that only you can read, and write the client
configuration:

```sh
install -d -m 700 ~/.config/hyperspace
umask 077 && printf '%s\n' '<token>' > ~/.config/hyperspace/token
cat > ~/.config/hyperspace/config.toml <<'EOF'
url = "https://hyperspace.example.com"
token_file = "~/.config/hyperspace/token"
# host = "laptop"           # default: the system hostname
# exclude_cwd = ["/tmp/**"] # workspaces never to upload
EOF
hyperspace whoami
```

Install the plugin from the marketplace, so that the hooks back up sessions as
they run:

```sh
claude plugin marketplace add wlritchi/wlr-cc-plugins
claude plugin install hyperspace@wlr-cc-plugins
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
