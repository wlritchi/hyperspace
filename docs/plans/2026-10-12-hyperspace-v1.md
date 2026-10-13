# Plan: Hyperspace v1

Date: 2026-10-12. Spec: [hyperspace](../specs/2026-10-12-hyperspace.md).

Each step is one branch or one commit, and keeps the test suite green. All
steps are in this repo; a marketplace (wlr-cc-plugins) lists the plugin. The
spec has no open questions for v1.

## Step 1: repository scaffold

- `CLAUDE.md`: layout, how to run the tests, Conventional Commits with the tag
  streams (`X.Y.Z`, `protocol-v*`, `client-v*`), and docs conventions.
- `README.md`: what Hyperspace is, and links to the spec.
- `ruff.toml` (py312, line length 88) and pyright strict for `protocol/` and
  `client/`.
- `tests/run.py`: a PEP 723 pytest runner, as in subspace.
- A test workflow: ruff, pyright, pytest on 3.12, and
  `prettier@3.9.10 --check` for Markdown (`.prettierrc.json` wraps prose at 78
  columns).

## Step 2: protocol package

- `protocol/pyproject.toml` with hatchling and hatch-vcs (`tag_regex` for
  `protocol-v*`, `fallback_version = "0.0.0"`).
- `src/hyperspace_protocol/__init__.py`:
  - `TypedDict` request and response bodies, including the placement in append
    responses
  - header names (`Hyperspace-Base-Length`, `Hyperspace-Sha256`)
  - file kinds, host kinds, and scope names
  - the body and decompressed-size limits
  - validation functions for session ids, host names, and session file paths
    (no `..`, no absolute paths, no empty segments, bounded length)
- Unit tests for the validation functions.

## Step 3: server storage core

- `lib/store.py`: the SQLite schema (hosts, workspaces, sessions, placements,
  files, generations, segments, blob versions, metadata events, leases,
  teleports, forks, grants, read audit, purges) and a migration table.
- `lib/framelog.py`: append a zstd frame with `fsync`; read a byte range by
  segment index; truncate to the last committed frame at startup.
- `lib/objects.py`: content-addressed blob objects.
- Generation reads that follow the parent chain and the fork offsets.
- Placement updates: a new placement when an append comes from a host and
  workspace that are not the latest placement.
- Leases: acquire on the first append of a new session; surrender (free or
  reserved for a host); take on teleport; steal. The lease never changes
  whether an append is accepted.
- Session state: activity counts per segment, generation causes, the active
  branch, the `conflicted` state, and recovery forks with a server-selected id
  that share the parent's bytes.
- Unit tests:
  - append and read
  - idempotent retry
  - `409` on a wrong base length
  - fork and read through the parent
  - crash recovery (a frame on disk with no index row)
  - blob version deduplication
  - placement changes and the `diverge` cause
  - `rewrite` branches never make a conflict; two `diverge` branches with
    activity do; a `diverge` branch with only metadata records, assistant
    text, or late tool results does not
  - a recovery fork reads the same bytes as its source branch
  - lease transitions: acquire, surrender, reserved take, refused take by a
    third host, steal; appends from a non-holder are still accepted

## Step 4: server HTTP API, auth, and limits

- `lib/accounts.py` adapted from subspace: `hsp_` credentials bound to one
  host, with scopes, expiry, and hot reload. No master token.
- `scripts/new-credential.py` and `scripts/revoke-credential.py`.
- `server/hyperspace-server.py`: the Starlette routes from the spec, with
  scope checks on each route, the same `404` for "not readable" and "not
  found", zstd bodies with a decompressed-size limit, per-credential quotas
  and rates, a failed-auth rate limit per client address (with
  `HYPERSPACE_TRUSTED_PROXIES`), and the read audit.
- `server/hyperspace-server.service` for systemd `--user`.
- E2E tests with a real server on a free port and a temporary data directory:
  - each scope, and the denials
  - grants, and a grant of a session that the credential cannot read
  - restore and teleport of a conflicted session give `409`; a fork works
  - teleport `409` when a different host holds the lease, and success with
    `steal`
  - expiry and revocation
  - oversized and malformed requests
  - a credential that tries to write as a different host

## Step 5: client sync engine and the Claude Code adapter

- `client/pyproject.toml` (`hyperspace-client`; dependencies `httpx`,
  `zstandard`, and `hyperspace-protocol`), with the `hyperspace` console
  script.
- `hyperspace_client/config.py`: `config.toml` and environment overrides; no
  default URL; refuse `http://` except loopback.
- `hyperspace_client/places.py`: host identity (`HYPERSPACE_HOST`, else the
  hostname) and workspace tracking (the hook input `cwd` at start and resume,
  a change of project directory, or the first record's `cwd` for a backfill).
- `hyperspace_client/state.py`: cursors, last placements, arrival records,
  session states, conflict and move notices, dirty marks, and session locks.
- `hyperspace_client/sync.py`:
  - upload only up to the commit boundary
  - rewrite detection, fork on rewrite, and fork on `409`
  - split large appends
  - upload blobs
  - send the activity count of each append
  - save the lease holder and state from each response
- `hyperspace_client/checkpoint.py`: the single-flight sync with a trailing
  run (`hyperspace sync --checkpoint <sid>`).
- `hyperspace_client/adapters/claude_code.py`: `discover`, `files`,
  `commit_boundary`, `activity` (user prompts and `tool_use` blocks),
  `metadata`, `is_running` (from `~/.claude/sessions/*.json`), and
  `move_aside`, with the mapping table from the spec.
- `hyperspace sync --transcript` and `hyperspace sync --all` (with the cwd
  exclusions).
- Tests with a fake `~/.claude` tree:
  - normal growth, and a partial last line
  - tail truncation, a full rewrite, and an inode change
  - an unreachable server, then recovery
  - many concurrent checkpoint triggers, which must give one running sync and
    a final upload of the last change
  - a backfill of many sessions
- Then run a manual backfill of a workstation into a local server, and check
  the time, the stored size, and the compression ratio. Record them outside
  the repo, or only as ratios with no host names.

## Step 6: restore, teleport, and notices

- `hyperspace ls`, `hyperspace show <sid>` (placements, branches, and state),
  `hyperspace grant`, `hyperspace teleport` (send), `hyperspace restore`
  (receive, with `--resume` and `--held`), and `hyperspace fork`.
- Target workspace: the current directory, or `--cwd`.
- `adapters/claude_code.py`: `restore_plan` (layout for the target `cwd`,
  `sanitize`, including how Claude Code shortens very long names, checked
  against real project directories), `arrival_notice`, and `fork`.
- The local-copy checks: write, fast-forward a prefix, or stop (move aside
  with `--force`). Lease checks (`--steal`), and `--no-tool-results`.
- Surrender, as part of `hyperspace teleport`: the live-process check, the
  grant, the final sync, the move aside, and the server call. On lease loss,
  the client syncs and then moves the local copy aside.
- Arrival records, and the `teleports` server event.
- The SessionStart notice (the pending arrival, move, or conflict notice,
  shown once) and the prevention check at the first prompt after a resume.
  `hyperspace hook` (step 9) runs them.
- The adapter fork: copy each record with `sessionId` changed and a
  `forkedFrom` marker, and move side files to the new id. A conformance test
  compares the result with `claude --resume <id> --fork-session` on a sample
  session.
- Tests:
  - back up from fake host A, restore into fake host B with a different home
    and workspace, and check that the bytes are unchanged and the layout is
    correct
  - a resume from a different directory on the same host gives the short
    location-changed notice
  - `teleport` on A while the session runs: refused; after it stops: the
    transcript moves aside and `claude --resume` cannot find it
  - restore on B while A holds the lease: refused; with `--steal`: works, and
    A moves its copy aside at its next sync
  - steal while A is offline, then resume on A: the prevention check blocks
    the first prompt on A
  - continue on B, then append activity on A with the check bypassed: the
    session becomes conflicted, restore gives `409`, and a fork of either
    branch gives a new id whose records differ from the branch only in
    `sessionId` and the `forkedFrom` marker
  - teleport back to A: restore writes the current branch next to the old
    moved-aside copy; and with a resumable old copy (no surrender), restore
    appends the missing bytes
  - a host that holds a lease and lost its config directory: `restore --held`
    restores the session
- Manual check: on a test session, check that a SessionStart hook's output
  with `source: resume` goes into the transcript and that the model sees it,
  and that a UserPromptSubmit hook can block the first prompt with a clear
  message. Then teleport a real session from a workstation to a runner and
  back.

## Step 7: container image and release workflows

- `Dockerfile` and `.dockerignore`, adapted from subspace (port 8139). The
  image includes `scripts/`, so that admin work is `kubectl exec`.
- `GET /healthz` in the server.
- `charts/hyperspace/`: the Helm chart from the spec (one replica, `Recreate`,
  PVC, Service, optional Ingress, restricted security context, probes,
  `NOTES.txt` with the credential command).
- Repository workflows: build (auto-semver, the image push, and the chart push
  as an OCI artifact), and protocol and client (tag streams).
- The test workflow also runs `helm lint` and validates the output of
  `helm template`.
- Repository settings: registry credentials as repository variables and
  secrets. This is a user action in the forge.

## Step 8: admin tools

- `scripts/purge.py`: remove a session, a generation, or a blob version in the
  data directory, and record the purge. It must work while the server runs
  (SQLite transaction; the server checks the index before it reads a file).
- `scripts/stats.py`: sizes per host, workspace, and harness; the largest
  sessions; the read audit.

## Step 9: the plugin

- `plugin/hyperspace/` in this repo, with the layout of a marketplace plugin.
  `plugin.json` has no version: the marketplace entry's git ref selects it.
- `hooks/hooks.json` runs `hooks/forward.sh` for every event: synchronous on
  SessionStart and UserPromptSubmit, async on the others. The forwarder runs
  `hyperspace hook` with the hook's JSON on stdin, or does nothing without the
  CLI.
- `hyperspace hook` in the client holds all hook behavior (see "Plugin" in the
  spec), and always exits 0 with no output on an error.
- Tests: the forwarder passes stdin and output through and does nothing with
  no CLI; `hyperspace hook` starts the right work for each event, ignores
  unknown events, and never fails, even with a bad configuration.
- A marketplace (wlr-cc-plugins) lists the plugin with a `git-subdir` entry.

## Step 10: deployment docs

- `docs/deploy.md`: install the chart, create one credential for each host
  with `kubectl exec` (workstations: `append`, `read: all`, `grant`; runners:
  `append`, `read: granted`, `grant`), install the client and the plugin on
  each workstation, and run the backfill.
- `docs/runners.md`: add the plugin and the client to a runner image; give
  each runner a Secret with its credential and the environment
  `HYPERSPACE_URL`, `HYPERSPACE_TOKEN`, `HYPERSPACE_HOST` (the runner's stable
  name), and `HYPERSPACE_HOST_KIND=runner`; keep the workspace on a persistent
  volume; and at boot, do not respawn a session whose lease the runner does
  not hold, and run `hyperspace restore --held` if the harness config
  directory does not persist.
- Then deploy with these docs, as a first user would.

## After v1

- If needed: classify tool calls with no side effects, so that they do not
  count as activity.
- Teleport skills for the send side, the receive side, or both (see the spec).
- Ephemeral hosts (see the spec).
- Agent-initiated teleport (see the spec): the teleport lifecycle, a launcher
  on each host, the watchdog that returns a failed teleport to its source, and
  the permission model.
- Restore of linked sessions (`continued-in`, `forkedFrom`, and `summary`
  records whose `leafUuid` points to another session).
- Adapters for other harnesses (Codex first), and a systemd timer for
  harnesses that do not have hooks.
- Search, an MCP tool to read old transcripts, and a web UI.
