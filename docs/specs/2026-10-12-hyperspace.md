# Hyperspace: transcript backup and session teleport

Status: draft. Date: 2026-10-12.

## Problem

Claude Code keeps each session only on the machine that ran it, in
`~/.claude/projects/<encoded cwd>/<session id>.jsonl` and some side files.
This causes three problems:

1. A lost disk, a crash, a reinstall, or Claude Code's `cleanupPeriodDays`
   cleanup (default 30 days) deletes the history permanently.
2. A session that starts on a workstation cannot continue on a cloud runner,
   or the reverse. `claude --resume` only finds sessions that are on the local
   disk.
3. Other agent harnesses (Codex, opencode, and others) have the same problems
   and use different file layouts.

Hyperspace is a server and a client. The client copies transcripts to the
server while the agent works, including during long turns. The server keeps
every byte that it receives and never changes or deletes it. The client can
also restore a session onto a different host, so that `claude --resume <id>`
continues the session there. We call this a _teleport_. A session keeps its id
through any number of teleports.

## Goals

- Continuous, incremental backup of Claude Code sessions: the main transcript,
  subagent transcripts, spilled tool results, file-history snapshots, and plan
  files.
- Checkpoints during a turn, so that a host crash in a long turn loses at most
  a few seconds of transcript.
- An append-only store. The server API cannot delete or overwrite data. When a
  local file is rewritten, the server keeps the old bytes and the new bytes.
- A backfill of the sessions that are already on each host.
- A teleport that restores the transcript bytes without change and makes the
  move visible to the agent: the transcript gets a notice that says where the
  session was and where it is now.
- A model of _hosts_ (workstations, and cloud runners with persistent volumes)
  and _workspaces_ (the directories where sessions run). Sessions move between
  workspaces. A workspace can hold many sessions.
- Stable session ids that other systems, such as subspace, can use as
  long-term identifiers.
- A server that does not know about harness formats. All format knowledge is
  in client _adapters_, so that a new harness needs only a new adapter.
- A security model for a server on the open internet, from the first release.
- A small stack: Python 3.12, PEP 723 `uv` scripts with lock files, a
  container image and a Helm chart built by repository workflows, and a thin
  client plugin in wlr-cc-plugins.

## Non-goals (v1)

- Changing the content of recorded history. Hyperspace never rewrites paths or
  other content in a transcript. A teleport adds a notice; it does not edit
  old records. A fork is a new session: its copy has the new session id, and
  the original stays unchanged.
- Adapters for harnesses other than Claude Code. The design must permit them;
  v1 does not include them.
- A web UI, full-text search, or an MCP tool to search old transcripts.
- Live, two-way sync of one session that is active on two hosts at the same
  time. Hyperspace keeps both histories, but only one can continue under the
  session id (see "Branches and conflicts").
- Backup of the working tree. A teleport restores the conversation, not the
  code. The code must get to the target workspace by other means (git, a
  shared disk, and so on).
- Sharing between accounts. The auth model has accounts, but v1 has one user.
- Ephemeral hosts (see "Later: ephemeral hosts").

## Facts about Claude Code storage

These facts come from the files and behavior of Claude Code 2.1.288 on a
workstation and on container runners, and from the Claude Code documentation.
They control the design.

- **Location.** The main transcript is
  `<config dir>/projects/<sanitize(cwd)>/<sid>.jsonl`. The config directory is
  `~/.claude` unless `CLAUDE_CONFIG_DIR` is set. `sanitize` replaces every
  UTF-16 code unit outside `[A-Za-z0-9]` with `-`. Above 200 units, it keeps
  the first 200 and adds `-` plus the absolute signed 32-bit polynomial hash
  of the original path (multiplier 31), in base 36. This matches Claude Code
  2.1.288. The encoding is lossy, so the client records the real `cwd` and
  does not decode the directory name.
- **Side files** are in `projects/<enc>/<sid>/`: `subagents/agent-*.jsonl`
  with `.meta.json` files, `tool-results/*` (large tool outputs that the
  transcript refers to by absolute path), and `workflows/`. Outside the
  project directory: `file-history/<sid>/` (used by `/rewind`) and
  `plans/<slug>.md`.
- **Resume needs only the main transcript.** The other files make the resumed
  session more complete: the model can read old tool outputs, `/rewind` works,
  and subagents can continue.
- **The transcript is almost append-only, but not completely.** Claude Code
  can shorten the file, rewrite the whole file, move it to a different project
  directory (`/cd`, and entry into a worktree), or set it aside
  (`.orphaned-*`, `.superseded-*`). The client must detect a rewrite and must
  not assume that the server copy is a prefix of the local file. Claude Code
  writes each record as one line, so a reader can see a partial last line.
- **Records contain absolute paths**: `cwd` on every message, tool inputs such
  as `file_path`, tool-result spill paths, and `worktreeSession` paths. Claude
  Code does not change these on resume.
- **Resume lookup.** `claude --resume <id>` looks in the project directory of
  the current directory first, and then in all other project directories. The
  lookup fails if more than one other project directory contains the ID. A
  restore must therefore leave exactly one copy.
- **Hooks.** The stdin of every hook event contains `session_id`,
  `transcript_path` and `cwd`. Inside a subagent it also contains `agent_id`.
  Tool hooks (PreToolUse, PostToolUse) fire for subagent tool calls too. A
  command hook with `"async": true` runs in the background and does not block
  the session. SessionEnd hooks have a short time budget.
- **Live sessions.** Claude Code writes a file for each running process in
  `~/.claude/sessions/<pid>.json`, with `sessionId`, `pid`, `procStart`,
  `cwd`, and (for background sessions) `jobId`. A client can use it to find
  out whether a session runs on the host.
- **Hook output is part of the transcript.** Claude Code writes the output of
  a SessionStart hook into the transcript as an `attachment` record of type
  `hook_success`, and gives it to the model. A synchronous SessionStart hook
  can therefore add a notice that the agent sees and that the backup keeps,
  with no need to write transcript records directly.
- **Container runners.** An agent can run in a container, for example a
  Kubernetes Deployment. The working directory (for example
  `/home/agent/workspace`) can be on a persistent volume. The harness config
  directory (`~/.claude`) can be on a persistent volume too, but it is not
  always. Pod names change at each restart, so the stable name of the runner
  must come from configuration. Runners often have no human who approves their
  tool calls (for example, with `--dangerously-skip-permissions`).
- **Size.** On one developer workstation with about 1,900 sessions, there were
  2.3 GB of main transcripts, 1.1 GB of subagent transcripts, and 0.4 GB of
  tool results. The median transcript was 175 KB and the largest was 85 MB.
  With zstd, transcripts become 10 to 30 times smaller.
- **The format is internal** and can change in any release. Adapters must pass
  unknown record types through without change. The server never parses
  transcripts.

## Model

### Places: hosts and workspaces

- **Account.** The owner of the data.
- **Host.** A named place where sessions run, with its own credential. A host
  is not a process and not a pod. It has a name, a kind, and attributes:
  - `workstation`: a physical or virtual machine, for example `laptop`. The
    default name is the system hostname.
  - `runner`: a container with a stable name, for example `agent-1`, whose
    workspace is on a persistent volume. The pod changes; the workspace and
    the name stay. The name comes from configuration (`HYPERSPACE_HOST`),
    never from the pod hostname. Hyperspace tolerates, but does not need, a
    persistent harness config directory. If the config directory does not
    persist, the local transcripts go away when the pod restarts. The runner's
    leases and the backups stay, so the runner restores the sessions whose
    leases it holds (see "Leases").

  A place that cannot keep its workspace is not a host in this sense. v1 does
  not support such places (see "Later: ephemeral hosts").

  Attributes: home directory, harness config directories, and OS. The client
  sends these when it registers. In v1, the server binds each credential to
  one host name.

- **Workspace.** The directory where a session runs: the directory where it
  started, or where it was resumed or restored. Examples:
  `laptop:/home/wlritchi/hyperspace`, `laptop:/home/wlritchi/work` (a root
  that holds many repos), `agent-1:/home/agent/workspace`. The key is
  `(host, path)`. There is no workspace configuration. For Claude Code, the
  workspace is the directory that selects the transcript's project directory.
  The client takes it from the `cwd` of the hook input when the session starts
  or resumes, and updates it when Claude Code moves the transcript to a
  different project directory (`/cd`). For a backfill with no hook input, it
  uses the `cwd` of the first record.

  The workspace changes only by a new start, a resume from a different
  directory, a `/cd`, or a teleport. Directory changes inside the session (for
  example `cd` in a shell tool) do not change it. A workspace can hold many
  sessions, and a session can work anywhere below or outside its workspace.
  Hyperspace does not assume one session for each repo.

- **Placement.** A record that a session was active in a workspace: session,
  host, workspace, `cwd`, first and last activity time, and the cause
  (`start`, `teleport`, `steal`, or `diverge`). The server adds a placement
  when an append arrives from a host and workspace that are not the latest
  placement. The list of placements is the travel history of the session.
- **Lease.** A marker that the session lives on a host: it is running there,
  or it can be resumed there. At most one host and workspace holds the lease
  of a session. The lease is _advisory_: the server never refuses an append
  because of it, and it cannot stop a local `claude --resume`. It tells
  people, hooks, and restore where the session lives. A lease has no time
  limit, because a workstation can be off for weeks and still be the home of
  its sessions. See "Leases".

This model keeps three names apart that are easy to merge on a runner with one
long-lived agent: the host (`agent-1`), the workspace (`/home/agent/workspace`
on `agent-1`), and the session (a Claude Code session id, which a messaging
system such as subspace can also give a handle). A workspace can hold more
than one session, and a session can leave its first workspace.

### Sessions and files

- **Harness.** The agent program, for example `claude-code`.
- **Session.** The key is `(account, harness, session id)`. The session id is
  the harness's id, and it does not change when the session is teleported.
  Claude Code makes a new id for `--fork-session`, `/branch` and `/clear`; the
  adapter records these links (`forkedFrom`, `continued-in`) as metadata.
- **File.** A path relative to the session root, for example
  `transcript.jsonl`, `subagents/agent-a1b2.jsonl`, or `tool-results/x.txt`.
  The adapter selects these names. They do not depend on the local layout. A
  file has one of two kinds:
  - A **log** is a byte stream that grows at the end (JSONL transcripts).
  - A **blob** is a sequence of complete versions (tool results, meta files,
    plan files, file-history snapshots). The server stores a new version only
    when the content hash changes.
- **Generation.** The server history of a log is a tree of generations. A
  generation has a parent generation, a fork offset in the parent, and its own
  bytes. The content of a generation is the first `fork offset` bytes of its
  parent, followed by its own bytes. The first generation has no parent. A new
  generation starts when the local file no longer matches the server copy (a
  rewrite), or when two hosts append at the same offset (a divergence).
  Generations share their common prefix, so a fork does not copy data.
- **Segment.** One accepted append to a generation. The server records its
  offset, length, sha256, activity count, host, workspace, and receive time.
- **Active branch.** The branch that continues under the session id (see
  "Branches and conflicts"). Restore and teleport use it.

### Append protocol

The client uploads only complete records. For a JSONL log, it uploads up to
the last newline and leaves a partial last line for the next sync.

An append request contains the generation id, the _base length_ (the
generation length that the client expects), the sha256 of the new bytes, the
activity count of the new bytes, and the bytes. A request that creates a
generation contains the parent, the fork offset, and the cause (`rewrite` or
`diverge`). The server does one of these:

- If the base length is the current generation length, it appends the bytes
  and returns the new length.
- If a segment with the same offset and the same sha256 already exists, the
  request is a retry. The server returns success and does not write again.
  This makes appends idempotent: the client can crash after the server commits
  and before the client saves its cursor.
- Otherwise it returns `409 Conflict` with the current generation length. The
  client then creates a new generation that forks at its base length, and
  appends to that generation.

Each response also contains the session's lease holder and state, so the
client learns when the session is active somewhere else (see "Branches and
conflicts").

The client detects a local rewrite before it appends. For each local file, the
client cursor stores the device and inode, the local length that it uploaded,
and the list of its segments (offset, length, sha256). If the file is shorter
than the cursor length, if the inode changed, or if the last segment's bytes
have a different hash, the client finds the last segment that still matches
the local file and forks a new generation at the end of that segment. Then it
uploads the rest of the file to the new generation.

### Metadata

The adapter extracts session metadata on the client and sends it with each
sync: the workspace, the current `cwd`, title (the latest `custom-title` or
`ai-title` record), first and last timestamps, harness version, record count,
and links to other sessions. The server stores each metadata update as an
event and shows the latest one. The server does not need to parse the harness
format, and list filters still work.

## Teleport

### Leases

A lease answers one question: where should this session continue? It replaces
guesses (such as "the last activity was recent, so the session is perhaps
still live") with an explicit record. It is also the base for the _in transit_
state of agent-initiated teleport (see "Later").

- **Acquire.** The first append of a new session acquires the lease for its
  host, if nobody holds it. A restore acquires the lease for the target host,
  if the lease is free or reserved for that host.
- **Surrender.** The send side of a teleport (`hyperspace teleport`, see "Send
  and receive") surrenders the lease. It runs on the host that holds the
  lease:
  1. Check that the session does not run on this host. The Claude Code adapter
     reads the live-session registry (`~/.claude/sessions/<pid>.json`, which
     has `sessionId`, `pid` and `procStart`) and checks that each process is
     alive and has the same start time on Linux. On other systems, a live pid
     conservatively blocks the move when its start time cannot be compared. If
     the session runs, stop.
  2. Sync the session. If the server cannot be reached, stop: a surrender must
     not leave bytes that only this host has. This includes an incomplete last
     JSONL record: surrender stops until that record can be backed up.
  3. Move the local transcript aside, to
     `<sid>.jsonl.hyperspace-surrendered-<time>`, so that `claude --resume` on
     this host does not find it. The side files stay; they do not make the
     session resumable. Discovery skips moved-aside files.
  4. Record the surrender on the server. With a target host, the lease is
     _reserved_ for that host, and only that host can take it without
     `--steal`. Without a target host, the lease is _free_.

  A runner that restarts background sessions at boot (from
  `~/.claude/jobs/*/state.json`) needs one more step: a surrender must also
  retire that job state, or the runner's entrypoint must skip sessions whose
  lease the runner does not hold.

- **Steal.** `hyperspace restore --steal` takes a lease that a different host
  holds, when that host cannot surrender (it crashed, it is off, or it is
  gone). The server records a `steal` placement.
- **Loss.** A host learns that it lost a lease from append responses and from
  state checks. It first syncs the bytes that it did not upload yet. If the
  new holder already appended, these go to a `diverge` branch, so no data is
  lost. Then it moves its local copy aside, as in a surrender. Until then, its
  prevention hook blocks a resume (see "Prevention"). The client moves a copy
  aside only if it saw this host hold the lease, the session does not run
  here, and the server has every local byte. Any other host that sees the same
  file (a second host name on a shared config directory, or a copy of a
  transcript) only gets a notice.
- **Recover.** A host can hold a lease and have no local copy, for example a
  runner whose harness config directory did not persist across a restart.
  `hyperspace restore <sid>` works for a lease that the host already holds.
  `hyperspace restore --held` restores each session whose lease the host holds
  and that has no local copy. A runner entrypoint can run it at boot.

The lease records intent: where the session must continue. The conflict rule
(see "Branches and conflicts") records what happened. Leases prevent most
conflicts. A conflict can still happen, for example when a host is offline
during a steal and a user resumes the old copy before the host learns of it.

### Send and receive

A teleport has two commands, one on each host:

- **Send:** `hyperspace teleport <sid> [--to HOST]` runs on the host that
  holds the lease. With `--to`, it grants the session to that host if
  necessary, surrenders the lease reserved for that host, and prints the
  command to run there. Without `--to`, it surrenders the lease as free, so
  that any host that can read the session can take it.
- **Receive:** `hyperspace restore <sid>` runs on the target host, in the
  directory where the session must continue. With `--resume`, it then runs
  `claude --resume <sid>`.

### Restore

`hyperspace restore <sid> [--cwd PATH] [--resume] [--no-tool-results] [--steal] [--force]`
runs on the target host, in the directory where the session must continue:

1. Get the manifest and the lease.
   - If the session is _conflicted_ (see "Branches and conflicts"), stop and
     show the branches; the user must fork one.
   - If a different host holds the lease, stop: "laptop holds the lease. Run
     `hyperspace teleport <sid> --to agent-1` on laptop, or use `--steal` if
     laptop is gone."
   - If the lease is free, reserved for this host, or held by this host,
     continue. Select the active branch of each log and the latest version of
     each blob.
2. The target workspace is the current directory, or `--cwd`. For example,
   `cd ~/my-project && hyperspace restore <sid>` restores the session so that
   it continues in `~/my-project`. Hyperspace does not try to find the "same"
   directory on the new host. If the agent needs files that are not there, it
   can look for them, clone them, or ask.
3. Check the local state. Moved-aside copies are not resumable and stay as
   they are.
   - No resumable local copy: write the files.
   - A resumable copy in the target project directory that is a prefix of the
     active branch: append the missing bytes. The file keeps its inode, and
     its cursor stays valid.
   - Any other resumable copy: stop. A second copy would make
     `claude --resume` fail. `--force` moves the old copy aside.
4. Write the files into the harness layout for the target workspace, with the
   bytes from the server and no changes. For Claude Code, that is the project
   directory of the target workspace, so `claude --resume <sid>` in that
   directory finds the session first.
5. Write cursors for the restored files, so that new appends on this host
   continue the same generation.
6. Write an _arrival record_ in the client state: the teleport id and the
   source and target placements.
7. In one request to the server: take the lease, and record a `teleport` event
   with the teleport id and the placements. If the lease changed after step 1,
   the request fails, and the client removes the files that it wrote.
8. Print `claude --resume <sid>`, or run it if `--resume` is given.

The session id stays the same. A session can teleport any number of times, and
back to a host where it was before.

**Side files.** Resume needs only the main transcript. Restore also writes the
subagent transcripts, plan files, and file-history snapshots, because they are
small. Tool results are larger (17% of the transcript volume on the
workstation in "Facts"). Claude Code does not read them on resume: the
transcript keeps a preview and the absolute path of the full output. The agent
needs a file only if it reads that path again. Restore writes tool results by
default, and `--no-tool-results` skips them. The notice then says that saved
tool outputs from before the teleport are not on this host. The tool-result
paths change only when the harness project directory changes (a different
config directory or workspace). Between two runners with the same paths, they
do not change, and the notice leaves out that line.

### The teleport notice

The plugin's SessionStart hook (synchronous, local state only, no network)
finds the arrival record for the session and prints a notice. Claude Code
records it in the transcript and gives it to the agent. The hook then marks
the arrival record as delivered. Example:

```
[hyperspace] This session was teleported (teleport 01JA6...).
Before: host laptop (workstation), workspace /home/wlritchi/hyperspace,
        last active 2026-10-12 21:40 UTC.
Now:    host agent-1 (runner), workspace /home/agent/workspace.
Earlier messages refer to paths at the old location. Saved tool outputs that
earlier messages cite under
/home/wlritchi/.claude/projects/-home-wlritchi-hyperspace/<sid>/tool-results/
are now under /home/agent/.claude/projects/-home-agent-workspace/<sid>/tool-results/.
Only the conversation moved. Files, uncommitted changes, and running
processes did not move.
```

The notice does not try to map other paths. The agent can look for what it
needs, clone it, or ask.

The notice is also useful for a move that Hyperspace did not make, for example
a `claude --resume` from a different directory. On every SessionStart with
`source: resume`, the hook compares the host and workspace with the last
placement in its local state. If they differ and there is no arrival record,
it prints a shorter notice that says the location changed.

The adapter writes the notice text, because the tool-result paths depend on
the harness layout.

## Branches and conflicts

A session id must name one line of work, because other systems can refer to
it. Hyperspace therefore keeps every branch, but it lets only one branch
continue under the session id.

### Activity

The server does not parse transcripts, so the client classifies each append.
The adapter counts the _activity records_ in the bytes, and the append request
carries the count. Activity is what can change the world or carry new intent:

- a user prompt: a `user` record with a prompt (not `isMeta`, not a tool
  result, not a compaction summary), and
- a new tool call: a `tool_use` block in an `assistant` record. A tool call
  can have side effects.

These are not activity:

- assistant text and thinking with no tool call. It is a stochastic output of
  the shared transcript, and it changes nothing outside it.
- tool results, including results that arrive late for a call from before the
  fork point.
- metadata records (`mode`, `last-prompt`, titles), hook attachments, and the
  teleport notice.

A resume that only opens the session and closes it again therefore adds no
activity. A generation's _activity after offset X_ is the sum of the activity
counts of its segments after X. One activity record is meaningful.

If conflicts from harmless tool calls become a problem, a later version can
classify some tool calls as free of side effects (for example, file reads) and
not count them. v1 counts every tool call.

### Branch causes

Each new generation has a cause:

- `rewrite`: the same host changed its own file (Claude Code removed a failed
  tail, or rewrote the file). The new generation replaces the old tail. The
  old tail never causes a conflict.
- `diverge`: the server refused an append with `409`, because a different
  append reached the same offset first.

### Session states

- **Normal.** At most one branch has meaningful activity after the point where
  it separated from the other branches. That branch is the _active branch_.
  Restore and teleport use it.
- **Conflicted.** Two or more `diverge` branches have meaningful activity
  after their common fork point. The server records the time and the branches.
  A conflicted session id stays conflicted.

For a conflicted session:

- The server refuses a restore or a teleport of the session id (`409` with
  `conflicted` and the list of branches). The manifest is still readable, for
  `hyperspace show`.
- `hyperspace fork <sid> --branch G` asks the server for a new session id (a
  UUID). The server records the new session with `forked_from` (session id,
  generation, offset) and the cause `conflict-recovery`.
- The adapter makes the fork on the client, with no harness process. It has
  all it needs: the bytes of branch G and the new id. It writes the same
  output that Claude Code writes for `--fork-session`: a copy of each record
  with `sessionId` changed to the new id, and a `forkedFrom` marker. All other
  fields stay unchanged, and records with no `sessionId` are copied as they
  are. Side files go into the directories of the new id. The client then
  uploads the fork as the first generation of the new session, and restores it
  in the current directory. A conformance test compares the adapter's output
  with the output of `claude --resume <id> --fork-session` on a sample
  session, so that a change in Claude Code's format shows up as a test
  failure.
- The fork is a new session. The bytes of the original session stay on the
  server without change.
- A session that was already live on a host when the conflict started
  continues there and keeps its backup. It gets a notice that the session is
  conflicted and that a resume elsewhere needs a fork. The backup never
  refuses data.

A recovery fork is not a normal event. The session gets a new id, so
references to the old id in other systems (for example, registrations and
undelivered messages in a messaging system) do not follow it automatically.
The server records a `forked` event that other systems can read; what they do
with it is their decision.

### Prevention

Most conflicts start when a host resumes an old local copy of a session that
moved. There are two protections:

1. **No resumable copy.** A surrender moves the local transcript aside, and a
   host that learns of a lost lease does the same. After a normal teleport,
   `claude --resume` on the old host does not find the session.
2. **The prevention hook**, for the cases that the first protection misses (a
   steal while the old host was off, or a resume before the old host learned
   of the steal):
   - Each append response contains the lease holder and the session state. The
     client saves them.
   - When a session starts with `source: resume`, the async SessionStart hook
     also asks the server for the current state.
   - The synchronous UserPromptSubmit hook, at the first prompt after a
     resume, checks the saved state. If a different host holds the lease, or
     the session is conflicted, it blocks the prompt with a message: "agent-1
     holds the lease of this session since 21:52 UTC. Bring it here with
     `hyperspace teleport <sid> --to laptop` on agent-1 and
     `hyperspace restore <sid>` here, or use `hyperspace fork` to continue
     this copy under a new id." If the saved state is older than the resume,
     the hook asks the server, with a timeout of 2 s.
   - If the server cannot be reached, the hook permits the prompt. The server
     detects any conflict that results.

The block applies only to the first prompt after a resume, and only when the
check finds a problem. It does not stop a session that was live when the other
host took over.

`hyperspace fork` also works on a session that is not conflicted. It gives a
copy of any branch a new id, so it is a way to continue an old copy without a
conflict.

## Architecture

```
 host A (workstation)                               host B (runner)
 ┌──────────────────────────────┐                   ┌──────────────────────────────┐
 │ Claude Code                  │                   │ hyperspace restore <sid>     │
 │  hooks ──> hyperspace sync ──┼──── HTTPS ────┐   │ claude --resume <sid>        │
 │  (plugin)   (client package) │               │   │  SessionStart: notice        │
 └──────────────────────────────┘               ▼   └──────────────▲───────────────┘
                                        ┌───────────────────┐      │
                                        │ hyperspace-server │──────┘
                                        │ SQLite index      │
                                        │ zstd frame logs   │
                                        │ blob objects      │
                                        └───────────────────┘
```

The hyperspace repo holds four parts.

| Part     | Location                      | Form                                                                                               |
| -------- | ----------------------------- | -------------------------------------------------------------------------------------------------- |
| Server   | `server/hyperspace-server.py` | PEP 723 script with a lock, container image                                                        |
| Protocol | `protocol/`                   | `hyperspace-protocol` package: `TypedDict` bodies, header names, limits. Tags `protocol-vX.Y.Z`    |
| Client   | `client/`                     | `hyperspace-client` package: sync engine, adapters, and the `hyperspace` CLI. Tags `client-vX.Y.Z` |
| Plugin   | `plugin/hyperspace/`          | Hooks that forward to `hyperspace hook`                                                            |

The client is a package with a CLI, not code in the plugin, for these reasons:

- A teleport must work on a host before Claude Code starts. The user installs
  the CLI with `uv tool install` and needs no plugin.
- Adapters for other harnesses use the same engine without the Claude Code
  plugin. They can use their own hooks, or a sweep from a systemd timer.
- The protocol, the client, and the server change together and are tested
  together.

The plugin does not depend on a client version: it forwards each hook to the
installed `hyperspace` CLI (see "Plugin").

## Server

### Stack

- Python 3.12. The server is a PEP 723 script with a committed
  `hyperspace-server.py.lock`. Shared modules are in `lib/`.
- HTTP with Starlette and uvicorn. Uploads are request and response, so
  WebSocket gives no advantage.
- `zstandard` for compression.
- SQLite from the standard library, in WAL mode, for the index. The server
  must handle thousands of sessions, range queries, and list filters.

### Storage layout

```
$HYPERSPACE_DATA_DIR/
  index.sqlite3            hosts, workspaces, sessions, placements, files,
                           generations, segments, metadata events, grants,
                           read audit, purges
  logs/<gen id>.zst        one file per generation: concatenated zstd frames,
                           one frame per segment
  objects/ab/<sha256>.zst  blob versions, content-addressed
  accounts.toml            credentials (sha256 of the secret only)
```

- The zstd format permits concatenated frames. A generation file is therefore
  append-only on disk too, and a reader decompresses it as one stream.
- For each segment the index stores the frame's offset and length in the
  `.zst` file, so the server can serve a byte range without decompressing the
  whole generation.
- Write order: append the frame, `fsync`, then commit the index row. At
  startup, the server truncates each generation file to the end of its last
  committed frame. This removes only bytes that no index row refers to.
- Content-addressed objects deduplicate identical tool results and snapshots.
- The data directory only grows, so backups of it are cheap and incremental.
- The data is readable on the server (compressed, not encrypted). Protection
  at rest comes from the volume and from access control on the cluster.

### HTTP API (v1)

All routes are under `/v1` and need `Authorization: Bearer <credential>`. The
one exception is `GET /healthz`, for probes. It needs no credential and
returns only `ok`, with no version or other details. Request bodies of file
data use `Content-Encoding: zstd`. One request carries at most 8 MiB of
uncompressed data. The client splits larger appends.

| Method and path                                               | Scope            | Purpose                                                                                                                                                     |
| ------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /whoami`                                                 | any              | Account, host, and scopes of the credential                                                                                                                 |
| `PUT /hosts/self`                                             | `append`         | Register or update the host's kind and attributes                                                                                                           |
| `GET /hosts`, `GET /workspaces`                               | `read`           | List hosts and workspaces                                                                                                                                   |
| `GET /sessions?harness=&host=&workspace=&since=&title=`       | `read`           | List readable sessions with their latest metadata and placement                                                                                             |
| `GET /sessions/{harness}/{sid}`                               | `read`           | Manifest: metadata, placements, files, generation tree, heads, blob versions                                                                                |
| `POST /sessions/{harness}/{sid}/meta`                         | `append`         | Add a metadata event                                                                                                                                        |
| `GET /sessions/{harness}/{sid}/state`                         | `read`           | Lease holder, state (`normal` or `conflicted`), and branches. Small, for hooks                                                                              |
| `POST /sessions/{harness}/{sid}/lease/surrender`              | `append`         | Surrender the caller's lease, optionally reserved for a named host. `409` if the caller does not hold it                                                    |
| `POST /sessions/{harness}/{sid}/teleports`                    | `read`, `append` | Take the lease for the caller's host and record a teleport. `409` if the session is conflicted, or a different host holds the lease (unless `steal` is set) |
| `POST /sessions/{harness}/{sid}/forks`                        | `read`, `append` | Fork a branch; the server returns the new session id                                                                                                        |
| `POST /sessions/{harness}/{sid}/generations`                  | `append`         | Create a generation of a log (`path`, `parent`, `fork_offset`, `cause`)                                                                                     |
| `POST /sessions/{harness}/{sid}/generations/{gen}/append`     | `append`         | Append (see "Append protocol")                                                                                                                              |
| `GET /sessions/{harness}/{sid}/generations/{gen}?start=&end=` | `read`           | Read the content, including the inherited prefix                                                                                                            |
| `PUT /sessions/{harness}/{sid}/blobs/{path}`                  | `append`         | Add a blob version (sha256 in a header; no-op if it is the latest version)                                                                                  |
| `GET /sessions/{harness}/{sid}/blobs/{path}?version=`         | `read`           | Read a blob version                                                                                                                                         |
| `POST /sessions/{harness}/{sid}/grants`                       | `grant`          | Let another host read this session                                                                                                                          |

### Compatible protocol extensions

`GET /whoami` includes a `capabilities` list. This release advertises
`workspace-utf8`:

- `Hyperspace-Workspace-Utf8` carries standard base64 of the UTF-8 workspace.
  It takes precedence over `Hyperspace-Workspace`. The server still accepts
  the old raw header. New clients use the encoded header when supported; with
  an older server they use the raw header only for printable ASCII paths, and
  otherwise report that a server upgrade is required.

Upgrade the server before the client. A subsequent release will remove support
for old clients: the raw workspace header will no longer be accepted. This
release retains it.

Known metadata fields are type-checked before storage. Unknown fields remain
available for extensions. Readers ignore malformed known fields in events
written by older servers; the original events remain unchanged.

There is no delete route and no admin route. Admin operations are scripts that
run inside the server container, so they need shell access to the pod
(`kubectl exec`), which needs admin access to the cluster:

- `scripts/new-credential.py`: create a credential for a host, with scopes and
  an optional expiry. It prints the secret once.
- `scripts/revoke-credential.py`.
- `scripts/purge.py`: remove a session, a generation, or a blob version in an
  emergency (for example, a secret in a transcript). It records each purge in
  the index.
- `scripts/stats.py`: sizes per host, per workspace, and per harness.

### Configuration

| Variable                                   | Default                      |
| ------------------------------------------ | ---------------------------- |
| `HYPERSPACE_DATA_DIR`                      | `$XDG_DATA_HOME/hyperspace`  |
| `HYPERSPACE_LISTEN_HOST`                   | `127.0.0.1`                  |
| `HYPERSPACE_PORT`                          | `8139`                       |
| `HYPERSPACE_MAX_BODY`                      | 8 MiB (also the maximum)     |
| `HYPERSPACE_TRUSTED_PROXIES`               | none (comma-separated CIDRs) |
| `HYPERSPACE_RATE_PER_MINUTE`               | 1200 requests per credential |
| `HYPERSPACE_BYTES_PER_DAY`                 | 4 GiB per credential         |
| `HYPERSPACE_FAILED_AUTH_PER_MINUTE`        | 30 per address (IPv6: /64)   |
| `HYPERSPACE_GLOBAL_FAILED_AUTH_PER_MINUTE` | 1000 from all addresses      |

`HYPERSPACE_TRUSTED_PROXIES` lists the proxies whose `X-Forwarded-For` the
server trusts. Behind a proxy that is not in the list, all clients have the
proxy's address, so they share one failed-auth limit; the server logs a
warning when it sees this. Over a failed-auth limit, the server checks only
credentials that authenticated from that address in the last day. The limits
are in memory and reset when the server restarts.

## Security

The first deployment is on a private network. The design assumes that the
server is on the open internet from the first release.

### Threats

1. A network attacker with no credential.
2. A stolen credential.
3. A compromised or prompt-injected agent on a host. Every agent on a host can
   read that host's credential. Runners often have no human approval of tool
   calls, so this is the most likely threat.
4. A host that sends hostile data (very large or malformed requests).

### Controls

- **Transport.** The client accepts only `https://` URLs, except for loopback
  addresses (for tests and local use). The client has no default URL; the user
  must configure it.
- **Credentials.** One credential per host, `hsp_<id>_<secret>`, with 256 bits
  of secret. The server stores only the sha256 of the secret, and compares
  with a constant-time function. Credentials can expire, and the server
  reloads `accounts.toml` when it changes, so a revoked credential stops at
  once. There is no master token on the network.
- **Scopes.** Each credential has scopes:
  - `append`: back up sessions as its own host. A credential cannot write as a
    different host, and the protocol cannot change or remove data. A stolen
    `append` credential can add data, but it cannot destroy history.
  - `read: own | granted | all`: `own` permits reads of sessions with a
    placement on this host. `granted` also permits sessions with a grant to
    this host. `all` permits every session in the account.
  - `grant`: permit another host to read one session. A credential can grant
    only a session that it can read, so `grant` never widens the reads of the
    credential itself.

  The defaults: workstations get `append`, `read: all` and `grant`; runners
  get `append`, `read: granted` and `grant`. A prompt-injected agent on a
  runner can then read only its own sessions and the sessions that were
  granted to it, not the history of every project. It can pass those sessions
  on to other hosts, so that agents can teleport between runners. To teleport
  a session to a runner, the lease holder runs
  `hyperspace teleport <sid> --to agent-1`, which also grants the session to
  agent-1. `hyperspace grant <sid> --to agent-1` gives read access without a
  move of the lease.

- **No existence oracle.** A session that the credential cannot read gives the
  same `404` as a session that does not exist.
- **Limits.** A body limit, a limit on the decompressed size (protection from
  zstd bombs), strict validation of session ids and file paths (no `..`, no
  absolute paths, no empty segments, bounded length), per-credential storage
  quotas and request rates, and a rate limit on failed authentication per
  client address.
- **Small attack surface.** The server never parses transcript content. It has
  no browser UI, no CORS, and no admin API.
- **Audit.** The server records each read of session content (credential,
  host, session, time) in the index. `scripts/stats.py` can show it.
- **Secrets on hosts.** On workstations, the token is in the system keyring,
  and on Linux also in a cache in kernel memory (see "Credentials"). A file
  with mode 0600 also works. On workstations, the token is not in the
  environment of the harness or of the agent's commands. On runners, the token
  comes from a Kubernetes Secret, through the environment. The cwd exclusion
  list keeps chosen projects off the server.

## Client

### Configuration

`$XDG_CONFIG_HOME/hyperspace/config.toml`, with environment overrides:

```toml
url = "https://hyperspace.example.com"    # required; no default
token_file = "~/.config/hyperspace/token"  # optional; or HYPERSPACE_TOKEN
token_cache_ttl = 86400                    # seconds with no read before the cache drops the token
host = "laptop"                            # default: system hostname; required on runners
host_kind = "workstation"                  # or "runner"
exclude_cwd = ["/tmp/**"]
```

If the URL or the token is missing, `hyperspace` commands stop with a clear
error, and the hooks do nothing and write one line to the log.

### Credentials

The client looks for the token in this order:

1. `HYPERSPACE_TOKEN` (runners).
2. The token file: `HYPERSPACE_TOKEN_FILE`, `token_file`, or
   `$XDG_CONFIG_HOME/hyperspace/token`.
3. The token cache: on Linux, a key in the kernel's user keyring. It is in
   memory only, and goes away at reboot or after `token_cache_ttl` seconds
   with no read. Each read restarts the time.
4. The system keyring (the `keyring` package, with the user's default
   backend), with the server's host and port as the user name. A read fills
   the cache.

`hyperspace login` checks a token with the server, then stores it in the
keyring and the cache. `hyperspace logout` removes it from both.

A keyring can ask for a passphrase, for example `pass` with a GPG key. A
prompt during a turn interrupts the user, and many hooks fire in each turn. So
the client reads the keyring only at these times:

- Commands that the user runs: if needed.
- Hooks: never, except at a session start or resume. There, the checkpoint and
  the sweep that the SessionStart hook starts can read it. The hook itself
  does not wait for the prompt.
- One read at a time: a process reads the keyring with a lock held, and the
  others wait and then use the cache. So many sessions that resume after a
  reboot give one prompt.
- After a failed or dismissed read, hooks do not read the keyring again for 10
  minutes. A command that the user runs reads it again.

Other hooks use only the cache. If the cache is empty, they do nothing and
write one line to the log; the next session start or resume, or any
`hyperspace` command, fills it.

On macOS, the default keyring is the login Keychain, which is unlocked while
the user is logged in. There is no cache, and all hooks read the Keychain
directly, but only when it is unlocked, so that a hook never shows the unlock
dialog.

### Sync engine

- `hyperspace sync --transcript <path>` syncs one session.
  `hyperspace sync --all` scans every adapter's sessions. The first `--all`
  run is the backfill. An error for one session (for example a conflict) goes
  to the log, and the scan continues. An error that applies to the credential
  (`unauthorized`, `forbidden`, or `rate_limited`, which includes the daily
  byte quota) stops the scan, and the command exits with an error.
- State is in `$XDG_STATE_HOME/hyperspace/state.sqlite3`: cursors for each
  local file, the last known placement of each session, arrival records, and
  conflict and move notices. Log cursors and blob caches are scoped to the
  server and credential identity. Legacy log cursors without that identity are
  not reused; the next sync compares and adopts the remote content.
- If the server cannot be reached, the client keeps its cursors and exits with
  no error. The local files are the queue, so the next sync uploads the
  difference.

### Checkpoint scheduling

Hooks fire often during a turn: before and after each tool call, also inside
subagents. A sync for each event would be too many requests, and two sync
processes for one session would compete. The client therefore uses a
_single-flight sync with a trailing run_ for each session:

1. The hook writes a dirty mark for the session (the transcript path and the
   time) and starts `hyperspace sync --checkpoint <sid>` as a detached
   process.
2. The sync process tries to get the session's lock without waiting. If
   another process holds it, it exits at once.
3. With the lock, it clears the dirty mark and syncs.
4. When the sync ends, it releases the lock and checks the dirty mark again.
   If the mark is set, it waits until at least `checkpoint_interval` (default
   10 s) has passed since its last sync started, and goes back to step 2.

The result: during a busy turn, a session uploads at most once each 10
seconds, and the last change is always uploaded. A crash loses at most about
10 seconds of transcript. During one long tool call, no new records come from
the main transcript, so nothing is lost there.

### Adapter interface

```python
class Adapter(Protocol):
    harness: str
    def discover(self) -> Iterator[LocalSession]: ...
    def session_for_transcript(self, path: Path) -> LocalSession: ...
    def files(self, session: LocalSession) -> list[LocalFile]: ...  # rel path, kind, local path
    def commit_boundary(self, rel_path: str, data: bytes) -> int: ...  # bytes safe to upload
    def activity(self, rel_path: str, data: bytes) -> int: ...  # activity records in data
    def metadata(self, session: LocalSession) -> SessionMeta: ...
    def restore_plan(self, manifest: Manifest, target: Placement) -> list[RestoreFile]: ...
    def arrival_notice(self, arrival: Arrival) -> str: ...
    def fork(self, branch: bytes, new_id: str) -> bytes: ...  # copy under a new id
    def is_running(self, session: LocalSession) -> bool: ...  # live-process check
    def move_aside(self, session: LocalSession) -> None: ...  # make it not resumable
```

The Claude Code adapter maps local paths to session file names like this:

| Local path                                      | Session file       | Kind |
| ----------------------------------------------- | ------------------ | ---- |
| `projects/<enc>/<sid>.jsonl`                    | `transcript.jsonl` | log  |
| `projects/<enc>/<sid>/subagents/**/*.jsonl`     | `subagents/...`    | log  |
| `projects/<enc>/<sid>/subagents/**/*.meta.json` | `subagents/...`    | blob |
| `projects/<enc>/<sid>/tool-results/**`          | `tool-results/...` | blob |
| `projects/<enc>/<sid>/workflows/**`             | `workflows/...`    | blob |
| `file-history/<sid>/*`                          | `file-history/...` | blob |
| `plans/<slug>.md` (slug from the transcript)    | `plans/<slug>.md`  | blob |

It skips `.orphaned-*`, `.superseded-*`, and `*.bak` files, and the legacy
top-level `agent-*.jsonl` files.

## Plugin

The plugin is in this repo (`plugin/hyperspace/`). A marketplace (for example
wlr-cc-plugins) lists it with a `git-subdir` source; the entry's `ref` selects
the version. `plugin.json` has no version, so Claude Code uses the commit, and
a release never edits the plugin.

The plugin holds no logic. For every event it runs `hooks/forward.sh`, which
runs `hyperspace hook` (or `$HYPERSPACE_CLI hook`) with the hook's JSON on
stdin, or does nothing if the CLI is not installed. The client installs and
upgrades separately (`uv tool install`, or the runner image). The only
contract between the plugin and the client is `hyperspace hook` with the
hook's JSON on stdin; the client ignores events that it does not know, so a
newer or older plugin works with any client.

`hyperspace hook` never fails and prints nothing on an error; errors go to
`$XDG_STATE_HOME/hyperspace/log`. Checkpoints and sweeps run in a detached
process, so the hook returns at once and the work outlives the session.

| Event                                                                                    | Hook  | Action                                                                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SessionStart                                                                             | sync  | Print the teleport, move, or conflict notice, if any (local state only). Start a checkpoint, and `sync --all` if the last sweep was more than one hour ago. At a start or resume, these can read the keyring (see "Credentials").                        |
| UserPromptSubmit                                                                         | sync  | Start a checkpoint. At the first prompt after a resume: block the prompt if the session moved or is conflicted (see "Prevention"). Otherwise print a new notice, if any. Network only when the saved state is older than the resume, with a 2 s timeout. |
| PreToolUse, PostToolUse, PostToolUseFailure, SubagentStop, PreCompact, PostCompact, Stop | async | Start a checkpoint. PreToolUse records the tool call before a long tool runs.                                                                                                                                                                            |
| SessionEnd                                                                               | async | Start a checkpoint with no wait for the interval.                                                                                                                                                                                                        |

The hooks use `transcript_path` from the hook input, so they follow a
transcript that Claude Code moved.

v1 has no skill. The CLI is the interface for teleports (see "Later: teleport
skills").

On container runners, the image includes the plugin and the client. The
Deployment gives the client its configuration: `HYPERSPACE_URL`,
`HYPERSPACE_TOKEN` from a Secret, `HYPERSPACE_HOST` (the runner's stable
name), and `HYPERSPACE_HOST_KIND=runner`. If the runner's harness config
directory does not persist, its entrypoint runs `hyperspace restore --held` at
boot (see "Leases").

## Later: agent-initiated teleport

This is not in v1. The aim is a Teleport tool that an agent can call. The
session suspends when its other tool calls return, the client uploads the
transcript, and a host somewhere else resumes the session. v1 must not make
this harder. These v1 parts are the base for it: stable session ids,
`teleport` events, placements, `grant` on runners, the arrival notice, and
conflict prevention.

It will also need:

- **A teleport lifecycle on the server**: `requested`, `accepted`, `arrived`,
  `refused`, `failed`, `returned`. While a teleport is in progress, the
  session is _in transit_: the source surrendered the lease, and it is
  reserved for the target. The lease is the base for this state.
- **A launcher on each host**: a long-running process that receives teleport
  requests and runs the harness resume. On a container runner, this can be the
  image's entrypoint. Subspace already pushes events to hosts and could carry
  the requests.
- **A watchdog**: if the target is offline or refuses, or does not reach
  `arrived` and a first checkpoint in time, or hangs or crashes after it
  arrives, the server marks the teleport `failed` and reserves the lease for
  the source again. The source launcher then restores the session from the
  last good point before the teleport and resumes it ("if you die in the
  matrix, you wake up in real life"). The server marks the target's branch as
  `abandoned`, and abandoned branches never make a conflict.
- **A permission model**: which agents can teleport, to which hosts, and
  whether the user must approve. A host can also have a policy for the
  teleports it accepts.
- **The suspend mechanism** in the harness: how the tool call ends the session
  cleanly after the final checkpoint.

## Later: teleport skills

v1 has no teleport skill. Later, there can be a skill for each side:

- **Send:** a skill that uses the hooks to do a final sync, winds the session
  down cleanly, and then runs `hyperspace teleport`.
- **Receive:** a skill that restores a session and starts it as a new
  background job, instead of asking the user to run `claude --resume`.

It is not decided whether one or both are useful. The send skill overlaps with
agent-initiated teleport.

## Later: ephemeral hosts

v1 binds each credential to one host with a stable name and a persistent
workspace. Some places cannot keep a workspace, for example a CI job or a pod
with only temporary storage. They need a different model:

- A credential that can create short-lived host identities, or one host
  identity for each run.
- No lease held for a long time: an ephemeral host surrenders its leases when
  it stops, or the server frees them when the host is gone.
- Restore into a new, empty workspace, and teleport out before the place goes
  away.

## Distribution

- `Dockerfile`: a uv build stage that installs from the lock files into
  `/opt/venv`, then a `python:3.12-slim-bookworm` runtime with `tini`, uid
  10001, a `/data` volume, and port 8139.
- Repository workflows:
  - Build: compute the semver from Conventional Commits, push an `X.Y.Z` tag,
    build the image and push it to a registry, and push the chart.
  - Protocol and client: tag `protocol-v*` and `client-v*` when those
    directories change.
- A test workflow, because the append protocol has many edge cases.
- A Helm chart (see "Helm chart"). A systemd `--user` unit is for local use.
- Client install:
  `uv tool install 'hyperspace-client @ git+https://github.com/wlritchi/hyperspace@client-vX.Y.Z#subdirectory=client'`.

### Helm chart

The repo ships a chart in `charts/hyperspace/`. The release workflow packages
it and pushes it to the image registry as an OCI artifact, with the chart
`version` and `appVersion` equal to the release version.

The chart contains:

- A Deployment with exactly one replica and the `Recreate` strategy. The
  SQLite index and the frame logs have one writer, so the chart does not offer
  a replica count.
- A PersistentVolumeClaim for `/data` (size, storage class, or an existing
  claim).
- A Service on port 8139.
- An optional Ingress (class, host, TLS secret, annotations). The default is
  no Ingress.
- A restricted security context: uid and gid 10001, `fsGroup` 10001, a
  read-only root file system, no added capabilities, and no privilege
  escalation.
- Liveness and readiness probes on `GET /healthz`.
- Values for the server configuration (`trustedProxies`, `maxBody`, quotas and
  rates) and `extraEnv`.

The chart creates no credentials and holds no secrets. After an install, its
`NOTES.txt` shows the `kubectl exec` command that runs
`scripts/new-credential.py`.

Runners are other workloads, so the chart does not configure them. The docs
describe the runner integration: the plugin and the client in the runner
image, a Secret with the runner's credential, the environment variables from
"Plugin", and the boot rule for leases (see "Leases").

CI runs `helm lint`, and checks the rendered manifests (`helm template`) with
a schema validator.

## Decisions

- Data on the server is readable by the server (not end-to-end encrypted).
- The client is a package in this repo, with the `hyperspace` CLI.
- A teleport keeps the session id. Session ids are long-term identifiers.
- Deletion is only by `scripts/purge.py`, inside the server container.
- The first deployment is private. The security model assumes a public server.
  The client has no default URL.
- Hyperspace does not rewrite transcript content. A teleport adds a notice
  through a SessionStart hook.
- A workspace is the directory where a session started, resumed, or was
  restored. There is no workspace configuration, and Hyperspace does not
  assume one session for each repo.
- Restore uses the current directory (or `--cwd`) as the target workspace. The
  teleport notice says where the session was and where it is now, and where
  the tool results are. It does not map other paths; the agent can look,
  clone, or ask.
- Restore writes tool results by default.
- Activity is a user prompt or a new tool call. Assistant text alone, tool
  results, and metadata are not activity. One activity record is meaningful.
- Each session has an advisory lease. A teleport surrenders the lease on the
  source and moves the local transcript aside, so that it cannot be resumed
  there by accident.
- Runners get `append`, `read: granted` and `grant`. Workstations get
  `append`, `read: all` and `grant`.
- When two branches of a session have meaningful activity, the session id is
  conflicted. The server refuses a restore or teleport of it, and permits a
  fork of any branch to a new id that the server selects. The adapter makes
  the fork, not the harness.
- `hyperspace teleport` is the send side and `hyperspace restore` is the
  receive side.
- Runners need a persistent workspace. A persistent harness config directory
  is optional.
- Workstations keep the token in the system keyring, with a cache in kernel
  memory on Linux. Hooks read the keyring only at a session start or resume,
  so a passphrase prompt comes at most once per session start.

## Open questions

None for v1. The open design questions for later work are in the "Later"
sections.
