# Hyperspace: session status and liveness

Status: draft. Date: 2026-10-15. This spec extends `2026-10-12-hyperspace.md`
(the main spec). Terms such as host, workspace, lease, placement and activity
have the meanings that the main spec gives. The facts about Claude Code in
this spec come from Claude Code 2.1.301.

## Problem

Hyperspace knows where a session lives (the lease) and what it said (the
transcript). It does not know where the work of the session stands. This
causes three problems:

1. On a workstation, the user cannot get one list of the sessions that still
   need them. `claude agents` shows only background jobs, not foreground
   sessions. Its `Done` state means that the last turn delivered a result. It
   does not mean that the work is finished, so many `Done` jobs still wait for
   the user. An agent cannot mark its own work as finished. The user can do it
   only with `claude rm`, one job at a time.
2. A runner resumes one session at boot: the newest background job in its
   workspace. A runner can have zero sessions that need a process, or more
   than one. When the runner starts a new session, the user can no longer see
   the old sessions on that runner.
3. An agent cannot collect a summary of the outstanding work across hosts, so
   that the user can set priorities. The user does not remember the state of
   old sessions, for example sessions on runners that were replaced.

## Goals

- A _status_ for each session: `working`, `waiting`, `ready` or `done`, set by
  hooks, by the agent, and by the user, from any host that can read the
  session.
- A _liveness_ for each session: whether a process for it runs now, and on
  which host. Liveness is observed, not declared.
- A list of open sessions (not `done`) across all hosts, for the user and for
  agents.
- Runners that resume the sessions that need a process, and leave the other
  sessions stopped until something wakes them.
- A clear answer to a message that is sent to a `done` session.
- A backfill: each older session decides its own status.

## Non-goals

- Scale-to-zero runners, and an operator that starts and stops runners. This
  spec keeps them possible (see "Later").
- Priorities, deadlines, or dependencies between sessions. An overview agent
  can propose priorities from the status and the transcript.
- Changes to Claude Code's own job states. Hyperspace uses `claude rm` to
  remove a finished job; it does not change how `claude agents` shows jobs.
- Status inference from transcript content on the server. The server never
  parses transcripts. Only the client adapter and the agent decide the status.

## Stories

Other sections refer to these stories by id.

### The user, on a workstation

- **U1. One list of open work.** I run `hyperspace ls --open` and see every
  session that is not `done`, on every host, grouped by status. The list
  includes foreground sessions and sessions with no process.
- **U2. What needs me.** Sessions that are `ready` come first. Each one shows
  why: the agent's note, or an excerpt of its last message.
- **U3. Mark done from inside.** In a session, I type `/done [note]`. The
  status becomes `done`. The UI tells me so and does not close the session.
  When I close the UI, the session stops as usual. If I detach from a
  background job instead, the job closes when it is no longer in use and
  leaves the `claude agents` view.
- **U4. Mark done from outside.** I run `hyperspace done <sid>...` for one or
  many sessions on any host. A live session gets a notice, finishes its
  current turn, and closes when it is no longer in use. An automatic status
  write from the session cannot replace my mark.
- **U5. Reopen.** A `done` session reopens at its first new activity: my
  prompt, a cron that fires, or an explicit reopen. To open a `done` session
  only to read it does not reopen it.
- **U6. Dead and dark sessions.** A session whose process stopped during a
  turn shows as _interrupted_. A session whose host stopped reporting shows as
  _unknown since T_. A session whose process runs but makes no progress shows
  as _stalled_. A session never stays in `working` after its process stops or
  its host goes silent.

### An agent in a session

- **A1. Mark my work done.** When the work is finished, I mark the session
  `done` with a one-line outcome. The session closes after my turn ends, so my
  last message is written and backed up.
- **A2. Say what I wait for.** When I end a turn, I can declare that I wait
  for an event (a CI run, a PR review, a message), with a note. If I declare
  nothing, the end of my turn means that I wait for the user.
- **A3. Learn of a mark from outside.** If the user marks my session `done`
  while I work, I get a notice that says who marked it and from where, so that
  I can stop at a clean point.

### An agent that sends a message to a session

- **M1. Bounce.** When I send a message to a `done` session, I get a notice
  that it was not delivered, with the session's outcome note. I do not get
  silence.
- **M2. Reopen with a reason.** To deliver a message to a `done` session, I
  reopen it with a reason. It wakes with my message and the reason.
- **M3. Queued.** If nothing can wake the session now, I get a notice that my
  message is queued and not yet delivered.

### A runner

- **R1. Resume what needs a process.** At boot, the runner resumes each
  session whose lease it holds and that needs a process: interrupted sessions,
  and sessions that wait for an event that only a running process can receive
  (a cron, a Monitor, a background task). Zero such sessions is valid, and so
  is more than one.
- **R2. Wake on demand.** The runner does not keep a session running when it
  waits for the user or for a message. The user, or a message, wakes it.
- **R3. Respect done.** The runner never resumes a `done` session, also when
  it was marked `done` from a different host.

### An agent that makes an overview

- **O1. One call.** One command returns every open session with its status,
  note, liveness, host, workspace, title, last activity, and an excerpt of its
  last message. This is enough to sort sessions without reading their
  transcripts.
- **O2. Drill down.** One command prints the last turns of one session, so
  that I can answer "what was this waiting for?".
- **O3. Act.** After the user approves, I mark sessions `done`, or send them a
  message.

### Backfill

- **B1. Each session decides.** For sessions from before this feature, the
  user sends each session in a time window a message that explains the status
  system and asks it to set its own status. A throttle limits the rate. Only
  the agent can tell "the work is finished" from "I gave a plan that the user
  did not execute", so the backfill does not guess.

## Model

### Status

The status records where the work of a session stands. Each status can have a
_note_: one line from the agent or the user, for example "waiting for CI on PR
#41" or "need a decision on the state backend". For `done`, the note is the
outcome.

| Status    | Meaning                                            | What wakes it                                                                                                      |
| --------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `working` | A turn is in progress.                             | (It is awake.)                                                                                                     |
| `waiting` | The turn ended. The agent expects an event.        | `wake: local`: a cron, Monitor or background task in the process. `wake: message`: a message or an external event. |
| `ready`   | The turn ended. The agent needs the user.          | Any user interaction, including a resume.                                                                          |
| `done`    | The user, the agent or the client closed the work. | A user prompt, a cron that fires, or an explicit reopen. Not a plain message.                                      |

`ready` has a _reason_:

- `asked`: the agent asked a question (AskUserQuestion, or a permission
  prompt).
- `idle`: the agent ended its turn and did not mark the session `done`.

`waiting` has a _wake_ kind. The wake kind decides whether a runner must keep
or resume the process:

- `local`: only the running process can receive the event. If the process
  stops, the event is lost, so a runner must keep the session running, or
  resume it at boot.
- `message`: something outside the process delivers the event (subspace, a PR
  subscription). The process can stop, and a launcher resumes it when the
  event comes.

A session with no status event has no status. Lists show it as `unknown`. The
backfill (B1) gives it a status.

### Status events

The server stores the status as an append-only list of _status events_ for
each session. Each event has:

- a sequence number `seq`, which starts at 1 for each session and increases by
  1 for each event;
- the status, the reason or wake kind, and the note;
- an optional `excerpt`: the start of the agent's last message, at most 1,000
  characters, made by the adapter on the client;
- the _source_: the kind (`user`, `agent` or `hook`), the host, and the
  session id when an agent in a different session wrote the event;
- `explicit`: true for an intent of the user or an agent (a mark, a reopen, a
  declaration), false for a transition that a hook or the client infers;
- the receive time.

The latest event is the session's current status.

### Conditional writes

Each status write carries `base_seq`: the `seq` of the latest event that the
writer saw. The server accepts the write only if `base_seq` is the current
`seq`. Otherwise it returns `409 stale_status` with the current status. The
writer reads the current status and decides again.

A sequence number, not the status value, is the base. A status value can
change and come back (`done`, then `ready`, then `done`). A writer that saw
only the first `done` must not be able to write over the second one.

Example: the user marks a session `done` from a different host while the
session works. The next automatic write from the session (`working` or
`ready`) fails with `409`. The client reads the `done` event and its source,
and tells the agent (A3). The work does not overwrite the mark (U4).

A new write from the client replaces a queued write that is not sent yet (see
"Transitions"). A sync can send the queued write while a hook decides the new
write on top of it, so the new write can have the base from before the queued
write. Thus the new write also tries again once on the event of a write that
it replaced, and only on that event. The event must be from this host, with
the same status, reason, wake kind, note, source kind and `explicit`. A field
match alone is not sufficient, because a later write can have the same fields.
Thus the event must also be the event that the replaced write became:

- If the client knows the outcome of the send of the replaced write, the event
  must have the `seq` of that outcome.
- Else (a send that failed, or a write that a hook replaced), the event must
  be the event just after the base of the new write.

The client also keeps each write that a sync sent, also after a send that
failed, until the queue is empty. Thus many new writes during one send, and a
failed send of the newest write, do not lose it. A rare case stays: a send
whose outcome is not known can try again after a 409 and land later than the
event just after the base. The new automatic write then gets a 409 on it and
is dropped, and the status stays as it is until the next status write. An
automatic write still does not overwrite a `done` or an explicit status from a
different writer.

### Transitions

The adapter maps harness events to status writes. For Claude Code:

| Event                                                                       | Write                                                                                       |
| --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| UserPromptSubmit                                                            | `working`. If the current status is `done`, this write reopens the session and is explicit. |
| A cron or other trigger starts a turn with no prompt                        | As UserPromptSubmit.                                                                        |
| PreToolUse, with no known status and no declaration in this turn            | `working`. For example, the first turn of a new background job (see below).                 |
| PreToolUse for AskUserQuestion, or a Notification for a permission prompt   | `ready`, reason `asked`.                                                                    |
| PostToolUse for AskUserQuestion, or the tool call after a permission prompt | `working`.                                                                                  |
| Stop, with an explicit status from the agent in this turn                   | No write. The agent's declaration stays (see below).                                        |
| Stop, with running tasks in the job's `inFlight` (see below)                | `waiting`, wake `local`.                                                                    |
| Stop, otherwise                                                             | `ready`, reason `idle`.                                                                     |
| SessionStart with `source: resume`                                          | No write. A resume alone does not reopen a `done` session (U5).                             |
| SessionEnd                                                                  | No write. Liveness records that the process stopped.                                        |

A question or a permission prompt after a declaration still writes `ready`,
reason `asked`, because the session needs the user while the prompt shows. The
tool call after it writes `working`. If such writes replaced the declaration,
the Stop hook writes the declaration again, as an explicit write.

The status writes go through the checkpoint sync of the session (main spec,
"Checkpoint scheduling"). The hook records the wanted status in local state,
and the sync process sends it while it holds the session's sync lock. Thus
status writes for one session are in order, and only the latest wanted status
goes to the server after an outage.

Claude Code writes `inFlight` only for background jobs, in
`~/.claude/jobs/<short>/state.json`. Its `tasks` count includes background
shells, background subagents, Monitors and crons. Its documentation says that
this file is not a stable interface. The adapter therefore reads it as a hint.
If the file or the field is missing, the adapter writes `ready`, reason
`idle`. A foreground session has no `inFlight`, so only an agent declaration
can set `waiting` there (A2).

When the user sends a foreground session S to the background (`/background`),
Claude Code makes a job, and stops the Monitors of the session. The job can
have its own id J (main spec, "Background jobs"). A live check with Claude
Code 2.1.301 showed this shape: Claude Code copies the conversation of S into
a new transcript J.jsonl, with J as the `sessionId` of each record, writes a
`continued-in` record with J into S's transcript, and ends S. The job's state
has `sessionId` J and `resumeSessionId` J. Its `sessions/<pid>.json` has
`sessionId` J and a `jobId` that is the name of the job's directory. Its hook
payloads have `session_id` J and `transcript_path` J.jsonl, and
`CLAUDE_CODE_SESSION_ID` is J in the hooks and in the shell tool. Just after
the job starts, J.jsonl holds no conversation; the copy comes some seconds
later. An older shape also exists: J.jsonl holds only title records or is
absent, the job continues to write the transcript of S, and the job's state
can have `resumeSessionId` S. Some jobs have the same id as their session. The
candidates for the session of a job are:

- only J, if J has a local transcript with a conversation and the job's state
  has no `resumeSessionId` or has J as its `resumeSessionId`. The sessions
  that continued in J do not count: they are the sources of the copy.
- Else: J, if J has a local transcript with a conversation; `resumeSessionId`,
  if the job's state has it and it is not J; and each session whose synced
  metadata says that it continued in J.

With exactly one candidate, that is the session of the job: the hooks, the
status and the `inFlight` hint of the job are for it. With no candidate or
more than one, the session of the job is not known, and the client acts on no
session that it would have to guess. A `resumeSessionId` that is not a session
id gives no candidates. The synced metadata keeps only the last `continued-in`
link of a session, so a missing link does not show that the job writes J. A
new job whose transcript has no conversation yet has no candidate, so its
hooks write no status until the first prompt is in the transcript. Thus the
UserPromptSubmit hook of the job's first prompt usually writes no status, and
the job's first tool call writes `working`. A tool call writes `working` from
no known status as it does from `ready` or `waiting`, so a long tool call in
the first turn does not leave the session with no status. The write cannot
replace a status that the client did not see, because its base is the latest
event that the client saw (see "Conditional writes"). A subagent's tool call
does not write.

Before the copy, a job that `/background` made looks like a job of the older
shape. If the `continued-in` link of S is already synced then, the session of
the job is S until the copy comes. Usually the link is not synced yet, so the
job's first hooks write no status.

After the copy, the work of S continues in J, and S gets no more records. Thus
the client marks S `done`, with the note `continued in <J>`, so that the list
of open sessions (U1) does not show the work twice. It writes the `done` only
if all of these are true:

- J is the last `continued-in` link of S: in the synced metadata of S, and in
  each local transcript of S. The client reads both again while it holds the
  sync lock of S, because S can continue in a later job K, and the transcript
  of S keeps the record of J.
- The state of exactly one job on this host has J as its `sessionId`, and J is
  the only candidate of that job: J has a local transcript with a
  conversation, and the job's state has no `resumeSessionId` or has J. A job
  of the older shape, a job that resumes a different session, and a job with
  no state on this host do not mark S. With no job state, the client cannot
  tell a copy from a `claude --resume J` that gave the stub of an older job a
  conversation of its own.
- No process runs under the id of S, and the state of no live job names S. The
  process of J does not count here, but it still makes S live (see
  "Liveness").
- The status of S (the queued write, else the latest event that the client
  saw) is not `done`.

The client does this check after each sync of S or of J, and at each tick of
the launcher. The copy can come before or after the sync of S's link, and the
job can be idle after the copy, so that no sync of J comes soon. Before the
copy, J has no conversation, so the check does not mark S. Thus the `done`
comes after the hooks of J stop to act on S, and a prompt in the job before
the copy cannot reopen it.

The write is a transition that the client infers: its source kind is `hook`,
and it is not explicit. It replaces a queued write for S, for example a write
of a hook of J from before the copy. Its base is the latest event of S that
the client saw, so it does not replace an explicit status that a different
writer set after that event (see "Conditional writes"). It gives no notice,
because no agent runs in S. A prompt in S or `hyperspace reopen` reopens S as
usual. The client decides only once for each S and J, so it does not mark S
`done` again after a reopen.

Only the job that runs a process changes the id of that process: its
`CLAUDE_JOB_DIR` names the job, the job's state names the id, and a
`sessions/<pid>.json` of the process or of a parent process has that `jobId`
and a `procStart` that is the start time of that process. A record with no
`procStart`, or a start time that the client cannot read (on Linux, field 22
of `/proc/<pid>/stat`; on other platforms, never), does not count. Thus a
stale record whose pid is now this process does not confirm a job. A
foreground `claude --resume J` writes J's transcript, so it stays J. A stale
`CLAUDE_JOB_DIR` changes nothing. In the job, the id can be J or the
`resumeSessionId`. Both give the session of the job, from the candidates of J.
If the job's state has no valid id J, the session of the job is not known. The
CLI and the hooks use this one rule. A hook also counts the session of the job
as not known if this host has no transcript of that session.

In a job whose session is not known, the hooks:

- queue no status write;
- show no status notice: SessionStart shows no `done` notice, and
  UserPromptSubmit and the PostToolUse notice hook show only the lease
  notices. The status notices stay pending;
- start no close of the job at SessionEnd.

The checkpoint sync, the lease notices, and the restore and resume checks at
SessionStart and UserPromptSubmit stay on the session of the payload's
transcript, as for a session that is not a job.

### Liveness

Liveness records where a process for the session runs now. It is a different
record from the lease:

- The lease is intent: where the session must continue. It has no time limit,
  because a workstation can be off for weeks and still be the home of its
  sessions.
- Liveness is fact: where a process runs now. It must expire, because a host
  can stop with no notice.

The lease and liveness can differ in two ways. A stopped runner session has a
lease and no process. A stale copy that a user resumes on the old host has a
process and no lease. Lists and the prevention hook show a warning for the
second case before the stale host appends.

Each host sends a _heartbeat_: the list of the sessions that run on the host,
from the adapter's live-process check (`is_running` in the main spec). A
process is live for each session whose transcript it can write: its registry
id, the ids in its job's state, and the sessions that continued in its
registry id. Thus a job under its own id J makes the session S that it writes
live. The heartbeat also gives its interval. SessionStart and SessionEnd send
an extra heartbeat at once, so that a change shows without a wait. The
SessionEnd heartbeat leaves out every id of the ending process.

A host can have more than one harness config directory, each with its own
launcher. Each heartbeat therefore has a _source_: the client's id for its
config directory. A heartbeat replaces only the earlier heartbeat of the same
source, and only a heartbeat of the source that listed a session can show that
the session stopped. Thus the launchers of two config directories on one host
do not mark each other's sessions `stopped`.

The server derives the liveness of a session:

| Liveness  | Rule                                                                                                      |
| --------- | --------------------------------------------------------------------------------------------------------- |
| `live`    | A heartbeat that has not expired lists the session.                                                       |
| `stopped` | Not `live`, and the latest heartbeat of the host that last listed the session does not list it.           |
| `unknown` | Not `live`, and the heartbeat that last listed the session expired, and the host sent no newer heartbeat. |

A copy that stops on one host does not hide a copy that runs on a different
host, or from a different source on the same host. The `live` rule applies
first, and its host is the host of the live copy.

A heartbeat expires after three of its intervals.

Lists combine status and liveness into the states of U6:

- _interrupted_: status `working`, liveness `stopped`.
- _unknown since T_: liveness `unknown`. T is the time of the last heartbeat.
- _stalled_: status `working`, liveness `live`, and no append for longer than
  a limit (default 30 minutes). A long tool call can also look like this, so
  stalled is a hint in the list, not a status.

On a workstation, an idle session at its prompt fires no hooks. The heartbeat
therefore comes from the launcher, a host process (see "Launcher"), and not
from hooks.

Liveness is current state, not history. The server keeps the latest heartbeat
of each host and overwrites it. This is the same as the lease. The append-only
rule of the main spec applies to session data, and liveness is not session
data.

## Closing a done session

When a session becomes `done` (by the agent, by `/done`, or from a different
host):

1. The session continues its current turn. A hook gives the main agent, not a
   subagent, a notice if the mark came from outside (A3). A prompt that
   reopens the session (U5) drops a notice that the agent did not get yet.
2. The UI shows that the session is `done`. It does not close the session.
3. A foreground session closes when the user closes the UI. Nothing more is
   necessary.
4. A background job closes when its process stops and its status is `done`.
   The client syncs the session, as a surrender does: it must not leave bytes
   that only this host has. Then it runs `claude rm <job>`. This removes the
   job from the `claude agents` view and deletes the job's worktree. It does
   not delete the transcript, so `claude --resume` can still reopen the
   session.
5. `claude rm` refuses when the worktree has changes or commits that are not
   pushed. The client then keeps the job and records a notice. `hyperspace ls`
   shows the session as `done, not closed: unpushed work`. The user decides.
6. If the worktree is deleted and the work is necessary again, the user can
   reopen the session and ask the agent to apply the work again from the
   transcript.

Claude Code has no supported signal for a terminal attach or detach: no hook
event, no environment variable, and no field in the job state. The client
therefore does not close a job at detach. It waits for the job's process to
stop. The Claude Code daemon stops an idle job after a grace period if no
terminal is attached and no task runs. The client must not stop its own job at
the end of a turn, because the user can be attached.

Two triggers do step 4:

- The SessionEnd hook of a background job (`CLAUDE_JOB_DIR` is set) whose
  session is beyond doubt (see below) and `done` starts a detached process
  that waits for the job's process to stop, syncs, and runs `claude rm`.
- A sweep in the launcher finds jobs that are not live and whose session is
  `done`. This catches a session that was marked `done` from a different host
  after its job stopped, and a SessionEnd hook that did not run. The sweep
  asks the server for the status of a stopped job when it first sees the job,
  when the job's state file changes, and every 15 minutes.

Both triggers ask the server for the status, and read the job's state and the
synced metadata again, just before they run `claude rm`. They act only on
`done`. When `claude rm` refuses, the launcher records the reason and sends it
in each heartbeat, until the job goes away or its state file changes.

Only a job whose session is beyond doubt closes automatically. That is a job
with its own id J (`sessionId` in its state) for which both of these are true:

- J has a local transcript with a conversation;
- the job's state has no `resumeSessionId`, or its `resumeSessionId` is J.

Then J is the one candidate (see "Transitions"), also if the synced metadata
of another session says that it continued in J, and the triggers decide on the
status of J. This includes a job that `/background` made. Every other job is
never closed automatically: a job under its own id that writes another
session, a job whose session is not known, and a job whose own id has a
conversation while its state resumes another session. While such a job does
not run, the sweep records it on J and on each candidate, with one of these
reasons:

- `job <short> writes another session; close it yourself` (one candidate,
  which is not J);
- `job <short> can write more than one session; close it yourself`;
- `the session of job <short> is not known; close it yourself` (no candidate).

The sweep checks these jobs again at each check, also if the state file does
not change. When a job can close but its backup fails, or the server does not
have its session, that reason replaces the earlier reason of the job, also on
the earlier candidates. The user closes such a job with `claude rm`.

This is a limit of the current client: a `done` job that writes another
session must be closed by hand, also when its session is clear from the job's
state, for example a job of the older shape that writes the session that the
user sent to the background, or a job that `claude --bg --resume <sid>`
started under a new id.

## Launcher

`hyperspace launcher` is a long-running process, one for each host. It
replaces the supervisor of the current runner entrypoint, and it is the base
for later work that starts sessions on a host with no user action, for example
agent-initiated teleport (main spec, "Later"). It:

- sends the heartbeat;
- closes `done` jobs (see "Closing a done session");
- resumes sessions that need a process (runner mode only);
- resumes a held session that has a pending wake request (see the
  [wake spec](2026-10-28-wake.md)).

A lock in the client state directory makes sure that only one launcher runs
for each host and harness config directory. The SessionStart and SessionEnd
hooks do not talk to the launcher. They send their extra heartbeat to the
server directly, with the full list of live sessions from the adapter and the
refused jobs from the client's local state. Thus the hooks need no channel to
the launcher, and two heartbeats that cross give the same result.

If the launcher stops while sessions run, its heartbeat expires and their
liveness becomes `unknown`. The next SessionStart hook starts the launcher
again.

### Workstation mode

The SessionStart hook starts the launcher as a detached process if it does not
run, in the same way that it starts a checkpoint. The hook does this only on a
workstation (`host_kind` `workstation`). On a runner, the launcher is the
entrypoint's main process, or there is none: a launcher that a hook started
would hold the host's launcher lock, and the runner's launcher would then
refuse to start. When no session has run on the host for 10 minutes, the
launcher sends a last heartbeat with an empty list and stops. The sessions of
the host then show as `stopped`, not `unknown`.

The launcher gets its token as the checkpoint that the SessionStart hook
starts does: it can read the keyring once, when it starts at a session start
or resume (main spec, "Credentials"). After that it uses the token cache. Each
heartbeat reads the cache, so the cache does not expire while the launcher
runs.

This mode needs no service manager, so it works on each OS where the hooks
work. (The client itself works only on Linux and macOS.) A user who wants the
launcher all the time, for example so that a message to a stopped session
resumes it (see the [wake spec](2026-10-28-wake.md)), can run
`hyperspace launcher --stay` from a systemd user unit or a launchd agent.
`docs/deploy.md` has examples of both.

### Runner mode

On a runner, the image's entrypoint runs `hyperspace launcher --runner` as its
main process, and it does not stop when no session runs. It registers the host
first. If the server is not available (a network error, a timeout, an
incorrect response, a server error, a rate limit, or a proxy that is not
available), it waits and tries again: after 5 seconds, then a wait that
doubles to at most 5 minutes. Thus the pod does not restart in a loop while
the server is not available. A permanent error stops the launcher at once with
an error, for example a bad credential, a proxy that refuses its credential
(`407`), no server URL, a redirect loop, or a local error in the request. The
client refuses a token with a space, a control character, or a character that
is not ASCII when it reads its configuration, because such a token makes each
request fail. `--stay` waits in the same way. At boot it:

1. Runs `hyperspace restore --held` (main spec, "Leases"). If the server is
   not available, or the restore of a session fails, it does the restore again
   at a later tick, until a restore has no failure. The first retry is after
   one tick interval, and the wait doubles after each failure, to at most one
   hour. A retry has the same checks as the first restore: it does not write
   over a local copy that differs, and it does not restore a session that
   runs.
2. Lists the sessions whose lease it holds, with their status and liveness.
3. Resumes each session that is interrupted, or that is `waiting` with wake
   `local`, as a background job, with `claude --bg --resume <sid>` in the
   session's workspace. It does not resume a session that is `ready`,
   `waiting` with wake `message`, or `done`, or that has no status (R1, R2,
   R3), unless the session has a pending wake request (see the
   [wake spec](2026-10-28-wake.md)). It also does not resume a session that
   the server shows as `live`, on any host, so that the resume does not make a
   second copy. This includes its own host, because a different harness config
   directory on the host can run the session, and the launcher's own
   live-process check does not see that process. The launcher sends its
   heartbeat before it resumes, so a `live` from its own heartbeat before a
   restart does not stay.

At each tick, it does step 3 again for the held sessions that do not run. It
does not resume a job that the Claude Code daemon stopped if its session is
`done`, or if no event needs a process (R2, R3). It resumes one session at
most 5 times in an hour, so that a session that stops at once does not start
in a loop. A runner with no session to resume runs only the launcher.

The resume runs in the workspace that the runner recorded for the session in
its local state, and only for a session with a local transcript and no
pending-restore marker (main spec, "Restore"). A session with an unfinished
restore resumes after a restore retry completes it. The launcher holds the
session's sync lock from its checks until `claude --bg` returns, so that no
restore or sync starts between the checks and the start. If another process
holds the lock, the launcher tries again at the next tick. The session's
process does not get the lock, and its hooks do not wait for it without a
limit, so the hold cannot block the new session. A checkpoint that a hook
starts during the hold does not sync. Thus, when the launcher releases the
lock, also after a failure or a timeout of `claude --bg`, it starts a
checkpoint if the session is dirty (main spec, "Checkpoint scheduling"). For a
resume that the status needs, the first message tells the agent that the
runner restarted, and, for a `waiting` session, that its crons, Monitors and
background tasks stopped. For a resume that a wake request needs, see "The
first message" in the [wake spec](2026-10-28-wake.md).

`claude --bg` returns 0 when the daemon has the job, before the job's process
starts. Thus the exit status does not show a process that fails before it
starts, for example because an option took the first message as a value. After
the lock release, the launcher finds the new job (the one job directory that
`claude --bg` added, or else the one new job whose state names the session)
and reads its `state.json` for at most 10 seconds, until the job leaves its
start state (Claude Code 2.1.301 writes `state` `working` with `detail`
`starting…`). If the job's `state` is `failed`, the launcher logs the job's
`detail` (printable, on one line, cut to the length of a status note) and
counts the resume as a failed resume. If it cannot find the job or read its
state, or the job still starts at the end of the wait, it counts the resume as
a resume that succeeded.

The launcher reads the Claude Code options for a resume (model, agent, and
channels) from `launcher_claude_args`, or from
`HYPERSPACE_LAUNCHER_CLAUDE_ARGS` as a JSON list. The runner image gives that
configuration. The launcher refuses an option that it sets itself, for example
`--resume` or `--fork-session`. It also refuses a list whose last option takes
a list of values, for example `--dangerously-load-development-channels <spec>`
or `--add-dir=<dir>`, because `claude --bg` puts the first message after the
options, and that option would take the message as a value.

## Messages to a session

Subspace delivers messages through the relay in the session's process. When
the session has no process, subspace sends a wake to the hyperspace server,
and the launcher of the host that holds the lease resumes the session at its
next tick. The [wake spec](2026-10-28-wake.md) gives the endpoint, the `wake`
scope, the verdicts, and the launcher rules. The launcher is not a webhook: it
makes only outbound requests, and the server is the meeting point.

The server refuses a wake for a `done` session with `409` and a message such
as "session done: <outcome note>" (M1). A `reopen` with a reason (M2), and the
notices to the sender (M1, M3), are later work (see "Later").

A resume must continue the session under the same id. Subspace keys its state
by session id, and the relay connects as that id. Hyperspace owns this rule
for each resume that it does, on each host kind and in each launcher mode
(wake spec, "Session identity").

A `done` session that is still live (its process did not stop yet) gets
messages directly, through its relay. The message is new activity, so it
reopens the session (U5). The process still runs, so the session can still be
in use.

## Overview

The CLI gives the overview stories their commands:

- `hyperspace ls --open [--json]`: every session that is not `done`, with
  status, reason or wake kind, note, source, liveness, host, workspace, title,
  last activity, and excerpt (O1, U1, U2).
- `hyperspace ls --status STATUS[,STATUS...]`: sessions with one of the
  statuses, for example `done` to find sessions that are done but not closed,
  or `none` for sessions with no status. The option takes a comma-separated
  list and is repeatable. The client sends each status as its own `status=`
  parameter, which also an older server accepts.
- With a server that has `child-sessions`, `ls` shows only top-level sessions:
  not the child sessions that tools start for the work of another session, for
  example security reviews (main spec, "Child sessions"). `ls --all` shows
  every session.
- `hyperspace tail [<sid>] [-n N]`: the last N turns of the active branch. The
  adapter formats the records on the client (O2).
- `hyperspace status [STATUS] [--session SID] [--reason R] [--wake W] [--note TEXT] [--by-user]`,
  with the short forms `hyperspace done [<sid>...]` and
  `hyperspace reopen <sid>` (U4, O3). With no `STATUS`, it shows the status.
  With no session id, the command applies to the current session, from
  `CLAUDE_CODE_SESSION_ID` (A1, A2). In a background job, this variable can
  hold the job's own id J. The client then uses the session of the job (see
  "Transitions"). If the session of the job is not known, a command with no
  session id stops with the error
  `give a session id; the session of this background job is not known`. A
  command with an explicit session id still works. The source is `agent` when
  the command runs in a Claude Code session, and `user` otherwise or with
  `--by-user`.

The plugin adds:

- a `/done [note]` command (U3);
- a skill that tells an agent when and how to set its status (A1, A2);
- a skill for the overview agent that uses `ls --status working,waiting,ready`
  and `tail` (O1, O2, O3). It does not use `ls --open`, because `--open` also
  gives the sessions with no status, usually old sessions from before session
  status.

This changes the main spec's decision "v1 has no skill". The skills only call
the CLI; the plugin still holds no logic.

## Backfill

`hyperspace backfill-status --since 30d [--rate N]` sends each session with no
status in the window a message. The message explains the status system and
asks the agent to set its status with a note, and to change nothing else.

- The command resumes each session for one turn with
  `claude --resume <sid> -p`, with permission only for `hyperspace status` and
  `hyperspace done`. No user watches the session, and its transcript can hold
  text from other sources. The resume therefore uses Claude Code's restricted
  mode: it does not load the user's settings or MCP servers, and Bash is the
  only tool. In the turn, `hyperspace status`, `done` and `reopen` act only on
  the session of the turn, and not as the user, so text in one transcript
  cannot change the status of other sessions. It skips sessions that are live
  (also a session that a job under its own id writes), that have a status, or
  whose lease a different host holds, transcripts that hold no conversation
  (no `user` or `assistant` record: the stub of a job's own id, or a file of
  summaries from an older Claude Code), and sessions that a tool started (with
  `automation` metadata, for example security reviews; see "Child sessions" in
  the main spec), which gain nothing from the message.
- It records each session that it messages before the resume starts. A second
  run, for example after an interruption, skips these sessions unless
  `--retry` is given. `--dry-run` lists the sessions and messages none.
- On a runner, the same command runs on the runner, for example with
  `kubectl exec`. It needs the client in the runner image, but no launcher and
  no subspace.
- The rate limit counts sessions per hour. A resume writes the whole session
  context to the prompt cache, so a large backlog can exceed the account's
  usage limits. Set the rate to stay within them.

## Server

### Storage

- `status_events`: one row per status event, append-only, with the fields of
  "Status events".
- `host_heartbeats`: one row per host and source, overwritten by each
  heartbeat of that source.

A purge of a session (`scripts/purge.py`) also removes its status events.

### HTTP API

| Method and path                         | Scope                    | Purpose                                                                       |
| --------------------------------------- | ------------------------ | ----------------------------------------------------------------------------- |
| `POST /sessions/{harness}/{sid}/status` | `append` and read access | Add a status event. `409 stale_status` if `base_seq` is not the current `seq` |
| `PUT /hosts/self/heartbeat`             | `append`                 | Replace the list of live sessions of one source of the host                   |

Changes to existing bodies:

- The session state body (`GET .../state`, append responses, manifests, and
  list entries) gets `status` (the latest event, with `seq`) and `liveness`.
  Thus a client learns of a mark from outside at its next append (A3).
- `GET /sessions` gets the filters `status=` (repeatable), `open=1` (not
  `done`), `lease_host=` and `liveness=`. A value of `status=` or `liveness=`
  can also be a comma-separated list, for example
  `status=working,waiting,ready`. Servers before the `list-cursor` capability
  refuse the comma form, so a client that must work with them repeats the
  parameter.
- `GET /whoami` advertises the capabilities `session-status` and `liveness`. A
  client does not send status or heartbeats to a server without them.

### Permissions

A status write needs the `append` scope and read access to the session (`own`,
`granted` or `all`). There is no rule for each host kind:

- A workstation with `read: all` can change the status of every session.
- A runner can change the status of its own sessions and of granted sessions.
  A runner with a grant can already take the session with a steal, so the
  status adds no power.

A prompt-injected agent on a runner can mark the runner's sessions `done`, so
that the runner does not resume them. That agent can already read the runner's
credential, as every session on a runner can. Hyperspace therefore does not
separate sessions on one host.

A heartbeat applies only to the credential's own host.

## Later

- **Reopen and notices.** A `reopen` with a reason for a `done` session (M2),
  and the notices to the sender that a message was not delivered or is queued
  (M1, M3).
- **Scale to zero.** An operator stops a runner when no session on it is
  `working` or `waiting` with wake `local`, and starts it when a wake request
  comes for a session whose lease the runner holds.
- **Idle limits.** A session that is `ready` for a long time can stop on a
  runner. With R2 this is already the default.
- **Stalled detection** from more signals than the append time, for example
  the job's `inFlight` data.
- **Close at detach.** Claude Code has no supported attach or detach signal.
  If it adds one, the client can close a `done` job at detach, as U3 asks, and
  not when the daemon stops the job.

## Resolved questions

- `CLAUDE_CODE_SESSION_ID` is set in the shell tool's environment of every
  Claude Code session, foreground and background.
- In a job that `/background` makes with Claude Code 2.1.301,
  `CLAUDE_CODE_SESSION_ID`, the hook payloads and the live-session registry
  hold the job's own id J, and the job writes J.jsonl, which has a copy of the
  conversation (see "Transitions"). Such a job closes automatically. For a job
  of the older shape, which writes the transcript of a session S, the ids are
  not confirmed. The client acts on S only when S is the one candidate, and it
  never closes such a job automatically (see "Closing a done session"): the
  user closes it by hand.
- A launcher resumes a session as a background job with
  `claude --bg --resume <sid>`, in the session's workspace. Claude Code then
  continues the session under the same id. If the session already runs, Claude
  Code starts a copy under a new id, so the launcher first checks that the
  session does not run on the host, also in a job under the job's own id.
  Hyperspace never writes Claude Code's job state. If the job gets its own id,
  it closes only by hand (see "Closing a done session").
- Tests will show whether the SessionEnd hook runs when the Claude Code daemon
  stops an idle job. The sweep makes the design correct without it.
- The heartbeat interval is 60 seconds, so a heartbeat expires after 3
  minutes. The stalled limit is 30 minutes. Both are configurable.
