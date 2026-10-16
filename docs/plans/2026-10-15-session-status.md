# Plan: session status and liveness

Date: 2026-10-15. Spec:
[session status and liveness](../specs/2026-10-15-session-status.md). It
extends the [main spec](../specs/2026-10-12-hyperspace.md).

Each step is one branch or one series of commits, and keeps the test suite
green. Steps 1 to 5 are in this repo. Step 6 is in other repos. A step can
start when the steps before it are merged. The names, shapes and limits in
this plan are the contract between the steps; "Decisions made in this plan"
lists the ones that the spec leaves open.

## Step 1: protocol and server

### Goal

The server stores status events, accepts heartbeats, derives liveness, and
shows status and liveness in every session state body. No client change is
necessary: an old client ignores the new fields.

### Files

- `protocol/src/hyperspace_protocol/__init__.py`: types, constants and
  validation.
- `lib/store.py`: migration 3, status events, heartbeats, liveness, list
  filters, revision, purge.
- `lib/app.py`: two routes, list query parameters, the `stale_status` error.
- `scripts/purge.py`: report the status events and liveness rows that a purge
  removes.
- `docs/specs/2026-10-12-hyperspace.md`: API table, and a pointer to the new
  spec.
- Tests: `tests/test_unit_protocol.py`, `tests/test_unit_store.py`,
  `tests/test_e2e_api.py`, `tests/test_e2e_admin.py`.

### Protocol

Constants:

```python
CAP_SESSION_STATUS = "session-status"
CAP_LIVENESS = "liveness"
CAPABILITIES = [CAP_WORKSPACE_UTF8, CAP_RESTORE_PRECONDITION,
                CAP_SESSION_STATUS, CAP_LIVENESS]

MAX_STATUS_NOTE = 300        # characters, one line
MAX_STATUS_EXCERPT = 1000    # characters
MAX_HEARTBEAT_SESSIONS = 1000
MIN_HEARTBEAT_INTERVAL = 10  # seconds
MAX_HEARTBEAT_INTERVAL = 3600

Status = Literal["working", "waiting", "ready", "done"]
ReadyReason = Literal["asked", "idle"]
WakeKind = Literal["local", "message"]
StatusSourceKind = Literal["user", "agent", "hook"]
LivenessState = Literal["live", "stopped", "unknown"]
```

Bodies:

```python
class StatusSource(TypedDict):
    kind: StatusSourceKind
    host: str                       # set by the server from the credential
    session_id: NotRequired[str]    # an agent in a different session

class StatusEvent(TypedDict):
    seq: int
    status: Status
    reason: NotRequired[ReadyReason]   # only with "ready"; required there
    wake: NotRequired[WakeKind]        # only with "waiting"; required there
    note: NotRequired[str]
    source: StatusSource
    explicit: bool
    received: str

class Liveness(TypedDict):
    state: LivenessState
    host: str          # a live copy's host, else the last to list it
    since: str         # see "Liveness rules"
    close_blocked: NotRequired[str]   # from the launcher (step 4)

class SessionStateBody(TypedDict):     # existing; three new fields
    state: SessionState
    lease: Lease
    branches: NotRequired[list[Branch]]
    status: NotRequired[StatusEvent]   # absent: no status event yet
    liveness: NotRequired[Liveness]    # absent: no host ever listed it
    last_append: NotRequired[str]      # sessions.last_activity

class SessionSummary(TypedDict):       # existing; one new field
    ...
    excerpt: NotRequired[str]          # excerpt of the latest status event

class StatusRequest(TypedDict):
    status: Status
    reason: NotRequired[ReadyReason]
    wake: NotRequired[WakeKind]
    note: NotRequired[str]
    excerpt: NotRequired[str]
    source_kind: StatusSourceKind
    source_session: NotRequired[str]
    explicit: bool
    base_seq: int                      # 0 when the writer saw no event

class StatusResponse(TypedDict):
    status: StatusEvent

class StaleStatus(TypedDict):          # body of 409 stale_status
    error: Literal["stale_status"]
    detail: str
    status: NotRequired[StatusEvent]   # the current event, if any

class HeartbeatSession(TypedDict):
    harness: str
    session_id: str

class UnclosedSession(TypedDict):
    harness: str
    session_id: str
    reason: str                        # one line, at most MAX_STATUS_NOTE

class HeartbeatRequest(TypedDict):
    interval: int
    sessions: list[HeartbeatSession]
    unclosed: NotRequired[list[UnclosedSession]]
    final: NotRequired[bool]           # the launcher stops after this one

class HeartbeatResponse(TypedDict):
    received: str
    recorded: int                      # listed sessions that the server knows
```

Validation functions, with unit tests: `validate_status_request` (status
value; `reason` exactly with `ready`; `wake` exactly with `waiting`; note is
one line and at most `MAX_STATUS_NOTE`; excerpt at most `MAX_STATUS_EXCERPT`;
`base_seq` is a nonnegative `int`, not a `bool`; `source_session` passes
`validate_session_id`) and `validate_heartbeat` (interval in range; at most
`MAX_HEARTBEAT_SESSIONS` entries; each harness and session id valid).

### Schema (migration 3)

Append one string to `_MIGRATIONS`. The migration runner splits on `;`, so the
SQL must not contain a `;` inside a literal.

```sql
CREATE TABLE status_events (
    session INTEGER NOT NULL REFERENCES sessions (id),
    seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    wake TEXT,
    note TEXT,
    excerpt TEXT,
    source_kind TEXT NOT NULL,
    source_host TEXT NOT NULL,
    source_session TEXT,
    explicit INTEGER NOT NULL,
    received TEXT NOT NULL,
    PRIMARY KEY (session, seq)
);
CREATE TABLE host_heartbeats (
    account TEXT NOT NULL,
    host TEXT NOT NULL,
    source TEXT NOT NULL,
    received TEXT NOT NULL,
    interval INTEGER NOT NULL,
    final INTEGER NOT NULL,
    unclosed TEXT NOT NULL,
    PRIMARY KEY (account, host, source)
);
CREATE TABLE session_liveness (
    session INTEGER NOT NULL REFERENCES sessions (id),
    host TEXT NOT NULL,
    source TEXT NOT NULL,
    first_listed TEXT NOT NULL,
    last_listed TEXT NOT NULL,
    PRIMARY KEY (session, host, source)
);
```

`status_events` is append-only. `host_heartbeats` and `session_liveness` are
current state, and the store overwrites their rows (spec, "Liveness").

### Behavior

**Status write** (`Store.add_status(key, host, request) -> StatusEvent`), in
one transaction inside `existing_authorized`:

1. The session must exist (`404` otherwise). The client sends status only
   after the sync that created the session.
2. `current = MAX(seq)` for the session, or 0.
3. If `request.base_seq != current`, raise `StoreError("stale_status")` with
   the current event attached. The route returns `409` with the `StaleStatus`
   body.
4. Insert the event with `seq = current + 1`, `source_host` = the credential's
   host, `received` = now.

The server treats explicit and automatic writes the same way. The client uses
`explicit` for its retry rules (step 2) and lists show it.

**Liveness rules** (`Store._liveness(session) -> Liveness | None`):

- A heartbeat updates `host_heartbeats` for its host. For each listed session
  that exists in the account and that the host can read (`_can_read_row`), it
  upserts `session_liveness (session, host, now)`. It skips other ids with no
  error, so the response gives no existence oracle; `recorded` counts only the
  rows it wrote.
- For each `session_liveness` row of a session, let `hb` be the
  `host_heartbeats` row of its host and source (see "Fixes after review"). If
  the session has no rows, there is no liveness.
- If a row has `hb.received <= last_listed` (its latest heartbeat lists the
  session) and `now - hb.received <= 3 * hb.interval`: `live` on that host,
  since `first_listed`. If more than one row agrees, take the one with the
  latest `last_listed` (see "Fixes after review", "Live copy").
- Else take the row with the latest `last_listed`:
  - `hb.received > last_listed` (a newer heartbeat did not list it):
    `stopped`, since `last_listed`.
  - else: `unknown`, since `hb.received`.
- `session_liveness` has one more column, `first_listed TEXT NOT NULL`. A
  heartbeat that lists the session sets it to now if the row is new, or if the
  session was not `live` on that host just before this heartbeat. Otherwise it
  keeps the old value. Thus `since` for `live` is the start of the current
  run.
- `close_blocked` comes from `hb.unclosed` for this session, for any state.
  The store keeps only the `unclosed` entries of sessions that the host can
  read. For such an entry with no liveness row for the host (the job stopped
  before any heartbeat listed it), the heartbeat adds a row with
  `first_listed = last_listed =` the session's last activity, so that the
  session shows as `stopped` with its `close_blocked` (changed in step 4).
- A final heartbeat (`final: true`) with an empty list makes the host's
  sessions `stopped` at once.

**State body**: `_session_state` adds `status` (the latest event, without
`excerpt`), `liveness`, and `last_append` (`sessions.last_activity`).
`list_sessions` adds `excerpt` from the latest event when it has one.

**Revision**: the manifest `revision` must not change because of a heartbeat
or a status write. Otherwise a heartbeat between the manifest read and the
teleport request causes `409 stale_snapshot`. Compute the hash over a copy of
the manifest whose `state` has no `status`, `liveness` or `last_append`.
`last_append` changes only with an append, which the revision already covers
through the files.

**List filters** (`GET /sessions`), added to `Store.list_sessions` and the
route:

- `status=<value>`, repeatable (`request.query_params.getlist`); the value
  `none` matches sessions with no status event.
- `open=1`: the latest status is not `done` (sessions with no status match).
- `lease_host=<host>`: the lease is `held` by that host.
- `liveness=<value>`, repeatable: `live`, `stopped`, `unknown`, or `none`.

**Routes**:

| Method and path                         | Scope    | Handler                                                                                     |
| --------------------------------------- | -------- | ------------------------------------------------------------------------------------------- |
| `POST /sessions/{harness}/{sid}/status` | `append` | `existing_authorized` with the read check, then `add_status`. No workspace header is needed |
| `PUT /hosts/self/heartbeat`             | `append` | `Store.heartbeat(account, host, scope, request)`, which returns the `HeartbeatResponse`     |

Add `"stale_status": 409` to `_STATUS`. The error handler adds the current
event to the body, as it adds `generation` and `length` for an append
conflict.

The usual JSON body limit (64 KiB) is too small for a heartbeat with
`MAX_HEARTBEAT_SESSIONS` entries in `sessions` and in `unclosed`. The
heartbeat route reads at most 1 MiB.

**Purge**: `purge_session` also deletes `status_events` and `session_liveness`
rows of the session. `scripts/purge.py` shows their counts in its dry run.

### Tests

- `test_unit_protocol.py`: `test_status_request_valid` and
  `test_status_request_invalid` (parametrized), `test_heartbeat_validation`.
- `test_unit_store.py`:
  - `test_status_first_write_needs_base_zero`
  - `test_status_cas_rejects_stale_base` (the error carries the current event)
  - `test_status_aba_is_rejected` (done, ready, done; a writer with the first
    done's seq fails)
  - `test_status_in_state_body_without_excerpt`, `test_list_has_excerpt`
  - `test_liveness_live_stopped_unknown` (with a fake clock)
  - `test_liveness_follows_the_host_that_listed_last`
  - `test_final_heartbeat_stops_sessions`
  - `test_heartbeat_close_blocked`
  - `test_heartbeat_skips_unknown_and_unreadable_sessions`
  - `test_revision_ignores_status_and_heartbeat`
  - `test_list_filters_status_open_lease_host_liveness`
  - `test_purge_session_removes_status_and_liveness`
  - `test_migration_3_on_a_version_2_index` (open a copy of a v2 index made by
    the migration list without the last entry)
- `test_e2e_api.py`:
  - `test_status_route_scopes` (append needed; a host that cannot read gets
    `404`; a runner with a grant can write)
  - `test_status_409_body`
  - `test_heartbeat_route` and `test_whoami_capabilities`
  - `test_teleport_after_heartbeat_has_no_stale_snapshot`
- `test_e2e_admin.py`: `test_purge_reports_status_events`.

### Done when

- [ ] All tests above pass, also with `UV_PYTHON=3.12`; ruff and pyright are
      clean.
- [ ] An old client syncs, restores and teleports against the new server with
      no change (the existing e2e suite passes).
- [ ] The main spec's API table lists the two routes, and its "Later" section
      points to the new spec.
- [ ] Commit type `feat(server)` and `feat(protocol)` (minor bumps).

## Step 2: client and plugin

### Goal

The hooks write status automatically, the agent and the user set it
explicitly, and the CLI lists and inspects open sessions. This gives stories
U1 to U5, A1 to A3, O1 and O2 on a workstation.

### Files

- `client/src/hyperspace_client/state.py`: new table and columns.
- `client/src/hyperspace_client/api.py`: `add_status`, `heartbeat` (used in
  step 4), new list filters.
- `client/src/hyperspace_client/status.py` (new): the transition rules, the
  wanted-status queue, the send and retry logic, and notices.
- `client/src/hyperspace_client/hooks.py`: record wanted status per event; add
  `Notification` to the handled events.
- `client/src/hyperspace_client/sync.py`: send the wanted status at the end of
  `Syncer.run`, and save the status from each state body.
- `client/src/hyperspace_client/cli.py`: `status`, `done`, `reopen`,
  `ls --open`, `tail`.
- `client/src/hyperspace_client/adapters/claude_code.py`: `excerpt`,
  `in_flight`, `format_tail`, `live_sessions`.
- `plugin/hyperspace/hooks/hooks.json`: add `Notification` (async) and a
  synchronous `PostToolUse` entry for notices.
- `plugin/hyperspace/hooks/notice.sh` (new),
  `plugin/hyperspace/commands/done.md`,
  `plugin/hyperspace/skills/session-status/SKILL.md`,
  `plugin/hyperspace/skills/session-overview/SKILL.md`.
- Tests: `tests/test_unit_client_status.py` (new),
  `tests/test_unit_client_hooks.py`, `tests/test_unit_plugin_hooks.py`,
  `tests/test_e2e_client_status.py` (new).

### Local state

Add to `_SCHEMA` (all `CREATE TABLE IF NOT EXISTS`) and to the column upgrade
in `State.__init__`:

```sql
CREATE TABLE IF NOT EXISTS wanted_status (
    harness TEXT NOT NULL,
    session_id TEXT NOT NULL,
    body TEXT NOT NULL,          -- a StatusRequest without base_seq
    created REAL NOT NULL,
    PRIMARY KEY (harness, session_id)
);
-- New columns in sessions:
--   status TEXT              (JSON of the latest StatusEvent seen, or NULL)
--   turn_declared INTEGER    (1 after an explicit agent write in this turn)
--   done_seen REAL           (time the client first saw status done)
```

`wanted_status` holds at most one row for each session: a new wanted status
replaces the old one. Thus only the latest wanted status goes to the server
after an outage (spec, "Transitions").

### Transitions (Claude Code)

`status.py` has one pure function,
`transition(event, payload, record, in_flight) -> StatusRequest | None`, which
the hook calls. `record` is the local `SessionRecord` with the last seen
status.

| Hook event and condition                                                         | Wanted status                          | Explicit |
| -------------------------------------------------------------------------------- | -------------------------------------- | -------- |
| UserPromptSubmit, last seen status is `done`                                     | `working`, note "reopened by a prompt" | yes      |
| UserPromptSubmit, otherwise                                                      | `working`; clear `turn_declared`       | no       |
| PreToolUse with `tool_name == "AskUserQuestion"`                                 | `ready`, reason `asked`                | no       |
| PostToolUse with `tool_name == "AskUserQuestion"`                                | `working`                              | no       |
| Notification with `notification_type == "permission_prompt"`                     | `ready`, reason `asked`                | no       |
| PostToolUse or PostToolUseFailure, last seen status is `ready` with reason asked | `working`                              | no       |
| Stop, `turn_declared` is set                                                     | none                                   | -        |
| Stop, last seen status is `done`                                                 | none                                   | -        |
| Stop, `in_flight > 0`                                                            | `waiting`, wake `local`, with excerpt  | no       |
| Stop, otherwise                                                                  | `ready`, reason `idle`, with excerpt   | no       |
| SessionStart, SessionEnd, other events                                           | none                                   | -        |

`in_flight` is `inFlight.tasks` from `$CLAUDE_JOB_DIR/state.json`, read by
`ClaudeCode.in_flight()`. A missing variable, file or field gives 0. The hook
process inherits the environment of the Claude Code process.

The hook writes the wanted status, then starts the checkpoint as now. A
transition that gives "none" changes nothing.

### Send and retry

At the end of `Syncer.run`, after the appends and the metadata, if the server
has `session-status` and `wanted_status` has a row for the session:

1. Send it with `base_seq` = the `seq` of the last seen status (0 if none).
2. On success: save the event as the last seen status, delete the row.
3. On `409 stale_status`: save the current event as the last seen status.
   - If the current event is `done` and explicit, and its source is not this
     session's own agent: drop the row and add a notice: "<host> marked this
     session done (by <kind>): <note>. Finish the current step and stop." This
     is A3. Set `done_seen`.
   - If the wanted status is explicit: retry once with the new base.
   - Otherwise drop the row. The next hook event decides again.
4. On a network error: keep the row for the next sync.

`_note_session_state` also saves `status` from every state body, so an append
response tells the client of a mark from outside (A3) even with no queued
write. When it sees a new explicit `done` from a different source, it adds the
same notice.

Explicit CLI writes for the current session (the agent runs
`hyperspace status ...`) take the session's sync lock with a wait of at most
10 seconds, delete any queued row, and send directly with the same retry
rules. Then they set `turn_declared`. Writes for other sessions use
`GET .../state` for the base, and retry once on `409`.

### Notices during a turn

The existing notices print at UserPromptSubmit, which is too late for A3. A
synchronous `PostToolUse` entry runs `hooks/notice.sh`. The script checks for
the flag file `$XDG_STATE_HOME/hyperspace/notify/<session id>` with shell
only, and exits at once if it is not there. If it is there, it runs
`hyperspace hook` with the payload, and the client prints the pending notices
as `hookSpecificOutput.additionalContext` and removes the flag. Thus a tool
call costs one `test -e` in the usual case. `State.add_notice` creates the
flag file.

### CLI

- `hyperspace status [STATUS] [--session SID] [--reason R] [--wake W] [--note TEXT] [--by-user] [--json]`.
  With no `STATUS`, it prints the current status. `--session` defaults to
  `CLAUDE_CODE_SESSION_ID`. The source kind is `agent` when
  `CLAUDE_CODE_SESSION_ID` is set and `--by-user` is not given, else `user`.
  `source_session` is `CLAUDE_CODE_SESSION_ID` when it differs from the
  target.
- `hyperspace done [SID ...] [--note TEXT] [--by-user]`: explicit `done` for
  each session (the current session when none is given).
- `hyperspace reopen SID [--note TEXT]`: explicit `ready`, reason `idle`.
- `hyperspace ls --open [--json] [--stalled-after MIN]`: `GET /sessions` with
  `open=1`. Columns: `SESSION`, `STATUS`, `WHERE`, `AGE`, `TITLE`, `NOTE`.
  - `STATUS` shows `ready:asked`, `waiting:local`, and so on. It shows
    `interrupted` for `working` with liveness `stopped`, `stalled` for
    `working`, `live`, and `last_append` older than the limit (default 30),
    `unknown since HH:MM` for liveness `unknown`, `-` for no status, and
    `done, not closed: <reason>` for `close_blocked`.
  - Sort: `ready`, then `interrupted` and `stalled`, then `working`,
    `waiting`, no status; then by `last_append`, newest first.
  - `NOTE` is the note, or the excerpt's first line, cut to fit.
  - `--json` prints the session summaries unchanged.
- `hyperspace tail SID [-n N]` (default 3 turns): reads the active branch of
  `transcript.jsonl` (reuse `teleport._active` and `_log_bytes`), and prints
  `ClaudeCode.format_tail(data, n)`: user prompts and assistant text in full,
  one line for each tool call. The server records the read in the audit.

### Adapter

- `excerpt(transcript: Path) -> str | None`: the text blocks (not thinking) of
  the last `assistant` record in the last 256 KiB of the file, joined and cut
  to `MAX_STATUS_EXCERPT`.
- `in_flight() -> int`: see "Transitions".
- `format_tail(data: bytes, turns: int) -> str`.
- `live_sessions() -> list[str]`: the session ids that `is_running` would
  find, from one pass over `<config dir>/sessions/*.json` (step 4 uses it).

### Plugin

- `commands/done.md`: runs `hyperspace done --by-user --note "$ARGUMENTS"` and
  tells the agent to report the result in one line and not to start new work.
- `skills/session-status/SKILL.md`: when the work is finished, run
  `hyperspace done --note "<outcome>"`; when waiting for an event, run
  `hyperspace status waiting --wake local|message --note "..."`; when a
  question is open, do nothing (the hooks set `ready`). It tells the agent
  that `done` does not close the UI.
- `skills/session-overview/SKILL.md`: use `hyperspace ls --open --json`, then
  `hyperspace tail` for sessions that need detail; propose priorities; mark
  sessions `done` only after the user approves.
- The main spec's decision "v1 has no skill" changes; update that line.

### Capability gating

- Hooks and sync: if the server has no `session-status`, keep wanted rows and
  send nothing. Log once a day.
- CLI: `api.require_capability(CAP_SESSION_STATUS)` before writes and before
  `ls --open`; the error says that the server needs an upgrade.

### Facts to confirm with Claude Code

Unit tests use recorded payloads. These facts need one live check each,
recorded in the test module docstring, before the step is done:

1. A turn that a cron (CronCreate or `/loop`) starts fires `UserPromptSubmit`.
   If not, such a turn sets no `working` status, and a `done` session does not
   reopen on a cron (U5 then needs a different trigger; note it in the spec).
2. The `Notification` payload for a permission prompt has
   `notification_type: "permission_prompt"`.
3. `PreToolUse` and `PostToolUse` fire for `AskUserQuestion`, and
   `PostToolUse` fires after the user answers.
4. A hook process has `CLAUDE_JOB_DIR` in a background job.
5. A synchronous `PostToolUse` hook's `additionalContext` reaches the model.
6. A hook process has `CLAUDE_CODE_SESSION_ID`. `notice.sh` uses it for its
   fast path.
7. Claude Code lists the plugin's `/done` command and both skills. Prettier
   wraps their `description` over several lines, which is valid YAML.

Live checks use the real `~/.claude`, because the Claude Code credentials are
there. Follow the memory note on the live test hazard: use a throwaway server,
make the sweep a no-op (pre-set `last_sweep` in the test state, or a
`HYPERSPACE_CLI` wrapper that drops `--all`), and afterwards check that every
session in `~/.claude/projects` is still there.

The step 2 code does not depend on one answer where that is cheap. All
assumptions 1 to 3 are in `status.transition`:

- Fact 1: a `PreToolUse` with the status `ready` or `waiting`, and no
  declaration in the turn, also sets `working`. Thus a turn that a cron starts
  shows as `working` also with no `UserPromptSubmit`. But only a prompt
  reopens a `done` session: if a cron turn fires no `UserPromptSubmit`, a cron
  does not reopen it (U5).
- Fact 2: with a different payload, a permission prompt does not set `ready`;
  the turn stays `working`.
- Fact 4: without `CLAUDE_JOB_DIR`, the Stop hook sets `ready`, not `waiting`
  with wake `local`.
- Fact 6: without `CLAUDE_CODE_SESSION_ID`, `notice.sh` runs the CLI when any
  session has a flag, and the CLI reads the session from the hook's JSON.

These seven checks are open. The user does them, because they need live Claude
Code sessions.

### Step 2 as built

The code follows this step, with these changes and additions:

- A queued write keeps `base_seq`: the `seq` of the status that the hook saw.
  The sync sends it on that base, not on the newest status that the sync saw
  in the append responses. Otherwise a stale `working` would write over a
  `done` from outside, because the append response tells the client of the
  `done` before the queued write goes out.
- An automatic write that gets `409 stale_status` tries once more only when
  the current event is an automatic write of this host (two hooks of one
  session during one sync). An explicit write tries once more, but an agent's
  write does not write over an explicit `done` from someone else; the CLI then
  exits 1 and the agent gets the notice.
- If the current status is `done`, only `UserPromptSubmit` writes. This
  explicit reopen also drops the pending `done` notices, so that the hook does
  not print "Finish the current step and stop" for the prompt that reopens the
  session (for example after a sweep of the stopped session saw the mark).
- Events with `agent_id` (a subagent's tool calls) do not change the status.
- `turn_declared` is cleared at `Stop` and at `UserPromptSubmit`, so that a
  declaration lasts one turn also when a turn starts with no prompt.
- The sync, not the hook, reads the excerpt, when it sends the write. The
  excerpt is the text of the last message: the text blocks after the last tool
  call or tool result.
- `notice.sh` runs `hyperspace hook --notices`, which only prints the notices.
  For a subagent's tool call (`agent_id` in the payload), it prints nothing
  and keeps the notices, because the context goes to the subagent and not to
  the main agent. The flag file is
  `$XDG_STATE_HOME/hyperspace/notify/<session id>`.
- A notice has an optional key in the `notices` table. `add_notice` does not
  add a notice if an earlier notice of the same kind, delivered or not, has
  the same key; with no key, the text is the key. A lease notice uses its
  text, so that a sync that sees the same problem again does not repeat it. A
  `done` notice uses the event's `seq`, so that each new `done` mark gives one
  notice, also when its host, source and note are the same as an earlier mark.
- `SessionStart` does not print a pending `done` notice ("Finish the current
  step and stop"), because no turn runs at a start. The notice stays pending,
  and `SessionStart` prints only that the session is marked done and that a
  new prompt reopens it. After a resume, the prompt that reopens the session
  drops the notice. After a compaction in a turn, the next tool call of the
  turn gets it.
- `hyperspace ls --status STATUS` (repeatable) shows sessions with a status,
  for example `done` sessions that are not closed, which `--open` leaves out.
  `hyperspace status` with no `STATUS` shows the status and the liveness.
  `hyperspace tail` takes the current session when no id is given.
- A CLI write for a different session that has a local copy also saves the
  event in that session's local record. An explicit `done` there also adds the
  notice (A3, U4), because a later sync does not see the saved event as new.
  Thus a mark from a different session or terminal on the same host reaches
  the live session. A write for the current session adds no notice.
- The test dependencies have no `pytest-mock`. The send and retry tests use
  the in-process server instead of mocks. `tests/conftest.py` removes
  `CLAUDE_CODE_SESSION_ID` and `CLAUDE_JOB_DIR`, so that the tests behave the
  same inside a Claude Code session.
- Test names: the stale-write tests are in `test_e2e_client_status.py`
  (`test_remote_done_is_not_overwritten`,
  `test_agent_does_not_write_over_an_outside_done`,
  `test_status_cli_defaults_to_env_session`).

### Tests

- `test_unit_client_status.py`: `test_transition_table` (parametrized over the
  table above), `test_wanted_status_keeps_latest`,
  `test_stale_against_outside_done_adds_notice`,
  `test_stale_automatic_write_is_dropped`, `test_explicit_write_retries_once`,
  `test_in_flight_reads_job_state`, `test_in_flight_missing_is_zero`,
  `test_excerpt_skips_thinking`, `test_format_tail`.
- `test_unit_client_hooks.py`: `test_hook_records_wanted_status`,
  `test_notification_event_is_handled`.
- `test_unit_plugin_hooks.py`: `test_notice_sh_fast_path` (no flag: exits 0
  with no output and does not run the CLI), `test_notice_sh_prints_notice`.
- `test_e2e_client_status.py`: `test_hook_sequence_sets_status` (prompt, tool
  calls, stop, against the in-process server),
  `test_remote_done_is_not_overwritten` (a second host marks done; the next
  automatic write fails; the notice is queued),
  `test_done_then_prompt_reopens`, `test_ls_open_columns`,
  `test_status_cli_defaults_to_env_session`, `test_tail_reads_active_branch`,
  `test_old_server_without_capability`.

### Done when

- [ ] All tests pass, also with `UV_PYTHON=3.12`; ruff and pyright are clean.
- [ ] The five facts are confirmed, or the spec records what differs.
- [ ] `hyperspace ls --open` on the workstation lists live sessions with the
      right status after a day of use.
- [ ] Commit type `feat(client)`, plus `feat(plugin)` changes in the same
      series.

## Step 3: backfill

### Goal

Each older session sets its own status (B1). The command works on any host
where the sessions are on disk. On a runner, run it with `kubectl exec`; this
needs the client in the runner image, but no launcher.

### Files

- `client/src/hyperspace_client/backfill.py` (new), `cli.py`.
- Tests: `tests/test_unit_client_backfill.py`,
  `tests/test_e2e_client_backfill.py`, and a fake `claude` script in
  `tests/fake_claude.py`.

### Behavior

`hyperspace backfill-status [--since 30d] [--rate 60] [--limit N] [--dry-run] [--timeout 600]`:

1. Needs `session-status` on the server.
2. Candidates: `adapter.discover()` on this host, with a last record in the
   window. Ask the server for their state with
   `GET /sessions?status=none&since=...&host=<this host>`, and keep only the
   candidates in that answer. A session with any status is skipped.
3. For each candidate, oldest first:
   - Skip it if `is_running` is true.
   - Run, in the session's last workspace (from local state, or the first
     `cwd`):
     `claude --resume <sid> -p <MESSAGE> --restricted --tools Bash --strict-mcp-config --permission-mode dontAsk --allowedTools 'Bash(hyperspace status:*)' 'Bash(hyperspace done:*)'`.
     The options remove the user's settings, the MCP servers, and all tools
     except Bash, because no user watches the session and its transcript can
     hold text from other sources, with `--timeout` seconds. The binary is
     `$HYPERSPACE_CLAUDE` or `claude` on the `PATH`.
   - Afterwards, read the session state. Log `set`, `no status`, `timeout` or
     `error` for the session.
   - Wait so that at most `--rate` sessions start each hour.
4. `--dry-run` prints the candidates and does nothing else.
5. Print a summary: counts for each result, and the sessions with no status.

The message (`MESSAGE`):

> Hyperspace now records a status for each session: working, waiting, ready,
> or done. This session has no status yet. Read the end of this conversation
> and decide which status is correct. If the work is finished and the user
> probably does not need to return here, run
> `hyperspace done --note "<one-line outcome>"`. If you wait for an event such
> as a CI run or a review, run
> `hyperspace status waiting --wake message --note "<what you wait for>"`. If
> the user must decide or do something, run
> `hyperspace status ready --reason idle --note "<what the user must do>"`. Do
> not change files, run other commands, or continue the work. Reply with one
> line that says which status you set and why.

### Tests

- `test_unit_client_backfill.py`: `test_candidates_skip_status_and_live`,
  `test_rate_limit_spacing` (fake clock and sleep), `test_dry_run`.
- `test_e2e_client_backfill.py`: `test_backfill_with_fake_claude` (the fake
  binary checks its arguments and runs `hyperspace status ...` against the
  in-process server), `test_backfill_timeout_is_reported`.

### Done when

- [x] Tests pass; ruff and pyright are clean.
- [ ] One manual run with `--limit 3` on the workstation sets a sensible
      status for three old sessions (with the live test precautions of step
      2).
- [x] Commit type `feat(client)`.

### Step 3 as built

- `hyperspace backfill-status [--since 30d] [--rate 60] [--limit N] [--timeout 600] [--dry-run] [--retry]`.
- Candidates are newest first, not oldest first: a run that stops or has a
  `--limit` then covers the sessions that the user most likely still needs.
- The server check is one `GET .../state` for each local candidate, not one
  `GET /sessions?status=none` list. The list has a maximum of 1,000 entries
  and filters `since` by the server's activity time, which for backfilled
  sessions is the upload time, not the time of the work. The window uses the
  transcript's modification time.
- Skip reasons: `messaged before`, `live`, `no workspace` (the workspace
  directory is not there), `excluded` (`exclude_cwd`), `not on server`,
  `has status`, and `lease not here` (the lease is not held by this host).
  `run` checks `live` and the server state again just before each resume,
  because the state can change while the throttle waits.
- The client records each attempt in its key-value state
  (`backfill:<harness>:<sid>`) before the resume starts, and adds the result
  after it. `--retry` ignores these records.
- The outcome is `set` only if the latest event is an explicit write from the
  agent. A `ready` that the Stop hook writes when the agent sets nothing
  counts as `no status`.
- The resume runs in its own process group, with `CLAUDE_CODE_SESSION_ID` and
  `CLAUDE_JOB_DIR` removed from its environment, so that a backfill that runs
  inside a Claude Code session does not act on that session. A timeout stops
  the whole group.
- Tests: `tests/test_unit_client_backfill.py`,
  `tests/test_e2e_client_backfill.py`, and `tests/fake_claude.py`. The e2e
  tests use an in-process runner in place of the binary, because a subprocess
  cannot reach the in-process server; the unit tests run `run_claude` against
  `fake_claude.py`.

## Step 4: launcher, workstation mode

### Goal

One `hyperspace launcher` process for each host sends the heartbeat and closes
`done` background jobs (U6, U3, U4).

### Files

- `client/src/hyperspace_client/launcher.py` (new), `cli.py`, `hooks.py`,
  `config.py` (`launcher_idle_exit`, `heartbeat_interval`, `claude_bin`).
- `docs/launcher.md` (new): the modes, and example units for systemd and
  launchd.
- Tests: `tests/test_unit_launcher.py`, `tests/test_e2e_launcher.py`.

### Behavior

`hyperspace launcher [--stay] [--runner] [--if-absent]`:

- **Lock**: an exclusive, non-blocking `flock` on
  `<state dir>/locks/launcher--<host>--<config dir hash>.lock`. If another
  process holds it, exit 0 (`--if-absent` makes this the normal path, with no
  log line).
- **Capabilities**: needs `liveness`. Without it, log once and exit.
- **Tick** (every `heartbeat_interval`, default 60 s):
  1. `live = adapter.live_sessions()`.
  2. `PUT /hosts/self/heartbeat` with `live`, the interval, and `unclosed`
     (see the close sweep).
  3. Run the close sweep.
  4. Idle exit (not with `--stay` or `--runner`): if `live` was empty for
     `launcher_idle_exit` (default 600 s), send a heartbeat with an empty list
     and `final: true`, and exit.
- **Close sweep**: for each `~/.claude/jobs/<short>/state.json` with a
  `sessionId` that is not in `live`, and whose server status is `done` (one
  `GET /sessions?status=done&host=<this host>` for each tick):
  1. Sync the session under its sync lock, as a surrender does (all local
     bytes on the server, including the last record). If that fails, skip the
     job for this tick.
  2. Run `claude rm <short>` (`claude_bin`).
  3. If it fails, record `UnclosedSession` with the first line of its error
     output (for example unpushed commits) for the next heartbeats, and do not
     run `claude rm` for that job again until its state changes.
- **Hooks**:
  - SessionStart starts `hyperspace launcher --if-absent` detached, with
    `--unlock` at a start or resume (same keyring rule as the checkpoint).
  - The checkpoint sync for `--event SessionStart` and `--event SessionEnd`
    sends a heartbeat after the sync. For SessionEnd, the list leaves out the
    ending session, because its process can still be alive when the hook runs.
  - SessionEnd, with `CLAUDE_JOB_DIR` set and a last seen status of `done`,
    starts `hyperspace launcher --close-job <short> --wait-pid <CLAUDE_PID>`
    detached. It waits up to 5 minutes for the pid to stop, then runs the
    close steps for that job only. The sweep covers a missed or failed run.
- **Off switch**: `HYPERSPACE_LAUNCHER=off` makes the hook start nothing and
  the launcher exit at once. Live tests must set it, because the close sweep
  runs `claude rm` on real jobs.

### Tests

- `test_unit_launcher.py`: `test_single_instance_lock`,
  `test_idle_exit_sends_final_heartbeat` (fake clock),
  `test_stay_never_exits`, `test_close_sweep_skips_live_and_not_done`,
  `test_close_sweep_syncs_before_rm` (fake `claude` records calls),
  `test_rm_failure_is_reported_once`, `test_off_switch`.
- `test_e2e_launcher.py`: `test_heartbeat_makes_session_live_then_stopped`,
  `test_close_job_waits_for_pid`, `test_unclosed_shows_in_ls`.

### Done when

- [ ] Tests pass; ruff and pyright are clean.
- [ ] On the workstation, `hyperspace ls --open` shows `live` for running
      sessions and `stopped` within one minute after they end.
- [ ] A `done` background job leaves the `claude agents` view after its
      process stops, and a job with unpushed commits stays, with
      `done, not closed: ...` in `ls`.
- [ ] `docs/launcher.md` has the systemd and launchd examples.
- [ ] Commit type `feat(client)`.

### Step 4 as built

The code follows this step, with these changes:

- **close_blocked**: the server rule changed (see step 1, "Liveness rules"),
  not the client. A job that stopped before any heartbeat listed it now shows
  `done, not closed: <reason>`. The store also drops `unclosed` entries for
  sessions that the host cannot read.
- **Close checks**: `close_job` holds the session's sync lock, checks that the
  session does not run, asks the server for the status, and acts only on
  `done`. It backs up the session with `teleport.back_up` (taken out of the
  surrender, which uses it too), checks again that the session does not run,
  and runs exactly `claude rm <job>`. The job name must match
  `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`, so it cannot be read as an option.
- **Rechecks**: the sweep asks the server for the status of a stopped job when
  it first sees the job, when its `state.json` changes, and every 15 minutes
  (`RECHECK`), not at each tick.
- **Refusals** are in the client state (`launcher_unclosed`), by job name,
  with the `state.json` mtime. A refusal holds until the job goes away or its
  state changes. Every heartbeat, also from a hook, sends them, because a
  heartbeat replaces the host's earlier list. The reason is the first line of
  the output of `claude rm`, with escape sequences removed (`printable` now
  removes whole ANSI sequences, not only the control characters).
- **`--close-job`** takes no `--wait-pid`. It polls the adapter's live check
  for the job's session, which also covers a reused pid.
- **Heartbeat from a hook**: the checkpoint sync for `--event SessionStart`
  and `--event SessionEnd` sends it after the sync, also when another process
  held the session's lock.
- **Token**: before each tick, the launcher reads the token cache again
  (`cache-only`), which keeps the cache alive, and takes a new token after a
  new login.
- **Docs**: the systemd and launchd examples are in `docs/deploy.md`, not in a
  new `docs/launcher.md`. The client is Unix-only (`fcntl`), so the spec no
  longer names the Task Scheduler.
- **Config**: `heartbeat_interval`, `launcher_idle_exit`, and `claude_bin`
  (`HYPERSPACE_CLAUDE` takes precedence; the backfill uses it too).
- **Tests**: all launcher tests are in `tests/test_e2e_launcher.py`, against
  the in-process server, with a fake `claude rm` and a fake clock. The hook
  tests are in `tests/test_unit_client_hooks.py`, and the server rule tests in
  `tests/test_unit_store.py`.

## Step 5: launcher, runner mode

### Goal

A runner resumes exactly the sessions that need a process (R1 to R3), and
never resumes a `done` session.

### Files

- `client/src/hyperspace_client/launcher.py`, `config.py`
  (`launcher_claude_args`, from `HYPERSPACE_LAUNCHER_CLAUDE_ARGS` as a JSON
  list).
- `docs/launcher.md`: the runner section.
- Tests: `tests/test_unit_launcher_runner.py`,
  `tests/test_e2e_launcher_runner.py`.

### Behavior

`hyperspace launcher --runner` never exits when idle. At boot:

1. Run the existing `restore --held` logic (`cli._restore_held`).
2. `GET /sessions?lease_host=<this host>&open=1`.
3. For each session, decide with `needs_process(summary, running)`:
   - `working` and not running here: resume with the prompt "The runner
     restarted while you worked. Check the state of your work, then continue."
   - `waiting` with wake `local` and not running: resume with the prompt "The
     runner restarted. Your crons, Monitors and background tasks stopped. Set
     them up again if you still need them, then continue to wait."
   - Anything else, or no status: do not resume. Log sessions with no status
     (they are from before the client upgrade; the backfill covers them).
4. Resume: check `is_running` again, then in the session's workspace run
   `claude --bg --resume <sid> <launcher_claude_args...> <prompt>`.

At each tick, after the heartbeat and the close sweep, run step 3 again for
sessions that are not live, with a backoff: after 5 failed resumes of one
session, stop and record an `UnclosedSession`-style reason in the heartbeat
(`reason: "resume failed"`) for that session.

### Facts to confirm

- `claude --bg --resume <sid> <prompt>` continues the same session id and
  starts with the prompt.
- On a runner with a fresh harness config directory, `restore --held` and then
  `claude --bg --resume` work with no job state.

### Tests

- `test_unit_launcher_runner.py`: `test_needs_process_table` (parametrized),
  `test_running_session_is_not_resumed`, `test_resume_backoff`.
- `test_e2e_launcher_runner.py`: `test_boot_resumes_interrupted_only` (fake
  `claude`, in-process server, sessions in each status).

### Done when

- [ ] Tests pass; ruff and pyright are clean.
- [ ] The two facts are confirmed in a throwaway runner pod.
- [ ] Commit type `feat(client)`.

### Step 5 as built

The code follows this step, with these changes:

- **Restore**: the `restore --held` logic moved from `cli.py` to
  `teleport.restore_held`, so that the launcher can use it. The launcher's
  fallback workspace is its working directory. A failed restore is tried again
  at later ticks (see "Fixes after review").
- **Decision**: `needs_process(summary, host, running)` returns the first
  message or None. It also returns None for a lease that is not `held` by this
  host, and for a session that the server shows as `live` on any host (see
  "Fixes after review").
- **Resume**: `Launcher.resume` uses the workspace from the local session
  record only, never a path from the server, and only for a session with a
  local transcript under that id (so the id is a valid UUID file name). It
  checks `is_running` just before the resume. The command is
  `[claude, *launcher_claude_args, "--bg", "--resume", sid, message]`, with no
  shell, run by `backfill.run_claude` (no session variables, own process
  group, a 120 s timeout).
- **Backoff**: at most 5 resumes of one session in an hour, successful or not,
  because a session that stops at once after a successful resume also makes a
  loop. The launcher logs a failure; it sends no reason to the server. The
  `unclosed` list of the heartbeat is for `done` sessions, and the client
  shows `close_blocked` only for them.
- **Config**: `check_claude_args` refuses a list with a non-string or empty
  entry, and options that the launcher sets (`--bg`, `--background`,
  `--resume`, `-r`, `--continue`, `-c`, `--fork-session`, `--session-id`,
  `--print`, `-p`), also in the `--option=value` form.
- **Docs**: the runner section is in `docs/runners.md` ("At boot: the
  launcher"), not in a new `docs/launcher.md`.
- **Tests**: in `tests/test_e2e_launcher.py`, with the step 4 fixtures:
  `test_needs_process_table`, `test_boot_resumes_interrupted_only`,
  `test_running_session_is_not_resumed`, `test_resume_limit`,
  `test_resume_needs_a_local_copy_and_workspace`,
  `test_boot_restores_held_sessions`, `test_launcher_claude_args_config`.

## Fixes after review

A review of steps 1 to 5 found these problems. Each fix has a test that fails
without it.

- **Close of a reopened session**: the backup before `claude rm` can send a
  queued write that reopens the session. `close_job` now refuses while a
  queued write that is not `done` waits, and asks the server for the status
  again after the backup. The SessionEnd trigger also refuses while such a
  write waits. Tests: `test_close_skips_a_queued_reopen`,
  `test_close_asks_again_after_the_backup`,
  `test_session_end_keeps_a_job_with_a_queued_reopen`.
- **Heartbeat source**: a heartbeat has an optional `source`, the first 16 hex
  digits of the SHA-256 of the resolved config directory path
  (`launcher.source_id`, which also names the launcher lock).
  `host_heartbeats` and `session_liveness` have a `source` column in their
  primary keys, and `_liveness` reads the heartbeat of the row's source.
  Migration 3 changed in place, because it is not released. The launcher keeps
  refused jobs under `launcher_unclosed:<source>`. Test:
  `test_config_dirs_of_one_host_keep_their_own_liveness`.
- **Writes during a long sync**: `set_status` queues the write if it does not
  get the sync lock in `STATUS_LOCK_WAIT`. `State.save_session` is an upsert
  that does not change `status`, `done_seen` and `declared` of a row that
  exists. `State.save_status` (which never replaces a higher `seq`) and
  `State.set_declared` write them. Tests:
  `test_write_during_a_long_sync_is_queued`,
  `test_a_stale_record_does_not_undo_status_fields`.
- **Backfill scope**: `backfill.run_turn` sets
  `HYPERSPACE_STATUS_SELF_ONLY=1`. Then `status`, `done` and `reopen` accept
  only the session in `CLAUDE_CODE_SESSION_ID`, one target, and no
  `--by-user`. Tests: `test_backfill_turn_limits_status_writes`,
  `test_backfill_turn_sets_only_its_own_status`.
- **`claude --bg`**: the launcher runs it with `launcher.run_bg`. The output
  goes to a temporary file, the launcher waits for the command and not for the
  end of its output, and a timeout stops only the command, because the Claude
  Code daemon that it starts can keep the output open and runs every job.
  Tests: `test_run_bg_does_not_wait_for_the_daemon`,
  `test_run_bg_timeout_stops_only_the_command`.
- **Live sessions**: `ClaudeCode.live_sessions()` skips invalid session ids,
  because the server refuses a heartbeat with one. Test:
  `test_live_sessions_skips_invalid_ids`.
- **Lists**: `list_sessions` reads the latest status event once for each row.
- **Declaration and a prompt in one turn**: a question or a permission prompt
  after a declaration writes `ready`, reason `asked`, and the next tool call
  writes `working`. Then the Stop hook wrote nothing, so the session stayed
  `working`. The `turn_declared` flag is now `sessions.declared`: the JSON of
  the declared request (`State.set_declared`). At Stop, if the declaration is
  set and the latest known status is not explicit, the hook queues the
  declaration again, as an explicit write. Test:
  `test_declaration_holds_over_a_prompt_in_the_turn`.
- **Live copy**: `_liveness` read only the row with the latest `last_listed`.
  When a copy on a second host listed the session and then stopped, the
  session showed `stopped` while the first copy still ran, and a runner could
  resume its own copy. Now a row whose heartbeat lists the session and has not
  expired makes it `live`; only with no such row does the latest row give
  `stopped` or `unknown`. `close_blocked` comes from the heartbeat of the row
  that gives the state. Test: `test_stopped_copy_does_not_hide_a_live_copy`.
- **Live on the runner's own host**: `needs_process` skipped only a session
  that was `live` on a different host. A different harness config directory on
  the runner can run the session, and the launcher's `is_running` does not see
  that process, so the runner made a second copy. Now it skips a session that
  is `live` on any host. The tick sends the launcher's heartbeat before the
  resumes, so a `live` from the launcher's own heartbeat before a restart
  clears first. Tests: `test_needs_process_table`,
  `test_session_live_from_another_config_dir_is_not_resumed`,
  `test_own_live_from_before_a_restart_does_not_block_the_resume`.
- **Restore retry**: `Launcher.boot` ran `restore_held` once before the first
  tick, and only logged a failure. On a runner with no local transcripts, a
  server that was not available at boot, or a failed restore of one session,
  left held sessions with no local copy, and `resume` refused them until the
  launcher restarted. Now `restore_held` returns a `HeldRestore` with the
  restored count and the failed session ids, and `Launcher.restore` runs at
  the runner's ticks, after the heartbeat and the sweep and before the
  resumes. It runs at the first tick, and again after a failure, after one
  tick interval, then a wait that doubles to at most `RESTORE_RETRY_MAX` (one
  hour), until a restore has no failure. A session with a pending restore
  marker is in the retry, because `restore_held` includes it. Tests:
  `test_failed_boot_restore_is_retried` (a failed list and a failed session
  restore), `test_restore_retry_backs_off`,
  `test_boot_restores_held_sessions`.

## Step 6: outside this repo

- **subspace**: treat the webhook response by HTTP status (2xx accepted, 4xx
  refused, 5xx or no answer queued and retried), send non-delivery and queued
  notices to the sender, and add a `reopen` option with a reason to a send.
- **Launcher webhook**: a later step adds an HTTP endpoint to
  `hyperspace launcher --runner` for subspace wake requests. It refuses a wake
  for a `done` session with `409` and the outcome note, and handles `reopen`
  with an explicit `ready` event.

## Decisions made in this plan

- The CLI is `hyperspace status [STATUS] --session SID`, not
  `hyperspace status <sid> <status>`, so that an agent can run it with no
  session id. Update the spec's "Overview" section in step 2.
- `/done` passes `--by-user`, so that its source is `user` although the agent
  runs the command.
- The state body carries the status without its excerpt, because every append
  response has the state body. List entries carry the excerpt.
- The state body carries `last_append`, and the client decides `stalled`. The
  server stores no stalled limit.
- A heartbeat records liveness only for sessions that exist and that the host
  can read, and skips the rest with no error.
- The heartbeat carries `unclosed` and `final`. `Liveness.close_blocked` shows
  the reason for "done, not closed".
- The manifest revision ignores status, liveness and `last_append`.
- Status writes need an existing session. The client sends them after the
  sync's appends.
- Notices during a turn use a synchronous `PostToolUse` hook with a shell fast
  path on a flag file, so that a usual tool call does not start Python.
- A runner does not resume sessions with no status.
- The client is Unix-only today (`fcntl` locks). The spec's mention of the
  Windows Task Scheduler stays a later option; the launcher uses the same
  `flock` as the sync lock.
