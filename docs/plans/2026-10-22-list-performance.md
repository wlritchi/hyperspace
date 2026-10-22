# Session list performance

## Problem

On a server with about 5,700 sessions and slow storage, `GET /sessions` takes
5 to 35 s when its pages are not in the cache, and `GET /workspaces` up to 35
s. While a list runs, every other request waits: appends, heartbeats, status
writes, and the hook's resume check. One stall of 81 s was seen, in which the
sync and the launcher timed out after 30 s. With a warm cache the same list
takes 0.03 to 0.05 s.

## Causes

1. `Store.list_sessions` reads every session of the account:
   `SELECT * FROM sessions WHERE account=? ORDER BY COALESCE(last_activity, created) DESC`
   with `fetchall()` and no `LIMIT`. The plan walks the primary key index (in
   session id order) and sorts in a temporary B-tree. For each row that it
   examines it runs 3 to 5 more queries: the session row again, the latest
   status event, the liveness, the placements, and the latest metadata event.
   The `status`, `open`, liveness, `host` and `workspace` filters run in
   Python after this work. Thus a filtered list examines all sessions.
2. The SQLite page cache is the default (about 2 MiB) on one connection, and
   the kernel page cache of the pod can be reclaimed. A list after idle time
   reads its pages from storage again.
3. `Store` has one connection behind one `RLock`, and `snapshot()` holds a
   `BEGIN IMMEDIATE` write transaction for the whole list. A slow list stops
   all other requests. A client timeout does not stop the server's work, and a
   retry waits behind it.
4. Periodic callers list up to 1000 sessions with full scans: the sweep's
   `refresh_states` (`host=<self>`), `restore --held`, and the stub search.
   With more than 1000 sessions they also miss the older sessions, because the
   API has no cursor.

A synthetic database of 6,000 sessions showed: a list of 50 runs 253
statements and about 490 page reads when cold; a list of 1000 runs 5,003
statements and about 1,590 page reads; `status=done` runs 18,129 statements.

## Changes

Each step is one or more commits. Steps 1 and 2 change only the server, step 3
the protocol and the server, step 4 the client and the plugin, and step 5 all
of them. A new client must work with an old server (use a capability, as for
session status), and an old client must work with a new server.

### Step 1: reads do not stop writes

- Read requests use their own connections (WAL mode lets readers run beside
  the writer), for example one read connection for each thread, without the
  writer's lock. Writes, and reads that also write (for example the audit of a
  read), stay on the writer connection.
- `snapshot()` (and every other read-only transaction) uses a deferred
  transaction, not `BEGIN IMMEDIATE`.
- `PRAGMA cache_size` of 128 MiB on the writer connection, and a suitable size
  on each read connection (keep the total well under the pod's memory limit;
  make it configurable).
- Log the duration of each request, and the time it waited for the store lock,
  at INFO for slow requests (for example over 1 s).

Done (`201a56f`). As built:

- Requests that only read the index (session and workspace lists, hosts,
  session state) use a pool of read connections. Each read is one deferred
  transaction on a `query_only` connection. Writes, and reads that write an
  audit row (manifest, log and blob content), stay on the writer connection
  behind the store lock.
- The writer connection has a page cache of 128 MiB.
  `HYPERSPACE_READ_CONNECTIONS` (default 8) and `HYPERSPACE_READ_CACHE_BYTES`
  (default 16 MiB) set the pool.
- The server logs each request with its duration and its waits for the writer
  and for a read connection, at INFO when it takes longer than
  `HYPERSPACE_SLOW_REQUEST_SECONDS` (default 1).

### Step 2: the list query does work only for the rows it returns

- A migration adds an index for the sort order, with the same expression as
  the `ORDER BY`, for example
  `sessions (account, COALESCE(last_activity, created))`, and an index on
  `placements` for the `host` filter.
- A migration adds the current summary of each session to its row (or to a
  `session_current` table): the latest status (status, reason, seq, explicit),
  the latest metadata needed for the list (title, workspace), and the last
  placement. Every write that adds a status event, a metadata event or a
  placement updates the summary in the same transaction. The event tables stay
  append-only and remain the source of truth. The migration fills the summary
  from the events. Check the purge script (`scripts/purge.py`) and every other
  writer.
- `list_sessions` filters in SQL (`harness`, `lease_host`, `host`,
  `workspace`, `status`, `open`, `since`), sorts with the index, and reads in
  batches until it has `limit` rows. It computes the remaining per-row data
  (liveness, excerpt) only for the rows that it returns. Remove the duplicate
  fetch of the session row.
- `list_workspaces` uses the summary or an aggregate query, not a scan with
  work for each session.
- `open` keeps its meaning (not `done`). `status` accepts more than one value,
  for example `status=working,waiting,ready`.

Done (`96fd4ee`, `f2611da`). As built:

- Migration 4 adds to each session row a copy of its latest status and of the
  body of its latest metadata event (column `metadata`), and fills them from
  the events. `add_status` and `add_metadata` update the copies in the same
  transaction. The last placement is not copied: only the returned rows read
  it.
- Indexes: `sessions_recent` (account, `COALESCE(last_activity, created)`,
  status, lease state, lease host, harness), `placements (host, session)` and
  `(workspace, session)`, and `metadata_events (session, host)` for the read
  access check.
- `list_sessions` applies all filters except `title` and `liveness` in SQL,
  with the limit in SQL. With `title` or `liveness`, it reads in batches until
  it has the limit. `list_workspaces` is one aggregate query, and counts each
  session once in a workspace.
- `status=` and `liveness=` also take comma-separated values.
- On a synthetic index of 6,000 sessions, a list of 50 went from 253 to 153
  statements, a list of 1000 from 5,003 to 3,003, and `host=` from 24,353 to
  1,053. The workspace list went from 6,003 statements to 3.

### Step 3: a cursor for the session list

- `GET /sessions` accepts a cursor (for example
  `before=<sort key, session id>`, opaque to the client) and returns the
  cursor of the next page. A new capability tells the client that the server
  has it.

Done (`487ea7b`). As built:

- `cursor=` and `next`, with the capability `list-cursor` (`CAP_LIST_CURSOR`)
  and the optional `next` field of `SessionList`.
- The order is `COALESCE(last_activity, created)` DESC, then harness and
  session id DESC, so it is total. The cursor is URL-safe base64 of
  `[1, sort key, harness, session id]`, with no row ids. A cursor that the
  server cannot read gives `400 bad_request` "invalid cursor".
- Without the `title` and `liveness` filters, `next` is absent exactly at the
  end of the list. With them, the last page can be empty.
- A session that gets new activity during a pass moves to the start of the
  list, and the pass does not return it again (or at all, if it did not reach
  it yet).

### Step 4: the client

- `restore --held`, `refresh_states` and the stub search read every page (with
  the cursor when the server has it), so that they do not miss sessions after
  the newest 1000.
- List calls get a longer read timeout (for example 120 s); hook calls keep
  their short timeouts. A list is not sent again after a timeout. A timeout
  gives a clear message ("the server did not answer in time"), not the bare
  httpx error.
- `hyperspace ls --status` accepts more than one value. The session-overview
  skill asks for `--status working,waiting,ready` instead of `--open`, so it
  does not get sessions with no status.
- Optional: cache the server's capabilities, so that `ls --open` does not need
  a `/whoami` call first.

Done. As built:

- `Api.all_sessions` reads every page, with pages of 1000 sessions. It sends
  `cursor=` only to a server with `list-cursor`; to an older server it sends
  one request with `limit=1000`. It stops at a page with no `next`, or with a
  `next` that an earlier page gave, and after 10,000 pages with a warning.
  `restore --held`, `refresh_states`, the stub search and the launcher's
  `resume_held` use it. `restore --held` now also sends `lease_host=<self>`,
  so that on a new server it reads only the sessions whose lease the host
  holds; it still checks the lease itself, because a server without session
  status ignores the filter.
- `Api.session_page` sends each list with a read timeout of 120 s
  (`LIST_TIMEOUT`), and other requests keep 30 s. The hook's resume check
  keeps 2 s. The launcher keeps 30 s for its lists, so that a slow list does
  not delay the next heartbeat past its expiry. No list is sent again after a
  timeout. The CLI gives
  `the server did not answer in time; it may be busy or its storage slow` for
  a timeout of any request.
- `ls --status` takes comma-separated values and is repeatable. The client
  sends repeated `status=` parameters. The session-overview skill uses
  `--status working,waiting,ready`. `ls --all-pages` reads every page, with
  `--limit` as the page size.
- The capability cache is not built. An older server ignores the parameters
  that it does not know (`open=`, `status=`, `cursor=`) and does not give
  `400`. Thus a stale cache after a server rollback gives a wrong list with no
  error, and the client cannot see it. `/whoami` does not read the session
  index, so it is fast also on slow storage.

### Step 5: child sessions with a parent link

About 1,400 sessions on the server are the reviews of the `security-guidance`
plugin: headless Agent SDK sessions that its hooks start for the change of a
session. They fill the lists of open work. This step hides them, and links
each to the session whose change it reviews.

- The Claude Code adapter knows a review by its first prompt
  (`entrypoint: "sdk-py"`, and the texts of the first and second passes), and
  finds its parent: the session on this host that changed the files of the
  review with edit tools in a window before the review started. Exactly one
  best match gives `links.parent`. Each review gets `automation`, also with no
  parent, so that it is still hidden.
- The scan is cheap: an index of the edits once in each sweep, a bound window,
  and no new read of large transcripts at each checkpoint.
- The server: `top_level=1` and `parent=<sid>` on `GET /sessions`, with a new
  capability, in SQL with an index. Without them, the list has every session.
- The client: `ls` sends `top_level=1` by default, `ls --all` shows every
  session, and `show` lists the children. The internal lists do not hide
  children.
- The reviews that the server already has get their links from the next sweeps
  of the new client.
- The spec records the guidance of the user: if subagents and child sessions
  are ever made into one model, subagents change toward the parent link.

Done. As built:

- `ClaudeCode.classify` reads a transcript up to its first prompt (at most 4
  MiB) and gives `automation: "security-review"`, with the start time and the
  absolute paths of the changed files (the "Changed files" list and the
  `=== DIFF: <path> ===` headers, relative to the review's `cwd`).
  `ClaudeCode.edits` gives the `Edit`, `MultiEdit`, `Write` and `NotebookEdit`
  calls of the main transcripts and of their subagent transcripts in a time
  window, each with a later tool result that is not an error (`is_error`):
  failed, rejected, refused and interrupted calls do not count. It reads only
  transcripts that changed in the window, and of each only the end: it goes
  back from the end in steps of 1 MiB that double until a record is older than
  the window.
- `children.link_children` runs in the sweep before the syncs. It reads the
  first prompt of each session once (`_classified` in the client metadata),
  searches for the parent of each new review once (`_parent_checked`), with
  one edit scan for all new reviews, and sends the changed metadata of the
  reviews that the server already has, at most 200 in each sweep. A new review
  gets its links with its first sync.
- The parent: the most of the review's files edited in the 12 hours before the
  review started, then the last such edit. Equal matches give no parent. On
  the local sample (about 1,000 reviews), a window of 6 h gave 702 unique
  matches, 12 h gave 706 and 24 h gave 714. Most reviews with no match
  reviewed files that no session changed with an edit tool (changes with shell
  commands, or commits of earlier work). Of 20 equal matches, 12 were copies
  of the same records (a fork), and the last edit resolves the other 8. The
  scan of all local transcripts for the first sweep took about 9 s.
- The second pass is linked to the same parent, with the paths of its own
  diff, not to the first pass: both passes review one change of one session,
  and no record ties the two passes.
- The server: capability `child-sessions`, metadata field `automation`
  (string), and migration 5. It adds the columns `parent` (a copy of
  `links.parent`) and `top_level`, which `add_metadata` updates with
  `metadata`, adds `top_level` as the last column of `sessions_recent`, and
  adds the partial index `sessions_parent (account, parent, sort key)`. The
  columns are copies, not expressions in the index, so that the filters do not
  depend on the SQLite version: the server image is on Debian 12, with SQLite
  3.40, and older versions do not always use the value of an indexed
  expression in place of the expression.
- The client: `ls` sends `top_level=1` to a server with the capability,
  `ls --all` does not. `show` shows `parent` and `automation`, and lists the
  children. `all_sessions` never sends `top_level`.

## Checks

- Tests for each step, including a test that a slow read does not stop a
  write, and tests of the migration on a database with events.
- A synthetic database of a few thousand sessions: count statements and page
  reads for a list of 50, a list of 1000 and a status filter, before and
  after.
- The usual checks in `CLAUDE.md`, on the default Python and on 3.12.
