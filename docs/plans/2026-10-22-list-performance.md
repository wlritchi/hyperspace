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
the protocol and the server, step 4 the client and the plugin. A new client
must work with an old server (use a capability, as for session status), and an
old client must work with a new server.

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

### Step 3: a cursor for the session list

- `GET /sessions` accepts a cursor (for example
  `before=<sort key, session id>`, opaque to the client) and returns the
  cursor of the next page. A new capability tells the client that the server
  has it.

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

## Checks

- Tests for each step, including a test that a slow read does not stop a
  write, and tests of the migration on a database with events.
- A synthetic database of a few thousand sessions: count statements and page
  reads for a list of 50, a list of 1000 and a status filter, before and
  after.
- The usual checks in `CLAUDE.md`, on the default Python and on 3.12.
