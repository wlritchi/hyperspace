---
name: session-overview
description:
  Use when the user asks for an overview of their open sessions or outstanding
  agent work, what a session was waiting for, or help to prioritize work
  across sessions and hosts. Reads session status from Hyperspace.
---

# Overview of open sessions

1. List the open sessions (every session with the status `working`, `waiting`
   or `ready`):

   ```
   hyperspace ls --status working,waiting,ready --json --limit 200
   ```

   Each entry has `state.status` (status, reason or wake kind, note, source),
   `state.liveness` (whether a process runs, and where), `state.last_append`,
   `metadata` (title, workspace) and `excerpt` (the start of the agent's last
   message). The list does not include sessions that have no status, usually
   old sessions from before session status. If the user asks for them, use
   `--status none`. The list also does not include child sessions, for example
   the sessions of security reviews, which tools start for the work of another
   session.

2. For sessions where the note and the excerpt do not say enough, read the
   last turns:

   ```
   hyperspace tail <session id> -n 3
   ```

3. Report to the user, grouped by what each session needs:
   - `ready`: the session waits for the user. Say what it asked or proposed.
   - interrupted (`working` with liveness `stopped`), or stalled: the work
     stopped. Say where.
   - `working` and `waiting`: no action needed now. Say what they wait for.

   Propose priorities, with the reasons. Group sessions that work on the same
   project.

4. Change nothing on your own. When the user approves, mark sessions done with
   `hyperspace done <session id> ... --by-user --note '<why>'`, or reopen one
   with `hyperspace reopen <session id>`.
