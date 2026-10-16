---
name: session-status
description:
  Use when you finish the work of this session, when you end a turn to wait
  for an event other than the user (CI, a PR review, a message, a cron or
  Monitor), or when you get a Hyperspace notice that the session was marked
  done. Sets the session's Hyperspace status, so that the user's list of open
  work stays correct.
---

# Session status

Hyperspace keeps a status for each session: `working`, `waiting`, `ready`
(waits for the user) or `done`. The user and other agents use it to find the
sessions that still need work. The hooks set `working` and `ready` for you.
You set the status only in these cases:

- **The work is finished.** The user has what they asked for, and you expect
  that they will not come back to this session unless they find a problem.
  Run:

  ```
  hyperspace done --note '<one-line outcome>'
  ```

  A plan that the user did not execute yet, or work with open follow-ups, is
  not finished: do not mark it done.

- **You wait for an event, not for the user.** Before you end the turn, run:

  ```
  hyperspace status waiting --wake local --note '<what you wait for>'
  ```

  Use `--wake local` when only this process receives the event (a cron, a
  Monitor, a background task). Use `--wake message` when something outside
  delivers it (a message, a PR notification), so that the process can stop.

- **You ask the user a question.** Do nothing: the hooks set `ready`.

Notes are one line, at most 300 characters. `done` does not close the session
or the UI. If the user writes again, the session reopens.

If you get a notice that someone marked this session done, finish the current
step, do not start new work, and end your turn.

If `hyperspace` reports that the server needs an upgrade, or that no session
id is set, tell the user once and continue.
