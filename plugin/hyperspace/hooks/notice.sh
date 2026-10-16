#!/bin/sh
# Give the agent Hyperspace notices during a turn, for example that a user
# marked this session done from a different host. This hook is synchronous
# and runs after each tool call, so the usual case is one file test with no
# other process: the CLI runs only when the client left a flag file for the
# session in its state directory.
dir="${XDG_STATE_HOME:-$HOME/.local/state}/hyperspace/notify"
if [ -n "${CLAUDE_CODE_SESSION_ID:-}" ]; then
    [ -e "$dir/$CLAUDE_CODE_SESSION_ID" ] || exit 0
else
    # No session id in the environment: any flag is a reason to ask the CLI,
    # which reads the session from the hook's JSON.
    [ -n "$(ls -A "$dir" 2>/dev/null)" ] || exit 0
fi
cli="${HYPERSPACE_CLI:-}"
if [ -z "$cli" ]; then
    cli="$(command -v hyperspace 2>/dev/null)" || exit 0
fi
# Word splitting of $cli is intended: HYPERSPACE_CLI can hold arguments. A
# client that does not know --notices fails; the hook must still exit 0 and
# print nothing on stderr, so that Claude Code shows no error.
# shellcheck disable=SC2086
$cli hook --notices 2>/dev/null
exit 0
