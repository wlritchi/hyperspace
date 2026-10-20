#!/bin/sh
# Give the agent Hyperspace notices during a turn, for example that a user
# marked this session done from a different host. This hook is synchronous
# and runs after each tool call, so the usual case is one file test with no
# other process: the CLI runs only when the client left a flag file for the
# session in its state directory.
dir="${XDG_STATE_HOME:-$HOME/.local/state}/hyperspace/notify"

# The session id in the `resumeSessionId` field of the background job's
# state, in $resumed, with only shell builtins. A job can have its own id in
# CLAUDE_CODE_SESSION_ID and write the transcript of the session that it
# continues, whose flag the client sets.
job_session() {
    resumed=""
    file="${CLAUDE_JOB_DIR:-}/state.json"
    [ -n "${CLAUDE_JOB_DIR:-}" ] && [ -r "$file" ] || return 1
    text=""
    while IFS= read -r line || [ -n "$line" ]; do
        text="$text$line"
    done 2>/dev/null <"$file"
    case "$text" in
        *'"resumeSessionId"'*) ;;
        *) return 1 ;;
    esac
    rest="${text#*\"resumeSessionId\"}"
    # Only a colon and white space can come before the value's quote.
    case "${rest%%\"*}" in
        *[!:\ \	]*) return 1 ;;
    esac
    rest="${rest#*\"}"
    value="${rest%%\"*}"
    case "$value" in
        "" | [!0-9A-Za-z]* | *[!0-9A-Za-z._-]*) return 1 ;;
    esac
    resumed="$value"
}

if [ -n "${CLAUDE_CODE_SESSION_ID:-}" ]; then
    if [ ! -e "$dir/$CLAUDE_CODE_SESSION_ID" ]; then
        # Only a job can write the transcript of a different session. The
        # flags are listed, and the job's state is read, only in a job.
        [ -n "${CLAUDE_JOB_DIR:-}" ] || exit 0
        set -- "$dir"/*
        [ -e "$1" ] || exit 0
        job_session || exit 0
        [ -e "$dir/$resumed" ] || exit 0
    fi
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
