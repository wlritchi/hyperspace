#!/bin/sh
# Forward a Claude Code hook to the hyperspace CLI, which reads the hook's
# JSON on stdin and does the work. The CLI is $HYPERSPACE_CLI (a command
# line), else `hyperspace` on PATH. Without the CLI, the hook does nothing.
cli="${HYPERSPACE_CLI:-}"
if [ -z "$cli" ]; then
    cli="$(command -v hyperspace 2>/dev/null)" || exit 0
fi
# Word splitting of $cli is intended: HYPERSPACE_CLI can hold arguments.
# shellcheck disable=SC2086
exec $cli hook
