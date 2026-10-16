# Hyperspace

Hyperspace is an append-only backup store for coding-agent transcripts, with
session teleport between hosts.

- The client copies transcripts to the server while the agent works, including
  during long turns. The server keeps every byte that it receives.
- `hyperspace teleport` on one host and `hyperspace restore` on another move a
  session, so that `claude --resume <id>` continues it there. The agent gets a
  notice that says where the session was and where it is now.
- Each session has a status (working, waiting, ready, or done).
  `hyperspace ls --open` lists the open work on all hosts. See
  [session status and liveness](docs/specs/2026-10-15-session-status.md).
- The server does not parse transcripts. Client adapters hold the harness
  knowledge. v1 has an adapter for Claude Code.

Status: v1 is built and not yet released. See the
[design spec](docs/specs/2026-10-12-hyperspace.md), the
[v1 plan](docs/plans/2026-10-12-hyperspace-v1.md),
[deploying Hyperspace](docs/deploy.md), and
[Hyperspace on container runners](docs/runners.md).

## Install the Claude Code plugin

This repository is also a plugin marketplace. The plugin needs the
`hyperspace` client and a server; see [deploying Hyperspace](docs/deploy.md).

```sh
claude plugin marketplace add wlritchi/hyperspace
claude plugin install hyperspace@hyperspace
```

## Layout

| Path              | Contents                                         |
| ----------------- | ------------------------------------------------ |
| `server/`         | The server, a PEP 723 script with a lock file    |
| `lib/`            | Server modules                                   |
| `protocol/`       | `hyperspace-protocol`: wire types and validation |
| `client/`         | `hyperspace-client`: sync engine, adapters, CLI  |
| `scripts/`        | Admin scripts that run inside the server image   |
| `tests/`          | Unit and end-to-end tests                        |
| `docs/`           | Design specs and implementation plans            |
| `charts/`         | The Helm chart                                   |
| `plugin/`         | The Claude Code plugin (hooks that call the CLI) |
| `.claude-plugin/` | The plugin marketplace that lists `plugin/`      |

## Development

```sh
uv run -qs tests/run.py                  # tests
uvx ruff@0.17.0 format && uvx ruff@0.17.0 check
bunx prettier@3.9.10 --write '**/*.md'   # Markdown
```

## License

Hyperspace is dedicated to the public domain under
[CC0 1.0 Universal](LICENSE).
