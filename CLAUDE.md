# Hyperspace

Append-only transcript backup server and client, with session teleport. Read
`docs/specs/2026-10-12-hyperspace.md` before you change behavior; it is the
source of truth for the model, the protocol, and the security rules.

## Layout

- `server/hyperspace-server.py`: the server (PEP 723 script, committed
  `.lock`). It puts `lib/` on `sys.path`.
- `lib/`: server modules. They must not import the client.
- `protocol/`: `hyperspace-protocol`, shared by the server and the client. No
  runtime dependencies.
- `client/`: `hyperspace-client` with the `hyperspace` CLI and the harness
  adapters.
- `scripts/`: admin scripts. They run inside the server container, on the data
  directory.
- `tests/`: `test_unit_*` and `test_e2e_*`. `tests/conftest.py` puts `lib/` on
  the path.

## Commands

- Tests: `uv run -qs tests/run.py` (add `-k <expr>` to filter). CI uses Python
  3.12, so check with `UV_PYTHON=3.12` before a push.
- Format and lint: `uvx ruff@0.17.0 format` and `uvx ruff@0.17.0 check`.
- Types:
  `uvx --with-editable ./protocol --with-editable ./client pyright@1.1.408 protocol/src client/src`.
- Markdown: `bunx prettier@3.9.10 --write '**/*.md'` (prose wraps at 78
  columns).
- After you change a PEP 723 script's dependencies, run
  `uv lock --script <script>` and commit the `.lock` file.

## Conventions

- Conventional Commits. The commit type drives the release version: `feat` is
  minor, `fix` is patch, `!` or `BREAKING CHANGE` is major.
- Tag streams: `X.Y.Z` (server image and chart), `protocol-vX.Y.Z`, and
  `client-vX.Y.Z`. The release workflows push them; do not tag by hand.
- Design specs go in `docs/specs/`, implementation plans in `docs/plans/`.
  Write prose and comments in ASD-STE100 style.
- The repo is public. Do not commit private host names, internal URLs, or
  details of private infrastructure.
- The server never parses transcript content. Harness knowledge belongs in
  `client/src/hyperspace_client/adapters/`.
