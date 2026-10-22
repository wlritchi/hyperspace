#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""The Hyperspace server.

    uv run -qs server/hyperspace-server.py

Configuration is in environment variables; see "Configuration" in the spec.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "starlette>=0.47",
#     "uvicorn>=0.35",
#     "zstandard>=0.23",
#     "hyperspace-protocol",
# ]
#
# [tool.uv.sources]
# hyperspace-protocol = { path = "../protocol" }
# ///

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import uvicorn

from accounts import AccountSources
from app import Settings, create_app
from store import Store


def main() -> None:
    # Data files hold transcripts: no access for other users.
    os.umask(0o027)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    store = Store(
        settings.data_dir,
        read_connections=settings.read_connections,
        read_cache_bytes=settings.read_cache_bytes,
    )
    cut = store.recover()
    if cut:
        logging.getLogger("hyperspace").warning(
            "cut uncommitted frames from %d generation files", len(cut)
        )
    accounts = AccountSources(
        settings.data_dir / "accounts.toml", settings.accounts_dir
    )
    app = create_app(store, accounts, settings)
    uvicorn.run(
        app,
        host=settings.listen_host,
        port=settings.port,
        # The app reads X-Forwarded-For itself, only from trusted proxies.
        proxy_headers=False,
        server_header=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
