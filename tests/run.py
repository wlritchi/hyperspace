#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Run the test suite.

    uv run -qs tests/run.py            # all tests
    uv run -qs tests/run.py -k append  # filter

The test dependencies are here (PEP 723), so there is no project to set up.
The tests do not use the network, except for servers that they start on
loopback.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest>=8",
#     "hyperspace-protocol",
# ]
#
# [tool.uv.sources]
# hyperspace-protocol = { path = "../protocol", editable = true }
# ///

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    raise SystemExit(pytest.main([str(here), *sys.argv[1:]]))
