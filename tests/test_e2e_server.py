# vim: filetype=python
"""Start the real server script and use it over a socket."""

import os
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def server(tmp_path: Path) -> Iterator[tuple[str, Path]]:
    port = _free_port()
    data = tmp_path / "data"
    env = {
        **os.environ,
        "HYPERSPACE_DATA_DIR": str(data),
        "HYPERSPACE_PORT": str(port),
    }
    proc = subprocess.Popen(
        ["uv", "run", "-qs", str(ROOT / "server/hyperspace-server.py")],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 120
        while True:
            if proc.poll() is not None:
                output = proc.stdout.read().decode() if proc.stdout else ""
                pytest.fail(f"server exited: {output}")
            try:
                if httpx.get(f"{url}/healthz", timeout=1).text == "ok":
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                pytest.fail("server did not start")
            time.sleep(0.2)
        yield url, data
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.mark.slow
def test_server_script(server: tuple[str, Path]) -> None:
    url, data = server
    out = subprocess.run(
        [
            "uv",
            "run",
            "-qs",
            str(ROOT / "scripts/new-credential.py"),
            "--host",
            "laptop",
            "--read",
            "all",
            "--data-dir",
            str(data),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    token = out.stdout.strip()
    assert token.startswith("hsp_")
    assert oct((data / "accounts.toml").stat().st_mode & 0o777) == "0o600"
    r = httpx.get(f"{url}/v1/whoami", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["host"] == "laptop"
    assert "server" not in {k.lower() for k in r.headers}

    credential_id = token.split("_")[1]
    subprocess.run(
        [
            "uv",
            "run",
            "-qs",
            str(ROOT / "scripts/revoke-credential.py"),
            credential_id,
            "--data-dir",
            str(data),
        ],
        check=True,
        capture_output=True,
    )
    r = httpx.get(f"{url}/v1/whoami", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 401
