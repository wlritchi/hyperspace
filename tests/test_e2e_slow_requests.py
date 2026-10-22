# vim: filetype=python
"""Slow requests over HTTP: a slow list does not stop writes, and the
request log shows the duration and the waits of slow requests."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from _hs import Server, make_server
from store import Store

SID = "20e924a9-68b5-48d3-b1a4-15aa285980fd"
S = f"/v1/sessions/claude-code/{SID}"
WS = "/home/wlritchi/hyperspace"
WAIT = 10
# Longer than WAIT, so that a request that blocks fails before the hold ends.
HOLD = 3 * WAIT


def headers(server: Server, who: str = "laptop") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {server.tokens[who]}",
        "Hyperspace-Workspace": WS,
    }


def test_a_slow_list_does_not_stop_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = make_server(tmp_path / "data")
    listing, release = threading.Event(), threading.Event()
    original = Store.list_sessions_page

    def slow_list(self: Store, *args: Any, **kwargs: Any) -> Any:
        result = original(self, *args, **kwargs)
        listing.set()
        release.wait(HOLD)
        return result

    monkeypatch.setattr(Store, "list_sessions_page", slow_list)

    async def exercise() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.app), base_url="http://testserver"
        ) as client:
            meta = await client.post(
                S + "/meta", headers=headers(server), json={"title": "first"}
            )
            assert meta.status_code == 200
            pending = asyncio.create_task(
                client.get("/v1/sessions", headers=headers(server))
            )
            try:
                assert await asyncio.to_thread(listing.wait, WAIT)
                # The list holds its read transaction open.
                write = client.post(
                    S + "/meta", headers=headers(server), json={"title": "second"}
                )
                assert (await asyncio.wait_for(write, WAIT)).status_code == 200
                state = client.get(S + "/state", headers=headers(server))
                assert (await asyncio.wait_for(state, WAIT)).status_code == 200
            finally:
                release.set()
            response = await pending
            assert response.status_code == 200
            # The list has the snapshot from its start.
            assert response.json()["sessions"][0]["metadata"]["title"] == "first"
            response = await client.get("/v1/sessions", headers=headers(server))
            assert response.json()["sessions"][0]["metadata"]["title"] == "second"

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        server.close()


def test_the_request_log_shows_slow_requests_and_writer_waits(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    server = make_server(tmp_path / "data", slow_request_seconds=0.2)
    caplog.set_level(logging.DEBUG, logger="hyperspace")
    client = server.api("laptop")._client
    holding = threading.Event()

    def hold() -> None:
        with server.store._tx():
            holding.set()
            time.sleep(0.4)

    try:
        assert client.get("/whoami").status_code == 200
        holder = threading.Thread(target=hold)
        holder.start()
        assert holding.wait(WAIT)
        response = client.post(
            f"/sessions/claude-code/{SID}/meta?title=private",
            headers={"Hyperspace-Workspace": WS},
            json={"title": "a private title"},
        )
        holder.join(WAIT)
        assert response.status_code == 200
    finally:
        server.close()

    records = [r for r in caplog.records if r.name == "hyperspace"]
    fast = [r for r in records if "/v1/whoami" in r.getMessage()]
    assert [r.levelno for r in fast] == [logging.DEBUG]
    assert "host=laptop" in fast[0].getMessage()
    slow = [r for r in records if r.getMessage().startswith("slow request POST")]
    assert len(slow) == 1
    assert slow[0].levelno == logging.INFO
    message = slow[0].getMessage()
    assert f"'/v1/sessions/claude-code/{SID}/meta'" in message
    assert "status=200" in message
    wait = float(message.split("writer_wait=")[1].split("s")[0])
    assert wait >= 0.3
    for record in records:
        text = record.getMessage()
        assert server.tokens["laptop"] not in text
        assert "private" not in text
