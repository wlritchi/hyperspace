# vim: filetype=python
"""The launch routes of the HTTP API, in-process through the Starlette test
client: the scopes, the caller's routes, and the launcher's routes."""

import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import zstandard
from starlette.testclient import TestClient

from accounts import (
    Account,
    Accounts,
    AccountSources,
    Credential,
    dump_accounts,
    new_token,
)
from app import Settings, create_app
from hyperspace_protocol import MAX_LAUNCH_PROMPT
from store import LAUNCH_CLAIM_TIMEOUT, Store

HARNESS = "claude-code"
L = f"/v1/launch/{HARNESS}"
PARENT = "20e924a9-68b5-48d3-b1a4-15aa285980fd"
P = f"/v1/sessions/{HARNESS}/{PARENT}"
WS_LAPTOP = "/home/user/project"


class Clock:
    """The real time, plus an offset that a test can move forward."""

    def __init__(self) -> None:
        self.offset = timedelta()

    def __call__(self) -> datetime:
        return datetime.now(UTC) + self.offset


@dataclass
class Env:
    client: TestClient
    tokens: dict[str, str]
    clock: Clock
    data: Path

    def h(self, who: str, workspace: str | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.tokens[who]}"}
        if workspace is not None:
            headers["Hyperspace-Workspace"] = workspace
        return headers

    def get(self, path: str, who: str) -> Any:
        return self.client.get(path, headers=self.h(who))

    def post(self, path: str, who: str, body: dict[str, Any] | None = None) -> Any:
        return self.client.post(path, headers=self.h(who), json=body or {})


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    data = tmp_path / "data"
    data.mkdir()
    specs = {
        "laptop": ("laptop", {"append", "read", "grant", "launch"}, "all"),
        "agent": ("agent-1", {"append", "read", "grant"}, "granted"),
        "agent2": ("agent-2", {"append", "read", "grant"}, "granted"),
        "caller": ("caller", {"launch"}, "own"),
        "reader": ("reader", {"read"}, "own"),
        "appender": ("appender", {"append"}, "own"),
        "waker": ("subspace", {"wake"}, "own"),
    }
    tokens: dict[str, str] = {}
    creds: list[Credential] = []
    for name, (host, scopes, read) in specs.items():
        token, cid, digest = new_token()
        tokens[name] = token
        creds.append(
            Credential(
                id=cid,
                secret_sha256=digest,
                host=host,
                scopes=frozenset(scopes),
                read=read,
            )
        )
    path = data / "accounts.toml"
    path.write_text(
        dump_accounts(
            Accounts({"default": Account(id="default", credentials=tuple(creds))})
        )
    )
    clock = Clock()
    store = Store(data, clock=clock)
    # A short sweep interval, so that a test sees the sweep.
    settings = Settings(data_dir=data, launch_sweep_seconds=0.05)
    app = create_app(store, AccountSources(path), settings)
    with TestClient(app) as client:
        e = Env(client, tokens, clock, data)
        for who in ("agent", "agent2"):
            r = client.put("/v1/hosts/self", headers=e.h(who), json={"kind": "runner"})
            assert r.status_code == 200, r.text
        yield e
    store.close()


def make_parent(env: Env) -> None:
    r = env.client.post(
        f"{P}/generations",
        headers=env.h("laptop", WS_LAPTOP),
        json={"path": "transcript.jsonl", "cause": "root"},
    )
    assert r.status_code == 200, r.text
    data = b"one\n"
    r = env.client.post(
        f"{P}/generations/{r.json()['id']}/append",
        headers={
            **env.h("laptop", WS_LAPTOP),
            "Hyperspace-Base-Length": "0",
            "Hyperspace-Sha256": hashlib.sha256(data).hexdigest(),
            "Content-Encoding": "zstd",
        },
        content=zstandard.ZstdCompressor().compress(data),
    )
    assert r.status_code == 200, r.text


def launch(env: Env, who: str = "caller", **extra: Any) -> Any:
    body = {"host": "agent-1", "prompt": "review the change", **extra}
    return env.post(L, who, body)


def launch_id(env: Env, **extra: Any) -> str:
    r = launch(env, **extra)
    assert r.status_code == 200, r.text
    return r.json()["launch"]


def heartbeat(env: Env, who: str = "agent", **extra: Any) -> None:
    r = env.client.put(
        "/v1/hosts/self/heartbeat",
        headers=env.h(who),
        json={"interval": 60, "sessions": [], **extra},
    )
    assert r.status_code == 200, r.text


def test_whoami_advertises_launch(env: Env) -> None:
    r = env.get("/v1/whoami", "caller")
    assert r.status_code == 200
    assert r.json()["scopes"] == ["launch"]
    assert "launch" in r.json()["capabilities"]


def test_launch_only_credential_reaches_only_the_caller_routes(env: Env) -> None:
    lid = launch_id(env)
    h = env.h("caller", WS_LAPTOP)
    refused = [
        env.client.put("/v1/hosts/self", headers=h, json={"kind": "runner"}),
        env.client.put("/v1/hosts/self/heartbeat", headers=h, json={"interval": 60}),
        env.client.get("/v1/hosts", headers=h),
        env.client.get("/v1/workspaces", headers=h),
        env.client.get("/v1/sessions", headers=h),
        env.client.get(P, headers=h),
        env.client.get(f"{P}/state", headers=h),
        env.client.post(f"{P}/meta", headers=h, json={"title": "x"}),
        env.client.post(f"{P}/generations", headers=h, json={"path": "x"}),
        env.client.post(f"{P}/grants", headers=h, json={"host": "caller"}),
        env.client.post("/v1/wake/claude-code", headers=h, json={"session": PARENT}),
        env.client.get(f"{L}?state=pending", headers=h),
        env.client.post(f"{L}/{lid}/claim", headers=h),
        env.client.post(f"{L}/{lid}/link", headers=h, json={"session": "s1"}),
        env.client.post(f"{L}/{lid}/fail", headers=h, json={"reason": "x"}),
    ]
    assert [r.status_code for r in refused] == [403] * len(refused)
    assert all(r.json()["error"] == "forbidden" for r in refused)
    assert env.get(f"{L}/{lid}", "caller").status_code == 200
    assert env.client.delete(f"{L}/{lid}", headers=h).status_code == 200


def test_wake_credential_gets_no_launch_route(env: Env) -> None:
    lid = launch_id(env)
    h = env.h("waker")
    refused = [
        env.client.post(L, headers=h, json={"host": "agent-1", "prompt": "x"}),
        env.client.get(f"{L}/{lid}", headers=h),
        env.client.delete(f"{L}/{lid}", headers=h),
        env.client.get(f"{L}?state=pending", headers=h),
        env.client.post(f"{L}/{lid}/claim", headers=h),
        env.client.post(f"{L}/{lid}/link", headers=h, json={"session": "s1"}),
        env.client.post(f"{L}/{lid}/fail", headers=h, json={"reason": "x"}),
    ]
    assert [r.status_code for r in refused] == [403] * len(refused)
    assert env.get(f"{L}/{lid}", "laptop").json()["state"] == "pending"


def test_caller_route_scopes(env: Env) -> None:
    for who in ("agent", "reader"):
        r = launch(env, who)
        assert r.status_code == 403
        assert r.json() == {"error": "forbidden", "detail": "missing scope: launch"}
    lid = launch_id(env)
    # A `read` credential of the account reads each launch of the account,
    # whatever its read scope. It cannot cancel.
    assert env.get(f"{L}/{lid}", "reader").status_code == 200
    r = env.client.delete(f"{L}/{lid}", headers=env.h("reader"))
    assert r.status_code == 403
    assert env.get(f"{L}/{lid}", "agent").status_code == 200
    r = env.get(f"{L}/{lid}", "appender")
    assert r.status_code == 403
    assert r.json() == {"error": "forbidden", "detail": "missing scope: read or launch"}


def test_launch_responses(env: Env) -> None:
    r = launch(env, host="nowhere")
    assert r.status_code == 404
    assert r.json() == {"error": "not_found", "detail": "no such host"}
    r = launch(env, parent=PARENT)
    assert r.status_code == 400
    assert r.json() == {
        "error": "bad_request",
        "detail": "parent is not a session in the account",
    }
    make_parent(env)
    r = launch(env, parent=PARENT, request="r1")
    assert r.status_code == 200, r.text
    first = r.json()
    assert first["verdict"] == "queued"
    # The same request again: the first response, and nothing recorded.
    heartbeat(env, wakes=True, launches=True)
    assert launch(env, parent=PARENT, request="r1").json() == first
    r = launch(env, parent=PARENT, request="r1", prompt="another")
    assert r.status_code == 409
    assert r.json() == {"error": "request_used", "detail": "request id used"}
    r = launch(env, parent=PARENT)
    assert r.json()["verdict"] == "starting"
    pending = env.get(f"{L}?state=pending", "agent").json()["launches"]
    assert [p["launch"] for p in pending] == [first["launch"], r.json()["launch"]]
    # A launcher that does not act on launches.
    heartbeat(env, wakes=True)
    assert launch(env).json()["verdict"] == "queued"


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"host": "agent-1"},
        {"prompt": "x"},
        {"host": "agent-1", "prompt": ""},
        {"host": "agent-1", "prompt": "x", "workspace": "relative"},
        {"host": "agent-1", "prompt": "x", "model": "-x"},
        {"host": "agent-1", "prompt": "x", "request": "a b"},
        {"host": "agent-1", "prompt": "x", "parent": "../x"},
    ],
)
def test_launch_bad_bodies(env: Env, body: dict[str, Any]) -> None:
    r = env.post(L, "caller", body)
    assert r.status_code == 400
    assert r.json()["error"] == "bad_request"


def test_launch_prompt_size(env: Env) -> None:
    # Each quote is two characters of JSON.
    largest = '"' * MAX_LAUNCH_PROMPT
    r = launch(env, prompt=largest)
    assert r.status_code == 200, r.text
    [pending] = env.get(f"{L}?state=pending", "agent").json()["launches"]
    assert pending["prompt"] == largest
    r = launch(env, prompt=largest + "x")
    assert r.status_code == 400
    raw = json.dumps({"host": "agent-1", "prompt": "x" * (600 * 1024)})
    r = env.client.post(L, headers=env.h("caller"), content=raw)
    assert r.status_code == 413


def test_launcher_flow(env: Env) -> None:
    make_parent(env)
    lid = launch_id(
        env,
        workspace="/home/claude/workspace",
        model="claude-opus-5-5",
        effort="high",
        name="reviewer",
        parent=PARENT,
    )
    # Only the launch's host sees and claims it.
    assert env.get(f"{L}?state=pending", "agent2").json() == {"launches": []}
    for who in ("agent2", "laptop"):
        r = env.post(f"{L}/{lid}/claim", who)
        assert r.status_code == 404
        assert r.json() == {"error": "not_found", "detail": "launch"}
    [pending] = env.get(f"{L}?state=pending", "agent").json()["launches"]
    assert pending == {
        "launch": lid,
        "harness": HARNESS,
        "host": "agent-1",
        "state": "pending",
        "caller": "caller",
        "created": pending["created"],
        "updated": pending["created"],
        "workspace": "/home/claude/workspace",
        "model": "claude-opus-5-5",
        "effort": "high",
        "name": "reviewer",
        "parent": PARENT,
        "prompt": "review the change",
    }
    r = env.post(f"{L}/{lid}/claim", "agent")
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "claimed"
    assert r.json()["prompt"] == "review the change"
    assert env.get(f"{L}?state=pending", "agent").json() == {"launches": []}
    r = env.post(f"{L}/{lid}/claim", "agent")
    assert r.status_code == 409
    assert r.json() == {"error": "launch_state", "detail": "launch is claimed"}
    r = env.client.delete(f"{L}/{lid}", headers=env.h("caller"))
    assert r.status_code == 409
    for who in ("agent2", "laptop"):
        r = env.post(f"{L}/{lid}/link", who, {"session": "new-1"})
        assert r.status_code == 404
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "new-1"})
    assert r.status_code == 200, r.text
    assert (r.json()["state"], r.json()["session"]) == ("started", "new-1")
    assert "prompt" not in r.json()
    assert env.post(f"{L}/{lid}/link", "agent", {"session": "new-1"}).json() == r.json()
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "new-2"})
    assert r.status_code == 409
    assert r.json() == {
        "error": "launch_state",
        "detail": "launch is linked to another session",
    }
    body = env.get(f"{L}/{lid}", "caller").json()
    assert body == {
        "launch": lid,
        "harness": HARNESS,
        "host": "agent-1",
        "state": "started",
        "caller": "caller",
        "created": pending["created"],
        "updated": body["updated"],
        "workspace": "/home/claude/workspace",
        "model": "claude-opus-5-5",
        "effort": "high",
        "name": "reviewer",
        "parent": PARENT,
        "session": "new-1",
    }
    r = env.post(f"{L}/{lid}/fail", "agent", {"reason": "late"})
    assert r.status_code == 409


def test_fail_and_cancel(env: Env) -> None:
    lid = launch_id(env)
    r = env.post(f"{L}/{lid}/fail", "agent", {"reason": "x"})
    assert r.status_code == 409
    assert r.json() == {"error": "launch_state", "detail": "launch is pending"}
    env.post(f"{L}/{lid}/claim", "agent")
    r = env.post(f"{L}/{lid}/fail", "agent2", {"reason": "x"})
    assert r.status_code == 404
    r = env.post(f"{L}/{lid}/fail", "agent", {"reason": "error: bad\noption"})
    assert r.status_code == 200, r.text
    assert (r.json()["state"], r.json()["reason"]) == ("failed", "error: bad option")
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "new-1"})
    assert r.status_code == 409
    assert r.json()["detail"] == "launch is failed"
    assert env.post(f"{L}/{lid}/fail", "agent", {"reason": ""}).status_code == 400
    other = launch_id(env)
    r = env.client.delete(f"{L}/{other}", headers=env.h("laptop"))
    assert r.status_code == 200, r.text
    assert r.json()["state"] == "cancelled" and "prompt" not in r.json()
    r = env.client.delete(f"{L}/{other}", headers=env.h("caller"))
    assert r.status_code == 409
    assert r.json() == {"error": "launch_state", "detail": "launch is cancelled"}
    assert env.post(f"{L}/{other}/claim", "agent").status_code == 409


def test_claim_without_a_session_fails_after_ten_minutes(env: Env) -> None:
    lid = launch_id(env)
    env.post(f"{L}/{lid}/claim", "agent")
    env.clock.offset += LAUNCH_CLAIM_TIMEOUT
    body = env.get(f"{L}/{lid}", "caller").json()
    assert (body["state"], body["reason"]) == ("failed", "no session reported")
    # The session can report late.
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "new-1"})
    assert r.status_code == 200, r.text
    assert env.get(f"{L}/{lid}", "caller").json()["session"] == "new-1"


def test_server_sweep_deletes_the_prompt_of_an_expired_claim(env: Env) -> None:
    # The launcher stops after its claim, and no request comes for the launch.
    lid = launch_id(env)
    env.post(f"{L}/{lid}/claim", "agent")
    env.clock.offset += LAUNCH_CLAIM_TIMEOUT

    def stored() -> tuple[str, str | None]:
        with closing(sqlite3.connect(env.data / "index.sqlite3")) as db:
            return db.execute(
                "SELECT state, prompt FROM launches WHERE id = ?", (lid,)
            ).fetchone()

    deadline = time.monotonic() + 5
    while stored() != ("failed", None) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert stored() == ("failed", None)
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "new-1"})
    assert r.status_code == 200, r.text


def test_bad_paths_and_queries(env: Env) -> None:
    assert env.get(f"{L}/l_nosuchlaunch", "caller").status_code == 404
    assert env.get(f"{L}/bad.id", "caller").status_code == 400
    assert env.get("/v1/launch/Bad_Harness/l_x", "caller").status_code == 400
    lid = launch_id(env)
    # A launch id under another harness gives 404.
    assert env.get(f"/v1/launch/other/{lid}", "caller").status_code == 404
    for query in ("", "?state=claimed"):
        r = env.get(f"{L}{query}", "agent")
        assert r.status_code == 400
        assert r.json() == {"error": "bad_request", "detail": "state must be pending"}
    r = env.post(f"{L}/{lid}/link", "agent", {"session": "../x"})
    assert r.status_code == 400
