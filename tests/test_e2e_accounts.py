# vim: filetype=python
"""The account boundary. Two accounts, A and B, have the same host names and
the same session ids. A request of A for an id of B gets the answer for an
id that does not exist, or acts only on A's own session with that id, and
the state of B does not change."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from _hs import FakeClaude, Server, prompt, text_reply
from accounts import (
    Account,
    Accounts,
    AccountSources,
    Credential,
    dump_accounts,
    new_token,
)
from app import Settings, create_app
from hyperspace_client.adapters.claude_code import ClaudeCode, ContinuedFrom
from hyperspace_client.backfill import RunResult
from hyperspace_client.launcher import RESUME_WORKING, Launcher
from hyperspace_client.state import State
from hyperspace_client.sync import Context, Syncer
from hyperspace_client.teleport import HeldRestore, restore_held
from store import Store

HOST = "agent-1"
WS = "/home/agent/workspace"
# B has SID. No account has UNUSED, G_UNUSED or T_UNUSED.
SID = "5b1d0d6e-2b1c-4b8a-9f4e-7d3c2a1b0e9f"
UNUSED = "0f9e8d7c-6b5a-4c3d-8e2f-1a0b9c8d7e6f"
G_UNUSED = "g_unused"
T_SHARED = "t_shared"
T_UNUSED = "t_unused"
WAKE = "/v1/wake/claude-code"
WAKE_BODY = {"event": "wake", "account": "x", "handle": "r", "pending": 1, "items": []}
STATUS = {
    "status": "ready",
    "reason": "idle",
    "source_kind": "hook",
    "explicit": False,
    "base_seq": 0,
}
# Values that differ between two equal actions, such as ids and times.
VOLATILE = {
    "id",
    "created",
    "last_append",
    "received",
    "revision",
    "session_id",
    "since",
}

# A request: method, path and keyword arguments for the test client.
Request = tuple[str, str, dict[str, Any]]


@dataclass
class Env:
    server: Server
    client: TestClient

    def h(self, who: str, **extra: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.server.tokens[who]}",
            "Hyperspace-Workspace": WS,
            **extra,
        }

    def call(self, who: str, request: Request) -> tuple[int, bytes]:
        """The status and the body as sent, so that two refusals compare
        byte for byte."""
        method, path, kw = request
        headers = self.h(who, **kw.pop("headers", {}))
        r = self.client.request(method, path, headers=headers, **kw)
        return r.status_code, r.content

    def json(self, who: str, request: Request) -> tuple[int, Any]:
        """The status and the decoded body; a file read stays bytes."""
        status, body = self.call(who, request)
        return status, json.loads(body) if body[:1] in (b"{", b"[") else body

    def ok(self, who: str, request: Request) -> Any:
        status, body = self.json(who, request)
        assert status == 200, body
        return body


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    """Two accounts, each with a runner credential for HOST and a wake
    credential. The tokens are `a-runner`, `a-waker`, `b-runner` and
    `b-waker`."""
    data = tmp_path / "data"
    data.mkdir()
    tokens: dict[str, str] = {}
    accounts: dict[str, Account] = {}
    for account in ("a", "b"):
        creds: list[Credential] = []
        for name, host, scopes, read in (
            ("runner", HOST, {"append", "read", "grant"}, "granted"),
            ("waker", "subspace", {"wake"}, "own"),
        ):
            token, cid, digest = new_token()
            tokens[f"{account}-{name}"] = token
            creds.append(
                Credential(
                    id=cid,
                    secret_sha256=digest,
                    host=host,
                    scopes=frozenset(scopes),
                    read=read,
                )
            )
        accounts[account] = Account(id=account, credentials=tuple(creds))
    path = data / "accounts.toml"
    path.write_text(dump_accounts(Accounts(accounts)))
    store = Store(data)
    settings = Settings(data_dir=data, requests_per_minute=100_000)
    app = create_app(store, AccountSources(path), settings)
    server = Server(store, app, tokens)
    with TestClient(app) as client:
        yield Env(server, client)
    server.close()


def s(sid: str) -> str:
    return f"/v1/sessions/claude-code/{sid}"


def append(sid: str, gen: str, base: int, data: bytes) -> Request:
    headers = {
        "Hyperspace-Base-Length": str(base),
        "Hyperspace-Sha256": hashlib.sha256(data).hexdigest(),
        "Hyperspace-Activity": "1",
    }
    return (
        "POST",
        f"{s(sid)}/generations/{gen}/append",
        {
            "headers": headers,
            "content": data,
        },
    )


def blob(sid: str, data: bytes) -> Request:
    headers = {"Hyperspace-Sha256": hashlib.sha256(data).hexdigest()}
    return "PUT", f"{s(sid)}/blobs/plan.md", {"headers": headers, "content": data}


def heartbeat(sid: str, **extra: Any) -> Request:
    body = {
        "interval": 60,
        "sessions": [{"harness": "claude-code", "session_id": sid}],
        **extra,
    }
    return "PUT", "/v1/hosts/self/heartbeat", {"json": body}


def wake(sid: str) -> Request:
    return "POST", WAKE, {"json": {**WAKE_BODY, "session": sid}}


def seed(env: Env, who: str, sid: str = SID) -> str:
    """A session of the account of `who`, with every kind of state: a log, a
    blob, metadata, a status, a teleport, liveness and a wake request. Return
    the id of its generation."""
    account = who[0]
    env.ok(who, ("PUT", "/v1/hosts/self", {"json": {"kind": "runner"}}))
    root = {"path": "transcript.jsonl", "cause": "root"}
    gen = env.ok(who, ("POST", f"{s(sid)}/generations", {"json": root}))["id"]
    env.ok(who, append(sid, gen, 0, b"one\n"))
    env.ok(who, ("POST", f"{s(sid)}/meta", {"json": {"title": account, "cwd": WS}}))
    env.ok(who, ("POST", f"{s(sid)}/status", {"json": STATUS}))
    env.ok(who, blob(sid, b"plan"))
    teleport = {"workspace": WS, "teleport_id": T_SHARED}
    env.ok(who, ("POST", f"{s(sid)}/teleports", {"json": teleport}))
    env.ok(who, heartbeat(sid, wakes=True))
    env.ok(f"{account}-waker", wake(sid))
    return gen


def snapshot(env: Env, who: str, gen: str, sid: str = SID) -> dict[str, Any]:
    """Everything that the account of `who` can see of `sid` and its host."""
    reads: dict[str, Request] = {
        "manifest": ("GET", s(sid), {}),
        "state": ("GET", f"{s(sid)}/state", {}),
        "log": ("GET", f"{s(sid)}/generations/{gen}", {}),
        "blob": ("GET", f"{s(sid)}/blobs/plan.md", {}),
        "sessions": ("GET", "/v1/sessions", {}),
        "pending": ("GET", "/v1/sessions", {"params": {"wake": "pending"}}),
        "children": ("GET", "/v1/sessions", {"params": {"parent": sid}}),
        "hosts": ("GET", "/v1/hosts", {}),
        "workspaces": ("GET", "/v1/workspaces", {}),
    }
    return {name: env.ok(who, request) for name, request in reads.items()}


def shape(value: Any) -> Any:
    """`value` with the volatile values replaced, for a comparison of two
    responses to equal actions."""
    if isinstance(value, dict):
        return {k: "*" if k in VOLATILE else shape(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(shape(v) for v in value)
    return value


def set_status(env: Env, who: str, sid: str, status: str) -> None:
    current = env.ok(who, ("GET", f"{s(sid)}/state", {})).get("status")
    body = {**STATUS, "status": status, "base_seq": current["seq"] if current else 0}
    if status != "ready":
        del body["reason"]
    env.ok(who, ("POST", f"{s(sid)}/status", {"json": body}))


def no_start(args: list[str]) -> None:
    raise AssertionError(f"the launcher started {args}")


# The session routes, each as a function of the session id and a generation
# id, with the credential that calls it. `teleport_id` is B's id.
ROUTES: list[tuple[str, str, Callable[[str, str], Request]]] = [
    ("manifest", "a-runner", lambda sid, gen: ("GET", s(sid), {})),
    ("state", "a-runner", lambda sid, gen: ("GET", f"{s(sid)}/state", {})),
    (
        "status",
        "a-runner",
        lambda sid, gen: ("POST", f"{s(sid)}/status", {"json": STATUS}),
    ),
    (
        "surrender",
        "a-runner",
        lambda sid, gen: (
            "POST",
            f"{s(sid)}/lease/surrender",
            {"json": {"reserved_for": HOST}},
        ),
    ),
    (
        "teleport",
        "a-runner",
        lambda sid, gen: ("POST", f"{s(sid)}/teleports", {"json": {"workspace": WS}}),
    ),
    (
        "steal",
        "a-runner",
        lambda sid, gen: (
            "POST",
            f"{s(sid)}/teleports",
            {"json": {"workspace": WS, "steal": True, "teleport_id": T_SHARED}},
        ),
    ),
    (
        "fork",
        "a-runner",
        lambda sid, gen: (
            "POST",
            f"{s(sid)}/forks",
            {"json": {"generation": gen, "offset": 0}},
        ),
    ),
    (
        "grant",
        "a-runner",
        lambda sid, gen: ("POST", f"{s(sid)}/grants", {"json": {"host": "laptop"}}),
    ),
    ("append", "a-runner", lambda sid, gen: append(sid, gen, 4, b"two\n")),
    ("log", "a-runner", lambda sid, gen: ("GET", f"{s(sid)}/generations/{gen}", {})),
    ("blob", "a-runner", lambda sid, gen: ("GET", f"{s(sid)}/blobs/plan.md", {})),
    ("wake", "a-waker", lambda sid, gen: wake(sid)),
]
# The routes that make the session if it does not exist in the account.
CREATES: list[tuple[str, Callable[[str], Request]]] = [
    ("meta", lambda sid: ("POST", f"{s(sid)}/meta", {"json": {"title": "a"}})),
    (
        "generation",
        lambda sid: (
            "POST",
            f"{s(sid)}/generations",
            {"json": {"path": "transcript.jsonl", "cause": "root"}},
        ),
    ),
    ("blob", lambda sid: blob(sid, b"plan")),
    ("heartbeat", lambda sid: heartbeat(sid)),
]
# The routes that take a generation id, as a function of the session id and
# the generation id. A generation of B, and an id that does not exist, must
# get the same answer for a session of A.
GENERATION_ROUTES: list[tuple[str, Callable[[str, str], Request]]] = [
    (
        "parent",
        lambda sid, gen: (
            "POST",
            f"{s(sid)}/generations",
            {
                "json": {
                    "path": "transcript.jsonl",
                    "cause": "rewrite",
                    "parent": gen,
                    "fork_offset": 0,
                }
            },
        ),
    ),
    ("append", lambda sid, gen: append(sid, gen, 0, b"x\n")),
    ("log", lambda sid, gen: ("GET", f"{s(sid)}/generations/{gen}", {})),
    (
        "fork",
        lambda sid, gen: (
            "POST",
            f"{s(sid)}/forks",
            {"json": {"generation": gen, "offset": 0}},
        ),
    ),
]


def test_a_cannot_tell_b_from_nothing(env: Env) -> None:
    b_gen = seed(env, "b-runner")
    before = snapshot(env, "b-runner", b_gen)
    assert before["state"]["lease"]["host"] == HOST
    assert before["state"]["liveness"]["state"] == "live"
    assert "wake_request" in before["state"]
    assert [t["id"] for t in before["manifest"]["teleports"]] == [T_SHARED]

    # A has no session yet. B's id and an unused id get the same refusal.
    for name, who, request in ROUTES:
        foreign = env.call(who, request(SID, b_gen))
        assert foreign == env.call(who, request(UNUSED, b_gen)), name
        assert foreign[0] == 404, name
    assert env.ok("a-runner", heartbeat(SID))["recorded"] == 0
    for params in ({}, {"wake": "pending"}, {"parent": SID}):
        sessions = env.ok("a-runner", ("GET", "/v1/sessions", {"params": params}))
        assert sessions == {"sessions": []}, params
    assert env.ok("a-runner", ("GET", "/v1/hosts", {})) == {"hosts": []}
    assert env.ok("a-runner", ("GET", "/v1/workspaces", {})) == {"workspaces": []}

    # The writes that make a session make one in A, for both ids alike.
    made: dict[str, Any] = {}
    for name, request in CREATES:
        made[name] = env.json("a-runner", request(SID))
        assert made[name][0] == 200, (name, made[name])
        assert shape(made[name]) == shape(env.json("a-runner", request(UNUSED))), name
    a_gen = made["generation"][1]["id"]
    env.ok("a-runner", append(SID, a_gen, 0, b"one\n"))

    # A has SID now. Each route acts on A's session only.
    for name, who, request in ROUTES:
        assert env.call(who, request(SID, a_gen))[0] == 200, name
    # A retry of the teleport with B's id returns A's event.
    steal = next(request for name, _, request in ROUTES if name == "steal")
    assert env.ok("a-runner", steal(SID, a_gen))["teleport"]["id"] == T_SHARED
    # B's generation id is an unused id in A.
    for name, request in GENERATION_ROUTES:
        foreign = env.call("a-runner", request(SID, b_gen))
        assert foreign == env.call("a-runner", request(SID, G_UNUSED)), name
        assert foreign[0] == 404, name
    teleports = env.ok("a-runner", ("GET", s(SID), {}))["teleports"]
    assert len(teleports) == 2 and teleports[1]["id"] == T_SHARED

    assert snapshot(env, "b-runner", b_gen) == before


class Host(ClaudeCode):
    """A Claude Code config directory with no live session."""

    def live_processes(
        self, continued_from: ContinuedFrom | None = None
    ) -> list[list[str]]:
        return []

    def is_running(self, session_id: str) -> bool:
        return False


def test_b_launcher_acts_only_on_b(env: Env, tmp_path: Path) -> None:
    # B's runner holds SID, with a local copy, and its status asks for a
    # process. A's runner has the same name, holds a session with the same
    # id that asks for a process and has a wake, and reserves another
    # session for the name.
    # The resume needs a workspace that exists here.
    ws = str(tmp_path / "workspace")
    (tmp_path / "workspace").mkdir()
    fake = FakeClaude(tmp_path / "claude")
    host = Host(fake.root)
    ctx = Context(
        api=env.server.api("b-runner"),
        state=State(tmp_path / "state"),
        adapter=host,
        host=HOST,
    )
    path = fake.transcript(ws, SID)
    fake.write(path, prompt("hi", cwd=ws), text_reply())
    found = host.session_for_transcript(path)
    assert found is not None
    Syncer(ctx, found, ws).run()
    set_status(env, "b-runner", SID, "working")
    for sid in (SID, UNUSED):
        seed(env, "a-runner", sid)
        set_status(env, "a-runner", sid, "working")
    reserve = {"json": {"reserved_for": HOST}}
    env.ok("a-runner", ("POST", f"{s(UNUSED)}/lease/surrender", reserve))
    before = env.ok("b-runner", ("GET", f"{s(SID)}/state", {}))
    assert "wake_request" not in before

    calls: list[tuple[list[str], str]] = []

    def run_claude(
        argv: list[str], cwd: str, timeout: float, env: Mapping[str, str]
    ) -> RunResult:
        calls.append((argv, cwd))
        return RunResult(0, "")

    launcher = Launcher(
        ctx,
        host,
        "claude",
        60,
        None,
        runner=True,
        workspace=ws,
        run_claude=run_claude,
        start=no_start,
    )
    try:
        launcher.tick()
        assert restore_held(ctx, host, ws) == HeldRestore(0, [])
    finally:
        ctx.state.close()
    assert [(argv[-1], cwd) for argv, cwd in calls] == [(RESUME_WORKING, ws)]
    transcripts = sorted(p.name for p in fake.root.rglob("*.jsonl"))
    assert transcripts == [f"{SID}.jsonl"]
    after = env.ok("b-runner", ("GET", f"{s(SID)}/state", {}))
    assert after["lease"] == before["lease"] and "wake_request" not in after
