# vim: filetype=python
"""The HTTP API, in-process through the Starlette test client."""

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import zstandard
from starlette.testclient import TestClient

from accounts import (
    Account,
    Accounts,
    AccountsFile,
    Credential,
    dump_accounts,
    new_token,
)
from app import Settings, _Limiter, create_app
from store import Store

SID = "20e924a9-68b5-48d3-b1a4-15aa285980fd"
S = f"/v1/sessions/claude-code/{SID}"
WS_LAPTOP = "/home/wlritchi/hyperspace"
WS_AGENT = "/home/agent/workspace"


@dataclass
class Env:
    client: TestClient
    tokens: dict[str, str]
    accounts_path: Path
    creds: dict[str, Credential]

    def h(self, who: str, workspace: str | None = None, **extra: str) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.tokens[who]}", **extra}
        if workspace is not None:
            headers["Hyperspace-Workspace"] = workspace
        return headers


def _cred(host: str, scopes: set[str], read: str) -> tuple[str, Credential]:
    token, cid, digest = new_token()
    return token, Credential(
        id=cid, secret_sha256=digest, host=host, scopes=frozenset(scopes), read=read
    )


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    data = tmp_path / "data"
    data.mkdir()
    specs = {
        "laptop": ("laptop", {"append", "read", "grant"}, "all"),
        "agent": ("agent-1", {"append", "read", "grant"}, "granted"),
        "reader": ("reader", {"read"}, "all"),
    }
    tokens: dict[str, str] = {}
    creds: dict[str, Credential] = {}
    for name, (host, scopes, read) in specs.items():
        tokens[name], creds[name] = _cred(host, scopes, read)
    path = data / "accounts.toml"
    path.write_text(
        dump_accounts(
            Accounts(
                {"default": Account(id="default", credentials=tuple(creds.values()))}
            )
        )
    )
    store = Store(data)
    settings = Settings(data_dir=data, failed_auth_per_minute=5)
    app = create_app(store, AccountsFile(path), settings)
    with TestClient(app) as client:
        yield Env(client, tokens, path, creds)
    store.close()


def new_gen(env: Env, who: str, workspace: str, **body: Any) -> str:
    body.setdefault("path", "transcript.jsonl")
    body.setdefault("cause", "root")
    r = env.client.post(f"{S}/generations", headers=env.h(who, workspace), json=body)
    assert r.status_code == 200, r.text
    return r.json()["id"]


def append(
    env: Env,
    who: str,
    workspace: str,
    gen: str,
    base: int,
    data: bytes,
    activity: int = 0,
    compress: bool = True,
) -> Any:
    body = zstandard.ZstdCompressor().compress(data) if compress else data
    headers = env.h(
        who,
        workspace,
        **{
            "Hyperspace-Base-Length": str(base),
            "Hyperspace-Sha256": hashlib.sha256(data).hexdigest(),
            "Hyperspace-Activity": str(activity),
        },
    )
    if compress:
        headers["Content-Encoding"] = "zstd"
    return env.client.post(
        f"{S}/generations/{gen}/append", headers=headers, content=body
    )


def test_healthz_needs_no_credential(env: Env) -> None:
    r = env.client.get("/healthz")
    assert r.status_code == 200 and r.text == "ok"


def test_auth(env: Env) -> None:
    assert env.client.get("/v1/whoami").status_code == 401
    bad = {"Authorization": "Bearer hsp_abcdefghijkl_" + "x" * 43}
    assert env.client.get("/v1/whoami", headers=bad).status_code == 401
    r = env.client.get("/v1/whoami", headers=env.h("agent"))
    assert r.json() == {
        "account": "default",
        "host": "agent-1",
        "scopes": ["append", "grant", "read"],
        "read_scope": "granted",
        "capabilities": ["workspace-utf8"],
    }


def test_failed_auth_is_rate_limited(env: Env) -> None:
    # The laptop has used its credential from this address before.
    assert env.client.get("/v1/whoami", headers=env.h("laptop")).status_code == 200
    bad = {"Authorization": "Bearer nope"}
    codes = [env.client.get("/v1/whoami", headers=bad).status_code for _ in range(7)]
    assert codes[:5] == [401] * 5
    assert codes[-1] == 429
    # A blocked address gets no check of a new credential, even a valid one,
    # so it cannot keep guessing.
    assert env.client.get("/v1/whoami", headers=env.h("agent")).status_code == 429
    # A credential that this address used before still works: failures from
    # a shared address must not lock out known clients.
    assert env.client.get("/v1/whoami", headers=env.h("laptop")).status_code == 200


def test_limiter_tables_are_bounded() -> None:
    limiter = _Limiter(Settings(data_dir=Path("/nonexistent")), max_entries=100)
    for i in range(1000):
        limiter.failed_auth(f"10.0.{i // 256}.{i % 256}")
        limiter.succeeded(f"10.0.{i // 256}.{i % 256}", "abcdefghijkl")
    assert len(limiter._failures) <= 100
    assert len(limiter._known) == 100


def test_blocked_entries_survive_a_flood() -> None:
    limiter = _Limiter(
        Settings(data_dir=Path("/nonexistent"), failed_auth_per_minute=3),
        max_entries=50,
    )
    for _ in range(3):
        limiter.failed_auth("192.0.2.1")
    assert not limiter.may_check("192.0.2.1", None)
    for i in range(500):
        limiter.failed_auth(f"10.1.{i // 256}.{i % 256}")
    assert len(limiter._failures) <= 50
    assert not limiter.may_check("192.0.2.1", None)


def test_global_limit_stops_many_addresses() -> None:
    limiter = _Limiter(
        Settings(
            data_dir=Path("/nonexistent"),
            failed_auth_per_minute=30,
            global_failed_auth_per_minute=20,
        )
    )
    limiter.succeeded("198.51.100.7", "knownknownkn")
    for i in range(20):
        limiter.failed_auth(f"10.2.0.{i}")
    # No single address is over its limit, but the total is.
    assert not limiter.may_check("203.0.113.9", "unknownunkno")
    assert not limiter.may_check("203.0.113.9", None)
    assert limiter.may_check("198.51.100.7", "knownknownkn")


def test_ipv6_addresses_count_by_prefix() -> None:
    limiter = _Limiter(
        Settings(data_dir=Path("/nonexistent"), failed_auth_per_minute=3)
    )
    for i in range(3):
        limiter.failed_auth(f"2001:db8::{i + 1}")
    assert not limiter.may_check("2001:db8::ffff", None)
    assert limiter.may_check("2001:db8:0:1::1", None)


def test_missing_scope(env: Env) -> None:
    r = env.client.post(
        f"{S}/generations",
        headers=env.h("reader", WS_LAPTOP),
        json={"path": "transcript.jsonl", "cause": "root"},
    )
    assert r.status_code == 403


def test_append_read_retry_and_conflict(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    r = append(env, "laptop", WS_LAPTOP, g, 0, b"one\n", activity=1)
    assert r.status_code == 200, r.text
    assert r.json()["length"] == 4
    assert r.json()["session"]["lease"]["host"] == "laptop"
    # Retry of the same segment.
    assert append(env, "laptop", WS_LAPTOP, g, 0, b"one\n").json()["length"] == 4
    r = append(env, "laptop", WS_LAPTOP, g, 4, b"two\n", compress=False)
    assert r.json()["length"] == 8
    r = append(env, "laptop", WS_LAPTOP, g, 4, b"other\n")
    assert r.status_code == 409
    assert r.json()["error"] == "conflict" and r.json()["length"] == 8
    assert "session" in r.json()
    r = env.client.get(f"{S}/generations/{g}?start=2", headers=env.h("laptop"))
    assert r.content == b"e\ntwo\n"
    manifest = env.client.get(S, headers=env.h("laptop")).json()
    assert manifest["files"][0]["active"] == g


def test_bad_digest_and_bad_bodies(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    headers = env.h(
        "laptop",
        WS_LAPTOP,
        **{"Hyperspace-Base-Length": "0", "Hyperspace-Sha256": "0" * 64},
    )
    r = env.client.post(f"{S}/generations/{g}/append", headers=headers, content=b"x")
    assert r.status_code == 400 and r.json()["error"] == "bad_digest"
    r = env.client.post(
        f"{S}/generations", headers=env.h("laptop", WS_LAPTOP), content=b"{not json"
    )
    assert r.status_code == 400
    r = env.client.post(
        f"{S}/generations",
        headers=env.h("laptop", WS_LAPTOP),
        json={"path": "../etc/passwd", "cause": "root"},
    )
    assert r.status_code == 400
    r = env.client.get("/v1/sessions/claude-code/..%2Fx", headers=env.h("laptop"))
    assert r.status_code in (400, 404)
    r = env.client.post(
        f"{S}/generations", headers=env.h("laptop"), json={"path": "x", "cause": "root"}
    )
    assert r.status_code == 400  # no workspace header


def test_oversized_and_bomb_bodies(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    big = b"x" * (8 * 1024 * 1024 + 1)
    r = append(env, "laptop", WS_LAPTOP, g, 0, big, compress=False)
    assert r.status_code == 413
    # Small when compressed, too large when decompressed.
    r = append(env, "laptop", WS_LAPTOP, g, 0, big, compress=True)
    assert r.status_code == 413


def test_runner_cannot_read_or_write_others_sessions(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    append(env, "laptop", WS_LAPTOP, g, 0, b"secret\n", activity=1)
    missing = env.client.get(
        "/v1/sessions/claude-code/does-not-exist", headers=env.h("agent")
    )
    hidden = env.client.get(S, headers=env.h("agent"))
    assert hidden.status_code == missing.status_code == 404
    assert hidden.json() == missing.json()
    r = env.client.get(f"{S}/generations/{g}", headers=env.h("agent"))
    assert r.status_code == 404
    # Writes to a session that it cannot read look the same as a missing one.
    r = append(env, "agent", WS_AGENT, g, 7, b"x\n")
    assert r.status_code == 404
    r = env.client.post(
        f"{S}/meta", headers=env.h("agent", WS_AGENT), json={"title": "x"}
    )
    assert r.status_code == 404
    # It cannot grant itself access either.
    r = env.client.post(f"{S}/grants", headers=env.h("agent"), json={"host": "agent-1"})
    assert r.status_code == 404
    listed = env.client.get("/v1/sessions", headers=env.h("agent")).json()
    assert listed["sessions"] == []
    assert env.client.get("/v1/workspaces", headers=env.h("agent")).json() == {
        "workspaces": []
    }


def test_grant_then_teleport(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    n = append(env, "laptop", WS_LAPTOP, g, 0, b"one\n", activity=1).json()["length"]
    # A reserve alone does not give read access.
    r = env.client.post(
        f"{S}/lease/surrender",
        headers=env.h("laptop"),
        json={"reserved_for": "agent-1"},
    )
    assert r.json()["lease"] == {
        "state": "reserved",
        "host": "agent-1",
        "since": r.json()["lease"]["since"],
    }
    assert env.client.get(S, headers=env.h("agent")).status_code == 404
    r = env.client.post(
        f"{S}/grants", headers=env.h("laptop"), json={"host": "agent-1"}
    )
    assert r.status_code == 200
    assert env.client.get(S, headers=env.h("agent")).status_code == 200
    r = env.client.post(
        f"{S}/teleports", headers=env.h("agent"), json={"workspace": WS_AGENT}
    )
    assert r.status_code == 200, r.text
    assert r.json()["session"]["lease"]["host"] == "agent-1"
    r = append(env, "agent", WS_AGENT, g, n, b"two\n", activity=1)
    assert r.status_code == 200
    causes = [
        p["cause"]
        for p in env.client.get(S, headers=env.h("laptop")).json()["placements"]
    ]
    assert causes == ["start", "teleport"]


def test_conflict_refuses_teleport_and_allows_fork(env: Env) -> None:
    g = new_gen(env, "laptop", WS_LAPTOP)
    n = append(env, "laptop", WS_LAPTOP, g, 0, b"base\n", activity=1).json()["length"]
    env.client.post(f"{S}/grants", headers=env.h("laptop"), json={"host": "agent-1"})
    append(env, "agent", WS_AGENT, g, n, b"agent\n", activity=1)
    d = new_gen(env, "laptop", WS_LAPTOP, parent=g, fork_offset=n, cause="diverge")
    r = append(env, "laptop", WS_LAPTOP, d, n, b"laptop\n", activity=1)
    assert r.json()["session"]["state"] == "conflicted"
    r = env.client.post(
        f"{S}/teleports",
        headers=env.h("laptop"),
        json={"workspace": WS_LAPTOP, "steal": True},
    )
    assert r.status_code == 409 and r.json()["error"] == "conflicted"
    r = env.client.post(
        f"{S}/forks", headers=env.h("laptop"), json={"generation": d, "offset": n + 7}
    )
    assert r.status_code == 200
    assert r.json()["session_id"] != SID


def test_blobs(env: Env) -> None:
    data = b"plan text"
    headers = env.h(
        "laptop", WS_LAPTOP, **{"Hyperspace-Sha256": hashlib.sha256(data).hexdigest()}
    )
    r = env.client.put(f"{S}/blobs/plans/my-plan.md", headers=headers, content=data)
    assert r.status_code == 200 and r.json()["version"] == 1
    r = env.client.get(f"{S}/blobs/plans/my-plan.md", headers=env.h("laptop"))
    assert r.content == data


def test_hosts_register_as_themselves(env: Env) -> None:
    r = env.client.put(
        "/v1/hosts/self",
        headers=env.h("agent"),
        json={"kind": "runner", "home": "/home/agent", "name": "laptop"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "agent-1"
    hosts = env.client.get("/v1/hosts", headers=env.h("laptop")).json()["hosts"]
    assert [h["name"] for h in hosts] == ["agent-1"]
    r = env.client.put("/v1/hosts/self", headers=env.h("agent"), json={"kind": "pod"})
    assert r.status_code == 400


def test_revocation_takes_effect(env: Env) -> None:
    assert env.client.get("/v1/whoami", headers=env.h("agent")).status_code == 200
    kept = tuple(c for name, c in env.creds.items() if name != "agent")
    env.accounts_path.write_text(
        dump_accounts(Accounts({"default": Account(id="default", credentials=kept)}))
    )
    assert env.client.get("/v1/whoami", headers=env.h("agent")).status_code == 401
    assert env.client.get("/v1/whoami", headers=env.h("laptop")).status_code == 200
