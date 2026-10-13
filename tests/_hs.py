# vim: filetype=python
"""Test helpers: an in-process server, API clients, and a fake Claude Code
config directory."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from starlette.testclient import TestClient

from accounts import (
    Account,
    Accounts,
    AccountsFile,
    Credential,
    dump_accounts,
    new_token,
)
from app import Settings, create_app
from hyperspace_client.adapters.claude_code import ClaudeCode, sanitize_path
from hyperspace_client.api import Api
from hyperspace_client.state import State
from hyperspace_client.sync import Context
from store import Store


@dataclass
class Server:
    store: Store
    app: Any
    tokens: dict[str, str] = field(default_factory=dict[str, str])
    clients: list[TestClient] = field(default_factory=list[TestClient])

    def api(self, who: str) -> Api:
        client = TestClient(
            self.app,
            base_url="http://testserver/v1",
            headers={"Authorization": f"Bearer {self.tokens[who]}"},
        )
        self.clients.append(client)
        return Api("http://testserver", self.tokens[who], client=client)  # type: ignore[arg-type]

    def close(self) -> None:
        for c in self.clients:
            c.close()
        self.store.close()


def make_server(
    data: Path, hosts: dict[str, tuple[str, set[str], str]] | None = None
) -> Server:
    """`hosts` maps a name to (host, scopes, read scope)."""
    hosts = hosts or {
        "laptop": ("laptop", {"append", "read", "grant"}, "all"),
        "agent": ("agent-1", {"append", "read", "grant"}, "granted"),
    }
    data.mkdir(parents=True, exist_ok=True)
    tokens: dict[str, str] = {}
    creds: list[Credential] = []
    for name, (host, scopes, read) in hosts.items():
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
    store = Store(data)
    app = create_app(
        store,
        AccountsFile(path),
        Settings(data_dir=data, requests_per_minute=100_000),
    )
    return Server(store, app, tokens)


class FakeClaude:
    """A Claude Code config directory with one project per workspace."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.adapter = ClaudeCode(root)

    def transcript(self, workspace: str, sid: str | None = None) -> Path:
        sid = sid or str(uuid.uuid4())
        project = self.root / "projects" / sanitize_path(workspace)
        project.mkdir(parents=True, exist_ok=True)
        path = project / f"{sid}.jsonl"
        path.touch()
        return path

    @staticmethod
    def write(path: Path, *records: dict[str, Any], partial: str = "") -> None:
        with open(path, "a") as f:
            f.writelines(json.dumps(r) + "\n" for r in records)
            f.write(partial)


def prompt(text: str, cwd: str = "/home/wlritchi/hyperspace") -> dict[str, Any]:
    return {
        "type": "user",
        "message": {"role": "user", "content": text},
        "cwd": cwd,
        "timestamp": "2026-10-13T10:00:00Z",
        "version": "2.1.288",
    }


def tool_call(name: str = "Bash") -> dict[str, Any]:
    return {
        "type": "assistant",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "tool_use", "id": "toolu_1", "name": name, "input": {}}
            ],
        },
        "timestamp": "2026-10-13T10:00:01Z",
    }


def text_reply(text: str = "ok") -> dict[str, Any]:
    return {
        "type": "assistant",
        "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
        "timestamp": "2026-10-13T10:00:02Z",
    }


def tool_result() -> dict[str, Any]:
    return {
        "type": "user",
        "message": {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "x"}
            ],
        },
        "toolUseResult": {"stdout": "x"},
    }


def context(api: Api, state_dir: Path, fake: FakeClaude, host: str) -> Context:
    return Context(api=api, state=State(state_dir), adapter=fake.adapter, host=host)
