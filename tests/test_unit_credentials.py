# vim: filetype=python
"""Where the client finds its token: the environment, a file, the token cache,
and the keyring (see "Credentials" in the spec)."""

import io
import logging
import sys
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from _hs import MemoryCache, MemoryKeyring, Server, make_server
from hyperspace_client import cli, credentials
from hyperspace_client.adapters.claude_code import ClaudeCode
from hyperspace_client.api import Api
from hyperspace_client.config import Config, ConfigError, load
from hyperspace_client.credentials import NotNow
from hyperspace_client.keycache import KernelCache
from hyperspace_client.state import State
from hyperspace_client.sync import Context

URL = "https://hs.test"
NAME = "token:hs.test"
KEY = ("hyperspace", "hs.test")


def cfg_with(tmp_path: Path, **kwargs: Any) -> Config:
    return Config(url=URL, host="laptop", state_dir=tmp_path / "state", **kwargs)


# --- Order --------------------------------------------------------------------


def test_the_environment_and_a_file_come_before_the_keyring(
    tmp_path: Path, fake_keyring: MemoryKeyring
) -> None:
    fake_keyring.items[KEY] = "from-keyring"
    token_file = tmp_path / "token"
    token_file.write_text("from-file\n")
    env = {"HYPERSPACE_URL": URL, "XDG_CONFIG_HOME": str(tmp_path)}
    cfg = load({**env, "HYPERSPACE_TOKEN": "from-env"}, tmp_path / "none.toml")
    assert cfg.require_server() == (URL, "from-env")
    assert cfg.token_source == "environment"
    cfg = load(
        {**env, "HYPERSPACE_TOKEN_FILE": str(token_file)}, tmp_path / "none.toml"
    )
    assert cfg.require_server() == (URL, "from-file")
    assert cfg.token_source == "file"
    assert fake_keyring.reads == 0


@pytest.mark.parametrize(
    "token", ["a\nb", "a\rb", "a b", "a\tb", "a\x00b", "a\x7fb", "t\u00f6ken"]
)
def test_a_token_with_whitespace_or_a_control_character_is_refused(
    tmp_path: Path, token: str
) -> None:
    # Such a token is not a valid header value, and each request would fail.
    none = tmp_path / "none.toml"
    env = {"HYPERSPACE_URL": URL, "XDG_CONFIG_HOME": str(tmp_path)}
    with pytest.raises(ConfigError, match="HYPERSPACE_TOKEN"):
        load({**env, "HYPERSPACE_TOKEN": token}, none)
    token_file = tmp_path / "token"
    token_file.write_text(token + "\n")
    with pytest.raises(ConfigError, match=str(token_file)):
        load({**env, "HYPERSPACE_TOKEN_FILE": str(token_file)}, none)
    # Whitespace around the token is not part of it.
    cfg = load({**env, "HYPERSPACE_TOKEN": " hs_a-b_c \n"}, none)
    assert cfg.token == "hs_a-b_c"


def test_a_token_from_the_cache_is_checked(
    tmp_path: Path, token_cache: MemoryCache
) -> None:
    token_cache.items[NAME] = "from\ncache"
    with pytest.raises(ConfigError, match="token cache"):
        cfg_with(tmp_path, keyring_use="interactive").require_server()


def test_the_launcher_refuses_a_bad_token_from_the_cache(
    tmp_path: Path, token_cache: MemoryCache
) -> None:
    cfg = cfg_with(tmp_path, keyring_use="interactive")
    token_cache.items[NAME] = "first"
    cfg.require_server()
    api = Api(URL, "first")
    state = State(tmp_path / "state")
    ctx = Context(
        api=api, state=state, adapter=ClaudeCode(tmp_path / "claude"), host="laptop"
    )
    # A new login with a bad token: the launcher keeps the token that works.
    token_cache.items[NAME] = "new\ntoken"
    cli._refresh_token(cfg, ctx)  # pyright: ignore[reportPrivateUsage]
    assert (cfg.token, ctx.api) == ("first", api)
    token_cache.items[NAME] = "second"
    cli._refresh_token(cfg, ctx)  # pyright: ignore[reportPrivateUsage]
    assert cfg.token == "second"
    assert ctx.api is not api
    ctx.api.close()
    state.close()


def test_the_cache_comes_before_the_keyring(
    tmp_path: Path, fake_keyring: MemoryKeyring, token_cache: MemoryCache
) -> None:
    fake_keyring.items[KEY] = "from-keyring"
    token_cache.items[NAME] = "from-cache"
    cfg = cfg_with(tmp_path, keyring_use="interactive")
    assert cfg.require_server() == (URL, "from-cache")
    assert cfg.token_source == "cache"
    assert fake_keyring.reads == 0


# --- Hooks --------------------------------------------------------------------


def test_hooks_never_read_the_keyring(
    tmp_path: Path, fake_keyring: MemoryKeyring
) -> None:
    fake_keyring.items[KEY] = "from-keyring"
    cfg = cfg_with(tmp_path)  # cache-only
    with pytest.raises(ConfigError, match="not in the cache"):
        cfg.require_server()
    assert fake_keyring.reads == 0


def test_a_session_start_fills_the_cache_for_later_hooks(
    tmp_path: Path, fake_keyring: MemoryKeyring, token_cache: MemoryCache
) -> None:
    fake_keyring.items[KEY] = "from-keyring"
    cfg = cfg_with(tmp_path, keyring_use="once")
    assert cfg.require_server() == (URL, "from-keyring")
    assert cfg.token_source == "keyring"
    assert token_cache.items[NAME] == "from-keyring"
    later = cfg_with(tmp_path)
    assert later.require_server() == (URL, "from-keyring")
    assert later.token_source == "cache"
    assert fake_keyring.reads == 1


def test_a_dismissed_prompt_is_not_shown_again_soon(
    tmp_path: Path, fake_keyring: MemoryKeyring, token_cache: MemoryCache
) -> None:
    state = tmp_path / "state"
    fake_keyring.items[KEY] = "from-keyring"
    fake_keyring.fail = RuntimeError("the user dismissed the prompt")
    with pytest.raises(credentials.CredentialError, match="dismissed"):
        credentials.find(URL, state, 60, "once")
    fake_keyring.fail = None
    with pytest.raises(NotNow, match="less than 10 minutes ago"):
        credentials.find(URL, state, 60, "once")
    assert fake_keyring.reads == 1
    # A command that the user runs asks again, and clears the marker.
    assert credentials.find(URL, state, 60, "interactive") == (
        "from-keyring",
        "keyring",
    )
    token_cache.items.clear()
    assert credentials.find(URL, state, 60, "once") == ("from-keyring", "keyring")


def test_many_session_starts_give_one_prompt(
    tmp_path: Path, fake_keyring: MemoryKeyring
) -> None:
    fake_keyring.items[KEY] = "from-keyring"
    fake_keyring.gate = threading.Event()
    results: list[tuple[str, str] | None] = []

    def start() -> None:
        results.append(credentials.find(URL, tmp_path / "state", 60, "once"))

    threads = [threading.Thread(target=start) for _ in range(4)]
    for t in threads:
        t.start()
    # One process prompts; the others wait for its lock.
    while fake_keyring.reads == 0:
        threading.Event().wait(0.01)
    threading.Event().wait(0.3)
    fake_keyring.gate.set()
    for t in threads:
        t.join(10)
    assert fake_keyring.reads == 1
    assert sorted(r[1] for r in results if r) == ["cache", "cache", "cache", "keyring"]


def test_no_token_anywhere(tmp_path: Path) -> None:
    cfg = cfg_with(tmp_path, keyring_use="interactive")
    with pytest.raises(ConfigError, match="hyperspace login"):
        cfg.require_server()


def test_a_backend_that_is_not_installed_says_how_to_install_it() -> None:
    exc = ModuleNotFoundError(
        "No module named 'example_backend'", name="example_backend"
    )
    text = credentials._describe(exc)
    assert "the package that provides example_backend" in text
    assert "uv tool install --force --with <package>" in text


# --- login, whoami, logout ----------------------------------------------------


@pytest.fixture
def server(tmp_path: Path) -> Iterator[Server]:
    s = make_server(tmp_path / "data")
    yield s
    s.close()


@pytest.fixture
def cli_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, server: Server
) -> Iterator[None]:
    for name in ("HYPERSPACE_TOKEN", "HYPERSPACE_TOKEN_FILE", "HYPERSPACE_CONFIG"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HYPERSPACE_URL", URL)
    monkeypatch.setenv("HYPERSPACE_HOST", "laptop")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))

    def api(url: str, token: str, **kwargs: Any) -> Api:
        client = TestClient(
            server.app,
            base_url="http://testserver/v1",
            headers={"Authorization": f"Bearer {token}"},
        )
        server.clients.append(client)
        return Api("http://testserver", token, client=client)  # type: ignore[arg-type]

    monkeypatch.setattr(cli, "Api", api)
    root = logging.getLogger()
    handlers = list(root.handlers)
    yield
    for h in root.handlers[len(handlers) :]:
        root.removeHandler(h)
        h.close()


def run(monkeypatch: pytest.MonkeyPatch, *argv: str, stdin: str = "") -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))
    return cli.main(list(argv))


@pytest.mark.usefixtures("cli_env")
def test_login_whoami_logout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    server: Server,
    fake_keyring: MemoryKeyring,
    token_cache: MemoryCache,
) -> None:
    token = server.tokens["laptop"]
    assert run(monkeypatch, "login", "--stdin", stdin=token + "\n") == 0
    assert fake_keyring.items[KEY] == token
    assert token_cache.items[NAME] == token
    assert "stored the token for laptop" in capsys.readouterr().out

    assert run(monkeypatch, "whoami") == 0
    assert "token from: cache" in capsys.readouterr().err
    token_cache.items.clear()
    assert run(monkeypatch, "whoami") == 0
    assert "token from: keyring" in capsys.readouterr().err
    assert token_cache.items[NAME] == token

    assert run(monkeypatch, "logout") == 0
    assert "from the keyring and the cache" in capsys.readouterr().out
    assert fake_keyring.items == {} and token_cache.items == {}
    assert run(monkeypatch, "whoami") == 2
    assert "hyperspace login" in capsys.readouterr().err


@pytest.mark.usefixtures("cli_env")
def test_login_refuses_a_bad_token(
    monkeypatch: pytest.MonkeyPatch, fake_keyring: MemoryKeyring
) -> None:
    assert run(monkeypatch, "login", "--stdin", stdin="hsp_aaaaaaaaaaaa_bad") == 1
    assert fake_keyring.items == {}


@pytest.mark.usefixtures("cli_env")
def test_login_warns_about_a_credential_for_another_host(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    server: Server,
) -> None:
    monkeypatch.setenv("HYPERSPACE_HOST", "desktop")
    assert run(monkeypatch, "login", "--stdin", stdin=server.tokens["laptop"]) == 0
    assert "this credential is for host 'laptop'" in capsys.readouterr().err


@pytest.mark.usefixtures("cli_env")
def test_a_hook_command_does_not_read_the_keyring(
    monkeypatch: pytest.MonkeyPatch,
    server: Server,
    fake_keyring: MemoryKeyring,
) -> None:
    fake_keyring.items[KEY] = server.tokens["laptop"]
    assert run(monkeypatch, "--quiet", "whoami") == 0
    assert fake_keyring.reads == 0


# --- The kernel keyring -------------------------------------------------------


def test_the_kernel_cache() -> None:
    try:
        cache = KernelCache()
    except OSError:
        pytest.skip("no kernel keyring")
    name = f"test:{uuid.uuid4().hex}"
    assert cache.get(name, 60) is None
    cache.put(name, "value", 60)
    try:
        assert cache.get(name, 60) == "value"
        cache.put(name, "x" * 10000, 60)
        assert cache.get(name, 60) == "x" * 10000
    finally:
        assert cache.delete(name)
    assert cache.get(name, 60) is None
    assert not cache.delete(name)
