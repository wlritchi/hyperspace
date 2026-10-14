"""Client configuration: `$XDG_CONFIG_HOME/hyperspace/config.toml` and the
environment.

| Setting            | Environment               | config.toml          |
| ------------------ | ------------------------- | -------------------- |
| Server URL         | `HYPERSPACE_URL`          | `url` (no default)   |
| Token              | `HYPERSPACE_TOKEN`        |                      |
| Token file         | `HYPERSPACE_TOKEN_FILE`   | `token_file`         |
| Token cache time   |                           | `token_cache_ttl`    |
| Host name          | `HYPERSPACE_HOST`         | `host`               |
| Host kind          | `HYPERSPACE_HOST_KIND`    | `host_kind`          |
| Excluded cwds      |                           | `exclude_cwd`        |
| Checkpoint seconds |                           | `checkpoint_interval`|

With no token in the environment or a file, the token comes from the token
cache or the system keyring (see `credentials`).
"""

from __future__ import annotations

import ipaddress
import os
import re
import socket
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from hyperspace_client import credentials
from hyperspace_client.credentials import CredentialError, KeyringUse, TokenSource
from hyperspace_protocol import HostKind, validate_host_name


class ConfigError(Exception):
    """The client is not configured, or the configuration is not valid."""


def _xdg(name: str, default: str) -> Path:
    value = os.environ.get(name)
    return Path(value) if value else Path.home() / default


def config_dir() -> Path:
    return _xdg("XDG_CONFIG_HOME", ".config") / "hyperspace"


def state_dir() -> Path:
    return _xdg("XDG_STATE_HOME", ".local/state") / "hyperspace"


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False


def check_url(url: str) -> str:
    """Return `url` without a trailing slash. Raise unless it is https, or
    http to a loopback address."""
    parts = urlsplit(url)
    if parts.scheme == "https" and parts.hostname:
        return url.rstrip("/")
    if parts.scheme == "http" and parts.hostname and _is_loopback(parts.hostname):
        return url.rstrip("/")
    raise ConfigError(
        f"server URL must be https (or http to a loopback address): {url!r}"
    )


def default_host_name() -> str:
    """The system hostname, lowercased and cut to a valid host name."""
    name = socket.gethostname().split(".")[0].lower()
    name = re.sub(r"[^a-z0-9._-]", "-", name).lstrip("._-")[:64]
    return name or "host"


@dataclass
class Config:
    url: str | None = None
    token: str | None = field(default=None, repr=False)
    token_source: TokenSource | None = None
    token_cache_ttl: int = 86400
    keyring_use: KeyringUse = "cache-only"
    host: str = ""
    host_kind: HostKind = "workstation"
    exclude_cwd: list[str] = field(default_factory=list[str])
    checkpoint_interval: float = 10.0
    state_dir: Path = field(default_factory=state_dir)

    def require_server(self) -> tuple[str, str]:
        """Return (url, token). Raise `ConfigError` if either is missing."""
        if not self.url:
            raise ConfigError(
                "no server URL: set HYPERSPACE_URL or `url` in "
                f"{config_dir() / 'config.toml'}"
            )
        if not self.token:
            try:
                found = credentials.find(
                    self.url, self.state_dir, self.token_cache_ttl, self.keyring_use
                )
            except CredentialError as exc:
                raise ConfigError(f"no token: {exc}") from None
            if found is None:
                raise ConfigError(
                    "no token: run `hyperspace login`, or set HYPERSPACE_TOKEN, "
                    "HYPERSPACE_TOKEN_FILE, or `token_file` in "
                    f"{config_dir() / 'config.toml'}"
                )
            token, self.token_source = found
            self.token = token
            return self.url, token
        return self.url, self.token


def _read_token(path: Path) -> str:
    try:
        token = path.expanduser().read_text().strip()
    except OSError as exc:
        raise ConfigError(f"cannot read the token file {path}: {exc}") from None
    if not token:
        raise ConfigError(f"the token file {path} is empty")
    return token


def load(env: dict[str, str] | None = None, path: Path | None = None) -> Config:
    env = dict(os.environ) if env is None else env
    path = path or Path(env.get("HYPERSPACE_CONFIG") or config_dir() / "config.toml")
    data: dict[str, Any] = {}
    if path.exists():
        try:
            data = tomllib.loads(path.read_text())
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"{path}: {exc}") from None
    cfg = Config()
    url = env.get("HYPERSPACE_URL") or data.get("url")
    if url:
        cfg.url = check_url(str(url))
    if env.get("HYPERSPACE_TOKEN"):
        cfg.token = env["HYPERSPACE_TOKEN"].strip()
        cfg.token_source = "environment"
    else:
        token_file = env.get("HYPERSPACE_TOKEN_FILE") or data.get("token_file")
        if token_file:
            cfg.token = _read_token(Path(str(token_file)))
            cfg.token_source = "file"
        elif (config_dir() / "token").exists():
            cfg.token = _read_token(config_dir() / "token")
            cfg.token_source = "file"
    host = env.get("HYPERSPACE_HOST") or data.get("host") or default_host_name()
    try:
        cfg.host = validate_host_name(str(host))
    except ValueError as exc:
        raise ConfigError(str(exc)) from None
    kind = env.get("HYPERSPACE_HOST_KIND") or data.get("host_kind") or "workstation"
    if kind not in ("workstation", "runner"):
        raise ConfigError(f"host_kind must be workstation or runner, not {kind!r}")
    cfg.host_kind = kind
    excludes = data.get("exclude_cwd", [])
    if not isinstance(excludes, list):
        raise ConfigError("exclude_cwd must be a list of glob patterns")
    cfg.exclude_cwd = [str(e) for e in excludes]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    interval = data.get("checkpoint_interval", 10.0)
    if not isinstance(interval, int | float) or interval < 1:
        raise ConfigError("checkpoint_interval must be a number of seconds >= 1")
    cfg.checkpoint_interval = float(interval)
    ttl = data.get("token_cache_ttl", 86400)
    if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl < 60:
        raise ConfigError("token_cache_ttl must be a whole number of seconds >= 60")
    cfg.token_cache_ttl = ttl
    if env.get("XDG_STATE_HOME"):
        cfg.state_dir = Path(env["XDG_STATE_HOME"]) / "hyperspace"
    return cfg
