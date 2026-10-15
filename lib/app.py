# vim: filetype=python
"""The Hyperspace HTTP API (Starlette).

See "HTTP API (v1)" and "Security" in the spec. `create_app` builds the
application from a store, an accounts file, and settings, so that tests can
run it in-process.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import zstandard
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from accounts import AccountsFile, Principal, parse_token
from hyperspace_protocol import (
    API_PREFIX,
    CAPABILITIES,
    HEADER_ACTIVITY,
    HEADER_BASE_LENGTH,
    HEADER_SHA256,
    HEADER_WORKSPACE,
    HEADER_WORKSPACE_UTF8,
    HOST_KINDS,
    MAX_BODY,
    ValidationError,
    decode_workspace,
    validate_file_path,
    validate_harness,
    validate_host_name,
    validate_id,
    validate_session_id,
    validate_sha256,
    validate_workspace,
)
from store import Conflict, SessionKey, Store, StoreError, Writer

log = logging.getLogger("hyperspace")

_JSON_LIMIT = 64 * 1024

_STATUS = {
    "not_found": 404,
    "conflict": 409,
    "exists": 409,
    "lease_held": 409,
    "conflicted": 409,
    "not_holder": 409,
    "bad_request": 400,
    "bad_range": 400,
    "bad_digest": 400,
    "wrong_kind": 400,
    "unauthorized": 401,
    "forbidden": 403,
    "too_large": 413,
    "rate_limited": 429,
}


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    listen_host: str = "127.0.0.1"
    port: int = 8139
    max_body: int = MAX_BODY
    trusted_proxies: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()
    requests_per_minute: int = 1200
    bytes_per_day: int = 4 * 1024**3
    failed_auth_per_minute: int = 30
    global_failed_auth_per_minute: int = 1000

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        env = dict(os.environ) if env is None else env
        default_data = (
            Path(env.get("XDG_DATA_HOME") or Path.home() / ".local/share")
            / "hyperspace"
        )
        proxies = tuple(
            ipaddress.ip_network(p.strip(), strict=False)
            for p in env.get("HYPERSPACE_TRUSTED_PROXIES", "").split(",")
            if p.strip()
        )
        return cls(
            data_dir=Path(env.get("HYPERSPACE_DATA_DIR") or default_data),
            listen_host=env.get("HYPERSPACE_LISTEN_HOST", "127.0.0.1"),
            port=int(env.get("HYPERSPACE_PORT", "8139")),
            max_body=min(int(env.get("HYPERSPACE_MAX_BODY", str(MAX_BODY))), MAX_BODY),
            trusted_proxies=proxies,
            requests_per_minute=int(env.get("HYPERSPACE_RATE_PER_MINUTE", "1200")),
            bytes_per_day=int(env.get("HYPERSPACE_BYTES_PER_DAY", str(4 * 1024**3))),
            failed_auth_per_minute=int(
                env.get("HYPERSPACE_FAILED_AUTH_PER_MINUTE", "30")
            ),
            global_failed_auth_per_minute=int(
                env.get("HYPERSPACE_GLOBAL_FAILED_AUTH_PER_MINUTE", "1000")
            ),
        )


class ApiError(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(detail or code)
        self.code = code
        self.detail = detail


@dataclass
class _Limiter:
    """In-memory limits. They reset when the server restarts.

    Failed authentications are counted for each client address (IPv6 by /64
    prefix). An address over the limit gets no further checks of its tokens,
    so it cannot keep guessing. A credential that authenticated from that
    address in the last day is still checked, so that the failures of other
    clients behind a shared address do not lock out known clients. Both tables
    have a fixed maximum size.
    """

    settings: Settings
    max_entries: int = 10_000
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _requests: dict[str, deque[float]] = field(default_factory=dict[str, deque[float]])
    _failures: OrderedDict[str, deque[float]] = field(
        default_factory=OrderedDict[str, deque[float]]
    )
    _known: OrderedDict[tuple[str, str], float] = field(
        default_factory=OrderedDict[tuple[str, str], float]
    )
    # Failed authentications from all addresses in the last minute. Its
    # length is at most the global limit.
    _global: deque[float] = field(default_factory=deque[float])
    _bytes: dict[tuple[str, str], int] = field(
        default_factory=dict[tuple[str, str], int]
    )

    @staticmethod
    def address_key(address: str) -> str:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return address
        if ip.version == 6:
            return str(ipaddress.ip_network(f"{ip}/64", strict=False))
        return str(ip)

    @staticmethod
    def _trim(window: deque[float], now: float) -> None:
        while window and window[0] <= now - 60:
            window.popleft()

    def request(self, credential: str) -> bool:
        with self._lock:
            now = time.monotonic()
            window = self._requests.setdefault(credential, deque())
            self._trim(window, now)
            if len(window) >= self.settings.requests_per_minute:
                return False
            window.append(now)
            return True

    def failed_auth(self, client: str) -> None:
        key = self.address_key(client)
        with self._lock:
            now = time.monotonic()
            window = self._failures.pop(key, None) or deque()
            self._trim(window, now)
            window.append(now)
            self._failures[key] = window
            if len(self._failures) > self.max_entries:
                self._evict(now)
            self._trim(self._global, now)
            if len(self._global) < self.settings.global_failed_auth_per_minute:
                self._global.append(now)

    def _evict(self, now: float) -> None:
        """Cut the failure table to 90% of its size, in one pass. Drop
        entries that are not blocked first, oldest first. The cut is larger
        than one entry, so the pass runs at most once for each tenth of the
        table. The global limit applies whatever this table holds."""
        target = self.max_entries * 9 // 10
        limit = self.settings.failed_auth_per_minute
        for key in list(self._failures):
            if len(self._failures) <= target:
                return
            window = self._failures[key]
            self._trim(window, now)
            if len(window) < limit:
                del self._failures[key]
        while len(self._failures) > target:
            self._failures.popitem(last=False)

    def succeeded(self, client: str, credential: str) -> None:
        key = (self.address_key(client), credential)
        with self._lock:
            self._known.pop(key, None)
            self._known[key] = time.monotonic()
            while len(self._known) > self.max_entries:
                self._known.popitem(last=False)

    def may_check(self, client: str, credential: str | None) -> bool:
        """False if the server must refuse this request without checking
        its token.

        Unknown credentials are refused from an address over its limit, and
        from all addresses while the global limit is reached. A credential
        that authenticated from this address in the last day is always
        checked.
        """
        key = self.address_key(client)
        now = time.monotonic()
        with self._lock:
            window = self._failures.get(key)
            if window is not None:
                self._trim(window, now)
                if not window:
                    del self._failures[key]
                    window = None
            self._trim(self._global, now)
            blocked = (
                window is not None
                and len(window) >= self.settings.failed_auth_per_minute
            ) or len(self._global) >= self.settings.global_failed_auth_per_minute
            if not blocked:
                return True
            if credential is None:
                return False
            seen = self._known.get((key, credential))
            return seen is not None and now - seen < 86_400

    def add_bytes(self, credential: str, count: int) -> bool:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        with self._lock:
            key = (credential, day)
            total = self._bytes.get(key, 0) + count
            if total > self.settings.bytes_per_day:
                return False
            self._bytes = {k: v for k, v in self._bytes.items() if k[1] == day}
            self._bytes[key] = total
            return True


_warned_peers: set[str] = set()


def _is_trusted(
    address: str, trusted: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]
) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    return any(ip in net for net in trusted)


def _warn_untrusted_forwarding(peer: str) -> None:
    """Log once for each peer that sends X-Forwarded-For but is not a
    trusted proxy. Behind such a proxy, every client has the proxy's
    address, so the failed-auth limit applies to all clients together."""
    if peer in _warned_peers or len(_warned_peers) > 100:
        return
    _warned_peers.add(peer)
    log.warning(
        "%s sends X-Forwarded-For but is not in HYPERSPACE_TRUSTED_PROXIES; "
        "all clients behind it share one address for rate limits",
        peer,
    )


def client_address(
    request: Request,
    trusted: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...],
) -> str:
    """The client address. `X-Forwarded-For` counts only through trusted
    proxies: the result is the rightmost address that is not trusted."""
    peer = request.client.host if request.client else "unknown"
    chain = [peer]
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        chain = [a.strip() for a in forwarded.split(",") if a.strip()] + [peer]
        if not _is_trusted(peer, trusted):
            _warn_untrusted_forwarding(peer)
    for address in reversed(chain):
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return address
        if not any(ip in net for net in trusted):
            return address
    return chain[0]


def blob_path(request: Request) -> str:
    """The session file path of a blob route, decoded once from the raw
    request path. Some ASGI layers decode `scope["path"]` more than once,
    which would turn an encoded `%20` in a file name into a space."""
    raw = request.scope.get("raw_path")
    if isinstance(raw, bytes):
        text = raw.decode("latin-1").split("?", 1)[0]
        marker = "/blobs/"
        index = text.find(marker)
        if index >= 0:
            return unquote(text[index + len(marker) :])
    return str(request.path_params["path"])


Handler = Callable[[Request, Principal], Awaitable[Response]]


def create_app(store: Store, accounts: AccountsFile, settings: Settings) -> Starlette:
    limiter = _Limiter(settings)

    def now() -> datetime:
        return datetime.now(UTC)

    def error(
        code: str, detail: str = "", body: dict[str, Any] | None = None
    ) -> JSONResponse:
        payload: dict[str, Any] = {"error": code}
        if detail:
            payload["detail"] = detail
        if body:
            payload.update(body)
        return JSONResponse(payload, status_code=_STATUS.get(code, 400))

    def authed(
        *scopes: str,
    ) -> Callable[[Handler], Callable[[Request], Awaitable[Response]]]:
        def wrap(handler: Handler) -> Callable[[Request], Awaitable[Response]]:
            async def endpoint(request: Request) -> Response:
                header = request.headers.get("authorization", "")
                token = header[7:].strip() if header.startswith("Bearer ") else ""
                parsed = parse_token(token)
                credential_id = parsed[0] if parsed is not None else None
                client = client_address(request, settings.trusted_proxies)
                if not limiter.may_check(client, credential_id):
                    return error("rate_limited", "too many failed authentications")
                principal = accounts.authenticate(token, now()) if token else None
                if principal is None:
                    limiter.failed_auth(client)
                    return error("unauthorized")
                limiter.succeeded(client, principal.credential)
                if not limiter.request(principal.credential):
                    return error("rate_limited", "too many requests")
                missing = [s for s in scopes if s not in principal.scopes]
                if missing:
                    return error("forbidden", f"missing scope: {', '.join(missing)}")
                try:
                    return await handler(request, principal)
                except Conflict as exc:
                    state = await run_in_threadpool(
                        store.session_state, session_key(request, principal)
                    )
                    return error(
                        "conflict",
                        exc.detail,
                        {
                            "generation": exc.generation,
                            "length": exc.length,
                            "session": state,
                        },
                    )
                except StoreError as exc:
                    return error(exc.code, exc.detail)
                except ApiError as exc:
                    return error(exc.code, exc.detail)
                except ValidationError as exc:
                    return error("bad_request", str(exc))

            return endpoint

        return wrap

    def session_key(request: Request, principal: Principal) -> SessionKey:
        harness = validate_harness(request.path_params["harness"])
        sid = validate_session_id(request.path_params["sid"])
        return SessionKey(principal.account, harness, sid)

    def writer(request: Request, principal: Principal) -> Writer:
        encoded = request.headers.get(HEADER_WORKSPACE_UTF8)
        workspace = (
            decode_workspace(encoded)
            if encoded is not None
            else request.headers.get(HEADER_WORKSPACE)
        )
        if workspace is None:
            raise ApiError("bad_request", f"missing {HEADER_WORKSPACE} header")
        return Writer(principal.account, principal.host, validate_workspace(workspace))

    async def read_body(request: Request, limit: int) -> bytes:
        declared = request.headers.get("content-length")
        if declared is not None and int(declared) > limit:
            raise ApiError("too_large", f"body is larger than {limit} bytes")
        chunks: list[bytes] = []
        size = 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > limit:
                raise ApiError("too_large", f"body is larger than {limit} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    async def read_data(request: Request) -> bytes:
        """Read a file-data body, and decompress it if it is zstd."""
        raw = await read_body(request, settings.max_body)
        encoding = request.headers.get("content-encoding", "identity").lower()
        if encoding == "identity":
            return raw
        if encoding != "zstd":
            raise ApiError("bad_request", f"unsupported encoding {encoding}")
        try:
            reader = zstandard.ZstdDecompressor().stream_reader(raw)
            data = reader.read(settings.max_body + 1)
        except zstandard.ZstdError as exc:
            raise ApiError("bad_request", f"invalid zstd body: {exc}") from None
        if len(data) > settings.max_body:
            raise ApiError("too_large", "decompressed body is too large")
        return data

    async def read_json(request: Request) -> dict[str, Any]:
        raw = await read_body(request, _JSON_LIMIT)
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError("bad_request", f"invalid JSON: {exc}") from None
        if not isinstance(value, dict):
            raise ApiError("bad_request", "the body must be a JSON object")
        return value  # type: ignore[return-value]

    async def require_readable(key: SessionKey, principal: Principal) -> None:
        readable = await run_in_threadpool(
            store.can_read,
            key,
            principal.host,
            principal.read,  # type: ignore[arg-type]
        )
        if not readable:
            raise StoreError("not_found", "session")

    async def require_writable(key: SessionKey, principal: Principal) -> None:
        """Writes to an existing session need read access to it, so that a
        host cannot add branches to sessions that it cannot see."""
        exists = await run_in_threadpool(store.session_exists, key)
        if exists:
            await require_readable(key, principal)

    def int_param(request: Request, name: str, default: int | None) -> int | None:
        value = request.query_params.get(name)
        if value is None:
            return default
        try:
            result = int(value)
        except ValueError:
            raise ApiError("bad_request", f"{name} must be an integer") from None
        if result < 0:
            raise ApiError("bad_request", f"{name} must not be negative")
        return result

    # --- Endpoints --------------------------------------------------------

    async def healthz(request: Request) -> Response:
        return PlainTextResponse("ok")

    @authed()
    async def whoami(request: Request, principal: Principal) -> Response:
        return JSONResponse(
            {
                "account": principal.account,
                "host": principal.host,
                "scopes": sorted(principal.scopes),
                "read_scope": principal.read,
                "capabilities": CAPABILITIES,
            }
        )

    @authed("append")
    async def put_host(request: Request, principal: Principal) -> Response:
        body = await read_json(request)
        kind = body.get("kind")
        if kind not in HOST_KINDS:
            raise ApiError("bad_request", f"kind must be one of {list(HOST_KINDS)}")
        info: dict[str, Any] = {"kind": kind}
        for name in ("home", "os"):
            if isinstance(body.get(name), str):
                info[name] = body[name][:4096]
        dirs = body.get("config_dirs")
        if isinstance(dirs, list):
            info["config_dirs"] = [str(d)[:4096] for d in dirs[:16]]
        record = await run_in_threadpool(
            store.register_host,
            principal.account,
            principal.host,
            info,  # type: ignore[arg-type]
        )
        return JSONResponse(record)

    @authed("read")
    async def get_hosts(request: Request, principal: Principal) -> Response:
        hosts = await run_in_threadpool(store.list_hosts, principal.account)
        return JSONResponse({"hosts": hosts})

    @authed("read")
    async def get_workspaces(request: Request, principal: Principal) -> Response:
        rows = await run_in_threadpool(
            store.list_workspaces,
            principal.account,
            principal.host,
            principal.read,  # type: ignore[arg-type]
        )
        return JSONResponse({"workspaces": rows})

    @authed("read")
    async def list_sessions(request: Request, principal: Principal) -> Response:
        q = request.query_params
        harness = q.get("harness")
        if harness is not None:
            validate_harness(harness)
        on_host = q.get("host")
        if on_host is not None:
            validate_host_name(on_host)
        limit = int_param(request, "limit", 200) or 200
        sessions = await run_in_threadpool(
            lambda: store.list_sessions(
                principal.account,
                principal.host,
                principal.read,  # type: ignore[arg-type]
                harness=harness,
                on_host=on_host,
                workspace=q.get("workspace"),
                since=q.get("since"),
                title=q.get("title"),
                limit=min(limit, 1000),
            )
        )
        return JSONResponse({"sessions": sessions})

    @authed("read")
    async def get_manifest(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        manifest = await run_in_threadpool(store.manifest, key)
        await run_in_threadpool(store.audit_read, key, principal.host, "manifest")
        return JSONResponse(manifest)

    @authed("read")
    async def get_state(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        return JSONResponse(await run_in_threadpool(store.session_state, key))

    @authed("append")
    async def post_meta(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_writable(key, principal)
        body = await read_json(request)
        w = writer(request, principal)
        await run_in_threadpool(store.add_metadata, key, w, body)  # type: ignore[arg-type]
        return JSONResponse({"ok": True})

    @authed("append")
    async def post_surrender(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        body = await read_json(request)
        reserved_for = body.get("reserved_for")
        if reserved_for is not None:
            validate_host_name(str(reserved_for))
        state = await run_in_threadpool(
            store.surrender, key, principal.host, reserved_for
        )
        return JSONResponse(state)

    @authed("read", "append")
    async def post_teleport(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        body = await read_json(request)
        workspace = validate_workspace(str(body.get("workspace", "")))
        w = Writer(principal.account, principal.host, workspace)
        event, state = await run_in_threadpool(
            store.teleport, key, w, bool(body.get("steal", False))
        )
        return JSONResponse({"teleport": event, "session": state})

    @authed("read", "append")
    async def post_fork(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        body = await read_json(request)
        generation = validate_id(str(body.get("generation", "")))
        offset = body.get("offset")
        if not isinstance(offset, int):
            raise ApiError("bad_request", "offset must be an integer")
        event = await run_in_threadpool(
            store.create_fork, key, generation, offset, principal.host
        )
        return JSONResponse(event)

    @authed("grant")
    async def post_grant(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        body = await read_json(request)
        host = validate_host_name(str(body.get("host", "")))
        await run_in_threadpool(store.grant, key, host, principal.host)
        return JSONResponse({"ok": True})

    @authed("append")
    async def post_generation(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_writable(key, principal)
        body = await read_json(request)
        path = validate_file_path(str(body.get("path", "")))
        cause = body.get("cause")
        parent = body.get("parent")
        if parent is not None:
            validate_id(str(parent))
        fork_offset = body.get("fork_offset", 0)
        if not isinstance(fork_offset, int):
            raise ApiError("bad_request", "fork_offset must be an integer")
        w = writer(request, principal)
        info = await run_in_threadpool(
            store.create_generation,
            key,
            path,
            w,
            cause,
            parent,
            fork_offset,  # type: ignore[arg-type]
        )
        return JSONResponse(info)

    @authed("append")
    async def post_append(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_writable(key, principal)
        generation = validate_id(request.path_params["gen"])
        try:
            base_length = int(request.headers[HEADER_BASE_LENGTH])
            activity = int(request.headers.get(HEADER_ACTIVITY, "0"))
        except (KeyError, ValueError):
            raise ApiError(
                "bad_request", f"{HEADER_BASE_LENGTH} must be an integer"
            ) from None
        sha256 = validate_sha256(request.headers.get(HEADER_SHA256, ""))
        w = writer(request, principal)
        data = await read_data(request)
        if not limiter.add_bytes(principal.credential, len(data)):
            raise ApiError("rate_limited", "daily byte quota is used up")
        info, state = await run_in_threadpool(
            store.append, key, generation, w, base_length, data, sha256, activity
        )
        return JSONResponse(
            {"generation": info["id"], "length": info["length"], "session": state}
        )

    @authed("read")
    async def get_generation(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        generation = validate_id(request.path_params["gen"])
        start = int_param(request, "start", 0) or 0
        end = int_param(request, "end", None)
        if end is None or end - start > settings.max_body:
            end = start + settings.max_body
        data = await run_in_threadpool(store.read_log, key, generation, start, end)
        await run_in_threadpool(
            store.audit_read, key, principal.host, f"log:{generation}"
        )
        return Response(data, media_type="application/octet-stream")

    @authed("append")
    async def put_blob(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_writable(key, principal)
        path = validate_file_path(blob_path(request))
        sha256 = validate_sha256(request.headers.get(HEADER_SHA256, ""))
        w = writer(request, principal)
        data = await read_data(request)
        if not limiter.add_bytes(principal.credential, len(data)):
            raise ApiError("rate_limited", "daily byte quota is used up")
        version = await run_in_threadpool(store.put_blob, key, path, w, data, sha256)
        return JSONResponse(version)

    @authed("read")
    async def get_blob(request: Request, principal: Principal) -> Response:
        key = session_key(request, principal)
        await require_readable(key, principal)
        path = validate_file_path(blob_path(request))
        version = int_param(request, "version", None)
        data = await run_in_threadpool(store.read_blob, key, path, version)
        await run_in_threadpool(store.audit_read, key, principal.host, f"blob:{path}")
        return Response(data, media_type="application/octet-stream")

    s = f"{API_PREFIX}/sessions/{{harness}}/{{sid}}"
    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route(f"{API_PREFIX}/whoami", whoami, methods=["GET"]),
        Route(f"{API_PREFIX}/hosts/self", put_host, methods=["PUT"]),
        Route(f"{API_PREFIX}/hosts", get_hosts, methods=["GET"]),
        Route(f"{API_PREFIX}/workspaces", get_workspaces, methods=["GET"]),
        Route(f"{API_PREFIX}/sessions", list_sessions, methods=["GET"]),
        Route(s, get_manifest, methods=["GET"]),
        Route(f"{s}/state", get_state, methods=["GET"]),
        Route(f"{s}/meta", post_meta, methods=["POST"]),
        Route(f"{s}/lease/surrender", post_surrender, methods=["POST"]),
        Route(f"{s}/teleports", post_teleport, methods=["POST"]),
        Route(f"{s}/forks", post_fork, methods=["POST"]),
        Route(f"{s}/grants", post_grant, methods=["POST"]),
        Route(f"{s}/generations", post_generation, methods=["POST"]),
        Route(f"{s}/generations/{{gen}}/append", post_append, methods=["POST"]),
        Route(f"{s}/generations/{{gen}}", get_generation, methods=["GET"]),
        Route(f"{s}/blobs/{{path:path}}", put_blob, methods=["PUT"]),
        Route(f"{s}/blobs/{{path:path}}", get_blob, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    if accounts.error:
        log.error("accounts file is invalid; no credentials work: %s", accounts.error)
    return app
