"""A small client for the Hyperspace HTTP API."""

from __future__ import annotations

import hashlib
from typing import Any, Self, cast
from urllib.parse import quote

import httpx
import zstandard

from hyperspace_protocol import (
    API_PREFIX,
    CAP_WORKSPACE_UTF8,
    HEADER_ACTIVITY,
    HEADER_BASE_LENGTH,
    HEADER_SHA256,
    HEADER_WORKSPACE,
    HEADER_WORKSPACE_UTF8,
    AppendResponse,
    BlobVersion,
    ForkEvent,
    GenerationCause,
    GenerationInfo,
    HostInfo,
    Manifest,
    SessionMetadata,
    SessionStateBody,
    SessionSummary,
    TeleportResponse,
    encode_workspace,
    validate_workspace,
)


class ApiError(Exception):
    def __init__(self, status: int, code: str, detail: str = "") -> None:
        super().__init__(f"{status} {code}: {detail}" if detail else f"{status} {code}")
        self.status = status
        self.code = code
        self.detail = detail


class NotFound(ApiError):
    pass


class AppendConflict(ApiError):
    """The generation's length is not the base length of the append."""

    def __init__(self, length: int, session: SessionStateBody | None) -> None:
        super().__init__(409, "conflict", f"generation length is {length}")
        self.length = length
        self.session = session


class Api:
    def __init__(
        self,
        url: str,
        token: str,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        """`client`, if given, is an HTTP client with the base URL (including
        the API prefix) and the Authorization header already set. Tests use
        it to talk to an in-process server."""
        self._client = client or httpx.Client(
            base_url=url + API_PREFIX,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
        self._zstd = zstandard.ZstdCompressor(level=3)
        self._capabilities: set[str] | None = None
        # Do not put a credential in the state database. A rotation causes
        # safe cache misses, and cannot reuse another account's cache.
        self.cache_identity = hashlib.sha256((url + "\n" + token).encode()).hexdigest()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _raise(response: httpx.Response) -> None:
        if response.is_success:
            return
        try:
            body: dict[str, Any] = response.json()
        except ValueError:
            body = {}
        code = str(body.get("error", "error"))
        detail = str(body.get("detail", response.text[:200]))
        if response.status_code == 409 and code == "conflict":
            raise AppendConflict(int(body.get("length", 0)), body.get("session"))
        if response.status_code == 404:
            raise NotFound(404, code, detail)
        raise ApiError(response.status_code, code, detail)

    def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._client.request(method, path, **kwargs)
        self._raise(response)
        return response.json()

    @staticmethod
    def _s(harness: str, sid: str) -> str:
        return f"/sessions/{harness}/{sid}"

    # --- Identity and hosts ----------------------------------------------

    def whoami(self) -> dict[str, Any]:
        body = cast(dict[str, Any], self._json("GET", "/whoami"))
        self._capabilities = set(body.get("capabilities", []))
        return body

    def require_capability(self, capability: str) -> None:
        if self._capabilities is None:
            self.whoami()
        if capability not in (self._capabilities or set()):
            raise ApiError(400, "upgrade_required", f"server must support {capability}")

    def _workspace_headers(self, workspace: str) -> dict[str, str]:
        validate_workspace(workspace)
        if self._capabilities is None:
            self.whoami()
        if CAP_WORKSPACE_UTF8 in (self._capabilities or set()):
            return {HEADER_WORKSPACE_UTF8: encode_workspace(workspace)}
        if all(32 <= ord(c) < 127 for c in workspace):
            return {HEADER_WORKSPACE: workspace}
        self.require_capability(CAP_WORKSPACE_UTF8)
        raise AssertionError("unreachable")

    def register_host(self, info: HostInfo) -> dict[str, Any]:
        return cast(dict[str, Any], self._json("PUT", "/hosts/self", json=info))

    def hosts(self) -> list[dict[str, Any]]:
        return cast(list[dict[str, Any]], self._json("GET", "/hosts")["hosts"])

    # --- Sessions ----------------------------------------------------------

    def sessions(self, **filters: str) -> list[SessionSummary]:
        params = {k: v for k, v in filters.items() if v}
        body = self._json("GET", "/sessions", params=params)
        return cast(list[SessionSummary], body["sessions"])

    def manifest(self, harness: str, sid: str) -> Manifest:
        return cast(Manifest, self._json("GET", self._s(harness, sid)))

    def state(self, harness: str, sid: str) -> SessionStateBody:
        return cast(
            SessionStateBody, self._json("GET", self._s(harness, sid) + "/state")
        )

    def add_metadata(
        self, harness: str, sid: str, workspace: str, metadata: SessionMetadata
    ) -> None:
        self._json(
            "POST",
            self._s(harness, sid) + "/meta",
            json=metadata,
            headers=self._workspace_headers(workspace),
        )

    # --- Logs --------------------------------------------------------------

    def create_generation(
        self,
        harness: str,
        sid: str,
        workspace: str,
        path: str,
        cause: GenerationCause,
        parent: str | None = None,
        fork_offset: int = 0,
    ) -> GenerationInfo:
        body: dict[str, Any] = {"path": path, "cause": cause}
        if parent is not None:
            body["parent"] = parent
            body["fork_offset"] = fork_offset
        return cast(
            GenerationInfo,
            self._json(
                "POST",
                self._s(harness, sid) + "/generations",
                json=body,
                headers=self._workspace_headers(workspace),
            ),
        )

    def append(
        self,
        harness: str,
        sid: str,
        workspace: str,
        generation: str,
        base_length: int,
        data: bytes,
        activity: int,
    ) -> AppendResponse:
        headers = {
            **self._workspace_headers(workspace),
            HEADER_BASE_LENGTH: str(base_length),
            HEADER_SHA256: hashlib.sha256(data).hexdigest(),
            HEADER_ACTIVITY: str(activity),
            "Content-Encoding": "zstd",
            "Content-Type": "application/octet-stream",
        }
        return cast(
            AppendResponse,
            self._json(
                "POST",
                self._s(harness, sid) + f"/generations/{generation}/append",
                content=self._zstd.compress(data),
                headers=headers,
            ),
        )

    def read_log(
        self, harness: str, sid: str, generation: str, start: int, end: int
    ) -> bytes:
        response = self._client.get(
            self._s(harness, sid) + f"/generations/{generation}",
            params={"start": start, "end": end},
        )
        self._raise(response)
        return response.content

    # --- Blobs -------------------------------------------------------------

    def put_blob(
        self, harness: str, sid: str, workspace: str, path: str, data: bytes
    ) -> BlobVersion:
        headers = {
            **self._workspace_headers(workspace),
            HEADER_SHA256: hashlib.sha256(data).hexdigest(),
            "Content-Encoding": "zstd",
            "Content-Type": "application/octet-stream",
        }
        return cast(
            BlobVersion,
            self._json(
                "PUT",
                self._s(harness, sid) + f"/blobs/{quote(path, safe='/')}",
                content=self._zstd.compress(data),
                headers=headers,
            ),
        )

    def read_blob(
        self, harness: str, sid: str, path: str, version: int | None = None
    ) -> bytes:
        params = {"version": version} if version is not None else {}
        response = self._client.get(
            self._s(harness, sid) + f"/blobs/{quote(path, safe='/')}", params=params
        )
        self._raise(response)
        return response.content

    # --- Leases, teleports, grants, forks ------------------------------------

    def surrender(
        self, harness: str, sid: str, reserved_for: str | None
    ) -> SessionStateBody:
        body = {"reserved_for": reserved_for} if reserved_for else {}
        return cast(
            SessionStateBody,
            self._json("POST", self._s(harness, sid) + "/lease/surrender", json=body),
        )

    def teleport(
        self, harness: str, sid: str, workspace: str, steal: bool
    ) -> TeleportResponse:
        return cast(
            TeleportResponse,
            self._json(
                "POST",
                self._s(harness, sid) + "/teleports",
                json={"workspace": workspace, "steal": steal},
            ),
        )

    def grant(self, harness: str, sid: str, host: str) -> None:
        self._json("POST", self._s(harness, sid) + "/grants", json={"host": host})

    def fork(self, harness: str, sid: str, generation: str, offset: int) -> ForkEvent:
        return cast(
            ForkEvent,
            self._json(
                "POST",
                self._s(harness, sid) + "/forks",
                json={"generation": generation, "offset": offset},
            ),
        )
