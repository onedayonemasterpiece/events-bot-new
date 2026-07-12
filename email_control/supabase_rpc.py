from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit


RpcTransport = Callable[[str, bytes, Mapping[str, str], float], tuple[int, bytes]]


class EmailControlRpcError(RuntimeError):
    pass


class EmailControlRpcTemporaryError(EmailControlRpcError):
    pass


def _transport(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return int(response.status), response.read(262_144)
    except urllib.error.HTTPError as exc:
        exc.read(262_144)
        return int(exc.code), b""
    except (TimeoutError, socket.timeout, OSError, urllib.error.URLError) as exc:
        raise EmailControlRpcTemporaryError("supabase_transport_failed") from exc


class EmailControlRpcClient:
    def __init__(
        self,
        base_url: str,
        secret_key: str,
        *,
        transport: RpcTransport = _transport,
        timeout_seconds: float = 10.0,
    ) -> None:
        base = str(base_url or "").strip().rstrip("/")
        parsed = urlsplit(base)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment:
            raise EmailControlRpcError("supabase_url_invalid")
        key = str(secret_key or "").strip()
        if not key:
            raise EmailControlRpcError("supabase_secret_missing")
        self.base_url = base
        self.secret_key = key
        self.transport = transport
        self.timeout_seconds = min(max(float(timeout_seconds), 1.0), 30.0)

    def call(self, name: str, payload: Mapping[str, Any] | None = None) -> Any:
        rpc_name = str(name or "").strip()
        if not rpc_name or not rpc_name.replace("_", "").isalnum():
            raise EmailControlRpcError("rpc_name_invalid")
        try:
            body = json.dumps(dict(payload or {}), separators=(",", ":")).encode()
        except (TypeError, ValueError) as exc:
            raise EmailControlRpcError("rpc_payload_invalid") from exc
        status, response = self.transport(
            f"{self.base_url}/rest/v1/rpc/{rpc_name}",
            body,
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "apikey": self.secret_key,
                "User-Agent": "KenigEventsEmailWorker/1.0",
            },
            self.timeout_seconds,
        )
        if status in {408, 425, 429} or status >= 500:
            raise EmailControlRpcTemporaryError("supabase_retryable")
        if status < 200 or status >= 300:
            raise EmailControlRpcError("supabase_rejected")
        if not response:
            return None
        try:
            return json.loads(response)
        except json.JSONDecodeError as exc:
            raise EmailControlRpcError("supabase_response_invalid") from exc
