from __future__ import annotations

import asyncio
import hmac
import json
import re
import time
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .config import OpsMCPConfig

_SUPPORTED_PROTOCOLS = frozenset({"2025-11-25", "2025-06-18"})
_SENSITIVE_KEY = re.compile(
    r"(?:token|secret|password|authorization|cookie|session|access[_-]?key|private[_-]?key)",
    re.IGNORECASE,
)
_SENSITIVE_QUERY_KEY = re.compile(
    r"(?:token|secret|password|key|signature|sig|auth|code)", re.IGNORECASE
)
_BEARER_VALUE = re.compile(r"(?i)\bBearer\s+[^\s\"']+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(token|secret|password|authorization|cookie|session|access[_-]?key|private[_-]?key)"
    r"(\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^,;\s}&]+)"
)
_SECRET_PREVIEW_PATH = re.compile(
    r"(?P<prefix>(?:^|/)_review/)[A-Za-z0-9_-]{20,}(?P<suffix>/|$)"
)


class AuthError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class AdmissionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class AuthContext:
    mode: str
    allowed_tools: frozenset[str] | None

    def permits(self, tool_name: str) -> bool:
        return self.allowed_tools is None or tool_name in self.allowed_tools


PATH_ONLY_TOOLS = frozenset(
    {"prod_health_snapshot", "events_find", "social_capabilities"}
)


def authenticate(headers: Mapping[str, str], config: OpsMCPConfig) -> AuthContext:
    origin = headers.get("Origin") or headers.get("origin")
    if origin and origin not in config.allowed_origins:
        raise AuthError(403, "origin_rejected", "request Origin is not allowlisted")

    protocol = headers.get("MCP-Protocol-Version") or headers.get(
        "mcp-protocol-version"
    )
    if protocol and protocol not in _SUPPORTED_PROTOCOLS:
        raise AuthError(400, "unsupported_protocol", "unsupported MCP protocol version")

    authorization = headers.get("Authorization") or headers.get("authorization")
    if authorization:
        scheme, _, presented = authorization.partition(" ")
        if scheme.lower() != "bearer" or not presented:
            raise AuthError(401, "invalid_authorization", "Bearer authorization is required")
        if config.bearer_token is None or not hmac.compare_digest(
            presented, config.bearer_token
        ):
            raise AuthError(401, "invalid_token", "invalid bearer token")
        return AuthContext(mode="bearer", allowed_tools=None)

    if config.allow_path_only_auth:
        return AuthContext(mode="path_only", allowed_tools=PATH_ONLY_TOOLS)
    raise AuthError(401, "missing_token", "Bearer authorization is required")


class TokenBucket:
    def __init__(self, rate_per_minute: int, capacity: int) -> None:
        self._rate_per_second = rate_per_minute / 60.0
        self._capacity = float(max(1, capacity))
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self, amount: float = 1.0) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self._updated)
            self._tokens = min(
                self._capacity, self._tokens + elapsed * self._rate_per_second
            )
            self._updated = now
            if self._tokens < amount:
                return False
            self._tokens -= amount
            return True


class RollingByteBudget:
    def __init__(self, limit_bytes: int, window_seconds: float = 3600.0) -> None:
        self._limit = max(1, int(limit_bytes))
        self._window = float(window_seconds)
        self._events: deque[tuple[float, int]] = deque()
        self._total = 0
        self._lock = asyncio.Lock()

    async def reserve(self, amount: int) -> bool:
        amount = max(0, int(amount))
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window
            while self._events and self._events[0][0] < cutoff:
                _, expired = self._events.popleft()
                self._total -= expired
            if amount > self._limit or self._total + amount > self._limit:
                return False
            self._events.append((now, amount))
            self._total += amount
            return True

    async def snapshot(self) -> dict[str, int]:
        async with self._lock:
            return {"used_bytes": self._total, "limit_bytes": self._limit}


class AdmissionController:
    def __init__(self, config: OpsMCPConfig) -> None:
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._ingress = TokenBucket(
            config.ingress_requests_per_minute, config.ingress_burst
        )
        self._rate = {
            "bearer": TokenBucket(config.requests_per_minute, config.burst),
            "path_only": TokenBucket(
                config.path_only_requests_per_minute,
                min(2, config.path_only_requests_per_minute),
            ),
        }
        self._egress = {
            "bearer": RollingByteBudget(config.egress_bytes_per_hour),
            "path_only": RollingByteBudget(config.path_only_egress_bytes_per_hour),
        }

    async def admit_ingress(self) -> None:
        if not await self._ingress.consume():
            raise AdmissionError("ingress_rate_limited", "global ingress rate limit exceeded")

    @asynccontextmanager
    async def request_slot(self, auth_mode: str):
        limiter = self._rate[auth_mode]
        if not await limiter.consume():
            raise AdmissionError("rate_limited", "request rate limit exceeded")
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.05)
        except asyncio.TimeoutError as exc:
            raise AdmissionError("busy", "gateway concurrency limit reached") from exc
        try:
            yield
        finally:
            self._semaphore.release()

    async def reserve_egress(self, auth_mode: str, size: int) -> bool:
        return await self._egress[auth_mode].reserve(size)

    async def egress_snapshot(self, auth_mode: str) -> dict[str, int]:
        return await self._egress[auth_mode].snapshot()


class TTLResultCache:
    def __init__(self, ttl_seconds: int, max_entries: int = 128) -> None:
        self._ttl = max(0, ttl_seconds)
        self._max_entries = max(1, max_entries)
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        if self._ttl <= 0:
            return None
        async with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            expires_at, value = item
            if expires_at <= time.monotonic():
                self._items.pop(key, None)
                return None
            self._items.move_to_end(key)
            return value

    async def put(self, key: str, value: Any) -> None:
        if self._ttl <= 0:
            return
        async with self._lock:
            self._items[key] = (time.monotonic() + self._ttl, value)
            self._items.move_to_end(key)
            while len(self._items) > self._max_entries:
                self._items.popitem(last=False)


def cache_key(tool_name: str, arguments: Mapping[str, Any]) -> str:
    normalized = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{tool_name}:{normalized}"


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return value
    query = []
    for key, item in parse_qsl(parsed.query, keep_blank_values=True):
        query.append((key, "<redacted>" if _SENSITIVE_QUERY_KEY.search(key) else item))
    path = _SECRET_PREVIEW_PATH.sub(
        lambda match: f"{match.group('prefix')}<redacted>{match.group('suffix')}",
        parsed.path,
    )
    return urlunsplit((parsed.scheme, parsed.netloc, path, urlencode(query), ""))


def _redact_string(value: str, *, max_string: int) -> Any:
    stripped = value.lstrip()
    if len(value) <= 64 * 1024 and stripped[:1] in {"{", "["}:
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, (Mapping, list)):
            return redact(parsed, max_string=max_string)
    text = _BEARER_VALUE.sub("Bearer <redacted>", value)
    text = _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}<redacted>", text
    )
    text = _redact_url(text)
    if len(text) > max_string:
        return text[:max_string] + "…<truncated>"
    return text


def redact(value: Any, *, key: str = "", max_string: int = 2000) -> Any:
    if _SENSITIVE_KEY.search(key):
        return "<redacted>"
    if isinstance(value, Mapping):
        return {
            str(item_key): redact(item_value, key=str(item_key), max_string=max_string)
            for item_key, item_value in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item, max_string=max_string) for item in value]
    if isinstance(value, str):
        return _redact_string(value, max_string=max_string)
    return value
