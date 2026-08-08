from __future__ import annotations

import os
import re
from dataclasses import dataclass

_URLSAFE_SECRET = re.compile(r"^[A-Za-z0-9_-]+$")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _csv(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class OpsMCPConfig:
    enabled: bool
    bind_host: str
    port: int
    database_path: str
    path_secret: str
    bearer_token: str | None
    allow_path_only_auth: bool
    allowed_origins: tuple[str, ...]
    max_request_bytes: int
    max_response_bytes: int
    max_concurrency: int
    ingress_requests_per_minute: int
    ingress_burst: int
    requests_per_minute: int
    burst: int
    egress_bytes_per_hour: int
    path_only_requests_per_minute: int
    path_only_egress_bytes_per_hour: int
    db_timeout_ms: int
    cache_ttl_seconds: int

    @classmethod
    def from_env(cls, *, require_enabled: bool = False) -> "OpsMCPConfig":
        enabled = _env_bool("ENABLE_PROD_OPS_MCP", False)
        if require_enabled and not enabled:
            raise ValueError("ENABLE_PROD_OPS_MCP=1 is required")

        path_secret = os.getenv("PROD_OPS_MCP_PATH_SECRET", "").strip()
        bearer_token = os.getenv("PROD_OPS_MCP_BEARER_TOKEN", "").strip() or None
        allow_path_only = _env_bool("PROD_OPS_MCP_ALLOW_PATH_ONLY_AUTH", False)

        if enabled:
            if len(path_secret) < 32 or not _URLSAFE_SECRET.fullmatch(path_secret):
                raise ValueError(
                    "PROD_OPS_MCP_PATH_SECRET must contain at least 32 URL-safe characters"
                )
            if bearer_token is None and not allow_path_only:
                raise ValueError(
                    "PROD_OPS_MCP_BEARER_TOKEN is required unless path-only auth is explicitly enabled"
                )
            if bearer_token is not None and len(bearer_token) < 32:
                raise ValueError("PROD_OPS_MCP_BEARER_TOKEN must contain at least 32 characters")
            if bearer_token == path_secret:
                raise ValueError("path secret and bearer token must be different")
            if _env_bool("PROD_OPS_MCP_ENABLE_WRITE", False):
                raise ValueError("write tools are not implemented in the read-only MVP")

        requests_per_minute = _env_int(
            "PROD_OPS_MCP_REQUESTS_PER_MINUTE", 12, minimum=1, maximum=120
        )
        burst = _env_int("PROD_OPS_MCP_BURST", 3, minimum=1, maximum=20)
        if burst > requests_per_minute:
            burst = requests_per_minute

        # Secret-link-only mode is deliberately clamped even if broader values
        # are supplied in the environment.
        path_only_rpm = min(
            _env_int(
                "PROD_OPS_MCP_PATH_ONLY_REQUESTS_PER_MINUTE",
                4,
                minimum=1,
                maximum=12,
            ),
            4,
        )
        path_only_egress = min(
            _env_int(
                "PROD_OPS_MCP_PATH_ONLY_EGRESS_BYTES_PER_HOUR",
                256 * 1024,
                minimum=16 * 1024,
                maximum=1024 * 1024,
            ),
            256 * 1024,
        )

        return cls(
            enabled=enabled,
            bind_host=os.getenv("PROD_OPS_MCP_BIND_HOST", "0.0.0.0").strip()
            or "0.0.0.0",
            port=_env_int("PROD_OPS_MCP_PORT", 8091, minimum=1024, maximum=65535),
            database_path=os.getenv("DB_PATH", "/data/db.sqlite").strip()
            or "/data/db.sqlite",
            path_secret=path_secret,
            bearer_token=bearer_token,
            allow_path_only_auth=allow_path_only,
            allowed_origins=_csv("PROD_OPS_MCP_ALLOWED_ORIGINS"),
            max_request_bytes=_env_int(
                "PROD_OPS_MCP_MAX_REQUEST_BYTES",
                32 * 1024,
                minimum=1024,
                maximum=256 * 1024,
            ),
            max_response_bytes=_env_int(
                "PROD_OPS_MCP_MAX_RESPONSE_BYTES",
                192 * 1024,
                minimum=8 * 1024,
                maximum=1024 * 1024,
            ),
            max_concurrency=_env_int(
                "PROD_OPS_MCP_MAX_CONCURRENCY", 1, minimum=1, maximum=4
            ),
            ingress_requests_per_minute=_env_int(
                "PROD_OPS_MCP_INGRESS_REQUESTS_PER_MINUTE",
                30,
                minimum=1,
                maximum=240,
            ),
            ingress_burst=_env_int(
                "PROD_OPS_MCP_INGRESS_BURST", 5, minimum=1, maximum=20
            ),
            requests_per_minute=requests_per_minute,
            burst=burst,
            egress_bytes_per_hour=_env_int(
                "PROD_OPS_MCP_EGRESS_BYTES_PER_HOUR",
                1024 * 1024,
                minimum=64 * 1024,
                maximum=32 * 1024 * 1024,
            ),
            path_only_requests_per_minute=path_only_rpm,
            path_only_egress_bytes_per_hour=path_only_egress,
            db_timeout_ms=_env_int(
                "PROD_OPS_MCP_DB_TIMEOUT_MS", 300, minimum=50, maximum=2000
            ),
            cache_ttl_seconds=_env_int(
                "PROD_OPS_MCP_CACHE_TTL_SECONDS", 10, minimum=0, maximum=120
            ),
        )
