from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


_TRUE = {"1", "true", "yes", "on"}
_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{24,160}$")
_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9._~-]{8,160}$")


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE


def _int(name: str, default: int, *, low: int, high: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(low, min(high, value))


def _normalise_base_url(value: str) -> str:
    value = value.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"https", "http"} or not parsed.netloc:
        raise ValueError("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL must be an absolute URL")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL must contain only scheme and host")
    if parsed.scheme != "https" and parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL must use HTTPS")
    return value


@dataclass(frozen=True, slots=True)
class PrivateEventsMCPConfig:
    """Runtime configuration for the private, read-only ChatGPT MCP surface.

    No secret has a default.  The feature is inert unless ``enabled`` is true,
    so importing this package cannot accidentally expose a route.
    """

    enabled: bool
    public_base_url: str
    path_secret: str
    database_path: str
    auth_database_path: str
    oauth_client_id: str
    oauth_client_secret: str
    codex_oauth_client_id: str
    operator_token: str
    signing_key: str
    repository_root: str
    repository_slug: str
    repository_sha_file: str
    access_ttl_seconds: int = 900
    refresh_ttl_seconds: int = 30 * 24 * 3600
    authorization_code_ttl_seconds: int = 300
    max_request_bytes: int = 64 * 1024
    max_response_bytes: int = 128 * 1024
    max_document_chars: int = 60_000
    max_rows: int = 25
    query_timeout_ms: int = 350
    max_concurrency: int = 2
    anonymous_requests_per_minute: int = 30
    authenticated_requests_per_minute: int = 60
    oauth_failures_per_10_minutes: int = 8
    egress_bytes_per_hour: int = 8 * 1024 * 1024
    cache_ttl_seconds: int = 20
    incident_index_ttl_seconds: int = 60
    incident_scan_bytes: int = 3 * 1024 * 1024

    @classmethod
    def from_env(cls) -> "PrivateEventsMCPConfig":
        enabled = _bool("PRIVATE_EVENTS_MCP_ENABLED", False)
        base = (os.getenv("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL") or "").strip()
        config = cls(
            enabled=enabled,
            # Disabled means inert even when stale deployment variables remain.
            # Parse and validate the public origin only when routes will be
            # attached; otherwise an unrelated malformed value must not break
            # the existing webhook/health application during startup.
            public_base_url=_normalise_base_url(base) if enabled and base else base.rstrip("/"),
            path_secret=(os.getenv("PRIVATE_EVENTS_MCP_PATH_SECRET") or "").strip(),
            database_path=(os.getenv("DB_PATH") or "/data/db.sqlite").strip(),
            auth_database_path=(
                os.getenv("PRIVATE_EVENTS_MCP_AUTH_DB_PATH")
                or "/data/private-events-mcp-auth.sqlite"
            ).strip(),
            oauth_client_id=(os.getenv("PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID") or "").strip(),
            oauth_client_secret=(
                os.getenv("PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET") or ""
            ).strip(),
            codex_oauth_client_id=(
                os.getenv("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID") or ""
            ).strip(),
            operator_token=(os.getenv("PRIVATE_EVENTS_MCP_OPERATOR_TOKEN") or "").strip(),
            signing_key=(os.getenv("PRIVATE_EVENTS_MCP_SIGNING_KEY") or "").strip(),
            repository_root=(os.getenv("PRIVATE_EVENTS_MCP_REPOSITORY_ROOT") or "/app").strip(),
            repository_slug=(
                os.getenv("PRIVATE_EVENTS_MCP_REPOSITORY_SLUG")
                or "onedayonemasterpiece/events-bot-new"
            ).strip(),
            repository_sha_file=(
                os.getenv("PRIVATE_EVENTS_MCP_REPOSITORY_SHA_FILE")
                or "/app/.static-site-repo-sha"
            ).strip(),
            access_ttl_seconds=_int(
                "PRIVATE_EVENTS_MCP_ACCESS_TTL_SECONDS", 900, low=300, high=3600
            ),
            refresh_ttl_seconds=_int(
                "PRIVATE_EVENTS_MCP_REFRESH_TTL_SECONDS",
                30 * 24 * 3600,
                low=3600,
                high=90 * 24 * 3600,
            ),
            authorization_code_ttl_seconds=_int(
                "PRIVATE_EVENTS_MCP_CODE_TTL_SECONDS", 300, low=60, high=600
            ),
            max_request_bytes=_int(
                "PRIVATE_EVENTS_MCP_MAX_REQUEST_BYTES",
                64 * 1024,
                low=4 * 1024,
                high=512 * 1024,
            ),
            max_response_bytes=_int(
                "PRIVATE_EVENTS_MCP_MAX_RESPONSE_BYTES",
                128 * 1024,
                low=16 * 1024,
                high=512 * 1024,
            ),
            max_document_chars=_int(
                "PRIVATE_EVENTS_MCP_MAX_DOCUMENT_CHARS",
                60_000,
                low=4_000,
                high=120_000,
            ),
            max_rows=_int("PRIVATE_EVENTS_MCP_MAX_ROWS", 25, low=1, high=50),
            query_timeout_ms=_int(
                "PRIVATE_EVENTS_MCP_QUERY_TIMEOUT_MS", 350, low=100, high=1500
            ),
            max_concurrency=_int(
                "PRIVATE_EVENTS_MCP_MAX_CONCURRENCY", 2, low=1, high=4
            ),
            anonymous_requests_per_minute=_int(
                "PRIVATE_EVENTS_MCP_ANON_RPM", 30, low=5, high=120
            ),
            authenticated_requests_per_minute=_int(
                "PRIVATE_EVENTS_MCP_AUTH_RPM", 60, low=10, high=240
            ),
            oauth_failures_per_10_minutes=_int(
                "PRIVATE_EVENTS_MCP_OAUTH_FAILURES_10M", 8, low=3, high=30
            ),
            egress_bytes_per_hour=_int(
                "PRIVATE_EVENTS_MCP_EGRESS_BYTES_PER_HOUR",
                8 * 1024 * 1024,
                low=512 * 1024,
                high=64 * 1024 * 1024,
            ),
            cache_ttl_seconds=_int(
                "PRIVATE_EVENTS_MCP_CACHE_TTL_SECONDS", 20, low=0, high=300
            ),
            incident_index_ttl_seconds=_int(
                "PRIVATE_EVENTS_MCP_INCIDENT_INDEX_TTL_SECONDS", 60, low=10, high=600
            ),
            incident_scan_bytes=_int(
                "PRIVATE_EVENTS_MCP_INCIDENT_SCAN_BYTES",
                3 * 1024 * 1024,
                low=256 * 1024,
                high=16 * 1024 * 1024,
            ),
        )
        if config.enabled:
            config.validate()
        return config

    def validate(self) -> None:
        if not self.public_base_url:
            raise ValueError("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL is required")
        _normalise_base_url(self.public_base_url)
        if not _SECRET_RE.fullmatch(self.path_secret):
            raise ValueError("PRIVATE_EVENTS_MCP_PATH_SECRET must be 24+ URL-safe characters")
        if not _CLIENT_ID_RE.fullmatch(self.oauth_client_id):
            raise ValueError("PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID is invalid")
        if not _CLIENT_ID_RE.fullmatch(self.codex_oauth_client_id):
            raise ValueError("PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID is invalid")
        if self.codex_oauth_client_id == self.oauth_client_id:
            raise ValueError("ChatGPT and Codex OAuth client IDs must be distinct")
        for name, value, minimum in (
            ("PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET", self.oauth_client_secret, 32),
            ("PRIVATE_EVENTS_MCP_OPERATOR_TOKEN", self.operator_token, 32),
            ("PRIVATE_EVENTS_MCP_SIGNING_KEY", self.signing_key, 43),
        ):
            if len(value) < minimum:
                raise ValueError(f"{name} must contain at least {minimum} characters")
        if not self.database_path:
            raise ValueError("DB_PATH is required")
        if not self.auth_database_path or self.auth_database_path == self.database_path:
            raise ValueError("OAuth state must use a separate SQLite file")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repository_slug):
            raise ValueError("PRIVATE_EVENTS_MCP_REPOSITORY_SLUG must be owner/repository")

    @property
    def private_prefix(self) -> str:
        return f"/_private/{self.path_secret}"

    @property
    def mcp_path(self) -> str:
        return f"{self.private_prefix}/mcp"

    @property
    def oauth_authorize_path(self) -> str:
        return f"{self.private_prefix}/oauth/authorize"

    @property
    def oauth_token_path(self) -> str:
        return f"{self.private_prefix}/oauth/token"

    @property
    def about_path(self) -> str:
        return f"{self.private_prefix}/about"

    @property
    def protected_resource_metadata_path(self) -> str:
        # RFC 9728 path-specific form.  A root alias is registered as well.
        return f"/.well-known/oauth-protected-resource{self.mcp_path}"

    @property
    def authorization_server_metadata_path(self) -> str:
        # RFC 8414 path-scoped metadata for an issuer that includes private_prefix.
        return f"/.well-known/oauth-authorization-server{self.private_prefix}"

    @property
    def resource(self) -> str:
        return f"{self.public_base_url}{self.mcp_path}"

    @property
    def issuer(self) -> str:
        # Path-scoped issuer avoids publishing the private endpoint through a root
        # authorization-server metadata document. OAuth remains the actual control.
        return f"{self.public_base_url}{self.private_prefix}"

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth_authorize_path}"

    @property
    def token_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth_token_path}"

    @property
    def resource_metadata_url(self) -> str:
        return f"{self.public_base_url}{self.protected_resource_metadata_path}"

    @property
    def oauth_client_ids(self) -> frozenset[str]:
        """Static bearer-client registry; dynamic registration is unsupported."""

        return frozenset((self.oauth_client_id, self.codex_oauth_client_id))

    @property
    def documentation_url(self) -> str:
        return f"{self.public_base_url}{self.about_path}"

    def ensure_auth_directory(self) -> None:
        path = Path(self.auth_database_path)
        if path.parent and str(path.parent) not in {"", "."}:
            path.parent.mkdir(parents=True, exist_ok=True)
