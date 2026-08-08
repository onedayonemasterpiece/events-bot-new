from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

from aiohttp import web

from .config import PrivateEventsMCPConfig
from .server import (
    ENDPOINT_FINGERPRINT_APP_KEY,
    SERVER_APP_KEY,
    PrivateEventsMCPServer,
)
from .social import SocialAdapter
from .social_workspace_runtime import SocialWorkspaceAdapter

logger = logging.getLogger(__name__)
_AUTHORIZATION_VALUE_RE = re.compile(
    r"(?i)\b(Basic|Bearer)\s+[A-Za-z0-9._~+/=-]+"
)


def _redact_log_value(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, "<redacted>")
        return _AUTHORIZATION_VALUE_RE.sub(
            lambda match: f"{match.group(1)} <redacted>",
            value,
        )
    if isinstance(value, tuple):
        return tuple(_redact_log_value(item, secrets) for item in value)
    if isinstance(value, list):
        return [_redact_log_value(item, secrets) for item in value]
    if isinstance(value, Mapping):
        return {
            key: _redact_log_value(item, secrets)
            for key, item in value.items()
        }
    return value


class _PrivateEventsMCPAccessLogFilter(logging.Filter):
    """Remove private MCP credentials before aiohttp handlers emit a record."""

    def __init__(self, secrets: tuple[str, ...]) -> None:
        super().__init__()
        self._secrets = secrets

    def extend(self, secrets: tuple[str, ...]) -> None:
        self._secrets = tuple(dict.fromkeys((*self._secrets, *secrets)))

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact_log_value(record.msg, self._secrets)
        record.args = _redact_log_value(record.args, self._secrets)
        return True


def _install_access_log_redaction(config: PrivateEventsMCPConfig) -> None:
    secrets = tuple(
        item
        for item in (
            config.path_secret,
            config.oauth_client_secret,
            config.operator_token,
            config.signing_key,
        )
        if item
    )
    access_logger = logging.getLogger("aiohttp.access")
    for existing in access_logger.filters:
        if isinstance(existing, _PrivateEventsMCPAccessLogFilter):
            existing.extend(secrets)
            return
    access_logger.addFilter(_PrivateEventsMCPAccessLogFilter(secrets))


def attach_private_events_mcp(
    app: web.Application,
    config: PrivateEventsMCPConfig | None = None,
    *,
    social_adapters: Mapping[str, SocialAdapter] | None = None,
    social_workspace_adapters: Mapping[str, SocialWorkspaceAdapter] | None = None,
) -> PrivateEventsMCPServer | None:
    """Attach the private MCP routes to the existing aiohttp app.

    The function is a strict no-op unless PRIVATE_EVENTS_MCP_ENABLED=1.  This
    keeps the current production process and route set unchanged before an
    explicit canary activation.
    """

    resolved = config or PrivateEventsMCPConfig.from_env()
    if not resolved.enabled:
        logger.info("private_events_mcp disabled")
        return None
    if SERVER_APP_KEY in app:
        return app[SERVER_APP_KEY]
    _install_access_log_redaction(resolved)
    server = PrivateEventsMCPServer(
        resolved,
        social_adapters=social_adapters,
        social_workspace_adapters=social_workspace_adapters,
    )
    server.register(app)
    logger.info(
        "private_events_mcp attached endpoint_fingerprint=%s mode=read_only",
        app.get(ENDPOINT_FINGERPRINT_APP_KEY, "unknown"),
    )
    return server
