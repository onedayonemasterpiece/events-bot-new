from __future__ import annotations

import logging

from aiohttp import web

from .config import PrivateEventsMCPConfig
from .server import (
    ENDPOINT_FINGERPRINT_APP_KEY,
    SERVER_APP_KEY,
    PrivateEventsMCPServer,
)


logger = logging.getLogger(__name__)


def attach_private_events_mcp(
    app: web.Application,
    config: PrivateEventsMCPConfig | None = None,
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
    server = PrivateEventsMCPServer(resolved)
    server.register(app)
    logger.info(
        "private_events_mcp attached endpoint_fingerprint=%s mode=read_only",
        app.get(ENDPOINT_FINGERPRINT_APP_KEY, "unknown"),
    )
    return server
