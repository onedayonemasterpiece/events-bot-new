from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from typing import Any

from aiohttp import web

from .chatgpt_refresh_policy import install_chatgpt_refresh_policy
from .config import PrivateEventsMCPConfig
from .media_contract import AssetIngestor
from .event_create import EventCreateRequest, EventCreateRuntime
from .event_create_adapter import MainEventCreateExecutor
from .queue_read import attach_owner_queue_observability
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
            config.social_approval_token,
        )
        if item
    )
    access_logger = logging.getLogger("aiohttp.access")
    for existing in access_logger.filters:
        if isinstance(existing, _PrivateEventsMCPAccessLogFilter):
            existing.extend(secrets)
            return
    access_logger.addFilter(_PrivateEventsMCPAccessLogFilter(secrets))



def event_create_actor_is_current(config: PrivateEventsMCPConfig, request: EventCreateRequest) -> bool:
    """R1 owner boundary, including durable jobs; never infer partner grants.

    Owner identity is the current configured resource/client/subject. Partner
    mutations require their own live grant check and are deliberately not accepted
    by this owner-only stage. Removing an owner client or disabling the capability
    on restart prevents its queued work from crossing the executor boundary.
    """
    from .oauth import SUBJECT
    return bool(config.enabled and config.event_create_enabled
                and request.actor_subject == SUBJECT
                and request.actor_audience == config.resource
                and request.actor_client_id in {config.oauth_client_id, config.opencode_oauth_client_id}
                and bool(request.actor_client_id))


async def recover_private_event_creates(app: web.Application) -> int:
    """Existing scheduler/startup hook; call only after canonical DB init."""
    server = app.get(SERVER_APP_KEY)
    if server is None or server.event_create_runtime is None:
        return 0
    runtime = server.event_create_runtime
    if not server.config.enabled or not server.config.event_create_enabled:
        return 0
    return await runtime.recover_queued(authorize=runtime.authorize, limit=25)


def attach_private_events_mcp(
    app: web.Application,
    config: PrivateEventsMCPConfig | None = None,
    *,
    social_adapters: Mapping[str, SocialAdapter] | None = None,
    social_workspace_adapters: Mapping[str, SocialWorkspaceAdapter] | None = None,
    asset_ingestor: AssetIngestor | None = None,
    event_database: Any | None = None,
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
    event_create_runtime = None
    if resolved.event_create_enabled:
        if event_database is None:
            raise ValueError(
                "event create requires the canonical EventsBot Database instance"
            )
        async def _authorize_event_create(request: EventCreateRequest) -> bool:
            return event_create_actor_is_current(resolved, request)

        event_create_runtime = EventCreateRuntime(
            config=resolved,
            database=event_database,
            executor=MainEventCreateExecutor(event_database),
            authorize=_authorize_event_create,
        )
    server = PrivateEventsMCPServer(
        resolved,
        social_adapters=social_adapters,
        social_workspace_adapters=social_workspace_adapters,
        asset_ingestor=asset_ingestor,
        event_create_runtime=event_create_runtime,
    )
    # R0 deliberately extends only the owner ChatGPT/OpenCode descriptor and
    # handler for the existing operations_snapshot tool. The Codex protocol,
    # scopes, database schema, workers, and provider adapters remain unchanged.
    attach_owner_queue_observability(server)
    install_chatgpt_refresh_policy(server.oauth)
    # Audio transcription is an independent, default-off capability. It extends
    # only the ChatGPT/OpenCode protocol and leaves the exact-seven Codex surface
    # unchanged. Avoid importing the Kaggle/Telethon orchestration package unless
    # the operator explicitly requests this capability. Invalid truthy values are
    # still rejected by AudioTranscriptionConfig.from_env().
    raw_audio_enabled = (
        os.getenv("PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED") or ""
    ).strip().casefold()
    audio_requested = raw_audio_enabled not in {"", "0", "false", "no", "off"}
    audio_service = None
    if audio_requested:
        from audio_transcription.mcp import attach_audio_transcription_mcp

        audio_service = attach_audio_transcription_mcp(
            app,
            server,
            mcp_enabled=resolved.enabled,
            signing_key=resolved.signing_key,
        )
        if audio_service is not None and server.social_workspace is not None:
            server.social_workspace.enable_audio_transcription(audio_service)
    server.register(app)
    if event_create_runtime is not None:
        async def _shutdown_event_create(_app: web.Application) -> None:
            await event_create_runtime.shutdown()

        app.on_cleanup.append(_shutdown_event_create)
    logger.info(
        "private_events_mcp attached endpoint_fingerprint=%s mode=%s",
        app.get(ENDPOINT_FINGERPRINT_APP_KEY, "unknown"),
        "read_plus_audio_transcription" if audio_service is not None else "read_only",
    )
    return server
