"""Narrow Telegram and VK transports for the private Events MCP.

The adapters deliberately expose only generic plain-text read and publish
operations.  They do not share the bot token or any event-publication flow.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from private_events_mcp.social import (
    ResolvedTarget,
    SocialAdapterError,
    SocialPost,
    SocialPublishReceipt,
    SocialReadResult,
)


_TELEGRAM_BUNDLE_ENV = "TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP"
_TELEGRAM_TARGET_RE = re.compile(r"^-100[0-9]{5,20}$")
_VK_TARGET_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_DEVICE_FIELDS = (
    "device_model",
    "system_version",
    "app_version",
    "lang_code",
    "system_lang_code",
)
_MAX_READ_LIMIT = 20
_MAX_PROVIDER_SCAN = 100

TelegramClientFactory = Callable[[str, int, str, Mapping[str, str]], Any]
VKAPICall = Callable[..., Awaitable[Any]]


def _generic_error(platform: str) -> SocialAdapterError:
    return SocialAdapterError(f"{platform.title()} social provider operation failed")


def _utc_iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        try:
            return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
                "+00:00", "Z"
            )
        except (OverflowError, OSError, ValueError):
            return None
    return None


def _bounded_limit(limit: int) -> int:
    if isinstance(limit, bool):
        raise ValueError("invalid limit")
    return max(1, min(int(limit), _MAX_READ_LIMIT))


def _provider_scan_limit(result_limit: int) -> int:
    return min(_MAX_PROVIDER_SCAN, result_limit * 5)


def _telegram_client_factory(
    session: str,
    api_id: int,
    api_hash: str,
    device: Mapping[str, str],
) -> Any:
    # Telethon remains optional while the disabled MCP feature is inert.
    from telethon import TelegramClient  # type: ignore
    from telethon.sessions import StringSession  # type: ignore

    return TelegramClient(
        StringSession(session),
        api_id,
        api_hash,
        **dict(device),
    )


def _telegram_credentials(
    environ: Mapping[str, str],
) -> tuple[str, int, str, dict[str, str]]:
    """Load only the role-scoped MCP session, without any session fallback."""

    raw_bundle = str(environ.get(_TELEGRAM_BUNDLE_ENV) or "").strip()
    if not raw_bundle:
        raise _generic_error("telegram")
    try:
        padded = raw_bundle + "=" * (-len(raw_bundle) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        raise _generic_error("telegram") from None
    if not isinstance(payload, dict):
        raise _generic_error("telegram")

    session = str(payload.get("session") or "").strip()
    raw_api_id = str(
        environ.get("TELEGRAM_API_ID") or environ.get("TG_API_ID") or ""
    ).strip()
    api_hash = str(
        environ.get("TELEGRAM_API_HASH") or environ.get("TG_API_HASH") or ""
    ).strip()
    try:
        api_id = int(raw_api_id)
    except (TypeError, ValueError):
        raise _generic_error("telegram") from None
    if not session or api_id <= 0 or not api_hash:
        raise _generic_error("telegram")

    device = {
        key: str(payload[key])
        for key in _DEVICE_FIELDS
        if payload.get(key) not in (None, "")
    }
    return session, api_id, api_hash, device


class TelegramSocialAdapter:
    """Serialized, per-call Telethon transport using the dedicated MCP role."""

    platform = "telegram"

    def __init__(
        self,
        *,
        environ: Mapping[str, str] | None = None,
        client_factory: TelegramClientFactory | None = None,
    ) -> None:
        self._environ = os.environ if environ is None else environ
        self._client_factory = client_factory or _telegram_client_factory
        self._session_lock = asyncio.Lock()

    def __repr__(self) -> str:
        return "<TelegramSocialAdapter platform='telegram'>"

    @staticmethod
    def _provider_target(target: ResolvedTarget) -> int:
        if target.platform != "telegram" or not _TELEGRAM_TARGET_RE.fullmatch(
            target.provider_target
        ):
            raise _generic_error("telegram")
        return int(target.provider_target)

    async def _disconnect(self, client: Any) -> None:
        try:
            result = client.disconnect()
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            # Disconnect has been attempted; provider details must never escape.
            pass

    async def read_text(
        self,
        *,
        target: ResolvedTarget,
        limit: int,
    ) -> SocialReadResult:
        async with self._session_lock:
            client: Any | None = None
            try:
                provider_target = self._provider_target(target)
                bounded_limit = _bounded_limit(limit)
                session, api_id, api_hash, device = _telegram_credentials(self._environ)
                client = self._client_factory(session, api_id, api_hash, device)
                await client.connect()
                if not await client.is_user_authorized():
                    raise _generic_error("telegram")

                posts: list[SocialPost] = []
                async for message in client.iter_messages(
                    provider_target,
                    limit=_provider_scan_limit(bounded_limit),
                ):
                    text = getattr(message, "message", None)
                    message_id = getattr(message, "id", None)
                    if not isinstance(text, str) or not text.strip() or message_id is None:
                        continue
                    posts.append(
                        SocialPost(
                            post_id=str(message_id),
                            text=text,
                            published_at=_utc_iso(getattr(message, "date", None)),
                        )
                    )
                    if len(posts) >= bounded_limit:
                        break
                return SocialReadResult(posts=tuple(posts))
            except asyncio.CancelledError:
                raise
            except Exception:
                raise _generic_error("telegram") from None
            finally:
                if client is not None:
                    await self._disconnect(client)

    async def publish_text(
        self,
        *,
        target: ResolvedTarget,
        text: str,
        idempotency_key: str,
    ) -> SocialPublishReceipt:
        del idempotency_key  # Core owns replay prevention; Telegram has no GUID field.
        async with self._session_lock:
            client: Any | None = None
            try:
                provider_target = self._provider_target(target)
                session, api_id, api_hash, device = _telegram_credentials(self._environ)
                client = self._client_factory(session, api_id, api_hash, device)
                await client.connect()
                if not await client.is_user_authorized():
                    raise _generic_error("telegram")
                message = await client.send_message(
                    provider_target,
                    text,
                    parse_mode=None,
                    link_preview=False,
                )
                message_id = getattr(message, "id", None)
                if message_id is None:
                    raise _generic_error("telegram")
                return SocialPublishReceipt(reference=f"telegram-message:{message_id}")
            except asyncio.CancelledError:
                raise
            except Exception:
                raise _generic_error("telegram") from None
            finally:
                if client is not None:
                    await self._disconnect(client)


class VKSocialAdapter:
    """Fixed-method wrapper around the bot runtime's existing ``main.vk_api``."""

    platform = "vk"

    def __init__(self, vk_api_call: VKAPICall) -> None:
        if not callable(vk_api_call):
            raise TypeError("vk_api_call must be callable")
        self._vk_api_call = vk_api_call

    def __repr__(self) -> str:
        return "<VKSocialAdapter platform='vk'>"

    @staticmethod
    def _owner_id(target: ResolvedTarget) -> int:
        if target.platform != "vk" or not _VK_TARGET_RE.fullmatch(
            target.provider_target
        ):
            raise _generic_error("vk")
        return -int(target.provider_target)

    async def read_text(
        self,
        *,
        target: ResolvedTarget,
        limit: int,
    ) -> SocialReadResult:
        try:
            owner_id = self._owner_id(target)
            bounded_limit = _bounded_limit(limit)
            response = await self._vk_api_call(
                "wall.get",
                owner_id=owner_id,
                count=_provider_scan_limit(bounded_limit),
                filter="owner",
                _private_events_mcp_log_boundary=True,
            )
            if not isinstance(response, Mapping) or not isinstance(
                response.get("items"), list
            ):
                raise _generic_error("vk")
            posts: list[SocialPost] = []
            for item in response["items"][: _provider_scan_limit(bounded_limit)]:
                if not isinstance(item, Mapping):
                    raise _generic_error("vk")
                post_id = item.get("id")
                text = item.get("text")
                if not isinstance(text, str) or not text.strip():
                    continue
                if isinstance(post_id, bool) or not isinstance(post_id, (int, str)):
                    raise _generic_error("vk")
                stable_id = str(post_id)
                if not stable_id.isdigit():
                    raise _generic_error("vk")
                posts.append(
                    SocialPost(
                        post_id=stable_id,
                        text=text,
                        published_at=_utc_iso(item.get("date")),
                    )
                )
                if len(posts) >= bounded_limit:
                    break
            return SocialReadResult(posts=tuple(posts))
        except asyncio.CancelledError:
            raise
        except Exception:
            raise _generic_error("vk") from None

    async def publish_text(
        self,
        *,
        target: ResolvedTarget,
        text: str,
        idempotency_key: str,
    ) -> SocialPublishReceipt:
        try:
            owner_id = self._owner_id(target)
            guid = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
            response = await self._vk_api_call(
                "wall.post",
                owner_id=owner_id,
                from_group=1,
                signed=0,
                message=text,
                guid=guid,
                _private_events_mcp_log_boundary=True,
            )
            if not isinstance(response, Mapping):
                raise _generic_error("vk")
            post_id = response.get("post_id")
            if isinstance(post_id, bool) or not isinstance(post_id, (int, str)):
                raise _generic_error("vk")
            stable_id = str(post_id)
            if not stable_id.isdigit():
                raise _generic_error("vk")
            return SocialPublishReceipt(reference=f"vk-post:{stable_id}")
        except asyncio.CancelledError:
            raise
        except Exception:
            raise _generic_error("vk") from None


def build_private_events_mcp_social_adapters(
    vk_api_call: VKAPICall,
) -> dict[str, TelegramSocialAdapter | VKSocialAdapter]:
    """Build lazy transports without reading credentials or calling providers."""

    return {
        "telegram": TelegramSocialAdapter(),
        "vk": VKSocialAdapter(vk_api_call),
    }
