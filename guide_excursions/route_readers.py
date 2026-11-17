"""Bounded, read-only searches of a registered excursion organizer's own wall or channel."""
from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp

from remote_telegram_session import raise_if_remote_telegram_session_busy
from .kaggle_service import _parse_auth_bundle, _require_env_any, remote_telegram_auth_scope

_LIMIT = 6
_MAX_TEXT = 4096
_OWNED_KINDS = {"guide_personal", "guide_project", "organization_with_tours", "excursion_operator"}


class RouteReadLimited(RuntimeError):
    def __init__(self, retry_after: int):
        self.retry_after = max(1, retry_after)
        super().__init__(f"Source search paused for {self.retry_after}s")


def _flags(source: Mapping) -> dict:
    value = source.get("flags_json") or source.get("flags") or {}
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


async def _telegram(username: str, title: str, flags: dict) -> list[dict]:
    from telethon import TelegramClient, errors, functions, types
    from telethon.sessions import StringSession

    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", username):
        raise ValueError("Missing registered Telegram channel username")
    scope = remote_telegram_auth_scope()
    await raise_if_remote_telegram_session_busy(current_job_type="guide_monitoring", current_auth_scope=scope)
    bundle = _parse_auth_bundle(scope) if scope != "TG_SESSION" else {}
    session = bundle.get("session") or _require_env_any("TG_SESSION", "TELEGRAM_SESSION")
    client = TelegramClient(
        StringSession(session), int(_require_env_any("TG_API_ID", "TELEGRAM_API_ID")),
        _require_env_any("TG_API_HASH", "TELEGRAM_API_HASH"), receive_updates=False,
        request_retries=0, connection_retries=0, auto_reconnect=False, flood_sleep_threshold=0,
        raise_last_call_error=True, device_model=bundle.get("device_model") or "Guide monitoring",
        system_version=bundle.get("system_version") or "Linux", app_version=bundle.get("app_version") or "1.0",
        lang_code=bundle.get("lang_code") or "ru", system_lang_code=bundle.get("system_lang_code") or bundle.get("lang_code") or "ru",
    )
    try:
        async with asyncio.timeout(25):
            await client.connect()
            resolution = await client(functions.contacts.ResolveUsernameRequest(username))
            peer_id = getattr(resolution.peer, "channel_id", None)
            channel = next((c for c in resolution.chats if c.id == peer_id), None)
            if not channel or not getattr(channel, "broadcast", False) or not channel.access_hash:
                raise ValueError("Registered Telegram source is not a broadcast channel")
            remembered = (flags.get("avatar") or {}).get("peer_id")
            if remembered:
                from telethon.utils import resolve_id
                if resolve_id(int(remembered))[0] != channel.id:
                    raise ValueError("Registered Telegram channel identity has changed")
            peer = types.InputPeerChannel(channel.id, channel.access_hash)
            result = await client(functions.messages.SearchRequest(
                peer=peer, q=title, filter=types.InputMessagesFilterEmpty(),
                min_date=None, max_date=None, offset_id=0, add_offset=0,
                limit=_LIMIT, max_id=0, min_id=0, hash=0,
            ))
            posts = []
            for msg in result.messages[:_LIMIT]:
                if (not getattr(msg, "post", False) or getattr(msg, "fwd_from", None)
                        or getattr(getattr(msg, "peer_id", None), "channel_id", None) != channel.id
                        or not getattr(msg, "message", None) or not msg.date):
                    continue
                posts.append({"url": f"https://t.me/{username}/{msg.id}",
                              "text": msg.message[:_MAX_TEXT], "post_date": msg.date.isoformat()})
            return posts
    except (errors.FloodWaitError, errors.FloodPremiumWaitError) as exc:
        raise RouteReadLimited(exc.seconds) from exc
    finally:
        await client.disconnect()


def _retry_after(value: str | None) -> int:
    if value and value.strip().isdigit():
        return max(1, int(value))
    if value:
        try:
            return max(1, int((parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()) + 1)
        except (ValueError, TypeError, OverflowError):
            pass
    return 3600


async def _vk_api(session, method: str, token: str, **params) -> dict:
    async with session.post(f"https://api.vk.com/method/{method}", allow_redirects=False,
                            data={**params, "access_token": token, "v": "5.199"}) as response:
        if response.status == 429:
            raise RouteReadLimited(_retry_after(response.headers.get("Retry-After")))
        if response.status != 200:
            raise RuntimeError(f"VK source search HTTP status {response.status}")
        payload = await response.json()
        if "error" in payload:
            code = payload["error"].get("error_code")
            if code in {6, 9, 29}:
                raise RouteReadLimited(_retry_after(response.headers.get("Retry-After")))
            raise RuntimeError(f"VK source search API error {code}")
        return payload["response"]


async def _vk(username: str, title: str, flags: dict) -> list[dict]:
    token_env = os.getenv("GUIDE_MONITORING_VK_TOKEN_ENV") or "VK_SERVICE_TOKEN"
    token = (os.getenv("GUIDE_MONITORING_VK_TOKEN") or os.getenv(token_env) or "").strip()
    if not token:
        raise RuntimeError("Missing guide monitoring VK credential")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
        if flags.get("vk_id"):
            identity = abs(int(flags["vk_id"]))
            owner = identity if flags.get("vk_personal_page") else -identity
        else:
            if not re.fullmatch(r"[A-Za-z0-9_.]{2,64}", username):
                raise ValueError("Missing registered VK screen name")
            match = await _vk_api(session, "utils.resolveScreenName", token, screen_name=username)
            if not isinstance(match, Mapping) or match.get("type") not in {"user", "group", "page", "event"}:
                raise ValueError("Unknown registered VK source")
            owner = int(match["object_id"]) * (1 if match["type"] == "user" else -1)
        result = await _vk_api(session, "wall.search", token, owner_id=owner, query=title,
                               owners_only=1, count=_LIMIT)
    posts = []
    for item in result.get("items", [])[:_LIMIT]:
        if (item.get("owner_id") != owner or item.get("from_id") != owner or item.get("copy_history")
                or item.get("marked_as_ads") or not item.get("date") or not item.get("text")
                or not item.get("id")):
            continue
        posts.append({"url": f"https://vk.com/wall{owner}_{item['id']}",
                      "text": item["text"][:_MAX_TEXT],
                      "post_date": datetime.fromtimestamp(item["date"], timezone.utc).isoformat()})
    return posts


async def search_route_posts(source: Mapping, title: str) -> list[dict]:
    if source.get("source_kind") not in _OWNED_KINDS:
        return []
    title = " ".join(title.split())[:160]
    if len(title) < 6:
        return []
    username = str(source.get("username") or "").strip().lstrip("@")
    flags = _flags(source)
    if source.get("platform") == "telegram":
        return await _telegram(username, title, flags)
    if source.get("platform") == "vk":
        return await _vk(username, title, flags)
    return []
