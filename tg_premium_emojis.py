"""Telethon premium-emoji post editor for events-bot Telegram surfaces."""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from telethon import TelegramClient
from telethon.errors import MessageNotModifiedError
from telethon.helpers import add_surrogate, del_surrogate
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityCustomEmoji

logger = logging.getLogger(__name__)

DEFAULT_FREE_EMOJI_FALLBACK = "🆓🆓🆓🆓"
DEFAULT_FREE_EMOJI_DOCUMENT_IDS: tuple[int, ...] = (
    5406749623865857008,
    5407072545276973461,
    5406815783542085177,
    5406927577245833438,
)
DAILY_ADDED_HEADING = "ДОБАВИЛИ В АНОНС"


@dataclass(slots=True, frozen=True)
class PremiumEmojiTelethonConfig:
    api_id: int
    api_hash: str
    session_string: str
    auth_scope: str
    device_model: str | None = None
    system_version: str | None = None
    app_version: str | None = None
    lang_code: str | None = None
    system_lang_code: str | None = None


@dataclass(slots=True, frozen=True)
class PremiumEmojiEditResult:
    chat: str | int
    message_id: int
    edited: bool
    replacements: int
    before_text: str
    after_text: str
    error: str | None = None


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _urlsafe_b64decode_text(value: str) -> str:
    padded = value + ("=" * (-len(value) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def parse_document_ids(raw: str | None = None) -> tuple[int, ...]:
    value = (raw or _env("TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS")).strip()
    if not value:
        return DEFAULT_FREE_EMOJI_DOCUMENT_IDS
    ids: list[int] = []
    for part in value.replace(";", ",").split(","):
        part = part.strip()
        if part:
            ids.append(int(part))
    if not ids:
        raise ValueError("TG_PREMIUM_EMOJI_FREE_DOCUMENT_IDS is empty")
    return tuple(ids)


def _session_bundle_from_env() -> tuple[str, dict[str, Any], str]:
    """Return (session_string, bundle_meta, auth_scope_env_name)."""
    dedicated_bundle = _env("TG_PREMIUM_EMOJI_AUTH_BUNDLE")
    if dedicated_bundle:
        bundle = json.loads(_urlsafe_b64decode_text(dedicated_bundle))
        return str(bundle.get("session") or "").strip(), bundle, "TG_PREMIUM_EMOJI_AUTH_BUNDLE"

    dedicated_session = _env("TG_PREMIUM_EMOJI_SESSION")
    if dedicated_session:
        return dedicated_session, {}, "TG_PREMIUM_EMOJI_SESSION"

    # E2E fallback is intentionally opt-in to preserve role-scoped sessions.
    if _env("TG_PREMIUM_EMOJI_ALLOW_E2E_FALLBACK") in {"1", "true", "yes", "on"}:
        e2e_bundle = _env("TELEGRAM_AUTH_BUNDLE_E2E")
        if e2e_bundle:
            bundle = json.loads(_urlsafe_b64decode_text(e2e_bundle))
            return str(bundle.get("session") or "").strip(), bundle, "TELEGRAM_AUTH_BUNDLE_E2E"
        e2e_session = _env("TELEGRAM_SESSION")
        if e2e_session:
            return e2e_session, {}, "TELEGRAM_SESSION"

    raise RuntimeError(
        "missing TG_PREMIUM_EMOJI_AUTH_BUNDLE/TG_PREMIUM_EMOJI_SESSION; "
        "set TG_PREMIUM_EMOJI_ALLOW_E2E_FALLBACK=1 only for local/manual E2E-session edits"
    )


def load_telethon_config_from_env() -> PremiumEmojiTelethonConfig:
    api_id_raw = _env("TG_PREMIUM_EMOJI_API_ID") or _env("TELEGRAM_API_ID") or _env("TG_API_ID")
    api_hash = _env("TG_PREMIUM_EMOJI_API_HASH") or _env("TELEGRAM_API_HASH") or _env("TG_API_HASH")
    if not api_id_raw or not api_hash:
        raise RuntimeError("missing TELEGRAM_API_ID/TELEGRAM_API_HASH or TG_API_ID/TG_API_HASH")
    session_string, bundle, auth_scope = _session_bundle_from_env()
    if not session_string:
        raise RuntimeError(f"invalid {auth_scope}: missing session")
    return PremiumEmojiTelethonConfig(
        api_id=int(api_id_raw),
        api_hash=api_hash,
        session_string=session_string,
        auth_scope=auth_scope,
        device_model=(bundle.get("device_model") or None),
        system_version=(bundle.get("system_version") or None),
        app_version=(bundle.get("app_version") or None),
        lang_code=(bundle.get("lang_code") or None),
        system_lang_code=(bundle.get("system_lang_code") or None),
    )


def telethon_client_from_config(cfg: PremiumEmojiTelethonConfig) -> TelegramClient:
    kwargs: dict[str, object] = {}
    for key in ("device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
        value = getattr(cfg, key)
        if value:
            kwargs[key] = value
    return TelegramClient(StringSession(cfg.session_string), cfg.api_id, cfg.api_hash, **kwargs)


def _clone_entity_with_offset(entity: Any, offset: int) -> Any:
    cloned = copy.copy(entity)
    cloned.offset = offset
    return cloned


def _apply_substitution_ops(
    text: str,
    entities: Sequence[Any] | None,
    ops: Sequence[tuple[int, int, str]],
    document_ids: Sequence[int],
) -> tuple[str, list[Any], int]:
    if not ops:
        return text, list(entities or []), 0

    sur_text = add_surrogate(text)
    adjusted_entities = list(entities or [])
    premium_entities: list[MessageEntityCustomEmoji] = []
    shift = 0

    for original_start, old_len, replacement in sorted(ops, key=lambda item: item[0]):
        start = original_start + shift
        replacement_sur = add_surrogate(replacement)
        new_len = len(replacement_sur)
        end = start + old_len
        sur_text = sur_text[:start] + replacement_sur + sur_text[end:]
        delta = new_len - old_len

        next_entities: list[Any] = []
        for entity in adjusted_entities:
            ent_start = int(getattr(entity, "offset", 0))
            ent_len = int(getattr(entity, "length", 0))
            ent_end = ent_start + ent_len
            if ent_end <= start:
                next_entities.append(entity)
            elif ent_start >= end:
                next_entities.append(_clone_entity_with_offset(entity, ent_start + delta))
            else:
                logger.warning(
                    "tg_premium_emoji.drop_overlapping_entity type=%s offset=%s length=%s substitution_start=%s substitution_len=%s",
                    type(entity).__name__,
                    ent_start,
                    ent_len,
                    start,
                    old_len,
                )
        adjusted_entities = next_entities

        emoji_offset = start
        for index, document_id in enumerate(document_ids):
            premium_entities.append(
                MessageEntityCustomEmoji(
                    offset=emoji_offset + index * 2,
                    length=2,
                    document_id=int(document_id),
                )
            )
        shift += delta

    merged = sorted([*adjusted_entities, *premium_entities], key=lambda entity: (entity.offset, entity.length))
    return del_surrogate(sur_text), merged, len(ops)


def _find_daily_free_label_ops(text: str) -> list[tuple[int, int, str]]:
    sur_text = add_surrogate(text)
    ops: list[tuple[int, int, str]] = []
    occupied: list[tuple[int, int]] = []

    def add_matches(pattern: str, start_at: int = 0) -> None:
        sur_pattern = add_surrogate(pattern)
        pos = sur_text.find(sur_pattern, start_at)
        while pos >= 0:
            end = pos + len(sur_pattern)
            if not any(not (end <= taken_start or pos >= taken_end) for taken_start, taken_end in occupied):
                ops.append((pos, len(sur_pattern), DEFAULT_FREE_EMOJI_FALLBACK))
                occupied.append((pos, end))
            pos = sur_text.find(sur_pattern, pos + 1)

    heading_pos = sur_text.find(add_surrogate(DAILY_ADDED_HEADING))
    if heading_pos >= 0:
        add_matches("🚩 🟡", heading_pos)

    add_matches("🟡 Бесплатно")
    return sorted(ops, key=lambda item: item[0])


def apply_daily_free_premium_emojis(
    text: str,
    entities: Sequence[Any] | None = None,
    *,
    document_ids: Sequence[int] | None = None,
) -> tuple[str, list[Any], int]:
    """Replace daily free markers with the configured premium custom emoji label."""
    ids = tuple(document_ids or parse_document_ids())
    if len(ids) != len(DEFAULT_FREE_EMOJI_FALLBACK):
        raise ValueError("daily free premium label must contain exactly 4 custom emoji document ids")
    return _apply_substitution_ops(text, entities, _find_daily_free_label_ops(text), ids)


async def raise_if_session_busy(auth_scope: str) -> None:
    from remote_telegram_session import raise_if_remote_telegram_session_busy

    await raise_if_remote_telegram_session_busy(
        current_job_type="tg_premium_emoji_editor",
        current_auth_scope=auth_scope,
    )


async def edit_message_daily_free_labels(
    client: TelegramClient,
    chat: str | int,
    message_id: int,
    *,
    document_ids: Sequence[int] | None = None,
    dry_run: bool = False,
) -> PremiumEmojiEditResult:
    entity = await client.get_entity(chat)
    message = await client.get_messages(entity, ids=int(message_id))
    if not message:
        return PremiumEmojiEditResult(chat, int(message_id), False, 0, "", "", "message_not_found")
    before = getattr(message, "message", None) or ""
    after, entities, count = apply_daily_free_premium_emojis(
        before,
        getattr(message, "entities", None) or [],
        document_ids=document_ids,
    )
    if count <= 0 or after == before:
        return PremiumEmojiEditResult(chat, int(message_id), False, count, before, after)
    if dry_run:
        return PremiumEmojiEditResult(chat, int(message_id), False, count, before, after)
    try:
        await client.edit_message(
            entity,
            int(message_id),
            after,
            formatting_entities=entities,
            link_preview=False,
            buttons=getattr(message, "reply_markup", None),
        )
    except MessageNotModifiedError:
        return PremiumEmojiEditResult(chat, int(message_id), False, count, before, after, "not_modified")
    return PremiumEmojiEditResult(chat, int(message_id), True, count, before, after)


async def edit_latest_daily_announcement(
    client: TelegramClient,
    chats: Iterable[str | int],
    *,
    document_ids: Sequence[int] | None = None,
    dry_run: bool = False,
) -> list[PremiumEmojiEditResult]:
    results: list[PremiumEmojiEditResult] = []
    for chat in chats:
        entity = await client.get_entity(chat)
        messages = await client.get_messages(entity, limit=5, search="#ежедневныйанонс")
        if not messages:
            results.append(PremiumEmojiEditResult(chat, 0, False, 0, "", "", "daily_announcement_not_found"))
            continue
        message = messages[0]
        results.append(
            await edit_message_daily_free_labels(
                client,
                chat,
                int(message.id),
                document_ids=document_ids,
                dry_run=dry_run,
            )
        )
    return results


async def edit_daily_messages_with_env(
    targets: Sequence[tuple[str | int, int]],
    *,
    delay_seconds: float = 0,
    dry_run: bool = False,
) -> list[PremiumEmojiEditResult]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    cfg = load_telethon_config_from_env()
    await raise_if_session_busy(cfg.auth_scope)
    ids = parse_document_ids()
    async with telethon_client_from_config(cfg) as client:
        results: list[PremiumEmojiEditResult] = []
        for chat, message_id in targets:
            try:
                results.append(
                    await edit_message_daily_free_labels(
                        client,
                        chat,
                        int(message_id),
                        document_ids=ids,
                        dry_run=dry_run,
                    )
                )
            except Exception as exc:
                logger.exception("tg_premium_emoji.edit_failed chat=%s message_id=%s", chat, message_id)
                results.append(
                    PremiumEmojiEditResult(chat, int(message_id), False, 0, "", "", f"{type(exc).__name__}: {exc}")
                )
        return results


def premium_emoji_editor_enabled() -> bool:
    return _env("ENABLE_TG_PREMIUM_EMOJI_EDITOR") in {"1", "true", "yes", "on"}


def premium_emoji_editor_delay_seconds() -> int:
    raw = _env("TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS")
    if not raw:
        return 150
    try:
        return max(0, int(raw))
    except Exception:
        logger.warning("invalid TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS=%r; using 150", raw)
        return 150
