"""Telethon premium-emoji post editor for events-bot Telegram surfaces."""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
import random
import re
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
DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS: dict[str, int] = {
    "🎭": 5390961951150988955,
    "👉": 5204036388789445008,
    "🤘": 5404517529362128309,
    "🎟": 5267071016747690521,
    "💰": 5305700407874449437,
    "📗": 5339143926638996892,
    "🏰": 5305794630866989617,
}
DEFAULT_TRETYAKOV_EMOJI_FALLBACK = "🖼🖼"
DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS: tuple[int, int] = (
    5188445640325099838,
    5188470637034758005,
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
class _SubstitutionOp:
    start: int
    old_len: int
    replacement: str
    document_ids: tuple[int, ...]


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


def parse_daily_single_emoji_document_ids(raw: str | None = None) -> dict[str, int]:
    value = (raw or _env("TG_PREMIUM_EMOJI_DAILY_SINGLE_DOCUMENT_IDS_JSON")).strip()
    if not value:
        return dict(DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS)
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("TG_PREMIUM_EMOJI_DAILY_SINGLE_DOCUMENT_IDS_JSON must be an object")
    parsed: dict[str, int] = {}
    for emoji, document_id in payload.items():
        emoji_text = str(emoji)
        if not emoji_text:
            continue
        parsed[emoji_text] = int(document_id)
    if not parsed:
        raise ValueError("TG_PREMIUM_EMOJI_DAILY_SINGLE_DOCUMENT_IDS_JSON is empty")
    return parsed


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
    ops: Sequence[_SubstitutionOp],
) -> tuple[str, list[Any], int]:
    if not ops:
        return text, list(entities or []), 0

    sur_text = add_surrogate(text)
    adjusted_entities = list(entities or [])
    premium_entities: list[MessageEntityCustomEmoji] = []
    shift = 0

    for op in sorted(ops, key=lambda item: item.start):
        start = op.start + shift
        old_len = op.old_len
        replacement = op.replacement
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
            elif not isinstance(entity, MessageEntityCustomEmoji) and ent_start <= start and ent_end >= end:
                cloned = copy.copy(entity)
                cloned.length = ent_len + delta
                next_entities.append(cloned)
            elif old_len == 0 and delta > 0 and ent_start < start < ent_end:
                cloned = copy.copy(entity)
                cloned.length = ent_len + delta
                next_entities.append(cloned)
            elif delta == 0 and not isinstance(entity, MessageEntityCustomEmoji):
                # Single-emoji premiumization keeps the visible text unchanged.
                # Preserve surrounding title/link formatting even when it covers
                # the emoji; Telegram accepts custom emoji entities alongside
                # ordinary formatting entities for the same text range.
                next_entities.append(entity)
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
        for index, document_id in enumerate(op.document_ids):
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


def _ranges_overlap(start: int, end: int, ranges: Sequence[tuple[int, int]]) -> bool:
    return any(not (end <= taken_start or start >= taken_end) for taken_start, taken_end in ranges)


def _find_daily_free_label_ops(text: str, document_ids: Sequence[int]) -> list[_SubstitutionOp]:
    sur_text = add_surrogate(text)
    ids = tuple(int(item) for item in document_ids)
    ops: list[_SubstitutionOp] = []
    occupied: list[tuple[int, int]] = []

    def add_matches(pattern: str, start_at: int = 0) -> None:
        sur_pattern = add_surrogate(pattern)
        pos = sur_text.find(sur_pattern, start_at)
        while pos >= 0:
            end = pos + len(sur_pattern)
            if not _ranges_overlap(pos, end, occupied):
                ops.append(_SubstitutionOp(pos, len(sur_pattern), DEFAULT_FREE_EMOJI_FALLBACK, ids))
                occupied.append((pos, end))
            pos = sur_text.find(sur_pattern, pos + 1)

    heading_pos = sur_text.find(add_surrogate(DAILY_ADDED_HEADING))
    if heading_pos >= 0:
        add_matches("🚩 🟡", heading_pos)

    add_matches("🟡 Бесплатно")
    return sorted(ops, key=lambda item: item.start)


def _find_daily_single_emoji_ops(
    text: str,
    entities: Sequence[Any] | None,
    mapping: dict[str, int],
) -> list[_SubstitutionOp]:
    sur_text = add_surrogate(text)
    custom_entities = [entity for entity in (entities or []) if isinstance(entity, MessageEntityCustomEmoji)]
    ops: list[_SubstitutionOp] = []
    occupied: list[tuple[int, int]] = []
    for emoji, document_id in mapping.items():
        expected_ranges = [
            (int(getattr(entity, "offset", 0)), int(getattr(entity, "offset", 0)) + int(getattr(entity, "length", 0)))
            for entity in custom_entities
            if int(getattr(entity, "document_id", 0)) == int(document_id)
        ]
        sur_emoji = add_surrogate(emoji)
        pos = sur_text.find(sur_emoji)
        while pos >= 0:
            end = pos + len(sur_emoji)
            if not _ranges_overlap(pos, end, expected_ranges) and not _ranges_overlap(pos, end, occupied):
                ops.append(_SubstitutionOp(pos, len(sur_emoji), emoji, (int(document_id),)))
                occupied.append((pos, end))
            pos = sur_text.find(sur_emoji, pos + 1)
    return sorted(ops, key=lambda item: item.start)


def _plain_prefix_is_daily_venue_context(prefix: str) -> bool:
    stripped = prefix.strip()
    if not stripped:
        return False
    if stripped.endswith("📍"):
        return True
    return bool(
        re.search(
            r"(?:^|\s)\d{1,2}\s+[а-яё]+(?:\s+\d{1,2}:\d{2})?$",
            stripped,
            flags=re.IGNORECASE,
        )
    )


def _daily_tretyakov_emoji_ids() -> tuple[int, int]:
    raw = _env("TG_PREMIUM_EMOJI_TRETYAKOV_DOCUMENT_IDS")
    if not raw:
        return DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS
    parts = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    if len(parts) != 2:
        raise ValueError("TG_PREMIUM_EMOJI_TRETYAKOV_DOCUMENT_IDS must contain exactly two ids")
    return int(parts[0]), int(parts[1])


def _tretyakov_replacement_op(start: int, old_len: int) -> _SubstitutionOp:
    return _SubstitutionOp(
        start,
        old_len,
        DEFAULT_TRETYAKOV_EMOJI_FALLBACK,
        _daily_tretyakov_emoji_ids(),
    )


def _tretyakov_pair_entity_op(start: int) -> _SubstitutionOp:
    return _tretyakov_replacement_op(start, len(add_surrogate(DEFAULT_TRETYAKOV_EMOJI_FALLBACK)))


def _tretyakov_insert_before_location_op(start: int) -> _SubstitutionOp:
    return _SubstitutionOp(
        start,
        0,
        f"{DEFAULT_TRETYAKOV_EMOJI_FALLBACK} ",
        _daily_tretyakov_emoji_ids(),
    )


def _tretyakov_title_cleanup_op(start: int) -> _SubstitutionOp:
    # Tretyakov picture pairs are venue markers only. If an older editor placed
    # the pair in a daily card title, collapse it back to the ordinary picture
    # emoji and drop overlapping custom-emoji entities.
    return _SubstitutionOp(start, len(add_surrogate(DEFAULT_TRETYAKOV_EMOJI_FALLBACK)), "🖼️", ())


def _custom_emoji_ranges(entities: Sequence[Any] | None) -> list[tuple[int, int]]:
    return [
        (int(getattr(entity, "offset", 0)), int(getattr(entity, "offset", 0)) + int(getattr(entity, "length", 0)))
        for entity in (entities or [])
        if isinstance(entity, MessageEntityCustomEmoji)
    ]


def _tretyakov_title_single_cleanup_op(start: int, marker: str) -> _SubstitutionOp:
    # A regular picture emoji in the title is not a Tretyakov venue marker.
    # Replace with itself to drop any old custom-emoji entity bound to it.
    return _SubstitutionOp(start, len(add_surrogate(marker)), marker, ())


def _tretyakov_pair_already_expected(
    start: int,
    entities: Sequence[Any] | None,
    document_ids: tuple[int, int],
) -> bool:
    expected = {
        start: int(document_ids[0]),
        start + len(add_surrogate("🖼")): int(document_ids[1]),
    }
    found: dict[int, int] = {}
    for entity in entities or []:
        if not isinstance(entity, MessageEntityCustomEmoji):
            continue
        offset = int(getattr(entity, "offset", 0))
        if offset in expected and int(getattr(entity, "length", 0)) == len(add_surrogate("🖼")):
            found[offset] = int(getattr(entity, "document_id", 0))
    return found == expected


def _maybe_tretyakov_pair_entity_op(start: int, entities: Sequence[Any] | None) -> _SubstitutionOp | None:
    document_ids = _daily_tretyakov_emoji_ids()
    if _tretyakov_pair_already_expected(start, entities, document_ids):
        return None
    return _SubstitutionOp(
        start,
        len(add_surrogate(DEFAULT_TRETYAKOV_EMOJI_FALLBACK)),
        DEFAULT_TRETYAKOV_EMOJI_FALLBACK,
        document_ids,
    )


def _find_daily_tretyakov_ops(text: str, entities: Sequence[Any] | None = None) -> list[_SubstitutionOp]:
    ops: list[_SubstitutionOp] = []
    sur_offset = 0
    custom_ranges = _custom_emoji_ranges(entities)
    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        stripped = visible_line.strip()
        line_sur = add_surrogate(visible_line)
        if stripped.startswith(f"👉 {DEFAULT_TRETYAKOV_EMOJI_FALLBACK} "):
            start = sur_offset + line_sur.find(add_surrogate(DEFAULT_TRETYAKOV_EMOJI_FALLBACK))
            if start >= sur_offset:
                ops.append(_tretyakov_title_cleanup_op(start))
        elif stripped.startswith("👉 "):
            for marker in ("🖼️", "🖼"):
                marker_pos = visible_line.find(marker)
                if marker_pos >= 0 and visible_line[:marker_pos].strip() == "👉":
                    start = sur_offset + len(add_surrogate(visible_line[:marker_pos]))
                    end = start + len(add_surrogate(marker))
                    if _ranges_overlap(start, end, custom_ranges):
                        ops.append(_tretyakov_title_single_cleanup_op(start, marker))
                    break

        date_location_match = re.match(
            r"^(\s*\d{1,2}\s+[а-яё]+(?:\s+\d{1,2}:\d{2})?\s+)(.+)$",
            visible_line,
            flags=re.IGNORECASE,
        )
        if date_location_match and re.search(r"Третьяков", date_location_match.group(2), flags=re.IGNORECASE):
            location_prefix = date_location_match.group(1)
            location_text = date_location_match.group(2)
            start = sur_offset + len(add_surrogate(location_prefix))
            if location_text.startswith(f"{DEFAULT_TRETYAKOV_EMOJI_FALLBACK} "):
                op = _maybe_tretyakov_pair_entity_op(start, entities)
                if op:
                    ops.append(op)
            else:
                ops.append(_tretyakov_insert_before_location_op(start))

        for venue_marker in ("Третьяков",):
            pos = visible_line.find(venue_marker)
            while pos >= 0:
                prefix = visible_line[:pos]
                if _plain_prefix_is_daily_venue_context(prefix):
                    if prefix.endswith(f"{DEFAULT_TRETYAKOV_EMOJI_FALLBACK} "):
                        pair_start = len(prefix) - len(f"{DEFAULT_TRETYAKOV_EMOJI_FALLBACK} ")
                        op = _maybe_tretyakov_pair_entity_op(
                            sur_offset + len(add_surrogate(visible_line[:pair_start])),
                            entities,
                        )
                        if op:
                            ops.append(op)
                    else:
                        start = sur_offset + len(add_surrogate(prefix))
                        ops.append(_tretyakov_insert_before_location_op(start))
                pos = visible_line.find(venue_marker, pos + len(venue_marker))

        # Added-announcement rows are one-line compact records. The generator,
        # not the editor, decides from structured venue data whether `🚩` should
        # become the Tretyakov venue marker; the editor only attaches/fixes the
        # custom-emoji entities when the visible pair is already present.
        if re.match(r"\s*\d{1,2}\.\d{1,2}\s+", visible_line):
            pair_pos = visible_line.find(DEFAULT_TRETYAKOV_EMOJI_FALLBACK)
            if pair_pos >= 0:
                op = _maybe_tretyakov_pair_entity_op(
                    sur_offset + len(add_surrogate(visible_line[:pair_pos])),
                    entities,
                )
                if op:
                    ops.append(op)

        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)



_ROCK_CONCERT_RE = re.compile(
    r"(?:\brock\b|(?<![а-яё])рок(?![а-яё])|метал(?:л)?|\bmetal\b|панк|\bpunk\b|хардкор|\bhardcore\b|крематор)",
    flags=re.IGNORECASE,
)
_ROCK_TITLE_ICON_VARIANTS = (
    "🎸",
    "🎵",
    "🎶",
    "🎤",
    "🎙️",
    "🎙",
    "🎧",
    "🎼",
    "🎷",
    "🎺",
    "🥁",
)


def _mentions_rock_concert(text: str) -> bool:
    return bool(_ROCK_CONCERT_RE.search(text.casefold()))


def _rock_replacement_op(start: int, old_len: int, mapping: dict[str, int]) -> _SubstitutionOp | None:
    document_id = mapping.get("🤘")
    if not document_id:
        return None
    return _SubstitutionOp(start, old_len, "🤘", (int(document_id),))


def _find_rock_concert_icon_ops(text: str, mapping: dict[str, int]) -> list[_SubstitutionOp]:
    if not mapping.get("🤘"):
        return []
    ops: list[_SubstitutionOp] = []
    sur_offset = 0
    global_rock = _mentions_rock_concert(text)
    first_non_empty_seen = False

    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        stripped = visible_line.strip()
        if not stripped:
            sur_offset += len(add_surrogate(line))
            continue

        is_first_non_empty = not first_non_empty_seen
        first_non_empty_seen = True
        line_rock = _mentions_rock_concert(visible_line)
        # Event posts often mention genre in the body, not in the bold title;
        # for channel posts only the first title line should inherit the
        # message-level rock-concert signal. Daily rows require a line-level
        # signal to avoid changing unrelated events in the same announcement.
        should_consider = line_rock or (is_first_non_empty and global_rock)
        if should_consider and not stripped.startswith("🤘") and "🤘" not in visible_line[:8]:
            for icon in _ROCK_TITLE_ICON_VARIANTS:
                pos = visible_line.find(icon)
                if pos < 0:
                    continue
                prefix = visible_line[:pos]
                # Replace only title/category icons near the beginning of a row:
                # event title, `👉 ...` full card, or `DD.MM ...` added row.
                if len(prefix.strip()) <= 18 and (
                    is_first_non_empty
                    or stripped.startswith("👉")
                    or re.match(r"^\d{1,2}\.\d{1,2}\b", stripped)
                ):
                    op = _rock_replacement_op(
                        sur_offset + len(add_surrogate(prefix)),
                        len(add_surrogate(icon)),
                        mapping,
                    )
                    if op:
                        ops.append(op)
                    break

        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)




def _find_event_calendar_ops(text: str, entities: Sequence[Any] | None, mapping: dict[str, int]) -> list[_SubstitutionOp]:
    """Premiumize event-post calendar/date row icons.

    Only calendar/date rows keep the curated `🎟` custom emoji. Ticket rows use
    the product-distinct ordinary `🎫` icon to avoid showing two calendar-like
    icons in one post.
    """
    document_id = mapping.get("🎟")
    if not document_id:
        return []
    existing_calendar_ranges = [
        (int(getattr(entity, "offset", 0)), int(getattr(entity, "offset", 0)) + int(getattr(entity, "length", 0)))
        for entity in (entities or [])
        if isinstance(entity, MessageEntityCustomEmoji) and int(getattr(entity, "document_id", 0)) == int(document_id)
    ]
    ops: list[_SubstitutionOp] = []
    sur_offset = 0
    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        stripped = visible_line.lstrip()
        if stripped.startswith("📅"):
            start = sur_offset + len(add_surrogate(visible_line[: len(visible_line) - len(stripped)]))
            end = start + len(add_surrogate("📅"))
            if not _ranges_overlap(start, end, existing_calendar_ranges):
                ops.append(_SubstitutionOp(start, len(add_surrogate("📅")), "🎟", (int(document_id),)))
        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)


def _find_event_ticket_icon_ops(text: str) -> list[_SubstitutionOp]:
    """Move legacy event-post ticket rows from `🎟` to `🎫`.

    `🎟` is reserved for the custom calendar/date marker. Ticket and registration
    rows use `🎫` so date and ticket semantics stay visually distinct.
    """
    ops: list[_SubstitutionOp] = []
    sur_offset = 0
    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        stripped = visible_line.lstrip()
        if stripped.startswith("🎟") and re.search(r"(?:Билеты|по регистрации)", stripped, flags=re.IGNORECASE):
            start = sur_offset + len(add_surrogate(visible_line[: len(visible_line) - len(stripped)]))
            ops.append(_SubstitutionOp(start, len(add_surrogate("🎟")), "🎫", ()))
        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)

def _find_event_ticket_price_ops(text: str, mapping: dict[str, int]) -> list[_SubstitutionOp]:
    """Convert event-post ticket prices from `Билеты 1000 руб.` to `Билеты 💰 1000`.

    Ticket rows use visible `🎫`; date/calendar rows are handled separately.
    Only ticket price lines receive the ruble/money custom emoji before the
    numeric price, with the textual `руб.` suffix removed.
    """
    money_id = mapping.get("💰")
    if not money_id:
        return []
    ops: list[_SubstitutionOp] = []
    sur_offset = 0
    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        match = re.search(
            r"(Билеты\s+(?:от\s+)?)(?!💰\s)(\d[\d\s]*(?:[.,]\d+)?(?:\s+до\s+\d[\d\s]*(?:[.,]\d+)?)?)(\s*руб\.?)",
            visible_line,
            flags=re.IGNORECASE,
        )
        if match:
            number_start = sur_offset + len(add_surrogate(visible_line[: match.start(2)]))
            number_text = match.group(2).rstrip()
            rub_start = number_start + len(add_surrogate(number_text))
            rub_end = sur_offset + len(add_surrogate(visible_line[: match.end(3)]))
            ops.append(_SubstitutionOp(number_start, 0, "💰 ", (int(money_id),)))
            ops.append(_SubstitutionOp(rub_start, rub_end - rub_start, "", ()))
        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)

def _find_daily_insert_emoji_ops(text: str, mapping: dict[str, int]) -> list[_SubstitutionOp]:
    ops: list[_SubstitutionOp] = []
    money_id = mapping.get("💰")
    if money_id:
        sur_text = add_surrogate(text)
        money_sur = add_surrogate("💰")
        price_pattern = re.compile(
            rf"(Билеты в источнике\s+)(?!{re.escape(money_sur)}\s)(\d[\d\s]*(?:[.,]\d+)?)"
        )
        for match in price_pattern.finditer(sur_text):
            ops.append(_SubstitutionOp(match.start(2), 0, "💰 ", (int(money_id),)))

    venue_specs = (
        ("Научная библиотека", "📗"),
        ("Замок Ноухайзен", "🏰"),
    )
    sur_offset = 0
    for line in text.splitlines(keepends=True):
        visible_line = line.rstrip("\r\n")
        for venue, icon in venue_specs:
            document_id = mapping.get(icon)
            if not document_id:
                continue
            pos = visible_line.find(venue)
            while pos >= 0:
                prefix = visible_line[:pos]
                if not prefix.endswith(f"{icon} ") and _plain_prefix_is_daily_venue_context(prefix):
                    start = sur_offset + len(add_surrogate(prefix))
                    ops.append(_SubstitutionOp(start, 0, f"{icon} ", (int(document_id),)))
                pos = visible_line.find(venue, pos + len(venue))
        sur_offset += len(add_surrogate(line))
    return sorted(ops, key=lambda item: item.start)


def apply_daily_free_premium_emojis(
    text: str,
    entities: Sequence[Any] | None = None,
    *,
    document_ids: Sequence[int] | None = None,
    single_emoji_document_ids: dict[str, int] | None = None,
) -> tuple[str, list[Any], int]:
    """Replace daily free markers and configured daily emoji with premium custom emoji entities."""
    ids = tuple(document_ids or parse_document_ids())
    if len(ids) != len(DEFAULT_FREE_EMOJI_FALLBACK):
        raise ValueError("daily free premium label must contain exactly 4 custom emoji document ids")
    single_ids = single_emoji_document_ids if single_emoji_document_ids is not None else parse_daily_single_emoji_document_ids()
    single_mapping = dict(single_ids)
    ops = [
        *_find_daily_free_label_ops(text, ids),
        *_find_daily_insert_emoji_ops(text, single_mapping),
        *_find_daily_tretyakov_ops(text, entities),
        *_find_rock_concert_icon_ops(text, single_mapping),
        *_find_event_calendar_ops(text, entities, single_mapping),
        *_find_event_ticket_icon_ops(text),
        *_find_event_ticket_price_ops(text, single_mapping),
        *_find_daily_single_emoji_ops(
            text,
            entities,
            {emoji: document_id for emoji, document_id in single_mapping.items() if emoji != "🎟"},
        ),
    ]
    return _apply_substitution_ops(text, entities, ops)


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
    single_emoji_document_ids: dict[str, int] | None = None,
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
        single_emoji_document_ids=single_emoji_document_ids,
    )
    if count <= 0:
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
    single_emoji_document_ids: dict[str, int] | None = None,
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
                single_emoji_document_ids=single_emoji_document_ids,
                dry_run=dry_run,
            )
        )
    return results


def premium_emoji_editor_jitter_seconds() -> int:
    raw = _env("TG_PREMIUM_EMOJI_EDIT_JITTER_SECONDS")
    if not raw:
        return 45
    try:
        return max(0, int(raw))
    except Exception:
        logger.warning("invalid TG_PREMIUM_EMOJI_EDIT_JITTER_SECONDS=%r; using 45", raw)
        return 45


def premium_emoji_between_edits_range() -> tuple[float, float]:
    raw = _env("TG_PREMIUM_EMOJI_BETWEEN_EDITS_SECONDS")
    if not raw:
        return 3.0, 12.0
    try:
        left, right = raw.replace(";", ",").split(",", 1)
        minimum = max(0.0, float(left.strip()))
        maximum = max(minimum, float(right.strip()))
        return minimum, maximum
    except Exception:
        logger.warning("invalid TG_PREMIUM_EMOJI_BETWEEN_EDITS_SECONDS=%r; using 3,12", raw)
        return 3.0, 12.0


async def edit_messages_with_env(
    targets: Sequence[tuple[str | int, int]],
    *,
    delay_seconds: float = 0,
    dry_run: bool = False,
) -> list[PremiumEmojiEditResult]:
    rng = random.SystemRandom()
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds + rng.uniform(0.0, float(premium_emoji_editor_jitter_seconds())))
    cfg = load_telethon_config_from_env()
    await raise_if_session_busy(cfg.auth_scope)
    ids = parse_document_ids()
    single_ids = parse_daily_single_emoji_document_ids()
    between_min, between_max = premium_emoji_between_edits_range()
    async with telethon_client_from_config(cfg) as client:
        results: list[PremiumEmojiEditResult] = []
        for index, (chat, message_id) in enumerate(targets):
            if index > 0 and not dry_run:
                await asyncio.sleep(rng.uniform(between_min, between_max))
            try:
                results.append(
                    await edit_message_daily_free_labels(
                        client,
                        chat,
                        int(message_id),
                        document_ids=ids,
                        single_emoji_document_ids=single_ids,
                        dry_run=dry_run,
                    )
                )
            except Exception as exc:
                logger.exception("tg_premium_emoji.edit_failed chat=%s message_id=%s", chat, message_id)
                results.append(
                    PremiumEmojiEditResult(chat, int(message_id), False, 0, "", "", f"{type(exc).__name__}: {exc}")
                )
        return results


async def edit_daily_messages_with_env(
    targets: Sequence[tuple[str | int, int]],
    *,
    delay_seconds: float = 0,
    dry_run: bool = False,
) -> list[PremiumEmojiEditResult]:
    return await edit_messages_with_env(targets, delay_seconds=delay_seconds, dry_run=dry_run)


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
