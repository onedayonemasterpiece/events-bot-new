from __future__ import annotations

import asyncio
import calendar
import hashlib
import json
import logging
import os
import random
import re
import shutil
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import aiohttp
import aiosqlite
from db import Database
from poster_media import (
    PosterMedia,
    apply_ocr_results_to_media,
    build_poster_summary,
    collect_poster_texts,
    is_supabase_storage_url,
    process_media,
)
import poster_ocr
from source_parsing.date_utils import normalize_implicit_iso_date_to_anchor
from source_parse_contract import (
    decision_from_provider_payload,
    EvidenceManifest,
    LifecycleAction,
    PARSE_VERSION,
    SourceDisposition,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
)

from sections import MONTHS_RU
from runtime import require_main_attr
from supabase_export import SBExporter

logger = logging.getLogger(__name__)

# Crawl tuning parameters
VK_CRAWL_PAGE_SIZE = int(os.getenv("VK_CRAWL_PAGE_SIZE", "30"))
VK_CRAWL_MAX_PAGES_INC = int(os.getenv("VK_CRAWL_MAX_PAGES_INC", "1"))
VK_CRAWL_OVERLAP_SEC = int(os.getenv("VK_CRAWL_OVERLAP_SEC", "300"))
VK_CRAWL_PAGE_SIZE_BACKFILL = int(os.getenv("VK_CRAWL_PAGE_SIZE_BACKFILL", "50"))
VK_CRAWL_MAX_PAGES_BACKFILL = int(os.getenv("VK_CRAWL_MAX_PAGES_BACKFILL", "3"))
VK_CRAWL_BACKFILL_DAYS = int(os.getenv("VK_CRAWL_BACKFILL_DAYS", "14"))
VK_CRAWL_BACKFILL_AFTER_IDLE_H = int(os.getenv("VK_CRAWL_BACKFILL_AFTER_IDLE_H", "24"))
VK_CRAWL_BACKFILL_OVERRIDE_MAX_DAYS = int(
    os.getenv("VK_CRAWL_BACKFILL_OVERRIDE_MAX_DAYS", "60")
)
VK_CRAWL_MIN_FREE_MB = int(os.getenv("VK_CRAWL_MIN_FREE_MB", "512"))
VK_USE_PYMORPHY = os.getenv("VK_USE_PYMORPHY", "false").lower() == "true"

VK_CRAWL_ADMISSION_PROMPT_VERSION = "vk-crawl-admission-v1"
VK_CRAWL_ADMISSION_MODEL = (
    os.getenv("VK_CRAWL_ADMISSION_MODEL", "gemini-3.1-flash-lite") or ""
).strip() or "gemini-3.1-flash-lite"

# Sentinel used to flag posts awaiting poster OCR before keyword/date checks.
OCR_PENDING_SENTINEL = "__ocr_pending__"

# Explicit self-publisher bindings. This is deliberately not derived from the
# generic vk_source.name: aggregators and venue/community publishers must not
# silently become organizers of every event they post.
_CURATED_VK_EVENT_ORGANIZERS: dict[int, tuple[str, ...]] = {
    12286984: ("Профи-тур",),
    190663987: ("Хранители руин",),
}


def _curated_vk_event_organizers(group_id: int | None) -> list[str]:
    return list(_CURATED_VK_EVENT_ORGANIZERS.get(int(group_id or 0), ()))

HISTORY_MATCHED_KEYWORD = "history"

_VK_PARSE_PREFILTER_VISIT_HINT_RE = re.compile(
    r"\b("
    r"билет\w*|регистрац\w*|вход\w*|стоимост\w*|донат\w*|"
    r"приглаша(?:ем|ют)\w*|приходите|жд[её]м|"
    r"состоит(?:ся|есь)\w*|пройдет\w*|пройд[её]т\w*|"
    r"начал\w*|открыти\w*|сеанс\w*|"
    r"экскурси\w*|лекци\w*|концерт\w*|спектакл\w*|"
    r"выставк\w*|кинопоказ\w*|мастер[ -]класс\w*|фестивал\w*"
    r")\b",
    re.I | re.U,
)
_VK_PARSE_PREFILTER_ADMIN_RE = re.compile(
    r"\b("
    r"администрац\w*|жител\w*|голосовани\w*|итог\w*|"
    r"проект[а-я-]*победител\w*|благоустройств\w*|"
    r"муниципальн\w*|округ\w*|район\w*|нацпроект\w*|"
    r"заседан\w*|депутат\w*|совет\w*|"
    r"поздрав\w*|наград\w*|юбиляр\w*"
    r")\b",
    re.I | re.U,
)
_VK_PARSE_GIVEAWAY_RE = re.compile(
    r"\b(розыгрыш|разыгрыва\w*|розыгра\w*|выигра\w*|конкурс|giveaway)\b",
    re.I | re.U,
)
_VK_PARSE_GIVEAWAY_TICKETS_RE = re.compile(
    r"\b(билет\w*|пригласительн\w*|абонемент\w*)\b",
    re.I | re.U,
)
_VK_PARSE_GIVEAWAY_MECHANICS_RE = re.compile(
    r"\b("
    r"услови\w*|участв\w*|подпиш\w*|репост\w*|коммент\w*|"
    r"отмет\w*|лайк\w*|победител\w*|итог\w*|приз\w*"
    r")\b",
    re.I | re.U,
)


def _read_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


VK_CRAWL_ADMISSION_BATCH_SIZE = max(
    1, min(_read_int_env("VK_CRAWL_ADMISSION_BATCH_SIZE", 8), 20)
)


def _require_vk_crawl_storage_headroom(db: Any) -> None:
    """Fail before VK fetch/persistence when the production volume is unsafe."""

    db_path = os.path.abspath(str(getattr(db, "path", "") or ""))
    runtime_root = os.path.abspath(
        (os.getenv("RUNTIME_DISK_PATH") or "/data").strip() or "/data"
    )
    try:
        on_runtime_volume = os.path.commonpath((db_path, runtime_root)) == runtime_root
    except ValueError:
        on_runtime_volume = False
    if not on_runtime_volume:
        return
    minimum_mb = max(0, _read_int_env("VK_CRAWL_MIN_FREE_MB", VK_CRAWL_MIN_FREE_MB))
    try:
        usage = shutil.disk_usage(runtime_root)
    except Exception as exc:
        raise RuntimeError(
            f"vk_crawl_storage_admission_unknown:{type(exc).__name__}"
        ) from exc
    free_mb = int(usage.free // (1024 * 1024))
    if free_mb < minimum_mb:
        raise RuntimeError(
            f"vk_crawl_storage_admission_blocked:free_mb={free_mb}:min_free_mb={minimum_mb}"
        )


def _vk_parse_should_add_giveaway_prize_hint(
    text: str,
    *,
    poster_texts: Sequence[str] | None = None,
) -> bool:
    combined_parts: list[str] = [text or ""]
    for block in list(poster_texts or [])[:2]:
        if isinstance(block, str) and block.strip():
            combined_parts.append(block)
    combined = "\n".join(part for part in combined_parts if part and part.strip())
    if not combined:
        return False
    norm = unicodedata.normalize("NFKC", combined).casefold().replace("ё", "е")
    if not (_VK_PARSE_GIVEAWAY_RE.search(norm) and _VK_PARSE_GIVEAWAY_TICKETS_RE.search(norm)):
        return False
    return bool(_VK_PARSE_GIVEAWAY_MECHANICS_RE.search(norm))


def _normalize_prompt_ocr_block(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    normalized = normalized.replace("\xa0", " ")
    lines: list[str] = []
    seen: set[str] = set()
    for raw_line in normalized.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line or line in seen:
            continue
        seen.add(line)
        lines.append(line)
    return "\n".join(lines).strip()


def _truncate_prompt_block(text: str, limit: int) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    if limit <= 80:
        return text[:limit].rstrip()
    head = max(1, int(limit * 0.65))
    tail = max(0, limit - head - 5)
    if tail <= 0:
        return text[:limit].rstrip()
    return f"{text[:head].rstrip()}\n...\n{text[-tail:].lstrip()}".strip()


_VK_PARSE_POSTER_LOGISTICS_RE = re.compile(
    r"(?:"
    r"\b[0-2]?\d[:.][0-5]\d\b|"
    r"\b[0-3]?\d\s*['’`.\-/]?\s*(?:январ[ьяе]?|феврал[ьяе]?|март[ае]?|апрел[ьяе]?|ма[йяе]|июн[ьяе]?|июл[ьяе]?|август[ае]?|сентябр[ьяе]?|октябр[ьяе]?|ноябр[ьяе]?|декабр[ьяе]?)\b|"
    r"\b(?:январ[ьяе]?|феврал[ьяе]?|март[ае]?|апрел[ьяе]?|ма[йяе]|июн[ьяе]?|июл[ьяе]?|август[ае]?|сентябр[ьяе]?|октябр[ьяе]?|ноябр[ьяе]?|декабр[ьяе]?)\b|"
    r"\bг\.\s*|"
    r"\b(?:г\.|город|адрес|ул\.?|улица|проспект|пр-?кт|пр\.|наб\.?|набережн\w*|площад\w*|сквер|парк|дк|дом культуры|центр|театр|музей|библиотек\w*|галере\w*|сцена|зал|клуб|дворец)\b|"
    r"\b(?:вход|свободн\w*|бесплатн\w*|0\+|6\+|12\+|16\+|18\+)\b"
    r")",
    re.I | re.U,
)


def _extract_vk_parse_poster_logistics_block(text: str, limit: int) -> str:
    """Return compact poster OCR logistics lines for long VK posts.

    Long source captions can already fill the LLM context, but the poster may be
    the only place with start time or venue.  Keep only lines that look like
    source-grounded logistics evidence; semantic event decisions stay with the
    downstream LLM parser.
    """

    block = _normalize_prompt_ocr_block(text)
    if not block:
        return ""
    selected: list[str] = []
    for line in block.splitlines():
        norm = unicodedata.normalize("NFKC", line).casefold().replace("ё", "е")
        if _VK_PARSE_POSTER_LOGISTICS_RE.search(norm):
            selected.append(line)
    if not selected:
        return ""
    compact = "\n".join(selected).strip()
    return _truncate_prompt_block(compact, limit).strip()


_LLM_FIELD_PLACEHOLDER_LITERALS: dict[str, frozenset[str]] = {
    "location_name": frozenset({"location_name", "venue", "place", "место", "площадка"}),
    "location_address": frozenset({"location_address", "address", "адрес"}),
    "city": frozenset({"city", "город"}),
}


def _clean_llm_text_field(value: Any, *, field_name: str | None = None) -> str | None:
    """Drop literal field-name placeholders from LLM output without semantic rewrites."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    if field_name:
        norm = unicodedata.normalize("NFKC", text).casefold().replace("ё", "е")
        placeholders = _LLM_FIELD_PLACEHOLDER_LITERALS.get(field_name, frozenset())
        if norm in placeholders:
            return None
    return text


def _budget_vk_parse_poster_texts(post_text: str, poster_texts: Sequence[str]) -> list[str]:
    """Return every available OCR block without semantic budgeting.

    Source evidence is never shortened because the post is long, lacks a
    logistics keyword, or has many cards. Provider/context overflow is a typed
    retry concern, not permission to omit a carrier fragment silently.
    """

    del post_text  # inclusion is deliberately independent of text shape
    return [
        block
        for block in (_normalize_prompt_ocr_block(text) for text in poster_texts)
        if block
    ]

def _normalize_group_title(value: str | None) -> str | None:
    if not value:
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.strip()
    if not normalized:
        return None
    return normalized.casefold()


def _display_group_title(value: str | None, gid: int) -> str:
    if not value:
        return f"club{gid}"
    display = unicodedata.normalize("NFKC", value)
    display = display.replace("\xa0", " ")
    display = re.sub(r"\s+", " ", display)
    display = display.strip()
    if not display:
        return f"club{gid}"
    return display


def _normalize_group_screen_name(value: str | None) -> str | None:
    if not value:
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.replace("\xa0", " ")
    normalized = normalized.strip().lstrip("@")
    if not normalized:
        return None
    normalized = re.sub(r"\s+", "", normalized)
    if not normalized:
        return None
    return normalized.casefold()


def _display_group_screen_name(value: str | None, gid: int) -> str:
    if not value:
        return f"club{gid}"
    display = unicodedata.normalize("NFKC", value)
    display = display.replace("\xa0", " ")
    display = display.strip().lstrip("@")
    if not display:
        return f"club{gid}"
    display = re.sub(r"\s+", "", display)
    if not display:
        return f"club{gid}"
    return display


# optional pymorphy3 initialisation
MORPH = None
if VK_USE_PYMORPHY:  # pragma: no cover - optional dependency
    try:
        import pymorphy3

        MORPH = pymorphy3.MorphAnalyzer()
    except Exception:
        VK_USE_PYMORPHY = False

# Keyword patterns for regex-based matching
GROUP_CONTEXT_PATTERN = r"групп[аы]\s+[\"«'][^\"»']+[\"»']"

KEYWORD_PATTERNS = [
    r"лекци(я|и|й|е|ю|ями|ях)",
    r"спектакл(ь|я|ю|ем|е|и|ей|ям|ями|ях)",
    r"концерт(?:ы|а|у|е|ом|ов|ам|ами|ах)?",
    r"фестивал(ь|я|ю|е|ем|и|ей|ям|ями|ях)|festival",
    r"ф[её]ст(а|у|ом|е|ы|ов|ам|ами|ах)?",
    r"fest",
    r"м(?:а|а?стер)[-\s]?класс(ы|а|е|ом|ов|ам|ами|ах)|мк\b",
    r"воркшоп(ы|а|е|ом|ов|ам|ами|ах)|workshop",
    r"показ(ы|а|е|ом|ов|ам|ами|ах)|кинопоказ",
    r"лекто(р|рия|рий|рии|риями|риях)|кинолекторий",
    r"выставк(а|и|е|у|ой|ам|ами|ах)",
    r"экскурси(я|и|е|ю|ей|ям|ями|ях)",
    r"читк(а|и|е|у|ой|ам|ами|ах)",
    r"перформанс(ы|а|е|ом|ов|ам|ами|ах)",
    r"встреч(а|и|е|у|ей|ам|ами|ах)",
    r"событ(?:ие|ия|ий|иях|иями|ию|ием|иям)",
    r"праздник(и|а|у|е|ом|ов|ам|ами|ах)?",
    r"праздничн(?:ый|ая|ое|ые|ого|ому|ым|ых|ую|ой|ыми|ом)",
    r"музыкальн(?:ое|ый|ая|ые|ым|ых|ом|ой|ому|ыми)",
    r"музык(?:а|и|е|у|ой|ою)",
    r"стих(?:и|отворен\w*)",
    r"песн(?:я|и|ей|е|ю|ями|ях|ью)",
    r"фортепиан(?:о|ный|ная|ные|ной|ном|ного|ному|ным|ных|нюю|ными)",
    r"сыгра\w*",
    r"жив(?:ой|ого|ым|ом)\s+звук(?:а|ом|у|и|ов)?",
    r"жив(?:ое|ом)?\s+исполнен\w*",
    r"выступлени(?:е|я|ю|ем|ями|ях)",
    r"хит(?:ы|ов|ом|ам|ами|ах)?",
    r"в\s+исполнен(?:ии|ием|ию)",
    r"в\s+программе[^\n,.!?]{0,40}?произведен(?:ие|ия|ий)",
    r"композитор(?:а|ов|ы)",
    GROUP_CONTEXT_PATTERN,
    r"band",
    r"бронировани(е|я|ю|ем)|билет(ы|а|ов)|регистраци(я|и|ю|ей)|афиш(а|и|е|у)",
    r"ведущ(ий|ая|ее|ие|его|ему|ем|им|их|ими|ую|ей)",
    r"караок[её]",
    r"трибь?ют|трибут|tribute(?:\s+show)?",
    r"дайджест(ы|а|у|ом|ах)?",
    r"приглашаем\s+(?:вас\s+)?на",
    r"пушкинск(?:ая|ой)\s+карт(?:а|у|е)",
]
KEYWORD_RE = re.compile(r"(?<!\w)#?(?:" + "|".join(KEYWORD_PATTERNS) + r")(?!\w)", re.I | re.U)
GROUP_CONTEXT_RE = re.compile(GROUP_CONTEXT_PATTERN, re.I | re.U)
GROUP_NAME_RE = re.compile(
    r"групп[аы]\s+[A-ZА-ЯЁ0-9][^\s,.:;!?]*(?:\s+[A-ZА-ЯЁ0-9][^\s,.:;!?]*){0,2}",
    re.U,
)

# Pricing patterns provide an additional hint for event-like posts
PRICE_AMOUNT_PATTERN = "\\d+(?:[ \\t\\u00a0\\u202f]\\d+)*"
PRICE_PATTERNS = [
    r"вход\s+свободн(?:ый|а|о)",
    r"бесплатн(?:о|ый|ая|ое|ые|ую|ым|ыми|ом|ых)",
    r"\bплатн(?:о|ый|ая|ое|ые|ую|ым|ыми|ом|ых)\b",
    r"\bстоимост[ьи]\b",
    r"\bпо\s+донат(?:у|ам)?\b",
    r"\bдонат(?:а|у|ом|ы)?\b",
    r"\bпожертвовани[еяюомьях]*\b",
    r"\bвзнос\b",
    r"\bоплат\w*\b",
    rf"(?:₽|руб(?:\.|лей|ля|ль)?|р\.?)\s*{PRICE_AMOUNT_PATTERN}",
    rf"\b{PRICE_AMOUNT_PATTERN}\s*(?:₽|руб(?:\.|лей|ля|ль)?|р\.?)",
    r"\bруб(?:\.|лей|ля|ль|ы)?\b",
]
PRICE_RE = re.compile("(?:" + "|".join(PRICE_PATTERNS) + ")", re.I | re.U)

# Canonical keywords for morphological mode
KEYWORD_LEMMAS = {
    "лекция",
    "спектакль",
    "концерт",
    "фестиваль",
    "фест",
    "fest",
    "мастер-класс",
    "воркшоп",
    "показ",
    "кинопоказ",
    "лекторий",
    "кинолекторий",
    "выставка",
    "экскурсия",
    "читка",
    "перформанс",
    "встреча",
    "событие",
    "праздник",
    "музыка",
    "музыкальный",
    "стих",
    "поэзия",
    "песня",
    "фортепиано",
    "сыграть",
    "хит",
    "исполнение",
    "выступление",
    "произведение",
    "композитор",
    "бронирование",
    "билет",
    "регистрация",
    "афиша",
    "ведущий",
    "караоке",
    "трибьют",
    "трибут",
    "tribute",
    "band",
    "дайджест",
    "приглашать",
}

# Date/time patterns used for quick detection
MONTH_NAMES_DET = "|".join(sorted(re.escape(m) for m in MONTHS_RU.keys()))
DATE_PATTERNS = [
    r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?(?!-\d)\b",
    r"\b\d{1,2}[–-]\d{1,2}(?:[./]\d{1,2})\b",
    rf"\b\d{{1,2}}\s+(?:{MONTH_NAMES_DET})\.?\b",
    r"\b(понед(?:ельник)?|вторник|сред(?:а)?|четверг|пятниц(?:а)?|суббот(?:а)?|воскресень(?:е|е)|пн|вт|ср|чт|пт|сб|вс)\b",
    r"\b(сегодня|завтра|послезавтра|в эти выходные)\b",
    r"\b([01]?\d|2[0-3])[:.][0-5]\d\b",
    r"\bв\s*([01]?\d|2[0-3])\s*(?:ч|час(?:а|ов)?)\b",
    r"\bс\s*([01]?\d|2[0-3])(?:[:.][0-5]\d)?\s*до\s*([01]?\d|2[0-3])(?:[:.][0-5]\d)?\b",
    r"\b20\d{2}\b",
]

COMPILED_DATE_PATTERNS = [re.compile(p, re.I | re.U) for p in DATE_PATTERNS]

DATE_PATTERN_STRONG_INDEXES = (0, 1, 2, 3, 4, 8)

PAST_EVENT_RE = re.compile(
    r"\b("
    r"состоял(?:ась|ось|ся|и|а)?|"
    r"прош[её]л(?:и|а)?|"
    r"проходил(?:и|а|о)?|"
    r"завершил(?:ись|ась|ось|ся|и|а|о)?|"
    r"отгремел(?:а|и|о)?"
    r")\b",
    re.I,
)

HISTORICAL_TOPONYMS = [
    "кёнигсберг",
    "кенигсберг",
    "гумбинен",
    "инстербург",
    "тильзит",
    "мемель",
    "тапиау",
    "кранц",
    "раушен",
    "пиллау",
    "роминта",
    "гердауэн",
    "гёрдауэн",
    "гердауен",
    "гёрдауен",
    "пруссия",
    "восточная пруссия",
    "восточной пруссии",
]
HISTORICAL_YEAR_RE = re.compile(r"\b(1\d{3})\b")

NUM_DATE_RE = re.compile(
    r"\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?(?!-\d)\b"
)
PHONE_LIKE_RE = re.compile(r"^(?:\d{2}-){2,}\d{2}$")
FEDERAL_PHONE_CANDIDATE_RE = re.compile(r"(?<!\d)(?:\+7|8)\D*\d(?:\D*\d){9}")
CITY_PHONE_CANDIDATE_RE = re.compile(r"(?<!\d)\d(?:\D*\d){5}(?!\d)")
PHONE_CANDIDATE_RE = re.compile(
    rf"(?:{FEDERAL_PHONE_CANDIDATE_RE.pattern})|(?:{CITY_PHONE_CANDIDATE_RE.pattern})"
)
PHONE_CONTEXT_RE = re.compile(
    r"(\bтел(?:[.:]|ефон\w*|\b|(?=\d))|\bзвоните\b|\bзвонок\w*)",
    re.I | re.U,
)
EVENT_LOCATION_PREFIXES = (
    "клуб",
    "бар",
    "каф",
    "рест",
    "театр",
    "музе",
    "дом",
    "дк",
    "центр",
    "парк",
    "сад",
    "площад",
    "зал",
    "галер",
    "библиот",
    "филармон",
    "кин",
    "сц",
    "арен",
    "лофт",
    "коворк",
    "конгресс",
    "форум",
    "павиль",
    "дворц",
    "манеж",
    "усадь",
    "гостин",
    "отел",
    "hotel",
    "пансион",
    "санатор",
    "лагер",
    "база",
    "стадион",
)
EVENT_ADDRESS_PREFIXES = (
    "ул",
    "улиц",
    "пр",
    "просп",
    "пл",
    "пер",
    "наб",
    "бульв",
    "бул",
    "шос",
    "тракт",
    "дор",
    "мкр",
    "микр",
    "проезд",
    "пр-д",
    "б-р",
    "корп",
    "строен",
    "офис",
)
EVENT_ACTION_PREFIXES = (
    "собира",
    "встреч",
    "приглаш",
    "ждем",
    "ждём",
    "приход",
    "начал",
    "старт",
    "будет",
    "проход",
    "пройдет",
    "пройдёт",
    "состо",
    "откры",
    "ждет",
    "ждёт",
    "обсужд",
    "танцу",
    "игра",
    "мастер",
    "лекци",
    "семинар",
    "экскурс",
    "кинопоказ",
    "показ",
    "фестив",
    "ярмар",
    "праздн",
)
DATE_RANGE_RE = re.compile(r"\b(\d{1,2})[–-](\d{1,2})(?:[./](\d{1,2}))\b")
MONTH_NAME_RE = re.compile(r"\b(\d{1,2})\s+([а-яё.]+)\b", re.I)
_DAY_MONTH_NUM_RE = re.compile(
    r"\b\d{1,2}\s*[./-]\s*\d{1,2}(?:\s*[./-]\s*\d{2,4})?\b",
    re.IGNORECASE | re.UNICODE,
)
_DAY_MONTH_WORD_RE = re.compile(
    rf"\b\d{{1,2}}\s+(?:{MONTH_NAMES_DET})\.?\b",
    re.IGNORECASE | re.UNICODE,
)
TIME_RE = re.compile(r"\b([01]?\d|2[0-3])[:.][0-5]\d\b")
TIME_H_RE = re.compile(r"\bв\s*([01]?\d|2[0-3])\s*(?:ч|час(?:а|ов)?)\b")
BARE_TIME_H_RE = re.compile(r"\b([01]?\d|2[0-3])\s*(?:ч|час(?:а|ов)?)\b")
TIME_RANGE_RE = re.compile(
    r"\bс\s*([01]?\d|2[0-3])(?:[:.](\d{2}))?\s*до\s*([01]?\d|2[0-3])(?:[:.](\d{2}))?\b"
)
DOW_RE = re.compile(
    r"\b(понед(?:ельник)?|вторник|сред(?:а)?|четверг|пятниц(?:а)?|суббот(?:а)?|воскресень(?:е|е)|пн|вт|ср|чт|пт|сб|вс)\b",
    re.I,
)
WEEKEND_RE = re.compile(r"в\s+эти\s+выходны", re.I)

# Maximum age of a past date mention that should not be rolled over to the next year
RECENT_PAST_THRESHOLD = timedelta(days=92)

# cumulative processing time for VK event intake (seconds)
processing_time_seconds_total: float = 0.0


_POSTER_EXACT_DATETIME_RE = re.compile(
    rf"\b(?P<day>\d{{1,2}})\s+(?P<month>{MONTH_NAMES_DET})\.?\s+"
    r"(?P<hour>[01]?\d|2[0-3])[:.](?P<minute>[0-5]\d)\b",
    re.IGNORECASE | re.UNICODE,
)
_RELATIVE_TEXT_DATE_RE = re.compile(
    r"\b("
    r"сегодня|завтра|послезавтра|"
    r"в\s+эт(?:от|у|и)\s+"
    r"(?:понед(?:ельник)?|вторник|сред(?:а)?|четверг|пятниц(?:а)?|суббот(?:а)?|воскресень(?:е|е)|выходные)|"
    r"в\s+(?:понед(?:ельник)?|вторник|сред(?:а)?|четверг|пятниц(?:а)?|суббот(?:а)?|воскресень(?:е|е)|пн|вт|ср|чт|пт|сб|вс)"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)
_POSTER_VENUE_RE = re.compile(
    r"\b("
    r"образовательн\w+\s+центр|музей|театр|кинотеатр|библиотек|филармони|"
    r"галере|центр\s+культур|дом\s+культур|дом\s+искусств|пространств|зал|клуб|кирх"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)
_POSTER_ADDRESS_RE = re.compile(
    r"\b(ул\.?|улица|пр-?т|проспект|наб\.?|набережн|пер\.?|переулок|пл\.?|площадь|"
    r"б-?р|бульвар|аллея|шоссе|д\.)\b",
    re.IGNORECASE | re.UNICODE,
)


def _poster_anchor_date(year_anchor: date, day: int, month: int) -> date | None:
    candidate = _safe_construct_date(year_anchor.year, month, day)
    if not candidate:
        return None
    # Normal yearly rollover: only jump a year when the date is not just a
    # nearby recent-past mention around the crawl date.
    if candidate < year_anchor and (year_anchor - candidate) > RECENT_PAST_THRESHOLD:
        candidate = _safe_construct_date(year_anchor.year + 1, month, day)
    return candidate


def _clean_poster_line(line: str | None) -> str:
    raw = unicodedata.normalize("NFKC", str(line or "")).strip()
    raw = raw.strip("•·-–—|: ")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _looks_like_poster_address_line(line: str | None) -> bool:
    raw = _clean_poster_line(line)
    return bool(raw and _POSTER_ADDRESS_RE.search(raw) and re.search(r"\d", raw))


def _looks_like_poster_venue_line(line: str | None) -> bool:
    raw = _clean_poster_line(line)
    if not raw or _looks_like_poster_address_line(raw):
        return False
    if re.search(r"\d{1,2}\s+[а-яё.]+\s+\d{1,2}[:.]\d{2}", raw, re.IGNORECASE):
        return False
    return bool(_POSTER_VENUE_RE.search(raw))


def _infer_poster_venue_address(lines: Sequence[str], *, date_line_index: int) -> tuple[str | None, str | None]:
    venue: str | None = None
    address: str | None = None

    # Prefer the local block after the date/time line: event posters typically
    # place title/speaker first and venue/address near the bottom.
    scan_lines = list(lines[date_line_index + 1 : date_line_index + 8]) + list(lines[:date_line_index])
    for idx, line in enumerate(scan_lines):
        clean = _clean_poster_line(line)
        if not clean:
            continue
        if venue is None and _looks_like_poster_venue_line(clean):
            venue = clean
            for follow in scan_lines[idx + 1 : idx + 4]:
                if _looks_like_poster_address_line(follow):
                    address = _clean_poster_line(follow)
                    break
        if address is None and _looks_like_poster_address_line(clean):
            address = clean
        if venue and address:
            break
    return venue, address


def _extract_single_poster_datetime_anchor(
    poster_texts: Sequence[str] | None,
    *,
    anchor_date: date,
) -> PosterDatetimeAnchor | None:
    anchors: list[PosterDatetimeAnchor] = []
    seen: set[tuple[str, str]] = set()
    for text in poster_texts or []:
        raw = str(text or "")
        if not raw.strip():
            continue
        lines = [_clean_poster_line(line) for line in raw.splitlines()]
        for idx, line in enumerate(lines):
            for match in _POSTER_EXACT_DATETIME_RE.finditer(line):
                month = MONTHS_RU.get((match.group("month") or "").casefold().replace("ё", "е").strip("."))
                if not month:
                    continue
                try:
                    day = int(match.group("day"))
                    hour = int(match.group("hour"))
                    minute = int(match.group("minute"))
                except Exception:
                    continue
                dt = _poster_anchor_date(anchor_date, day, int(month))
                if not dt:
                    continue
                time_value = f"{hour:02d}:{minute:02d}"
                key = (dt.isoformat(), time_value)
                if key in seen:
                    continue
                seen.add(key)
                venue, address = _infer_poster_venue_address(lines, date_line_index=idx)
                anchors.append(
                    PosterDatetimeAnchor(
                        date=dt.isoformat(),
                        time=time_value,
                        venue=venue,
                        address=address,
                    )
                )
    if len(anchors) != 1:
        return None
    return anchors[0]


def _source_text_has_relative_date_anchor(text: str | None) -> bool:
    return bool(_RELATIVE_TEXT_DATE_RE.search(str(text or "")))


def _source_text_has_absolute_date_anchor(text: str | None) -> bool:
    raw = str(text or "")
    if not raw.strip():
        return False
    return bool(_DAY_MONTH_NUM_RE.search(raw) or _DAY_MONTH_WORD_RE.search(raw))


def match_keywords(text: str) -> tuple[bool, list[str]]:
    """Return True and list of matched keywords or pricing hints."""

    text_low = text.lower()
    price_matches = [m.group(0).strip() for m in PRICE_RE.finditer(text_low)]

    if VK_USE_PYMORPHY and MORPH:
        tokens = re.findall(r"\w+", text_low)
        matched: list[str] = []
        lemmas: list[str] = []
        for t in tokens:
            lemma = MORPH.parse(t)[0].normal_form
            lemmas.append(lemma)
            if lemma in KEYWORD_LEMMAS and lemma not in matched:
                matched.append(lemma)
        for idx, (first, second) in enumerate(zip(lemmas, lemmas[1:])):
            if first == "живой" and second == "звук":
                if "живой звук" not in matched:
                    matched.append("живой звук")
            if first == "пушкинский" and second == "карта":
                phrase = f"{tokens[idx]} {tokens[idx + 1]}"
                if phrase not in matched:
                    matched.append(phrase)
        for m in GROUP_CONTEXT_RE.finditer(text):
            group_match = m.group(0).lower()
            if group_match and group_match not in matched:
                matched.append(group_match)
        for m in GROUP_NAME_RE.finditer(text):
            group_match = m.group(0).lower()
            if group_match and group_match not in matched:
                matched.append(group_match)
        for hint in price_matches:
            if hint and hint not in matched:
                matched.append(hint)
        return bool(matched), matched

    matched = [m.group(0).lower().lstrip("#") for m in KEYWORD_RE.finditer(text_low)]
    for m in GROUP_CONTEXT_RE.finditer(text):
        group_match = m.group(0).lower()
        if group_match and group_match not in matched:
            matched.append(group_match)
    for m in GROUP_NAME_RE.finditer(text):
        group_match = m.group(0).lower()
        if group_match and group_match not in matched:
            matched.append(group_match)
    for hint in price_matches:
        if hint and hint not in matched:
            matched.append(hint)
    return bool(matched), matched


def detect_date(text: str) -> bool:
    """Heuristically detect a date or time mention in the text."""
    return any(
        COMPILED_DATE_PATTERNS[index].search(text)
        for index in DATE_PATTERN_STRONG_INDEXES
    )


def detect_historical_context(text: str) -> bool:
    """Return True if text mentions a pre-1995 year or historical toponyms."""

    text_low = text.lower()
    for match in HISTORICAL_YEAR_RE.findall(text_low):
        try:
            year = int(match)
        except ValueError:
            continue
        if year <= 1994:
            return True
    return any(name in text_low for name in HISTORICAL_TOPONYMS)


_VK_LLM_RESCUE_INVITE_RE = re.compile(
    r"(?iu)\b("
    r"приглаша(?:ем|ет|ют)|"
    r"состо(?:ится|ятся)|"
    r"пройд(?:е|ё)т|"
    r"регистрац\w*|"
    r"по\s+регистрации|"
    r"билет\w*|"
    r"встреч[аеуы]|"
    r"лекци[яюи]|"
    r"мастер-?класс\w*|"
    r"экскурси[яюи]|"
    r"фестивал\w*"
    r")\b"
)
_VK_LLM_RESCUE_PLACE_RE = re.compile(
    r"(?iu)\b("
    r"зал|лектори[йя]|музе[йя]|библиотек\w*|галере[яи]|театр\w*|"
    r"дк|дом\s+культур\w*|ул\.|улиц[аеы]|проспект|пр-т|набережн\w*|"
    r"этаж|кабинет|аудитори[яи]"
    r")\b"
)


def _vk_should_rescue_to_llm_without_ts_hint(text: str) -> bool:
    """Fail open into the normal LLM parser for event-like VK posts.

    This does not extract or create event semantics. It only prevents crawl-time
    deterministic date-hint uncertainty from dropping posts that have strong
    invite/registration/offline-place signals; the downstream VK import remains
    LLM-first and may still reject the post.
    """

    clean = (text or "").strip()
    if len(clean) < 40:
        return False
    if not detect_date(clean):
        return False
    if not _VK_LLM_RESCUE_INVITE_RE.search(clean):
        return False
    if not _VK_LLM_RESCUE_PLACE_RE.search(clean):
        return False
    return True


@dataclass(frozen=True)
class VKCrawlAdmissionCandidate:
    group_id: int
    post_id: int
    source_url: str
    published_at: int
    text: str
    keyword_hints: tuple[str, ...]
    date_hints: tuple[str, ...]
    event_ts_hint: int | None
    visual_evidence_count: int
    source_revision_hash: str

    @property
    def key(self) -> str:
        return f"{self.group_id}:{self.post_id}"


@dataclass(frozen=True)
class VKCrawlAdmissionDecision:
    admitted: bool
    outcome: str
    reason: str
    route: str
    confidence: float | None = None
    evidence_quote: str | None = None
    prompt_version: str | None = None
    model: str | None = None

    def receipt(self) -> dict[str, Any]:
        return {
            "schema": VK_CRAWL_ADMISSION_PROMPT_VERSION,
            "admitted": bool(self.admitted),
            "outcome": self.outcome,
            "reason": self.reason,
            "route": self.route,
            "confidence": self.confidence,
            "evidence_quote": self.evidence_quote,
            "prompt_version": self.prompt_version,
            "model": self.model,
        }


def _vk_source_revision_hash_for_post(post: Mapping[str, Any]) -> str:
    from vk_source_envelope import is_vk_source_envelope, vk_source_packet_hashes

    if is_vk_source_envelope(post):
        return vk_source_packet_hashes(post)[1]
    canonical = _vk_packet_json(_vk_source_revision_payload(dict(post)))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _vk_visual_evidence_count(post: Mapping[str, Any]) -> int:
    counts = post.get("counts")
    if isinstance(counts, Mapping):
        try:
            return max(0, int(counts.get("visual_candidate_count") or 0))
        except (TypeError, ValueError):
            pass
    photos = post.get("photos")
    if isinstance(photos, Sequence) and not isinstance(
        photos, (str, bytes, bytearray)
    ):
        return len(photos)
    return 0


def _vk_crawl_admission_candidate(
    *,
    group_id: int,
    owner_type: str,
    post: Mapping[str, Any],
    default_time: str | None,
    tz: timezone,
) -> VKCrawlAdmissionCandidate:
    from vk_owner import vk_wall_url

    post_id = int(post["post_id"])
    published_at = int(post["date"])
    text = str(post.get("text") or "")
    source_url = str(
        post.get("url") or vk_wall_url(group_id, post_id, owner_type)
    )
    history_hit = detect_historical_context(text)
    kw_ok, keywords = match_keywords(text)
    has_date = detect_date(text)
    keyword_hints = list(dict.fromkeys(str(item) for item in keywords if item))
    if history_hit:
        keyword_hints.append(HISTORY_MATCHED_KEYWORD)
    if not kw_ok:
        keyword_hints.append("hint:no_keywords")
    if not has_date:
        keyword_hints.append("hint:no_date")
    if not text.strip() and _vk_visual_evidence_count(post):
        keyword_hints.append(OCR_PENDING_SENTINEL)
    date_hints = [
        match.group(0)
        for pattern in (DATE_RANGE_RE, NUM_DATE_RE, MONTH_NAME_RE)
        for match in pattern.finditer(text)
    ]
    event_ts_hint = extract_event_ts_hint(
        text,
        default_time,
        publish_ts=published_at,
        tz=tz,
    )
    now_priority = int(time.time())
    if event_ts_hint is None and has_date:
        past_probe = extract_event_ts_hint(
            text,
            default_time,
            publish_ts=published_at,
            allow_past=True,
            tz=tz,
        )
        if past_probe is not None and past_probe < now_priority + 2 * 3600:
            keyword_hints.append("hint:past_event")
    if event_ts_hint is not None and event_ts_hint < now_priority + 2 * 3600:
        keyword_hints.append("hint:past_event")
    if event_ts_hint is not None and event_ts_hint > now_priority + 2 * 365 * 86400:
        keyword_hints.append("hint:too_far")
    return VKCrawlAdmissionCandidate(
        group_id=int(group_id),
        post_id=post_id,
        source_url=source_url,
        published_at=published_at,
        text=text,
        keyword_hints=tuple(dict.fromkeys(keyword_hints)),
        date_hints=tuple(dict.fromkeys(date_hints)),
        event_ts_hint=event_ts_hint,
        visual_evidence_count=_vk_visual_evidence_count(post),
        source_revision_hash=_vk_source_revision_hash_for_post(post),
    )


def _vk_crawl_deterministic_admission(
    candidate: VKCrawlAdmissionCandidate,
) -> VKCrawlAdmissionDecision | None:
    """Admit only high-recall deterministic positives; never reject semantics.

    A failed/ambiguous deterministic probe is deliberately returned as ``None``
    and must be adjudicated by the small LLM gate.  This keeps crawl and the
    later bounded auto-import as separate stages while preventing obvious
    non-events from entering the expensive queue.
    """

    hints = set(candidate.keyword_hints)
    if not candidate.text.strip() and candidate.visual_evidence_count:
        return VKCrawlAdmissionDecision(
            admitted=True,
            outcome="ADMIT",
            reason="visual_evidence_requires_ocr",
            route="deterministic",
        )
    has_keyword = "hint:no_keywords" not in hints
    has_date = "hint:no_date" not in hints
    future_hint = candidate.event_ts_hint
    now_priority = int(time.time())
    if (
        has_keyword
        and has_date
        and future_hint is not None
        and future_hint >= now_priority + 2 * 3600
        and future_hint <= now_priority + 2 * 365 * 86400
    ):
        return VKCrawlAdmissionDecision(
            admitted=True,
            outcome="ADMIT",
            reason="deterministic_future_event",
            route="deterministic",
        )
    return None


def _get_vk_crawl_admission_client() -> Any | None:
    try:
        return require_main_attr("_get_event_parse_gemma_client")()
    except Exception:
        logger.warning("vk.crawl.admission client unavailable", exc_info=True)
        return None


def _vk_admission_compact_text(text: str, limit: int = 3600) -> str:
    clean = unicodedata.normalize("NFKC", str(text or "")).strip()
    if len(clean) <= limit:
        return clean
    head = max(1, int(limit * 0.7))
    tail = max(1, limit - head)
    return f"{clean[:head]}\n[…середина сокращена…]\n{clean[-tail:]}"


def _vk_admission_quote_is_grounded(quote: str, text: str) -> bool:
    def norm(value: str) -> str:
        return re.sub(
            r"\s+",
            " ",
            unicodedata.normalize("NFKC", str(value or "")).casefold(),
        ).strip(" \t\r\n.,;:!?—–-«»\"'")

    normalized_quote = norm(quote)
    return len(normalized_quote) >= 8 and normalized_quote in norm(text)


def _vk_admission_fail_open(reason: str) -> VKCrawlAdmissionDecision:
    return VKCrawlAdmissionDecision(
        admitted=True,
        outcome="UNCERTAIN",
        reason=f"fail_open_{reason}",
        route="fail_open",
        prompt_version=VK_CRAWL_ADMISSION_PROMPT_VERSION,
        model=VK_CRAWL_ADMISSION_MODEL,
    )


async def _load_existing_vk_crawl_admissions(
    db: Database,
    candidates: Sequence[VKCrawlAdmissionCandidate],
    *,
    reclassify_legacy_pending: bool = False,
) -> dict[str, VKCrawlAdmissionDecision]:
    result: dict[str, VKCrawlAdmissionDecision] = {}
    if not candidates:
        return result
    async with db.raw_conn() as conn:
        for candidate in candidates:
            row = await (await conn.execute(
                """
                SELECT packet.admission_status,packet.admission_reason,
                       packet.admission_receipt_json,inbox.status
                FROM vk_source_packet AS packet
                LEFT JOIN vk_inbox AS inbox ON inbox.source_packet_id=packet.id
                WHERE packet.source_type='vk' AND packet.owner_id=? AND packet.post_id=?
                  AND packet.source_revision_hash=?
                LIMIT 1
                """,
                (
                    candidate.group_id,
                    candidate.post_id,
                    candidate.source_revision_hash,
                ),
            )).fetchone()
            if not row:
                continue
            admission_status = str(row[0] or "")
            inbox_status = str(row[3] or "")
            if admission_status not in {"admitted", "rejected"}:
                if inbox_status == "pending" and not reclassify_legacy_pending:
                    result[candidate.key] = VKCrawlAdmissionDecision(
                        admitted=True,
                        outcome="ADMIT",
                        reason="legacy_pending_preserved",
                        route="existing_receipt",
                    )
                    continue
                if inbox_status in {"imported", "rejected", "failed"}:
                    rejected = inbox_status in {"rejected", "failed"}
                    result[candidate.key] = VKCrawlAdmissionDecision(
                        admitted=not rejected,
                        outcome="NON_EVENT" if rejected else "ADMIT",
                        reason="legacy_terminal_preserved",
                        route="existing_receipt",
                    )
                continue
            try:
                receipt = json.loads(str(row[2] or "{}"))
            except Exception:
                continue
            if not isinstance(receipt, dict):
                continue
            result[candidate.key] = VKCrawlAdmissionDecision(
                admitted=str(row[0]) == "admitted",
                outcome=str(receipt.get("outcome") or "UNCERTAIN"),
                reason=str(row[1] or receipt.get("reason") or "existing_receipt"),
                route="existing_receipt",
                confidence=(
                    float(receipt["confidence"])
                    if receipt.get("confidence") is not None
                    else None
                ),
                evidence_quote=(
                    str(receipt["evidence_quote"])
                    if receipt.get("evidence_quote")
                    else None
                ),
                prompt_version=str(receipt.get("prompt_version") or "") or None,
                model=str(receipt.get("model") or "") or None,
            )
    return result


async def _call_vk_crawl_admission_llm(
    candidates: Sequence[VKCrawlAdmissionCandidate],
    *,
    tz: timezone,
) -> dict[str, VKCrawlAdmissionDecision]:
    if not candidates:
        return {}
    client = _get_vk_crawl_admission_client()
    if client is None:
        raise RuntimeError("admission_client_unavailable")
    payload = {
        "now": datetime.now(tz).isoformat(),
        "timezone": str(tz),
        "posts": [
            {
                "id": item.key,
                "published_at": datetime.fromtimestamp(
                    item.published_at, tz
                ).isoformat(),
                "text": _vk_admission_compact_text(item.text),
                "deterministic_hints": list(item.keyword_hints),
                "date_hints": list(item.date_hints),
                "event_ts_hint": item.event_ts_hint,
                "visual_attachments_not_inspected": item.visual_evidence_count,
            }
            for item in candidates
        ],
    }
    prompt = (
        "Ты выполняешь ТОЛЬКО первичный admission VK-постов перед очередью разбора. "
        "Не извлекай события и не создавай их. Для каждого posts.id выбери ровно один outcome:\n"
        "ADMIT — пост содержит или обновляет/отменяет хотя бы одно посещаемое будущее либо "
        "продолжающееся событие; ONGOING выставки с будущей датой закрытия тоже ADMIT.\n"
        "PAST_ONLY — все конкретные посещаемые события уже закончились и будущего приглашения нет.\n"
        "NON_EVENT — это точно не анонс/обновление посещаемого события.\n"
        "UNCERTAIN — данных недостаточно. Если visual_attachments_not_inspected > 0 и текст сам "
        "не доказывает решение, обязательно UNCERTAIN: содержимое афиши тебе не показано. "
        "Онлайн/другой регион сами по себе не повод отклонять: продуктовый scope решит поздний parser. "
        "Рекап с отдельным будущим приглашением — ADMIT. Сообщение об отмене/переносе будущего "
        "события — ADMIT. Для PAST_ONLY/NON_EVENT дай дословную evidence_quote из text и confidence; "
        "не выдумывай цитату. Верни только JSON: "
        '{"decisions":[{"id":"1:2","outcome":"ADMIT|PAST_ONLY|NON_EVENT|UNCERTAIN",'
        '"confidence":0.0,"evidence_quote":"...","reason":"short_code"}]}.\n'
        f"INPUT:\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )
    try:
        timeout = float(os.getenv("VK_CRAWL_ADMISSION_TIMEOUT_SEC", "75") or "75")
    except (TypeError, ValueError):
        timeout = 75.0
    raw, _usage = await asyncio.wait_for(
        client.generate_content_async(
            model=VK_CRAWL_ADMISSION_MODEL,
            prompt=prompt,
            generation_config={
                "temperature": 0,
                "response_mime_type": "application/json",
                "response_json_schema": {
                    "type": "object",
                    "properties": {
                        "decisions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"},
                                    "outcome": {
                                        "type": "string",
                                        "enum": [
                                            "ADMIT",
                                            "PAST_ONLY",
                                            "NON_EVENT",
                                            "UNCERTAIN",
                                        ],
                                    },
                                    "confidence": {"type": "number"},
                                    "evidence_quote": {"type": "string"},
                                    "reason": {"type": "string"},
                                },
                                "required": [
                                    "id",
                                    "outcome",
                                    "confidence",
                                    "evidence_quote",
                                    "reason",
                                ],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["decisions"],
                    "additionalProperties": False,
                },
            },
            max_output_tokens=max(500, 180 * len(candidates)),
            use_provider_count_tokens=True,
            prompt_version=VK_CRAWL_ADMISSION_PROMPT_VERSION,
            max_provider_attempts=1,
            allow_model_fallback=False,
        ),
        timeout=max(10.0, min(timeout, 180.0)),
    )
    extract_json = require_main_attr("_event_parse_extract_json")
    decoded = extract_json(str(raw or ""))
    if not isinstance(decoded, dict) or not isinstance(decoded.get("decisions"), list):
        raise ValueError("admission_schema_invalid")
    by_key = {item.key: item for item in candidates}
    resolved: dict[str, VKCrawlAdmissionDecision] = {}
    for raw_item in decoded["decisions"]:
        if not isinstance(raw_item, Mapping):
            continue
        key = str(raw_item.get("id") or "")
        candidate = by_key.get(key)
        if candidate is None or key in resolved:
            continue
        outcome = str(raw_item.get("outcome") or "").strip().upper()
        try:
            confidence = float(raw_item.get("confidence"))
        except (TypeError, ValueError):
            confidence = 0.0
        quote = str(raw_item.get("evidence_quote") or "").strip()
        reason = str(raw_item.get("reason") or "").strip().casefold()
        reason = re.sub(r"[^a-z0-9_:-]+", "_", reason).strip("_")[:80]
        if outcome in {"PAST_ONLY", "NON_EVENT"}:
            grounded = _vk_admission_quote_is_grounded(quote, candidate.text)
            # The prompt requires UNCERTAIN whenever an uninspected attachment
            # could change the verdict.  Do not override a grounded, high-
            # confidence text verdict merely because the post also has media:
            # that blanket override kept almost every ordinary VK photo post in
            # the expensive queue.  Ambiguous visual carriers still fail open
            # because the model must return UNCERTAIN for them.
            if confidence >= 0.90 and grounded:
                resolved[key] = VKCrawlAdmissionDecision(
                    admitted=False,
                    outcome=outcome,
                    reason=(
                        "llm_past_only" if outcome == "PAST_ONLY" else "llm_non_event"
                    ),
                    route="llm",
                    confidence=confidence,
                    evidence_quote=quote,
                    prompt_version=VK_CRAWL_ADMISSION_PROMPT_VERSION,
                    model=VK_CRAWL_ADMISSION_MODEL,
                )
            else:
                resolved[key] = _vk_admission_fail_open(
                    "ungrounded_or_low_confidence"
                )
        elif outcome == "ADMIT":
            resolved[key] = VKCrawlAdmissionDecision(
                admitted=True,
                outcome="ADMIT",
                reason="llm_future_or_ongoing_event",
                route="llm",
                confidence=confidence,
                evidence_quote=quote or None,
                prompt_version=VK_CRAWL_ADMISSION_PROMPT_VERSION,
                model=VK_CRAWL_ADMISSION_MODEL,
            )
        elif outcome == "UNCERTAIN":
            resolved[key] = _vk_admission_fail_open("llm_uncertain")
        else:
            resolved[key] = _vk_admission_fail_open("invalid_outcome")
    return resolved


async def _resolve_vk_crawl_admissions(
    db: Database,
    candidates: Sequence[VKCrawlAdmissionCandidate],
    *,
    tz: timezone,
    reclassify_legacy_pending: bool = False,
) -> dict[str, VKCrawlAdmissionDecision]:
    decisions = await _load_existing_vk_crawl_admissions(
        db,
        candidates,
        reclassify_legacy_pending=reclassify_legacy_pending,
    )
    pending: list[VKCrawlAdmissionCandidate] = []
    for candidate in candidates:
        if candidate.key in decisions:
            continue
        deterministic = _vk_crawl_deterministic_admission(candidate)
        if deterministic is not None:
            decisions[candidate.key] = deterministic
        else:
            pending.append(candidate)
    for offset in range(0, len(pending), VK_CRAWL_ADMISSION_BATCH_SIZE):
        chunk = pending[offset : offset + VK_CRAWL_ADMISSION_BATCH_SIZE]
        try:
            resolved = await _call_vk_crawl_admission_llm(chunk, tz=tz)
        except Exception as exc:
            logger.warning(
                "vk.crawl.admission fail_open posts=%s error=%s",
                [item.key for item in chunk],
                type(exc).__name__,
                exc_info=True,
            )
            resolved = {}
        for candidate in chunk:
            decisions[candidate.key] = resolved.get(candidate.key) or _vk_admission_fail_open(
                "provider_or_schema_failure"
            )
    return decisions


async def requalify_vk_inbox_admission(
    db: Database,
    *,
    limit: int = 100,
    newest_first: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Apply the crawl admission contract to legacy selectable inbox rows.

    This is a bounded one-off recovery entrypoint for the August raw-first
    backlog.  It does not parse or create events.  Admitted/fail-open rows stay
    ``pending`` for the separately scheduled auto-import; only grounded LLM
    PAST_ONLY/NON_EVENT rows leave that queue as ``rejected``.
    """

    bounded_limit = max(1, min(int(limit), 500))
    get_tz_offset = require_main_attr("get_tz_offset")
    await get_tz_offset(db)
    local_tz = require_main_attr("LOCAL_TZ")
    order = "DESC" if newest_first else "ASC"
    async with db.raw_conn() as conn:
        rows = await (await conn.execute(
            f"""
            SELECT inbox.id,inbox.group_id,inbox.owner_type,inbox.source_packet_id,
                   packet.raw_payload_json,source.default_time
            FROM vk_inbox AS inbox
            JOIN vk_source_packet AS packet ON packet.id=inbox.source_packet_id
            LEFT JOIN vk_source AS source ON source.group_id=inbox.group_id
            WHERE inbox.status='pending'
              AND inbox.locked_by IS NULL
              AND COALESCE(packet.admission_status,'legacy_unclassified')='legacy_unclassified'
            ORDER BY inbox.date {order},inbox.id {order}
            LIMIT ?
            """,
            (bounded_limit,),
        )).fetchall()
    prepared: list[tuple[int, int, str, int, dict[str, Any], str | None]] = []
    invalid_rows: list[int] = []
    for inbox_id, group_id, owner_type, packet_id, raw_payload_json, default_time in rows:
        try:
            post = json.loads(str(raw_payload_json or ""))
        except Exception:
            post = None
        if not isinstance(post, dict):
            invalid_rows.append(int(inbox_id))
            continue
        prepared.append(
            (
                int(inbox_id),
                int(group_id),
                str(owner_type or "group"),
                int(packet_id),
                post,
                str(default_time) if default_time else None,
            )
        )
    candidates = [
        _vk_crawl_admission_candidate(
            group_id=group_id,
            owner_type=owner_type,
            post=post,
            default_time=default_time,
            tz=local_tz,
        )
        for _inbox_id, group_id, owner_type, _packet_id, post, default_time in prepared
    ]
    decisions = await _resolve_vk_crawl_admissions(
        db,
        candidates,
        tz=local_tz,
        reclassify_legacy_pending=True,
    )
    stats: dict[str, Any] = {
        "selected": len(rows),
        "classified": len(candidates),
        "admitted": 0,
        "rejected": 0,
        "llm_checked": 0,
        "fail_open": 0,
        "invalid_source_packets": invalid_rows,
        "dry_run": bool(dry_run),
        "remaining_legacy_pending": 0,
    }
    for prepared_row, candidate in zip(prepared, candidates):
        _inbox_id, group_id, owner_type, _packet_id, post, _default_time = prepared_row
        decision = decisions[candidate.key]
        if decision.admitted:
            stats["admitted"] += 1
        else:
            stats["rejected"] += 1
        if decision.route in {"llm", "fail_open"}:
            stats["llm_checked"] += 1
        if decision.route == "fail_open":
            stats["fail_open"] += 1
        if dry_run:
            continue
        await _persist_vk_source_packet(
            db,
            group_id=group_id,
            owner_type=owner_type,
            post=post,
            source_url=candidate.source_url,
            keyword_hints=candidate.keyword_hints,
            date_hints=candidate.date_hints,
            event_ts_hint=candidate.event_ts_hint,
            admission_decision=decision,
            apply_admission_gate=True,
        )
    async with db.raw_conn() as conn:
        remaining = await (await conn.execute(
            """
            SELECT COUNT(*)
            FROM vk_inbox AS inbox
            JOIN vk_source_packet AS packet ON packet.id=inbox.source_packet_id
            WHERE inbox.status='pending'
              AND COALESCE(packet.admission_status,'legacy_unclassified')='legacy_unclassified'
            """
        )).fetchone()
    stats["remaining_legacy_pending"] = int((remaining[0] if remaining else 0) or 0)
    return stats


def normalize_phone_candidates(text: str) -> str:
    """Strip separators from phone-like sequences without touching valid dates."""

    date_intervals: list[tuple[int, int, str]] = []

    def _collect_intervals(pattern: re.Pattern[str], kind: str) -> None:
        for match in pattern.finditer(text):
            date_intervals.append((match.start(), match.end(), kind))

    _collect_intervals(DATE_RANGE_RE, "date_range")
    _collect_intervals(NUM_DATE_RE, "num_date")
    _collect_intervals(MONTH_NAME_RE, "month_name")

    phone_spans: list[tuple[int, int]] = [
        (m.start(), m.end()) for m in PHONE_CANDIDATE_RE.finditer(text)
    ]

    filtered_intervals: list[tuple[int, int]] = []
    for start, end, kind in date_intervals:
        skip_interval = False
        for p_start, p_end in phone_spans:
            if p_start <= start and end <= p_end:
                if p_start < start or end < p_end:
                    # Month-name dates ("16 мая 2026 г. в 16:00") can be swallowed by
                    # broad phone-like spans because PHONE_CANDIDATE_RE allows arbitrary
                    # non-digits between digit groups. Keep the date token intact; the
                    # semantic event decision still belongs to the LLM parser downstream.
                    if kind == "month_name":
                        mon_match = MONTH_NAME_RE.match(text[start:end])
                        mon_word = (mon_match.group(2).rstrip(".") if mon_match else "")
                        if MONTHS_RU.get(mon_word) is not None:
                            continue
                    skip_interval = True
                    break
                context_start = max(0, start - 20)
                context = text[context_start:start]
                if PHONE_CONTEXT_RE.search(context):
                    skip_interval = True
                    break
        if not skip_interval:
            filtered_intervals.append((start, end))

    date_intervals = sorted(filtered_intervals)

    def is_in_date_interval(index: int) -> bool:
        for interval_start, interval_end in date_intervals:
            if interval_end <= index:
                continue
            if interval_start > index:
                break
            return interval_start <= index < interval_end
        return False

    result: List[str] = []
    pos = 0
    separators = set(" +()\t\r\n.-–\u00a0\u202f")
    while True:
        match = PHONE_CANDIDATE_RE.search(text, pos)
        if not match:
            break
        start = match.start()
        result.append(text[pos:start])
        original = match.group(0)
        trimmed_end = 0
        for rel_idx, ch in enumerate(original):
            if ch.isdigit() or ch in separators:
                trimmed_end = rel_idx + 1
            else:
                break
        trimmed = original[:trimmed_end]
        if trimmed_end:
            normalized_chars: list[str] = []
            for rel_idx, ch in enumerate(trimmed):
                if ch.isdigit():
                    absolute_idx = start + rel_idx
                    if is_in_date_interval(absolute_idx):
                        normalized_chars.append(ch)
                    else:
                        normalized_chars.append("x")
                else:
                    normalized_chars.append(ch)
            result.append("".join(normalized_chars))
        else:
            result.append(trimmed)
        pos = start + trimmed_end
    result.append(text[pos:])
    return "".join(result)


def extract_event_ts_hint(
    text: str,
    default_time: str | None = None,
    *,
    tz: timezone | None = None,
    publish_ts: datetime | int | float | None = None,
    allow_past: bool = False,
) -> int | None:
    """Return Unix timestamp for the nearest future datetime mentioned in text."""
    tzinfo = tz or require_main_attr("LOCAL_TZ")

    if publish_ts is None:
        now = datetime.now(tzinfo)
    elif isinstance(publish_ts, datetime):
        if publish_ts.tzinfo is None:
            now = publish_ts.replace(tzinfo=tzinfo)
        else:
            now = publish_ts.astimezone(tzinfo)
    else:
        now = datetime.fromtimestamp(publish_ts, tzinfo)
    raw_text_low = text.lower()
    text_low = normalize_phone_candidates(raw_text_low)

    day = month = year = None
    m = None
    date_span: tuple[int, int] | None = None
    for candidate in NUM_DATE_RE.finditer(text_low):
        start = candidate.start()
        # A compact alphanumeric/product token such as ``ГАЗ-24-10`` is an
        # identifier, not a calendar anchor.  This only protects the queue hint
        # parser; the LLM still owns the event date extracted from source text.
        if start > 0 and text_low[start - 1] in "./-":
            before_sep = text_low[: start - 1].rstrip()
            if before_sep and before_sep[-1].isalpha():
                continue
        prev_idx = start - 1
        while prev_idx >= 0 and text_low[prev_idx].isspace():
            prev_idx -= 1
        if prev_idx >= 0 and text_low[prev_idx] in "./-":
            digit_count = 0
            check_idx = prev_idx - 1
            while check_idx >= 0 and text_low[check_idx].isdigit():
                digit_count += 1
                check_idx -= 1
            if digit_count >= 3:
                continue
        trailing_chars = " \t\r\n.;:!?()[]{}«»\"'—–-"
        trailing_idx = candidate.end()
        while trailing_idx < len(text_low) and text_low[trailing_idx] in trailing_chars:
            trailing_idx += 1
        if trailing_idx < len(text_low):
            raw_remainder = raw_text_low[trailing_idx:]
            trimmed_remainder = raw_remainder.lstrip(trailing_chars)
            if trimmed_remainder and trimmed_remainder[0].isdigit():
                continue
        remainder = text_low[trailing_idx:] if trailing_idx < len(text_low) else ""

        if PHONE_LIKE_RE.match(candidate.group(0)):
            context_start = max(0, start - 30)
            context_end = min(len(text_low), candidate.end() + 10)
            context_slice = text_low[context_start:context_end]
            skip_candidate = False
            has_event_tail = False
            next_alpha_word = None
            following_is_phone_tail = False
            skip_due_to_action_tail = False
            skip_due_to_location_tail = False
            if trailing_idx < len(text_low):
                word_match = re.match(r"[a-zа-яё]+", remainder)
                if word_match:
                    next_alpha_word = word_match.group(0)
                    if PHONE_CONTEXT_RE.match(next_alpha_word):
                        following_is_phone_tail = True
                if PHONE_CONTEXT_RE.match(remainder):
                    following_is_phone_tail = True
                if not following_is_phone_tail:
                    def _tail_has_datetime(segment: str) -> bool:
                        return bool(
                            NUM_DATE_RE.search(segment)
                            or DATE_RANGE_RE.search(segment)
                            or TIME_RE.search(segment)
                            or TIME_H_RE.search(segment)
                            or TIME_RANGE_RE.search(segment)
                            or MONTH_NAME_RE.search(segment)
                        )

                    if TIME_RE.match(remainder) or TIME_H_RE.match(remainder) or TIME_RANGE_RE.match(remainder):
                        has_event_tail = True
                    elif DOW_RE.match(remainder):
                        has_event_tail = True
                    else:
                        if remainder.startswith("по адресу"):
                            after_location = remainder[len("по адресу") :]
                            after_location = after_location.lstrip(
                                " \t\r\n.;:!?()[]{}«»\"'—–-"
                            )
                            if _tail_has_datetime(after_location):
                                skip_due_to_location_tail = True
                        elif next_alpha_word and next_alpha_word.startswith(
                            EVENT_ADDRESS_PREFIXES
                        ):
                            address_tail = remainder[len(next_alpha_word) :]
                            address_tail = address_tail.lstrip(
                                " \t\r\n.;:!?()[]{}«»\"'—–-"
                            )
                            if _tail_has_datetime(address_tail):
                                has_event_tail = True
                                skip_due_to_location_tail = True
                        else:
                            loc_match = re.match(r"(?:в|на)\s+([a-zа-яё.]+)", remainder)
                            if loc_match:
                                loc_word = loc_match.group(1).strip(".")
                                if loc_word.startswith(EVENT_LOCATION_PREFIXES):
                                    after_location = remainder[loc_match.end() :]
                                    after_location = after_location.lstrip(
                                        " \t\r\n.;:!?()[]{}«»\"'—–-"
                                    )
                                    if _tail_has_datetime(after_location):
                                        skip_due_to_location_tail = True
                        if (
                            not has_event_tail
                            and next_alpha_word
                            and next_alpha_word.startswith(EVENT_ACTION_PREFIXES)
                        ):
                            action_tail = remainder[len(next_alpha_word) :]
                            action_tail = action_tail.lstrip(
                                " \t\r\n.;:!?()[]{}«»\"'—–-"
                            )
                            if action_tail:
                                has_action_tail_datetime = bool(
                                    NUM_DATE_RE.search(action_tail)
                                    or DATE_RANGE_RE.search(action_tail)
                                    or TIME_RE.search(action_tail)
                                    or TIME_H_RE.search(action_tail)
                                    or TIME_RANGE_RE.search(action_tail)
                                    or MONTH_NAME_RE.search(action_tail)
                                )
                                if has_action_tail_datetime:
                                    has_event_tail = True
                                    skip_due_to_action_tail = True
            if skip_due_to_action_tail:
                continue
            if skip_due_to_location_tail:
                continue
            if not has_event_tail:
                for phone_match in PHONE_CONTEXT_RE.finditer(context_slice):
                    match_end = context_start + phone_match.end()
                    if match_end <= start:
                        intervening = text_low[match_end:start]
                        if "\n" in intervening or "\r" in intervening:
                            continue
                        trimmed = intervening.strip()
                        if not trimmed:
                            skip_candidate = True
                            break
                        if "," in trimmed:
                            break
                        if re.search(r"[a-zа-яё]", trimmed):
                            break
                        if (
                            re.search(r"\d", trimmed)
                            and re.search(r"[a-zа-яё]", remainder)
                            and not re.search(r"\d", remainder)
                        ):
                            skip_candidate = True
                            continue
                        compact = trimmed.replace(" ", "")
                        compact = re.sub(r"^[.,:;-–—]+", "", compact)
                        if not compact or re.fullmatch(r"[\d()+\-–—]*", compact):
                            skip_candidate = True
                            break
            if skip_candidate:
                continue
        m = candidate
        date_span = candidate.span()
        break

    if m:
        day, month = int(m.group(1)), int(m.group(2))
        if m.group(3):
            y = m.group(3)
            year = int("20" + y if len(y) == 2 else y)
        if date_span is None:
            date_span = m.span()
    else:
        m = DATE_RANGE_RE.search(text_low)
        if m:
            day = int(m.group(1))
            month = int(m.group(3))
            date_span = m.span()
        else:
            # MONTH_NAME_RE is intentionally broad (number + word) to catch
            # "13 февраля", but it also matches unrelated fragments like
            # "3 этаж" or "2 зал". Pick the first match that is a real month.
            for cand in MONTH_NAME_RE.finditer(text_low):
                mon_word = cand.group(2).rstrip(".")
                mon_num = MONTHS_RU.get(mon_word)
                if mon_num is None:
                    continue
                m = cand
                day = int(cand.group(1))
                month = int(mon_num)
                y = re.search(r"\b20\d{2}\b", text_low[cand.end() :])
                if y:
                    year = int(y.group(0))
                date_span = cand.span()
                break

    if day is None or month is None:
        if "сегодня" in text_low:
            dt = now
            idx = text_low.find("сегодня")
            if idx != -1:
                date_span = (idx, idx + len("сегодня"))
        elif "завтра" in text_low:
            dt = now + timedelta(days=1)
            idx = text_low.find("завтра")
            if idx != -1:
                date_span = (idx, idx + len("завтра"))
        elif "послезавтра" in text_low:
            dt = now + timedelta(days=2)
            idx = text_low.find("послезавтра")
            if idx != -1:
                date_span = (idx, idx + len("послезавтра"))
        else:
            dow_matches = list(DOW_RE.finditer(text_low))
            dow_m = None
            for candidate in dow_matches:
                context_start = max(0, candidate.start() - 40)
                context_end = min(len(text_low), candidate.end() + 40)
                context_slice = text_low[context_start:context_end]
                if PAST_EVENT_RE.search(context_slice):
                    continue
                dow_m = candidate
                break
            if dow_m:
                dow_map = {
                    "понедельник": 0,
                    "понед": 0,
                    "пн": 0,
                    "вторник": 1,
                    "вт": 1,
                    "среда": 2,
                    "ср": 2,
                    "четверг": 3,
                    "чт": 3,
                    "пятница": 4,
                    "пт": 4,
                    "суббота": 5,
                    "сб": 5,
                    "воскресенье": 6,
                    "вс": 6,
                }
                key = dow_m.group(1).lower().rstrip(".")
                dow = dow_map.get(key)
                if dow is None:
                    dow = dow_map.get(key[:2])
                days_ahead = (dow - now.weekday()) % 7
                dt = now + timedelta(days=days_ahead)
                date_span = (dow_m.start(), dow_m.end())
            elif dow_matches:
                return None
            elif (weekend_m := WEEKEND_RE.search(text_low)):
                days_ahead = (5 - now.weekday()) % 7
                dt = now + timedelta(days=days_ahead)
                date_span = (weekend_m.start(), weekend_m.end())
            else:
                return None
    else:
        explicit_year = year is not None
        year = year or now.year
        try:
            dt = datetime(year, month, day, tzinfo=tzinfo)
        except ValueError:
            return None
        if dt < now:
            skip_year_rollover = explicit_year
            if not explicit_year and now - dt <= RECENT_PAST_THRESHOLD:
                skip_year_rollover = True
            if not skip_year_rollover:
                try:
                    dt = datetime(year + 1, month, day, tzinfo=tzinfo)
                except ValueError:
                    return None

    tm = TIME_RE.search(text_low)
    if tm:
        hhmm = tm.group(0).replace(".", ":")
        hour, minute = map(int, hhmm.split(":"))
    else:
        tr = TIME_RANGE_RE.search(text_low)
        if tr:
            hour = int(tr.group(1))
            minute = int(tr.group(2) or 0)
        else:
            th = TIME_H_RE.search(text_low)
            if th:
                hour = int(th.group(1))
                minute = 0
            else:
                bare_th = None
                bare_hour_rejected = False
                if date_span is not None:
                    allowed_connector_words = {
                        "в",
                        "к",
                        "ровно",
                        "начало",
                        "начала",
                        "начнем",
                        "начнём",
                        "начнется",
                        "начнётся",
                        "начинаем",
                        "старт",
                        "стартуем",
                        "стартует",
                    }
                    duration_hint_prefixes = ("жив", "длит", "продолж", "програм")

                    for candidate in BARE_TIME_H_RE.finditer(text_low):
                        if candidate.start() < date_span[1]:
                            continue
                        between = text_low[date_span[1] : candidate.start()]
                        if re.search(r"[.!?]", between):
                            continue
                        between_stripped = between.strip()
                        reject_candidate = False
                        if between_stripped:
                            normalized_between = between_stripped
                            normalized_between = re.sub(r"[—–-]", " ", normalized_between)
                            normalized_between = re.sub(r"[,;:]", " ", normalized_between)
                            normalized_between = re.sub(r"\s+", " ", normalized_between).strip()
                            if normalized_between:
                                tokens = normalized_between.split(" ")
                                if any(token not in allowed_connector_words for token in tokens):
                                    reject_candidate = True
                        trailing_segment = text_low[candidate.end() :]
                        trailing_segment = trailing_segment.lstrip(
                            " \t\r\n,.;:!?()[]{}«»\"'—–-"
                        )
                        if trailing_segment:
                            next_word_match = re.match(r"[a-zа-яё]+", trailing_segment)
                            if next_word_match and next_word_match.group(0).startswith(
                                duration_hint_prefixes
                            ):
                                reject_candidate = True
                        if reject_candidate:
                            bare_hour_rejected = True
                            continue
                        bare_th = candidate
                        break
                if bare_th:
                    hour = int(bare_th.group(1))
                    minute = 0
                elif bare_hour_rejected:
                    return None
                elif default_time:
                    try:
                        hour, minute = map(int, default_time.split(":"))
                    except Exception:
                        hour = minute = 0
                else:
                    hour = minute = 0

    dt = dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if dt < now and not allow_past:
        return None
    return int(dt.timestamp())


@dataclass
class EventDraft:
    title: str
    date: str | None = None
    time: str | None = None
    time_is_default: bool = False
    venue: str | None = None
    description: str | None = None
    festival: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_price_min: int | None = None
    ticket_price_max: int | None = None
    event_type: str | None = None
    emoji: str | None = None
    end_date: str | None = None
    is_free: bool = False
    pushkin_card: bool = False
    links: List[str] | None = None
    source_text: str | None = None
    poster_media: list[PosterMedia] = field(default_factory=list)
    allow_raw_photo_fallback: bool = True
    poster_summary: str | None = None
    ocr_tokens_spent: int = 0
    ocr_tokens_remaining: int | None = None
    ocr_limit_notice: str | None = None
    ocr_failed: bool = False
    search_digest: str | None = None
    verification_warnings: list[str] = field(default_factory=list)


class DraftParseResult(list[EventDraft]):
    """Legacy-compatible draft list carrying the typed source verdict."""

    def __init__(
        self,
        drafts: Sequence[EventDraft] | None = None,
        *,
        decision: SourceParseDecision | None = None,
    ) -> None:
        super().__init__(drafts or ())
        self.decision = decision if decision is not None else SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
        )
        self.disposition = self.decision.disposition
        self.lifecycle_actions = self.decision.lifecycle_actions
        self.evidence_manifest = self.decision.evidence_manifest
        self.evidence_complete = self.decision.evidence_complete
        self.parse_version = self.decision.parse_version
        self.retry_reason = self.decision.retry_reason
        self.no_event_reason = self.decision.no_event_reason
        self.enrichment_required = self.decision.enrichment_required

    def to_receipt_payload(self) -> dict[str, Any]:
        return {
            "decision": self.decision.to_payload(),
            "drafts": [
                {
                    key: value
                    for key, value in vars(draft).items()
                    if key not in {"poster_media"}
                }
                for draft in self
            ],
        }

    @classmethod
    def from_receipt_payload(cls, payload: dict[str, Any]) -> "DraftParseResult":
        decision_payload = payload.get("decision") if isinstance(payload, dict) else None
        if not isinstance(decision_payload, dict):
            raise ValueError("missing typed decision receipt")
        manifest_payload = decision_payload.get("evidence_manifest")
        if not isinstance(manifest_payload, dict):
            raise ValueError("missing evidence_manifest in typed decision receipt")
        required_decision_fields = {
            "disposition",
            "events",
            "lifecycle_actions",
            "evidence_complete",
            "parse_version",
        }
        missing_fields = sorted(required_decision_fields - decision_payload.keys())
        if missing_fields:
            if missing_fields == ["disposition"]:
                raise ValueError("missing disposition in typed decision receipt")
            raise ValueError(
                "missing typed decision receipt fields: " + ",".join(missing_fields)
            )
        disposition = decision_payload.get("disposition")
        try:
            typed_disposition = SourceDisposition(str(disposition))
        except ValueError as exc:
            raise ValueError(f"unknown disposition in typed decision receipt: {disposition!r}") from exc
        retry_reason = decision_payload.get("retry_reason")
        if typed_disposition is SourceDisposition.RETRY_REQUIRED and retry_reason is None:
            raise ValueError("missing retry_reason in retry receipt")
        if retry_reason is not None:
            try:
                SourceParseRetryReason(str(retry_reason))
            except ValueError as exc:
                raise ValueError(
                    f"unknown retry_reason in typed decision receipt: {retry_reason!r}"
                ) from exc
        no_event_reason = decision_payload.get("no_event_reason")
        if (
            typed_disposition is SourceDisposition.CONFIRMED_NO_EVENT
            and no_event_reason is None
        ):
            raise ValueError("missing no_event_reason in confirmed no-event receipt")
        if no_event_reason is not None:
            try:
                SourceNoEventReason(str(no_event_reason))
            except ValueError as exc:
                raise ValueError(
                    f"unknown no_event_reason in typed decision receipt: {no_event_reason!r}"
                ) from exc
            if typed_disposition is not SourceDisposition.CONFIRMED_NO_EVENT:
                raise ValueError("no_event_reason is only valid for CONFIRMED_NO_EVENT")
        manifest = EvidenceManifest.from_mapping(manifest_payload)
        events_payload = decision_payload.get("events")
        actions_payload = decision_payload.get("lifecycle_actions")
        if not isinstance(events_payload, list):
            raise ValueError("events is not a list in typed decision receipt")
        if not isinstance(actions_payload, list) or not all(
            isinstance(item, dict) for item in actions_payload
        ):
            raise ValueError("lifecycle_actions is invalid in typed decision receipt")
        actions = tuple(
            LifecycleAction.from_mapping(item)
            for item in actions_payload
        )
        decision = SourceParseDecision(
            events_payload,
            disposition=typed_disposition,
            lifecycle_actions=actions,
            evidence_manifest=manifest,
            evidence_complete=bool(decision_payload.get("evidence_complete", False)),
            parse_version=str(decision_payload.get("parse_version") or PARSE_VERSION),
            retry_reason=retry_reason,
            no_event_reason=no_event_reason,
            festival=decision_payload.get("festival"),
            enrichment_required=bool(decision_payload.get("enrichment_required", False)),
            provider_attempts=decision_payload.get("provider_attempts") or (),
        )
        drafts: list[EventDraft] = []
        allowed = set(EventDraft.__dataclass_fields__)
        drafts_payload = payload.get("drafts")
        if not isinstance(drafts_payload, list) or not all(
            isinstance(item, dict) for item in drafts_payload
        ):
            raise ValueError("drafts is invalid in typed decision receipt")
        for item in drafts_payload:
            drafts.append(EventDraft(**{k: v for k, v in item.items() if k in allowed}))
        decision_events = list(decision.events)
        if len(decision_events) != len(drafts):
            raise ValueError("decision/draft child count mismatch in typed receipt")

        def title_key(value: Any) -> str:
            text = unicodedata.normalize("NFKC", str(value or "")).casefold().strip()
            return re.sub(r"^[^0-9a-zа-я]+", "", text)

        if any(
            not title_key(event.get("title"))
            or title_key(event.get("title")) != title_key(draft.title)
            for event, draft in zip(decision_events, drafts)
        ):
            raise ValueError("decision/draft title mismatch in typed receipt")
        return cls(drafts, decision=decision)


async def _llm_assign_source_posters_to_drafts(
    *,
    source_text: str,
    drafts: Sequence[EventDraft],
    posters: Sequence[PosterMedia],
    score_matrix: Sequence[Sequence[float]],
) -> dict[int, list[int]] | None:
    """Adjudicate a multi-event source's media in one bounded Gemma call.

    Lexical/date scores are retrieval hints only.  The LLM owns the semantic
    decision, including whether one roundup poster is shared by all extracted
    events.  One request is made per multi-event source, never per event/photo.
    """

    if not drafts or not posters:
        return None
    try:
        get_client = require_main_attr("_get_event_parse_gemma_client")
        extract_json = require_main_attr("_event_parse_extract_json")
        client = get_client()
    except Exception:
        logger.warning("vk_intake.media_assignment client unavailable", exc_info=True)
        return None
    if client is None:
        return None
    payload = {
        "source_text": str(source_text or "")[:6000],
        "events": [
            {
                "index": idx,
                "title": draft.title,
                "date": draft.date,
                "time": draft.time,
                "venue": draft.venue,
            }
            for idx, draft in enumerate(drafts[:16])
        ],
        "posters": [
            {
                "index": idx,
                "ocr_title": str(poster.ocr_title or "")[:300],
                "ocr_text": str(poster.ocr_text or "")[:1400],
                "retrieval_scores": list(score_matrix[idx])[:16]
                if idx < len(score_matrix)
                else [],
            }
            for idx, poster in enumerate(posters[:10])
        ],
    }
    prompt = (
        "Ты распределяешь изображения ОДНОГО исходного VK-поста между уже извлечёнными событиями. "
        "Векторы/lexical retrieval_scores — только кандидаты, окончательное смысловое решение твоё.\n"
        "Для каждого poster верни event_indices, к каким событиям он действительно относится. "
        "Если единственное изображение является общей афишей/обложкой roundup-поста и источник явно "
        "перечисляет все events, его можно назначить всем событиям. Если это афиша одного пункта, не "
        "назначай соседним. Если связь не подтверждается source/OCR — оставь event_indices пустым. "
        "Ничего не придумывай. Верни только JSON вида "
        "{\"assignments\":[{\"poster_index\":0,\"event_indices\":[0],\"confidence\":0.9}]}.\n"
        f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    model = (os.getenv("VK_MEDIA_ASSIGN_MODEL", "gemma-4-31b-it") or "").strip() or "gemma-4-31b-it"
    try:
        raw, _usage = await client.generate_content_async(
            model=model,
            prompt=prompt,
            generation_config={"temperature": 0},
            max_output_tokens=900,
        )
        data = extract_json(raw or "")
    except Exception:
        logger.warning(
            "vk_intake.media_assignment failed model=%s drafts=%s posters=%s",
            model,
            len(drafts),
            len(posters),
            exc_info=True,
        )
        return None
    if not isinstance(data, dict) or not isinstance(data.get("assignments"), list):
        return None
    result: dict[int, list[int]] = {}
    for item in data["assignments"]:
        if not isinstance(item, dict):
            continue
        try:
            poster_idx = int(item.get("poster_index"))
            confidence = float(item.get("confidence") or 0.0)
        except (TypeError, ValueError):
            continue
        if not (0 <= poster_idx < len(posters)) or confidence < 0.75:
            continue
        event_indices: list[int] = []
        for raw_idx in item.get("event_indices") or []:
            try:
                event_idx = int(raw_idx)
            except (TypeError, ValueError):
                continue
            if 0 <= event_idx < len(drafts) and event_idx not in event_indices:
                event_indices.append(event_idx)
        result[poster_idx] = event_indices
    logger.info(
        "vk_intake.media_assignment model=%s drafts=%s posters=%s assigned=%s",
        model,
        len(drafts),
        len(posters),
        result,
    )
    return result


_VK_GENERIC_LOCATION_TOKENS = {
    "калининград",
    "кенигсберг",
    "область",
    "город",
    "г",
    "ул",
    "улица",
    "проспект",
    "дом",
    "д",
    "бар",
    "кафе",
    "клуб",
    "театр",
    "музей",
    "зал",
    "центр",
    "дом",
    "дворец",
    "студия",
}


def _vk_grounding_norm(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = text.replace("\xa0", " ").casefold().replace("ё", "е")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^0-9a-zа-я]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _vk_grounding_tokens(value: str | None) -> set[str]:
    return {
        token
        for token in re.findall(r"[0-9a-zа-я]{2,}", _vk_grounding_norm(value), flags=re.IGNORECASE)
        if token not in _VK_GENERIC_LOCATION_TOKENS
    }


def _vk_location_value_ungrounded(
    location_name: str | None,
    *,
    source_text: str | None,
    source_name: str | None = None,
    location_hint: str | None = None,
    poster_texts: Sequence[str] | None = None,
) -> bool:
    """Return True when a VK-extracted venue name has no source support.

    This is a fail-closed guardrail, not a semantic venue chooser: it only
    removes unsupported LLM facts before Smart Update/vector identity can create
    a public card. The LLM still owns extracting the venue when the source text,
    source title/default hint, or OCR actually supports it.
    """

    name = (location_name or "").strip()
    if not name:
        return False
    name_norm = _vk_grounding_norm(name)
    if not name_norm:
        return False

    probes = [
        source_text or "",
        source_name or "",
        location_hint or "",
        "\n".join(p for p in list(poster_texts or []) if isinstance(p, str) and p.strip()),
    ]
    probe_norm = _vk_grounding_norm("\n".join(probes))
    if not probe_norm:
        return True
    if name_norm in probe_norm:
        return False

    significant = _vk_grounding_tokens(name)
    if not significant:
        return False
    probe_tokens = _vk_grounding_tokens(probe_norm)
    return significant.isdisjoint(probe_tokens)


_VK_WEEKDAY_TITLE_RE = re.compile(
    r"^\s*(?:[^\wа-яё]+)?(?:пн|вт|ср|чт|пт|сб|вс|"
    r"понедельник|вторник|среда|четверг|пятница|суббота|воскресенье)"
    r"(?:\s+\d{1,2}[:.]\d{2}(?:\s*[-–—]\s*\d{1,2}[:.]\d{2})?)?\s*$",
    re.IGNORECASE | re.U,
)


def _vk_title_is_schedule_fragment(title: str | None) -> bool:
    """Detect non-title snippets such as ``пятница 22:00``.

    The guard does not invent a replacement title; it only routes the draft to
    low-confidence/fail-closed handling if the LLM did not recover a real name.
    """

    raw = unicodedata.normalize("NFKC", title or "").replace("\xa0", " ").strip()
    if not raw:
        return False
    raw = re.sub(r"\s+", " ", raw)
    return bool(_VK_WEEKDAY_TITLE_RE.match(raw))


def _sanitize_vk_ticket_link_for_source(ticket_link: str | None, source_url: str | None) -> str | None:
    """Keep VK contact links public on VK-originated events."""

    raw = (ticket_link or "").strip()
    if not raw:
        return None
    if not re.search(r"(?i)(?:^|//)(?:m\.)?vk\.com/", source_url or "") and not re.search(
        r"(?i)vk\.com/wall", source_url or ""
    ):
        return raw
    m = re.match(r"(?i)^tg://user\?id=(\d+)$", raw)
    if m:
        return f"https://vk.com/id{m.group(1)}"
    return raw


def _extract_vk_structured_footer_datetime(
    source_text: str | None,
    *,
    anchor_year: int,
) -> tuple[str, str] | None:
    """Extract one explicit structured footer anchor such as ``📅 31 июля, начало в 21:00``.

    This is used only as a contradiction guard. It does not rewrite the event:
    if the LLM chooses a conflicting prose/context date, the draft becomes
    low-confidence and is skipped instead of publishing the wrong day.
    """

    text = unicodedata.normalize("NFKC", source_text or "").replace("\xa0", " ")
    if not text:
        return None

    month_aliases: dict[str, int] = {}
    for word, num in MONTHS_RU.items():
        try:
            month_aliases[unicodedata.normalize("NFKC", str(word)).casefold().replace("ё", "е")] = int(num)
        except Exception:
            continue
    if not month_aliases:
        return None
    month_re = "|".join(re.escape(k) for k in sorted(month_aliases, key=len, reverse=True) if k)
    if not month_re:
        return None

    anchors: list[tuple[str, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Require a structured marker so prose dates remain LLM-owned.
        if not re.search(r"(?iu)(?:📅|^|\s)(?:дата|когда|начало|двери)\b|📅", line):
            continue
        norm_line = unicodedata.normalize("NFKC", line).casefold().replace("ё", "е")
        match = re.search(
            rf"\b(?P<day>\d{{1,2}})\s+(?P<month>{month_re})\b"
            rf".{{0,80}}?\b(?P<time>[0-2]?\d[:.][0-5]\d)\b",
            norm_line,
            flags=re.IGNORECASE | re.U,
        )
        if not match:
            continue
        try:
            day = int(match.group("day"))
            month = month_aliases[unicodedata.normalize("NFKC", match.group("month")).casefold().replace("ё", "е")]
            d_obj = date(anchor_year, month, day)
            time_val = match.group("time").replace(".", ":")
            hh, mm = time_val.split(":", 1)
            anchors.append((d_obj.isoformat(), f"{int(hh):02d}:{int(mm):02d}"))
        except Exception:
            continue
    unique = list(dict.fromkeys(anchors))
    return unique[0] if len(unique) == 1 else None


@dataclass(frozen=True)
class PosterDatetimeAnchor:
    date: str
    time: str
    venue: str | None = None
    address: str | None = None


@dataclass
class PersistResult:
    event_id: int | None
    telegraph_url: str
    ics_supabase_url: str
    ics_tg_url: str
    event_date: str
    event_end_date: str | None
    event_time: str
    event_type: str | None
    is_free: bool
    # Smart Update outcome (for unified operator report)
    smart_status: str | None = None
    smart_created: bool = False
    smart_merged: bool = False
    smart_added_posters: int = 0
    # Typed Smart Update boundary. Legacy booleans above remain constructor
    # compatible for older fixtures, but production callers use this result.
    smart_result: Any | None = None


_EXTERNAL_SHORT_TICKET_HOSTS = {"clck.ru"}
_RESOLVED_SHORTLINK_TRACKING_KEYS = {
    "clckid",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "yclid",
    "fbclid",
    "gclid",
}


def _ticket_link_host(url: str | None) -> str:
    try:
        return urlsplit(str(url or "").strip()).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _strip_resolved_shortlink_tracking(url: str) -> str:
    try:
        parts = urlsplit(url)
    except Exception:
        return url
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() not in _RESOLVED_SHORTLINK_TRACKING_KEYS
    ]
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query, doseq=True),
            "",
        )
    ).rstrip("/")


async def _resolve_external_short_ticket_link(url: str | None) -> str | None:
    """Resolve source-owned short registration links before they reach public posts.

    This is intentionally narrow: it expands third-party shorteners such as
    clck.ru, but does not touch our managed vk.cc output links or arbitrary
    ticket hosts. On any network/provider problem the original URL is kept.
    """

    raw = str(url or "").strip()
    if not raw:
        return None
    if raw.lower().startswith(("clck.ru/",)):
        raw = "https://" + raw
    if _ticket_link_host(raw) not in _EXTERNAL_SHORT_TICKET_HOSTS:
        return raw
    headers = {
        "User-Agent": os.getenv("HTTP_SHORTLINK_UA", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        timeout_total = float(os.getenv("VK_TICKET_SHORTLINK_RESOLVE_TIMEOUT_SEC", "8"))
    except (TypeError, ValueError):
        timeout_total = 8.0
    timeout = aiohttp.ClientTimeout(total=timeout_total)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.head(
                    raw,
                    headers=headers,
                    allow_redirects=True,
                    max_redirects=8,
                    ssl=False,
                ) as resp:
                    final_url = str(resp.url)
            except Exception:
                async with session.get(
                    raw,
                    headers=headers,
                    allow_redirects=True,
                    max_redirects=8,
                    ssl=False,
                ) as resp:
                    final_url = str(resp.url)
        if final_url and _ticket_link_host(final_url) not in _EXTERNAL_SHORT_TICKET_HOSTS:
            resolved = _strip_resolved_shortlink_tracking(final_url)
            if resolved and resolved != raw:
                logger.info("vk_intake ticket_shortlink_resolved url=%s resolved=%s", raw, resolved)
                return resolved
    except Exception as exc:
        logger.warning("vk_intake ticket_shortlink_resolve_failed url=%s err=%s", raw, exc)
    return raw


def _vk_wall_source_ids_from_url(source_post_url: str | None) -> tuple[int | None, int | None]:
    if not source_post_url:
        return None, None
    m = re.search(r"wall-?(\d+)_([0-9]+)", source_post_url)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _collapse_same_post_exact_drafts(drafts_in: list[EventDraft]) -> list[EventDraft]:
    """Collapse obviously duplicated child drafts emitted from one VK multi-post.

    Safety: collapse only when date + explicit time + venue + normalized title are
    identical inside the same parsed draft set. This targets duplicated poster/card
    extraction, not real parallel events from one schedule post.
    """

    if len(drafts_in) < 2:
        return drafts_in

    def _norm_text(value: str | None, *, keep_digits: bool = True) -> str:
        text = unicodedata.normalize("NFKC", (value or "")).casefold().replace("ё", "е")
        pattern = r"[^a-zа-я0-9]+" if keep_digits else r"[^a-zа-я]+"
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE).strip()
        return re.sub(r"\s+", " ", text)

    def _norm_title(value: str | None) -> str:
        return _norm_text(value, keep_digits=True)

    def _norm_location(value: str | None) -> str:
        return _norm_text(value, keep_digits=True)

    def _norm_link(value: str | None) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""
        return raw.rstrip("/")

    def _draft_score(draft: EventDraft) -> tuple[int, int, int, int]:
        score = 0
        score += min(len((_norm_title(draft.title) or "").split()), 8)
        score += min(len(str(draft.description or "").split()), 20)
        score += 4 if (draft.location_address or "").strip() else 0
        score += 2 if (draft.city or "").strip() else 0
        score += 2 if draft.links else 0
        score += min(len(draft.poster_media or []), 3)
        score += 1 if (draft.search_digest or "").strip() else 0
        score += 1 if (draft.event_type or "").strip() else 0
        score += 1 if draft.ticket_price_min is not None or draft.ticket_price_max is not None else 0
        return score, len(draft.source_text or ""), len(draft.description or ""), len(draft.title or "")

    def _poster_key(item: PosterMedia) -> tuple[str, str, str, str]:
        return (
            str(item.digest or ""),
            str(item.supabase_url or ""),
            str(item.catbox_url or ""),
            str(item.name or ""),
        )

    def _merge_links(primary: list[str] | None, secondary: list[str] | None) -> list[str] | None:
        merged: list[str] = []
        seen: set[str] = set()
        for url in list(primary or []) + list(secondary or []):
            norm = _norm_link(url)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            merged.append(url)
        return merged or None

    def _prefer_longer(current: str | None, candidate: str | None) -> str | None:
        cur = (current or "").strip()
        cand = (candidate or "").strip()
        if len(cand) > len(cur):
            return candidate
        return current

    groups: dict[tuple[str, str, str, str], list[EventDraft]] = {}
    passthrough: list[EventDraft] = []
    for draft in drafts_in:
        date_key = str(draft.date or "").strip()
        time_key = str(draft.time or "").strip()
        title_key = _norm_title(draft.title)
        venue_key = _norm_location(draft.venue or draft.location_address)
        if not (date_key and time_key and title_key and venue_key):
            passthrough.append(draft)
            continue
        groups.setdefault((date_key, time_key, venue_key, title_key), []).append(draft)

    collapsed_any = False
    out: list[EventDraft] = list(passthrough)
    for _, group in groups.items():
        if len(group) == 1:
            out.append(group[0])
            continue
        collapsed_any = True
        keep = max(group, key=_draft_score)
        posters_by_key: dict[tuple[str, str, str, str], PosterMedia] = {
            _poster_key(item): item for item in list(keep.poster_media or [])
        }
        for other in group:
            if other is keep:
                continue
            keep.description = _prefer_longer(keep.description, other.description)
            keep.source_text = _prefer_longer(keep.source_text, other.source_text)
            keep.poster_summary = _prefer_longer(keep.poster_summary, other.poster_summary)
            keep.search_digest = _prefer_longer(keep.search_digest, other.search_digest)
            keep.location_address = _prefer_longer(keep.location_address, other.location_address)
            keep.city = _prefer_longer(keep.city, other.city)
            keep.event_type = keep.event_type or other.event_type
            keep.emoji = keep.emoji or other.emoji
            keep.festival = keep.festival or other.festival
            keep.is_free = bool(keep.is_free or other.is_free)
            keep.pushkin_card = bool(keep.pushkin_card or other.pushkin_card)
            if keep.ticket_price_min is None:
                keep.ticket_price_min = other.ticket_price_min
            if keep.ticket_price_max is None:
                keep.ticket_price_max = other.ticket_price_max
            keep.links = _merge_links(keep.links, other.links)
            keep.ocr_tokens_spent += int(other.ocr_tokens_spent or 0)
            for poster in list(other.poster_media or []):
                posters_by_key.setdefault(_poster_key(poster), poster)
        keep.poster_media = list(posters_by_key.values())
        keep.poster_summary = build_poster_summary(keep.poster_media)
        out.append(keep)

    if collapsed_any:
        logger.info(
            "vk_intake: collapsed same-post exact duplicate drafts dropped=%s kept=%s",
            len(drafts_in) - len(out),
            len(out),
        )
    return out


async def _download_photo_media(urls: Sequence[str]) -> list[tuple[bytes, str]]:
    if not urls:
        return []
    import sys

    main_mod = sys.modules.get("main") or sys.modules.get("__main__")
    if main_mod is None:  # pragma: no cover - defensive
        raise RuntimeError("main module not found")
    session = main_mod.get_http_session()
    semaphore = main_mod.HTTP_SEMAPHORE
    timeout = main_mod.HTTP_TIMEOUT
    max_size = main_mod.MAX_DOWNLOAD_SIZE
    ensure_jpeg = main_mod.ensure_jpeg
    detect_image_type = getattr(main_mod, "detect_image_type", None)
    if detect_image_type is None:  # pragma: no cover - defensive
        raise RuntimeError("detect_image_type not found")
    validate_jpeg_markers = getattr(main_mod, "validate_jpeg_markers", None)
    if validate_jpeg_markers is None:  # pragma: no cover - defensive
        raise RuntimeError("validate_jpeg_markers not found")
    results: list[tuple[bytes, str]] = []

    request_headers = getattr(main_mod, "VK_PHOTO_FETCH_HEADERS", None)
    if request_headers is None:
        request_headers = {
            "User-Agent": getattr(
                main_mod,
                "VK_BROWSER_USER_AGENT",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 "
                "Safari/537.36",
            ),
            "Accept": getattr(
                main_mod,
                "VK_BROWSER_ACCEPT",
                "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            ),
            "Referer": getattr(main_mod, "VK_BROWSER_REFERER", "https://vk.com/"),
            "Sec-Fetch-Dest": getattr(
                main_mod, "VK_BROWSER_SEC_FETCH_DEST", "image"
            ),
            "Sec-Fetch-Mode": getattr(
                main_mod, "VK_BROWSER_SEC_FETCH_MODE", "no-cors"
            ),
            "Sec-Fetch-Site": getattr(
                main_mod, "VK_BROWSER_SEC_FETCH_SITE", "same-origin"
            ),
        }
    else:
        request_headers = dict(request_headers)

    for idx, url in enumerate(urls):

        async def _fetch() -> tuple[bytes, str | None, str | None]:
            async with semaphore:
                async with session.get(url, headers=request_headers) as resp:
                    resp.raise_for_status()
                    content_type = resp.headers.get("Content-Type")
                    content_length = resp.headers.get("Content-Length")
                    if resp.content_length and resp.content_length > max_size:
                        raise ValueError("file too large")
                    buf = bytearray()
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        buf.extend(chunk)
                        if len(buf) > max_size:
                            raise ValueError("file too large")
                    return bytes(buf), content_type, content_length

        size = None
        content_type: str | None = None
        content_length: str | None = None
        try:
            data, content_type, content_length = await asyncio.wait_for(
                _fetch(), timeout
            )
            size = len(data)
            if size > max_size:
                raise ValueError("file too large")
            if content_length:
                try:
                    expected_size = int(content_length)
                except ValueError as exc:
                    raise ValueError("invalid Content-Length header") from exc
                if expected_size != size:
                    raise ValueError("content-length mismatch")
            orig_subtype = detect_image_type(data)
            if orig_subtype == "jpeg":
                validate_jpeg_markers(data)
            data, name = ensure_jpeg(data, f"vk_poster_{idx + 1}.jpg")
            subtype = detect_image_type(data)
            if subtype == "jpeg":
                validate_jpeg_markers(data)
        except Exception as exc:  # pragma: no cover - network dependent
            logging.warning(
                "vk.download_photo_failed url=%s size=%s content_type=%s "
                "content_length=%s error=%s",
                url,
                size if size is not None else "unknown",
                content_type or "unknown",
                content_length or "unknown",
                exc,
            )
            continue
        logging.info(
            "vk.photo_media processed idx=%s url=%s size=%d subtype=%s "
            "filename=%s content_type=%s content_length=%s",
            idx,
            url,
            size if size is not None else 0,
            subtype or "unknown",
            name,
            content_type or "unknown",
            content_length or "unknown",
        )
        results.append((data, name))
    return results


async def _download_video_evidence(urls: Sequence[str]) -> list[tuple[bytes, str]]:
    """Download bounded short MP4 evidence for same-invocation Gemini analysis."""

    if not urls:
        return []
    import sys

    main_mod = sys.modules.get("main") or sys.modules.get("__main__")
    if main_mod is None:  # pragma: no cover - defensive
        raise RuntimeError("main module not found")
    session = main_mod.get_http_session()
    semaphore = main_mod.HTTP_SEMAPHORE
    timeout = main_mod.HTTP_TIMEOUT
    try:
        max_size = int(os.getenv("VK_VIDEO_EVIDENCE_MAX_BYTES", str(18 * 1024 * 1024)))
    except ValueError:
        max_size = 18 * 1024 * 1024
    max_size = max(1024 * 1024, min(max_size, 19 * 1024 * 1024))
    results: list[tuple[bytes, str]] = []
    for idx, url in enumerate(urls):
        try:
            async with semaphore:
                async with asyncio.timeout(timeout):
                    async with session.get(
                        url,
                        headers={"User-Agent": getattr(main_mod, "VK_BROWSER_USER_AGENT", "Mozilla/5.0")},
                    ) as resp:
                        resp.raise_for_status()
                        content_type = str(resp.headers.get("Content-Type") or "").casefold()
                        if content_type and "video" not in content_type and "octet-stream" not in content_type:
                            raise ValueError(f"unexpected video content type: {content_type}")
                        if resp.content_length and int(resp.content_length) > max_size:
                            raise ValueError("video evidence file too large")
                        buf = bytearray()
                        async for chunk in resp.content.iter_chunked(128 * 1024):
                            buf.extend(chunk)
                            if len(buf) > max_size:
                                raise ValueError("video evidence file too large")
            payload = bytes(buf)
            if not payload:
                raise ValueError("empty video evidence")
            results.append((payload, f"vk_video_{idx + 1}.mp4"))
            logging.info("vk.video_evidence downloaded idx=%d bytes=%d", idx, len(payload))
        except Exception as exc:
            logging.warning("vk.video_evidence download_failed idx=%d error=%s", idx, exc)
    return results


async def vk_intake_parse_llm(
    prompt_text: str,
    *,
    source_text: str | None = None,
    source_name: str | None = None,
    festival_names: Sequence[str] | None = None,
    festival_alias_pairs: Sequence[tuple[str, int]] | None = None,
    poster_media: Sequence[PosterMedia] | None = None,
    rate_limit_max_wait_sec: float | int | str | None = None,
    parse_gemma_model: str | None = None,
    evidence_manifest: EvidenceManifest | None = None,
    additional_ocr_blocks: Sequence[str] | None = None,
) -> Any:
    """Parse a VK post text into structured events using the universal LLM parser.

    Default backend is Gemma; set `EVENT_PARSE_LLM=4o` to force the legacy OpenAI parser.
    """
    parse_event_via_llm = require_main_attr("parse_event_via_llm")

    extra: dict[str, str] = {}
    if source_name:
        # ``parse_event_via_llm`` accepts ``channel_title`` for context.
        extra["channel_title"] = source_name

    parse_kwargs: dict[str, Any] = {}
    poster_items = list(poster_media or [])
    raw_poster_texts = collect_poster_texts(poster_items)
    raw_poster_texts.extend(
        str(value).strip()
        for value in (additional_ocr_blocks or ())
        if isinstance(value, str) and str(value).strip()
    )
    poster_texts = _budget_vk_parse_poster_texts(
        source_text if source_text is not None else prompt_text,
        raw_poster_texts,
    )
    poster_summary = build_poster_summary(poster_items)
    if poster_texts:
        parse_kwargs["poster_texts"] = poster_texts
    if poster_summary:
        parse_kwargs["poster_summary"] = poster_summary
    if festival_alias_pairs:
        parse_kwargs["festival_alias_pairs"] = festival_alias_pairs
    if rate_limit_max_wait_sec is not None:
        parse_kwargs["rate_limit_max_wait_sec"] = str(rate_limit_max_wait_sec)
    if parse_gemma_model:
        parse_kwargs["gemma_model"] = str(parse_gemma_model).strip()
    if evidence_manifest is not None:
        parse_kwargs["evidence_manifest"] = evidence_manifest.to_payload()
    # VK auto-import is an operator-visible linear batch: schema/provider
    # uncertainty gets one bounded final adjudication in the same invocation,
    # never an invisible background semantic retry.
    parse_kwargs["require_terminal_decision"] = True
    if source_text is not None:
        # The primary prompt contains VK policy overlays.  Contradiction facts
        # and the verifier must receive the untouched carrier, not those
        # instructions as if they were source evidence.
        parse_kwargs["semantic_source_text"] = source_text

    return await parse_event_via_llm(
        prompt_text,
        festival_names=festival_names,
        **extra,
        **parse_kwargs,
    )


async def build_event_drafts_from_vk(
    text: str,
    *,
    source_name: str | None = None,
    location_hint: str | None = None,
    default_time: str | None = None,
    default_ticket_link: str | None = None,
    operator_extra: str | None = None,
    publish_ts: datetime | int | float | None = None,
    event_ts_hint: int | None = None,
    festival_names: list[str] | None = None,
    festival_alias_pairs: Sequence[tuple[str, int]] | None = None,
    festival_hint: bool = False,
    poster_media: Sequence[PosterMedia] | None = None,
    ocr_tokens_spent: int = 0,
    ocr_tokens_remaining: int | None = None,
    rate_limit_max_wait_sec: float | int | str | None = None,
    parse_gemma_model: str | None = None,
    evidence_manifest: EvidenceManifest | None = None,
    additional_ocr_blocks: Sequence[str] | None = None,
) -> tuple[list[EventDraft], dict[str, Any] | None]:
    """Return normalised event drafts extracted from a VK post.

    The function delegates parsing to the same LLM helper used by ``/add`` and
    forwarded posts.  When ``operator_extra`` is supplied it takes precedence
    over conflicting fragments of the original text.  ``source_name`` and
    ``location_hint`` are passed to the extractor for additional context and
    ``default_time`` (if set for the VK source) is used as a low-priority
    fallback when the post has no explicit time. Such time is marked with
    ``draft.time_is_default=True`` so Smart Update treats it as a weak anchor
    and can override it when explicit time arrives from other sources.

    The resulting :class:`EventDraft` contains normalised event attributes such
    as title, schedule, venue, ticket details and other metadata needed by the
    import pipeline.  The function returns a tuple ``(drafts, festival_payload)``
    where ``festival_payload`` is the raw festival structure, if any, provided
    by :func:`main.parse_event_via_llm`.
    """
    timings_on = (os.getenv("PIPELINE_TIMINGS") or "").strip().lower() in {"1", "true", "yes", "on"}
    poster_items = list(poster_media or [])
    poster_texts = collect_poster_texts(poster_items)
    poster_texts.extend(
        str(value).strip()
        for value in (additional_ocr_blocks or ())
        if isinstance(value, str) and str(value).strip()
    )
    poster_summary = build_poster_summary(poster_items)
    evidence_manifest = evidence_manifest or EvidenceManifest.complete_source(
        text or "", poster_texts, attachment_count=len(poster_items)
    )

    fallback_ticket_link = (
        default_ticket_link.strip()
        if isinstance(default_ticket_link, str)
        else default_ticket_link
    )
    if isinstance(fallback_ticket_link, str) and not fallback_ticket_link:
        fallback_ticket_link = None

    llm_text = text
    if operator_extra:
        llm_text = f"{llm_text}\n{operator_extra}"

    # LLM-first hinting: if the source explicitly says it's a standup/comedy show,
    # nudge the parser to make the format visible in the title (without hardcoding
    # deterministic renames after parsing).
    llm_text += (
        "\nОбязательный source-level verdict: верни один типизированный объект "
        "SourceParseDecision с disposition, events, lifecycle_actions, evidence_complete, parse_version и no_event_reason. "
        "Допустимы только EVENTS_FOUND, CONFIRMED_NO_EVENT, LIFECYCLE_ONLY, MIXED и RETRY_REQUIRED. "
        "CONFIRMED_NO_EVENT допустим только для доказанного полного non-event при полном тексте и всех OCR-карточках; "
        "no_event_reason обязателен только для этого disposition и должен быть одним из NO_ATTENDABLE_EVENT, "
        "GIVEAWAY_ONLY, VAGUE_TEASER, REFERRAL_ONLY, SERVICE_OR_RENTAL, RECAP_ONLY, OUT_OF_SCOPE. "
        "Если карточки/вложения доступны не полностью, используй RETRY_REQUIRED с retry_reason=EVIDENCE_INCOMPLETE. "
        "Если при неполных доказательствах найдены положительные события, сохрани их с EVENTS_FOUND либо MIXED "
        "и evidence_complete=false: они требуют дальнейшего enrichment, но не должны исчезнуть. "
        "Для сообщения только об отмене, переносе или изменении уже известного события используй LIFECYCLE_ONLY. "
        "Розыгрыш без самостоятельного описания посещаемого события — доказанный non-event и получает "
        "CONFIRMED_NO_EVENT с no_event_reason=GIVEAWAY_ONLY только при полном evidence; "
        "розыгрыш вместе с реальным событием сохраняет событие. "
        "Расплывчатый тизер без конкретного посещаемого слота получает CONFIRMED_NO_EVENT "
        "с no_event_reason=VAGUE_TEASER при полном evidence, "
        "а при неполных карточках — RETRY_REQUIRED/EVIDENCE_INCOMPLETE. "
        "\nПравила извлечения локации: если пост содержит несколько дат/блоков/репостов, "
        "для каждого события бери площадку, адрес и город из ближайшего к нему блока даты/названия. "
        "Хинт источника или дефолт группы используй только когда в самом блоке нет своей площадки. "
        "Если текст события явно называет библиотеку, музей, бар или другую площадку, она важнее "
        "дефолтной площадки источника. Никогда не возвращай буквальные плейсхолдеры вроде "
        "`location_address`, `address`, `location_name`, `venue`, `city`, `адрес`, `город`: "
        "оставь поле пустым. Если билетная страница или URL ясно содержит каноническое название "
        "спектакля/концерта/показа, не заменяй его рекламной или сюжетной фразой из поста. "
        "Если пост описывает один арт-маркет/ярмарку/праздничную программу с сопутствующей "
        "культурной программой в одном месте и в один день, верни один зонтичный event, "
        "а пункты программы перенеси в description/search_digest. Не дроби такую программу на "
        "отдельные события, если у пунктов нет самостоятельной продажи билетов/регистрации "
        "или отдельного venue/source anchor. "
        "Исключение: обложка-подборка с общим диапазоном дат и подписью, что расписание/места "
        "находятся в карточках, НЕ является одним зонтичным событием. Если карточки называют "
        "разные соревнования/события, даты, города или площадки, верни отдельный event для каждой "
        "достаточно подтверждённой карточки; диапазон с обложки — только envelope, не date/end_date "
        "одного события. Не создавай aggregate event. Если видна лишь часть карточек и нельзя "
        "надёжно восстановить конкретный пункт, используй RETRY_REQUIRED с "
        "retry_reason=EVIDENCE_INCOMPLETE вместо свёртки подборки. "
        "Если пост про выставку/ярмарку только тизерит будущий анонс без точного дня, периода или даты окончания "
        "(например «готовим выставку», «анонс через пару дней», «точную дату анонсируем позже», «в мае откроем»), "
        "используй CONFIRMED_NO_EVENT с no_event_reason=VAGUE_TEASER при полном evidence "
        "или RETRY_REQUIRED/EVIDENCE_INCOMPLETE "
        "при неполных карточках: не ставь дату публикации и не подставляй первое число месяца. "
        "Если текст поста даёт относительный или разговорный якорь даты вроде «в этот четверг», "
        "а OCR афиши даёт точные `DD месяц HH:MM` и площадку/адрес, считай OCR афиши более точным "
        "источником для date/time/location и обязательно перенеси эти значения в событие. "
        "Но если сам текст поста явно пишет точную календарную дату (`18 июня`, `18.06`), "
        "не считай афишу автоматически сильнее этой явной даты. "
        "Не оставляй time пустым, когда афиша явно содержит время начала. "
        "Если в посте есть контекстная/сюжетная дата в прозе и отдельная структурная строка "
        "с `📅 DD месяц, начало в HH:MM`, датой события является структурная строка `📅`, "
        "а не дата из описания. Не используй фрагменты расписания вроде `пятница 22:00` "
        "как title: восстанови название события из заголовка/поста/афиши или верни пустой title. "
        "Не придумывай venue из похожих известных мест: если пост, OCR, название источника или "
        "location hint не подтверждают площадку, оставь location_name пустым."
    )
    room_probe = "\n".join(
        part
        for part in [
            text or "",
            source_name or "",
            "\n".join(poster_texts),
        ]
        if part
    ).casefold().replace("ё", "е")
    if re.search(
        r"\b(?:лекционн\w*\s+зал|читальн\w*\s+зал|конференц[-\s]?зал|аудитори\w*|[24]\s*этаж)\b",
        room_probe,
    ):
        llm_text += (
            "\nRoom/floor is not venue: `лекционный зал`/`читальный зал`/`2 этаж`/`4 этаж` -> infer building. "
            "If the explicit place is only a room, hall, audience room, or floor, treat venue "
            "as missing and use the source organization/source_location/location hint as the "
            "building when source text/OCR supports it. КОНБ+Мира9 => "
            "location_name=Научная библиотека."
        )
    try:
        hint_parts: list[str] = [text or ""]
        if poster_texts:
            hint_parts.extend([p for p in poster_texts if isinstance(p, str) and p.strip()])
        hint_norm = unicodedata.normalize("NFKC", "\n".join(hint_parts)).casefold().replace("ё", "е")
        if re.search(r"\b(?:стендап|стенд-?ап|stand\s*-?up|комик\w*|юмор\w*)\b", hint_norm, flags=re.IGNORECASE):
            llm_text += (
                "\nЕсли это стендап/комедия, сделай это явно в title (например «Стендап: …»), "
                "даже если оригинальное название звучит как «медитация». Не выдумывай детали."
            )
    except Exception:
        pass
    if _vk_parse_should_add_giveaway_prize_hint(text, poster_texts=poster_texts):
        llm_text += (
            "\nЕсли это розыгрыш/конкурс и мероприятие упомянуто только как приз "
            "(например билеты на матч/концерт), не создавай событие: при полном evidence используй "
            "CONFIRMED_NO_EVENT с no_event_reason=GIVEAWAY_ONLY, при неполном — "
            "RETRY_REQUIRED/EVIDENCE_INCOMPLETE. "
            "Извлекай событие только если пост отдельно описывает само посещаемое "
            "мероприятие, а не только механику розыгрыша; тогда используй EVENTS_FOUND или MIXED."
        )
    if location_hint:
        hint_clean = str(location_hint).strip()
        if hint_clean:
            llm_text = (
                f"{llm_text}\n"
                "Хинт по локации (используй ТОЛЬКО если пост действительно описывает посещаемое событие, "
                f"но место не указано явно): {hint_clean}. "
                "Не создавай событие только из-за этого хинта. Доказанный полный non-event обозначай "
                "disposition=CONFIRMED_NO_EVENT с no_event_reason=NO_ATTENDABLE_EVENT."
            )
    if fallback_ticket_link:
        llm_text = (
            f"{llm_text}\n"
            "Хинт по ссылке: если и только если это событие и в посте нет ссылки на билеты/регистрацию, "
            f"используй {fallback_ticket_link} как ссылку по умолчанию. "
            "Не заменяй ссылки, которые уже указаны. Доказанный полный non-event обозначай "
            "disposition=CONFIRMED_NO_EVENT с no_event_reason=NO_ATTENDABLE_EVENT."
        )
    if festival_hint:
        llm_text = (
            f"{llm_text}\n"
            "Оператор подтверждает, что пост описывает фестиваль. "
            "Сопоставь с существующими фестивалями (JSON ниже) или создай новый."
        )

    t0 = time.monotonic()
    raw_parsed = await vk_intake_parse_llm(
        llm_text,
        source_text=text,
        source_name=source_name,
        festival_names=festival_names,
        festival_alias_pairs=festival_alias_pairs,
        poster_media=poster_media,
        rate_limit_max_wait_sec=rate_limit_max_wait_sec,
        parse_gemma_model=parse_gemma_model,
        evidence_manifest=evidence_manifest,
        additional_ocr_blocks=additional_ocr_blocks,
    )
    if timings_on:
        try:
            logger.info(
                "timing vk_intake_parse_llm events_hint=%s posters=%s took_sec=%.3f",
                "unknown",
                len(list(poster_media or [])),
                float(time.monotonic() - t0),
            )
        except Exception:
            pass
    if isinstance(raw_parsed, SourceParseDecision):
        parsed = raw_parsed
    else:
        parsed = decision_from_provider_payload(
            raw_parsed,
            evidence_manifest=evidence_manifest,
        )
        legacy_festival = getattr(raw_parsed, "festival", None)
        if parsed.festival is None and isinstance(legacy_festival, dict):
            parsed.festival = dict(legacy_festival)
        if parsed.disposition is SourceDisposition.RETRY_REQUIRED:
            logger.warning(
                "vk_intake: untyped/invalid source parse payload rejected "
                "payload_type=%s retry_reason=%s",
                type(raw_parsed).__name__,
                getattr(parsed.retry_reason, "value", parsed.retry_reason),
            )
    festival_payload = getattr(parsed, "festival", None)
    parsed_events = list(parsed or [])
    if not parsed_events and not festival_payload:
        # For VK auto-import we treat "no events extracted" as a valid outcome (0 drafts),
        # not a technical failure. Callers that require an event (manual flows) can
        # enforce that at a higher level (see build_event_draft/build_event_payload_from_vk).
        return DraftParseResult([], decision=parsed), None

    combined_text = text or ""
    extra_clean = (operator_extra or "").strip()
    if extra_clean:
        trimmed = combined_text.rstrip()
        combined_text = f"{trimmed}\n\n{extra_clean}" if trimmed else extra_clean

    # Date normalization logic
    tzinfo = require_main_attr("LOCAL_TZ")
    if publish_ts is None:
        anchor_dt = datetime.now(tzinfo)
    elif isinstance(publish_ts, datetime):
        anchor_dt = publish_ts.astimezone(tzinfo) if publish_ts.tzinfo else publish_ts.replace(tzinfo=tzinfo)
    else:
        anchor_dt = datetime.fromtimestamp(publish_ts, tzinfo)

    effective_ts_hint = event_ts_hint
    if operator_extra or effective_ts_hint is None:
        computed = extract_event_ts_hint(
            combined_text,
            default_time=None,
            publish_ts=publish_ts,
            allow_past=False,
            tz=tzinfo
        )
        if computed:
            effective_ts_hint = computed

    hint_dt = None
    if effective_ts_hint:
        hint_dt = datetime.fromtimestamp(effective_ts_hint, tzinfo)

    _numeric_year_re = re.compile(r"\b\d{1,2}[./-]\d{1,2}[./-](19|20)\d{2}\b")
    _month_names_patt = "|".join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    _textual_year_re = re.compile(rf"\b\d{{1,2}}\s+(?:{_month_names_patt})\s+(?:19|20)\d{{2}}\b", re.IGNORECASE)
    has_explicit_year_in_text = bool(_numeric_year_re.search(combined_text) or _textual_year_re.search(combined_text))

    def clean_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return int(float(value))
            except ValueError:
                return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def clean_str(value: Any) -> str | None:
        return _clean_llm_text_field(value)

    def clean_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, str):
            val = value.strip().lower()
            if not val:
                return False
            if val in {"true", "1", "yes", "да", "y"}:
                return True
            if val in {"false", "0", "no", "нет", "n"}:
                return False
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return bool(value)

    drafts: list[EventDraft] = []

    def _source_norm_text() -> str:
        parts = [combined_text]
        if poster_texts:
            parts.extend([p for p in poster_texts if isinstance(p, str) and p.strip()])
        s = unicodedata.normalize("NFKC", "\n".join([p for p in parts if p]))
        s = s.replace("\xa0", " ")
        return s.casefold().replace("ё", "е")

    source_norm = _source_norm_text()
    poster_datetime_anchor = _extract_single_poster_datetime_anchor(
        poster_texts,
        anchor_date=anchor_dt.date(),
    )
    structured_footer_datetime_anchor = _extract_vk_structured_footer_datetime(
        combined_text,
        anchor_year=anchor_dt.year,
    )
    source_has_relative_date_anchor = _source_text_has_relative_date_anchor(text)
    source_has_absolute_date_anchor = _source_text_has_absolute_date_anchor(text)

    def _sanitize_false_time_from_date(
        *,
        draft_date: str | None,
        draft_time: str | None,
    ) -> str | None:
        """Fix common LLM confusion: date token DD.MM -> time HH:MM.

        Example: source mentions "21.02" (Feb 21), but model outputs time "21:02".
        We treat this as a correctness fix (not an editorial rewrite) and only apply
        it when date and time numerals match exactly.
        """
        t_raw = (draft_time or "").strip()
        if not t_raw:
            return draft_time
        m = re.match(r"^\s*(\d{1,2})[:.](\d{2})\s*$", t_raw)
        if not m:
            return draft_time
        try:
            hh = int(m.group(1))
            mm = int(m.group(2))
        except Exception:
            return draft_time
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return draft_time
        d_raw = (draft_date or "").strip()
        if not d_raw:
            return draft_time
        try:
            d_obj = date.fromisoformat(d_raw.split("..", 1)[0].strip())
        except Exception:
            return draft_time
        # Strong signal: time numerals equal the event date day/month.
        if (d_obj.day, d_obj.month) != (hh, mm):
            return draft_time

        date_dot = f"{d_obj.day}.{d_obj.month:02d}"
        date_dot2 = f"{d_obj.day:02d}.{d_obj.month:02d}"
        time_colon = f"{hh:02d}:{mm:02d}"
        # Source contains DD.MM, but not HH:MM -> likely date, not time.
        if (date_dot in source_norm or date_dot2 in source_norm) and (time_colon not in source_norm):
            return None
        return draft_time

    def _looks_like_program_schedule_source() -> bool:
        if not source_norm:
            return False
        if not re.search(r"\b(?:программ\w*|расписан\w*|тайминг|тайм-?инг|в\s+программ\w*)\b", source_norm):
            return False
        times = re.findall(r"\b\d{1,2}[:.]\d{2}\b", source_norm)
        # Require 2+ time tokens to treat it as a schedule/program.
        return len(times) >= 2

    def _maybe_collapse_program_schedule_drafts(drafts_in: list[EventDraft]) -> list[EventDraft]:
        """Collapse duplicate drafts produced from a single program/schedule post.

        Applies only when the source clearly looks like one umbrella event with a program.
        Guardrails are intentionally strict to avoid collapsing theatre multi-show posts.
        """
        if len(drafts_in) < 2:
            return drafts_in
        if not _looks_like_program_schedule_source():
            return drafts_in

        def _norm_title(value: str | None) -> str:
            t = unicodedata.normalize("NFKC", (value or "")).strip()
            t = re.sub(r"^[^a-zа-яё0-9]+", "", t, flags=re.IGNORECASE).strip()
            t = re.sub(r"[^a-zа-яё0-9]+", " ", t, flags=re.IGNORECASE).strip()
            return t.casefold().replace("ё", "е")

        def _parse_hhmm(value: str | None) -> tuple[int, int] | None:
            s = (value or "").strip()
            if not s:
                return None
            s = s.split("..", 1)[0].strip()
            m2 = re.match(r"^(\d{1,2})[:.](\d{2})$", s)
            if not m2:
                return None
            try:
                hh2 = int(m2.group(1))
                mm2 = int(m2.group(2))
            except Exception:
                return None
            if not (0 <= hh2 <= 23 and 0 <= mm2 <= 59):
                return None
            return hh2, mm2

        def _fmt(hh2: int, mm2: int) -> str:
            return f"{hh2:02d}:{mm2:02d}"

        # Use times from source, not from drafts, to get the full range.
        src_times: list[tuple[int, int]] = []
        for tok in re.findall(r"\b\d{1,2}[:.]\d{2}\b", source_norm):
            m2 = re.match(r"^(\d{1,2})[:.](\d{2})$", tok)
            if not m2:
                continue
            try:
                hh2 = int(m2.group(1))
                mm2 = int(m2.group(2))
            except Exception:
                continue
            if 0 <= hh2 <= 23 and 0 <= mm2 <= 59:
                src_times.append((hh2, mm2))
        src_times = sorted(set(src_times))
        if len(src_times) < 2:
            return drafts_in
        start_hh, start_mm = src_times[0]
        end_hh, end_mm = src_times[-1]
        if (end_hh, end_mm) <= (start_hh, start_mm):
            return drafts_in
        merged_time = f"{_fmt(start_hh, start_mm)}..{_fmt(end_hh, end_mm)}"

        # Group by date + venue + normalized title; collapse only within a single clear group.
        groups: dict[tuple[str, str, str], list[EventDraft]] = {}
        for d in drafts_in:
            d_date = (d.date or "").strip()
            d_venue = (d.venue or "").strip()
            key = (d_date, d_venue, _norm_title(d.title))
            groups.setdefault(key, []).append(d)

        # Pick the largest eligible group.
        best_key = None
        best_group: list[EventDraft] = []
        for key, grp in groups.items():
            if len(grp) < 2:
                continue
            # Skip theatre shows: multi-time theatre posts should remain separate.
            et = (grp[0].event_type or "").strip().casefold()
            if et == "спектакль":
                continue
            # All titles should match after normalization.
            if len({_norm_title(x.title) for x in grp}) != 1:
                continue
            # All must have the same date and venue already by key.
            if not key[0] or not key[1] or not key[2]:
                continue
            # Require that drafts appear to differ mainly by time.
            parsed_times = [_parse_hhmm(x.time) for x in grp]
            if not all(pt is not None for pt in parsed_times):
                continue
            # Prefer collapsing when there are 3+ time mentions (typical "program").
            if len(src_times) < 3:
                continue
            if len(grp) > len(best_group):
                best_key = key
                best_group = grp

        if not best_group or best_key is None:
            return drafts_in

        # Keep one draft (stable choice: earliest time among group), override its time with the range.
        best_group_sorted = sorted(
            best_group,
            key=lambda d: _parse_hhmm(d.time) or (99, 99),
        )
        keep = best_group_sorted[0]
        keep.time = merged_time
        out: list[EventDraft] = []
        for d in drafts_in:
            if d is keep:
                out.append(d)
                continue
            if d in best_group:
                continue
            out.append(d)
        return out

    for data in parsed_events:
        ticket_price_min = clean_int(data.get("ticket_price_min"))
        ticket_price_max = clean_int(data.get("ticket_price_max"))
        ticket_link = clean_str(data.get("ticket_link"))
        if ticket_link:
            ticket_link = await _resolve_external_short_ticket_link(ticket_link)
        elif fallback_ticket_link:
            fallback_ticket_link = await _resolve_external_short_ticket_link(fallback_ticket_link)
        links: list[str] | None
        if ticket_link:
            links = [ticket_link]
        elif fallback_ticket_link:
            links = [fallback_ticket_link]
        else:
            links = None

        raw_date = clean_str(data.get("date"))
        raw_time = clean_str(data.get("time"))
        final_date = raw_date
        final_time = raw_time
        time_is_default = False

        if raw_date and not has_explicit_year_in_text:
            final_date = _maybe_rollover_llm_iso_date(
                raw_date,
                anchor_date=anchor_dt.date(),
                has_explicit_year_in_text=has_explicit_year_in_text,
            )
            if hint_dt:
                try:
                    d_check = date.fromisoformat((final_date or raw_date).split("..", 1)[0].strip())
                    if (d_check.month, d_check.day) == (hint_dt.month, hint_dt.day):
                        if d_check.year != hint_dt.year:
                            final_date = hint_dt.date().isoformat()
                        if not final_time and hint_dt.strftime("%H:%M") != "00:00":
                            final_time = hint_dt.strftime("%H:%M")
                except ValueError:
                    pass

        final_time = _sanitize_false_time_from_date(draft_date=final_date, draft_time=final_time)
        if final_date and (not final_time) and default_time:
            final_time = default_time
            time_is_default = True
        poster_anchor_applied = False
        if poster_datetime_anchor and len(parsed_events) == 1:
            poster_date = poster_datetime_anchor.date
            poster_time = poster_datetime_anchor.time
            has_poster_date_conflict = bool(final_date and final_date != poster_date)
            should_apply_poster_anchor = False
            if not final_date:
                should_apply_poster_anchor = True
            elif final_date == poster_date and (not final_time or final_time == poster_time):
                should_apply_poster_anchor = True
            elif (
                has_poster_date_conflict
                and source_has_relative_date_anchor
                and not source_has_absolute_date_anchor
            ):
                # Text such as "в этот четверг" is often copied before final
                # poster details are ready. A single poster with exact DD month
                # HH:MM is stronger than that relative anchor, but not stronger
                # than an explicit date written in the text.
                should_apply_poster_anchor = True
            if should_apply_poster_anchor:
                logger.warning(
                    "vk_intake.poster_datetime_anchor_applied old_date=%s old_time=%s new_date=%s new_time=%s source=%s reason=%s",
                    final_date or "",
                    final_time or "",
                    poster_date,
                    poster_time,
                    source_name or "vk",
                    "relative_text_conflict" if has_poster_date_conflict else "missing_or_matching_anchor",
                )
                final_date = poster_date
                final_time = poster_time
                time_is_default = False
                poster_anchor_applied = True

        title_raw = clean_str(data.get("title")) or ""
        event_type_val = clean_str(data.get("event_type"))

        venue_value = _clean_llm_text_field(data.get("location_name"), field_name="location_name")
        address_value = _clean_llm_text_field(data.get("location_address"), field_name="location_address")
        if poster_datetime_anchor and len(parsed_events) == 1 and poster_anchor_applied:
            if poster_datetime_anchor.venue:
                venue_value = poster_datetime_anchor.venue
            if poster_datetime_anchor.address:
                address_value = poster_datetime_anchor.address
        if _vk_location_value_ungrounded(
            venue_value,
            source_text=combined_text,
            source_name=source_name,
            location_hint=location_hint,
            poster_texts=poster_texts,
        ):
            logger.warning(
                "vk_intake.location_ungrounded_cleared venue=%s source=%s",
                venue_value,
                source_name or "vk",
            )
            venue_value = None
            # The address may still be source-grounded, but without a supported
            # venue name it can bind the event to a wrong nearby place. Let Smart
            # Update/source defaults fill a safe location later if available.
            address_value = None

        drafts.append(
            EventDraft(
                title=title_raw,
                date=final_date,
                time=final_time,
                time_is_default=time_is_default,
                venue=venue_value,
                description=data.get("short_description"),
                festival=clean_str(data.get("festival")),
                location_address=address_value,
                city=_clean_llm_text_field(data.get("city"), field_name="city"),
                ticket_price_min=ticket_price_min,
                ticket_price_max=ticket_price_max,
                event_type=event_type_val,
                emoji=clean_str(data.get("emoji")),
                end_date=clean_str(data.get("end_date")),
                is_free=clean_bool(data.get("is_free")),
                pushkin_card=clean_bool(data.get("pushkin_card")),
                links=links,
                source_text=combined_text,
                poster_media=list(poster_items),
                poster_summary=poster_summary,
                ocr_tokens_spent=ocr_tokens_spent,
                ocr_tokens_remaining=ocr_tokens_remaining,
                search_digest=clean_str(data.get("search_digest")),
            )
        )

        draft = drafts[-1]
        if _vk_title_is_schedule_fragment(draft.title):
            draft.verification_warnings.append("GENERIC_UNGROUNDED_TITLE")
        if structured_footer_datetime_anchor:
            footer_date, footer_time = structured_footer_datetime_anchor
            draft_date = (draft.date or "").split("..", 1)[0].strip()
            draft_time = (draft.time or "").strip().replace(".", ":")
            if draft_date and draft_date != footer_date:
                draft.verification_warnings.append(
                    f"EVENT_DATE_CONFLICT:{draft_date}!={footer_date}"
                )
            elif draft_time and re.match(r"^\d{1,2}:\d{2}$", draft_time):
                hh, mm = draft_time.split(":", 1)
                draft_time_norm = f"{int(hh):02d}:{int(mm):02d}"
                if draft_time_norm != footer_time:
                    draft.verification_warnings.append(
                        f"EVENT_TIME_CONFLICT:{draft_time_norm}!={footer_time}"
                    )

    # If a single VK post describes multiple events, do not blindly attach the whole
    # poster gallery to every event: this often results in the wrong cover/poster
    # on Telegraph pages. Instead, try to assign posters to drafts by OCR relevance
    # and drop ambiguous matches.
    if len(drafts) > 1 and poster_items:
        try:
            month_words: dict[int, set[str]] = {}
            for word, num in MONTHS_RU.items():
                try:
                    num_i = int(num)
                except Exception:
                    continue
                month_words.setdefault(num_i, set()).add(str(word).casefold())

            stop_tokens = {
                "афиша",
                "вход",
                "билет",
                "билеты",
                "руб",
                "рублей",
                "цена",
                "стоимость",
                "начало",
                "начнется",
                "начнётся",
                "сбор",
                "регистрация",
            }

            def _norm_text(value: str | None) -> str:
                text_val = (value or "").strip().casefold().replace("ё", "е")
                text_val = unicodedata.normalize("NFKC", text_val)
                text_val = text_val.replace("\xa0", " ")
                return text_val

            def _tokens(value: str | None) -> set[str]:
                raw = _norm_text(value)
                if not raw:
                    return set()
                found = re.findall(r"[a-zа-я0-9]{3,}", raw, flags=re.IGNORECASE)
                return {t for t in found if t and t not in stop_tokens}

            def _date_bonus(draft: EventDraft, ocr_raw: str) -> float:
                d_raw = (draft.date or "").strip()
                if not d_raw:
                    return 0.0
                try:
                    d_obj = date.fromisoformat(d_raw.split("..", 1)[0].strip())
                except Exception:
                    return 0.0
                day = d_obj.day
                month = d_obj.month
                # Numeric formats: 14.02, 14/02, 14-02, allow 1-digit month/day.
                if re.search(rf"\\b0?{day}[./-]0?{month}\\b", ocr_raw):
                    return 3.0
                # Text month formats: "14 февраля"
                words = month_words.get(month) or set()
                if words:
                    if any(w in ocr_raw for w in words) and re.search(rf"\\b{day}\\b", ocr_raw):
                        return 2.0
                return 0.0

            def _time_bonus(draft: EventDraft, ocr_raw: str) -> float:
                t = (draft.time or "").strip()
                if not t or t == "00:00":
                    return 0.0
                hhmm = re.sub(r"\\s+", "", t)
                if not re.match(r"^\\d{1,2}:\\d{2}$", hhmm):
                    return 0.0
                hh, mm = hhmm.split(":", 1)
                hh = hh.zfill(2)
                needle1 = f"{hh}:{mm}"
                needle2 = f"{hh}.{mm}"
                if needle1 in ocr_raw or needle2 in ocr_raw:
                    return 1.5
                return 0.0

            def _poster_score(draft: EventDraft, poster: PosterMedia) -> float:
                ocr_combined = " ".join(
                    x
                    for x in [
                        (poster.ocr_title or "").strip(),
                        (poster.ocr_text or "").strip(),
                    ]
                    if x
                ).strip()
                if not ocr_combined:
                    return 0.0
                ocr_raw = _norm_text(ocr_combined)

                draft_text = " ".join(
                    x
                    for x in [
                        (draft.title or "").strip(),
                        (draft.venue or "").strip(),
                        (draft.festival or "").strip(),
                    ]
                    if x
                )

                draft_tokens = _tokens(draft_text)
                ocr_tokens = _tokens(ocr_combined)
                overlap = len(draft_tokens & ocr_tokens) if (draft_tokens and ocr_tokens) else 0

                score = float(min(12, overlap * 2))
                score += _date_bonus(draft, ocr_raw)
                score += _time_bonus(draft, ocr_raw)

                # If OCR title contains a substantial part of the event title, boost.
                title_norm = _norm_text(draft.title)
                if title_norm and len(title_norm) >= 10 and title_norm in ocr_raw:
                    score += 4.0

                return score

            max_per_draft = 3
            assigned: dict[int, list[PosterMedia]] = {i: [] for i in range(len(drafts))}

            score_matrix: list[list[float]] = [
                [_poster_score(draft, poster) for draft in drafts]
                for poster in poster_items
            ]
            llm_assignment = await _llm_assign_source_posters_to_drafts(
                source_text=text,
                drafts=drafts,
                posters=poster_items,
                score_matrix=score_matrix,
            )

            for poster_idx, poster in enumerate(poster_items):
                if llm_assignment is not None and poster_idx in llm_assignment:
                    for event_idx in llm_assignment[poster_idx]:
                        if len(assigned[event_idx]) < max_per_draft:
                            assigned[event_idx].append(poster)
                    continue

                # Fail-closed fallback when the one bounded LLM adjudication is
                # unavailable or omitted this poster: keep only an unambiguous
                # high-confidence retrieval match. Retrieval never assigns a
                # shared/ambiguous image by itself.
                scores = [
                    (score, idx)
                    for idx, score in enumerate(score_matrix[poster_idx])
                ]
                scores.sort(key=lambda x: x[0], reverse=True)
                best_score, best_idx = scores[0]
                second = scores[1][0] if len(scores) > 1 else 0.0

                # Guardrails: require confident match; otherwise drop to avoid wrong posters.
                if best_score < 3.0:
                    continue
                if (best_score - second) < 1.5:
                    continue
                if len(assigned[best_idx]) >= max_per_draft:
                    continue
                assigned[best_idx].append(poster)

            for idx, draft in enumerate(drafts):
                draft.poster_media = list(assigned.get(idx) or [])
                draft.poster_summary = build_poster_summary(draft.poster_media)
                if not draft.poster_media:
                    # Multi-event VK posts can mix unrelated photo sets. If OCR
                    # cannot confidently attach a photo to this draft, do not
                    # fall back to the whole raw gallery.
                    draft.allow_raw_photo_fallback = False
        except Exception:
            logging.warning("vk_intake: poster assignment failed", exc_info=True)
            for draft in drafts:
                draft.allow_raw_photo_fallback = False

    def _venue_looks_like_organizer_not_place(venue: str | None, address: str | None) -> bool:
        name = (venue or "").strip()
        if not name:
            return False
        if (address or "").strip():
            return False
        low = name.casefold()
        # Heuristic: some LLM parses put an organizer/artist into location_name when the post
        # doesn't contain an explicit venue. Reject such drafts to avoid garbage events.
        bad_tokens = (
            "оркестр",
            "ансамбль",
            "коллектив",
            "солист",
            "дириж",
            "лауреат",
            "исполнитель",
        )
        good_tokens = (
            "театр",
            "музей",
            "библиотек",
            "центр",
            "дк",
            "дом культуры",
            "зал",
            "кино",
            "галере",
            "филармон",
            "клуб",
            "студ",
            "выставоч",
        )
        if any(t in low for t in good_tokens):
            return False
        if any(t in low for t in bad_tokens):
            return True
        # Very long names without an address-like token are suspicious.
        if len(name) >= 52 and not re.search(r"\\b(ул\\.?|просп\\.?|пр-т|наб\\.?|пл\\.?|дом|д\\.)\\b", low):
            return True
        return False

    for draft in drafts:
        if _venue_looks_like_organizer_not_place(draft.venue, draft.location_address):
            draft.verification_warnings.append("SUSPICIOUS_VENUE_CLEARED")
            draft.venue = None
            draft.location_address = None

    combined_lower = (combined_text or "").lower()
    paid_keywords = ("руб", "₽", "платн", "стоимост", "взнос", "донат")
    has_paid_keywords = any(keyword in combined_lower for keyword in paid_keywords)
    explicit_free_keywords = ("вход свобод", "бесплат", "участие свобод")
    has_explicit_free_keywords = any(keyword in combined_lower for keyword in explicit_free_keywords)

    for draft in drafts:
        venue_text = (draft.venue or "").lower()
        address_text = (draft.location_address or "").lower()
        if draft.ticket_price_min is not None or draft.ticket_price_max is not None:
            continue
        if has_paid_keywords:
            continue
        if not has_explicit_free_keywords:
            continue
        if not draft.is_free and (
            "библиотек" in venue_text
            or "библиотек" in address_text
            or has_explicit_free_keywords
        ):
            draft.is_free = True

    # Guardrail: do not accept a parsed `date` when the source contains no explicit/relative
    # datetime signals. This protects VK auto-import from "today" hallucinations on non-event posts.
    datetime_signal_re = re.compile(
        r"(?iu)\b("
        r"\d{1,2}[./-]\d{1,2}(?:[./-](?:19|20)\d{2})?|"
        r"(?:[01]?\d|2[0-3])[:.][0-5]\d|"
        r"(?:январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*|"
        r"сегодня|завтра|послезавтра|"
        r"выходн\w*|"
        r"понедел\w*|вторник\w*|сред\w*|четверг\w*|пятниц\w*|суббот\w*|воскресен\w*"
        r")\b"
    )
    has_datetime_evidence = bool(datetime_signal_re.search(source_norm or ""))
    if not has_datetime_evidence:
        for draft in drafts:
            if (draft.date or "").strip() or (draft.end_date or "").strip():
                draft.verification_warnings.append("EVENT_DATE_NOT_REGEX_VISIBLE")

    # Guardrail: do not create one-off events that are already in the past relative to
    # the post publish time. Recap posts may contain past dates (for context), but those
    # should not become standalone events.
    for draft in drafts:
        start_d, end_d = _parse_iso_date_range(draft.date, end_value=draft.end_date)
        if not start_d:
            continue
        end_d = end_d or start_d
        if end_d >= anchor_dt.date():
            continue
        event_type_cf = str(getattr(draft, "event_type", "") or "").strip().casefold()
        # For long-running events without an explicit end date, allow (best-effort).
        if ".." not in str(draft.date or "") and not str(draft.end_date or "").strip():
            if event_type_cf in {"выставка", "экспозиция", "ярмарка"}:
                continue
        draft.verification_warnings.append(f"EVENT_DATE_POSSIBLY_PAST:{end_d.isoformat()}")

    # Low-confidence guardrail: do not create events when the extracted title appears
    # to be copied from a recap of a past event, while the future announcement lacks
    # an explicit title. Mark drafts as rejected so callers can skip with a clear reason.
    for draft in drafts:
        reason = _looks_like_recap_title_copied_to_future_event(
            source_text=combined_text,
            title=draft.title,
            draft_date=draft.date,
            draft_time=draft.time,
            anchor_date=anchor_dt.date(),
        )
        if reason:
            draft.verification_warnings.append(f"RECAP_CONFLICT:{reason}")

    # Additional guardrail for recap-style posts: if the post looks like a recent recap,
    # and the "future mention" is too generic (e.g. "тематический концерт"), skip it.
    recap_reason = _looks_like_recent_recap_with_past_date(
        source_text=combined_text,
        anchor_date=anchor_dt.date(),
    )
    if recap_reason:
        for draft in drafts:
            if not _looks_like_vague_teaser_title(draft.title):
                continue
            try:
                d_obj = date.fromisoformat((draft.date or "").split("..", 1)[0].strip())
            except Exception:
                continue
            if d_obj < anchor_dt.date():
                continue
            draft.verification_warnings.append(f"RECAP_CONFLICT:{recap_reason}")

    return DraftParseResult(drafts, decision=parsed), festival_payload


async def build_event_payload_from_vk(
    text: str,
    *,
    source_name: str | None = None,
    location_hint: str | None = None,
    default_time: str | None = None,
    default_ticket_link: str | None = None,
    operator_extra: str | None = None,
    festival_names: list[str] | None = None,
    poster_media: Sequence[PosterMedia] | None = None,
    ocr_tokens_spent: int = 0,
    ocr_tokens_remaining: int | None = None,
) -> tuple[EventDraft, dict[str, Any] | None]:
    drafts, festival_payload = await build_event_drafts_from_vk(
        text,
        source_name=source_name,
        location_hint=location_hint,
        default_time=default_time,
        default_ticket_link=default_ticket_link,
        operator_extra=operator_extra,
        festival_names=festival_names,
        poster_media=poster_media,
        ocr_tokens_spent=ocr_tokens_spent,
        ocr_tokens_remaining=ocr_tokens_remaining,
    )
    if not drafts:
        raise RuntimeError("LLM returned no event")
    return drafts[0], festival_payload


async def build_event_drafts(
    text: str,
    *,
    photos: Sequence[str] | None = None,
    videos: Sequence[str] | None = None,
    source_name: str | None = None,
    location_hint: str | None = None,
    default_time: str | None = None,
    default_ticket_link: str | None = None,
    operator_extra: str | None = None,
    publish_ts: datetime | int | float | None = None,
    event_ts_hint: int | None = None,
    festival_names: list[str] | None = None,
    festival_alias_pairs: list[tuple[str, int]] | None = None,
    festival_hint: bool = False,
    rate_limit_max_wait_sec: float | int | str | None = None,
    parse_gemma_model: str | None = None,
    attachment_count_hint: int | None = None,
    unavailable_attachment_count_hint: int = 0,
    db: Database,
) -> tuple[list[EventDraft], dict[str, Any] | None]:
    """Download posters, run OCR and return event drafts for a VK post.

    Returns a tuple ``(drafts, festival_payload)`` mirroring
    :func:`build_event_drafts_from_vk`.
    """
    timings_on = (os.getenv("PIPELINE_TIMINGS") or "").strip().lower() in {"1", "true", "yes", "on"}
    timing: dict[str, float] = {}
    def _tmark(name: str, sec: float) -> None:
        if timings_on:
            timing[name] = float(sec)

    t_all = time.monotonic()
    t0 = time.monotonic()
    photo_bytes = await _download_photo_media(photos or [])
    _tmark("download_photos", time.monotonic() - t0)
    t0 = time.monotonic()
    video_bytes = await _download_video_evidence(videos or [])
    _tmark("download_videos", time.monotonic() - t0)
    video_evidence_results: list[poster_ocr.OcrResult] = []
    for payload, _name in video_bytes:
        try:
            video_evidence_results.append(
                await poster_ocr.recognize_video_evidence(payload, detail="video")
            )
        except Exception as exc:
            logging.warning("vk.video_evidence analysis_failed error=%s", exc, exc_info=True)
    poster_items: list[PosterMedia] = []
    # This flag participates in the source-level evidence verdict even when a
    # post has no downloadable photos.  Keep it explicitly initialised rather
    # than relying on one of the OCR branches to assign it.
    ocr_failed = False
    ocr_tokens_spent = 0
    ocr_tokens_remaining: int | None = None
    ocr_limit_notice: str | None = None
    hash_to_indices: dict[str, list[int]] | None = None
    ocr_disabled = (os.getenv("POSTER_OCR_DISABLED") or "").strip().lower() in {"1", "true", "yes", "on"}
    if photo_bytes:
        hash_to_indices = {}
        for idx, (payload, _name) in enumerate(photo_bytes):
            digest = hashlib.sha256(payload).hexdigest()
            hash_to_indices.setdefault(digest, []).append(idx)
        t0 = time.monotonic()
        poster_items, catbox_msg = await process_media(
            photo_bytes, need_catbox=True, need_ocr=False
        )
        _tmark("upload_catbox", time.monotonic() - t0)
        ocr_source = source_name or "vk"
        ocr_log_context = {"event_id": None, "source": ocr_source}
        ocr_results: list[poster_ocr.PosterOcrCache] = []
        if ocr_disabled:
            logging.info("vk.build_event_draft OCR disabled via POSTER_OCR_DISABLED=1", extra=ocr_log_context)
            ocr_failed = True
        else:
            try:
                t0 = time.monotonic()
                (
                    ocr_results,
                    ocr_tokens_spent,
                    ocr_tokens_remaining,
                ) = await poster_ocr.recognize_posters(
                    db, photo_bytes, log_context=ocr_log_context
                )
                _tmark("ocr_posters", time.monotonic() - t0)
            except poster_ocr.PosterOcrLimitExceededError as exc:
                logging.warning(
                    "vk.build_event_draft OCR skipped: %s",
                    exc,
                    extra=ocr_log_context,
                )
                ocr_results = list(exc.results or [])
                ocr_tokens_spent = exc.spent_tokens
                ocr_tokens_remaining = exc.remaining
                ocr_limit_notice = (
                    "OCR недоступен: дневной лимит токенов исчерпан, распознавание пропущено."
                )
                ocr_failed = len(ocr_results) < len(photo_bytes)
            except Exception as exc:
                # OCR is a best-effort enrichment. Do not fail the entire VK post import
                # when OCR backend is temporarily unavailable (network/provider errors).
                logging.warning(
                    "vk.build_event_draft OCR failed: %s",
                    exc,
                    extra=ocr_log_context,
                    exc_info=True,
                )
                ocr_results = []
                ocr_limit_notice = "OCR недоступен: ошибка распознавания, распознавание пропущено."
                ocr_failed = True
        if ocr_results:
            apply_ocr_results_to_media(
                poster_items,
                ocr_results,
                hash_to_indices=hash_to_indices if hash_to_indices else None,
            )
        logging.info(
            "vk.build_event_draft posters=%d storage=%s",
            len(poster_items),
            catbox_msg or "",
        )
    else:
        ocr_source = source_name or "vk"
        ocr_log_context = {"event_id": None, "source": ocr_source}
        if not ocr_disabled:
            _, _, ocr_tokens_remaining = await poster_ocr.recognize_posters(
                db, [], log_context=ocr_log_context
            )
    photo_urls = list(photos or ())
    video_urls = list(videos or ())
    ocr_blocks = collect_poster_texts(poster_items)
    video_evidence_blocks = [
        "\n".join(part for part in (result.title, result.text) if (part or "").strip()).strip()
        for result in video_evidence_results
    ]
    video_evidence_blocks = [value for value in video_evidence_blocks if value]
    # A successful OCR result is an evidence unit even when the image contains
    # no readable text. ``collect_poster_texts`` intentionally omits empty text
    # from the prompt, so it cannot be used to decide whether OCR ran. Doing so
    # made blank-success posters look permanently unavailable and sent the same
    # VK carriers through the retry queue forever.
    image_processed_count = min(len(photo_bytes), len(ocr_results)) if photo_bytes else 0
    video_processed_count = min(len(video_bytes), len(video_evidence_results)) if video_bytes else 0
    ocr_processed_count = image_processed_count + video_processed_count
    attachment_count = max(
        len(photo_urls) + len(video_urls), int(attachment_count_hint or 0)
    )
    unavailable_count = max(
        int(unavailable_attachment_count_hint or 0),
        max(0, attachment_count - len(photo_bytes) - len(video_bytes)),
    )
    missing_ocr_count = max(0, len(photo_bytes) - image_processed_count) + max(
        0, len(video_bytes) - video_processed_count
    )
    ocr_failed = bool(ocr_failed or missing_ocr_count or unavailable_count)
    evidence_manifest = EvidenceManifest(
        raw_text_chars=len(text or ""),
        raw_text_hash=hashlib.sha256((text or "").encode("utf-8")).hexdigest(),
        attachment_count=attachment_count,
        ocr_blocks_available=ocr_processed_count,
        ocr_blocks_included=ocr_processed_count,
        included_chars=len(text or "") + sum(
            len(block) for block in [*ocr_blocks, *video_evidence_blocks]
        ),
        omitted_blocks=tuple(
            f"attachment:{idx}:ocr_unavailable" for idx in range(missing_ocr_count)
        ),
        unavailable_attachment_count=unavailable_count,
        ocr_complete=(not ocr_failed and unavailable_count == 0 and missing_ocr_count == 0),
    )
    drafts, festival_payload = await build_event_drafts_from_vk(
        text,
        source_name=source_name,
        location_hint=location_hint,
        default_time=default_time,
        default_ticket_link=default_ticket_link,
        operator_extra=operator_extra,
        publish_ts=publish_ts,
        event_ts_hint=event_ts_hint,
        festival_names=festival_names,
        festival_alias_pairs=festival_alias_pairs,
        festival_hint=festival_hint,
        poster_media=poster_items,
        ocr_tokens_spent=ocr_tokens_spent,
        ocr_tokens_remaining=ocr_tokens_remaining,
        rate_limit_max_wait_sec=rate_limit_max_wait_sec,
        parse_gemma_model=parse_gemma_model,
        evidence_manifest=evidence_manifest,
        additional_ocr_blocks=video_evidence_blocks,
    )
    _tmark("build_drafts_from_vk_total", time.monotonic() - t_all)
    for draft in drafts:
        draft.ocr_limit_notice = ocr_limit_notice
    if timings_on:
        try:
            logger.info(
                "timing vk_intake_build_drafts photos=%s posters=%s stages=%s",
                len(photos or []),
                len(poster_items or []),
                {k: round(v, 3) for k, v in sorted(timing.items())},
            )
        except Exception:
            pass
    return drafts, festival_payload


async def build_event_draft(
    text: str,
    *,
    photos: Sequence[str] | None = None,
    source_name: str | None = None,
    location_hint: str | None = None,
    default_time: str | None = None,
    default_ticket_link: str | None = None,
    operator_extra: str | None = None,
    publish_ts: datetime | int | float | None = None,
    event_ts_hint: int | None = None,
    festival_names: list[str] | None = None,
    festival_alias_pairs: list[tuple[str, int]] | None = None,
    festival_hint: bool = False,
    db: Database,
) -> tuple[EventDraft, dict[str, Any] | None]:
    drafts, festival_payload = await build_event_drafts(
        text,
        photos=photos,
        source_name=source_name,
        location_hint=location_hint,
        default_time=default_time,
        default_ticket_link=default_ticket_link,
        operator_extra=operator_extra,
        festival_names=festival_names,
        festival_alias_pairs=festival_alias_pairs,
        festival_hint=festival_hint,
        db=db,
    )
    if not drafts:
        raise RuntimeError("LLM returned no event")
    return drafts[0], festival_payload


_DASH_CHAR_PATTERN = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]")
_MONTH_NAME_PATTERN = "|".join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
_TEXT_RANGE_TWO_MONTHS_RE = re.compile(
    rf"^\s*(?P<start_day>\d{{1,2}})\s*(?P<start_month>{_MONTH_NAME_PATTERN})\s*-\s*(?P<end_day>\d{{1,2}})\s*(?P<end_month>{_MONTH_NAME_PATTERN})\s*$",
    re.IGNORECASE,
)
_TEXT_RANGE_SAME_MONTH_RE = re.compile(
    rf"^\s*(?P<start_day>\d{{1,2}})\s*-\s*(?P<end_day>\d{{1,2}})\s*(?P<month>{_MONTH_NAME_PATTERN})\s*$",
    re.IGNORECASE,
)
_TEXT_SINGLE_RE = re.compile(
    rf"^\s*(?P<day>\d{{1,2}})\s*(?P<month>{_MONTH_NAME_PATTERN})\s*$",
    re.IGNORECASE,
)


def _month_from_token(token: str) -> int | None:
    lookup = token.strip().strip(".,").casefold()
    return MONTHS_RU.get(lookup)


def _safe_construct_date(year: int, month: int, day: int) -> date | None:
    if not (1 <= month <= 12):
        return None
    if day < 1:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        try:
            last_day = calendar.monthrange(year, month)[1]
        except Exception:
            return None
        day = min(day, last_day)
        try:
            return date(year, month, day)
        except ValueError:
            return None


def _looks_like_vague_teaser_title(title: str | None) -> bool:
    raw = (title or "").strip()
    if not raw:
        return False
    t = unicodedata.normalize("NFKC", raw)
    t = t.replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    # Drop leading emoji/prefix noise.
    t = re.sub(r"^[^a-zа-яё0-9]+", "", t, flags=re.IGNORECASE).strip()
    words = re.findall(r"[а-яё]+", t, flags=re.IGNORECASE)
    words = [w.casefold().replace("ё", "е") for w in words if w]
    if len(words) != 2:
        return False
    return words[0].startswith("тематичес") and words[1] == "концерт"


def _looks_like_recent_recap_with_past_date(
    *,
    source_text: str | None,
    anchor_date: date,
) -> str | None:
    """Detect recap-style posts that mention a very recent past date.

    We use this as a context signal: such posts often contain a small "teaser" for a
    different future event without a proper title. Creating a standalone event from
    that teaser is high-risk.
    """
    raw_text = (source_text or "").strip()
    if not raw_text:
        return None

    def _norm(s: str) -> str:
        s2 = unicodedata.normalize("NFKC", s)
        s2 = s2.replace("\xa0", " ")
        s2 = re.sub(r"\s+", " ", s2).strip()
        return s2.casefold().replace("ё", "е")

    text_norm = _norm(raw_text)
    recap_markers = (
        "позавчера",
        "вчера",
        "прош",
        "состоял",
        "состоялась",
        "состоялось",
        "вновь",
        "исполнил",
        "исполнила",
        "исполн",
        "прозвуч",
    )
    if not any(tok in text_norm for tok in recap_markers):
        return None

    # Require a recent past date mention to avoid false positives on generic
    # "история/справка" posts.
    max_past = timedelta(days=14)
    month_names_patt = "|".join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    date_mentions: list[date] = []

    for m in re.finditer(r"\b(\d{1,2})[./-](\d{1,2})(?:[./-]((?:19|20)\d{2}))?\b", text_norm):
        try:
            day = int(m.group(1))
            month = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else int(anchor_date.year)
        except Exception:
            continue
        d = _safe_construct_date(year, month, day)
        if d:
            date_mentions.append(d)

    for m in re.finditer(
        rf"\b(\d{{1,2}})\s+({month_names_patt})(?:\s+((?:19|20)\d{{2}}))?\b",
        text_norm,
        flags=re.IGNORECASE,
    ):
        try:
            day = int(m.group(1))
            mon = _month_from_token(m.group(2))
            if not mon:
                continue
            year = int(m.group(3)) if m.group(3) else int(anchor_date.year)
        except Exception:
            continue
        d = _safe_construct_date(year, int(mon), day)
        if d:
            date_mentions.append(d)

    if not date_mentions:
        return None

    has_recent_past = any(
        (d < anchor_date and (anchor_date - d) <= max_past)
        for d in date_mentions
    )
    if not has_recent_past:
        return None

    return (
        "Низкая уверенность: пост похож на отчёт о недавнем прошедшем событии, "
        "а будущее упоминание слишком общее (нет явного названия)."
    )


def _maybe_rollover_llm_iso_date(
    raw_date: str | None,
    *,
    anchor_date: date,
    has_explicit_year_in_text: bool,
) -> str | None:
    """Roll over an LLM-produced ISO date to the next year when the year is implicit.

    VK posts often contain "DD month" without a year. The parser may resolve it to
    the current year; if that date is in the far past relative to the publish date,
    it likely refers to the next year's event.

    IMPORTANT: do NOT roll over *recent* past mentions (recaps), otherwise we create
    bogus future events (e.g. 12 Feb -> 12 Feb next year).
    """
    rd = (raw_date or "").strip()
    if not rd:
        return raw_date
    if has_explicit_year_in_text:
        return raw_date
    normalized = normalize_implicit_iso_date_to_anchor(
        rd,
        anchor_date=anchor_date,
        recent_past_days=int(RECENT_PAST_THRESHOLD.days),
    )
    return normalized or raw_date


def _parse_iso_date_maybe(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    raw = raw.split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _parse_iso_date_range(value: str | None, *, end_value: str | None) -> tuple[date | None, date | None]:
    raw = (value or "").strip()
    if not raw:
        return None, None
    if ".." in raw:
        left, right = raw.split("..", 1)
        start = _parse_iso_date_maybe(left)
        end = _parse_iso_date_maybe(right) or start
    else:
        start = _parse_iso_date_maybe(raw)
        end = start
    # Prefer explicit end_date field when present.
    end_override = _parse_iso_date_maybe(end_value)
    if end_override:
        end = end_override
    return start, end


def _looks_like_recap_title_copied_to_future_event(
    *,
    source_text: str | None,
    title: str | None,
    draft_date: str | None,
    draft_time: str | None,
    anchor_date: date,
) -> str | None:
    """Detect a common VK pattern: recap of a past event + mention of a future event without a name.

    Example (real-world): "12 февраля ... вновь исполнила программу «X» ...", then
    "19 марта ... исполнят тематический концерт" (no explicit title). LLM may
    incorrectly reuse the past program title for the future date.

    Returns a human-readable reject reason when confidence is low.
    """
    raw_text = (source_text or "").strip()
    raw_title = (title or "").strip()
    raw_date = (draft_date or "").strip()
    if not (raw_text and raw_title and raw_date):
        return None
    try:
        d_obj = date.fromisoformat(raw_date.split("..", 1)[0].strip())
    except Exception:
        return None

    def _norm(s: str) -> str:
        s2 = unicodedata.normalize("NFKC", s)
        s2 = s2.replace("\xa0", " ")
        for ch in ("«", "»", "“", "”", "„", "‟", '"', "'"):
            s2 = s2.replace(ch, " ")
        s2 = re.sub(r"\s+", " ", s2).strip()
        s2 = s2.casefold().replace("ё", "е")
        return s2

    text_norm = _norm(raw_text)
    title_norm = _norm(raw_title)
    title_norm = re.sub(r"[^\w\s]+", " ", title_norm, flags=re.UNICODE)
    title_norm = re.sub(r"\s+", " ", title_norm).strip()
    if len(title_norm) < 6:
        return None

    title_positions: list[int] = []
    start = 0
    while True:
        idx = text_norm.find(title_norm, start)
        if idx < 0:
            break
        title_positions.append(int(idx))
        start = idx + max(1, len(title_norm))
        if len(title_positions) >= 6:
            break
    if not title_positions:
        return None

    # Extract date mentions with rough positions (in the normalized text).
    month_names_patt = "|".join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    date_mentions: list[tuple[date, int]] = []

    # Numeric: dd.mm, dd/mm, dd-mm (year optional).
    for m in re.finditer(r"\b(\d{1,2})[./-](\d{1,2})(?:[./-]((?:19|20)\d{2}))?\b", text_norm):
        try:
            day = int(m.group(1))
            month = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else int(anchor_date.year)
        except Exception:
            continue
        d = _safe_construct_date(year, month, day)
        if d:
            date_mentions.append((d, int(m.start())))

    # Text: "12 февраля" (+ optional year).
    for m in re.finditer(
        rf"\b(\d{{1,2}})\s+({month_names_patt})(?:\s+((?:19|20)\d{{2}}))?\b",
        text_norm,
        flags=re.IGNORECASE,
    ):
        try:
            day = int(m.group(1))
            mon = _month_from_token(m.group(2))
            if not mon:
                continue
            year = int(m.group(3)) if m.group(3) else int(anchor_date.year)
        except Exception:
            continue
        d = _safe_construct_date(year, int(mon), day)
        if d:
            date_mentions.append((d, int(m.start())))

    if len(date_mentions) < 2:
        return None

    draft_date_positions = [pos for d, pos in date_mentions if (d.day, d.month) == (d_obj.day, d_obj.month)]
    if not draft_date_positions:
        return None

    def _min_dist(pos: int, others: list[int]) -> int:
        return int(min(abs(pos - x) for x in others)) if others else 10**9

    def _no_sentence_boundary_between(a: int, b: int) -> bool:
        lo = max(0, min(a, b))
        hi = min(len(text_norm), max(a, b))
        if hi <= lo:
            return True
        between = text_norm[lo:hi]
        # Treat sentence ends and explicit newlines as hard boundaries.
        return not bool(re.search(r"[.!?]\s|[\r\n]", between))

    # Find the date mention closest to the title (likely the date the title belongs to).
    near_title_date, near_title_pos = min(
        date_mentions,
        key=lambda item: _min_dist(item[1], title_positions),
    )

    # If the title appears near the draft's date mention, it's probably fine.
    close_to_draft_date = False
    for pos in title_positions:
        # Require that the mention is in the same sentence/fragment; otherwise a recap paragraph
        # followed by a new-sentence future date can look "close" in short posts.
        nearest_dpos = min(draft_date_positions, key=lambda p: abs(p - pos))
        if abs(pos - nearest_dpos) <= 220 and _no_sentence_boundary_between(pos, nearest_dpos):
            close_to_draft_date = True
            break
    if close_to_draft_date:
        return None

    # Strong signal: title is anchored to a past date in the same post, while the extracted
    # draft date is in the future (relative to the post publish date).
    if near_title_date == d_obj:
        return None
    if not (near_title_date < anchor_date <= d_obj):
        return None

    # Check that the title lives in a "recap" window (past tense / "вновь исполнил").
    # This reduces false positives where multiple future dates are listed.
    nearest_title_pos = min(title_positions, key=lambda p: abs(p - near_title_pos))
    win = text_norm[max(0, nearest_title_pos - 220) : min(len(text_norm), nearest_title_pos + 220)]
    recap_markers = (
        "позавчера",
        "вчера",
        "прош",
        "состоял",
        "состоялась",
        "состоялось",
        "вновь",
        "исполнил",
        "исполнила",
        "исполн",
        "прозвуч",
    )
    if not any(tok in win for tok in recap_markers):
        return None

    # If time is explicitly present (not placeholder), allow — the announcement is more likely grounded.
    t_raw = (draft_time or "").strip().replace(".", ":")
    if t_raw and t_raw not in {"00:00", "0:00"}:
        return None

    near_title_iso = near_title_date.isoformat()
    return (
        "Низкая уверенность: заголовок выглядит как название прошедшего концерта "
        f"({near_title_iso}), а анонс на {d_obj.isoformat()} не содержит явного названия."
    )


def _parse_single_date_token(token: str, target_year: int) -> date | None:
    token = token.strip()
    if not token:
        return None

    token = token.strip(".,")
    dot_match = re.match(r"^(?P<day>\d{1,2})\.(?P<month>\d{1,2})$", token)
    if dot_match:
        day = int(dot_match.group("day"))
        month = int(dot_match.group("month"))
        return _safe_construct_date(target_year, month, day)

    legacy_match = re.match(r"^(?P<month>\d{1,2})-(?P<day>\d{1,2})$", token)
    if legacy_match:
        month = int(legacy_match.group("month"))
        day = int(legacy_match.group("day"))
        return _safe_construct_date(target_year, month, day)

    text_match = _TEXT_SINGLE_RE.match(token)
    if text_match:
        month = _month_from_token(text_match.group("month"))
        day = int(text_match.group("day"))
        if month is None:
            return None
        return _safe_construct_date(target_year, month, day)

    return None


def _orthodox_easter_gregorian(target_year: int) -> date:
    """Return Orthodox Easter (Pascha) date in Gregorian calendar.

    Algorithm: compute Julian Easter (Meeus Julian algorithm) and convert to
    Gregorian by adding the calendar offset for the given year.
    """
    a = target_year % 4
    b = target_year % 7
    c = target_year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    month = (d + e + 114) // 31  # 3=March, 4=April (Julian)
    day = ((d + e + 114) % 31) + 1

    julian_easter = date(target_year, month, day)
    gregorian_delta_days = target_year // 100 - target_year // 400 - 2
    return julian_easter + timedelta(days=gregorian_delta_days)


def _movable_holiday_date_range(token: str, target_year: int) -> tuple[date | None, date | None]:
    key = token.split(":", 1)[1].strip().casefold()
    if key in {"maslenitsa", "масленица"}:
        easter = _orthodox_easter_gregorian(target_year)
        start = easter - timedelta(days=55)
        end = easter - timedelta(days=49)
        return start, end
    return None, None


def _holiday_date_range(record: Any, target_year: int) -> tuple[str | None, str | None]:
    raw = (record.date or "").strip()
    if not raw:
        return None, None

    if raw.casefold().startswith("movable:"):
        start, end = _movable_holiday_date_range(raw, target_year)
        start_iso = start.isoformat() if start else None
        end_iso = end.isoformat() if end else None
        return start_iso, end_iso

    normalized = _DASH_CHAR_PATTERN.sub("-", raw)
    normalized = re.sub(r"\s+", " ", normalized.strip())
    normalized = normalized.strip(".,")
    if not normalized:
        return None, None

    if ".." in normalized:
        parts = [part.strip() for part in normalized.split("..") if part.strip()]
        if not parts:
            return None, None
        start = _parse_single_date_token(parts[0], target_year)
        end_token = parts[-1]
        end = _parse_single_date_token(end_token, target_year)
    else:
        if re.match(r"^\d{1,2}-\d{1,2}$", normalized):
            start = _parse_single_date_token(normalized, target_year)
            end = start
        else:
            dot_range = re.match(
                r"^(?P<start_day>\d{1,2})\.(?P<start_month>\d{1,2})\s*-\s*(?P<end_day>\d{1,2})\.(?P<end_month>\d{1,2})$",
                normalized,
            )
            partial_numeric = re.match(
                r"^(?P<start_day>\d{1,2})\s*-\s*(?P<end_day>\d{1,2})\.(?P<month>\d{1,2})$",
                normalized,
            )
            text_range = _TEXT_RANGE_TWO_MONTHS_RE.match(normalized)
            partial_text = re.match(
                r"^(?P<start_day>\d{1,2})\s*-\s*(?P<end_day>\d{1,2})\s+(?P<month>[\wё]+)\.?$",
                normalized,
                flags=re.IGNORECASE,
            )
            text_same_month = _TEXT_RANGE_SAME_MONTH_RE.match(normalized)

            if dot_range:
                start = _safe_construct_date(
                    target_year,
                    int(dot_range.group("start_month")),
                    int(dot_range.group("start_day")),
                )
                end = _safe_construct_date(
                    target_year,
                    int(dot_range.group("end_month")),
                    int(dot_range.group("end_day")),
                )
            elif partial_numeric:
                month = int(partial_numeric.group("month"))
                start = _safe_construct_date(
                    target_year,
                    month,
                    int(partial_numeric.group("start_day")),
                )
                end = _safe_construct_date(
                    target_year,
                    month,
                    int(partial_numeric.group("end_day")),
                )
            elif text_range:
                start_month = _month_from_token(text_range.group("start_month"))
                end_month = _month_from_token(text_range.group("end_month"))
                start = (
                    _safe_construct_date(
                        target_year,
                        start_month,
                        int(text_range.group("start_day")),
                    )
                    if start_month is not None
                    else None
                )
                end = (
                    _safe_construct_date(
                        target_year,
                        end_month,
                        int(text_range.group("end_day")),
                    )
                    if end_month is not None
                    else None
                )
            elif partial_text:
                month = _month_from_token(partial_text.group("month"))
                if month is not None:
                    start = _safe_construct_date(
                        target_year,
                        month,
                        int(partial_text.group("start_day")),
                    )
                    end = _safe_construct_date(
                        target_year,
                        month,
                        int(partial_text.group("end_day")),
                    )
                else:
                    start = None
                    end = None
            elif text_same_month:
                month = _month_from_token(text_same_month.group("month"))
                if month is not None:
                    start = _safe_construct_date(
                        target_year, month, int(text_same_month.group("start_day"))
                    )
                    end = _safe_construct_date(
                        target_year, month, int(text_same_month.group("end_day"))
                    )
                else:
                    start = None
                    end = None
            else:
                parts = [part.strip() for part in re.split(r"\s*-\s*", normalized) if part.strip()]
                if len(parts) >= 2:
                    start = _parse_single_date_token(parts[0], target_year)
                    end = _parse_single_date_token(parts[-1], target_year)
                else:
                    start = _parse_single_date_token(normalized, target_year)
                    end = start

    if start and end and end < start:
        rollover = _safe_construct_date(end.year + 1, end.month, end.day)
        end = rollover if rollover else end

    start_iso = start.isoformat() if start else None
    end_iso = end.isoformat() if end else None
    return start_iso, end_iso


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip())
    except Exception:
        return None


def _event_date_range(
    event_date: str | None, event_end_date: str | None
) -> tuple[date | None, date | None]:
    if not event_date:
        return None, None

    if ".." in event_date:
        parts = [part.strip() for part in event_date.split("..") if part.strip()]
        if not parts:
            return None, None
        start = _parse_iso_date(parts[0])
        end = _parse_iso_date(parts[-1])
    else:
        start = _parse_iso_date(event_date.strip())
        if event_end_date:
            end = _parse_iso_date(event_end_date.strip())
        else:
            end = start

    if start and end and end < start:
        start, end = end, start

    return start, end


def _event_date_matches_holiday(
    record: Any,
    event_date: str | None,
    event_end_date: str | None,
    tolerance_days: int | None,
) -> bool:
    if record is None:
        return False

    start, end = _event_date_range(event_date, event_end_date)
    if start is None and end is None:
        return False

    tolerance = tolerance_days if tolerance_days is not None else 0
    if tolerance < 0:
        tolerance = 0

    event_start = start or end
    event_end = end or start or event_start
    if event_start is None or event_end is None:
        return False
    if event_end < event_start:
        event_start, event_end = event_end, event_start

    years: set[int] = set()
    years.add(event_start.year)
    years.add(event_end.year)
    expanded_years: set[int] = set()
    for year in years:
        expanded_years.add(year)
        expanded_years.add(year - 1)
        expanded_years.add(year + 1)

    tolerance_delta = timedelta(days=tolerance)

    for year in sorted(expanded_years):
        start_iso, end_iso = _holiday_date_range(record, year)
        if not start_iso and not end_iso:
            continue
        holiday_start = _parse_iso_date(start_iso)
        holiday_end = _parse_iso_date(end_iso)
        if holiday_start is None and holiday_end is None:
            continue
        if holiday_start is None:
            holiday_start = holiday_end
        if holiday_end is None:
            holiday_end = holiday_start
        if holiday_start is None or holiday_end is None:
            continue
        if holiday_end < holiday_start:
            holiday_start, holiday_end = holiday_end, holiday_start

        window_start = holiday_start - tolerance_delta
        window_end = holiday_end + tolerance_delta
        if event_end >= window_start and event_start <= window_end:
            return True

    return False


async def persist_event_and_pages(
    draft: EventDraft,
    photos: list[str],
    db: Database,
    source_post_url: str | None = None,
    *,
    holiday_tolerance_days: int | None = None,
    wait_for_telegraph_url: bool = True,
    producer_ordinal: int | None = None,
    source_disposition: str | None = None,
    source_parse_version: str | None = None,
    source_evidence_complete: bool | None = None,
    source_verification_reasons: Sequence[str] | None = None,
) -> PersistResult:
    """Store a drafted event and produce all public artefacts.

    The helper encapsulates the legacy import pipeline used by the bot.  It
    persists the event to the database, uploads images to Catbox and creates the
    Telegraph page, generates an ICS file and posts it to the asset channel.
    Links to these artefacts are returned in :class:`PersistResult`.
    """
    from datetime import datetime
    from models import Event, Festival
    from sqlalchemy import select
    import sys

    main_mod = sys.modules.get("main") or sys.modules.get("__main__")
    if main_mod is None:  # pragma: no cover - defensive
        raise RuntimeError("main module not found")
    rebuild_fest_nav_if_changed = main_mod.rebuild_fest_nav_if_changed
    normalize_event_type = getattr(main_mod, "normalize_event_type", None)

    from smart_event_update import (
        EventCandidate,
        PosterCandidate,
        SmartUpdateTerminalOutcome,
        smart_event_update,
    )

    posters = _build_smart_update_posters(
        draft,
        photos=photos,
        poster_cls=PosterCandidate,
    )

    normalized_event_type = (
        normalize_event_type(
            draft.title or "",
            f"{draft.description or ''}\n{draft.source_text or ''}".strip(),
            draft.event_type,
        )
        if callable(normalize_event_type)
        else (draft.event_type or None)
    )

    vk_source_chat_id, vk_source_message_id = _vk_wall_source_ids_from_url(source_post_url)

    ticket_link = _sanitize_vk_ticket_link_for_source(
        (draft.links[0] if draft.links else None),
        source_post_url,
    )

    candidate = EventCandidate(
        source_type="vk",
        source_url=source_post_url,
        source_chat_id=vk_source_chat_id,
        source_message_id=vk_source_message_id,
        source_text=draft.source_text or draft.title,
        title=draft.title,
        # Never default missing date/time to "today" or "00:00": it creates pseudo-events.
        # Let Smart Update reject/skip incomplete drafts (vk_auto_queue treats it as an expected skip).
        date=draft.date or None,
        time=draft.time or "",
        time_is_default=bool(getattr(draft, "time_is_default", False)),
        end_date=draft.end_date or None,
        festival=draft.festival or None,
        location_name=draft.venue or "",
        location_address=draft.location_address or None,
        city=draft.city or None,
        ticket_link=ticket_link,
        ticket_price_min=draft.ticket_price_min,
        ticket_price_max=draft.ticket_price_max,
        event_type=normalized_event_type,
        emoji=draft.emoji or None,
        is_free=bool(draft.is_free),
        pushkin_card=bool(draft.pushkin_card),
        search_digest=draft.search_digest,
        raw_excerpt=draft.description or "",
        posters=posters,
        organizer_names=_curated_vk_event_organizers(vk_source_chat_id),
        producer_ordinal=producer_ordinal,
        source_disposition=source_disposition,
        source_parse_version=source_parse_version,
        source_evidence_complete=source_evidence_complete,
        source_verification_reasons=list(source_verification_reasons or ()),
    )

    update_result = await smart_event_update(
        db,
        candidate,
        check_source_url=False,
    )
    if not update_result.is_accepted:
        return PersistResult(
            event_id=None,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date=str(draft.date or ""),
            event_end_date=draft.end_date or None,
            event_time=str(draft.time or ""),
            event_type=draft.event_type or None,
            is_free=bool(draft.is_free),
            smart_result=update_result,
        )
    if update_result.event_id is None:
        raise RuntimeError(
            "accepted smart_update result returned no event_id: "
            f"outcome={update_result.outcome.value} reason={update_result.reason}"
        )
    async with db.get_session() as session:
        saved = (
            await session.get(Event, update_result.event_id)
        )
    if saved is None:
        raise RuntimeError(
            "smart_update failed to persist event: "
            f"event_id={getattr(update_result, 'event_id', None)} "
            f"outcome={update_result.outcome.value} reason={update_result.reason}"
        )
    text_length = len(saved.title or "") + len(saved.description or "") + len(saved.source_text or "")
    logging.info(
        "event_topics_classify eid=%s text_len=%d topics=%s manual=%s",
        saved.id,
        text_length,
        list(saved.topics or []),
        bool(saved.topics_manual),
    )
    logging.info(
        "persist_event_and_pages: source_post_url=%s", saved.source_post_url
    )

    nav_update_needed = False
    if (
        update_result.outcome is not SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        and saved.festival
    ):
        parts = [p.strip() for p in (saved.date or "").split("..") if p.strip()]
        start_str = parts[0] if parts else None
        end_str = parts[-1] if len(parts) > 1 else None
        explicit_end = bool(saved.end_date) or len(parts) > 1
        if not end_str:
            end_str = saved.end_date or start_str
        if start_str or end_str:
            async with db.get_session() as session:
                res = await session.execute(
                    select(Festival).where(Festival.name == saved.festival)
                )
                festival = res.scalar_one_or_none()
                if festival is not None:
                    changed = False
                    if start_str and explicit_end:
                        if (
                            festival.start_date is None
                            or start_str < festival.start_date
                        ):
                            festival.start_date = start_str
                            changed = True
                    if end_str:
                        if (
                            festival.end_date is None
                            or (explicit_end and end_str > festival.end_date)
                        ):
                            festival.end_date = end_str
                            changed = True
                    if changed:
                        session.add(festival)
                        await session.commit()
                        nav_update_needed = True
    if nav_update_needed:
        await rebuild_fest_nav_if_changed(db)
    if wait_for_telegraph_url:
        # Wait for Telegraph URL to become available (async job). Callers that
        # already run inline Telegraph jobs can skip this extra wait.
        start_wait = time.time()
        for _ in range(20):  # Wait up to 10 seconds
            async with db.get_session() as session:
                saved = await session.get(Event, saved.id)
            if saved.telegraph_url:
                elapsed = time.time() - start_wait
                logging.info("persist_event_and_pages: telegraph_url appeared after %.2fs", elapsed)
                break
            await asyncio.sleep(0.5)

    return PersistResult(
        event_id=saved.id,
        telegraph_url=saved.telegraph_url or "",
        ics_supabase_url=saved.ics_url or "",
        ics_tg_url=saved.ics_post_url or "",
        event_date=saved.date,
        event_end_date=saved.end_date,
        event_time=saved.time,
        event_type=saved.event_type,
        is_free=bool(saved.is_free),
        smart_created=(
            update_result.outcome is SmartUpdateTerminalOutcome.CREATED
        ),
        smart_merged=(
            update_result.outcome is SmartUpdateTerminalOutcome.MERGED
        ),
        smart_added_posters=int(getattr(update_result, "added_posters", 0) or 0),
        smart_result=update_result,
    )


def _build_smart_update_posters(
    draft: EventDraft,
    *,
    photos: Sequence[str] | None,
    poster_cls: type,
) -> list[object]:
    """Build Smart Update poster candidates with VK URL fallback.

    Catbox can be disabled in tests/live runs. In this case we still want event
    posters rendered on Telegraph by passing original VK media URLs through the
    same `catbox_url` field consumed by the unified event-page pipeline.
    """
    poster_urls = [m.catbox_url for m in draft.poster_media if m.catbox_url]
    photo_urls = poster_urls or (
        list(photos or []) if bool(getattr(draft, "allow_raw_photo_fallback", True)) else []
    )
    posters: list[object] = []
    for idx, item in enumerate(draft.poster_media):
        url = (item.catbox_url or "").strip()
        if not url and idx < len(photo_urls):
            url = str(photo_urls[idx] or "").strip()
        supabase_url = (item.supabase_url or "").strip() or (
            url if is_supabase_storage_url(url) else None
        )
        catbox_url = url if url and not is_supabase_storage_url(url) else None
        posters.append(
            poster_cls(
                catbox_url=catbox_url,
                supabase_url=supabase_url,
                sha256=item.digest,
                phash=getattr(item, "phash", None),
                ocr_text=item.ocr_text,
                ocr_title=item.ocr_title,
                prompt_tokens=int(getattr(item, "prompt_tokens", 0) or 0),
                completion_tokens=int(getattr(item, "completion_tokens", 0) or 0),
                total_tokens=int(getattr(item, "total_tokens", 0) or 0),
            )
        )
    if not posters and photo_urls:
        posters = [
            poster_cls(
                catbox_url=(u if u and not is_supabase_storage_url(u) else None),
                supabase_url=(u if is_supabase_storage_url(u) else None),
            )
            for u in (str(url).strip() for url in photo_urls)
        ]
    return posters


async def process_event(
    text: str,
    photos: list[str] | None = None,
    *,
    source_name: str | None = None,
    location_hint: str | None = None,
    default_time: str | None = None,
    operator_extra: str | None = None,
    db: Database,
) -> list[PersistResult]:
    """Process VK post text into an event and track processing time."""
    start = time.perf_counter()
    from sqlalchemy import select
    from models import Festival

    async with db.get_session() as session:
        res_f = await session.execute(select(Festival.name))
        festival_names = [row[0] for row in res_f.fetchall()]
    drafts, _ = await build_event_drafts(
        text,
        photos=photos or [],
        source_name=source_name,
        location_hint=location_hint,
        default_time=default_time,
        operator_extra=operator_extra,
        festival_names=festival_names,
        festival_hint=False,
        db=db,
    )
    results: list[PersistResult] = []
    for draft in drafts:
        results.append(
            await persist_event_and_pages(draft, photos or [], db)
        )
    duration = time.perf_counter() - start
    global processing_time_seconds_total
    processing_time_seconds_total += duration
    try:
        import sys

        main_mod = sys.modules.get("main") or sys.modules.get("__main__")
        if main_mod is not None:
            main_mod.vk_import_duration_sum += duration
            main_mod.vk_import_duration_count += 1
            for bound in main_mod.vk_import_duration_buckets:
                if duration <= bound:
                    main_mod.vk_import_duration_buckets[bound] += 1
                    break
    except Exception:
        pass
    return results


def _vk_packet_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _vk_source_revision_payload(post: dict[str, Any]) -> dict[str, Any]:
    """Return semantic source bytes excluding volatile popularity counters."""

    from vk_source_envelope import is_vk_source_envelope, vk_source_semantic_projection

    if is_vk_source_envelope(post):
        return vk_source_semantic_projection(post)
    return {
        "text": str(post.get("text") or ""),
        "photos": list(post.get("photos") or ()),
        "attachments": post.get("attachments") or (),
        "copy_history": post.get("copy_history") or (),
    }


async def _persist_vk_source_packet(
    db: Database,
    *,
    group_id: int,
    owner_type: str,
    post: dict[str, Any],
    source_url: str,
    keyword_hints: Sequence[str],
    date_hints: Sequence[str],
    event_ts_hint: int | None,
    admission_decision: VKCrawlAdmissionDecision | None = None,
    apply_admission_gate: bool = False,
) -> tuple[int, bool]:
    """Durably append one fetched revision and point the inbox at it.

    The caller must not advance its crawl cursor unless this succeeds for every
    fetched in-horizon post.  Exact unchanged revisions reuse their immutable
    packet; changed revisions append a new row and re-open the inbox.
    """

    from vk_source_envelope import (
        VK_SOURCE_ENVELOPE_VERSION,
        is_vk_source_envelope,
        sanitize_vk_source_value,
        vk_source_envelope_attachment_metadata,
        vk_source_envelope_replayability,
        vk_source_packet_hashes,
    )

    if is_vk_source_envelope(post):
        sanitized_post = sanitize_vk_source_value(post)
        if isinstance(sanitized_post, dict):
            post = sanitized_post
    raw_payload_json = _vk_packet_json(post)
    revision_payload_json = _vk_packet_json(_vk_source_revision_payload(post))
    if is_vk_source_envelope(post):
        payload_hash, revision_hash = vk_source_packet_hashes(post)
    else:
        payload_hash = hashlib.sha256(raw_payload_json.encode("utf-8")).hexdigest()
        revision_hash = hashlib.sha256(
            revision_payload_json.encode("utf-8")
        ).hexdigest()
    post_id = int(post["post_id"])
    published_at = int(post["date"])
    raw_text = str(post.get("text") or "")
    envelope_version: int | None = None
    capture_complete = False
    replayability = "replayable_legacy_incomplete"
    if is_vk_source_envelope(post):
        envelope_version = VK_SOURCE_ENVELOPE_VERSION
        replayability = vk_source_envelope_replayability(post)
        capture_complete = replayability == "replayable_lossless"
        attachment_metadata = vk_source_envelope_attachment_metadata(post)
    else:
        attachment_metadata = {
            "photos": list(post.get("photos") or ()),
            "attachments": post.get("attachments") or (),
            "copy_history": post.get("copy_history") or (),
        }
    keyword_json = _vk_packet_json(list(keyword_hints))
    date_json = _vk_packet_json(list(date_hints))
    if OCR_PENDING_SENTINEL in keyword_hints:
        inbox_matched_kw = OCR_PENDING_SENTINEL
    elif HISTORY_MATCHED_KEYWORD in keyword_hints:
        inbox_matched_kw = HISTORY_MATCHED_KEYWORD
    else:
        inbox_matched_kw = ",".join(
            value for value in keyword_hints if not str(value).startswith("hint:")
        )

    _require_vk_crawl_storage_headroom(db)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id FROM vk_source_packet
            WHERE source_type='vk' AND owner_id=? AND post_id=? AND source_revision_hash=?
            """,
            (int(group_id), post_id, revision_hash),
        )
        row = await cur.fetchone()
        is_new = row is None
        if row is None:
            cur = await conn.execute(
                """
                SELECT COALESCE(MAX(revision), 0) + 1
                FROM vk_source_packet
                WHERE source_type='vk' AND owner_id=? AND post_id=?
                """,
                (int(group_id), post_id),
            )
            revision_row = await cur.fetchone()
            revision = int((revision_row[0] if revision_row else 1) or 1)
            cur = await conn.execute(
                """
                INSERT INTO vk_source_packet(
                    source_type, owner_id, owner_type, post_id, revision,
                    source_url, published_at, raw_text, raw_payload_json,
                    attachment_metadata_json, envelope_version, capture_complete,
                    evidence_replayability, payload_hash, source_revision_hash,
                    discovery_keyword_hints_json, discovered_date_hints_json,
                    event_ts_hint, ocr_status, llm_status, status
                ) VALUES('vk',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'pending','pending','pending')
                """,
                (
                    int(group_id), owner_type, post_id, revision, source_url,
                    published_at, raw_text, raw_payload_json,
                    _vk_packet_json(attachment_metadata), envelope_version,
                    1 if capture_complete else 0, replayability,
                    payload_hash, revision_hash,
                    keyword_json, date_json, event_ts_hint,
                ),
            )
            packet_id = int(cur.lastrowid)
        else:
            packet_id = int(row[0])

        if apply_admission_gate and admission_decision is None:
            # Exact legacy revisions may predate this gate. Preserve their
            # existing inbox/terminal state instead of silently re-enqueuing.
            await conn.commit()
            return packet_id, is_new

        if apply_admission_gate and admission_decision is not None:
            if admission_decision.reason == "legacy_pending_preserved":
                # Normal crawling must neither trigger an unbounded historical
                # LLM drain nor make the row ineligible for the explicit
                # bounded requalification command.
                await conn.commit()
                return packet_id, is_new
            receipt_json = _vk_packet_json(admission_decision.receipt())
            typed_reason = f"VK_ADMISSION_{admission_decision.outcome}"
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET admission_status=?,admission_reason=?,admission_receipt_json=?,
                    status=CASE
                        WHEN ?=1 AND ?=1 THEN 'pending'
                        WHEN ?=0 THEN 'completed'
                        ELSE status END,
                    last_typed_reason=CASE
                        WHEN ?=0 THEN ? ELSE last_typed_reason END,
                    terminal_carrier_outcome=CASE
                        WHEN ?=0 THEN 'ADMISSION_REJECTED'
                        WHEN ?=1 AND ?=1 THEN NULL
                        ELSE terminal_carrier_outcome END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    "admitted" if admission_decision.admitted else "rejected",
                    admission_decision.reason,
                    receipt_json,
                    1 if admission_decision.admitted else 0,
                    1 if is_new else 0,
                    1 if admission_decision.admitted else 0,
                    1 if admission_decision.admitted else 0,
                    typed_reason,
                    1 if admission_decision.admitted else 0,
                    1 if admission_decision.admitted else 0,
                    1 if is_new else 0,
                    packet_id,
                ),
            )
            if not admission_decision.admitted:
                # A changed revision that is confidently past/non-event must
                # also leave the selectable queue. New rejected packets need no
                # inbox row at all; the immutable packet is the terminal receipt.
                await conn.execute(
                    """
                    UPDATE vk_inbox
                    SET date=?,text=?,matched_kw=?,has_date=?,event_ts_hint=?,
                        owner_type=?,source_packet_id=?,status='rejected',
                        next_attempt_at=NULL,locked_by=NULL,locked_at=NULL,
                        review_batch=NULL,last_typed_reason=?
                    WHERE group_id=? AND post_id=?
                    """,
                    (
                        published_at,
                        raw_text,
                        inbox_matched_kw,
                        1 if date_hints else 0,
                        event_ts_hint,
                        owner_type,
                        packet_id,
                        typed_reason,
                        int(group_id),
                        post_id,
                    ),
                )
                await conn.commit()
                return packet_id, is_new

        # A changed admitted revision must re-enter automatic parsing. Exact
        # replay keeps the current queue terminal/due state and therefore
        # cannot duplicate a successful provider call.
        await conn.execute(
            """
            INSERT INTO vk_inbox(
                group_id,post_id,date,text,matched_kw,has_date,event_ts_hint,
                status,owner_type,source_packet_id,next_attempt_at
            ) VALUES(?,?,?,?,?,?,?,'pending',?,?,CURRENT_TIMESTAMP)
            ON CONFLICT(group_id,post_id) DO UPDATE SET
                date=excluded.date,
                text=excluded.text,
                matched_kw=excluded.matched_kw,
                has_date=excluded.has_date,
                event_ts_hint=excluded.event_ts_hint,
                owner_type=excluded.owner_type,
                source_packet_id=excluded.source_packet_id,
                status=CASE
                    WHEN COALESCE(vk_inbox.source_packet_id,0)<>excluded.source_packet_id
                    THEN 'pending' ELSE vk_inbox.status END,
                next_attempt_at=CASE
                    WHEN COALESCE(vk_inbox.source_packet_id,0)<>excluded.source_packet_id
                    THEN CURRENT_TIMESTAMP ELSE vk_inbox.next_attempt_at END,
                locked_by=CASE
                    WHEN COALESCE(vk_inbox.source_packet_id,0)<>excluded.source_packet_id
                    THEN NULL ELSE vk_inbox.locked_by END,
                locked_at=CASE
                    WHEN COALESCE(vk_inbox.source_packet_id,0)<>excluded.source_packet_id
                    THEN NULL ELSE vk_inbox.locked_at END
            """,
            (
                int(group_id), post_id, published_at, raw_text,
                inbox_matched_kw, 1 if date_hints else 0,
                event_ts_hint, owner_type, packet_id,
            ),
        )
        await conn.commit()
    return packet_id, is_new


async def _schedule_vk_crawl_continuation(
    db: Database,
    *,
    group_id: int,
    owner_type: str,
    since_ts: int,
    offset: int,
    horizon_ts: int,
    scan_mode: str,
    page_size: int,
    original_cursor_ts: int,
    original_cursor_post_id: int,
    reason: str,
    last_page_fingerprint: str | None = None,
    deepest_page_ts: int | None = None,
    deepest_page_post_id: int | None = None,
) -> None:
    continuation_key = hashlib.sha256(
        _vk_packet_json(
            {
                "source_type": "vk",
                "owner_id": int(group_id),
                "owner_type": owner_type,
                "scan_mode": scan_mode,
                "since_ts": int(since_ts),
                "horizon_ts": int(horizon_ts),
                "original_cursor_ts": int(original_cursor_ts),
                "original_cursor_post_id": int(original_cursor_post_id),
            }
        ).encode("utf-8")
    ).hexdigest()
    async with db.raw_conn() as conn:
        # Adopt an older pre-key row instead of creating parallel work for the
        # same immutable crawl boundary after its mutable offset has advanced.
        existing = await conn.execute(
            """
            SELECT id,status,last_typed_reason FROM vk_crawl_continuation
            WHERE source_type='vk' AND owner_id=? AND COALESCE(owner_type,'group')=?
              AND COALESCE(scan_mode,'incremental')=? AND since_ts=? AND horizon_ts=?
              AND COALESCE(original_cursor_ts,0)=?
              AND COALESCE(original_cursor_post_id,0)=?
            ORDER BY id LIMIT 1
            """,
            (
                int(group_id), owner_type, scan_mode, int(since_ts), int(horizon_ts),
                int(original_cursor_ts), int(original_cursor_post_id),
            ),
        )
        existing_row = await existing.fetchone()
        if existing_row is not None:
            await conn.execute(
                """
                UPDATE vk_crawl_continuation
                SET continuation_key=COALESCE(continuation_key,?),
                    deepest_page_ts=COALESCE(deepest_page_ts,?),
                    deepest_page_post_id=COALESCE(deepest_page_post_id,?),
                    status=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN 'retry' ELSE status END,
                    next_attempt_at=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN CURRENT_TIMESTAMP ELSE next_attempt_at END,
                    completed_at=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE completed_at END,
                    lease_owner=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE lease_owner END,
                    locked_by=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE locked_by END,
                    lease_expires_at=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE lease_expires_at END,
                    locked_at=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE locked_at END,
                    run_id=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN NULL ELSE run_id END,
                    last_typed_reason=CASE
                        WHEN status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'
                        THEN 'LEGACY_EXACT_PAGE_REPLAY_REOPENED'
                        ELSE last_typed_reason END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    continuation_key,
                    int(deepest_page_ts) if deepest_page_ts is not None else None,
                    (
                        int(deepest_page_post_id)
                        if deepest_page_post_id is not None
                        else None
                    ),
                    int(existing_row[0]),
                ),
            )
            await conn.commit()
            return
        await conn.execute(
            """
            INSERT OR IGNORE INTO vk_crawl_continuation(
                source_type,owner_id,owner_type,continuation_key,scan_mode,page_size,since_ts,offset,
                horizon_ts,original_cursor_ts,original_cursor_post_id,reason,status,
                last_page_fingerprint,deepest_page_ts,deepest_page_post_id
            ) VALUES('vk',?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?)
            """,
            (
                int(group_id), owner_type, continuation_key, scan_mode, max(1, int(page_size)),
                int(since_ts), int(offset), int(horizon_ts),
                int(original_cursor_ts), int(original_cursor_post_id), reason,
                last_page_fingerprint,
                int(deepest_page_ts) if deepest_page_ts is not None else None,
                (
                    int(deepest_page_post_id)
                    if deepest_page_post_id is not None
                    else None
                ),
            ),
        )
        await conn.commit()


@dataclass(frozen=True)
class VKCrawlContinuationClaim:
    id: int
    owner_id: int
    owner_type: str
    scan_mode: str
    page_size: int
    since_ts: int
    offset: int
    horizon_ts: int
    original_cursor_ts: int
    original_cursor_post_id: int
    attempts: int
    lease_owner: str
    run_id: str
    last_page_fingerprint: str | None
    deepest_page_ts: int | None
    deepest_page_post_id: int | None
    stale_recovered: bool = False


class VKCrawlContinuationLeaseLost(RuntimeError):
    """The continuation no longer belongs to this worker/run."""


async def _open_vk_continuation_conn(db: Database) -> aiosqlite.Connection:
    """Open a dedicated connection so BEGIN IMMEDIATE cannot interleave.

    ``Database.raw_conn`` intentionally reuses one connection. A queue claim is
    a cross-process synchronization primitive, so it must own its transaction
    and connection from BEGIN through COMMIT.
    """

    conn = await aiosqlite.connect(db.path, timeout=db._sqlite_timeout_sec())
    await conn.execute(f"PRAGMA busy_timeout={db._sqlite_busy_timeout_ms()}")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def _claim_vk_crawl_continuation(
    db: Database,
    *,
    lease_owner: str,
    run_id: str,
    lease_seconds: int,
    excluded_ids: Sequence[int] = (),
) -> VKCrawlContinuationClaim | None:
    """Atomically claim one due row, including an expired running lease."""

    owner = str(lease_owner or "").strip()
    run = str(run_id or "").strip()
    if not owner or not run:
        raise ValueError("vk_crawl_continuation_owner_and_run_required")
    lease_seconds = max(30, min(int(lease_seconds), 3600))
    excluded = tuple(int(value) for value in excluded_ids)
    exclusion_sql = ""
    params: list[Any] = []
    if excluded:
        exclusion_sql = f" AND id NOT IN ({','.join('?' for _ in excluded)})"
        params.extend(excluded)
    conn = await _open_vk_continuation_conn(db)
    try:
        await conn.execute("BEGIN IMMEDIATE")
        cursor = await conn.execute(
            f"""
            SELECT id,owner_id,COALESCE(owner_type,'group'),
                   COALESCE(scan_mode,'incremental'),COALESCE(page_size,30),
                   since_ts,offset,horizon_ts,COALESCE(original_cursor_ts,0),
                   COALESCE(original_cursor_post_id,0),attempts,
                   last_page_fingerprint,status,deepest_page_ts,deepest_page_post_id
            FROM vk_crawl_continuation
            WHERE (
                    (status IN ('pending','retry')
                     AND (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP))
                 OR (status='running'
                     AND (lease_expires_at IS NULL OR lease_expires_at<=CURRENT_TIMESTAMP))
                  )
              {exclusion_sql}
            ORDER BY COALESCE(next_attempt_at,created_at),id
            LIMIT 1
            """,
            tuple(params),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            await conn.commit()
            return None
        stale_recovered = str(row[12]) == "running"
        update = await conn.execute(
            f"""
            UPDATE vk_crawl_continuation
            SET status='running', attempts=attempts+1, lease_owner=?, locked_by=?, run_id=?,
                locked_at=CURRENT_TIMESTAMP,
                lease_expires_at=datetime(CURRENT_TIMESTAMP,'+{lease_seconds} seconds'),
                last_typed_reason=CASE WHEN status='running'
                    THEN 'STALE_LEASE_RECOVERED' ELSE last_typed_reason END,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND (
                    (status IN ('pending','retry')
                     AND (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP))
                 OR (status='running'
                     AND (lease_expires_at IS NULL OR lease_expires_at<=CURRENT_TIMESTAMP))
                  )
            """,
            (owner, owner, run, int(row[0])),
        )
        if update.rowcount != 1:
            await conn.rollback()
            return None
        await conn.commit()
        return VKCrawlContinuationClaim(
            id=int(row[0]), owner_id=int(row[1]), owner_type=str(row[2]),
            scan_mode=str(row[3]), page_size=max(1, int(row[4])),
            since_ts=int(row[5]), offset=int(row[6]), horizon_ts=int(row[7]),
            original_cursor_ts=int(row[8]), original_cursor_post_id=int(row[9]),
            attempts=int(row[10]) + 1, lease_owner=owner, run_id=run,
            last_page_fingerprint=(str(row[11]) if row[11] else None),
            deepest_page_ts=(int(row[13]) if row[13] is not None else None),
            deepest_page_post_id=(int(row[14]) if row[14] is not None else None),
            stale_recovered=stale_recovered,
        )
    except Exception:
        await conn.rollback()
        raise
    finally:
        await conn.close()


async def _continuation_cas_update(
    db: Database,
    claim: VKCrawlContinuationClaim,
    sql_set: str,
    params: Sequence[Any],
) -> None:
    conn = await _open_vk_continuation_conn(db)
    try:
        cursor = await conn.execute(
            f"""
            UPDATE vk_crawl_continuation SET {sql_set},updated_at=CURRENT_TIMESTAMP
            WHERE id=? AND status='running' AND lease_owner=? AND run_id=?
            """,
            (*params, claim.id, claim.lease_owner, claim.run_id),
        )
        await conn.commit()
        if cursor.rowcount != 1:
            raise VKCrawlContinuationLeaseLost(f"continuation_lease_lost:{claim.id}")
    finally:
        await conn.close()


async def _renew_vk_crawl_continuation_lease(
    db: Database,
    claim: VKCrawlContinuationClaim,
    *,
    lease_seconds: int,
) -> None:
    lease_seconds = max(30, min(int(lease_seconds), 3600))
    await _continuation_cas_update(
        db,
        claim,
        f"lease_expires_at=datetime(CURRENT_TIMESTAMP,'+{lease_seconds} seconds')",
        (),
    )


def _vk_continuation_page_fingerprint(page: Sequence[dict[str, Any]]) -> str:
    canonical = _vk_packet_json(list(page))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _vk_continuation_deepest_boundary(
    page: Sequence[dict[str, Any]],
) -> tuple[int, int] | None:
    """Return the oldest durable page key under VK's reverse wall ordering."""

    if not page:
        return None
    return min((int(post["date"]), int(post["post_id"])) for post in page)


def _vk_continuation_retry_reason(exc: BaseException, *, stage: str) -> str:
    status = getattr(exc, "status", None)
    if status == 429:
        return "VK_CRAWL_RATE_LIMITED"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "VK_CRAWL_FETCH_TIMEOUT" if stage == "fetch" else "VK_CRAWL_PERSIST_TIMEOUT"
    if isinstance(exc, (aiohttp.ClientError, OSError)):
        return "VK_CRAWL_TRANSPORT" if stage == "fetch" else "VK_CRAWL_PERSIST_FAILED"
    return "VK_CRAWL_FETCH_FAILED" if stage == "fetch" else "VK_CRAWL_PERSIST_FAILED"


def _vk_continuation_retry_after_seconds(exc: BaseException) -> int | None:
    for name, divisor in (("retry_after_ms", 1000), ("retry_after", 1)):
        raw = getattr(exc, name, None)
        try:
            if raw is not None:
                return max(1, int(float(raw) / divisor))
        except (TypeError, ValueError):
            pass
    headers = getattr(exc, "headers", None)
    if headers:
        try:
            return max(1, int(float(headers.get("Retry-After"))))
        except (AttributeError, TypeError, ValueError):
            pass
    return None


async def _retry_vk_crawl_continuation(
    db: Database,
    claim: VKCrawlContinuationClaim,
    exc: BaseException,
    *,
    stage: str,
) -> int:
    base = max(1, _read_int_env("VK_CRAWL_CONTINUATION_BACKOFF_BASE_SECONDS", 30))
    cap = max(base, _read_int_env("VK_CRAWL_CONTINUATION_BACKOFF_MAX_SECONDS", 3600))
    exponential = base * (2 ** min(max(0, claim.attempts - 1), 16))
    provider_delay = _vk_continuation_retry_after_seconds(exc) or 0
    delay = min(cap, max(exponential, provider_delay))
    reason = _vk_continuation_retry_reason(exc, stage=stage)
    await _continuation_cas_update(
        db,
        claim,
        "status='retry',next_attempt_at=datetime(CURRENT_TIMESTAMP,?),"
        "lease_owner=NULL,locked_by=NULL,lease_expires_at=NULL,locked_at=NULL,run_id=NULL,"
        "last_typed_reason=?",
        (f"+{delay} seconds", reason),
    )
    return delay


async def _defer_vk_crawl_offset_drift(
    db: Database,
    claim: VKCrawlContinuationClaim,
    *,
    next_offset: int,
    fingerprint: str,
    deepest_boundary: tuple[int, int] | None,
    reason: str,
) -> int:
    """Durably rebase a mutable VK offset without claiming false completion."""

    base = max(1, _read_int_env("VK_CRAWL_CONTINUATION_BACKOFF_BASE_SECONDS", 30))
    cap = max(base, _read_int_env("VK_CRAWL_CONTINUATION_BACKOFF_MAX_SECONDS", 3600))
    delay = min(cap, base * (2 ** min(max(0, claim.attempts - 1), 16)))
    deepest_ts = deepest_boundary[0] if deepest_boundary is not None else None
    deepest_post_id = deepest_boundary[1] if deepest_boundary is not None else None
    try:
        await _continuation_cas_update(
            db,
            claim,
            "status='retry',offset=?,next_attempt_at=datetime(CURRENT_TIMESTAMP,?),"
            "lease_owner=NULL,locked_by=NULL,lease_expires_at=NULL,locked_at=NULL,run_id=NULL,"
            "last_page_fingerprint=?,deepest_page_ts=?,deepest_page_post_id=?,"
            "last_typed_reason=?",
            (
                max(0, int(next_offset)),
                f"+{delay} seconds",
                fingerprint,
                deepest_ts,
                deepest_post_id,
                reason,
            ),
        )
    except aiosqlite.IntegrityError:
        # New producers have one stable continuation_key, so supported rows
        # cannot collide while their mutable offset advances. A pre-key legacy
        # duplicate may still occupy the target offset under the historical
        # UNIQUE(source,owner,since,offset,horizon). Fail durable/observable at
        # the current offset rather than losing the lease into stale-running or
        # declaring completion; the duplicate row remains independently due.
        await _continuation_cas_update(
            db,
            claim,
            "status='retry',next_attempt_at=datetime(CURRENT_TIMESTAMP,?),"
            "lease_owner=NULL,locked_by=NULL,lease_expires_at=NULL,locked_at=NULL,run_id=NULL,"
            "last_page_fingerprint=?,deepest_page_ts=?,deepest_page_post_id=?,"
            "last_typed_reason='OFFSET_DRIFT_COLLISION'",
            (f"+{delay} seconds", fingerprint, deepest_ts, deepest_post_id),
        )
    return delay


async def _persist_vk_continuation_page(
    db: Database,
    claim: VKCrawlContinuationClaim,
    page: Sequence[dict[str, Any]],
    *,
    lease_seconds: int,
) -> tuple[int, int, int]:
    """Persist the whole fetched page before any durable offset movement."""

    added = duplicates = rejected = 0
    get_tz_offset = require_main_attr("get_tz_offset")
    await get_tz_offset(db)
    local_tz = require_main_attr("LOCAL_TZ")
    async with db.raw_conn() as conn:
        source_row = await (await conn.execute(
            "SELECT default_time FROM vk_source WHERE group_id=? LIMIT 1",
            (claim.owner_id,),
        )).fetchone()
    default_time = source_row[0] if source_row else None
    candidates = [
        _vk_crawl_admission_candidate(
            group_id=claim.owner_id,
            owner_type=claim.owner_type,
            post=post,
            default_time=default_time,
            tz=local_tz,
        )
        for post in page
    ]

    # Preserve the raw-first contract before any provider call.  Admission can
    # take seconds or fail ambiguously; the fetched page must already be
    # durable while its continuation offset is still unchanged.
    raw_results: list[tuple[int, bool]] = []
    for post, candidate in zip(page, candidates):
        await _renew_vk_crawl_continuation_lease(
            db, claim, lease_seconds=lease_seconds
        )
        raw_results.append(await _persist_vk_source_packet(
            db,
            group_id=claim.owner_id,
            owner_type=claim.owner_type,
            post=post,
            source_url=candidate.source_url,
            keyword_hints=candidate.keyword_hints,
            date_hints=candidate.date_hints,
            event_ts_hint=candidate.event_ts_hint,
            admission_decision=None,
            apply_admission_gate=True,
        ))
    unique_candidates = list({candidate.key: candidate for candidate in candidates}.values())
    decisions = await _resolve_vk_crawl_admissions(db, unique_candidates, tz=local_tz)
    candidates_by_key = {candidate.key: candidate for candidate in candidates}

    for index, post in enumerate(page):
        await _renew_vk_crawl_continuation_lease(
            db, claim, lease_seconds=lease_seconds
        )
        post_id = int(post["post_id"])
        candidate = candidates_by_key[f"{claim.owner_id}:{post_id}"]
        admission = decisions[candidate.key]
        await _persist_vk_source_packet(
            db,
            group_id=claim.owner_id,
            owner_type=claim.owner_type,
            post=post,
            source_url=candidate.source_url,
            keyword_hints=candidate.keyword_hints,
            date_hints=candidate.date_hints,
            event_ts_hint=candidate.event_ts_hint,
            admission_decision=admission,
            apply_admission_gate=True,
        )
        _packet_id, is_new = raw_results[index]
        if is_new:
            if admission.admitted:
                added += 1
            else:
                rejected += 1
        else:
            duplicates += 1
    return added, duplicates, rejected


async def process_vk_crawl_continuations(
    db: Database,
    *,
    max_jobs: int = 2,
    max_pages_per_job: int = 3,
    lease_seconds: int = 300,
    worker_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, int]:
    """Consume due durable VK crawl pages with bounded, idempotent work.

    The canonical ``vk_crawl_cursor`` is deliberately untouched. The stored
    offset advances only after every post in the fetched page has reached the
    immutable raw packet ledger.
    """

    max_jobs = max(1, min(int(max_jobs), 25))
    max_pages_per_job = max(1, min(int(max_pages_per_job), 25))
    lease_seconds = max(30, min(int(lease_seconds), 3600))
    worker = str(worker_id or f"vk-continuation:{os.getpid()}:{uuid4().hex[:8]}")
    invocation_run = str(run_id or uuid4().hex)
    result = {
        "claimed": 0, "pages": 0, "posts": 0, "added": 0,
        "duplicates": 0, "completed": 0, "retried": 0,
        "admission_rejected": 0,
        "stale_recovered": 0, "lease_lost": 0, "rebased": 0,
    }
    processed_ids: list[int] = []
    vk_wall_since = require_main_attr("vk_wall_since")
    _require_vk_crawl_storage_headroom(db)

    for job_index in range(max_jobs):
        _require_vk_crawl_storage_headroom(db)
        claim = await _claim_vk_crawl_continuation(
            db,
            lease_owner=worker,
            run_id=f"{invocation_run}:{job_index}",
            lease_seconds=lease_seconds,
            excluded_ids=processed_ids,
        )
        if claim is None:
            break
        processed_ids.append(claim.id)
        result["claimed"] += 1
        if claim.stale_recovered:
            result["stale_recovered"] += 1
        current_offset = claim.offset
        previous_fingerprint = claim.last_page_fingerprint
        deepest_boundary = (
            (claim.deepest_page_ts, claim.deepest_page_post_id)
            if claim.deepest_page_ts is not None
            and claim.deepest_page_post_id is not None
            else None
        )

        for page_index in range(max_pages_per_job):
            try:
                await _renew_vk_crawl_continuation_lease(
                    db, claim, lease_seconds=lease_seconds
                )
                _require_vk_crawl_storage_headroom(db)
                page = await vk_wall_since(
                    claim.owner_id,
                    claim.since_ts,
                    count=claim.page_size,
                    offset=current_offset,
                    owner_type=claim.owner_type,
                )
            except VKCrawlContinuationLeaseLost:
                result["lease_lost"] += 1
                break
            except Exception as exc:
                await _retry_vk_crawl_continuation(db, claim, exc, stage="fetch")
                result["retried"] += 1
                logging.warning(
                    "vk.crawl.continuation retry id=%s stage=fetch offset=%s",
                    claim.id, current_offset, exc_info=True,
                )
                break

            page = list(page or ())
            fingerprint = _vk_continuation_page_fingerprint(page)
            try:
                added, duplicates, admission_rejected = await _persist_vk_continuation_page(
                    db, claim, page, lease_seconds=lease_seconds
                )
            except VKCrawlContinuationLeaseLost:
                result["lease_lost"] += 1
                break
            except Exception as exc:
                await _retry_vk_crawl_continuation(db, claim, exc, stage="persist")
                result["retried"] += 1
                logging.warning(
                    "vk.crawl.continuation retry id=%s stage=persist offset=%s",
                    claim.id, current_offset, exc_info=True,
                )
                break

            result["pages"] += 1
            result["posts"] += len(page)
            result["added"] += added
            result["duplicates"] += duplicates
            result["admission_rejected"] += admission_rejected

            dates = [int(post["date"]) for post in page]
            page_deepest = _vk_continuation_deepest_boundary(page)
            made_deeper_progress = bool(
                page_deepest is not None
                and (
                    deepest_boundary is None
                    or page_deepest < deepest_boundary
                )
            )
            effective_deepest = deepest_boundary
            if page_deepest is not None and (
                effective_deepest is None or page_deepest < effective_deepest
            ):
                effective_deepest = page_deepest
            cursor_overlap = claim.scan_mode == "incremental" and any(
                int(post["date"]) < claim.original_cursor_ts
                or (
                    int(post["date"]) == claim.original_cursor_ts
                    and int(post["post_id"]) <= claim.original_cursor_post_id
                )
                for post in page
            )
            horizon_reached = (
                claim.scan_mode == "backfill"
                and bool(dates)
                and min(dates) < claim.horizon_ts
            )
            terminal_reason: str | None = None
            if not page:
                terminal_reason = "EMPTY_PAGE"
            elif len(page) < claim.page_size:
                terminal_reason = "SHORT_PAGE"
            elif horizon_reached:
                terminal_reason = "HORIZON_REACHED"
            elif cursor_overlap:
                terminal_reason = "ORIGINAL_CURSOR_OVERLAP"

            if terminal_reason:
                await _continuation_cas_update(
                    db,
                    claim,
                    "status='done',lease_owner=NULL,locked_by=NULL,lease_expires_at=NULL,"
                    "locked_at=NULL,run_id=NULL,completed_at=CURRENT_TIMESTAMP,"
                    "last_page_fingerprint=?,deepest_page_ts=?,deepest_page_post_id=?,"
                    "last_typed_reason=?",
                    (
                        fingerprint,
                        effective_deepest[0] if effective_deepest else None,
                        effective_deepest[1] if effective_deepest else None,
                        terminal_reason,
                    ),
                )
                result["completed"] += 1
                break

            next_offset = current_offset + claim.page_size
            same_full_page = bool(
                previous_fingerprint and fingerprint == previous_fingerprint
            )
            duplicate_full_page = bool(page) and duplicates == len(page)
            boundary_not_deeper = bool(
                page_deepest is not None
                and deepest_boundary is not None
                and not made_deeper_progress
            )
            if same_full_page or duplicate_full_page or boundary_not_deeper:
                # VK wall offsets are relative to a mutable head. A full page
                # already at/above the deepest durable boundary means head
                # insertions displaced our absolute offset; moving it by one
                # full page deterministically consumes that drift. It is never
                # evidence that the older tail ended.
                reason = "OFFSET_DRIFT" if (
                    same_full_page or boundary_not_deeper
                ) else "NO_PROGRESS"
                await _defer_vk_crawl_offset_drift(
                    db,
                    claim,
                    next_offset=next_offset,
                    fingerprint=fingerprint,
                    deepest_boundary=effective_deepest,
                    reason=reason,
                )
                result["retried"] += 1
                result["rebased"] += 1
                break

            current_offset = next_offset
            previous_fingerprint = fingerprint
            deepest_boundary = effective_deepest
            if page_index + 1 >= max_pages_per_job:
                await _continuation_cas_update(
                    db,
                    claim,
                    "status='pending',offset=?,next_attempt_at=CURRENT_TIMESTAMP,"
                    "lease_owner=NULL,locked_by=NULL,lease_expires_at=NULL,locked_at=NULL,run_id=NULL,"
                    "last_page_fingerprint=?,deepest_page_ts=?,deepest_page_post_id=?,"
                    "last_typed_reason='BOUNDED_YIELD'",
                    (
                        current_offset,
                        fingerprint,
                        deepest_boundary[0] if deepest_boundary else None,
                        deepest_boundary[1] if deepest_boundary else None,
                    ),
                )
                break
            await _continuation_cas_update(
                db,
                claim,
                f"offset=?,last_page_fingerprint=?,deepest_page_ts=?,deepest_page_post_id=?,"
                f"lease_expires_at=datetime(CURRENT_TIMESTAMP,'+{lease_seconds} seconds'),"
                "last_typed_reason='PAGE_ADVANCED'",
                (
                    current_offset,
                    fingerprint,
                    deepest_boundary[0] if deepest_boundary else None,
                    deepest_boundary[1] if deepest_boundary else None,
                ),
            )

    return result


async def vk_crawl_continuation_scheduler(
    db: Database,
    _bot: Any | None = None,
    *,
    run_id: str | None = None,
    max_jobs: int = 2,
    max_pages_per_job: int = 3,
    lease_seconds: int = 300,
) -> dict[str, int]:
    return await process_vk_crawl_continuations(
        db,
        max_jobs=max_jobs,
        max_pages_per_job=max_pages_per_job,
        lease_seconds=lease_seconds,
        run_id=run_id,
    )


async def crawl_once(
    db,
    *,
    broadcast: bool = False,
    bot: Any | None = None,
    force_backfill: bool = False,
    backfill_days: int | None = None,
) -> dict[str, Any]:
    """Crawl configured VK groups once and enqueue matching posts.

    The function scans groups listed in ``vk_source`` and uses cursors from
    ``vk_crawl_cursor`` to fetch in-horizon posts. Every fetched post is first
    persisted as an immutable ``vk_source_packet`` revision. High-recall
    deterministic positives enter ``vk_inbox``; deterministic failures are
    checked by a small semantic LLM admission gate first. Only grounded,
    high-confidence PAST_ONLY/NON_EVENT results stay out of the auto-import
    queue; uncertainty/provider failure fails open into it. Crawl collection
    never executes the later bounded auto-import stage.

    If ``broadcast`` is True and ``bot`` is supplied, a crawl summary is sent
    to the admin chat specified by ``ADMIN_CHAT_ID`` environment variable.
    """

    _require_vk_crawl_storage_headroom(db)

    vk_wall_since = require_main_attr(
        "vk_wall_since"
    )  # imported lazily to avoid circular import
    get_supabase_client = require_main_attr("get_supabase_client")
    get_tz_offset = require_main_attr("get_tz_offset")
    await get_tz_offset(db)
    local_tz = require_main_attr("LOCAL_TZ")
    exporter = SBExporter(get_supabase_client)

    start = time.perf_counter()
    override_backfill_days = (
        max(1, min(backfill_days, VK_CRAWL_BACKFILL_OVERRIDE_MAX_DAYS))
        if backfill_days is not None
        else None
    )

    stats = {
        "groups_checked": 0,
        "posts_scanned": 0,
        "matches": 0,
        "duplicates": 0,
        "added": 0,
        "packets_added": 0,
        "admission_rejected": 0,
        "admission_llm_checked": 0,
        "admission_llm_admitted": 0,
        "admission_fail_open": 0,
        "errors": 0,
        "inbox_total": 0,
        "queue": {},
        "safety_cap_hits": 0,
        "deep_backfill_triggers": 0,
        "forced_backfill": force_backfill,
        "backfill_days_used": (
            override_backfill_days
            if override_backfill_days is not None
            else (VK_CRAWL_BACKFILL_DAYS if force_backfill else None)
        ),
        "backfill_days_requested": backfill_days if force_backfill else None,
    }

    try:
        from source_parsing.post_metrics import compute_age_day as _compute_age_day
        from source_parsing.post_metrics import normalize_age_day as _normalize_age_day
        from source_parsing.post_metrics import upsert_vk_post_metric as _upsert_vk_post_metric
    except Exception:  # pragma: no cover - optional helper
        _compute_age_day = None
        _normalize_age_day = None
        _upsert_vk_post_metric = None

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT
                group_id,
                screen_name,
                name,
                location,
                default_time,
                default_ticket_link,
                COALESCE(owner_type, 'group')
            FROM vk_source
            """
        )
        groups = [
            {
                "group_id": row[0],
                "screen_name": row[1],
                "name": row[2],
                "location": row[3],
                "default_time": row[4],
                "default_ticket_link": row[5],
                "owner_type": row[6],
            }
            for row in await cur.fetchall()
        ]
        await conn.commit()

    random.shuffle(groups)
    logging.info(
        "vk.crawl start groups=%d overlap=%s", len(groups), VK_CRAWL_OVERLAP_SEC
    )

    pages_per_group: list[int] = []

    now_ts = int(time.time())
    for group in groups:
        _require_vk_crawl_storage_headroom(db)
        gid = group["group_id"]
        raw_owner_type = group.get("owner_type") or "group"
        owner_type = "user" if str(raw_owner_type).strip().lower() == "user" else "group"
        group_title_norm = _normalize_group_title(group.get("name"))
        group_screen_name_norm = _normalize_group_screen_name(
            group.get("screen_name")
        )
        group_title_display = _display_group_title(group.get("name"), gid)
        group_screen_name_display = _display_group_screen_name(
            group.get("screen_name"), gid
        )
        default_time = group.get("default_time")
        stats["groups_checked"] += 1
        await asyncio.sleep(random.uniform(0.7, 1.2))  # safety pause
        exporter.upsert_group_meta(
            gid,
            screen_name=group.get("screen_name"),
            name=group.get("name"),
            location=group.get("location"),
            default_time=default_time,
            default_ticket_link=group.get("default_ticket_link"),
        )
        backfill = False
        pages_loaded = 0
        group_posts = 0
        group_matches = 0
        group_added = 0
        group_duplicates = 0
        group_blank_single_photo_matches = 0
        group_history_matches = 0
        group_errors = 0
        safety_cap_triggered = False
        hard_cap_triggered = False
        reached_cursor_overlap = False
        scan_terminal_reached = False
        last_fetched_page: list[dict[str, Any]] = []
        deep_backfill_scheduled = False
        mode = "inc"
        try:
            async with db.raw_conn() as conn:
                cur = await conn.execute(
                    "SELECT last_seen_ts, last_post_id, updated_at, checked_at FROM vk_crawl_cursor WHERE group_id=?",
                    (gid,),
                )
                row = await cur.fetchone()
                continuation_cur = await conn.execute(
                    """
                    SELECT 1 FROM vk_crawl_continuation
                    WHERE source_type='vk' AND owner_id=?
                      AND reason='hard_cap'
                      AND status IN ('pending','retry','running')
                    LIMIT 1
                    """,
                    (gid,),
                )
                hard_cap_continuation_owed = await continuation_cur.fetchone() is not None
            cursor_updated_at_existing_raw: Any = None
            if row:
                last_seen_ts, last_post_id, updated_at, checked_at = row
                cursor_updated_at_existing_raw = updated_at
                # Quiet sources use checked_at. An incomplete hard-cap scan is
                # distinguished by its durable continuation record rather than
                # overloading the cursor timestamp.
                idle_anchor = updated_at if hard_cap_continuation_owed else (
                    checked_at if checked_at is not None else updated_at
                )
                if isinstance(idle_anchor, str):
                    try:
                        idle_anchor_ts = int(
                            datetime.fromisoformat(idle_anchor).timestamp()
                        )
                    except ValueError:
                        try:
                            idle_anchor_ts = int(idle_anchor)
                        except (TypeError, ValueError):
                            idle_anchor_ts = 0
                elif idle_anchor:
                    idle_anchor_ts = int(idle_anchor)
                else:
                    idle_anchor_ts = 0
            else:
                last_seen_ts = last_post_id = 0
                idle_anchor_ts = 0
                cursor_updated_at_existing_raw = None

            # ``updated_at`` is the last discovered post, not the last scan.
            # Quiet sources legitimately leave it old; using it as the idle
            # anchor repeatedly replays the full history after every 24 hours.
            idle_h = (now_ts - idle_anchor_ts) / 3600 if idle_anchor_ts else None
            backfill = force_backfill or last_seen_ts == 0 or (
                idle_h is not None and idle_h >= VK_CRAWL_BACKFILL_AFTER_IDLE_H
            )
            mode = "backfill" if backfill else "inc"

            posts: list[dict] = []

            next_cursor_ts = last_seen_ts
            next_cursor_pid = last_post_id
            cursor_updated_at_override: int | None = None
            cursor_payload: tuple[int, int, Any, int] | None = None
            has_new_posts = False

            if backfill:
                window_days = (
                    override_backfill_days
                    if override_backfill_days is not None
                    else VK_CRAWL_BACKFILL_DAYS
                )
                stats["backfill_days_used"] = window_days
                horizon = now_ts - window_days * 86400
                offset = 0
                while pages_loaded < VK_CRAWL_MAX_PAGES_BACKFILL:
                    _require_vk_crawl_storage_headroom(db)
                    page = await vk_wall_since(
                        gid,
                        0,
                        count=VK_CRAWL_PAGE_SIZE_BACKFILL,
                        offset=offset,
                        owner_type=owner_type,
                    )
                    last_fetched_page = list(page)
                    pages_loaded += 1
                    # Raw-first means even the boundary-crossing page is
                    # persisted in full; the horizon only terminates paging.
                    posts.extend(page)
                    if len(page) < VK_CRAWL_PAGE_SIZE_BACKFILL:
                        scan_terminal_reached = True
                        break
                    if page and min(p["date"] for p in page) < horizon:
                        scan_terminal_reached = True
                        break
                    offset += VK_CRAWL_PAGE_SIZE_BACKFILL
            else:
                since = max(0, last_seen_ts - VK_CRAWL_OVERLAP_SEC)
                offset = 0
                safety_cap_threshold = max(1, VK_CRAWL_MAX_PAGES_INC)
                hard_cap = safety_cap_threshold * 10
                while True:
                    _require_vk_crawl_storage_headroom(db)
                    page = await vk_wall_since(
                        gid,
                        since,
                        count=VK_CRAWL_PAGE_SIZE,
                        offset=offset,
                        owner_type=owner_type,
                    )
                    last_fetched_page = list(page)
                    pages_loaded += 1
                    posts.extend(page)

                    if page:
                        oldest_page_post = min(
                            page, key=lambda p: (p["date"], p["post_id"])
                        )
                        if oldest_page_post["date"] < last_seen_ts or (
                            oldest_page_post["date"] == last_seen_ts
                            and oldest_page_post["post_id"] <= last_post_id
                        ):
                            reached_cursor_overlap = True

                    if not page or len(page) < VK_CRAWL_PAGE_SIZE:
                        scan_terminal_reached = True
                        break

                    if reached_cursor_overlap:
                        scan_terminal_reached = True
                        break

                    if pages_loaded >= safety_cap_threshold:
                        safety_cap_triggered = True
                    if pages_loaded >= hard_cap:
                        hard_cap_triggered = True
                        logging.warning(
                            "vk.crawl.inc.hard_cap group=%s pages=%s since=%s last_seen=%s",
                            gid,
                            pages_loaded,
                            since,
                            last_seen_ts,
                        )
                        break

                    offset += VK_CRAWL_PAGE_SIZE

                if safety_cap_triggered:
                    stats["safety_cap_hits"] += 1
                    logging.warning(
                        "vk.crawl.inc.safety_cap group=%s pages=%s threshold=%s", 
                        gid,
                        pages_loaded,
                        safety_cap_threshold,
                    )
                    try:
                        import main

                        main.vk_crawl_safety_cap_total += 1
                    except Exception:
                        pass

            max_ts, max_pid = last_seen_ts, last_post_id

            admission_candidates = [
                _vk_crawl_admission_candidate(
                    group_id=int(gid),
                    owner_type=owner_type,
                    post=post,
                    default_time=default_time,
                    tz=local_tz,
                )
                for post in posts
            ]

            # Commit every fetched revision before invoking the admission LLM.
            # A provider timeout therefore cannot lose the raw source; the
            # canonical crawl cursor still remains blocked until each packet is
            # classified or explicitly failed open into the later queue.
            raw_results: list[tuple[int, bool]] = []
            for post, candidate in zip(posts, admission_candidates):
                _require_vk_crawl_storage_headroom(db)
                raw_results.append(await _persist_vk_source_packet(
                    db,
                    group_id=int(gid),
                    owner_type=owner_type,
                    post=post,
                    source_url=candidate.source_url,
                    keyword_hints=candidate.keyword_hints,
                    date_hints=candidate.date_hints,
                    event_ts_hint=candidate.event_ts_hint,
                    admission_decision=None,
                    apply_admission_gate=True,
                ))
            unique_admission_candidates = list(
                {
                    candidate.key: candidate
                    for candidate in admission_candidates
                }.values()
            )
            admission_decisions = await _resolve_vk_crawl_admissions(
                db,
                unique_admission_candidates,
                tz=local_tz,
            )
            admission_candidates_by_key = {
                candidate.key: candidate for candidate in admission_candidates
            }

            for post_index, post in enumerate(posts):
                _require_vk_crawl_storage_headroom(db)
                ts = int(post["date"])
                pid = int(post["post_id"])
                is_new_for_cursor = ts > last_seen_ts or (
                    ts == last_seen_ts and pid > last_post_id
                )
                if is_new_for_cursor and (
                    ts > max_ts or (ts == max_ts and pid > max_pid)
                ):
                    max_ts, max_pid = ts, pid
                stats["posts_scanned"] += 1
                group_posts += 1

                candidate_key = f"{int(gid)}:{pid}"
                candidate = admission_candidates_by_key[candidate_key]
                admission = admission_decisions[candidate_key]
                post_text = candidate.text
                source_url = candidate.source_url
                matched_kw_list = list(candidate.keyword_hints)
                date_hints = list(candidate.date_hints)
                event_ts_hint = candidate.event_ts_hint
                history_hit = HISTORY_MATCHED_KEYWORD in matched_kw_list

                if admission.admitted:
                    stats["matches"] += 1
                    group_matches += 1
                else:
                    stats["admission_rejected"] += 1
                if admission.route == "llm":
                    stats["admission_llm_checked"] += 1
                    if admission.admitted:
                        stats["admission_llm_admitted"] += 1
                elif admission.route == "fail_open":
                    stats["admission_llm_checked"] += 1
                    stats["admission_fail_open"] += 1
                if history_hit:
                    group_history_matches += 1
                if not post_text.strip() and candidate.visual_evidence_count:
                    group_blank_single_photo_matches += 1

                # Popularity metrics are optional. Raw packet persistence below
                # is the cursor-advancement boundary and is never best-effort.
                try:
                    collected_ts = int(time.time())
                    age_raw = (
                        _compute_age_day(published_ts=ts, collected_ts=collected_ts)
                        if _compute_age_day else None
                    )
                    age_day = _normalize_age_day(age_raw) if _normalize_age_day else age_raw
                    if _upsert_vk_post_metric and isinstance(age_day, int) and age_day >= 0:
                        views = post.get("views")
                        likes = post.get("likes")
                        if isinstance(views, int) or isinstance(likes, int):
                            await _upsert_vk_post_metric(
                                db,
                                group_id=int(gid),
                                post_id=pid,
                                age_day=int(age_day),
                                source_url=source_url,
                                post_ts=ts,
                                views=int(views) if isinstance(views, int) else None,
                                likes=int(likes) if isinstance(likes, int) else None,
                                collected_ts=collected_ts,
                            )
                except Exception:
                    logging.warning(
                        "vk.crawl.metrics persist failed gid=%s post_id=%s",
                        gid,
                        pid,
                        exc_info=True,
                    )

                try:
                    await _persist_vk_source_packet(
                        db,
                        group_id=int(gid),
                        owner_type=owner_type,
                        post=post,
                        source_url=source_url,
                        keyword_hints=matched_kw_list,
                        date_hints=date_hints,
                        event_ts_hint=event_ts_hint,
                        admission_decision=admission,
                        apply_admission_gate=True,
                    )
                    _packet_id, packet_is_new = raw_results[post_index]
                except Exception:
                    stats["errors"] += 1
                    group_errors += 1
                    logging.exception(
                        "vk.crawl.raw_packet_persist_failed group=%s post=%s; cursor blocked",
                        gid,
                        pid,
                    )
                    raise

                if packet_is_new:
                    stats["packets_added"] += 1
                    has_new_posts = True
                    if admission.admitted:
                        stats["added"] += 1
                        group_added += 1
                else:
                    stats["duplicates"] += 1
                    group_duplicates += 1

            next_cursor_ts = max_ts
            next_cursor_pid = max_pid
            continuation_needed = bool(
                hard_cap_triggered
                or (
                    backfill
                    and pages_loaded >= VK_CRAWL_MAX_PAGES_BACKFILL
                    and not scan_terminal_reached
                    and len(last_fetched_page) >= VK_CRAWL_PAGE_SIZE_BACKFILL
                )
            )
            if continuation_needed:
                page_size = (
                    VK_CRAWL_PAGE_SIZE_BACKFILL if backfill else VK_CRAWL_PAGE_SIZE
                )
                await _schedule_vk_crawl_continuation(
                    db,
                    group_id=int(gid),
                    owner_type=owner_type,
                    since_ts=(0 if backfill else max(0, last_seen_ts - VK_CRAWL_OVERLAP_SEC)),
                    offset=max(0, pages_loaded * page_size),
                    horizon_ts=(horizon if backfill else max(0, last_seen_ts - VK_CRAWL_OVERLAP_SEC)),
                    scan_mode=("backfill" if backfill else "incremental"),
                    page_size=page_size,
                    original_cursor_ts=int(last_seen_ts),
                    original_cursor_post_id=int(last_post_id),
                    reason=("hard_cap" if hard_cap_triggered else "page_safety_cap"),
                    last_page_fingerprint=(
                        _vk_continuation_page_fingerprint(last_fetched_page)
                        if last_fetched_page
                        else None
                    ),
                    deepest_page_ts=(
                        _vk_continuation_deepest_boundary(last_fetched_page)[0]
                        if last_fetched_page
                        else None
                    ),
                    deepest_page_post_id=(
                        _vk_continuation_deepest_boundary(last_fetched_page)[1]
                        if last_fetched_page
                        else None
                    ),
                )
            if hard_cap_triggered and max_ts > 0 and not reached_cursor_overlap:
                deep_backfill_scheduled = True
                next_cursor_ts = last_seen_ts
                next_cursor_pid = last_post_id
                idle_threshold = VK_CRAWL_BACKFILL_AFTER_IDLE_H * 3600
                cursor_updated_at_override = max(0, now_ts - idle_threshold - 60)
            elif safety_cap_triggered and not scan_terminal_reached and max_ts > 0:
                adjusted_ts = max(last_seen_ts, max_ts - VK_CRAWL_OVERLAP_SEC)
                if adjusted_ts < next_cursor_ts:
                    next_cursor_ts = adjusted_ts
                    next_cursor_pid = 0

            if deep_backfill_scheduled:
                stats["deep_backfill_triggers"] += 1
                logging.warning(
                    "vk.crawl.inc.deep_backfill_trigger group=%s pages=%s last_seen=%s next_ts=%s",
                    gid,
                    pages_loaded,
                    last_seen_ts,
                    max_ts,
                )

            mode = "backfill" if backfill else "inc"
            logging.info(
                "vk.crawl group=%s posts=%s matched=%s pages=%s mode=%s",
                gid,
                group_posts,
                group_added,
                pages_loaded,
                mode,
            )
            cursor_checked_at = int(time.time())
            if cursor_updated_at_override is not None:
                cursor_updated_at = cursor_updated_at_override
            elif has_new_posts:
                cursor_updated_at = now_ts
            else:
                cursor_updated_at = cursor_updated_at_existing_raw
            cursor_payload = (
                next_cursor_ts,
                next_cursor_pid,
                cursor_updated_at,
                cursor_checked_at,
            )
        except Exception:
            stats["errors"] += 1
            group_errors += 1
            cursor_payload = None
        else:
            if cursor_payload is not None:
                async with db.raw_conn() as conn:
                    await conn.execute(
                        "INSERT OR REPLACE INTO vk_crawl_cursor(group_id, last_seen_ts, last_post_id, updated_at, checked_at, owner_type) VALUES(?,?,?,?,?,?)",
                        (gid, *cursor_payload, owner_type),
                    )
                    await conn.commit()
        finally:
            pages_per_group.append(pages_loaded)
            match_rate = group_matches / max(1, group_posts)
            snapshot_counters = {
                "posts_scanned": group_posts,
                "matched": group_matches,
                "duplicates": group_duplicates,
                "errors": group_errors,
                "pages_loaded": pages_loaded,
            }
            exporter.write_snapshot(
                group_id=gid,
                group_title=group.get("name"),
                group_screen_name=group.get("screen_name"),
                ts=int(time.time()),
                match_rate=match_rate,
                errors=group_errors,
                counters=snapshot_counters,
            )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, COUNT(*) FROM vk_inbox GROUP BY status"
        )
        rows = await cur.fetchall()
    for st, cnt in rows:
        stats["queue"][st] = cnt
    stats["inbox_total"] = sum(stats["queue"].values())
    stats["pages_per_group"] = pages_per_group
    stats["overlap_sec"] = VK_CRAWL_OVERLAP_SEC
    try:
        import main
        main.vk_crawl_groups_total += stats["groups_checked"]
        main.vk_crawl_posts_scanned_total += stats["posts_scanned"]
        main.vk_crawl_matched_total += stats["matches"]
        main.vk_crawl_duplicates_total += stats["duplicates"]
        main.vk_inbox_inserted_total += stats["added"]
    except Exception:
        pass

    took_ms = int((time.perf_counter() - start) * 1000)
    logging.info(
        "vk.crawl.finish groups=%s posts_scanned=%s matches=%s rejected=%s llm_checked=%s fail_open=%s packets_added=%s dups=%s added=%s inbox_total=%s pages=%s overlap=%s took_ms=%s",
        stats["groups_checked"],
        stats["posts_scanned"],
        stats["matches"],
        stats["admission_rejected"],
        stats["admission_llm_checked"],
        stats["admission_fail_open"],
        stats["packets_added"],
        stats["duplicates"],
        stats["added"],
        stats["inbox_total"],
        "/".join(str(p) for p in pages_per_group),
        VK_CRAWL_OVERLAP_SEC,
        took_ms,
    )
    if broadcast and bot:
        admin_chat = os.getenv("ADMIN_CHAT_ID")
        if admin_chat:
            q = stats.get("queue", {})
            forced_note = ""
            if stats.get("forced_backfill"):
                used_days = stats.get("backfill_days_used") or VK_CRAWL_BACKFILL_DAYS
                requested_days = stats.get("backfill_days_requested")
                forced_note = f", принудительный бэкафилл до {used_days} дн."
                if (
                    requested_days is not None
                    and requested_days != used_days
                ):
                    forced_note += f" (запрошено {requested_days})"

            msg = (
                f"Проверено {stats['groups_checked']} сообществ, "
                f"просмотрено {stats['posts_scanned']} постов, "
                f"допущено в очередь {stats['matches']}, "
                f"отсечено admission {stats['admission_rejected']}, "
                f"LLM-проверок {stats['admission_llm_checked']} "
                f"(fail-open: {stats['admission_fail_open']}), "
                f"дубликатов {stats['duplicates']}, "
                f"добавлено {stats['added']}, "
                f"теперь в очереди {stats['inbox_total']} "
                f"(pending: {q.get('pending',0)}, locked: {q.get('locked',0)}, "
                f"skipped: {q.get('skipped',0)}, imported: {q.get('imported',0)}, "
                f"rejected: {q.get('rejected',0)}), "
                f"страниц на группу: {'/'.join(str(p) for p in stats['pages_per_group'])}, "
                f"перекрытие: {stats['overlap_sec']} сек"
                f"{forced_note}"
            )
            try:
                await bot.send_message(int(admin_chat), msg)
            except Exception:
                logging.exception("vk.crawl.broadcast.error")
    exporter.retention()
    return stats
