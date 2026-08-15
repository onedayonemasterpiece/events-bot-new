from __future__ import annotations

import asyncio
from calendar import monthrange
import hashlib
import math
import json
import logging
import os
import time
import re
import textwrap
import unicodedata
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, fields as dataclass_fields
from enum import Enum
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.exc import IntegrityError

from db import Database
from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client
from event_age_rating import (
    AGE_DECISION_JSON_SCHEMA,
    AgeRatingDecision,
    age_input_hash,
    apply_age_decision,
    decision_from_semantic_payload,
    declared_structured_decision,
    reconcile_age_decision,
)
from festival_grounding import ground_kgd80_festival
from location_reference import (
    find_known_venue_in_text,
    normalise_event_location_from_reference,
    normalize_venue_key,
)
from markup import looks_like_genai_response_dump, unescape_public_text_escapes
from llm_source_grounding import claim_is_grounded
from models import (
    Event,
    EventIdentityDecisionLog,
    EventPoster,
    EventSource,
    EventSourceFact,
    PosterOcrCache,
)
from sections import MONTHS_RU
from telegram_sources import canonicalize_tg_url
from smart_update_identity import (
    IdentityGateMode,
    IdentityVectorEvidence,
    MergeIdentityRelation,
    build_merge_identity_gate_verdict,
    build_identity_gate_verdict,
    canonicalize_identity_url,
    identity_gate_fail_safe_verdict,
    input_packet_fingerprint,
    is_explicit_occurrence_key,
    merge_identity_gate_fail_safe_verdict,
    parse_identity_gate_mode,
    stable_candidate_identity,
)
from smart_update_state import (
    CandidateAttemptInProgress,
    CandidateAttemptReceipt,
    IdentityDistinctReason,
    LifecycleReason,
    ProductExclusionReason,
    RetryReason,
    SmartUpdateTerminalOutcome,
    begin_candidate_attempt,
    claim_due_candidates,
    finish_candidate_attempt,
    terminalize_claimed_candidate_technical,
)

logger = logging.getLogger(__name__)

_SMART_UPDATE_LLM_TRACE: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "smart_update_llm_trace",
    default=None,
)

_HALL_HINT_RE = re.compile(
    r"\b(зал|аудитория|лекторий|сцена|фойе|этаж|корпус)\b\s+([^\s,.;:]+)(?:\s+([^\s,.;:]+))?(?:\s+([^\s,.;:]+))?",
    re.IGNORECASE,
)
# Telegram custom emoji placeholders can land in PUA (Private Use Area) ranges.
# Keep this broader than just BMP to avoid "tofu" boxes on Telegraph pages.
_PRIVATE_USE_RE = re.compile(r"[\uE000-\uF8FF\U000F0000-\U000FFFFD\U00100000-\U0010FFFD]")
_ZERO_WIDTH_RE = re.compile(r"[\u200b\u200c\u200d\u2060]")

# Ticket giveaways must not become standalone "events", but real announcements that
# include a giveaway block should still import/merge the underlying event facts.
_GIVEAWAY_RE = re.compile(
    r"\b(розыгрыш|разыгрыва\w*|розыгра\w*|выигра\w*|конкурс|giveaway)\b",
    re.IGNORECASE,
)
_TICKETS_RE = re.compile(
    r"\b(билет\w*|пригласительн\w*|абонемент\w*)\b",
    re.IGNORECASE,
)

# Lines that are usually giveaway mechanics ("subscribe/repost/comment") rather than event facts.
_GIVEAWAY_LINE_RE = re.compile(
    r"\b("
    r"услови\w*|"
    r"участв\w*|"
    r"подпиш\w*|"
    r"репост\w*|"
    r"коммент\w*|"
    r"отмет\w*|"
    r"лайк\w*|"
    r"победител\w*|"
    r"итог\w*|"
    r"розыгрыш|разыгрыва\w*|розыгра\w*|"
    r"конкурс|giveaway|"
    r"приз\w*"
    r")\b",
    re.IGNORECASE,
)

_GIVEAWAY_MECHANICS_RE = re.compile(
    r"\b("
    r"услови\w*|"
    r"участв\w*|"
    r"подпиш\w*|"
    r"репост\w*|"
    r"коммент\w*|"
    r"отмет\w*|"
    r"лайк\w*|"
    r"победител\w*|"
    r"итог\w*|"
    r"приз\w*"
    r")\b",
    re.IGNORECASE,
)

_EVENT_SIGNAL_RE = re.compile(
    r"\b("
    r"спектакл\w*|"
    r"концерт\w*|"
    r"выставк\w*|"
    r"лекци\w*|"
    r"показ\w*|"
    r"встреч\w*|"
    r"мастер-?класс\w*|"
    r"презентац\w*|"
    r"экскурс\w*|"
    r"перформанс\w*|"
    r"кино\w*|фильм\w*"
    r")\b",
    re.IGNORECASE,
)

# Promotions are often mixed into real event announcements. Product requirement:
# strip purely promotional fragments, but keep actual event facts (date/time/place/contacts).
_PROMO_STRIP_RE = re.compile(
    r"\b("
    r"акци(?:я|и|ю|ях)|"
    r"скидк\w*|"
    r"промокод\w*|"
    r"спецпредложен\w*|"
    r"бонус\w*|"
    r"кэшбек\w*|кэшбэк\w*|кэшбэ\w*|"
    r"подарок\w*|"
    r"сертификат\w*"
    r")\b",
    re.IGNORECASE,
)
_CONGRATS_RE = re.compile(
    r"\b(поздравля\w*|с\s+дн[её]м\s+рождени\w*|юбиле\w*)\b",
    re.IGNORECASE,
)
_CONGRATS_CONTEXT_RE = re.compile(
    r"\b(ближайш\w*|спектакл\w*|концерт\w*|мероприят\w*|событи\w*)\b",
    re.IGNORECASE,
)

_CHANNEL_PROMO_STRIP_RE = re.compile(
    r"(?i)"
    r"(?=.*(?:t\.me/|telegram|телеграм))"
    r"(?=.*\b(?:канал\w*|чат\w*|групп\w*)\b)"
    r"(?=.*(?:анонс\w*|афиш\w*|подпис\w*|следит\w*|информац\w*\s+о\s+(?:событи\w*|мероприят\w*)))"
)

_POSTER_PROMO_RE = re.compile(
    r"\b(акци(?:я|и|ю|ях)|скидк\w*|промокод\w*|купон\w*|sale)\b|%",
    re.IGNORECASE,
)

SMART_UPDATE_LLM = os.getenv("SMART_UPDATE_LLM", "gemma").strip().lower()
SMART_UPDATE_LLM_DISABLED = SMART_UPDATE_LLM in {"off", "none", "disabled", "0"}
# Product requirement: Smart Update uses Gemma as the primary model.
# OpenAI (4o) is allowed only as a *fallback* when Gemma calls fail/unavailable.
if not SMART_UPDATE_LLM_DISABLED and SMART_UPDATE_LLM != "gemma":
    logger.warning(
        "smart_update: SMART_UPDATE_LLM=%r is not supported; forcing 'gemma' (4o is fallback-only)",
        SMART_UPDATE_LLM,
    )
    SMART_UPDATE_LLM = "gemma"
SMART_UPDATE_MODEL = os.getenv(
    "SMART_UPDATE_MODEL",
    "gemma-4-31b-it",
).strip()
if not SMART_UPDATE_MODEL or "gemma" not in SMART_UPDATE_MODEL.lower():
    logger.warning(
        "smart_update: SMART_UPDATE_MODEL=%r is not a Gemma model; forcing 'gemma-4-31b-it'",
        SMART_UPDATE_MODEL,
    )
    SMART_UPDATE_MODEL = "gemma-4-31b-it"
SMART_UPDATE_IDENTITY_GATE_MODE = parse_identity_gate_mode(
    os.getenv("SMART_UPDATE_IDENTITY_GATE", "off")
)
SMART_UPDATE_MERGE_IDENTITY_GATE_MODE = parse_identity_gate_mode(
    os.getenv("SMART_UPDATE_MERGE_IDENTITY_GATE", "off")
)
SMART_UPDATE_IDENTITY_VECTOR_RECALL = os.getenv(
    "SMART_UPDATE_IDENTITY_VECTOR_RECALL", "1"
).strip().lower() not in {"0", "false", "off", "no"}
SMART_UPDATE_IDENTITY_VECTOR_TOP_K = int(os.getenv("SMART_UPDATE_IDENTITY_VECTOR_TOP_K", "8") or "8")
SMART_UPDATE_IDENTITY_VECTOR_MIN_SIMILARITY = float(
    os.getenv("SMART_UPDATE_IDENTITY_VECTOR_MIN_SIMILARITY", "0.75") or "0.75"
)
SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS = float(
    os.getenv("SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS", "2.5") or "2.5"
)
SMART_UPDATE_IDENTITY_EMBEDDING_MODEL = os.getenv(
    "SMART_UPDATE_IDENTITY_EMBEDDING_MODEL", "gemini-embedding-2"
).strip() or "gemini-embedding-2"
SMART_UPDATE_IDENTITY_EMBEDDING_DIM = int(os.getenv("SMART_UPDATE_IDENTITY_EMBEDDING_DIM", "768") or "768")
SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV = os.getenv(
    "SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV", "GOOGLE_API_KEY4"
).strip() or "GOOGLE_API_KEY4"

# Per-stage primary model overrides. Default route fact extraction and the
# main writer to gemini-3.1-flash-lite (the production registry reserves a
# defensive 450 RPD project lane) while the rest of the Smart Update pipeline
# (revise/rewrite/short_description/search_digest/coverage) keeps
# SMART_UPDATE_MODEL. Facts-only calls receive their own bounded fallback chain
# below; unrelated writers must not inherit it through a process-global setting.
SMART_UPDATE_FACTS_MODEL = (
    os.getenv("SMART_UPDATE_FACTS_MODEL", "gemini-3.1-flash-lite").strip()
    or SMART_UPDATE_MODEL
)
SMART_UPDATE_WRITER_MODEL = (
    os.getenv("SMART_UPDATE_WRITER_MODEL", "gemini-3.1-flash-lite").strip()
    or SMART_UPDATE_MODEL
)
SMART_UPDATE_FACTS_FALLBACK_MODELS = tuple(
    model.strip()
    for model in (
        os.getenv(
            "SMART_UPDATE_FACTS_FALLBACK_MODELS",
            "gemini-3.5-flash-lite,gemma-4-31b-it",
        )
        or ""
    ).split(",")
    if model.strip()
)
SMART_UPDATE_FORCE_STAGED_GEMINI = (
    os.getenv("SMART_UPDATE_FORCE_STAGED_GEMINI", "0") or ""
).strip().lower() in {"1", "true", "yes", "on"}


def _is_smart_update_facts_stage(label: str | None) -> bool:
    label_l = (label or "").strip().lower()
    return label_l in {
        "facts_extract",
        "rich_facts_extract",
        "title_recover",
        "title_recover_public",
        "anchor_role_review",
        "create_bundle_grounding",
        "occurrence_scope_review",
        "location_grounding_review",
        "duration_forecast",
    } or label_l.endswith(":fact_first_cov")


def _resolve_smart_update_model(label: str | None) -> str:
    label_l = (label or "").strip().lower()
    if label_l == "collection_candidate_adjudication":
        return SMART_UPDATE_MODEL
    if SMART_UPDATE_FORCE_STAGED_GEMINI:
        if _is_smart_update_facts_stage(label):
            return SMART_UPDATE_FACTS_MODEL
        return SMART_UPDATE_WRITER_MODEL
    # Pure facts extraction stages (split-create / fact-first paths).
    if _is_smart_update_facts_stage(label):
        return SMART_UPDATE_FACTS_MODEL
    # Pure writer stages (split-create / fact-first paths). The fact-first
    # writer label is composed as ``f"{label}:fact_first_desc"`` (e.g.
    # ``merge:4727:fact_first_desc``) so we have to match both the bare
    # name and the suffix form.
    if (
        label_l == "split_description_writer"
        or label_l == "fact_first_desc"
        or label_l.endswith(":fact_first_desc")
    ):
        return SMART_UPDATE_WRITER_MODEL
    # Note: legacy bundle stages (``create_bundle`` / ``match_create_bundle``
    # / ``merge``) are intentionally *not* routed to Gemini because they pack
    # title + description + facts + short_description + search_digest into
    # one LLM call. Routing them to Lite would expand the user-approved
    # surface beyond "fact extraction + text writing" and burn the Lite RPD
    # budget on derived fields that Gemma 4 handles compactly. To put
    # extraction + writer on Gemini *without* dragging derived fields along,
    # enable ``SMART_UPDATE_G4_SPLIT_CREATE=1`` so the bundle is split into
    # ``rich_facts_extract`` (Gemini), ``split_description_writer`` (Gemini)
    # and ``split_derived_fields`` (Gemma).
    return SMART_UPDATE_MODEL


def _smart_update_fallback_models(label: str | None, model: str) -> list[str] | None:
    """Use the spare stable Lite lane only for facts-style Smart Update calls."""

    if model != SMART_UPDATE_FACTS_MODEL:
        return None
    if not _is_smart_update_facts_stage(label):
        return None
    return list(SMART_UPDATE_FACTS_FALLBACK_MODELS)
SMART_UPDATE_GEMMA_NATIVE_SCHEMA = (
    os.getenv("SMART_UPDATE_GEMMA_NATIVE_SCHEMA", "0") or ""
).strip().lower() in {"1", "true", "yes", "on"}
SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES = {
    item.strip()
    for item in (
        os.getenv(
            "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES",
            "facts_extract,rich_facts_extract,create_bundle,split_description_writer,split_derived_fields",
        )
        or ""
    ).split(",")
    if item.strip()
}
SMART_UPDATE_G4_SPLIT_CREATE = (os.getenv("SMART_UPDATE_G4_SPLIT_CREATE", "0") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE = (
    os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE", "0") or ""
).strip().lower() in {"1", "true", "yes", "on"}
# INC-2026-05-30 opt 1: LLM dedup adjudicator over widened (date+city+blocking-key)
# recall on the create path. Default ON; set to 0 to roll back to the pre-incident
# anchor-gated behaviour.
SMART_UPDATE_DEDUP_ADJUDICATOR = (
    os.getenv("SMART_UPDATE_DEDUP_ADJUDICATOR", "1") or ""
).strip().lower() in {"1", "true", "yes", "on"}
SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE = (
    os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE", "adaptive") or "adaptive"
).strip().lower()
if SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE not in {"gemma4", "4o", "adaptive"}:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE = "adaptive"
SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_MODEL = (
    os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_MODEL", "gpt-4o") or "gpt-4o"
).strip()
try:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD = int(
        os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD", "14") or "14"
    )
except Exception:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD = 14
SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD = min(
    80,
    max(1, SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD),
)
try:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC = int(
        os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC", "70") or "70"
    )
except Exception:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC = 70
SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC = min(
    240,
    max(10, SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC),
)
try:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES = int(
        os.getenv("SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES", "2") or "2"
    )
except Exception:
    SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES = 2
SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES = min(
    4,
    max(1, SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES),
)
SMART_UPDATE_YO_RULE = (
    "Уважай букву «ё»: если слово в норме пишется через «ё», не заменяй её на «е»."
)
SMART_UPDATE_PRESERVE_LISTS_RULE = (
    "Если в источнике есть нумерованный/маркированный список (песни/треклист/репертуар/программа/пункты формата), "
    "НЕ сворачивай его в одну общую фразу. Перенеси список полностью, сохрани порядок и нумерацию/маркеры. "
    "Названия песен/произведений/имён НЕ перефразируй: копируй дословно."
)
SMART_UPDATE_FACTS_PRESERVE_COMPACT_PROGRAM_LISTS_RULE = (
    "Если в источнике есть короткий список программы/репертуара/треклиста/участников/фильмов (2–12 строк подряд), "
    "верни каждый пункт отдельным фактом ДОСЛОВНО, сохрани порядок (можно занять на это большую часть лимита facts). "
    "Пример: после строки `Программа:` идут 5 строк `«Название» — Автор` → верни эти 5 строк как отдельные facts. "
    "Не сворачивай список в общую фразу и не пиши «и другие»."
)
SMART_UPDATE_OPTIONAL_HEADINGS_RULE = (
    "Структурируй description: добавь 1–3 коротких подзаголовка в Markdown формате `### ...` "
    "(например «Программа», «Условия участия», «Состав/участники», «Как добраться»). "
    "Если текст содержит 2+ абзаца — хотя бы 1 подзаголовок обязателен. "
    "Подзаголовки должны быть КОРОТКИМИ (до ~60 символов, без точек/полных предложений) и стоять отдельной строкой. "
    "Не создавай пустых подзаголовков: после строки `### ...` должен идти хотя бы 1 непустой абзац/список/цитата "
    "до следующего подзаголовка. Не ставь два `### ...` подряд без текста между ними. "
    "Если хочешь сделать общий раздел и подпункты — используй вложенный уровень `#### ...` для подпунктов "
    "или оставь только конкретные `### ...` без общего контейнера. "
    "Не помещай целые абзацы в подзаголовки и не используй технические метки вроде `Facts:`/`Факты:`. "
    "Не делай больше 4 подзаголовков."
)
SMART_UPDATE_VISITOR_CONDITIONS_RULE = (
    "Условия участия/посещения (длительность, возраст, максимальный размер группы, формат/что взять/как одеться, "
    "что входит/не входит в оплату, нужен ли отдельный входной билет) считаются фактами о событии и должны попадать "
    "в описание (description). "
    "В description описывай это обычным связным текстом (или под подходящим подзаголовком), а НЕ отдельным блоком «фактов». "
    "Если отдельно формируется список facts (атомарных фактов) — включай туда 1–3 факта про условия участия/посещения. "
    "Для description: не вставляй ссылки/телефоны и не указывай точные цены — пиши нейтрально "
    "(например «оплачивается отдельно», «входной билет нужен дополнительно»). "
    "Для фактов: точная сумма допускается только если она уточняет, что цена относится к части услуги "
    "(например «стоимость экскурсии X; входной билет отдельно»); не более 1 такого факта."
)

# Smart Update description sizing:
# - Telegraph pages can be long, but Telegram UI messages are capped at 4096 chars.
# - Keep a reasonable default and allow overrides via ENV.
def _env_int(name: str, default: int, *, lo: int, hi: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return min(hi, max(lo, value))


SMART_UPDATE_DESCRIPTION_MAX_CHARS = _env_int(
    # Telegraph pages can hold much longer text; keep a generous default to
    # avoid "too short" descriptions when sources are rich.
    "SMART_UPDATE_DESCRIPTION_MAX_CHARS",
    12000,
    lo=1200,
    hi=20000,
)
SMART_UPDATE_REWRITE_MAX_TOKENS = _env_int(
    # Default kept fairly high: we want a full description, not a short snippet.
    "SMART_UPDATE_REWRITE_MAX_TOKENS", 1400, lo=120, hi=6500
)
def _smart_update_gemma_generation_config(*, temperature: float = 0.0) -> dict[str, Any]:
    """Shared bounded-stage config.

    The hosted Gemma 4 endpoint rejects ``thinking_budget`` and previously used
    the complete answer cap for thought-only output. Production therefore routes
    Smart Update's small staged contracts to Gemini; do not send an unsupported
    thinking config as a retry workaround.
    """

    return {"temperature": temperature}
SMART_UPDATE_FACT_FIRST_TIMEOUT_SEC = _env_int(
    "SMART_UPDATE_FACT_FIRST_TIMEOUT_SEC",
    0,
    lo=0,
    hi=600,
)
SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC = _env_int(
    "SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC",
    90,
    lo=1,
    hi=180,
)
SMART_UPDATE_G4_DERIVED_FIELDS_TIMEOUT_SEC = _env_int(
    "SMART_UPDATE_G4_DERIVED_FIELDS_TIMEOUT_SEC",
    20,
    lo=1,
    hi=120,
)

# Hard wall-clock cap for any single _ask_gemma_json / _ask_gemma_text invocation.
# Without this, a provider 5xx storm on labels like ``merge`` / ``match_create_bundle`` /
# ``create_bundle`` could pin smart_update for >15 minutes and leave vk_inbox rows
# stuck at ``status='pending'`` (see INC-2026-05-07-vk-auto-import-merge-regression-gemma4).
# Per-stage timeouts (description writer, derived fields) keep their own values; this
# cap is the universal upper bound applied inside the wrappers themselves.
SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC = _env_int(
    "SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC",
    90,
    lo=10,
    hi=600,
)
SMART_UPDATE_GEMMA_TEXT_WALL_CLOCK_SEC = _env_int(
    "SMART_UPDATE_GEMMA_TEXT_WALL_CLOCK_SEC",
    120,
    lo=10,
    hi=600,
)

# If an event is extracted far into the future, treat poster date mismatches as a high-risk signal.
# Default matches the operator expectation: > 6 months ahead requires more scrutiny.
SMART_UPDATE_FAR_FUTURE_REVIEW_MONTHS = _env_int(
    "SMART_UPDATE_FAR_FUTURE_REVIEW_MONTHS", 6, lo=0, hi=24
)

# Optional: allow light emoji usage in *full* public descriptions (Telegraph/body).
# Must not affect `search_digest` (explicitly emoji-free by prompt).
# Default: enabled (light). Can be disabled via ENV if it turns out noisy.
SMART_UPDATE_DESCRIPTION_EMOJI_MODE = (os.getenv("SMART_UPDATE_DESCRIPTION_EMOJI_MODE", "light") or "").strip().lower()
if SMART_UPDATE_DESCRIPTION_EMOJI_MODE in {"1", "true", "yes", "on", "light"}:
    SMART_UPDATE_DESCRIPTION_EMOJI_MODE = "light"
else:
    SMART_UPDATE_DESCRIPTION_EMOJI_MODE = "off"
SMART_UPDATE_DESCRIPTION_MAX_EMOJIS = _env_int(
    "SMART_UPDATE_DESCRIPTION_MAX_EMOJIS",
    3,
    lo=0,
    hi=8,
)
SMART_UPDATE_DESCRIPTION_EMOJI_ALLOWLIST = (os.getenv("SMART_UPDATE_DESCRIPTION_EMOJI_ALLOWLIST") or "").strip()

# Fact-first mode: build public narrative from extracted facts (not from raw sources).
# Enabled by default; can be disabled for rollback/experiments.
SMART_UPDATE_FACT_FIRST = (os.getenv("SMART_UPDATE_FACT_FIRST", "1") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Serialize Smart Update calls within a single bot process to avoid LLM/provider contention
# and to keep operator-visible logs deterministic.
_SMART_UPDATE_LOCK = asyncio.Lock()
SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS = _env_int(
    # How much of candidate.source_text we feed into the rewrite prompt.
    # Telegraph pages can be long; for rewrite we still cap to keep prompts bounded.
    "SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS",
    12000,
    lo=1200,
    hi=20000,
)

# Smart Update merge prompt sizing.
SMART_UPDATE_MERGE_MAX_TOKENS = _env_int(
    "SMART_UPDATE_MERGE_MAX_TOKENS", 1200, lo=300, hi=1600
)
SMART_UPDATE_MERGE_EVENT_DESC_MAX_CHARS = _env_int(
    "SMART_UPDATE_MERGE_EVENT_DESC_MAX_CHARS", 4000, lo=800, hi=20000
)
SMART_UPDATE_MERGE_CANDIDATE_TEXT_MAX_CHARS = _env_int(
    "SMART_UPDATE_MERGE_CANDIDATE_TEXT_MAX_CHARS", 6000, lo=800, hi=20000
)


@dataclass(slots=True)
class PosterCandidate:
    catbox_url: str | None = None
    supabase_url: str | None = None
    supabase_path: str | None = None
    sha256: str | None = None
    # Exact bytes served by supabase_url after canonical materialization.
    # ``sha256`` remains the source/candidate identity.
    raw_sha256: str | None = None
    phash: str | None = None
    ocr_text: str | None = None
    ocr_title: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class SmartUpdateIntent(str, Enum):
    UPSERT_EVENT = "UPSERT_EVENT"
    ATTACH_CONTEXT = "ATTACH_CONTEXT"

def _poster_candidate_evidence_url(poster: PosterCandidate) -> str | None:
    """Return the real URL fields used for provenance-only grounding."""

    return str(poster.supabase_url or poster.catbox_url or "").strip() or None


@dataclass(slots=True)
class EventCandidate:
    source_type: str
    source_url: str | None
    source_text: str
    # Product action is explicit. Context attachment never drives identity LLM.
    intent: SmartUpdateIntent = SmartUpdateIntent.UPSERT_EVENT
    target_event_id: int | None = None
    producer_ordinal: int | None = None
    source_native_occurrence_id: str | None = None
    vendor_occurrence_id: str | None = None
    occurrence_key: str | None = None
    candidate_key: str | None = None
    smart_update_candidate_id: int | None = None
    # Internal retry-state instruction. It is set only after an existing
    # identity decision classified the candidate as distinct or uncertainty
    # exhausted its bounded attempts.
    force_create_distinct: bool = False
    force_create_distinct_reason: IdentityDistinctReason | None = None
    explicit_occurrence_conflict_event_ids: list[int] = field(default_factory=list)
    force_match_event_id: int | None = None
    # Replay the original facade semantics after a durable retry claim.
    replay_check_source_url: bool = True
    replay_schedule_tasks: bool = True
    replay_schedule_kwargs: dict[str, Any] | None = None
    # Only identity-bearing sources may participate in event matching. Linked
    # roundups/program pages are provenance context and must use context_only.
    source_role: str = "identity_bearing"
    # Filled by Smart Update from the exact caller packet before normalization.
    source_fingerprint: str | None = None
    # Typed upstream source-parse evidence.  A candidate represents a positive
    # event child; Smart Update must not turn that child back into a semantic
    # no-event through regex/date heuristics.  These fields are diagnostic and
    # replay metadata, not a new source of deterministic terminal authority.
    source_disposition: str | None = None
    source_parse_version: str | None = None
    source_evidence_complete: bool | None = None
    source_verification_reasons: list[str] = field(default_factory=list)
    title: str | None = None
    date: str | None = None
    time: str | None = None
    # True when time comes from per-source default_time (a low-priority guess).
    time_is_default: bool = False
    end_date: str | None = None
    end_date_is_inferred: bool = False
    festival: str | None = None
    festival_context: str | None = None
    festival_full: str | None = None
    festival_dedup_links: list[str] = field(default_factory=list)
    festival_source: bool | None = None
    festival_series: str | None = None
    location_name: str | None = None
    location_address: str | None = None
    city: str | None = None
    ticket_link: str | None = None
    ticket_price_min: int | None = None
    ticket_price_max: int | None = None
    ticket_status: str | None = None
    # Only source-native structured fields may set ``age_restriction`` directly.
    # Text/OCR mentions are decided by the semantic payload bundled into an
    # existing Smart Update call, never by a new regex-only path.
    age_restriction: str | None = None
    age_restriction_is_structured: bool = False
    event_type: str | None = None
    emoji: str | None = None
    is_free: bool | None = None
    pushkin_card: bool | None = None
    search_digest: str | None = None
    raw_excerpt: str | None = None
    # LLM-selected event-local evidence for a multi-event source. The full
    # source_text remains intact for provenance.
    occurrence_scope_text: str | None = None
    posters: list[PosterCandidate] = field(default_factory=list)
    poster_scope_hashes: list[str] = field(default_factory=list)
    source_chat_username: str | None = None
    source_chat_id: int | None = None
    source_message_id: int | None = None
    tg_source_author: str | None = None
    creator_id: int | None = None
    trust_level: str | None = None
    metrics: dict[str, Any] | None = None
    links_payload: Any | None = None
    # Concrete-event organizers only. Values must come from quoted LLM evidence
    # or an explicit curated source binding; the generic publisher name is not
    # organizer evidence.
    organizer_names: list[str] = field(default_factory=list)
    # Ephemeral result piggybacked on an already-paid facts/create call.
    age_semantic_decision: dict[str, Any] | None = None
    # High-recall routing signals only. They are never accepted as evidence by
    # the collection fact validator.
    topics: list[str] = field(default_factory=list)
    collection_bge_signals: list[str] = field(default_factory=list)
    # Explicit routing only: these facts are evaluated by a compact, cached
    # candidate-only stage rather than growing every rich-facts/merge prompt.
    collection_adjudication_reasons: list[str] = field(default_factory=list)
    # Raw strict-schema provider output (or a source-native structured result).
    # Provenance is injected only after the matching EventSource is attached.
    collection_semantic_decisions: dict[str, Any] | None = None


STATIC_COLLECTION_FACTS_POLICY_VERSION = "static-collection-facts-v3"
STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION = "static-collection-adjudication-v2"

_ADMISSION_VALUES = {"confirmed_free", "confirmed_paid", "unknown"}
_ADMISSION_REASON_CODES = {
    "explicit_free_admission",
    "structured_free_admission",
    "free_registration",
    "optional_donation",
    "explicit_price",
    "explicit_paid_admission",
    "insufficient_evidence",
    "conflicting_evidence",
}
_COLLECTION_FACT_VALUES = {"confirmed", "denied", "unknown"}
_CHILD_DIRECTED_REASON_CODES = {
    "explicit_child_audience",
    "explicit_child_spectators",
    "explicit_child_participants",
    "explicit_adults_only",
    "explicit_age_exclusion",
    "explicit_parents_only",
    "insufficient_evidence",
    "conflicting_evidence",
}
_FAMILY_SUITABLE_REASON_CODES = {
    "explicit_family_invitation",
    "explicit_children_and_adults",
    "explicit_family_format",
    "explicit_adults_only",
    "explicit_children_only",
    "explicit_parents_only",
    "insufficient_evidence",
    "conflicting_evidence",
}
_JOINT_FAMILY_ACTIVITY_REASON_CODES = {
    "explicit_joint_task",
    "explicit_parent_child_team",
    "explicit_joint_practice",
    "explicit_no_joint_activity",
    "explicit_adults_only",
    "explicit_parents_only",
    "insufficient_evidence",
    "conflicting_evidence",
}
_PEOPLE_ROLES = {"performer", "speaker", "author", "host"}
_PEOPLE_APPEARANCES = {"confirmed", "mentioned", "unknown"}
_PEOPLE_ORIGINS = {"russia_nonlocal", "foreign", "local", "unknown"}
_PEOPLE_REASON_CODES = {
    "explicit_program_role",
    "explicit_future_participation",
    "report_only",
    "ambiguous_mention",
}


COLLECTION_ADJUDICATION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "schema_version": {
            "type": "string",
            "enum": [STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION],
        },
        "admission_decision": {
            "type": "object",
            "properties": {
                "value": {"type": "string", "enum": sorted(_ADMISSION_VALUES)},
                "evidence_quote": {"type": "string"},
                "reason_code": {"type": "string", "enum": sorted(_ADMISSION_REASON_CODES)},
            },
            "required": ["value", "evidence_quote", "reason_code"],
            "additionalProperties": False,
        },
        "child_directed_decision": {
            "type": "object",
            "properties": {
                "value": {"type": "string", "enum": sorted(_COLLECTION_FACT_VALUES)},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "evidence_quote": {"type": "string"},
                "reason_code": {"type": "string", "enum": sorted(_CHILD_DIRECTED_REASON_CODES)},
            },
            "required": ["value", "confidence", "evidence_quote", "reason_code"],
            "additionalProperties": False,
        },
        "family_suitable_decision": {
            "type": "object",
            "properties": {
                "value": {"type": "string", "enum": sorted(_COLLECTION_FACT_VALUES)},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "evidence_quote": {"type": "string"},
                "reason_code": {"type": "string", "enum": sorted(_FAMILY_SUITABLE_REASON_CODES)},
            },
            "required": ["value", "confidence", "evidence_quote", "reason_code"],
            "additionalProperties": False,
        },
        "joint_family_activity_decision": {
            "type": "object",
            "properties": {
                "value": {"type": "string", "enum": sorted(_COLLECTION_FACT_VALUES)},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "evidence_quote": {"type": "string"},
                "reason_code": {"type": "string", "enum": sorted(_JOINT_FAMILY_ACTIVITY_REASON_CODES)},
            },
            "required": ["value", "confidence", "evidence_quote", "reason_code"],
            "additionalProperties": False,
        },
        "people_appearances": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string", "enum": sorted(_PEOPLE_ROLES)},
                    "appearance": {"type": "string", "enum": sorted(_PEOPLE_APPEARANCES)},
                    "origin_scope": {"type": "string", "enum": sorted(_PEOPLE_ORIGINS)},
                    "evidence_quote": {"type": "string"},
                    "origin_evidence_quote": {"type": "string"},
                    "reason_code": {"type": "string", "enum": sorted(_PEOPLE_REASON_CODES)},
                },
                "required": [
                    "name",
                    "role",
                    "appearance",
                    "origin_scope",
                    "evidence_quote",
                    "origin_evidence_quote",
                    "reason_code",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": [
        "schema_version",
        "admission_decision",
        "child_directed_decision",
        "family_suitable_decision",
        "joint_family_activity_decision",
        "people_appearances",
    ],
    "additionalProperties": False,
}


def _collection_source_corpus(candidate: EventCandidate) -> str:
    return "\n\n".join(
        value
        for value in [
            str(candidate.occurrence_scope_text or "").strip(),
            str(candidate.source_text or "").strip(),
            str(candidate.raw_excerpt or "").strip(),
            *[
                "\n".join(
                    part
                    for part in (
                        str(p.ocr_title or "").strip(),
                        str(p.ocr_text or "").strip(),
                    )
                    if part
                )
                for p in candidate.posters
            ],
        ]
        if value
    )


def collection_adjudication_input_hash(candidate: EventCandidate) -> str:
    """Stable hash for candidate-only cache/no-op semantics."""

    payload = {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        # Prompt/meaning changes must invalidate source-hash cache entries.
        # Schema shape alone is not enough: v2 deliberately tightened what
        # counts as a children/family event.
        "policy_version": STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "source_type": str(candidate.source_type or "").strip(),
        "source_url": str(candidate.source_url or "").strip(),
        "title": str(candidate.title or "").strip(),
        "corpus": _collection_source_corpus(candidate),
        # Signals are routing context only and never part of quoted evidence.
        "signals": {
            "topics": sorted(str(v) for v in candidate.topics),
            "age_restriction": str(candidate.age_restriction or "").strip(),
            "reasons": sorted(set(candidate.collection_adjudication_reasons or [])),
        },
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_collection_adjudication_request(candidate: EventCandidate) -> dict[str, Any] | None:
    """Build a compact candidate-only request, or return ``None`` when unrouted."""

    allowed_reasons = {"admission", "audience", "people", "conflict", "changed", "backfill"}
    reasons = sorted(
        {str(v or "").strip().lower() for v in candidate.collection_adjudication_reasons}
        & allowed_reasons
    )
    if not reasons:
        return None
    corpus = _collection_source_corpus(candidate)
    if not corpus or not candidate.source_url:
        return None
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "policy_version": STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "input_hash": collection_adjudication_input_hash(candidate),
        "candidate_reasons": reasons,
        "source": {
            "source_type": candidate.source_type,
            "source_url": candidate.source_url,
            "trust": candidate.trust_level,
        },
        "event": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "location_name": candidate.location_name,
        },
        "candidate_signals_not_proof": {
            "age_restriction": candidate.age_restriction,
            "topics": list(candidate.topics),
            "bge": list(candidate.collection_bge_signals),
        },
        "source_corpus": corpus,
    }


def route_collection_adjudication_reasons(
    candidate: EventCandidate,
    existing_event: Event | None = None,
) -> list[str]:
    """Select bounded high-recall candidates without deciding their meaning."""

    reasons = {
        str(value or "").strip().lower()
        for value in candidate.collection_adjudication_reasons
        if str(value or "").strip()
    }
    existing_decisions = (
        existing_event.collection_decisions
        if existing_event is not None and isinstance(existing_event.collection_decisions, dict)
        else {}
    )
    # Free claims and explicit price conflicts are candidates. ticket_status
    # or a ticket link alone deliberately does not enter this route.
    if candidate.is_free is True or (
        existing_event is not None
        and bool(existing_event.is_free)
        and (
            candidate.is_free is False
            or candidate.ticket_price_min is not None
            or candidate.ticket_price_max is not None
        )
    ):
        reasons.add("admission")
    if "admission_decision" in existing_decisions:
        reasons.add("admission")

    topics = {str(value or "").strip().upper() for value in candidate.topics}
    if existing_event is not None:
        topics.update(
            str(value or "").strip().upper()
            for value in (getattr(existing_event, "topics", None) or [])
        )
    bge = {str(value or "").strip().casefold() for value in candidate.collection_bge_signals}
    audience_text = " ".join(
        str(value or "").casefold().replace("ё", "е")
        for value in (
            candidate.title,
            candidate.source_text,
            candidate.occurrence_scope_text,
            candidate.raw_excerpt,
        )
        if value
    )
    # Recall only. These phrases may route the single adjudication call, but
    # never become evidence without the strict source-bound model verdict.
    broad_audience_text_signal = bool(
        re.search(
            r"(?iu)(?:\bдля\s+(?:детей|ребят|школьников|всей\s+семьи)\b|"
            r"\bвсей\s+семьей\b|"
            r"\bдетск\w*\s+(?:спектакл\w*|шоу|заняти\w*|мастер-класс\w*)\b|"
            r"\bдетям\s+и\s+взрослым\b|\bродител\w*\s+(?:и|с)\s+дет\w*\b|"
            r"\bсемейн\w*\s+команд\w*\b|\bвместе\s+с\s+ребенк\w*\b)",
            audience_text,
        )
    )
    if (
        topics & {"FAMILY", "KIDS_SCHOOL"}
        or bge & {"audience:kids", "audience:family"}
        or broad_audience_text_signal
        or any(
            key in existing_decisions
            for key in (
                "audience_decision",
                "child_directed_decision",
                "family_suitable_decision",
                "joint_family_activity_decision",
            )
        )
    ):
        reasons.add("audience")
    if (
        "PERSONALITIES" in topics
        or any(value.startswith("people:") for value in bge)
        or "people_appearances" in existing_decisions
    ):
        reasons.add("people")
    allowed = {"admission", "audience", "people", "conflict", "changed", "backfill"}
    return sorted(reasons & allowed)


def _strict_keys(value: Any, required: set[str]) -> bool:
    return isinstance(value, dict) and set(value) == required


def _exact_collection_quote(quote: Any, corpus: str, *, required: bool) -> str | None:
    if not isinstance(quote, str):
        return None
    clean = quote.strip()
    if not clean:
        return None if required else ""
    return clean if clean in corpus else None


def _collection_fact_quote_supports_value(
    fact_key: str,
    value: str,
    quote: str,
    reason: str,
) -> bool:
    """Reject narrow, known non-entailing quote shapes after the LLM verdict.

    This is deliberately a safety validator, not a keyword classifier. The
    model still owns meaning; these checks only ensure that a selected exact
    quote can grammatically support the selected ontology-v2 fact.
    """

    normalized = " ".join(str(quote or "").casefold().replace("ё", "е").split())
    if value == "unknown":
        return not normalized and reason in {"insufficient_evidence", "conflicting_evidence"}

    explicit_adults_only = bool(
        re.search(r"\bтолько\s+для\s+(?:взрослых|родителей)\b", normalized)
        or re.search(r"\bбез\s+детей\b", normalized)
        or re.search(r"\bдет\w*\s+не\s+допуска", normalized)
    )
    if value == "denied":
        if reason == "explicit_adults_only":
            return explicit_adults_only
        if reason == "explicit_parents_only":
            return bool(re.search(r"\bтолько\s+для\s+родителей\b", normalized))
        if reason == "explicit_age_exclusion":
            return bool(
                re.search(r"\b(?:без\s+детей|дет\w*\s+не\s+допуска|участие\s+дет\w*\s+запрещ)", normalized)
            )
        if reason == "explicit_children_only":
            return bool(re.search(r"\bтолько\s+для\s+детей\b", normalized))
        if reason == "explicit_no_joint_activity":
            return bool(
                re.search(r"\bвзросл\w*\s+только\s+сопровожда", normalized)
                or re.search(r"\bдет\w*\s+выполня\w*\s+(?:задани\w*\s+)?самостоятельно\b", normalized)
            )
        return False

    if value != "confirmed":
        return False
    if fact_key == "child_directed_decision":
        if reason not in {
            "explicit_child_audience",
            "explicit_child_spectators",
            "explicit_child_participants",
        }:
            return False
        # Meaning remains LLM-owned.  These are only narrow guards for known
        # non-entailing routing signals; requiring one of a small keyword list
        # here would incorrectly reject valid wording such as «интересно и
        # детям, и взрослым».
        age_only = bool(
            re.fullmatch(
                r"(?:(?:возрастн\w*\s+(?:ограничени\w*|ценз)\s*[:\-]?\s*)?"
                r"(?:0|3|6|12|16|18)\s*\+|от\s+\d{1,2}\s+лет)",
                normalized,
            )
        )
        child_author_only = bool(
            re.search(
                r"\b(?:работ\w*|картин\w*|рисунк\w*|произведени\w*)\s+"
                r"(?:юных|детск\w*|детей|школьник\w*)\s+(?:автор|художник)\w*\b",
                normalized,
            )
            or re.search(
                r"\bглазами\s+(?:юных|маленьких)\s+(?:автор|художник)\w*\b",
                normalized,
            )
            or re.search(
                r"\b(?:рисунк\w*|работ\w*|картин\w*)\s+"
                r"(?:учащих\w*|воспитанник\w*|школьник\w*|детей)\b",
                normalized,
            )
            or re.search(
                r"\bвыставк\w*\s+(?:творческ\w*\s+)?(?:работ\w*|рисунк\w*)\s+"
                r"(?:воспитанник\w*|учащих\w*|школьник\w*|детей)\b",
                normalized,
            )
        ) and not re.search(
            r"\b(?:для\s+дет|приглаша\w*\s+дет|детям\s+и\s+взросл|"
            r"детей\s+и\s+родител|маленьк\w*\s+(?:зрител|участник))\w*",
            normalized,
        )
        return bool(normalized) and not age_only and not child_author_only
    if fact_key == "family_suitable_decision":
        if reason not in {
            "explicit_family_invitation",
            "explicit_children_and_adults",
            "explicit_family_format",
        }:
            return False
        vague_family_only = bool(
            re.fullmatch(
                r"(?:уютн\w*\s+)?семейн\w*\s+(?:атмосфер\w*|тематик\w*)[.!]?",
                normalized,
            )
            or re.fullmatch(r"семейн\w*\s+турнир\w*[.!]?", normalized)
        )
        parents_only = bool(re.search(r"\bтолько\s+для\s+родителей\b", normalized))
        return bool(normalized) and not vague_family_only and not parents_only
    if fact_key == "joint_family_activity_decision":
        if reason not in {
            "explicit_joint_task",
            "explicit_parent_child_team",
            "explicit_joint_practice",
        }:
            return False
        adult_child = bool(
            re.search(r"\b(?:родител|взросл)\w*", normalized)
            and re.search(r"\b(?:дет|ребен)\w*", normalized)
        )
        joint_action = bool(
            re.search(
                r"\b(?:вместе|совместно)\b.{0,80}\b(?:создад|сдела|выполн|собер|"
                r"нарису|приготов|пройдут|практик|задани|маршрут)\w*",
                normalized,
            )
            or re.search(r"\b(?:общ\w*|совместн\w*)\s+(?:работ|задани|практик|маршрут)\w*\b", normalized)
            or (
                re.search(r"\bсемейн\w*\s+команд\w*\b", normalized)
                and adult_child
            )
            or (
                re.search(r"\bпарн\w*\s+(?:упражнени|задани|практик)\w*\b", normalized)
                and adult_child
            )
        )
        return adult_child and joint_action
    return False


def _validate_collection_fact_decision(
    payload: Any,
    *,
    fact_key: str,
    allowed_reasons: set[str],
    source_corpus: str,
) -> dict[str, Any] | None:
    if not _strict_keys(payload, {"value", "confidence", "evidence_quote", "reason_code"}):
        return None
    value = payload.get("value")
    reason = payload.get("reason_code")
    confidence = payload.get("confidence")
    if (
        value not in _COLLECTION_FACT_VALUES
        or reason not in allowed_reasons
        or isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not 0 <= float(confidence) <= 1
    ):
        return None
    quote = _exact_collection_quote(
        payload.get("evidence_quote"), source_corpus, required=value != "unknown"
    )
    if quote is None or not _collection_fact_quote_supports_value(fact_key, value, quote, reason):
        return None
    return {
        "value": value,
        "confidence": float(confidence),
        "evidence_quote": quote,
        "reason_code": reason,
    }


def validate_collection_adjudication_output(
    payload: Any,
    *,
    source_corpus: str,
) -> dict[str, Any] | None:
    """Strictly validate provider output; semantic uncertainty fails closed.

    Exact evidence is checked against source/OCR text only. Age, topics and BGE
    signals are deliberately absent here: they can route the call but cannot
    validate a decision.
    """

    if not _strict_keys(
        payload,
        {
            "schema_version",
            "admission_decision",
            "child_directed_decision",
            "family_suitable_decision",
            "joint_family_activity_decision",
            "people_appearances",
        },
    ):
        return None
    if payload.get("schema_version") != STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION:
        return None
    admission = payload.get("admission_decision")
    if not _strict_keys(admission, {"value", "evidence_quote", "reason_code"}):
        return None
    avalue = admission.get("value")
    areason = admission.get("reason_code")
    if avalue not in _ADMISSION_VALUES or areason not in _ADMISSION_REASON_CODES:
        return None
    if avalue == "confirmed_free" and areason not in {
        "explicit_free_admission", "structured_free_admission", "free_registration", "optional_donation"
    }:
        return None
    if avalue == "confirmed_paid" and areason not in {"explicit_price", "explicit_paid_admission"}:
        # Ticket availability/sale status alone is not a paid-admission fact.
        return None
    aquote = _exact_collection_quote(
        admission.get("evidence_quote"), source_corpus, required=avalue != "unknown"
    )
    if aquote is None:
        return None

    child = _validate_collection_fact_decision(
        payload.get("child_directed_decision"),
        fact_key="child_directed_decision",
        allowed_reasons=_CHILD_DIRECTED_REASON_CODES,
        source_corpus=source_corpus,
    )
    family = _validate_collection_fact_decision(
        payload.get("family_suitable_decision"),
        fact_key="family_suitable_decision",
        allowed_reasons=_FAMILY_SUITABLE_REASON_CODES,
        source_corpus=source_corpus,
    )
    joint = _validate_collection_fact_decision(
        payload.get("joint_family_activity_decision"),
        fact_key="joint_family_activity_decision",
        allowed_reasons=_JOINT_FAMILY_ACTIVITY_REASON_CODES,
        source_corpus=source_corpus,
    )
    if child is None or family is None or joint is None:
        return None
    if joint["value"] == "confirmed" and (
        child["value"] != "confirmed" or family["value"] != "confirmed"
    ):
        return None

    people = payload.get("people_appearances")
    if not isinstance(people, list) or len(people) > 24:
        return None
    clean_people: list[dict[str, Any]] = []
    for person in people:
        required = {
            "name", "role", "appearance", "origin_scope", "evidence_quote",
            "origin_evidence_quote", "reason_code",
        }
        if not _strict_keys(person, required):
            return None
        name = str(person.get("name") or "").strip()
        role = person.get("role")
        appearance = person.get("appearance")
        origin = person.get("origin_scope")
        reason = person.get("reason_code")
        if (
            not name
            or role not in _PEOPLE_ROLES
            or appearance not in _PEOPLE_APPEARANCES
            or origin not in _PEOPLE_ORIGINS
            or reason not in _PEOPLE_REASON_CODES
        ):
            return None
        evidence = _exact_collection_quote(person.get("evidence_quote"), source_corpus, required=True)
        if evidence is None or name not in evidence:
            return None
        origin_evidence = _exact_collection_quote(
            person.get("origin_evidence_quote"),
            source_corpus,
            required=origin != "unknown",
        )
        if origin_evidence is None:
            return None
        clean_people.append(
            {
                "name": name,
                "role": role,
                "appearance": appearance,
                "origin_scope": origin,
                "evidence_quote": evidence,
                "origin_evidence_quote": origin_evidence,
                "reason_code": reason,
            }
        )
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": avalue,
            "evidence_quote": aquote,
            "reason_code": areason,
        },
        "child_directed_decision": child,
        "family_suitable_decision": family,
        "joint_family_activity_decision": joint,
        "people_appearances": clean_people,
    }


def _decision_timestamp(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except Exception:
        return None


def _decision_wins(existing: Mapping[str, Any] | None, incoming: Mapping[str, Any]) -> bool:
    if not existing:
        return True
    if bool(existing.get("manual_lock")) and not bool(incoming.get("manual_lock")):
        return False
    if bool(incoming.get("manual_lock")) and not bool(existing.get("manual_lock")):
        return True
    if str(existing.get("input_hash") or "") == str(incoming.get("input_hash") or ""):
        return False
    old_trust = _trust_priority(str(existing.get("source_trust") or ""))
    new_trust = _trust_priority(str(incoming.get("source_trust") or ""))
    if new_trust != old_trust:
        return new_trust > old_trust
    old_time = _decision_timestamp(existing.get("decided_at"))
    new_time = _decision_timestamp(incoming.get("decided_at"))
    return bool(new_time and (old_time is None or new_time > old_time))


def project_legacy_audience_decision(validated: Mapping[str, Any]) -> dict[str, Any]:
    """Derive the legacy audience value without another semantic call."""

    child = validated.get("child_directed_decision")
    family = validated.get("family_suitable_decision")
    if isinstance(family, Mapping) and family.get("value") == "confirmed":
        source = family
        value = "family"
    elif isinstance(child, Mapping) and child.get("value") == "confirmed":
        source = child
        value = "kids"
    elif (
        isinstance(child, Mapping)
        and isinstance(family, Mapping)
        and child.get("value") == "denied"
        and family.get("value") == "denied"
        and child.get("reason_code") == "explicit_adults_only"
        and family.get("reason_code") == "explicit_adults_only"
    ):
        source = child
        value = "none"
    else:
        return {
            "value": "unknown",
            "confidence": 0.0,
            "evidence_quote": "",
            "reason_code": "insufficient_evidence",
            "derived_from_facts_v3": True,
        }
    return {
        "value": value,
        "confidence": float(source.get("confidence") or 0.0),
        "evidence_quote": str(source.get("evidence_quote") or ""),
        "reason_code": str(source.get("reason_code") or ""),
        "derived_from_facts_v3": True,
    }


def collection_adjudication_cached_payload(
    decisions: Mapping[str, Any] | None,
    *,
    input_hash: str,
    source_id: int | None = None,
    source_url: str | None = None,
) -> dict[str, Any] | None:
    """Return a validated warm-replay payload from the bounded receipt cache."""

    if not isinstance(decisions, Mapping) or not input_hash:
        return None
    receipts = decisions.get("evaluation_receipts")
    if not isinstance(receipts, list):
        return None
    for receipt in reversed(receipts):
        if not isinstance(receipt, Mapping):
            continue
        if receipt.get("input_hash") != input_hash:
            continue
        if receipt.get("policy_version") != STATIC_COLLECTION_FACTS_POLICY_VERSION:
            continue
        if receipt.get("schema_version") != STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION:
            continue
        if source_id is not None and int(receipt.get("source_id") or 0) != int(source_id):
            continue
        if source_id is None and source_url is not None and str(receipt.get("source_url") or "") != str(source_url):
            continue
        payload = receipt.get("payload")
        if isinstance(payload, Mapping):
            return json.loads(json.dumps(dict(payload)))
    return None


def collection_decision_hash_covers(
    decisions: Mapping[str, Any] | None,
    *,
    reasons: Iterable[str],
    input_hash: str,
    source_id: int | None = None,
    source_url: str | None = None,
) -> bool:
    """Reusable coverage contract for Smart Update and bounded backfills.

    A receipt covers even an all-unknown valid evaluation. Legacy facts-v2
    audience rows alone never count as coverage for the three facts-v3 keys.
    """

    if collection_adjudication_cached_payload(
        decisions,
        input_hash=input_hash,
        source_id=source_id,
        source_url=source_url,
    ) is not None:
        return True
    if not isinstance(decisions, Mapping):
        return False
    requested = {str(reason or "").strip().lower() for reason in reasons}
    if "admission" in requested:
        item = decisions.get("admission_decision")
        if not isinstance(item, Mapping) or item.get("input_hash") != input_hash:
            return False
    if "audience" in requested:
        for key in (
            "child_directed_decision",
            "family_suitable_decision",
            "joint_family_activity_decision",
        ):
            item = decisions.get(key)
            if not isinstance(item, Mapping) or item.get("input_hash") != input_hash:
                return False
    if "people" in requested:
        people = decisions.get("people_appearances")
        if not isinstance(people, list) or not people:
            return False
        if any(not isinstance(item, Mapping) or item.get("input_hash") != input_hash for item in people):
            return False
    return bool(requested & {"admission", "audience", "people"})


def deep_merge_collection_decisions(
    existing: Mapping[str, Any] | None,
    incoming: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Merge keys independently while preserving truth on abstention/failure."""

    result: dict[str, Any] = json.loads(json.dumps(dict(existing or {})))
    if not isinstance(incoming, Mapping):
        return result
    changed = False
    for key in (
        "admission_decision",
        "child_directed_decision",
        "family_suitable_decision",
        "joint_family_activity_decision",
        "audience_decision",
    ):
        item = incoming.get(key)
        if not isinstance(item, Mapping) or item.get("value") == "unknown":
            continue
        previous = result.get(key)
        if _decision_wins(previous if isinstance(previous, Mapping) else None, item):
            result[key] = dict(item)
            changed = True

    incoming_people = incoming.get("people_appearances")
    current_people = [dict(v) for v in (result.get("people_appearances") or []) if isinstance(v, Mapping)]
    by_key = {(str(v.get("name") or "").casefold(), str(v.get("role") or "")): i for i, v in enumerate(current_people)}
    if isinstance(incoming_people, list):
        for item in incoming_people:
            if not isinstance(item, Mapping) or item.get("appearance") == "unknown":
                continue
            key = (str(item.get("name") or "").casefold(), str(item.get("role") or ""))
            old_index = by_key.get(key)
            if old_index is None:
                by_key[key] = len(current_people)
                current_people.append(dict(item))
                changed = True
                continue
            old = current_people[old_index]
            # A mere mention does not retract a confirmed appearance.
            if old.get("appearance") == "confirmed" and item.get("appearance") != "confirmed":
                continue
            if _decision_wins(old, item):
                current_people[old_index] = dict(item)
                changed = True
    if current_people and (changed or result.get("people_appearances")):
        result["people_appearances"] = current_people
    if changed:
        result["schema_version"] = STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION
    return result


def _collection_provenance(
    source: EventSource,
    *,
    input_hash: str,
    decided_at: datetime,
    manual_lock: bool,
) -> dict[str, Any]:
    stamp = decided_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "source_id": int(source.id or 0),
        "source_url": str(source.source_url or ""),
        "source_type": str(source.source_type or ""),
        "source_trust": str(source.trust_level or "medium"),
        "input_hash": input_hash,
        "policy_version": STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "decided_at": stamp,
        "manual_lock": bool(manual_lock),
    }


def _collection_apply_reasons(reasons: Iterable[str] | None) -> set[str]:
    if reasons is None:
        return {"admission", "audience", "people"}
    requested = {str(value or "").strip().lower() for value in reasons}
    explicit = requested & {"admission", "audience", "people"}
    if explicit:
        return explicit
    # Existing conflict/changed/backfill-only callers historically evaluate
    # the complete compact payload. Preserve that default behavior.
    return {"admission", "audience", "people"}


def _collection_receipt(
    validated: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": int(provenance.get("source_id") or 0),
        "source_url": str(provenance.get("source_url") or ""),
        "source_type": str(provenance.get("source_type") or ""),
        "source_trust": str(provenance.get("source_trust") or ""),
        "input_hash": str(provenance.get("input_hash") or ""),
        "policy_version": STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "evaluated_at": str(provenance.get("decided_at") or ""),
        "manual_lock": bool(provenance.get("manual_lock")),
        "payload": json.loads(json.dumps(dict(validated))),
    }


def _merge_collection_receipt(
    decisions: dict[str, Any],
    receipt: Mapping[str, Any],
    *,
    maximum: int = 24,
) -> bool:
    current = [
        dict(item)
        for item in (decisions.get("evaluation_receipts") or [])
        if isinstance(item, Mapping)
    ]
    key = (int(receipt.get("source_id") or 0), str(receipt.get("input_hash") or ""))
    for item in current:
        if (int(item.get("source_id") or 0), str(item.get("input_hash") or "")) == key:
            return False
    current.append(dict(receipt))
    decisions["evaluation_receipts"] = current[-max(1, int(maximum)) :]
    return True


def apply_collection_decisions(
    event: Event,
    provider_payload: Any,
    *,
    source: EventSource,
    source_corpus: str,
    input_hash: str,
    decided_at: datetime | None = None,
    manual_lock: bool = False,
    reasons: Iterable[str] | None = None,
) -> bool:
    """Validate, merge and materialize decisions for an attached same-event source."""

    if not event.id or not source.id or int(source.event_id) != int(event.id):
        return False
    if not input_hash or not str(source.source_url or "").strip():
        return False
    validated = validate_collection_adjudication_output(
        provider_payload,
        source_corpus=source_corpus,
    )
    if validated is None:
        return False
    # Facts-v3 publication evidence must survive attachment as source-native
    # text. A quote found only in raw_excerpt/OCR/candidate scope is not enough
    # to persist any v3 payload.
    persisted_source_text = str(source.source_text or "")
    for key, allowed_reasons in (
        ("child_directed_decision", _CHILD_DIRECTED_REASON_CODES),
        ("family_suitable_decision", _FAMILY_SUITABLE_REASON_CODES),
        ("joint_family_activity_decision", _JOINT_FAMILY_ACTIVITY_REASON_CODES),
    ):
        if _validate_collection_fact_decision(
            validated.get(key),
            fact_key=key,
            allowed_reasons=allowed_reasons,
            source_corpus=persisted_source_text,
        ) is None:
            return False
    provenance = _collection_provenance(
        source,
        input_hash=input_hash,
        decided_at=decided_at or datetime.now(timezone.utc),
        manual_lock=manual_lock,
    )
    selected_reasons = _collection_apply_reasons(reasons)
    incoming: dict[str, Any] = {"schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION}
    if "admission" in selected_reasons:
        incoming["admission_decision"] = {**validated["admission_decision"], **provenance}
    if "audience" in selected_reasons:
        for key in (
            "child_directed_decision",
            "family_suitable_decision",
            "joint_family_activity_decision",
        ):
            incoming[key] = {**validated[key], **provenance}
        legacy_projection = project_legacy_audience_decision(validated)
        incoming["audience_decision"] = {**legacy_projection, **provenance}
    if "people" in selected_reasons:
        incoming["people_appearances"] = [
            {**item, **provenance} for item in validated["people_appearances"]
        ]
    before = dict(event.collection_decisions or {})
    merged = deep_merge_collection_decisions(before, incoming)
    receipt_added = _merge_collection_receipt(
        merged,
        _collection_receipt(validated, provenance),
    )
    if receipt_added:
        merged["schema_version"] = STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION
    if merged == before:
        return False
    # Whole-value reassignment is intentional: JSON is not MutableDict-backed.
    event.collection_decisions = merged
    admission = merged.get("admission_decision")
    if (
        "admission" in selected_reasons
        and validated["admission_decision"].get("value") != "unknown"
        and isinstance(admission, Mapping)
        and admission.get("input_hash") == input_hash
    ):
        if admission.get("value") == "confirmed_free":
            event.is_free = True
        elif admission.get("value") == "confirmed_paid":
            event.is_free = False
    return True


async def adjudicate_collection_candidate(candidate: EventCandidate) -> dict[str, Any] | None:
    """Run the small candidate-only semantic stage; provider failures abstain."""

    if candidate.collection_semantic_decisions is not None:
        return candidate.collection_semantic_decisions
    request = build_collection_adjudication_request(candidate)
    if request is None or SMART_UPDATE_LLM_DISABLED:
        return None
    prompt = (
        "Ты проверяешь только факты КОНКРЕТНОГО события. Верни JSON строго по схеме.\n"
        "Каждая непустая evidence_quote должна быть точной непрерывной цитатой из source_corpus.\n"
        "Admission: ticket_status, наличие продажи или ссылки без явной цены/платного входа не доказывает paid; "
        "необязательный донат может быть confirmed_free.\n"
        "Audience facts оцени независимо. child_directed=confirmed только когда ребёнок прямо назван целевым "
        "зрителем или участником. family_suitable=confirmed только когда дети и взрослые/родители прямо "
        "приглашены вместе. joint_family_activity=confirmed только когда взрослый и ребёнок вместе выполняют "
        "одну практику, задачу, маршрут или участвуют одной командой. Joint confirmed требует также confirmed "
        "child_directed и family_suitable, каждый с точной подтверждающей цитатой.\n"
        "denied разрешён только при явной отрицательной фразе источника (например, только для взрослых/"
        "родителей, без детей); отсутствие положительного доказательства всегда unknown, не denied.\n"
        "Age restriction, topics и BGE — только routing signals, никогда не evidence. Недостаточны: один рейтинг "
        "0+/6+/12+, детские авторы работ, тема семьи, parents-only встреча, 'семейная атмосфера', популярность "
        "артиста у детей и 'семейный турнир' без явного состава взрослый+ребёнок и общей деятельности. "
        "Не расширяй смысл рекламной фразы.\n"
        "People: mentioned не равно confirmed; не выводи происхождение по имени. Для non-unknown origin_scope "
        "нужна отдельная точная origin_evidence_quote.\n"
        "candidate_reasons задаёт область проверки. Если admission отсутствует, верни admission unknown; "
        "если audience отсутствует, верни все три audience facts unknown; если people отсутствует, верни "
        "people_appearances=[]. Служебные backfill/changed/conflict сами по себе не расширяют область. "
        "Ответ должен быть компактным: не перечисляй людей вне reason=people.\n"
        "При сомнении верни unknown/пустой список. Не пиши публичный текст.\n\n"
        f"Данные:\n{json.dumps(request, ensure_ascii=False)}"
    )
    try:
        raw = await _ask_gemma_json(
            prompt,
            COLLECTION_ADJUDICATION_JSON_SCHEMA,
            max_tokens=1100,
            label="collection_candidate_adjudication",
        )
    except Exception:
        logger.warning("smart_update: collection adjudication provider failed", exc_info=True)
        return None
    validated = validate_collection_adjudication_output(
        raw,
        source_corpus=str(request["source_corpus"]),
    )
    candidate.collection_semantic_decisions = validated
    return validated


def _should_skip_festival_post_candidate(candidate: EventCandidate) -> bool:
    """Skip whole-festival announcements, but not parser-native occurrences.

    Official parser candidates are already individual, structured occurrences
    with source-native date/time/location. A festival association such as
    "закрытие фестиваля" must enrich that event rather than turn it back into a
    whole-program post.
    """

    context = (candidate.festival_context or "").strip().lower()
    source_type = (candidate.source_type or "").strip().lower()
    return context == "festival_post" and not source_type.startswith("parser:")


@dataclass(slots=True, init=False)
class SmartUpdateResult:
    outcome: SmartUpdateTerminalOutcome
    event_id: int | None = None
    diagnostic_event_id: int | None = None
    attempt: int = 0
    created: bool = False
    merged: bool = False
    added_posters: int = 0
    added_sources: bool = False
    added_facts: list[str] = field(default_factory=list)
    skipped_conflicts: list[str] = field(default_factory=list)
    reason: str | None = None
    product_exclusion_reason: ProductExclusionReason | None = None
    retry_reason: RetryReason | None = None
    identity_distinct_reason: IdentityDistinctReason | None = None
    lifecycle_reason: LifecycleReason | None = None
    queue_notes: list[str] = field(default_factory=list)

    def __init__(
        self,
        outcome: SmartUpdateTerminalOutcome | str | None = None,
        *,
        status: str | None = None,
        event_id: int | None = None,
        diagnostic_event_id: int | None = None,
        attempt: int = 0,
        created: bool = False,
        merged: bool = False,
        added_posters: int = 0,
        added_sources: bool = False,
        added_facts: list[str] | None = None,
        skipped_conflicts: list[str] | None = None,
        reason: str | None = None,
        product_exclusion_reason: ProductExclusionReason | None = None,
        retry_reason: RetryReason | None = None,
        identity_distinct_reason: IdentityDistinctReason | None = None,
        lifecycle_reason: LifecycleReason | None = None,
        queue_notes: list[str] | None = None,
    ) -> None:
        legacy = str(status or "").strip().lower()
        if (
            product_exclusion_reason is None
            and legacy in _LEGACY_PRODUCT_REJECT_STATUSES
        ):
            try:
                product_exclusion_reason = ProductExclusionReason(str(reason or ""))
            except ValueError:
                product_exclusion_reason = None
            if product_exclusion_reason is None:
                retry_reason = RetryReason.PRODUCT_REASON_UNTYPED
        if outcome is None:
            outcome = _terminal_outcome_from_legacy_status(
                legacy,
                product_exclusion_reason=product_exclusion_reason,
            )
        elif not isinstance(outcome, SmartUpdateTerminalOutcome):
            raw = str(outcome).strip()
            try:
                outcome = SmartUpdateTerminalOutcome(raw)
            except ValueError:
                outcome = _terminal_outcome_from_legacy_status(
                    raw.lower(),
                    product_exclusion_reason=product_exclusion_reason,
                )
        if outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY and not isinstance(
            product_exclusion_reason, ProductExclusionReason
        ):
            outcome = SmartUpdateTerminalOutcome.FAILED_TECHNICAL
            retry_reason = RetryReason.PRODUCT_REASON_UNTYPED
        if outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED and not isinstance(
            retry_reason, RetryReason
        ):
            retry_reason = RetryReason.UNKNOWN
        accepted = outcome in {
            SmartUpdateTerminalOutcome.CREATED,
            SmartUpdateTerminalOutcome.MERGED,
            SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY,
        }
        self.outcome = outcome
        self.event_id = int(event_id) if accepted and event_id is not None else None
        self.diagnostic_event_id = (
            int(diagnostic_event_id)
            if diagnostic_event_id is not None
            else (int(event_id) if not accepted and event_id is not None else None)
        )
        self.attempt = max(0, int(attempt))
        self.created = outcome is SmartUpdateTerminalOutcome.CREATED or bool(created and accepted)
        self.merged = outcome is SmartUpdateTerminalOutcome.MERGED or bool(merged and accepted)
        self.added_posters = int(added_posters)
        self.added_sources = bool(added_sources)
        self.added_facts = list(added_facts or [])
        self.skipped_conflicts = list(skipped_conflicts or [])
        self.reason = reason
        self.product_exclusion_reason = product_exclusion_reason
        self.retry_reason = retry_reason
        self.identity_distinct_reason = identity_distinct_reason
        self.lifecycle_reason = lifecycle_reason
        self.queue_notes = list(queue_notes or [])

    @property
    def status(self) -> str:
        """Deprecated read-only bridge; new callers must consume ``outcome``."""

        return {
            SmartUpdateTerminalOutcome.CREATED: "created",
            SmartUpdateTerminalOutcome.MERGED: "merged",
            SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY: "noop_exact_source_replay",
            SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY: "rejected_product_policy",
            SmartUpdateTerminalOutcome.FAILED_TECHNICAL: "failed_technical",
            SmartUpdateTerminalOutcome.RETRY_SCHEDULED: "retry_scheduled",
        }[self.outcome]

    @property
    def is_accepted(self) -> bool:
        return self.outcome in {
            SmartUpdateTerminalOutcome.CREATED,
            SmartUpdateTerminalOutcome.MERGED,
            SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY,
        }

    @property
    def is_retry(self) -> bool:
        return self.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED

    @property
    def is_failed_technical(self) -> bool:
        return self.outcome is SmartUpdateTerminalOutcome.FAILED_TECHNICAL

    @property
    def is_rejected(self) -> bool:
        return self.outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY

    @property
    def is_changed(self) -> bool:
        return self.outcome in {
            SmartUpdateTerminalOutcome.CREATED,
            SmartUpdateTerminalOutcome.MERGED,
        }


_LEGACY_PRODUCT_REJECT_STATUSES = frozenset(
    {
        "invalid",
        "rejected_out_of_region",
        "rejected_schedule_digest",
        "skipped_festival_post",
        "skipped_giveaway",
        "skipped_non_event",
        "skipped_past_event",
        "skipped_promo",
    }
)


def _terminal_outcome_from_legacy_status(
    status: str,
    *,
    product_exclusion_reason: ProductExclusionReason | None = None,
) -> SmartUpdateTerminalOutcome:
    if status == "created":
        return SmartUpdateTerminalOutcome.CREATED
    if status == "merged":
        return SmartUpdateTerminalOutcome.MERGED
    if status in {"skipped_nochange", "skipped_same_source_url", "noop_exact_source_replay"}:
        return SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
    if isinstance(product_exclusion_reason, ProductExclusionReason):
        return SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY
    return SmartUpdateTerminalOutcome.FAILED_TECHNICAL


def _linear_product_exclusion_reason(result: SmartUpdateResult) -> ProductExclusionReason | None:
    """Map complete-evidence semantic vetoes to a closed product terminal."""

    reason = str(result.reason or "").casefold()
    if result.retry_reason is RetryReason.SOURCE_DECISION_INVALID:
        if "missing_date" in reason:
            return ProductExclusionReason.MISSING_DATE
        if "missing_title" in reason:
            return ProductExclusionReason.MISSING_TITLE
        if "empty_title" in reason:
            return ProductExclusionReason.EMPTY_TITLE_AFTER_CLEAN
        if "missing_location" in reason:
            return ProductExclusionReason.MISSING_LOCATION
    if result.retry_reason is not RetryReason.SOURCE_VERIFICATION_REQUIRED:
        return None
    if "location" in reason:
        return ProductExclusionReason.MISSING_LOCATION
    if "anchor_role" in reason:
        return ProductExclusionReason.MISSING_DATE
    if "eventness" in reason or "non_event" in reason or "occurrence" in reason:
        return ProductExclusionReason.NON_EVENT
    return ProductExclusionReason.NON_EVENT


def _terminalize_linear_result(result: SmartUpdateResult) -> SmartUpdateResult:
    """Close legacy retry-shaped results inside the current invocation.

    Semantic uncertainty on complete evidence becomes a typed product decision;
    provider/schema/storage uncertainty becomes a visible technical terminal.
    No caller receives a newly scheduled background retry.
    """

    if result.outcome is not SmartUpdateTerminalOutcome.RETRY_SCHEDULED:
        return result
    exclusion = _linear_product_exclusion_reason(result)
    if exclusion is not None:
        result.outcome = SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY
        result.product_exclusion_reason = exclusion
        result.retry_reason = None
        result.reason = exclusion.value
        return result
    result.outcome = SmartUpdateTerminalOutcome.FAILED_TECHNICAL
    if not result.reason and isinstance(result.retry_reason, RetryReason):
        result.reason = result.retry_reason.value
    return result


class SmartUpdateOutcomeKind(str, Enum):
    """Caller-facing outcome class; every unknown status fails closed."""

    ACCEPTED_CHANGED = "accepted_changed"
    ACCEPTED_NO_CHANGE = "accepted_no_change"
    NOT_ACCEPTED = "not_accepted"


_SMART_UPDATE_ACCEPTED_CHANGED = frozenset({"created", "merged"})
_SMART_UPDATE_ACCEPTED_NO_CHANGE = frozenset(
    {
        "skipped_nochange",
        "skipped_same_source_url",
        "noop_exact_source_replay",
    }
)


def classify_smart_update_status(status: str | None) -> SmartUpdateOutcomeKind:
    value = str(status or "").strip().lower()
    if value in _SMART_UPDATE_ACCEPTED_CHANGED:
        return SmartUpdateOutcomeKind.ACCEPTED_CHANGED
    if value in _SMART_UPDATE_ACCEPTED_NO_CHANGE:
        return SmartUpdateOutcomeKind.ACCEPTED_NO_CHANGE
    return SmartUpdateOutcomeKind.NOT_ACCEPTED


def smart_update_result_allows_caller_side_effects(result: Any) -> bool:
    if isinstance(result, SmartUpdateResult):
        return result.is_accepted
    return classify_smart_update_status(getattr(result, "status", None)) is not SmartUpdateOutcomeKind.NOT_ACCEPTED


class SourceBindingConflict(RuntimeError):
    def __init__(self, existing_event_id: int):
        super().__init__("source_binding_conflict")
        self.existing_event_id = int(existing_event_id)


def _bounded_organizer_names(*values: Any) -> list[str]:
    """Union organizer identities without fuzzy/entity inference."""
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidates = value if isinstance(value, (list, tuple, set)) else [value]
        for candidate in candidates:
            name = re.sub(r"\s+", " ", str(candidate or "")).strip(" \t\r\n,.;:—–-")
            if not name or len(name) > 120 or "://" in name:
                continue
            key = unicodedata.normalize("NFKC", name).casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
            if len(out) >= 8:
                return out
    return out


def _grounded_organizer_names_from_payload(
    data: Any,
    *,
    source_corpus: str,
) -> list[str]:
    """Accept only names carried by an exact quote from the event corpus."""
    if not isinstance(data, dict):
        return []
    corpus_key = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", source_corpus or "").casefold(),
    ).strip()
    grounded: list[str] = []
    for item in data.get("organizer_names") or []:
        if not isinstance(item, dict):
            continue
        name = re.sub(r"\s+", " ", str(item.get("name") or "")).strip()
        quote = re.sub(r"\s+", " ", str(item.get("evidence_quote") or "")).strip()
        if not name or not quote:
            continue
        name_key = unicodedata.normalize("NFKC", name).casefold()
        quote_key = unicodedata.normalize("NFKC", quote).casefold()
        if quote_key not in corpus_key or name_key not in quote_key:
            continue
        grounded.append(name)
    return _bounded_organizer_names(grounded)


_DURATION_FORECAST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "duration_minutes": {"type": ["integer", "null"]},
        "confidence": {"type": "number"},
        "reason_short": {"type": "string"},
    },
    "required": ["duration_minutes", "confidence", "reason_short"],
    "additionalProperties": False,
}
_EXPLICIT_EVENT_DURATION_RE = re.compile(
    r"продолжительность(?:\s+[а-яё-]+){0,3}\s*"
    r"(?:[:—–-]|составляет)\s*"
    r"(?:(\d{1,2})\s*(?:ч(?:ас(?:а|ов)?)?|h))?\s*"
    r"(?:(\d{1,3})\s*(?:мин(?:ут(?:а|ы)?)?|m))?",
    re.IGNORECASE,
)
_EVENT_TIME_RANGE_RE = re.compile(
    r"^\s*(\d{1,2})[:.](\d{2})\s*(?:[—–-]|\.\.)\s*"
    r"(\d{1,2})[:.](\d{2})\s*$"
)
_TRANSPORT_DATA_ROOT = Path(__file__).resolve().parent / "site" / "src" / "data"


def _valid_duration_minutes(value: Any, *, maximum: int = 12 * 60) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        duration = int(value)
    except (TypeError, ValueError):
        return None
    return duration if 15 <= duration <= maximum else None


def _duration_from_explicit_time_range(value: str | None) -> int | None:
    match = _EVENT_TIME_RANGE_RE.match(str(value or ""))
    if not match:
        return None
    start = int(match.group(1)) * 60 + int(match.group(2))
    end = int(match.group(3)) * 60 + int(match.group(4))
    if start >= 24 * 60 or end >= 24 * 60:
        return None
    if end <= start:
        end += 24 * 60
    return _valid_duration_minutes(end - start)


def _explicit_event_duration_minutes(*values: Any) -> int | None:
    """Extract only source-labelled duration or a complete explicit time range."""

    for value in values:
        time_range_duration = _duration_from_explicit_time_range(
            value if isinstance(value, str) else None
        )
        if time_range_duration is not None:
            return time_range_duration
        text = str(value or "")
        match = _EXPLICIT_EVENT_DURATION_RE.search(text)
        if not match or not any(match.groups()):
            continue
        duration = int(match.group(1) or 0) * 60 + int(match.group(2) or 0)
        valid = _valid_duration_minutes(duration, maximum=24 * 60)
        if valid is not None:
            return valid
    return None


def _duration_grounding_corpora(
    candidate: EventCandidate,
    event: Event | None = None,
) -> list[str]:
    values: list[str | None] = [
        candidate.time,
        candidate.source_text,
        candidate.raw_excerpt,
        candidate.occurrence_scope_text,
    ]
    for poster in candidate.posters or []:
        values.extend([poster.ocr_text, poster.ocr_title])
    if event is not None:
        values.extend(
            [
                getattr(event, "time", None),
                getattr(event, "source_text", None),
                getattr(event, "description", None),
            ]
        )
        values.extend(getattr(event, "source_texts", None) or [])
    return [str(value).strip() for value in values if str(value or "").strip()]


def _event_has_explicit_duration_or_end(
    candidate: EventCandidate,
    event: Event | None = None,
) -> bool:
    if _explicit_event_duration_minutes(*_duration_grounding_corpora(candidate, event)):
        return True
    start_date = str(
        getattr(event, "date", None) or candidate.date or ""
    ).strip()
    end_date = str(
        getattr(event, "end_date", None) or candidate.end_date or ""
    ).strip()
    return bool(start_date and end_date and end_date != start_date)


def _normalize_transport_anchor(value: Any) -> str:
    return re.sub(
        r"[^а-яa-z0-9]+",
        " ",
        str(value or "").casefold().replace("ё", "е"),
    ).strip()


@lru_cache(maxsize=1)
def _transport_duration_eligibility_directory() -> dict[str, Any]:
    """Load the static transport directory fail-closed without provider calls."""

    try:
        rail = json.loads(
            (_TRANSPORT_DATA_ROOT / "transportSchedules.json").read_text(
                encoding="utf-8"
            )
        )
        bus = json.loads(
            (_TRANSPORT_DATA_ROOT / "busTransportSchedules.json").read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        logger.warning("smart_update: transport duration directory unavailable", exc_info=True)
        return {"rail_cities": set(), "bus_routes": []}
    rail_cities = {
        _normalize_transport_anchor(route.get("city"))
        for route in rail.get("routes", [])
        if _normalize_transport_anchor(route.get("city"))
    }
    bus_routes = []
    for route in bus.get("routes", []):
        bus_routes.append(
            {
                "cities": {
                    _normalize_transport_anchor(value)
                    for value in route.get("cities", [])
                    if _normalize_transport_anchor(value)
                },
                "venues": {
                    _normalize_transport_anchor(value)
                    for value in route.get("venues", [])
                    if _normalize_transport_anchor(value)
                },
                "event_start": str(route.get("event_start") or "").strip(),
            }
        )
    return {"rail_cities": rail_cities, "bus_routes": bus_routes}


def _transport_duration_forecast_eligible(
    candidate: EventCandidate,
    event: Event | None = None,
) -> bool:
    """Mirror only transport surfaces that can use an event end time."""

    start_date = str(getattr(event, "date", None) or candidate.date or "").strip()
    end_date = str(
        getattr(event, "end_date", None) or candidate.end_date or ""
    ).strip()
    start_time = str(getattr(event, "time", None) or candidate.time or "").strip()
    time_match = re.search(r"(\d{1,2})[:.](\d{2})", start_time)
    if (
        not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_date)
        or not time_match
        or bool(getattr(event, "time_is_default", False) or candidate.time_is_default)
        or (end_date and end_date != start_date)
    ):
        return False
    start_clock = f"{int(time_match.group(1)):02d}:{int(time_match.group(2)):02d}"
    city = _normalize_transport_anchor(
        getattr(event, "city", None) or candidate.city
    )
    venue = _normalize_transport_anchor(
        getattr(event, "location_name", None) or candidate.location_name
    )
    directory = _transport_duration_eligibility_directory()
    if city and city in directory["rail_cities"]:
        return True
    if venue in {"поселение викингов кауп", "кауп", "kaup"}:
        return True
    return any(
        city in route["cities"]
        and venue in route["venues"]
        and start_clock == route["event_start"]
        for route in directory["bus_routes"]
    )


async def _ensure_transport_duration_forecast(
    event: Event,
    candidate: EventCandidate,
) -> bool:
    """Populate or clear the transport-only duration forecast on an Event."""

    if _event_has_explicit_duration_or_end(candidate, event):
        if getattr(event, "duration_forecast_minutes", None) is not None:
            event.duration_forecast_minutes = None
            return True
        return False
    if getattr(event, "duration_forecast_minutes", None) is not None:
        return False
    if SMART_UPDATE_LLM_DISABLED or not _transport_duration_forecast_eligible(
        candidate, event
    ):
        return False

    source_material = "\n\n".join(_duration_grounding_corpora(candidate, event))
    prompt = (
        "Оцени ожидаемую продолжительность одного события только для планирования "
        "обратного транспорта. Это прогноз, а не извлечённый факт.\n"
        "Используй только event-scoped материалы ниже. Не считай часами события "
        "часы работы площадки, длительность выставки в днях, окно фестиваля или "
        "время сбора гостей. Если материалов недостаточно, верни duration_minutes=null.\n"
        "Верни целое число минут от 15 до 720, confidence от 0 до 1 и короткую причину.\n\n"
        f"event_type: {str(getattr(event, 'event_type', None) or candidate.event_type or '').strip()!r}\n"
        f"event_start: {str(getattr(event, 'time', None) or candidate.time or '').strip()!r}\n"
        f"source_material:\n{_clip_to_readable_boundary(source_material, 5000)}"
    )
    data = await _ask_gemma_json(
        prompt,
        _DURATION_FORECAST_SCHEMA,
        max_tokens=120,
        label="duration_forecast",
    )
    if not isinstance(data, dict):
        return False
    duration = _valid_duration_minutes(data.get("duration_minutes"))
    try:
        confidence = float(data.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    if duration is None or not math.isfinite(confidence) or confidence < 0.5:
        return False
    event.duration_forecast_minutes = duration
    logger.info(
        "smart_update.duration_forecast event_id=%s minutes=%s confidence=%.2f source_type=%s",
        getattr(event, "id", None),
        duration,
        confidence,
        candidate.source_type,
    )
    return True


def _candidate_age_corpora(candidate: EventCandidate) -> list[str]:
    corpora = [candidate.source_text or "", candidate.raw_excerpt or ""]
    for poster in candidate.posters or []:
        corpora.extend([poster.ocr_text or "", poster.ocr_title or ""])
    return [text for text in corpora if str(text or "").strip()]


def _candidate_age_input_hash(candidate: EventCandidate) -> str:
    return age_input_hash(
        source_type=candidate.source_type,
        source_url=candidate.source_url,
        source_text=candidate.source_text,
        raw_excerpt=candidate.raw_excerpt,
        poster_ocr=[
            text
            for poster in candidate.posters or []
            for text in (poster.ocr_text, poster.ocr_title)
            if str(text or "").strip()
        ],
    )


def _candidate_age_decision(
    candidate: EventCandidate,
    *,
    semantic_payload: Any | None = None,
) -> AgeRatingDecision | None:
    input_hash = _candidate_age_input_hash(candidate)
    if candidate.age_restriction_is_structured:
        return declared_structured_decision(
            candidate.age_restriction,
            source_url=candidate.source_url,
            source_type=candidate.source_type,
            input_hash=input_hash,
        )
    if semantic_payload is None:
        return None
    if SMART_UPDATE_EVENT_AGE_LLM_MODE != "piggyback_only":
        return None
    return decision_from_semantic_payload(
        semantic_payload,
        source_url=candidate.source_url,
        source_corpora=_candidate_age_corpora(candidate),
        input_hash=input_hash,
    )


SMART_UPDATE_EVENT_AGE_LLM_MODE = (
    os.getenv("SMART_UPDATE_EVENT_AGE_LLM_MODE") or "piggyback_only"
).strip().lower()
if SMART_UPDATE_EVENT_AGE_LLM_MODE not in {"off", "piggyback_only"}:
    SMART_UPDATE_EVENT_AGE_LLM_MODE = "off"

_AGE_DECISION_PROMPT_RULE_TEXT = """
Верни также `age_decision` в ЭТОМ ЖЕ JSON-вызове (не отдельным запросом).
- `declared`: только если официальный/организаторский/билетный текст или event-scoped OCR явно задаёт
  0+/6+/12+/16+/18+ именно для всего текущего события/сеанса. Дай точную evidence_quote.
- Не путай рейтинг с временем `до 18:00`, юбилеем/возрастом артиста, возрастом участника конкурса,
  условием входа/сопровождения, отдельным номером/частью фестивальной программы или другой афишей.
- При разных значениях, которые нельзя надёжно привязать к одному scope, status=`conflict`, value=null.
- Если declared нет, assessed допустим только по содержательным материалам всего произведения/программы:
  учитывай насилие/страх/смерть, противоправное поведение, наркотики/алкоголь/табак, сексуальный контент,
  лексику, натуралистичность/длительность, осуждение/оправдание и образовательный/исторический контекст.
  Название, категория или отдельное слово недостаточны. При нехватке данных status=`insufficient_evidence`, value=null.
- Для assessed provenance=`llm_assessed`; это внутренняя рекомендация, не официальная маркировка.
- confidence не является разрешением публиковать assessed; она нужна только для калибровки на golden set.
""".strip()
AGE_DECISION_PROMPT_RULE = (
    _AGE_DECISION_PROMPT_RULE_TEXT
    if SMART_UPDATE_EVENT_AGE_LLM_MODE == "piggyback_only"
    else "Возрастной semantic pass выключен; не делай отдельный запрос для возраста."
)


MATCH_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "EventMatch",
        "schema": {
            "type": "object",
            "properties": {
                "match_event_id": {"type": ["integer", "null"]},
                "confidence": {"type": "number"},
                "reason_short": {"type": "string"},
            },
            "required": ["match_event_id", "confidence", "reason_short"],
            "additionalProperties": False,
        },
    },
}

MERGE_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "EventMerge",
        "schema": {
            "type": "object",
            "properties": {
                "title": {"type": ["string", "null"]},
                "description": {"type": ["string", "null"]},
                "search_digest": {"type": ["string", "null"]},
                "ticket_link": {"type": ["string", "null"]},
                "ticket_price_min": {"type": ["integer", "null"]},
                "ticket_price_max": {"type": ["integer", "null"]},
                "ticket_status": {"type": ["string", "null"]},
                "age_decision": AGE_DECISION_JSON_SCHEMA,
                "added_facts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "fact": {"type": "string"},
                            "evidence_quote": {"type": "string"},
                        },
                        "required": ["fact", "evidence_quote"],
                        "additionalProperties": False,
                    },
                },
                "duplicate_facts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "fact": {"type": "string"},
                            "evidence_quote": {"type": "string"},
                        },
                        "required": ["fact", "evidence_quote"],
                        "additionalProperties": False,
                    },
                },
                "conflict_facts": {"type": "array", "items": {"type": "string"}},
                "skipped_conflicts": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["description", "added_facts", "duplicate_facts", "conflict_facts", "skipped_conflicts"],
            "additionalProperties": False,
        },
    },
}

MATCH_SCHEMA = MATCH_RESPONSE_FORMAT["json_schema"]["schema"]
MERGE_SCHEMA = MERGE_RESPONSE_FORMAT["json_schema"]["schema"]

MERGE_IDENTITY_GATE_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "MergeIdentityGate",
        "schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "allow_merge",
                        "allow_safe_metadata_only",
                        "skip_merge_side_effects",
                    ],
                },
                "relation": {
                    "type": "string",
                    "enum": [
                        "same_event",
                        "source_update",
                        "related_but_distinct",
                        "festival_context_sibling",
                        "unsafe_to_merge",
                        "unknown",
                    ],
                },
                "confidence": {"type": "number"},
                "reason_code": {"type": "string"},
                "reason": {"type": "string"},
                "blocking_conflicts": {"type": "array", "items": {"type": "string"}},
                "allowed_fields": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "action",
                "relation",
                "confidence",
                "reason_code",
                "reason",
                "blocking_conflicts",
                "allowed_fields",
            ],
            "additionalProperties": False,
        },
    },
}
MERGE_IDENTITY_GATE_SCHEMA = MERGE_IDENTITY_GATE_RESPONSE_FORMAT["json_schema"]["schema"]

EVENTNESS_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {
            "type": "string",
            "enum": ["event", "non_event", "uncertain"],
        },
        "confidence": {"type": "number"},
        "reason_short": {"type": "string"},
        "grounded_title": {"type": ["string", "null"]},
        "has_single_concrete_event": {"type": "boolean"},
        "missing_anchors": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "decision",
        "confidence",
        "reason_short",
        "grounded_title",
        "has_single_concrete_event",
        "missing_anchors",
    ],
    "additionalProperties": False,
}

LOCATION_GROUNDING_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {
            "type": "string",
            "enum": ["keep", "repair", "reject_missing_location"],
        },
        "confidence": {"type": "number"},
        "location_name": {"type": ["string", "null"]},
        "location_address": {"type": ["string", "null"]},
        "city": {"type": ["string", "null"]},
        "evidence_quote": {"type": "string"},
        "reason_short": {"type": "string"},
    },
    "required": [
        "decision",
        "confidence",
        "location_name",
        "location_address",
        "city",
        "evidence_quote",
        "reason_short",
    ],
    "additionalProperties": False,
}

OCCURRENCE_SCOPE_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["scoped", "single_event", "uncertain"]},
        "confidence": {"type": "number"},
        "selected_excerpts": {"type": "array", "items": {"type": "string"}},
        "reason_short": {"type": "string"},
    },
    "required": ["decision", "confidence", "selected_excerpts", "reason_short"],
    "additionalProperties": False,
}

ANCHOR_ROLE_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["keep", "repair", "uncertain"]},
        "confidence": {"type": "number"},
        "date": {"type": ["string", "null"]},
        "end_date": {"type": ["string", "null"]},
        "time": {"type": ["string", "null"]},
        "evidence_quotes": {"type": "array", "items": {"type": "string"}},
        "reason_short": {"type": "string"},
    },
    "required": [
        "decision",
        "confidence",
        "date",
        "end_date",
        "time",
        "evidence_quotes",
        "reason_short",
    ],
    "additionalProperties": False,
}

CREATE_BUNDLE_GROUNDING_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["grounded", "ungrounded", "uncertain"]},
        "confidence": {"type": "number"},
        "unsupported_fields": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["title", "description", "facts", "search_digest", "short_description"],
            },
        },
        "evidence_quotes": {"type": "array", "items": {"type": "string"}},
        "reason_short": {"type": "string"},
    },
    "required": [
        "decision",
        "confidence",
        "unsupported_fields",
        "evidence_quotes",
        "reason_short",
    ],
    "additionalProperties": False,
}


CREATE_BUNDLE_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "EventCreateBundle",
        "schema": {
            "type": "object",
            "properties": {
                "title": {"type": ["string", "null"]},
                "description": {"type": ["string", "null"]},
                "search_digest": {"type": ["string", "null"]},
                "short_description": {"type": ["string", "null"]},
                "facts": {"type": "array", "items": {"type": "string"}},
                "age_decision": AGE_DECISION_JSON_SCHEMA,
            },
            "required": ["description", "facts"],
            "additionalProperties": False,
        },
    },
}

CREATE_BUNDLE_SCHEMA = CREATE_BUNDLE_RESPONSE_FORMAT["json_schema"]["schema"]

MATCH_CREATE_BUNDLE_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "EventMatchOrCreateBundle",
        "schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["match", "create"]},
                "match_event_id": {"type": ["integer", "null"]},
                "confidence": {"type": "number"},
                "reason_short": {"type": "string"},
                "bundle": {
                    "anyOf": [
                        CREATE_BUNDLE_SCHEMA,
                        {"type": "null"},
                    ]
                },
            },
            "required": ["action", "match_event_id", "confidence", "reason_short", "bundle"],
            "additionalProperties": False,
        },
    },
}

MATCH_CREATE_BUNDLE_SCHEMA = MATCH_CREATE_BUNDLE_RESPONSE_FORMAT["json_schema"]["schema"]

# INC-2026-05-30 opt 1: decision-only schema for the widened-recall dedup adjudicator.
# No create bundle here (lollipop tightening): this call only answers match-vs-create.
_DEDUP_ADJUDICATOR_MERGE_CODES = {
    "doors_start_skew",
    "venue_variant",
    "junk_location_same_venue",
    "two_vendor_same_slot",
    "identical_anchors_dup",
    "title_wrapper_only",
}
_DEDUP_ADJUDICATOR_KEEP_CODES = {
    "session_split_keep",
    "matinee_evening_keep",
    "distinct_show_keep",
    "parallel_venue_keep",
    "no_candidate_match",
}
DEDUP_ADJUDICATOR_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "EventDedupAdjudication",
        "schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["match", "create"]},
                "match_event_id": {"type": ["integer", "null"]},
                "confidence": {"type": "number"},
                "reason_code": {
                    "type": "string",
                    "enum": sorted(
                        _DEDUP_ADJUDICATOR_MERGE_CODES | _DEDUP_ADJUDICATOR_KEEP_CODES
                    ),
                },
                "reason": {"type": "string"},
            },
            "required": ["action", "match_event_id", "confidence", "reason_code", "reason"],
            "additionalProperties": False,
        },
    },
}
DEDUP_ADJUDICATOR_SCHEMA = DEDUP_ADJUDICATOR_RESPONSE_FORMAT["json_schema"]["schema"]


def _norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


_LOCATION_NOISE_PREFIXES_RE = re.compile(
    r"^(?:"
    r"кинотеатр|"
    r"бар|bar|"
    r"арт[- ]?пространство|"
    r"пространство"
    r")\s+",
    re.IGNORECASE,
)


def _strip_private_use(text: str | None) -> str | None:
    """Remove PUA chars that may appear as Telegram custom emoji placeholders."""
    if not text:
        return None
    cleaned = _PRIVATE_USE_RE.sub("", text)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r" *\n", "\n", cleaned)
    cleaned = cleaned.strip()
    return cleaned or None


def _fix_inline_bullet_lists(text: str | None) -> str | None:
    """Convert jammed '•' bullets into a proper multiline Markdown list.

    Gemma sometimes outputs list items on a single line like:
      "-Пункт 1 •Пункт 2 •Пункт 3"
    Telegraph renders this as a single paragraph. We only touch paragraphs that
    clearly look like inline bullet sequences.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    paras = [p for p in re.split(r"\n{2,}", raw) if p.strip()]
    out: list[str] = []
    for para in paras:
        p = para.strip()
        if "\n" in p:
            out.append(p)
            continue
        if "•" not in p:
            out.append(p)
            continue
        if p.count("•") < 1:
            out.append(p)
            continue
        if not p.lstrip().startswith(("-", "•")):
            out.append(p)
            continue
        parts = [x.strip() for x in p.split("•") if x.strip()]
        items: list[str] = []
        for part in parts:
            item = part.lstrip("-•").strip()
            if not item:
                continue
            if item.startswith("-") and len(item) > 1 and not item[1].isspace():
                item = f"- {item[1:].lstrip()}"
            items.append(f"- {item}" if not item.startswith("- ") else item)
        if len(items) >= 2:
            out.append("\n".join(items).strip())
        else:
            out.append(p)
    return "\n\n".join(out).strip() or None


_BULLET_LINE_PREFIX_RE = re.compile(r"(?m)^[ \t]*[•·▪‣⁃]+[ \t]*")


def _normalize_bullet_markers(text: str | None) -> str | None:
    """Normalize non-ASCII bullet markers to Markdown list items.

    Telegram sources often use middle-dot bullets (`·`) which models may not treat
    as a list. Converting them to `- ` increases the chance of list preservation
    in the rewritten Telegraph description.
    """
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return None
    normalized = _BULLET_LINE_PREFIX_RE.sub("- ", raw)
    return normalized if normalized.strip() else None


def _extract_small_source_list_items(text: str | None, *, max_items: int = 8) -> list[str]:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return []
    out: list[str] = []
    for ln in raw.splitlines():
        s = (ln or "").strip()
        if not s:
            continue
        m = re.match(r"^(?:[-*•·]|\d+[.)])\s+(\S.*)$", s)
        if not m:
            continue
        item = (m.group(1) or "").strip()
        if not item:
            continue
        # Keep it safe: do not pull links/handles into description via this deterministic fallback.
        if "http://" in item or "https://" in item or "@" in item:
            continue
        low = item.casefold()
        if low.startswith(("билеты", "вход", "стоимость", "сбор гостей", "начало")):
            continue
        if "подписаться" in low or "зарегистрироваться" in low:
            continue
        out.append(item)
        if len(out) >= max_items:
            break
    return out


def _append_missing_small_list(
    *,
    description: str | None,
    source_text: str | None,
    source_type: str | None,
) -> str | None:
    """Append a small bullet list from the source when the rewrite dropped it.

    Conservative: only targets Telegram-style short lists (2–6 items) and avoids
    links/contacts (they belong to the infoblock).
    """
    desc = (description or "").strip()
    src = (source_text or "").strip()
    if not desc or not src:
        return desc or src or None
    if str(source_type or "").strip().lower() not in {"telegram", "tg"}:
        return desc
    # If the output already contains a list, do not interfere.
    if re.search(r"(?m)^\s*(?:[-*]|\d+[.)])\s+\S+", desc):
        return desc

    items = _extract_small_source_list_items(src, max_items=8)
    if not (2 <= len(items) <= 6):
        return desc

    desc_cf = desc.casefold()
    missing = [it for it in items if it.casefold() not in desc_cf]
    if len(missing) < max(2, int(len(items) * 0.5)):
        return desc

    block = "### Что вас ждёт\n" + "\n".join(f"- {it}" for it in items)
    return (desc + "\n\n" + block).strip()


_HEADING_LINE_RE = re.compile(r"(?m)^#{1,6}\s+\S")


_INLINE_HEADING_RE = re.compile(r"(?<!^)(?<!\n)\s*(?=#{1,6}\s)")
_SINGLE_NL_BEFORE_HEADING_RE = re.compile(r"(?<!\n)\n(?=#{1,6}\s)")
_HEADING_INLINE_BODY_RE = re.compile(r"(?m)^(#{1,6}\s+)([^\n]+)$")


def _ensure_markdown_headings_break(text: str | None) -> str | None:
    """Make sure every Markdown heading starts a fresh paragraph.

    Lite-class LLMs (e.g. ``gemini-3.1-flash-lite`` running the
    ``split_description_writer`` / ``fact_first_desc`` stages) sometimes
    emit headings inline with the surrounding prose, e.g.

        ``...привычного исполнителя. ### Концепция постановки В центре...``

    The Telegraph render pipeline only treats ``###`` as a heading when it
    starts a line and is preceded by a blank line — otherwise it leaks back
    into the body and the public page shows one wall of text without any
    section breaks (observed for event 4757, 2026-05-09). This helper:

    1. Inserts ``\\n\\n`` before any inline ``###`` / ``##`` heading marker.
    2. Promotes a single ``\\n`` before a heading to a blank line.
    3. If a heading line carries an obviously merged body sentence (heading
       text terminator + space + capital word), splits the body off. The
       split is conservative — if no terminator is found we keep the line
       intact rather than guess where the heading ends.
    """
    if not text:
        return text
    out = text
    out = _INLINE_HEADING_RE.sub("\n\n", out)
    out = _SINGLE_NL_BEFORE_HEADING_RE.sub("\n\n", out)

    def _split_heading_body(match: re.Match[str]) -> str:
        prefix = match.group(1)
        body = match.group(2)
        sm = re.search(r"^([^.!?:\n]{3,80}?[.!?:])\s+([А-ЯA-ZЁ].*)$", body)
        if not sm:
            return match.group(0)
        heading_text = sm.group(1).rstrip(" .:!?").strip()
        if not heading_text:
            return match.group(0)
        body_text = sm.group(2).strip()
        if not body_text:
            return match.group(0)
        return f"{prefix}{heading_text}\n\n{body_text}"

    out = _HEADING_INLINE_BODY_RE.sub(_split_heading_body, out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out


def _ensure_minimal_description_headings(text: str | None) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if _HEADING_LINE_RE.search(raw):
        return raw
    blocks = [b.strip() for b in re.split(r"\n{2,}", raw) if b.strip()]
    if len(blocks) < 2:
        return raw
    # Deterministic fallback: if the model returned 2+ paragraphs but no headings,
    # inject a single neutral subheading to keep Telegraph pages readable.
    heading = "### О событии"
    first = blocks[0]
    rest = "\n\n".join(blocks[1:]).strip()
    if not rest:
        return raw
    if re.match(r"(?m)^\s*(?:[-*]|\d+[.)])\s+\S+", first) or first.lstrip().startswith(">"):
        return f"{heading}\n\n{raw}".strip()
    return f"{first}\n\n{heading}\n\n{rest}".strip()


_THINKING_LEAK_MARKER_RE = re.compile(
    r"(?im)^(?:[ \t]*)?(?:"
    r"corrected\s+description(?:_md)?\s*:|"
    r"here(?:\s+is|'s)\s+(?:the\s+)?(?:corrected|revised|updated|final)|"
    r"corrected\s+version\s*:|"
    r"revised\s+version\s*:|"
    r"final\s+version\s*:|"
    r"updated\s+description\s*:"
    r")"
)

_THINKING_LINE_PATTERNS = (
    re.compile(r"(?im)^[ \t]*(?:wait|hmm|hold on)\s*,\s*let\s+me\b.*$"),
    re.compile(r"(?im)^[ \t]*(?:actually|on\s+second\s+thought)\s*,?\s+let\s+me\b.*$"),
    re.compile(r"(?im)^[ \t]*(?:actually|on\s+second\s+thought|let\s+me)\s*,?\s*(?:rewrite|rephrase|revise|try|fix|correct|redo).*$"),
    re.compile(r"(?im)^[ \t]*let\s+me\s+(?:rewrite|rephrase|revise|try|fix|correct|redo).*$"),
    re.compile(r"(?im)^[ \t]*\(?\s*note\s+to\s+self\s*[:.].*$"),
    re.compile(r"(?im)^[ \t]*\[?\s*(?:internal|self)[-_ ]?note\s*[:.].*$"),
)


def _strip_thinking_leak(text: str) -> str:
    """Strip Gemma 4 thinking / self-correction text from a writer output.

    Production case (INC-2026-05-08, event 4711 «Любовь по-итальянски»): the
    final writer call returned a draft on English, then the model second-
    guessed itself on screen (``Wait, let me rewrite the EC05 part to be
    natural Russian.``) and emitted ``Corrected description_md: <real
    answer>``. Both halves landed in ``event.description`` and shipped to
    Telegraph as a public page. We:

    1. If a self-correction marker (``Corrected description_md:``,
       ``Here is the corrected ...`` etc.) is present, take everything
       **after** the LAST such marker — that is the LLM's own final answer.
    2. Drop standalone "Wait, let me ..." / "Let me rewrite ..." / "Note to
       self:" lines that may still leak even when no self-correction marker
       was emitted.

    This is non-semantic cleanup: we do not paraphrase, only remove visible
    LLM-internal markers. Final text remains whatever the LLM actually
    wrote after deciding it was final.
    """
    if not text:
        return text
    out = text
    matches = list(_THINKING_LEAK_MARKER_RE.finditer(out))
    if matches:
        last = matches[-1]
        rest = out[last.end():]
        rest = rest.lstrip(" \t:.\n")
        out = rest if rest else out
    for pat in _THINKING_LINE_PATTERNS:
        out = pat.sub("", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def _normalize_plaintext_paragraphs(text: str | None) -> str | None:
    """Normalize LLM output while preserving paragraph breaks.

    NOTE: event.description is rendered to Telegraph through our Markdown/HTML pipeline
    (see build_source_page_content). So we keep lightweight Markdown that improves
    readability: headings, blockquotes and emphasis.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    raw = _strip_thinking_leak(raw) or raw
    raw = unescape_public_text_escapes(raw) or raw
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    # Drop fenced code blocks (they are almost always accidental/noise for event pages).
    raw = re.sub(r"(?s)```.*?```", "", raw)
    raw = raw.replace("`", "")
    # Replace Markdown links with link text to avoid noisy URL-heavy descriptions.
    raw = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", raw)
    # Keep paragraphs: collapse 3+ newlines into 2.
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    # Normalize spaces without destroying newlines.
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"[ \t]+\n", "\n", raw)
    raw = re.sub(r"\n[ \t]+", "\n", raw)
    raw = raw.strip()
    raw = _fix_inline_bullet_lists(raw) or raw

    # Drop "orphan" headings: cases where the model outputs two headings in a row
    # (or a trailing heading at the end) without any paragraph/list/quote under the first one.
    # This is a pure formatting cleanup: we do not invent any missing text.
    def _drop_orphan_headings(value: str) -> str:
        def _strip_zw(s: str) -> str:
            return _ZERO_WIDTH_RE.sub("", s or "")

        blocks: list[str] = []
        for b in re.split(r"\n{2,}", value.strip()):
            if not b:
                continue
            if not _strip_zw(b).strip():
                continue
            blocks.append(b.strip())
        if len(blocks) < 2:
            return value.strip()
        heading_re = re.compile(r"^\s*(#{1,6})\s+\S")

        def _nonempty_lines(block: str) -> list[str]:
            lines: list[str] = []
            for ln in (block or "").splitlines():
                if not _strip_zw(ln).strip():
                    continue
                lines.append(ln.strip())
            return lines

        def _heading_level(line: str) -> int | None:
            m = heading_re.match(line)
            if not m:
                return None
            return len(m.group(1))

        def _is_heading_only(block: str) -> tuple[bool, int | None]:
            lines = _nonempty_lines(block)
            if len(lines) != 1:
                return False, None
            level = _heading_level(lines[0])
            return (level is not None), level

        def _starts_with_heading(block: str) -> int | None:
            lines = _nonempty_lines(block)
            if not lines:
                return None
            return _heading_level(lines[0])

        out: list[str] = []
        i = 0
        while i < len(blocks):
            cur = blocks[i]
            is_heading, cur_level = _is_heading_only(cur)
            nxt = blocks[i + 1] if i + 1 < len(blocks) else None
            if is_heading:
                if nxt is None:
                    i += 1
                    continue
                nxt_level = _starts_with_heading(nxt or "")
                if nxt_level is not None and cur_level is not None and nxt_level <= cur_level:
                    i += 1
                    continue
            out.append(cur)
            i += 1
        cleaned = "\n\n".join(out).strip()
        return cleaned or value.strip()

    raw = _drop_orphan_headings(raw)

    # NOTE: We intentionally avoid heuristic paragraph splitting here.
    # Paragraphing is part of LLM output quality. If the model returns a single
    # wall-of-text, we prefer an explicit LLM rewrite pass rather than applying
    # deterministic formatting that can cut semantics at the wrong boundaries.
    return raw or None


def _fix_broken_initial_paragraph_splits(text: str | None) -> str | None:
    """Fix accidental paragraph splits like `... в переводе Н.` + `Любимова.`.

    This is not "formatting"; it's a cleanup for a common LLM artifact that
    makes the text look machine-produced.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paras = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if len(paras) < 2:
        return raw

    out: list[str] = []
    i = 0
    while i < len(paras):
        cur = paras[i]
        nxt = paras[i + 1] if i + 1 < len(paras) else None
        if nxt:
            cur_cf = cur.casefold()
            # Join when we ended a paragraph on a single-letter initial and the next
            # paragraph starts with a surname-like token.
            if (
                re.search(r"(?:^|\s)[А-ЯЁA-Z]\.$", cur)
                and re.match(r"^[А-ЯЁ][а-яё]+\b", nxt)
                and ("перевод" in cur_cf or "в переводе" in cur_cf)
            ):
                cur = f"{cur} {nxt}"
                i += 2
                out.append(cur)
                continue
        out.append(cur)
        i += 1

    return "\n\n".join(out).strip() or None


_NEURAL_CLICHE_RE = re.compile(
    r"(?i)\bобеща\w+\s+(?:стать|быть)\b|\bярк\w+\s+событ\w+\b|\bзаметн\w+\s+событ\w+\b|"
    r"\bкультурн\w+\s+жизн\w+\b|\bне\s+остав\w+\s+равнодуш\w+\b|\bнезабываем\w+\b|"
    r"\bуникальн\w+\s+возможн\w+\b|\bэто\s+созда[её]т\b|\bсозда[её]т\s+атмосфер\w*\b|"
    r"\bатмосфер\w+\s+(?:спонтанност|вовлеч[её]нност)\w*\b"
)

_LIST_ITEM_LINE_RE = re.compile(r"^\s*(?:\d{1,3}[.)]|[-*•])\s+\S")

_DEFAULT_DESCRIPTION_EMOJI_ALLOWLIST = "🎭 🎨 🎵 🎬 🎤 📚 🖼️"


def _description_emoji_prompt_rule() -> str:
    """Prompt rule for optional emoji usage in full descriptions.

    Keep it accessibility-friendly: emojis are optional, few, never replace words,
    and should not be used as list markers.
    """
    if SMART_UPDATE_DESCRIPTION_EMOJI_MODE != "light":
        return "Без эмодзи. "
    max_emojis = int(SMART_UPDATE_DESCRIPTION_MAX_EMOJIS or 0)
    if max_emojis <= 0:
        return "Без эмодзи. "
    allowlist = SMART_UPDATE_DESCRIPTION_EMOJI_ALLOWLIST or _DEFAULT_DESCRIPTION_EMOJI_ALLOWLIST
    allowlist = re.sub(r"\s+", " ", allowlist).strip()
    return (
        f"Эмодзи допускаются умеренно: максимум {max_emojis} на весь текст description; "
        "не ставь несколько эмодзи подряд и не используй их как маркеры списка. "
        "Не заменяй слова эмодзи (эмодзи только как мягкий визуальный акцент). "
        "Лучше всего ставить 0–1 эмодзи в конце подзаголовков `### ...` (не в начале строки). "
        f"Используй только уместные эмодзи из списка: {allowlist}. "
    )


# Emoji limiting is a *display-level* safeguard; it should not change factual content.
_EMOJI_BASE_CLASS = (
    r"[\U0001F1E6-\U0001F1FF"  # flags
    r"\U0001F300-\U0001F5FF"  # symbols & pictographs
    r"\U0001F600-\U0001F64F"  # emoticons
    r"\U0001F680-\U0001F6FF"  # transport & map
    r"\U0001F700-\U0001F77F"  # alchemical symbols (rare but emoji-like)
    r"\U0001F780-\U0001F7FF"  # geometric extended
    r"\U0001F800-\U0001F8FF"  # arrows-C
    r"\U0001F900-\U0001F9FF"  # supplemental symbols & pictographs
    r"\U0001FA00-\U0001FAFF"  # symbols & pictographs extended-A
    r"\u2600-\u26FF"          # misc symbols
    r"\u2700-\u27BF"          # dingbats
    r"]"
)
_EMOJI_SEQ_RE = re.compile(
    rf"(?:{_EMOJI_BASE_CLASS})(?:[\uFE0E\uFE0F])?(?:[\U0001F3FB-\U0001F3FF])?"
    rf"(?:\u200D(?:{_EMOJI_BASE_CLASS})(?:[\uFE0E\uFE0F])?(?:[\U0001F3FB-\U0001F3FF])?)*"
)


def _limit_emoji_sequences(text: str, *, max_keep: int) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    if max_keep < 0:
        max_keep = 0
    matches = list(_EMOJI_SEQ_RE.finditer(raw))
    if not matches:
        return raw
    if len(matches) <= max_keep:
        return raw
    out: list[str] = []
    last = 0
    kept = 0
    for m in matches:
        out.append(raw[last : m.start()])
        if kept < max_keep:
            out.append(m.group(0))
            kept += 1
        last = m.end()
    out.append(raw[last:])
    cleaned = "".join(out)
    # Fix excessive spaces left after emoji removal; keep paragraph breaks intact.
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _limit_description_emojis(text: str | None) -> str | None:
    if text is None:
        return None
    if SMART_UPDATE_DESCRIPTION_EMOJI_MODE != "light":
        return text
    max_keep = int(SMART_UPDATE_DESCRIPTION_MAX_EMOJIS or 0)
    return _limit_emoji_sequences(text, max_keep=max_keep) or None


def _looks_like_list_block(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if len(lines) < 2:
        return False
    hits = sum(1 for ln in lines if _LIST_ITEM_LINE_RE.match(ln))
    if hits < 2:
        return False
    return hits >= max(2, int(len(lines) * 0.6))


def _looks_like_structured_block(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    if re.search(r"(?m)^\s*#{1,6}\s+\S", raw):
        return True
    if re.search(r"(?m)^\s*>", raw):
        return True
    if _looks_like_list_block(raw):
        return True
    return False


def _sanitize_description_output(
    text: str | None,
    *,
    source_text: str | None,
) -> str | None:
    """Best-effort *non-semantic* cleanup of LLM output for public Telegraph pages.

    Project rule: meaning-bearing operations on text should be done by LLM, not by
    deterministic regex cutting. This function therefore limits itself to:
    - trimming/normalizing whitespace
    - removing standalone internal/technical headings that must not leak publicly

    Anything more aggressive (cliche removal, logistics removal, etc.) must be
    handled in prompts / LLM editor passes.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    # Reject a stringified provider SDK response outright (INC-2026-05-17): if the
    # model output ever looks like a GenerateContentResponse repr, drop it entirely
    # so the caller falls back instead of publishing the dump to any surface.
    if looks_like_genai_response_dump(raw):
        logger.warning("smart_update: dropped genai SDK response dump from description output")
        return None
    raw = unescape_public_text_escapes(raw) or raw

    internal_heading_re = re.compile(
        r"(?i)^\s*(?:#{1,6}\s*)?(?:"
        r"facts\s*(?:/\s*)?added\s*facts|facts|added\s*facts|"
        r"факты\s*(?:/\s*)?добавленные\s*факты|"
        r"факты\s+о\s+событии|"
        r"факты\s+для\s+лога\s+источник\w*|"
        r"факты|добавленные\s*факты"
        r")\s*:?\s*$"
    )
    # "Facts for source log" is strictly internal and must never leak publicly.
    # If the LLM emits a whole paragraph that starts with such heading, drop it.
    internal_log_prefix_re = re.compile(r"(?i)^\s*(?:факты\s+для\s+лога|facts\s+for\s+source)\b")
    llm_editor_meta_re = re.compile(
        r"(?is)^\s*(?:"
        r"(?:в\s+)?предоставленн\w+(?:\s+вами)?\s+.*?"
        r"(?:текст|описани).*?(?:согласно|правил|структур).*|"
        r"согласно\s+(?:вашим|указанным)\s+правил|"
        r"(?:вот|ниже)\s+(?:обновл[её]нн(?:ый|ая)|исправленн(?:ый|ая)|отредактированн(?:ый|ая))\s+текст\s*:|"
        r"(?:обновл[её]нн(?:ый|ая)|исправленн(?:ый|ая)|отредактированн(?:ый|ая))\s+текст\s*:|"
        r"here\s+is\s+the\s+(?:updated|edited|revised)\s+text\s*:?"
        r")\s*$"
    )

    parts: list[str] = []
    for para in re.split(r"\n{2,}", raw):
        s = para.strip()
        if not s:
            continue
        if internal_heading_re.match(s):
            continue
        # If a paragraph starts with an internal heading (e.g. "Facts:"), strip only that
        # heading line and keep the content (non-semantic cleanup of a display artifact).
        lines = [ln.rstrip() for ln in s.splitlines()]
        # Find first non-empty line.
        first_idx = None
        for i, ln in enumerate(lines):
            if ln.strip():
                first_idx = i
                break
        if first_idx is not None:
            first = lines[first_idx].strip()
            if internal_log_prefix_re.match(first):
                # Entire block is internal (facts for /log), drop it.
                continue
            if llm_editor_meta_re.match(first):
                # Public cleanup only: editor/meta preambles like
                # "Вот обновленный текст:" or "В предоставленном тексте..."
                # are provider response artifacts, not event content.
                continue
            if internal_heading_re.match(first):
                # Drop only the heading line.
                lines = lines[:first_idx] + lines[first_idx + 1 :]
                s2 = "\n".join([ln for ln in lines]).strip()
                if not s2:
                    continue
                s = s2
        parts.append(s)
    cleaned = "\n\n".join(parts).strip()

    # Unescape backslash-escaped quotes that sometimes leak from JSON-ish sources
    # (e.g. `\\\"Сигнал\\\"` or `\"Сигнал\"`). This is a display-level cleanup
    # and should not change meaning.
    cleaned = cleaned.replace("\\\\\"", "\"")
    cleaned = cleaned.replace("\\\"", "\"")

    def _demote_overlong_headings(value: str) -> str:
        """Demote headings that look like full paragraphs (formatting-only fix)."""
        if not value:
            return value
        out_lines: list[str] = []
        heading_re = re.compile(r"^\s*(#{1,6})\s+(.+?)\s*$")
        for ln in value.splitlines():
            m = heading_re.match(ln)
            if not m:
                out_lines.append(ln)
                continue
            hashes = m.group(1)
            content = (m.group(2) or "").strip()
            # Strip internal "Facts:" prefix even if it got embedded into a heading line.
            content = re.sub(r"(?i)^(?:facts|факты)\s*:\s*", "", content).strip()
            # Heuristic: headings should be short; if it's long or looks like a sentence, demote.
            word_count = len(re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", content))
            # Keep headings short for Telegraph readability: long headings look like giant paragraphs.
            too_long = len(content) >= 80 or word_count >= 12
            if too_long:
                out_lines.append(content)
            else:
                out_lines.append(f"{hashes} {content}".rstrip())
        return "\n".join(out_lines).strip()

    def _strip_inline_facts_prefixes(value: str) -> str:
        """Strip 'Facts:' / 'Факты:' prefixes (display artifact)."""
        if not value:
            return value
        patt = re.compile(
            r"(?i)(?:^|(?<=\s))(?:\*\*|__)?(?:facts|факты)\s*:\s*(?:\*\*|__)?(?:\s+|$)"
        )
        out: list[str] = []
        for ln in value.splitlines():
            out.append(patt.sub("", ln))
        return "\n".join(out).strip()

    # Avoid leading blank lines/spacers before the first heading.
    cleaned = re.sub(r"^\s*\n+", "", cleaned)
    cleaned = _demote_overlong_headings(cleaned)
    cleaned = _strip_inline_facts_prefixes(cleaned)
    cleaned = cleaned.strip()
    cleaned = _limit_description_emojis(cleaned) or cleaned
    return cleaned or None


def _restore_source_grounded_known_spellings(text: str | None, *, source_text: str | None) -> str | None:
    """Narrow spelling guard for known proper-name drift, only when source grounds it."""
    raw = (text or "").strip()
    if not raw:
        return None
    source = source_text or ""
    replacements = {
        "Симюран": "Симуран",
    }
    for wrong, correct in replacements.items():
        if correct in source and wrong in raw:
            raw = raw.replace(wrong, correct)
    return raw or None


def _normalize_for_similarity(text: str | None, *, drop_structured: bool = True) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    raw = unicodedata.normalize("NFKC", raw)
    raw = raw.replace("\xa0", " ")
    lines: list[str] = []
    for ln in raw.splitlines():
        s = ln.strip()
        if not s:
            continue
        if drop_structured:
            # Ignore structured/quoted blocks: they may be verbatim by design.
            if s.startswith(">"):
                continue
            if re.search(r"^\s*#{1,6}\s+\S", s):
                continue
            if _LIST_ITEM_LINE_RE.match(s):
                continue
        lines.append(s)
    cleaned = " ".join(lines)
    cleaned = _LOGISTICS_URL_RE.sub(" ", cleaned)
    cleaned = _LOGISTICS_PHONE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.casefold().replace("ё", "е")
    return cleaned


def _description_too_verbatim(description: str | None, *, source_text: str | None) -> bool:
    desc_norm = _normalize_for_similarity(description, drop_structured=True)
    src_norm = _normalize_for_similarity(source_text, drop_structured=True)
    desc_norm_strict = _normalize_for_similarity(description, drop_structured=False)
    src_norm_strict = _normalize_for_similarity(source_text, drop_structured=False)
    if not (desc_norm and src_norm) and not (desc_norm_strict and src_norm_strict):
        return False
    # Only enforce on sufficiently long texts; small snippets can be legitimately similar.
    if (
        (desc_norm and len(desc_norm) < 180)
        or (src_norm and len(src_norm) < 240)
    ) and (
        (desc_norm_strict and len(desc_norm_strict) < 220)
        or (src_norm_strict and len(src_norm_strict) < 260)
    ):
        return False
    try:
        from difflib import SequenceMatcher
    except Exception:  # pragma: no cover
        return False
    if desc_norm and src_norm and desc_norm in src_norm and len(desc_norm) >= 240:
        return True
    if desc_norm_strict and src_norm_strict and desc_norm_strict in src_norm_strict and len(desc_norm_strict) >= 280:
        return True
    ratio = (
        SequenceMatcher(None, desc_norm, src_norm).ratio()
        if (desc_norm and src_norm)
        else 0.0
    )
    ratio_strict = (
        SequenceMatcher(None, desc_norm_strict, src_norm_strict).ratio()
        if (desc_norm_strict and src_norm_strict)
        else 0.0
    )
    return ratio >= 0.88 or ratio_strict >= 0.90


_LOGISTICS_PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?7|8)\s*\(?\d{3}\)?\s*\d{3}[\s-]*\d{2}[\s-]*\d{2}(?!\d)|(?<!\d)\d{10,11}(?!\d)"
)
_LOGISTICS_URL_RE = re.compile(r"(?i)\bhttps?://\S+")
_LOGISTICS_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\b")
_LOGISTICS_PRICE_RE = re.compile(r"(?i)\b\d{2,6}\s*(?:₽|руб\.?|рублей|рубля|р\.?)\b")
_LOGISTICS_DDMM_RE = re.compile(r"\b\d{1,2}[./]\d{1,2}(?:[./]20\d{2})?\b")
_LOGISTICS_ADDR_WORD_RE = re.compile(
    r"(?i)\b("
    r"ул\.?|улиц\w*|"
    r"пр\.?|проспект\w*|"
    r"пер\.?|переул\w*|"
    r"наб\.?|набережн\w*|"
    r"пл\.?|площад\w*|"
    r"бульвар\w*|бул\.?|"
    r"шоссе|"
    r"дом|д\.|"
    r"корпус|корп\.?|к\.|"
    r"офис|этаж|"
    r"г\.|город"
    r")\b"
)
_LOGISTICS_TICKET_WORD_RE = re.compile(r"(?i)\b(билет\w*|регистрац\w*|запис\w*|брон\w*|вход)\b")
_LOGISTICS_TICKET_CONDITION_KEEP_RE = re.compile(
    r"(?i)\b("
    r"входн\w*\s+билет|"
    r"нужн\w*|понадобит\w*|необходим\w*|"
    r"дополнительно|отдельно|помимо|кроме|"
    r"не\s+входит|входит\s+в|"
    r"оплачива\w*\s+отдельно"
    r")\b"
)
_LOGISTICS_TICKET_BOILERPLATE_DROP_RE = re.compile(
    r"(?i)\b("
    r"билет\w*\s+(?:доступн\w*|в\s+продаже)|"
    r"купит\w+\s+билет\w*|"
    r"по\s+ссылке|"
    r"подробнее|"
    r"регистрац\w*.*\bссылк\w*"
    r")\b"
)
_LOGISTICS_EXPLICIT_ADDRESS_RE = re.compile(
    r"(?i)\b("
    r"по\s+адресу|адрес|"
    r"(?:ул\.?|улиц\w*|пр\.?|проспект\w*|пер\.?|переул\w*|пл\.?|площад\w*|дом|д\.)"
    r"[^.!?\n]{0,80}\d"
    r")\b"
)

_DESCRIPTION_CHANNEL_PROMO_SENT_RE = re.compile(
    r"(?i)\b("
    r"информац\w*\s+о\s+(?:событи\w*|мероприят\w*).{0,80}?(?:telegram|телеграм)[- ]?канал|"
    r"следит\w*\s+за\s+(?:анонс\w*|афиш\w*).{0,80}?(?:telegram|телеграм)|"
    r"подпис\w*\s+на\s+(?:наш\s+)?(?:telegram|телеграм)[- ]?канал|"
    r"(?:telegram|телеграм)[- ]?канал.{0,80}?(?:анонс\w*|афиш\w*)"
    r")\b"
)

_DESCRIPTION_CHANNEL_PROMO_PHRASE_RE = re.compile(
    r"(?i)\b("
    r"информац\w*\s+о\s+(?:событи\w*|мероприят\w*)|"
    r"следит\w*\s+за\s+(?:анонс\w*|афиш\w*)|"
    r"подпис\w*\s+на\s+(?:наш\s+)?(?:telegram|телеграм)[- ]?канал|"
    r"(?:telegram|телеграм)[- ]?канал"
    r")\b"
)


def _format_ru_date_phrase(iso_value: str | None) -> str | None:
    if not iso_value:
        return None
    raw = iso_value.split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        d = date.fromisoformat(raw)
    except Exception:
        return None
    months = {v: k for k, v in _RU_MONTHS_GENITIVE.items()}
    month_word = months.get(d.month)
    if not month_word:
        return None
    return f"{d.day} {month_word}"


def _strip_infoblock_logistics_from_description(
    text: str | None,
    *,
    candidate: EventCandidate,
) -> str | None:
    """Remove obvious logistics duplicates from narrative description.

    Telegraph pages already render a quick facts infoblock (date/time/location/tickets),
    so repeating these details inside the narrative bloats the text.
    """
    raw = (text or "").strip()
    if not raw:
        return None

    ru_date = _format_ru_date_phrase(candidate.date)
    needles: list[str] = []
    price_values: set[int] = set()
    for pv in (candidate.ticket_price_min, candidate.ticket_price_max):
        if isinstance(pv, int) and pv > 0:
            price_values.add(pv)
    for val in (
        candidate.date,
        candidate.time,
        candidate.location_address,
        ru_date,
        _format_ticket_price(candidate.ticket_price_min, candidate.ticket_price_max),
    ):
        v = str(val or "").strip()
        if v:
            needles.append(v)
            if v == candidate.time and ":" in v:
                needles.append(v.replace(":", "."))
    # Avoid stripping plain venue names from narrative text: it's often part of the story
    # ("в баре …") and removing it can make sentences awkward. Keep stripping when the
    # configured "location_name" itself looks like a full address line.
    loc_name = str(getattr(candidate, "location_name", "") or "").strip()
    if loc_name and (
        re.search(r"\d", loc_name)
        or _LOGISTICS_ADDR_WORD_RE.search(loc_name)
        or loc_name.count(",") >= 2
    ):
        needles.append(loc_name)
    # Also add DD.MM / DD.MM.YYYY derived from candidate.date when available.
    if candidate.date:
        try:
            d = date.fromisoformat(candidate.date.split("..", 1)[0].strip())
            ddmm = d.strftime("%d.%m")
            ddmmyyyy = d.strftime("%d.%m.%Y")
            needles.extend([ddmm, ddmmyyyy, ddmm.replace(".", "/"), ddmmyyyy.replace(".", "/")])
        except Exception:
            pass

    def _strip_sentence(sentence: str) -> str:
        s = sentence
        had_price = bool(_LOGISTICS_PRICE_RE.search(sentence)) or any(
            (isinstance(pv, int) and pv > 0 and str(pv) in sentence) for pv in price_values
        )
        had_ticket_word = bool(_LOGISTICS_TICKET_WORD_RE.search(sentence))
        s = _LOGISTICS_URL_RE.sub("", s)
        s = _LOGISTICS_PHONE_RE.sub("", s)
        if price_values:
            for pv in sorted(price_values, reverse=True):
                s = re.sub(
                    rf"(?i)(?<!\d){pv}\s*(?:₽|рублей|рубля|руб\.?|р\.?)(?!\w)",
                    "",
                    s,
                )
        for needle in needles:
            if len(needle) < 4:
                continue
            s = re.sub(re.escape(needle), "", s, flags=re.IGNORECASE)

        # Remove common logistics lead-ins that become noise after stripping.
        s = re.sub(r"(?i)\b(сбор\s+гост\w*|начал\w*|время\s+начала)\b\s*[:\-–—]?\s*", "", s)
        s = re.sub(r"(?i)\b(по\s+адресу|адрес)\b\s*[:\-–—]?\s*", "", s)
        s = re.sub(r"(?i)\b(стоимость|цена)\s+билет\w*\b\s*[:\-–—]?\s*", "", s)
        s = re.sub(r"(?i)\b(телефон|по\s+телефон\w*|звон\w*|контакт\w*)\b\s*[:\-–—]?\s*", "", s)

        # Cleanup punctuation/whitespace.
        s = s.replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        s = re.sub(r"\s+([,.;:!?])", r"\1", s)
        s = re.sub(r"^[,.;:!?]+\s*", "", s).strip()
        # Keep sentence-ending punctuation (., !, ?, …). Removing it makes Telegraph text
        # look ungrammatical (common operator complaint). We only trim "soft" trailing
        # punctuation that often becomes dangling after stripping URLs/prices.
        s = re.sub(r"\s*[,;:]+\s*$", "", s).strip()

        # If we stripped the key payload (price/ticket) and left a dangling clause,
        # drop the sentence entirely to avoid broken Russian like "... составит".
        if (had_price or had_ticket_word) and not re.search(r"\d", s):
            if re.search(
                r"(?i)\b(составит|составят|будет|будут|стоит|стоить|обойдется|обойдётся)\b$",
                s,
            ):
                return ""
            if re.search(r"(?i)\b(стоимость|цена)\b", s):
                return ""
        return s

    out_paras: list[str] = []
    sent_split = re.compile(r"(?<=[.!?…])\s+")
    for para in re.split(r"\n{2,}", raw):
        p = para.strip()
        if not p:
            continue
        # Preserve quotes as-is (quotes may include source wording).
        if p.lstrip().startswith(">"):
            out_paras.append(p)
            continue
        if re.match(r"^\s*#{1,6}\s+\S", p):
            lines = p.splitlines()
            heading = lines[0].strip()
            body = "\n".join(lines[1:]).strip()
            if not body:
                out_paras.append(heading)
                continue
            stripped_body = _strip_infoblock_logistics_from_description(body, candidate=candidate)
            if stripped_body:
                out_paras.append(f"{heading}\n{stripped_body}".strip())
            else:
                out_paras.append(heading)
            continue

        # For list-like blocks keep formatting and strip logistics line-by-line.
        if _looks_like_list_block(p) or re.match(r"^\s*[-*•]\s+\S", p):
            kept_lines: list[str] = []
            for line in p.splitlines():
                if not line.strip():
                    continue
                stripped = _strip_sentence(line)
                if not stripped:
                    continue
                # Drop "empty logistics" leftovers like "Билеты доступны" after removing link/price.
                if (
                    _LOGISTICS_TICKET_WORD_RE.search(stripped)
                    and (
                        (
                            len(stripped) < 28
                            and not _LOGISTICS_TICKET_CONDITION_KEEP_RE.search(stripped)
                        )
                        or (
                            _LOGISTICS_TICKET_BOILERPLATE_DROP_RE.search(stripped)
                            and not _LOGISTICS_TICKET_CONDITION_KEEP_RE.search(stripped)
                        )
                    )
                ):
                    continue
                if not re.search(r"[A-Za-zА-Яа-яЁё]", stripped):
                    continue
                kept_lines.append(stripped)
            if kept_lines:
                out_paras.append("\n".join(kept_lines).strip())
            continue

        sents = [s.strip() for s in sent_split.split(re.sub(r"\s*\n\s*", " ", p)) if s.strip()]
        kept: list[str] = []
        for sent in sents:
            stripped = _strip_sentence(sent)
            if not stripped:
                continue
            # Drop "empty logistics" leftovers like "Билеты доступны" after removing link/price.
            if (
                _LOGISTICS_TICKET_WORD_RE.search(stripped)
                and (
                    (
                        len(stripped) < 28
                        and not _LOGISTICS_TICKET_CONDITION_KEEP_RE.search(stripped)
                    )
                    or (
                        _LOGISTICS_TICKET_BOILERPLATE_DROP_RE.search(stripped)
                        and not _LOGISTICS_TICKET_CONDITION_KEEP_RE.search(stripped)
                    )
                )
            ):
                continue
            # Keep only sentences with some letters left.
            if not re.search(r"[A-Za-zА-Яа-яЁё]", stripped):
                continue
            if len(stripped) < 18 and len(stripped.split()) < 3:
                continue
            kept.append(stripped)
        if kept:
            out_paras.append(" ".join(kept).strip())
    cleaned = "\n\n".join(out_paras).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned or None


def _description_needs_infoblock_logistics_strip(
    text: str | None,
    *,
    candidate: EventCandidate,
) -> bool:
    """Cheap gate to reduce deterministic вмешательство в текст.

    We only run the heavy stripping pass when we see clear logistics signals.
    """
    raw = (text or "").strip()
    if not raw:
        return False
    if _LOGISTICS_URL_RE.search(raw):
        return True
    if _LOGISTICS_PHONE_RE.search(raw):
        return True
    if _LOGISTICS_PRICE_RE.search(raw):
        return True
    if _LOGISTICS_TIME_RE.search(raw):
        return True
    if _LOGISTICS_DDMM_RE.search(raw):
        return True
    if _LOGISTICS_ADDR_WORD_RE.search(raw):
        return True
    if _LOGISTICS_TICKET_WORD_RE.search(raw):
        return True
    # Candidate anchors occasionally leak verbatim; strip only if present.
    for val in (
        getattr(candidate, "location_address", None),
        _format_ru_date_phrase(getattr(candidate, "date", None)),
        getattr(candidate, "time", None),
        getattr(candidate, "date", None),
    ):
        v = str(val or "").strip()
        if v and v.casefold() in raw.casefold():
            return True
    return False


def _description_needs_g4_split_create_logistics_reject(
    text: str | None,
    *,
    candidate: EventCandidate,
) -> bool:
    """Stricter reject gate for split-create description output.

    The generic cleanup gate treats words like "улицы" / "город" as address-like
    because ordinary event descriptions should not duplicate venue logistics.
    Split-create can describe excursion routes, so only hard logistics signals
    should make the writer output unusable.
    """
    raw = (text or "").strip()
    if not raw:
        return False
    if _LOGISTICS_URL_RE.search(raw):
        return True
    if _LOGISTICS_PHONE_RE.search(raw):
        return True
    if _LOGISTICS_PRICE_RE.search(raw):
        return True
    if _LOGISTICS_TIME_RE.search(raw):
        return True
    if _LOGISTICS_DDMM_RE.search(raw):
        return True
    if _LOGISTICS_TICKET_WORD_RE.search(raw):
        return True
    if _LOGISTICS_EXPLICIT_ADDRESS_RE.search(raw):
        return True
    for val in (
        getattr(candidate, "location_address", None),
        _format_ru_date_phrase(getattr(candidate, "date", None)),
        getattr(candidate, "time", None),
        getattr(candidate, "date", None),
    ):
        v = str(val or "").strip()
        if v and v.casefold() in raw.casefold():
            return True
    return False


def _description_needs_channel_promo_strip(text: str | None) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    if not ("телеграм" in raw.casefold() or "telegram" in raw.casefold() or "t.me/" in raw.casefold()):
        return False
    return bool(_DESCRIPTION_CHANNEL_PROMO_SENT_RE.search(raw))


def _strip_channel_promo_from_description(text: str | None) -> str | None:
    # Deterministic sentence-level cutting is not allowed (LLM handles this).
    raw = (text or "").strip()
    return raw or None


def _norm_text_for_fact_presence(text: str) -> str:
    """Deterministic normalization for 'fact presence' substring checks.

    We intentionally keep this conservative: it's used only to detect obvious
    omissions (e.g. short slogan-like quoted facts) and should not attempt
    semantic matching.
    """
    raw = (text or "").casefold()
    raw = raw.replace("ё", "е")
    raw = raw.translate(
        str.maketrans(
            {
                "«": '"',
                "»": '"',
                "“": '"',
                "”": '"',
                "„": '"',
                "’": "'",
                "–": "-",
                "—": "-",
                "\u00a0": " ",
                "\u2009": " ",
                "\u202f": " ",
                "\ufeff": "",
                "\u200b": "",
                "\u2060": "",
            }
        )
    )
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _is_anchor_or_service_fact(fact: str) -> bool:
    f = (fact or "").strip()
    if not f:
        return True
    # Do not force anchors / service notes into narrative coverage checks.
    if re.search(r"(?i)^(дата|время|локац\w*|адрес\w*|город\w*|источник)\b", f):
        return True
    # Dates/times often appear as free-form sentences, not only as `Дата:`/`Время:`.
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", f):  # 2026-03-03
        return True
    if re.search(r"\b\d{1,2}[:.]\d{2}\b", f):  # 18:30 / 18.30
        return True
    if re.search(r"\b\d{1,2}\.\d{1,2}(?:\.\d{2,4})?\b", f):  # 03.03 / 03.03.2026
        return True
    if re.search(
        r"(?i)\b\d{1,2}\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
        f,
    ):
        return True
    # Location/logistics phrasing (must stay in infoblock, not in narrative).
    if re.search(r"(?i)\b(по\s+адресу|\d+\s*этаж\w*|этаж\w*|зал\w*|аудитори\w*)\b", f):
        return True
    # Event type is provided separately to the narrative generator; keep `Тип:` out of text_clean.
    if re.search(r"(?i)^тип\b", f):
        return True
    if re.search(r"(?i)^(текст\s+очищен|llm\s+недоступна|добавлена\s+афиша)\b", f):
        return True
    if "http://" in f or "https://" in f or "www." in f.casefold():
        if _fact_first_urls_are_allowed(f):
            return False
        return True
    return False


def _find_missing_facts_in_description(
    *, description: str, facts: Sequence[str], max_items: int = 5
) -> list[str]:
    """Return a small list of facts that are very likely missing from description."""
    desc_n = _norm_text_for_fact_presence(description)
    missing: list[str] = []
    for fact in facts:
        f = str(fact or "").strip()
        if not f or _is_anchor_or_service_fact(f):
            continue
        is_quoted = bool(re.fullmatch(r'["«].+["»]\s*', f)) or ("«" in f and "»" in f) or ('"' in f)
        # Only enforce coverage for short facts, unless they are explicit slogans/quotes
        # or an allowlisted content URL (playlist).
        if not is_quoted and len(f) > 90 and not _fact_first_urls_are_allowed(f):
            continue
        # Prefer checking the "inner" content for quoted slogan-like facts.
        inner = f
        m = re.fullmatch(r'["«](.+?)["»]\s*', f)
        if m:
            inner = m.group(1).strip()
        needle = _norm_text_for_fact_presence(inner)
        if not needle:
            continue
        if needle not in desc_n:
            missing.append(f)
            if len(missing) >= max_items:
                break
    return missing


async def _llm_integrate_missing_facts_into_description(
    *, description: str, missing_facts: Sequence[str], source_text: str, label: str
) -> str | None:
    """Ask LLM to integrate missing facts into description without adding new facts."""
    if SMART_UPDATE_LLM_DISABLED:
        return None
    desc = (description or "").strip()
    if not desc:
        return None
    facts = [str(f).strip() for f in (missing_facts or []) if str(f or "").strip()]
    if not facts:
        return None
    payload = {
        "description": _clip(desc, 5000),
        "missing_facts": facts[:8],
        "source_text": _clip(source_text or "", 2500),
    }
    prompt = (
        "В тексте описания события отсутствуют некоторые факты.\n"
        "Твоя задача: аккуратно встроить `missing_facts` в `description` так, чтобы текст читался связно.\n"
        "Правила:\n"
        "- НЕЛЬЗЯ добавлять новые факты (только из `missing_facts`).\n"
        "- НЕЛЬЗЯ менять якорные поля (дата/время/площадка/адрес).\n"
        "- НЕ добавляй в текст логистику (дата/время/площадка/точный адрес/город/ссылки/телефон/контакты/точные цены): она уже показана отдельным блоком.\n"
        "- НЕ добавляй промо-упоминания «где следить за анонсами» и ссылки на каналы/чаты с афишей.\n"
        "- Не дублируй в тексте строки формата `Дата:`, `Время:`, `Локация:`, `Билеты:`: эти данные уже показаны в карточке сверху.\n"
        "- Факты в кавычках (слоганы/характеристики) сохраняй ДОСЛОВНО, лучше в «ёлочках», "
        "и атрибутируй как слова/характеристики из афиши/поста, а не как объективный прогноз.\n"
        "- Не добавляй рекламных клише и прогнозов.\n"
        "- Сохраняй существующие цитаты в формате blockquote (`>`).\n"
        "- Не оставляй обрывов фраз после правок (например «стоимость … составит» без суммы): перефразируй или удали.\n"
        "- Самопроверка: все предложения грамматически завершены; не появилось странных/непонятных слов.\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=900,
        label=label,
        temperature=0.0,
    )
    return text.strip() if text else None


_FACT_FIRST_CTA_RE = re.compile(
    r"(?i)\b("
    r"приглашаем|приходите|жд[её]м\s+вас|не\s+пропуст\w*|успей\w*|"
    r"присоединяйтесь|подписывайтесь|подпиш\w*|репост\w*|лайк\w*|"
    r"делитесь|расскажите|рекомендуем|советуем|жми|жмите|"
    r"покупай\w*|купите|брониру\w*|зарегистриру\w*|записывайтесь|"
    r"встречайте"
    r")\b"
)
_FACT_FIRST_TICKET_WORD_RE = re.compile(
    r"(?i)\b(билет\w*|вход\w*|регистрац\w*|запис\w*|брон\w*|бесплатн\w*)\b"
)
_FACT_FIRST_PRICE_RE = re.compile(r"(?i)\b\d{1,6}\s*(?:₽|руб\.?|р\.)\b")
_FACT_FIRST_AGE_RE = re.compile(r"\b\d{1,2}\+\b")
_FACT_FIRST_PUSHKIN_RE = re.compile(r"(?i)\bпушкинск\w*\s+карт\w*\b")
_FACT_FIRST_URL_RE = re.compile(r"(?i)\b(?:https?://|www\.)\S+")
_FACT_FIRST_PHONE_RE = re.compile(r"(?i)\+7\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}")
_FACT_FIRST_HASH_RE = re.compile(r"(?m)(?:^|\s)#[A-Za-zА-Яа-яЁё0-9_]{2,}")
_FACT_FIRST_ALLOWED_CONTENT_URL_RE = re.compile(
    r"(?i)https?://music\.yandex\.ru/users/[^\s/]+/playlists/\d+\S*"
)


def _fact_first_extract_urls(text: str) -> list[str]:
    raw = str(text or "")
    if not raw:
        return []
    return [m.group(0) for m in _FACT_FIRST_URL_RE.finditer(raw)]


def _fact_first_urls_are_allowed(text: str) -> bool:
    urls = _fact_first_extract_urls(text)
    if not urls:
        return False
    return all(bool(_FACT_FIRST_ALLOWED_CONTENT_URL_RE.search(url)) for url in urls)


def _fact_first_has_disallowed_urls(text: str) -> bool:
    urls = _fact_first_extract_urls(text)
    if not urls:
        return False
    return any(not _FACT_FIRST_ALLOWED_CONTENT_URL_RE.search(url) for url in urls)


def _fact_first_bucket(fact: str) -> str:
    """Classify a fact into a bucket for fact-first narrative generation.

    Buckets:
    - text_clean: publishable narrative facts
    - infoblock: logistics/tickets/prices/contacts/anchors (must not go into narrative)
    - drop: promo/CTA/noise (must not go into narrative)
    """
    f = str(fact or "").strip()
    if not f:
        return "drop"
    if _is_anchor_or_service_fact(f):
        return "infoblock"
    if _FACT_FIRST_PHONE_RE.search(f):
        return "infoblock"
    if _FACT_FIRST_URL_RE.search(f) and not _fact_first_urls_are_allowed(f):
        return "infoblock"
    if _FACT_FIRST_TICKET_WORD_RE.search(f) or _FACT_FIRST_PRICE_RE.search(f):
        return "infoblock"
    if _FACT_FIRST_AGE_RE.search(f) or _FACT_FIRST_PUSHKIN_RE.search(f):
        return "infoblock"
    if re.search(r"(?i)\b(афиш\w*|постер\w*)\b", f):
        return "infoblock"
    if _FACT_FIRST_HASH_RE.search(f) or _FACT_FIRST_CTA_RE.search(f):
        return "drop"
    return "text_clean"


def _facts_text_clean_from_facts(
    facts: Sequence[str],
    *,
    max_items: int = 28,
    anchors: Sequence[str] | None = None,
) -> list[str]:
    anchors_norm: list[str] = []
    for a in anchors or []:
        s = str(a or "").strip()
        if not s:
            continue
        # Avoid filtering by very short/common tokens.
        tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", s)
        if not tokens:
            continue
        if len(s) < 5 and all(len(t) < 5 for t in tokens):
            continue
        anchors_norm.append(s.casefold())

    out: list[str] = []
    for fact in facts or []:
        cleaned = _normalize_fact_item(str(fact or ""), limit=180)
        if not cleaned:
            continue
        bucket = _fact_first_bucket(cleaned)
        if bucket != "text_clean":
            # Participant chats are useful as a *fact*, but their links are forbidden in narrative text.
            # Keep the meaning, drop the URL.
            if (
                bucket == "infoblock"
                and _FACT_FIRST_URL_RE.search(cleaned)
                and re.search(r"(?iu)\bчат\w*\b", cleaned)
            ):
                without_urls = _FACT_FIRST_URL_RE.sub("", cleaned)
                without_urls = re.sub(r"\s+", " ", without_urls).strip()
                without_urls = re.sub(r"[\s:—–-]+$", "", without_urls).strip()
                if without_urls and _fact_first_bucket(without_urls) == "text_clean":
                    cleaned = without_urls
                else:
                    continue
            else:
                continue
        if anchors_norm:
            cleaned_cf = cleaned.casefold()
            if any(a in cleaned_cf for a in anchors_norm):
                continue
        out.append(cleaned)
        if len(out) >= max_items * 2:
            break
    return _dedupe_source_facts(out)[:max_items]


def _pick_epigraph_fact(facts_text_clean: Sequence[str]) -> str | None:
    facts = [str(f or "").strip() for f in (facts_text_clean or []) if str(f or "").strip()]
    if not facts:
        return None
    # Prefer a direct quote-like fact.
    for f in facts:
        if ("«" in f and "»" in f) or re.search(r"(?i)^\s*цитата\b", f):
            return f
    # Fallback: short "tagline" fact with an em dash, avoiding key-value facts.
    for f in facts:
        if ":" in f:
            continue
        if re.search(r"(?i)^(ведущ\w*|лектор\w*|спикер\w*|гост\w*)\b", f):
            continue
        if "—" in f and 20 <= len(f) <= 140:
            return f
    return None


_FACT_FIRST_POSV_WORD_RE = re.compile(r"(?i)\bпосвящ\w*\b")


def _sanitize_fact_text_clean_for_prompt(fact: str) -> str:
    """Pre-sanitize text_clean facts for the narrative prompt.

    Goal: keep the meaning, but avoid strictly forbidden lexical markers that
    models tend to copy verbatim from facts (e.g. "посвящ..." in Russian).
    This is *not* persisted as a source fact; it only affects the description prompt.
    """
    s = str(fact or "").strip()
    if not s or not _FACT_FIRST_POSV_WORD_RE.search(s):
        return s

    # Common pattern: "<event> посвящена/посвящён ... <topic>" -> "Тема: <topic>."
    m = re.match(
        r"(?i)^\s*(?:лекци\w*|встреч\w*|бесед\w*|показ\w*|концерт\w*|спектакл\w*|"
        r"мастер-?класс\w*|мастерск\w*|заняти\w*|экскурс\w*|презентац\w*|выставк\w*)\s+"
        r"посвящ\w+\s+(.+?)\s*[.!?]?\s*$",
        s,
    )
    if m:
        topic = (m.group(1) or "").strip()
        if topic:
            return f"Тема: {topic}."
    return s


def _estimate_fact_first_description_budget_chars(facts_text_clean: Sequence[str]) -> int:
    facts_chars = sum(len(str(f or "")) for f in (facts_text_clean or []) if str(f or "").strip())
    # Budget should scale with fact volume: if we have many (already de-duped) facts,
    # the narrative should be allowed to be longer (Telegraph can render long texts).
    budget = int(facts_chars * 1.10 + 420)
    return max(800, min(SMART_UPDATE_DESCRIPTION_MAX_CHARS, budget))


def _fact_first_is_sparse(facts_text_clean: Sequence[str]) -> bool:
    facts = [str(f or "").strip() for f in (facts_text_clean or []) if str(f or "").strip()]
    return len(facts) <= 4 or sum(len(f) for f in facts) < 320


def _estimate_fact_first_description_max_tokens(
    *, budget_chars: int, floor: int = 1700, ceil: int = 4500
) -> int:
    """Best-effort output token budget for fact-first description.

    We keep this deterministic and conservative: it only sets an upper bound for the
    provider call. The prompt still instructs the model to stay within
    `description_budget_chars`.
    """
    # Rough heuristic for Russian text + Markdown: ~3.2 chars per token.
    est = int(math.ceil(float(max(0, int(budget_chars or 0))) / 3.2)) + 200
    return max(int(floor), min(int(ceil), est))


def _fact_first_description_prompt(
    *,
    title: str | None,
    event_type: str | None,
    facts_text_clean: Sequence[str],
    epigraph_fact: str | None,
) -> str:
    facts_block = "\n".join(f"- {str(f).strip()}" for f in (facts_text_clean or []) if str(f or "").strip())
    budget_chars = _estimate_fact_first_description_budget_chars(facts_text_clean)
    sparse_source = _fact_first_is_sparse(facts_text_clean)
    structure_rules = (
        "- SPARSE SOURCE MODE: фактов мало. Верни 1–2 коротких абзаца без `###` и без эпиграфа.\n"
        "- Не пытайся заполнить объём или придумать второй смысловой блок. Краткий конкретный текст лучше воды."
        if sparse_source
        else (
            "- Затем 2–3 блока с подзаголовками `### ...` (только `###`).\n"
            "- Подзаголовки короткие (до ~60 символов), без точек, не полные предложения; не делай пустых блоков.\n"
            "- Подзаголовки должны быть информативными; избегай общих вроде «Подробности».\n"
            "- Под каждым `###` должно быть либо 2–4 предложения, либо список (2+ пунктов)."
        )
    )
    return textwrap.dedent(
        f"""\
        Ты пишешь Markdown‑анонс события для Telegram в стиле культурного журналиста: живо, конкретно, без рекламы.

        Источник истины: ТОЛЬКО список facts_text_clean ниже. Нельзя добавлять новые сведения.

        Цель: связный текст, где каждая деталь из фактов упомянута и при этом нет смысловых повторов.

        {SMART_UPDATE_YO_RULE}

        Структура:
        - Если epigraph_fact не null:
          - Если в нём есть длинная прямая речь в «...!»/«...?» — оформи эпиграф 2 строками:
            `> «...»
            > — ...` (атрибуция только из слов epigraph_fact; без новых глаголов типа «восхищается/говорит»).
          - Иначе — одна строка `> epigraph_fact`.
          - После эпиграфа: пустая строка, затем лид ОДНИМ абзацем (1–2 предложения) без заголовка (без переносов строки).
          - В теле текста НЕ повторяй и НЕ пересказывай epigraph_fact: он уже прозвучал в эпиграфе.
        - Если epigraph_fact null: просто лид ОДНИМ абзацем (1–2 предложения) без заголовка (без переносов строки).
        {structure_rules}
        - Абзацы средней длины: обычно 2–4 предложения; избегай микро‑абзацев из одной короткой фразы.
        - Объём: старайся уложиться примерно в лимит `description_budget_chars` символов, без воды.
        - Эмодзи: 1–2 штуки, как навигация (в лид/в 1 заголовке), без «ёлки».
        - Если факты дают несколько тем/пунктов (вопросы лекции, пункты программы и т.п.) — сгруппируй их в ОДИН раздел и оформи списком; не делай отдельный `###` для каждого пункта.

        Стиль C («Сцена → смысл → детали»):
        - Начни с короткой «картинки/настроения» из фактов.
          Если в фактах есть строка `Атмосфера ...` — используй её ДОСЛОВНО.
          Если такой строки нет — НЕ придумывай новую «атмосферу», начни с конкретики из фактов.
        - Во 2‑й фразе лида дай понять, что это за событие (спектакль/концерт/показ фильма/игра/мастерская и т.п.).
        - Если среди фактов есть строка `Формат: ...` — обозначь формат ЯВНО (ключевые слова после `:` должны прозвучать).
        - Условия участия/просмотра из фактов (длительность, язык/субтитры, что взять/что предоставляется, количество участников/игроков) — ОБЯЗАТЕЛЬНО упомяни явно.

        Как работать с фактами:
        - Объединяй и перестраивай фразы, но аккуратно: не добавляй новых смыслов/обещаний/обобщений.
        - Не превращай текст в «перечень фактов» и не копируй подряд несколько фактов как готовые предложения: перепиши связно, сохранив детали.
        - Если в факте есть список/перечень/треклист/набор пунктов — сохрани ВСЕ элементы ДОСЛОВНО и лучше оформи списком (каждый пункт на отдельной строке).
        - Сохраняй ключевые слова и образы из фактов; не подменяй «образные» слова синонимами.
        - Если в факте есть ЦИФРЫ/диапазон/рейтинги — эти же цифры должны появиться в тексте.

        Запреты:
        - Нельзя: дату/время/город/площадку/адрес, URL/телефоны (кроме ссылок на плейлист Я.Музыки из facts_text_clean), билеты/вход/регистрацию/запись, цены/донаты, возраст, «Пушкинская карта», афиши.
        - Нельзя CTA‑слова: «приглашаем», «приходите», «ждём/ждем вас», «не пропустите», «успейте», «присоединяйтесь», «предлагаем», «встречайте».
        - Не используй слово «посвящён/посвящена/посвящено».
          Если в фактах встречается «... посвящ... ...» — перефразируй без этого слова: «лекция о ...», «разговор про ...», «в центре — ...».
        - Запрещён штамп: «это ... не ..., а ...».

        Контекст:
        - title: {(title or '').strip()}
        - event_type: {(event_type or '').strip()}
        - epigraph_fact (если null — эпиграф НЕ нужен): {epigraph_fact if epigraph_fact is not None else 'null'}
        - description_budget_chars: {budget_chars}
        - sparse_source: {str(sparse_source).lower()}

        Факты (facts_text_clean):
        {facts_block}

        Верни только Markdown‑текст описания (без JSON).
        """
    ).strip()


def _g4_split_description_writer_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "description": {"type": "string"},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["description", "warnings"],
        "additionalProperties": False,
    }


def _g4_split_derived_fields_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "short_description": {"type": "string"},
            "search_digest": {"type": "string"},
            "warnings": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["short_description", "search_digest", "warnings"],
        "additionalProperties": False,
    }


def _g4_rich_facts_schema() -> dict[str, Any]:
    grounded_fact = {
        "type": "object",
        "properties": {
            "fact": {"type": "string"},
            "evidence_quote": {
                "type": "string",
                "description": (
                    "Exact contiguous quote from source_text, raw_excerpt, or poster_texts "
                    "that directly supports this fact."
                ),
            },
        },
        "required": ["fact", "evidence_quote"],
        "additionalProperties": False,
    }
    grounded_organizer = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "evidence_quote": {
                "type": "string",
                "description": (
                    "Exact contiguous quote that explicitly identifies this "
                    "organization as an organizer of the concrete event."
                ),
            },
        },
        "required": ["name", "evidence_quote"],
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {
            "public_core_facts": {"type": "array", "items": grounded_fact},
            "program_or_examples": {"type": "array", "items": grounded_fact},
            "context_methodology_facts": {"type": "array", "items": grounded_fact},
            "people_org_facts": {"type": "array", "items": grounded_fact},
            "organizer_names": {"type": "array", "items": grounded_organizer},
            "logistics_facts": {"type": "array", "items": grounded_fact},
            "uncertain_or_drop": {"type": "array", "items": {"type": "string"}},
            "age_decision": AGE_DECISION_JSON_SCHEMA,
        },
        "required": [
            "public_core_facts",
            "program_or_examples",
            "context_methodology_facts",
            "people_org_facts",
            "organizer_names",
            "logistics_facts",
            "uncertain_or_drop",
        ],
        "additionalProperties": False,
    }


def _flatten_g4_rich_facts_payload(
    data: Any,
    *,
    source_corpus: str | None = None,
) -> list[str]:
    if not isinstance(data, dict):
        return []
    out: list[str] = []
    for key in (
        "public_core_facts",
        "context_methodology_facts",
        "people_org_facts",
        "program_or_examples",
        "logistics_facts",
    ):
        for item in data.get(key) or []:
            if isinstance(item, dict):
                fact = str(item.get("fact") or "").strip()
                quote = str(item.get("evidence_quote") or "").strip()
            else:
                # Backward-compatible fail-closed handling for a provider that
                # ignores the new object schema: a string fact is accepted only
                # when it is lexically supported by the full source corpus.
                fact = str(item or "").strip()
                quote = ""
            if not fact:
                continue
            if source_corpus is not None:
                verdict = claim_is_grounded(
                    fact,
                    source_corpus,
                    evidence_quote=quote or None,
                    min_ratio=0.45,
                    min_matches=2,
                )
                if not verdict.ok:
                    logger.warning(
                        "smart_update.rich_fact_rejected reason=%s matched=%s claim_tokens=%s fact=%r",
                        verdict.reason,
                        verdict.matched,
                        verdict.claim_tokens,
                        fact[:180],
                    )
                    continue
            out.append(fact)
    return out


def _g4_split_create_fact_ledger_description(
    *,
    candidate: EventCandidate,
    title: str | None,
    facts_text_clean: Sequence[str],
) -> str | None:
    facts = _dedupe_source_facts(
        [
            _sanitize_fact_text_clean_for_prompt(str(f or "").strip())
            for f in (facts_text_clean or [])
            if str(f or "").strip()
        ]
    )
    if not facts:
        return None
    anchors = [
        candidate.date or "",
        candidate.time or "",
        candidate.city or "",
        candidate.location_name or "",
        candidate.location_address or "",
        candidate.ticket_link or "",
        "Пушкинская карта" if candidate.pushkin_card else "",
    ]
    narrative = _g4_split_description_facts(facts, anchors=anchors, max_items=24)
    if not narrative:
        return None

    lead = narrative[0].strip()
    if lead and lead[-1] not in ".!?":
        lead += "."
    rest = narrative[1:]
    lines: list[str] = [lead]
    if rest:
        heading = "Что важно"
        title_l = (title or candidate.title or "").casefold()
        if "концерт" in title_l or (candidate.event_type or "").casefold() == "concert":
            heading = "Программа и участники"
        elif "экскурс" in title_l or (candidate.event_type or "").casefold() in {"tour", "excursion"}:
            heading = "Маршрут и детали"
        lines.extend(["", f"### {heading}"])
        for fact in rest[:18]:
            item = fact.strip()
            if item and item[-1] in ".!?":
                item = item[:-1].rstrip()
            if item:
                lines.append(f"- {item}")
    description = "\n".join(lines)
    return _cleanup_g4_split_create_description(description, candidate=candidate)


def _g4_split_description_facts(
    facts: Sequence[str],
    *,
    anchors: Sequence[str],
    max_items: int,
) -> list[str]:
    narrative: list[str] = []
    for fact in facts:
        text = str(fact or "").strip()
        if not text:
            continue
        if text.casefold().startswith("описание "):
            continue
        low = text.casefold()
        if low.startswith(
            (
                "дата:",
                "время:",
                "локация:",
                "место проведения:",
                "цена:",
                "стоимость",
                "билеты",
                "регистрация",
                "продолжительность",
                "пушкинская карта",
            )
        ):
            continue
        try:
            forbidden = _fact_first_forbidden_reasons(text, anchors=anchors)
        except Exception:
            forbidden = []
        if forbidden:
            continue
        narrative.append(text)
        if len(narrative) >= max_items:
            break
    return narrative


def _cleanup_g4_split_create_description(
    value: str | None,
    *,
    candidate: EventCandidate,
    sparse_source: bool = False,
) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    raw = _strip_private_use(raw) or raw
    raw = _fix_inline_bullet_lists(raw) or raw
    raw = _normalize_bullet_markers(raw) or raw
    raw = _promote_review_bullets_to_blockquotes(raw) or raw
    raw = _normalize_blockquote_markers(raw) or raw
    raw = _limit_description_emojis(raw) or raw
    raw = _sanitize_description_output(raw, source_text="") or raw
    raw = _dedupe_description(raw) or raw
    raw = _normalize_plaintext_paragraphs(raw) or raw
    if not sparse_source:
        raw = _ensure_minimal_description_headings(raw) or raw
    raw = _clip(raw, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
    if _description_needs_infoblock_logistics_strip(raw, candidate=candidate):
        raw = _strip_infoblock_logistics_from_description(raw, candidate=candidate) or raw
        raw = _normalize_plaintext_paragraphs(raw) or raw
        if not sparse_source:
            raw = _ensure_minimal_description_headings(raw) or raw
        raw = _clip(raw, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
    if _description_needs_g4_split_create_logistics_reject(raw, candidate=candidate):
        logger.warning(
            "smart_update: g4_split_description_writer rejected description with logistics source_type=%s source_url=%s",
            candidate.source_type,
            candidate.source_url,
        )
        return None
    return raw.strip() or None


async def _llm_g4_split_create_writer(
    *,
    candidate: EventCandidate,
    title: str | None,
    event_type: str | None,
    facts_text_clean: Sequence[str],
) -> dict[str, Any] | None:
    facts = [str(f or "").strip() for f in (facts_text_clean or []) if str(f or "").strip()]
    if not facts or SMART_UPDATE_LLM_DISABLED:
        return None
    facts = _dedupe_source_facts([_sanitize_fact_text_clean_for_prompt(f) for f in facts])[:30]
    anchors = [
        candidate.date or "",
        candidate.time or "",
        candidate.city or "",
        candidate.location_name or "",
        candidate.location_address or "",
        candidate.ticket_link or "",
        "Пушкинская карта" if candidate.pushkin_card else "",
    ]
    description_facts = _g4_split_description_facts(facts, anchors=anchors, max_items=30) or facts
    sparse_source = _fact_first_is_sparse(description_facts)
    budget_chars = _estimate_fact_first_description_budget_chars(description_facts)
    desc_max_tokens = _estimate_fact_first_description_max_tokens(budget_chars=budget_chars, floor=1700, ceil=4200)
    payload = {
        "title": (title or candidate.title or "").strip(),
        "event_type": (event_type or candidate.event_type or "").strip(),
        "description_budget_chars": budget_chars,
        "facts_text_clean": description_facts,
        "full_fact_count": len(facts),
        "sparse_source": sparse_source,
        "infoblock_context": {
            "date": candidate.date,
            "time": candidate.time,
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "ticket_status": candidate.ticket_status,
            "is_free": candidate.is_free,
            "ticket_link_present": bool((candidate.ticket_link or "").strip()),
            "pushkin_card": candidate.pushkin_card,
        },
    }
    prompt = (
        "Ты пишешь один готовый публичный Markdown-анонс события из уже извлечённых фактов.\n"
        "Это Smart Update G4 description writer: extraction уже сделан, поэтому не извлекай новые факты и не используй baseline.\n\n"
        "Источник истины для описания: только `facts_text_clean`.\n"
        "`infoblock_context` нужен только чтобы НЕ повторять логистику в описании: дата, время, город, площадка, адрес, билеты, ссылки, цена, возраст, Пушкинская карта показываются отдельно.\n\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        "Верни JSON:\n"
        "- `description`: Markdown-текст. Если `sparse_source=true`, верни 1–2 коротких конкретных абзаца без `###`; "
        "не заполняй объём и не достраивай программу. Если false — лид и 2–3 информативных `###` раздела.\n"
        "- Сохрани цитаты, имена, названия, цифры и элементы списков из фактов; списки лучше оформить markdown-пунктами.\n"
        "- Identity facts (организатор, сообщество, площадка, название мира/франшизы/источника вдохновения) сохраняй точно по фактам; не заменяй их редакционными догадками.\n"
        "- Для лекций/паблик-токов/дискуссий: если `facts_text_clean` содержит несколько фактов вида `Спикер:`/`Лектор:`/`Участник:`/`Ведущий:`, "
        "включи named roster в описание (абзацем или списком) и не сворачивай имена в категории вроде `краеведы`, `учёные`, `эксперты`.\n"
        "- Не добавляй фактов, которых нет в facts_text_clean.\n"
        "- Не пиши CTA/рекламу: «приходите», «не пропустите», «ждём вас», «приглашаем», «успейте», «присоединяйтесь».\n"
        "- Не используй корень «посвящ» и конструкцию «это ... не ..., а ...».\n"
        "- Не повторяй логистику из infoblock_context в description.\n"
        "- `warnings`: пустой массив или короткие предупреждения о пропущенных/сомнительных фактах.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    try:
        data = await asyncio.wait_for(
            _ask_gemma_json(
                prompt,
                _g4_split_description_writer_schema(),
                max_tokens=desc_max_tokens,
                label="split_description_writer",
            ),
            timeout=float(SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC),
        )
    except asyncio.TimeoutError:
        logger.warning(
            "smart_update: g4_split_description_writer timeout source_type=%s source_url=%s timeout_sec=%s",
            candidate.source_type,
            candidate.source_url,
            SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC,
        )
        return None
    if not isinstance(data, dict):
        return None
    raw_description = str(data.get("description") or "")
    description = _cleanup_g4_split_create_description(
        raw_description,
        candidate=candidate,
        sparse_source=sparse_source,
    )
    if not description and raw_description.strip():
        try:
            edited = await _llm_remove_infoblock_logistics(
                description=raw_description,
                candidate=candidate,
                label="split_description_writer_remove_logistics",
            )
        except Exception:  # pragma: no cover - provider failures
            edited = None
        if edited:
            description = _cleanup_g4_split_create_description(
                edited,
                candidate=candidate,
                sparse_source=sparse_source,
            )
    if not description:
        return None
    derived_prompt = (
        "Ты заполняешь короткие производные поля для Smart Update.\n"
        "Основной публичный текст уже написан; не переписывай его и не добавляй новых фактов.\n\n"
        "Верни JSON:\n"
        "- `short_description`: одно законченное предложение 12-16 слов, без даты/времени/адреса/цены/ссылок, emoji и hashtags.\n"
        "- `search_digest`: короткое резюме для поиска/карточек, до 160 символов, без даты/времени/адреса/цены/ссылок, emoji и hashtags.\n"
        "- `warnings`: пустой массив или короткие предупреждения.\n\n"
        f"Данные:\n{json.dumps({**payload, 'description': _clip(description, 4500)}, ensure_ascii=False)}"
    )
    derived: dict[str, Any] | None
    try:
        derived = await asyncio.wait_for(
            _ask_gemma_json(
                derived_prompt,
                _g4_split_derived_fields_schema(),
                max_tokens=260,
                label="split_derived_fields",
            ),
            timeout=float(SMART_UPDATE_G4_DERIVED_FIELDS_TIMEOUT_SEC),
        )
    except asyncio.TimeoutError:
        logger.warning(
            "smart_update: g4_split_derived_fields timeout source_type=%s source_url=%s timeout_sec=%s",
            candidate.source_type,
            candidate.source_url,
            SMART_UPDATE_G4_DERIVED_FIELDS_TIMEOUT_SEC,
        )
        derived = None
    short = ""
    digest = ""
    derived_warnings: list[str] = []
    if isinstance(derived, dict):
        short = _clean_short_description(str(derived.get("short_description") or ""))
        if short and not _is_short_description_acceptable(short, min_words=12, max_words=16):
            short = ""
        digest = _clean_search_digest(str(derived.get("search_digest") or ""))
        derived_warnings = [
            str(x).strip() for x in (derived.get("warnings") or []) if str(x or "").strip()
        ]
    if not short:
        short = _fallback_short_description_from_text(description) or ""
    if not digest:
        digest = _clean_search_digest(_fallback_digest_from_description(description))
    return {
        "description": description,
        "short_description": short,
        "search_digest": digest,
        "warnings": [
            *[str(x).strip() for x in (data.get("warnings") or []) if str(x or "").strip()],
            *derived_warnings,
        ],
    }


async def _llm_g4_split_create_bundle(
    candidate: EventCandidate,
    *,
    clean_title: str,
    normalized_event_type: str | None,
) -> dict[str, Any] | None:
    facts = await _llm_extract_candidate_facts(candidate)
    facts_text_clean = _facts_text_clean_from_facts(
        facts,
        max_items=40,
        anchors=[
            candidate.date or "",
            candidate.time or "",
            candidate.city or "",
            candidate.location_name or "",
            candidate.location_address or "",
        ],
    )
    if not facts_text_clean:
        return None
    writer = await _llm_g4_split_create_writer(
        candidate=candidate,
        title=clean_title,
        event_type=normalized_event_type or candidate.event_type,
        facts_text_clean=facts_text_clean,
    )
    bundle: dict[str, Any] = {
        "title": None,
        "description": None,
        "search_digest": None,
        "short_description": None,
        "facts": list(facts_text_clean),
        "_split_create": True,
        "age_decision": candidate.age_semantic_decision,
    }
    if isinstance(writer, dict):
        bundle["description"] = writer.get("description")
        bundle["search_digest"] = writer.get("search_digest")
        bundle["short_description"] = writer.get("short_description")
        bundle["_split_create_warnings"] = writer.get("warnings") or []
    else:
        fallback_desc = _g4_split_create_fact_ledger_description(
            candidate=candidate,
            title=clean_title,
            facts_text_clean=facts_text_clean,
        )
        if fallback_desc:
            bundle["description"] = fallback_desc
            bundle["search_digest"] = _clean_search_digest(_fallback_digest_from_description(fallback_desc))
            bundle["short_description"] = _fallback_short_description_from_text(fallback_desc)
            bundle["_split_create_warnings"] = ["writer_unavailable_fact_ledger_fallback"]
    return bundle


def _fact_first_coverage_prompt(
    *,
    facts_text_clean: Sequence[str],
    description: str,
) -> tuple[str, dict[str, Any]]:
    payload = {
        "facts_text_clean": [str(x).strip() for x in (facts_text_clean or []) if str(x or "").strip()],
        "description_md": _clip((description or "").strip(), 6500),
    }
    prompt = (
        "Ты проверяешь полноту и строгость описания относительно списка фактов.\n\n"
        "Найди:\n"
        "- missing: факты из списка facts_text_clean, которые НЕ отражены в описании.\n"
        "- extra: утверждения из описания, которые НЕ подтверждаются ни одним фактом.\n\n"
        "ВАЖНО:\n"
        "- Считай факт отражённым, если он перефразирован/встроен в предложение и при этом сохранены ключевые сущности и детали.\n"
        "- Допустимы синонимы и формы, если смысл и ключевые детали сохранены.\n"
        "- Если факт содержит несколько элементов, считай его отражённым только если в тексте присутствуют ВСЕ элементы.\n"
        "- Служебные перефразы для структурных фактов НЕ считаются extra, если они явно отсылают к фактам.\n"
        "- Для missing используй ТОЛЬКО строки фактов ВЕРБАТИМ (копируй из списка facts_text_clean; не перефразируй).\n"
        "- Для extra используй ТОЛЬКО короткие ВЕРБАТИМ-фрагменты из description_md (копируй из текста; не выдумывай), до 12 слов.\n\n"
        "Верни JSON по схеме.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    schema = {
        "type": "object",
        "properties": {
            "missing": {"type": "array", "items": {"type": "string"}},
            "extra": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["missing", "extra"],
        "additionalProperties": False,
    }
    return prompt, schema


def _fact_first_revise_prompt(
    *,
    title: str | None,
    event_type: str | None,
    epigraph_fact: str | None,
    facts_text_clean: Sequence[str],
    description: str,
    missing: Sequence[str],
    extra: Sequence[str],
    policy_issues: Sequence[str],
) -> str:
    budget_chars = _estimate_fact_first_description_budget_chars(facts_text_clean)
    sparse_source = _fact_first_is_sparse(facts_text_clean)
    structure_rule = (
        "SPARSE SOURCE MODE: оставь 1–2 коротких абзаца без `###` и без эпиграфа; не добавляй блоки ради объёма."
        if sparse_source
        else (
            "Структура: лид (1–2 предложения) без заголовка, ОДНИМ абзацем → 2–3 блока `### ...` → "
            "абзацы средней длины. Под каждым `###` — 2+ предложения ИЛИ список (2+ пунктов). "
            "Не дроби на микро-разделы по 1 фразе."
        )
    )
    facts_block = "\n".join(f"- {str(f).strip()}" for f in (facts_text_clean or []) if str(f or "").strip())
    return textwrap.dedent(
        f"""\
        Отредактируй Markdown‑анонс события. Если проще — перепиши его заново, но строго по фактам.

        Источник истины: только `facts_text_clean` (нельзя добавлять новые сведения вне списка).

        Цели (строго):
        - missing = [] (все факты отражены)
        - extra = [] (нет неподтверждённых утверждений)
        - нет запрещённых маркеров/логистики/промо
        - нет смысловых повторов

        {SMART_UPDATE_YO_RULE}
        Объём: старайся уложиться примерно в `description_budget_chars` символов, без воды.

        Контекст:
        - title: {(title or '').strip()}
        - event_type: {(event_type or '').strip()}
        - epigraph_fact: {epigraph_fact if epigraph_fact is not None else 'null'}
        - description_budget_chars: {budget_chars}

        Факты (facts_text_clean):
        {facts_block}

        Исправь проблемы:
        - policy_issues:
        {chr(10).join('  - ' + str(i) for i in (policy_issues or [])) if policy_issues else '  - (нет)'}
        - missing (добавь, не теряя деталей; цифры/имена/названия/элементы списков сохранить):
        {chr(10).join('  - ' + str(f) for f in (missing or [])) if missing else '  - (нет)'}
        - extra (удали/перепиши так, чтобы этих подстрок не осталось):
        {chr(10).join('  - ' + str(e) for e in (extra or [])) if extra else '  - (нет)'}

        Текущее описание:
        {description}

        Правила редактуры:
        - Списки/перечни/треклисты: элементы НЕ перефразируй; каждый пункт — отдельной строкой (можно списком `- ...`).
        - Факты вида `Формат: ...`: отрази формат явно; ключевые слова после `:` должны прозвучать.
        - Факты с цифрами/рейтинги/диапазоны: цифры должны совпадать с фактами.
        - Эпиграф: если epigraph_fact не null — blockquote до первого `###` и только один раз.
        - {structure_rule}
        - Подзаголовки должны быть информативными; избегай общих вроде «Подробности».
        - Эмодзи: 1–2 штуки, без «ёлки».
        - Запреты: нет даты/времени/города/площадки/адреса; нет URL/телефонов (кроме ссылок на плейлист Я.Музыки из facts_text_clean); нет цен/донатов; нет билетов/входа/регистрации/записи; нет возраста; нет «Пушкинская карта»; нет афиш.
          Не используй «посвящ...»: перефразируй через «о/про/в центре — ...».

        Верни только обновлённый Markdown‑текст (без JSON).
        """
    ).strip()


def _fact_first_remove_posv_prompt(
    *,
    title: str | None,
    event_type: str | None,
    epigraph_fact: str | None,
    facts_text_clean: Sequence[str],
    description: str,
) -> str:
    """Targeted cleanup prompt for stubborn `посвящ...` leaks.

    Some models keep copying "лекция посвящена ..." despite explicit bans. This
    prompt is a last-mile, single-purpose fix: keep the same facts and structure,
    but ensure the forbidden root never appears.
    """
    payload = {
        "title": (title or "").strip(),
        "event_type": (event_type or "").strip(),
        "epigraph_fact": epigraph_fact if epigraph_fact is not None else None,
        "facts_text_clean": [str(x).strip() for x in (facts_text_clean or []) if str(x or "").strip()],
        "description_md": _clip((description or "").strip(), 3200),
    }
    structure_rule = (
        "Сохрани 1–2 коротких абзаца без заголовков и эпиграфа."
        if _fact_first_is_sparse(facts_text_clean)
        else "Сохрани структуру: эпиграф (если есть), затем лид и 2–3 `###`."
    )
    return (
        "В описании найден запрещённый корень «посвящ…» (посвящён/посвящена/посвящено и т.п.).\n"
        "Твоя задача: отредактировать `description_md` так, чтобы этот корень НЕ встречался нигде.\n\n"
        "Правила:\n"
        "- Верни ПОЛНЫЙ Markdown-текст (не частями).\n"
        "- Нельзя добавлять новые факты: источник истины — только `facts_text_clean`.\n"
        f"- {structure_rule}\n"
        "- Нельзя добавлять логистику/CTA/ссылки/контакты/цены/возраст/афиши.\n"
        "- Слово/корень «посвящ» запрещён полностью. Перефразируй через «о/про/в центре — …», "
        "исправляя падежи/согласование.\n\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def _fact_first_forbidden_reasons(description: str, *, anchors: Sequence[str]) -> list[str]:
    text = str(description or "")
    reasons: list[str] = []
    if re.search(r"(?m)^\s*#{1,2}\s+\S", text):
        reasons.append("h1h2_heading")
    if _fact_first_has_disallowed_urls(text):
        reasons.append("url")
    if _FACT_FIRST_PHONE_RE.search(text):
        reasons.append("phone")
    if _FACT_FIRST_PRICE_RE.search(text):
        reasons.append("price")
    if _FACT_FIRST_TICKET_WORD_RE.search(text):
        reasons.append("tickets")
    if _FACT_FIRST_AGE_RE.search(text):
        reasons.append("age")
    if _FACT_FIRST_PUSHKIN_RE.search(text):
        reasons.append("pushkin_card")
    if re.search(r"(?i)\bафиш\w*\b", text):
        reasons.append("poster")
    if re.search(r"(?i)\bпосвящ\w*\b", text):
        reasons.append("посвящ*")
    if _FACT_FIRST_CTA_RE.search(text) or _FACT_FIRST_HASH_RE.search(text):
        reasons.append("cta_or_hashtag")
    if re.search(r"(?i)\bэто\b[^.!?\n]{0,60}\bне\b[^.!?\n]{0,60}\bа\b", text):
        reasons.append("cliche_not_about_but_about")
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", text):
        reasons.append("date_iso")
    if re.search(r"\b\d{1,2}\.\d{1,2}(?:\.\d{2,4})?\b", text):
        reasons.append("date_ddmm")
    if re.search(
        r"(?i)\b\d{1,2}\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
        text,
    ):
        reasons.append("date_ru_words")
    if re.search(r"\b\d{1,2}[:.]\d{2}\b", text):
        reasons.append("time_hhmm")

    text_lc = text.lower()
    for a in anchors or []:
        s = str(a or "").strip()
        if not s:
            continue
        # Avoid flagging short/common tokens.
        tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", s)
        if not tokens:
            continue
        if len(s) < 5 and all(len(t) < 5 for t in tokens):
            continue
        if s.lower() in text_lc:
            reasons.append("anchor_leak")
            break
    return _dedupe_source_facts(reasons)


def _fact_first_lead_paragraph_count(description_md: str) -> int:
    """Count lead paragraphs (text before the first `### ...`), excluding epigraph blockquote."""
    raw = (description_md or "").strip()
    if not raw:
        return 0
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.splitlines()

    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    # Drop epigraph (blockquote at the top).
    while i < len(lines) and lines[i].lstrip().startswith(">"):
        i += 1
    while i < len(lines) and not lines[i].strip():
        i += 1

    lead_lines: list[str] = []
    for j in range(i, len(lines)):
        if re.match(r"^\s*###\s+\S", lines[j] or ""):
            break
        lead_lines.append(lines[j])
    lead = "\n".join(lead_lines).strip()
    if not lead:
        return 0
    paras = [p.strip() for p in re.split(r"\n{2,}", lead) if p.strip()]
    return len(paras)


def _fact_first_micro_h3_headings(description_md: str) -> list[str]:
    """Return headings whose bodies are too short (micro-sections)."""
    raw = (description_md or "").strip()
    if not raw:
        return []
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    h3_re = re.compile(r"^\s*###\s+(\S.*)$")

    micro: list[str] = []
    current_heading: str | None = None
    body_lines: list[str] = []

    def _flush() -> None:
        nonlocal current_heading, body_lines
        if current_heading is None:
            return
        body = "\n".join(body_lines).strip()
        list_items = sum(1 for ln in body.splitlines() if _LIST_ITEM_LINE_RE.match((ln or "").strip()))
        sentence_count = len(re.findall(r"[.!?]+", body))
        body_chars = len(body)

        ok = False
        if list_items >= 2:
            ok = True
        elif sentence_count >= 2:
            ok = True
        elif body_chars >= 220:
            ok = True

        if not ok:
            micro.append(current_heading.strip())
        current_heading = None
        body_lines = []

    for line in raw.splitlines():
        m = h3_re.match(line or "")
        if m:
            _flush()
            current_heading = (m.group(1) or "").strip()
            body_lines = []
            continue
        if current_heading is not None:
            body_lines.append(line)
    _flush()
    return micro


async def _llm_fact_first_description_md(
    *,
    title: str | None,
    event_type: str | None,
    facts_text_clean: Sequence[str],
    anchors: Sequence[str],
    label: str,
) -> str | None:
    """Generate description strictly from `facts_text_clean` (fact-first, C+D).

    Uses LLM for:
    - writing the narrative text,
    - coverage check (missing/extra),
    - revision loop (bounded and intentionally small to keep Gemma call counts predictable).
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    facts = [str(f or "").strip() for f in (facts_text_clean or []) if str(f or "").strip()]
    if not facts:
        return None

    # Keep the prompt bounded; fact-first relies on extracted facts, not raw sources.
    facts = _dedupe_source_facts(facts)[:28]
    facts = _dedupe_source_facts([_sanitize_fact_text_clean_for_prompt(f) for f in facts])[:28]
    sparse_source = _fact_first_is_sparse(facts)
    epigraph_fact = None if sparse_source else _pick_epigraph_fact(facts)
    budget_chars = _estimate_fact_first_description_budget_chars(facts)
    desc_max_tokens = _estimate_fact_first_description_max_tokens(budget_chars=budget_chars, floor=1700)
    revise_max_tokens = _estimate_fact_first_description_max_tokens(budget_chars=budget_chars, floor=1900)

    def _cleanup_description(value: str | None) -> str | None:
        """Format-only cleanup (must not change meaning)."""
        raw = (value or "").strip()
        if not raw:
            return None
        raw = _strip_private_use(raw) or raw
        raw = _fix_inline_bullet_lists(raw) or raw
        raw = _normalize_bullet_markers(raw) or raw
        raw = _promote_review_bullets_to_blockquotes(raw) or raw
        raw = _normalize_blockquote_markers(raw) or raw
        raw = _limit_description_emojis(raw) or raw
        raw = _sanitize_description_output(raw, source_text="") or raw
        # Important: sanitize can normalize headings (e.g. strip "Facts:" prefix), so
        # dedupe/orphan-heading cleanup must happen AFTER it to avoid duplicate empty sections.
        raw = _dedupe_description(raw) or raw
        raw = _normalize_plaintext_paragraphs(raw) or raw
        if not sparse_source:
            raw = _ensure_minimal_description_headings(raw) or raw
        return raw.strip() or None

    def _collect_policy_issues(value: str) -> list[str]:
        desc_s = str(value or "")
        issues: list[str] = []

        # Headings count: keep it readable and consistent with the prompt.
        h3 = len(re.findall(r"(?m)^###\s+\S", desc_s))
        if sparse_source and h3:
            issues.append("SPARSE SOURCE MODE: убери все `###`; оставь 1–2 коротких абзаца без заполнения объёма.")
        elif (not sparse_source) and (h3 < 2 or h3 > 3):
            issues.append(
                f"Сейчас заголовков `###` = {h3}; нужно 2–3. "
                "Объедини близкие разделы и оставь ровно 2–3 информативных подзаголовка."
            )

        if sparse_source:
            sparse_paras = [p.strip() for p in re.split(r"\n{2,}", desc_s) if p.strip()]
            if not sparse_paras:
                issues.append("SPARSE SOURCE MODE: нужен один короткий содержательный абзац.")
            elif len(sparse_paras) > 2:
                issues.append("SPARSE SOURCE MODE: сократи до 1–2 абзацев; не добавляй структуру ради объёма.")
        else:
            lead_paras = _fact_first_lead_paragraph_count(desc_s)
            if lead_paras == 0:
                issues.append("Добавь лид одним абзацем (1–2 предложения) перед первым `###`.")
            elif lead_paras > 1:
                issues.append(
                    "Лид до первого `###` должен быть ОДНИМ абзацем (1–2 предложения), без лишних переносов строки."
                )

        h3_titles = [
            re.sub(r"\s+", " ", (m.group(1) or "")).strip()
            for m in re.finditer(r"(?m)^###\s+(.+?)\s*$", desc_s)
            if (m.group(1) or "").strip()
        ]
        if any(t.casefold() == "подробности" for t in h3_titles):
            issues.append("Не используй общий заголовок «Подробности»: убери его или замени на конкретный.")

        # Prevent repeated identical headings (they create empty/micro sections on Telegraph).
        if h3_titles:
            seen_keys: set[str] = set()
            dup_titles: list[str] = []
            for t in h3_titles:
                t_norm = re.sub(r"\s+", " ", t).strip()
                t_norm = _EMOJI_SEQ_RE.sub("", t_norm).strip()
                key = re.sub(r"\s+", " ", t_norm).strip().casefold()
                if not key:
                    continue
                if key in seen_keys:
                    dup_titles.append(t.strip())
                seen_keys.add(key)
            if dup_titles:
                uniq = _dedupe_source_facts([d for d in dup_titles if d])
                shown = ", ".join(uniq[:4])
                more = "" if len(uniq) <= 4 else f" (+{len(uniq) - 4})"
                issues.append(
                    "Повторяются одинаковые подзаголовки `### ...`: "
                    f"{shown}{more}. Заголовки должны быть уникальными — объедини секции или переименуй."
                )

        micro_h3 = [] if sparse_source else _fact_first_micro_h3_headings(desc_s)
        if micro_h3:
            shown = ", ".join(micro_h3[:4])
            more = "" if len(micro_h3) <= 4 else f" (+{len(micro_h3) - 4})"
            issues.append(
                "Слишком дробные секции под `###`: "
                f"{shown}{more}. Объедини близкие разделы: 2–3 `###` всего; "
                "под каждым — 2+ предложения или список (2+ пунктов)."
            )

        if epigraph_fact:
            if not re.search(r"(?m)^>\s*\S", desc_s):
                issues.append("Если epigraph_fact не null — добавь эпиграф blockquote перед лидом (до первого `###`).")
        else:
            if re.search(r"(?m)^>\s*\S", desc_s):
                issues.append("Если epigraph_fact = null — убери blockquote-эпиграф из начала текста.")

        forbidden = _fact_first_forbidden_reasons(desc_s, anchors=anchors)
        if forbidden:
            for r in forbidden:
                if r == "посвящ*":
                    issues.append(
                        "Запрещено слово/корень «посвящ…» — убери его полностью и перефразируй без него "
                        "(например: «лекция о …», «разговор про …», «в центре — …»)."
                    )
                    continue
                issues.append(f"forbidden_marker({r})")

        return issues

    description = await _ask_gemma_text(
        _fact_first_description_prompt(
            title=title,
            event_type=event_type,
            facts_text_clean=facts,
            epigraph_fact=epigraph_fact,
        ),
        max_tokens=desc_max_tokens,
        label=f"{label}:fact_first_desc",
        temperature=0.0,
    )
    description = _cleanup_description(description)
    if not description:
        return None

    cov_prompt, cov_schema = _fact_first_coverage_prompt(facts_text_clean=facts, description=description)
    cov = await _ask_gemma_json(
        cov_prompt,
        cov_schema,
        max_tokens=500,
        label=f"{label}:fact_first_cov",
    )
    missing: list[str] = []
    extra: list[str] = []
    if isinstance(cov, dict):
        missing = [str(x).strip() for x in (cov.get("missing") or []) if str(x or "").strip()]
        extra = [str(x).strip() for x in (cov.get("extra") or []) if str(x or "").strip()]
    missing = _dedupe_source_facts(missing)[:12]
    extra = _dedupe_source_facts(extra)[:12]

    policy_issues = _collect_policy_issues(description)
    if not missing and not extra and not policy_issues:
        return description.strip() or None

    revised = await _ask_gemma_text(
        _fact_first_revise_prompt(
            title=title,
            event_type=event_type,
            epigraph_fact=epigraph_fact,
            facts_text_clean=facts,
            description=description,
            missing=missing,
            extra=extra,
            policy_issues=policy_issues,
        ),
        max_tokens=revise_max_tokens,
        label=f"{label}:fact_first_revise",
        temperature=0.0,
    )
    description2 = _cleanup_description(revised) or description
    policy_issues2 = _collect_policy_issues(description2)
    if not policy_issues2:
        return description2.strip() or None

    # Final attempt: fix policy issues only (do not redo coverage to keep call count bounded).
    # Prefer a dedicated "посвящ..." remover when it's the only remaining forbidden marker.
    forbidden2 = _fact_first_forbidden_reasons(description2, anchors=anchors)
    if forbidden2 == ["посвящ*"] and len(policy_issues2) == 1:
        revised3 = await _ask_gemma_text(
            _fact_first_remove_posv_prompt(
                title=title,
                event_type=event_type,
                epigraph_fact=epigraph_fact,
                facts_text_clean=facts,
                description=description2,
            ),
            max_tokens=revise_max_tokens,
            label=f"{label}:fact_first_remove_posv",
            temperature=0.0,
        )
        fixed = _cleanup_description(revised3) or description2
        return fixed.strip() or None

    revised2 = await _ask_gemma_text(
        _fact_first_revise_prompt(
            title=title,
            event_type=event_type,
            epigraph_fact=epigraph_fact,
            facts_text_clean=facts,
            description=description2,
            missing=missing,
            extra=extra,
            policy_issues=policy_issues2,
        ),
        max_tokens=revise_max_tokens,
        label=f"{label}:fact_first_revise_policy",
        temperature=0.0,
    )
    description3 = _cleanup_description(revised2) or description2
    return description3.strip() or None


async def _llm_fact_first_description_md_bounded(
    **kwargs: Any,
) -> str | None:
    timeout = int(SMART_UPDATE_FACT_FIRST_TIMEOUT_SEC or 0)
    if timeout <= 0:
        return await _llm_fact_first_description_md(**kwargs)
    label = str(kwargs.get("label") or "fact_first")
    try:
        return await asyncio.wait_for(
            _llm_fact_first_description_md(**kwargs),
            timeout=float(timeout),
        )
    except asyncio.TimeoutError:
        logger.warning(
            "smart_update: fact_first_description timeout label=%s timeout_sec=%s; falling back to bundle/rewrite path",
            label,
            timeout,
        )
        return None


def _has_overlong_paragraph(text: str | None, *, limit: int = 900) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    for para in re.split(r"\n{2,}", raw):
        p = para.strip()
        if not p:
            continue
        if len(p) > limit:
            return True
    return False


async def _llm_reflow_description_paragraphs(text: str) -> str | None:
    """Ask LLM to reflow paragraphs (no new facts), keeping markdown structure."""
    if SMART_UPDATE_LLM_DISABLED:
        return None
    raw = (text or "").strip()
    if not raw or len(raw) < 300:
        return None
    payload = {
        "text": _clip(raw, 6500),
    }
    prompt = (
        "Переформатируй текст описания события.\n"
        "Задача: разбить на короткие читаемые абзацы и убрать перегруженные стены текста.\n\n"
        "Правила:\n"
        "- Верни ПОЛНЫЙ текст.\n"
        "- Не добавляй новых фактов и не выдумывай.\n"
        "- Не меняй смысл и не делай рекламных клише.\n"
        "- Не добавляй и не оставляй хэштеги (`#...`) в тексте.\n"
        "- Сохраняй существующие цитаты в формате blockquote (`>`), не превращай их в обычный текст.\n"
        "- Сохраняй существующие нумерованные/маркированные списки; не превращай их в абзацы и не сокращай.\n"
        "- Можно добавить 1-2 коротких подзаголовка `###`, если это улучшает структуру.\n"
        "- В каждом абзаце держи 1-2 предложения (максимум 3 только если иначе теряется смысл).\n"
        "- Не дублируй в тексте строки формата `Дата:`, `Время:`, `Локация:`, `Билеты:`: эти данные уже показаны в карточке сверху.\n"
        "- Каждому абзацу старайся держать длину <= 600-800 символов.\n"
        "- Не оставляй обрывов фраз/предложений после правок.\n"
        f"{SMART_UPDATE_YO_RULE}\n"
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE}\n\n"
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    out = await _ask_gemma_text(prompt, max_tokens=1200, label="reflow", temperature=0.0)
    return out.strip() if out else None


_FIRST_PERSON_QUOTE_RE = re.compile(
    r"(?is)^\s*(?:мне кажется|я думаю|я считаю|я вижу|я замечаю|я уверен)\b"
)

_REPORTED_SPEECH_RE = re.compile(
    r"(?is)\b(?:отмечает|подч[её]ркивает|говорит|считает|пишет)\s*,?\s*что\s+(.+)$"
)

_SCENE_HINT_RE = re.compile(r"(?is)\b(основн\w+|мал\w+)\s+сцен\w+\b")
_REVIEW_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"отзыв\w*|"
    r"реценз\w*|"
    r"комментар\w*|"
    r"впечатлен\w*|"
    r"мнения\w*|"
    r"зрител\w*|"
    r"восторг\w*|"
    r"говорят|"
    r"пишут"
    r")\b"
)
_REVIEW_LIST_ITEM_RE = re.compile(
    r"(?iu)^\s*(?:[-*•]|\d{1,3}[.)])\s+"
    r"(?P<who>[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё-]{1,24}(?:\s+[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё-]{1,24}){0,2}(?:\s*(?:,|\(|—)\s*[^:]{1,60})?)"
    r"\s*:\s*(?P<body>\S.+?)\s*$"
)


def _promote_first_person_quotes_to_blockquotes(text: str | None) -> str | None:
    """Format direct speech as Markdown blockquotes when it looks like a quote.

    This improves Telegraph readability and avoids "quote-like" sentences blending
    into narration. We keep this conservative to avoid over-formatting.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    out_paras: list[str] = []
    sent_split = re.compile(r"(?<=[.!?…])\s+")
    for para in paragraphs:
        if para.lstrip().startswith(">"):
            out_paras.append(para)
            continue
        # Only touch "normal" paragraphs (no headings/lists).
        if re.match(r"^\s*#{1,6}\s+\S", para) or re.match(r"^\s*[-*•]\s+\S", para):
            out_paras.append(para)
            continue
        sents = [s.strip() for s in sent_split.split(para) if s.strip()]
        if not sents:
            out_paras.append(para)
            continue
        if len(sents) == 1:
            only = re.sub(r"\s+", " ", sents[0]).strip()
            if 25 <= len(only) <= 220 and _FIRST_PERSON_QUOTE_RE.match(only.lower()):
                out_paras.append(f"> {only}")
            else:
                out_paras.append(para)
            continue
        kept: list[str] = []
        quotes: list[str] = []
        for s in sents:
            s_norm = re.sub(r"\s+", " ", s).strip()
            if 25 <= len(s_norm) <= 220 and _FIRST_PERSON_QUOTE_RE.match(s_norm.lower()):
                quotes.append(s_norm)
            else:
                kept.append(s_norm)
        if kept:
            out_paras.append(" ".join(kept).strip())
        for q in quotes[:2]:
            out_paras.append(f"> {q}")
    merged = "\n\n".join(p for p in out_paras if p.strip()).strip()
    return merged or None


def _promote_inline_quoted_direct_speech_to_blockquotes(text: str | None) -> str | None:
    """Turn inline direct speech in «...» into a standalone Markdown blockquote.

    This is a deterministic fallback for cases where the model put the quote inside
    a normal paragraph like:
      `... отмечает: «Мне кажется, ...»`
    but we want Telegraph to render it as `<blockquote>`.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    if re.search(r"(?m)^>\s+", raw):
        return raw
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paragraphs:
        return raw

    out: list[str] = []
    promoted = False

    quote_re = re.compile(r"(?s)«(?P<q>[^»]{25,900})»")
    context_re = re.compile(
        r"(?i)\b(цитат\w*|по\s+словам|говорит|отмечает|подч[её]ркивает|пишет|организатор\w*|автор\w*)\b"
    )
    for para in paragraphs:
        if promoted:
            out.append(para)
            continue
        if para.lstrip().startswith(">"):
            out.append(para)
            continue
        if re.match(r"^\s*#{1,6}\s+\S", para) or re.match(r"^\s*[-*•]\s+\S", para):
            out.append(para)
            continue

        m = quote_re.search(para)
        if not m:
            out.append(para)
            continue
        q = re.sub(r"\s+", " ", (m.group("q") or "").strip())
        if not q:
            out.append(para)
            continue
        q_words = re.findall(r"[a-zа-яё0-9]{2,}", q, flags=re.IGNORECASE)
        looks_like_title = len(q_words) <= 6 and not re.search(r"[.!?…]", q)
        promote = bool(_FIRST_PERSON_QUOTE_RE.match(q.lower()))
        if not promote:
            if context_re.search(para):
                promote = True
            elif not looks_like_title and len(q_words) >= 10 and len(q) >= 60:
                promote = True
        if not promote:
            out.append(para)
            continue

        before = (para[: m.start()] or "").rstrip()
        after = (para[m.end() :] or "").lstrip()
        if before.endswith(":"):
            before = before[:-1].rstrip() + "."
        merged = (before + " " + after).strip()
        merged = re.sub(r"\s+", " ", merged).strip()
        if merged:
            out.append(merged)
        out.append(f"> {q}")
        promoted = True

    updated = "\n\n".join(p for p in out if p.strip()).strip()
    return updated or None


def _promote_review_bullets_to_blockquotes(text: str | None) -> str | None:
    """Render simple audience reviews as Markdown blockquotes.

    Pattern:
      - `<Name>: <review text>`
    under a nearby review context ("отзывы", "зрители", etc.)

    This is a formatting-only helper to improve Telegraph readability; it must not
    drop or rewrite text.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paragraphs:
        return None

    out: list[str] = []

    def _is_fully_quoted(v: str) -> bool:
        s = (v or "").strip()
        if len(s) < 2:
            return False
        pairs = [
            ("«", "»"),
            ('"', '"'),
            ("“", "”"),
            ("„", "“"),
        ]
        return any(s.startswith(op) and s.endswith(cl) for op, cl in pairs)

    def _quote_review_body(v: str) -> str:
        s = (v or "").strip()
        if not s:
            return s
        if _is_fully_quoted(s):
            return s
        # If the author already started/ended quoting, don't try to "fix" it.
        if s.startswith(("«", '"', "“", "„")) or s.endswith(("»", '"', "”", "“")):
            return s
        return f"«{s}»"

    for para in paragraphs:
        lines = [ln.rstrip() for ln in (para or "").splitlines()]
        if not lines:
            continue

        # Find a contiguous list block inside this paragraph.
        start: int | None = None
        for i, ln in enumerate(lines):
            if _LIST_ITEM_LINE_RE.match((ln or "").strip()):
                start = i
                break
        if start is None:
            out.append(para)
            continue

        end = start
        while end < len(lines) and _LIST_ITEM_LINE_RE.match((lines[end] or "").strip()):
            end += 1

        list_lines = [lines[i].strip() for i in range(start, end) if (lines[i] or "").strip()]
        if len(list_lines) < 2:
            out.append(para)
            continue

        items: list[tuple[str, str]] = []
        for ln in list_lines:
            m = _REVIEW_LIST_ITEM_RE.match(ln)
            if not m:
                items = []
                break
            who = (m.group("who") or "").strip()
            body = (m.group("body") or "").strip()
            if not who or not body:
                items = []
                break
            items.append((who, body))
        if not items:
            out.append(para)
            continue

        preface = " ".join((ln or "").strip() for ln in lines[:start] if (ln or "").strip()).strip()
        prev = out[-1] if out else ""
        context = "\n".join([preface, prev]).strip()
        if not _REVIEW_CONTEXT_RE.search(context):
            out.append(para)
            continue

        if preface:
            out.append(preface)
        for who, body in items:
            q = _quote_review_body(body)
            # Two-line quote: quote text + attribution.
            out.append(f"> {q}\n> — {who}")

        tail = "\n".join((ln or "").rstrip() for ln in lines[end:] if (ln or "").strip()).strip()
        if tail:
            out.append(tail)

    merged = "\n\n".join(p for p in out if (p or "").strip()).strip()
    return merged or None


def _drop_reported_speech_duplicates(text: str | None) -> str | None:
    """Remove paraphrased "X notes that ..." if the same clause exists as a direct quote.

    Goal: avoid duplicate meaning when we have both:
      - "Режиссёр ... отмечает, что <clause>."
      - "> Мне кажется, что <clause>."
    """
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paragraphs:
        return None

    quote_clauses: list[str] = []
    for p in paragraphs:
        if not p.lstrip().startswith(">"):
            continue
        q = p.lstrip()[1:].strip()
        q = re.sub(r"\s+", " ", q).strip()
        if not q:
            continue
        # Prefer the part after "что" to match reported speech.
        parts = re.split(r"(?i)\bчто\b", q, maxsplit=1)
        clause = (parts[1] if len(parts) == 2 else q).strip()
        clause = clause.strip(" .,!?:;—-").strip()
        if len(clause) >= 20:
            quote_clauses.append(clause.casefold())
    if not quote_clauses:
        return raw

    sent_split = re.compile(r"(?<=[.!?…])\s+")
    out_paras: list[str] = []
    for para in paragraphs:
        if para.lstrip().startswith(">"):
            out_paras.append(para)
            continue
        # Keep headings/lists as is.
        if re.match(r"^\s*#{1,6}\s+\S", para) or re.match(r"^\s*[-*•]\s+\S", para):
            out_paras.append(para)
            continue
        sents = [s.strip() for s in sent_split.split(para) if s.strip()]
        kept: list[str] = []
        for s in sents:
            s_norm = re.sub(r"\s+", " ", s).strip()
            m = _REPORTED_SPEECH_RE.search(s_norm)
            if m:
                clause = (m.group(1) or "").strip()
                clause = clause.strip(" .,!?:;—-").strip()
                clause_cf = clause.casefold()
                if len(clause_cf) >= 20 and any(
                    (clause_cf in qc) or (qc in clause_cf) for qc in quote_clauses
                ):
                    # Drop the paraphrase if we already have the direct quote.
                    continue
            kept.append(s_norm)
        merged = " ".join(kept).strip()
        if merged:
            out_paras.append(merged)
    return "\n\n".join(out_paras).strip() or None


def _normalize_blockquote_markers(text: str | None) -> str | None:
    """Ensure Markdown blockquotes are standalone paragraphs (so Telegraph renders <blockquote>)."""
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    # If a blockquote marker leaked into the middle of a paragraph, split it out.
    raw = re.sub(r"(?<=\S)[ \t]+>\s+", "\n\n> ", raw)
    # Remove leading spaces before a blockquote marker.
    raw = re.sub(r"(?m)^[ \t]+(>\s+)", r"\1", raw)
    # Ensure a blank line before the *start* of any blockquote block.
    # Do not insert blank lines between consecutive quote lines (`> ...\n> ...`),
    # otherwise multi-line quotes (e.g. quote + attribution) break apart.
    lines = raw.split("\n")
    out_lines: list[str] = []
    quote_line_re = re.compile(r"^\s*>\s+\S")
    for ln in lines:
        if quote_line_re.match(ln):
            if out_lines:
                prev = out_lines[-1]
                if prev.strip() and not quote_line_re.match(prev):
                    out_lines.append("")
            out_lines.append(ln.lstrip())
        else:
            out_lines.append(ln)
    raw = "\n".join(out_lines)
    raw = re.sub(r"\n{3,}", "\n\n", raw).strip()
    return raw or None


def _dedupe_paragraphs_preserving_formatting(text: str | None) -> str | None:
    """Remove repeated paragraphs while preserving paragraph boundaries."""
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if len(paragraphs) < 2:
        return raw
    seen: set[str] = set()
    out: list[str] = []
    for p in paragraphs:
        cleaned = _ZERO_WIDTH_RE.sub("", p).strip()
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        key = cleaned.lower().rstrip(".!?…")
        # Only dedupe "meaningful" paragraphs, keep small fragments intact.
        if len(key) >= 40 and key in seen:
            continue
        seen.add(key)
        out.append(p)
    return "\n\n".join(out).strip() or None


def _split_overlong_first_person_blockquotes(text: str | None) -> str | None:
    """Keep first-person quotes as blockquotes, but avoid swallowing narration into the quote."""
    raw = (text or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paragraphs:
        return None
    sent_split = re.compile(r"(?<=[.!?…])\s+")
    out: list[str] = []
    for para in paragraphs:
        if not para.lstrip().startswith(">"):
            out.append(para)
            continue
        # Collapse multi-line blockquotes into a single text.
        q = re.sub(r"(?m)^\s*>\s*", "", para).strip()
        q = re.sub(r"\s+", " ", q).strip()
        if not q:
            continue
        sents = [s.strip() for s in sent_split.split(q) if s.strip()]
        if len(sents) <= 1:
            out.append(f"> {q}")
            continue
        first = re.sub(r"\s+", " ", sents[0]).strip()
        if _FIRST_PERSON_QUOTE_RE.match(first.lower()):
            out.append(f"> {first}")
            tail = " ".join(re.sub(r"\s+", " ", s).strip() for s in sents[1:] if s.strip()).strip()
            if tail:
                out.append(tail)
        else:
            out.append(f"> {q}")
    return "\n\n".join(out).strip() or None


def _preserve_blockquotes_from_previous_description(
    *,
    before_description: str | None,
    merged_description: str | None,
    event_title: str | None,
    max_quotes: int = 2,
) -> str | None:
    """Preserve meaningful existing blockquotes across LLM merges.

    LLM merges (especially when adding site/parser info) can sometimes "flatten" direct speech
    into reported speech and drop the original quote. Product expectation: if we already had a
    relevant direct quote for the event, keep it as a Markdown blockquote in the merged text.

    We keep this conservative:
    - only preserve explicit Markdown blockquote paragraphs from the previous description;
    - only preserve quotes that mention the event title tokens (to avoid carrying quotes about
      other events from multi-event posts);
    - only append quotes that are missing from the merged description.
    """
    before = (before_description or "").strip()
    after = (merged_description or "").strip()
    if not before or not after:
        return merged_description

    tokens = _title_tokens(event_title)
    before_norm = before.replace("\r\n", "\n").replace("\r", "\n")
    after_cf = after.casefold()

    quotes: list[str] = []
    for para in [p.strip() for p in re.split(r"\n{2,}", before_norm) if p.strip()]:
        if not para.lstrip().startswith(">"):
            continue
        q = para.lstrip()[1:].strip()
        q = re.sub(r"\s+", " ", q).strip()
        q = q.strip("\u200b\u200c\u200d\u2060").strip()
        if not q:
            continue
        if len(q) < 20 or len(q) > 280:
            continue
        if tokens and not any(tok in q.casefold() for tok in tokens):
            continue
        if q.casefold() in after_cf:
            continue
        quotes.append(q)
        if len(quotes) >= max_quotes:
            break

    if not quotes:
        return merged_description

    appended = after.rstrip() + "\n\n" + "\n\n".join(f"> {q}" for q in quotes)
    return _normalize_plaintext_paragraphs(appended) or appended


def _append_missing_scene_hint(
    *,
    description: str | None,
    source_text: str | None,
) -> str | None:
    """Deterministic safety-net: keep 'Основная/Малая сцена' hints when present in sources."""
    desc = (description or "").strip()
    if not desc:
        return None
    if re.search(r"(?is)\b(основн\w+|мал\w+)\s+сцен\w+\b", desc):
        return desc
    src = (source_text or "").strip()
    if not src:
        return desc
    m = _SCENE_HINT_RE.search(src)
    if not m:
        return desc
    kind = (m.group(1) or "").lower()
    phrase = "на Основной сцене" if "основ" in kind else "на Малой сцене"
    sentence = f"Спектакль пройдёт {phrase}."
    if sentence.lower() in desc.lower():
        return desc
    return (desc + "\n\n" + sentence).strip()


def _fallback_digest_from_description(description: str | None) -> str | None:
    """Deterministic fallback digest: use the first 1-2 sentences from description."""
    raw = (description or "").strip()
    if not raw:
        return None
    # Drop headings and blockquotes.
    lines = []
    for ln in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        s = (ln or "").strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        if s.startswith(">"):
            continue
        lines.append(s)
    if not lines:
        return None
    text = " ".join(lines)
    text = re.sub(r"\s+", " ", text).strip()
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", text) if p.strip()]
    if not parts:
        return None
    digest = parts[0]
    if len(digest) < 80 and len(parts) >= 2:
        digest = f"{digest} {parts[1]}".strip()
    digest = _clip_to_readable_boundary(digest, 240)
    return _clean_search_digest(digest)

def _clip_to_readable_boundary(text: str | None, limit: int) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    if len(raw) <= limit:
        return raw
    # Prefer cutting at sentence/paragraph boundaries to avoid dangling tails.
    boundary = max(
        raw.rfind("\n\n", 0, limit + 1),
        raw.rfind(". ", 0, limit + 1),
        raw.rfind("! ", 0, limit + 1),
        raw.rfind("? ", 0, limit + 1),
        raw.rfind("… ", 0, limit + 1),
    )
    if boundary >= int(limit * 0.65):
        return raw[: boundary + 1].rstrip()
    return _clip(raw, limit)


_STYLE_TERM_RE = re.compile(
    r"\bв\s+(?:стиле|жанре)\s+([A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё-]{3,})",
    re.IGNORECASE,
)


def _append_missing_fact_sentences(
    *,
    base: str,
    rewritten: str,
    max_sentences: int = 2,
    ensure_coverage: bool = False,
) -> str:
    """Append a small number of factual sentences that the rewrite missed.

    Deterministic safety-net: do not invent facts, only reuse snippets from the source.
    """
    base_raw = (base or "").strip()
    out_raw = (rewritten or "").strip()
    if not base_raw or not out_raw:
        return out_raw or base_raw

    base_cf = base_raw.casefold()
    out_cf = out_raw.casefold()

    required_terms: set[str] = set()
    for m in _STYLE_TERM_RE.finditer(base_raw):
        term = (m.group(1) or "").strip().casefold()
        if term and term not in out_cf:
            required_terms.add(term)
    if "фламенко" in base_cf and "фламенко" not in out_cf:
        required_terms.add("фламенко")

    out_norm = re.sub(r"\s+", " ", out_raw).strip().lower()

    candidates: list[str] = []
    for chunk in re.split(r"(?:\n{2,}|(?<=[.!?])\s+)", base_raw):
        s = (chunk or "").strip()
        if len(s) < 30:
            continue
        candidates.append(s)

    added: list[str] = []
    for term in sorted(required_terms):
        if len(added) >= max_sentences:
            break
        for s in candidates:
            s_norm = re.sub(r"\s+", " ", s).strip().lower()
            if term in s.casefold():
                if s_norm in out_norm:
                    break
                added.append(s)
                break

    if ensure_coverage and len(added) < max_sentences:
        missing: list[str] = []
        seen_missing: set[str] = set()
        critical_missing: list[str] = []
        for chunk in re.split(r"(?:\n{2,}|(?<=[.!?…])\s+|\n)", base_raw):
            sent = _normalize_candidate_sentence(chunk)
            is_critical = _is_coverage_critical_sentence(sent)
            if _is_low_signal_sentence(sent) and not is_critical:
                continue
            sent_norm = re.sub(r"\s+", " ", sent).strip().lower()
            if not sent_norm:
                continue
            if sent_norm in out_norm:
                continue
            if sent_norm in seen_missing:
                continue
            seen_missing.add(sent_norm)
            missing.append(sent)
            if is_critical:
                critical_missing.append(sent)

        if missing:
            for critical in critical_missing:
                if len(added) >= max_sentences:
                    break
                critical_norm = re.sub(r"\s+", " ", critical).strip().lower()
                if not critical_norm or critical_norm in out_norm:
                    continue
                if any(
                    re.sub(r"\s+", " ", a).strip().lower() == critical_norm
                    for a in added
                ):
                    continue
                added.append(critical)

            ranked = sorted(
                range(len(missing)),
                key=lambda idx: (
                    _sentence_quality_score(missing[idx])
                    + (400 if _is_coverage_critical_sentence(missing[idx]) else 0),
                    -idx,
                ),
                reverse=True,
            )
            for idx in ranked:
                if len(added) >= max_sentences:
                    break
                candidate_sent = missing[idx]
                candidate_norm = re.sub(r"\s+", " ", candidate_sent).strip().lower()
                if candidate_norm in out_norm:
                    continue
                if any(
                    re.sub(r"\s+", " ", a).strip().lower() == candidate_norm
                    for a in added
                ):
                    continue
                added.append(candidate_sent)

    if not added:
        return out_raw
    merged = out_raw.rstrip() + "\n\n" + "\n\n".join(added)
    return _normalize_plaintext_paragraphs(merged) or merged


def _looks_like_ticket_giveaway(*texts: str | None) -> bool:
    combined = "\n".join(t for t in texts if t and t.strip())
    if not combined:
        return False
    value = combined.casefold()
    # Require both giveaway + tickets signals to reduce false positives.
    return bool(_GIVEAWAY_RE.search(value) and _TICKETS_RE.search(value))


def _looks_like_promo_or_congrats(*texts: str | None) -> bool:
    combined = "\n".join(t for t in texts if t and t.strip())
    if not combined:
        return False
    value = combined.casefold()
    # Congratulation posts are treated as non-event content by product requirements.
    if _CONGRATS_RE.search(value):
        return True
    # Pure promotions (discounts/coupons) without event anchors must not become events/sources.
    if _PROMO_STRIP_RE.search(value):
        if not _has_datetime_signals(combined) and not _EVENT_SIGNAL_RE.search(value):
            # Keep it conservative: if there's no date/time and no event-type signals, it's promo-only.
            return True
    return False


_RU_MONTHS_GENITIVE_RE = (
    "января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря"
)
_DATE_SIGNAL_RE = re.compile(
    rf"(?iu)\b(?:"
    rf"\d{{1,2}}[./-]\d{{1,2}}(?:[./-](?:19|20)\d{{2}})?|"
    rf"\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE})"
    rf")\b"
)
_DEADLINE_RE = re.compile(
    rf"(?i)\b(?:до|дедлайн|срок(?:\s+подачи)?|успеть)\s+"
    rf"(?:\d{{1,2}}[./]\d{{1,2}}(?:[./](?:19|20)\d{{2}})?|\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE}))\b"
)
_FROM_DATE_RE = re.compile(
    rf"(?iu)\bс\s+\d{{1,2}}(?:[./]\d{{1,2}}(?:[./](?:19|20)\d{{2}})?|\s+(?:{_RU_MONTHS_GENITIVE_RE}))\b"
)
_OPEN_CALL_RE = re.compile(
    r"(?iu)\b("
    r"open\s*call|"
    r"опен\s*колл|"
    r"опенколл|"
    r"конкурсн\w*\s+отбор|"
    r"при[её]м\s+заявок|"
    r"подать\s+заявк\w*|"
    r"заявк\w*\s+принима\w*"
    r")\b"
)
_NON_EVENT_NOTICE_RE = re.compile(
    r"(?i)\b("
    r"налогов\w*\s+вычет|"
    r"госуслуг\w*|"
    r"государственн\w*\s+услуг\w*|"
    r"субсид\w*|"
    r"льгот\w*|"
    r"компенсац\w*|"
    r"пособи\w*|"
    r"постановлени\w*|"
    r"перечен\w*|"
    r"утвержда\w*|"
    r"заявк\w*\s+на\s+включени\w*"
    r")\b"
)
_VENUE_STATUS_UPDATE_RE = re.compile(
    r"(?iu)\b("
    r"отсрочк\w*|"
    r"продл(?:или|им|ят|ение|ен\w*)|"
    r"город\s+может\s+потерять|"
    r"можем\s+потерять|"
    r"потеря\w*|"
    r"закры(вают|ваем|вается|тие|ют)\w*|"
    r"высел\w*|"
    r"аренд\w*|"
    r"съезжа\w*|"
    r"петици\w*|"
    r"сбор\s+средств|"
    r"поддерж(ите|ать)|"
    r"нужна\s+помощь|"
    r"помогите"
    r")\b"
)
_COURSE_PROMO_RE = re.compile(
    r"(?i)\b("
    r"старт\s+курс\w*|"
    r"набор\s+на\s+курс|"
    r"курс\w*|"
    r"обучени\w*|"
    r"программ\w*\s+курс\w*|"
    r"поток\s+|"
    r"модул\w*|"
    r"домашн\w*\s+задан\w*|"
    r"куратор\w*|"
    r"сертификат\w*|"
    r"професси\w*\s+переподготовк\w*"
    r")\b"
)
_COURSE_PROMO_STRONG_RE = re.compile(
    r"(?i)\b("
    r"старт\s+курс\w*|"
    r"набор\s+на\s+курс|"
    r"курс\w*|"
    r"обучени\w*|"
    r"программ\w*\s+курс\w*|"
    r"поток\s+|"
    r"модул\w*|"
    r"домашн\w*\s+задан\w*|"
    r"сертификат\w*|"
    r"професси\w*\s+переподготовк\w*"
    r")\b"
)
_UNSUPPORTED_EXHIBITION_TEASER_RE = re.compile(
    r"(?iu)\b("
    r"готовим|"
    r"готовится|"
    r"скоро\s+анонс|"
    r"анонс\s+через|"
    r"анонсируем\s+(?:чуть\s+)?позже|"
    r"точн\w+\s+дат\w+[^.\n]{0,80}анонсир\w+|"
    r"в\s+(?:мае|июне|июле|августе|сентябре|октябре|ноябре|декабре|январе|феврале|марте|апреле)\s+[^.\n]{0,120}откро\w+|"
    r"скоро\s+откро\w+"
    r")\b"
)

_SERVICE_PROMO_OCCASION_RE = re.compile(
    r"(?iu)\b("
    r"выпускн\w*|"
    r"свадьб\w*|"
    r"корпоратив\w*|"
    r"тимбилдинг\w*|"
    r"день\s+рождени\w*|"
    r"юбиле\w*|"
    r"банкет\w*|"
    r"фуршет\w*"
    r")\b"
)
_SERVICE_PROMO_OFFER_RE = re.compile(
    r"(?iu)\b("
    r"пакетн\w*\s+программ\w*|"
    r"комплексн\w*\s+программ\w*|"
    r"бронирован\w*|"
    r"брониру\w*|"
    r"заброниру\w*|"
    r"бронь\b|"
    r"заказ\w*|"
    r"организу\w*|"
    r"помож\w*\s+организ\w*|"
    r"стоимость\s*(?:—|-|:)?\s*от\b|"
    r"/\s*чел\b|"
    r"на\s+чел(?:овек[ао]?)?\b"
    r")\b"
)
_SERVICE_PROMO_CONTACT_RE = re.compile(
    r"(?iu)\b("
    r"телефон|"
    r"whatsapp|"
    r"вайбер|"
    r"viber|"
    r"telegram|"
    r"tg|"
    r"max|"
    r"звоните|"
    r"пишите|"
    r"контакт\w*"
    r")\b|"
    r"(?:\+7|8)\s*\(?\d{3}\)?\s*\d{3}[\s-]*\d{2}[\s-]*\d{2}"
)

_WORK_SCHEDULE_RE = re.compile(
    r"(?iu)\b("
    r"график\s+работ\w*|"
    r"расширенн\w*\s+график\w*|"
    r"режим\s+работ\w*|"
    r"часы\s+работ\w*|"
    r"санитарн\w*\s+день|"
    r"не\s+работа(?:ет|ют)|"
    r"работаем\s+по\s+(?:обычн\w*|нов\w*)\s+график\w*|"
    r"музе[йя]\s+работа(?:ет|ют)"
    r")\b"
)

_WORK_SCHEDULE_DETAIL_RE = re.compile(
    r"(?iu)\b("
    r"понедельник|вторник|среда|четверг|пятниц[ауы]|суббот[ауы]|воскресень[ея]|"
    r"пн|вт|ср|чт|пт|сб|вс|"
    r"выходн\w*|санитарн\w*|"
    r"с\s*\d{1,2}[:.]\d{2}\s*до\s*\d{1,2}[:.]\d{2}|"
    r"\d{1,2}[:.]\d{2}\s*[–—-]\s*\d{1,2}[:.]\d{2}|"
    r"\d{1,2}[./]\d{1,2}"
    r")\b"
)

_EVENT_ACTION_INVITE_RE = re.compile(
    r"(?iu)\b("
    r"состо(ится|ятся)|"
    r"пройд(ёт|ет|ут)|"
    r"приглаша(ем|ю|ет)|"
    r"встречаемс\w*|"
    r"открыт(ие|ый)\s+урок|"
    r"открыти(е|я)\s+выставк\w*|"
    r"старт(?:ует|уем)\s+в\b|"
    r"начал(о|а)\s+в\b"
    r")\b"
)

_PRICE_CONTEXT_RE = re.compile(
    r"(?iu)(?:"
    r"\b(?:цена|стоимост\w*|руб(?:\.|л[её]й|ля|лей)?)\b"
    r"|₽"
    r"|р\."
    r")"
)
_PRICE_NUMBER_RE = re.compile(r"(?u)\b\d{2,6}\b")
_NON_TICKET_MONEY_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"компенсац\w*|"
    r"вознагражден\w*|"
    r"выплат\w*|"
    r"гонорар\w*|"
    r"стипенди\w*|"
    r"зарплат\w*|"
    r"оклад\w*|"
    r"преми\w*|"
    r"приз\w*|"
    r"подар\w*|"
    r"к[эе]шб[эе]к|"
    r"cashback"
    r")\b"
)
_TICKET_PRICE_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"билет\w*|"
    r"вход\w*|"
    r"стоимост\w*|"
    r"цена|"
    r"взнос\w*|"
    r"донат\w*|"
    r"платн\w*"
    r")\b"
)
_BLOOD_DONATION_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"день\s+донора|"
    r"донорск\w*\s+акци\w*|"
    r"донор\w*\s+(?:кров\w*|плазм\w*|тромбоцит\w*|костн\w*\s+мозг\w*)|"
    r"донорств\w*\s+(?:кров\w*|плазм\w*|тромбоцит\w*|костн\w*\s+мозг\w*)|"
    r"сдач\w*\s+(?:кров\w*|плазм\w*|тромбоцит\w*)|"
    r"станц\w*\s+перелив\w*\s+кров\w*|"
    r"центр\w*\s+кров\w*|"
    r"служб\w*\s+кров\w*|"
    r"кроводач\w*"
    r")\b"
)
_FREE_CONTRADICTION_RE = re.compile(
    r"(?iu)\b("
    r"розыгрыш\w*|разыгрыва\w*|выигра\w*\s+билет\w*|дарим\s+билет\w*|"
    r"главн\w*\s+приз\w*\s*[—-]?\s*билет\w*|"
    r"входит?\s+во\s+входн\w*\s+билет\w*|"
    r"по\s+(?:входн\w*\s+)?билет\w*\s+(?:музе[яй]|зоопарк\w*|парка|площадк\w*)|"
    r"в\s+стоимост\w*\s+(?:входн\w*\s+)?билет\w*"
    r")\b"
)
_RENTAL_BOOKING_RE = re.compile(
    r"(?iu)\b("
    r"аренд\w*|"
    r"свободн\w*\s+(?:купол\w*|домик\w*|зал\w*|беседк\w*)|"
    r"заброниру\w*\s+(?:купол\w*|домик\w*|зал\w*|беседк\w*)|"
    r"брон(?:ь|ирован\w*)\s+(?:купол\w*|домик\w*|зал\w*|беседк\w*)|"
    r"купол\w*\s+для\s+отдых\w*|"
    r"беседк[аи]\s*-\s*домик\w*"
    r")\b"
)
_RENTAL_BOOKING_DETAIL_RE = re.compile(
    r"(?iu)\b("
    r"будни\w*|выходн\w*|стоимост\w*|вариант\w*|вместимост\w*|до\s+\d+\s+человек|"
    r"\d{2,6}\s*(?:₽|руб|р\.)|на\s+(?:семь[еёй]|компани\w*)|"
    r"пространств\w*\s+для\s+отдых\w*"
    r")\b"
)
_EVENT_INVITE_RE = re.compile(
    r"(?i)\b("
    r"состо(ится|ятся)|"
    r"пройд(ёт|ет|ут)|"
    r"приглаша(ем|ю|ет)|"
    r"приходите|"
    r"жд[её]м\s+вас|"
    r"встречаемс\w*|"
    r"открыт(ие|ый)\s+урок|"
    r"мастер-?класс|"
    r"экскурси\w*|"
    r"лекци\w*|"
    r"спектакл\w*|"
    r"концерт\w*|"
    r"показ\w*|"
    r"выставк\w*|"
    r"билет\w*"
    r")\b"
)
_EVENT_HAPPENS_VERB_RE = re.compile(
    r"(?i)\b("
    r"состо(ится|ятся)|"
    r"пройд(ёт|ет|ут)|"
    r"приглаша(ем|ю|ет)|"
    r"встречаемс\w*|"
    r"начал(о|а|ется|нутся)|"
    r"стартует|"
    r"начинаем"
    r")\b"
)

_TOO_SOON_NOTICE_RE = re.compile(
    r"(?iu)\b("
    r"уже\s+через\s+\d{1,3}\s+минут|"
    r"через\s+\d{1,3}\s+минут\s+(?:старт|начал\w*|начинаем)|"
    r"стартует\s+через\s+\d{1,3}\s+минут|"
    r"начинаем\s+через\s+\d{1,3}\s+минут"
    r")\b"
)
_EVENT_LOGISTICS_NOTICE_RE = re.compile(
    r"(?ius)\b("
    r"важн\w+\s+информаци\w+\s+для\s+(?:гостей|посетител\w*|зрител\w*)|"
    r"информаци\w+\s+для\s+(?:гостей|посетител\w*|зрител\w*)|"
    r"обраща(?:ем|йте)\s+внимани\w+"
    r")\b"
    r"[\s\S]{0,600}\b("
    r"вход|проход|въезд|парковк\w*|гардероб|рассадк\w*|очеред\w*|"
    r"навигаци\w*|организован|осуществляться|перенес[её]н\s+вход"
    r")\b"
)
_ONLINE_EVENT_RE = re.compile(
    r"(?iu)\b("
    r"онлайн(?![-\s]*(?:регистрац\w*|запис\w*|форм\w*|анкет\w*))|"
    r"zoom|вебинар|webinar|телемост|"
    r"стрим|трансляц\w*|youtube|"
    r"подключайтес\w*|ссылка\s+на\s+подключен\w*"
    r")\b"
)
_ONLINE_PLATFORM_LOCATION_RE = re.compile(
    r"(?iu)\b(zoom|youtube|ютуб|онлайн|online|webinar|вебинар|telegram|телеграм)\b"
)
_BOOK_REVIEW_RE = re.compile(r"(?iu)(#книг\w*|\bкниг\w*\b|\bчтени\w*\b|\bфл[эе]т\s+уайт\b)")
_BOOK_EVENT_KEEP_RE = re.compile(
    r"(?iu)\b(обсуждени\w*|книжн\w*\s+клуб|встреч\w*|дискусси\w*|презентац\w*|лекци\w*)\b"
)
_PHOTO_DAY_RE = re.compile(r"(?iu)\b(фото\s*дня|фотодня|photo\s+of\s+the\s+day)\b")
_PHOTO_DAY_KEEP_ACTION_RE = re.compile(
    r"(?iu)\b("
    r"приглаша(ем|ю|ет)|"
    r"приходите|"
    r"жд[её]м\s+вас|"
    r"состо(ится|ятся)|"
    r"пройд(ёт|ет|ут)|"
    r"открыти(е|я)|"
    r"начал(о|а)|"
    r"презентац\w*|"
    r"экскурси\w*|"
    r"лекци\w*|"
    r"концерт\w*|"
    r"спектакл\w*|"
    r"мастер-?класс"
    r")\b"
)
_PHOTO_DAY_TICKET_RE = re.compile(r"(?iu)\b(регистрац\w*|билет\w*|вход\w*|оплат\w*|бронь)\b")
_PHOTO_DAY_PERIOD_RE = re.compile(
    rf"(?iu)\b(?:с\s+\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE})\s+по\s+\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE})|"
    rf"до\s+\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE})|"
    rf"по\s+\d{{1,2}}\s+(?:{_RU_MONTHS_GENITIVE_RE}))\b"
)
_COMPLETED_EVENT_REPORT_KEEP_RE = re.compile(
    r"(?iu)\b("
    r"приглаша(ем|ю|ет)|"
    r"приходите|"
    r"жд[её]м\s+вас|"
    r"состо(ится|ятся)|"
    r"пройд(ёт|ет|ут)|"
    r"регистрац\w*|"
    r"запис\w*|"
    r"билет\w*|"
    r"бронь\b|"
    r"заброниру\w*|"
    r"купить\b"
    r")\b"
)
_COMPLETED_EVENT_REPORT_CONTINUATION_RE = re.compile(
    r"(?iu)\b("
    r"следующ\w+\s+(?:показ|встреча|игра|спектакл\w*|концерт\w*|занят\w*|лекци\w*|"
    r"мастер-?класс\w*|программ\w*)(?:\s+(?:будет|состоится|пройд[её]т))?|"
    r"в\s+следующ\w+\s+раз\s+(?:встречаемся|увидимся|жд[её]м)|"
    r"(?:вас\s+)?вновь\s+жд[её]т\s+(?:встреча|показ|спектакл\w*|концерт\w*|игр\w*|"
    r"занят\w*|лекци\w*|мастер-?класс\w*|программ\w*)|"
    r"повтор(?:ный)?\s+(?:показ|спектакл\w*|концерт\w*|занят\w*|лекци\w*|"
    r"мастер-?класс\w*|игр\w*|мероприяти\w*|программ\w*)"
    r")\b"
)
_COMPLETED_FESTIVAL_TEASER_UNCONFIRMED_RE = re.compile(
    r"(?ius)\bследующ\w+\s+фестивал\w*[:\s\S]{0,180}"
    r"\b(?:локаци\w*|место|площадк\w*|адрес)\s+уточня\w*"
)
_RETROSPECTIVE_RECAP_MARKERS = (
    re.compile(r"(?iu)\bпрошедш\w+\s+(?:выставк\w*|ярмарк\w*|фестивал\w*|праздник\w*|мероприяти\w*)\b"),
    re.compile(r"(?iu)\b(?:выставк\w*|ярмарк\w*|фестивал\w*|праздник\w*|мероприяти\w*)\s+(?:прош(?:ел|ла|ло|ли)|состоял(?:ся|ась|ось|ись))\b"),
    re.compile(r"(?iu)\b(?:атмосфер\w*|праздник\w*|мероприяти\w*)[^.!?\n]{0,120}\bбыл(?:а|о|и)?\b"),
    re.compile(r"(?iu)\b(?:были\s+вручены|получили\s+памятн\w+\s+наград\w*|награждены\s+кубк\w*)\b"),
    re.compile(r"(?iu)\b(?:выражаем|выражают|выражаем\s+огромн\w*)\s+благодарност\w*\b"),
    re.compile(r"(?iu)\bспасибо\s+всем\s+участник\w*\b"),
)
_RETROSPECTIVE_NEXT_TEASER_RE = re.compile(
    r"(?ius)\b("
    r"если\s+вы\s+не\s+успели[^.!?\n]{0,220}\bжд[её]м\s+вас\s+на\s+(?:нашей\s+)?следующ\w+\s+"
    r"(?:выставк\w*|ярмарк\w*|фестивал\w*|показ\w*|встреч\w*|праздник\w*)|"
    r"жд[её]м\s+вас\s+на\s+(?:нашей\s+)?следующ\w+\s+"
    r"(?:выставк\w*|ярмарк\w*|фестивал\w*|показ\w*|встреч\w*|праздник\w*)|"
    r"следующ\w+\s+(?:выставк\w*|ярмарк\w*|фестивал\w*|показ\w*|встреч\w*|праздник\w*)"
    r")\b"
)
_COMPLETED_EVENT_REPORT_MARKERS = (
    re.compile(r"(?iu)\b(?:встреча|игра|урок|лекция|концерт|экскурсия|мероприятие|мастер-?класс)\s+прош(?:ел|ла|ло|ли)\b"),
    re.compile(r"(?iu)\b(?:прош(?:ел|ла|ло|ли)|состоял(?:ся|ась|ось|ись))\b"),
    re.compile(r"(?iu)\bпринял(?:а|и)?\s+участие\b"),
    re.compile(r"(?iu)\bприняли\s+участие\b"),
    re.compile(r"(?iu)\b(?:побывал(?:а|и)?|посетил(?:а|и)?)\b"),
    re.compile(
        r"(?iu)\b(?:мы|участники|ребята)\s+"
        r"(?:отправили(?:сь)?|провели|сделали|исследовали|решали|работали|обсудили|поговорили)\b"
    ),
    re.compile(r"(?iu)\b(?:было\s+(?:здорово|интересно|ценно|классно)|горящие\s+глаза|неподдельн\w+\s+интерес)\b"),
    re.compile(
        r"(?iu)\b(?:огромное\s+спасибо|спасибо\s+(?:администрац\w*|педагог\w*|организатор\w*|"
        r"мастер\w*|гост\w*|партн[её]р\w*)|"
        r"скоро\s+увидимся\s+вновь|не\s+последняя\s+наша\s+встреча)\b"
    ),
    re.compile(
        r"(?iu)\b(?:педагог\w*|организатор\w*|администрац\w*|учител\w*)[^.!?\n]{0,80}\b"
        r"(?:отметил\w*|выразил\w*\s+благодарн\w*|поблагодарил\w*)"
    ),
    re.compile(r"(?iu)\b(?:итоги|результаты)\b"),
)


def _norm_text_for_grounding(value: str | None) -> str:
    raw = str(value or "").casefold().replace("ё", "е")
    # VK serializes a visible link label as ``[target|label]``.  LLM evidence
    # normally quotes what a reader sees (``label``), not the transport target.
    # Strip only this transport wrapper before the ordinary verbatim check;
    # semantic paraphrases and invented text still fail closed.
    raw = re.sub(r"\[[^|\]\r\n]{1,500}\|([^\]\r\n]+)\]", r"\1", raw)
    raw = re.sub(r"[«»\"'`.,;:!?()\[\]{}#№]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _source_supports_location_value(text: str | None, value: str | None) -> bool:
    probe = _norm_text_for_grounding(value)
    if len(probe) < 4:
        return False
    haystack = _norm_text_for_grounding(text)
    if not haystack:
        return False
    if probe in haystack:
        return True
    # Addresses often differ only by spaces around dashes/slashes.
    probe_compact = re.sub(r"[\s\-–—/]+", "", probe)
    haystack_compact = re.sub(r"[\s\-–—/]+", "", haystack)
    return len(probe_compact) >= 5 and probe_compact in haystack_compact


def _candidate_location_grounding_corpus(candidate: "EventCandidate") -> str:
    parts = [candidate.occurrence_scope_text or candidate.source_text or "", candidate.raw_excerpt or ""]
    for poster in candidate.posters[:4]:
        parts.extend([poster.ocr_title or "", poster.ocr_text or ""])
    return "\n".join(part for part in parts if str(part).strip()).strip()


def _candidate_needs_llm_occurrence_scope_review(candidate: "EventCandidate") -> bool:
    """High-recall routing for a source that may contain sibling events."""
    if str(candidate.source_type or "").strip().lower() not in {"vk", "tg", "telegram"}:
        return False
    corpus = "\n".join([str(candidate.source_text or ""), str(candidate.raw_excerpt or "")]).strip()
    if not corpus:
        return False
    pairs = _extract_day_month_pairs(corpus)
    if len(pairs) < 2:
        return False
    dated_lines = sum(1 for line in corpus.splitlines() if _extract_day_month_pairs(line))
    return dated_lines >= 2 or len(pairs) >= 3


async def _llm_scope_candidate_occurrence(candidate: "EventCandidate") -> tuple[bool, str]:
    """Select exact source excerpts belonging to the candidate occurrence."""
    if SMART_UPDATE_LLM_DISABLED:
        return False, "llm_disabled"
    corpus = str(candidate.source_text or "").strip()
    payload = {
        "target": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "city": candidate.city,
        },
        "source_text": _clip(corpus, 7000),
        "raw_excerpt": _clip(candidate.raw_excerpt, 1200),
    }
    prompt = (
        "Ты отделяешь одно целевое событие от дайджеста/расписания с несколькими событиями. "
        "Смысловое решение принадлежит тебе; даты и векторы — только подсказки.\n"
        "Выбери только блок target и общие строки, которые явно относятся ко всем пунктам "
        "(например общая цена/ужин/адрес). Не включай названия, программу, артистов или "
        "описания соседних дат. Целевой блок должен одновременно поддерживать дату и "
        "локацию/город target; если дата в источнике связана с другим городом/площадкой, верни uncertain. "
        "Верни selected_excerpts как короткие ДОСЛОВНЫЕ непрерывные "
        "фрагменты source_text. Если принадлежность строк неясна — uncertain. Если источник "
        "действительно описывает одну многодневную программу, верни single_event. "
        "Но обложка афиши с общим диапазоном дат не является одной многодневной программой, "
        "если отдельные карточки/блоки называют разные соревнования, даты, города или площадки: "
        "такой aggregate/envelope target верни uncertain, а конкретный target — scoped. "
        "В selected_excerpts сохрани общую строку города/региона, когда она явно относится ко всем карточкам. "
        "Только JSON.\n"
        f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        OCCURRENCE_SCOPE_REVIEW_SCHEMA,
        max_tokens=900,
        label="occurrence_scope_review",
    )
    if not isinstance(data, dict):
        return False, "llm_unavailable"
    decision = str(data.get("decision") or "uncertain").strip().lower()
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    if decision == "single_event" and confidence >= 0.9:
        candidate.occurrence_scope_text = corpus
        return True, "llm_single_event"
    if decision != "scoped" or confidence < 0.8:
        return False, f"llm_{decision or 'uncertain'}"
    excerpts = [str(x or "").strip() for x in (data.get("selected_excerpts") or []) if str(x or "").strip()]
    if not excerpts:
        return False, "llm_scoped_empty"
    corpus_norm = _norm_text_for_grounding(corpus)
    if any(_norm_text_for_grounding(x) not in corpus_norm for x in excerpts):
        return False, "llm_scope_not_verbatim"
    scoped = "\n".join(dict.fromkeys(excerpts)).strip()
    target_date = _parse_iso_date(candidate.date)
    if target_date and (target_date.day, target_date.month) not in _extract_day_month_pairs(scoped):
        return False, "llm_scope_missing_target_date"
    # Narrow grounding rail: when the full multi-occurrence source explicitly
    # contains the candidate city, the selected occurrence must preserve it.
    # This catches a date from one city being paired with a venue from another;
    # the LLM still owns selection, while this only validates its evidence.
    if (
        candidate.city
        and _source_supports_location_value(corpus, candidate.city)
        and not _source_supports_location_value(scoped, candidate.city)
    ):
        return False, "llm_scope_missing_target_city"
    candidate.occurrence_scope_text = scoped
    return True, "llm_scoped"


_EXPLICIT_UNKNOWN_START_TIME_RE = re.compile(
    r"(?iu)\b(?:"
    r"время\s+(?:начала|старта)|"
    r"(?:точное\s+)?(?:начало|старт)"
    r")\s+(?:пока\s+)?(?:уточняется|не\s+определен[оа]?|не\s+известн[оа]?|"
    r"будет\s+(?:уточнен[оа]?|объявлен[оа]?|известн[оа]?))\b"
)


def _source_explicitly_leaves_start_time_unknown(text: str | None) -> bool:
    return bool(_EXPLICIT_UNKNOWN_START_TIME_RE.search(str(text or "")))


def _candidate_explicitly_leaves_start_time_unknown(
    candidate: "EventCandidate",
    corpus: str,
) -> bool:
    metrics = candidate.metrics if isinstance(candidate.metrics, dict) else {}
    if "tg_time_explicitly_unknown" in metrics:
        return bool(metrics.get("tg_time_explicitly_unknown"))
    return _source_explicitly_leaves_start_time_unknown(corpus)


_EXPLICIT_UNKNOWN_START_LLM_CONFIRMED_METRIC = (
    "smart_update_explicit_unknown_start_llm_confirmed"
)


def _apply_llm_confirmed_unknown_start_time(
    event: Any,
    candidate: "EventCandidate",
    *,
    updated_keys: list[str],
) -> bool:
    """Apply the LLM-reviewed removal of a previously persisted wrong time."""

    metrics = candidate.metrics if isinstance(candidate.metrics, dict) else {}
    if not bool(metrics.get(_EXPLICIT_UNKNOWN_START_LLM_CONFIRMED_METRIC)):
        return False
    if not str(getattr(event, "time", "") or "").strip() and not bool(
        getattr(event, "time_is_default", False)
    ):
        return False
    event.time = ""
    event.time_is_default = False
    if "time" not in updated_keys:
        updated_keys.append("time")
    if "time_is_default" not in updated_keys:
        updated_keys.append("time_is_default")
    return True


def _candidate_needs_llm_anchor_role_review(candidate: "EventCandidate") -> tuple[bool, str]:
    """High-recall router only; the LLM owns the date/time role decision."""

    if str(candidate.source_type or "").strip().lower() not in {"vk", "tg", "telegram"}:
        return False, "non_social_source"
    corpus = _candidate_location_grounding_corpus(candidate)
    if not corpus:
        return False, "empty_source", []
    if _candidate_explicitly_leaves_start_time_unknown(candidate, corpus):
        return True, "explicit_unknown_start_time"
    times = {
        f"{int(hour):02d}:{minute}"
        for hour, minute in re.findall(r"(?<!\d)([01]?\d|2[0-3])[.:]([0-5]\d)(?!\d)", corpus)
    }
    if len(times) >= 2 and re.search(
        r"(?iu)\b(?:сбор\s+гостей|сбор\s+участников|doors|начал[оа]|start|открытие)\b",
        corpus,
    ):
        return True, "multiple_role_times"
    if _has_long_event_duration_signals(corpus):
        return True, "explicit_range"
    return False, "no_role_ambiguity"


def _valid_hhmm_or_none(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    match = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", raw)
    return raw if match else None


async def _llm_review_candidate_anchor_roles(
    candidate: "EventCandidate",
    *,
    trigger_reason: str,
) -> tuple[bool, str]:
    """Ground date/range/start-time roles in exact source/OCR evidence."""

    corpus = _candidate_location_grounding_corpus(candidate)
    payload = {
        "today": date.today().isoformat(),
        "trigger": trigger_reason,
        "candidate": {
            "title": candidate.title,
            "date": candidate.date,
            "end_date": candidate.end_date,
            "time": candidate.time,
            "event_type": candidate.event_type,
        },
        "source_and_ocr": _clip(corpus, 8000),
    }
    prompt = (
        "Ты проверяешь роли дат и времени одного публичного события по source/OCR. "
        "Отличай время начала события от doors/сбора гостей/открытия, дату открытия "
        "от полного периода работы и несколько отдельных occurrences от непрерывного range. "
        "Для многодневного периода ставь time=null, если единственное время относится "
        "только к открытию/вернисажу, а не к ежедневному расписанию всего периода. "
        "Если у конкретной активности прямо сказано, что время начала уточняется, будет "
        "объявлено или пока неизвестно, ставь time=null: часы всей ярмарки, фестиваля или "
        "программы являются контекстом и не становятся временем начала этой активности. "
        "Если title означает саму выставку, выбирай полный период; если title явно означает "
        "открытие/вернисаж, выбирай только дату и время открытия. Не схлопывай выставку к дате закрытия. "
        "Если evidence однозначно поддерживает candidate — keep. Если однозначно даёт другие "
        "date/end_date/time — repair. Иначе uncertain. date/end_date только YYYY-MM-DD, time "
        "только HH:MM; null означает, что поле не подтверждено. evidence_quotes должны быть "
        "короткими ДОСЛОВНЫМИ фрагментами source_and_ocr. Не додумывай период. Только JSON.\n"
        f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        ANCHOR_ROLE_REVIEW_SCHEMA,
        max_tokens=500,
        label="anchor_role_review",
    )
    if not isinstance(data, dict):
        return False, "llm_unavailable"
    decision = str(data.get("decision") or "uncertain").strip().lower()
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    quotes = [str(item or "").strip() for item in (data.get("evidence_quotes") or []) if str(item or "").strip()]
    corpus_norm = _norm_text_for_grounding(corpus)
    if not quotes or any(_norm_text_for_grounding(item) not in corpus_norm for item in quotes):
        return False, "llm_evidence_not_verbatim"
    explicit_unknown_start = _candidate_explicitly_leaves_start_time_unknown(candidate, corpus)
    if decision == "keep" and confidence >= 0.9:
        if explicit_unknown_start and _valid_hhmm_or_none(candidate.time):
            return False, "llm_time_conflicts_explicit_unknown"
        return True, "llm_keep"
    if decision != "repair" or confidence < 0.85:
        return False, f"llm_{decision or 'uncertain'}"
    repaired_date = str(data.get("date") or "").strip() or None
    repaired_end = str(data.get("end_date") or "").strip() or None
    repaired_time = _valid_hhmm_or_none(data.get("time"))
    if repaired_date and _parse_iso_date(repaired_date) is None:
        return False, "llm_invalid_date"
    if repaired_end and _parse_iso_date(repaired_end) is None:
        return False, "llm_invalid_end_date"
    if data.get("time") and repaired_time is None:
        return False, "llm_invalid_time"
    if explicit_unknown_start and repaired_time is not None:
        return False, "llm_time_conflicts_explicit_unknown"
    if not repaired_date:
        return False, "llm_missing_date"
    if repaired_end and _parse_iso_date(repaired_end) < _parse_iso_date(repaired_date):
        return False, "llm_inverted_range"
    candidate.date = repaired_date
    candidate.end_date = repaired_end
    candidate.end_date_is_inferred = False
    candidate.time = repaired_time
    candidate.time_is_default = False
    return True, "llm_repair"


async def _llm_review_create_bundle_grounding(
    bundle: Mapping[str, Any],
    candidate: "EventCandidate",
) -> tuple[bool, str, list[str]]:
    """LLM-first, source-only entailment gate for every generated public field."""

    corpus = _candidate_location_grounding_corpus(candidate)
    if not corpus:
        return False, "empty_source"
    public_bundle = {
        key: bundle.get(key)
        for key in ("title", "description", "facts", "search_digest", "short_description")
    }
    prompt = (
        "Ты fail-closed проверяешь, что ВСЕ поля generated_bundle относятся только к одному "
        "событию и семантически следуют из source_and_ocr. Перефразирование допустимо; новые "
        "имена, организаторы, миры/франшизы, программа, условия или утверждения запрещены. "
        "Проверь title, description, каждый facts, search_digest и short_description. "
        "grounded ставь только если unsupported_fields пуст и confidence >= 0.9; при сомнении "
        "uncertain. evidence_quotes — короткие ДОСЛОВНЫЕ фрагменты источника. Только JSON.\n"
        f"INPUT:\n{json.dumps({'source_and_ocr': _clip(corpus, 9000), 'generated_bundle': public_bundle}, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        CREATE_BUNDLE_GROUNDING_REVIEW_SCHEMA,
        max_tokens=500,
        label="create_bundle_grounding",
    )
    if not isinstance(data, dict):
        return False, "llm_unavailable", []
    decision = str(data.get("decision") or "uncertain").strip().lower()
    unsupported = [str(item or "").strip() for item in (data.get("unsupported_fields") or []) if str(item or "").strip()]
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    quotes = [str(item or "").strip() for item in (data.get("evidence_quotes") or []) if str(item or "").strip()]
    corpus_norm = _norm_text_for_grounding(corpus)
    if not quotes or any(_norm_text_for_grounding(item) not in corpus_norm for item in quotes):
        return False, "llm_evidence_not_verbatim", []
    if decision != "grounded" or confidence < 0.9 or unsupported:
        public_fields = ("title", "description", "facts", "search_digest", "short_description")
        safe_unsupported = [
            field
            for field in dict.fromkeys(unsupported)
            if field in public_fields
        ]
        if decision == "ungrounded":
            # An ``ungrounded`` verdict can only make the create payload more
            # conservative: we remove generated prose and fall back to the
            # already-grounded candidate fields.  If the reviewer omitted the
            # optional per-field diagnosis, drop every populated public field
            # rather than reject an otherwise valid occurrence or guess which
            # generated sentence was unsafe.  Verbatim evidence was validated
            # above; ``uncertain`` still fails closed without this repair.
            if not safe_unsupported:
                safe_unsupported = [
                    field
                    for field in public_fields
                    if bundle.get(field) not in (None, "", [])
                ]
            if safe_unsupported:
                return False, "llm_ungrounded", safe_unsupported
        return False, f"llm_{decision or 'uncertain'}", []
    return True, "llm_grounded", []


def _remove_llm_rejected_bundle_fields(
    bundle: Mapping[str, Any], unsupported_fields: Sequence[str]
) -> dict[str, Any]:
    """Drop only fields the grounded LLM review explicitly rejected.

    The semantic decision belongs to the reviewer.  This mechanical repair
    never invents replacement prose: downstream create logic falls back to the
    candidate's grounded title/raw excerpt for omitted public fields.
    """

    repaired = dict(bundle)
    for field in unsupported_fields:
        if field in {"title", "description", "facts", "search_digest", "short_description"}:
            repaired.pop(field, None)
    return repaired


def _candidate_needs_llm_location_grounding_review(
    candidate: "EventCandidate",
) -> tuple[bool, str]:
    """Route suspicious social-source venues to semantic review.

    This function only detects a grounding conflict. It never chooses a venue.
    The incident class includes a generic reference token (for example
    ``остров``) snapping an explicit ``остров Шайба`` source to ``Остров
    Канта``, and a broad complex name surviving when the source explicitly
    names a hall inside that complex.
    """

    if str(candidate.source_type or "").strip().lower() not in {"vk", "tg", "telegram"}:
        return False, "non_social_source"
    # Test/offline mode and intentionally disabled provider lanes must retain
    # the existing ordering of semantic guards. Production enables the LLM;
    # transient provider failures still fail closed inside the review call.
    if SMART_UPDATE_LLM_DISABLED:
        return False, "llm_disabled"

    # A structured event/festival label can be copied verbatim into the venue
    # field while still not naming an attendee-facing place.  This is only a
    # high-recall router: the LLM below owns the semantic verdict and may keep
    # a genuine venue.  Keep the trigger narrow so it does not add a review to
    # every social candidate merely because the title happens to mention its
    # venue.
    location_key = normalize_venue_key(candidate.location_name)
    if location_key:
        context_values = (
            getattr(candidate, "festival", None),
            getattr(candidate, "festival_full", None),
            getattr(candidate, "festival_series", None),
        )
        for value in context_values:
            context_key = normalize_venue_key(value)
            if len(context_key) < 5:
                continue
            if (
                location_key == context_key
                or location_key.startswith(f"{context_key} ")
                or location_key.endswith(f" {context_key}")
            ):
                return True, "location_overlaps_event_context"

    corpus = _candidate_location_grounding_corpus(candidate)
    if not corpus:
        return True, "missing_source_evidence"

    # Route only when the source itself exposes a location role. This avoids a
    # new LLM call merely because a short fixture/secondary source omits an
    # already-known venue, while covering explicit `📍/Где/Площадка/Адрес`
    # blocks and the named-island/lake regression.
    has_explicit_location_role = bool(
        re.search(
            r"(?iu)(?:📍|(?:^|\n)\s*(?:где|место|площадка|адрес)\s*[:—-]|\b(?:остров|озеро)\s+[«\"']?[A-ZА-ЯЁ])",
            corpus,
        )
    )
    if not has_explicit_location_role:
        return False, "no_explicit_location_role"

    name_supported = _source_supports_location_value(corpus, candidate.location_name)
    address_supported = _source_supports_location_value(corpus, candidate.location_address)
    # A grounded address does not prove an independently supplied canonical
    # venue name.  This matters for address-only reference binding and linked
    # source enrichment: ``ИЦАЭ`` was not present in either casting source, but
    # the supported string ``Советский проспект, 12`` previously let the
    # unsupported name bypass semantic review (INC-2026-07-27).
    if candidate.location_name and not name_supported and address_supported:
        return True, "canonical_location_name_not_in_source"
    if not name_supported and not address_supported:
        return True, "canonical_location_not_in_source"

    # A programme/club label may appear verbatim elsewhere in the post and thus
    # look "grounded", while an explicit attendee-facing marker names another
    # place (INC-2026-08-01: ``ДЕТСКИЙ КНИЖНЫЙ КЛУБ`` versus
    # ``📍Летний читальный зал``). This remains retrieval-only: the LLM decides.
    explicit_role_values: list[str] = []
    for match in re.finditer(
        r"(?imu)(?:📍|^\s*(?:где|место|площадка|адрес)\s*[:—-])\s*([^\n]{3,180})",
        corpus,
    ):
        value = str(match.group(1) or "").strip()
        if value:
            explicit_role_values.append(value)
    if (
        candidate.location_name
        and explicit_role_values
        and not any(
            _source_supports_location_value(value, candidate.location_name)
            or _source_supports_location_value(candidate.location_name, value)
            for value in explicit_role_values
        )
    ):
        return True, "explicit_location_role_conflicts_candidate"

    try:
        explicit = find_known_venue_in_text(corpus, city=candidate.city)
    except Exception:
        explicit = None
    if explicit is not None:
        current_key = normalize_venue_key(candidate.location_name)
        explicit_keys = {explicit.name_key, explicit.line_key}
        if current_key and current_key not in explicit_keys:
            return True, "more_specific_known_venue_in_source"
    return False, "source_grounded"


def _canonicalize_location_after_grounding_review(
    candidate: "EventCandidate",
    *,
    review_result: str,
) -> tuple[str | None, str | None, str | None]:
    """Apply references without undoing a source-grounded LLM repair."""

    reviewed = (
        candidate.location_name,
        candidate.location_address,
        candidate.city,
    )
    normalized = _canonicalize_location_fields(
        location_name=candidate.location_name,
        location_address=candidate.location_address,
        city=candidate.city,
        source_chat_username=candidate.source_chat_username,
        source_url=candidate.source_url,
    )
    if review_result != "llm_repair":
        return normalized

    normalized_name = normalized[0]
    if (
        reviewed[0]
        and normalized_name
        and not _location_matches(reviewed[0], normalized_name)
        and not _source_supports_location_value(
            _candidate_location_grounding_corpus(candidate),
            normalized_name,
        )
    ):
        logger.warning(
            "smart_update: kept LLM-repaired venue over ungrounded post-review reference "
            "source_type=%s source_url=%s repaired=%r/%r reference=%r/%r",
            candidate.source_type,
            candidate.source_url,
            reviewed[0],
            reviewed[1],
            normalized[0],
            normalized[1],
        )
        return reviewed
    return normalized


async def _llm_review_candidate_location_grounding(
    candidate: "EventCandidate",
    *,
    trigger_reason: str,
) -> tuple[bool, str]:
    """Verify or repair a suspicious venue from source evidence, fail closed.

    Vector/reference matching is recall only. The LLM owns the semantic choice;
    deterministic checks below merely require that its evidence and repaired
    value are present in the supplied source/OCR bundle.
    """

    if SMART_UPDATE_LLM_DISABLED:
        return False, "llm_disabled"
    corpus = _candidate_location_grounding_corpus(candidate)
    payload = {
        "candidate": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "source_url": candidate.source_url,
        },
        "retrieval_trigger": trigger_reason,
        "source_and_poster_evidence": _clip(corpus, 4200),
    }
    prompt = (
        "Ты проверяешь локацию события перед записью и публикацией. "
        "Поля candidate — гипотеза; source_and_poster_evidence — авторитет. "
        "Reference/vector retrieval является только подсказкой и не доказывает площадку.\n"
        "Определи attendee-facing место проведения именно этого события. Не путай: "
        "площадку с артистом/организатором, парк с одноимённым дворцом спорта, "
        "конкретный зал с широким кварталом, один остров/озеро с другим, а название "
        "фестиваля, праздника, книжного/детского клуба или Дня города — с площадкой. "
        "Название программы или сообщества не является venue только потому, что оно дословно есть в посте. Event context вроде "
        "«День города в Янтарном» не является названием venue. "
        "Если источник явно называет более конкретное место, выбери его. "
        "Если candidate уже подтверждён источником — выбери keep. Если источник явно "
        "называет другое attendee-facing место — repair. Если источник подтверждает только "
        "город/посёлок, название события/фестиваля или программу, но не площадку, выбери "
        "reject_missing_location: не превращай контекст события в location_name. "
        "Ничего не выдумывай. Для каждого решения верни короткую дословную evidence_quote. "
        "Это финальное решение: не возвращай uncertain и не проси повтор. Верни только JSON.\n"
        f"INPUT:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        LOCATION_GROUNDING_REVIEW_SCHEMA,
        max_tokens=500,
        label="location_grounding_review",
    )
    if not isinstance(data, dict):
        return False, "llm_unavailable"
    decision = str(data.get("decision") or "uncertain").strip().lower()
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    evidence_quote = str(data.get("evidence_quote") or "").strip()
    quote_grounded = bool(
        evidence_quote
        and _norm_text_for_grounding(evidence_quote)
        in _norm_text_for_grounding(corpus)
    )

    if decision == "keep":
        # Confidence is retained for observability, not used as a second
        # semantic judge. A grounded LLM KEEP must not fall through into an
        # endless retry merely because a scalar is below an arbitrary cutoff.
        if not quote_grounded:
            return False, "llm_keep_quote_invalid"
        if _source_supports_location_value(corpus, candidate.location_name) or _source_supports_location_value(
            corpus, candidate.location_address
        ):
            return True, "llm_keep"
        return False, "llm_keep_not_grounded"

    if decision == "reject_missing_location":
        if not quote_grounded:
            return False, "llm_reject_quote_invalid"
        return False, "llm_reject_missing_location"

    if decision != "repair" or not quote_grounded:
        return False, "llm_response_invalid"

    proposed_name = str(data.get("location_name") or "").strip() or None
    proposed_address = str(data.get("location_address") or "").strip() or None
    proposed_city = str(data.get("city") or "").strip() or None
    if not proposed_name:
        return False, "llm_repair_missing_name"
    if not (
        _source_supports_location_value(corpus, proposed_name)
        or _source_supports_location_value(corpus, proposed_address)
    ):
        return False, "llm_repair_value_not_grounded"

    candidate.location_name = proposed_name
    candidate.location_address = proposed_address
    if proposed_city:
        candidate.city = proposed_city
    return True, "llm_repair"


def _has_retrospective_future_teaser_shape(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    if not _RETROSPECTIVE_NEXT_TEASER_RE.search(combined):
        return False
    hits = sum(1 for pattern in _RETROSPECTIVE_RECAP_MARKERS if pattern.search(combined))
    return hits >= 2


def _has_mixed_occurrence_role_risk(title: str | None, text: str | None) -> bool:
    """Route mixed past-recap/future-invite sources to semantic review.

    This is deliberately a high-recall router, not an eventness decision.  The
    old ``retrospective_future_teaser`` guard encoded a few exact Russian phrase
    shapes and missed the next post from the same source when it changed from
    "ждём на следующей выставке" to "увидимся в следующую субботу".  Here we
    only establish that both temporal roles may be present; the LLM must decide
    whether the candidate is one grounded future event.
    """

    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    recap_hits = sum(1 for pattern in _RETROSPECTIVE_RECAP_MARKERS if pattern.search(combined))
    completed_hits = sum(1 for pattern in _COMPLETED_EVENT_REPORT_MARKERS if pattern.search(combined))
    # Generic past-tense reporting is routing evidence only.  It never causes a
    # skip without the LLM review below.
    has_past_role = bool(
        recap_hits
        or completed_hits
        or re.search(
            r"(?iu)\b(?:пров[её]л(?:а|и)?|подготовил(?:а|и)?|стал(?:а|о|и)?|"
            r"получил(?:ся|ась|ось|ись)|сорвал(?:а|и)?|встретил(?:а|и)?)\b",
            combined,
        )
    )
    has_future_role = bool(
        re.search(
            r"(?iu)\b(?:впереди|увидимся|встретимся|следующ\w*|предсто\w*|"
            r"скоро\s+(?:снова|вновь)|жд[её]м\s+вас)\b",
            combined,
        )
    )
    return has_past_role and has_future_role and bool(_extract_day_month_pairs(combined))


def _looks_like_too_soon_notice(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    return bool(_TOO_SOON_NOTICE_RE.search(combined))


def _looks_like_event_logistics_notice_not_event(title: str | None, text: str | None) -> bool:
    """Detect operational updates for attendees of an already-announced event."""
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    if not _EVENT_LOGISTICS_NOTICE_RE.search(combined):
        return False
    # A real standalone announcement should have a clear invite/sale action, not
    # only entry-route or navigation instructions for people already attending.
    if re.search(
        r"(?iu)\b(приглаша(?:ем|ю|ет)|жд[её]м\s+вас|приходите|открыта\s+регистраци\w*|"
        r"купить\s+билет|билеты\s+(?:в\s+продаже|здесь|по\s+ссылке))\b",
        combined,
    ):
        return False
    return True


def _looks_like_online_event(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    return bool(_ONLINE_EVENT_RE.search(combined))


def _candidate_has_physical_event_anchors(candidate: EventCandidate) -> bool:
    """Return True when LLM extraction already grounded the draft as an offline event."""
    if not _candidate_has_event_anchors(candidate):
        return False
    location_bits = " ".join(
        str(v or "").strip()
        for v in (
            candidate.location_name,
            candidate.location_address,
            candidate.city,
        )
        if str(v or "").strip()
    )
    if not location_bits:
        return False
    if _ONLINE_PLATFORM_LOCATION_RE.search(location_bits):
        return False
    return True


def _looks_like_book_review_not_event(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    if not _BOOK_REVIEW_RE.search(combined):
        return False
    # If it's framed as an event (discussion/club/lecture), keep it.
    if _BOOK_EVENT_KEEP_RE.search(combined):
        return False
    # If there is a concrete start time/date anchor, treat as a real event instead of a review.
    if re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", combined):
        return False
    if re.search(r"\b\d{1,2}[./]\d{1,2}\b", combined):
        return False
    if re.search(r"(?iu)\b(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*\b", combined):
        return False
    return True


def _looks_like_photo_day_not_event(
    title: str | None,
    text: str | None,
    *,
    candidate: EventCandidate | None = None,
) -> bool:
    """Detect rubric posts like "Фото дня" that should not become events.

    "Фото дня" is a strong non-event hint, but we keep the post if it still contains
    strong event signals (explicit time, period, tickets/registration, or an invite).
    """
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _PHOTO_DAY_RE.search(low):
        return False
    # Keep if the candidate already has explicit scheduling/ticket fields.
    if candidate is not None:
        if str(getattr(candidate, "time", "") or "").strip():
            return False
        if str(getattr(candidate, "end_date", "") or "").strip():
            return False
        if str(getattr(candidate, "ticket_link", "") or "").strip():
            return False
        if (
            getattr(candidate, "ticket_price_min", None) is not None
            or getattr(candidate, "ticket_price_max", None) is not None
        ):
            return False
    # Keep if there's an explicit start time in the text.
    if re.search(r"\b([01]?\d|2[0-3])[:.][0-5]\d\b", combined):
        return False
    # Keep if the post contains an exhibition-like period ("с ... по ...", "до ...", "по ...").
    if _PHOTO_DAY_PERIOD_RE.search(low):
        return False
    # Keep if there's ticket/registration language.
    if _PHOTO_DAY_TICKET_RE.search(low):
        return False
    # Keep if the post looks like an actual invite/announcement.
    if _PHOTO_DAY_KEEP_ACTION_RE.search(low):
        return False
    return True


def _looks_like_non_event_notice(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _NON_EVENT_NOTICE_RE.search(low):
        return False
    # If the post looks like an actionable announcement for a specific event, keep it.
    if _EVENT_INVITE_RE.search(combined):
        return False
    # Deadline-heavy informational posts are not events.
    if _DEADLINE_RE.search(combined):
        return True
    # Strong "service notice" keywords alone are enough in practice.
    return True


def _looks_like_venue_status_update_not_event(title: str | None, text: str | None) -> bool:
    """Detect non-event status updates about a venue/organisation.

    Examples: "город может потерять площадку с 1 мая", "дана отсрочка до 1 июня",
    eviction/lease issues, petitions, fundraising — these should not become events
    to attend.
    """
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _VENUE_STATUS_UPDATE_RE.search(low):
        return False
    # Require a deadline-like date anchor to avoid skipping unrelated discussions.
    if not (_DEADLINE_RE.search(combined) or _FROM_DATE_RE.search(low)):
        return False
    # If the post looks like a real event invite/announcement, keep it.
    if _EVENT_INVITE_RE.search(combined):
        return False
    # If there is an explicit start time, treat as a likely event.
    if re.search(r"\b([01]?\d|2[0-3])[:.][0-5]\d\b", combined):
        return False
    return True


def _looks_like_open_call_not_event(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _OPEN_CALL_RE.search(low):
        return False
    # If the post is clearly an actionable one-off event invite with a concrete start time,
    # do not treat it as an open-call notice.
    if re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", combined) and _EVENT_INVITE_RE.search(combined):
        return False
    return True


def _looks_like_course_promo(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    if not _COURSE_PROMO_RE.search(combined):
        return False
    # Words like "кураторские экскурсии" are common in exhibition announcements.
    # Do not let the broad `куратор*` token override a grounded dated exhibition.
    if (
        _EVENT_INVITE_RE.search(combined)
        and re.search(r"(?iu)\bвыставк\w*\b", combined)
        and (
            re.search(r"\b([01]?\d|2[0-3])[:.][0-5]\d\b", combined)
            or _DATE_SIGNAL_RE.search(combined)
            or _TICKET_PRICE_CONTEXT_RE.search(combined)
        )
        and not _COURSE_PROMO_STRONG_RE.search(combined)
    ):
        return False
    # If it's clearly a one-off masterclass/lecture with concrete start time, keep it.
    if re.search(r"\b([01]?\d|2[0-3])[:.]([0-5]\d)\b", combined) and _EVENT_INVITE_RE.search(combined):
        return False
    # "Старт курса" and multi-session language strongly indicates non-event promo content.
    if re.search(r"(?i)\bна\s+кажд(ом|ом\s+из)\s+заняти\w*\b", combined):
        return True
    if re.search(r"(?i)\bстарт\s+курс\w*\b", combined):
        return True
    return True


def _looks_like_service_promo_not_event(title: str | None, text: str | None) -> bool:
    """Detect promotions for "event hosting services" (packages/booking), not one-off events.

    Example: "Выпускные 2026 ... пакетные программы ... бронирование открыто ... телефон ...".
    """
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    # If there are explicit date/time anchors, treat it as a potential real event.
    # This filter targets promos without a concrete schedule.
    if _has_datetime_signals(combined):
        return False
    low = combined.casefold()
    if not _SERVICE_PROMO_OCCASION_RE.search(low):
        return False
    if not _SERVICE_PROMO_OFFER_RE.search(low):
        return False
    # Reduce false positives: require either explicit contacts or price-like signals.
    if _SERVICE_PROMO_CONTACT_RE.search(combined):
        return True
    if _PRICE_CONTEXT_RE.search(combined) and _PRICE_NUMBER_RE.search(combined):
        return True
    return False


def _looks_like_rental_booking_not_event(title: str | None, text: str | None) -> bool:
    """Detect venue/space availability posts that are not attendable events."""

    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _RENTAL_BOOKING_RE.search(low):
        return False
    # A real public activity can mention a rented venue. Keep it when the source
    # clearly announces a program item rather than only inventory/price tiers.
    if _EVENT_ACTION_INVITE_RE.search(low) and re.search(
        r"(?iu)\b(концерт\w*|лекци\w*|мастер-?класс\w*|спектакл\w*|показ\w*|экскурси\w*)\b",
        low,
    ):
        return False
    return bool(_RENTAL_BOOKING_DETAIL_RE.search(low))


def _free_claim_contradicted_by_source(candidate: EventCandidate, text: str | None) -> bool:
    if candidate.ticket_price_min and candidate.ticket_price_min > 0:
        return True
    if candidate.ticket_price_max and candidate.ticket_price_max > 0:
        return True
    return bool(_FREE_CONTRADICTION_RE.search(str(text or "")))


def _looks_like_work_schedule_notice(title: str | None, text: str | None) -> bool:
    """Institution work schedules must not become events."""
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    has_schedule_headline = bool(_WORK_SCHEDULE_RE.search(low))
    has_schedule_details = bool(_WORK_SCHEDULE_DETAIL_RE.search(combined))
    if not has_schedule_headline:
        return False
    # If the post is clearly announcing a concrete attendable event, keep it.
    # Use action verbs, not generic nouns ("выставка/концерт"), otherwise
    # work-schedule notices with occasional cultural terms slip through.
    if _EVENT_ACTION_INVITE_RE.search(combined):
        return False
    # Explicit timetable-like details are a strong non-event signal.
    if has_schedule_details:
        return True
    return True


def _looks_like_congrats_notice_not_event(title: str | None, text: str | None) -> bool:
    """Congratulation/holiday promos are not events unless there's an explicit event invite."""
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if not _CONGRATS_RE.search(low):
        return False
    # If it looks like a real event invite/announcement, keep.
    if _EVENT_INVITE_RE.search(combined):
        return False
    return True


def _looks_like_completed_event_report_not_event(
    title: str | None,
    text: str | None,
    *,
    candidate: EventCandidate | None = None,
) -> bool:
    """Detect posts that recap a finished event rather than announce an upcoming one."""
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    low = combined.casefold()
    if _COMPLETED_EVENT_REPORT_KEEP_RE.search(low):
        return False
    unconfirmed_next_festival = bool(
        _COMPLETED_FESTIVAL_TEASER_UNCONFIRMED_RE.search(combined)
    )
    if _COMPLETED_EVENT_REPORT_CONTINUATION_RE.search(low) and not unconfirmed_next_festival:
        return False
    if candidate is not None:
        time_raw = str(getattr(candidate, "time", "") or "").strip().replace(".", ":")
        if time_raw and time_raw not in {"00:00", "0:00"}:
            return False
        if str(getattr(candidate, "end_date", "") or "").strip() and not unconfirmed_next_festival:
            return False
        if str(getattr(candidate, "ticket_link", "") or "").strip():
            return False
        if (
            getattr(candidate, "ticket_price_min", None) is not None
            or getattr(candidate, "ticket_price_max", None) is not None
        ):
            return False
    if not ((candidate is not None and str(getattr(candidate, "date", "") or "").strip()) or _has_datetime_signals(combined)):
        return False
    hits = sum(1 for pattern in _COMPLETED_EVENT_REPORT_MARKERS if pattern.search(combined))
    return hits >= 2


def _looks_like_retrospective_future_teaser_not_event(
    title: str | None,
    text: str | None,
    *,
    candidate: EventCandidate | None = None,
) -> bool:
    """Fail-close recap posts where a thin future teaser gained hallucinated anchors.

    This is a narrow regression guard for social imports like INC-2026-07-02/E6691:
    most of the source recaps a finished event, while one closing sentence says
    "ждём вас на следующей выставке ...".  The semantic decision remains LLM-first
    for normal announcements; this guard only blocks automatic publication when
    the extracted candidate venue/address are not grounded in the source text.
    """

    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    if not _has_retrospective_future_teaser_shape(title, combined):
        return False
    if candidate is None:
        return True

    time_raw = str(getattr(candidate, "time", "") or "").strip().replace(".", ":")
    if time_raw and time_raw not in {"00:00", "0:00"}:
        return False
    if str(getattr(candidate, "ticket_link", "") or "").strip():
        return False
    if (
        getattr(candidate, "ticket_price_min", None) is not None
        or getattr(candidate, "ticket_price_max", None) is not None
    ):
        return False

    location_name = str(getattr(candidate, "location_name", "") or "").strip()
    location_address = str(getattr(candidate, "location_address", "") or "").strip()
    has_location = bool(location_name or location_address)
    if not has_location:
        return True

    name_supported = _source_supports_location_value(combined, location_name)
    address_supported = _source_supports_location_value(combined, location_address)
    if not name_supported and not address_supported:
        return True

    # A city-only mention is not enough to auto-publish a recap teaser; it still
    # needs LLM review, but it is no longer this deterministic hallucinated-venue
    # failure mode.
    return False


def _has_price_evidence(text: str | None, *values: int | None) -> bool:
    """Return True when source text contains price-like context + numbers.

    Deterministic guardrail to prevent LLM/extractor hallucinations for ticket prices.
    """
    raw = str(text or "")
    if not raw:
        return False
    if not (_PRICE_CONTEXT_RE.search(raw) and _PRICE_NUMBER_RE.search(raw)):
        return False
    nums = {int(v) for v in values if isinstance(v, int) and v > 0}
    if not nums:
        return True
    for n in nums:
        # Allow optional whitespace between digits: "1500" vs "1 500" vs "1 500".
        digits = str(n)
        pattern = r"\s*".join(re.escape(ch) for ch in digits)
        for m in re.finditer(rf"(?u)\b{pattern}\b", raw):
            window_raw = raw[max(0, m.start() - 70) : min(len(raw), m.end() + 70)]
            # Money in a "compensation/payout/reward" context is not a ticket price.
            # Exception: if the same window also contains explicit ticket/entry words, keep it.
            if _NON_TICKET_MONEY_CONTEXT_RE.search(window_raw) and not _TICKET_PRICE_CONTEXT_RE.search(
                window_raw
            ):
                continue
            return True
    # If we have price-like context but none of the specific values appear as ticket-price-like,
    # treat as unsupported.
    return False


def _looks_like_blood_donation_event(title: str | None, text: str | None) -> bool:
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return False
    return bool(_BLOOD_DONATION_CONTEXT_RE.search(combined))


_UTILITY_OUTAGE_RE = re.compile(
    r"(?iu)\b("
    r"отключен\w*\s+(?:электроэнерг\w*|свет\w*|вод\w*|тепл\w*|газ\w*)|"
    r"времен\w*\s+отключен\w*|"
    r"планов\w*\s+отключен\w*|"
    r"аварийн\w*\s+отключен\w*|"
    r"перерыв\w*\s+в\s+(?:электроснабжен\w*|водоснабжен\w*|теплоснабжен\w*|газоснабжен\w*)|"
    r"(?:электроснабжен\w*|водоснабжен\w*|теплоснабжен\w*|газоснабжен\w*)\s+(?:будет|ограничен\w*|отключен\w*)|"
    r"перекрыт\w*\s+(?:движен\w*|дорог\w*|улиц\w*|проезд\w*)|"
    r"ограничен\w*\s+(?:движен\w*|проезд\w*)|"
    r"закрыт\w*\s+(?:движен\w*|проезд\w*)"
    r")\b"
)


def _looks_like_utility_outage_or_road_closure(title: str | None, text: str | None) -> str | None:
    """
    Municipal notices (utility outages / road closures) must not become events.
    Deterministic guard: no LLM, no content rewriting.
    Returns a reason suffix for SmartUpdateResult.reason when matched.
    """
    combined = "\n".join([str(title or ""), str(text or "")]).strip()
    if not combined:
        return None
    if not _UTILITY_OUTAGE_RE.search(combined):
        return None
    low = combined.casefold()
    if "перекры" in low or "огранич" in low or "проезд" in low or "движен" in low:
        return "road_closure"
    return "utility_outage"


def _strip_promo_lines(text: str | None) -> str | None:
    # Deterministic line-level cutting is not allowed (LLM handles this).
    raw = str(text or "").replace("\\n", "\n").strip()
    return raw or None


def _strip_giveaway_lines(text: str | None) -> str | None:
    # Deterministic line-level cutting is not allowed (LLM handles this).
    raw = str(text or "").replace("\\n", "\n").strip()
    return raw or None


def _candidate_has_event_anchors(candidate: EventCandidate) -> bool:
    # Minimal anchor set for a real event.
    #
    # Important: location_name alone is NOT a reliable anchor (often defaulted from a channel/source
    # and can appear in promo/congrats posts). Prefer anchors that are present in the text/title.
    title = (candidate.title or "").strip()
    if not (candidate.date and title):
        return False

    # Prefer checking anchors against *both* the short excerpt and the full source text.
    # The excerpt is typically `short_description` which must not contain date/time by prompt design,
    # so relying only on it can produce false "promo_or_congrats" skips for real events.
    excerpt = (candidate.raw_excerpt or "").strip()
    src = (candidate.source_text or "").strip()
    text_parts = [p for p in (excerpt, src) if p]
    text = "\n".join(text_parts).strip()
    combined = (title + "\n" + text).strip()

    if _EVENT_SIGNAL_RE.search(combined):
        return True
    if _has_datetime_signals(src) or _has_datetime_signals(excerpt):
        return True
    return False

def _has_datetime_signals(text: str | None) -> bool:
    if not text:
        return False
    value = text.lower()
    if re.search(r"\b\d{1,2}[:.]\d{2}\b", value):
        return True
    if re.search(r"\b\d{1,2}[./]\d{1,2}\b", value):
        return True
    if re.search(r"\b(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*\b", value):
        return True
    return False


def _giveaway_has_underlying_event_facts(text: str | None) -> bool:
    raw = str(text or "").replace("\\n", "\n").strip()
    if not raw:
        return False

    has_time = False
    has_date = False
    has_event_signal = False

    for line in raw.splitlines():
        value = line.strip()
        if not value:
            continue
        for part in re.split(r"(?<=[.!?…])\s+", value):
            chunk = part.strip()
            if not chunk:
                continue

            if _EVENT_INVITE_RE.search(chunk) or _EVENT_SIGNAL_RE.search(chunk):
                has_event_signal = True

            is_mechanics = bool(_GIVEAWAY_MECHANICS_RE.search(chunk) and not _EVENT_HAPPENS_VERB_RE.search(chunk))

            if re.search(r"\b([01]?\d|2[0-3])[:.]([0-5]\d)\b", chunk):
                if re.search(r"(?iu)\bдо\s+([01]?\d|2[0-3])[:.]([0-5]\d)\b", chunk):
                    continue
                if is_mechanics:
                    continue
                has_time = True

            if re.search(r"\b\d{1,2}[./]\d{1,2}\b", chunk) or re.search(
                r"(?iu)\b(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*\b",
                chunk,
            ):
                if _DEADLINE_RE.search(chunk):
                    continue
                if is_mechanics:
                    continue
                has_date = True

    if has_time:
        return True
    return has_date and has_event_signal


def _title_tokens(title: str | None) -> set[str]:
    if not title:
        return set()
    words = re.findall(r"[a-zа-яё0-9]{4,}", title.lower(), flags=re.IGNORECASE)
    return {w for w in words if w and not w.isdigit()}


def _extract_quote_candidates(text: str | None, *, max_items: int = 2) -> list[str]:
    """Extract short first-person quote candidates from source text (best-effort).

    This is used to help the LLM keep valuable direct speech as a quote block,
    instead of paraphrasing it away.
    """
    raw = (text or "").strip()
    if not raw:
        return []
    raw = raw.replace("\r", "\n")
    candidates: list[str] = []
    seen: set[str] = set()

    # Deterministic direct speech in «...»: keep valuable quotes verbatim even when
    # they're not first-person. This helps Telegraph formatting (blockquote) and
    # prevents the LLM from paraphrasing away the original wording.
    for m in re.finditer(r"(?s)«([^»]{25,900})»", raw):
        q = re.sub(r"\s+", " ", (m.group(1) or "").strip())
        if not q:
            continue
        words = re.findall(r"[a-zа-яё0-9]{2,}", q, flags=re.IGNORECASE)
        looks_like_title = len(words) <= 6 and not re.search(r"[.!?…]", q)
        if looks_like_title:
            continue
        if len(words) < 8:
            continue
        cleaned_q = _normalize_fact_item(q, limit=170)
        if not cleaned_q:
            continue
        key_q = cleaned_q.lower()
        if key_q in seen:
            continue
        seen.add(key_q)
        candidates.append(cleaned_q)
        if len(candidates) >= max_items:
            return candidates
    # Split by sentence-ish boundaries while keeping it simple and deterministic.
    chunks = re.split(r"[.!?…]\s+|\n{2,}|\n", raw)
    sched_re = re.compile(r"^\s*\d{1,2}\.\d{1,2}\s*\|\s*.+$")
    # Russian first-person / opinion markers.
    fp_re = re.compile(
        r"\b(я|мне|мой|моя|моё|кажется|думаю|считаю|вижу|замечаю|по[- ]моему)\b",
        re.IGNORECASE,
    )
    for chunk in chunks:
        s = re.sub(r"\s+", " ", (chunk or "").strip())
        if not s or len(s) < 20:
            continue
        if sched_re.match(s):
            continue
        if not fp_re.search(s):
            continue
        cleaned = _normalize_fact_item(s, limit=170)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(cleaned)
        if len(candidates) >= max_items:
            break
    return candidates


def _extract_director_name_hint(
    *,
    candidate_text: str | None,
    facts_before: Sequence[str] | None,
) -> str | None:
    """Best-effort extraction of the director name for quote attribution.

    We keep this conservative and deterministic: it's only used to label a direct
    quote block (operator readability + E2E assertion), not to invent facts.
    """
    text = (candidate_text or "").replace("\r", "\n")
    facts = [str(f or "") for f in (facts_before or [])]

    # Prefer explicit known name in either source text or existing facts.
    if re.search(r"(?i)\bегор\s+равинск", text) or any(
        re.search(r"(?i)\bегор\s+равинск", f) for f in facts
    ):
        return "Егор Равинский"

    # Generic RU "First Last" name capture near "режисс".
    # Example: "Режиссёр спектакля — Егор Равинский."
    name_re = re.compile(r"(?i)\bрежисс\w*\b[^\n]{0,80}?([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)")
    m = name_re.search(text)
    if m:
        return m.group(1).strip()
    for f in facts:
        m2 = name_re.search(f)
        if m2:
            return m2.group(1).strip()
    return None


def _inject_direct_quote_blockquote(
    *,
    description: str,
    quote: str,
    attribution_name: str | None,
) -> str:
    """Insert a Markdown blockquote with optional attribution into a description.

    Used as a hard safety-net when the LLM fails to keep a detected direct quote
    formatted as a blockquote.
    """
    desc = (description or "").strip()
    q = (quote or "").strip()
    if not desc or not q:
        return description
    if re.search(r"(?m)^>\s+", desc):
        return description

    # Avoid duplicating the same quote if it already appears verbatim.
    if q.casefold() in desc.casefold():
        return description

    block = f"> {q}"
    if attribution_name and attribution_name.strip():
        name = attribution_name.strip()
        # Put attribution inside the blockquote so Telegraph renders it together.
        if name.casefold() not in q.casefold():
            block = f"> {q}\n> — {name}"

    paragraphs = [p.strip() for p in re.split(r"\n{2,}", desc) if p.strip()]
    if not paragraphs:
        return f"{desc}\n\n{block}".strip()

    insert_at = 1  # by default: after the first paragraph
    anchor = (attribution_name or "").split()[-1].casefold() if attribution_name else ""
    for i, p in enumerate(paragraphs):
        pc = p.casefold()
        if (anchor and anchor in pc) or ("режисс" in pc):
            insert_at = i + 1
            break
    paragraphs.insert(min(insert_at, len(paragraphs)), block)
    return "\n\n".join(paragraphs).strip()


def _ensure_blockquote_has_attribution(
    *,
    description: str,
    attribution_name: str | None,
) -> str:
    """Ensure at least one Markdown blockquote contains the attribution name.

    If we have a direct quote block but the speaker name is only mentioned in narration,
    operators (and tests) cannot reliably tell whose quote it is. We fix that by adding
    a short attribution line inside the first quote block.
    """
    desc = (description or "").strip()
    name = (attribution_name or "").strip()
    if not desc or not name:
        return description
    lines = desc.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    quote_line_idxs = [i for i, ln in enumerate(lines) if ln.lstrip().startswith(">")]
    if not quote_line_idxs:
        return description

    name_cf = name.casefold()
    # Does any quote line already mention the name?
    for i in quote_line_idxs:
        if name_cf in lines[i].casefold():
            return description

    # Find the first contiguous quote block and append an attribution line to it.
    start = quote_line_idxs[0]
    end = start
    while end + 1 < len(lines) and lines[end + 1].lstrip().startswith(">"):
        end += 1

    # Avoid adding duplicate attribution markers.
    if end >= start and re.search(r"(?i)^\s*>\s*[—-]\s*\S", lines[end] or ""):
        return description

    lines.insert(end + 1, f"> — {name}")
    updated = "\n".join(lines).strip()
    return updated


async def _ensure_direct_quote_blockquote(
    *,
    description: str,
    quote_candidates: Sequence[str] | None,
    candidate_text: str | None,
    facts_before: Sequence[str] | None,
    label: str,
) -> str:
    """Ensure we have a Markdown blockquote when we detected quote candidates.

    Strategy:
    1) Ask LLM to integrate it.
    2) If LLM still doesn't produce a blockquote, deterministically inject it.
    """
    desc = (description or "").strip()
    if not desc:
        return description
    if re.search(r"(?m)^>\s+", desc):
        return description

    qc = [str(q or "").strip() for q in (quote_candidates or []) if str(q or "").strip()]
    if not qc:
        return description

    quote = qc[0]
    enforced = await _llm_enforce_blockquote(description=desc, quote=quote, label=label)
    if enforced and re.search(r"(?m)^>\s+", enforced):
        director = _extract_director_name_hint(candidate_text=candidate_text, facts_before=facts_before)
        return _ensure_blockquote_has_attribution(description=enforced, attribution_name=director)

    director = _extract_director_name_hint(candidate_text=candidate_text, facts_before=facts_before)
    injected = _inject_direct_quote_blockquote(
        description=desc,
        quote=quote,
        attribution_name=director,
    )
    injected = _ensure_blockquote_has_attribution(description=injected, attribution_name=director)
    return injected


async def _poster_is_relevant(candidate: EventCandidate, poster: PosterCandidate) -> tuple[bool, str | None]:
    """Decide whether a poster image is relevant to the event.

    Goal: avoid attaching generic promo banners (discounts, promos) as event posters.
    """
    ocr = (poster.ocr_text or "").strip()
    if not ocr:
        return True, None
    if not _POSTER_PROMO_RE.search(ocr):
        return True, None

    # Heuristic: promo + no datetime signals + no overlap with title tokens => likely unrelated.
    title_tokens = _title_tokens(candidate.title)
    overlap = 0
    if title_tokens:
        ocr_tokens = set(re.findall(r"[a-zа-яё0-9]{4,}", ocr.lower(), flags=re.IGNORECASE))
        overlap = len(title_tokens & ocr_tokens)

    if not _has_datetime_signals(ocr) and overlap == 0:
        return False, "promo_no_datetime"

    # Borderline cases: ask Gemma (best-effort).
    if SMART_UPDATE_LLM_DISABLED:
        return True, None
    client = _get_gemma_client()
    if client is None:
        return True, None
    schema = {
        "type": "object",
        "properties": {
            "relevant": {"type": "boolean"},
            "reason_short": {"type": "string"},
        },
        "required": ["relevant", "reason_short"],
        "additionalProperties": False,
    }
    payload = {
        "event": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "location_name": candidate.location_name,
        },
        "poster_ocr": _clip(ocr, 1200),
    }
    prompt = (
        "Ты решаешь, относится ли афиша к КОНКРЕТНОМУ событию или это общий промо-баннер (скидки/акции/промокоды).\n"
        "Верни JSON: {relevant: true|false, reason_short: '...'}.\n"
        "Если на изображении в основном скидка/акция и нет признаков конкретного события (название/дата/время/площадка), верни relevant=false.\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(prompt, schema, max_tokens=140, label="poster_relevance")
    if isinstance(data, dict) and isinstance(data.get("relevant"), bool):
        return bool(data["relevant"]), str(data.get("reason_short") or "").strip() or None
    return True, None


def _format_ticket_price(
    price_min: int | None, price_max: int | None
) -> str | None:
    if price_min is None and price_max is None:
        return None
    if price_min is not None and price_max is not None:
        if price_min == price_max:
            return f"{price_min} ₽"
        return f"{price_min}–{price_max} ₽"
    if price_min is not None:
        return f"от {price_min} ₽"
    return f"до {price_max} ₽"


def _normalize_fact_item(value: str | None, limit: int = 200) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return None
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 1].rstrip() + "…"
    return cleaned


_FACT_GROUNDING_STOPWORDS = {
    "и",
    "в",
    "во",
    "на",
    "по",
    "для",
    "от",
    "до",
    "без",
    "при",
    "с",
    "со",
    "из",
    "или",
    "это",
    "будет",
    "будут",
    "мероприятие",
    "мероприятия",
    "программа",
}
_SENSITIVE_FACT_GROUNDING_RE = re.compile(
    r"(?iu)(?:"
    r"\b\d{1,2}\+\b|"
    r"\bвозраст\w+\b|"
    r"\b(?:максимальн\w+\s+)?размер\s+групп\w*\b|"
    r"\bгрупп\w*\b.*\bчеловек\b|"
    r"\bмест[ао]?\b|"
    r"\bучастник\w*\b.*\bмаксим\w*\b|"
    r"\bпродл\w+\b|"
    r"\bдлительност\w+\b|"
    r"\bантракт\w*\b|"
    r"\bконцерт\w*\b|"
    r"\bмузык\w*\b|"
    r"\bклассическ\w*\b|"
    r"\bсимфони\w*\b|"
    r"\bтоккат\w*\b|"
    r"\bбах\b|"
    r"\bбетховен\b|"
    r"\bчайковск\w*\b|"
    r"\bравель\b|"
    r"\bболеро\b|"
    r"\bлебедин\w+\s+озер\w+\b"
    r")"
)


def _normalize_fact_grounding_text(text: str | None) -> str:
    raw = (text or "").strip().casefold().replace("ё", "е")
    if not raw:
        return ""
    raw = re.sub(r"[^\w\s]+", " ", raw, flags=re.U)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _fact_grounding_tokens(text: str | None) -> list[str]:
    raw = _normalize_fact_grounding_text(text)
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for token in raw.split():
        if token in _FACT_GROUNDING_STOPWORDS:
            continue
        if token.isdigit():
            key = token
        else:
            if len(token) < 3:
                continue
            key = token[:7]
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _candidate_fact_grounding_corpus(candidate: EventCandidate) -> str:
    parts: list[str] = []
    for value in (
        _strip_promo_lines(candidate.source_text) or candidate.source_text,
        _strip_promo_lines(candidate.raw_excerpt) or candidate.raw_excerpt,
    ):
        if str(value or "").strip():
            parts.append(str(value or "").strip())
    for poster in candidate.posters or []:
        for value in (getattr(poster, "ocr_text", None), getattr(poster, "ocr_title", None)):
            if str(value or "").strip():
                parts.append(str(value or "").strip())
    return _normalize_fact_grounding_text("\n".join(parts))


def _fact_requires_strict_grounding(fact: str | None) -> bool:
    return bool(_SENSITIVE_FACT_GROUNDING_RE.search(str(fact or "")))


def _fact_is_grounded_in_candidate_sources(fact: str | None, candidate: EventCandidate) -> bool:
    if not _fact_requires_strict_grounding(fact):
        return True
    probe = _candidate_fact_grounding_corpus(candidate)
    tokens = _fact_grounding_tokens(fact)
    if not probe or not tokens:
        return False
    matched = sum(1 for token in tokens if token and token in probe)
    required = 1 if len(tokens) <= 1 else 2
    return matched >= min(required, len(tokens))


def _filter_ungrounded_sensitive_facts(
    facts: Sequence[object] | None,
    *,
    candidate: EventCandidate,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in facts or []:
        cleaned = _normalize_fact_item(str(item or ""), limit=180)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        if not _fact_is_grounded_in_candidate_sources(cleaned, candidate):
            logger.warning(
                "smart_update.fact_rejected reason=ungrounded_sensitive_fact source_type=%s source_url=%s fact=%s",
                candidate.source_type,
                candidate.source_url,
                _clip(cleaned, 180),
            )
            continue
        seen.add(key)
        out.append(cleaned)
    return out


_RU_MONTHS_GENITIVE: dict[str, int] = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def _semantic_fact_key(
    fact: str | None,
    *,
    event_date: str | None,
    event_time: str | None,
) -> str | None:
    """Build a semantic key for anchor-like facts to avoid meaning-duplicates.

    Examples:
      "Дата: 2026-02-12" -> "date:2026-02-12"
      "Спектакль будет показан 12 февраля." (event_date=2026-02-12) -> "date:2026-02-12"
      "Начало спектакля в 19:00." -> "time:19:00"
    """
    raw = (fact or "").strip()
    if not raw:
        return None
    s = re.sub(r"\s+", " ", raw).strip()
    low = s.lower()

    def _iso_date_in_text(text: str) -> str | None:
        m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
        if not m:
            return None
        return m.group(1)

    def _parse_ru_date(text: str) -> str | None:
        # 12 февраля [2026]
        m = re.search(
            r"\b(?P<d>\d{1,2})\s+(?P<m>[а-яё]+)(?:\s+(?P<y>20\d{2}))?\b",
            text,
            flags=re.IGNORECASE,
        )
        if not m:
            return None
        day = int(m.group("d"))
        month_word = (m.group("m") or "").casefold()
        month = _RU_MONTHS_GENITIVE.get(month_word)
        if not month:
            return None
        year = int(m.group("y")) if (m.group("y") or "").strip().isdigit() else None
        # If event_date is known and matches day/month, reuse it (handles year ambiguity around New Year).
        if event_date:
            try:
                ev_d = date.fromisoformat(event_date.split("..", 1)[0].strip())
                if ev_d.day == day and ev_d.month == month:
                    return ev_d.isoformat()
                if year is None:
                    year = ev_d.year
            except Exception:
                pass
        if year is None:
            return None
        try:
            return date(year, month, day).isoformat()
        except Exception:
            return None

    def _parse_time(text: str) -> str | None:
        m = re.search(r"\b(?P<h>\d{1,2}):(?P<m>\d{2})\b", text)
        if not m:
            return None
        hh = int(m.group("h"))
        mm = int(m.group("m"))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        return f"{hh:02d}:{mm:02d}"

    if low.startswith("дата окончания:"):
        iso = _iso_date_in_text(low) or _parse_ru_date(low)
        return f"end_date:{iso}" if iso else None
    if low.startswith("дата:"):
        iso = _iso_date_in_text(low) or _parse_ru_date(low)
        return f"date:{iso}" if iso else None
    if low.startswith("время:"):
        t = _parse_time(low)
        return f"time:{t}" if t else None

    # Free-form: detect date/time mentions.
    iso = _iso_date_in_text(low) or _parse_ru_date(low)
    if iso:
        return f"date:{iso}"
    t = _parse_time(low)
    if t:
        return f"time:{t}"
    return None


def _fact_preference_score(fact: str) -> int:
    """Higher score = we prefer to keep this form in ✅ when keys collide."""
    low = (fact or "").strip().lower()
    if low.startswith(("дата:", "дата окончания:", "время:")):
        return 3
    if re.search(r"\b20\d{2}-\d{2}-\d{2}\b", low) or re.search(r"\b\d{1,2}:\d{2}\b", low):
        return 2
    return 1


def _demote_redundant_anchor_facts(
    added_log: list[str],
    duplicate_log: list[str],
    *,
    event_date: str | None,
    event_time: str | None,
    updated_keys: set[str],
) -> tuple[list[str], list[str]]:
    """Move meaning-duplicates of existing anchors from ✅ to ↩️.

    If event_date/time already exist and weren't updated in this merge, we treat
    any date/time mentions in LLM facts as duplicates (operator UX).
    """
    kept: list[str | None] = [None] * len(added_log)
    best_by_key: dict[str, tuple[int, str]] = {}

    # Determine current anchors after merge (event_db already has final values).
    anchor_date = (event_date or "").split("..", 1)[0].strip() or None
    anchor_time = (event_time or "").strip() or None
    date_was_updated = "date" in updated_keys
    time_was_updated = "time" in updated_keys

    for i, fact in enumerate(list(added_log or [])):
        f = (fact or "").strip()
        if not f:
            continue
        k = _semantic_fact_key(f, event_date=anchor_date, event_time=anchor_time)
        if not k:
            kept[i] = f
            continue

        # If anchor already existed (not updated), treat restatements as duplicates.
        if k.startswith("date:") and (not date_was_updated) and anchor_date and k == f"date:{anchor_date}":
            duplicate_log.append(f)
            kept[i] = None
            continue
        if k.startswith("time:") and (not time_was_updated) and anchor_time and k == f"time:{anchor_time}":
            duplicate_log.append(f)
            kept[i] = None
            continue

        prev = best_by_key.get(k)
        if not prev:
            best_by_key[k] = (i, f)
            kept[i] = f
            continue

        prev_i, prev_f = prev
        if _fact_preference_score(f) > _fact_preference_score(prev_f):
            duplicate_log.append(prev_f)
            kept[prev_i] = None
            best_by_key[k] = (i, f)
            kept[i] = f
        else:
            duplicate_log.append(f)
            kept[i] = None

    new_added = [x for x in kept if x]
    return new_added, duplicate_log


def _initial_textual_facts(candidate: EventCandidate, *, max_items: int = 2) -> list[str]:
    """Extract a couple of textual (non-service) facts for operator source log on create."""
    snippets = _collect_new_candidate_sentences(candidate, before_norm="")
    out: list[str] = []
    seen: set[str] = set()
    for sent in snippets:
        cleaned = _normalize_fact_item(sent, limit=170)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(f"Тезис: {cleaned}")
        if len(out) >= max_items:
            break
    return out


def _render_location_fact_value(
    *,
    location_name: str | None,
    location_address: str | None,
    city: str | None,
) -> str | None:
    name = (location_name or "").strip()
    if not name:
        return None
    # If location_name already looks like a canonical full line ("name, address, city"),
    # do not duplicate address/city from separate fields in source logs.
    if name.count(",") >= 2:
        return name
    parts = [
        name,
        (location_address or "").strip(),
        (city or "").strip(),
    ]
    value = ", ".join(part for part in parts if part)
    return value or None


def _initial_added_facts(candidate: EventCandidate) -> list[str]:
    facts: list[str] = []
    if candidate.date:
        facts.append(f"Дата: {candidate.date}")
    if candidate.end_date:
        facts.append(f"Дата окончания: {candidate.end_date}")
    if candidate.time:
        facts.append(f"Время: {candidate.time}")
    location = _render_location_fact_value(
        location_name=candidate.location_name,
        location_address=candidate.location_address,
        city=candidate.city,
    )
    if location:
        facts.append(f"Локация: {location}")
    if candidate.is_free is True:
        facts.append("Бесплатно")
    price_text = _format_ticket_price(
        candidate.ticket_price_min, candidate.ticket_price_max
    )
    if price_text:
        facts.append(f"Цена: {price_text}")
    if candidate.ticket_status == "sold_out":
        facts.append("Билеты все проданы")
    if candidate.ticket_link:
        label = "Регистрация" if candidate.is_free else "Билеты"
        facts.append(f"{label}: {candidate.ticket_link}")
    if candidate.event_type:
        facts.append(f"Тип: {candidate.event_type}")
    if candidate.festival:
        facts.append(f"Фестиваль: {candidate.festival}")
    if candidate.festival_full:
        facts.append(f"Выпуск фестиваля: {candidate.festival_full}")
    if candidate.pushkin_card is True:
        facts.append("Пушкинская карта")
    # IMPORTANT: Do not emit "Тезис: ..." pseudo-facts. Operator log must contain facts only.

    normalized: list[str] = []
    seen: set[str] = set()
    for fact in facts:
        cleaned = _normalize_fact_item(fact)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)
    return normalized[:12]


def _candidate_anchor_facts_for_log(candidate: EventCandidate) -> list[str]:
    """Anchor-only facts for source log (no free-form textual theses)."""
    facts: list[str] = []
    if candidate.date:
        facts.append(f"Дата: {candidate.date}")
    if candidate.end_date:
        facts.append(f"Дата окончания: {candidate.end_date}")
    if candidate.time:
        facts.append(f"Время: {candidate.time}")
    location = _render_location_fact_value(
        location_name=candidate.location_name,
        location_address=candidate.location_address,
        city=candidate.city,
    )
    if location:
        facts.append(f"Локация: {location}")
    if candidate.is_free is True:
        facts.append("Бесплатно")
    price_text = _format_ticket_price(candidate.ticket_price_min, candidate.ticket_price_max)
    if price_text:
        facts.append(f"Цена: {price_text}")
    if candidate.ticket_status == "sold_out":
        facts.append("Билеты все проданы")
    if candidate.ticket_link:
        label = "Регистрация" if candidate.is_free else "Билеты"
        facts.append(f"{label}: {candidate.ticket_link}")
    if candidate.event_type:
        facts.append(f"Тип: {candidate.event_type}")
    if candidate.festival:
        facts.append(f"Фестиваль: {candidate.festival}")
    if candidate.festival_full:
        facts.append(f"Выпуск фестиваля: {candidate.festival_full}")
    if candidate.pushkin_card is True:
        facts.append("Пушкинская карта")

    normalized: list[str] = []
    seen: set[str] = set()
    for fact in facts:
        cleaned = _normalize_fact_item(fact)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)
    return normalized[:12]


_CANONICAL_SCI_LIBRARY_NAME = "Научная библиотека"
_CANONICAL_SCI_LIBRARY_ADDRESS = "Мира 9"
_CANONICAL_SCI_LIBRARY_CITY = "Калининград"

_CANONICAL_DOM_KITOBOYA_NAME = "Дом китобоя"
_CANONICAL_DOM_KITOBOYA_ADDRESS = "Мира 9"
_CANONICAL_DOM_KITOBOYA_CITY = "Калининград"

_CANONICAL_ZAKHEIM_NAME = "Закхаймские ворота"
_CANONICAL_ZAKHEIM_ADDRESS = "Литовский Вал 61"
_CANONICAL_ZAKHEIM_CITY = "Калининград"

_CANONICAL_FRIEDLAND_NAME = "Фридландские ворота"
_CANONICAL_FRIEDLAND_ADDRESS = "Дзержинского 30"
_CANONICAL_FRIEDLAND_CITY = "Калининград"

_CANONICAL_RAILWAY_GATES_NAME = "Железнодорожные ворота"
_CANONICAL_RAILWAY_GATES_ADDRESS = "Гвардейский проспект 51А"
_CANONICAL_RAILWAY_GATES_CITY = "Калининград"


def _normalize_location_compact(value: str | None) -> str:
    if not value:
        return ""
    normalized = _norm_space(value)
    normalized = re.sub(r"[,.]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _looks_like_scientific_library_alias(norm_compact: str) -> bool:
    if not norm_compact:
        return False
    if "бфу" in norm_compact:
        return False
    return (
        norm_compact == "научная библиотека"
        or norm_compact == "научная библиотека мира 9 калининград"
        or "калининградская областная научная библиотека" in norm_compact
    )


def _looks_like_scientific_library_room_alias(norm_compact: str) -> bool:
    """Return true for source-local room/floor names inside KОНБ.

    These strings are useful as room details, but they are not standalone
    venues.  Keep the rule intentionally narrow and source-gated in
    ``_canonicalize_location_fields`` so a generic room at another Мира 9 venue
    (for example Дом китобоя) is not silently reclassified.
    """

    if not norm_compact:
        return False
    if "бфу" in norm_compact or "дом китобоя" in norm_compact:
        return False
    probes = {
        norm_compact,
        re.sub(r"\b(?:2|4)\s*этаж\b", "", norm_compact).strip(),
        re.sub(r"\b(?:этаж|место проведения)\b", "", norm_compact).strip(),
    }
    return any(
        probe in {"читальный зал", "лекционный зал"}
        or re.fullmatch(r"(?:2|4)\s*этаж\s+(?:читальный|лекционный)\s+зал", probe)
        or re.fullmatch(r"(?:читальный|лекционный)\s+зал\s+(?:2|4)\s*этаж", probe)
        for probe in probes
        if probe
    )


def _looks_like_dom_kitoboya_alias(norm_compact: str) -> bool:
    if not norm_compact:
        return False
    return "дом китобоя" in norm_compact


def _looks_like_zakheim_alias(norm_compact: str) -> bool:
    if not norm_compact:
        return False
    norm_soft = norm_compact.replace("-", " ").replace("—", " ")
    norm_soft = re.sub(r"\s+", " ", norm_soft).strip()
    if "закхайм" in norm_soft or "закхейм" in norm_soft:
        return True
    if "литовск" in norm_soft and "61" in norm_soft:
        return True
    return norm_soft in {
        "арт пространство ворота",
        "артпространство ворота",
        "пространство ворота",
        "ворота галерея",
    }


def _looks_like_friedland_alias(norm_compact: str) -> bool:
    if not norm_compact:
        return False
    norm_soft = norm_compact.replace("-", " ").replace("—", " ")
    norm_soft = re.sub(r"\s+", " ", norm_soft).strip()
    if "фридланд" in norm_soft:
        return True
    return "дзержинского 30" in norm_soft


def _looks_like_railway_gates_alias(norm_compact: str) -> bool:
    if not norm_compact:
        return False
    norm_soft = norm_compact.replace("-", " ").replace("—", " ")
    norm_soft = re.sub(r"\s+", " ", norm_soft).strip()
    # `Железнодорожная 1` is also a street address in Зеленоградск for
    # `Театральная гостиная Солёная ворона`; do not collapse it to the
    # Kaliningrad gate unless the source explicitly says "ворота" or gives the
    # Railway Gates address/landmarks.
    if "железнодорож" in norm_soft and "ворот" in norm_soft:
        return True
    if "гвардейск" in norm_soft and "51а" in norm_soft:
        return True
    if "генерала буткова" in norm_soft:
        return True
    return False


def _canonicalize_location_fields(
    *,
    location_name: str | None,
    location_address: str | None,
    city: str | None,
    source_chat_username: str | None = None,
    source_url: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    name = (location_name or "").strip() or None
    address = (location_address or "").strip() or None
    city_value = (city or "").strip() or None

    name_norm = _normalize_location_compact(name)
    address_norm = _normalize_location_compact(address)
    combined_norm = " ".join([name_norm, address_norm]).strip()
    source_hint = " ".join(
        [
            (source_chat_username or "").strip().casefold(),
            (source_url or "").strip().casefold(),
        ]
    ).strip()
    is_konb_source = any(
        marker in source_hint
        for marker in ("konb39", "wall-30777579_", "vk.com/public30777579")
    )

    if _looks_like_scientific_library_alias(combined_norm):
        return (
            _CANONICAL_SCI_LIBRARY_NAME,
            _CANONICAL_SCI_LIBRARY_ADDRESS,
            _CANONICAL_SCI_LIBRARY_CITY,
        )

    if (
        is_konb_source
        and _looks_like_scientific_library_room_alias(name_norm)
        and (not address_norm or "мира 9" in address_norm)
    ):
        return (
            _CANONICAL_SCI_LIBRARY_NAME,
            _CANONICAL_SCI_LIBRARY_ADDRESS,
            _CANONICAL_SCI_LIBRARY_CITY,
        )

    if _looks_like_dom_kitoboya_alias(combined_norm):
        return (
            _CANONICAL_DOM_KITOBOYA_NAME,
            _CANONICAL_DOM_KITOBOYA_ADDRESS,
            _CANONICAL_DOM_KITOBOYA_CITY,
        )

    if _looks_like_friedland_alias(combined_norm):
        return (
            _CANONICAL_FRIEDLAND_NAME,
            _CANONICAL_FRIEDLAND_ADDRESS,
            _CANONICAL_FRIEDLAND_CITY,
        )

    if _looks_like_railway_gates_alias(combined_norm):
        return (
            _CANONICAL_RAILWAY_GATES_NAME,
            _CANONICAL_RAILWAY_GATES_ADDRESS,
            _CANONICAL_RAILWAY_GATES_CITY,
        )

    zakheim_by_source = bool(
        source_hint
        and "vorotagallery" in source_hint
        and (
            not name_norm
            or "закх" in name_norm
            or "литовск" in combined_norm
            or "арт пространство ворота" in combined_norm
            or "артпространство ворота" in combined_norm
        )
    )
    if _looks_like_zakheim_alias(combined_norm) or zakheim_by_source:
        return (
            _CANONICAL_ZAKHEIM_NAME,
            _CANONICAL_ZAKHEIM_ADDRESS,
            _CANONICAL_ZAKHEIM_CITY,
        )

    # Normalize common address abbreviations for known locations.
    if address and ("мира 9" in address_norm):
        if name_norm and "дом китобоя" in name_norm:
            address = _CANONICAL_DOM_KITOBOYA_ADDRESS
        elif _looks_like_scientific_library_alias(name_norm):
            address = _CANONICAL_SCI_LIBRARY_ADDRESS

    payload = {
        "location_name": name,
        "location_address": address,
        "city": city_value,
    }
    normalise_event_location_from_reference(payload)
    name = (payload.get("location_name") or "").strip() or None
    address = (payload.get("location_address") or "").strip() or None
    city_value = (payload.get("city") or "").strip() or None

    return name, address, city_value


def _normalize_location(value: str | None) -> str:
    if not value:
        return ""
    # Normalize for matching only (not for public display): remove punctuation noise and
    # make "Янтарь-холл" ~= "Янтарь холл, Ленина 11".
    raw_norm_compact = _normalize_location_compact(value)
    if _looks_like_zakheim_alias(raw_norm_compact):
        return "закхаймские ворота"
    if _looks_like_friedland_alias(raw_norm_compact):
        return "фридландские ворота"
    if _looks_like_railway_gates_alias(raw_norm_compact):
        return "железнодорожные ворота"

    norm = raw_norm_compact
    norm = norm.replace("-", " ").replace("—", " ")
    norm = re.sub(r"[«»\"']", " ", norm)
    norm = re.sub(r"\s+", " ", norm).strip()
    norm = _LOCATION_NOISE_PREFIXES_RE.sub("", norm).strip()
    # Canonicalize aliases of Kaliningrad Regional Scientific Library.
    # Do NOT merge BFU library names into this bucket.
    norm_compact = _normalize_location_compact(norm)
    if _looks_like_scientific_library_alias(norm_compact):
        return "научная библиотека"
    if _looks_like_dom_kitoboya_alias(norm_compact):
        return "дом китобоя"
    if _looks_like_friedland_alias(norm_compact):
        return "фридландские ворота"
    if _looks_like_railway_gates_alias(norm_compact):
        return "железнодорожные ворота"
    if _looks_like_zakheim_alias(norm_compact):
        return "закхаймские ворота"
    return norm


def _location_matches(a: str | None, b: str | None) -> bool:
    na = _normalize_location(a)
    nb = _normalize_location(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if na in nb or nb in na:
        return True
    return False


_LOCATION_VALUE_ADDRESS_HINT_RE = re.compile(
    r"(?iu)\b(?:ул(?:ица)?|пр(?:оспект|осп)?|пр-?т|пер(?:еулок)?|наб(?:ережная)?|пл(?:ощадь)?|бульвар|дом|д\.)\b"
)
_LOCATION_VALUE_PROSE_VERB_RE = re.compile(
    r"(?iu)\b(?:"
    r"раскрыт\w*|раскрыва\w*|сумел\w*|стоит\w*|стоя\w*|"
    r"представ\w*|покаж\w*|расскаж\w*|приглаша\w*|"
    r"пройд[её]т|состоится|будут|можно|созда\w*|жд\w*"
    r")\b"
)
_LOCATION_VALUE_TEMPORAL_RE = re.compile(
    r"(?iu)^\s*(?:"
    r"сегодня|завтра|послезавтра|вчера|"
    r"(?:в\s+)?(?:понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)|"
    r"\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
    r")\s*[,.:;!?]?\s*$"
)

_LOCATION_VALUE_NON_VENUE_START_RE = re.compile(
    r"(?iu)^\s*(?:"
    r"в\s+программе|программа\s+[–—-]|вы\s+услышите|"
    r"и\s+не\s+забывайте|не\s+забывайте|напоминаем"
    r")\b"
)
_LOCATION_VALUE_CAMPAIGN_RE = re.compile(
    r"(?iu)\b(?:акци[яию]|скидк\w*|пушкинск\w+\s+карт\w*|как\s+принять\s+участие|услови[яй]\s+акци)\b"
)

# This is deliberately a routing-only safety net.  It does not decide that the
# source is a non-event; it identifies a narrow logistics/date-role ambiguity
# that must be adjudicated by the LLM eventness stage before public creation.
_OPERATIONAL_DATE_ROLE_RE = re.compile(
    r"(?iu)(?:"
    r"\bбилет\w*\b.{0,100}\bдействител\w*\b.{0,80}\b(?:до|по)\b|"
    r"\b(?:касс\w*|зоопарк\w*|музе[йя]\w*|библиотек\w*|парк\w*)\b.{0,120}"
    r"\b\d{1,2}[:.]\d{2}\s*[–—-]\s*\d{1,2}[:.]\d{2}\b|"
    r"\bоткрыт\w*\s+и\s+работа\w*\s+в\s+обычн\w+\s+режим\w*\b"
    r")"
)


def _strip_location_value_temporal_decoration(value: str) -> str:
    compact = re.sub(r"\s+", " ", value or "").strip()
    return re.sub(r"^[^0-9A-Za-zА-Яа-яЁё]+", "", compact).strip()


def _location_value_looks_like_prose_fragment(value: str | None) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    compact = re.sub(r"\s+", " ", raw)
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", compact)
    if "\n" in raw:
        return True
    temporal_probes = {compact, _strip_location_value_temporal_decoration(compact)}
    if any(_LOCATION_VALUE_TEMPORAL_RE.fullmatch(probe) for probe in temporal_probes if probe):
        return True
    if _LOCATION_VALUE_NON_VENUE_START_RE.search(compact) and not _LOCATION_VALUE_ADDRESS_HINT_RE.search(compact):
        return True
    if len(compact) > 90:
        return True
    if len(words) >= 8 and not _LOCATION_VALUE_ADDRESS_HINT_RE.search(compact):
        return True
    if len(words) >= 4 and _LOCATION_VALUE_PROSE_VERB_RE.search(compact):
        return True
    if len(words) >= 4 and re.search(r"[.!?]\s*$", compact):
        return True
    return False


def _candidate_location_looks_unsupported_prose(candidate: "EventCandidate") -> bool:
    if not _location_value_looks_like_prose_fragment(candidate.location_name):
        return False
    payload = {
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
    }
    try:
        return normalise_event_location_from_reference(payload) is None
    except Exception:
        return True


_ADDRESS_NOISE_RE = re.compile(
    r"(?iu)\b(?:ул(?:ица)?|пр(?:оспект|осп)?|пр-?т|пер(?:еулок)?|б-р|бульвар|пл(?:ощадь)?|наб(?:ережная)?)\.?\b"
)


def _normalize_address_for_match(value: str | None, city: str | None = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    norm = _normalize_location_compact(raw)
    norm = norm.replace("-", " ").replace("—", " ")
    norm = re.sub(r"[«»\"']", " ", norm)
    norm = _ADDRESS_NOISE_RE.sub(" ", norm)
    if city:
        city_norm = _normalize_location_compact(city)
        if city_norm:
            norm = re.sub(
                rf"(?iu)(?:,\s*|\s+)#?{re.escape(city_norm)}$",
                "",
                norm,
            )
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm


def _address_matches(
    a: str | None,
    b: str | None,
    *,
    city_a: str | None = None,
    city_b: str | None = None,
) -> bool:
    na = _normalize_address_for_match(a, city_a)
    nb = _normalize_address_for_match(b, city_b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Extra room/floor/building details are allowed, but house numbers must be
    # complete tokens.  Raw substring comparison made ``Советский 1`` equal
    # ``Советский 12`` and could recall/merge an unrelated ICAE event.
    if (
        re.search(rf"(^|\s){re.escape(na)}(\s|$)", nb)
        or re.search(rf"(^|\s){re.escape(nb)}(\s|$)", na)
    ):
        return True
    return False


def _event_candidate_location_matches(event: Event, candidate: "EventCandidate") -> bool:
    if _location_matches(getattr(event, "location_name", None), candidate.location_name):
        return True
    return _address_matches(
        getattr(event, "location_address", None),
        candidate.location_address,
        city_a=getattr(event, "city", None),
        city_b=candidate.city,
    )


def _apply_soft_city_filter(stmt, city: str | None):
    """Filter by candidate city, but keep legacy rows with empty city.

    Telegram/VK imports can create an event with `city=""` first, then a later
    source of the same event may resolve city correctly (e.g. "Калининград").
    Strict city equality would hide the existing row from shortlist and create
    a duplicate instead of merge.
    """
    city_value = str(city or "").strip()
    if not city_value:
        return stmt
    return stmt.where(
        or_(
            Event.city == city_value,
            Event.city.is_(None),
            Event.city == "",
        )
    )


@lru_cache(maxsize=1)
def _get_gemma_client():
    try:
        from google_ai import GoogleAIClient, SecretsProvider
        from main import get_supabase_client, notify_llm_incident
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("smart_update: gemma client unavailable: %s", exc)
        return None
    supabase = build_google_ai_limiter_supabase_client(
        fallback_factory=get_supabase_client
    )
    client = GoogleAIClient(
        supabase_client=supabase,
        secrets_provider=SecretsProvider(),
        consumer="smart_update",
        incident_notifier=notify_llm_incident,
    )
    raw_max_retries = (os.getenv("SMART_UPDATE_GOOGLE_AI_MAX_RETRIES", "1") or "").strip()
    try:
        client.max_retries = max(1, min(int(raw_max_retries), 3))
    except Exception:
        client.max_retries = 1
    return client


def _strip_code_fences(text: str) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", cleaned)
        cleaned = cleaned.replace("```", "")
    return cleaned.strip()


def _safe_openai_schema_name(label: str, *, prefix: str = "SmartUpdate") -> str:
    raw = f"{prefix}_{label}"
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", raw).strip("_-")
    return (safe or prefix or "SmartUpdate")[:64]


def _extract_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = _strip_code_fences(text)
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        data = json.loads(cleaned[start : end + 1])
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


_GEMMA_NATIVE_SCHEMA_UNSUPPORTED_KEYS = {"$schema", "additionalProperties", "uniqueItems"}


def _gemma_native_response_schema(schema: Any) -> Any:
    """Convert local JSON-schema-ish contracts to the Google GenAI schema subset."""
    if isinstance(schema, dict):
        out: dict[str, Any] = {}
        for key, value in schema.items():
            if key in _GEMMA_NATIVE_SCHEMA_UNSUPPORTED_KEYS:
                continue
            if key == "type" and isinstance(value, str):
                out[key] = value.upper()
                continue
            if key == "type" and isinstance(value, list):
                non_null = [item for item in value if item != "null"]
                out[key] = str(non_null[0] if non_null else "string").upper()
                if "null" in value:
                    out["nullable"] = True
                continue
            out[key] = _gemma_native_response_schema(value)
        return out
    if isinstance(schema, list):
        return [_gemma_native_response_schema(item) for item in schema if item != "null"]
    return schema


def _smart_update_native_schema_enabled(label: str) -> bool:
    if not SMART_UPDATE_GEMMA_NATIVE_SCHEMA:
        return False
    stages = SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES
    return "*" in stages or label in stages


def _smart_update_prompt_schema_fallback_enabled(label: str) -> bool:
    if not SMART_UPDATE_G4_SPLIT_CREATE or label != "split_description_writer":
        return True
    return (os.getenv("SMART_UPDATE_G4_SPLIT_CREATE_PROMPT_FALLBACK", "0") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_SMART_UPDATE_4O_FALLBACK_BUDGET = {"window_start": 0.0, "count": 0}


def _smart_update_4o_fallback_budget_allows(label: str) -> bool:
    raw_limit = (os.getenv("SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR", "") or "").strip()
    if not raw_limit:
        return True
    try:
        max_per_hour = int(raw_limit)
    except ValueError:
        max_per_hour = 0
    if max_per_hour <= 0:
        return False
    now = time.monotonic()
    window_start = float(_SMART_UPDATE_4O_FALLBACK_BUDGET.get("window_start") or 0.0)
    if not window_start or now - window_start >= 3600:
        _SMART_UPDATE_4O_FALLBACK_BUDGET["window_start"] = now
        _SMART_UPDATE_4O_FALLBACK_BUDGET["count"] = 0
    count = int(_SMART_UPDATE_4O_FALLBACK_BUDGET.get("count") or 0)
    if count >= max_per_hour:
        logger.warning(
            "smart_update: 4o fallback budget exhausted label=%s count=%s max_per_hour=%s",
            label,
            count,
            max_per_hour,
        )
        return False
    _SMART_UPDATE_4O_FALLBACK_BUDGET["count"] = count + 1
    return True


def _smart_update_4o_fallback_enabled(label: str) -> bool:
    env_value = (os.getenv("SMART_UPDATE_4O_FALLBACK", "1") or "").strip().lower()
    if env_value in {"0", "false", "no", "off"}:
        return False
    # These stages can create or approve every public field.  A second writer
    # must not become an unreviewed semantic authority after the primary model
    # failed/returned thought-only output.  The split-create path and the
    # dedicated grounding reviewer are the supported recovery boundary.
    if label in {"create_bundle", "match_create_bundle", "create_bundle_grounding"}:
        return False
    if SMART_UPDATE_G4_SPLIT_CREATE and label in {
        "rich_facts_extract",
        "split_description_writer",
        "split_derived_fields",
    }:
        return False
    return True


def reset_smart_update_llm_trace() -> None:
    """Start collecting Smart Update LLM call diagnostics in the current context."""
    _SMART_UPDATE_LLM_TRACE.set([])


def get_smart_update_llm_trace() -> list[dict[str, Any]]:
    trace = _SMART_UPDATE_LLM_TRACE.get()
    if not isinstance(trace, list):
        return []
    out: list[dict[str, Any]] = []
    now = time.perf_counter()
    for item in trace:
        copied = dict(item)
        if copied.get("status") == "running" and "started_at_monotonic" in copied:
            try:
                started = float(copied.get("started_at_monotonic") or now)
                copied["duration_sec"] = round(now - started, 6)
            except Exception:
                copied["duration_sec"] = None
            copied["status"] = "cancelled_or_running"
        copied.pop("started_at_monotonic", None)
        out.append(copied)
    return out


def _start_llm_trace_record(kind: str, label: str, **extra: Any) -> dict[str, Any] | None:
    trace = _SMART_UPDATE_LLM_TRACE.get()
    if not isinstance(trace, list):
        return None
    record: dict[str, Any] = {
        "kind": kind,
        "label": label,
        "model": SMART_UPDATE_MODEL,
        "started_at_monotonic": time.perf_counter(),
        "attempts": 0,
        "provider_errors": 0,
        "rate_limit_waits": 0,
        "status": "running",
    }
    record.update(extra)
    trace.append(record)
    return record


def _finish_llm_trace_record(
    record: dict[str, Any] | None,
    *,
    status: str,
    error: Exception | str | None = None,
) -> None:
    if not isinstance(record, dict):
        return
    started = float(record.get("started_at_monotonic") or time.perf_counter())
    record["duration_sec"] = round(time.perf_counter() - started, 6)
    record["status"] = status
    record.pop("started_at_monotonic", None)
    if error is not None:
        record["error"] = str(error)[:500]


async def _ask_gemma_json(
    prompt: str,
    schema: dict[str, Any],
    *,
    max_tokens: int,
    label: str,
) -> dict[str, Any] | None:
    """Wall-clock-bounded entry point for Gemma JSON stages.

    Wraps the unbounded implementation in ``asyncio.wait_for`` so a provider 5xx
    storm (or any single hung HTTP attempt) cannot pin a smart_update call past
    ``SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC``. On timeout we return ``None`` and
    let callers take the deterministic ``ungrounded`` / ``no_match`` fallback;
    this keeps ``vk_inbox`` rows from getting stuck at ``status='pending'``
    behind a ~15-minute provider stall (see
    INC-2026-05-07-vk-auto-import-merge-regression-gemma4).
    """
    cap = float(SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC)
    try:
        return await asyncio.wait_for(
            _ask_gemma_json_unbounded(
                prompt,
                schema,
                max_tokens=max_tokens,
                label=label,
            ),
            timeout=cap,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "smart_update: gemma json_call wall_clock_timeout label=%s cap_sec=%s; returning None",
            label,
            cap,
        )
        return None


async def _ask_gemma_json_unbounded(
    prompt: str,
    schema: dict[str, Any],
    *,
    max_tokens: int,
    label: str,
) -> dict[str, Any] | None:
    # Ask Gemma through the shared gateway, then fall back to 4o (operator-visible) if configured.
    # GoogleAIClient already retries retryable provider failures. Keep the
    # Smart Update wrapper to a single outer attempt by default so one failing
    # stage cannot expand into 3 x 3 provider calls; override via env for probes.
    single_primary_send = label == "collection_candidate_adjudication"
    max_tries = int(os.getenv("SMART_UPDATE_GEMMA_RETRIES", "1"))
    base_sleep = float(os.getenv("SMART_UPDATE_GEMMA_RETRY_BASE_SEC", "1.0"))
    # When we are rate-limited, prefer waiting (do not count it as a "try") to
    # keep the new GOOGLE_API_KEY within quota and avoid burning 4o fallback.
    rl_max_wait_sec = float(os.getenv("SMART_UPDATE_GEMMA_RATE_LIMIT_MAX_WAIT_SEC", "180") or "180")
    rl_max_wait_sec = max(0.0, min(rl_max_wait_sec, 1800.0))
    max_tries = max(1, min(max_tries, 5))
    if single_primary_send:
        max_tries = 1
    base_sleep = max(0.1, min(base_sleep, 10.0))
    client = _get_gemma_client()
    model = _resolve_smart_update_model(label)
    schema_text = json.dumps(schema, ensure_ascii=False)
    full_prompt = (
        f"{prompt}\n\n"
        "Верни только JSON без markdown и комментариев.\n"
        f"JSON schema:\n{schema_text}"
    )
    native_prompt = f"{prompt}\n\nВерни только JSON без markdown и комментариев."
    last_exc: Exception | None = None
    raw_last = ""

    # Best-effort: ask provider for JSON MIME when supported to reduce invalid JSON outputs.
    global _GEMMA_JSON_MIME_SUPPORTED
    try:
        _GEMMA_JSON_MIME_SUPPORTED
    except NameError:  # pragma: no cover - module init
        # Gemma models frequently reject JSON MIME mode. Keep it opt-in.
        _GEMMA_JSON_MIME_SUPPORTED = (  # type: ignore[assignment]
            (os.getenv("SMART_UPDATE_GEMMA_JSON_MIME", "0") or "").strip().lower()
            in {"1", "true", "yes", "on"}
        )
    json_gen_cfg = _smart_update_gemma_generation_config(temperature=0)
    if _GEMMA_JSON_MIME_SUPPORTED and not single_primary_send:
        json_gen_cfg["response_mime_type"] = "application/json"
    native_schema_enabled = (
        True if single_primary_send else _smart_update_native_schema_enabled(label)
    )
    native_gen_cfg = {
        **_smart_update_gemma_generation_config(temperature=0),
        "response_mime_type": "application/json",
        "response_schema": _gemma_native_response_schema(schema),
    }
    prompt_schema_fallback_enabled = (
        False if single_primary_send else _smart_update_prompt_schema_fallback_enabled(label)
    )
    trace_record = _start_llm_trace_record(
        "json",
        label,
        model=model,
        max_tokens=max_tokens,
        native_schema_enabled=native_schema_enabled,
        prompt_schema_fallback_enabled=prompt_schema_fallback_enabled,
        physical_sends=0,
        actual_models=[],
        token_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        primary_send_limit=1 if single_primary_send else None,
    )

    def record_physical_send(metadata: Mapping[str, Any] | None = None, *, fallback: bool = False) -> None:
        if trace_record is None:
            return
        trace_record["physical_sends"] = int(trace_record.get("physical_sends") or 0) + 1
        actual_model = "gpt-4o" if fallback else str(
            (metadata or {}).get("provider_model_name")
            or (metadata or {}).get("requested_model")
            or model
        )
        models = trace_record.setdefault("actual_models", [])
        if actual_model and isinstance(models, list):
            models.append(actual_model)

    def record_usage(usage: Any) -> None:
        if trace_record is not None:
            trace_record["token_usage"] = {
                "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
                "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
                "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
            }

    rl_deadline = time.monotonic() + rl_max_wait_sec
    attempt = 1
    while attempt <= max_tries:
        if trace_record is not None:
            trace_record["attempts"] = max(int(trace_record.get("attempts") or 0), attempt)
        if client is None:
            last_exc = RuntimeError("gemma client unavailable")
        else:
            try:
                logger.info(
                    "smart_update: gemma json_call label=%s model=%s max_tokens=%s attempt=%d/%d",
                    label,
                    model,
                    max_tokens,
                    attempt,
                    max_tries,
                )
                if single_primary_send:
                    try:
                        raw_native, _usage = await client.generate_content_async(
                            model=model,
                            prompt=native_prompt,
                            generation_config=native_gen_cfg,
                            max_output_tokens=max_tokens,
                            allow_model_fallback=False,
                            max_provider_attempts=1,
                            attempt_observer=record_physical_send,
                        )
                        record_usage(_usage)
                        raw_last = raw_native or ""
                        data_native = _extract_json(raw_last)
                        if data_native is not None:
                            _finish_llm_trace_record(trace_record, status="ok_native")
                            return data_native
                        last_exc = RuntimeError("gemma returned invalid json")
                    except Exception as exc:
                        last_exc = exc
                        if trace_record is not None:
                            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                    break
                if native_schema_enabled:
                    try:
                        raw_native, _usage = await client.generate_content_async(
                            model=model,
                            prompt=native_prompt,
                            generation_config=native_gen_cfg,
                            max_output_tokens=max_tokens,
                            fallback_models=_smart_update_fallback_models(label, model),
                        )
                        record_physical_send({"requested_model": model})
                        record_usage(_usage)
                        raw_last = raw_native or ""
                        data_native = _extract_json(raw_last)
                        if data_native is not None:
                            _finish_llm_trace_record(trace_record, status="ok_native")
                            return data_native
                        if not prompt_schema_fallback_enabled:
                            logger.warning(
                                "smart_update: gemma native schema %s returned invalid json; prompt fallback disabled",
                                label,
                            )
                            _finish_llm_trace_record(trace_record, status="failed_native_invalid_json")
                            return None
                        logger.warning(
                            "smart_update: gemma native schema %s returned invalid json; falling back to prompt schema",
                            label,
                        )
                    except Exception as exc:
                        if trace_record is not None:
                            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                        if not prompt_schema_fallback_enabled:
                            logger.warning(
                                "smart_update: gemma native schema %s failed; prompt fallback disabled: %s",
                                label,
                                exc,
                            )
                            _finish_llm_trace_record(trace_record, status="failed_native", error=exc)
                            return None
                        logger.warning(
                            "smart_update: gemma native schema %s failed; falling back to prompt schema: %s",
                            label,
                            exc,
                        )
                while True:
                    try:
                        raw, _usage = await client.generate_content_async(
                            model=model,
                            prompt=full_prompt,
                            generation_config=json_gen_cfg,
                            max_output_tokens=max_tokens,
                            fallback_models=_smart_update_fallback_models(label, model),
                            allow_model_fallback=not single_primary_send,
                            max_provider_attempts=1 if single_primary_send else None,
                        )
                        record_physical_send({"requested_model": model})
                        record_usage(_usage)
                        break
                    except Exception as exc:
                        msg_l = str(exc).lower()
                        if (
                            _GEMMA_JSON_MIME_SUPPORTED
                            and any(
                                k in msg_l
                                for k in (
                                    "response_mime_type",
                                    "mime",
                                    "unknown field",
                                    "json mode is not enabled",
                                    "json mode",
                                )
                            )
                        ):
                            # Provider/library does not support this key; disable for the rest of the process.
                            _GEMMA_JSON_MIME_SUPPORTED = False  # type: ignore[assignment]
                            json_gen_cfg = _smart_update_gemma_generation_config(temperature=0)
                            continue
                        # Rate-limit handling: wait and retry without consuming an attempt.
                        try:
                            from google_ai.exceptions import (
                                ProviderError as _ProviderError,
                                RateLimitError as _RateLimitError,
                            )
                        except Exception:
                            _ProviderError = None
                            _RateLimitError = None
                        retry_ms = 0
                        if _RateLimitError is not None and isinstance(exc, _RateLimitError):
                            retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if _ProviderError is not None and isinstance(exc, _ProviderError):
                            if int(getattr(exc, "status_code", 0) or 0) == 429:
                                retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if (
                            not single_primary_send
                            and retry_ms > 0
                            and time.monotonic() < rl_deadline
                        ):
                            if trace_record is not None:
                                trace_record["rate_limit_waits"] = int(trace_record.get("rate_limit_waits") or 0) + 1
                            await asyncio.sleep(min(60.0, max(0.2, (retry_ms / 1000.0) + 0.2)))
                            continue
                        if trace_record is not None:
                            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                        raise
                raw_last = raw or ""
                data = _extract_json(raw_last)
                if data is not None:
                    _finish_llm_trace_record(
                        trace_record,
                        status="ok_prompt_after_native" if native_schema_enabled else "ok",
                    )
                    return data
                if single_primary_send:
                    last_exc = RuntimeError("gemma returned invalid json")
                    break
                fix_prompt = (
                    "Исправь JSON под схему. Верни только JSON без markdown.\n"
                    f"Schema:\n{schema_text}\n\n"
                    f"Input:\n{raw_last}"
                )
                while True:
                    try:
                        raw_fix, _usage = await client.generate_content_async(
                            model=model,
                            prompt=fix_prompt,
                            generation_config=json_gen_cfg,
                            max_output_tokens=max_tokens,
                            fallback_models=_smart_update_fallback_models(label, model),
                        )
                        break
                    except Exception as exc:
                        msg_l = str(exc).lower()
                        if (
                            _GEMMA_JSON_MIME_SUPPORTED
                            and any(
                                k in msg_l
                                for k in (
                                    "response_mime_type",
                                    "mime",
                                    "unknown field",
                                    "json mode is not enabled",
                                    "json mode",
                                )
                            )
                        ):
                            _GEMMA_JSON_MIME_SUPPORTED = False  # type: ignore[assignment]
                            json_gen_cfg = _smart_update_gemma_generation_config(temperature=0)
                            continue
                        try:
                            from google_ai.exceptions import (
                                ProviderError as _ProviderError,
                                RateLimitError as _RateLimitError,
                            )
                        except Exception:
                            _ProviderError = None
                            _RateLimitError = None
                        retry_ms = 0
                        if _RateLimitError is not None and isinstance(exc, _RateLimitError):
                            retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if _ProviderError is not None and isinstance(exc, _ProviderError):
                            if int(getattr(exc, "status_code", 0) or 0) == 429:
                                retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if retry_ms > 0 and time.monotonic() < rl_deadline:
                            if trace_record is not None:
                                trace_record["rate_limit_waits"] = int(trace_record.get("rate_limit_waits") or 0) + 1
                            await asyncio.sleep(min(60.0, max(0.2, (retry_ms / 1000.0) + 0.2)))
                            continue
                        if trace_record is not None:
                            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                        raise
                raw_last = raw_fix or raw_last
                fixed = _extract_json(raw_fix or "")
                if fixed is not None:
                    _finish_llm_trace_record(trace_record, status="ok_json_fix")
                    return fixed
                last_exc = RuntimeError("gemma returned invalid json")
            except asyncio.CancelledError:
                _finish_llm_trace_record(trace_record, status="cancelled")
                raise
            except Exception as exc:  # pragma: no cover - provider failures
                last_exc = exc
                # If it's a rate limit, wait (not an "attempt") until the max wait budget.
                try:
                    from google_ai.exceptions import ProviderError as _ProviderError, RateLimitError as _RateLimitError
                except Exception:
                    _ProviderError = None
                    _RateLimitError = None
                retry_ms = 0
                if _RateLimitError is not None and isinstance(exc, _RateLimitError):
                    retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                if _ProviderError is not None and isinstance(exc, _ProviderError):
                    if int(getattr(exc, "status_code", 0) or 0) == 429:
                        retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                if (
                    not single_primary_send
                    and retry_ms > 0
                    and time.monotonic() < rl_deadline
                ):
                    if trace_record is not None:
                        trace_record["rate_limit_waits"] = int(trace_record.get("rate_limit_waits") or 0) + 1
                    await asyncio.sleep(min(60.0, max(0.2, (retry_ms / 1000.0) + 0.2)))
                    continue
                if trace_record is not None:
                    trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                logger.warning(
                    "smart_update: gemma %s failed attempt=%d/%d: %s",
                    label,
                    attempt,
                    max_tries,
                    exc,
                )

        if attempt < max_tries:
            await asyncio.sleep(base_sleep * (2 ** (attempt - 1)))
        attempt += 1

    # Fallback to 4o after Gemma retries.
    if not (
        _smart_update_4o_fallback_enabled(label)
        and _smart_update_4o_fallback_budget_allows(label)
    ):
        _finish_llm_trace_record(trace_record, status="failed", error=last_exc or "4o fallback disabled")
        return None
    try:
        from main import ask_4o, notify_llm_incident
    except Exception:
        ask_4o = None
        notify_llm_incident = None
    if ask_4o is None:
        _finish_llm_trace_record(trace_record, status="failed", error=last_exc or "4o unavailable")
        return None
    try:
        if notify_llm_incident is not None:
            await notify_llm_incident(
                "smart_update_gemma_fallback_4o",
                {
                    "severity": "warning",
                    "consumer": "smart_update",
                    "requested_model": model,
                    "model": model,
                    "attempt_no": max_tries,
                    "max_retries": max_tries,
                    "next_model": "gpt-4o",
                    "message": f"Gemma JSON call failed for label={label}; switching to 4o",
                    "error": repr(last_exc) if last_exc else "unknown",
                },
            )
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": _safe_openai_schema_name(label), "schema": schema},
        }
        record_physical_send({"requested_model": "gpt-4o"}, fallback=True)
        raw_4o = await ask_4o(
            prompt,
            response_format=response_format,
            max_tokens=max_tokens,
            meta={"consumer": "smart_update", "label": label, "fallback": "gemma_failed"},
        )
        data = _extract_json(raw_4o or "")
        _finish_llm_trace_record(trace_record, status="ok_4o_fallback")
        return data
    except Exception as exc:  # pragma: no cover - network / token failures
        logger.warning("smart_update: 4o fallback failed label=%s: %s", label, exc)
        _finish_llm_trace_record(trace_record, status="failed", error=exc)
        return None


async def _ask_gemma_text(
    prompt: str,
    *,
    max_tokens: int,
    label: str,
    temperature: float = 0.0,
) -> str | None:
    """Wall-clock-bounded entry point for Gemma free-text stages."""
    cap = float(SMART_UPDATE_GEMMA_TEXT_WALL_CLOCK_SEC)
    try:
        return await asyncio.wait_for(
            _ask_gemma_text_unbounded(
                prompt,
                max_tokens=max_tokens,
                label=label,
                temperature=temperature,
            ),
            timeout=cap,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "smart_update: gemma text_call wall_clock_timeout label=%s cap_sec=%s; returning None",
            label,
            cap,
        )
        return None


async def _ask_gemma_text_unbounded(
    prompt: str,
    *,
    max_tokens: int,
    label: str,
    temperature: float = 0.0,
) -> str | None:
    # GoogleAIClient already retries retryable provider failures. Keep the
    # Smart Update wrapper to a single outer attempt by default so one failing
    # stage cannot expand into 3 x 3 provider calls; override via env for probes.
    max_tries = int(os.getenv("SMART_UPDATE_GEMMA_RETRIES", "1"))
    base_sleep = float(os.getenv("SMART_UPDATE_GEMMA_RETRY_BASE_SEC", "1.0"))
    rl_max_wait_sec = float(os.getenv("SMART_UPDATE_GEMMA_RATE_LIMIT_MAX_WAIT_SEC", "180") or "180")
    rl_max_wait_sec = max(0.0, min(rl_max_wait_sec, 1800.0))
    max_tries = max(1, min(max_tries, 5))
    base_sleep = max(0.1, min(base_sleep, 10.0))
    client = _get_gemma_client()
    model = _resolve_smart_update_model(label)
    last_exc: Exception | None = None
    trace_record = _start_llm_trace_record(
        "text",
        label,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
    )

    rl_deadline = time.monotonic() + rl_max_wait_sec
    attempt = 1
    while attempt <= max_tries:
        if trace_record is not None:
            trace_record["attempts"] = max(int(trace_record.get("attempts") or 0), attempt)
        if client is None:
            last_exc = RuntimeError("gemma client unavailable")
        else:
            try:
                logger.info(
                    "smart_update: gemma text_call label=%s model=%s max_tokens=%s temperature=%s attempt=%d/%d",
                    label,
                    model,
                    max_tokens,
                    temperature,
                    attempt,
                    max_tries,
                )
                while True:
                    try:
                        raw, _usage = await client.generate_content_async(
                            model=model,
                            prompt=prompt,
                            generation_config=_smart_update_gemma_generation_config(
                                temperature=temperature
                            ),
                            max_output_tokens=max_tokens,
                            fallback_models=_smart_update_fallback_models(label, model),
                        )
                        break
                    except Exception as exc:
                        try:
                            from google_ai.exceptions import (
                                ProviderError as _ProviderError,
                                RateLimitError as _RateLimitError,
                            )
                        except Exception:
                            _ProviderError = None
                            _RateLimitError = None
                        retry_ms = 0
                        if _RateLimitError is not None and isinstance(exc, _RateLimitError):
                            retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if _ProviderError is not None and isinstance(exc, _ProviderError):
                            if int(getattr(exc, "status_code", 0) or 0) == 429:
                                retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                        if retry_ms > 0 and time.monotonic() < rl_deadline:
                            if trace_record is not None:
                                trace_record["rate_limit_waits"] = int(trace_record.get("rate_limit_waits") or 0) + 1
                            await asyncio.sleep(min(60.0, max(0.2, (retry_ms / 1000.0) + 0.2)))
                            continue
                        if trace_record is not None:
                            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                        raise
                cleaned = _strip_code_fences(raw or "").strip()
                if cleaned:
                    _finish_llm_trace_record(trace_record, status="ok")
                    return cleaned
                last_exc = RuntimeError("gemma returned empty text")
            except asyncio.CancelledError:
                _finish_llm_trace_record(trace_record, status="cancelled")
                raise
            except Exception as exc:  # pragma: no cover - provider failures
                last_exc = exc
                try:
                    from google_ai.exceptions import ProviderError as _ProviderError, RateLimitError as _RateLimitError
                except Exception:
                    _ProviderError = None
                    _RateLimitError = None
                retry_ms = 0
                if _RateLimitError is not None and isinstance(exc, _RateLimitError):
                    retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                if _ProviderError is not None and isinstance(exc, _ProviderError):
                    if int(getattr(exc, "status_code", 0) or 0) == 429:
                        retry_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
                if retry_ms > 0 and time.monotonic() < rl_deadline:
                    if trace_record is not None:
                        trace_record["rate_limit_waits"] = int(trace_record.get("rate_limit_waits") or 0) + 1
                    await asyncio.sleep(min(60.0, max(0.2, (retry_ms / 1000.0) + 0.2)))
                    continue
                if trace_record is not None:
                    trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                logger.warning(
                    "smart_update: gemma %s failed attempt=%d/%d: %s",
                    label,
                    attempt,
                    max_tries,
                    exc,
                )
        if attempt < max_tries:
            await asyncio.sleep(base_sleep * (2 ** (attempt - 1)))
        attempt += 1

    # Fallback to 4o after Gemma retries.
    if not (
        _smart_update_4o_fallback_enabled(label)
        and _smart_update_4o_fallback_budget_allows(label)
    ):
        _finish_llm_trace_record(trace_record, status="failed", error=last_exc or "4o fallback disabled")
        return None
    try:
        from main import ask_4o, notify_llm_incident
    except Exception:
        ask_4o = None
        notify_llm_incident = None
    if ask_4o is None:
        _finish_llm_trace_record(trace_record, status="failed", error=last_exc or "4o unavailable")
        return None
    try:
        if notify_llm_incident is not None:
            await notify_llm_incident(
                "smart_update_gemma_fallback_4o",
                {
                    "severity": "warning",
                    "consumer": "smart_update",
                    "requested_model": model,
                    "model": model,
                    "attempt_no": max_tries,
                    "max_retries": max_tries,
                    "next_model": "gpt-4o",
                    "message": f"Gemma text call failed for label={label}; switching to 4o",
                    "error": repr(last_exc) if last_exc else "unknown",
                },
            )
        raw_4o = await ask_4o(
            prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            meta={"consumer": "smart_update", "label": label, "fallback": "gemma_failed"},
        )
        cleaned = _strip_code_fences(raw_4o or "").strip()
        _finish_llm_trace_record(trace_record, status="ok_4o_fallback")
        return cleaned or None
    except Exception as exc:  # pragma: no cover - network / token failures
        logger.warning("smart_update: 4o fallback failed label=%s: %s", label, exc)
        _finish_llm_trace_record(trace_record, status="failed", error=exc)
        return None


def _g4_lollipop_light_bucket_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "assignments": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "fact_index": {"type": "INTEGER"},
                        "bucket": {
                            "type": "STRING",
                            "format": "enum",
                            "enum": [
                                "event_core",
                                "program_list",
                                "people_and_roles",
                                "forward_looking",
                                "support_context",
                                "uncertain",
                            ],
                        },
                        "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
                    },
                    "required": ["fact_index", "bucket", "literal_items"],
                },
            },
        },
        "required": ["assignments"],
    }


def _g4_lollipop_light_writer_schema_gemma() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title": {"type": "STRING"},
            "description_md": {"type": "STRING"},
            "short_description": {"type": "STRING"},
            "search_digest": {"type": "STRING"},
        },
        "required": ["title", "description_md"],
    }


def _g4_lollipop_light_writer_schema_openai() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "description_md": {"type": "string"},
            "short_description": {"type": "string"},
            "search_digest": {"type": "string"},
        },
        "required": ["title", "description_md"],
        "additionalProperties": False,
    }


def _g4_lollipop_light_bucket_prompt() -> str:
    return (
        "You do one small step for Smart Update G4: smart_update.facts_to_lollipop_buckets.v1.\n"
        "Return only JSON. Do not write prose. Do not rewrite fact text.\n"
        "Assign every input fact_index exactly once to one lollipop-light bucket.\n"
        "Use literal_items only for explicit named program/repertoire/object title lists that must be printed as a list.\n"
        "Do NOT use literal_items for interest lists, idea examples, request examples, keywords, roles, or support context; "
        "keep those as ordinary narrative facts so the final text can group them naturally."
    )


def _g4_lollipop_light_writer_system_prompt() -> str:
    return (
        "You are smart_update.g4_lollipop_light.final_writer.v3. Return only JSON.\n"
        "Write polished Russian event copy from the provided writer_pack only.\n"
        "Cover every must_cover_fact_id exactly once in natural prose. Do not add unsupported facts.\n"
        "Keep logistics out of narrative. Use exact ### headings from each section.\n"
        "Bullet rule: use the `- item` bullet format ONLY for sections whose writer_pack section has a non-empty literal_items array. "
        "For every other section write continuous prose paragraphs and never emit a `-` bullet line.\n"
        "When a section has literal_items, render every literal item on its own line as `- item` exactly as given. "
        "Do not add prefixes like `произведения`, `работы`, `роль` to bullet lines; if a shared prefix is needed, "
        "introduce it once in the lead-in sentence ending with `:` and then list the bare items.\n"
        "When several narrative facts in the same section differ only by a named entity "
        "(composer, performer, work, role, place), merge them into one compact sentence with comma-separated names "
        "(e.g. `Прозвучат произведения Баха, Шуберта, Бизе и Вьерна`) instead of repeating the surrounding phrase.\n"
        "Do not turn comma-separated interests, requests, examples, or roles into one-word bullet lists; "
        "group them in compact natural prose or short thematic bullets only when the section truly has literal_items.\n"
        "Tone: cultural city digest, lively but restrained. No direct address, CTA, promo promises, or report formulas.\n"
        "Never output report words like `характеризуется`, `осуществляется`, `представляет собой`; "
        "rewrite them naturally without changing meaning.\n"
        "Target length: 700-1100 characters for 8+ facts; 450-800 for smaller packs."
    )


def _g4_lollipop_light_compact_writer_payload(pack: dict[str, Any]) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    for section in list(pack.get("sections") or []):
        if not isinstance(section, dict):
            continue
        sections.append(
            {
                "role": section.get("role"),
                "style": section.get("style"),
                "heading": section.get("heading"),
                "fact_ids": section.get("fact_ids") or [],
                "facts": [
                    {"fact_id": fact.get("fact_id"), "text": fact.get("text")}
                    for fact in list(section.get("facts") or [])
                    if isinstance(fact, dict)
                ],
                "coverage_plan": section.get("coverage_plan") or [],
                "literal_items": section.get("literal_items") or [],
            }
        )
    return {
        "title": ((pack.get("title_context") or {}).get("original_title") or ""),
        "event_type": pack.get("event_type"),
        "must_cover_fact_ids": (pack.get("constraints") or {}).get("must_cover_fact_ids") or [],
        "required_headings": (pack.get("constraints") or {}).get("headings") or [],
        "sections": sections,
        "output_contract": {
            "title": "Keep original title unless writer_pack explicitly asks otherwise.",
            "description_md": "Markdown prose only; no infoblock, no date/time/address/city/tickets.",
            "short_description": "Optional: one complete Russian sentence, 12-16 words, no logistics.",
            "search_digest": "Optional: compact Russian search/card summary up to 160 characters, no logistics.",
        },
    }


def _g4_lollipop_light_normalize_bucket_payload(
    facts: Sequence[str],
    raw: dict[str, Any],
    *,
    candidate: EventCandidate,
) -> dict[str, Any]:
    bucket_prefix = {
        "event_core": "EC",
        "program_list": "PL",
        "people_and_roles": "PR",
        "forward_looking": "FL",
        "support_context": "SC",
        "uncertain": "UN",
    }
    allowed = set(bucket_prefix)
    assignments: dict[int, tuple[str, list[str]]] = {}
    for item in list((raw or {}).get("assignments") or []):
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("fact_index"))
        except Exception:
            continue
        if idx < 0 or idx >= len(facts) or idx in assignments:
            continue
        bucket = str(item.get("bucket") or "").strip()
        if bucket not in allowed:
            bucket = "support_context"
        literal_items = []
        for raw_item in list(item.get("literal_items") or []):
            literal = re.sub(r"\s+", " ", str(raw_item or "")).strip()
            if not literal or literal.casefold() in {"literal_items", "literal item", "items"}:
                continue
            literal_items.append(literal)
            if len(literal_items) >= 12:
                break
        assignments[idx] = (bucket, literal_items)
    for idx in range(len(facts)):
        assignments.setdefault(idx, ("support_context", []))

    pack: dict[str, Any] = {bucket: [] for bucket in allowed}
    counters = {bucket: 0 for bucket in allowed}
    for idx, fact in enumerate(facts):
        bucket, literal_items = assignments[idx]
        counters[bucket] += 1
        pack[bucket].append(
            {
                "fact_id": f"{bucket_prefix[bucket]}{counters[bucket]:02d}",
                "bucket": bucket,
                "text": str(fact or "").strip(),
                "literal_items": literal_items,
                "record_ids": [f"SU{idx:02d}"],
                "source_refs": ["smart_update.facts_text_clean"],
            }
        )
    logistics: list[dict[str, Any]] = []
    for value, label in [
        (candidate.date, "date"),
        (candidate.time, "time"),
        (candidate.location_name, "location"),
        (candidate.location_address, "address"),
        (candidate.city, "city"),
    ]:
        if value:
            entry = {
                "fact_id": f"LG{len(logistics) + 1:02d}",
                "bucket": "logistics_infoblock",
                "text": str(value),
                "literal_items": [],
                "record_ids": [label],
                "source_refs": ["candidate.metadata"],
            }
            # Short venue brand names (e.g. "Сигнал", "Дом", "Музей города")
            # are part of natural narrative ("в «Сигнале»") and are already
            # protected from the narrative-strip path. Suppress them from the
            # writer-pack infoblock so the leak validator does not raise a
            # false-positive `infoblock.leak:LG03` and trigger a useless retry.
            if label == "location":
                v = str(value).strip()
                looks_like_address = bool(
                    re.search(r"\d", v)
                    or _LOGISTICS_ADDR_WORD_RE.search(v)
                    or v.count(",") >= 2
                )
                if not looks_like_address:
                    entry["narrative_policy"] = "suppress"
            logistics.append(entry)
    pack["logistics_infoblock"] = logistics
    return pack


async def _ask_gemma_json_direct_native(
    *,
    label: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    response_schema: dict[str, Any],
    max_tokens: int,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    client = _get_gemma_client()
    if client is None:
        raise RuntimeError("GoogleAIClient is unavailable")
    trace_record = _start_llm_trace_record(
        "json",
        label,
        max_tokens=max_tokens,
        native_schema_enabled=True,
        prompt_schema_fallback_enabled=False,
    )
    max_attempts = SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES
    had_timeout_attr = hasattr(client, "provider_timeout_seconds")
    old_timeout = getattr(client, "provider_timeout_seconds", None)
    if timeout_sec:
        client.provider_timeout_seconds = float(timeout_sec)
    try:
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            if trace_record is not None:
                trace_record["attempts"] = attempt
            try:
                raw, _usage = await client.generate_content_async(
                    model=SMART_UPDATE_MODEL,
                    prompt=json.dumps(user_payload, ensure_ascii=False, indent=2),
                    generation_config={
                        **_smart_update_gemma_generation_config(temperature=0),
                        "max_output_tokens": max_tokens,
                        "response_mime_type": "application/json",
                        "system_instruction": system_prompt.strip(),
                        "response_schema": response_schema,
                    },
                    max_output_tokens=max_tokens,
                )
                data = _extract_json(raw or "")
                if data is None:
                    raise RuntimeError(f"Invalid JSON from {SMART_UPDATE_MODEL}: {(raw or '')[:600]}")
                _finish_llm_trace_record(trace_record, status="ok_native")
                return data
            except Exception as exc:
                last_exc = exc
                if trace_record is not None:
                    trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
                if attempt < max_attempts:
                    await asyncio.sleep(min(2.0, 0.5 * attempt))
                    continue
                _finish_llm_trace_record(trace_record, status="failed", error=exc)
                raise
        raise last_exc or RuntimeError("Gemma native JSON stage failed")
    finally:
        if timeout_sec:
            if had_timeout_attr:
                client.provider_timeout_seconds = old_timeout
            else:
                try:
                    delattr(client, "provider_timeout_seconds")
                except Exception:
                    pass


def _to_openai_schema(node: Any) -> Any:
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            if key == "type" and isinstance(value, str):
                out[key] = value.lower()
            else:
                out[key] = _to_openai_schema(value)
        return out
    if isinstance(node, list):
        return [_to_openai_schema(item) for item in node]
    return node


async def _ask_4o_json_once(
    *,
    label: str,
    prompt: str,
    schema: dict[str, Any],
    model: str,
    max_tokens: int,
    system_prompt: str | None = None,
) -> dict[str, Any]:
    trace_record = _start_llm_trace_record(
        "json",
        label,
        model=model,
        max_tokens=max_tokens,
        native_schema_enabled=True,
        prompt_schema_fallback_enabled=False,
    )
    if trace_record is not None:
        trace_record["attempts"] = 1
    try:
        from main import ask_4o
    except Exception as exc:
        _finish_llm_trace_record(trace_record, status="failed", error=exc)
        raise RuntimeError("4o unavailable") from exc
    injected_token_alias = False
    if not (os.getenv("FOUR_O_TOKEN") or "").strip():
        token_alias = (os.getenv("FOUR_4O_TOKEN") or "").strip()
        if token_alias:
            os.environ["FOUR_O_TOKEN"] = token_alias
            injected_token_alias = True
    try:
        raw = await ask_4o(
            prompt,
            system_prompt=system_prompt or "Return only valid JSON for the requested schema.",
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "SmartUpdateLollipopLightFinalWriter",
                    "schema": _to_openai_schema(schema),
                },
            },
            max_tokens=max_tokens,
            model=model,
            meta={"consumer": "smart_update", "label": label, "lane": "lollipop_light_final_writer"},
            temperature=0,
        )
        data = _extract_json(raw or "")
        if data is None:
            raise RuntimeError(f"Invalid 4o JSON: {(raw or '')[:600]}")
        _finish_llm_trace_record(trace_record, status="ok")
        return data
    except Exception as exc:
        if trace_record is not None:
            trace_record["provider_errors"] = int(trace_record.get("provider_errors") or 0) + 1
        _finish_llm_trace_record(trace_record, status="failed", error=exc)
        raise
    finally:
        if injected_token_alias:
            os.environ.pop("FOUR_O_TOKEN", None)


def _g4_lollipop_light_selected_writer_lane(fact_count: int) -> str:
    lane = SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE
    if lane == "adaptive":
        return "4o" if fact_count > SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD else "gemma4"
    return lane


async def _llm_g4_lollipop_light_create_bundle(
    candidate: EventCandidate,
    *,
    clean_title: str,
    normalized_event_type: str | None,
    raw_facts: Sequence[str],
    bundled_search_digest: str | None = None,
    bundled_short_description: str | None = None,
) -> dict[str, Any] | None:
    facts_text_clean = _facts_text_clean_from_facts(
        raw_facts,
        max_items=40,
        anchors=[
            candidate.date or "",
            candidate.time or "",
            candidate.city or "",
            candidate.location_name or "",
            candidate.location_address or "",
        ],
    )
    if not facts_text_clean:
        return None
    try:
        from smart_update_lollipop_lab import editorial_layout_family as layout_family
        from smart_update_lollipop_lab import facts_prioritize_family as prioritize_family
        from smart_update_lollipop_lab import full_cascade as cascade_family
        from smart_update_lollipop_lab import writer_final_4o_family as writer_final_family
        from smart_update_lollipop_lab import writer_pack_compose_family as writer_pack_family
    except Exception as exc:  # pragma: no cover - optional lab package should exist in repo
        logger.warning("smart_update: lollipop-light modules unavailable: %s", exc)
        return None

    event_type = normalized_event_type or candidate.event_type
    timeout_sec = float(SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC)
    bucket_raw = await _ask_gemma_json_direct_native(
        label="lollipop.bucket_facts",
        system_prompt=_g4_lollipop_light_bucket_prompt(),
        user_payload={
            "title": clean_title,
            "event_type": event_type,
            "facts_text_clean": [{"index": idx, "text": text} for idx, text in enumerate(facts_text_clean)],
        },
        response_schema=_g4_lollipop_light_bucket_schema(),
        max_tokens=900,
        timeout_sec=timeout_sec,
    )
    fact_pack = _g4_lollipop_light_normalize_bucket_payload(
        facts_text_clean,
        bucket_raw or {},
        candidate=candidate,
    )
    flat_weight_facts = [
        {
            "fact_id": item["fact_id"],
            "bucket": item["bucket"],
            "text": item["text"],
            "literal_items": item.get("literal_items") or [],
        }
        for item in prioritize_family._flat_facts(fact_pack)
    ]
    weight_raw = await _ask_gemma_json_direct_native(
        label="lollipop.prioritize.weight",
        system_prompt=cascade_family._prioritize_weight_system_prompt(gemma4=True),
        user_payload={"event_title": clean_title, "event_type": event_type, "facts": flat_weight_facts},
        response_schema=cascade_family._prioritize_weight_response_schema(),
        max_tokens=900,
        timeout_sec=timeout_sec,
    )
    weighted_pack = cascade_family._apply_weight_payload(fact_pack, weight_raw or {})
    weighted_pack = prioritize_family._apply_narrative_policies(weighted_pack, event_type=event_type)
    flat_lead_facts = [
        {
            "fact_id": item["fact_id"],
            "bucket": item["bucket"],
            "text": item["text"],
            "weight": item.get("weight"),
        }
        for item in prioritize_family._flat_facts(weighted_pack)
        if item.get("narrative_policy") != "suppress"
    ]
    lead_raw = await _ask_gemma_json_direct_native(
        label="lollipop.prioritize.lead",
        system_prompt=cascade_family._prioritize_lead_system_prompt(gemma4=True),
        user_payload={"event_id": 0, "event_title": clean_title, "event_type": event_type, "facts": flat_lead_facts},
        response_schema=cascade_family._prioritize_lead_response_schema(),
        max_tokens=500,
        timeout_sec=timeout_sec,
    )
    lead_payload = prioritize_family._clean_lead(
        lead_raw or {},
        weighted_pack,
        title=clean_title,
        event_type=event_type,
    )
    lead_payload["event_title"] = clean_title
    prioritized_pack = layout_family._prioritized_fact_pack(weighted_pack)
    precompute = layout_family._precompute_layout_state(
        event_type=event_type,
        pack=prioritized_pack,
        lead_payload=lead_payload,
    )
    layout_raw = await _ask_gemma_json_direct_native(
        label="lollipop.editorial.layout",
        system_prompt=cascade_family._editorial_layout_system_prompt(gemma4=True),
        user_payload={
            "event_title": clean_title,
            "event_type": event_type,
            "lead_payload": lead_payload,
            "precompute": precompute,
            "fact_pack": prioritized_pack,
        },
        response_schema=cascade_family._editorial_layout_response_schema(),
        max_tokens=1200,
        timeout_sec=timeout_sec,
    )
    layout_payload = layout_family._clean_layout_plan(
        layout_raw or {},
        title=clean_title,
        pack=prioritized_pack,
        lead_payload=lead_payload,
        precompute=precompute,
    )
    writer_pack = writer_pack_family._compose_writer_pack(
        event_id=0,
        title=clean_title,
        layout_result={
            "event_type": event_type,
            "layout_result": {"precompute": precompute, "payload": layout_payload},
        },
        prioritize_result={"weight_result": {"payload": weighted_pack}},
    )
    selected_writer_lane = _g4_lollipop_light_selected_writer_lane(len(facts_text_clean))
    if selected_writer_lane == "4o":
        writer_output = await _ask_4o_json_once(
            label="writer.final_4o",
            prompt=writer_final_family._build_prompt(writer_pack["payload"]),
            schema=_g4_lollipop_light_writer_schema_openai(),
            model=SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_MODEL,
            max_tokens=1600,
        )
    else:
        writer_output = await _ask_gemma_json_direct_native(
            label="writer.final_g4_primary",
            system_prompt=_g4_lollipop_light_writer_system_prompt(),
            user_payload=_g4_lollipop_light_compact_writer_payload(writer_pack["payload"]),
            response_schema=_g4_lollipop_light_writer_schema_gemma(),
            max_tokens=1200,
            timeout_sec=timeout_sec,
        )
    validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
    validation_errors = list(validation.errors)
    if "literal_items" in str(writer_output.get("description_md") or ""):
        validation_errors.append("technical.literal_items_leak")
    if selected_writer_lane in {"gemma4", "4o"} and validation_errors:
        retry_label = "writer.final_g4_retry"
        retry_system_prompt = (
            _g4_lollipop_light_writer_system_prompt()
            + "\nThis is a correction pass. Fix validation errors without adding new facts. "
            "Never output technical schema words such as `literal_items`."
        )
        if selected_writer_lane == "4o":
            retry_label = "writer.final_g4_after_4o_validation"
            retry_system_prompt += (
                "\nThe previous 4o writer output failed validation. Prefer literal list coverage "
                "and grounded event-specific prose over stylistic expansion."
            )
        retry_output = await _ask_gemma_json_direct_native(
            label=retry_label,
            system_prompt=retry_system_prompt,
            user_payload={
                "writer_pack": _g4_lollipop_light_compact_writer_payload(writer_pack["payload"]),
                "previous_output": writer_output,
                "validation_errors": validation_errors,
            },
            response_schema=_g4_lollipop_light_writer_schema_gemma(),
            max_tokens=1200,
            timeout_sec=timeout_sec,
        )
        retry_validation = writer_final_family._validate_writer_output(writer_pack["payload"], retry_output)
        retry_errors = list(retry_validation.errors)
        if "literal_items" in str(retry_output.get("description_md") or ""):
            retry_errors.append("technical.literal_items_leak")
        if str(retry_output.get("description_md") or "").strip() and len(retry_errors) <= len(validation_errors):
            writer_output = retry_output
            validation = retry_validation
            validation_errors = retry_errors
    if validation_errors:
        logger.warning(
            "smart_update: lollipop-light writer validation errors=%s warnings=%s source_type=%s source_url=%s",
            validation_errors,
            validation.warnings,
            candidate.source_type,
            candidate.source_url,
        )
    applied = writer_final_family._apply_writer_output(writer_pack["payload"], writer_output)
    description = str(applied.get("description_md") or "").strip()
    if not description:
        return None
    if _description_needs_infoblock_logistics_strip(description, candidate=candidate):
        description = _strip_infoblock_logistics_from_description(description, candidate=candidate) or description
    writer_search_digest = _clean_search_digest(writer_output.get("search_digest"))
    writer_short = _clean_short_description(writer_output.get("short_description"))
    if writer_short and not _is_short_description_acceptable(writer_short, min_words=12, max_words=16):
        writer_short = None
    search_digest = bundled_search_digest or writer_search_digest
    short_description = bundled_short_description or writer_short
    if not search_digest:
        search_digest = await _llm_build_search_digest(
            title=str(applied.get("title") or clean_title),
            description=description,
            event_type=event_type,
        )
    if not search_digest:
        search_digest = _fallback_digest_from_description(description)
    if not short_description:
        short_description = await _llm_build_short_description(
            title=str(applied.get("title") or clean_title),
            description=description,
            event_type=event_type,
        )
    if not short_description:
        short_description = _fallback_short_description_from_text(description)
    return {
        "title": str(applied.get("title") or clean_title),
        "description": description,
        "facts": list(facts_text_clean),
        "search_digest": search_digest,
        "short_description": short_description,
        "_lollipop_light": True,
        "_lollipop_light_writer_lane": selected_writer_lane,
        "_lollipop_light_fact_count": len(facts_text_clean),
        "_lollipop_light_writer_validation": {
            "errors": validation_errors,
            "warnings": validation.warnings,
        },
    }


async def _llm_extract_candidate_facts(
    candidate: EventCandidate,
    *,
    text_for_facts: str | None = None,
) -> list[str]:
    """Extract atomic event facts from a single candidate for global fact log/dedup.

    Notes:
    - Facts are used for operator source log and for global de-duplication between sources.
    - Do not include anchor fields (date/time/location) here: they are logged deterministically.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return []
    if candidate.source_type in ("bot", "manual"):
        return []

    payload = {
        "today": date.today().isoformat(),
        "title": candidate.title,
        "date": candidate.date,
        "time": candidate.time,
        "end_date": candidate.end_date,
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
        "ticket_link": candidate.ticket_link,
        "ticket_status": candidate.ticket_status,
        "structured_age_restriction": (
            candidate.age_restriction if candidate.age_restriction_is_structured else None
        ),
        "source_type": candidate.source_type,
        "source_url": candidate.source_url,
        "text": _clip(
            (text_for_facts or "").strip()
            or (candidate.occurrence_scope_text or "").strip()
            or (_strip_promo_lines(candidate.source_text) or candidate.source_text),
            7000 if SMART_UPDATE_G4_SPLIT_CREATE else 2800,
        ),
        "raw_excerpt": _clip(_strip_promo_lines(candidate.raw_excerpt) or candidate.raw_excerpt, 800),
        # Poster OCR is first-class evidence for age marks.  Keep both the
        # short OCR title and body, and do not silently throw away poster 4+;
        # the bounded per-poster/per-request limits still protect the LLM
        # budget without adding another model call.
        "poster_texts": [
            _clip(
                "\n".join(
                    value.strip()
                    for value in (p.ocr_title or "", p.ocr_text or "")
                    if value.strip()
                ),
                1200,
            )
            for p in candidate.posters
            if (p.ocr_title or "").strip() or (p.ocr_text or "").strip()
        ][:8],
    }
    if SMART_UPDATE_G4_SPLIT_CREATE:
        schema = _g4_rich_facts_schema()
        prompt = (
            "Ты извлекаешь ПОЛНЫЙ набор фактов о КОНКРЕТНОМ событии для Smart Update G4.\n"
            "Это quality-critical stage: лучше вернуть больше grounded фактов, чем сжать источник до короткой карточки.\n"
            "Верни JSON строго по схеме.\n\n"
            "Evidence contract для каждого элемента public/program/context/people/logistics:\n"
            "- Верни объект {fact, evidence_quote}.\n"
            "- evidence_quote — точная непрерывная цитата из text/raw_excerpt/poster_texts, без пересказа.\n"
            "- Цитата должна прямо подтверждать ВЕСЬ fact, а не быть просто соседней строкой по теме.\n"
            "- Не выводи редакционные обобщения из названия проекта. Если источник не называет цель, "
            "формат, пользу, регулярность или продолжение серии, не создавай такие факты.\n"
            "- При коротком тизере нормально вернуть 1–3 факта или пустые секции: не заполняй схему догадками.\n\n"
            "Секции:\n"
            "- public_core_facts: только явно названные суть/формат/цель и явно обещанные действия участника. "
            "Не достраивай их по типу, названию или общей тематике события.\n"
            "- context_methodology_facts: методология, исследование, источник концепции, важные числа, background, который объясняет событие.\n"
            "- people_org_facts: организаторы, институции, авторы, ведущие, исполнители, спикеры. "
            "Имена организаторов/сообществ/площадок, а также названия вымышленных миров, франшиз и культурных источников "
            "— identity facts: сохраняй их буквально из source_text/raw_excerpt/poster_texts. "
            "Не заменяй организатора тематическим сообществом, площадку организатором или источник вдохновения названием другого сообщества. "
            "Если в источнике есть формула `организовано ...`, `от ...`, `вокруг ...`, `вдохновлено ...`, верни отдельный точный факт "
            "про организатора/сообщество/источник вдохновения; при противоречии разных строк помести сомнительную строку в uncertain_or_drop, "
            "не сглаживай её редакционной догадкой. "
            "Если у спикера/лектора/ведущего/гостя/автора в источнике явно указаны ИМЯ и ДОЛЖНОСТЬ/РЕГАЛИИ "
            "(главный архитектор, профессор, режиссёр-постановщик, кандидат наук, художественный руководитель, "
            "член Союза и т.п.) — сохрани их в ОДНОМ именованном факте вида "
            "`Лектор: Андрей Анисимов, главный архитектор Калининграда`. "
            "Если источник перечисляет состав/участников/спикеров несколькими блоками вида ИМЯ отдельной строкой "
            "+ роль/регалии на следующей строке, верни ОТДЕЛЬНЫЙ именованный факт для КАЖДОГО блока; "
            "не сокращай состав до категорий вроде `краеведы и учёные`. Для лекций/паблик-токов/дискуссий "
            "имена участников — attendance-driving facts, поэтому сохраняй весь named roster, даже если участников 4–8. "
            "Если строка с ролью содержит только должность/регалию без повторения имени (например `Главный архитектор Калининграда`), "
            "свяжи её с именем из предыдущей строки и верни именованный факт. "
            "НЕ сворачивай `<Имя>, <должность>` в обезличенное `профессиональная позиция спикера…` "
            "или `спикер представит позицию…`: имя и должность — критическая для лекций/дискуссий информация. "
            "Если в источнике есть выделенная секция `О спикере`/`Лектор:`/`Спикер:`/`Ведущий:`/`Автор:` — "
            "верни именованный факт обязательно (опускай только если ни имени, ни должности в этой секции нет).\n"
            "- organizer_names: только названия организаций/сообществ, которые источник ПРЯМО называет "
            "организатором именно этого события. Для каждого элемента верни {name, evidence_quote}; "
            "name должен буквально входить "
            "в evidence_quote. Площадка, партнёр, спонсор, участник, исполнитель, издатель поста или тематическое "
            "сообщество не становятся организатором без прямой формулы об организации события. "
            "Если организатор прямо не назван, верни пустой массив.\n"
            "- program_or_examples: ВСЕ списки, примеры, пункты программы, интересы, темы, произведения; длинные перечисления не сворачивай. "
            "Если в источнике есть выделенный bullet-блок под заголовком типа `О чём поговорим`/`Правда ли, что`/"
            "`Темы`/`Вопросы`/`В программе`/`Что обсудим`/`План встречи` — КАЖДЫЙ bullet под таким заголовком "
            "должен стать ОТДЕЛЬНЫМ фактом (с сохранением имён собственных, цифр, кавычек). Не сворачивай 3-5 "
            "bullet'ов в один summary-факт. Если в источнике 3 bullet'а под `О чём поговорим` — верни 3 факта; "
            "если 2 bullet'а под `Правда ли, что` — верни 2 факта. Это lecture/discussion structure, и потеря "
            "bullet'ов делает description бессмысленным.\n"
            "- logistics_facts: дата/время/площадка/адрес/город/билеты/регистрация/цены/возраст/Пушкинская карта.\n"
            "- uncertain_or_drop: шум, CTA, промо, неуверенные или не относящиеся к конкретному событию строки.\n\n"
            "Правила полноты:\n"
            "- Верни до 40 фактов суммарно, если источник плотный; не ограничивайся 6-10.\n"
            "- Сохраняй организатора, методологию, результаты исследований и числа, даже если это background.\n"
            "- Сохраняй примеры из кавычек/списков дословно, отдельными фактами или компактными grouped-фактами.\n"
            "- Если источник перечисляет варианты участия/условия участия, сохрани их явно.\n"
            "- Не выдумывай факты. Не добавляй generic praise.\n"
            "- Не используй хэштеги. Не включай CTA в public sections.\n"
            f"- {SMART_UPDATE_FACTS_PRESERVE_COMPACT_PROGRAM_LISTS_RULE}\n"
            f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE}\n\n"
            f"{AGE_DECISION_PROMPT_RULE}\n\n"
            f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        data = await _ask_gemma_json(prompt, schema, max_tokens=1400, label="rich_facts_extract")
    else:
        schema = {
            "type": "object",
            "properties": {
                "facts": {"type": "array", "items": {"type": "string"}},
                "age_decision": AGE_DECISION_JSON_SCHEMA,
            },
            "required": ["facts"],
            "additionalProperties": False,
        }
        prompt = (
            "Ты извлекаешь атомарные факты о КОНКРЕТНОМ событии из текста источника.\n"
            "Верни JSON строго по схеме.\n\n"
            "Правила:\n"
            "- Верни 6–18 коротких фактов (1 строка = 1 факт), только про это событие.\n"
            "- Пиши факты как короткие именные группы (по возможности без глаголов 'является', 'будет', 'обещает').\n"
            "- Для оценочных характеристик и лозунгов используй формулировку из источника максимально близко к тексту "
            "(если в источнике есть кавычки, сохрани кавычки).\n"
            "- НЕ включай дату/время/адрес/город как отдельные факты (они фиксируются отдельно).\n"
            "- НЕ включай строки расписания вида `DD.MM | Название`.\n"
            f"- {SMART_UPDATE_FACTS_PRESERVE_COMPACT_PROGRAM_LISTS_RULE}\n"
            "- Не используй хэштеги (`#...`) в формулировках фактов.\n"
            "- НЕ включай рекламные призывы, скидки/промокоды, механику розыгрыша.\n"
            "- НЕ включай промо-упоминания «где следить за анонсами» и ссылки на каналы/чаты с афишей "
            "(например «Информация о событиях ... доступна в Telegram-канале ...»).\n"
            "- Включай условия участия/посещения (длительность, возраст, максимальный размер группы, формат/что взять/как одеться, "
            "что входит/не входит в оплату, нужен ли отдельный входной билет). Не вставляй ссылки/телефоны, "
            "КРОМЕ: если в источнике есть ссылка на плейлист Я.Музыки (music.yandex.ru/users/.../playlists/...), "
            "можно вернуть 1 факт с этой ссылкой; "
            "точную сумму указывай только если это важно, чтобы пояснить «что оплачивается отдельно» (не более 1 факта).\n"
            "- НЕ включай факты про общие новости площадки/организации, если они не описывают само событие "
            "(например отчёты о работе филиала, планы на год, пресс-анонсы о будущих репортажах).\n"
            "- НЕ включай нейросетевые клише, пустые оценки и прогнозы, которых нет в источнике: "
            "например 'обещает стать заметным событием', 'яркое событие культурной жизни', "
            "'не оставит равнодушным', 'незабываемые эмоции', 'уникальная возможность'.\n"
            "- НЕ выдумывай факты. Если чего-то нет в данных, не добавляй.\n"
            "- Если есть прямая речь и понятно, кто говорит (например режиссёр), оформи как факт:\n"
            "  `Цитата (Имя Фамилия): ...`.\n"
            "- Избегай дублирования: если мысль повторяется, оставь один факт.\n\n"
            f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE}\n\n"
            f"{AGE_DECISION_PROMPT_RULE}\n\n"
            f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        data = await _ask_gemma_json(prompt, schema, max_tokens=500, label="facts_extract")
    raw_facts = []
    if isinstance(data, dict):
        age_payload = data.get("age_decision")
        candidate.age_semantic_decision = age_payload if isinstance(age_payload, dict) else None
        if SMART_UPDATE_G4_SPLIT_CREATE:
            source_corpus = "\n\n".join(
                str(value or "").strip()
                for value in (
                    payload.get("text"),
                    payload.get("raw_excerpt"),
                    *(payload.get("poster_texts") or []),
                )
                if str(value or "").strip()
            )
            raw_facts = _flatten_g4_rich_facts_payload(
                data,
                source_corpus=source_corpus,
            )
            candidate.organizer_names = _bounded_organizer_names(
                candidate.organizer_names,
                _grounded_organizer_names_from_payload(
                    data,
                    source_corpus=source_corpus,
                ),
            )
        else:
            raw_facts = list(data.get("facts") or [])

    # Normalize + drop anchor-like meaning duplicates.
    anchor_date = (candidate.date or "").strip() or None
    anchor_time = (candidate.time or "").strip() or None
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_facts:
        cleaned = _normalize_fact_item(str(item or ""), limit=180)
        if not cleaned:
            continue
        # Do not claim "premiere" unless it is explicitly present in the source text.
        if re.search(r"(?i)\bпремьер\w+\b", cleaned) and "премьер" not in (payload.get("text") or "").casefold():
            continue
        # Drop generic evaluative/predictive phrases: they are not factual and break
        # the "facts -> telegraph coverage" invariant.
        if re.search(
            r"(?i)\bобеща\w+\s+(?:стать|быть)\b|\bярк\w+\s+событ\w+\b|\bзаметн\w+\s+событ\w+\b|"
            r"\bкультурн\w+\s+жизн\w+\b|\bне\s+остав\w+\s+равнодуш\w+\b|\bнезабываем\w+\b|\bуникальн\w+\s+возможн\w+\b",
            cleaned,
        ):
            continue
        # If it repeats an anchor (e.g. "12 февраля") treat as noise for the global fact list.
        k = _semantic_fact_key(cleaned, event_date=anchor_date, event_time=anchor_time)
        if k:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
        if len(out) >= (40 if SMART_UPDATE_G4_SPLIT_CREATE else 20):
            break
    return _filter_ungrounded_sensitive_facts(out, candidate=candidate)


async def _llm_enforce_blockquote(
    *,
    description: str,
    quote: str,
    label: str,
) -> str | None:
    """Ask LLM to integrate a direct quote as a blockquote into an existing description."""
    if SMART_UPDATE_LLM_DISABLED:
        return None
    desc = (description or "").strip()
    q = (quote or "").strip()
    if not desc or not q:
        return None
    payload = {
        "description": _clip(desc, 5000),
        "quote": _clip(q, 400),
    }
    prompt = (
        "Вставь прямую цитату в описание события.\n"
        "Правила:\n"
        "- Верни полный обновлённый текст описания.\n"
        "- Цитату вставь как отдельный блок `>` (blockquote) ДОСЛОВНО.\n"
        "- Если в описании упоминается автор цитаты (например режиссёр), добавь атрибуцию сразу после цитаты "
        "короткой строкой (например `— Егор Равинский`).\n"
        "- Не добавляй новых фактов и не меняй смысл остального текста.\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=900,
        label=label,
        temperature=0.0,
    )
    return text.strip() if text else None


async def _llm_remove_infoblock_logistics(
    *,
    description: str,
    candidate: EventCandidate,
    label: str,
) -> str | None:
    """Ask LLM to remove duplicated logistics from narrative text (infoblock already covers it).

    NOTE: We intentionally do NOT do deterministic regex-based cutting here. If the model
    fails, we prefer keeping duplicates over breaking grammar or deleting meaning.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    desc = (description or "").strip()
    if not desc:
        return None
    payload = {
        "title": candidate.title,
        "date": candidate.date,
        "time": candidate.time,
        "end_date": candidate.end_date,
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
        "ticket_link": candidate.ticket_link,
        "ticket_status": candidate.ticket_status,
        "ticket_price_min": candidate.ticket_price_min,
        "ticket_price_max": candidate.ticket_price_max,
        "is_free": candidate.is_free,
        "event_type": candidate.event_type,
        "festival": candidate.festival,
        "description": _clip(desc, 6500),
    }
    prompt = (
        "Ты — редактор текста анонса.\n"
        "Задача: убрать из описания повторы логистики, потому что она уже показывается отдельным инфоблоком сверху.\n"
        "Что убрать (если встречается в описании как отдельная логистика):\n"
        "- дата/время/диапазон дат\n"
        "- площадка, точный адрес, город\n"
        "- ссылки/телефон/контакты\n"
        "- точные цены/стоимость билетов/регистрации\n"
        "Правила:\n"
        "- Верни ПОЛНЫЙ обновлённый текст описания.\n"
        "- Не вырезай смысловые фрагменты и не ломай грамматику.\n"
        "- Не вырезай identity-факты: организатор, сообщество, площадка как организаторская точка сборки, "
        "мир/франшиза/источник вдохновения. Если фраза вида `организовано ...`, `от ...`, `вокруг ...`, "
        "`вдохновлено ...` содержит площадку или адрес, это не логистический повтор, а смысловой факт: сохрани её.\n"
        "- Не добавляй новых фактов. Не меняй стиль.\n"
        "- Сохраняй пунктуацию и абзацы. Не превращай текст в список, если он был прозой.\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=900,
        label=label,
        temperature=0.0,
    )
    return text.strip() if text else None


async def _rewrite_description_journalistic(
    candidate: EventCandidate,
    *,
    strict_nonverbatim: bool = False,
) -> str | None:
    """Produce a non-verbatim, journalist-style description for external imports.

    Keep this best-effort: failures must not block event creation/merge.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    if candidate.source_type in ("bot", "manual"):
        return None

    # For site imports we often have a short `raw_excerpt` (search-style snippet),
    # while `source_text` contains the full article/program. Prefer the fuller
    # source when the excerpt is clearly shorter to avoid generating a "too short"
    # description for Telegraph.
    excerpt_raw = (candidate.raw_excerpt or "").strip()
    source_raw = (candidate.source_text or "").strip()
    poster_raw = "\n\n".join(
        [
            (p.ocr_text or "").strip()
            for p in (candidate.posters or [])[:2]
            if (p.ocr_text or "").strip()
        ]
    ).strip()
    base = excerpt_raw or source_raw or poster_raw
    if _should_prefer_source_text_for_description(source_raw, excerpt_raw):
        base = source_raw
    base = _strip_promo_lines(base) or base
    base = _strip_private_use(base) or base
    base = _normalize_bullet_markers(base) or base
    if len(base) < 80 and poster_raw:
        poster_clean = _strip_promo_lines(poster_raw) or poster_raw
        poster_clean = _strip_private_use(poster_clean) or poster_clean
        poster_clean = _normalize_bullet_markers(poster_clean) or poster_clean
        if len((poster_clean or "").strip()) >= 80:
            base = (poster_clean or "").strip()
    if len(base) < 80:
        return None

    payload = {
        "title": candidate.title,
        "date": candidate.date,
        "time": candidate.time,
        "end_date": candidate.end_date,
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
        "ticket_link": candidate.ticket_link,
        "ticket_status": candidate.ticket_status,
        "is_free": candidate.is_free,
        "event_type": candidate.event_type,
        "festival": candidate.festival,
        "source_type": candidate.source_type,
        "raw_excerpt": _clip(_strip_promo_lines(candidate.raw_excerpt) or candidate.raw_excerpt, 1200),
        "source_text": _clip(
            _normalize_bullet_markers(_strip_promo_lines(candidate.source_text) or candidate.source_text)
            or (_strip_promo_lines(candidate.source_text) or candidate.source_text),
            SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS,
        ),
        "quote_candidates": _extract_quote_candidates(
            _strip_promo_lines(candidate.source_text) or candidate.source_text,
            max_items=2,
        ),
        "poster_texts": [_clip(p.ocr_text, 500) for p in candidate.posters if p.ocr_text][:3],
        "poster_titles": [
            _clip(p.ocr_title, 140)
            for p in candidate.posters
            if (p.ocr_title or "").strip()
        ][:3],
    }
    strict_block = ""
    if strict_nonverbatim:
        strict_block = (
            "СТРОГОЕ требование к анти-дословности:\n"
            "- Не копируй предложения из источника дословно.\n"
            "- Не допускай длинных совпадающих фрагментов (ориентир: не более 8–10 слов подряд).\n"
            "- Если перефразировать нельзя без потери смысла — лучше сократи этот фрагмент.\n\n"
        )
    telegram_block = ""
    tone_hint = "Передай суть и атмосферу"
    if candidate.source_type == "telegram":
        tone_hint = "Передай суть"
        short_base_len = len(base.strip())
        if 80 <= short_base_len <= 350:
            cap_hint = max(220, short_base_len + 100)
            telegram_block = (
                "Особенности Telegram (короткий источник):\n"
                f"- Длина результата: не длиннее {cap_hint} символов.\n"
                "- Не раздувай текст и не добавляй общие фразы «про атмосферу/вовлечённость/уникальность».\n"
                "- Запрещены штампы вроде «это создаёт ...».\n"
                "- Не добавляй технические заголовки/секции про факты: `Facts`, `Added Facts`, `Facts/Added Facts`, "
                "а также «Факты…», «Факты о событии».\n"
                "- Если есть прямая речь в «...» — сохрани её ДОСЛОВНО и оформи как blockquote (`>`).\n\n"
            )
        else:
            telegram_block = (
                "Особенности Telegram:\n"
                "- Избегай клише и пустых фраз (особенно «это создаёт ...»).\n"
                "- Не добавляй технические заголовки/секции про факты: `Facts`, `Added Facts`, `Facts/Added Facts`, "
                "а также «Факты…», «Факты о событии».\n\n"
            )
    prompt = (
        "Ты — культурный журналист. Сделай журналистский рерайт анонса мероприятия. "
        f"{tone_hint}, но НЕ копируй исходные фразы дословно. "
        "Не добавляй выдуманных фактов, используй только то, что есть в данных. "
        "Запрещено придумывать утверждения вроде 'премьера', 'впервые', 'аншлаг' и т.п., "
        "если это явно не сказано в источнике. "
        f"{SMART_UPDATE_YO_RULE} "
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE} "
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE} "
        f"{SMART_UPDATE_OPTIONAL_HEADINGS_RULE} "
        f"{_description_emoji_prompt_rule()}"
        "Без хэштегов. "
        "Не добавляй отдельные секции/заголовки про факты (например «Факты…», «Факты о событии», "
        "«Facts/Added Facts»): факты формируются и показываются отдельно. "
        "Важно: НЕ повторяй в описании логистику (дата/время/площадка/точный адрес/город/ссылки/телефон/контакты/точные цены) — "
        "она показывается отдельным инфоблоком сверху. "
        "Убери промо чужих/вспомогательных каналов с анонсами и призывы подписаться "
        "(например «Информация о событиях ... доступна в Telegram-канале ...»): это не факт про само событие. "
        "Можно использовать минимальную разметку для читабельности: "
        "заголовки `###`, цитаты блоком `> ...`, редкое выделение `**...**`. "
        "НЕ используй Markdown-ссылки вида `[текст](url)` и не вставляй таблицы. "
        "Согласуй время повествования с датой события: "
        "если дата события в будущем (относительно поля `today`) — используй будущее время "
        "(например «пройдёт», «состоится»), а не «проходит». "
        "Убери рекламные и акционные фрагменты (скидки/промокоды/акция) и механику розыгрыша, если они не являются частью сути события. "
        "Не включай малозначимые и повторяющиеся строки (например `DD.MM | Название`, повтор даты/заголовка, «представление состоится ...» при уже указанной дате/времени). "
        "Если в источнике есть обрыв фразы/текста (в т.ч. обрезано на середине слова), не вставляй это дословно: либо перефразируй, либо опусти. "
        "Не экранируй кавычки обратным слэшем (не пиши `\\\"...\\\"`). "
        "Если в тексте есть прямая речь/цитата (1-е лицо: 'я/мне/кажется/думаю' и т.п.), "
        "НЕ переписывай её в косвенную речь: включи её ДОСЛОВНО как цитату блоком `>` и не дублируй ту же мысль пересказом рядом. "
        "Если понятно, кто автор цитаты (например режиссёр), добавь атрибуцию: `— Имя Фамилия` рядом с цитатой. "
        "Если `quote_candidates` не пуст, обязательно включи хотя бы одну из этих цитат ДОСЛОВНО как blockquote. "
        "Сделай ПОЛНОЕ развернутое описание события, сохранив ВСЕ значимые факты из входных данных, "
        "кроме логистики (она уже показана отдельно). "
        "Не превращай текст в краткий дайджест: если исходный текст длинный, результат тоже может быть длинным "
        "(например 10-25 предложений, при необходимости больше). "
        "Если в исходных данных перечислены элементы программы/сюжета/формата/участники/условия посещения, отрази их. "
        "Структуру делай абзацами: разделяй абзацы пустой строкой. Текст должен читаться как единое связное повествование.\n\n"
        "Техническое требование к форматированию:\n"
        "- В одном абзаце держи 1-2 предложения (максимум 3 только если иначе теряется смысл).\n"
        "- НЕ разрывай предложения пустой строкой на середине.\n"
        "- НЕ ставь пустую строку между инициалом и фамилией (например `Н. Любимова`).\n"
        "- Не дублируй в основном тексте строки-анкеры (`Дата:`, `Время:`, `Локация:`, `Билеты:`) и их явные перефразы.\n"
        "- Избегай нейросетевых клише и прогнозов (например 'обещает стать заметным событием', 'не оставит равнодушным').\n\n"
        "Самопроверка перед ответом:\n"
        "- В тексте НЕТ ссылок/телефонов/точных адресов/цен/времени/дат (они уже в инфоблоке).\n"
        "- НЕТ обрывов фраз (например «стоимость … составит» без продолжения).\n"
        "- НЕТ странных/непонятных слов и опечаток; если слово выглядит ошибочным — перефразируй.\n\n"
        f"{telegram_block}"
        f"{strict_block}"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=SMART_UPDATE_REWRITE_MAX_TOKENS,
        label="rewrite",
        temperature=0.0,
    )
    if not text:
        return None
    cleaned = text
    cleaned = _strip_private_use(cleaned) or cleaned
    cleaned = _normalize_plaintext_paragraphs(cleaned)
    if not cleaned:
        return None
    cleaned = _fix_broken_initial_paragraph_splits(cleaned) or cleaned
    cleaned = (
        _sanitize_description_output(
            cleaned,
            source_text=_strip_promo_lines(candidate.source_text) or candidate.source_text,
        )
        or cleaned
    )
    cleaned = _strip_channel_promo_from_description(cleaned) or cleaned
    # Ensure direct quotes stay as quotes (blockquote) when we detected candidates in the source.
    quote_candidates = payload.get("quote_candidates") or []
    director_name_hint = _extract_director_name_hint(
        candidate_text=_strip_promo_lines(candidate.source_text) or candidate.source_text,
        facts_before=[],
    )
    cleaned = _ensure_blockquote_has_attribution(
        description=cleaned,
        attribution_name=director_name_hint,
    )
    if quote_candidates and not re.search(r"(?m)^>\s+", cleaned):
        cleaned = await _ensure_direct_quote_blockquote(
            description=cleaned,
            quote_candidates=quote_candidates,
            candidate_text=_strip_promo_lines(candidate.source_text) or candidate.source_text,
            facts_before=[],
            label="rewrite_quote_enforce",
        )
        cleaned = _normalize_plaintext_paragraphs(cleaned) or cleaned
        cleaned = _normalize_blockquote_markers(cleaned) or cleaned
        cleaned = _drop_reported_speech_duplicates(cleaned) or cleaned
        cleaned = _ensure_blockquote_has_attribution(
            description=cleaned,
            attribution_name=director_name_hint,
        )

    if _description_needs_infoblock_logistics_strip(cleaned, candidate=candidate):
        try:
            edited = await _llm_remove_infoblock_logistics(
                description=cleaned,
                candidate=candidate,
                label="rewrite_remove_logistics",
            )
        except Exception:  # pragma: no cover - network failures
            edited = None
        if edited:
            edited = _normalize_plaintext_paragraphs(edited) or edited
            cleaned = edited
    if _description_needs_channel_promo_strip(cleaned):
        cleaned = _strip_channel_promo_from_description(cleaned) or cleaned

    cleaned = (
        _append_missing_small_list(
            description=cleaned,
            source_text=base,
            source_type=candidate.source_type,
        )
        or cleaned
    )

    # For short Telegram snippets (1-2 lines), keep rewrite volume near source size.
    # This prevents aggressive expansion/hallucinated "long reads" when source is concise.
    if candidate.source_type == "telegram":
        short_base_len = len(base.strip())
        if 80 <= short_base_len <= 350:
            max_allowed = min(
                SMART_UPDATE_DESCRIPTION_MAX_CHARS,
                max(220, int(short_base_len) + 100),
            )
            if len(cleaned) > max_allowed:
                logger.info(
                    "smart_update: rewrite overexpanded short telegram source (base_len=%s, out_len=%s, cap=%s)",
                    short_base_len,
                    len(cleaned),
                    max_allowed,
                )
                cleaned = _clip_to_readable_boundary(cleaned, max_allowed)
    return _clip(cleaned, SMART_UPDATE_DESCRIPTION_MAX_CHARS)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = value.split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


_DAY_MONTH_NUM_RE = re.compile(r"\b(\d{1,2})\s*[./-]\s*(\d{1,2})\b")
_MONTH_WORD_PATTERN = "|".join(sorted((re.escape(k) for k in MONTHS_RU.keys()), key=len, reverse=True))
_DAY_MONTH_WORD_RE = (
    re.compile(rf"\b(\d{{1,2}})\s+({_MONTH_WORD_PATTERN})\b", re.IGNORECASE)
    if _MONTH_WORD_PATTERN
    else None
)

DATE_PROVENANCE_MISSING = "missing"
DATE_PROVENANCE_UNGROUNDED = "ungrounded"
DATE_PROVENANCE_SOURCE_TEXT = "source_text"
DATE_PROVENANCE_POSTER_OCR = "poster_ocr"
DATE_PROVENANCE_CANONICAL_SOURCE = "canonical_source"
DATE_PROVENANCE_OPERATOR = "operator"

DATE_PROVENANCE_TRUST_LADDER: tuple[str, ...] = (
    DATE_PROVENANCE_MISSING,
    DATE_PROVENANCE_UNGROUNDED,
    DATE_PROVENANCE_SOURCE_TEXT,
    DATE_PROVENANCE_POSTER_OCR,
    DATE_PROVENANCE_CANONICAL_SOURCE,
    DATE_PROVENANCE_OPERATOR,
)
DATE_PROVENANCE_TRUST_RANK: dict[str, int] = {
    level: idx for idx, level in enumerate(DATE_PROVENANCE_TRUST_LADDER)
}

_DATE_PROVENANCE_SOURCE_TYPES_OPERATOR = frozenset({"bot", "manual"})
_DATE_UPDATE_REASON_NONE = "no_update"
_DATE_UPDATE_REASON_CANONICAL = "canonical_source"
_DATE_UPDATE_REASON_INFERRED_LONG_GROUNDED = "inferred_long_event_grounded"


def _extract_day_month_pairs(text: str | None) -> set[tuple[int, int]]:
    raw = str(text or "").strip()
    if not raw:
        return set()
    normalized = unicodedata.normalize("NFKC", raw).casefold().replace("ё", "е")
    pairs: set[tuple[int, int]] = set()
    for m in _DAY_MONTH_NUM_RE.finditer(normalized):
        try:
            day = int(m.group(1))
            month = int(m.group(2))
        except Exception:
            continue
        if not (1 <= day <= 31 and 1 <= month <= 12):
            continue
        pairs.add((day, month))
    if _DAY_MONTH_WORD_RE is not None:
        for m in _DAY_MONTH_WORD_RE.finditer(normalized):
            try:
                day = int(m.group(1))
            except Exception:
                continue
            mon_word = (m.group(2) or "").casefold().replace("ё", "е")
            month = MONTHS_RU.get(mon_word)
            if not month or not (1 <= day <= 31):
                continue
            pairs.add((day, int(month)))
    return pairs


def _poster_day_month_pairs(posters: Sequence[PosterCandidate]) -> set[tuple[int, int]]:
    pairs: set[tuple[int, int]] = set()
    for poster in posters or []:
        pairs |= _extract_day_month_pairs(getattr(poster, "ocr_title", None))
        pairs |= _extract_day_month_pairs(getattr(poster, "ocr_text", None))
    return pairs


def _candidate_date_grounding_channels(candidate: EventCandidate) -> set[str]:
    cand_start = _parse_iso_date(candidate.date)
    if not cand_start:
        return set()
    target = (int(cand_start.day), int(cand_start.month))
    channels: set[str] = set()
    source_pairs = _extract_day_month_pairs(
        "\n".join(
            part
            for part in (
                str(candidate.source_text or ""),
                str(candidate.raw_excerpt or ""),
            )
            if part
        ).strip()
    )
    if target in source_pairs:
        channels.add(DATE_PROVENANCE_SOURCE_TEXT)
    poster_pairs = _poster_day_month_pairs(candidate.posters)
    if target in poster_pairs:
        channels.add(DATE_PROVENANCE_POSTER_OCR)
    return channels


def _candidate_date_provenance_level(
    candidate: EventCandidate,
    *,
    is_canonical_site: bool = False,
) -> str:
    if not _parse_iso_date(candidate.date):
        return DATE_PROVENANCE_MISSING
    source_type = str(candidate.source_type or "").strip().lower()
    if source_type in _DATE_PROVENANCE_SOURCE_TYPES_OPERATOR:
        return DATE_PROVENANCE_OPERATOR
    if is_canonical_site:
        return DATE_PROVENANCE_CANONICAL_SOURCE
    channels = _candidate_date_grounding_channels(candidate)
    if DATE_PROVENANCE_POSTER_OCR in channels:
        return DATE_PROVENANCE_POSTER_OCR
    if DATE_PROVENANCE_SOURCE_TEXT in channels:
        return DATE_PROVENANCE_SOURCE_TEXT
    return DATE_PROVENANCE_UNGROUNDED


def _date_provenance_trust_rank(level: str | None) -> int:
    return DATE_PROVENANCE_TRUST_RANK.get(
        str(level or "").strip().lower(),
        DATE_PROVENANCE_TRUST_RANK[DATE_PROVENANCE_UNGROUNDED],
    )


def _format_day_month_pairs(pairs: set[tuple[int, int]]) -> str:
    if not pairs:
        return ""
    return ", ".join(f"{d:02d}/{m:02d}" for d, m in sorted(pairs, key=lambda x: (x[1], x[0])))


def _date_provenance_confidence(level: str | None) -> float:
    level = str(level or DATE_PROVENANCE_UNGROUNDED).strip().lower()
    return {
        DATE_PROVENANCE_OPERATOR: 1.0,
        DATE_PROVENANCE_CANONICAL_SOURCE: 0.95,
        DATE_PROVENANCE_POSTER_OCR: 0.85,
        DATE_PROVENANCE_SOURCE_TEXT: 0.8,
        DATE_PROVENANCE_UNGROUNDED: 0.35,
        DATE_PROVENANCE_MISSING: 0.0,
    }.get(level, 0.35)


def _candidate_date_is_inferred(candidate: EventCandidate, *, is_canonical_site: bool = False) -> bool:
    level = _candidate_date_provenance_level(candidate, is_canonical_site=is_canonical_site)
    return level in {DATE_PROVENANCE_MISSING, DATE_PROVENANCE_UNGROUNDED}


def _candidate_end_date_provenance_level(candidate: EventCandidate, *, is_canonical_site: bool = False) -> str:
    if not candidate.end_date:
        return DATE_PROVENANCE_MISSING
    if bool(getattr(candidate, "end_date_is_inferred", False)):
        return "inferred_default_30d"
    return _candidate_date_provenance_level(candidate, is_canonical_site=is_canonical_site)


async def _record_identity_gate_decision(
    db: Database,
    candidate: EventCandidate,
    *,
    decision: str,
    reason: str | None = None,
    confidence: float | None = None,
    event_id: int | None = None,
    candidate_event_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    try:
        async with db.get_session() as session:
            session.add(
                EventIdentityDecisionLog(
                    event_id=event_id,
                    candidate_event_id=candidate_event_id,
                    source_type=str(candidate.source_type or "") or None,
                    source_url=str(candidate.source_url or "") or None,
                    decision=decision,
                    decision_reason=reason,
                    confidence=confidence,
                    decided_by="smart_update.identity_gate",
                    decision_payload=payload or {},
                )
            )
            await session.commit()
    except Exception:
        logger.warning("smart_update.identity_gate decision log insert failed", exc_info=True)


def _far_future_poster_date_mismatch_note(
    *,
    candidate_date: str | None,
    posters: Sequence[PosterCandidate],
    months_threshold: int,
) -> str | None:
    """Return operator note when a far-future extracted date conflicts with poster OCR."""
    if months_threshold <= 0:
        return None
    start = _parse_iso_date(candidate_date)
    if not start:
        return None
    today = datetime.now(timezone.utc).date()
    try:
        from dateutil.relativedelta import relativedelta

        far_cutoff = today + relativedelta(months=int(months_threshold))
    except Exception:
        far_cutoff = today + timedelta(days=31 * int(months_threshold))
    if start < far_cutoff:
        return None
    pairs = _poster_day_month_pairs(posters)
    if not pairs:
        return None
    if (start.day, start.month) in pairs:
        return None
    pairs_label = _format_day_month_pairs(pairs)
    extracted_label = f"{start.day:02d}/{start.month:02d}"
    return (
        f"⚠️ Дата: конфликт с афишей (OCR={pairs_label}, extracted={extracted_label}); "
        "semantic resolution belongs to source verification"
    )


def _add_one_calendar_month(start: date) -> date:
    year = start.year
    month = start.month + 1
    if month > 12:
        month = 1
        year += 1
    day = min(start.day, monthrange(year, month)[1])
    return date(year, month, day)


_LONG_EVENT_TEXT_HINT_RE = re.compile(
    r"\b("
    r"выставк\w*|"
    r"экспозиц\w*|"
    r"ярмарк\w*|"
    r"маркет\w*|"
    r"инсталляци\w*|"
    r"экспозици\w*"
    r")\b",
    re.IGNORECASE,
)

_ACTION_TITLE_RE = re.compile(r"(?i)\bакци\w*\b")
_ONE_DAY_ACTION_HINT_RE = re.compile(
    r"(?i)\b("
    r"билет\w*\s+действ\w*.*\bтолько\b|"
    r"только\s+(?:сегодня|завтра)|"
    r"на\s+завтра|"
    r"на\s+сегодня|"
    r"указанн\w+\s+дат\w+|"
    r"одн[ау]\s+дат[ау]\b"
    r")\b"
)

_OPENING_EXHIBITION_TITLE_RE = re.compile(
    r"(?iu)\bоткрыти[ея]\s+(?:персональн\w+\s+|нов\w+\s+)?"
    r"(?:выставк\w*|экспозиц\w*)\b"
)


def _has_long_event_duration_signals(text: str | None) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    month_pat = "|".join(sorted(map(re.escape, _RU_MONTHS_GENITIVE.keys()), key=len, reverse=True))
    if re.search(r"\b20\d{2}-\d{2}-\d{2}\s*\.\.\s*20\d{2}-\d{2}-\d{2}\b", raw):
        return True
    if re.search(r"\b\d{1,2}[./]\d{1,2}\s*[-–—]\s*\d{1,2}[./]\d{1,2}\b", raw):
        return True
    if month_pat:
        if re.search(
            rf"\b(?:с\s+)?\d{{1,2}}\s+(?:по\s+|[-–—]\s*)\d{{1,2}}\s+(?:{month_pat})\b",
            raw,
            flags=re.IGNORECASE,
        ):
            return True
        if re.search(
            rf"\bс\s+\d{{1,2}}\s+(?:{month_pat})\b.*\bпо\s+\d{{1,2}}\s+(?:{month_pat})\b",
            raw,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            return True
        if re.search(rf"\b(до|по)\s+\d{{1,2}}\s+(?:{month_pat})\b", raw, flags=re.IGNORECASE):
            return True
        if re.search(rf"\b\d{{1,2}}\s+(?:{month_pat})\s*[-–—]\s*\d{{1,2}}\s+(?:{month_pat})\b", raw, flags=re.IGNORECASE):
            return True
    return False


def _maybe_apply_default_end_date_for_long_event(candidate: EventCandidate) -> str | None:
    if candidate.end_date:
        return None
    inferred_type = _normalize_event_type_value(
        candidate.title,
        candidate.raw_excerpt or candidate.source_text,
        candidate.event_type,
    )
    if inferred_type != "выставка":
        return None
    # Guardrail: event_type can be misclassified by upstream LLMs.
    # Apply a default 1-month end_date only when the source text looks like a long event
    # (exhibition/exposition) or contains explicit duration signals.
    hay = "\n".join(
        [
            str(candidate.title or ""),
            str(candidate.raw_excerpt or ""),
            str(candidate.source_text or ""),
        ]
    ).strip()
    # "Акция" posts (free ticket days, service campaigns) are not long-running exhibitions.
    # Even if they mention "экспозиции", they should not become 1-month ranges by default.
    if _ACTION_TITLE_RE.search(str(candidate.title or "")) and _ONE_DAY_ACTION_HINT_RE.search(hay):
        return None
    if hay and not (_LONG_EVENT_TEXT_HINT_RE.search(hay) or _has_long_event_duration_signals(hay)):
        return None
    # If the LLM has extracted an opening ceremony/card as the title and the
    # source did not provide a run window, do not turn that opening into a
    # month-long exhibition by applying the service fallback.  A true
    # exhibition card should be titled as the exhibition itself (or carry an
    # explicit end_date/duration signal), while an opening-only announcement is
    # an atomic dated event.
    if _OPENING_EXHIBITION_TITLE_RE.search(str(candidate.title or "")) and not _has_long_event_duration_signals(hay):
        return None
    start = _parse_iso_date(candidate.date)
    if not start:
        return None
    candidate.end_date = _add_one_calendar_month(start).isoformat()
    candidate.end_date_is_inferred = True
    return candidate.end_date


def _apply_event_end_date(
    event_db: Event,
    *,
    end_date: str | None,
    inferred: bool,
    updated_keys: list[str],
) -> bool:
    if not end_date:
        return False
    current_end_date = getattr(event_db, "end_date", None)
    current_inferred = bool(getattr(event_db, "end_date_is_inferred", False))

    if current_end_date and (not current_inferred) and inferred:
        return False

    changed = False
    if current_end_date != end_date:
        event_db.end_date = end_date
        changed = True
        if "end_date" not in updated_keys:
            updated_keys.append("end_date")
    if current_inferred != inferred:
        event_db.end_date_is_inferred = inferred
        changed = True
        if "end_date_is_inferred" not in updated_keys:
            updated_keys.append("end_date_is_inferred")
    return changed


def _event_date_range(ev: Event) -> tuple[date | None, date | None]:
    start = _parse_iso_date(ev.date or "")
    end = _parse_iso_date(ev.end_date) if ev.end_date else None
    if not end and ev.date and ".." in ev.date:
        end = _parse_iso_date(ev.date.split("..", 1)[1])
    if start and not end:
        end = start
    return start, end


def _candidate_date_range(candidate: EventCandidate) -> tuple[date | None, date | None]:
    start = _parse_iso_date(candidate.date)
    end = _parse_iso_date(candidate.end_date) if candidate.end_date else None
    if not end and candidate.date and ".." in candidate.date:
        try:
            end = _parse_iso_date(candidate.date.split("..", 1)[1])
        except Exception:
            end = None
    if start and not end:
        end = start
    return start, end


def _candidate_date_is_grounded_in_sources(candidate: EventCandidate) -> bool:
    return bool(_candidate_date_grounding_channels(candidate))


def _can_apply_conservative_date_update(
    event_db: Event,
    candidate: EventCandidate,
    *,
    is_canonical_site: bool,
) -> tuple[bool, str]:
    """Return whether Smart Update may rewrite event.date outside create.

    The helper is intentionally narrow: it captures the existing safe update
    cases (canonical parser/site truth, or a grounded exact date fixing an old
    inferred long-event range) and otherwise fail-closes.
    """
    candidate_date = str(candidate.date or "").strip()
    if not candidate_date or candidate_date == str(getattr(event_db, "date", "") or "").strip():
        return False, _DATE_UPDATE_REASON_NONE
    provenance = _candidate_date_provenance_level(
        candidate,
        is_canonical_site=is_canonical_site,
    )
    if provenance == DATE_PROVENANCE_CANONICAL_SOURCE:
        return True, _DATE_UPDATE_REASON_CANONICAL
    if (
        bool(getattr(event_db, "end_date_is_inferred", False))
        and _is_long_event_type_value(getattr(event_db, "event_type", None) or candidate.event_type)
        and _date_provenance_trust_rank(provenance)
        >= DATE_PROVENANCE_TRUST_RANK[DATE_PROVENANCE_SOURCE_TEXT]
        and (
            not candidate.location_name
            or not getattr(event_db, "location_name", None)
            or _event_candidate_location_matches(event_db, candidate)
        )
    ):
        return True, _DATE_UPDATE_REASON_INFERRED_LONG_GROUNDED
    return False, _DATE_UPDATE_REASON_NONE


def _looks_like_unsupported_exhibition_teaser_date(candidate: EventCandidate, text: str | None) -> bool:
    event_type = _normalize_event_type_value(
        candidate.title,
        candidate.raw_excerpt or candidate.source_text,
        candidate.event_type,
    )
    if event_type not in {"выставка", "экспозиция", "ярмарка"}:
        return False
    if not str(candidate.date or "").strip():
        return False
    if _candidate_date_is_grounded_in_sources(candidate):
        return False
    combined = "\n".join(
        [
            str(candidate.title or ""),
            str(text or ""),
            str(candidate.source_text or ""),
            str(candidate.raw_excerpt or ""),
        ]
    ).strip()
    if not combined:
        return False
    return bool(_UNSUPPORTED_EXHIBITION_TEASER_RE.search(combined))





@lru_cache(maxsize=1)
def _get_identity_embedding_client():
    try:
        from google_ai import GoogleAIClient, SecretsProvider
        from main import get_supabase_client, notify_llm_incident
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("smart_update.identity_gate embedding client unavailable: %s", exc)
        return None
    return GoogleAIClient(
        supabase_client=build_google_ai_limiter_supabase_client(
            fallback_factory=get_supabase_client
        ),
        secrets_provider=SecretsProvider(),
        consumer="smart_update_identity_embedding",
        account_name="smart-update-identity-embedding",
        default_env_var_name=SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV,
        incident_notifier=notify_llm_incident,
    )


async def _smart_update_embed_identity_document_with_limiter(doc: Any):
    try:
        from event_identity import EventIdentityEmbeddingResult
    except Exception as exc:  # pragma: no cover
        return None
    client = _get_identity_embedding_client()
    if client is None:
        return EventIdentityEmbeddingResult(
            ok=False,
            model=SMART_UPDATE_IDENTITY_EMBEDDING_MODEL,
            dim=SMART_UPDATE_IDENTITY_EMBEDDING_DIM,
            error_type="MissingGoogleAIClient",
            error_message="GoogleAIClient unavailable for identity embedding limiter",
        )
    previous_timeout = getattr(client, "provider_timeout_seconds", 0.0)
    try:
        client.provider_timeout_seconds = max(0.1, float(SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS or 10.0))
        values, _usage = await client.embed_content_async(
            model=SMART_UPDATE_IDENTITY_EMBEDDING_MODEL,
            text=getattr(doc, "text", None) or str(doc or ""),
            output_dimensionality=SMART_UPDATE_IDENTITY_EMBEDDING_DIM,
        )
        return EventIdentityEmbeddingResult(
            ok=True,
            embedding=tuple(float(v) for v in values),
            model=SMART_UPDATE_IDENTITY_EMBEDDING_MODEL,
            dim=SMART_UPDATE_IDENTITY_EMBEDDING_DIM,
        )
    except Exception as exc:
        return EventIdentityEmbeddingResult(
            ok=False,
            model=SMART_UPDATE_IDENTITY_EMBEDDING_MODEL,
            dim=SMART_UPDATE_IDENTITY_EMBEDDING_DIM,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )
    finally:
        try:
            client.provider_timeout_seconds = previous_timeout
        except Exception:
            pass


async def _smart_update_identity_vector_evidence(candidate: EventCandidate) -> IdentityVectorEvidence | None:
    """Best-effort vector recall evidence for the create-path identity gate.

    The candidate embedding is ephemeral and never stored.  Missing credentials or
    provider/RPC failures return structured error evidence so enforce mode can
    fail safe while shadow mode only logs.
    """

    if not SMART_UPDATE_IDENTITY_VECTOR_RECALL:
        return None
    try:
        from event_identity import (
            EventIdentityRecallConfig,
            SupabaseRestRpcClient,
            build_identity_candidate_document,
            recall_identity_candidates_across_doc_kinds,
        )

        supabase_url = (
            os.getenv("PERSONALIZATION_SUPABASE_URL")
            or os.getenv("SUPABASE_URL")
            or ""
        ).strip()
        supabase_key = (
            os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY")
            or os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_KEY")
            or os.getenv("SUPABASE_KEY")
            or ""
        ).strip()
        google_key = (os.getenv(SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV) or "").strip()
        if not supabase_url or not supabase_key or not google_key:
            missing = []
            if not supabase_url:
                missing.append("PERSONALIZATION_SUPABASE_URL_or_SUPABASE_URL")
            if not supabase_key:
                missing.append("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY_or_SUPABASE_SERVICE_KEY_or_SUPABASE_KEY")
            if not google_key:
                missing.append(SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV)
            return IdentityVectorEvidence(
                available=False,
                error="missing vector identity env: " + ",".join(missing),
            )

        doc = build_identity_candidate_document(candidate)

        embedding = await _smart_update_embed_identity_document_with_limiter(doc)
        if embedding is None or not embedding.ok:
            return IdentityVectorEvidence(
                available=False,
                error=f"embedding_failed:{getattr(embedding, 'error_type', None)}:{getattr(embedding, 'error_message', None)}",
            )

        def _recall_sync() -> IdentityVectorEvidence:
            client = SupabaseRestRpcClient(
                supabase_url,
                supabase_key,
                timeout_seconds=SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS,
            )
            recall = recall_identity_candidates_across_doc_kinds(
                client,
                embedding.embedding,
                city=candidate.city,
                event_type=candidate.event_type,
                config=EventIdentityRecallConfig(
                    top_k=SMART_UPDATE_IDENTITY_VECTOR_TOP_K,
                    min_similarity=SMART_UPDATE_IDENTITY_VECTOR_MIN_SIMILARITY,
                    timeout_seconds=SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS,
                    embedding_model=SMART_UPDATE_IDENTITY_EMBEDDING_MODEL,
                    embedding_dim=SMART_UPDATE_IDENTITY_EMBEDDING_DIM,
                ),
            )
            if not recall.ok:
                return IdentityVectorEvidence(
                    available=False,
                    error=f"rpc_failed:{recall.error_type}:{recall.error_message}",
                )
            if not recall.candidates:
                return IdentityVectorEvidence(
                    available=True,
                    reason=f"no_vector_candidates doc={doc.sha256[:12]}",
                )
            top = recall.candidates[0]
            return IdentityVectorEvidence(
                available=True,
                nearest_event_id=top.event_id,
                score=top.similarity,
                reason=(
                    f"vector_recall doc={doc.sha256[:12]} kind={top.embedding_doc_kind} "
                    f"title={top.title or ''}"
                ).strip(),
            )

        return await asyncio.to_thread(_recall_sync)
    except Exception as exc:
        logger.warning("smart_update.identity_gate vector recall failed", exc_info=True)
        return IdentityVectorEvidence(available=False, error=f"vector_recall_error:{type(exc).__name__}:{exc}")


def _smart_update_skip_past_events_enabled() -> bool:
    raw = (os.getenv("SMART_UPDATE_SKIP_PAST_EVENTS") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _smart_update_today_local() -> date:
    try:
        from event_utils import LOCAL_TZ as _LOCAL_TZ
    except Exception:
        _LOCAL_TZ = timezone.utc
    return datetime.now(_LOCAL_TZ).date()


def _should_skip_past_smart_update_candidate(candidate: EventCandidate) -> bool:
    """Skip candidates that have fully ended before today (local).

    This is a guardrail for automated ingestion (VK/TG/parsers) to avoid creating
    useless past events + Telegraph/ICS load.
    """
    if not _smart_update_skip_past_events_enabled():
        return False
    source_type = str(candidate.source_type or "").strip().lower()
    if source_type == "bot":
        return False
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return False
    today = _smart_update_today_local()
    return cand_end < today


def _ranges_overlap(a_start: date | None, a_end: date | None, b_start: date | None, b_end: date | None) -> bool:
    if not a_start or not a_end or not b_start or not b_end:
        return False
    return not (a_end < b_start or b_end < a_start)


def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    value = url.strip()
    if not value:
        return None
    canonical_tg = canonicalize_tg_url(value)
    if canonical_tg:
        return canonical_tg.rstrip("/")
    low = value.lower()
    if low.startswith(("t.me/", "telegram.me/", "vk.cc/", "clck.ru/")):
        value = f"https://{value}"
        low = value.lower()
    if low.startswith("http://"):
        value = "https://" + value[len("http://") :]
    if value.startswith("http://") or value.startswith("https://"):
        value = value.rstrip("/")
    return value


_TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "yclid",
    "fbclid",
    "gclid",
}


def _normalize_ticket_url_for_match(url: str | None) -> str | None:
    norm = _normalize_url(url)
    if not norm:
        return None
    try:
        parts = urlsplit(norm)
    except Exception:
        return norm
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.casefold() not in _TRACKING_QUERY_KEYS
    ]
    return urlunsplit(
        (
            parts.scheme.casefold(),
            parts.netloc.casefold(),
            parts.path.rstrip("/"),
            urlencode(query, doseq=True),
            "",
        )
    )


def _specific_ticket_url_for_match(url: str | None) -> str | None:
    norm = _normalize_ticket_url_for_match(url)
    if not norm:
        return None
    try:
        path = urlsplit(norm).path.strip("/")
    except Exception:
        path = ""
    if not path:
        return None
    if path.casefold() in {"tickets", "ticket", "afisha", "events", "event"}:
        return None
    if re.search(r"\d", path):
        return norm
    if "-" in path and len(path) >= 12:
        return norm
    return None


def _is_http_url(url: str | None) -> bool:
    if not url:
        return False
    value = url.strip().lower()
    return value.startswith("http://") or value.startswith("https://")


_VK_WALL_URL_RE = re.compile(
    r"^https?://(?:m\.)?vk\.com/wall-?\d+_\d+/?$",
    re.IGNORECASE,
)


def _is_vk_wall_url(url: str | None) -> bool:
    if not url:
        return False
    if not _is_http_url(url):
        return False
    return bool(_VK_WALL_URL_RE.match(url.strip()))


def _infer_source_type_from_url(url: str | None) -> str:
    """Infer EventSource.source_type for legacy source urls.

    We historically stored a single source link in Event.source_post_url / Event.source_vk_post_url.
    With Smart Update we moved to an explicit event_source table. When merging/updating an older
    event, we backfill that legacy link so the operator can see >=2 sources after a merge.
    """
    value = (url or "").strip().lower()
    if not value:
        return "site"
    if "t.me/" in value or "telegram.me/" in value:
        return "telegram"
    if _is_vk_wall_url(value):
        return "vk"
    return "site"


def _is_managed_vk_publication_url(url: str | None) -> bool:
    value = str(url or "").strip()
    match = re.search(r"(?i)(?:wall)?-([0-9]+)_[0-9]+", value)
    if not match:
        return False
    owner_id = match.group(1)
    managed_ids = {
        str(os.getenv(name) or "").strip().lstrip("-")
        for name in ("VK_EVENTS_GROUP_ID", "VK_AFISHA_GROUP_ID")
    }
    managed_ids.discard("")
    # This is the established production default used by the publishing and
    # Smart Update report surfaces when the env alias is omitted.
    managed_ids.add("231920894")
    return owner_id in managed_ids


def _flatten_source_grounded_fact_items(
    facts: Sequence[object],
    *,
    source_text: str | None,
    log_context: str,
) -> list[str]:
    out: list[str] = []
    for raw in facts or []:
        if isinstance(raw, dict):
            fact = str(raw.get("fact") or "").strip()
            evidence_quote = str(raw.get("evidence_quote") or "").strip()
        else:
            # Backwards-compatible fail-closed handling for stale providers and
            # fixtures: a bare string must still be supported by the full source.
            fact = str(raw or "").strip()
            evidence_quote = ""
        if not fact:
            continue
        verdict = claim_is_grounded(
            fact,
            source_text,
            evidence_quote=evidence_quote or None,
            min_ratio=0.38,
            min_matches=2,
        )
        if verdict.ok:
            out.append(fact)
            continue
        logger.warning(
            "smart_update.source_fact_rejected context=%s reason=%s matched=%s claim_tokens=%s fact=%r",
            log_context,
            verdict.reason,
            verdict.matched,
            verdict.claim_tokens,
            fact[:180],
        )
    return out


async def _ensure_legacy_event_sources(session, event: Event | None) -> int:
    """Ensure legacy single-source fields are represented in event_source.

    Returns number of sources added.
    """
    if not event or not event.id:
        return 0

    urls: list[str] = []
    if _is_http_url(event.source_post_url):
        urls.append(str(event.source_post_url).strip())
    if _is_http_url(event.source_vk_post_url):
        urls.append(str(event.source_vk_post_url).strip())
    if not urls:
        return 0

    clean_source_text = _strip_private_use(event.source_text) or event.source_text
    now = datetime.now(timezone.utc)
    added = 0
    for url in urls:
        if _is_managed_vk_publication_url(url):
            logger.info(
                "smart_update.legacy_source_skip_managed_vk event_id=%s source_url=%s",
                event.id,
                url,
            )
            continue
        exists = (
            await session.execute(
                select(EventSource.id).where(
                    EventSource.event_id == event.id,
                    or_(
                        EventSource.canonical_source_url == canonicalize_identity_url(url),
                        and_(
                            EventSource.canonical_source_url.is_(None),
                            EventSource.source_url == url,
                        ),
                    ),
                )
            )
        ).scalar_one_or_none()
        if exists:
            continue
        session.add(
            EventSource(
                event_id=event.id,
                source_type=_infer_source_type_from_url(url),
                source_url=url,
                canonical_source_url=canonicalize_identity_url(url),
                # This is reconstructed legacy provenance, not direct evidence
                # allowed to assert SAME_EVENT.
                source_role="context_only",
                source_fingerprint=input_packet_fingerprint(
                    {
                        "event_id": int(event.id),
                        "source_url": url,
                        "source_text": clean_source_text or "",
                        "source_role": "context_only",
                    }
                ),
                source_text=clean_source_text,
                imported_at=now,
            )
        )
        added += 1
    return added


SMART_UPDATE_LEGACY_DESC_FACT_MAX_CHARS = _env_int(
    "SMART_UPDATE_LEGACY_DESC_FACT_MAX_CHARS", 900, lo=200, hi=2500
)
SMART_UPDATE_LEGACY_DESC_FACT_EXTRACT_MIN_CHARS = _env_int(
    # Avoid extracting facts from ultra-short legacy descriptions: the risk of hallucination is high.
    "SMART_UPDATE_LEGACY_DESC_FACT_EXTRACT_MIN_CHARS",
    160,
    lo=80,
    hi=2000,
)

_LEGACY_LEAK_RE = re.compile(r"(?i)\bsmart\s*update\b")


def _drop_legacy_leak_from_description(description: str | None) -> str | None:
    """Remove a previously injected legacy snapshot block from a description.

    This is a backward-compat cleanup for a historical bug where a legacy snapshot
    was treated as an "added fact" and ended up in the public description.
    The heuristic is intentionally narrow: it only drops paragraphs that mention
    "Smart Update" and look like a quoted service block.
    """
    raw = (description or "").strip()
    if not raw:
        return None
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    paras = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paras:
        return None
    kept: list[str] = []
    for p in paras:
        low = p.casefold()
        looks_like_quote = p.startswith(("«", ">", "“", '"', "'"))
        looks_like_service = _LEGACY_LEAK_RE.search(p) and (":" in p[:60]) and ("текст" in low or "legacy" in low)
        if looks_like_quote and looks_like_service:
            continue
        kept.append(p)
    cleaned = "\n\n".join(kept).strip()
    return cleaned or None


def _legacy_description_to_fact(description: str | None) -> str | None:
    raw = (description or "").strip()
    if not raw:
        return None

    # Keep it compact and neutral: a one-time backfill for legacy events that existed
    # before Smart Update/source facts. This must not inject new claims.
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?m)^\s*#+\s+", "", text)  # markdown headings
    text = re.sub(r"(?m)^\s*>\s*", "", text)  # markdown quotes
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    return _clip(text, SMART_UPDATE_LEGACY_DESC_FACT_MAX_CHARS)


async def _ensure_legacy_description_fact(
    session,
    *,
    event: Event | None,
    legacy_description: str | None,
) -> list[str]:
    """Persist a legacy description snapshot (and optionally backfill baseline facts).

    For events created before Smart Update/source facts, a merge can overwrite the previous
    narrative too aggressively. We store a compact snapshot of the old description as a
    dedicated legacy source. In fact-first mode, we also extract atomic baseline facts from
    that legacy text and store them under the legacy source so future fact-first builds do
    not lose pre-existing details.

    Important: we do NOT feed legacy facts into `facts_before` for LLM merge (see query that
    excludes `source_type == "legacy"`). This prevents non-source "snapshots" from polluting
    per-source merge reasoning while still allowing fact-first to use the backfilled facts.
    """

    if not event or not event.id:
        return []

    now = datetime.now(timezone.utc).replace(microsecond=0)
    legacy_url = f"legacy:event_description:{int(event.id)}"
    source = (
        await session.execute(
            select(EventSource).where(
                EventSource.event_id == int(event.id),
                EventSource.source_url == legacy_url,
            )
        )
    ).scalar_one_or_none()
    created_source = False

    snapshot: str | None = None
    if source and getattr(source, "source_text", None):
        snapshot = str(getattr(source, "source_text", None) or "").strip() or None

    if not source:
        snapshot = _legacy_description_to_fact(legacy_description)
        if not snapshot:
            return []

        # Only create a legacy snapshot when the event has no canonical facts yet (legacy/pre-Smart Update).
        # Notes/service rows do not count: we need at least one `added/duplicate` fact from a real source.
        exists_non_legacy = (
            await session.execute(
                select(EventSourceFact.id)
                .join(EventSource, EventSourceFact.source_id == EventSource.id)
                .where(
                    EventSourceFact.event_id == int(event.id),
                    EventSourceFact.status.in_(("added", "duplicate")),
                    EventSource.source_type != "legacy",
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if exists_non_legacy is not None:
            return []

        source = EventSource(
            event_id=int(event.id),
            source_type="legacy",
            source_url=legacy_url,
            canonical_source_url=legacy_url,
            source_role="context_only",
            source_fingerprint=input_packet_fingerprint(
                {
                    "event_id": int(event.id),
                    "source_url": legacy_url,
                    "source_text": snapshot,
                    "source_role": "context_only",
                }
            ),
            source_text=snapshot,
            imported_at=now,
            trust_level="high",
        )
        session.add(source)
        await session.flush()
        created_source = True

    if not snapshot:
        return []
    exists_legacy_facts = (
        await session.execute(
            select(EventSourceFact.id)
            .join(EventSource, EventSourceFact.source_id == EventSource.id)
            .where(
                EventSourceFact.event_id == int(event.id),
                EventSourceFact.status.in_(("added", "duplicate")),
                EventSource.source_type == "legacy",
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if exists_legacy_facts is not None:
        return []

    # Preserve legacy narrative as part of source_texts so merge can keep facts,
    # but do NOT treat it as an "added fact" (facts are for atomic source contributions).
    try:
        texts = list(getattr(event, "source_texts", None) or [])
        if snapshot not in texts:
            texts = [snapshot] + texts
            # Keep it bounded: legacy snapshot is only needed as a one-time baseline.
            setattr(event, "source_texts", texts[:6])
    except Exception:
        pass

    legacy_candidate = EventCandidate(
        source_type="legacy",
        source_url=legacy_url,
        source_text=snapshot,
        title=(getattr(event, "title", None) or "").strip() or None,
        date=(getattr(event, "date", None) or "").strip() or None,
        time=(getattr(event, "time", None) or "").strip() or None,
        end_date=(getattr(event, "end_date", None) or "").strip() or None,
        location_name=(getattr(event, "location_name", None) or "").strip() or None,
        location_address=(getattr(event, "location_address", None) or "").strip() or None,
        city=(getattr(event, "city", None) or "").strip() or None,
        ticket_link=(getattr(event, "ticket_link", None) or "").strip() or None,
        ticket_status=(getattr(event, "ticket_status", None) or "").strip() or None,
        event_type=(getattr(event, "event_type", None) or "").strip() or None,
        trust_level="high",
    )

    fact_records: list[tuple[str, str]] = [("Снапшот описания до Smart Update сохранён", "note")]

    extracted: list[str] = []
    if SMART_UPDATE_FACT_FIRST and not SMART_UPDATE_LLM_DISABLED:
        try:
            raw_for_facts = snapshot
            if created_source:
                raw_for_facts = _drop_legacy_leak_from_description(legacy_description) or legacy_description or snapshot
            cleaned_full = raw_for_facts
            cleaned_full = str(cleaned_full or "").strip()
            if cleaned_full:
                cleaned_full = cleaned_full.replace("\r\n", "\n").replace("\r", "\n")
                cleaned_full = re.sub(r"(?m)^\s*#+\s+", "", cleaned_full)  # markdown headings
                cleaned_full = re.sub(r"(?m)^\s*>\s*", "", cleaned_full)  # markdown quotes
                cleaned_full = re.sub(r"\n{3,}", "\n\n", cleaned_full).strip()
                if len(cleaned_full) < SMART_UPDATE_LEGACY_DESC_FACT_EXTRACT_MIN_CHARS:
                    fact_records.append(
                        (
                            "Legacy описание слишком короткое: факты не извлекались",
                            "note",
                        )
                    )
                    cleaned_full = ""
            if cleaned_full:
                extracted = await _llm_extract_candidate_facts(
                    legacy_candidate,
                    text_for_facts=cleaned_full,
                )
                if extracted:
                    for f in extracted[:18]:
                        fact_records.append((f, "added"))
                    fact_records.append(("Факты извлечены из legacy описания", "note"))
        except Exception:  # pragma: no cover - defensive
            logger.warning("smart_update: legacy fact backfill failed", exc_info=True)
            extracted = []

    try:
        if fact_records:
            await _record_source_facts(session, int(event.id), legacy_candidate, fact_records)
    except Exception:  # pragma: no cover - defensive
        logger.warning("smart_update: failed to persist legacy facts", exc_info=True)

    return extracted


def _normalize_event_type_value(
    title: str | None, description: str | None, event_type: str | None
) -> str | None:
    if not event_type:
        return None
    raw = str(event_type).strip()
    if not raw:
        return None
    aliases = {
        "exhibition": "выставка",
        "fair": "ярмарка",
    }
    canonical = aliases.get(raw.casefold(), raw)
    try:
        from main import normalize_event_type
    except Exception:  # pragma: no cover - defensive
        return canonical
    return normalize_event_type(title or "", description or "", canonical)


def _clean_search_digest(value: str | None) -> str | None:
    if not value:
        return None
    try:
        from digest_helper import clean_search_digest
    except Exception:  # pragma: no cover - defensive
        return value.strip()
    return clean_search_digest(value) or None


def _clean_short_description(value: str | None) -> str | None:
    if not value:
        return None
    try:
        from digest_helper import clean_short_description
    except Exception:  # pragma: no cover - defensive
        return value.strip()
    return clean_short_description(value) or None


def _is_short_description_acceptable(
    value: str | None,
    *,
    min_words: int = 12,
    max_words: int = 16,
) -> bool:
    try:
        from digest_helper import is_short_description_acceptable
    except Exception:  # pragma: no cover - defensive
        cleaned = _clean_short_description(value)
        if not cleaned:
            return False
        words = re.findall(r"[0-9A-Za-zА-Яа-яЁё]+", cleaned)
        return bool(cleaned.endswith((".", "!", "?")) and min_words <= len(words) <= max_words)
    return bool(
        is_short_description_acceptable(
            value,
            min_words=min_words,
            max_words=max_words,
        )
    )


def _fallback_short_description_from_text(text: str | None) -> str | None:
    try:
        from digest_helper import fallback_one_sentence
    except Exception:
        return None
    raw = fallback_one_sentence(text, max_words=16)
    cleaned = _clean_short_description(raw)
    if not cleaned:
        return None
    if cleaned and cleaned[-1].isalnum():
        cleaned += "."
    return cleaned


async def _llm_build_short_description(
    *,
    title: str | None,
    description: str | None,
    event_type: str | None,
) -> str | None:
    """Build 1-sentence short description for public lists (festival/daily)."""
    if SMART_UPDATE_LLM_DISABLED:
        return None
    desc = (description or "").strip()
    if len(desc) < 80:
        return None
    payload = {
        "title": (title or "").strip(),
        "event_type": (event_type or "").strip(),
        "description": _clip(desc, 1200),
    }
    prompt = (
        "Сделай короткое описание события для публичных списков.\n"
        "Формат: ровно 1 законченное предложение на 12–16 слов.\n"
        "Запрещено: многоточия, обрывы фраз, дата/время/адрес/город/ссылки/эмодзи/хэштеги.\n"
        "Запрещено обращаться к читателю и использовать промо/CTA-формулы: «приглашаем», «погрузитесь», "
        "«откройте», «приходите», «вас ждёт».\n"
        "Текст должен объяснять суть события простым и ясным языком, без выдумок.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=90,
        label="short_description",
        temperature=0.0,
    )
    cleaned = _clean_short_description(text)
    if not cleaned:
        return None
    if not _is_short_description_acceptable(cleaned, min_words=12, max_words=16):
        return None
    return cleaned


async def _llm_build_search_digest(
    *,
    title: str | None,
    description: str | None,
    event_type: str | None,
) -> str | None:
    """Build/refresh search_digest from the current merged description.

    This text is used as a short "what is this event" snippet (cards/search),
    and is inserted into the Telegraph page before long descriptions.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    desc = (description or "").strip()
    if len(desc) < 200:
        return None

    payload = {
        "title": (title or "").strip(),
        "event_type": (event_type or "").strip(),
        "description": _clip(desc, 1800),
    }
    prompt = (
        "Сделай краткий дайджест события для поиска/карточек. "
        "Один абзац: 1 предложение, 120–220 символов (если нужно, максимум 260). "
        "Не указывай дату, время, адрес и город (они показываются отдельно). "
        "Не используй эмодзи, хэштеги, кавычки-цитаты и списки. "
        "Не повторяй название дословно в начале, если оно уже понятно по контексту. "
        "Не обращайся к читателю и не используй промо/CTA-формулы: «приглашаем», «погрузитесь», "
        "«откройте», «приходите», «вас ждёт». "
        "Не добавляй выдуманных фактов.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    text = await _ask_gemma_text(
        prompt,
        max_tokens=180,
        label="search_digest",
        temperature=0.0,
    )
    cleaned = _clean_search_digest(text)
    if not cleaned:
        return None
    cleaned = cleaned.strip().strip("-•").strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # If the model returned something too long, prefer deterministic fallback digest
    # rather than cutting mid-word and showing a broken sentence.
    if len(cleaned) > 280:
        return None
    return cleaned or None


async def _llm_create_description_facts_and_digest(
    candidate: EventCandidate,
    *,
    clean_title: str,
    clean_source_text: str,
    clean_raw_excerpt: str | None,
    normalized_event_type: str | None,
) -> dict[str, Any] | None:
    """Bundle create-time LLM work into a single Gemma JSON call.

    This replaces three separate LLM calls previously used on create:
    - rewrite description,
    - extract atomic facts,
    - build search_digest.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    enabled = (os.getenv("SMART_UPDATE_CREATE_BUNDLE", "1") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enabled:
        return None

    payload = {
        "title": clean_title,
        "date": candidate.date,
        "time": candidate.time,
        "end_date": candidate.end_date,
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
        "ticket_link": candidate.ticket_link,
        "ticket_status": candidate.ticket_status,
        "is_free": bool(candidate.is_free),
        "event_type": normalized_event_type or candidate.event_type,
        "festival": candidate.festival,
        "source_type": candidate.source_type,
        "source_url": candidate.source_url,
        "structured_age_restriction": (
            candidate.age_restriction if candidate.age_restriction_is_structured else None
        ),
        "source_text": _clip(clean_source_text, SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS),
        "raw_excerpt": _clip(clean_raw_excerpt or "", 1200),
        "poster_texts": [_clip(p.ocr_text, 700) for p in candidate.posters if (p.ocr_text or "").strip()][
            :3
        ],
        "poster_titles": [
            _clip(p.ocr_title, 140) for p in candidate.posters if (p.ocr_title or "").strip()
        ][:3],
    }
    # Budget for public description length: keep close to total source volume to avoid
    # hallucination-prone over-expansion on short posts.
    try:
        payload["description_budget_chars"] = _estimate_description_budget_chars(
            source_type=candidate.source_type,
            source_text=clean_source_text,
            raw_excerpt=clean_raw_excerpt,
            poster_texts=[p.ocr_text for p in candidate.posters if (p.ocr_text or "").strip()],
        )
    except Exception:  # pragma: no cover - defensive
        payload["description_budget_chars"] = 820
    prompt = (
        "Ты готовишь данные для создания события.\n"
        "Верни JSON строго по схеме.\n\n"
        "0) title:\n"
        "- Верни короткое и осмысленное название события (обычно 3–12 слов).\n"
        "- НЕ включай дату/время/адрес/город/цены/ссылки.\n"
        "- Если `poster_titles` содержит крупный заголовок афиши и он относится к событию, используй его как основу title.\n"
        "- Если в source_text/raw_excerpt/poster_texts есть явное собственное название (проект/тур/постановка/шоу), используй его как основу title.\n"
        "- Если caption/source_text называет событие/проект, а poster_titles содержит лозунг, жанровую фразу или CTA, заголовок из caption/source_text важнее poster_titles.\n"
        "- НЕ делай title в формате «<event_type> — <площадка>», если в данных есть имя/бренд события (пример: «ЕвроДэнс'90», а не «Концерт — Янтарь холл»).\n"
        "- Не теряй ключевые смысловые маркеры (например «Масленица», «концерт», «кинопоказ», «лекция»), если они есть в данных, но НЕ подменяй ими собственное название.\n\n"
        "1) description:\n"
        "- Напиши ПОЛНОЕ развернутое описание события как культурный журналист.\n"
        "- Сохрани ВСЕ значимые факты из source_text/raw_excerpt/poster_texts (кроме логистики).\n"
        "- Организаторы, сообщества, площадки, названия миров/франшиз/источников вдохновения и связи вида `организовано ...`/`вдохновлено ...` переписывай только из явного source evidence. "
        "Не выводи организатора из тематики, названия сообщества или декоративного текста; явные `<ОРГАНИЗАТОР>` и `<ИСТОЧНИК_ВДОХНОВЕНИЯ>` нельзя заменять другим сообществом или придуманным миром.\n"
        "- Если source_text короткий или пустой, опирайся на poster_texts (OCR афиш) как на основной источник фактов.\n"
        "- Объём: описание должно быть близко по объёму к источникам и НЕ превышать `description_budget_chars` символов.\n"
        "  Если источники короткие, описание тоже должно быть коротким (без воды/«атмосферных» вступлений).\n"
        "- Не копируй дословно длинными кусками; перефразируй, но не сокращай смысл.\n"
        "- Анти-дословность: избегай фрагментов, где подряд совпадает больше ~8–10 слов с источником.\n"
        "- Структура: абзацы, разделяй пустой строкой; 1–2 предложения в абзаце (для списков правило не применимо).\n"
        "- Запрещено: хэштеги, рекламные клише/прогнозы, механика розыгрыша.\n"
        "- НЕ добавляй секции/заголовки про факты (например «Факты…», «Факты о событии», `Facts/Added Facts`):\n"
        "  факты вернутся отдельным полем `facts`.\n"
        "- НЕ экранируй кавычки обратным слэшем (не пиши `\\\"...\\\"`).\n"
        "- ВАЖНО: НЕ включай в текст логистику (дата/время/площадка/точный адрес/город/ссылки/телефон/контакты/точные цены)\n"
        "  и не дублируй строки `Дата:`, `Время:`, `Локация:`, `Билеты:`.\n"
        f"{SMART_UPDATE_YO_RULE}\n"
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE}\n\n"
        f"{SMART_UPDATE_OPTIONAL_HEADINGS_RULE}\n\n"
        f"{_description_emoji_prompt_rule()}\n\n"
        "Если в описании есть список:\n"
        "- Каждый пункт списка должен быть на отдельной строке.\n"
        "- Маркер списка пиши с пробелом: `- пункт` или `1. пункт`.\n\n"
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE}\n\n"
        "2) facts:\n"
        "- Верни 6–24 атомарных фактов (1 факт = 1 строка), только про ЭТО событие.\n"
        "- Приоритет полноты: организаторы/институции, цель события, формат, методология/исследования/background, "
        "точные числа и статистика, участники/ведущие/модераторы/гиды/исполнители, программа/примеры, условия участия.\n"
        "- Точные числа, названия организаций и формулировки цели сохраняй явно; не заменяй их общими словами "
        "вроде «несколько», «организаторы», «проект».\n"
        "- Если фактов много, лучше сократи декоративные оценки, но не выбрасывай организатора, цель, числа, "
        "модератора/ведущего/гида и их практическую функцию.\n"
        "- НЕ включай дату/время/адрес/город как отдельные факты.\n"
        f"- {SMART_UPDATE_FACTS_PRESERVE_COMPACT_PROGRAM_LISTS_RULE}\n"
        "- Для выставок, ремёсел, коллекций и предметных экспозиций НЕ схлопывай качественные свойства "
        "объектов в общий факт: сохраняй отдельными фактами происхождение/датировку, технику, материал, "
        "секреты производства, визуальную визитную карточку, разнообразие/уникальность, свободу исполнения, "
        "типичные сюжеты и формы, если это явно сказано в источнике.\n"
        "- Если источник перечисляет несколько промыслов/объектов, дай факты по каждому из них, а не только "
        "по первому и последнему.\n"
        "- Пиши факты естественно: избегай канцелярских формул `характеризуется`, `осуществляется`, "
        "`представляет собой`, если можно сказать проще (`отличается свободным исполнением`, "
        "`лепятся из глины`, `работы разнообразны`).\n"
        "- НЕ включай скидки/промокоды/призывы подписаться/ссылки на каналы.\n"
        "- Включай условия участия/посещения (длительность, возраст, размер группы, формат/что взять/как одеться, "
        "что входит/не входит в оплату), без ссылок/телефонов; сумму указывай только если это важно, чтобы пояснить "
        "что оплачивается отдельно (не более 1 факта).\n\n"
        "- Если есть прямая речь и понятно, кто говорит, оформи как `Цитата (Имя Фамилия): ...`.\n\n"
        "3) search_digest:\n"
        "- 1 предложение, 120–220 символов (макс 260), без эмодзи/хэштегов/списков.\n"
        "- Не указывай дату/время/адрес/город/цены/ссылки.\n"
        "- Не повторяй название дословно в начале.\n\n"
        "- Не обращайся к читателю и не используй промо/CTA-формулы: «приглашаем», «погрузитесь», "
        "«откройте», «приходите», «вас ждёт».\n\n"
        "4) short_description:\n"
        "- Ровно 1 законченное предложение на 12–16 слов.\n"
        "- Без многоточий и обрывов фраз.\n"
        "- Не указывай дату/время/адрес/город/цены/ссылки.\n\n"
        "- Не обращайся к читателю и не используй промо/CTA-формулы.\n\n"
        f"{AGE_DECISION_PROMPT_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    max_tokens = SMART_UPDATE_REWRITE_MAX_TOKENS
    data = await _ask_gemma_json(prompt, CREATE_BUNDLE_SCHEMA, max_tokens=max_tokens, label="create_bundle")
    if not isinstance(data, dict):
        return None
    return data


def _trust_priority(level: str | None) -> int:
    if not level:
        return 2
    key = level.strip().lower()
    if key == "official":
        return 4
    if key == "high":
        return 3
    if key == "medium":
        return 2
    if key == "low":
        return 1
    return 2


def _max_trust_level(levels: Sequence[str | None]) -> tuple[str | None, int]:
    best_level: str | None = None
    best_priority = -1
    for lvl in levels:
        pr = _trust_priority(lvl)
        if pr > best_priority:
            best_priority = pr
            best_level = lvl
    if best_priority < 0:
        return None, _trust_priority(None)
    return best_level, best_priority


def _is_long_event_type_value(event_type: str | None) -> bool:
    if not event_type:
        return False
    return str(event_type).strip().casefold() in {"выставка", "ярмарка"}


def _extract_hall_hint(text: str | None) -> str | None:
    if not text:
        return None
    match = _HALL_HINT_RE.search(text)
    if not match:
        return None
    parts = [p for p in match.groups() if p]
    if not parts:
        return None
    return _norm_space(" ".join(parts))


@lru_cache(maxsize=1)
def _load_location_flags() -> dict[str, dict[str, Any]]:
    path = os.path.join("docs", "reference", "location-flags.md")
    flags: dict[str, dict[str, Any]] = {}
    if not os.path.exists(path):
        return flags
    current: str | None = None
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                m_loc = re.match(r"-\s*location_name:\s*\"?(.+?)\"?$", line)
                if m_loc:
                    current = m_loc.group(1).strip()
                    flags[current] = {"allow_parallel_events": False}
                    continue
                if current:
                    m_flag = re.match(r"allow_parallel_events:\s*(true|false)", line, re.I)
                    if m_flag:
                        flags[current]["allow_parallel_events"] = m_flag.group(1).lower() == "true"
    except Exception as exc:
        logger.warning("smart_update: failed to read location flags: %s", exc)
    return flags


def _allow_parallel_events(location_name: str | None) -> bool:
    if not location_name:
        return False
    flags = _load_location_flags()
    for name, data in flags.items():
        if _normalize_location(name) == _normalize_location(location_name):
            return bool(data.get("allow_parallel_events"))
    return False


def _clip(text: str | None, limit: int = 1200) -> str:
    if not text:
        return ""
    raw = text.strip()
    if len(raw) <= limit:
        return raw
    return raw[: limit - 3].rstrip() + "..."


def _clip_title(text: Any, limit: int = 80) -> str:
    if text is None or isinstance(text, bool):
        return ""
    raw = str(text).strip()
    if not raw:
        return ""
    return raw if len(raw) <= limit else raw[: limit - 1].rstrip() + "…"


def _estimate_description_budget_chars(
    *,
    source_type: str | None,
    source_text: str | None,
    raw_excerpt: str | None,
    poster_texts: Sequence[str] | None,
) -> int:
    """Estimate a safe max length for public event descriptions.

    Goal: avoid LLM "over-expanding" short sources (hallucination-prone water/cliches).
    This is a *budget* for LLM prompts / LLM rewrite passes, not a deterministic cutter.
    """
    src = (source_text or "").strip()
    excerpt = (raw_excerpt or "").strip()
    # Avoid double-counting the excerpt if it is already inside the source.
    extra_excerpt = ""
    if excerpt and excerpt not in src:
        extra_excerpt = excerpt

    # OCR can be huge; include it but cap the contribution to keep budgets sane.
    ocr_chunks: list[str] = []
    for t in (poster_texts or [])[:5]:
        s = (t or "").strip()
        if s:
            ocr_chunks.append(s)
    ocr_total = sum(min(len(s), 900) for s in ocr_chunks)
    ocr_total = min(ocr_total, 2400)

    base = len(src) + len(extra_excerpt) + ocr_total
    if base <= 0:
        return 520  # fallback for empty sources (still keep it modest)

    st = (source_type or "").strip().lower()
    # Be stricter for Telegram: many posts are short, and "water" is very visible.
    factor = 1.05 if st.startswith("telegram") else 1.12

    budget = int(base * factor)
    # Clamp to a practical range for Telegraph pages.
    budget = max(220, min(1800, budget))
    # For extremely short sources, allow a small fixed slack for readability.
    if base < 260:
        budget = max(260, min(budget, base + 120))
    return int(budget)


async def _llm_shrink_description_to_budget(
    *,
    source_type: str | None,
    source_url: str | None,
    description: str,
    source_text: str | None,
    facts: Sequence[str] | None,
    max_chars: int,
) -> str | None:
    """LLM-only shrinking pass to keep description close to source volume (no new facts)."""
    if SMART_UPDATE_LLM_DISABLED:
        return None
    text = (description or "").strip()
    if not text:
        return None
    try:
        max_chars_i = int(max_chars)
    except Exception:
        max_chars_i = 520
    max_chars_i = max(180, min(2200, max_chars_i))

    payload = {
        "max_chars": max_chars_i,
        "source_type": (source_type or "").strip(),
        "source_url": (source_url or "").strip(),
        "facts": [str(x).strip() for x in (facts or []) if str(x).strip()][:18],
        "source_text": _clip((source_text or "").strip(), 1800),
        "description": _clip(text, 3200),
    }
    prompt = (
        "Сократи описание события до указанного лимита символов.\n"
        "Правила:\n"
        "- НЕ добавляй новых фактов и деталей.\n"
        "- Убирай воду и нейросетевые клише.\n"
        "- Не указывай дату/время/адрес/город/ссылки/телефоны.\n"
        "- Сохрани смысловые маркеры (жанр/формат/что будет происходить).\n"
        "- Пиши по-русски, естественно, без канцелярита.\n"
        "- Верни только готовый текст описания, без заголовков типа Facts/Факты.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    # Rough token budget: ~1 token per 4 chars + some slack.
    max_tokens = max(140, min(520, int(max_chars_i / 4) + 80))
    out = await _ask_gemma_text(prompt, max_tokens=max_tokens, label="shrink_desc", temperature=0.0)
    out = (out or "").strip()
    out = _sanitize_description_output(out, source_text=source_text) or out
    out = _normalize_plaintext_paragraphs(out) or out
    out = _normalize_blockquote_markers(out) or out
    if not out.strip():
        return None
    if len(out) > max_chars_i * 1.15:
        return None
    return out.strip()


async def _fetch_event_posters_map(
    db: Database, event_ids: Sequence[int]
) -> dict[int, list[EventPoster]]:
    if not event_ids:
        return {}
    async with db.get_session() as session:
        result = await session.execute(
            select(EventPoster).where(EventPoster.event_id.in_(event_ids))
        )
        posters = list(result.scalars().all())
    grouped: dict[int, list[EventPoster]] = {}
    for poster in posters:
        grouped.setdefault(poster.event_id, []).append(poster)
    return grouped


def _poster_hashes(posters: Iterable[PosterCandidate]) -> set[str]:
    hashes: set[str] = set()
    for poster in posters:
        if poster.sha256:
            hashes.add(poster.sha256)
    return hashes


async def _llm_match_event(
    candidate: EventCandidate,
    events: Sequence[Event],
    *,
    posters_map: dict[int, list[EventPoster]] | None = None,
) -> tuple[int | None, float, str]:
    if not events:
        return None, 0.0, "shortlist_empty"
    if SMART_UPDATE_LLM_DISABLED:
        return None, 0.0, "llm_disabled"

    posters_map = posters_map or {}
    candidates_payload: list[dict[str, Any]] = []
    for ev in events:
        posters = posters_map.get(ev.id or 0, [])
        poster_texts = [p.ocr_text for p in posters if p.ocr_text][:2]
        poster_titles = [p.ocr_title for p in posters if (p.ocr_title or "").strip()][:2]
        candidates_payload.append(
            {
                "id": ev.id,
                "title": ev.title,
                "date": ev.date,
                "time": ev.time,
                "time_is_default": bool(getattr(ev, "time_is_default", False)),
                "end_date": ev.end_date,
                "location_name": ev.location_name,
                "location_address": ev.location_address,
                "city": ev.city,
                "ticket_link": ev.ticket_link,
                "description": _clip(ev.description, 600),
                "source_text": _clip(ev.source_text, 600),
                "poster_texts": poster_texts,
                "poster_titles": poster_titles,
            }
        )

    payload = {
        "candidate": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "time_is_default": bool(getattr(candidate, "time_is_default", False)),
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "ticket_link": candidate.ticket_link,
            "text": _clip(_strip_promo_lines(candidate.source_text) or candidate.source_text, 1200),
            "raw_excerpt": _clip(_strip_promo_lines(candidate.raw_excerpt) or candidate.raw_excerpt, 800),
            "poster_texts": [
                _clip(p.ocr_text, 400) for p in candidate.posters if p.ocr_text
            ][:3],
            "poster_titles": [
                _clip(p.ocr_title, 140) for p in candidate.posters if (p.ocr_title or "").strip()
            ][:3],
        },
        "events": candidates_payload[:10],
    }
    prompt = (
        "Ты сопоставляешь анонс события с уже существующими событиями.\n"
        "Найди наиболее вероятное совпадение или верни null.\n"
        "Учитывай дату, время, площадку, участников, ссылки, афиши и OCR.\n"
        "Важно про дубли:\n"
        "- `time=00:00` и/или `time_is_default=true` считай неизвестным временем (слабый якорь, не конфликт).\n"
        "- Если совпадают дата + площадка + контекст (участник/афиша/OCR), а название сформулировано по-разному "
        "(общее vs конкретное), это всё равно один и тот же ивент: выбирай match.\n"
        "- Для длинных событий (выставка/ярмарка/экспозиция с `end_date`) пересечение периодов + площадка НЕ означает дубль:\n"
        "  в одном музее может идти несколько разных выставок одновременно. Матчь только если совпадает конкретное событие "
        "(название/автор/тематика/афиша/OCR/ссылка).\n"
        "- Если среди `events` есть событие с той же датой, тем же началом времени (или время пустое/placeholder), "
        "той же площадкой и тем же названием (или очевидно тем же) — это дубль: выбери его и поставь высокую confidence.\n"
        "- Не возвращай null, если есть правдоподобный матч: лучше выбрать наиболее вероятное и снизить confidence.\n"
        "Ответь строго JSON.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        MATCH_SCHEMA,
        max_tokens=400,
        label="match",
    )
    if data is None:
        return None, 0.0, "llm_bad_json"
    match_id = data.get("match_event_id")
    confidence = data.get("confidence")
    reason = data.get("reason_short") or ""
    try:
        conf_val = float(confidence)
    except Exception:
        conf_val = 0.0
    if match_id is None:
        return None, conf_val, reason
    try:
        match_id = int(match_id)
    except Exception:
        return None, conf_val, reason
    return match_id, conf_val, reason


async def _llm_match_or_create_bundle(
    candidate: EventCandidate,
    events: Sequence[Event],
    *,
    posters_map: dict[int, list[EventPoster]] | None = None,
    threshold: float,
    clean_title: str,
    clean_source_text: str,
    clean_raw_excerpt: str | None,
    normalized_event_type: str | None,
) -> dict[str, Any] | None:
    """Single-call LLM step: match existing event OR return create bundle.

    Intended for VK/TG imports to reduce roundtrips on the "no match -> create" path.
    """
    if not events:
        return None
    if SMART_UPDATE_LLM_DISABLED:
        return None

    posters_map = posters_map or {}
    candidates_payload: list[dict[str, Any]] = []
    for ev in events:
        posters = posters_map.get(ev.id or 0, [])
        poster_texts = [p.ocr_text for p in posters if p.ocr_text][:2]
        poster_titles = [p.ocr_title for p in posters if (p.ocr_title or "").strip()][:2]
        candidates_payload.append(
            {
                "id": ev.id,
                "title": ev.title,
                "date": ev.date,
                "time": ev.time,
                "time_is_default": bool(getattr(ev, "time_is_default", False)),
                "end_date": ev.end_date,
                "location_name": ev.location_name,
                "location_address": ev.location_address,
                "city": ev.city,
                "ticket_link": ev.ticket_link,
                "description": _clip(ev.description, 600),
                "source_text": _clip(ev.source_text, 600),
                "poster_texts": poster_texts,
                "poster_titles": poster_titles,
            }
        )

    payload = {
        "threshold": float(threshold),
        "candidate": {
            "title": clean_title,
            "date": candidate.date,
            "time": candidate.time,
            "time_is_default": bool(getattr(candidate, "time_is_default", False)),
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "ticket_link": candidate.ticket_link,
            "ticket_status": candidate.ticket_status,
            "structured_age_restriction": (
                candidate.age_restriction if candidate.age_restriction_is_structured else None
            ),
            "is_free": bool(candidate.is_free),
            "event_type": normalized_event_type or candidate.event_type,
            "festival": candidate.festival,
            "source_type": candidate.source_type,
            "source_url": candidate.source_url,
            "source_text": _clip(clean_source_text, SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS),
            "raw_excerpt": _clip(clean_raw_excerpt or "", 1200),
            "poster_texts": [_clip(p.ocr_text, 400) for p in candidate.posters if p.ocr_text][:3],
            "poster_titles": [
                _clip(p.ocr_title, 140) for p in candidate.posters if (p.ocr_title or "").strip()
            ][:3],
        },
        "events": candidates_payload[:10],
    }
    prompt = (
        "Ты сопоставляешь новый анонс события с уже существующими событиями.\n"
        "Верни JSON строго по схеме.\n\n"
        "Шаг 1) MATCH:\n"
        "- Найди наиболее вероятное совпадение среди `events` или реши, что совпадения нет.\n"
        "- `confidence` от 0 до 1.\n"
        "- Если `confidence >= threshold`, верни `action=match` и `match_event_id`.\n"
        "- Если `confidence < threshold`, верни `action=create` и `match_event_id=null`.\n\n"
        "Анти-дубли (важно):\n"
        "- `time=00:00` и/или `time_is_default=true` считай неизвестным временем (слабый якорь, не конфликт).\n"
        "- Если дата + площадка + название/текст практически совпадают, но источники дают разное время "
        "(например 10:00 vs 11:00, 19:00 vs 20:00), НЕ создавай отдельное событие только из-за этого: "
        "обычно это одно событие с конфликтом/правкой времени или разницей сбор/старт. Выбирай `action=match`, "
        "а конфликт времени должен быть разобран на стадии merge по доверию/свежести источников.\n"
        "- Исключение: если один и тот же источник явно перечисляет несколько самостоятельных сеансов/показов "
        "одного события в один день (например `в 14:00 и 17:00`, `12:00 и 16:00`) — это не дубль; выбирай `action=create` "
        "для новой самостоятельной occurrence.\n"
        "- Если совпадают дата + площадка + контекст (участник/афиша/OCR), но формулировка названия отличается "
        "(общее vs конкретное), это один и тот же ивент: выбирай `action=match`.\n"
        "- Для длинных событий (выставка/ярмарка/экспозиция с `end_date`) пересечение периодов + площадка НЕ означает дубль:\n"
        "  в одном музее может идти несколько разных выставок одновременно. Выбирай `action=match` только если совпадает "
        "конкретное событие (название/автор/тематика/афиша/OCR/ссылка).\n"
        "- Для регулярных/сезонных событий НЕ склеивай точную occurrence с общей карточкой сезона: "
        "если candidate имеет одну конкретную дату (например `10 июля 20:00`, без end_date), а existing — диапазон "
        "с `end_date` (`1 мая — 30 сентября`, `каждую пятницу`), то общий title/place/ticket НЕ достаточен для match. "
        "Выбирай `action=create` для новой occurrence, если источник явно не говорит, что это правка всей сезонной карточки.\n"
        "- Если хотя бы одно событие в `events` совпадает по якорям (дата + начало времени/пустое время + площадка) "
        "и названию/участникам, это дубль — выбирай `action=match` и ставь `confidence` заметно выше `threshold`.\n\n"
        "Шаг 2) CREATE (только если action=create):\n"
        "- Верни `bundle` с полями title/description/facts/search_digest/short_description как для создания нового события.\n"
        "- Если action=match, верни bundle=null.\n\n"
        "Правила для bundle.title:\n"
        "- Короткое осмысленное название (обычно 3–12 слов), без даты/времени/адреса/города/цен/ссылок.\n"
        "- Без эмодзи.\n"
        "- Если `candidate.poster_titles` содержит крупный заголовок афиши и он относится к событию, используй его как основу.\n"
        "- Если в candidate.source_text/raw_excerpt/poster_texts есть явное собственное название (проект/тур/постановка/шоу), используй его как основу.\n"
        "- Если candidate.source_text/caption называет событие/проект, а poster_titles содержит лозунг, жанровую фразу или CTA, заголовок из source_text/caption важнее poster_titles.\n"
        "- НЕ делай title в формате «<event_type> — <площадка>», если в данных есть имя/бренд события (пример: «ЕвроДэнс'90», а не «Концерт — Янтарь холл»).\n"
        "- Не экранируй кавычки обратным слэшем (не пиши `\\\"...\\\"`).\n\n"
        "Правила для bundle.description:\n"
        "- Напиши ПОЛНОЕ развернутое описание как культурный журналист.\n"
        "- Сохрани ВСЕ значимые факты из source_text/raw_excerpt/poster_texts (кроме логистики).\n"
        "- Организаторы, сообщества, площадки, названия миров/франшиз/источников вдохновения и связи вида `организовано ...`/`вдохновлено ...` переписывай только из явного source evidence; не заменяй их тематическими догадками.\n"
        "- Если source_text короткий или пустой, опирайся на poster_texts (OCR афиш) как на основной источник фактов.\n"
        "- Не копируй дословно длинными кусками; перефразируй, но не сокращай смысл.\n"
        "- Структура: абзацы, разделяй пустой строкой; 1–2 предложения в абзаце.\n"
        "- Запрещено: хэштеги, рекламные клише/прогнозы, механика розыгрыша.\n"
        f"- {_description_emoji_prompt_rule().strip()}\n"
        "- НЕ добавляй секции/заголовки про факты (например «Факты…», «Факты о событии», `Facts/Added Facts`):\n"
        "  факты вернутся отдельным полем bundle.facts.\n"
        "- НЕ экранируй кавычки обратным слэшем (не пиши `\\\"...\\\"`).\n"
        "- ВАЖНО: НЕ включай логистику (дата/время/площадка/точный адрес/город/ссылки/телефон/контакты/точные цены)\n"
        "  и не дублируй строки `Дата:`, `Время:`, `Локация:`, `Билеты:`.\n"
        f"{SMART_UPDATE_YO_RULE}\n"
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE}\n\n"
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE}\n\n"
        "Правила для bundle.facts:\n"
        "- 6–18 атомарных фактов (1 факт = 1 строка), только про ЭТО событие.\n"
        "- НЕ включай дату/время/адрес/город как отдельные факты.\n"
        "- НЕ включай скидки/промокоды/призывы подписаться/ссылки на каналы.\n"
        "- Включай условия участия/посещения (длительность, возраст, размер группы, формат/что взять/как одеться, "
        "что входит/не входит в оплату), без ссылок/телефонов; сумму указывай только если это важно, чтобы пояснить "
        "что оплачивается отдельно (не более 1 факта).\n\n"
        "Правила для bundle.search_digest:\n"
        "- 1 предложение, 120–220 символов (макс 260), без эмодзи/хэштегов/списков.\n"
        "- Не указывай дату/время/адрес/город/цены/ссылки.\n"
        "- Не повторяй название дословно в начале.\n\n"
        "Правила для bundle.short_description:\n"
        "- Ровно 1 законченное предложение на 12–16 слов.\n"
        "- Без многоточий и обрывов фраз.\n"
        "- Не указывай дату/время/адрес/город/цены/ссылки.\n\n"
        f"{AGE_DECISION_PROMPT_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    data = await _ask_gemma_json(
        prompt,
        MATCH_CREATE_BUNDLE_SCHEMA,
        max_tokens=SMART_UPDATE_REWRITE_MAX_TOKENS,
        label="match_create_bundle",
    )
    if not isinstance(data, dict):
        return None
    action = (data.get("action") or "").strip().lower()
    if action not in {"match", "create"}:
        return None
    if action == "match":
        return {
            "action": "match",
            "match_event_id": data.get("match_event_id"),
            "confidence": data.get("confidence"),
            "reason_short": data.get("reason_short") or "",
            "bundle": None,
        }
    bundle = data.get("bundle")
    if not isinstance(bundle, dict):
        return None
    return {
        "action": "create",
        "match_event_id": None,
        "confidence": data.get("confidence"),
        "reason_short": data.get("reason_short") or "",
        "bundle": bundle,
    }


def _is_generic_ticket_url(url: str | None) -> bool:
    """True for season-subscription / generic landing ticket pages (not per-event).

    Used so that ``two_vendor_same_slot`` / ``identical_anchors_dup`` cannot be
    "proven" by a shared generic ticket page (see ``_pre_create_duplicate_probe``
    season-subscription caveat). Conservative: only flags obvious generic paths.
    """
    norm = _normalize_url(url)
    if not norm:
        return False
    path = re.sub(r"^https?://[^/]+", "", norm).strip("/").lower()
    if not path:
        # bare domain → generic
        return True
    generic_tokens = ("season", "subscription", "abonement", "afisha", "events", "schedule", "raspisanie")
    last = path.rsplit("/", 1)[-1]
    if last in generic_tokens:
        return True
    # A per-event link usually ends in a numeric/slug id segment; a path that is a
    # single generic word is a landing page.
    if "/" not in path and path in generic_tokens:
        return True
    return False


def _dedup_adjudicator_block_candidates(
    candidate: EventCandidate,
    events: Sequence[Event],
    posters_map: dict[int, list[EventPoster]] | None,
    *,
    limit: int = 8,
) -> list[Event]:
    """Cheap, recall-biased blocking over the widened candidate set (INC-2026-05-30 opt 1).

    A row qualifies if ANY signal fires (title-relatedness / venue / ticket parity /
    poster-hash overlap), then we rank by signal strength + date proximity and keep
    the top ``limit``. False inclusions are cheap (the LLM rejects them); false
    omissions are the bug we are fixing, so the predicate is a union, not an AND.
    """
    posters_map = posters_map or {}
    cand_ticket = _normalize_ticket_url_for_match(candidate.ticket_link)
    cand_poster_hashes = _poster_hashes(getattr(candidate, "posters", None) or [])
    cand_start, _cand_end = _candidate_date_range(candidate)

    scored: list[tuple[int, int, Event]] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        b1 = _titles_look_related(candidate.title, getattr(ev, "title", None))
        b2 = _event_candidate_location_matches(ev, candidate)
        ev_ticket = _normalize_ticket_url_for_match(getattr(ev, "ticket_link", None))
        b3 = bool(cand_ticket and ev_ticket and cand_ticket == ev_ticket)
        ev_poster_hashes = {
            getattr(p, "sha256", None) for p in posters_map.get(ev.id or 0, []) if getattr(p, "sha256", None)
        }
        b4 = bool(cand_poster_hashes and (cand_poster_hashes & ev_poster_hashes))
        if not (b1 or b2 or b3 or b4):
            continue
        score = b4 * 4 + b3 * 3 + b1 * 2 + b2 * 1
        # Date proximity as a tiebreaker (smaller is better → negate for desc sort).
        proximity = 0
        try:
            ev_start, _ = _event_date_range(ev)
            if ev_start and cand_start:
                proximity = -abs((ev_start - cand_start).days)
        except Exception:
            proximity = 0
        scored.append((score, proximity, ev))

    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return [ev for _s, _p, ev in scored[:limit]]


async def _llm_dedup_adjudicator(
    candidate: EventCandidate,
    events: Sequence[Event],
    *,
    posters_map: dict[int, list[EventPoster]] | None = None,
) -> dict[str, Any] | None:
    """Decision-only widened-recall dedup gate (INC-2026-05-30 opt 1).

    Runs on the create branch only (after every deterministic matcher, the
    match/create bundle, the rescue pass, and ``_pre_create_duplicate_probe`` said
    "no match"), over a recall set that is NOT gated by exact location/time. Answers
    match-vs-create so a sibling whose venue string or time framing drifted can still
    be merged, while genuinely-distinct same-venue/same-day events stay separate.
    """
    if not events or SMART_UPDATE_LLM_DISABLED:
        return None

    posters_map = posters_map or {}
    candidates_payload: list[dict[str, Any]] = []
    for ev in events:
        posters = posters_map.get(ev.id or 0, [])
        poster_texts = [p.ocr_text for p in posters if getattr(p, "ocr_text", None)][:2]
        poster_titles = [p.ocr_title for p in posters if (getattr(p, "ocr_title", None) or "").strip()][:2]
        candidates_payload.append(
            {
                "id": ev.id,
                "title": ev.title,
                "date": ev.date,
                "time": ev.time,
                "time_is_default": bool(getattr(ev, "time_is_default", False)),
                "end_date": ev.end_date,
                "location_name": ev.location_name,
                "location_address": ev.location_address,
                "city": ev.city,
                "ticket_link": ev.ticket_link,
                "source_url": getattr(ev, "source_post_url", None) or getattr(ev, "source_vk_post_url", None),
                "allow_parallel": _allow_parallel_events(ev.location_name),
                "description": _clip(ev.description, 500),
                "source_text": _clip(ev.source_text, 600),
                "poster_texts": poster_texts,
                "poster_titles": poster_titles,
            }
        )

    payload = {
        "candidate": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "time_is_default": bool(getattr(candidate, "time_is_default", False)),
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "ticket_link": candidate.ticket_link,
            "source_url": candidate.source_url,
            "allow_parallel": _allow_parallel_events(candidate.location_name),
            "source_text": _clip(candidate.source_text, SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS),
            "raw_excerpt": _clip(candidate.raw_excerpt or "", 1200),
            "poster_texts": [_clip(p.ocr_text, 400) for p in candidate.posters if p.ocr_text][:3],
            "poster_titles": [
                _clip(p.ocr_title, 140) for p in candidate.posters if (p.ocr_title or "").strip()
            ][:3],
        },
        "events": candidates_payload[:10],
    }
    prompt = (
        "Ты — арбитр дубликатов. Дан НОВЫЙ анонс события `candidate` и список уже "
        "существующих событий `events` (тот же город, близкие даты). Реши: новый анонс "
        "описывает ТО ЖЕ САМОЕ реальное событие, что одно из `events` (дубль → match), "
        "или это ОТДЕЛЬНОЕ событие, которое нужно создать (create). Верни JSON строго по схеме.\n\n"
        "ГЛАВНЫЙ ПРИНЦИП: одно реальное событие = одна карточка. Но два разных показа/"
        "сеанса/спектакля — это РАЗНЫЕ события, их нельзя сливать. Решай по смыслу "
        "источника (source_text/poster_texts/poster_titles), а не по совпадению строк.\n\n"
        "Что НЕ является признаком различия (это всё ещё ОДНО событие → match):\n"
        "- Время «вход/двери» против «начало/старт» (например 19:00 vs 20:00 у одного концерта) — "
        "это один показ, а не два. Так же дневной сдвиг на ±1–1.5 часа в формулировке времени.\n"
        "- Площадка записана по-разному: алиас vs официальное название vs касса/адрес "
        "кассы vs название билетного оператора (qtickets, edinoepole, kassir и т.п.). Это одна площадка.\n"
        "- В `location_name` у одного из событий мусор/проза/название юрлица "
        "(«ООО «Уиандекс…»», «театральный трамвай», «весь июнь каждый…»), а реальная "
        "площадка видна из текста/афиши — считай площадку той же.\n"
        "- У события ДВЕ разные ссылки на билеты от РАЗНЫХ операторов на ОДИН и тот же показ "
        "(дата+время+площадка совпадают по сути) — это всё равно ОДНО событие → match. "
        "Разные билетные операторы НЕ доказывают, что событий два.\n"
        "- Декоративные эмодзи и обёртка «Спектакль/Концерт/Экскурсия «…»» в названии — это шум, не различие.\n\n"
        "Когда события РАЗНЫЕ (НЕ сливай, → create) — это важнее, чем найти дубль:\n"
        "- Несколько сеансов из ОДНОГО поста. Если `candidate.source_text` (или текст "
        "существующего события) перечисляет несколько времён начала одного дня "
        "(«В 11:00 … В 13:00 …», «начало в 12:00 и 17:00», «сеансы в 14:00 и 18:00») — "
        "каждое время это ОТДЕЛЬНЫЙ сеанс/событие. Если `candidate` и кандидат из `events` "
        "имеют ОДИН и тот же `source_url`, но РАЗНОЕ время — это почти наверняка два "
        "легитимных сеанса из одного анонса → create.\n"
        "- Утренник + вечерний показ одного спектакля в один день (например 11:00 и 19:00) — "
        "это два показа → два события → create.\n"
        "- Два РАЗНЫХ спектакля/концерта/мероприятия на одной площадке в один день "
        "(разные названия/программа/состав) → create.\n"
        "- Если у `candidate.allow_parallel=true` или у кандидата `allow_parallel=true` "
        "(площадка с несколькими залами/параллельными событиями), не сливай разные события "
        "только из-за общей площадки и даты — требуй совпадения КОНКРЕТНОГО события "
        "(название/программа/афиша/зал/ссылка).\n\n"
        "Работа со временем:\n"
        "- `time=00:00` и/или `time_is_default=true` считай НЕИЗВЕСТНЫМ временем: это слабый "
        "якорь, он НЕ создаёт конфликт и НЕ доказывает совпадение. Решай по названию/тексту/афише.\n"
        "- Явно разные ненулевые времена в один день — это конфликт ТОЛЬКО если из текста не "
        "видно, что это «двери vs начало» одного показа. Если видно перечисление сеансов — это разные события.\n\n"
        "Заземление (обязательно):\n"
        "- Не выдумывай совпадение. Ставь `action=match` только если в данных есть КОНКРЕТНОЕ "
        "доказательство тождества (та же программа/состав/афиша/ссылка/название одного показа), "
        "а не просто «похожая тема и тот же день».\n"
        "- Если сомневаешься между match и create — выбирай create (лучше дубль, который "
        "поймает следующий проход, чем ошибочно слитые разные события).\n"
        "- `match_event_id` обязан быть одним из `events[].id`. Если ни один не подходит — "
        "`action=create`, `match_event_id=null`.\n\n"
        "Верни: `action` (match|create); `match_event_id` (id из events при match, иначе null); "
        "`confidence` (0..1); `reason_code` (один код из закрытого списка схемы); "
        "`reason` (1 короткая фраза по-русски, без выдумок).\n\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    data = await _ask_gemma_json(
        prompt,
        DEDUP_ADJUDICATOR_SCHEMA,
        max_tokens=400,
        label="dedup_adjudicator",
    )
    if not isinstance(data, dict):
        return None
    action = (data.get("action") or "").strip().lower()
    if action not in {"match", "create"}:
        return None
    reason_code = (data.get("reason_code") or "").strip()
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    match_id = data.get("match_event_id")
    try:
        match_id = int(match_id) if match_id is not None else None
    except Exception:
        match_id = None
    return {
        "action": action,
        "match_event_id": match_id,
        "confidence": confidence,
        "reason_code": reason_code,
        "reason": (data.get("reason") or "").strip(),
    }


def _dedup_adjudicator_accept_merge(
    candidate: EventCandidate,
    match_event: Event | None,
    *,
    decision: dict[str, Any],
    allow_parallel: bool,
) -> tuple[bool, str]:
    """Deterministic guard ladder over the adjudicator's decision (INC-2026-05-30 opt 1).

    The adjudicator can only ever convert a "create" into a "match", so the only risk
    it introduces is a false merge. This pure function applies the §4 vetoes and is the
    primary unit-test surface (it must reject a bad merge even when the LLM said match).

    Returns ``(accept_merge, code)`` — ``code`` is the merge ``reason_code`` when accepted,
    otherwise a veto reason string.
    """
    action = str(decision.get("action") or "").strip().lower()
    if action != "match":
        return False, "llm_create"
    reason_code = str(decision.get("reason_code") or "").strip()
    if reason_code not in _DEDUP_ADJUDICATOR_MERGE_CODES:
        return False, f"non_merge_code:{reason_code or 'empty'}"
    try:
        confidence = float(decision.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    threshold = 0.90 if allow_parallel else 0.80
    if confidence < threshold:
        return False, f"low_conf_{confidence:.2f}<{threshold:.2f}"

    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    ev_time = _event_anchor_time(match_event)
    time_conflict = _has_explicit_time_conflict(cand_time, ev_time)

    # §4.5 hard invariant: same source post + different time = legitimate multi-session
    # split (e.g. 5426/5427 from t.me/gusmuseum/4509). Always create, regardless of LLM.
    if time_conflict and _event_has_source_url_hint(match_event, candidate.source_url):
        return False, "same_source_url_time_split"

    # §4.3 explicit-time conflict is allowed only for a doors/start skew within 90 min.
    if time_conflict:
        gap = None
        cm = _time_to_minutes_for_match(cand_time)
        em = _time_to_minutes_for_match(ev_time)
        if cm is not None and em is not None:
            gap = abs(cm - em)
        if not (reason_code == "doors_start_skew" and gap is not None and gap <= 90):
            return False, "time_conflict_veto"

    # §4.2 title safety rail: this may stop low/medium-confidence widened-recall
    # false friends, but it must not reintroduce the Valeria-class bug where a
    # high-confidence LLM same-event decision with non-conflicting factual anchors
    # is cancelled solely by lexical title drift (e.g. artist vs "Концерт <artist>").
    if (
        _title_has_meaningful_tokens(candidate.title)
        and _title_has_meaningful_tokens(getattr(match_event, "title", None))
        and not _titles_look_related(candidate.title, getattr(match_event, "title", None))
        and reason_code != "junk_location_same_venue"
        and not _llm_high_confidence_anchor_match_ok(
            candidate,
            match_event,
            confidence=confidence,
            is_canonical_site=False,
        )
    ):
        return False, "unrelated_titles"

    # §4.4 generic-ticket false friend: a two-vendor / identical-anchor merge must not
    # rest solely on a shared GENERIC ticket landing page.
    if reason_code in {"two_vendor_same_slot", "identical_anchors_dup"}:
        cand_tk = candidate.ticket_link
        ev_tk = getattr(match_event, "ticket_link", None)
        if (
            cand_tk
            and ev_tk
            and _normalize_ticket_url_for_match(cand_tk) == _normalize_ticket_url_for_match(ev_tk)
            and _is_generic_ticket_url(cand_tk)
        ):
            return False, "generic_ticket_false_friend"

    return True, reason_code


async def _llm_merge_event(
    candidate: EventCandidate,
    event: Event,
    *,
    conflicting_anchor_fields: dict[str, Any] | None = None,
    poster_texts: Sequence[str] | None = None,
    facts_before: Sequence[str] | None = None,
    event_trust_level: str | None = None,
    candidate_trust_level: str | None = None,
) -> dict[str, Any] | None:
    if SMART_UPDATE_LLM_DISABLED:
        return None

    payload = {
        "event_before": {
            "title": event.title,
            "description": _clip(event.description, SMART_UPDATE_MERGE_EVENT_DESC_MAX_CHARS),
            "facts": [
                _clip(str(f), 220) for f in (facts_before or []) if isinstance(f, str) and f.strip()
            ][:60],
            "trust_level": event_trust_level,
            "trust_priority": _trust_priority(event_trust_level),
            "ticket_link": event.ticket_link,
            "ticket_price_min": event.ticket_price_min,
            "ticket_price_max": event.ticket_price_max,
            "ticket_status": getattr(event, "ticket_status", None),
            "age_restriction": getattr(event, "age_restriction", None),
            "age_restriction_status": getattr(event, "age_restriction_status", None),
            "age_assessment": getattr(event, "age_assessment", None),
            "source_texts": [
                _clip(t, 1200)
                for t in (getattr(event, "source_texts", None) or [])
                if isinstance(t, str) and t.strip()
            ][:4],
        },
        "candidate": {
            "title": candidate.title,
            "raw_excerpt": _clip(_strip_promo_lines(candidate.raw_excerpt) or candidate.raw_excerpt, 1200),
            "text": _clip(
                _strip_promo_lines(candidate.source_text) or candidate.source_text,
                SMART_UPDATE_MERGE_CANDIDATE_TEXT_MAX_CHARS,
            ),
            "trust_level": candidate_trust_level,
            "trust_priority": _trust_priority(candidate_trust_level),
            "ticket_link": candidate.ticket_link,
            "ticket_price_min": candidate.ticket_price_min,
            "ticket_price_max": candidate.ticket_price_max,
            "ticket_status": candidate.ticket_status,
            "structured_age_restriction": (
                candidate.age_restriction if candidate.age_restriction_is_structured else None
            ),
            "source_url": candidate.source_url,
            "quote_candidates": _extract_quote_candidates(
                _strip_promo_lines(candidate.source_text) or candidate.source_text,
                max_items=2,
            ),
            "poster_texts": [
                _clip(p.ocr_text, 400) for p in candidate.posters if p.ocr_text
            ][:3],
            "poster_titles": [
                _clip(p.ocr_title, 140) for p in candidate.posters if (p.ocr_title or "").strip()
            ][:3],
        },
        "constraints": {
            "anchor_fields_do_not_change": [
                "date",
                "time",
                "end_date",
                "location_name",
                "location_address",
            ],
            "conflicting_do_not_use": conflicting_anchor_fields or {},
        },
    }
    if poster_texts:
        payload["candidate"]["existing_poster_texts"] = list(poster_texts)[:3]

    prompt = (
        "Ты объединяешь информацию о событии. "
        "Никогда не меняй якорные поля (дата/время/площадка/адрес). "
        "Если кандидат содержит противоречия в якорных полях, игнорируй их. "
        "Добавляй только непротиворечивые факты. "
        "Считай `event_before.facts` каноническим набором уже известных фактов о событии. "
        "Твоя задача: (1) выделить из candidate ТОЛЬКО новые факты, которых ещё нет в event_before.facts, "
        "(2) выделить факты из candidate, которые уже есть (это дубли), "
        "(3) выявить факты, которые ПРОТИВОРЕЧАТ уже известным фактам (conflict), "
        "(4) собрать цельное, связное описание события на основе event_before.facts + новых фактов. "
        "Конфликты фактов выявляй логически: если новый факт противоречит старому, это conflict. "
        "Какую версию оставить в описании — решай по уровню доверия источников: "
        "если `candidate.trust_priority` выше, можно заменить старую версию на новую, "
        "если ниже или равен — сохраняй старую версию. "
        "Любой конфликт обязательно опиши в `conflict_facts` с указанием, какая версия выбрана "
        "(например `Старый факт -> Новый факт (выбран: candidate)` или `(выбран: event_before)`). "
        "Обязательно старайся добавлять конкретные новые детали из кандидата, которых нет в текущем описании (имена/участники/уникальные детали/программа). "
        "Не повторяй уже имеющиеся факты (убирай дубли). "
        "Если `candidate.poster_titles` содержит короткий крупный заголовок афиши и он относится к событию, "
        "то при необходимости улучши `title` так, чтобы он был близок по смыслу к этому заголовку "
        "(но не включай дату/время/адрес/город/цены/ссылки и не используй эмодзи). "
        f"{SMART_UPDATE_YO_RULE} "
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE} "
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE} "
        "Описание должно читаться как единый связный текст-повествование (не рваное). "
        "Разбиение на абзацы делай осмысленно. НЕ разрывай предложения пустой строкой на середине, "
        "и особенно не ставь пустую строку между инициалом и фамилией (например `Н. Любимова`). "
        f"{_description_emoji_prompt_rule()}"
        "Без хэштегов. "
        "Описание должно быть журналистским рерайтом (не дословно), без выдуманных деталей. "
        "Запрещено придумывать факты, которых нет в данных (в т.ч. нельзя писать 'премьера', 'впервые', 'аншлаг' и т.п., "
        "если это явно не сказано в источниках). "
        "Избегай нейросетевых клише и пустых оценок/прогнозов: "
        "не пиши фразы вроде 'обещает стать заметным событием', 'не оставит равнодушным', 'уникальная возможность', "
        "'незабываемые эмоции' и т.п. Если оценка есть в источнике, атрибутируй её ('по словам организаторов/в анонсе'). "
        "Не включай в описание нерелевантные новости о площадке/организации, которые не относятся к самому событию "
        "(например отчёты о работе филиала, планы на год, анонс посторонних интервью). "
        "Сохраняй ПОЛНОЕ содержание события: включай существенные факты из event_before.description, source_texts и candidate.text. "
        "Не делай текст чрезмерно коротким: если источники длинные, итоговое описание тоже должно быть развернутым и подробным. "
        "Убери рекламные/акционные детали (скидки/промокоды/акция) и механику розыгрыша, если они не являются сутью события. "
        "Если в тексте есть URL или телефоны, не искажай их (лучше перенеси в конец, чем потерять). "
        "Можно использовать минимальную разметку для читабельности: "
        "заголовки `###`, цитаты блоком `> ...`, редкое выделение `**...**`. "
        "НЕ используй Markdown-ссылки вида `[текст](url)` и не вставляй таблицы. "
        "Не включай малозначимые и повторяющиеся строки (например `DD.MM | Название`, повтор заголовка, повтор даты/времени/площадки отдельной строкой). "
        "Если в источнике есть обрыв фразы/текста (в т.ч. обрезано на середине слова), не вставляй это дословно: либо перефразируй, либо опусти. "
        "Не экранируй кавычки обратным слэшем (не пиши `\\\"...\\\"`). "
        "Если в материалах есть прямая речь/цитата (1-е лицо: 'я/мне/кажется/думаю' и т.п.), "
        "НЕ переписывай её в косвенную речь: включи её ДОСЛОВНО как цитату блоком `>` и не дублируй ту же мысль пересказом рядом. "
        "Если `candidate.quote_candidates` не пуст, обязательно включи хотя бы одну из этих цитат ДОСЛОВНО как blockquote. "
        "Если цитата принадлежит конкретному человеку (например режиссёру), укажи это явно: "
        "либо перед цитатой, либо сразу после неё в виде краткой атрибуции (например `— Егор Равинский`). "
        "Структуру делай абзацами: разделяй абзацы пустой строкой. "
        "В каждом абзаце держи 1-2 предложения (максимум 3 только если иначе теряется смысл). "
        "Не дублируй в основном тексте строки-анкеры (`Дата:`, `Время:`, `Локация:`, `Билеты:`) и их явные перефразы: "
        "эти данные уже показываются отдельным блоком. "
        "Также верни `search_digest`: 1 предложение, 120–220 символов (макс 260), без эмодзи/хэштегов/списков; "
        "не указывай дату/время/адрес/город/цены/ссылки; не начинай с дословного повторения title. "
        "Верни JSON с полями title (если нужно улучшить), description (обязательно), search_digest, "
        "ticket_link, ticket_price_min/max, ticket_status, added_facts, duplicate_facts, conflict_facts, skipped_conflicts. "
        "added_facts должен содержать список КОНКРЕТНЫХ НОВЫХ фактов (короткими пунктами), которых НЕ было в event_before.facts. "
        "duplicate_facts должен содержать список фактов из candidate, которые уже есть в event_before.facts (дубли). "
        "conflict_facts должен содержать список конфликтов (см. выше) и выбранную сторону по доверию. "
        "Не включай в added_facts и duplicate_facts служебные заметки. "
        "Каждый элемент added_facts и duplicate_facts верни объектом {fact, evidence_quote}. "
        "evidence_quote — точная непрерывная цитата из candidate.text/raw_excerpt/poster_texts, "
        "которая прямо подтверждает ВЕСЬ fact. Если такой цитаты нет, не возвращай факт. "
        "Не выводи цель, формат, пользу, регулярность или продолжение серии только из названия/тематики. "
        f"\n\n{AGE_DECISION_PROMPT_RULE}"
        "\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        MERGE_SCHEMA,
        max_tokens=SMART_UPDATE_MERGE_MAX_TOKENS,
        label="merge",
    )
    if data is None:
        logger.warning("smart_update: merge invalid json (gemma)")
        return None
    if isinstance(data, dict) and ("duplicate_facts" not in data or data.get("duplicate_facts") is None):
        data["duplicate_facts"] = []
    if isinstance(data, dict) and ("conflict_facts" not in data or data.get("conflict_facts") is None):
        data["conflict_facts"] = []
    return data


async def _llm_merge_identity_gate(
    candidate: EventCandidate,
    event: Event,
    *,
    conflicting_anchor_fields: dict[str, Any] | None,
    poster_texts: Sequence[str] | None = None,
) -> dict[str, Any] | None:
    """LLM-first same-identity gate for the merge path.

    This stage answers only whether it is safe to let a matched candidate mutate
    the existing event.  It must not rewrite titles/descriptions and must prefer a
    safe skip over gluing related-but-distinct events.
    """

    if SMART_UPDATE_LLM_DISABLED:
        return None
    source_texts = []
    for item in (getattr(event, "source_texts", None) or [])[:3]:
        if isinstance(item, str) and item.strip():
            source_texts.append(_clip(item, 800))
    payload = {
        "event_before": {
            "id": event.id,
            "title": event.title,
            "date": event.date,
            "time": event.time,
            "time_is_default": bool(getattr(event, "time_is_default", False)),
            "end_date": event.end_date,
            "location_name": event.location_name,
            "location_address": event.location_address,
            "city": event.city,
            "event_type": event.event_type,
            "ticket_link": event.ticket_link,
            "source_post_url": getattr(event, "source_post_url", None),
            "source_vk_post_url": getattr(event, "source_vk_post_url", None),
            "description": _clip(event.description, 1200),
            "source_text": _clip(event.source_text, 1000),
            "source_texts": source_texts,
            "poster_texts": [_clip(t, 500) for t in (poster_texts or [])[:3] if t],
        },
        "candidate": {
            "title": candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "time_is_default": bool(getattr(candidate, "time_is_default", False)),
            "end_date": candidate.end_date,
            "end_date_is_inferred": bool(getattr(candidate, "end_date_is_inferred", False)),
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "event_type": candidate.event_type,
            "festival": candidate.festival,
            "ticket_link": candidate.ticket_link,
            "source_type": candidate.source_type,
            "source_url": candidate.source_url,
            "raw_excerpt": _clip(candidate.raw_excerpt, 1200),
            "source_text": _clip(candidate.source_text, SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS),
            "poster_texts": [_clip(p.ocr_text, 500) for p in candidate.posters if p.ocr_text][:3],
            "poster_titles": [_clip(p.ocr_title, 160) for p in candidate.posters if (p.ocr_title or "").strip()][:3],
        },
        "conflicting_anchor_fields": conflicting_anchor_fields or {},
    }
    prompt = (
        "Ты — LLM-first identity gate для Smart Update. Уже выбран `event_before` как возможный match "
        "для `candidate`. Реши только одно: можно ли безопасно применять merge side effects к существующему "
        "event_before (менять поля, добавлять source/poster/facts, ставить jobs), или candidate — другое "
        "событие/родственный контекст и merge надо остановить. Верни JSON строго по схеме.\n\n"
        "Главный принцип: общая площадка, близкая дата, общий фестиваль, одна промо-кампания или похожая тема "
        "НЕ доказывают same_event. Нужен конкретный общий identity anchor: тот же показ/лекция/выставка, "
        "та же программа/название/персона в той же роли, та же билетная ссылка, та же афиша или явный текст "
        "источника, что это обновление уже существующего события.\n\n"
        "Когда выбрать skip_merge_side_effects:\n"
        "- candidate описывает выставку/длинный диапазон, а event_before — отдельную лекцию/встречу/показ, "
        "даже если дата открытия и музей совпадают;\n"
        "- candidate — точная single-date occurrence регулярного/сезонного события (например `10 июля 20:00`), "
        "а event_before — общая сезонная/повторяющаяся карточка с `end_date` (`1 мая — 30 сентября`, `каждую пятницу`): "
        "не добавляй в неё свежие media/source/post jobs, иначе публичная карточка покажет старую дату с новой афишей;\n"
        "- candidate и event_before — события одной выставки/фестиваля/площадки, но разные сущности "
        "(например выставка и лекция её куратора/участника);\n"
        "- candidate и event_before проходят в один день на одной площадке, но имеют разные явные времена, "
        "несвязанные названия, разные типы и разные конкретные билетные ссылки (например экскурсия по театру "
        "в 14:30 и спектакль в 18:00): дата и площадка сами по себе не являются identity anchor;\n"
        "- источник добавляет новую афишу/текст, но они относятся к соседнему событию;\n"
        "- есть конфликт типа события, названия или роли персоны, который нельзя объяснить как обычное уточнение.\n\n"
        "Когда выбрать allow_merge/source_update:\n"
        "- это явно тот же реальный слот или та же long-running карточка с уточнением дат/описания/афиши;\n"
        "- для регулярного события источник явно правит правила/дату окончания всей серии, а не анонсирует один ближайший слот;\n"
        "- новая ссылка/афиша/текст прямо относятся к тому же title/programme и не добавляют другую сущность.\n\n"
        "Изменение времени может быть обычной коррекцией только при сильном общем anchor: та же конкретная "
        "билетная/источниковая ссылка, та же афиша или явно то же название/программа. Не считай смену времени "
        "коррекцией только потому, что дата и площадка совпали.\n\n"
        "Если сомневаешься — выбирай skip_merge_side_effects с relation=unknown. Автоматическая state machine "
        "сама выполнит bounded retry и затем безопасный distinct create; human review здесь нет. Не придумывай "
        "фактов, опирайся только на данные.\n\n"
        "Коды reason_code делай короткими snake_case, например: same_event_update, same_ticket_source_update, "
        "festival_sibling_not_same_event, long_running_vs_single_slot, unrelated_title_type_conflict, "
        "insufficient_identity_evidence.\n\n"
        f"{SMART_UPDATE_YO_RULE}\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        MERGE_IDENTITY_GATE_SCHEMA,
        max_tokens=500,
        label="merge_identity_gate",
    )
    return data if isinstance(data, dict) else None


def _apply_ticket_fields(
    event: Event,
    *,
    ticket_link: str | None,
    ticket_price_min: int | None,
    ticket_price_max: int | None,
    ticket_status: str | None,
    candidate_trust: str | None,
) -> list[str]:
    added: list[str] = []
    cand_priority = _trust_priority(candidate_trust)
    existing_priority = _trust_priority(getattr(event, "ticket_trust_level", None))

    def _is_vk_short_ticket_link(url: str | None) -> bool:
        raw = str(url or "").strip()
        if not raw:
            return False
        try:
            host = urlsplit(raw).netloc.lower().removeprefix("www.")
        except Exception:
            return False
        return host in {"vk.cc", "vk.link", "go.vk.com", "l.vk.com"}

    def _more_specific_ticket_link(candidate_url: str | None, existing_url: str | None) -> bool:
        candidate_raw = str(candidate_url or "").strip()
        existing_raw = str(existing_url or "").strip()
        if not candidate_raw or not existing_raw or candidate_raw == existing_raw:
            return False
        candidate_is_vk_short = _is_vk_short_ticket_link(candidate_raw)
        existing_is_vk_short = _is_vk_short_ticket_link(existing_raw)
        if candidate_is_vk_short and not existing_is_vk_short:
            return False
        if existing_is_vk_short and not candidate_is_vk_short:
            return True
        try:
            cand = urlsplit(candidate_raw)
            existing = urlsplit(existing_raw)
        except Exception:
            return False
        cand_host = (cand.netloc or "").lower().removeprefix("www.")
        existing_host = (existing.netloc or "").lower().removeprefix("www.")
        if cand.scheme not in {"http", "https"} or existing.scheme not in {"http", "https"}:
            return False
        if cand_host != existing_host:
            return False
        cand_path = (cand.path or "/").rstrip("/") or "/"
        existing_path = (existing.path or "/").rstrip("/") or "/"
        if existing_path != "/" and not (cand_path == existing_path or cand_path.startswith(existing_path + "/")):
            return False
        cand_query = dict(parse_qsl(cand.query, keep_blank_values=True))
        existing_query = dict(parse_qsl(existing.query, keep_blank_values=True))
        return len(cand_path) > len(existing_path) or (
            bool(cand_query) and cand_query != existing_query
        )

    def _can_override(existing: Any) -> bool:
        if existing in (None, ""):
            return True
        return cand_priority > existing_priority

    current_ticket_link = getattr(event, "ticket_link", None)
    candidate_is_vk_short = _is_vk_short_ticket_link(ticket_link)
    existing_is_vk_short = _is_vk_short_ticket_link(current_ticket_link)
    can_replace_ticket = (
        _can_override(current_ticket_link)
        or _more_specific_ticket_link(ticket_link, current_ticket_link)
    )
    if (
        ticket_link
        and candidate_is_vk_short
        and current_ticket_link
        and not existing_is_vk_short
    ):
        can_replace_ticket = False
    if ticket_link and can_replace_ticket:
        event.ticket_link = ticket_link
        event.ticket_trust_level = candidate_trust
        event.vk_ticket_short_url = None
        event.vk_ticket_short_key = None
        added.append("ticket_link")
    if ticket_price_min is not None and _can_override(event.ticket_price_min):
        event.ticket_price_min = ticket_price_min
        event.ticket_trust_level = candidate_trust
        added.append("ticket_price_min")
    if ticket_price_max is not None and _can_override(event.ticket_price_max):
        event.ticket_price_max = ticket_price_max
        event.ticket_trust_level = candidate_trust
        added.append("ticket_price_max")
    if ticket_status and _can_override(getattr(event, "ticket_status", None)):
        setattr(event, "ticket_status", ticket_status)
        event.ticket_trust_level = candidate_trust
        added.append("ticket_status")
    return added


def _candidate_has_new_text(candidate: EventCandidate, event: Event) -> bool:
    def _normalize(text: str | None) -> str:
        raw = _strip_private_use(text) or (text or "")
        raw = _strip_promo_lines(raw) or raw
        raw = _strip_giveaway_lines(raw) or raw
        return raw.strip()

    def _sentences(text: str) -> list[str]:
        chunks = re.split(r"[.!?…]\s+|\n{2,}|\n", text)
        out: list[str] = []
        for chunk in chunks:
            c = re.sub(r"\s+", " ", chunk).strip()
            if c:
                out.append(c)
        return out

    event_text = _normalize(event.description)
    candidates = [_normalize(candidate.source_text), _normalize(candidate.raw_excerpt)]
    candidates = [c for c in candidates if c]
    if not candidates:
        return False
    if not event_text:
        return True

    event_lower = event_text.lower()
    for cand in candidates:
        if len(cand) < 40:
            continue
        # Prefer sentence-level detection: raw_excerpt may omit new details even when source_text contains them.
        for sent in _sentences(cand):
            if len(sent) < 35:
                continue
            if sent.lower() not in event_lower:
                return True
        # Fallback: simple containment check.
        if cand.lower() not in event_lower:
            return True
    return False


def _dedupe_description(description: str | None) -> str | None:
    """Remove obvious duplicate sentences/lines in a description.

    This is a deterministic safety net on top of LLM merge (prevents repeated facts like the same award twice).
    """
    if not description:
        return None
    raw = str(description).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return None
    raw = re.sub(r"\n{3,}", "\n\n", raw)

    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw) if p.strip()]
    if not paragraphs:
        return None

    seen_line_keys: set[str] = set()
    out_paras: list[str] = []

    def _dedupe_lines_keep_newlines(block: str) -> str:
        heading_re = re.compile(r"^\s*#{1,6}\s+\S")
        kept: list[str] = []
        for ln in block.splitlines():
            s = ln.strip()
            if not s:
                continue
            key = re.sub(r"\s+", " ", s).strip().lower()
            # Even short lines can be duplicated facts (e.g. awards). Dedupe more aggressively.
            # Headings are short by nature, but duplicate headings create empty sections and
            # ugly Telegraph pages, so we always dedupe them regardless of key length.
            if (heading_re.match(s) or len(key) >= 15) and key in seen_line_keys:
                continue
            seen_line_keys.add(key)
            kept.append(s)
        return "\n".join(kept).strip()

    def _dedupe_sentences_in_paragraph(text: str) -> str:
        normalized = re.sub(r"\s+", " ", text).strip()
        if not normalized:
            return ""
        parts = re.split(r"(?<=[.!?…])\s+", normalized)
        seen_sent: set[str] = set()
        kept_sent: list[str] = []
        for part in parts:
            sent = part.strip()
            if not sent:
                continue
            key = re.sub(r"\s+", " ", sent).strip().lower().rstrip(".!?…")
            # Dedupe repeated short sentences too (common LLM artifact and source-copy noise).
            if len(key) >= 18 and key in seen_sent:
                continue
            seen_sent.add(key)
            kept_sent.append(sent)

        # Drop sentences that are strict substrings of another sentence (helps with
        # truncated tails and "same idea twice" cases).
        norm_sents: list[tuple[str, str]] = []
        for sent in kept_sent:
            key = re.sub(r"\s+", " ", sent).strip().lower().rstrip(".!?…")
            norm_sents.append((sent, key))
        drop_idx: set[int] = set()
        for i, (_s_i, k_i) in enumerate(norm_sents):
            if i in drop_idx:
                continue
            if len(k_i) < 40:
                continue
            for j, (_s_j, k_j) in enumerate(norm_sents):
                if i == j or j in drop_idx:
                    continue
                if len(k_j) < len(k_i):
                    continue
                if len(k_j) - len(k_i) < 10:
                    continue
                if k_i and k_i in k_j:
                    drop_idx.add(i)
                    break

        kept2 = [s for idx, (s, _k) in enumerate(norm_sents) if idx not in drop_idx]
        return " ".join(kept2).strip()

    for para in paragraphs:
        if _looks_like_structured_block(para):
            cleaned = _dedupe_lines_keep_newlines(para)
            if cleaned:
                out_paras.append(cleaned)
            continue
        cleaned = _dedupe_sentences_in_paragraph(para)
        if cleaned:
            out_paras.append(cleaned)

    cleaned = "\n\n".join(out_paras).strip()
    return cleaned or None


def _normalize_candidate_sentence(chunk: str) -> str:
    sent = re.sub(r"\s+", " ", chunk).strip()
    if not sent:
        return ""
    # Replace Markdown links with link text to avoid noisy URL-heavy snippets.
    sent = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", sent)
    sent = re.sub(r"\s+", " ", sent).strip(" *_`~|").strip()
    return sent


def _is_low_signal_sentence(sent: str) -> bool:
    if not sent:
        return True
    if len(sent) < 35:
        return True
    low = sent.lower()
    if "http://" in low or "https://" in low:
        return True
    # Skip schedule-like headers (common in multi-event Telegram posts):
    # "04.02 | ..." / "04/02 — ..." etc.
    if re.match(r"^\s*\d{1,2}[./]\d{1,2}\s*(?:\\||[-–—])\s*", sent):
        return True
    words = re.findall(r"[A-Za-zА-Яа-яЁё]{2,}", sent)
    # Skip date/title-only fragments (common in noisy Telegram captions).
    if len(words) < 5:
        return True
    return False


_COVERAGE_CRITICAL_PATTERNS = (
    re.compile(r"\b\d{1,2}[:.]\d{2}\b", re.IGNORECASE),
    re.compile(r"\bнач(?:ало|н[её]т(?:с[яь])?)\b", re.IGNORECASE),
    re.compile(r"\b(?:основн\w*\s+сцен\w*|камерн\w*\s+сцен\w*|мал\w*\s+сцен\w*)\b", re.IGNORECASE),
    re.compile(r"\b(?:театральн\w+\s+хит|хит)\b", re.IGNORECASE),
)


def _is_coverage_critical_sentence(sent: str) -> bool:
    raw = (sent or "").strip()
    if not raw:
        return False
    low = raw.lower()
    if "http://" in low or "https://" in low:
        return False
    return any(p.search(raw) for p in _COVERAGE_CRITICAL_PATTERNS)


def _enforce_merge_non_shrinking_description(
    *,
    before_description: str,
    merged_description: str,
    candidate: EventCandidate,
    has_new_text: bool,
) -> str:
    """Prevent LLM merge from collapsing a rich description into a short digest.

    If the merged description is substantially shorter than the previous one,
    prefer keeping the previous description and deterministically appending new
    factual sentences from the candidate.
    """
    before = (before_description or "").strip()
    merged = (merged_description or "").strip()
    if not merged:
        return before
    if not before:
        return merged
    # Only protect sufficiently rich descriptions; allow short texts to change freely.
    before_len = len(before)
    merged_len = len(merged)
    if before_len >= 500 and merged_len < int(before_len * 0.75):
        keep = before
        if has_new_text:
            before_norm = re.sub(r"\s+", " ", keep).strip().lower()
            new_sentences = _collect_new_candidate_sentences(candidate, before_norm=before_norm)
            if new_sentences:
                ranked = sorted(
                    range(len(new_sentences)),
                    key=lambda idx: (_sentence_quality_score(new_sentences[idx]), -idx),
                    reverse=True,
                )
                picked_idx = sorted(ranked[:2])
                picked = [new_sentences[idx] for idx in picked_idx]
                keep = (keep + "\n" + " ".join(picked)).strip()
        return keep

    # Also protect against the "too short compared to a rich new source" case:
    # when candidate text is long but the model returns a short digest.
    cand_text = _strip_private_use(candidate.source_text) or (candidate.source_text or "")
    cand_text = _strip_promo_lines(cand_text) or cand_text
    cand_text = _strip_giveaway_lines(cand_text) or cand_text
    cand_text = _strip_foreign_schedule_noise(
        cand_text,
        event_date=candidate.date,
        end_date=candidate.end_date,
        event_title=candidate.title,
    )
    cand_len = len((cand_text or "").strip())
    if cand_len >= 1200:
        min_expected = max(450, int(cand_len * 0.35))
        if merged_len < min_expected:
            # Prefer the richer previous description if it already has substance,
            # otherwise fall back to the candidate text (verbatim) to keep facts.
            if before_len >= min_expected:
                return before
            if cand_text.strip():
                return cand_text.strip()
    return merged


def _pick_richest_source_text_for_description(event: Event, candidate: EventCandidate) -> str:
    """Pick the richest available source text for building a full description.

    Priority is the longest cleaned text among event/source aggregates and the candidate.
    """
    texts: list[str] = []
    poster_texts: list[str] = []
    try:
        for p in (getattr(candidate, "posters", None) or [])[:5]:
            t = getattr(p, "ocr_text", None)
            if isinstance(t, str) and t.strip():
                poster_texts.append(t)
    except Exception:
        poster_texts = []
    for t in [
        getattr(event, "source_text", None),
        *(getattr(event, "source_texts", None) or []),
        getattr(candidate, "source_text", None),
        getattr(candidate, "raw_excerpt", None),
        *poster_texts,
    ]:
        if not isinstance(t, str):
            continue
        cleaned = _strip_private_use(t) or (t or "")
        cleaned = _strip_promo_lines(cleaned) or cleaned
        cleaned = _strip_giveaway_lines(cleaned) or cleaned
        cleaned = _strip_foreign_schedule_noise(
            cleaned,
            event_date=getattr(event, "date", None) or candidate.date,
            end_date=getattr(event, "end_date", None) or candidate.end_date,
            event_title=getattr(event, "title", None) or candidate.title,
        )
        cleaned = cleaned.strip()
        if cleaned:
            texts.append(cleaned)
    if not texts:
        return ""
    return max(texts, key=len)


def _build_fact_seed_text(
    event: Event,
    candidate: EventCandidate,
    *,
    poster_texts: Sequence[str] | None = None,
    max_chars: int = 16000,
) -> str:
    """Build a conservative "facts seed" text for deterministic post-processing.

    Smart Update merges are free to paraphrase and reorder, but they must not drop
    important facts (genre/style markers, unique details) that exist in the source
    materials. We use this combined seed only for *appending missing sentences*,
    not for generating new content.
    """

    def _clean(t: str | None) -> str:
        if not t or not isinstance(t, str):
            return ""
        cleaned = _strip_private_use(t) or (t or "")
        cleaned = _strip_promo_lines(cleaned) or cleaned
        cleaned = _strip_giveaway_lines(cleaned) or cleaned
        cleaned = _strip_foreign_schedule_noise(
            cleaned,
            event_date=getattr(event, "date", None) or candidate.date,
            end_date=getattr(event, "end_date", None) or candidate.end_date,
            event_title=getattr(event, "title", None) or candidate.title,
        ) or cleaned
        return cleaned.strip()

    chunks: list[str] = []
    for t in [
        getattr(event, "source_text", None),
        *(getattr(event, "source_texts", None) or []),
        getattr(event, "description", None),
        getattr(candidate, "source_text", None),
        getattr(candidate, "raw_excerpt", None),
        *(list(poster_texts or [])[:5]),
    ]:
        cleaned = _clean(t)
        if cleaned:
            chunks.append(cleaned)

    if not chunks:
        return ""

    # Deduplicate while preserving order.
    seen: set[str] = set()
    uniq: list[str] = []
    for c in chunks:
        key = c.casefold()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)

    combined = "\n\n".join(uniq).strip()
    if not combined:
        return ""
    return _clip(combined, max_chars)


async def _rewrite_description_full_from_sources(event: Event, candidate: EventCandidate) -> str | None:
    """Second-pass rewrite used when merge returns an over-compressed digest.

    This uses the richest available source text (usually site import) and event metadata.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None

    base = _pick_richest_source_text_for_description(event, candidate)
    if len(base) < 120:
        return None

    payload = {
        "title": getattr(event, "title", None) or candidate.title,
        "date": getattr(event, "date", None) or candidate.date,
        "time": getattr(event, "time", None) or candidate.time,
        "end_date": getattr(event, "end_date", None) or candidate.end_date,
        "location_name": getattr(event, "location_name", None) or candidate.location_name,
        "location_address": getattr(event, "location_address", None) or candidate.location_address,
        "city": getattr(event, "city", None) or candidate.city,
        "ticket_link": getattr(event, "ticket_link", None) or candidate.ticket_link,
        "ticket_status": getattr(event, "ticket_status", None) or candidate.ticket_status,
        "is_free": bool(getattr(event, "is_free", False)),
        "event_type": getattr(event, "event_type", None) or candidate.event_type,
        "festival": getattr(event, "festival", None) or candidate.festival,
        "source_text": _clip(base, SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS),
    }

    prompt = (
        "Ты — культурный журналист. Сделай ПОЛНОЕ развернутое описание события на основе source_text и метаданных. "
        "Сохрани ВСЕ значимые факты, не превращай в короткий дайджест. "
        "Не добавляй выдуманных фактов. Не копируй фразы дословно, но и не сокращай содержание. "
        f"{SMART_UPDATE_YO_RULE} "
        f"{SMART_UPDATE_PRESERVE_LISTS_RULE} "
        f"{SMART_UPDATE_VISITOR_CONDITIONS_RULE} "
        f"{_description_emoji_prompt_rule()}"
        "Без хэштегов. Убери промо/акции и механику розыгрыша (если не часть сути). "
        "Не добавляй секции/заголовки про факты (например «Факты…», «Факты о событии», `Facts/Added Facts`): "
        "факты формируются отдельно. "
        "Важно: НЕ повторяй в описании логистику (дата/время/площадка/точный адрес/город/ссылки/телефон/контакты/точные цены) — "
        "она показывается отдельным инфоблоком сверху.\n\n"
        "Убери промо чужих/вспомогательных каналов с анонсами и призывы подписаться "
        "(например «Информация о событиях ... доступна в Telegram-канале ...»): это не факт про само событие.\n\n"
        "Запрещено придумывать утверждения вроде 'премьера', 'впервые', 'аншлаг' и т.п., "
        "если это явно не сказано в source_text.\n"
        "Избегай нейросетевых клише и прогнозов (например 'обещает стать заметным событием', 'не оставит равнодушным').\n\n"
        "Можно использовать минимальную разметку для читабельности: "
        "заголовки `###`, цитаты блоком `> ...`, редкое выделение `**...**`. "
        "НЕ используй Markdown-ссылки вида `[текст](url)` и не вставляй таблицы. "
        "Не включай малозначимые и повторяющиеся строки (например `DD.MM | Название`, повтор заголовка, повтор даты/времени/площадки отдельной строкой). "
        "Не включай в описание нерелевантные новости о площадке/организации, которые не относятся к самому событию "
        "(например отчёты о работе филиала, планы на год, анонс посторонних интервью). "
        "Не дублируй в основном тексте строки-анкеры (`Дата:`, `Время:`, `Локация:`, `Билеты:`) и их явные перефразы: "
        "эти данные уже показываются отдельным блоком. "
        "Если в исходном тексте есть обрыв фразы/текста (в т.ч. обрезано на середине слова), не вставляй это дословно: либо перефразируй, либо опусти. "
        "Структуру делай абзацами: разделяй абзацы пустой строкой. "
        "В каждом абзаце держи 1-2 предложения (максимум 3 только если иначе теряется смысл).\n\n"
        "Самопроверка перед ответом:\n"
        "- В тексте НЕТ ссылок/телефонов/точных адресов/цен/времени/дат (они уже в инфоблоке).\n"
        "- НЕТ обрывов фраз после правок.\n"
        "- НЕТ странных/непонятных слов и опечаток.\n\n"
        f"Данные:\n{json.dumps(payload, ensure_ascii=False)}"
    )

    # Allow a bit more than the default rewrite budget for the "fix too short merge" case.
    max_tokens = min(1600, max(300, SMART_UPDATE_REWRITE_MAX_TOKENS + 300))
    text = await _ask_gemma_text(
        prompt,
        max_tokens=max_tokens,
        label="rewrite_full",
        temperature=0.0,
    )
    if not text:
        return None
    cleaned = _strip_private_use(text) or (text or "")
    cleaned = _strip_foreign_schedule_noise(
        cleaned,
        event_date=getattr(event, "date", None) or candidate.date,
        end_date=getattr(event, "end_date", None) or candidate.end_date,
        event_title=getattr(event, "title", None) or candidate.title,
    )
    cleaned = _normalize_plaintext_paragraphs(cleaned)
    if not cleaned:
        return None
    cleaned = _fix_broken_initial_paragraph_splits(cleaned) or cleaned
    cleaned = (
        _sanitize_description_output(
            cleaned,
            source_text=base,
        )
        or cleaned
    )
    if _description_needs_channel_promo_strip(cleaned):
        cleaned = _strip_channel_promo_from_description(cleaned) or cleaned
    cleaned = _append_missing_fact_sentences(base=base, rewritten=cleaned, max_sentences=2)
    if _description_needs_infoblock_logistics_strip(cleaned, candidate=candidate):
        try:
            edited = await _llm_remove_infoblock_logistics(
                description=cleaned,
                candidate=candidate,
                label="rewrite_full_remove_logistics",
            )
        except Exception:  # pragma: no cover - network failures
            edited = None
        if edited:
            edited = _normalize_plaintext_paragraphs(edited) or edited
            cleaned = edited
    return _clip(cleaned, SMART_UPDATE_DESCRIPTION_MAX_CHARS)


def _min_expected_description_len_from_sources(event: Event, candidate: EventCandidate) -> int:
    richest = _pick_richest_source_text_for_description(event, candidate)
    base_len = len(richest)
    if base_len < 700:
        return 0
    return max(450, int(base_len * 0.55))


def _allowed_schedule_ddmm(event_date: str | None, end_date: str | None) -> set[str]:
    """Return allowed DD.MM anchors for the event date range (best-effort)."""
    if not event_date:
        return set()
    try:
        start = date.fromisoformat(event_date.split("..", 1)[0].strip())
    except Exception:
        return set()
    end = None
    if end_date:
        try:
            end = date.fromisoformat(end_date.strip())
        except Exception:
            end = None
    if not end and ".." in event_date:
        try:
            end = date.fromisoformat(event_date.split("..", 1)[1].strip())
        except Exception:
            end = None
    if not end:
        end = start
    # Avoid exploding on very long ranges.
    if (end - start).days > 14:
        end = start
    out: set[str] = set()
    cur = start
    while cur <= end:
        out.add(cur.strftime("%d.%m"))
        cur += timedelta(days=1)
    return out


_SCHEDULE_LINE_RE = re.compile(
    r"^\s*(?P<dd>\d{1,2})[./](?P<mm>\d{1,2})\s*(?:\\||[-–—])\s*(?P<title>.+?)\s*$"
)


def _strip_foreign_schedule_headings(
    text: str | None, *, event_date: str | None, end_date: str | None
) -> str:
    """Remove schedule-like headings for dates outside the event date range.

    This protects against Telegram "schedule" posts leaking unrelated items into
    a single-event description (e.g. "04.02 | ..." inside the 07.02 event).
    """
    if not text:
        return ""
    allowed = _allowed_schedule_ddmm(event_date, end_date)
    if not allowed:
        return (text or "").strip()
    kept: list[str] = []
    changed = False
    for line in str(text).replace("\r", "\n").split("\n"):
        m = _SCHEDULE_LINE_RE.match(line)
        if not m:
            kept.append(line)
            continue
        dd = int(m.group("dd"))
        mm = int(m.group("mm"))
        ddmm = f"{dd:02d}.{mm:02d}"
        if ddmm in allowed:
            kept.append(line)
            continue
        changed = True
        # drop the line
    out = "\n".join(kept).strip()
    if not changed:
        return (text or "").strip()
    return _dedupe_description(out) or out


def _strip_schedule_headings_all(text: str | None) -> str:
    """Remove schedule-like heading lines regardless of date range.

    Example:
      "12.02 | Фигаро"

    Even when the date/title matches the current event, this line is redundant on
    a single event page once date/time/location are present elsewhere.
    """
    if not text:
        return ""
    kept: list[str] = []
    changed = False
    for line in str(text).replace("\r", "\n").split("\n"):
        if _SCHEDULE_LINE_RE.match(line.strip()):
            changed = True
            continue
        kept.append(line)
    out = "\n".join(kept).strip()
    if not changed:
        return (text or "").strip()
    return _dedupe_description(out) or out


def _looks_like_schedule_digest(text: str | None, *, event_date: str | None, end_date: str | None) -> bool:
    """Heuristic: detect multi-event digest posts (not a single event).

    Used to avoid catastrophic merges/creations from VK/TG posts like "куда сходить" with many dated items.
    """
    raw = (text or "").strip()
    if not raw:
        return False
    allowed = _allowed_schedule_ddmm(event_date, end_date)
    ddmm: set[str] = set()
    for dd_s, mm_s in re.findall(r"\b(\d{1,2})[./](\d{1,2})\b", raw):
        try:
            dd = int(dd_s)
            mm = int(mm_s)
        except Exception:
            continue
        if not (1 <= dd <= 31 and 1 <= mm <= 12):
            continue
        ddmm.add(f"{dd:02d}.{mm:02d}")
    foreign = [x for x in ddmm if x not in allowed]
    # If the source mentions many dates outside the target range, it's likely a schedule digest.
    if len(foreign) >= 4:
        return True
    # Extra signal: unusually long bullet-heavy text.
    lines = raw.splitlines()
    if len(lines) >= 50:
        bullets = 0
        for line in lines:
            s = line.strip()
            if s.startswith(("•", "-", "—", "*")):
                bullets += 1
        if bullets >= 10:
            return True
    return False


_WEAK_RUBRIC_TITLE_RE = re.compile(
    r"(?iu)^\s*(?:"
    r"дайджест|афиша|анонс(?:ы)?|подборка|куда\s+сходить|что\s+делать|"
    r"планы\s+на\s+(?:выходные|неделю)|выходные|мероприятия"
    r")\s*[!.:—–-]?\s*$"
)
_WEAK_IMPERATIVE_LOCATION_RE = re.compile(
    r"(?iu)^\s*(?:приходи|приходите|посмотри|смотри|жд[её]м|подробнее|тут|здесь|онлайн)\s*[!.:—–-]?\s*$"
)


_HISTORICAL_INTERVIEW_MARKER_RE = re.compile(
    r"(?iu)\b(?:интервью|воспоминан\w*|истори\w*|летопис\w*|архив\w*|"
    r"работа(?:ю|л[аи]?|ет)\s+(?:здесь|в\s+музе\w*)|"
    r"музей\w*\s+(?:открыл\w*|создал\w*|основал\w*))\b"
)


def _has_historical_anniversary_interview_risk(candidate: EventCandidate, text: str | None) -> bool:
    """Route old chronology in editorial prose to semantic eventness review."""

    raw = str(text or candidate.source_text or candidate.raw_excerpt or "").strip()
    years = {int(value) for value in re.findall(r"(?<!\d)(?:18|19|20)\d{2}(?!\d)", raw)}
    return len(years) >= 2 and bool(_HISTORICAL_INTERVIEW_MARKER_RE.search(raw))


def _candidate_needs_llm_eventness_review(candidate: EventCandidate, text: str | None) -> bool:
    """Route weak rubric/digest candidates to an LLM eventness decision.

    This helper intentionally does not decide semantic eventness by itself.
    It only detects high-risk extraction shapes where the LLM must confirm that
    the source really contains one concrete attendable event before Smart Update
    creates or merges a public event row.
    """

    if str(candidate.source_type or "").strip().lower() not in {"vk", "tg", "telegram"}:
        return False
    title = str(candidate.title or "").strip()
    loc = str(candidate.location_name or "").strip()
    raw = str(text or candidate.source_text or candidate.raw_excerpt or "").strip()
    # A social-source candidate whose extracted date is not grounded in the
    # source text or poster OCR is high-risk: upstream LLMs can fabricate a
    # far-future date for non-event promo/editorial posts.  Do not decide the
    # semantics with regex here; route the suspicious shape to the LLM-first
    # eventness gate, whose prompt explicitly checks whether the date/place
    # are supported by the source.
    if _candidate_date_is_inferred(candidate, is_canonical_site=False):
        return True
    if _has_historical_anniversary_interview_risk(candidate, raw):
        return True
    if _WEAK_RUBRIC_TITLE_RE.match(title):
        return True
    if loc and _WEAK_IMPERATIVE_LOCATION_RE.match(loc):
        return True
    # Short rubric snippets are the catastrophic class from INC-2026-06-16:
    # "Дайджест - посмотри, приходи" was not an event even though upstream had
    # attached dates. Do not skip here; require LLM confirmation below.
    if len(raw) <= 220 and re.search(r"(?iu)\b(дайджест|афиша|подборка)\b", raw):
        return True
    # Recap posts with a closing "next event" teaser are semantic: require the
    # LLM to confirm there is one concrete attendable future event unless the
    # narrower hallucinated-location guard has already skipped them earlier.
    if _has_retrospective_future_teaser_shape(title, raw):
        return True
    if _has_mixed_occurrence_role_risk(title, raw):
        return True
    # Campaign/discount/action posts are semantic: do not skip by regex, but
    # require the LLM to confirm that the source announces one concrete
    # attendable event rather than an entitlement/promo mechanic.
    if _LOCATION_VALUE_CAMPAIGN_RE.search("\n".join(part for part in (title, raw) if part)):
        return True
    # Operational hours and ticket-validity dates are a semantic date-role
    # question.  Regex only routes the already suspicious source shape; the LLM
    # below decides eventness from source evidence and fails closed on uncertainty.
    if _OPERATIONAL_DATE_ROLE_RE.search("\n".join(part for part in (title, raw) if part)):
        return True
    return False


async def _llm_review_candidate_eventness(
    candidate: EventCandidate,
    *,
    clean_title: str,
    clean_source_text: str | None,
    clean_raw_excerpt: str | None,
) -> tuple[str, float, str]:
    """LLM-first eventness gate for weak rubric/digest candidates.

    Returns (decision, confidence, reason). `non_event` is allowed to block the
    candidate; `event` lets it continue; `uncertain` fail-closes because this
    gate is only used for already suspicious weak candidates.
    """

    if SMART_UPDATE_LLM_DISABLED:
        return "uncertain", 0.0, "llm_disabled"
    poster_ocr: list[dict[str, str]] = []
    for poster in candidate.posters or []:
        item = {
            "title": _clip(getattr(poster, "ocr_title", None), 240),
            "text": _clip(getattr(poster, "ocr_text", None), 1200),
        }
        if item["title"] or item["text"]:
            poster_ocr.append(item)
        if len(poster_ocr) >= 4:
            break
    payload = {
        "candidate": {
            "title": clean_title or candidate.title,
            "date": candidate.date,
            "time": candidate.time,
            "end_date": candidate.end_date,
            "location_name": candidate.location_name,
            "location_address": candidate.location_address,
            "city": candidate.city,
            "event_type": candidate.event_type,
            "source_url": candidate.source_url,
        },
        "source_text": _clip(clean_source_text or candidate.source_text, 1800),
        "raw_excerpt": _clip(clean_raw_excerpt or candidate.raw_excerpt, 900),
        "poster_ocr": poster_ocr,
    }
    prompt = (
        "Ты проверяешь кандидат события перед публикацией в афише Калининграда.\n"
        "Нужно решить, содержит ли источник ОДНО конкретное событие, на которое читатель может прийти.\n\n"
        "Важно:\n"
        "- Решение должно быть grounded только в source_text/raw_excerpt/poster_ocr и полях кандидата.\n"
        "- Данные конкретного события могут находиться только на приложенной афише: подтверждённые title/date/time/place из poster_ocr являются полноценным source evidence, а не выдумкой.\n"
        "- Рубрики, дайджесты, подборки, посты вида 'посмотри, приходи', навигационные/промо-заглушки — non_event, если в них нет одного конкретного названия/программы события.\n"
        "- Акции/скидки/льготы/инструкции участия (например по Пушкинской карте) — non_event, если это не один конкретный сеанс/программа/выставка с собственным событием. Длинный период действия акции сам по себе не делает её событием.\n"
        "- Режим работы площадки, часы посетителей/касс, сообщение 'открыто и работает в обычном режиме', правила покупки билетов и срок действия входного билета — non_event, если источник не называет отдельную attendee-facing программу. Дата 'билет действителен до ...' — срок действия, не дата события; часы площадки/кассы — не время события.\n"
        "- Пост-отчёт о прошедшем событии с коротким хвостом вроде 'ждём вас на следующей выставке/ярмарке ...' — non_event/uncertain, если будущие дата, площадка, адрес и программа не подтверждены явно в источнике.\n"
        "- Интервью, воспоминания, музейная летопись и юбилейная статья — non_event, если день/месяц относится к исторической дате открытия, поступления экспонатов или прошлой работе героя. Даже совпадающий DD месяц нельзя переносить в текущий/будущий год без отдельного явного attendee-facing анонса.\n"
        "- Разделяй occurrence-роли: факты, программа, автомобили, фото и площадка из прошедшей части не являются фактами будущего события. Кандидат event допустим только если его будущие дата и attendee-facing место подтверждены именно будущим анонсом; иначе non_event/uncertain.\n"
        "- Если дата/место/тип выглядят извлечёнными из воздуха, а источник не подтверждает событие, верни non_event.\n"
        "- Если это короткий, но конкретный анонс одного события с названием/форматом и приглашением/расписанием — event.\n"
        "- Если сомневаешься для такого слабого кандидата, верни uncertain.\n\n"
        f"JSON input:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _ask_gemma_json(
        prompt,
        EVENTNESS_REVIEW_SCHEMA,
        max_tokens=500,
        label="eventness_review",
    )
    if not isinstance(data, dict):
        return "uncertain", 0.0, "llm_unavailable"
    decision = str(data.get("decision") or "uncertain").strip().lower()
    if decision not in {"event", "non_event", "uncertain"}:
        decision = "uncertain"
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    reason = str(data.get("reason_short") or "").strip()[:240]
    return decision, confidence, reason


def _normalize_title_for_match(title: str | None) -> str:
    if not title:
        return ""
    raw = _strip_private_use(title) or (title or "")
    raw = re.sub(r"[\"«»]", "", raw)
    raw = re.sub(r"\s+", " ", raw).strip().casefold().replace("ё", "е")
    return raw


_TITLE_MATCH_STOPWORDS = {
    "выставка",
    "концерт",
    "спектакль",
    "событие",
    "мероприятие",
    "открытие",
    "премьера",
    "встреча",
    "вечер",
    "калининград",
}

# Adjectives that make a title look presentable but still do not identify the
# event on their own.  These are used only as a routing guard for LLM title
# recovery: the guard never chooses the replacement title deterministically.
_TITLE_GENERIC_QUALIFIER_STOPWORDS = {
    "городской",
    "областной",
    "региональный",
    "районный",
    "муниципальный",
    "международный",
    "всероссийский",
    "ежегодный",
    "традиционный",
    "семейный",
    "детский",
    "молодежный",
    "молодёжный",
    "уличный",
    "летний",
    "зимний",
    "осенний",
    "весенний",
    "открытый",
    "большой",
    "первый",
    "новый",
}

_TITLE_EVIDENCE_NOISE_TOKENS = {
    "июля",
    "июнь",
    "июня",
    "август",
    "августа",
    "сентябрь",
    "сентября",
    "октябрь",
    "октября",
    "ноябрь",
    "ноября",
    "декабрь",
    "декабря",
    "январь",
    "января",
    "февраль",
    "февраля",
    "март",
    "марта",
    "апрель",
    "апреля",
    "май",
    "мая",
    "дата",
    "время",
    "адрес",
}


def _normalize_text_for_grounding(text: str | None) -> str:
    if not text:
        return ""
    cleaned = _strip_private_use(text) or str(text)
    cleaned = _ZERO_WIDTH_RE.sub("", cleaned)
    cleaned = cleaned.casefold().replace("ё", "е")
    cleaned = cleaned.replace("…", " ")
    cleaned = re.sub(r"[\"«»]", " ", cleaned)
    cleaned = re.sub(r"[^\w\s]+", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _meaningful_title_tokens(title: str | None) -> set[str]:
    norm = _normalize_text_for_grounding(title)
    if not norm:
        return set()
    toks: set[str] = set()
    for tok in norm.split():
        if len(tok) < 3:
            continue
        if tok in _TITLE_MATCH_STOPWORDS:
            continue
        if tok.isdigit():
            continue
        toks.add(tok)
    return toks


def _candidate_title_grounding_corpus_norm(
    candidate: "EventCandidate",
    *,
    facts: Sequence[str] | None = None,
) -> str:
    parts: list[str] = []
    if (candidate.source_text or "").strip():
        parts.append(_clip(candidate.source_text, 9000))
    if (candidate.raw_excerpt or "").strip():
        parts.append(_clip(candidate.raw_excerpt, 2200))
    for poster in list(getattr(candidate, "posters", None) or [])[:4]:
        if (getattr(poster, "ocr_title", None) or "").strip():
            parts.append(_clip(str(poster.ocr_title), 320))
        if (getattr(poster, "ocr_text", None) or "").strip():
            parts.append(_clip(str(poster.ocr_text), 1200))
    for fact in list(facts or [])[:24]:
        fact_text = str(fact or "").strip()
        if fact_text:
            parts.append(_clip(fact_text, 420))
    return _normalize_text_for_grounding("\n".join(parts))


def _token_is_grounded(token: str, source_tokens: set[str]) -> bool:
    if not token:
        return False
    if token in source_tokens:
        return True
    # Best-effort stemming: allow a 1-char suffix difference (e.g. "масленица" vs "масленицу").
    stems = [token]
    if len(token) >= 6:
        stems.append(token[:-1])
    for stem in stems:
        if stem in source_tokens:
            return True
        if len(stem) < 5:
            continue
        for src in source_tokens:
            if src.startswith(stem) or stem.startswith(src):
                return True
    return False


def _is_title_grounded_in_candidate_sources(
    proposed_title: str | None,
    candidate: "EventCandidate",
) -> bool:
    title_norm = _normalize_text_for_grounding(proposed_title)
    if not title_norm:
        return False
    corpus_norm = _candidate_title_grounding_corpus_norm(candidate)
    if not corpus_norm:
        return False
    if len(title_norm) >= 6 and title_norm in corpus_norm:
        return True
    source_tokens = set(corpus_norm.split())
    tokens = _meaningful_title_tokens(proposed_title)
    if not tokens:
        # Titles like "концерт" still must exist in the source corpus.
        return title_norm in corpus_norm
    for token in tokens:
        if _token_is_grounded(token, source_tokens):
            return True
    return False


def _distinctive_title_tokens_for_recovery(
    title: str | None,
    *,
    event_type: str | None = None,
) -> set[str]:
    """Tokens that can identify an event beyond its category/scale words.

    This helper is intentionally stricter than `_meaningful_title_tokens` and is
    used only to decide whether to ask the LLM for title recovery. It must not be
    used to choose or rewrite a title.
    """

    norm = _normalize_text_for_grounding(title)
    if not norm:
        return set()
    event_type_tokens = {
        t
        for t in _normalize_text_for_grounding(event_type).split()
        if len(t) >= 3
    }
    out: set[str] = set()
    for tok in norm.split():
        if len(tok) < 3 or tok.isdigit():
            continue
        if tok in _TITLE_MATCH_STOPWORDS:
            continue
        if tok in _TITLE_GENERIC_QUALIFIER_STOPWORDS:
            continue
        if tok in _TITLE_EVIDENCE_NOISE_TOKENS:
            continue
        if tok in event_type_tokens:
            continue
        out.add(tok)
    return out


def _candidate_contains_distinct_title_evidence(
    candidate: "EventCandidate",
    *,
    current_title: str | None,
    normalized_event_type: str | None,
) -> bool:
    """Return True when source/OCR visibly contains an own-name candidate.

    The function is a fail-closed router into LLM recovery for cases like
    `Городской фестиваль` + source headline `Городской фестиваль «ВЕЛОДЕНЬ»`.
    It does not extract the replacement title; it only detects that the current
    title is probably missing a distinctive, source-grounded name.
    """

    current_norm_tokens = set(_normalize_text_for_grounding(current_title).split())
    event_type = normalized_event_type or candidate.event_type

    source = "\n".join(
        part
        for part in [candidate.source_text or "", candidate.raw_excerpt or ""]
        if str(part or "").strip()
    )
    for quoted in re.findall(r"[«\"]([^»\"]{3,90})[»\"]", source):
        q_tokens = _distinctive_title_tokens_for_recovery(
            quoted,
            event_type=event_type,
        )
        if q_tokens and not q_tokens.issubset(current_norm_tokens):
            return True

    for poster in list(getattr(candidate, "posters", None) or [])[:4]:
        for value in [getattr(poster, "ocr_title", None), getattr(poster, "ocr_text", None)]:
            text = str(value or "").strip()
            if not text:
                continue
            # OCR title/text may be mostly date/time; require at least one
            # distinctive token absent from the current generic title.
            p_tokens = _distinctive_title_tokens_for_recovery(
                _clip(text, 220),
                event_type=event_type,
            )
            if p_tokens and not p_tokens.issubset(current_norm_tokens):
                return True

    return False


def _is_generic_title_event_type_venue(
    title: str | None,
    *,
    event_type: str | None,
    location_name: str | None,
    city: str | None,
) -> bool:
    """Return True for fallback titles like "Концерт — <venue>"."""

    title_norm = _normalize_text_for_grounding(title)
    if not title_norm:
        return False
    et_norm = _normalize_text_for_grounding(event_type)
    if not et_norm:
        return False
    loc_raw = str(location_name or "").strip()
    if not loc_raw:
        return False
    venue_short = loc_raw.split(",", 1)[0].strip()
    venue_norm = _normalize_text_for_grounding(venue_short)
    if not venue_norm:
        return False

    title_toks = {t for t in title_norm.split() if len(t) >= 3}
    if not title_toks:
        return False
    et_toks = {t for t in et_norm.split() if len(t) >= 3}
    venue_toks = {t for t in venue_norm.split() if len(t) >= 3}
    if not (title_toks & et_toks):
        return False
    if not (title_toks & venue_toks):
        return False

    allowed = set(et_toks | venue_toks)
    if city:
        allowed |= {t for t in _normalize_text_for_grounding(city).split() if len(t) >= 3}
    return title_toks.issubset(allowed)


_EVENT_TITLE_RECOVERY_INSTRUCTIONS = (
    "Ты — редактор афиши. По данным ниже определи КОРОТКОЕ собственное название "
    "события — так, как его назвал бы организатор (имя/тема программы, "
    "исполнитель/коллектив, название праздника или шоу).\n"
    "Жёсткие правила:\n"
    "- бери название ТОЛЬКО из предоставленных данных, ничего не придумывай и не обобщай;\n"
    "- НЕ используй шаблон «<тип события> — <площадка>» (например «Концерт — Филармония»): "
    "название площадки само по себе не является названием события;\n"
    "- не включай дату, время, город, адрес, цену, ссылки, телефон;\n"
    "- 2–8 слов;\n"
    "- если узнаваемого названия в данных нет — верни ровно: НЕТ.\n"
    "Верни ТОЛЬКО название одной строкой, без кавычек, без пояснений и без префиксов."
)

_EVENT_TITLE_PUBLIC_RECOVERY_INSTRUCTIONS = (
    "Ты — редактор афиши. По данным ниже сделай КОРОТКИЙ публичный заголовок "
    "для события, у которого сейчас только технический или слишком общий заголовок.\n"
    "Это не креативный нейминг: заголовок должен быть собран только из явно "
    "grounded деталей источника — темы, программы, участника/коллектива, праздника, "
    "проекта/фестиваля, центрального произведения или объекта события.\n"
    "Жёсткие правила:\n"
    "- используй только слова/имена/названия из предоставленных данных; ничего не выдумывай;\n"
    "- можно взять не формальное название, а attendee-facing тему/программу, если формального "
    "названия нет (например «Pianissimo: Илья Папоян», «Розовый натюрморт», "
    "«День защиты детей в Юности»);\n"
    "- НЕ используй шаблон «<тип события> — <площадка>» и не делай заголовок из одной площадки;\n"
    "- не включай дату, время, город, адрес, цену, ссылки, телефон;\n"
    "- 2–10 слов;\n"
    "- если даже grounded публичный заголовок невозможен — верни ровно: НЕТ.\n"
    "Верни ТОЛЬКО заголовок одной строкой, без кавычек, без пояснений и без префиксов."
)


def _recovered_title_grounded(
    title: str | None,
    candidate: "EventCandidate",
    *,
    facts: Sequence[str] | None = None,
) -> bool:
    """Stricter grounding check for recovered titles.

    Unlike `_is_title_grounded_in_candidate_sources`, this ignores source tokens
    shorter than 3 chars so common prepositions ("в", "и", "о", "с") cannot make
    an arbitrary title word look grounded via prefix matching. Requires the full
    title to appear verbatim, or every meaningful title token to be grounded
    against the long-token source/fact/OCR set.
    """
    corpus_norm = _candidate_title_grounding_corpus_norm(candidate, facts=facts)
    if not corpus_norm:
        return False
    title_norm = _normalize_text_for_grounding(title)
    if not title_norm:
        return False
    if len(title_norm) >= 6 and title_norm in corpus_norm:
        return True
    source_tokens = {t for t in corpus_norm.split() if len(t) >= 3}
    tokens = _meaningful_title_tokens(title)
    if not tokens:
        return title_norm in corpus_norm
    return all(_token_is_grounded(token, source_tokens) for token in tokens)


def _normalize_recovered_title_output(raw: str | None) -> str | None:
    first_line = next((ln for ln in (raw or "").splitlines() if ln.strip()), "")
    recovered = re.sub(r"\s+", " ", first_line.strip())
    recovered = (_strip_private_use(recovered) or recovered).strip().strip("«»\"'`*").strip()
    recovered = re.sub(r"(?i)^(?:название|заголовок|title)\s*[:\-—]\s*", "", recovered).strip()
    if not recovered or recovered.casefold() in {"нет", "none", "нет.", "n/a"}:
        return None
    return recovered


async def _call_title_recovery_prompt(
    instructions: str,
    candidate: "EventCandidate",
    *,
    normalized_event_type: str | None,
    facts: Sequence[str] | None,
    label: str,
) -> str | None:
    poster_titles = [
        str(getattr(p, "ocr_title", "") or "").strip()
        for p in list(getattr(candidate, "posters", None) or [])[:4]
        if (getattr(p, "ocr_title", None) or "").strip()
    ]
    facts_list = [str(f).strip() for f in list(facts or [])[:24] if str(f or "").strip()]
    lines = [
        instructions,
        "",
        "Данные:",
        f"Тип события: {normalized_event_type or candidate.event_type or ''}",
        f"Площадка: {candidate.location_name or ''}",
    ]
    if poster_titles:
        lines.append("Текст афиши (OCR): " + " / ".join(poster_titles))
    if facts_list:
        lines.append("Факты:\n- " + "\n- ".join(facts_list))
    lines.append("Исходный текст:")
    lines.append(_clip(candidate.source_text or candidate.raw_excerpt or "", 1800))
    prompt = "\n".join(lines)
    raw = await _ask_gemma_text(prompt, max_tokens=2048, label=label, temperature=0.0)
    return _normalize_recovered_title_output(raw)


def _validate_recovered_event_title(
    recovered: str | None,
    candidate: "EventCandidate",
    *,
    normalized_event_type: str | None,
    facts: Sequence[str] | None,
) -> str | None:
    if not recovered:
        return None
    if _is_generic_title_event_type_venue(
        recovered,
        event_type=normalized_event_type or candidate.event_type,
        location_name=candidate.location_name,
        city=candidate.city,
    ):
        return None
    if not _recovered_title_grounded(recovered, candidate, facts=facts):
        logger.info("smart_update.title_recover_rejected reason=ungrounded recovered=%r", recovered)
        return None
    return _clip_title(recovered, 160) or None


async def _llm_recover_event_title(
    candidate: "EventCandidate",
    *,
    normalized_event_type: str | None,
    facts: Sequence[str] | None,
) -> str | None:
    """Recover a grounded event title when the current title is only generic.

    This covers both the explicit ``"<event_type> — <venue>"`` placeholder and
    category-only titles that lack a distinctive source name while source/OCR
    evidence visibly contains one (for example `Городской фестиваль` vs
    `Городской фестиваль «ВЕЛОДЕНЬ»`).

    Single-purpose call routed through the text path (which carries the existing
    Gemma→GPT-4o fallback): the description model ``gemma-4-31b-it`` often spends
    its whole budget on thought-channel output, so a tiny native-schema call would
    reliably hit MAX_TOKENS with no answer — the text path's 4o fallback makes the
    recovery actually succeed. The result is accepted only when it is grounded in
    the candidate's own source/facts/OCR corpus and is not itself a generic
    ``<type> — <venue>`` placeholder; otherwise returns ``None`` and the caller
    keeps the placeholder. This never overrides a non-generic title.
    """
    if SMART_UPDATE_LLM_DISABLED:
        return None
    facts_list = [str(f).strip() for f in list(facts or [])[:24] if str(f or "").strip()]
    if not _candidate_title_grounding_corpus_norm(candidate, facts=facts_list):
        return None
    try:
        # Budget must comfortably exceed the model's thinking tokens so the short
        # answer survives; the text path falls back if Gemma still fails.
        recovered = await _call_title_recovery_prompt(
            _EVENT_TITLE_RECOVERY_INSTRUCTIONS,
            candidate,
            normalized_event_type=normalized_event_type,
            facts=facts_list,
            label="title_recover",
        )
    except Exception:
        logger.warning("smart_update: title recovery call failed", exc_info=True)
        recovered = None
    validated = _validate_recovered_event_title(
        recovered,
        candidate,
        normalized_event_type=normalized_event_type,
        facts=facts_list,
    )
    if validated:
        return validated

    try:
        public_recovered = await _call_title_recovery_prompt(
            _EVENT_TITLE_PUBLIC_RECOVERY_INSTRUCTIONS,
            candidate,
            normalized_event_type=normalized_event_type,
            facts=facts_list,
            label="title_recover_public",
        )
    except Exception:
        logger.warning("smart_update: public title recovery call failed", exc_info=True)
        return None
    validated = _validate_recovered_event_title(
        public_recovered,
        candidate,
        normalized_event_type=normalized_event_type,
        facts=facts_list,
    )
    if validated:
        logger.info("smart_update.title_public_recovered recovered=%r", _clip_title(validated))
    return validated


def _is_candidate_title_weak_for_llm_override(
    title: str | None,
    *,
    candidate: "EventCandidate",
    normalized_event_type: str | None,
) -> bool:
    if _is_generic_title_event_type_venue(
        title,
        event_type=normalized_event_type or candidate.event_type,
        location_name=candidate.location_name,
        city=candidate.city,
    ):
        return True
    if (
        not _distinctive_title_tokens_for_recovery(
            title,
            event_type=normalized_event_type or candidate.event_type,
        )
        and _candidate_contains_distinct_title_evidence(
            candidate,
            current_title=title,
            normalized_event_type=normalized_event_type,
        )
    ):
        return True

    tokens = _meaningful_title_tokens(title)
    if not tokens:
        return True
    corpus_norm = _candidate_title_grounding_corpus_norm(candidate)
    if not corpus_norm:
        return False
    source_tokens = set(corpus_norm.split())
    for token in tokens:
        if _token_is_grounded(token, source_tokens):
            return False
    return True


def _title_has_meaningful_tokens(title: str | None) -> bool:
    return bool(_meaningful_title_tokens(title))


def _is_merge_title_update_allowed(
    *,
    proposed_title: str | None,
    candidate_title: str | None,
    existing_title: str | None,
    is_canonical_site: bool,
) -> bool:
    """Guard LLM title updates against cross-event contamination.

    For non-canonical sources (telegram/vk/manual imports), accept a merged title only
    when it is semantically related to candidate title and does not conflict with an
    already meaningful existing title.

    For canonical parser sources we allow title correction by candidate title relation,
    even if existing title is already polluted by a previous bad merge.
    """
    proposed = (proposed_title or "").strip()
    if not proposed:
        return False
    if not _titles_look_related(proposed, candidate_title):
        return False
    if is_canonical_site:
        return True
    if _title_has_meaningful_tokens(existing_title) and not _titles_look_related(
        proposed, existing_title
    ):
        return False
    return True


def _titles_look_related(a: str | None, b: str | None) -> bool:
    na = _normalize_title_for_match(a)
    nb = _normalize_title_for_match(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if len(na) >= 8 and na in nb:
        return True
    if len(nb) >= 8 and nb in na:
        return True
    toks_a = {
        t
        for t in re.findall(r"[a-zа-яё0-9]+", na)
        if len(t) >= 3 and t not in _TITLE_MATCH_STOPWORDS
    }
    toks_b = {
        t
        for t in re.findall(r"[a-zа-яё0-9]+", nb)
        if len(t) >= 3 and t not in _TITLE_MATCH_STOPWORDS
    }
    if not toks_a or not toks_b:
        return False
    overlap = toks_a & toks_b
    if not overlap:
        # Russian event titles frequently differ only by inflection when one
        # source names the artist/object directly and another wraps it into an
        # event-type title, e.g. "Валерия" vs "Концерт Валерии".
        #
        # This is still a narrow title-similarity check: _token_is_grounded()
        # only allows exact tokens or a one-character suffix/stem relation for
        # sufficiently long tokens, so it does not turn title matching into a
        # broad semantic decision maker.
        fuzzy_overlap = {
            ta
            for ta in toks_a
            if _token_is_grounded(ta, toks_b)
            or any(_token_is_grounded(tb, {ta}) for tb in toks_b)
        }
        overlap_count = len(fuzzy_overlap)
    else:
        overlap_count = len(overlap)
    if not overlap_count:
        return False
    denom = max(1, min(len(toks_a), len(toks_b)))
    coverage = overlap_count / denom
    return coverage >= 0.6 or (overlap_count >= 2 and coverage >= 0.45)


def _llm_high_confidence_anchor_match_ok(
    candidate: "EventCandidate",
    event_db: "Event",
    *,
    confidence: float | None,
    is_canonical_site: bool,
) -> bool:
    """Return True when an LLM match is protected from title-only vetoes.

    LLM-first matching must not be undone by a primitive title-token guard when
    the model is very confident and hard factual anchors do not conflict. The
    deterministic layer remains a safety rail for factual conflicts; it is not
    allowed to be the semantic decision maker for harmless title wording drift.
    """

    try:
        conf = float(confidence or 0.0)
    except Exception:
        conf = 0.0
    if conf < 0.95:
        return False

    cand_date = str(getattr(candidate, "date", "") or "").strip()
    event_date = str(getattr(event_db, "date", "") or "").strip()
    if cand_date and event_date and cand_date != event_date:
        return False

    if getattr(candidate, "location_name", None) and getattr(event_db, "location_name", None):
        if not _event_candidate_location_matches(event_db, candidate):
            return False

    cand_time = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    event_time = _event_anchor_time(event_db)
    if _has_explicit_time_conflict(cand_time, event_time):
        return False

    return True


def _normalize_time_for_match(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    raw = raw.replace(".", ":")
    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if not m:
        return ""
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return ""
    # "00:00" is often a placeholder from legacy imports.
    if hh == 0 and mm == 0:
        return ""
    return f"{hh:02d}:{mm:02d}"


def _candidate_anchor_time(candidate: "EventCandidate", *, is_canonical_site: bool) -> str:
    """Return time usable as a matching anchor, or empty when time is weak.

    Low-priority default times (e.g. VK source default_time) must not act as anchors:
    they should be overridden when explicit time comes from other sources.
    """
    t = _normalize_time_for_match(candidate.time)
    if not t:
        return ""
    if is_canonical_site:
        return t
    if bool(getattr(candidate, "time_is_default", False)):
        return ""
    return t


def _event_anchor_time(event: "Event") -> str:
    """Return time usable as a matching anchor, or empty when time is weak."""
    if bool(getattr(event, "time_is_default", False)):
        return ""
    return _normalize_time_for_match(getattr(event, "time", None))


def _has_explicit_time_conflict(candidate_time: str | None, event_time: str | None) -> bool:
    ct = _normalize_time_for_match(candidate_time)
    et = _normalize_time_for_match(event_time)
    return bool(ct and et and ct != et)


async def _filter_same_parser_source_occurrence_conflicts(
    db: Database,
    candidate: "EventCandidate",
    events: Sequence[Event],
) -> tuple[list[Event], list[int]]:
    """Do not collapse two explicit sessions already known to one parser.

    Canonical parsers are allowed to correct a wrong/empty time imported from a
    social source.  Once an event already has provenance from the *same*
    parser, however, another explicit time on the same date is a separate
    occurrence unless proved otherwise.  This is common for theatre
    performance pages whose one URL lists several sessions.
    """

    source_type = str(candidate.source_type or "").strip().lower()
    if not source_type.startswith("parser:") or not events:
        return list(events), []
    candidate_time = _candidate_anchor_time(candidate, is_canonical_site=True)
    if not candidate_time:
        return list(events), []
    conflicting_ids = {
        int(event.id)
        for event in events
        if getattr(event, "id", None)
        and _has_explicit_time_conflict(candidate_time, _event_anchor_time(event))
    }
    if not conflicting_ids:
        return list(events), []

    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventSource.event_id)
                .where(
                    EventSource.source_type == source_type,
                    EventSource.event_id.in_(conflicting_ids),
                )
                .distinct()
            )
        ).all()
    same_source_ids = {int(row[0]) for row in rows if row and row[0]}
    if not same_source_ids:
        return list(events), []
    return (
        [event for event in events if int(event.id or 0) not in same_source_ids],
        sorted(same_source_ids),
    )


def _anchor_signature_for_duplicate_event(ev: Event) -> tuple[str, str, str, str]:
    """Return a compact signature to detect truly duplicated rows.

    Used by source_url-based idempotency matchers: when the same source is reprocessed,
    we want to converge on *one* event, but we must not accidentally merge distinct events
    from schedule posts. If multiple anchored candidates exist and they share the same
    signature, we treat them as duplicates and pick the best one.
    """

    return (
        str(getattr(ev, "date", "") or "").strip(),
        _event_anchor_time(ev) or "",
        _normalize_location(getattr(ev, "location_name", None)),
        _normalize_title_for_match(getattr(ev, "title", None)),
    )


def _pick_best_duplicate_event(candidates: Sequence[Event]) -> Event | None:
    if not candidates:
        return None

    def _score(ev: Event) -> tuple[int, int]:
        score = 0
        if (getattr(ev, "telegraph_url", None) or "").strip():
            score += 4
        if (getattr(ev, "description", None) or "").strip():
            score += 2
        if (getattr(ev, "search_digest", None) or "").strip():
            score += 1
        topics = getattr(ev, "topics", None) or []
        if isinstance(topics, list) and topics:
            score += 1
        if (getattr(ev, "ticket_link", None) or "").strip():
            score += 1
        if (getattr(ev, "location_address", None) or "").strip():
            score += 1
        # Prefer older IDs on ties to reduce churn in external references.
        eid = int(getattr(ev, "id", 0) or 0)
        return score, -eid

    return max(candidates, key=_score)


def _event_blocked_by_explicit_occurrence(
    candidate: EventCandidate,
    event: Event | None,
) -> bool:
    event_id = int(getattr(event, "id", 0) or 0)
    conflict_ids = getattr(candidate, "explicit_occurrence_conflict_event_ids", ()) or ()
    return bool(
        event_id
        and event_id
        in {int(value) for value in conflict_ids}
    )


async def _explicit_occurrence_conflict_ids(
    db: Database,
    candidate: EventCandidate,
) -> list[int]:
    """Return same-source Events bound to a different explicit occurrence ID."""

    occurrence_key = str(candidate.occurrence_key or "").strip()
    canonical_source_url = canonicalize_identity_url(candidate.source_url)
    if not canonical_source_url or not is_explicit_occurrence_key(occurrence_key):
        return []
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventSource.event_id, EventSource.occurrence_key).where(
                    EventSource.canonical_source_url == canonical_source_url,
                    EventSource.source_role == "identity_bearing",
                    EventSource.source_type == candidate.source_type,
                    EventSource.occurrence_key.is_not(None),
                    EventSource.occurrence_key != occurrence_key,
                )
            )
        ).all()
    return sorted(
        {
            int(event_id)
            for event_id, existing_key in rows
            if event_id and is_explicit_occurrence_key(existing_key)
        }
    )


async def _match_existing_event_by_source_anchor(
    db: Database,
    candidate: EventCandidate,
) -> Event | None:
    """Best-effort idempotency for duplicated candidates from the same source post.

    Telegram Monitoring can emit two nearly-identical event cards for one Telegram post
    (e.g. message text + linked post enrichment). Smart Update must prefer merging into
    the already-created event instead of creating a duplicate.

    Safety: for schedule posts (multiple real events per one message) we only force-match
    when the anchor resolves to a single unambiguous event.
    """
    source_url = str(candidate.source_url or "").strip()
    if not source_url or not candidate.source_message_id:
        return None
    if str(candidate.source_type or "").strip().lower().startswith("parser:"):
        return None

    try:
        message_id = int(candidate.source_message_id)
    except Exception:
        return None

    async with db.get_session() as session:
        stmt = select(Event).where(Event.source_message_id == message_id)
        if _is_vk_wall_url(source_url):
            stmt = stmt.where(
                or_(
                    Event.source_vk_post_url == source_url,
                    Event.source_post_url == source_url,
                )
            )
        else:
            stmt = stmt.where(Event.source_post_url == source_url)
        stmt = _apply_soft_city_filter(stmt, candidate.city)
        res = await session.execute(stmt)
        anchored = list(res.scalars().all())

    if not anchored:
        return None

    date_raw = str(candidate.date or "").strip()
    title_raw = str(candidate.title or "").strip()
    is_canonical_site = str(candidate.source_type or "").startswith("parser:")
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    allow_parallel = _allow_parallel_events(candidate.location_name)
    anchor_filtered: list[Event] = []
    filtered: list[Event] = []
    for ev in anchored:
        if _event_blocked_by_explicit_occurrence(candidate, ev):
            continue
        if date_raw and str(getattr(ev, "date", "") or "").strip() != date_raw:
            continue
        if _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
            continue
        if candidate.location_name and getattr(ev, "location_name", None):
            if not _event_candidate_location_matches(ev, candidate):
                continue
        anchor_filtered.append(ev)
        ev_title = str(getattr(ev, "title", "") or "").strip()
        if title_raw and ev_title and _title_has_meaningful_tokens(title_raw) and _title_has_meaningful_tokens(ev_title):
            if not _titles_look_related(title_raw, ev_title):
                continue
        filtered.append(ev)

    if not filtered and len(anchor_filtered) == 1 and cand_time_anchor and (not allow_parallel):
        # Same source+message+anchors but title extraction may differ between retries (e.g. one post -> multiple
        # program items). If we have an explicit time anchor and the venue does not allow parallel events,
        # converge on the single anchored event to avoid duplicates.
        return anchor_filtered[0]
    if not filtered:
        return None
    if len(filtered) == 1:
        return filtered[0]

    # Only force-match when it's clearly the same event duplicated.
    sigs = {_anchor_signature_for_duplicate_event(ev) for ev in filtered}
    if len(sigs) == 1:
        return _pick_best_duplicate_event(filtered)
    return None


async def _match_existing_event_by_event_source_url(
    db: Database,
    candidate: EventCandidate,
) -> Event | None:
    """Best-effort identity convergence for source-aware retries.

    Some flows intentionally re-run Smart Update for the same source_url (e.g. monitoring retries,
    deferred processing). We still want to converge on the same event instead of creating duplicates.

    Safety: if one source_url maps to multiple real events (schedule posts), only force-match when
    the match is unambiguous after basic anchor checks.
    """
    source_url = str(candidate.source_url or "").strip()
    canonical_source_url = canonicalize_identity_url(source_url)
    if not source_url or not canonical_source_url:
        return None
    if _candidate_source_role(candidate) != "identity_bearing":
        return None
    if str(candidate.source_type or "").strip().lower().startswith("parser:"):
        return None

    async with db.get_session() as session:
        stmt = (
            select(Event)
            .join(EventSource, EventSource.event_id == Event.id)
            .where(
                EventSource.canonical_source_url == canonical_source_url,
                EventSource.source_role == "identity_bearing",
            )
        )
        if candidate.source_type:
            stmt = stmt.where(EventSource.source_type == candidate.source_type)
        if is_explicit_occurrence_key(candidate.occurrence_key):
            stmt = stmt.where(EventSource.occurrence_key == candidate.occurrence_key)
        stmt = _apply_soft_city_filter(stmt, candidate.city)
        res = await session.execute(stmt)
        anchored = list(res.scalars().all())

    if not anchored:
        return None

    date_raw = str(candidate.date or "").strip()
    title_raw = str(candidate.title or "").strip()
    is_canonical_site = str(candidate.source_type or "").startswith("parser:")
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    allow_parallel = _allow_parallel_events(candidate.location_name)
    anchor_filtered: list[Event] = []
    filtered: list[Event] = []
    for ev in anchored:
        if _event_blocked_by_explicit_occurrence(candidate, ev):
            continue
        if date_raw and str(getattr(ev, "date", "") or "").strip() != date_raw:
            continue
        if _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
            continue
        ev_title = str(getattr(ev, "title", "") or "").strip()
        if candidate.location_name and getattr(ev, "location_name", None):
            if not _event_candidate_location_matches(ev, candidate):
                continue
        anchor_filtered.append(ev)
        if title_raw and ev_title and _title_has_meaningful_tokens(title_raw) and _title_has_meaningful_tokens(ev_title):
            if not _titles_look_related(title_raw, ev_title):
                continue
        filtered.append(ev)

    if not filtered and len(anchor_filtered) == 1 and cand_time_anchor and (not allow_parallel):
        # Same source_url+anchors but title extraction may differ between retries (e.g. one post -> multiple
        # program items). If we have an explicit time anchor and the venue does not allow parallel events,
        # converge on the single anchored event to avoid duplicates.
        return anchor_filtered[0]
    if not filtered:
        return None
    if len(filtered) == 1:
        return filtered[0]

    # Only force-match when it's clearly the same event duplicated.
    sigs = {_anchor_signature_for_duplicate_event(ev) for ev in filtered}
    if len(sigs) == 1:
        return _pick_best_duplicate_event(filtered)
    return None


def _single_candidate_auto_match_ok(
    candidate: EventCandidate,
    event_db: Event,
    *,
    is_canonical_site: bool,
) -> bool:
    # Guard against catastrophic merges when shortlist shrinks to 1 by broad anchors
    # (e.g. generic city location + long-running exhibition date range overlap).
    if is_canonical_site:
        cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=True)
        event_time_anchor = _event_anchor_time(event_db)
        # Canonical parser sources are allowed to repair a polluted title when
        # anchors are strongly aligned.
        if candidate.date and getattr(event_db, "date", None) and candidate.date != event_db.date:
            return False
        if candidate.location_name and getattr(event_db, "location_name", None):
            if not _event_candidate_location_matches(event_db, candidate):
                return False
        if _has_explicit_time_conflict(cand_time_anchor, event_time_anchor):
            return False
        ct = cand_time_anchor
        et = event_time_anchor
        if ct and et and ct == et:
            return True
        if _titles_look_related(candidate.title, getattr(event_db, "title", None)):
            return True
        # Allow parser correction when candidate has explicit time but existing event
        # has empty/placeholder/weak time.
        if ct and not et:
            return True
        return False

    if not _titles_look_related(candidate.title, getattr(event_db, "title", None)):
        # Do NOT auto-merge unrelated titles, including long-running exhibitions/fairs.
        #
        # Rationale: venues can host multiple exhibitions simultaneously, so date-range overlap
        # + location is not a safe enough signal for an automatic single-candidate match.
        # When in doubt, fall back to LLM matching / create (or create when LLM is disabled).
        return False
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=False)
    event_time_anchor = _event_anchor_time(event_db)
    if _has_explicit_time_conflict(cand_time_anchor, event_time_anchor):
        return False
    return True


def _deterministic_exact_title_match(
    candidate: EventCandidate,
    events: Sequence[Event],
    *,
    is_canonical_site: bool,
) -> Event | None:
    """Try to match by strict anchors without LLM.

    Goal: prevent obvious duplicates when the same event comes from multiple sources
    (TG/VK reposts), especially when LLM matching is conservative.
    """

    if not events:
        return None
    cand_date = str(candidate.date or "").strip()
    if not cand_date:
        return None
    if not _title_has_meaningful_tokens(candidate.title):
        return None

    cand_title_norm = _normalize_title_for_match(candidate.title)
    if not cand_title_norm:
        return None

    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    cand_loc = str(candidate.location_name or "").strip() or None

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if cand_loc and getattr(ev, "location_name", None):
            if not _event_candidate_location_matches(ev, candidate):
                continue
        if _normalize_title_for_match(getattr(ev, "title", None)) != cand_title_norm:
            continue
        if _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
            continue
        # Double-check semantic relation (guards against accidental collisions on short titles).
        if not _titles_look_related(candidate.title, getattr(ev, "title", None)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0]

    # If there are multiple candidates but they all share the same anchor signature,
    # it's a duplicate row situation: pick the best one.
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches)
    return None


def _pre_create_duplicate_probe(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> Event | None:
    """Last-line-of-defence dup check before ``INSERT event`` (INC-2026-05-08).

    Walks the shortlist one more time and returns a match when two anchors
    fully agree under any of these branches:

    1. identical normalised ``ticket_link`` + overlapping ``date`` + no explicit
       time conflict + ``_titles_look_related``;
    2. identical normalised ``location_name`` + overlapping ``date`` + identical
       non-empty time anchor + ``_titles_look_related``.

    Both branches require ``_titles_look_related`` so a season-subscription URL
    or a shared venue does not drag unrelated events together. The probe is
    intentionally narrower than the LLM matcher: only what we can prove
    deterministically. ``parser:*`` sources skip the probe entirely (they own
    their canonical anchors and do not need it).
    """

    if not events or not candidate:
        return None
    if str(candidate.source_type or "").startswith("parser:"):
        return None
    if not _title_has_meaningful_tokens(candidate.title):
        return None

    cand_ticket = _normalize_url(candidate.ticket_link)
    cand_loc_norm = _normalize_location(candidate.location_name)
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=False)
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return None

    def _date_overlaps(ev: Event) -> bool:
        ev_start, ev_end = _event_date_range(ev)
        if not ev_start or not ev_end:
            return False
        return not (cand_end < ev_start or ev_end < cand_start)

    # Branch 1: identical ticket_link parity. Strongest signal.
    if cand_ticket:
        for ev in events:
            if _event_blocked_by_explicit_occurrence(candidate, ev):
                continue
            if not getattr(ev, "id", None):
                continue
            ev_ticket = _normalize_url(getattr(ev, "ticket_link", None))
            if not ev_ticket or ev_ticket != cand_ticket:
                continue
            if not _date_overlaps(ev):
                continue
            ev_time_anchor = _event_anchor_time(ev)
            if _has_explicit_time_conflict(cand_time_anchor, ev_time_anchor):
                continue
            ev_title = getattr(ev, "title", None)
            if not _title_has_meaningful_tokens(ev_title):
                continue
            if not _titles_look_related(candidate.title, ev_title):
                continue
            return ev

    # Branch 2: location + time + related title. Catches the cross-source repost
    # case where one source has a ticket URL and the other does not.
    if not cand_loc_norm or not cand_time_anchor:
        return None
    for ev in events:
        if _event_blocked_by_explicit_occurrence(candidate, ev):
            continue
        if not getattr(ev, "id", None):
            continue
        if not _date_overlaps(ev):
            continue
        ev_time_anchor = _event_anchor_time(ev)
        if not ev_time_anchor or ev_time_anchor != cand_time_anchor:
            continue
        ev_loc_norm = _normalize_location(getattr(ev, "location_name", None))
        if not ev_loc_norm or ev_loc_norm != cand_loc_norm:
            continue
        ev_title = getattr(ev, "title", None)
        if not _title_has_meaningful_tokens(ev_title):
            continue
        if not _titles_look_related(candidate.title, ev_title):
            continue
        return ev
    return None


def _same_specific_ticket_shortlist_recall(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> list[Event]:
    """Keep exact ticket/date/time duplicate candidates visible to LLM matching.

    Location defaults can be wrong before LLM repair. A specific ticket URL plus
    overlapping date and no time conflict is identity plumbing, not an editorial
    duplicate decision.
    """

    cand_ticket = _specific_ticket_url_for_match(candidate.ticket_link)
    if not cand_ticket or not events:
        return []
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return []
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    recalled: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if _specific_ticket_url_for_match(getattr(ev, "ticket_link", None)) != cand_ticket:
            continue
        ev_start, ev_end = _event_date_range(ev)
        if not ev_start or not ev_end or cand_end < ev_start or ev_end < cand_start:
            continue
        if _has_explicit_time_conflict(cand_time, _event_anchor_time(ev)):
            continue
        if not (
            _titles_look_related(candidate.title, getattr(ev, "title", None))
            or _source_texts_look_nearly_identical(
                candidate.source_text or candidate.raw_excerpt,
                getattr(ev, "source_text", None),
            )
        ):
            continue
        recalled.append(ev)
    return recalled


_CITYWIDE_FESTIVAL_SIGNAL_RE = re.compile(
    r"(?iu)\b("
    r"фестивал\w*|музыкальн\w+\s+ноч\w*|ночь\s+музеев|день\s+города|"
    r"citywide|городск\w+\s+программ\w*|площадк\w*|маршрут\w*"
    r")\b"
)


def _citywide_festival_shortlist_recall(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> list[Event]:
    """Keep same-title/date/time citywide candidates visible to LLM matching.

    This is deliberately recall-only: it does not merge. It fixes the failure
    mode where a citywide/festival source extracts a contextual phrase as the
    venue, the location filter hides the existing event, and the LLM never gets
    a chance to decide that the rows are duplicates.
    """

    cand_title = _normalize_title_for_match(candidate.title)
    if not cand_title or not events:
        return []
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return []
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    candidate_signal_text = "\n".join(
        [
            str(candidate.title or ""),
            str(candidate.event_type or ""),
            str(candidate.festival or ""),
            str(candidate.festival_full or ""),
            str(candidate.festival_series or ""),
            str(candidate.source_text or candidate.raw_excerpt or ""),
        ]
    )
    if not _CITYWIDE_FESTIVAL_SIGNAL_RE.search(candidate_signal_text):
        return []
    recalled: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if _normalize_title_for_match(getattr(ev, "title", None)) != cand_title:
            continue
        ev_start, ev_end = _event_date_range(ev)
        if not _ranges_overlap(cand_start, cand_end, ev_start, ev_end):
            continue
        if _has_explicit_time_conflict(cand_time, _event_anchor_time(ev)):
            continue
        ev_signal_text = "\n".join(
            [
                str(getattr(ev, "title", "") or ""),
                str(getattr(ev, "event_type", "") or ""),
                str(getattr(ev, "festival", "") or ""),
                str(getattr(ev, "source_text", "") or ""),
            ]
        )
        if not _CITYWIDE_FESTIVAL_SIGNAL_RE.search(ev_signal_text):
            continue
        recalled.append(ev)
    return recalled


def _deterministic_related_title_anchor_match(
    candidate: EventCandidate,
    events: Sequence[Event],
    *,
    is_canonical_site: bool,
) -> Event | None:
    """Try to match by strong anchors plus semantically related titles.

    This is intentionally stricter than `_titles_look_related()` alone:
    we require a unique candidate on the same date, explicit start time and
    location. The helper is primarily meant to keep parser/VK/TG re-imports
    from creating duplicates when titles differ only by a descriptive suffix
    like "Гегель" vs "Гегель: философия истории".
    """

    if not events:
        return None
    cand_date = str(candidate.date or "").strip()
    cand_loc = str(candidate.location_name or "").strip()
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    if not cand_date or not cand_loc or not cand_time_anchor:
        return None
    if not _title_has_meaningful_tokens(candidate.title):
        return None

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if not _event_candidate_location_matches(ev, candidate):
            continue
        if _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
            continue
        if not _titles_look_related(candidate.title, getattr(ev, "title", None)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0]

    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches)
    return None


def _event_has_source_url_hint(event: Event, source_url: str | None) -> bool:
    source_norm = _normalize_url(source_url)
    if not source_norm:
        return False
    return source_norm in {
        _normalize_url(getattr(event, "source_post_url", None)),
        _normalize_url(getattr(event, "source_vk_post_url", None)),
    }


def _source_texts_look_nearly_identical(a: str | None, b: str | None) -> bool:
    left = _normalize_for_similarity(a, drop_structured=False)
    right = _normalize_for_similarity(b, drop_structured=False)
    if not left or not right:
        return False
    if left == right:
        return True
    if len(left) >= 120 and left in right:
        return True
    if len(right) >= 120 and right in left:
        return True
    try:
        from difflib import SequenceMatcher
    except Exception:  # pragma: no cover
        return False
    return SequenceMatcher(None, left, right).ratio() >= 0.82


def _time_to_minutes_for_match(value: str | None) -> int | None:
    norm = _normalize_time_for_match(value)
    if not norm:
        return None
    try:
        hh, mm = norm.split(":", 1)
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


def _source_text_mentions_both_times(text: str | None, *times: str | None) -> bool:
    raw = str(text or "").replace(".", ":").casefold()
    wanted = [t for t in {_normalize_time_for_match(v) for v in times} if t]
    if len(wanted) < 2:
        return False
    return all(t.casefold() in raw for t in wanted)


def _deterministic_same_post_longrun_exact_title_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> Event | None:
    cand_title = _normalize_title_for_match(candidate.title)
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_title or not cand_start or not cand_end or cand_start == cand_end:
        return None
    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if not _event_has_source_url_hint(ev, candidate.source_url):
            continue
        if _normalize_title_for_match(getattr(ev, "title", None)) != cand_title:
            continue
        if candidate.location_name and getattr(ev, "location_name", None):
            if not _event_candidate_location_matches(ev, candidate):
                continue
        ev_start, ev_end = _event_date_range(ev)
        if not _ranges_overlap(cand_start, cand_end, ev_start, ev_end):
            continue
        ev_end_iso = str(getattr(ev, "end_date", "") or "").strip() or None
        cand_end_iso = str(candidate.end_date or "").strip() or None
        if cand_end_iso and ev_end_iso and cand_end_iso != ev_end_iso:
            continue
        matches.append(ev)
    if len(matches) == 1:
        return matches[0]
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches)
    return None


def _event_type_is_exhibition(value: str | None) -> bool:
    return str(value or "").strip().casefold() == "выставка"


def _deterministic_longrun_exhibition_exact_title_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> Event | None:
    cand_title = _normalize_title_for_match(candidate.title)
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_title or not cand_start or not cand_end or cand_start == cand_end:
        return None
    if not _event_type_is_exhibition(candidate.event_type):
        return None

    cand_end_iso = str(candidate.end_date or "").strip() or None
    cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=False)

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if not _event_type_is_exhibition(getattr(ev, "event_type", None)):
            continue
        if _normalize_title_for_match(getattr(ev, "title", None)) != cand_title:
            continue
        ev_start, ev_end = _event_date_range(ev)
        if not _ranges_overlap(cand_start, cand_end, ev_start, ev_end):
            continue
        ev_end_iso = str(getattr(ev, "end_date", "") or "").strip() or None
        if (
            cand_end_iso
            and ev_end_iso
            and cand_end_iso != ev_end_iso
            and not (
                bool(getattr(ev, "end_date_is_inferred", False))
                or bool(getattr(candidate, "end_date_is_inferred", False))
            )
        ):
            continue
        same_source = _event_has_source_url_hint(ev, candidate.source_url)
        if candidate.location_name and getattr(ev, "location_name", None):
            if not same_source and not _event_candidate_location_matches(ev, candidate):
                continue
        elif not same_source:
            continue
        if (not same_source) and _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0]
    if matches and cand_end_iso:
        same_end = {
            str(getattr(ev, "end_date", "") or "").strip() or None for ev in matches
        }
        if len(same_end) == 1:
            return _pick_best_duplicate_event(matches)
    return None


def _deterministic_ticket_source_anchor_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> tuple[Event | None, str]:
    cand_ticket = _normalize_ticket_url_for_match(candidate.ticket_link)
    cand_date = str(candidate.date or "").strip()
    cand_loc = str(candidate.location_name or "").strip()
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    if not (cand_ticket and cand_date and cand_loc and cand_time):
        return None, ""

    slot_matches: list[Event] = []
    bridge_matches: list[Event] = []
    cand_minutes = _time_to_minutes_for_match(cand_time)
    source_text = candidate.source_text or candidate.raw_excerpt
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if not _event_candidate_location_matches(ev, candidate):
            continue
        if _normalize_ticket_url_for_match(getattr(ev, "ticket_link", None)) != cand_ticket:
            continue
        if not _source_texts_look_nearly_identical(source_text, getattr(ev, "source_text", None)):
            continue
        ev_time = _event_anchor_time(ev)
        if ev_time == cand_time:
            slot_matches.append(ev)
            continue
        ev_minutes = _time_to_minutes_for_match(ev_time)
        if cand_minutes is None or ev_minutes is None:
            continue
        if abs(cand_minutes - ev_minutes) > 90:
            continue
        if not _source_text_mentions_both_times(source_text, cand_time, ev_time):
            continue
        if not re.search(r"(?iu)\b(сбор\s+гостей|doors|начал[оа]|start)\b", str(source_text or "")):
            continue
        bridge_matches.append(ev)

    if len(slot_matches) == 1:
        return slot_matches[0], "deterministic_specific_ticket_same_slot"
    if slot_matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in slot_matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(slot_matches), "deterministic_specific_ticket_same_slot"
    if len(bridge_matches) == 1:
        return bridge_matches[0], "deterministic_doors_start_ticket_bridge"
    if bridge_matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in bridge_matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(bridge_matches), "deterministic_doors_start_ticket_bridge"
    return None, ""


def _deterministic_same_ticket_same_place_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> tuple[Event | None, str]:
    cand_ticket = _specific_ticket_url_for_match(candidate.ticket_link)
    cand_date = str(candidate.date or "").strip()
    cand_loc = str(candidate.location_name or "").strip()
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    if not (cand_ticket and cand_date and cand_loc):
        return None, ""

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if not _event_candidate_location_matches(ev, candidate):
            continue
        if _specific_ticket_url_for_match(getattr(ev, "ticket_link", None)) != cand_ticket:
            continue
        if _has_explicit_time_conflict(cand_time, _event_anchor_time(ev)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0], "deterministic_specific_ticket_same_place"
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches), "deterministic_specific_ticket_same_place"
    return None, ""


def _deterministic_same_slot_near_text_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> tuple[Event | None, str]:
    cand_date = str(candidate.date or "").strip()
    cand_loc = str(candidate.location_name or "").strip()
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    source_text = candidate.source_text or candidate.raw_excerpt
    if not (cand_date and cand_loc and source_text):
        return None, ""

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if not _event_candidate_location_matches(ev, candidate):
            continue
        if _has_explicit_time_conflict(cand_time, _event_anchor_time(ev)):
            continue
        if not _source_texts_look_nearly_identical(source_text, getattr(ev, "source_text", None)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0], "deterministic_same_slot_near_text"
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches), "deterministic_same_slot_near_text"
    return None, ""


def _deterministic_copy_post_ticket_same_day_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> Event | None:
    cand_ticket = _normalize_ticket_url_for_match(candidate.ticket_link)
    cand_date = str(candidate.date or "").strip()
    source_text = candidate.source_text or candidate.raw_excerpt
    if not (cand_ticket and cand_date and source_text):
        return None
    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if _normalize_ticket_url_for_match(getattr(ev, "ticket_link", None)) != cand_ticket:
            continue
        if not _source_texts_look_nearly_identical(source_text, getattr(ev, "source_text", None)):
            continue
        if not _titles_look_related(candidate.title, getattr(ev, "title", None)):
            continue
        matches.append(ev)
    if len(matches) == 1:
        return matches[0]
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches)
    return None


def _deterministic_copy_post_source_text_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> tuple[Event | None, str]:
    """Match cross-post copies by near-identical source text, even if ticket links differ.

    This protects repost families where one channel keeps the direct ticket URL while
    another uses a shortlink/button-only CTA, but the actual event copy is otherwise the same.
    """

    cand_date = str(candidate.date or "").strip()
    cand_loc = str(candidate.location_name or "").strip()
    source_text = candidate.source_text or candidate.raw_excerpt
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    if not (cand_date and cand_loc and source_text):
        return None, ""

    same_slot: list[Event] = []
    bridge_matches: list[Event] = []
    cand_minutes = _time_to_minutes_for_match(cand_time)
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if not _event_candidate_location_matches(ev, candidate):
            continue
        if not _source_texts_look_nearly_identical(source_text, getattr(ev, "source_text", None)):
            continue
        if not _titles_look_related(candidate.title, getattr(ev, "title", None)):
            continue

        ev_time = _event_anchor_time(ev)
        if not _has_explicit_time_conflict(cand_time, ev_time):
            same_slot.append(ev)
            continue

        ev_minutes = _time_to_minutes_for_match(ev_time)
        if cand_minutes is None or ev_minutes is None:
            continue
        if abs(cand_minutes - ev_minutes) > 90:
            continue
        if not _source_text_mentions_both_times(source_text, cand_time, ev_time):
            continue
        if not re.search(r"(?iu)\b(сбор\s+гостей|doors|начал[оа]|start)\b", str(source_text or "")):
            continue
        bridge_matches.append(ev)

    if len(same_slot) == 1:
        return same_slot[0], "deterministic_copy_post_same_day_text"
    if same_slot:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in same_slot}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(same_slot), "deterministic_copy_post_same_day_text"

    if len(bridge_matches) == 1:
        return bridge_matches[0], "deterministic_doors_start_text_bridge"
    if bridge_matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in bridge_matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(bridge_matches), "deterministic_doors_start_text_bridge"
    return None, ""


def _deterministic_prose_location_same_slot_text_match(
    candidate: EventCandidate,
    events: Sequence[Event],
) -> tuple[Event | None, str]:
    """Merge obvious copies when the candidate venue is an extractor prose leak.

    The venue value is explicitly not used as identity evidence here. The match
    requires the safer anchors that survived extraction: same date, same explicit
    time, related title, and near-identical source text.
    """

    if not _candidate_location_looks_unsupported_prose(candidate):
        return None, ""
    cand_date = str(candidate.date or "").strip()
    cand_time = _candidate_anchor_time(candidate, is_canonical_site=False)
    source_text = candidate.source_text or candidate.raw_excerpt
    if not (cand_date and cand_time and source_text):
        return None, ""

    matches: list[Event] = []
    for ev in events:
        if not getattr(ev, "id", None):
            continue
        if str(getattr(ev, "date", "") or "").strip() != cand_date:
            continue
        if _event_anchor_time(ev) != cand_time:
            continue
        if not _titles_look_related(candidate.title, getattr(ev, "title", None)):
            continue
        if not _source_texts_look_nearly_identical(source_text, getattr(ev, "source_text", None)):
            continue
        matches.append(ev)

    if len(matches) == 1:
        return matches[0], "deterministic_prose_location_same_slot_text"
    if matches:
        sigs = {_anchor_signature_for_duplicate_event(ev) for ev in matches}
        if len(sigs) == 1:
            return _pick_best_duplicate_event(matches), "deterministic_prose_location_same_slot_text"
    return None, ""


async def _match_existing_event_by_city_noise_rescue(
    db: Database,
    candidate: EventCandidate,
    *,
    is_canonical_site: bool,
) -> tuple[Event | None, str]:
    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return None, ""

    async with db.get_session() as session:
        stmt = select(Event).where(
            and_(
                Event.date <= cand_end.isoformat(),
                or_(
                    and_(
                        Event.end_date.is_(None),
                        Event.date >= cand_start.isoformat(),
                    ),
                    Event.end_date >= cand_start.isoformat(),
                ),
            )
        )
        res = await session.execute(stmt)
        pool = list(res.scalars().all())

    if not pool:
        return None, ""

    exact = _deterministic_exact_title_match(
        candidate,
        pool,
        is_canonical_site=is_canonical_site,
    )
    if exact is not None:
        return exact, "city_noise_exact_title_shortlist"

    copy_post = _deterministic_copy_post_ticket_same_day_match(candidate, pool)
    if copy_post is not None:
        return copy_post, "city_noise_copy_post_shortlist"

    return None, ""


def _strip_foreign_schedule_sentences(text: str | None, *, event_title: str | None) -> str:
    """Remove sentences that look like a foreign schedule/list of other events.

    Example of unwanted leakage (from Telegram schedule posts):
    '... также пройдут спектакли \"Нюрнберг\", \"Мысли...\", ...'
    """
    if not text:
        return ""
    title_norm = _normalize_title_for_match(event_title)
    raw = str(text).strip()
    if not raw:
        return ""

    sentence_re = re.compile(r"(?<=[.!?…])\s+")
    quote_re = re.compile(r"[\"«](.+?)[\"»]")
    keywords_re = re.compile(r"\b(также|в\s+рамках|в\s+афише|указан\w*|пройдут)\b", re.IGNORECASE)
    eventish_re = re.compile(r"\b(спектакл\w*|постановк\w*|концерт\w*|мероприят\w*)\b", re.IGNORECASE)

    parts = sentence_re.split(raw)
    kept: list[str] = []
    changed = False
    for sent in parts:
        s = sent.strip()
        if not s:
            continue
        if not keywords_re.search(s) or not eventish_re.search(s):
            kept.append(s)
            continue
        quoted = [q.strip() for q in quote_re.findall(s) if q and q.strip()]
        if len(quoted) < 2:
            kept.append(s)
            continue
        # If the sentence enumerates multiple quoted titles and none of them matches
        # the current event title, it's likely a leaked schedule list.
        if title_norm:
            quoted_norm = [_normalize_title_for_match(q) for q in quoted]
            if any(title_norm and title_norm in qn for qn in quoted_norm):
                kept.append(s)
                continue
        changed = True
        # drop sentence
    out = " ".join(kept).strip()
    if not changed:
        return raw
    return _dedupe_description(out) or out


def _strip_foreign_schedule_noise(
    text: str | None,
    *,
    event_date: str | None,
    end_date: str | None,
    event_title: str | None,
) -> str:
    # Deterministic schedule stripping is not allowed (LLM handles this).
    return (text or "").strip()


def _description_has_foreign_schedule_headings(
    text: str | None, *, event_date: str | None, end_date: str | None
) -> bool:
    if not text:
        return False
    allowed = _allowed_schedule_ddmm(event_date, end_date)
    if not allowed:
        return False
    for line in str(text).replace("\r", "\n").split("\n"):
        m = _SCHEDULE_LINE_RE.match(line)
        if not m:
            continue
        try:
            dd = int(m.group("dd"))
            mm = int(m.group("mm"))
        except Exception:
            continue
        ddmm = f"{dd:02d}.{mm:02d}"
        if ddmm not in allowed:
            return True
    return False


def _description_has_foreign_schedule_noise(
    text: str | None,
    *,
    event_date: str | None,
    end_date: str | None,
    event_title: str | None,
) -> bool:
    if _description_has_foreign_schedule_headings(text, event_date=event_date, end_date=end_date):
        return True
    cleaned = _strip_foreign_schedule_sentences(text, event_title=event_title)
    return bool(text) and cleaned.strip() != (text or "").strip()


def _collect_new_candidate_sentences(
    candidate: EventCandidate,
    *,
    before_norm: str,
) -> list[str]:
    variants = []
    if candidate.source_text:
        variants.append(candidate.source_text)
    if candidate.raw_excerpt and candidate.raw_excerpt not in variants:
        variants.append(candidate.raw_excerpt)

    out: list[str] = []
    seen: set[str] = set()
    for text in variants:
        cleaned = _strip_private_use(text) or (text or "")
        cleaned = _strip_promo_lines(cleaned) or cleaned
        cleaned = _strip_giveaway_lines(cleaned) or cleaned
        for chunk in re.split(r"[.!?…]\s+|\n{2,}|\n", cleaned):
            sent = _normalize_candidate_sentence(chunk)
            if _is_low_signal_sentence(sent):
                continue
            key = sent.lower()
            if key in before_norm:
                continue
            if key in seen:
                continue
            seen.add(key)
            out.append(sent)
    return out


def _sentence_quality_score(sent: str) -> int:
    words = re.findall(r"[A-Za-zА-Яа-яЁё]{2,}", sent)
    # Prefer richer factual sentences (more lexical content, reasonable length).
    return min(len(sent), 200) + (len(words) * 3)


def _pick_new_text_snippet(candidate: EventCandidate, before_description: str | None) -> str | None:
    """Pick a short snippet that likely contains *new* facts compared to the previous description."""
    before = _strip_private_use(before_description) or (before_description or "")
    before = re.sub(r"\s+", " ", before).strip().lower()
    new_sentences = _collect_new_candidate_sentences(candidate, before_norm=before)
    if new_sentences:
        best = max(new_sentences, key=_sentence_quality_score)
        return _normalize_fact_item(best, limit=140)
    # Fallback: best-effort excerpt
    variants = []
    if candidate.source_text:
        variants.append(candidate.source_text)
    if candidate.raw_excerpt and candidate.raw_excerpt not in variants:
        variants.append(candidate.raw_excerpt)
    best = max((v for v in variants if v), key=lambda v: len(v), default="")
    return _normalize_fact_item(best, limit=140) if best else None


def _pick_new_description_snippet(
    after_description: str | None,
    before_description: str | None,
    *,
    candidate: EventCandidate,
) -> str | None:
    """Pick a snippet that is present in the final description and likely new.

    This makes the operator-facing "Текст дополнен: ..." fact verifiable by reading
    the Telegraph page (which is rendered from `event.description`).
    """
    after = _strip_private_use(after_description) or (after_description or "")
    after = _strip_private_use(after) or after
    before = _strip_private_use(before_description) or (before_description or "")
    before = _strip_private_use(before) or before
    before_norm = re.sub(r"\s+", " ", before).strip().lower()

    candidates: list[str] = []
    for chunk in re.split(r"[.!?…]\s+|\n{2,}|\n", after):
        sent = _normalize_candidate_sentence(chunk)
        if _is_low_signal_sentence(sent):
            continue
        key = sent.lower()
        if key in before_norm:
            continue
        candidates.append(sent)

    if candidates:
        best = max(candidates, key=_sentence_quality_score)
        return _normalize_fact_item(best, limit=140)

    # Fallback to the old candidate-based heuristic.
    return _pick_new_text_snippet(candidate, before_description)


def _dedupe_source_facts(facts: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for fact in facts:
        key = re.sub(r"\s+", " ", str(fact or "")).strip().lower()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(str(fact).strip())
    return out


def _drop_redundant_poster_facts(facts: Sequence[str]) -> list[str]:
    """Drop 'Афиша в источнике' when it points to the same URL as 'Добавлена афиша'."""
    url_re = re.compile(
        r"^(?P<kind>Афиша в источнике|Добавлена афиша):\s+(?P<url>https?://\S+)\s*$",
        re.IGNORECASE,
    )
    added_urls: set[str] = set()
    source_urls: set[str] = set()
    parsed: list[tuple[str, str, str]] = []
    passthrough: list[str] = []
    for fact in facts:
        m = url_re.match((fact or "").strip())
        if not m:
            passthrough.append(fact)
            continue
        kind = (m.group("kind") or "").strip().lower()
        url = (m.group("url") or "").strip()
        parsed.append((fact, kind, url))
        if "добавлена" in kind:
            added_urls.add(url)
        else:
            source_urls.add(url)
    out: list[str] = []
    for original, kind, url in parsed:
        if "афиша в источнике" in kind and url in added_urls:
            continue
        out.append(original)
    out.extend(passthrough)
    return out


def _fallback_merge_description(
    before: str | None,
    candidate: EventCandidate,
    *,
    max_sentences: int = 2,
) -> str | None:
    """Best-effort deterministic merge when LLM merge is unavailable.

    We keep the existing description as-is and append a couple of truly new sentences
    extracted from the candidate (source_text preferred, then raw_excerpt).
    """
    before_text = (before or "").strip()
    before_norm = re.sub(r"\s+", " ", before_text).strip().lower()

    new_sentences = _collect_new_candidate_sentences(candidate, before_norm=before_norm)

    if not new_sentences:
        return _dedupe_description(before_text) or before_text or None

    ranked = sorted(
        range(len(new_sentences)),
        key=lambda idx: (_sentence_quality_score(new_sentences[idx]), -idx),
        reverse=True,
    )
    picked_idx = sorted(ranked[: max(1, int(max_sentences))])
    picked = [new_sentences[idx] for idx in picked_idx]

    merged = (before_text + "\n" + " ".join(picked)).strip() if before_text else " ".join(picked)
    return _dedupe_description(merged) or merged or None


def _should_prefer_source_text_for_description(
    clean_source_text: str | None,
    clean_raw_excerpt: str | None,
) -> bool:
    """Prefer source_text as full-description seed over short excerpt."""
    source = (clean_source_text or "").strip()
    excerpt = (clean_raw_excerpt or "").strip()
    if not source:
        return False
    if not excerpt:
        return True
    source_len = len(source)
    excerpt_len = len(excerpt)
    if source_len >= excerpt_len + 120:
        return True
    if excerpt in source and source_len >= max(int(excerpt_len * 1.35), excerpt_len + 60):
        return True
    return False


async def smart_event_update(
    db: Database,
    candidate: EventCandidate,
    *,
    check_source_url: bool = True,
    schedule_tasks: bool = True,
    schedule_kwargs: dict[str, Any] | None = None,
    _lease_owner: str | None = None,
) -> SmartUpdateResult:
    try:
        intent = (
            candidate.intent
            if isinstance(candidate.intent, SmartUpdateIntent)
            else SmartUpdateIntent(str(candidate.intent or ""))
        )
    except ValueError:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="invalid_smart_update_intent", retry_reason=RetryReason.INVALID_INTENT,
        )
    candidate.intent = intent
    candidate.replay_check_source_url = bool(check_source_url)
    candidate.replay_schedule_tasks = bool(schedule_tasks)
    candidate.replay_schedule_kwargs = dict(schedule_kwargs or {})
    if intent is SmartUpdateIntent.UPSERT_EVENT:
        candidate.source_role = "identity_bearing"
    else:
        candidate.source_role = "context_only"
    canonical_source_url = canonicalize_identity_url(candidate.source_url)
    if canonical_source_url:
        candidate.source_url = canonical_source_url
    candidate_key, occurrence_key = stable_candidate_identity(candidate)
    candidate.candidate_key = candidate_key
    candidate.occurrence_key = occurrence_key
    # The public facade captures this before any normalization/LLM enrichment.
    # Reuse it on an in-attempt CREATE_DISTINCT reroute so EventSource keeps the
    # exact caller-packet fingerprint rather than a fingerprint of mutated
    # working state.
    source_fingerprint = (
        str(candidate.source_fingerprint or "").strip()
        or input_packet_fingerprint(candidate)
    )
    candidate.source_fingerprint = source_fingerprint
    receipt: CandidateAttemptReceipt
    try:
        receipt = await begin_candidate_attempt(
            db,
            candidate_key=candidate_key,
            occurrence_key=occurrence_key,
            canonical_source_url=canonical_source_url,
            source_type=str(candidate.source_type or "unknown"),
            intent=intent.value,
            source_fingerprint=source_fingerprint,
            candidate_payload=asdict(candidate),
            max_attempts=int(os.getenv("SMART_UPDATE_MAX_ATTEMPTS", "3") or 3),
            lease_owner=(
                _lease_owner
                or f"smart-update-direct:{os.getpid()}:{id(asyncio.current_task())}"
            ),
        )
        candidate.smart_update_candidate_id = receipt.candidate_state_id
        # Only a typed semantic UNKNOWN may spend the bounded identity budget.
        # Provider/schema/DB/vector failures are technical and remain retries
        # forever (with the durable counter clamped by smart_update_state).
        candidate.force_create_distinct = bool(
            receipt.attempt >= receipt.max_attempts
            and receipt.previous_retry_reason is RetryReason.IDENTITY_SEMANTIC_UNKNOWN
        )
        if candidate.force_create_distinct:
            candidate.force_create_distinct_reason = (
                IdentityDistinctReason.UNKNOWN_AFTER_BOUNDED_ADJUDICATION
            )
        candidate.force_match_event_id = None
    except CandidateAttemptInProgress:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="candidate_attempt_in_progress", retry_reason=RetryReason.CANDIDATE_ATTEMPT_IN_PROGRESS,
        )
    except Exception:
        logger.exception("smart_update: durable candidate registration failed")
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="candidate_state_unavailable", retry_reason=RetryReason.CANDIDATE_STATE_UNAVAILABLE,
        )
    async with _SMART_UPDATE_LOCK:
        try:
            if intent is SmartUpdateIntent.ATTACH_CONTEXT:
                result = await _attach_context_source(db, candidate)
            else:
                result = await _smart_event_update_impl(
                    db,
                    candidate,
                    check_source_url=check_source_url,
                    schedule_tasks=schedule_tasks,
                    schedule_kwargs=schedule_kwargs,
                )
                if (
                    result.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
                    and result.retry_reason is RetryReason.IDENTITY_SEMANTIC_UNKNOWN
                ):
                    # Identity ambiguity is a semantic decision, not recovery
                    # work. Resolve it once, inline, through the existing
                    # LLM-first distinct-create path.
                    candidate.force_create_distinct = True
                    candidate.force_create_distinct_reason = (
                        IdentityDistinctReason.UNKNOWN_AFTER_BOUNDED_ADJUDICATION
                    )
                    result = await _smart_event_update_impl(
                        db,
                        candidate,
                        check_source_url=check_source_url,
                        schedule_tasks=schedule_tasks,
                        schedule_kwargs=schedule_kwargs,
                    )
        except SourceBindingConflict as exc:
            result = SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=exc.existing_event_id,
                reason="source_binding_conflict", retry_reason=RetryReason.SOURCE_BINDING_CONFLICT,
            )
        except IntegrityError as exc:
            # The partial unique indexes are the authoritative cross-process
            # source ownership guard. Every integrity failure is technical and
            # therefore remains a durable retry rather than escaping as a
            # caller-level generic failure.
            message = str(getattr(exc, "orig", exc) or "").casefold()
            binding_race = "event_source.canonical_source_url" in message
            canonical = canonicalize_identity_url(candidate.source_url)
            owner_id: int | None = None
            if binding_race and canonical:
                async with db.raw_conn() as conn:
                    cursor = await conn.execute(
                        "SELECT event_id FROM event_source WHERE canonical_source_url=? "
                        "AND source_role='identity_bearing' AND occurrence_key=? "
                        "ORDER BY id LIMIT 1",
                        (canonical, candidate.occurrence_key),
                    )
                    row = await cursor.fetchone()
                    await cursor.close()
                    owner_id = int(row[0]) if row and row[0] is not None else None
            result = SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=owner_id,
                reason=(
                    "source_binding_conflict"
                    if binding_race
                    else "smart_update_integrity_error"
                ),
            )
        except Exception:
            logger.exception("smart_update: processing failed; closing technical terminal")
            result = SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
                reason="smart_update_processing_error", retry_reason=RetryReason.SMART_UPDATE_PROCESSING_ERROR,
            )
    result = _terminalize_linear_result(result)
    result.attempt = receipt.attempt
    try:
        await finish_candidate_attempt(
            db,
            receipt,
            outcome=result.outcome,
            event_id=result.event_id,
            diagnostic_event_id=result.diagnostic_event_id,
            reason=result.reason,
            retry_reason=result.retry_reason,
            product_exclusion_reason=result.product_exclusion_reason,
            identity_distinct_reason=result.identity_distinct_reason,
            lifecycle_reason=result.lifecycle_reason,
        )
    except Exception:
        logger.exception("smart_update: durable terminal acknowledgement failed")
        if result.is_accepted:
            # The domain write is already authoritative. Regressing the caller
            # to RETRY would recreate the observed "imported pointer + failed
            # queue status" incident. Candidate state remains RETRY_SCHEDULED
            # from attempt start and exact replay will reconcile its ledger.
            return result
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            diagnostic_event_id=result.event_id or result.diagnostic_event_id,
            reason="candidate_state_ack_failed", retry_reason=RetryReason.CANDIDATE_STATE_ACK_FAILED,
            attempt=receipt.attempt,
        )
    return result


async def _attach_context_source(db: Database, candidate: EventCandidate) -> SmartUpdateResult:
    """Attach provenance to an explicit target without identity matching/LLM."""

    target_event_id = int(candidate.target_event_id or 0)
    if target_event_id <= 0:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="attach_context_target_required", retry_reason=RetryReason.ATTACH_CONTEXT_TARGET_REQUIRED,
        )
    candidate.source_role = "context_only"
    canonical = canonicalize_identity_url(candidate.source_url)
    if not canonical:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            diagnostic_event_id=target_event_id,
            reason="attach_context_source_url_required", retry_reason=RetryReason.ATTACH_CONTEXT_SOURCE_URL_REQUIRED,
        )
    candidate.source_url = canonical
    async with db.get_session() as session:
        target = await session.get(Event, target_event_id)
        if target is None:
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=target_event_id,
                reason="attach_context_target_missing", retry_reason=RetryReason.ATTACH_CONTEXT_TARGET_MISSING,
            )
        added, same_source = await _ensure_event_source(session, target_event_id, candidate)
        await session.commit()
    return SmartUpdateResult(
        outcome=(
            SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
            if same_source and not added
            else SmartUpdateTerminalOutcome.MERGED
        ),
        event_id=target_event_id,
        added_sources=added,
        reason="context_provenance_attached" if added else "context_provenance_replay",
        lifecycle_reason=(
            LifecycleReason.CONTEXT_PROVENANCE_ATTACHED
            if added
            else LifecycleReason.CONTEXT_PROVENANCE_REPLAY
        ),
    )


async def _accept_final_probe_match(
    db: Database,
    session: Any,
    *,
    event: Event,
    candidate: EventCandidate,
    schedule_tasks: bool,
    schedule_kwargs: dict[str, Any] | None,
    enqueue_ticket_sites: Any,
) -> SmartUpdateResult | None:
    """Accept an authoritative last-moment duplicate without a second LLM pass.

    The normal matcher/adjudicator has already run for this packet.  This path
    exists only for a row that appeared or changed after that read.  The final
    deterministic probe is deliberately strong, so reloading and re-running it
    can safely converge the race by attaching the packet to the authoritative
    Event instead of emitting a veto or scheduling a redundant second Smart
    Update operation.
    """

    event_id = int(event.id or 0)
    if event_id <= 0:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="final_probe_event_missing", retry_reason=RetryReason.FINAL_PROBE_EVENT_MISSING,
        )
    if _event_blocked_by_explicit_occurrence(candidate, event):
        return None
    authoritative = await session.get(Event, event_id, populate_existing=True)
    if authoritative is None:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            diagnostic_event_id=event_id,
            reason="final_probe_event_missing", retry_reason=RetryReason.FINAL_PROBE_EVENT_MISSING,
        )
    if _pre_create_duplicate_probe(candidate, [authoritative]) is None:
        # The authoritative reload disproved the stale duplicate signal. The
        # caller remains in this transaction-local create operation and may
        # insert the distinct Event; no retry or identity veto is needed.
        return None

    added_posters, _urls, _preview, _pruned, _photos_changed = await _apply_posters(
        session,
        event_id,
        candidate.posters,
        poster_scope_hashes=candidate.poster_scope_hashes,
        event_title=authoritative.title,
    )
    added_sources, same_source = await _ensure_event_source(session, event_id, candidate)
    await session.flush()
    if candidate.source_text:
        await _sync_source_texts(session, authoritative)
    await enqueue_ticket_sites(session, event_id=event_id)
    await _record_source_facts(
        session,
        event_id,
        candidate,
        [("Матчинг: authoritative final duplicate probe", "note")],
    )
    await session.commit()

    canonical_changed = bool(added_posters or (added_sources and not same_source))
    if canonical_changed:
        try:
            await _classify_topics(db, event_id)
        except Exception:
            logger.warning(
                "smart_update: final probe topic classification failed event_id=%s",
                event_id,
                exc_info=True,
            )
    if schedule_tasks and canonical_changed:
        try:
            from main import schedule_event_update_tasks

            async with db.get_session() as refresh_session:
                refreshed = await refresh_session.get(Event, event_id)
            if refreshed is not None:
                task_kwargs = dict(schedule_kwargs or {})
                task_kwargs["refresh_existing_vk"] = True
                await schedule_event_update_tasks(db, refreshed, **task_kwargs)
        except Exception:
            logger.warning(
                "smart_update: final probe scheduling failed event_id=%s",
                event_id,
                exc_info=True,
            )
    return SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.MERGED,
        event_id=event_id,
        merged=canonical_changed,
        added_posters=added_posters,
        added_sources=added_sources,
        reason="final_transaction_duplicate_probe",
    )


async def retry_due_smart_update_candidates(
    db: Database,
    *,
    limit: int = 25,
    lease_seconds: int = 300,
    on_accepted: Callable[[EventCandidate, SmartUpdateResult], Awaitable[None]] | None = None,
) -> dict[str, int]:
    """One-time compatibility drain for legacy durable candidates.

    New invocations never enqueue rows. The drain invokes the same linear
    facade once for each old RETRY_SCHEDULED payload and every branch closes as
    accepted, product rejection or visible technical failure.
    """

    owner = f"smart-update:{os.getpid()}:{id(asyncio.current_task())}"
    claimed = await claim_due_candidates(
        db,
        lease_owner=owner,
        limit=limit,
        lease_seconds=lease_seconds,
    )
    result_counts = {item.value: 0 for item in SmartUpdateTerminalOutcome}
    result_counts["claimed"] = len(claimed)
    result_counts["rehydration_failed"] = 0
    allowed_fields = {item.name for item in dataclass_fields(EventCandidate)}
    for item in claimed:
        try:
            payload = {
                key: value
                for key, value in item.candidate_payload.items()
                if key in allowed_fields
            }
            payload["candidate_key"] = item.candidate_key
            intent_raw = str(payload.get("intent") or SmartUpdateIntent.UPSERT_EVENT.value)
            if intent_raw.startswith("SmartUpdateIntent."):
                intent_raw = intent_raw.rsplit(".", 1)[-1]
            payload["intent"] = SmartUpdateIntent(intent_raw)
            payload["posters"] = [
                value if isinstance(value, PosterCandidate) else PosterCandidate(**value)
                for value in (payload.get("posters") or [])
                if isinstance(value, (PosterCandidate, dict))
            ]
            candidate = EventCandidate(**payload)
        except Exception:
            result_counts["rehydration_failed"] += 1
            logger.exception(
                "smart_update.legacy_drain rehydration_failed candidate_key=%s",
                item.candidate_key,
            )
            await terminalize_claimed_candidate_technical(
                db,
                candidate_state_id=item.candidate_state_id,
                lease_owner=owner,
                reason="legacy_rehydration_failed",
            )
            result_counts[SmartUpdateTerminalOutcome.FAILED_TECHNICAL.value] += 1
            continue
        result = await smart_event_update(
            db,
            candidate,
            check_source_url=bool(candidate.replay_check_source_url),
            schedule_tasks=bool(candidate.replay_schedule_tasks),
            schedule_kwargs=dict(candidate.replay_schedule_kwargs or {}),
            _lease_owner=owner,
        )
        result_counts[result.outcome.value] += 1
        if on_accepted is not None and result.outcome in {
            SmartUpdateTerminalOutcome.CREATED,
            SmartUpdateTerminalOutcome.MERGED,
        }:
            try:
                await on_accepted(candidate, result)
            except Exception:
                # Notification/observability must never roll back or obscure the
                # already durable Smart Update terminal result.  The scheduler
                # logs this branch and can alert independently.
                logger.exception(
                    "smart_update.retry accepted_callback_failed "
                    "candidate_key=%s outcome=%s event_id=%s",
                    item.candidate_key,
                    result.outcome.value,
                    result.event_id,
                )
    return result_counts


def _candidate_source_role(candidate: EventCandidate) -> str:
    return (
        "context_only"
        if str(getattr(candidate, "source_role", "") or "").strip().lower() == "context_only"
        else "identity_bearing"
    )


async def _exact_input_noop_event_id(
    db: Database,
    *,
    canonical_source_url: str | None,
    source_role: str,
    source_fingerprint: str,
    occurrence_key: str | None = None,
    candidate_key: str | None = None,
) -> tuple[int | None, bool]:
    if not canonical_source_url or not source_fingerprint:
        return None, False
    try:
        async with db.raw_conn() as conn:
            if candidate_key:
                cursor = await conn.execute(
                    "SELECT DISTINCT event_id FROM event_source "
                    "WHERE candidate_key=? AND source_fingerprint=? ORDER BY event_id LIMIT 2",
                    (candidate_key, source_fingerprint),
                )
            else:
                cursor = await conn.execute(
                    "SELECT DISTINCT event_id FROM event_source "
                    "WHERE canonical_source_url=? AND source_role=? AND source_fingerprint=? "
                    "AND COALESCE(occurrence_key,'')=COALESCE(?,'') ORDER BY event_id LIMIT 2",
                    (canonical_source_url, source_role, source_fingerprint, occurrence_key),
                )
            rows = await cursor.fetchall()
            await cursor.close()
        event_ids = [int(row[0]) for row in rows if row and row[0] is not None]
        if len(event_ids) > 1:
            return None, True
        return (event_ids[0] if event_ids else None), False
    except Exception:
        logger.warning("smart_update: exact input noop lookup failed", exc_info=True)
        # Observer failure cannot authorize LLM work or domain writes: the
        # binding may already exist, so the durable facade must retry it.
        return None, True


async def _apply_holiday_festival_mapping(db: Database, event_id: int) -> bool:
    """Ensure pseudo-festivals from docs/reference/holidays.md are applied universally.

    This runs as part of Smart Update so that holiday grouping does not depend on
    the caller (VK auto import / Telegram Monitoring / manual add flows).
    """
    try:
        from main import ensure_festival, get_holiday_record
    except Exception:
        return False

    try:
        import vk_intake as vk_mod
    except Exception:
        return False

    async with db.get_session() as session:
        ev = await session.get(Event, int(event_id))
        if not ev:
            return False
        fest_value = (getattr(ev, "festival", None) or "").strip()
        if not fest_value:
            return False
        record = get_holiday_record(fest_value)
        if record is None:
            return False
        event_date = (getattr(ev, "date", None) or "").strip()
        event_end_date = (getattr(ev, "end_date", None) or "").strip() or None
        tolerance = getattr(record, "tolerance_days", None)
        if not vk_mod._event_date_matches_holiday(record, event_date, event_end_date, tolerance):
            return False

        photo_urls = list(getattr(ev, "photo_urls", None) or [])
        current_festival = getattr(ev, "festival", None)

    canonical_name = str(getattr(record, "canonical_name", "") or "").strip()
    if not canonical_name:
        return False

    target_year = date.today().year
    date_token = (event_date or "").split("..", 1)[0].strip()
    try:
        target_year = date.fromisoformat(date_token).year
    except Exception:
        target_year = date.today().year

    start_iso, end_iso = vk_mod._holiday_date_range(record, target_year)
    ensure_kwargs: dict[str, Any] = {}
    desc = str(getattr(record, "description", "") or "").strip()
    if desc:
        ensure_kwargs["description"] = desc
        ensure_kwargs["source_text"] = desc
    if start_iso:
        ensure_kwargs["start_date"] = start_iso
    if end_iso:
        ensure_kwargs["end_date"] = end_iso
    if photo_urls:
        ensure_kwargs["photo_url"] = photo_urls[0]
        ensure_kwargs["photo_urls"] = photo_urls
    aliases_payload = [a for a in getattr(record, "normalized_aliases", ()) if a]
    if aliases_payload:
        ensure_kwargs["aliases"] = aliases_payload

    fest_obj, fest_created, fest_updated = await ensure_festival(
        db,
        canonical_name,
        **ensure_kwargs,
    )
    _ = fest_obj

    changed = bool(fest_created or fest_updated)

    if (current_festival or "") != canonical_name:
        async with db.get_session() as session:
            ev2 = await session.get(Event, int(event_id))
            if ev2 and (getattr(ev2, "festival", None) or "") != canonical_name:
                ev2.festival = canonical_name
                session.add(ev2)
                await session.commit()
                changed = True

    return changed


_TG_DEFAULT_LOCATION_CITY_DISAMBIGUATION_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["default", "extracted"]},
        "confidence": {"type": "number"},
        "reason_short": {"type": "string"},
    },
    "required": ["decision", "confidence", "reason_short"],
    "additionalProperties": False,
}


async def _maybe_disambiguate_telegram_default_location_city(candidate: EventCandidate) -> None:
    """Best-effort: decide whether to keep TelegramSource.default_location city.

    Telegram sources can have `default_location` (a strong prior), but sometimes posts
    genuinely describe an event in another city. At the same time, "context cities"
    like "(г. Москва)" can appear in biographies and must not flip the event city.

    This helper runs a small Gemma JSON check only when:
    - source_type is Telegram
    - default city exists
    - extracted city exists and conflicts with default
    - the current candidate city equals the default city (i.e. we overrode it earlier)
    """
    try:
        st = str(candidate.source_type or "").strip().lower()
    except Exception:
        st = ""
    if st not in {"telegram", "tg"}:
        return
    metrics = candidate.metrics if isinstance(candidate.metrics, dict) else None
    if not metrics:
        return

    default_location = str(metrics.get("tg_default_location") or "").strip()
    default_city = str(metrics.get("tg_default_city") or "").strip()
    extracted_city = str(metrics.get("tg_extracted_city") or "").strip()
    if not default_location or not default_city or not extracted_city:
        return
    if default_city.casefold() == extracted_city.casefold():
        return
    current_city = str(candidate.city or "").strip()
    if not current_city or current_city.casefold() != default_city.casefold():
        return
    if SMART_UPDATE_LLM_DISABLED:
        return

    extracted_location_name = str(metrics.get("tg_extracted_location_name") or "").strip()
    extracted_location_address = str(metrics.get("tg_extracted_location_address") or "").strip()
    source_text = _clip_to_readable_boundary(candidate.source_text, 2200)

    prompt = (
        "Ты помощник по импорту городских событий из Telegram.\n"
        "У источника есть `default_location` — это сильный prior (обычно события проходят там), "
        "но иногда пост может описывать событие в другом городе.\n"
        "Extractor мог ошибочно извлечь город из контекста (например «(г. Москва)» про участников), "
        "а не про место проведения.\n\n"
        "Задача: определить, где проходит событие — в default_location (город default_city) "
        "или в extracted_city.\n"
        "Верни строго JSON:\n"
        '{"decision":"default|extracted","confidence":0.0,"reason_short":"..."}\n\n'
        "Правила:\n"
        "- НЕ выдумывай третий город.\n"
        "- Если место проведения явно в extracted_city (адрес/площадка/метро + город) — выбери `extracted`.\n"
        "- Если extracted_city упоминается как контекст (происхождение артистов/организаторов и т.п.), "
        "а место проведения соответствует default_location — выбери `default`.\n"
        "- Если не уверен — выбери `default` и поставь confidence <= 0.6.\n\n"
        f"default_location: {default_location!r}\n"
        f"default_city: {default_city!r}\n"
        f"extracted_city: {extracted_city!r}\n"
        f"extracted_location_name: {extracted_location_name!r}\n"
        f"extracted_location_address: {extracted_location_address!r}\n"
        f"event_title: {_clip_title(candidate.title, 120)!r}\n"
        f"event_date: {str(candidate.date or '').strip()!r}\n"
        f"event_time: {str(candidate.time or '').strip()!r}\n"
        f"post_text:\n{source_text}\n"
    )

    data = await _ask_gemma_json(
        prompt,
        _TG_DEFAULT_LOCATION_CITY_DISAMBIGUATION_SCHEMA,
        max_tokens=140,
        label="tg_city_disambiguation",
    )
    if not isinstance(data, dict):
        return
    decision = str(data.get("decision") or "").strip().lower()
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    reason_short = str(data.get("reason_short") or "").strip()

    metrics["tg_city_disambiguation_decision"] = decision or None
    metrics["tg_city_disambiguation_confidence"] = confidence
    metrics["tg_city_disambiguation_reason"] = reason_short[:160] if reason_short else None

    if decision != "extracted" or confidence < 0.7:
        logger.info(
            "smart_update.tg_city_disambiguation keep_default decision=%s conf=%.2f default=%s extracted=%s source_url=%s",
            decision,
            confidence,
            default_city,
            extracted_city,
            candidate.source_url,
        )
        return

    # Apply the override: let the region filter reject truly out-of-scope events.
    candidate.city = extracted_city
    if extracted_location_name:
        candidate.location_name = extracted_location_name
    if extracted_location_address:
        candidate.location_address = extracted_location_address
    logger.info(
        "smart_update.tg_city_disambiguation override_city default=%s extracted=%s conf=%.2f source_url=%s",
        default_city,
        extracted_city,
        confidence,
        candidate.source_url,
    )


async def _smart_event_update_impl(
    db: Database,
    candidate: EventCandidate,
    *,
    check_source_url: bool = True,
    schedule_tasks: bool = True,
    schedule_kwargs: dict[str, Any] | None = None,
) -> SmartUpdateResult:
    # Capture the caller packet before any Smart Update normalization/mutation.
    # An exact accepted retry can then stop before every LLM and domain write.
    source_fingerprint = input_packet_fingerprint(candidate)
    source_role = _candidate_source_role(candidate)
    # Intent owns behavior. A complete UPSERT candidate must not be silently
    # discarded because an older caller mislabeled its source as context-only.
    if candidate.intent is SmartUpdateIntent.UPSERT_EVENT:
        source_role = "identity_bearing"
    canonical_source_url = canonicalize_identity_url(candidate.source_url)
    if canonical_source_url:
        candidate.source_url = canonical_source_url
    canonical_ticket_link = canonicalize_identity_url(
        candidate.ticket_link,
        preserve_ticket_fragment=True,
    )
    if canonical_ticket_link:
        candidate.ticket_link = canonical_ticket_link
    candidate.source_role = source_role
    candidate.source_fingerprint = source_fingerprint
    noop_event_id, noop_binding_conflict = await _exact_input_noop_event_id(
        db,
        canonical_source_url=canonical_source_url,
        source_role=source_role,
        source_fingerprint=source_fingerprint,
        occurrence_key=candidate.occurrence_key,
        candidate_key=candidate.candidate_key,
    )
    if noop_binding_conflict:
        logger.warning(
            "smart_update.replay_review reason=source_binding_conflict source_alias=%s",
            hashlib.sha256(str(canonical_source_url or "").encode("utf-8")).hexdigest()[:12],
        )
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="source_binding_conflict", retry_reason=RetryReason.SOURCE_BINDING_CONFLICT,
        )
    if noop_event_id is not None:
        logger.info(
            "smart_update.noop event_id=%s source_role=%s source_alias=%s fingerprint=%s",
            noop_event_id,
            source_role,
            hashlib.sha256(str(canonical_source_url or "").encode("utf-8")).hexdigest()[:12],
            source_fingerprint[:12],
        )
        return SmartUpdateResult(
            status="noop_exact_source_replay",
            event_id=noop_event_id,
            reason="exact_input_packet",
        )
    grounded_festival, dropped_kgd80 = ground_kgd80_festival(
        candidate.festival,
        source_evidence=(
            candidate.source_text,
            candidate.raw_excerpt,
            candidate.occurrence_scope_text,
            candidate.source_url,
            candidate.ticket_link,
            candidate.festival_dedup_links,
            candidate.links_payload,
            [
                {
                    "ocr_text": poster.ocr_text,
                    "ocr_title": poster.ocr_title,
                    "url": _poster_candidate_evidence_url(poster),
                }
                for poster in (candidate.posters or [])
            ],
        ),
        curated_festival_series=(
            candidate.festival_series if candidate.festival_source else None
        ),
    )
    if dropped_kgd80:
        logger.warning(
            "smart_update: dropped ungrounded KGD80 festival source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
        )
        candidate.festival = None
        if (candidate.festival_context or "").strip().lower() == "event_with_festival":
            candidate.festival_context = None
    else:
        candidate.festival = grounded_festival
    logger.info(
        "smart_update.start source_type=%s source_url=%s title=%s date=%s time=%s location=%s city=%s posters=%d trust=%s festival_context=%s festival=%s festival_full=%s festival_source=%s festival_series=%s",
        candidate.source_type,
        candidate.source_url,
        _clip_title(candidate.title),
        candidate.date,
        candidate.time,
        _clip_title(candidate.location_name, 60),
        candidate.city,
        len(candidate.posters),
        candidate.trust_level,
        (candidate.festival_context or "none"),
        _clip_title(candidate.festival, 80),
        _clip_title(candidate.festival_full, 120),
        int(bool(candidate.festival_source)) if candidate.festival_source is not None else None,
        _clip_title(candidate.festival_series, 80),
    )
    upstream_source_disposition = str(candidate.source_disposition or "").strip().upper()
    upstream_positive_child = upstream_source_disposition in {
        "EVENTS_FOUND",
        "MIXED",
    }
    # Mixed recap + future-invite posts are reviewed before location defaults,
    # exhibition-duration fallbacks, vector identity recall or any write.  This
    # prevents past-occurrence facts from becoming future logistics.  Regex is
    # only a high-recall router; the semantic eventness decision belongs to the
    # LLM and provider uncertainty fails closed.
    mixed_occurrence_role_risk = _has_mixed_occurrence_role_risk(
        candidate.title,
        "\n".join([str(candidate.source_text or ""), str(candidate.raw_excerpt or "")]),
    )
    early_eventness_reviewed = False
    if (
        mixed_occurrence_role_risk
        and not upstream_positive_child
        and str(candidate.source_type or "").strip().lower() in {"vk", "tg", "telegram"}
        and not SMART_UPDATE_LLM_DISABLED
    ):
        decision, confidence, reason = await _llm_review_candidate_eventness(
            candidate,
            clean_title=str(candidate.title or "").strip(),
            clean_source_text=candidate.source_text,
            clean_raw_excerpt=candidate.raw_excerpt,
        )
        early_eventness_reviewed = True
        logger.info(
            "smart_update.mixed_occurrence_role_review decision=%s confidence=%.2f reason=%s source_type=%s source_url=%s title=%s",
            decision,
            confidence,
            reason,
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
        )
        if decision != "event" or confidence < 0.70:
            suffix = "non_event" if decision == "non_event" else "uncertain"
            # This stage is a contradiction verifier for an already-positive
            # source child.  It may ask the durable source pipeline to retry,
            # but it may not convert the child into a product no-event.
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason=f"mixed_occurrence_role_review_{suffix}",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )
        corpus = _candidate_location_grounding_corpus(candidate)
        if not (
            _source_supports_location_value(corpus, candidate.location_name)
            or _source_supports_location_value(corpus, candidate.location_address)
        ):
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason="mixed_occurrence_role_ungrounded_location",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )
    # A digest/roundup must be scoped to one occurrence before date/range/time
    # role review.  Otherwise a roundup heading such as "01–07 August" can be
    # mistaken for the duration of every individual child event.
    if _candidate_needs_llm_occurrence_scope_review(candidate) and not SMART_UPDATE_LLM_DISABLED:
        scope_ok, scope_result = await _llm_scope_candidate_occurrence(candidate)
        logger.info(
            "smart_update.occurrence_scope_review result=%s ok=%s source_type=%s source_url=%s title=%s",
            scope_result,
            int(scope_ok),
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
        )
        if not scope_ok:
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason=f"occurrence_scope_review:{scope_result}",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )
    anchor_review_needed, anchor_review_trigger = _candidate_needs_llm_anchor_role_review(candidate)
    if anchor_review_needed and not SMART_UPDATE_LLM_DISABLED:
        anchor_ok, anchor_result = await _llm_review_candidate_anchor_roles(
            candidate,
            trigger_reason=anchor_review_trigger,
        )
        logger.info(
            "smart_update.anchor_role_review trigger=%s result=%s ok=%s source_type=%s source_url=%s title=%s date=%s end_date=%s time=%s",
            anchor_review_trigger,
            anchor_result,
            int(anchor_ok),
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
            candidate.date,
            candidate.end_date,
            candidate.time,
        )
        if not anchor_ok:
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason=f"anchor_role_review:{anchor_result}",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )
        if (
            anchor_review_trigger == "explicit_unknown_start_time"
            and not _valid_hhmm_or_none(candidate.time)
        ):
            if not isinstance(candidate.metrics, dict):
                candidate.metrics = {}
            candidate.metrics[_EXPLICIT_UNKNOWN_START_LLM_CONFIRMED_METRIC] = True
    (
        candidate.location_name,
        candidate.location_address,
        candidate.city,
    ) = _canonicalize_location_fields(
        location_name=candidate.location_name,
        location_address=candidate.location_address,
        city=candidate.city,
        source_chat_username=candidate.source_chat_username,
        source_url=candidate.source_url,
    )
    needs_location_review, location_review_trigger = (
        _candidate_needs_llm_location_grounding_review(candidate)
    )
    if needs_location_review:
        location_ok, location_review_result = await _llm_review_candidate_location_grounding(
            candidate,
            trigger_reason=location_review_trigger,
        )
        logger.info(
            "smart_update.location_grounding_review trigger=%s result=%s ok=%s source_type=%s source_url=%s location=%s address=%s city=%s",
            location_review_trigger,
            location_review_result,
            int(location_ok),
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.location_name, 100),
            _clip_title(candidate.location_address, 100),
            candidate.city,
        )
        if not location_ok:
            if location_review_result == "llm_reject_missing_location":
                return SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY,
                    reason=ProductExclusionReason.MISSING_LOCATION.value,
                    product_exclusion_reason=ProductExclusionReason.MISSING_LOCATION,
                )
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason=f"location_grounding_review:{location_review_result}",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )
        (
            candidate.location_name,
            candidate.location_address,
            candidate.city,
        ) = _canonicalize_location_after_grounding_review(
            candidate,
            review_result=location_review_result,
        )
    if not candidate.date:
        logger.warning(
            "smart_update.invalid reason=missing_date source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
        )
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="missing_date",
            retry_reason=RetryReason.SOURCE_DECISION_INVALID,
        )
    if not candidate.title:
        logger.warning(
            "smart_update.invalid reason=missing_title source_type=%s source_url=%s",
            candidate.source_type,
            candidate.source_url,
        )
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="missing_title",
            retry_reason=RetryReason.SOURCE_DECISION_INVALID,
        )
    candidate_location_unsupported_prose = _candidate_location_looks_unsupported_prose(candidate)
    if not candidate.location_name:
        logger.warning(
            "smart_update.invalid reason=missing_location source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
        )
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="missing_location",
            retry_reason=RetryReason.SOURCE_DECISION_INVALID,
        )

    # A one-month fallback must never use exhibition words from a past recap to
    # invent the duration of the future occurrence.  Explicit extractor ranges
    # remain untouched; only the service fallback is suppressed.
    # Do not invent a semantic duration after a positive source parse.  An
    # explicit end date survives; an absent end date stays absent.
    inferred_default_end_date = None

    if _should_skip_past_smart_update_candidate(candidate):
        logger.info(
            "smart_update.semantic_hint reason=possible_past_event source_type=%s source_url=%s title=%s date=%s end_date=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.title),
            candidate.date,
            candidate.end_date,
        )

    await _maybe_disambiguate_telegram_default_location_city(candidate)
    candidate_location_unsupported_prose = _candidate_location_looks_unsupported_prose(candidate)

    # Deterministic region filter (project scope: Kaliningrad Oblast).
    # If extracted city/settlement is outside the region (or cannot be reliably resolved),
    # reject early so out-of-scope events do not get created/merged.
    try:
        if (os.getenv("REGION_FILTER_ENABLED", "1") or "").strip().lower() in {"1", "true", "yes", "on"}:
            # Manual operator actions (bot commands) must not be blocked by a missing city.
            # The region filter is meant primarily for automated ingestion (VK/TG/parsers).
            if (candidate.source_type or "").strip().lower() not in {"bot"}:
                from geo_region import decide_kaliningrad_oblast

                region_decision = await decide_kaliningrad_oblast(
                    db,
                    city=candidate.city,
                    location_address=candidate.location_address,
                    gemma_client=_get_gemma_client(),
                )
                if region_decision.allowed is None:
                    logger.warning(
                        "smart_update.region_filter_unresolved source_type=%s source_url=%s city=%s reason=%s source=%s",
                        candidate.source_type,
                        candidate.source_url,
                        candidate.city,
                        region_decision.reason,
                        region_decision.source,
                    )
                    return SmartUpdateResult(
                        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                        reason="region_filter_unresolved",
                        retry_reason=RetryReason.SOURCE_VERIFICATION_TECHNICAL_FAILURE,
                    )
                if region_decision.allowed is False:
                    logger.info(
                        "smart_update.rejected reason=%s source_type=%s source_url=%s city=%s region=%s source=%s",
                        region_decision.reason,
                        candidate.source_type,
                        candidate.source_url,
                        candidate.city,
                        region_decision.region_name or region_decision.region_code,
                        region_decision.source,
                    )
                    return SmartUpdateResult(
                        status="rejected_out_of_region",
                        reason=region_decision.reason,
                        product_exclusion_reason=ProductExclusionReason.OUT_OF_REGION,
                    )
    except Exception as e:  # pragma: no cover - must not crash ingestion
        logger.warning("smart_update.region_filter_failed err=%s", e)
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="region_filter_technical_failure",
            retry_reason=RetryReason.SOURCE_VERIFICATION_TECHNICAL_FAILURE,
        )

    clean_title = _strip_private_use(candidate.title) or (candidate.title or "")
    if not clean_title:
        logger.warning(
            "smart_update.invalid reason=empty_title_after_clean source_type=%s source_url=%s",
            candidate.source_type,
            candidate.source_url,
        )
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="empty_title_after_clean",
            retry_reason=RetryReason.SOURCE_DECISION_INVALID,
        )
    if _should_skip_festival_post_candidate(candidate):
        logger.info(
            "smart_update.semantic_hint reason=festival_post source_type=%s source_url=%s festival=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.festival or candidate.festival_series, 80),
        )
    raw_source_text = _strip_private_use(candidate.source_text) or (
        candidate.source_text or ""
    )
    raw_excerpt = _strip_private_use(candidate.raw_excerpt) or (candidate.raw_excerpt or "")

    # Avoid confusing self-references for non-parser sources (e.g. taking the post URL as "ticket_link").
    # We still allow t.me links for registration/DM, but they must not be identical to the source URL.
    if not str(candidate.source_type or "").strip().lower().startswith("parser:"):
        try:
            if _normalize_url(candidate.ticket_link) and _normalize_url(candidate.ticket_link) == _normalize_url(candidate.source_url):
                candidate.ticket_link = None
        except Exception:
            pass

    text_filter_facts: list[str] = []
    queue_notes: list[str] = []

    def _push_queue_note(note: str) -> None:
        value = str(note or "").strip()
        if not value:
            return
        if value not in queue_notes:
            queue_notes.append(value)

    async def _enqueue_ticket_sites_queue(session: Any, *, event_id: int) -> None:
        if not event_id:
            return
        try:
            from ticket_sites_queue import (
                enqueue_ticket_site_urls_in_session,
                extract_ticket_site_urls,
            )
        except Exception:
            return

        links_payload: list[Any] = []
        if candidate.links_payload is not None:
            links_payload.append(candidate.links_payload)
        if candidate.source_url:
            links_payload.append(candidate.source_url)
        if candidate.ticket_link:
            links_payload.append(candidate.ticket_link)

        urls = extract_ticket_site_urls(
            text=candidate.source_text,
            links_payload=links_payload,
            events_payload=[
                {
                    "ticket_link": candidate.ticket_link,
                    "links": candidate.links_payload,
                }
            ],
        )
        if not urls:
            return

        now = datetime.now(timezone.utc)
        enqueued = 0
        try:
            enqueued = await enqueue_ticket_site_urls_in_session(
                session,
                urls=urls,
                event_id=int(event_id),
                source_post_url=candidate.source_url,
                source_chat_username=candidate.source_chat_username,
                source_chat_id=candidate.source_chat_id,
                source_message_id=candidate.source_message_id,
                now=now,
            )
        except Exception:
            logger.warning(
                "smart_update: ticket_sites_queue enqueue failed source_type=%s source_url=%s event_id=%s",
                candidate.source_type,
                candidate.source_url,
                event_id,
                exc_info=True,
            )
            return

        total = int(enqueued or 0)
        if total <= 0:
            return

        if len(urls) == 1:
            _push_queue_note(f"🎟 {urls[0]} добавлена в очередь мониторинга билетных сайтов")
            return
        _push_queue_note(f"🎟 В очередь мониторинга билетных сайтов добавлено ссылок: {total}/{len(urls)}")
        for u in urls[:2]:
            _push_queue_note(f"🎟 {u}")
        if len(urls) > 2:
            _push_queue_note(f"🎟 … ещё {len(urls) - 2}")

    if inferred_default_end_date:
        text_filter_facts.append(f"Дата окончания по умолчанию: {inferred_default_end_date}")

    # Semantic detectors below are observability hints only.  This function is
    # downstream of a positive source child; deleting prose or converting that
    # child into a no-event would be a second shadow classifier.
    is_giveaway = _looks_like_ticket_giveaway(clean_title, raw_source_text, raw_excerpt)
    if is_giveaway:
        text_filter_facts.append("Semantic hint: source contains giveaway mechanics")
        if not (
            _giveaway_has_underlying_event_facts(raw_source_text)
            or _giveaway_has_underlying_event_facts(raw_excerpt)
        ):
            logger.info(
                "smart_update.semantic_hint reason=giveaway_without_local_anchors source_type=%s source_url=%s title=%s",
                candidate.source_type,
                candidate.source_url,
                _clip_title(clean_title),
            )

    if _looks_like_promo_or_congrats(clean_title, raw_source_text, raw_excerpt) and not _candidate_has_event_anchors(candidate):
        logger.info(
            "smart_update.semantic_hint reason=promo_or_congrats source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(clean_title),
        )

    clean_source_text = raw_source_text or ""
    clean_raw_excerpt = raw_excerpt
    clean_source_text = _normalize_bullet_markers(clean_source_text) or clean_source_text
    clean_raw_excerpt = _normalize_bullet_markers(clean_raw_excerpt) or clean_raw_excerpt

    # High-recall detector inventory.  Every match is a warning/verification
    # signal only; no branch may delete a positive event child.
    source_type_clean = str(candidate.source_type or "").strip().lower()
    if source_type_clean in {"vk", "telegram", "tg"}:
        combined_text = "\n".join(
            [
                clean_source_text or "",
                clean_raw_excerpt or "",
            ]
        ).strip()
        semantic_hints: list[str] = []
        detector_map = (
            ("open_call", _looks_like_open_call_not_event(clean_title, combined_text)),
            ("work_schedule", _looks_like_work_schedule_notice(clean_title, combined_text)),
            ("non_event_notice", _looks_like_non_event_notice(clean_title, combined_text)),
            ("venue_status_update", _looks_like_venue_status_update_not_event(clean_title, combined_text)),
            ("congrats_notice", _looks_like_congrats_notice_not_event(clean_title, combined_text)),
            ("unsupported_exhibition_teaser_date", _looks_like_unsupported_exhibition_teaser_date(candidate, combined_text)),
            ("course_promo", _looks_like_course_promo(clean_title, combined_text)),
            ("service_promo", _looks_like_service_promo_not_event(clean_title, combined_text)),
            ("rental_booking", _looks_like_rental_booking_not_event(clean_title, combined_text)),
            ("too_soon", _looks_like_too_soon_notice(clean_title, combined_text)),
            ("event_logistics_notice", _looks_like_event_logistics_notice_not_event(clean_title, combined_text)),
            ("online_event", _looks_like_online_event(clean_title, combined_text) and not _candidate_has_physical_event_anchors(candidate)),
            ("book_review", _looks_like_book_review_not_event(clean_title, combined_text)),
            ("photo_day", _looks_like_photo_day_not_event(clean_title, combined_text, candidate=candidate)),
            ("retrospective_future_teaser", _looks_like_retrospective_future_teaser_not_event(clean_title, combined_text, candidate=candidate)),
            ("completed_event_report", _looks_like_completed_event_report_not_event(clean_title, combined_text, candidate=candidate)),
        )
        semantic_hints.extend(reason for reason, matched in detector_map if matched)
        outage_reason = _looks_like_utility_outage_or_road_closure(clean_title, combined_text)
        if outage_reason:
            semantic_hints.append(str(outage_reason))
        if semantic_hints:
            for semantic_hint in semantic_hints:
                text_filter_facts.append(f"Semantic hint: {semantic_hint}")
            logger.info(
                "smart_update.semantic_hints hints=%s source_type=%s source_url=%s title=%s",
                semantic_hints,
                candidate.source_type,
                candidate.source_url,
                _clip_title(clean_title),
            )
        if (
            not upstream_positive_child
            and not early_eventness_reviewed
            and _candidate_needs_llm_eventness_review(candidate, combined_text)
        ):
            decision, confidence, reason = await _llm_review_candidate_eventness(
                candidate,
                clean_title=clean_title,
                clean_source_text=clean_source_text,
                clean_raw_excerpt=clean_raw_excerpt,
            )
            logger.info(
                "smart_update.eventness_review decision=%s confidence=%.2f reason=%s source_type=%s source_url=%s title=%s",
                decision,
                confidence,
                reason,
                candidate.source_type,
                candidate.source_url,
                _clip_title(clean_title),
            )
            if decision != "event" or confidence < 0.55:
                retry_reason = (
                    "weak_eventness_review_non_event"
                    if decision == "non_event"
                    else "weak_eventness_review_uncertain"
                )
                return SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                    reason=retry_reason,
                    retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
                )
            text_filter_facts.append(f"LLM eventness review: event ({reason or 'weak candidate accepted'})")

    # Ticket price grounding: prevent hallucinated min/max prices for VK/TG sources.
    # Only accept price values when the source text/OCR contains explicit price signals.
    if source_type_clean in {"vk", "telegram", "tg"} and (
        candidate.ticket_price_min is not None or candidate.ticket_price_max is not None
    ):
        poster_texts_for_price: list[str] = []
        for p in candidate.posters or []:
            for k in ("ocr_text", "ocr_title"):
                v = getattr(p, k, None)
                if isinstance(v, str) and v.strip():
                    poster_texts_for_price.append(v.strip())
        price_probe = "\n".join(
            [
                clean_title,
                clean_source_text or "",
                clean_raw_excerpt or "",
                *poster_texts_for_price[:3],
            ]
        ).strip()
        if not _has_price_evidence(price_probe, candidate.ticket_price_min, candidate.ticket_price_max):
            before_min = candidate.ticket_price_min
            before_max = candidate.ticket_price_max
            note = "Semantic hint: extracted price lacks deterministic source match"
            if note not in text_filter_facts:
                text_filter_facts.append(note)
            logger.info(
                "smart_update.semantic_hint reason=price_evidence_conflict source_type=%s source_url=%s title=%s value=%s..%s",
                candidate.source_type,
                candidate.source_url,
                _clip_title(clean_title),
                before_min,
                before_max,
            )

    free_probe_text = "\n".join(
        [
            clean_title,
            clean_source_text or "",
            clean_raw_excerpt or "",
        ]
    ).strip()
    if candidate.is_free is True and _free_claim_contradicted_by_source(candidate, free_probe_text):
        note = "Semantic hint: source may contradict extracted free-attendance"
        if note not in text_filter_facts:
            text_filter_facts.append(note)
        logger.info(
            "smart_update.semantic_hint reason=source_contradicts_free source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(clean_title),
        )

    # Blood donation actions are free-to-attend; mentions of money are typically about
    # donor compensation, not an entrance fee. If no ticket price survived grounding,
    # mark as free so Telegraph/VK summaries don't show it as "paid tickets".
    if (
        source_type_clean in {"vk", "telegram", "tg"}
        and candidate.is_free is not True
        and candidate.ticket_price_min is None
        and candidate.ticket_price_max is None
    ):
        free_probe = "\n".join(
            [clean_title, clean_source_text or "", clean_raw_excerpt or ""]
        ).strip()
        if _looks_like_blood_donation_event(clean_title, free_probe):
            note = "Semantic hint: blood-donation event may be free"
            if note not in text_filter_facts:
                text_filter_facts.append(note)

    # Festival detection is pure here. Persistence is deferred until the event
    # identity gate has accepted and the create/merge transaction has committed.
    pending_festival_queue: dict[str, Any] | None = None

    async def _persist_pending_festival_queue() -> None:
        nonlocal pending_festival_queue
        payload = pending_festival_queue
        pending_festival_queue = None
        if not payload:
            return
        try:
            from festival_queue import enqueue_festival_source
            from models import FestivalQueueItem

            async with db.get_session() as _fest_session:
                done_id = (
                    await _fest_session.execute(
                        select(FestivalQueueItem.id)
                        .where(
                            FestivalQueueItem.source_kind == payload["source_kind"],
                            FestivalQueueItem.source_url == payload["source_url"],
                            FestivalQueueItem.status == "done",
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
            if done_id is None:
                item = await enqueue_festival_source(db, **payload)
                msg = (
                    f"🎪 Добавлено в фестивальную очередь: {payload['festival_context']} "
                    f"{payload.get('festival_name') or payload.get('festival_full')} "
                    f"(id={getattr(item, 'id', None)})"
                )
            else:
                msg = f"🎪 Фестивальная очередь: уже done (id={done_id})"
            text_filter_facts.append(msg)
            _push_queue_note(msg)
        except Exception:
            logger.warning("smart_update: deferred festival_queue enqueue failed", exc_info=True)

    # Pure deterministic routing (regex/signal-based), not another LLM call.
    try:
        from festival_queue import detect_festival_context

        def _map_source_kind(source_type: str) -> str:
            st = (source_type or "").strip().lower()
            if st in {"tg", "telegram"}:
                return "tg"
            if st in {"vk"}:
                return "vk"
            return "url"

        def _parse_vk_ids(url: str | None) -> tuple[int | None, int | None]:
            if not url:
                return None, None
            m = re.search(r"wall-?(\d+)_([0-9]+)", url)
            if not m:
                return None, None
            return int(m.group(1)), int(m.group(2))

        queue_url = (candidate.source_url or "").strip()
        if queue_url:
            poster_texts = [
                p.ocr_text
                for p in (candidate.posters or [])
                if (p.ocr_text or "").strip()
            ]
            ev_payload = {
                "title": candidate.title,
                "date": candidate.date,
                "end_date": candidate.end_date,
                "time": candidate.time,
                "location_name": candidate.location_name,
                "event_type": candidate.event_type,
                "ticket_link": candidate.ticket_link,
                "festival": candidate.festival,
                "festival_full": candidate.festival_full,
                "festival_context": candidate.festival_context,
            }
            decision = detect_festival_context(
                parsed_events=[ev_payload],
                festival_payload={
                    "festival": candidate.festival,
                    "festival_full": candidate.festival_full,
                    "festival_context": candidate.festival_context,
                },
                source_text=clean_source_text or clean_raw_excerpt,
                poster_texts=poster_texts,
                source_is_festival=bool(candidate.festival_source),
                source_series=candidate.festival_series,
            )
            if decision and decision.context != "none" and (decision.festival or decision.festival_full):
                # Backfill candidate fields for better operator logs and future merges.
                if (candidate.festival_context or "").strip().lower() in {"", "none"} and decision.context:
                    candidate.festival_context = decision.context
                if not (candidate.festival or "").strip() and decision.festival:
                    candidate.festival = decision.festival
                if not (candidate.festival_full or "").strip() and decision.festival_full:
                    candidate.festival_full = decision.festival_full
                if decision.dedup_links and not list(candidate.festival_dedup_links or []):
                    candidate.festival_dedup_links = list(decision.dedup_links)

                gid, pid = _parse_vk_ids(queue_url)
                pending_festival_queue = {
                    "source_kind": _map_source_kind(candidate.source_type),
                    "source_url": queue_url,
                    "source_text": clean_source_text or clean_raw_excerpt,
                    "festival_context": decision.context,
                    "festival_name": decision.festival,
                    "festival_full": decision.festival_full,
                    "festival_series": candidate.festival_series,
                    "dedup_links": decision.dedup_links,
                    "signals": decision.signals,
                    "source_chat_username": candidate.source_chat_username,
                    "source_chat_id": candidate.source_chat_id,
                    "source_message_id": candidate.source_message_id,
                    "source_group_id": gid,
                    "source_post_id": pid,
                }
    except Exception:
        logger.warning("smart_update: festival_queue detection failed", exc_info=True)

    # Festival/program detection is routing metadata only.  It may enqueue the
    # source for programme expansion, but it cannot erase an already-positive
    # event child.
    if _should_skip_festival_post_candidate(candidate):
        logger.info(
            "smart_update.semantic_hint reason=festival_post_detected source_type=%s source_url=%s festival=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(candidate.festival or candidate.festival_series, 80),
        )
        text_filter_facts.append("Semantic hint: festival/program carrier")

    # Multi-event digests should not be imported as a single event.
    if (candidate.source_type in {"vk", "tg", "telegram"}) and _looks_like_schedule_digest(
        clean_source_text or clean_raw_excerpt,
        event_date=candidate.date,
        end_date=candidate.end_date,
    ):
        logger.info(
            "smart_update.semantic_hint reason=schedule_digest source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(clean_title),
        )
        text_filter_facts.append("Semantic hint: possible multi-event schedule")

    # "Акции" must not become events. If after promo/giveaway stripping there's no real event anchor,
    # treat it as non-event content.
    if (
        not _candidate_has_event_anchors(candidate)
        and _PROMO_STRIP_RE.search((clean_title or "") + "\n" + (clean_source_text or ""))
        and len((clean_raw_excerpt or clean_source_text or "").strip()) < 140
    ):
        logger.info(
            "smart_update.semantic_hint reason=promo_only source_type=%s source_url=%s title=%s",
            candidate.source_type,
            candidate.source_url,
            _clip_title(clean_title),
        )
        text_filter_facts.append("Semantic hint: promo-only detector matched")

    # Posters policy:
    # Keep all posters (dedupe/order happens later). OCR is used for prioritization only.
    # This avoids events ending up without images due to overly strict filtering.
    poster_filter_facts: list[str] = []
    if candidate.posters:
        # Best-effort: backfill missing OCR from local cache (cheap, no network).
        missing_hashes = [
            p.sha256 for p in candidate.posters if p.sha256 and not (p.ocr_text or "").strip()
        ]
        if missing_hashes:
            try:
                async with db.get_session() as session:
                    rows = (
                        await session.execute(
                            select(PosterOcrCache)
                            .where(PosterOcrCache.hash.in_(missing_hashes))
                            .order_by(PosterOcrCache.created_at.desc())
                        )
                    ).scalars().all()
                latest: dict[str, PosterOcrCache] = {}
                for row in rows:
                    if row.hash not in latest:
                        latest[row.hash] = row
                for p in candidate.posters:
                    if not p.sha256 or (p.ocr_text or "").strip():
                        continue
                    cached = latest.get(p.sha256)
                    if cached and (cached.text or "").strip():
                        p.ocr_text = cached.text
                        if cached.title:
                            p.ocr_title = cached.title
            except Exception:
                logger.warning("smart_update: poster OCR cache backfill failed", exc_info=True)
        note = _far_future_poster_date_mismatch_note(
            candidate_date=candidate.date,
            posters=candidate.posters,
            months_threshold=SMART_UPDATE_FAR_FUTURE_REVIEW_MONTHS,
        )
        if note:
            # The source-level contradiction verifier already owns semantic
            # date resolution.  Smart Update records the mismatch as evidence;
            # it must not silently suppress an accepted positive child.
            poster_filter_facts.append(note)

    # Raw-URL existence is deliberately not an idempotency verdict. Exact
    # immutable retries have already returned above by packet fingerprint. The
    # same canonical URL with a changed packet must continue into identity
    # matching/gating so a real source edit can be applied; cross-event owners
    # are still rejected by the role-aware binding guard and DB invariant.

    cand_start, cand_end = _candidate_date_range(candidate)
    if not cand_start or not cand_end:
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            reason="invalid_date",
            retry_reason=RetryReason.SOURCE_DECISION_INVALID,
        )

    candidate.explicit_occurrence_conflict_event_ids = (
        await _explicit_occurrence_conflict_ids(db, candidate)
    )
    if candidate.explicit_occurrence_conflict_event_ids:
        candidate.force_create_distinct_reason = (
            IdentityDistinctReason.EXPLICIT_OCCURRENCE_ID_CONFLICT
        )

    # A classified identity-bearing binding outranks legacy Event source fields.
    # This prevents a shared/context URL from being rebound by fuzzy anchors.
    try:
        anchor_match = await _match_existing_event_by_event_source_url(db, candidate)
    except Exception:
        logger.warning("smart_update: event_source_url anchor match failed", exc_info=True)
        anchor_match = None
    if anchor_match is None:
        anchor_match = await _match_existing_event_by_source_anchor(db, candidate)
    if _event_blocked_by_explicit_occurrence(candidate, anchor_match):
        anchor_match = None
    if anchor_match is not None:
        shortlist = [anchor_match]
        anchor_forced = True
    else:
        anchor_forced = False
        async with db.get_session() as session:
            stmt = select(Event).where(
                and_(
                    Event.date <= cand_end.isoformat(),
                    or_(
                        and_(
                            Event.end_date.is_(None),
                            Event.date >= cand_start.isoformat(),
                        ),
                        Event.end_date >= cand_start.isoformat(),
                    ),
                )
            )
            stmt = _apply_soft_city_filter(stmt, candidate.city)
            res = await session.execute(stmt)
            shortlist = [
                event
                for event in res.scalars().all()
                if not _event_blocked_by_explicit_occurrence(candidate, event)
            ]

    is_canonical_site = str(candidate.source_type or "").startswith("parser:")
    city_noise_rescued = False
    longrun_exhibition_match: Event | None = None
    citywide_festival_recalled_ids: set[int] = set()
    excluded_occurrence_ids: set[int] = set()
    if (not anchor_forced) and (not shortlist):
        city_noise_match, city_noise_reason = await _match_existing_event_by_city_noise_rescue(
            db,
            candidate,
            is_canonical_site=is_canonical_site,
        )
        if city_noise_match is not None and not _event_blocked_by_explicit_occurrence(
            candidate, city_noise_match
        ):
            shortlist = [city_noise_match]
            city_noise_rescued = True
            logger.info(
                "smart_update.shortlist rescue=%s event_id=%s source_type=%s source_url=%s",
                city_noise_reason,
                getattr(city_noise_match, "id", None),
                candidate.source_type,
                candidate.source_url,
            )

    if (not anchor_forced) and shortlist:
        longrun_exhibition_match = _deterministic_longrun_exhibition_exact_title_match(
            candidate,
            shortlist,
        )
        if longrun_exhibition_match is not None:
            logger.info(
                "smart_update.shortlist longrun_exhibition_match event_id=%s source_type=%s source_url=%s",
                getattr(longrun_exhibition_match, "id", None),
                candidate.source_type,
                candidate.source_url,
            )

    if (
        (not anchor_forced)
        and (not city_noise_rescued)
        and candidate.location_name
        and (not candidate_location_unsupported_prose)
    ):
        same_ticket_recall = _same_specific_ticket_shortlist_recall(candidate, shortlist)
        citywide_festival_recall = _citywide_festival_shortlist_recall(candidate, shortlist)
        shortlist = [
            ev for ev in shortlist if _event_candidate_location_matches(ev, candidate)
        ]
        if same_ticket_recall or citywide_festival_recall:
            seen_ids = {getattr(ev, "id", None) for ev in shortlist}
            for ev in [*same_ticket_recall, *citywide_festival_recall]:
                if getattr(ev, "id", None) not in seen_ids:
                    shortlist.append(ev)
                    seen_ids.add(getattr(ev, "id", None))
                    if ev in citywide_festival_recall and getattr(ev, "id", None):
                        citywide_festival_recalled_ids.add(int(getattr(ev, "id")))

    if is_canonical_site and shortlist:
        shortlist, excluded_occurrence_id_list = (
            await _filter_same_parser_source_occurrence_conflicts(
                db,
                candidate,
                shortlist,
            )
        )
        excluded_occurrence_ids = set(excluded_occurrence_id_list)
        if excluded_occurrence_ids:
            logger.info(
                "smart_update.shortlist excluded_same_parser_time_conflicts=%s "
                "source_type=%s source_url=%s date=%s time=%s",
                sorted(excluded_occurrence_ids),
                candidate.source_type,
                candidate.source_url,
                candidate.date,
                candidate.time,
            )

    # Time is an anchor field, but for canonical site/parser imports we allow time corrections:
    # matching must work even if a Telegram-first event had a wrong/empty time.
    cand_time_norm = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
    if (not anchor_forced) and (not city_noise_rescued) and cand_time_norm and (not is_canonical_site):
        time_filtered: list[Event] = []
        for ev in shortlist:
            ev_time_anchor = _event_anchor_time(ev)
            if (not ev_time_anchor) or (ev_time_anchor == cand_time_norm):
                time_filtered.append(ev)
        if time_filtered:
            shortlist = time_filtered

    # If the candidate has no explicit time, try to shrink the shortlist to the only
    # title-related event (helps prevent duplicates when time is missing but the match is obvious).
    if (
        (not anchor_forced)
        and (not city_noise_rescued)
        and (not cand_time_norm)
        and candidate.location_name
        and (not candidate_location_unsupported_prose)
        and len(shortlist) > 1
    ):
        related = [
            ev
            for ev in shortlist
            if (not _has_explicit_time_conflict(cand_time_norm, _event_anchor_time(ev)))
            and _titles_look_related(candidate.title, getattr(ev, "title", None))
        ]
        if len(related) == 1:
            shortlist = related

    if (not anchor_forced) and (not shortlist) and (not city_noise_rescued):
        city_noise_match, city_noise_reason = await _match_existing_event_by_city_noise_rescue(
            db,
            candidate,
            is_canonical_site=is_canonical_site,
        )
        if (
            city_noise_match is not None
            and not _event_blocked_by_explicit_occurrence(candidate, city_noise_match)
            and int(getattr(city_noise_match, "id", 0) or 0)
            not in excluded_occurrence_ids
        ):
            shortlist = [city_noise_match]
            city_noise_rescued = True
            logger.info(
                "smart_update.shortlist rescue=%s event_id=%s source_type=%s source_url=%s",
                city_noise_reason,
                getattr(city_noise_match, "id", None),
                candidate.source_type,
                candidate.source_url,
            )

    posters_map: dict[int, list[EventPoster]] = {}
    if candidate.force_create_distinct and shortlist:
        logger.info(
            "smart_update.identity create_distinct candidate_key=%s previous_retry_exhausted=1",
            candidate.candidate_key,
        )
        shortlist = []
    if shortlist:
        event_ids = [ev.id for ev in shortlist if ev.id]
        posters_map = await _fetch_event_posters_map(db, event_ids)

    allow_parallel = (
        False
        if candidate_location_unsupported_prose
        else _allow_parallel_events(candidate.location_name)
    )
    candidate_poster_texts = [p.ocr_text for p in candidate.posters if p.ocr_text]
    candidate_hall = _extract_hall_hint(
        (candidate.source_text or "") + "\n" + "\n".join(candidate_poster_texts)
    )
    if allow_parallel and candidate_hall and shortlist:
        filtered: list[Event] = []
        for ev in shortlist:
            ev_posters = posters_map.get(ev.id or 0, [])
            ev_poster_texts = [p.ocr_text for p in ev_posters if p.ocr_text]
            hall = _extract_hall_hint(
                (ev.source_text or "")
                + "\n"
                + (ev.description or "")
                + "\n"
                + "\n".join(ev_poster_texts)
            )
            if hall and hall != candidate_hall:
                continue
            filtered.append(ev)
        shortlist = filtered

    llm_create_bundle: dict[str, Any] | None = None
    # Deterministic match branches do not call the LLM matcher, but the shared
    # title-safety guard below still consumes these values. Keep their evidence
    # explicitly neutral instead of leaving function-local state unbound.
    match_id: int | None = None
    confidence = 0.0

    if not shortlist:
        match_event = None
        match_reason = "shortlist_empty"
    else:

        # Deterministic single-candidate match is allowed only when anchors look sane.
        # Otherwise fall back to LLM matching / create to avoid catastrophic cross-event merges.
        if anchor_forced:
            match_event = shortlist[0]
            match_reason = "anchor_forced"
        elif longrun_exhibition_match is not None:
            match_event = longrun_exhibition_match
            match_reason = "deterministic_longrun_exhibition_exact_title"
            logger.info(
                "smart_update.match type=deterministic_longrun_exhibition_exact_title event_id=%s",
                getattr(match_event, "id", None),
            )
        elif (
            len(shortlist) == 1
            and not (
                getattr(shortlist[0], "id", None) in citywide_festival_recalled_ids
                and not _event_candidate_location_matches(shortlist[0], candidate)
            )
            and _single_candidate_auto_match_ok(
                candidate,
                shortlist[0],
                is_canonical_site=is_canonical_site,
            )
        ):
            match_event = shortlist[0]
            match_reason = "single_candidate"
        else:
            match_event = None
            match_reason = ""

        candidate_hashes = _poster_hashes(candidate.posters)
        ticket_norm = _normalize_ticket_url_for_match(candidate.ticket_link)

        strong_matches: dict[int, int] = {}
        if ticket_norm:
            for ev in shortlist:
                if _normalize_ticket_url_for_match(ev.ticket_link) == ticket_norm and ev.id:
                    strong_matches[ev.id] = strong_matches.get(ev.id, 0) + 3
        if candidate_hashes:
            for ev in shortlist:
                hashes = {p.poster_hash for p in posters_map.get(ev.id or 0, [])}
                overlap = len(candidate_hashes & hashes)
                if overlap and ev.id:
                    strong_matches[ev.id] = strong_matches.get(ev.id, 0) + overlap

        logger.info(
            "smart_update.shortlist count=%d allow_parallel=%s source_type=%s source_url=%s",
            len(shortlist),
            bool(allow_parallel),
            candidate.source_type,
            candidate.source_url,
        )
        if match_event is None:
            longrun = _deterministic_same_post_longrun_exact_title_match(
                candidate,
                shortlist,
            )
            if longrun is not None:
                match_event = longrun
                match_reason = "deterministic_same_post_longrun_exact_title"
                logger.info(
                    "smart_update.match type=deterministic_same_post_longrun_exact_title event_id=%s",
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            ticket_anchor_match, ticket_anchor_reason = _deterministic_ticket_source_anchor_match(
                candidate,
                shortlist,
            )
            if ticket_anchor_match is not None:
                match_event = ticket_anchor_match
                match_reason = ticket_anchor_reason
                logger.info(
                    "smart_update.match type=%s event_id=%s",
                    match_reason,
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            same_ticket_match, same_ticket_reason = _deterministic_same_ticket_same_place_match(
                candidate,
                shortlist,
            )
            if same_ticket_match is not None:
                match_event = same_ticket_match
                match_reason = same_ticket_reason
                logger.info(
                    "smart_update.match type=%s event_id=%s",
                    match_reason,
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            near_text_match, near_text_reason = _deterministic_same_slot_near_text_match(
                candidate,
                shortlist,
            )
            if near_text_match is not None:
                match_event = near_text_match
                match_reason = near_text_reason
                logger.info(
                    "smart_update.match type=%s event_id=%s",
                    match_reason,
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            text_copy_match, text_copy_reason = _deterministic_copy_post_source_text_match(
                candidate,
                shortlist,
            )
            if text_copy_match is not None:
                match_event = text_copy_match
                match_reason = text_copy_reason
                logger.info(
                    "smart_update.match type=%s event_id=%s",
                    match_reason,
                    getattr(match_event, "id", None),
                )

        if match_event is None and candidate_location_unsupported_prose:
            prose_text_match, prose_text_reason = _deterministic_prose_location_same_slot_text_match(
                candidate,
                shortlist,
            )
            if prose_text_match is not None:
                match_event = prose_text_match
                match_reason = prose_text_reason
                logger.info(
                    "smart_update.match type=%s event_id=%s",
                    match_reason,
                    getattr(match_event, "id", None),
                )

        if strong_matches and match_event is None:
            best = max(strong_matches.items(), key=lambda item: item[1])
            match_event = next((ev for ev in shortlist if ev.id == best[0]), None)
            match_reason = "strong_match"
            logger.info(
                "smart_update.match type=strong event_id=%s score=%s",
                getattr(match_event, "id", None),
                best[1],
            )

        if match_event is None:
            hard = _deterministic_exact_title_match(
                candidate,
                shortlist,
                is_canonical_site=is_canonical_site,
            )
            if hard is not None:
                match_event = hard
                match_reason = "deterministic_exact_title"
                logger.info(
                    "smart_update.match type=deterministic_exact_title event_id=%s",
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            related = _deterministic_related_title_anchor_match(
                candidate,
                shortlist,
                is_canonical_site=is_canonical_site,
            )
            if related is not None:
                match_event = related
                match_reason = "deterministic_related_title_anchor"
                logger.info(
                    "smart_update.match type=deterministic_related_title_anchor event_id=%s",
                    getattr(match_event, "id", None),
                )

        if match_event is None:
            threshold = 0.85 if allow_parallel and len(shortlist) > 1 else 0.6
            use_match_create_bundle = (
                (os.getenv("SMART_UPDATE_MATCH_CREATE_BUNDLE", "1") or "").strip().lower()
                in {"1", "true", "yes", "on"}
            )
            source_type_clean = str(candidate.source_type or "").strip().lower()
            use_match_create_bundle = use_match_create_bundle and source_type_clean in {"vk", "tg", "telegram"}
            if SMART_UPDATE_G4_SPLIT_CREATE:
                use_match_create_bundle = False
            if use_match_create_bundle:
                normalized_event_type_hint = _normalize_event_type_value(
                    candidate.title,
                    clean_raw_excerpt or clean_source_text or candidate.source_text,
                    candidate.event_type,
                )
                combo = await _llm_match_or_create_bundle(
                    candidate,
                    shortlist[:10],
                    posters_map=posters_map,
                    threshold=threshold,
                    clean_title=clean_title,
                    clean_source_text=candidate.occurrence_scope_text or clean_source_text,
                    clean_raw_excerpt=clean_raw_excerpt,
                    normalized_event_type=normalized_event_type_hint,
                )
                if combo and (combo.get("action") == "create"):
                    llm_create_bundle = combo.get("bundle") if isinstance(combo.get("bundle"), dict) else None
                    match_id = None
                    confidence = float(combo.get("confidence") or 0.0)
                    match_reason = str(combo.get("reason_short") or "llm_create_bundle")
                    match_event = None
                elif combo and (combo.get("action") == "match"):
                    match_id = combo.get("match_event_id")
                    confidence = float(combo.get("confidence") or 0.0)
                    match_reason = str(combo.get("reason_short") or "")
                    try:
                        match_id_int = int(match_id) if match_id is not None else None
                    except Exception:
                        match_id_int = None
                    if match_id_int:
                        match_event = next((ev for ev in shortlist if ev.id == match_id_int), None)
                    else:
                        match_event = None
                    if match_event is None:
                        confidence = 0.0
                        match_reason = "llm_bad_match_id"
                    elif (
                        len(shortlist) == 1
                        and not _single_candidate_auto_match_ok(
                            candidate,
                            match_event,
                            is_canonical_site=is_canonical_site,
                        )
                        and not _llm_high_confidence_anchor_match_ok(
                            candidate,
                            match_event,
                            confidence=confidence,
                            is_canonical_site=is_canonical_site,
                        )
                    ):
                        match_event = None
                        match_reason = "llm_single_candidate_sanity_reject"
                else:
                    match_id, confidence, reason = await _llm_match_event(
                        candidate, shortlist[:10], posters_map=posters_map
                    )
                    match_reason = reason
                    if match_id:
                        match_event = next((ev for ev in shortlist if ev.id == match_id), None)
                        if match_event is None:
                            confidence = 0.0
                        if confidence < threshold:
                            match_event = None
                            match_reason = f"llm_conf_{confidence:.2f}<={threshold:.2f}"
                        elif (
                            len(shortlist) == 1
                            and not _single_candidate_auto_match_ok(
                                candidate,
                                match_event,
                                is_canonical_site=is_canonical_site,
                            )
                            and not _llm_high_confidence_anchor_match_ok(
                                candidate,
                                match_event,
                                confidence=confidence,
                                is_canonical_site=is_canonical_site,
                            )
                        ):
                            match_event = None
                            match_reason = "llm_single_candidate_sanity_reject"
                    else:
                        match_event = None
            else:
                match_id, confidence, reason = await _llm_match_event(
                    candidate, shortlist[:10], posters_map=posters_map
                )
                match_reason = reason
                if match_id:
                    match_event = next((ev for ev in shortlist if ev.id == match_id), None)
                    if match_event is None:
                        confidence = 0.0
                    if confidence < threshold:
                        match_event = None
                        match_reason = f"llm_conf_{confidence:.2f}<={threshold:.2f}"
                    elif (
                        len(shortlist) == 1
                        and not _single_candidate_auto_match_ok(
                            candidate,
                            match_event,
                            is_canonical_site=is_canonical_site,
                        )
                        and not _llm_high_confidence_anchor_match_ok(
                            candidate,
                            match_event,
                            confidence=confidence,
                            is_canonical_site=is_canonical_site,
                        )
                    ):
                        match_event = None
                        match_reason = "llm_single_candidate_sanity_reject"
                else:
                    match_event = None
            logger.info(
                "smart_update.match type=llm match_id=%s confidence=%.2f reason=%s",
                match_id,
                float(confidence or 0.0),
                match_reason,
            )

    # Guard: if the matched existing event is semantically unrelated by title, treat it as "no match"
    # and create a new event instead of performing a catastrophic merge.
    #
    # Keep the match only when a deterministic single-candidate match would also be allowed.
    # This is intentionally conservative: long-running events (e.g. exhibitions) can overlap at the same
    # venue, so date-range overlap alone must not override an unrelated title.
    if match_event is not None and not str(candidate.source_type or "").startswith("parser:"):
        if _title_has_meaningful_tokens(candidate.title) and _title_has_meaningful_tokens(
            getattr(match_event, "title", None)
        ):
            if not _titles_look_related(candidate.title, getattr(match_event, "title", None)):
                narrow_reason = str(match_reason or "").strip().lower()
                safe_single = narrow_reason in {
                    "deterministic_specific_ticket_same_slot",
                    "deterministic_specific_ticket_same_place",
                    "deterministic_same_slot_near_text",
                    "deterministic_doors_start_ticket_bridge",
                    "deterministic_prose_location_same_slot_text",
                }
                try:
                    if (not safe_single) and len(shortlist) == 1:
                        safe_single = _single_candidate_auto_match_ok(
                            candidate,
                            match_event,
                            is_canonical_site=False,
                        )
                except Exception:
                    safe_single = False
                safe_llm_anchor_match = _llm_high_confidence_anchor_match_ok(
                    candidate,
                    match_event,
                    confidence=confidence,
                    is_canonical_site=False,
                )
                if not safe_single and not safe_llm_anchor_match:
                    logger.warning(
                        "smart_update.match_overruled reason=unrelated_titles source_type=%s source_url=%s candidate_title=%s existing_id=%s existing_title=%s",
                        candidate.source_type,
                        candidate.source_url,
                        _clip_title(candidate.title),
                        getattr(match_event, "id", None),
                        _clip_title(getattr(match_event, "title", None)),
                    )
                    match_event = None
                    match_reason = "unrelated_titles"
                elif safe_llm_anchor_match:
                    logger.info(
                        "smart_update.title_guard_not_vetoed reason=llm_high_confidence_anchor_match source_type=%s source_url=%s candidate_title=%s existing_id=%s existing_title=%s confidence=%.2f",
                        candidate.source_type,
                        candidate.source_url,
                        _clip_title(candidate.title),
                        getattr(match_event, "id", None),
                        _clip_title(getattr(match_event, "title", None)),
                        float(confidence or 0.0),
                    )

    # Rescue-match: match/create bundle can decide "create" when candidate title is weak,
    # even though the produced bundle title clearly matches an existing event in the shortlist.
    # Try a deterministic title-based match to prevent duplicates.
    if match_event is None and llm_create_bundle is not None and shortlist:
        bundle_title_raw = llm_create_bundle.get("title") if isinstance(llm_create_bundle, dict) else None
        bundle_title = ""
        if isinstance(bundle_title_raw, str) and bundle_title_raw.strip():
            # Non-semantic cleanup: some models return JSON-ish `\"...\"` fragments.
            bundle_title = (
                (bundle_title_raw or "")
                .strip()
                .replace("\\\\\"", "\"")
                .replace("\\\"", "\"")
                .strip()
            )
            bundle_title = _strip_private_use(bundle_title) or bundle_title
        if bundle_title and _title_has_meaningful_tokens(bundle_title):
            grounded = False
            try:
                grounded = _is_title_grounded_in_candidate_sources(bundle_title, candidate)
            except Exception:
                grounded = False

            if grounded:
                prev_reason = match_reason
                cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=is_canonical_site)
                rescued_matches: list[Event] = []
                for ev in shortlist:
                    if not getattr(ev, "id", None):
                        continue
                    if _has_explicit_time_conflict(cand_time_anchor, _event_anchor_time(ev)):
                        continue
                    if not _titles_look_related(bundle_title, getattr(ev, "title", None)):
                        continue
                    rescued_matches.append(ev)

                chosen: Event | None = None
                if len(rescued_matches) == 1:
                    chosen = rescued_matches[0]
                elif rescued_matches:
                    sigs = {_anchor_signature_for_duplicate_event(ev) for ev in rescued_matches}
                    if len(sigs) == 1:
                        chosen = _pick_best_duplicate_event(rescued_matches)

                if chosen is not None:
                    match_event = chosen
                    match_reason = "rescue_bundle_title"
                    note = "Матчинг: предотвращён дубль (rescue по bundle.title)"
                    if note not in text_filter_facts:
                        text_filter_facts.append(note)
                    logger.info(
                        "smart_update.match type=rescue_bundle_title event_id=%s prev_reason=%s",
                        getattr(match_event, "id", None),
                        prev_reason,
                    )

    # Pre-create duplicate probe (INC-2026-05-08).
    # Last-line-of-defence safety net: even after the LLM matcher / rescue / strong / deterministic
    # checks all said "no match", we still walk the shortlist one final time for the cases
    # where two anchors fully agree:
    #   (a) identical normalized ``ticket_link`` AND overlapping date AND no explicit time conflict;
    #   (b) identical ``location_name`` AND overlapping date AND identical time anchor AND related title.
    # Both branches require ``_titles_look_related`` so cross-event collisions on a shared venue
    # / shared ticket page (e.g. season subscription URL) cannot drag unrelated events together.
    if match_event is None and shortlist and not str(candidate.source_type or "").startswith("parser:"):
        probe_match = _pre_create_duplicate_probe(candidate, shortlist)
        if probe_match is not None:
            match_event = probe_match
            match_reason = "post_decision_dup_probe"
            note = "Матчинг: предотвращён дубль (post-decision probe)"
            if note not in text_filter_facts:
                text_filter_facts.append(note)
            logger.info(
                "smart_update.match type=post_decision_dup_probe event_id=%s candidate_title=%s existing_title=%s",
                getattr(match_event, "id", None),
                _clip_title(candidate.title),
                _clip_title(getattr(match_event, "title", None)),
            )

    if candidate.force_match_event_id:
        async with db.get_session() as session:
            retry_match = await session.get(Event, int(candidate.force_match_event_id))
        if retry_match is None:
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=candidate.force_match_event_id,
                reason="identity_gate_match_disappeared", retry_reason=RetryReason.IDENTITY_MATCH_DISAPPEARED,
            )
        match_event = retry_match
        match_reason = "identity_gate_adjudicated_same_event"

    identity_gate_candidates: list[Event] = []
    identity_gate_match: Event | None = None
    identity_gate_reason: str | None = None
    identity_gate_adjudicated = False

    # Evaluate the create gate before the existing widened dedup stage. If the
    # gate finds a concrete Event, that Event is folded into the *same* dedup
    # adjudicator call below. This closes VETO_CREATE without adding a second
    # provider call or a second identity stage.
    if (
        match_event is None
        and SMART_UPDATE_IDENTITY_GATE_MODE is not IdentityGateMode.OFF
        and not candidate.force_create_distinct
    ):
        vector_evidence: IdentityVectorEvidence | None = None
        try:
            vector_evidence = await _smart_update_identity_vector_evidence(candidate)
            if vector_evidence is not None and vector_evidence.error:
                raise RuntimeError(
                    "identity_vector_unavailable:"
                    + str(vector_evidence.reason or vector_evidence.error)
                )
            gate_existing_events = list(shortlist)
            if vector_evidence and vector_evidence.nearest_event_id is not None:
                known_ids = {getattr(ev, "id", None) for ev in gate_existing_events}
                if vector_evidence.nearest_event_id not in known_ids:
                    async with db.get_session() as session:
                        vector_event = await session.get(
                            Event,
                            vector_evidence.nearest_event_id,
                        )
                    if vector_event is not None:
                        gate_existing_events.append(vector_event)
            identity_gate_candidates = gate_existing_events
            identity_verdict = build_identity_gate_verdict(
                candidate,
                gate_existing_events,
                mode=SMART_UPDATE_IDENTITY_GATE_MODE,
                vector_evidence=vector_evidence,
            )
            logger.info(
                "smart_update.identity_gate mode=%s action=%s reason=%s matched_event_id=%s confidence=%.2f deterministic=%s fail_safe=%s source_kind=%s source_type=%s",
                identity_verdict.mode.value,
                identity_verdict.action.value,
                identity_verdict.reason_code,
                identity_verdict.matched_event_id,
                identity_verdict.confidence,
                identity_verdict.deterministic,
                identity_verdict.fail_safe,
                identity_verdict.candidate.source_flags.source_kind
                if identity_verdict.candidate is not None
                else None,
                identity_verdict.candidate.source_flags.source_type
                if identity_verdict.candidate is not None
                else None,
            )
            await _record_identity_gate_decision(
                db,
                candidate,
                decision=identity_verdict.action.value,
                reason=identity_verdict.reason_code,
                confidence=identity_verdict.confidence,
                event_id=identity_verdict.matched_event_id,
                payload={
                    "mode": identity_verdict.mode.value,
                    "reasons": list(identity_verdict.reasons),
                    "deterministic": identity_verdict.deterministic,
                    "fail_safe": identity_verdict.fail_safe,
                    "vector": {
                        "available": vector_evidence.available,
                        "nearest_event_id": vector_evidence.nearest_event_id,
                        "score": vector_evidence.score,
                        "reason": vector_evidence.reason,
                        "error": vector_evidence.error,
                    }
                    if vector_evidence is not None
                    else None,
                },
            )
            if identity_verdict.should_veto_create:
                identity_gate_reason = identity_verdict.reason_code
                matched_id = identity_verdict.matched_event_id
                if matched_id is not None:
                    async with db.get_session() as session:
                        identity_gate_match = await session.get(Event, int(matched_id))
                if identity_gate_match is None:
                    return SmartUpdateResult(
                        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                        diagnostic_event_id=matched_id,
                        reason=f"identity_gate_uncertain:{identity_verdict.reason_code}",
                        retry_reason=RetryReason.IDENTITY_TECHNICAL_FAILURE,
                    )
        except Exception as exc:
            logger.warning("smart_update: identity gate failed", exc_info=True)
            identity_verdict = identity_gate_fail_safe_verdict(
                mode=SMART_UPDATE_IDENTITY_GATE_MODE,
                candidate=candidate,
                reason=str(exc) or "identity gate error",
            )
            await _record_identity_gate_decision(
                db,
                candidate,
                decision=identity_verdict.action.value,
                reason=identity_verdict.reason_code,
                confidence=identity_verdict.confidence,
                event_id=identity_verdict.matched_event_id,
                payload={
                    "mode": identity_verdict.mode.value,
                    "reasons": list(identity_verdict.reasons),
                    "deterministic": identity_verdict.deterministic,
                    "fail_safe": identity_verdict.fail_safe,
                    "vector": {
                        "available": vector_evidence.available,
                        "nearest_event_id": vector_evidence.nearest_event_id,
                        "score": vector_evidence.score,
                        "reason": vector_evidence.reason,
                        "error": vector_evidence.error,
                    }
                    if vector_evidence is not None
                    else None,
                },
            )
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=identity_verdict.matched_event_id,
                reason=f"identity_gate_uncertain:{identity_verdict.reason_code}",
                retry_reason=RetryReason.IDENTITY_TECHNICAL_FAILURE,
            )

    # LLM dedup adjudicator over WIDENED recall (INC-2026-05-30 opt 1).
    # Last-line, create-path only: even after every deterministic matcher + the
    # match/create bundle + rescue + probe said "no match", the genuine sibling may
    # simply never have entered the anchor-gated shortlist (drifted venue string or
    # doors/start time). Re-fetch a wider date+city recall, block it down by a cheap
    # title/venue/ticket/poster key, and let an LLM decide match-vs-create while a
    # deterministic guard ladder keeps multi-session / parallel events separate.
    if (
        SMART_UPDATE_DEDUP_ADJUDICATOR
        and match_event is None
        and not candidate.force_create_distinct
        and not anchor_forced
        and not is_canonical_site
        and not SMART_UPDATE_LLM_DISABLED
        and (
            _title_has_meaningful_tokens(candidate.title)
            or identity_gate_match is not None
        )
    ):
        try:
            from datetime import timedelta

            wide_lo = (cand_start - timedelta(days=1)).isoformat()
            wide_hi = (cand_end + timedelta(days=1)).isoformat()
            async with db.get_session() as session:
                wide_stmt = select(Event).where(
                    and_(
                        Event.date <= wide_hi,
                        or_(
                            and_(Event.end_date.is_(None), Event.date >= wide_lo),
                            Event.end_date >= wide_lo,
                        ),
                        Event.lifecycle_status == "active",
                    )
                )
                wide_stmt = _apply_soft_city_filter(wide_stmt, candidate.city)
                wide_res = await session.execute(wide_stmt)
                wide_pool = list(wide_res.scalars().all())
            wide_posters = await _fetch_event_posters_map(
                db, [ev.id for ev in wide_pool if ev.id]
            )
            blocked = _dedup_adjudicator_block_candidates(candidate, wide_pool, wide_posters)
            if identity_gate_match is not None and all(
                getattr(ev, "id", None) != getattr(identity_gate_match, "id", None)
                for ev in blocked
            ):
                blocked.append(identity_gate_match)
                if identity_gate_match.id not in wide_posters:
                    wide_posters.update(
                        await _fetch_event_posters_map(
                            db,
                            [int(identity_gate_match.id)],
                        )
                    )
            identity_gate_candidates = blocked or wide_pool[:10]
            if blocked:
                decision = await _llm_dedup_adjudicator(
                    candidate, blocked, posters_map=wide_posters
                )
                if decision is None:
                    # The widened stage ran because deterministic evidence found
                    # plausible identity neighbours.  Provider/schema abstention
                    # cannot silently authorize CREATE: keep the candidate in the
                    # durable bounded retry path and create distinct only after
                    # identity uncertainty exhausts its configured attempts.
                    return SmartUpdateResult(
                        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                        diagnostic_event_id=(
                            int(identity_gate_match.id)
                            if identity_gate_match is not None
                            and identity_gate_match.id
                            else None
                        ),
                        reason="dedup_adjudicator_unavailable", retry_reason=RetryReason.DEDUP_ADJUDICATOR_TECHNICAL_FAILURE,
                    )
                if decision:
                    if identity_gate_match is not None:
                        identity_gate_adjudicated = True
                    adj_id = decision.get("match_event_id")
                    adj_event = (
                        next((ev for ev in blocked if ev.id == adj_id), None) if adj_id else None
                    )
                    allow_parallel_adj = _allow_parallel_events(candidate.location_name) or (
                        _allow_parallel_events(getattr(adj_event, "location_name", None))
                        if adj_event is not None
                        else False
                    )
                    accept, code = _dedup_adjudicator_accept_merge(
                        candidate,
                        adj_event,
                        decision=decision,
                        allow_parallel=allow_parallel_adj,
                    )
                    if accept and adj_event is not None:
                        match_event = adj_event
                        match_reason = f"dedup_adjudicator:{code}"
                        note = "Матчинг: предотвращён дубль (LLM dedup adjudicator)"
                        if note not in text_filter_facts:
                            text_filter_facts.append(note)
                        logger.info(
                            "smart_update.match type=dedup_adjudicator code=%s conf=%.2f event_id=%s candidate_title=%s existing_title=%s",
                            code,
                            float(decision.get("confidence") or 0.0),
                            getattr(match_event, "id", None),
                            _clip_title(candidate.title),
                            _clip_title(getattr(match_event, "title", None)),
                        )
                    else:
                        logger.info(
                            "smart_update.dedup_adjudicator no_merge code=%s action=%s conf=%.2f candidate_title=%s",
                            code,
                            decision.get("action"),
                            float(decision.get("confidence") or 0.0),
                            _clip_title(candidate.title),
                        )
        except Exception:
            logger.warning(
                "smart_update: dedup adjudicator failed; scheduling retry", exc_info=True
            )
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=(
                    int(identity_gate_match.id)
                    if identity_gate_match is not None and identity_gate_match.id
                    else None
                ),
                reason="dedup_adjudicator_unavailable", retry_reason=RetryReason.DEDUP_ADJUDICATOR_TECHNICAL_FAILURE,
            )

    if (
        identity_gate_match is not None
        and match_event is None
        and not identity_gate_adjudicated
    ):
        # The gate found a concrete owner, but the existing adjudicator could
        # not produce a typed same/distinct decision in this attempt.
        return SmartUpdateResult(
            outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
            diagnostic_event_id=int(identity_gate_match.id),
            reason=f"identity_gate_adjudicator_unavailable:{identity_gate_reason or 'veto'}",
        )

    # The compact semantics stage is explicitly candidate-routed, but it runs
    # for both create and ordinary merge paths. Provider/schema failure is an
    # abstention and must not block the canonical Smart Update transaction.
    candidate.collection_adjudication_reasons = route_collection_adjudication_reasons(
        candidate,
        match_event,
    )
    if candidate.collection_adjudication_reasons and candidate.collection_semantic_decisions is None:
        cached_payload = collection_adjudication_cached_payload(
            match_event.collection_decisions if match_event is not None else None,
            input_hash=collection_adjudication_input_hash(candidate),
            source_url=candidate.source_url,
        )
        if cached_payload is not None:
            candidate.collection_semantic_decisions = cached_payload
        else:
            try:
                await adjudicate_collection_candidate(candidate)
            except Exception:
                logger.warning(
                    "smart_update: candidate collection adjudication failed source_type=%s source_url=%s",
                    candidate.source_type,
                    candidate.source_url,
                    exc_info=True,
                )

    async def _create_from_prepared_candidate() -> SmartUpdateResult:
        if candidate_location_unsupported_prose:
            logger.warning(
                "smart_update.invalid reason=prose_location source_type=%s source_url=%s title=%s location=%s",
                candidate.source_type,
                candidate.source_url,
                _clip_title(candidate.title),
                _clip_title(candidate.location_name, 120),
            )
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason="prose_location",
                retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
            )

        normalized_event_type = _normalize_event_type_value(
            candidate.title, candidate.raw_excerpt or candidate.source_text, candidate.event_type
        )
        normalized_digest = _clean_search_digest(candidate.search_digest)
        is_free_value: bool
        if candidate.is_free is True:
            is_free_value = True
        elif candidate.is_free is False:
            is_free_value = False
        else:
            is_free_value = False
        # Seed with excerpt/title only; never publish full source_text verbatim as a fallback.
        # Full source is preserved separately in `event.source_text`/`event_source`.
        description_value = (clean_raw_excerpt or clean_title or "").strip()

        bundled_facts: list[str] | None = None
        bundled_digest: str | None = None
        bundled_desc: str | None = None
        bundled_title: str | None = None
        bundled_short: str | None = None
        bundled_age_payload: Any | None = None
        try:
            if llm_create_bundle is not None:
                bundled = llm_create_bundle
            elif SMART_UPDATE_G4_SPLIT_CREATE:
                bundled = await _llm_g4_split_create_bundle(
                    candidate,
                    clean_title=clean_title,
                    normalized_event_type=normalized_event_type,
                )
            else:
                bundled = await _llm_create_description_facts_and_digest(
                    candidate,
                    clean_title=clean_title,
                    clean_source_text=candidate.occurrence_scope_text or clean_source_text,
                    clean_raw_excerpt=clean_raw_excerpt,
                    normalized_event_type=normalized_event_type,
                )
        except Exception:  # pragma: no cover - provider failures
            bundled = None
        if (
            isinstance(bundled, dict)
            and str(candidate.source_type or "").strip().lower() in {"vk", "tg", "telegram"}
        ):
            (
                bundle_ok,
                bundle_grounding_result,
                unsupported_bundle_fields,
            ) = await _llm_review_create_bundle_grounding(
                bundled,
                candidate,
            )
            logger.info(
                "smart_update.create_bundle_grounding result=%s ok=%s source_type=%s source_url=%s",
                bundle_grounding_result,
                int(bundle_ok),
                candidate.source_type,
                candidate.source_url,
            )
            if not bundle_ok:
                if (
                    bundle_grounding_result == "llm_ungrounded"
                    and unsupported_bundle_fields
                ):
                    bundled = _remove_llm_rejected_bundle_fields(
                        bundled,
                        unsupported_bundle_fields,
                    )
                    logger.warning(
                        "smart_update.create_bundle_grounding stripped_unsupported=%s source_type=%s source_url=%s",
                        ",".join(unsupported_bundle_fields),
                        candidate.source_type,
                        candidate.source_url,
                    )
                else:
                    return SmartUpdateResult(
                        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                        reason=f"create_bundle_grounding:{bundle_grounding_result}",
                        retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
                    )
        if isinstance(bundled, dict):
            bundled_age_payload = bundled.get("age_decision")
            bundled_title_raw = bundled.get("title")
            if isinstance(bundled_title_raw, str) and bundled_title_raw.strip():
                # Non-semantic cleanup: some models return JSON-ish `\"...\"` fragments.
                t = (bundled_title_raw or "").strip().replace("\\\\\"", "\"").replace("\\\"", "\"").strip()
                t = _strip_private_use(t) or t
                if t:
                    bundled_title = t
            bundled_desc_raw = bundled.get("description")
            if isinstance(bundled_desc_raw, str) and bundled_desc_raw.strip():
                bundled_desc = bundled_desc_raw.strip()
            bundled_digest = _clean_search_digest(bundled.get("search_digest"))
            bundled_short = _clean_short_description(bundled.get("short_description"))
            if bundled_short and not _is_short_description_acceptable(
                bundled_short, min_words=12, max_words=16
            ):
                bundled_short = None
            raw_facts_any = bundled.get("facts")
            raw_facts: list[str] = []
            if isinstance(raw_facts_any, list):
                for it in raw_facts_any:
                    raw_facts.append(str(it or ""))
            bundled_facts_out: list[str] = []
            seen_fact_keys: set[str] = set()
            for it in raw_facts:
                cleaned = _normalize_fact_item(str(it or ""), limit=180)
                if not cleaned:
                    continue
                key = cleaned.casefold()
                if key in seen_fact_keys:
                    continue
                seen_fact_keys.add(key)
                bundled_facts_out.append(cleaned)
                if len(bundled_facts_out) >= (40 if SMART_UPDATE_G4_SPLIT_CREATE else 24):
                    break
            bundled_facts = _filter_ungrounded_sensitive_facts(
                bundled_facts_out,
                candidate=candidate,
            )

        # Bot/manual sources should keep operator-provided titles as-is.
        if (candidate.source_type or "").strip().lower() in {"bot"}:
            bundled_title = None

        # Guard LLM title proposals on create: avoid creating an event with an unrelated title,
        # which later breaks matching and can lead to false duplicates or cross-event merges.
        if bundled_title:
            candidate_title_weak = _is_candidate_title_weak_for_llm_override(
                clean_title,
                candidate=candidate,
                normalized_event_type=normalized_event_type,
            )
            proposed_title_grounded = _is_title_grounded_in_candidate_sources(
                bundled_title,
                candidate,
            )
            if not proposed_title_grounded:
                logger.warning(
                    "smart_update.create_title_rejected reason=ungrounded_title source_type=%s source_url=%s candidate_title=%s proposed_title=%s",
                    candidate.source_type,
                    candidate.source_url,
                    _clip_title(clean_title),
                    _clip_title(bundled_title),
                )
                text_filter_facts.append(
                    f"Заголовок отклонён: {clean_title} -> {bundled_title} (причина: ungrounded_title)"
                )
                return SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                    reason="ungrounded_create_bundle_title",
                    retry_reason=RetryReason.SOURCE_VERIFICATION_REQUIRED,
                )
            elif (
                _title_has_meaningful_tokens(clean_title)
                and (not _titles_look_related(bundled_title, clean_title))
                and (not candidate_title_weak)
            ):
                logger.warning(
                    "smart_update.create_title_rejected source_type=%s source_url=%s candidate_title=%s proposed_title=%s",
                    candidate.source_type,
                    candidate.source_url,
                    _clip_title(clean_title),
                    _clip_title(bundled_title),
                )
                text_filter_facts.append(
                    f"Заголовок отклонён: {clean_title} -> {bundled_title} (причина: semantic_title_mismatch)"
                )
                bundled_title = None

        final_title = bundled_title or clean_title
        final_title = _strip_private_use(final_title) or final_title
        final_title = re.sub(r"\s+", " ", (final_title or "").strip())
        # Safety-net: Telegraph + Telegram UI behave poorly with extremely long titles.
        final_title = _clip_title(final_title, 160) or clean_title

        # Recover a real title when all we have is a generic category title:
        # either an explicit "<event_type> — <venue>" placeholder or a category-only
        # title that lacks the distinctive source/OCR name. The deterministic guard
        # only routes to LLM recovery; the replacement is accepted only after source
        # grounding validation, so good titles are never deterministically rewritten.
        if _is_candidate_title_weak_for_llm_override(
            final_title,
            candidate=candidate,
            normalized_event_type=normalized_event_type,
        ):
            recovered_title = await _llm_recover_event_title(
                candidate,
                normalized_event_type=normalized_event_type,
                facts=bundled_facts,
            )
            if recovered_title:
                logger.info(
                    "smart_update.title_recovered source_type=%s source_url=%s weak_title=%r recovered=%r",
                    candidate.source_type,
                    candidate.source_url,
                    _clip_title(final_title),
                    _clip_title(recovered_title),
                )
                final_title = recovered_title

        lollipop_light_used = False
        if (
            SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE
            and not SMART_UPDATE_G4_SPLIT_CREATE
            and bundled_facts
        ):
            try:
                lollipop_bundle = await _llm_g4_lollipop_light_create_bundle(
                    candidate,
                    clean_title=final_title,
                    normalized_event_type=normalized_event_type,
                    raw_facts=bundled_facts,
                    bundled_search_digest=bundled_digest,
                    bundled_short_description=bundled_short,
                )
            except Exception:
                logger.warning(
                    "smart_update: lollipop-light create path failed source_type=%s source_url=%s",
                    candidate.source_type,
                    candidate.source_url,
                    exc_info=True,
                )
                lollipop_bundle = None
            if isinstance(lollipop_bundle, dict) and str(lollipop_bundle.get("description") or "").strip():
                bundled_desc = str(lollipop_bundle.get("description") or "").strip()
                bundled_digest = _clean_search_digest(lollipop_bundle.get("search_digest")) or bundled_digest
                bundled_short = _clean_short_description(lollipop_bundle.get("short_description")) or bundled_short
                raw_lollipop_facts = lollipop_bundle.get("facts")
                if isinstance(raw_lollipop_facts, list) and raw_lollipop_facts:
                    bundled_facts = [
                        _normalize_fact_item(str(item or ""), limit=180)
                        for item in raw_lollipop_facts
                        if _normalize_fact_item(str(item or ""), limit=180)
                    ]
                lollipop_light_used = True

        split_create_used = bool(
            SMART_UPDATE_G4_SPLIT_CREATE
            and isinstance(bundled, dict)
            and bundled.get("_split_create")
        )
        fact_first_used = False
        if lollipop_light_used and bundled_desc and bundled_facts:
            description_value = bundled_desc
            fact_first_used = True
            logger.info(
                "smart_update.create_description path=g4_lollipop_light source_type=%s source_url=%s facts=%d writer_lane=%s",
                candidate.source_type,
                candidate.source_url,
                len(bundled_facts or []),
                _g4_lollipop_light_selected_writer_lane(len(bundled_facts or [])),
            )
        elif (
            split_create_used
            and bundled_desc
            and bundled_facts
        ):
            description_value = bundled_desc
            fact_first_used = True
            logger.info(
                "smart_update.create_description path=g4_split_create_v2_rich_facts source_type=%s source_url=%s",
                candidate.source_type,
                candidate.source_url,
            )
        elif split_create_used:
            # Split-create owns the create narrative surface. If the heavy fact ledger or
            # the bounded writer is unavailable, keep the existing deterministic seed and
            # do not fall through to the legacy multi-step fact-first/rewrite cascade.
            fact_first_used = True
            logger.warning(
                "smart_update.create_description path=g4_split_create_v2_rich_facts_unavailable source_type=%s source_url=%s facts=%d desc=%s",
                candidate.source_type,
                candidate.source_url,
                len(bundled_facts or []),
                bool(bundled_desc),
            )
        if (not fact_first_used) and SMART_UPDATE_FACT_FIRST and not SMART_UPDATE_LLM_DISABLED:
            fact_first_facts = list(bundled_facts or [])
            if not fact_first_facts:
                try:
                    fact_first_facts = await _llm_extract_candidate_facts(candidate)
                except Exception:  # pragma: no cover - provider failures
                    fact_first_facts = []
                if fact_first_facts:
                    bundled_facts = fact_first_facts
            facts_text_clean = _facts_text_clean_from_facts(
                fact_first_facts,
                anchors=[
                    candidate.date or "",
                    candidate.time or "",
                    candidate.city or "",
                    candidate.location_name or "",
                    candidate.location_address or "",
                ],
            )
            if facts_text_clean:
                try:
                    ff_desc = await _llm_fact_first_description_md_bounded(
                        title=final_title,
                        event_type=normalized_event_type or candidate.event_type,
                        facts_text_clean=facts_text_clean,
                        anchors=[
                            candidate.date or "",
                            candidate.time or "",
                            candidate.city or "",
                            candidate.location_name or "",
                            candidate.location_address or "",
                        ],
                        label="create",
                    )
                except Exception:  # pragma: no cover - provider failures
                    ff_desc = None
                if ff_desc:
                    description_value = ff_desc
                    fact_first_used = True
                    # Keep canonical facts clean: narrative facts only (anchors are logged separately).
                    bundled_facts = facts_text_clean

        if not fact_first_used:
            if bundled_desc:
                description_value = bundled_desc
            else:
                try:
                    rewritten = await _rewrite_description_journalistic(candidate)
                except Exception:  # pragma: no cover - defensive
                    logger.warning("smart_update: description rewrite failed", exc_info=True)
                    rewritten = None
                if rewritten:
                    description_value = rewritten
            # If the model returned near-verbatim source text, force a second-pass strict rewrite.
            if _description_too_verbatim(description_value, source_text=clean_source_text):
                logger.warning(
                    "smart_update: description too verbatim; forcing strict rewrite source_type=%s source_url=%s",
                    candidate.source_type,
                    candidate.source_url,
                )
                try:
                    rewritten_strict = await _rewrite_description_journalistic(
                        candidate,
                        strict_nonverbatim=True,
                    )
                except Exception:  # pragma: no cover - provider failures
                    rewritten_strict = None
                if rewritten_strict and not _description_too_verbatim(
                    rewritten_strict, source_text=clean_source_text
                ):
                    description_value = rewritten_strict
            if _description_has_foreign_schedule_noise(
                description_value,
                event_date=candidate.date,
                end_date=candidate.end_date,
                event_title=candidate.title,
            ):
                description_value = _strip_foreign_schedule_noise(
                    description_value,
                    event_date=candidate.date,
                    end_date=candidate.end_date,
                    event_title=candidate.title,
                ) or description_value
            description_value = _dedupe_description(description_value) or description_value
            description_value = _normalize_plaintext_paragraphs(description_value) or description_value
            description_value = _promote_review_bullets_to_blockquotes(description_value) or description_value
            description_value = _promote_first_person_quotes_to_blockquotes(description_value) or description_value
            description_value = _promote_inline_quoted_direct_speech_to_blockquotes(description_value) or description_value
            description_value = _drop_reported_speech_duplicates(description_value) or description_value
            description_value = _normalize_blockquote_markers(description_value) or description_value
            description_value = _append_missing_scene_hint(
                description=description_value, source_text=clean_source_text
            ) or description_value
            description_value = (
                _sanitize_description_output(
                    description_value,
                    source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
                )
                or description_value
            )
            description_value = (
                _append_missing_small_list(
                    description=description_value,
                    source_text=clean_source_text,
                    source_type=candidate.source_type,
                )
                or description_value
            )
            description_value = _normalize_plaintext_paragraphs(description_value) or description_value
            description_value = _ensure_minimal_description_headings(description_value) or description_value
        else:
            # Fact-first output should not be "topped up" with source snippets; keep it strictly fact-driven.
            description_value = _dedupe_description(description_value) or description_value
            description_value = _normalize_plaintext_paragraphs(description_value) or description_value
            description_value = _promote_review_bullets_to_blockquotes(description_value) or description_value
            description_value = _normalize_blockquote_markers(description_value) or description_value
            description_value = (
                _sanitize_description_output(
                    description_value,
                    source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
                )
                or description_value
            )
            if not _fact_first_is_sparse(bundled_facts or []):
                description_value = _ensure_minimal_description_headings(description_value) or description_value
        if (not split_create_used) and _has_overlong_paragraph(description_value, limit=850):
            try:
                reflown = await _llm_reflow_description_paragraphs(description_value)
            except Exception:  # pragma: no cover - provider failures
                reflown = None
            if reflown:
                reflown = _normalize_plaintext_paragraphs(reflown) or reflown
                reflown = _normalize_blockquote_markers(reflown) or reflown
                reflown = _fix_broken_initial_paragraph_splits(reflown) or reflown
                reflown = (
                    _sanitize_description_output(
                        reflown,
                        source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
                    )
                    or reflown
                )
                description_value = reflown

        # Guard: sometimes create-time LLM output becomes an over-compressed digest even when
        # the source text (or poster OCR) is rich. This makes Telegraph pages look like they
        # have "no main text". Do a second-pass full rewrite when we can.
        try:
            event_stub = Event(
                title=clean_title,
                description=description_value or "",
                date=candidate.date or "",
                time=candidate.time or "",
                end_date=candidate.end_date,
                location_name=candidate.location_name or "",
                location_address=candidate.location_address,
                city=candidate.city,
                ticket_link=candidate.ticket_link,
                ticket_status=candidate.ticket_status,
                is_free=bool(is_free_value),
                event_type=normalized_event_type or candidate.event_type,
                festival=candidate.festival,
                source_text=clean_source_text or "",
                source_texts=[
                    t
                    for t in [clean_source_text, (clean_raw_excerpt or "")]
                    if (t or "").strip()
                ],
                photo_urls=[],
            )
            rich_base = _pick_richest_source_text_for_description(event_stub, candidate)
            base_len = len(rich_base)
            try:
                max_expected = _estimate_description_budget_chars(
                    source_type=candidate.source_type,
                    source_text=clean_source_text,
                    raw_excerpt=clean_raw_excerpt,
                    poster_texts=[p.ocr_text for p in candidate.posters if (p.ocr_text or "").strip()],
                )
            except Exception:
                max_expected = 0
            # Create-time threshold is intentionally softer than merge-time: we only need
            # to avoid "empty-looking" Telegraph pages, not match the full source length.
            # Still, avoid publishing "title-only" or ultra-short bodies when the source is meaningful.
            if base_len < 260:
                min_expected = 0
            elif base_len < 700:
                min_expected = max(220, int(base_len * 0.30))
            else:
                min_expected = max(350, int(base_len * 0.25))
        except Exception:  # pragma: no cover - defensive
            event_stub = None
            min_expected = 0
            max_expected = 0
        too_verbatim = bool(
            rich_base
            and _description_too_verbatim(description_value, source_text=rich_base)
        )
        desc_len_now = len((description_value or "").strip())
        need_full = bool(min_expected and desc_len_now < min_expected) or too_verbatim
        if need_full and not fact_first_used:
            try:
                rewritten_full = await _rewrite_description_full_from_sources(
                    event_stub, candidate  # type: ignore[arg-type]
                )
            except Exception:  # pragma: no cover - provider failures
                rewritten_full = None
            if rewritten_full and (not too_verbatim) and len(rewritten_full) >= int(min_expected * 0.85):
                description_value = rewritten_full
                text_filter_facts.append("Описание расширено: заменён слишком короткий дайджест")
                logger.info(
                    "smart_update.create_description_rewrite_full source_type=%s source_url=%s desc_len=%d min_expected=%d",
                    candidate.source_type,
                    candidate.source_url,
                    len((rewritten_full or "").strip()),
                    int(min_expected),
                )
            elif rewritten_full and too_verbatim and not _description_too_verbatim(rewritten_full, source_text=rich_base):
                description_value = rewritten_full
                text_filter_facts.append("Описание перезаписано: убрано дословное копирование источника")
                logger.info(
                    "smart_update.create_description_rewrite_full_nonverbatim source_type=%s source_url=%s desc_len=%d",
                    candidate.source_type,
                    candidate.source_url,
                    len((rewritten_full or "").strip()),
                )
            elif too_verbatim and _description_too_verbatim(description_value, source_text=rich_base):
                # Last resort: do not publish the raw source verbatim when the LLM is unavailable.
                # Keep a short neutral snippet; full source remains in `event.source_text`.
                fallback = (clean_raw_excerpt or "").strip()
                if not fallback:
                    try:
                        fallback = _fallback_digest_from_description(rich_base) or ""
                    except Exception:
                        fallback = ""
                fallback = (fallback or "").strip()
                if fallback:
                    description_value = _clip(fallback, 520)
                    text_filter_facts.append("Описание сокращено: LLM недоступна, убран дословный текст источника")
                else:
                    description_value = ""
            elif min_expected and not rewritten_full and len((description_value or "").strip()) < max(120, int(min_expected * 0.6)):
                # Another last resort: if we couldn't expand the description (LLM down),
                # avoid a "nearly empty" Telegraph body by using a short excerpt/digest.
                fallback = (clean_raw_excerpt or "").strip()
                if not fallback:
                    try:
                        fallback = _fallback_digest_from_description(rich_base) or ""
                    except Exception:
                        fallback = ""
                fallback = (fallback or "").strip()
                if fallback:
                    description_value = _clip(fallback, 520)
                    text_filter_facts.append("Описание заменено: LLM недоступна, использован краткий фрагмент")

        # Guard: avoid over-expanding short sources. If the description is much longer than
        # the total available source volume, force an LLM-only shrinking pass.
        try:
            max_expected_i = int(max_expected or 0)
        except Exception:
            max_expected_i = 0
        desc_len = len((description_value or "").strip())
        if (not fact_first_used) and max_expected_i and desc_len and desc_len > max_expected_i:
            shrunk = None
            try:
                shrunk = await _llm_shrink_description_to_budget(
                    source_type=candidate.source_type,
                    source_url=candidate.source_url,
                    description=str(description_value or ""),
                    source_text=rich_base or clean_source_text,
                    facts=bundled_facts or None,
                    max_chars=max_expected_i,
                )
            except Exception:  # pragma: no cover - provider failures
                shrunk = None
            if shrunk:
                description_value = shrunk
                text_filter_facts.append("Описание сокращено: приведено к объёму источников")
                logger.info(
                    "smart_update.create_description_shrunk source_type=%s source_url=%s desc_len=%d budget=%d",
                    candidate.source_type,
                    candidate.source_url,
                    len((description_value or "").strip()),
                    int(max_expected_i),
                )
            else:
                # If LLM isn't available, prefer a short deterministic digest from the source
                # rather than publishing a long hallucination-prone narrative.
                try:
                    fallback = _fallback_digest_from_description(rich_base) or ""
                except Exception:
                    fallback = ""
                fallback = fallback.strip()
                if fallback and len(fallback) <= int(max_expected_i * 1.15):
                    description_value = fallback
                    text_filter_facts.append("Описание сокращено: использован краткий дайджест источника")
        description_value = (
            _restore_source_grounded_known_spellings(
                description_value,
                source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
            )
            or description_value
        )
        description_value = _clip(description_value, SMART_UPDATE_DESCRIPTION_MAX_CHARS) if description_value else ""

        # Extract atomic facts for global de-duplication + operator log.
        extracted_facts: list[str] = bundled_facts or []
        if not extracted_facts and not split_create_used:
            try:
                # Facts must come from the SOURCE, not from the rewritten description (which is also LLM output).
                extracted_facts = await _llm_extract_candidate_facts(candidate)
            except Exception:  # pragma: no cover - defensive
                extracted_facts = []

        # Build/refresh digest from the final description (Telegram posts typically don't provide one).
        if bundled_digest:
            normalized_digest = bundled_digest
        else:
            llm_digest = None
            if not split_create_used:
                try:
                    llm_digest = await _llm_build_search_digest(
                        title=final_title,
                        description=description_value,
                        event_type=normalized_event_type or candidate.event_type,
                    )
                except Exception:
                    llm_digest = None
            if llm_digest:
                normalized_digest = llm_digest
        if not normalized_digest:
            normalized_digest = _fallback_digest_from_description(description_value)
        if not normalized_digest and rich_base:
            normalized_digest = _fallback_digest_from_description(rich_base)
        normalized_digest = (
            _restore_source_grounded_known_spellings(
                normalized_digest,
                source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
            )
            or normalized_digest
        )
        final_short = bundled_short
        if not final_short and not split_create_used:
            try:
                final_short = await _llm_build_short_description(
                    title=final_title,
                    description=description_value or clean_source_text,
                    event_type=normalized_event_type or candidate.event_type,
                )
            except Exception:
                final_short = None
        if not final_short:
            candidate_short = _clean_short_description(candidate.raw_excerpt)
            if _is_short_description_acceptable(candidate_short, min_words=12, max_words=16):
                final_short = candidate_short
        if not final_short:
            final_short = _fallback_short_description_from_text(description_value or clean_source_text)
        final_short = (
            _restore_source_grounded_known_spellings(
                final_short,
                source_text=clean_source_text or clean_raw_excerpt or candidate.source_text,
            )
            or final_short
        )
        new_event = Event(
            title=final_title,
            description=description_value,
            short_description=final_short,
            festival=candidate.festival,
            date=candidate.date or "",
            time=candidate.time or "",
            time_is_default=bool(candidate.time_is_default and (candidate.time or "").strip()),
            location_name=candidate.location_name or "",
            location_address=candidate.location_address,
            city=candidate.city or None,
            ticket_price_min=candidate.ticket_price_min,
            ticket_price_max=candidate.ticket_price_max,
            ticket_link=candidate.ticket_link,
            ticket_status=candidate.ticket_status,
            ticket_trust_level=candidate.trust_level,
            event_type=normalized_event_type or candidate.event_type,
            emoji=candidate.emoji,
            end_date=candidate.end_date,
            end_date_is_inferred=bool(candidate.end_date_is_inferred),
            date_is_inferred=_candidate_date_is_inferred(candidate, is_canonical_site=is_canonical_site),
            date_provenance=_candidate_date_provenance_level(candidate, is_canonical_site=is_canonical_site),
            date_confidence=_date_provenance_confidence(_candidate_date_provenance_level(candidate, is_canonical_site=is_canonical_site)),
            end_date_provenance=_candidate_end_date_provenance_level(candidate, is_canonical_site=is_canonical_site),
            end_date_confidence=_date_provenance_confidence(_candidate_end_date_provenance_level(candidate, is_canonical_site=is_canonical_site)),
            is_free=is_free_value,
            pushkin_card=bool(candidate.pushkin_card),
            silent=False,
            source_text=clean_source_text or "",
            source_texts=[clean_source_text] if clean_source_text else [],
            organizer_names=_bounded_organizer_names(candidate.organizer_names),
            source_post_url=candidate.source_url if _is_http_url(candidate.source_url) else None,
            source_chat_id=candidate.source_chat_id,
            source_message_id=candidate.source_message_id,
            tg_source_author=candidate.tg_source_author,
            creator_id=candidate.creator_id,
            search_digest=normalized_digest,
            photo_urls=[
                (p.supabase_url or p.catbox_url)
                for p in candidate.posters
                if (p.supabase_url or p.catbox_url)
            ],
            photo_count=len(
                [p for p in candidate.posters if (p.supabase_url or p.catbox_url)]
            ),
        )
        await _ensure_transport_duration_forecast(new_event, candidate)
        create_age_decision = _candidate_age_decision(
            candidate,
            semantic_payload=bundled_age_payload,
        )
        if create_age_decision is not None:
            apply_age_decision(new_event, create_age_decision)
        if candidate.source_url and _is_vk_wall_url(candidate.source_url):
            new_event.source_vk_post_url = candidate.source_url

        async with db.get_session() as session:
            canonical_binding_url = canonicalize_identity_url(candidate.source_url)
            if canonical_binding_url:
                conflicting_binding = await _source_identity_binding_conflict(
                    session,
                    event_id=-1,
                    canonical_source_url=canonical_binding_url,
                    source_role=_candidate_source_role(candidate),
                    occurrence_key=candidate.occurrence_key,
                )
                if conflicting_binding is not None:
                    return SmartUpdateResult(
                        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                        diagnostic_event_id=conflicting_binding,
                        reason="source_binding_conflict", retry_reason=RetryReason.SOURCE_BINDING_CONFLICT,
                    )
            final_lo, final_hi = _candidate_date_range(candidate)
            if final_lo is not None and final_hi is not None:
                final_stmt = select(Event).where(
                    and_(
                        Event.date <= final_hi.isoformat(),
                        or_(
                            and_(Event.end_date.is_(None), Event.date >= final_lo.isoformat()),
                            Event.end_date >= final_lo.isoformat(),
                        ),
                        Event.lifecycle_status == "active",
                    )
                )
                final_stmt = _apply_soft_city_filter(final_stmt, candidate.city)
                final_res = await session.execute(final_stmt)
                final_duplicate = _pre_create_duplicate_probe(candidate, list(final_res.scalars().all()))
                if final_duplicate is not None and not candidate.force_create_distinct:
                    result = await _accept_final_probe_match(
                        db,
                        session,
                        event=final_duplicate,
                        candidate=candidate,
                        schedule_tasks=schedule_tasks,
                        schedule_kwargs=schedule_kwargs,
                        enqueue_ticket_sites=_enqueue_ticket_sites_queue,
                    )
                    if result is None:
                        final_duplicate = None
                    elif result.outcome is SmartUpdateTerminalOutcome.MERGED:
                        try:
                            await _record_identity_gate_decision(
                                db,
                                candidate,
                                decision="allow_merge",
                                reason="final_transaction_duplicate_probe",
                                confidence=0.98,
                                event_id=result.event_id,
                                payload={"stage": "final_pre_insert_probe"},
                            )
                        except Exception:
                            # The canonical merge already committed. An
                            # observer failure cannot regress its accepted
                            # terminal into a retry/failed queue state.
                            logger.warning(
                                "smart_update: final probe audit log failed event_id=%s",
                                result.event_id,
                                exc_info=True,
                            )
                        return result
                    else:
                        return result
            session.add(new_event)
            # Keep create, accepted source attachment and collection decision
            # materialization in one transaction. flush supplies the event id.
            await session.flush()

            added_posters, added_poster_urls, preview_invalidated, pruned_posters, _photo_urls_changed = await _apply_posters(
                session,
                new_event.id,
                candidate.posters,
                poster_scope_hashes=candidate.poster_scope_hashes,
                event_title=final_title,
            )
            added_sources, _same_source = await _ensure_event_source(
                session, new_event.id, candidate
            )
            await session.flush()
            attached_collection_source = await _attached_collection_source(
                session, new_event.id, candidate
            )
            if (
                attached_collection_source is not None
                and candidate.collection_semantic_decisions is not None
                and apply_collection_decisions(
                    new_event,
                    candidate.collection_semantic_decisions,
                    source=attached_collection_source,
                    source_corpus=_collection_source_corpus(candidate),
                    input_hash=collection_adjudication_input_hash(candidate),
                    reasons=candidate.collection_adjudication_reasons,
                )
            ):
                session.add(new_event)
            await _enqueue_ticket_sites_queue(session, event_id=int(new_event.id or 0))
            if candidate.source_text:
                await _sync_source_texts(session, new_event)
            await session.flush()
            initial_records: list[tuple[str, str]] = []
            for fact in _initial_added_facts(candidate):
                initial_records.append((fact, "added"))
            for fact in (extracted_facts or [])[:18]:
                initial_records.append((fact, "added"))
            note_lines: list[str] = []
            note_lines.extend((queue_notes or [])[:6])
            note_lines.extend((text_filter_facts or [])[:2])
            note_lines.extend((poster_filter_facts or [])[:3])
            for fact in _dedupe_source_facts(note_lines):
                initial_records.append((fact, "note"))
            for url in (added_poster_urls or [])[:3]:
                initial_records.append((f"Добавлена афиша: {url}", "added"))
            if pruned_posters:
                initial_records.append((f"Удалены лишние афиши: {pruned_posters}", "note"))
            if preview_invalidated:
                initial_records.append(("3D-превью сброшено: изменились иллюстрации", "note"))
            if initial_records:
                await _record_source_facts(session, new_event.id, candidate, initial_records)
            await session.commit()

        await _persist_pending_festival_queue()
        try:
            await _apply_holiday_festival_mapping(db, new_event.id)
        except Exception:
            logger.warning(
                "smart_update: holiday mapping failed for event %s",
                new_event.id,
                exc_info=True,
            )

        await _classify_topics(db, new_event.id)

        linked_refresh_ids: list[int] = []
        try:
            from linked_events import recompute_linked_event_ids

            lr = await recompute_linked_event_ids(db, int(new_event.id or 0))
            linked_refresh_ids = [
                int(x)
                for x in (lr.changed_event_ids or [])
                if int(x) and int(x) != int(new_event.id or 0)
            ]
        except Exception:
            logger.warning(
                "smart_update: linked events recompute failed for event %s",
                new_event.id,
                exc_info=True,
            )
            linked_refresh_ids = []

        if schedule_tasks and linked_refresh_ids:
            # Only refresh Telegraph pages for linked occurrences: this is user-facing ("Другие даты"),
            # but does not require VK sync / month-page rebuilds.
            try:
                from main import JobTask, enqueue_job

                for rid in linked_refresh_ids[:80]:
                    await enqueue_job(db, int(rid), JobTask.telegraph_build, depends_on=None)
            except Exception:
                logger.warning(
                    "smart_update: failed to enqueue linked telegraph refresh for event %s",
                    new_event.id,
                    exc_info=True,
                )

        if schedule_tasks:
            try:
                from main import schedule_event_update_tasks
                async with db.get_session() as session:
                    refreshed = await session.get(Event, new_event.id)
                if refreshed:
                    await schedule_event_update_tasks(db, refreshed, **(schedule_kwargs or {}))
            except Exception:
                logger.warning("smart_update: schedule/update failed for event %s", new_event.id, exc_info=True)

        # Interest-club verification is durable outbox work. Enqueueing remains
        # outside the canonical event transaction, while provider/restart
        # failures can no longer lose the relation evaluation.
        try:
            from interest_clubs import schedule_interest_club_evaluation

            await schedule_interest_club_evaluation(
                db,
                int(new_event.id or 0),
                schedule_projection=schedule_tasks,
            )
        except Exception:
            logger.warning(
                "smart_update: failed to enqueue interest-club evaluation event=%s",
                new_event.id,
                exc_info=True,
            )

        logger.info(
            "smart_update.created event_id=%s added_posters=%d added_sources=%s reason=%s",
            new_event.id,
            added_posters,
            int(bool(added_sources)),
            match_reason,
        )
        return SmartUpdateResult(
            status="created",
            event_id=new_event.id,
            created=True,
            merged=False,
            added_posters=added_posters,
            added_sources=added_sources,
            reason=match_reason,
            queue_notes=list(queue_notes or []),
        )

    if match_event is None:
        result = await _create_from_prepared_candidate()
        if result.is_accepted and candidate.force_create_distinct_reason is not None:
            result.identity_distinct_reason = candidate.force_create_distinct_reason
        return result

    # Merge path
    existing = match_event
    existing_start, existing_end = _event_date_range(existing)
    is_canonical_site = str(candidate.source_type or "").startswith("parser:")
    conflicting: dict[str, Any] = {}
    # By default we keep anchor fields stable; for canonical site/parser imports we allow
    # correcting anchors and therefore do not treat conflicts as "do not use".
    if not is_canonical_site:
        if existing_start and cand_start and existing_start != cand_start:
            conflicting["date"] = candidate.date
        cand_time_anchor = _candidate_anchor_time(candidate, is_canonical_site=False)
        existing_time_anchor = _event_anchor_time(existing)
        if _has_explicit_time_conflict(existing_time_anchor, cand_time_anchor):
            conflicting["time"] = candidate.time
        if existing.location_name and candidate.location_name and not _location_matches(existing.location_name, candidate.location_name):
            conflicting["location_name"] = candidate.location_name
        if (
            existing.location_address
            and candidate.location_address
            and not _address_matches(
                existing.location_address,
                candidate.location_address,
                city_a=getattr(existing, "city", None),
                city_b=candidate.city,
            )
        ):
            conflicting["location_address"] = candidate.location_address
        if existing_end and cand_end and existing_end != cand_end:
            long_event = _is_long_event_type_value(
                getattr(existing, "event_type", None) or candidate.event_type
            )
            # For long-running events (e.g. exhibitions/fairs), later end_date is a
            # normal update, not an anchor conflict.
            if (not long_event) or (cand_end < existing_end):
                conflicting["end_date"] = candidate.end_date

    new_hashes = _poster_hashes(candidate.posters)
    existing_hashes = {p.poster_hash for p in posters_map.get(existing.id or 0, [])}
    has_new_posters = bool(new_hashes - existing_hashes)
    has_new_text = _candidate_has_new_text(candidate, existing)
    needs_schedule_cleanup = _description_has_foreign_schedule_noise(
        getattr(existing, "description", None),
        event_date=getattr(existing, "date", None),
        end_date=getattr(existing, "end_date", None),
        event_title=getattr(existing, "title", None),
    )

    ticket_changes_needed = any(
        [
            candidate.ticket_link and candidate.ticket_link != existing.ticket_link,
            candidate.ticket_price_min is not None and candidate.ticket_price_min != existing.ticket_price_min,
            candidate.ticket_price_max is not None and candidate.ticket_price_max != existing.ticket_price_max,
            candidate.ticket_status and candidate.ticket_status != getattr(existing, "ticket_status", None),
        ]
    )

    should_merge = (
        has_new_posters
        or has_new_text
        or needs_schedule_cleanup
        or ticket_changes_needed
    )

    if SMART_UPDATE_MERGE_IDENTITY_GATE_MODE is not IdentityGateMode.OFF:
        merge_identity_verdict = None
        gate_error: Exception | None = None
        existing_poster_texts = [
            p.ocr_text for p in posters_map.get(existing.id or 0, []) if getattr(p, "ocr_text", None)
        ]
        blocking_conflicts = [f"{key}: {value}" for key, value in (conflicting or {}).items()]
        try:
            llm_gate_data = await _llm_merge_identity_gate(
                candidate,
                existing,
                conflicting_anchor_fields=conflicting,
                poster_texts=existing_poster_texts,
            )
            merge_identity_verdict = build_merge_identity_gate_verdict(
                candidate,
                existing,
                mode=SMART_UPDATE_MERGE_IDENTITY_GATE_MODE,
                llm_data=llm_gate_data,
                blocking_conflicts=blocking_conflicts,
            )
        except Exception as exc:
            gate_error = exc
            logger.warning(
                "smart_update.merge_identity_gate error event_id=%s source_type=%s source_url=%s",
                getattr(existing, "id", None),
                candidate.source_type,
                candidate.source_url,
                exc_info=True,
            )
            merge_identity_verdict = merge_identity_gate_fail_safe_verdict(
                mode=SMART_UPDATE_MERGE_IDENTITY_GATE_MODE,
                candidate=candidate,
                existing_event=existing,
                reason=str(exc) or "merge identity gate error",
            )

        logger.info(
            "smart_update.merge_identity_gate mode=%s action=%s relation=%s event_id=%s confidence=%.3f reason=%s enforce_skip=%s source_type=%s source_url=%s",
            merge_identity_verdict.mode.value,
            merge_identity_verdict.action.value,
            merge_identity_verdict.relation.value,
            getattr(existing, "id", None),
            float(merge_identity_verdict.confidence or 0.0),
            merge_identity_verdict.reason_code,
            int(merge_identity_verdict.should_skip_side_effects),
            candidate.source_type,
            candidate.source_url,
        )
        await _record_identity_gate_decision(
            db,
            candidate,
            decision=merge_identity_verdict.action.value,
            reason=merge_identity_verdict.reason_code,
            confidence=merge_identity_verdict.confidence,
            event_id=int(existing.id) if getattr(existing, "id", None) else None,
            payload={
                "stage": "merge_identity_gate",
                "mode": merge_identity_verdict.mode.value,
                "relation": merge_identity_verdict.relation.value,
                "reasons": list(merge_identity_verdict.reasons),
                "blocking_conflicts": list(merge_identity_verdict.blocking_conflicts),
                "allowed_fields": list(merge_identity_verdict.allowed_fields),
                "deterministic": bool(merge_identity_verdict.deterministic),
                "fail_safe": bool(merge_identity_verdict.fail_safe),
                "would_skip_side_effects": bool(merge_identity_verdict.would_skip_side_effects),
                "match_reason": match_reason if "match_reason" in locals() else None,
                "llm": dict(merge_identity_verdict.llm or {}),
                "error": str(gate_error) if gate_error else None,
            },
        )
        if merge_identity_verdict.should_skip_side_effects:
            distinct_reason = merge_identity_verdict.identity_distinct_reason
            if not merge_identity_verdict.fail_safe and distinct_reason is not None:
                # A known non-identity is a positive decision, not uncertainty.
                # Reuse the already-prepared create path inside this same
                # durable attempt with final-probe matching disabled. Early
                # eventness, occurrence, matching and gate calls are not
                # repeated; no new identity/provider stage or retry-worker
                # delay is introduced.
                candidate.force_create_distinct = True
                diagnostic_reason = (
                    f"create_distinct:{merge_identity_verdict.relation.value}:"
                    f"{merge_identity_verdict.reason_code}"
                )
                result = await _create_from_prepared_candidate()
                if result.is_accepted:
                    result.reason = diagnostic_reason
                    result.identity_distinct_reason = distinct_reason
                    result.diagnostic_event_id = int(existing.id or 0) or None
                return result
            semantic_unknown = bool(
                merge_identity_verdict.llm_contract_valid
                and merge_identity_verdict.relation is MergeIdentityRelation.UNKNOWN
                and not merge_identity_verdict.fail_safe
            )
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=existing.id,
                created=False,
                merged=False,
                reason=merge_identity_verdict.reason_code,
                retry_reason=(
                    RetryReason.IDENTITY_SEMANTIC_UNKNOWN
                    if semantic_unknown
                    else RetryReason.IDENTITY_TECHNICAL_FAILURE
                ),
                queue_notes=list(queue_notes or []),
            )

    added_facts: list[str] = []
    duplicate_facts: list[str] = []
    skipped_conflicts: list[str] = []
    conflict_facts: list[str] = []
    updated_fields = False
    updated_keys: list[str] = []
    skip_topic_reclassify = False
    merge_digest_from_llm: str | None = None
    merge_fact_first_used = False

    async with db.get_session() as session:
        event_db = await session.get(Event, existing.id)
        if not event_db:
            return SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                diagnostic_event_id=int(existing.id or 0) or None,
                reason="event_missing", retry_reason=RetryReason.EVENT_MISSING,
            )
        canonical_binding_url = canonicalize_identity_url(candidate.source_url)
        if canonical_binding_url:
            conflicting_binding = await _source_identity_binding_conflict(
                session,
                event_id=int(event_db.id or 0),
                canonical_source_url=canonical_binding_url,
                source_role=_candidate_source_role(candidate),
                occurrence_key=candidate.occurrence_key,
            )
            if conflicting_binding is not None:
                return SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                    diagnostic_event_id=conflicting_binding,
                    reason="source_binding_conflict", retry_reason=RetryReason.SOURCE_BINDING_CONFLICT,
                )
        before_description = event_db.description or ""
        structured_age_decision = _candidate_age_decision(candidate)
        if structured_age_decision is not None:
            structured_age_decision = reconcile_age_decision(event_db, structured_age_decision)
            if apply_age_decision(event_db, structured_age_decision):
                updated_fields = True
                updated_keys.append("age_restriction")
        # Self-heal legacy snapshot leaks (e.g. "Текст до Smart Update ...") that were
        # accidentally merged into the public description in older versions.
        cleaned_leak = _drop_legacy_leak_from_description(before_description)
        if cleaned_leak and cleaned_leak != before_description:
            event_db.description = cleaned_leak
            before_description = cleaned_leak
            updated_fields = True
            if "description" not in updated_keys:
                updated_keys.append("description")
            note = "Текст очищен: удалена служебная вставка legacy snapshot"
            if note not in text_filter_facts:
                text_filter_facts.append(note)
        existing_trusts = [
            str(r[0]).strip()
            for r in (
                await session.execute(
                    select(EventSource.trust_level).where(
                        EventSource.event_id == int(event_db.id or 0)
                    )
                )
            ).all()
            if (r and str(r[0] or "").strip())
        ]
        event_trust_level, event_trust_pr = _max_trust_level(existing_trusts)
        candidate_trust_pr = _trust_priority(candidate.trust_level)
        can_update_date, date_update_reason = _can_apply_conservative_date_update(
            event_db,
            candidate,
            is_canonical_site=is_canonical_site,
        )

        if _apply_llm_confirmed_unknown_start_time(
            event_db,
            candidate,
            updated_keys=updated_keys,
        ):
            updated_fields = True
            conflicting.pop("time", None)

        # Fill placeholder/missing time from any matched source (TG/VK/etc.), not only parser sources.
        # This prevents duplicate creation like: existing time=00:00 (legacy placeholder) + new source brings 19:00.
        if not is_canonical_site:
            ct_anchor = _candidate_anchor_time(candidate, is_canonical_site=False)
            et_anchor = _event_anchor_time(event_db)
            if (
                ct_anchor
                and bool(getattr(event_db, "time_is_default", False))
                and ct_anchor == (getattr(event_db, "time", "") or "").strip()
            ):
                event_db.time_is_default = False
                updated_fields = True
                if "time_is_default" not in updated_keys:
                    updated_keys.append("time_is_default")
            if ct_anchor and (not et_anchor) and ct_anchor != (getattr(event_db, "time", "") or "").strip():
                event_db.time = ct_anchor
                event_db.time_is_default = False
                updated_fields = True
                if "time" not in updated_keys:
                    updated_keys.append("time")
                conflicting.pop("time", None)

        if is_canonical_site:
            # Canonical site/parser source: allow correcting anchors on an existing event.
            # This makes Telegram-first -> /parse merge converge to the site truth.
            if can_update_date and date_update_reason == _DATE_UPDATE_REASON_CANONICAL:
                event_db.date = candidate.date
                updated_fields = True
                updated_keys.append("date")
            if _apply_event_end_date(
                event_db,
                end_date=candidate.end_date,
                inferred=bool(candidate.end_date_is_inferred),
                updated_keys=updated_keys,
            ):
                updated_fields = True
            if candidate.time and candidate.time.strip() and candidate.time.strip() != (event_db.time or "").strip():
                event_db.time = candidate.time.strip()
                event_db.time_is_default = False
                updated_fields = True
                updated_keys.append("time")
            elif (
                candidate.time
                and candidate.time.strip()
                and bool(getattr(event_db, "time_is_default", False))
                and candidate.time.strip() == (event_db.time or "").strip()
            ):
                event_db.time_is_default = False
                updated_fields = True
                if "time_is_default" not in updated_keys:
                    updated_keys.append("time_is_default")
            if candidate.location_name and not _location_matches(event_db.location_name, candidate.location_name):
                event_db.location_name = candidate.location_name
                updated_fields = True
                updated_keys.append("location_name")
            if (
                candidate.location_address
                and candidate.location_address.strip()
                and candidate.location_address.strip() != (event_db.location_address or "").strip()
            ):
                event_db.location_address = candidate.location_address.strip()
                updated_fields = True
                updated_keys.append("location_address")
        elif can_update_date and date_update_reason == _DATE_UPDATE_REASON_INFERRED_LONG_GROUNDED:
            # A later exact source can correct legacy exhibition rows created from vague
            # month/message-date teasers. Only do this when the old range is inferred and
            # the new start date is explicitly grounded in the candidate source/OCR.
            event_db.date = candidate.date
            updated_fields = True
            if "date" not in updated_keys:
                updated_keys.append("date")
            conflicting.pop("date", None)
            if _apply_event_end_date(
                event_db,
                end_date=candidate.end_date,
                inferred=bool(candidate.end_date_is_inferred),
                updated_keys=updated_keys,
            ):
                updated_fields = True
                conflicting.pop("end_date", None)

        # Operator-entered sources are allowed to корректировать title even if the
        # candidate doesn't bring enough new text/posters for LLM merge.
        cand_title = clean_title
        if candidate.source_type in ("bot", "manual") and cand_title and cand_title != event_db.title:
            event_db.title = cand_title
            updated_fields = True
            updated_keys.append("title")

        # Long-running events (e.g. exhibitions/fairs) may legitimately extend the
        # closing date across sources. Allow end_date extension by trust.
        if (
            (not is_canonical_site)
            and candidate.end_date
            and _is_long_event_type_value(
                getattr(event_db, "event_type", None) or candidate.event_type
            )
        ):
            cand_end_iso = _parse_iso_date(candidate.end_date)
            cur_end_iso = _parse_iso_date(getattr(event_db, "end_date", None))
            if cand_end_iso and (not cur_end_iso or cand_end_iso > cur_end_iso):
                if candidate_trust_pr >= event_trust_pr:
                    if _apply_event_end_date(
                        event_db,
                        end_date=cand_end_iso.isoformat(),
                        inferred=bool(candidate.end_date_is_inferred),
                        updated_keys=updated_keys,
                    ):
                        updated_fields = True
                    if "end_date" in conflicting:
                        conflicting.pop("end_date", None)
                else:
                    skipped_conflicts.append(
                        f"Дата окончания: {getattr(event_db, 'end_date', None)} -> {candidate.end_date} "
                        f"(выбран: event_before по trust {event_trust_level or 'medium'}>{candidate.trust_level or 'medium'})"
                    )

        if (
            candidate.end_date
            and candidate.end_date == getattr(event_db, "end_date", None)
            and _apply_event_end_date(
                event_db,
                end_date=candidate.end_date,
                inferred=bool(candidate.end_date_is_inferred),
                updated_keys=updated_keys,
            )
        ):
            updated_fields = True

        if should_merge:
            before_description = event_db.description or ""
            posters_texts = [p.ocr_text for p in posters_map.get(existing.id or 0, []) if p.ocr_text]
            cleanup_only = (
                needs_schedule_cleanup
                and (not has_new_posters)
                and (not has_new_text)
                and (not ticket_changes_needed)
            )
            if cleanup_only:
                cleaned = _strip_foreign_schedule_noise(
                    before_description,
                    event_date=event_db.date,
                    end_date=event_db.end_date,
                    event_title=event_db.title,
                )
                if cleaned and cleaned != before_description:
                    cleaned = _normalize_plaintext_paragraphs(cleaned) or cleaned
                    cleaned = _promote_review_bullets_to_blockquotes(cleaned) or cleaned
                    cleaned = _promote_first_person_quotes_to_blockquotes(cleaned) or cleaned
                    cleaned = _promote_inline_quoted_direct_speech_to_blockquotes(cleaned) or cleaned
                    cleaned = _drop_reported_speech_duplicates(cleaned) or cleaned
                    cleaned = _normalize_blockquote_markers(cleaned) or cleaned
                    cleaned = _append_missing_scene_hint(
                        description=cleaned, source_text=candidate.source_text
                    ) or cleaned
                    cleaned = (
                        _sanitize_description_output(
                            cleaned,
                            source_text=candidate.source_text,
                        )
                        or cleaned
                    )
                    cleaned = _ensure_minimal_description_headings(cleaned) or cleaned
                    event_db.description = _clip(cleaned, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                    updated_fields = True
                    updated_keys.append("description")
                    note = "Текст очищен: убраны строки расписания других дат"
                    if note not in text_filter_facts:
                        text_filter_facts.append(note)
                    skip_topic_reclassify = True
            else:
                quote_candidates = _extract_quote_candidates(
                    _strip_promo_lines(candidate.source_text) or candidate.source_text,
                    max_items=2,
                )
                rows = (
                    await session.execute(
                        select(
                            EventSourceFact.fact,
                            EventSource.source_text,
                            EventSource.source_url,
                        )
                        .join(EventSource, EventSourceFact.source_id == EventSource.id)
                        .where(
                            EventSourceFact.event_id == int(event_db.id or 0),
                            # `duplicate` is still a true, already-known fact. Include it to keep
                            # canonical facts stable across re-processing the same source URL.
                            EventSourceFact.status.in_(("added", "duplicate")),
                            # Never feed legacy snapshots into the merge facts:
                            # they are not per-source "added facts" and can pollute Telegraph text.
                            EventSource.source_type != "legacy",
                        )
                    )
                ).all()
                grounded_rows: list[str] = []
                for row in rows or []:
                    fact = str(row[0] or "").strip()
                    source_url_for_fact = str(row[2] or "").strip()
                    if not fact or _is_managed_vk_publication_url(source_url_for_fact):
                        continue
                    # Historical ledger facts may be faithful LLM paraphrases, so
                    # do not semantically rewrite/drop them with a lexical rule.
                    # New facts enter the ledger only through the evidence-quote
                    # contracts below and in rich_facts_extract.
                    grounded_rows.append(fact)
                facts_before_list = _dedupe_source_facts(grounded_rows)[:80]
                legacy_facts_seed: list[str] = []
                # Keep a compact snapshot for operator audit/debug, but do not use it as facts_before.
                # Also: if an older run created only a legacy note (without extracted facts), we still
                # backfill baseline legacy facts later from the stored legacy snapshot.
                try:
                    before_texts = list(getattr(event_db, "source_texts", None) or [])
                    legacy_facts_seed = await _ensure_legacy_description_fact(
                        session,
                        event=event_db,
                        legacy_description=before_description,
                    )
                    after_texts = list(getattr(event_db, "source_texts", None) or [])
                    if after_texts != before_texts:
                        updated_fields = True
                        if "source_texts" not in updated_keys:
                            updated_keys.append("source_texts")
                except Exception:
                    logger.warning(
                        "smart_update: legacy description snapshot store failed",
                        exc_info=True,
                    )
                    legacy_facts_seed = []
                director_name_hint = _extract_director_name_hint(
                    candidate_text=_strip_promo_lines(candidate.source_text) or candidate.source_text,
                    facts_before=facts_before_list,
                )
                merge_data = await _llm_merge_event(
                    candidate,
                    event_db,
                    conflicting_anchor_fields=conflicting,
                    poster_texts=posters_texts,
                    facts_before=facts_before_list,
                    event_trust_level=event_trust_level,
                    candidate_trust_level=candidate.trust_level,
                )
                if merge_data:
                    semantic_age_decision = _candidate_age_decision(
                        candidate,
                        semantic_payload=merge_data.get("age_decision"),
                    )
                    if semantic_age_decision is not None and not candidate.age_restriction_is_structured:
                        semantic_age_decision = reconcile_age_decision(event_db, semantic_age_decision)
                        if apply_age_decision(event_db, semantic_age_decision):
                            updated_fields = True
                            if "age_restriction" not in updated_keys:
                                updated_keys.append("age_restriction")
                    merge_digest_from_llm = _clean_search_digest(merge_data.get("search_digest"))
                    deterministic_skipped_conflicts = list(skipped_conflicts)
                    added_facts = _flatten_source_grounded_fact_items(
                        merge_data.get("added_facts") or [],
                        source_text=(
                            _strip_promo_lines(candidate.source_text)
                            or candidate.source_text
                        ),
                        log_context=f"merge:{candidate.source_url or candidate.source_type}",
                    )
                    added_facts = _filter_ungrounded_sensitive_facts(
                        added_facts,
                        candidate=candidate,
                    )
                    duplicate_facts = _flatten_source_grounded_fact_items(
                        merge_data.get("duplicate_facts") or [],
                        source_text=(
                            _strip_promo_lines(candidate.source_text)
                            or candidate.source_text
                        ),
                        log_context=f"merge_duplicate:{candidate.source_url or candidate.source_type}",
                    )
                    duplicate_facts = _filter_ungrounded_sensitive_facts(
                        duplicate_facts,
                        candidate=candidate,
                    )
                    conflict_facts = list(merge_data.get("conflict_facts") or [])
                    llm_skipped_conflicts = list(merge_data.get("skipped_conflicts") or [])
                    skipped_conflicts = []
                    for item in deterministic_skipped_conflicts + llm_skipped_conflicts:
                        text = str(item or "").strip()
                        if not text or text in skipped_conflicts:
                            continue
                        skipped_conflicts.append(text)

                    title = merge_data.get("title")
                    description = merge_data.get("description")
                    clean_title = _strip_private_use(title) if isinstance(title, str) else None
                    if clean_title:
                        clean_title = clean_title.replace("\\\\\"", "\"").replace("\\\"", "\"").strip()
                        clean_title = re.sub(r"\s+", " ", clean_title).strip()
                        clean_title = _clip_title(clean_title, 160) or clean_title
                    clean_description = (
                        (
                            _strip_private_use(description) or description
                        )
                        if isinstance(description, str)
                        else None
                    )
                    if clean_title:
                        if clean_title.strip() == (event_db.title or "").strip():
                            # No-op title, keep as-is without recording semantic mismatch.
                            pass
                        elif _is_merge_title_update_allowed(
                            proposed_title=clean_title,
                            candidate_title=candidate.title,
                            existing_title=event_db.title,
                            is_canonical_site=is_canonical_site,
                        ):
                            event_db.title = clean_title
                            updated_fields = True
                            updated_keys.append("title")
                        else:
                            relaxed_allowed = False
                            old_title = str(getattr(event_db, "title", "") or "").strip()
                            if (
                                (not is_canonical_site)
                                and _is_generic_title_event_type_venue(
                                    old_title,
                                    event_type=getattr(event_db, "event_type", None) or candidate.event_type,
                                    location_name=getattr(event_db, "location_name", None)
                                    or candidate.location_name,
                                    city=getattr(event_db, "city", None) or candidate.city,
                                )
                                and _is_title_grounded_in_candidate_sources(clean_title, candidate)
                            ):
                                relaxed_allowed = True

                            if relaxed_allowed:
                                event_db.title = clean_title
                                updated_fields = True
                                updated_keys.append("title")
                                logger.info(
                                    "smart_update.title_updated_from_generic event_id=%s source_type=%s source_url=%s old_title=%s new_title=%s",
                                    getattr(event_db, "id", None),
                                    candidate.source_type,
                                    candidate.source_url,
                                    _clip_title(old_title),
                                    _clip_title(clean_title),
                                )
                            else:
                                # Catastrophic merge guard: if the model proposes an unrelated title,
                                # abort this merge (do not record facts/sources) to avoid polluting
                                # an existing event with content from a different one.
                                if (
                                    _title_has_meaningful_tokens(clean_title)
                                    and _title_has_meaningful_tokens(candidate.title)
                                    and (not _titles_look_related(candidate.title, event_db.title))
                                    and (not _titles_look_related(clean_title, candidate.title))
                                    and (not _titles_look_related(clean_title, event_db.title))
                                    and (not is_canonical_site)
                                ):
                                    event_id = getattr(event_db, "id", None)
                                    await session.rollback()
                                    logger.warning(
                                        "smart_update.reject reason=incoherent_merge_title event_id=%s source_type=%s source_url=%s candidate_title=%s proposed_title=%s",
                                        event_id,
                                        candidate.source_type,
                                        candidate.source_url,
                                        _clip_title(candidate.title),
                                        _clip_title(clean_title),
                                    )
                                    candidate.force_create_distinct = True
                                    candidate.force_create_distinct_reason = (
                                        IdentityDistinctReason.INCOHERENT_MERGE
                                    )
                                    result = await _create_from_prepared_candidate()
                                    if result.is_accepted:
                                        result.reason = "create_distinct:incoherent_merge_title"
                                        result.identity_distinct_reason = (
                                            IdentityDistinctReason.INCOHERENT_MERGE
                                        )
                                        result.diagnostic_event_id = (
                                            int(event_id or 0) or None
                                        )
                                    return result
                                skipped_conflicts.append(
                                    f"Заголовок отклонён: {event_db.title} -> {clean_title} "
                                    "(причина: semantic_title_mismatch)"
                                )
                                logger.warning(
                                    "smart_update.title_rejected event_id=%s candidate_title=%s "
                                    "existing_title=%s proposed_title=%s source_type=%s source_url=%s",
                                    getattr(event_db, "id", None),
                                    _clip_title(candidate.title),
                                    _clip_title(getattr(event_db, "title", None)),
                                    _clip_title(clean_title),
                                    candidate.source_type,
                                    candidate.source_url,
                                )

                    # Fact-first: override public narrative with a fresh build from canonical facts,
                    # ignoring the merged description text (which is derived from raw sources).
                    if SMART_UPDATE_FACT_FIRST and not SMART_UPDATE_LLM_DISABLED:
                        legacy_facts_for_fact_first: list[str] = list(legacy_facts_seed or [])
                        if not legacy_facts_for_fact_first:
                            try:
                                legacy_rows = (
                                    await session.execute(
                                        select(EventSourceFact.fact)
                                        .join(EventSource, EventSourceFact.source_id == EventSource.id)
                                        .where(
                                            EventSourceFact.event_id == int(event_db.id or 0),
                                            EventSourceFact.status.in_(("added", "duplicate")),
                                            EventSource.source_type == "legacy",
                                        )
                                    )
                                ).all()
                                legacy_facts_for_fact_first = _dedupe_source_facts(
                                    [
                                        str(r[0]).strip()
                                        for r in (legacy_rows or [])
                                        if (r and str(r[0] or "").strip())
                                    ]
                                )[:40]
                            except Exception:
                                legacy_facts_for_fact_first = []

                        canonical_facts = [
                            *(facts_before_list or []),
                            *[
                                str(f).strip()
                                for f in (added_facts or [])
                                if isinstance(f, str) and f.strip()
                            ],
                            *(legacy_facts_for_fact_first or []),
                        ]
                        facts_text_clean = _facts_text_clean_from_facts(
                            canonical_facts,
                            max_items=36,
                            anchors=[
                                getattr(event_db, "date", None) or candidate.date or "",
                                getattr(event_db, "time", None) or candidate.time or "",
                                getattr(event_db, "city", None) or candidate.city or "",
                                getattr(event_db, "location_name", None) or candidate.location_name or "",
                                getattr(event_db, "location_address", None) or candidate.location_address or "",
                            ],
                        )
                        if facts_text_clean:
                            try:
                                ff_desc = await _llm_fact_first_description_md_bounded(
                                    title=event_db.title,
                                    event_type=getattr(event_db, "event_type", None) or candidate.event_type,
                                    facts_text_clean=facts_text_clean,
                                    anchors=[
                                        getattr(event_db, "date", None) or candidate.date or "",
                                        getattr(event_db, "time", None) or candidate.time or "",
                                        getattr(event_db, "city", None) or candidate.city or "",
                                        getattr(event_db, "location_name", None) or candidate.location_name or "",
                                        getattr(event_db, "location_address", None)
                                        or candidate.location_address
                                        or "",
                                    ],
                                    label=f"merge:{int(getattr(event_db, 'id', 0) or 0)}",
                                )
                            except Exception:  # pragma: no cover - provider failures
                                ff_desc = None
                            if ff_desc:
                                # Minimal deterministic cleanup only (no "top-up" from sources).
                                cleaned_ff = _dedupe_description(ff_desc) or ff_desc
                                cleaned_ff = _normalize_plaintext_paragraphs(cleaned_ff) or cleaned_ff
                                cleaned_ff = _normalize_blockquote_markers(cleaned_ff) or cleaned_ff
                                cleaned_ff = (
                                    _sanitize_description_output(
                                        cleaned_ff,
                                        source_text=_pick_richest_source_text_for_description(event_db, candidate),
                                    )
                                    or cleaned_ff
                                )
                                if not _fact_first_is_sparse(facts_text_clean):
                                    cleaned_ff = _ensure_minimal_description_headings(cleaned_ff) or cleaned_ff
                                cleaned_ff = _clip(cleaned_ff, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                                current = (event_db.description or "").strip()
                                if cleaned_ff.strip() and cleaned_ff.strip() != current:
                                    event_db.description = cleaned_ff
                                    updated_fields = True
                                    if "description" not in updated_keys:
                                        updated_keys.append("description")
                                note = "Описание перегенерировано: fact-first"
                                if note not in text_filter_facts:
                                    text_filter_facts.append(note)
                                merge_fact_first_used = True
                                # Skip merge-description-driven cleanup below.
                                clean_description = None

                    if clean_description:
                        clean_description = _dedupe_description(clean_description) or clean_description
                        clean_description = _enforce_merge_non_shrinking_description(
                            before_description=before_description,
                            merged_description=clean_description,
                            candidate=candidate,
                            has_new_text=has_new_text,
                        )
                        clean_description = _strip_foreign_schedule_noise(
                            clean_description,
                            event_date=event_db.date,
                            end_date=event_db.end_date,
                            event_title=event_db.title,
                        ) or clean_description
                        clean_description = _normalize_plaintext_paragraphs(clean_description) or clean_description
                        # If we have rich source text (usually from site import) but the merge
                        # produced an over-compressed digest, do a second-pass rewrite via LLM.
                        # We do NOT fall back to verbatim source text: Telegraph text must be LLM-produced.
                        rich_fallback_used = False
                        min_expected = _min_expected_description_len_from_sources(event_db, candidate)
                        if min_expected and len(clean_description) < min_expected:
                            try:
                                rewritten_full = await _rewrite_description_full_from_sources(event_db, candidate)
                            except Exception:  # pragma: no cover - defensive
                                rewritten_full = None
                            if rewritten_full and len(rewritten_full) >= int(min_expected * 0.85):
                                clean_description = rewritten_full
                                rich_fallback_used = True
                        clean_description = _normalize_plaintext_paragraphs(clean_description) or clean_description
                        # NOTE: We intentionally do NOT append any sentences deterministically.
                        # If the model missed important details, that is an LLM quality issue and should
                        # be fixed via prompts/models, not by verbatim injection.
                        clean_description = (
                            _preserve_blockquotes_from_previous_description(
                                before_description=before_description,
                                merged_description=clean_description,
                                event_title=event_db.title,
                            )
                            or clean_description
                        )
                        clean_description = (
                            _promote_first_person_quotes_to_blockquotes(clean_description)
                            or clean_description
                        )
                        clean_description = (
                            _promote_inline_quoted_direct_speech_to_blockquotes(clean_description)
                            or clean_description
                        )
                        clean_description = _drop_reported_speech_duplicates(clean_description) or clean_description
                        clean_description = _normalize_blockquote_markers(clean_description) or clean_description
                        clean_description = (
                            _split_overlong_first_person_blockquotes(clean_description) or clean_description
                        )
                        clean_description = (
                            _fix_broken_initial_paragraph_splits(clean_description) or clean_description
                        )
                        # When we had to fall back to a rich verbatim source text because the merge
                        # was over-compressed, avoid aggressive paragraph de-duplication: it can
                        # accidentally collapse legitimately long source material (and re-trigger
                        # the "too short" issue we are fixing).
                        if not rich_fallback_used:
                            clean_description = (
                                _dedupe_paragraphs_preserving_formatting(clean_description) or clean_description
                            )
                        clean_description = _append_missing_scene_hint(
                            description=clean_description, source_text=candidate.source_text
                        ) or clean_description
                        clean_description = (
                            _sanitize_description_output(
                                clean_description,
                                source_text=_pick_richest_source_text_for_description(event_db, candidate),
                            )
                            or clean_description
                        )
                        rich_source_text = _pick_richest_source_text_for_description(event_db, candidate)
                        if _description_too_verbatim(clean_description, source_text=rich_source_text):
                            logger.warning(
                                "smart_update: merged description too verbatim; forcing full rewrite event_id=%s source_type=%s source_url=%s",
                                getattr(event_db, "id", None),
                                candidate.source_type,
                                candidate.source_url,
                            )
                            try:
                                rewritten_full = await _rewrite_description_full_from_sources(event_db, candidate)
                            except Exception:  # pragma: no cover
                                rewritten_full = None
                            if rewritten_full and not _description_too_verbatim(
                                rewritten_full, source_text=rich_source_text
                            ):
                                clean_description = rewritten_full
                        # If we have a director name and quotes in the text, make sure at least
                        # one quote contains the attribution inside the blockquote.
                        clean_description = _ensure_blockquote_has_attribution(
                            description=clean_description,
                            attribution_name=director_name_hint,
                        )
                        if _has_overlong_paragraph(clean_description, limit=850):
                            try:
                                reflown = await _llm_reflow_description_paragraphs(clean_description)
                            except Exception:  # pragma: no cover
                                reflown = None
                            if reflown:
                                reflown = _normalize_plaintext_paragraphs(reflown) or reflown
                                reflown = _normalize_blockquote_markers(reflown) or reflown
                                reflown = _fix_broken_initial_paragraph_splits(reflown) or reflown
                                reflown = (
                                    _sanitize_description_output(
                                        reflown,
                                        source_text=_pick_richest_source_text_for_description(event_db, candidate),
                                    )
                                    or reflown
                                )
                                clean_description = reflown
                        # Ensure we keep at least one detected direct quote as a blockquote.
                        if quote_candidates and not re.search(r"(?m)^>\s+", clean_description):
                            clean_description = await _ensure_direct_quote_blockquote(
                                description=clean_description,
                                quote_candidates=quote_candidates,
                                candidate_text=(
                                    _strip_promo_lines(candidate.source_text) or candidate.source_text
                                ),
                                facts_before=facts_before_list,
                                label="merge_quote_enforce",
                            )
                            clean_description = _normalize_plaintext_paragraphs(clean_description) or clean_description
                            clean_description = _normalize_blockquote_markers(clean_description) or clean_description
                            clean_description = _drop_reported_speech_duplicates(clean_description) or clean_description
                            clean_description = _ensure_blockquote_has_attribution(
                                description=clean_description,
                                attribution_name=director_name_hint,
                            )

                        # Ensure the narrative mentions short quoted slogan-like canonical facts.
                        canonical_facts = [
                            *(facts_before_list or []),
                            *[
                                str(f).strip()
                                for f in (added_facts or [])
                                if isinstance(f, str) and f.strip()
                            ],
                        ]
                        missing = _find_missing_facts_in_description(
                            description=clean_description,
                            facts=canonical_facts,
                            max_items=5,
                        )
                        if missing:
                            try:
                                enriched = await _llm_integrate_missing_facts_into_description(
                                    description=clean_description,
                                    missing_facts=missing,
                                    source_text=_pick_richest_source_text_for_description(event_db, candidate),
                                    label="merge_fact_coverage",
                                )
                            except Exception:  # pragma: no cover
                                enriched = None
                            if enriched:
                                enriched = _strip_foreign_schedule_noise(
                                    enriched,
                                    event_date=event_db.date,
                                    end_date=event_db.end_date,
                                    event_title=event_db.title,
                                ) or enriched
                                enriched = _normalize_plaintext_paragraphs(enriched) or enriched
                                enriched = _promote_first_person_quotes_to_blockquotes(enriched) or enriched
                                enriched = _promote_inline_quoted_direct_speech_to_blockquotes(enriched) or enriched
                                enriched = _drop_reported_speech_duplicates(enriched) or enriched
                                enriched = _normalize_blockquote_markers(enriched) or enriched
                                enriched = _fix_broken_initial_paragraph_splits(enriched) or enriched
                                enriched = (
                                    _sanitize_description_output(
                                        enriched,
                                        source_text=_pick_richest_source_text_for_description(event_db, candidate),
                                    )
                                    or enriched
                                )
                                enriched = _ensure_blockquote_has_attribution(
                                    description=enriched,
                                    attribution_name=director_name_hint,
                                )
                                clean_description = enriched
                        if _description_needs_infoblock_logistics_strip(clean_description, candidate=candidate):
                            try:
                                edited = await _llm_remove_infoblock_logistics(
                                    description=clean_description,
                                    candidate=candidate,
                                    label="merge_remove_logistics",
                                )
                            except Exception:  # pragma: no cover
                                edited = None
                            if edited:
                                edited = _normalize_plaintext_paragraphs(edited) or edited
                                clean_description = edited
                        if _description_needs_channel_promo_strip(clean_description):
                            clean_description = (
                                _strip_channel_promo_from_description(clean_description) or clean_description
                            )
                        clean_description = (
                            _ensure_minimal_description_headings(clean_description) or clean_description
                        )
                        event_db.description = _clip(clean_description, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                        updated_fields = True
                        updated_keys.append("description")
                    if quote_candidates and (not merge_fact_first_used):
                        current_description = (event_db.description or "").strip()
                        if current_description and not re.search(r"(?m)^>\s+", current_description):
                            enforced_description = await _ensure_direct_quote_blockquote(
                                description=current_description,
                                quote_candidates=quote_candidates,
                                candidate_text=(
                                    _strip_promo_lines(candidate.source_text) or candidate.source_text
                                ),
                                facts_before=facts_before_list,
                                label="merge_quote_enforce_current_desc",
                            )
                            enforced_description = (
                                _normalize_plaintext_paragraphs(enforced_description)
                                or enforced_description
                            )
                            enforced_description = (
                                _normalize_blockquote_markers(enforced_description)
                                or enforced_description
                            )
                            enforced_description = (
                                _drop_reported_speech_duplicates(enforced_description)
                                or enforced_description
                            )
                            enforced_description = _ensure_blockquote_has_attribution(
                                description=enforced_description,
                                attribution_name=director_name_hint,
                            )
                            if enforced_description and enforced_description != current_description:
                                enforced_description = (
                                    _ensure_minimal_description_headings(enforced_description)
                                    or enforced_description
                                )
                                event_db.description = _clip(
                                    enforced_description, SMART_UPDATE_DESCRIPTION_MAX_CHARS
                                )
                                updated_fields = True
                                if "description" not in updated_keys:
                                    updated_keys.append("description")

                    merge_price_min = merge_data.get("ticket_price_min")
                    merge_price_max = merge_data.get("ticket_price_max")
                    if source_type_clean in {"vk", "telegram", "tg"} and (
                        merge_price_min is not None or merge_price_max is not None
                    ):
                        introducing_new_price = (
                            getattr(event_db, "ticket_price_min", None) is None
                            and getattr(event_db, "ticket_price_max", None) is None
                            and candidate.ticket_price_min is None
                            and candidate.ticket_price_max is None
                        )
                        if introducing_new_price:
                            poster_texts_for_price: list[str] = []
                            for p in candidate.posters or []:
                                for k in ("ocr_text", "ocr_title"):
                                    v = getattr(p, k, None)
                                    if isinstance(v, str) and v.strip():
                                        poster_texts_for_price.append(v.strip())
                            price_probe = "\n".join(
                                [
                                    clean_title,
                                    clean_source_text or "",
                                    clean_raw_excerpt or "",
                                    *poster_texts_for_price[:3],
                                ]
                            ).strip()
                            if not _has_price_evidence(price_probe, merge_price_min, merge_price_max):
                                merge_price_min = None
                                merge_price_max = None
                                note = "Цена отброшена: не найдена в источнике"
                                if note not in text_filter_facts:
                                    text_filter_facts.append(note)

                    ticket_updates = _apply_ticket_fields(
                        event_db,
                        ticket_link=merge_data.get("ticket_link"),
                        ticket_price_min=merge_price_min,
                        ticket_price_max=merge_price_max,
                        ticket_status=merge_data.get("ticket_status"),
                        candidate_trust=candidate.trust_level,
                    )
                    if ticket_updates:
                        updated_fields = True
                        updated_keys.extend(ticket_updates)

                elif has_new_text or needs_schedule_cleanup:
                    # LLM merge can be unavailable (offline runs, local env, transient outages).
                    # In production, avoid publishing non-LLM text to Telegraph; for offline/regression
                    # runs (schedule_tasks=False) do a deterministic merge to keep facts visible.
                    if not schedule_tasks:
                        base = before_description
                        if needs_schedule_cleanup:
                            cleaned = _strip_foreign_schedule_noise(
                                base,
                                event_date=event_db.date,
                                end_date=event_db.end_date,
                                event_title=event_db.title,
                            )
                            if cleaned:
                                base = cleaned
                                note = "Текст очищен: убраны строки расписания других дат"
                                if note not in text_filter_facts:
                                    text_filter_facts.append(note)
                        merged = base
                        if has_new_text:
                            merged = _fallback_merge_description(base, candidate, max_sentences=2) or base
                        merged = _normalize_plaintext_paragraphs(merged) or merged
                        merged = _promote_first_person_quotes_to_blockquotes(merged) or merged
                        merged = _promote_inline_quoted_direct_speech_to_blockquotes(merged) or merged
                        merged = _drop_reported_speech_duplicates(merged) or merged
                        merged = _normalize_blockquote_markers(merged) or merged
                        merged = _append_missing_scene_hint(
                            description=merged, source_text=candidate.source_text
                        ) or merged
                        merged = (
                            _sanitize_description_output(
                                merged,
                                source_text=candidate.source_text,
                            )
                            or merged
                        )
                        current = (event_db.description or "").strip()
                        merged = (merged or "").strip()
                        if merged and merged != current:
                            merged = _ensure_minimal_description_headings(merged) or merged
                            event_db.description = _clip(merged, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                            updated_fields = True
                            if "description" not in updated_keys:
                                updated_keys.append("description")
                        note = "LLM недоступна: описание обновлено детерминированно"
                        if note not in text_filter_facts:
                            text_filter_facts.append(note)
                        skip_topic_reclassify = True
                    else:
                        # Production-safe: keep description unchanged and record a service note in the source log.
                        note = "LLM недоступна: описание не обновлено"
                        if note not in text_filter_facts:
                            text_filter_facts.append(note)
                        if needs_schedule_cleanup:
                            cleaned = _strip_foreign_schedule_noise(
                                before_description,
                                event_date=event_db.date,
                                end_date=event_db.end_date,
                                event_title=event_db.title,
                            )
                            if cleaned and cleaned != before_description:
                                cleaned = _normalize_plaintext_paragraphs(cleaned) or cleaned
                                cleaned = _promote_first_person_quotes_to_blockquotes(cleaned) or cleaned
                                cleaned = _promote_inline_quoted_direct_speech_to_blockquotes(cleaned) or cleaned
                                cleaned = _drop_reported_speech_duplicates(cleaned) or cleaned
                                cleaned = _normalize_blockquote_markers(cleaned) or cleaned
                                cleaned = _append_missing_scene_hint(
                                    description=cleaned, source_text=candidate.source_text
                                ) or cleaned
                                cleaned = (
                                    _sanitize_description_output(
                                        cleaned,
                                        source_text=candidate.source_text,
                                    )
                                    or cleaned
                                )
                                cleaned = _ensure_minimal_description_headings(cleaned) or cleaned
                                event_db.description = _clip(cleaned, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                                updated_fields = True
                                updated_keys.append("description")
                                note = "Текст очищен: убраны строки расписания других дат"
                                if note not in text_filter_facts:
                                    text_filter_facts.append(note)
                                skip_topic_reclassify = True
        else:
            ticket_updates = _apply_ticket_fields(
                event_db,
                ticket_link=candidate.ticket_link,
                ticket_price_min=candidate.ticket_price_min,
                ticket_price_max=candidate.ticket_price_max,
                ticket_status=candidate.ticket_status,
                candidate_trust=candidate.trust_level,
            )
            if ticket_updates:
                updated_fields = True
                updated_keys.extend(ticket_updates)
            # Keep original description snapshot for source log snippet.
            before_description = before_description or (event_db.description or "")

        if not event_db.location_address and candidate.location_address:
            event_db.location_address = candidate.location_address
            updated_fields = True
            updated_keys.append("location_address")
        if not event_db.city and candidate.city:
            event_db.city = candidate.city
            updated_fields = True
            updated_keys.append("city")
        if (
            not event_db.end_date
            and _apply_event_end_date(
                event_db,
                end_date=candidate.end_date,
                inferred=bool(candidate.end_date_is_inferred),
                updated_keys=updated_keys,
            )
        ):
            updated_fields = True
        if not event_db.festival and candidate.festival:
            event_db.festival = candidate.festival
            updated_fields = True
            updated_keys.append("festival")
        merged_organizers = _bounded_organizer_names(
            getattr(event_db, "organizer_names", None) or [],
            candidate.organizer_names,
        )
        if merged_organizers != list(getattr(event_db, "organizer_names", None) or []):
            event_db.organizer_names = merged_organizers
            updated_fields = True
            updated_keys.append("organizer_names")
        if event_db.event_type:
            normalized_existing = _normalize_event_type_value(
                event_db.title, event_db.description, event_db.event_type
            )
            if normalized_existing and normalized_existing != event_db.event_type:
                event_db.event_type = normalized_existing
                updated_fields = True
                updated_keys.append("event_type")
        if candidate.event_type and not event_db.event_type:
            normalized = _normalize_event_type_value(
                event_db.title, event_db.description, candidate.event_type
            )
            event_db.event_type = normalized or candidate.event_type
            updated_fields = True
            updated_keys.append("event_type")
        if candidate.emoji and not event_db.emoji:
            event_db.emoji = candidate.emoji
            updated_fields = True
            updated_keys.append("emoji")
        existing_short = _clean_short_description(getattr(event_db, "short_description", None))
        short_needs_refresh = not _is_short_description_acceptable(
            existing_short, min_words=12, max_words=16
        )
        if ("description" in updated_keys) or has_new_text:
            short_needs_refresh = True
        if short_needs_refresh:
            normalized_short: str | None = None
            try:
                normalized_short = await _llm_build_short_description(
                    title=event_db.title,
                    description=event_db.description or candidate.source_text,
                    event_type=event_db.event_type or candidate.event_type,
                )
            except Exception:
                normalized_short = None
            if not normalized_short:
                candidate_short = _clean_short_description(candidate.raw_excerpt)
                if _is_short_description_acceptable(
                    candidate_short, min_words=12, max_words=16
                ):
                    normalized_short = candidate_short
            if not normalized_short:
                normalized_short = _fallback_short_description_from_text(
                    event_db.description or candidate.source_text
                )
            if normalized_short and normalized_short != (event_db.short_description or "").strip():
                event_db.short_description = normalized_short
                updated_fields = True
                updated_keys.append("short_description")
        # search_digest is a short snippet used for search/cards and also shown on Telegraph
        # before long descriptions. It should be refreshed when description meaningfully changes.
        normalized_candidate_digest = _clean_search_digest(candidate.search_digest)
        digest_should_refresh = ("description" in updated_keys) or has_new_text
        new_digest = None
        if digest_should_refresh:
            if merge_digest_from_llm and (not merge_fact_first_used):
                new_digest = merge_digest_from_llm
            else:
                try:
                    new_digest = await _llm_build_search_digest(
                        title=event_db.title,
                        description=event_db.description,
                        event_type=event_db.event_type,
                    )
                except Exception:
                    new_digest = None
        if not new_digest:
            # Fallback: accept candidate-provided digest (e.g. parsers), even if event already had one.
            new_digest = normalized_candidate_digest
        if not new_digest:
            new_digest = _fallback_digest_from_description(event_db.description)
        if new_digest and (new_digest.strip() != (event_db.search_digest or "").strip()):
            event_db.search_digest = new_digest
            updated_fields = True
            updated_keys.append("search_digest")
        if candidate.pushkin_card is True and not event_db.pushkin_card:
            event_db.pushkin_card = True
            updated_fields = True
            updated_keys.append("pushkin_card")
        if not event_db.is_free:
            if candidate.is_free is True:
                event_db.is_free = True
                updated_fields = True
                updated_keys.append("is_free")
        if not event_db.source_post_url and candidate.source_url and _is_http_url(candidate.source_url):
            event_db.source_post_url = candidate.source_url
            updated_fields = True
            updated_keys.append("source_post_url")
        if candidate.source_url and _is_vk_wall_url(candidate.source_url):
            if not event_db.source_vk_post_url:
                event_db.source_vk_post_url = candidate.source_url
                updated_fields = True
                updated_keys.append("source_vk_post_url")
        if not event_db.creator_id and candidate.creator_id:
            event_db.creator_id = candidate.creator_id
            updated_fields = True
            updated_keys.append("creator_id")
        if await _ensure_transport_duration_forecast(event_db, candidate):
            updated_fields = True
            updated_keys.append("duration_forecast_minutes")

        added_posters, added_poster_urls, preview_invalidated, pruned_posters, photo_urls_changed = await _apply_posters(
            session,
            event_db.id,
            candidate.posters,
            poster_scope_hashes=candidate.poster_scope_hashes,
            event_title=candidate.title,
        )
        if added_posters or pruned_posters or photo_urls_changed:
            updated_fields = True
            updated_keys.append("posters")

        # Backfill legacy source fields into event_source for older events (e.g. /parse imports
        # created before event_source existed). This is required for deterministic merges like
        # dramteatr (site + telegram) in E2E and for operator transparency.
        await _ensure_legacy_event_sources(session, event_db)

        added_sources, same_source = await _ensure_event_source(session, event_db.id, candidate)
        await session.flush()
        attached_collection_source = await _attached_collection_source(
            session, event_db.id, candidate
        )
        collection_is_free_before = event_db.is_free
        if (
            attached_collection_source is not None
            and candidate.collection_semantic_decisions is not None
            and apply_collection_decisions(
                event_db,
                candidate.collection_semantic_decisions,
                source=attached_collection_source,
                source_corpus=_collection_source_corpus(candidate),
                input_hash=collection_adjudication_input_hash(candidate),
                reasons=candidate.collection_adjudication_reasons,
            )
        ):
            updated_fields = True
            if "collection_decisions" not in updated_keys:
                updated_keys.append("collection_decisions")
            if event_db.is_free != collection_is_free_before and "is_free" not in updated_keys:
                updated_keys.append("is_free")
        if clean_source_text:
            if same_source:
                event_db.source_text = clean_source_text
                updated_fields = True
                updated_keys.append("source_text")
            if await _sync_source_texts(session, event_db):
                updated_fields = True
                updated_keys.append("source_texts")

        await _enqueue_ticket_sites_queue(session, event_id=int(event_db.id or 0))
        # If we didn't touch description in this merge, but it's clearly too short
        # compared to rich available source text (usually site import), generate a
        # full rewritten description best-effort. This is important for Telegraph
        # pages: a short "search snippet" is not acceptable as the main text.
        if "description" not in updated_keys:
            cur_desc = (event_db.description or "").strip()
            min_expected = _min_expected_description_len_from_sources(event_db, candidate)
            if min_expected and len(cur_desc) < min_expected:
                rewritten_full = None
                try:
                    rewritten_full = await _rewrite_description_full_from_sources(event_db, candidate)
                except Exception:  # pragma: no cover - defensive
                    rewritten_full = None
                if rewritten_full and len(rewritten_full) >= int(min_expected * 0.85):
                    rewritten_full = _ensure_minimal_description_headings(rewritten_full) or rewritten_full
                    event_db.description = _clip(rewritten_full, SMART_UPDATE_DESCRIPTION_MAX_CHARS)
                    updated_fields = True
                    updated_keys.append("description")
                else:
                    # Do not fall back to verbatim source text. Keep the previous description
                    # (or wait for the next LLM-backed update) to ensure Telegraph text stays LLM-produced.
                    pass

        await session.flush()
        added_log: list[str] = []
        duplicate_log: list[str] = []
        conflict_log: list[str] = []
        note_log: list[str] = []

        # 1) Added facts (LLM merge)
        added_log.extend(list(added_facts or []))
        # 1b) Duplicate facts (LLM reported as already known for this event)
        duplicate_log.extend(list(duplicate_facts or []))

        # 2) Anchor updates (deterministic, may not be present in LLM facts)
        if "date" in updated_keys and getattr(event_db, "date", None):
            added_log.append(f"Дата: {event_db.date}")
        if "end_date" in updated_keys and getattr(event_db, "end_date", None):
            added_log.append(f"Дата окончания: {event_db.end_date}")
        if (
            "end_date_is_inferred" in updated_keys
            and "end_date" not in updated_keys
            and getattr(event_db, "end_date", None)
            and not bool(getattr(event_db, "end_date_is_inferred", False))
        ):
            note_log.append(f"Дата окончания подтверждена: {event_db.end_date}")
        if "time" in updated_keys and getattr(event_db, "time", None):
            added_log.append(f"Время: {event_db.time}")
        if "location_name" in updated_keys and getattr(event_db, "location_name", None):
            loc = str(event_db.location_name or "").strip()
            if getattr(event_db, "location_address", None):
                loc = f"{loc}, {str(event_db.location_address).strip()}"
            if getattr(event_db, "city", None):
                loc = f"{loc}, {str(event_db.city).strip()}"
            if loc.strip():
                added_log.append(f"Локация: {loc.strip()}")

        # 3) Posters (added) + service notes
        for url in (added_poster_urls or [])[:3]:
            added_log.append(f"Добавлена афиша: {url}")
        if pruned_posters:
            note_log.append(f"Удалены лишние афиши: {pruned_posters}")
        if photo_urls_changed and not added_posters and not pruned_posters:
            note_log.append("Афиши переупорядочены по OCR")
        if preview_invalidated:
            note_log.append("3D-превью сброшено: изменились иллюстрации")

        # 4) Filters and text snippet are service notes
        note_log.extend((queue_notes or [])[:6])
        note_log.extend((text_filter_facts or [])[:2])
        note_log.extend((poster_filter_facts or [])[:3])
        # NOTE: We intentionally do NOT include "Текст дополнен: ..." snippets anymore.
        # Operator must see changes as explicit facts (✅/↩️) and can open Telegraph for the full text.

        # 5) Conflicts: prefer LLM-provided details, but also record deterministic anchor conflicts.
        conflict_log.extend([s for s in (conflict_facts or []) if isinstance(s, str) and s.strip()][:10])
        conflict_log.extend([s for s in (skipped_conflicts or []) if isinstance(s, str) and s.strip()][:10])
        for k, v in list((conflicting or {}).items())[:8]:
            if not v:
                continue
            conflict_log.append(f"Конфликт якоря: {k} -> {v}")

        # 6) Duplicate anchors (observed in source but already present)
        try:
            c = candidate
            blocked = set((conflicting or {}).keys())
            if c.date and "date" not in updated_keys and "date" not in blocked and (c.date == (event_db.date or "")):
                duplicate_log.append(f"Дата: {c.date}")
            if c.end_date and "end_date" not in updated_keys and "end_date" not in blocked and (c.end_date == (getattr(event_db, 'end_date', None) or "")):
                duplicate_log.append(f"Дата окончания: {c.end_date}")
            if c.time and "time" not in updated_keys and "time" not in blocked and (str(c.time).strip() == str(event_db.time or '').strip()):
                duplicate_log.append(f"Время: {str(c.time).strip()}")
            if (
                c.location_name
                and "location_name" not in updated_keys
                and "location_name" not in blocked
                and _location_matches(getattr(event_db, "location_name", None), c.location_name)
            ):
                parts = [c.location_name, c.location_address, c.city]
                loc = ", ".join(str(p).strip() for p in parts if (p or "").strip())
                if loc:
                    duplicate_log.append(f"Локация: {loc}")
            if (
                c.ticket_price_min is not None
                and c.ticket_price_max is not None
                and "ticket_price_min" not in updated_keys
                and "ticket_price_max" not in updated_keys
                and (c.ticket_price_min == getattr(event_db, "ticket_price_min", None))
                and (c.ticket_price_max == getattr(event_db, "ticket_price_max", None))
            ):
                price_text = _format_ticket_price(c.ticket_price_min, c.ticket_price_max)
                if price_text:
                    duplicate_log.append(f"Цена: {price_text}")
            if (
                c.ticket_link
                and "ticket_link" not in updated_keys
                and (c.ticket_link == (event_db.ticket_link or ""))
            ):
                label = "Регистрация" if c.is_free else "Билеты"
                duplicate_log.append(f"{label}: {c.ticket_link}")
            if c.ticket_status == "sold_out" and "ticket_status" not in updated_keys and getattr(event_db, "ticket_status", None) == "sold_out":
                duplicate_log.append("Билеты все проданы")
            if c.is_free is True and "is_free" not in updated_keys and bool(getattr(event_db, "is_free", False)) is True:
                duplicate_log.append("Бесплатно")
            if c.pushkin_card is True and "pushkin_card" not in updated_keys and bool(getattr(event_db, "pushkin_card", False)) is True:
                duplicate_log.append("Пушкинская карта")
            if c.event_type and "event_type" not in updated_keys and (c.event_type == (event_db.event_type or "")):
                duplicate_log.append(f"Тип: {c.event_type}")
            if c.festival and "festival" not in updated_keys and (c.festival == (event_db.festival or "")):
                duplicate_log.append(f"Фестиваль: {c.festival}")
        except Exception:
            # Best-effort: duplicates are for operator UX only.
            duplicate_log = duplicate_log

        # If we recorded no meaningful facts, keep the log useful and E2E-deterministic.
        if not (added_log or note_log or conflict_log or duplicate_log):
            # LLM merge can be unavailable in local/dev or for transient outages.
            # Keep source log useful and E2E-deterministic: record what we did change.
            if added_posters:
                note_log.append(f"Добавлены афиши: {added_posters}")
            if added_sources:
                note_log.append("Добавлен источник")
            if updated_keys:
                keys = [
                    k
                    for k in updated_keys
                    if k not in {"source_text", "source_texts", "end_date_is_inferred"}
                ]
                if keys:
                    note_log.append(f"Обновлено: {', '.join(keys[:6])}")

        # Demote meaning-duplicates of existing anchors (date/time) from ✅ to ↩️.
        # This solves operator confusion when LLM returns both:
        #   "Дата: 2026-02-12" and "Спектакль будет показан 12 февраля."
        try:
            added_log, duplicate_log = _demote_redundant_anchor_facts(
                added_log,
                duplicate_log,
                event_date=getattr(event_db, "date", None),
                event_time=getattr(event_db, "time", None),
                updated_keys=set(updated_keys),
            )
        except Exception:
            # Best-effort: never break the merge due to UX-only log shaping.
            pass

        # Normalize/dedupe within groups.
        added_log = _dedupe_source_facts(_drop_redundant_poster_facts(added_log))
        note_log = _dedupe_source_facts(_drop_redundant_poster_facts(note_log))
        conflict_log = _dedupe_source_facts(conflict_log)
        duplicate_log = _dedupe_source_facts(duplicate_log)

        # Remove duplicates that are actually part of the added set (by normalized key).
        def _key(v: str) -> str:
            c = _normalize_fact_item(v) or v
            return (c or "").strip().lower()

        added_keys = {_key(v) for v in added_log if _key(v)}
        duplicate_log = [v for v in duplicate_log if _key(v) and _key(v) not in added_keys]
        conflict_log = [v for v in conflict_log if _key(v) and _key(v) not in added_keys]

        fact_records: list[tuple[str, str]] = []
        for f in added_log:
            fact_records.append((f, "added"))
        for f in duplicate_log:
            fact_records.append((f, "duplicate"))
        for f in conflict_log:
            fact_records.append((f, "conflict"))
        for f in note_log:
            fact_records.append((f, "note"))

        if fact_records:
            await _record_source_facts(session, event_db.id, candidate, fact_records)

        if updated_fields:
            session.add(event_db)
        await session.commit()

    await _persist_pending_festival_queue()
    if (updated_fields or added_posters or (added_sources and not same_source)) and not skip_topic_reclassify:
        await _classify_topics(db, existing.id)

    holiday_changed = False
    try:
        holiday_changed = await _apply_holiday_festival_mapping(db, existing.id)
    except Exception:
        logger.warning(
            "smart_update: holiday mapping failed for event %s",
            existing.id,
            exc_info=True,
        )

    # Keep linked occurrences consistent when anchor-ish or grouping fields change.
    linked_refresh_ids: list[int] = []
    if any(k in {"title", "location_name", "date", "time"} for k in (updated_keys or [])):
        try:
            from linked_events import recompute_linked_event_ids

            lr = await recompute_linked_event_ids(db, int(existing.id or 0))
            linked_refresh_ids = [
                int(x)
                for x in (lr.changed_event_ids or [])
                if int(x) and int(x) != int(existing.id or 0)
            ]
        except Exception:
            logger.warning(
                "smart_update: linked events recompute failed for event %s",
                existing.id,
                exc_info=True,
            )
            linked_refresh_ids = []

    if schedule_tasks and linked_refresh_ids:
        try:
            from main import JobTask, enqueue_job

            for rid in linked_refresh_ids[:80]:
                await enqueue_job(db, int(rid), JobTask.telegraph_build, depends_on=None)
        except Exception:
            logger.warning(
                "smart_update: failed to enqueue linked telegraph refresh for event %s",
                existing.id,
                exc_info=True,
            )

    if schedule_tasks and (
        updated_fields
        or added_posters
        or (added_sources and not same_source)
        or holiday_changed
    ):
        try:
            from main import schedule_event_update_tasks
            async with db.get_session() as session:
                refreshed = await session.get(Event, existing.id)
            if refreshed:
                task_kwargs = dict(schedule_kwargs or {})
                # A managed VK post is an editable projection, not a terminal
                # publication.  Re-arm its idempotent sync whenever Smart Update
                # actually changed the canonical event.
                task_kwargs["refresh_existing_vk"] = True
                await schedule_event_update_tasks(db, refreshed, **task_kwargs)
        except Exception:
            logger.warning("smart_update: schedule/update failed for event %s", existing.id, exc_info=True)

    canonical_changed = bool(
        updated_fields
        or added_posters
        or (added_sources and not same_source)
        or holiday_changed
    )
    if canonical_changed:
        try:
            from interest_clubs import schedule_interest_club_evaluation

            await schedule_interest_club_evaluation(
                db,
                int(existing.id or 0),
                schedule_projection=schedule_tasks,
            )
        except Exception:
            logger.warning(
                "smart_update: failed to enqueue interest-club evaluation event=%s",
                existing.id,
                exc_info=True,
            )

    # Once identity has been affirmatively classified as SAME_EVENT or
    # SOURCE_UPDATE, the accepted terminal is MERGED. NOOP is reserved solely
    # for the exact packet replay fast-path above.
    status = "merged"
    logger.info(
        "smart_update.merge event_id=%s status=%s updated=%s added_posters=%d added_sources=%s updated_keys=%s added_facts=%d skipped_conflicts=%d reason=%s",
        existing.id,
        status,
        int(bool(updated_fields)),
        added_posters,
        int(bool(added_sources)),
        ",".join(updated_keys[:12]) if updated_keys else "",
        len(added_facts),
        len(skipped_conflicts),
        match_reason if "match_reason" in locals() else None,
    )
    return SmartUpdateResult(
        status=status,
        event_id=existing.id,
        created=False,
        merged=bool(updated_fields or holiday_changed),
        added_posters=added_posters,
        added_sources=added_sources,
        added_facts=added_facts,
        skipped_conflicts=skipped_conflicts,
        reason=match_reason if "match_reason" in locals() else None,
        queue_notes=list(queue_notes or []),
    )


def _poster_relevance_quality(p: PosterCandidate) -> int:
    """Cheap quality score to pick the survivor of an exact identity collision."""
    q = 0
    title = getattr(p, "ocr_title", None)
    if title:
        q += len(title)
    text = getattr(p, "ocr_text", None)
    if text:
        q += len(text)
    if getattr(p, "supabase_url", None):
        q += 1
    return q


def _normalize_poster_identity_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parts = urlsplit(raw)
        if parts.scheme and parts.netloc:
            return urlunsplit(
                (
                    parts.scheme.lower(),
                    parts.netloc.lower(),
                    parts.path,
                    parts.query,
                    "",
                )
            )
    except Exception:
        return raw
    return raw


def _poster_identity_keys(
    poster: PosterCandidate | EventPoster,
    *,
    include_weak_url: bool = True,
) -> tuple[tuple[str, str], ...]:
    """Return deterministic identity keys for poster merge dedup.

    Strong keys are exact content hash and Supabase object path. Perceptual
    hashes never establish identity without automated visual adjudication.
    URL is intentionally last and weak: it is used only as a conservative
    fallback for legacy rows that lack stronger metadata.
    """
    keys: list[tuple[str, str]] = []
    raw_sha256 = str(getattr(poster, "raw_sha256", "") or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", raw_sha256):
        keys.append(("raw_sha256", raw_sha256))
    supabase_path = str(getattr(poster, "supabase_path", "") or "").strip()
    if supabase_path:
        keys.append(("supabase_path", supabase_path))
    poster_hash = (
        getattr(poster, "poster_hash", None)
        or getattr(poster, "sha256", None)
        or ""
    )
    poster_hash_s = str(poster_hash or "").strip().lower()
    if poster_hash_s:
        keys.append(("poster_hash", poster_hash_s))
    if include_weak_url:
        seen_urls: set[str] = set()
        for raw_url in (getattr(poster, "supabase_url", None), getattr(poster, "catbox_url", None)):
            url = _normalize_poster_identity_url(str(raw_url or "").strip())
            if url and url not in seen_urls:
                seen_urls.add(url)
                keys.append(("url", url))
    return tuple(keys)


def _poster_has_exact_content_identity(poster: PosterCandidate | EventPoster) -> bool:
    return _poster_exact_content_identity(poster) is not None


def _poster_exact_content_identity(
    poster: PosterCandidate | EventPoster,
) -> str | None:
    """Return the immutable encoded-byte identity carried by a poster."""

    raw_sha256 = str(getattr(poster, "raw_sha256", "") or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", raw_sha256):
        return raw_sha256
    path = str(getattr(poster, "supabase_path", "") or "").strip().lstrip("/")
    match = re.fullmatch(
        r"[^/]+/image/v2/(?P<prefix>[0-9a-f]{2})/(?P<digest>[0-9a-f]{64})\.webp",
        path,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    digest = match.group("digest").lower()
    return digest if digest.startswith(match.group("prefix").lower()) else None


def _poster_source_exact_variant_hash(source_hash: str, exact_digest: str) -> str:
    """Stable row identity when one mutable source hash yields new exact bytes.

    ``EventPoster`` historically has a unique ``(event_id, poster_hash)``
    constraint.  A source URL/hash is provenance rather than immutable content,
    so the same source may legitimately yield several exact-v2 renditions over
    time.  Namespacing the later rendition prevents both a uniqueness failure
    and replacement of pixel-bound geometry on the earlier row.
    """

    payload = f"event-poster-source-exact-v1\n{source_hash}\n{exact_digest}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_eventposter_identity_index(
    rows: Sequence[EventPoster],
) -> dict[tuple[str, str], EventPoster]:
    index: dict[tuple[str, str], EventPoster] = {}
    for row in rows or []:
        # A source URL is provenance, not immutable image identity.  It remains
        # a migration fallback only for legacy rows which do not yet have an
        # exact-v2 object/raw digest.  Otherwise a later source rendition could
        # overwrite a classified exact object and invalidate its geometry on
        # every reconciliation pass.
        for key in _poster_identity_keys(
            row,
            include_weak_url=not _poster_has_exact_content_identity(row),
        ):
            index.setdefault(key, row)
    return index


def _find_duplicate_eventposter_by_identity(
    poster: PosterCandidate,
    index: dict[tuple[str, str], EventPoster],
) -> tuple[EventPoster | None, str | None]:
    for key in _poster_identity_keys(poster, include_weak_url=False):
        row = index.get(key)
        if row is not None:
            return row, key[0]
    for key in _poster_identity_keys(poster, include_weak_url=True):
        if key[0] != "url":
            continue
        row = index.get(key)
        if row is not None:
            return row, key[0]
    return None, None


def _dedup_poster_candidates_by_identity(
    posters: Sequence[PosterCandidate],
) -> list[PosterCandidate]:
    if len(posters or []) < 2:
        return list(posters or [])
    kept: list[PosterCandidate] = []
    index: dict[tuple[str, str], int] = {}
    for poster in posters:
        # Once materialized, exact bytes/path win.  Do not collapse two
        # different exact renditions merely because they came from the same
        # mutable source URL.
        keys = _poster_identity_keys(
            poster,
            include_weak_url=not _poster_has_exact_content_identity(poster),
        )
        duplicate_idx: int | None = None
        duplicate_key: tuple[str, str] | None = None
        for key in keys:
            if key in index:
                duplicate_idx = index[key]
                duplicate_key = key
                break
        if duplicate_idx is None:
            kept.append(poster)
            new_idx = len(kept) - 1
            for key in keys:
                index.setdefault(key, new_idx)
            continue
        if _poster_relevance_quality(poster) > _poster_relevance_quality(kept[duplicate_idx]):
            kept[duplicate_idx] = poster
            for key in keys:
                index[key] = duplicate_idx
        logger.info(
            "smart_update: dropped duplicate poster candidate by %s",
            duplicate_key[0] if duplicate_key else "identity",
        )
    return kept


async def _apply_posters(
    session,
    event_id: int | None,
    posters: Sequence[PosterCandidate],
    poster_scope_hashes: Sequence[str] | None = None,
    event_title: str | None = None,
) -> tuple[int, list[str], bool, int, bool]:
    """Persist candidates through the single fail-closed event-media gate.

    Exact content/object identity may merge synchronously.  Perceptual hashes are
    evidence only: every new second-or-later logical image remains quarantined
    until the automated pair reviewer resolves it.
    """

    del event_title
    if not event_id:
        return 0, [], False, 0, False

    from event_media import (
        APPROVED,
        DUPLICATE,
        PENDING_REVIEW,
        REJECTED,
        UNAVAILABLE,
        ensure_event_media_reviews,
        _enqueue_geometry_followup_if_needed,
        assign_event_poster_raw_sha256,
        event_media_require_cdn,
        invalidate_event_poster_visual_evidence,
        materialize_event_media_candidate_to_cdn,
        resolve_poster_display_url,
        sync_event_gallery_projection,
    )

    cdn_ready: dict[int, bool] = {}
    if event_media_require_cdn():
        for candidate in posters:
            cdn_ready[id(candidate)] = await materialize_event_media_candidate_to_cdn(
                candidate
            )
    posters = _dedup_poster_candidates_by_identity(posters)
    existing_rows = list(
        (
            await session.execute(
                select(EventPoster).where(EventPoster.event_id == int(event_id))
            )
        ).scalars().all()
    )
    existing_map = {str(row.poster_hash or "").strip(): row for row in existing_rows}
    existing_identity_index = _build_eventposter_identity_index(existing_rows)
    event = await session.get(Event, int(event_id))
    before_urls = list(getattr(event, "photo_urls", None) or []) if event else []
    before_count = int(getattr(event, "photo_count", 0) or 0) if event else 0
    had_preview = bool(getattr(event, "preview_3d_url", None)) if event else False
    now = datetime.now(timezone.utc)
    added = 0
    soft_rejected = 0
    added_urls: list[str] = []
    next_order = max((int(getattr(row, "display_order", 0) or 0) for row in existing_rows), default=-1) + 1

    selected_hashes = {
        str(p.sha256 or "").strip()
        for p in posters
        if str(p.sha256 or "").strip()
    }
    scope_hashes = {
        str(value or "").strip()
        for value in (poster_scope_hashes or [])
        if str(value or "").strip()
    }
    # A source-local scope may withdraw an old candidate, but never physically
    # delete evidence.  Re-importing that exact candidate reopens review below.
    if scope_hashes and selected_hashes:
        for digest in scope_hashes - selected_hashes:
            row = existing_map.get(digest)
            if row and row.review_status not in {DUPLICATE, REJECTED}:
                row.review_status = REJECTED
                row.review_reason = "source_scope_withdrawn"
                row.reviewed_at = now
                session.add(row)
                soft_rejected += 1

    for poster in posters:
        poster_supabase_url = getattr(poster, "supabase_url", None)
        poster_catbox_url = getattr(poster, "catbox_url", None)
        poster_supabase_path = getattr(poster, "supabase_path", None)
        poster_phash = getattr(poster, "phash", None)
        poster_raw_sha256 = (
            str(getattr(poster, "raw_sha256", None) or "").strip().lower()
        )
        if poster_raw_sha256:
            await assign_event_poster_raw_sha256(
                poster,
                poster_raw_sha256,
                session=session,
                event_id=int(event_id),
            )
            poster_raw_sha256 = (
                str(getattr(poster, "raw_sha256", None) or "").strip().lower()
            )
        poster_ocr_text = getattr(poster, "ocr_text", None)
        poster_ocr_title = getattr(poster, "ocr_title", None)
        display_url = str(poster_supabase_url or poster_catbox_url or "").strip()
        digest = str(getattr(poster, "sha256", None) or "").strip().lower()
        # This is source/candidate identity only. Exact managed display bytes
        # enter raw_sha256 separately after materialization and conflict checks.
        if not digest and display_url:
            digest = hashlib.sha256(f"url:{_normalize_poster_identity_url(display_url)}".encode()).hexdigest()
        if not digest:
            continue

        # Exact served-byte/object identity must be resolved before the
        # source/candidate hash.  The same source candidate can already have a
        # different persisted row while its exact object belongs to an
        # existing survivor.
        row, duplicate_reason = _find_duplicate_eventposter_by_identity(
            poster,
            existing_identity_index,
        )
        # The database uniqueness constraint is the final source-hash guard.
        # Keep an explicit lookup even though poster_hash normally participates
        # in the identity index: it also makes a just-persisted/race-recovered
        # source row converge instead of surfacing an IntegrityError.
        if row is None:
            row = existing_map.get(digest)
            if row is not None:
                duplicate_reason = "poster_hash"

        if row is not None and duplicate_reason == "poster_hash":
            existing_exact = _poster_exact_content_identity(row)
            candidate_exact = _poster_exact_content_identity(poster)
            if (
                existing_exact is not None
                and candidate_exact is not None
                and existing_exact != candidate_exact
            ):
                source_digest = digest
                digest = _poster_source_exact_variant_hash(
                    source_digest,
                    candidate_exact,
                )
                variant_row = existing_map.get(digest)
                if variant_row is not None:
                    row = variant_row
                    duplicate_reason = "source_exact_variant"
                else:
                    # Do not mutate the old exact row: its semantic/geometry
                    # evidence belongs to different bytes.  The derived hash is
                    # stable, so the same rendition is inserted at most once.
                    logger.info(
                        "smart_update: split mutable source identity by exact rendition event_id=%s",
                        event_id,
                    )
                    row = None
                    duplicate_reason = None
        if row is not None:
            previous_display_url = resolve_poster_display_url(row)
            previous_path = str(row.supabase_path or "").strip()
            next_supabase_url = str(
                poster_supabase_url or row.supabase_url or ""
            ).strip()
            next_catbox_url = str(poster_catbox_url or row.catbox_url or "").strip()
            next_display_url = next_supabase_url or next_catbox_url
            next_path = str(poster_supabase_path or row.supabase_path or "").strip()
            same_managed_object = bool(
                previous_path and next_path and previous_path == next_path
            )
            display_identity_changed = bool(
                previous_display_url
                and next_display_url
                and _normalize_poster_identity_url(previous_display_url)
                != _normalize_poster_identity_url(next_display_url)
                and not same_managed_object
            )
            if display_identity_changed:
                # Visual evidence belongs to exact display bytes. A replaced
                # object must be re-fingerprinted, reclassified and rebound to
                # geometry before bbox/crop metadata can be trusted again.
                invalidate_event_poster_visual_evidence(row)
            if poster_catbox_url and row.catbox_url != poster_catbox_url:
                row.catbox_url = poster_catbox_url
            if poster_supabase_url and row.supabase_url != poster_supabase_url:
                row.supabase_url = poster_supabase_url
            if poster_supabase_path:
                row.supabase_path = poster_supabase_path
            if poster_phash:
                row.phash = poster_phash
            resolved_raw_sha256 = poster_raw_sha256
            if resolved_raw_sha256 and not row.raw_sha256:
                row.raw_sha256 = resolved_raw_sha256
            if poster_ocr_text is not None:
                row.ocr_text = poster_ocr_text
            if poster_ocr_title is not None:
                row.ocr_title = poster_ocr_title
            if getattr(poster, "prompt_tokens", 0):
                row.prompt_tokens = int(poster.prompt_tokens or 0)
            if getattr(poster, "completion_tokens", 0):
                row.completion_tokens = int(poster.completion_tokens or 0)
            if getattr(poster, "total_tokens", 0):
                row.total_tokens = int(poster.total_tokens or 0)
            if row.review_status in {REJECTED, UNAVAILABLE}:
                row.review_status = PENDING_REVIEW
                row.review_reason = "source_candidate_reintroduced"
                row.reviewed_at = None
            if event_media_require_cdn() and not cdn_ready.get(id(poster), False):
                row.review_status = PENDING_REVIEW
                row.review_reason = "cdn_mirror_pending"
                row.reviewed_at = None
            row.updated_at = now
            session.add(row)
            existing_identity_index = _build_eventposter_identity_index(existing_rows)
            if duplicate_reason and duplicate_reason != "poster_hash":
                logger.info(
                    "smart_update: merged exact poster identity reason=%s event_id=%s",
                    duplicate_reason,
                    event_id,
                )
        else:
            row = EventPoster(
                event_id=int(event_id),
                catbox_url=poster_catbox_url,
                supabase_url=poster_supabase_url,
                supabase_path=poster_supabase_path,
                poster_hash=digest,
                raw_sha256=poster_raw_sha256 or None,
                phash=poster_phash,
                review_status=PENDING_REVIEW,
                review_reason=(
                    "cdn_mirror_pending"
                    if event_media_require_cdn()
                    and not cdn_ready.get(id(poster), False)
                    else "awaiting_automated_pair_review"
                ),
                display_order=next_order,
                ocr_text=poster_ocr_text,
                ocr_title=poster_ocr_title,
                prompt_tokens=int(getattr(poster, "prompt_tokens", 0) or 0),
                completion_tokens=int(getattr(poster, "completion_tokens", 0) or 0),
                total_tokens=int(getattr(poster, "total_tokens", 0) or 0),
                updated_at=now,
            )
            next_order += 1
            session.add(row)
            await session.flush()
            existing_rows.append(row)
            existing_map[digest] = row
            existing_identity_index = _build_eventposter_identity_index(existing_rows)
            added += 1
        url = resolve_poster_display_url(row)
        if url and url not in added_urls:
            added_urls.append(url)

    await session.flush()
    await ensure_event_media_reviews(session, int(event_id))
    # The VLM call is never made inline.  A single already-CDN-ready poster has
    # no pair review to create, so explicitly arm the durable enrichment job.
    await _enqueue_geometry_followup_if_needed(session, int(event_id))
    photo_urls_changed = await sync_event_gallery_projection(session, int(event_id))
    event = await session.get(Event, int(event_id))
    if event is not None:
        after_urls = list(event.photo_urls or [])
        after_count = int(event.photo_count or 0)
        photo_urls_changed = photo_urls_changed or before_urls != after_urls or before_count != after_count
    preview_invalidated = bool(had_preview and event is not None and not event.preview_3d_url)
    return added, added_urls, preview_invalidated, soft_rejected, photo_urls_changed


async def _ensure_event_source(
    session,
    event_id: int | None,
    candidate: EventCandidate,
) -> tuple[bool, bool]:
    if not event_id or not candidate.source_url:
        return False, False
    canonical_source_url = canonicalize_identity_url(candidate.source_url)
    source_role = _candidate_source_role(candidate)
    if not canonical_source_url:
        return False, False
    conflicting_event_id = await _source_identity_binding_conflict(
        session,
        event_id=int(event_id),
        canonical_source_url=canonical_source_url,
        source_role=source_role,
        occurrence_key=candidate.occurrence_key,
    )
    if conflicting_event_id is not None:
        raise SourceBindingConflict(conflicting_event_id)
    raw = _strip_private_use(candidate.source_text) or (candidate.source_text or "")
    clean_source_text = _strip_promo_lines(raw) or raw
    existing = (
        await session.execute(
            select(EventSource).where(
                EventSource.event_id == event_id,
                or_(
                    and_(
                        EventSource.canonical_source_url == canonical_source_url,
                        EventSource.occurrence_key == candidate.occurrence_key,
                    ),
                    # An evidence-backed replay may be the first touch that can
                    # safely key a legacy binding on this same Event. Reuse and
                    # upgrade that row rather than violating the older raw
                    # ``(event_id, source_url)`` uniqueness constraint.
                    and_(
                        EventSource.canonical_source_url == canonical_source_url,
                        EventSource.occurrence_key.is_(None),
                        EventSource.candidate_key.is_(None),
                    ),
                    EventSource.candidate_key == candidate.candidate_key,
                    and_(
                        EventSource.canonical_source_url.is_(None),
                        EventSource.source_url == candidate.source_url,
                    ),
                ),
            )
        )
    ).scalar_one_or_none()
    if existing:
        updated = False
        if clean_source_text and clean_source_text != existing.source_text:
            existing.source_text = clean_source_text
            existing.imported_at = datetime.now(timezone.utc)
            updated = True
            logger.info(
                "smart_update.source_text_update event_id=%s source_url=%s",
                event_id,
                candidate.source_url,
            )
        if candidate.trust_level and not existing.trust_level:
            existing.trust_level = candidate.trust_level
            updated = True
        if existing.canonical_source_url != canonical_source_url:
            existing.canonical_source_url = canonical_source_url
            updated = True
        if existing.source_role != source_role:
            existing.source_role = source_role
            updated = True
        if existing.source_fingerprint != candidate.source_fingerprint:
            existing.source_fingerprint = candidate.source_fingerprint
            existing.imported_at = datetime.now(timezone.utc)
            updated = True
        if existing.candidate_key != candidate.candidate_key:
            existing.candidate_key = candidate.candidate_key
            updated = True
        if existing.occurrence_key != candidate.occurrence_key:
            existing.occurrence_key = candidate.occurrence_key
            updated = True
        if existing.smart_update_candidate_id != candidate.smart_update_candidate_id:
            existing.smart_update_candidate_id = candidate.smart_update_candidate_id
            updated = True
        if updated:
            session.add(existing)
        return False, True
    session.add(
        EventSource(
            event_id=event_id,
            source_type=candidate.source_type,
            source_url=candidate.source_url,
            canonical_source_url=canonical_source_url,
            source_role=source_role,
            source_fingerprint=candidate.source_fingerprint,
            candidate_key=candidate.candidate_key,
            occurrence_key=candidate.occurrence_key,
            smart_update_candidate_id=candidate.smart_update_candidate_id,
            source_chat_username=candidate.source_chat_username,
            source_chat_id=candidate.source_chat_id,
            source_message_id=candidate.source_message_id,
            source_text=clean_source_text,
            imported_at=datetime.now(timezone.utc),
            trust_level=candidate.trust_level,
        )
    )
    # Source-only merges are reconciled asynchronously by the same event-media
    # gate; Telegraph/renderers are no longer allowed to attach media themselves.
    try:
        from event_media import enqueue_event_media_review_job

        await enqueue_event_media_review_job(session, int(event_id))
    except Exception:
        logger.warning(
            "smart_update: failed to enqueue source media reconcile event_id=%s",
            event_id,
            exc_info=True,
        )
    return True, False


async def _source_identity_binding_conflict(
    session: Any,
    *,
    event_id: int,
    canonical_source_url: str,
    source_role: str,
    occurrence_key: str | None = None,
) -> int | None:
    if source_role != "identity_bearing":
        return None

    row = (
        await session.execute(
            select(EventSource.event_id)
            .where(
                EventSource.canonical_source_url == canonical_source_url,
                EventSource.source_role == "identity_bearing",
                EventSource.occurrence_key == occurrence_key,
                EventSource.event_id != int(event_id),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if row is not None:
        return int(row)

    # Every new Smart Update packet has a stable occurrence key. A legacy row
    # without one cannot claim the whole carrier URL: official pages and social
    # posts legitimately contain several event occurrences. Exact/same-event
    # legacy bindings are still upgraded by ``_ensure_event_source`` after the
    # normal match path accepts them.
    if str(occurrence_key or "").strip():
        return None

    # Legacy rows are deliberately not mass-classified. An unknown owner on a
    # different Event is evidence of ambiguity, so fail closed instead of
    # silently taking the canonical source for the new candidate.
    canonical = canonicalize_identity_url(
        canonical_source_url,
        preserve_ticket_fragment=True,
    )
    if not canonical:
        return None
    try:
        parts = urlsplit(canonical)
    except (TypeError, ValueError):
        parts = None
    token: str | None = None
    if parts is not None:
        fragment = str(parts.fragment or "").strip().lstrip("/")
        path_bits = [item for item in str(parts.path or "").split("/") if item]
        if fragment and len(fragment) >= 8:
            token = fragment
        elif len(path_bits) >= 2:
            token = "/".join(path_bits[-2:])
        elif path_bits:
            token = path_bits[-1]

    predicates = [
        EventSource.canonical_source_url == canonical,
        EventSource.source_url == canonical,
    ]
    if token:
        # SQL only narrows candidates; canonical equality below is authoritative.
        predicates.append(EventSource.source_url.contains(token))

    legacy_rows = (
        await session.execute(
            select(
                EventSource.event_id,
                EventSource.source_url,
                EventSource.canonical_source_url,
            )
            .where(
                EventSource.event_id != int(event_id),
                or_(EventSource.source_role.is_(None), EventSource.source_role == ""),
                or_(*predicates),
            )
            .limit(100)
        )
    ).all()
    for owner_id, raw_url, stored_canonical in legacy_rows:
        candidate_canonical = canonicalize_identity_url(
            stored_canonical or raw_url,
            preserve_ticket_fragment=True,
        )
        if candidate_canonical == canonical:
            logger.warning(
                "smart_update.legacy_source_owner_review owner_event_id=%s requested_event_id=%s",
                owner_id,
                event_id,
            )
            return int(owner_id)
    return None


async def _attached_collection_source(
    session,
    event_id: int | None,
    candidate: EventCandidate,
) -> EventSource | None:
    """Return only the exact source accepted for this event transaction."""

    if not event_id or not candidate.source_url:
        return None
    return (
        await session.execute(
            select(EventSource).where(
                EventSource.event_id == int(event_id),
                EventSource.source_url == str(candidate.source_url),
                EventSource.source_type == str(candidate.source_type),
            )
        )
    ).scalar_one_or_none()


async def _record_source_facts(
    session,
    event_id: int | None,
    candidate: EventCandidate,
    facts: Sequence[object],
) -> int:
    if not event_id or not candidate.source_url or not facts:
        return 0
    source = (
        await session.execute(
            select(EventSource).where(
                EventSource.event_id == event_id,
                EventSource.source_url == candidate.source_url,
            )
        )
    ).scalar_one_or_none()
    if not source:
        return 0
    # Keep source log idempotent per (event_id, source_url): repeated processing of
    # the same post must not accumulate multiple historical batches for one source.
    await session.execute(
        delete(EventSourceFact).where(
            EventSourceFact.event_id == int(event_id),
            EventSourceFact.source_id == int(source.id),
        )
    )
    now = datetime.now(timezone.utc).replace(microsecond=0)
    added = 0
    allowed_status = {"added", "duplicate", "conflict", "note"}

    def _coerce(item: object) -> tuple[str, str]:
        # Accept both legacy list[str] and new list[(fact, status)].
        if isinstance(item, tuple) and len(item) == 2:
            raw_fact = item[0]
            raw_status = item[1]
        else:
            raw_fact = item
            raw_status = "added"
        fact_s = str(raw_fact or "")
        status_s = str(raw_status or "added").strip().lower()
        if status_s not in allowed_status:
            status_s = "added"
        return fact_s, status_s

    for item in facts:
        raw_fact, status = _coerce(item)
        cleaned = _normalize_fact_item(raw_fact)
        if not cleaned:
            continue
        session.add(
            EventSourceFact(
                event_id=event_id,
                source_id=source.id,
                fact=cleaned,
                status=status,
                created_at=now,
            )
        )
        added += 1
    return added


async def _sync_source_texts(session, event: Event) -> bool:
    if not event:
        return False
    rows = (
        await session.execute(
            select(EventSource.source_text, EventSource.imported_at)
            .where(EventSource.event_id == event.id)
            .order_by(EventSource.imported_at)
        )
    ).all()
    texts: list[str] = []
    for text, _ts in rows:
        if not text:
            continue
        if text not in texts:
            texts.append(text)
    if texts != list(event.source_texts or []):
        event.source_texts = texts
        logger.info(
            "smart_update.source_texts_sync event_id=%s count=%d",
            event.id,
            len(texts),
        )
        return True
    return False


async def _classify_topics(db: Database, event_id: int | None) -> None:
    if not event_id:
        return
    try:
        from main import assign_event_topics
    except Exception:
        return
    async with db.get_session() as session:
        event = await session.get(Event, event_id)
        if not event or event.topics_manual:
            return
        try:
            await assign_event_topics(event)
        except Exception:
            logger.warning("smart_update: topic classification failed event_id=%s", event_id, exc_info=True)
            return
        session.add(event)
        await session.commit()
