from __future__ import annotations

import json
import logging
import os
import re
import hashlib
from html import escape
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from db import Database
from models import Event
from runtime import require_main_attr

logger = logging.getLogger(__name__)

PROFILE_PROD = "prod"
PROFILE_DEBUG = "debug"
STATUS_OPEN = "open"
STATUS_SKIPPED_LOW_INVENTORY = "skipped_low_inventory"
STATUS_SKIPPED_LOW_POPULARITY_INVENTORY = "skipped_low_popularity_inventory"
STATUS_SKIPPED_TOPIC_UNDERFILL = "skipped_topic_underfill"
STATUS_SKIPPED_NO_VOTES = "skipped_no_votes"
STATUS_SKIPPED_NO_CANDIDATE = "skipped_no_candidate"
STATUS_SKIPPED_FEEDBACK_OTHER = "skipped_feedback_other"
STATUS_FORWARDED = "forwarded"
STATUS_FAILED = "failed"
EVENT_LINK_PLACEHOLDER = "{{EVENT_LINK}}"
FEEDBACK_OPTION_KEY = "feedback_other"
FEEDBACK_OPTION_TEXT = "Другое — в этот раз темы не попали"
FREE_TOPIC_MIN_FREE_EVENTS = 2
FREE_TOPIC_MIN_OPTIONS = 6
TOPIC_PLAN_DEFAULT_MAX_OPTIONS = 8
TELEGRAM_POLL_MAX_OPTIONS = 10
DEFAULT_POLL_QUESTION_TEXT = (
    "Сегодня вечером подберу рекомендацию на завтра. Давайте выберем тип события вместе."
)
DEFAULT_POLL_QUESTION_VARIANTS = (
    DEFAULT_POLL_QUESTION_TEXT,
    "Давайте выберем тему для завтрашней рекомендации. Вечером я возьму один анонс из варианта, за который будет больше голосов.",
    "Что взять для рекомендации на завтра? Вы выбираете тематику, а вечером я выберу один конкретный анонс из неё.",
    "Голосуем за тему завтрашнего события. Вечером покажу один анонс из той темы, которую выберет большинство.",
    "Давайте вместе решим, из какой темы сделать рекомендацию на завтра. Я вечером выберу один анонс и перешлю его сюда.",
    "Какую тематику взять на завтра? Вы голосуете, я вечером выбираю один анонс внутри победившего варианта.",
    "Помогите выбрать направление для завтрашней рекомендации: выставки, прогулки, музыка, семья или что-то ещё. Вечером покажу один анонс.",
    "Что берём для завтрашней рекомендации? Вы выбираете тему, я вечером выбираю один анонс из неё.",
    "Давайте так: вы голосуете за тематику на завтра, а я вечером выберу из неё один анонс и покажу в канале.",
    "Из какой темы завтра хочется рекомендацию? Голосуйте, а вечером я выберу один конкретный анонс из победившего варианта.",
    "Что завтра подсветить в канале? Выбирайте тематику, а вечером будет один конкретный анонс по голосам.",
)
PROD_MIN_VOTES_BASE = 10
PROD_MIN_VOTES_START_DATE = date(2026, 6, 12)


@dataclass(slots=True, frozen=True)
class CandidateEvent:
    id: int
    title: str
    date: str
    end_date: str | None
    time: str
    event_type: str | None
    festival: str | None
    city: str | None
    location_name: str | None
    is_free: bool
    tg_event_post_id: int
    tg_event_post_url: str | None
    telegraph_url: str | None
    summary: str
    source_vk_post_url: str | None = None
    topics: tuple[str, ...] = ()
    popularity_score: float = 0.0
    popularity_group_key: str | None = None
    popularity_trace: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class PollOptionPlan:
    key: str
    text: str
    candidate_event_ids: tuple[int, ...]
    rationale: str = ""


@dataclass(slots=True, frozen=True)
class LlmRepostReply:
    reply_text: str
    event_link_text: str


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_str(name: str, default: str) -> str:
    raw = (os.getenv(name) or "").strip()
    return raw or default


def _env_date(name: str, default: date) -> date:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return date.fromisoformat(raw)
    except Exception:
        return default


def production_min_vote_threshold(target_date: date | datetime | None = None) -> int:
    value = target_date or _now_utc().astimezone(_local_tz()).date()
    if isinstance(value, datetime):
        value = value.astimezone(_local_tz()).date() if value.tzinfo else value.date()
    start_date = _env_date("POLL_TO_FORWARD_PROD_MIN_VOTES_START_DATE", PROD_MIN_VOTES_START_DATE)
    base = max(1, _env_int("POLL_TO_FORWARD_PROD_MIN_VOTES_BASE", PROD_MIN_VOTES_BASE))
    weeks = max(0, (value - start_date).days // 7)
    return base + weeks


def min_vote_threshold_for_profile(profile_key: str, target_date: date | datetime | None = None) -> int:
    if str(profile_key or "").strip().lower() == PROFILE_PROD:
        return production_min_vote_threshold(target_date)
    return 1


def _poll_question_variants() -> tuple[str, ...]:
    fixed = (os.getenv("POLL_TO_FORWARD_QUESTION_TEXT") or "").strip()
    if fixed:
        return (fixed,)
    raw = (os.getenv("POLL_TO_FORWARD_QUESTION_VARIANTS") or "").strip()
    if not raw:
        return DEFAULT_POLL_QUESTION_VARIANTS
    variants = tuple(
        re.sub(r"\s+", " ", part).strip()
        for part in raw.split("||")
        if re.sub(r"\s+", " ", part).strip()
    )
    return variants or DEFAULT_POLL_QUESTION_VARIANTS


def _poll_question_text(run_key: str | None = None) -> str:
    variants = _poll_question_variants()
    if len(variants) == 1:
        return variants[0]
    raw_key = str(run_key or _now_utc().date().isoformat())
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(variants)
    previous_key = _previous_question_run_key(raw_key)
    if previous_key:
        previous_digest = hashlib.sha256(previous_key.encode("utf-8")).hexdigest()
        previous_index = int(previous_digest[:8], 16) % len(variants)
        if previous_index == index:
            index = (index + 1) % len(variants)
    return variants[index]


def _poll_question_with_feedback_hint(text: str) -> str:
    base = re.sub(r"\s+", " ", str(text or "").strip())
    hint = "Если темы не те — выбирайте «Другое»."
    if not base:
        return hint
    if "другое" in base.casefold():
        return base
    combined = f"{base} {hint}"
    return combined[:300].rstrip()


def _previous_question_run_key(raw_key: str) -> str | None:
    text = str(raw_key or "").strip()
    debug_prefix = "debug:"
    if text.startswith(debug_prefix):
        slot = text.removeprefix(debug_prefix)
        try:
            previous = datetime.strptime(slot, "%Y-%m-%dT%H") - timedelta(hours=1)
        except ValueError:
            return None
        return f"{debug_prefix}{previous.strftime('%Y-%m-%dT%H')}"
    for prefix in ("prod:", ""):
        value = text.removeprefix(prefix) if prefix else text
        try:
            previous_date = date.fromisoformat(value) - timedelta(days=1)
        except ValueError:
            continue
        return f"{prefix}{previous_date.isoformat()}"
    return None


def _event_telegraph_url(event: CandidateEvent | Event | None) -> str | None:
    if event is None:
        return None
    url = str(getattr(event, "telegraph_url", "") or "").strip()
    if url:
        return url
    path = str(getattr(event, "telegraph_path", "") or "").strip()
    if path:
        return f"https://telegra.ph/{path.lstrip('/')}"
    return None


def _repost_intro_text(
    winner_text: str,
    reason: str | None,
    *,
    total_votes: int = 0,
    event_title: str | None = None,
    telegraph_url: str | None = None,
    tied_texts: Sequence[str] | None = None,
    reply_template: str | None = None,
    event_link_text: str | None = None,
    fact_context: str | None = None,
) -> str:
    winner = re.sub(r"\s+", " ", str(winner_text or "").strip()).rstrip(".")
    tied = [
        re.sub(r"\s+", " ", str(text or "").strip()).rstrip(".")
        for text in (tied_texts or [])
        if re.sub(r"\s+", " ", str(text or "").strip()).rstrip(".")
    ]
    reason_text = _compact_repost_reason(reason)
    title = re.sub(r"\s+", " ", str(event_title or "").strip()).rstrip(".") or "этот анонс"
    link_title = _clean_event_link_text(event_link_text, fallback=title)
    link = str(telegraph_url or "").strip()
    linked_title = (
        f'<a href="{escape(link, quote=True)}">{escape(link_title)}</a>'
        if link
        else escape(link_title)
    )
    fallback = _fallback_repost_intro_text(
        winner,
        reason_text,
        linked_title=linked_title,
        tied=tied,
    )
    rendered = _render_llm_repost_reply(
        reply_template,
        event_link_html=linked_title,
        fact_context=fact_context,
        is_tie=len(tied) > 1,
    )
    return rendered or fallback


def _clean_event_link_text(value: str | None, *, fallback: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip()).strip(" .")
    fallback_text = re.sub(r"\s+", " ", str(fallback or "").strip()).strip(" .") or "этот анонс"
    if not text:
        return _soften_all_caps_title(fallback_text)
    lowered = text.lower()
    if any(fragment in lowered for fragment in ("http://", "https://", "<a ", "</a>", "{{", "}}")):
        return _soften_all_caps_title(fallback_text)
    if len(text) > 120:
        return _soften_all_caps_title(fallback_text)
    if _is_all_caps_title(text) and _is_all_caps_title(fallback_text):
        return _soften_all_caps_title(text)
    return text


def _is_all_caps_title(text: str) -> bool:
    letters = [char for char in str(text or "") if char.isalpha()]
    if len(letters) < 4:
        return False
    upper = sum(1 for char in letters if char.upper() == char and char.lower() != char)
    return upper / max(1, len(letters)) >= 0.8


def _soften_all_caps_title(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip()).strip(" .")
    if not _is_all_caps_title(value):
        return value or "этот анонс"
    lowered = value.lower()
    return lowered[:1].upper() + lowered[1:]


def _fallback_repost_intro_text(
    winner: str,
    reason_text: str,
    *,
    linked_title: str,
    tied: Sequence[str],
) -> str:
    if len(tied) > 1:
        shown = tied[:3]
        if len(shown) == 2:
            tied_text = f"«{escape(shown[0])}» и «{escape(shown[1])}»"
        else:
            tied_text = ", ".join(f"«{escape(text)}»" for text in shown[:-1])
            tied_text = f"{tied_text} и «{escape(shown[-1])}»"
        parts = [f"Голоса разделились поровну между {tied_text}. Беру один из этих вариантов."]
        recommendation_lead = f"Беру в рекомендацию один анонс из этих тем — {linked_title}"
    else:
        parts = [f"Спасибо за голоса — берём тему «{escape(winner)}»."]
        recommendation_lead = f"Беру в рекомендацию анонс {linked_title}"
    if reason_text:
        reason_end = "" if reason_text.endswith((".", "!", "?", "…")) else "."
        parts.append(f"{recommendation_lead} — {escape(reason_text)}{reason_end}")
    else:
        parts.append(f"{recommendation_lead}.")
    parts.append("Если рекомендация зашла — поставьте 👍. Если нет — 👎, буду сверяться с вами дальше.")
    parts.append("Сейчас перешлю анонс 👇")
    return "\n\n".join(parts)


def _render_llm_repost_reply(
    reply_template: str | None,
    *,
    event_link_html: str,
    fact_context: str | None = None,
    is_tie: bool = False,
) -> str | None:
    text = re.sub(r"[ \t]+\n", "\n", str(reply_template or "").strip())
    text = re.sub(r"\n{3,}", "\n\n", text)
    if not text or text.count(EVENT_LINK_PLACEHOLDER) != 1:
        return None
    lowered = text.lower()
    forbidden = (
        "алгоритм",
        "репост",
        "форвард",
        "контент",
        "лучшее событие",
        "идеальный выбор",
        "отличный вариант",
        "интересный вариант",
        "хороший вариант",
        "подробнее",
        "http://",
        "https://",
        "<a ",
        "</a>",
        "на этом:",
        "остановился на этом:",
        "остановимся на этом:",
        "рекомендация такая:",
        "выбрал вот что:",
        "вот что выбрал для этой темы:",
    )
    if any(fragment in lowered for fragment in forbidden):
        return None
    if not is_tie and any(
        fragment in lowered
        for fragment in (
            "объедин",
            "эти запрос",
            "запросы",
            "голоса разделились",
            "мнения разделились",
        )
    ):
        return None
    if re.search(r":\s*" + re.escape(EVENT_LINK_PLACEHOLDER), text):
        return None
    if _has_unsupported_outdoor_claim(lowered, fact_context):
        return None
    if "👍" not in text or "👎" not in text:
        return None
    if "перешлю анонс" not in lowered:
        return None
    before, after = text.split(EVENT_LINK_PLACEHOLDER, 1)
    rendered = f"{escape(before)}{event_link_html}{escape(after)}"
    rendered = re.sub(r"\n{3,}", "\n\n", rendered).strip()
    rendered = re.sub(r"\n(Сейчас перешлю анонс 👇)$", r"\n\n\1", rendered)
    rendered = re.sub(r"\n{3,}", "\n\n", rendered).strip()
    return rendered or None


def _has_unsupported_outdoor_claim(lowered_reply: str, fact_context: str | None) -> bool:
    outdoor_claims = (
        "под открытым небом",
        "на открытом воздухе",
        "на свежем воздухе",
        "open air",
        "опен-эйр",
        "опен эйр",
    )
    if not any(claim in lowered_reply for claim in outdoor_claims):
        return False
    context = str(fact_context or "").lower()
    outdoor_evidence = outdoor_claims + (
        "уличн",
        "на улице",
        "во дворе",
        "парк",
        "сквер",
        "площад",
        "сад",
        "пляж",
        "набережн",
    )
    return not any(marker in context for marker in outdoor_evidence)


def _event_reply_fact_context(event: CandidateEvent, reason: str | None) -> str:
    fields = (
        event.title,
        event.event_type,
        event.festival,
        event.city,
        event.location_name,
        event.summary,
        reason,
    )
    return "\n".join(str(value or "") for value in fields if str(value or "").strip())


def _compact_repost_reason(reason: str | None, limit: int = 100) -> str:
    text = re.sub(r"\s+", " ", str(reason or "").strip()).rstrip(".")
    text = re.sub(r"\s+[—–-]\s+", ", ", text)
    text = _soften_repost_reason_lead(text)
    if len(text) <= limit:
        return text
    cutoff = text.rfind(" ", 0, max(1, limit - 1))
    if cutoff < int(limit * 0.65):
        cutoff = max(1, limit - 1)
    return text[:cutoff].rstrip(" ,;:—–-") + "..."


def _soften_repost_reason_lead(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    value = re.sub(
        r"^(?:отличный|интересный|хороший)\s+вариант\s+для\s+тех,\s+кто\s+",
        "для тех, кто ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r"^(?:отличный|интересный|хороший)\s+вариант:\s*",
        "",
        value,
        flags=re.IGNORECASE,
    )
    return value


def _local_tz() -> ZoneInfo:
    try:
        return ZoneInfo(_env_str("POLL_TO_FORWARD_TZ", "Europe/Kaliningrad"))
    except Exception:
        return ZoneInfo("Europe/Kaliningrad")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime | None = None) -> str:
    value = dt or _now_utc()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _event_active_on(event: Event, target: date) -> bool:
    start = _parse_date(getattr(event, "date", None))
    if start is None:
        return False
    if start > target:
        return False
    end = _parse_date(getattr(event, "end_date", None))
    if end is not None:
        return end >= target
    return start == target


def _event_forward_matches_target_date(event: Event, target: date) -> bool:
    start = _parse_date(getattr(event, "date", None))
    return start == target


def _extract_post_id_from_url(url: str | None) -> int | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    m = re.search(r"t\.me/(?:c/\d+/|[^/\s]+/)(\d+)", raw, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        value = int(m.group(1))
    except Exception:
        return None
    return value if value > 0 else None


def _event_kldevents_message_id(event: Event) -> int | None:
    raw_id = getattr(event, "tg_event_post_id", None)
    try:
        post_id = int(raw_id) if raw_id is not None else 0
    except Exception:
        post_id = 0
    if post_id > 0:
        return post_id
    return _extract_post_id_from_url(getattr(event, "tg_event_post_url", None))


def _short_summary(event: Event) -> str:
    parts = [
        getattr(event, "short_description", None),
        getattr(event, "search_digest", None),
        getattr(event, "description", None),
        getattr(event, "source_text", None),
    ]
    text = next((str(p or "").strip() for p in parts if str(p or "").strip()), "")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:500]


def _event_topics(event: Event) -> tuple[str, ...]:
    raw = getattr(event, "topics", None)
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item or "").strip())
    try:
        data = json.loads(str(raw or "[]"))
    except Exception:
        return ()
    if not isinstance(data, list):
        return ()
    return tuple(str(item).strip() for item in data if str(item or "").strip())


async def _recent_forwarded_event_ids(
    db: Database,
    *,
    profile_key: str,
    now_utc: datetime,
    days: int,
) -> set[int]:
    cutoff = _iso_utc(now_utc - timedelta(days=max(1, int(days))))
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT chosen_event_id
            FROM poll_repost_run
            WHERE profile_key=?
              AND status=?
              AND chosen_event_id IS NOT NULL
              AND updated_at >= ?
            """,
            (profile_key, STATUS_FORWARDED, cutoff),
        )
        rows = await cur.fetchall()
    out: set[int] = set()
    for (event_id,) in rows:
        try:
            out.add(int(event_id))
        except Exception:
            continue
    return out


async def load_eligible_events(
    db: Database,
    *,
    target_date: date,
    profile_key: str = PROFILE_DEBUG,
    now_utc: datetime | None = None,
) -> list[CandidateEvent]:
    now_value = now_utc or _now_utc()
    target_iso = target_date.isoformat()
    repeat_days = _env_int("POLL_TO_FORWARD_ANTI_REPEAT_DAYS", 7)
    recent_ids = await _recent_forwarded_event_ids(
        db,
        profile_key=profile_key,
        now_utc=now_value,
        days=repeat_days,
    )
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(Event)
                .where(
                    Event.date <= target_iso,
                    or_(Event.end_date.is_(None), Event.end_date == "", Event.end_date >= target_iso),
                    Event.lifecycle_status == "active",
                    Event.silent == False,  # noqa: E712
                    or_(Event.tg_event_post_id.is_not(None), Event.tg_event_post_url.is_not(None)),
                )
                .order_by(Event.time, Event.id)
            )
        ).scalars().all()

    candidates: list[CandidateEvent] = []
    for event in rows:
        event_id = int(getattr(event, "id", 0) or 0)
        if event_id <= 0 or event_id in recent_ids:
            continue
        if not _event_active_on(event, target_date):
            continue
        if not _event_forward_matches_target_date(event, target_date):
            continue
        post_id = _event_kldevents_message_id(event)
        if not post_id:
            continue
        title = str(getattr(event, "title", "") or "").strip()
        if not title:
            continue
        candidates.append(
            CandidateEvent(
                id=event_id,
                title=title,
                date=str(getattr(event, "date", "") or "").strip(),
                end_date=(str(getattr(event, "end_date", "") or "").strip() or None),
                time=str(getattr(event, "time", "") or "").strip(),
                event_type=(str(getattr(event, "event_type", "") or "").strip() or None),
                festival=(str(getattr(event, "festival", "") or "").strip() or None),
                city=(str(getattr(event, "city", "") or "").strip() or None),
                location_name=(str(getattr(event, "location_name", "") or "").strip() or None),
                is_free=bool(getattr(event, "is_free", False)),
                tg_event_post_id=int(post_id),
                tg_event_post_url=(str(getattr(event, "tg_event_post_url", "") or "").strip() or None),
                telegraph_url=_event_telegraph_url(event),
                summary=_short_summary(event),
                source_vk_post_url=(str(getattr(event, "source_vk_post_url", "") or "").strip() or None),
                topics=_event_topics(event),
            )
        )
    return candidates


async def _apply_popularity_preflight(
    db: Database,
    events: Sequence[CandidateEvent],
    *,
    target_date: date,
    now_utc: datetime | None,
) -> tuple[list[CandidateEvent], dict[str, Any]]:
    if not events:
        return [], {"eligible_before_popularity": 0, "popular_events": 0}
    if not _env_enabled("POLL_TO_FORWARD_POPULARITY_ENABLED", True):
        return list(events), {"disabled": True, "eligible_before_popularity": len(events)}
    try:
        from poll_to_forward_popularity import build_event_popularity, popularity_trace_for_event

        result = await build_event_popularity(
            db,
            events,
            target_date=target_date,
            now_utc=now_utc,
        )
    except Exception as exc:
        logger.warning("poll_to_forward: popularity preflight failed: %s", exc, exc_info=True)
        return list(events), {
            "error": str(exc) or type(exc).__name__,
            "eligible_before_popularity": len(events),
            "popular_events": 0,
        }
    enriched: list[CandidateEvent] = []
    for event in events:
        popularity = result.by_event_id.get(event.id)
        if popularity:
            enriched.append(
                replace(
                    event,
                    popularity_score=float(popularity.score),
                    popularity_group_key=popularity.group_key,
                    popularity_trace=popularity_trace_for_event(popularity),
                )
            )
        else:
            enriched.append(event)
    popular = [event for event in enriched if float(event.popularity_score or 0.0) > 0]
    diagnostics = {
        **result.diagnostics,
        "eligible_before_popularity": len(events),
        "popular_events": len(popular),
    }
    # Keep old unit/local flows alive when no metric surface is available at all.
    if not popular and not any(
        int(diagnostics.get(key, 0) or 0) > 0
        for key in (
            "source_metric_rows",
            "kldevents_direct_found",
            "kldevents_wall_scan_found",
            "kldevents_wall_items_scanned",
        )
    ):
        return enriched, diagnostics
    return popular, diagnostics


def _extract_json_object(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            data = json.loads(raw[start : end + 1])
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    return None


async def _google_generate_json(
    *,
    prompt: str,
    max_output_tokens: int,
    temperature: float,
) -> dict[str, Any] | None:
    if not _env_enabled("POLL_TO_FORWARD_LLM_ENABLED", True):
        return None
    try:
        from google_ai import GoogleAIClient, SecretsProvider

        supabase_client = None
        incident_notifier = None
        try:
            supabase_client = require_main_attr("get_supabase_client")()
        except Exception:
            supabase_client = None
        try:
            incident_notifier = require_main_attr("notify_llm_incident")
        except Exception:
            incident_notifier = None
        client = GoogleAIClient(
            supabase_client=supabase_client,
            secrets_provider=SecretsProvider(),
            consumer="poll_to_forward",
            incident_notifier=incident_notifier,
        )
        raw, _usage = await client.generate_content_async(
            model=_env_str("POLL_TO_FORWARD_LLM_MODEL", "gemini-3.1-flash-lite"),
            prompt=prompt,
            generation_config={"temperature": float(temperature)},
            max_output_tokens=int(max_output_tokens),
        )
    except Exception as exc:
        logger.warning("poll_to_forward: LLM request failed: %s", exc, exc_info=True)
        return None
    return _extract_json_object(str(raw or ""))


def _free_topic_possible(events: Sequence[CandidateEvent]) -> bool:
    return sum(1 for event in events if bool(event.is_free)) >= FREE_TOPIC_MIN_FREE_EVENTS


def _topic_plan_min_options(events: Sequence[CandidateEvent], base_min_options: int) -> int:
    base = max(2, int(base_min_options))
    if _free_topic_possible(events) and len(events) >= FREE_TOPIC_MIN_OPTIONS:
        return max(base, FREE_TOPIC_MIN_OPTIONS)
    return base


def _option_mentions_free(*values: str | None) -> bool:
    text = " ".join(str(value or "") for value in values).casefold()
    return any(marker in text for marker in ("бесплат", "вход свобод", "free", "0 руб"))


def _is_feedback_option(option: PollOptionPlan | None) -> bool:
    return bool(option and option.key == FEEDBACK_OPTION_KEY)


def _with_feedback_option(options: Sequence[PollOptionPlan]) -> list[PollOptionPlan]:
    if not _env_enabled("POLL_TO_FORWARD_FEEDBACK_OPTION_ENABLED", True):
        return list(options)
    out = [option for option in options if not _is_feedback_option(option)]
    if len(out) >= TELEGRAM_POLL_MAX_OPTIONS:
        out = out[: TELEGRAM_POLL_MAX_OPTIONS - 1]
    out.append(
        PollOptionPlan(
            key=FEEDBACK_OPTION_KEY,
            text=FEEDBACK_OPTION_TEXT,
            candidate_event_ids=(),
            rationale="Feedback option: categories did not match audience intent.",
        )
    )
    return out


async def _call_llm_topic_planner(
    events: Sequence[CandidateEvent],
    *,
    min_options: int = 3,
) -> tuple[str | None, list[PollOptionPlan]]:
    required_options = _topic_plan_min_options(events, min_options)
    max_options = min(TELEGRAM_POLL_MAX_OPTIONS, max(TOPIC_PLAN_DEFAULT_MAX_OPTIONS, required_options))
    free_count = sum(1 for event in events if bool(event.is_free))
    free_instruction = (
        f"В списке есть {free_count} бесплатных событий. Если среди кандидатов есть несколько "
        "бесплатных событий, обязательно рассмотри отдельную живую опцию про бесплатность — не сухо "
        "«бесплатные мероприятия», а в духе «куда угодно, только бесплатно». Эта опция должна быть "
        "дополнительной осью выбора, а не заменой одной из обычных тематик. Поэтому при такой опции "
        f"верни минимум {required_options} вариантов, не сжимай опрос до 5. "
        "В эту опцию включай только события с is_free=true. "
        if free_count >= FREE_TOPIC_MIN_FREE_EVENTS
        else ""
    )
    items = [
        {
            "id": ev.id,
            "title": ev.title,
            "time": ev.time,
            "type": ev.event_type,
            "festival": ev.festival,
            "city": ev.city,
            "location": ev.location_name,
            "is_free": ev.is_free,
            "topics": list(ev.topics),
            "popularity_score": round(float(ev.popularity_score or 0.0), 3),
            "popularity": (ev.popularity_trace or {}).get("best_signal") if ev.popularity_trace else None,
            "summary": ev.summary[:350],
        }
        for ev in events[:40]
    ]
    prompt = (
        "Ты редактор Telegram-афиши Калининграда. Нужно составить варианты для дневного опроса: "
        "какое направление аудитория выберет, чтобы сегодня вечером получить одну рекомендацию "
        "о том, куда можно пойти завтра.\n"
        "Работай только с переданными событиями. Не придумывай темы, под которые нет кандидатов. "
        "Если в событиях есть popularity, используй её как сигнал аудитории: такие события уже показали "
        "интерес выше медианы в источниках или на стене kldevents. "
        "Опции должны быть живыми job-to-be-done, а не сухими категориями базы. "
        "Пиши дружелюбно и спокойно, как обращение к подписчикам канала с анонсами. "
        f"{free_instruction}"
        "Не используй рекламные суперлативы и промо-слоганы вроде «лучшие события», "
        "«на волне драйва», «прикоснуться к прекрасному».\n"
        "Поле question_text можешь оставить пустым: вопрос опроса задаёт продуктовый шаблон.\n"
        "Верни JSON строго такого вида: "
        "{\"question_text\":\"...\",\"options\":[{\"key\":\"music\",\"text\":\"...\",\"candidate_event_ids\":[1,2],\"rationale\":\"...\"}]}.\n"
        f"Нужно {required_options}-{max_options} опций, текст каждой опции до 100 символов. "
        "Каждая опция должна иметь хотя бы один candidate_event_id из списка.\n\n"
        f"События на завтра:\n{json.dumps(items, ensure_ascii=False)}"
    )
    data = await _google_generate_json(prompt=prompt, max_output_tokens=900, temperature=0.55)
    if not data:
        return None, []
    event_ids = {ev.id for ev in events}
    free_by_id = {ev.id: bool(ev.is_free) for ev in events}
    question = str(data.get("question_text") or "").strip() or None
    options: list[PollOptionPlan] = []
    for idx, item in enumerate(data.get("options") or []):
        if not isinstance(item, dict):
            continue
        text = re.sub(r"\s+", " ", str(item.get("text") or "")).strip()
        if not text:
            continue
        key = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(item.get("key") or f"opt{idx+1}")).strip("_")
        rationale = str(item.get("rationale") or "").strip()[:500]
        free_only = _option_mentions_free(key, text, rationale)
        ids = []
        for raw_id in item.get("candidate_event_ids") or []:
            try:
                event_id = int(raw_id)
            except Exception:
                continue
            if event_id in event_ids and (not free_only or free_by_id.get(event_id, False)):
                ids.append(event_id)
        if not ids:
            continue
        options.append(
            PollOptionPlan(
                key=key or f"opt{idx+1}",
                text=text[:100],
                candidate_event_ids=tuple(dict.fromkeys(ids)),
                rationale=rationale,
            )
        )
    return question, options[:TELEGRAM_POLL_MAX_OPTIONS]


def _popularity_group_count(option: PollOptionPlan, events_by_id: dict[int, CandidateEvent]) -> int:
    groups: set[str] = set()
    for event_id in option.candidate_event_ids:
        event = events_by_id.get(int(event_id))
        if not event:
            continue
        groups.add(event.popularity_group_key or f"event:{event.id}")
    return len(groups)


def _popularity_filter_active(events: Sequence[CandidateEvent]) -> bool:
    return any(float(event.popularity_score or 0.0) > 0 for event in events)


def _filter_options_by_popularity_inventory(
    options: Sequence[PollOptionPlan],
    events: Sequence[CandidateEvent],
) -> list[PollOptionPlan]:
    if not _popularity_filter_active(events):
        return list(options)
    events_by_id = {event.id: event for event in events}
    min_candidates = max(1, _env_int("POLL_TO_FORWARD_MIN_POPULAR_CANDIDATES_PER_OPTION", 2))
    out: list[PollOptionPlan] = []
    for option in options:
        if _is_feedback_option(option):
            out.append(option)
            continue
        ids = tuple(
            event_id
            for event_id in option.candidate_event_ids
            if events_by_id.get(int(event_id)) and float(events_by_id[int(event_id)].popularity_score or 0.0) > 0
        )
        filtered = PollOptionPlan(
            key=option.key,
            text=option.text,
            candidate_event_ids=ids,
            rationale=option.rationale,
        )
        if _popularity_group_count(filtered, events_by_id) < min_candidates:
            continue
        out.append(filtered)
    return out


def _effective_min_options(events: Sequence[CandidateEvent], configured_min_options: int) -> int:
    effective = _topic_plan_min_options(events, configured_min_options)
    if _popularity_filter_active(events):
        popularity_min = max(2, _env_int("POLL_TO_FORWARD_POPULAR_MIN_OPTIONS", 2))
        return max(2, min(effective, popularity_min))
    return effective


async def build_poll_plan(
    events: Sequence[CandidateEvent],
    *,
    min_options: int,
    run_key: str | None = None,
) -> tuple[str, list[PollOptionPlan], str]:
    effective_min_options = _effective_min_options(events, min_options)
    _question, llm_options = await _call_llm_topic_planner(events, min_options=effective_min_options)
    llm_options = _filter_options_by_popularity_inventory(llm_options, events)
    question = _poll_question_with_feedback_hint(_poll_question_text(run_key))
    content_options = [option for option in llm_options if not _is_feedback_option(option)]
    if len(content_options) >= effective_min_options:
        return question, _with_feedback_option(content_options), "llm"
    return (
        question,
        [],
        "llm_underfilled",
    )


async def _run_exists(db: Database, *, profile_key: str, run_key: str) -> bool:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM poll_repost_run WHERE profile_key=? AND run_key=? LIMIT 1",
            (profile_key, run_key),
        )
        row = await cur.fetchone()
    return bool(row)


async def _latest_visible_debug_poll_without_result(db: Database) -> dict[str, Any] | None:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, run_key, status, poll_message_id, forwarded_message_id,
                   result_json, error_json, created_at, updated_at
            FROM poll_repost_run
            WHERE profile_key=?
              AND poll_message_id IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (PROFILE_DEBUG,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    keys = [
        "id",
        "run_key",
        "status",
        "poll_message_id",
        "forwarded_message_id",
        "result_json",
        "error_json",
        "created_at",
        "updated_at",
    ]
    data = dict(zip(keys, row))
    status = str(data.get("status") or "")
    if status in {STATUS_SKIPPED_NO_VOTES, STATUS_SKIPPED_FEEDBACK_OTHER}:
        return None
    if status == STATUS_FORWARDED and data.get("forwarded_message_id"):
        return None
    return data


async def _insert_run(
    db: Database,
    *,
    profile_key: str,
    run_key: str,
    status: str,
    target_event_date: date,
    question_text: str | None = None,
    options: Sequence[PollOptionPlan] | None = None,
    poll_chat_id: str | int | None = None,
    poll_message_id: int | None = None,
    poll_id: str | None = None,
    resolve_after: datetime | None = None,
    error: dict[str, Any] | None = None,
) -> None:
    now = _iso_utc()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT OR IGNORE INTO poll_repost_run(
                profile_key, run_key, status, target_event_date,
                poll_chat_id, poll_message_id, poll_id, question_text, options_json,
                resolve_after, error_json, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                profile_key,
                run_key,
                status,
                target_event_date.isoformat(),
                str(poll_chat_id) if poll_chat_id is not None else None,
                int(poll_message_id) if poll_message_id else None,
                str(poll_id) if poll_id else None,
                question_text,
                json.dumps(
                    [
                        {
                            "key": option.key,
                            "text": option.text,
                            "candidate_event_ids": list(option.candidate_event_ids),
                            "rationale": option.rationale,
                        }
                        for option in (options or [])
                    ],
                    ensure_ascii=False,
                ),
                _iso_utc(resolve_after) if resolve_after else None,
                json.dumps(error or {}, ensure_ascii=False),
                now,
                now,
            ),
        )
        await conn.commit()


def _debug_publish_window_contains(now_local: datetime) -> bool:
    start_hour = _env_int("POLL_TO_FORWARD_DEBUG_START_HOUR", 10)
    end_hour = _env_int("POLL_TO_FORWARD_DEBUG_END_HOUR", 19)
    return int(start_hour) <= int(now_local.hour) < int(end_hour)


def _debug_create_slot(now_local: datetime) -> bool:
    return _debug_publish_window_contains(now_local) and int(now_local.minute) < 15


async def create_debug_poll_if_due(
    db: Database,
    bot: Any,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    if not _env_enabled("ENABLE_POLL_TO_FORWARD_DEBUG", False):
        return {"created": False, "reason": "disabled"}
    if bot is None:
        logger.warning("poll_to_forward.debug_create skipped: missing bot")
        return {"created": False, "reason": "missing_bot"}
    now_value = now_utc or _now_utc()
    now_local = now_value.astimezone(_local_tz())
    if not _debug_create_slot(now_local):
        return {"created": False, "reason": "outside_create_slot"}
    run_key = f"debug:{now_local.strftime('%Y-%m-%dT%H')}"
    if await _run_exists(db, profile_key=PROFILE_DEBUG, run_key=run_key):
        return {"created": False, "reason": "run_exists", "run_key": run_key}
    pending_visible = await _latest_visible_debug_poll_without_result(db)
    if pending_visible:
        logger.info(
            "poll_to_forward.debug_create skipped previous_poll_without_result run_key=%s previous_run_id=%s previous_run_key=%s previous_status=%s",
            run_key,
            pending_visible.get("id"),
            pending_visible.get("run_key"),
            pending_visible.get("status"),
        )
        return {
            "created": False,
            "reason": "previous_poll_without_result",
            "run_key": run_key,
            "previous_run_id": pending_visible.get("id"),
            "previous_run_key": pending_visible.get("run_key"),
            "previous_status": pending_visible.get("status"),
        }

    target_date = (now_local.date() + timedelta(days=1))
    events = await load_eligible_events(db, target_date=target_date, now_utc=now_value)
    events, popularity_diag = await _apply_popularity_preflight(
        db,
        events,
        target_date=target_date,
        now_utc=now_value,
    )
    min_events = max(1, _env_int("POLL_TO_FORWARD_DEBUG_MIN_ELIGIBLE_EVENTS", 3))
    if len(events) < min_events:
        has_popularity_surface = any(
            int(popularity_diag.get(key, 0) or 0) > 0
            for key in (
                "source_metric_rows",
                "kldevents_direct_found",
                "kldevents_wall_scan_found",
                "kldevents_wall_items_scanned",
            )
        )
        status = STATUS_SKIPPED_LOW_POPULARITY_INVENTORY if has_popularity_surface else STATUS_SKIPPED_LOW_INVENTORY
        reason = "low_popularity_inventory" if has_popularity_surface else "low_inventory"
        await _insert_run(
            db,
            profile_key=PROFILE_DEBUG,
            run_key=run_key,
            status=status,
            target_event_date=target_date,
            error={"eligible_events": len(events), "min_events": min_events, "popularity": popularity_diag},
        )
        logger.info(
            "poll_to_forward.debug_create skipped %s run_key=%s target_date=%s eligible=%s min=%s popularity=%s",
            reason,
            run_key,
            target_date,
            len(events),
            min_events,
            popularity_diag,
        )
        return {"created": False, "reason": reason, "eligible_events": len(events)}

    configured_min_options = max(2, _env_int("POLL_TO_FORWARD_DEBUG_MIN_OPTIONS", 3))
    min_options = _effective_min_options(events, configured_min_options)
    question, options, strategy = await build_poll_plan(events, min_options=min_options, run_key=run_key)
    if len(options) < min_options:
        await _insert_run(
            db,
            profile_key=PROFILE_DEBUG,
            run_key=run_key,
            status=STATUS_SKIPPED_TOPIC_UNDERFILL,
            target_event_date=target_date,
            error={
                "eligible_events": len(events),
                "options": len(options),
                "min_options": min_options,
                "popularity": popularity_diag,
            },
        )
        logger.info(
            "poll_to_forward.debug_create skipped topic_underfill run_key=%s target_date=%s eligible=%s options=%s min=%s strategy=%s",
            run_key,
            target_date,
            len(events),
            len(options),
            min_options,
            strategy,
        )
        return {"created": False, "reason": "topic_underfill", "eligible_events": len(events)}

    target_chat = _env_str("POLL_TO_FORWARD_DEBUG_TARGET_CHAT", "@keniggpt")
    resolve_after = now_value + timedelta(minutes=max(10, _env_int("POLL_TO_FORWARD_DEBUG_RESOLVE_AFTER_MINUTES", 30)))
    resolve_after = resolve_after.replace(second=0, microsecond=0)
    sent = await bot.send_poll(
        chat_id=target_chat,
        question=question[:300],
        options=[option.text for option in options[:10]],
        is_anonymous=True,
        allows_multiple_answers=False,
    )
    poll = getattr(sent, "poll", None)
    await _insert_run(
        db,
        profile_key=PROFILE_DEBUG,
        run_key=run_key,
        status=STATUS_OPEN,
        target_event_date=target_date,
        question_text=question,
        options=options[:10],
        poll_chat_id=target_chat,
        poll_message_id=int(getattr(sent, "message_id", 0) or 0),
        poll_id=str(getattr(poll, "id", "") or ""),
        resolve_after=resolve_after,
        error={"strategy": strategy, "eligible_events": len(events), "popularity": popularity_diag},
    )
    logger.info(
        "poll_to_forward.debug_create published run_key=%s chat=%s poll_message_id=%s target_date=%s eligible=%s options=%s strategy=%s resolve_after=%s",
        run_key,
        target_chat,
        getattr(sent, "message_id", None),
        target_date,
        len(events),
        len(options[:10]),
        strategy,
        _iso_utc(resolve_after),
    )
    return {
        "created": True,
        "run_key": run_key,
        "strategy": strategy,
        "eligible_events": len(events),
        "options": len(options[:10]),
    }


def _decode_options(value: Any) -> list[PollOptionPlan]:
    try:
        raw = json.loads(value or "[]")
    except Exception:
        return []
    out: list[PollOptionPlan] = []
    for idx, item in enumerate(raw if isinstance(raw, list) else []):
        if not isinstance(item, dict):
            continue
        ids = []
        for raw_id in item.get("candidate_event_ids") or []:
            try:
                ids.append(int(raw_id))
            except Exception:
                continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        key = str(item.get("key") or f"opt{idx+1}")
        if not ids and key != FEEDBACK_OPTION_KEY:
            continue
        out.append(
            PollOptionPlan(
                key=key,
                text=text,
                candidate_event_ids=tuple(dict.fromkeys(ids)),
                rationale=str(item.get("rationale") or ""),
            )
        )
    return out


def _poll_result_snapshot(poll: Any, fallback_options: Sequence[PollOptionPlan]) -> dict[str, Any]:
    poll_options = list(getattr(poll, "options", None) or [])
    rows: list[dict[str, Any]] = []
    for idx, option in enumerate(fallback_options):
        voter_count = 0
        if idx < len(poll_options):
            try:
                voter_count = int(getattr(poll_options[idx], "voter_count", 0) or 0)
            except Exception:
                voter_count = 0
        rows.append(
            {
                "index": idx,
                "key": option.key,
                "text": option.text,
                "candidate_event_ids": list(option.candidate_event_ids),
                "voter_count": voter_count,
            }
        )
    total = getattr(poll, "total_voter_count", None)
    try:
        total_count = int(total)
    except Exception:
        total_count = sum(int(row["voter_count"]) for row in rows)
    return {"total_voter_count": total_count, "options": rows}


def _popularity_reason(event: CandidateEvent) -> str:
    trace = event.popularity_trace or {}
    best = trace.get("best_signal") if isinstance(trace, dict) else None
    source = str((best or {}).get("source") or "")
    above = (best or {}).get("above") or []
    if source == "kldevents_vk":
        if any(metric in above for metric in ("comments", "reposts")):
            return "у этого анонса заметный живой отклик на стене kldevents"
        return "у этого анонса отклик на стене kldevents выше обычного"
    if source == "source_vk":
        return "у исходного анонса уже есть отклик выше обычного"
    return "по популярности этот анонс выглядит сильнее внутри выбранной темы"


def _feedback_other_reply_text() -> str:
    return (
        "Спасибо, понял: в этот раз темы не попали.\n\n"
        "Тогда без рекомендации по этому голосованию — в следующий опрос соберу варианты иначе."
    )


def _weighted_pick_from_top3(
    events: Sequence[CandidateEvent],
    *,
    seed: str,
) -> CandidateEvent | None:
    unique_by_group: dict[str, CandidateEvent] = {}
    for event in sorted(events, key=lambda item: (float(item.popularity_score or 0.0), -item.id), reverse=True):
        group_key = event.popularity_group_key or f"event:{event.id}"
        unique_by_group.setdefault(group_key, event)
    top = list(unique_by_group.values())[:3]
    if not top:
        return None
    weights = [max(0.001, float(event.popularity_score or 0.0)) for event in top]
    total = sum(weights)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    point = (int(digest[:12], 16) / float(0xFFFFFFFFFFFF)) * total
    acc = 0.0
    for event, weight in zip(top, weights, strict=True):
        acc += weight
        if point <= acc:
            return event
    return top[-1]


def _choose_winner_by_popularity(
    *,
    tied_options: Sequence[PollOptionPlan],
    events: Sequence[CandidateEvent],
    target_date: date | None = None,
) -> tuple[PollOptionPlan | None, int | None, str]:
    if not _popularity_filter_active(events):
        return None, None, "popularity_inactive"
    by_id = {event.id: event for event in events}
    ranked_options: list[tuple[float, str, PollOptionPlan, list[CandidateEvent]]] = []
    for option in tied_options:
        option_events = [
            by_id[event_id]
            for event_id in option.candidate_event_ids
            if event_id in by_id and float(by_id[event_id].popularity_score or 0.0) > 0
        ]
        top_event = max(option_events, key=lambda event: float(event.popularity_score or 0.0), default=None)
        if not top_event:
            continue
        ranked_options.append((float(top_event.popularity_score or 0.0), option.key, option, option_events))
    if not ranked_options:
        return None, None, "popularity_no_candidates"
    ranked_options.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _score, _key, option, option_events = ranked_options[0]
    seed = ":".join(
        [
            target_date.isoformat() if target_date else "",
            option.key,
            ",".join(str(event.id) for event in sorted(option_events, key=lambda item: item.id)),
        ]
    )
    chosen = _weighted_pick_from_top3(option_events, seed=seed)
    if not chosen:
        return None, None, "popularity_no_top3"
    return option, chosen.id, _popularity_reason(chosen)


async def _choose_winner_with_llm(
    *,
    tied_options: Sequence[PollOptionPlan],
    events: Sequence[CandidateEvent],
    target_date: date | None = None,
) -> tuple[PollOptionPlan | None, int | None, str]:
    by_id = {event.id: event for event in events}
    candidate_ids = {
        event_id
        for option in tied_options
        for event_id in option.candidate_event_ids
        if event_id in by_id
    }
    if not tied_options or not candidate_ids:
        return None, None, "no_candidates"
    popularity_option, popularity_event_id, popularity_reason = _choose_winner_by_popularity(
        tied_options=tied_options,
        events=events,
        target_date=target_date,
    )
    if popularity_option and popularity_event_id:
        return popularity_option, popularity_event_id, popularity_reason
    option_payload = [
        {
            "key": option.key,
            "text": option.text,
            "candidate_event_ids": list(option.candidate_event_ids),
        }
        for option in tied_options
    ]
    event_payload = [
        {
            "id": ev.id,
            "title": ev.title,
            "date": ev.date,
            "end_date": ev.end_date,
            "time": ev.time,
            "type": ev.event_type,
            "festival": ev.festival,
            "city": ev.city,
            "location": ev.location_name,
            "is_free": ev.is_free,
            "popularity_score": round(float(ev.popularity_score or 0.0), 3),
            "popularity": (ev.popularity_trace or {}).get("best_signal") if ev.popularity_trace else None,
            "summary": ev.summary[:350],
        }
        for ev in events
        if ev.id in candidate_ids
    ]
    prompt = (
        "Ты выбираешь итоговый Telegram-репост по результатам опроса афиши. "
        "Нужно выбрать одну опцию и одно событие из её candidate_event_ids. "
        "Если опция одна, всё равно оцени события внутри неё. "
        "Выбирай наиболее сильную публичную рекомендацию на завтра: событие должно быть интересно само по себе, "
        "соответствовать теме опроса и не выглядеть случайным. Ничего не придумывай.\n"
        "reason должен быть короткой дружелюбной причиной для сообщения перед анонсом, до 90 символов: "
        "почему именно этот анонс подходит под выбранную тему. Пиши как человек из локального канала, "
        "не как рекомендательная система. Покажи связь с голосами людей, но не пересказывай весь анонс "
        "и не повторяй название события. "
        "Если есть ничья между темами, не называй тему победителем: прямо скажи, что голоса разделились, "
        "и объясни, почему в итоге выбираешь именно это событие. "
        "Можно ссылаться на популярность/ожидаемость только если это явно видно из переданных метрик или текста события; "
        "если таких данных нет, не утверждай, что событие популярнее в каналах. "
        "Используй конкретику события, если она есть в данных: например, если выставка начинается в целевую дату "
        "или из описания видно открытие, уместно сказать «как раз открывается выставка» / "
        "«можно сходить на открывающуюся выставку». Не выдумывай открытие, если оно не следует из даты/описания. "
        "Не используй слова и смыслы: алгоритм, репост, форвард, подборка, разбор, рекомендательная модель, контент. "
        "Не обещай, что это «лучшее событие» или «идеальный выбор»; формулируй мягко: "
        "«хорошо попадает», «подходит», «ложится в выбранную тему». Не используй рекламные клише и оценки вроде "
        "«отличный повод», «отличный вариант», «интересный вариант», «хороший вариант», "
        "«уникальные вещи», «лучший», «с пользой», «драйв». "
        "Верни JSON строго такого вида: "
        "{\"winner_key\":\"...\",\"event_id\":123,\"reason\":\"коротко почему\"}.\n\n"
        f"Целевая дата события: {target_date.isoformat() if target_date else ''}\n\n"
        f"Опции-победители/ничья:\n{json.dumps(option_payload, ensure_ascii=False)}\n\n"
        f"События-кандидаты:\n{json.dumps(event_payload, ensure_ascii=False)}"
    )
    data = await _google_generate_json(prompt=prompt, max_output_tokens=500, temperature=0.25)
    if not data:
        return None, None, "llm_unavailable"
    winner_key = str(data.get("winner_key") or "").strip()
    try:
        event_id = int(data.get("event_id"))
    except Exception:
        return None, None, "llm_bad_event_id"
    selected_option = next(
        (
            option
            for option in tied_options
            if option.key == winner_key or event_id in option.candidate_event_ids
        ),
        None,
    )
    if not selected_option or event_id not in selected_option.candidate_event_ids or event_id not in by_id:
        return None, None, "llm_invalid_choice"
    return selected_option, event_id, str(data.get("reason") or "").strip()


async def _compose_repost_reply_with_llm(
    *,
    winner_option: PollOptionPlan,
    chosen: CandidateEvent,
    reason: str | None,
    tied_options: Sequence[PollOptionPlan],
    total_votes: int,
    target_date: date | None = None,
) -> LlmRepostReply | None:
    tied_texts = [option.text for option in tied_options]
    payload = {
        "winner_topic": winner_option.text,
        "tied_topics": tied_texts,
        "is_tie": len(tied_options) > 1,
        "total_votes": total_votes,
        "target_date": target_date.isoformat() if target_date else "",
        "event": {
            "title": chosen.title,
            "date": chosen.date,
            "end_date": chosen.end_date,
            "time": chosen.time,
            "type": chosen.event_type,
            "festival": chosen.festival,
            "city": chosen.city,
            "location": chosen.location_name,
            "is_free": chosen.is_free,
            "summary": chosen.summary[:350],
        },
        "selection_reason": _compact_repost_reason(reason, limit=140),
    }
    prompt = (
        "Ты пишешь публичный комментарий в Telegram-канале афиши перед пересылкой выбранного анонса. "
        "Это не рекламный текст и не отчёт алгоритма: пиши как живой локальный автор канала, дружелюбно, "
        "немного по-блогерски, но без сюсюканья.\n"
        "Смысл фиксированный, формулировки свободные: поблагодари за голоса; скажи, какую тему выбрали "
        "или что голоса разделились поровну; покажи, что автор выбрал один конкретный анонс из этой темы/этих тем; "
        "коротко объясни почему; попроси поставить 👍, если рекомендация понравилась/зашла, и 👎, если нет; "
        "последней строкой строго напиши: Сейчас перешлю анонс 👇\n"
        "Причинность важна: если is_tie=false, не раскладывай winner_topic на несколько отдельных запросов аудитории, "
        "не пиши «объединил запросы», «и то, и другое», «большинство захотело X и Y». "
        "Люди выбрали один вариант опроса; формулируй как «выбрали тему X, поэтому беру анонс Y». "
        "Про равные голоса и несколько тем можно писать только при is_tie=true.\n"
        f"В тексте должен быть плейсхолдер {EVENT_LINK_PLACEHOLDER} ровно один раз. "
        "Это место, куда код вставит HTML-ссылку. Не пиши название события отдельно, "
        "не пиши URL, Markdown или HTML.\n"
        "Отдельно верни event_link_text — видимый текст этой ссылки. Его тоже формулируешь ты: "
        "можно взять название события из контекста, но приведи его к естественному виду, если в базе оно капсом, "
        "и можно добавить/убрать родовое слово так, чтобы фраза читалась по-человечески. "
        "Например для title=ОТКРЫТЫЙ МИКРОФОН уместно event_link_text=\"«Открытый микрофон»\" "
        "или \"стендап «Открытый микрофон»\" в зависимости от фразы. "
        "Не меняй смысл события и не выдумывай другое название.\n"
        "Вписывай плейсхолдер в живую фразу через родовое слово/тип события из контекста, "
        "чтобы название не висело после двоеточия и не требовало склонения: "
        f"«поэтому сегодня выбрал турнир {EVENT_LINK_PLACEHOLDER}», "
        f"«для этой темы подходит лекция {EVENT_LINK_PLACEHOLDER}», "
        f"«можно присмотреться к игре {EVENT_LINK_PLACEHOLDER}», "
        f"«беру в рекомендацию анонс {EVENT_LINK_PLACEHOLDER}». "
        f"Нельзя писать: «я бы предложил {EVENT_LINK_PLACEHOLDER}», "
        f"«сходить на {EVENT_LINK_PLACEHOLDER}», «расскажу про {EVENT_LINK_PLACEHOLDER}», "
        f"«остановился на этом: {EVENT_LINK_PLACEHOLDER}», "
        f"«сегодня рекомендация такая: {EVENT_LINK_PLACEHOLDER}», "
        f"«выбрал вот что: {EVENT_LINK_PLACEHOLDER}». "
        "Вообще не ставь плейсхолдер сразу после двоеточия.\n"
        "Не добавляй факты, которых нет в контексте. Особенно не пиши, что событие/фестиваль проходит "
        "под открытым небом, на свежем воздухе, в парке или на улице, если это прямо не указано в контексте. "
        "У фестивалей бывает смешанный формат: не угадывай формат площадок по слову «фестиваль».\n"
        "Не делай текст одинаковым от раза к разу, но не теряй смысл. 3–5 коротких абзацев, 180–650 символов. "
        "Не используй слова и смыслы: алгоритм, репост, форвард, подборка, разбор, рекомендательная модель, контент. "
        "Не используй рекламные клише и оценки: отличный вариант, интересный вариант, хороший вариант, лучший, "
        "идеальный выбор, уникальные вещи, с пользой, драйв.\n"
        "Верни JSON строго такого вида: {\"reply_text\":\"текст\", \"event_link_text\":\"видимый текст ссылки\"}.\n\n"
        f"Контекст:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    data = await _google_generate_json(prompt=prompt, max_output_tokens=900, temperature=0.72)
    if not data:
        return None
    reply_text = str(data.get("reply_text") or "").strip()
    event_link_text = str(data.get("event_link_text") or "").strip()
    if not reply_text or not event_link_text:
        return None
    return LlmRepostReply(reply_text=reply_text, event_link_text=event_link_text)


async def _load_open_due_runs(db: Database, *, now_utc: datetime) -> list[dict[str, Any]]:
    due_cutoff = now_utc + timedelta(seconds=30)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT id, run_key, target_event_date, poll_chat_id, poll_message_id,
                   question_text, options_json
            FROM poll_repost_run
            WHERE profile_key=?
              AND status=?
              AND resolve_after IS NOT NULL
              AND resolve_after <= ?
            ORDER BY resolve_after, id
            LIMIT 5
            """,
            (PROFILE_DEBUG, STATUS_OPEN, _iso_utc(due_cutoff)),
        )
        rows = await cur.fetchall()
    keys = ["id", "run_key", "target_event_date", "poll_chat_id", "poll_message_id", "question_text", "options_json"]
    return [dict(zip(keys, row)) for row in rows]


async def _update_run(
    db: Database,
    run_id: int,
    *,
    status: str,
    result: dict[str, Any] | None = None,
    winner_option_id: str | None = None,
    winner_text: str | None = None,
    chosen_event_id: int | None = None,
    kldevents_chat_id: str | None = None,
    kldevents_message_id: int | None = None,
    kldevents_post_url: str | None = None,
    reply_message_id: int | None = None,
    forwarded_message_id: int | None = None,
    error: dict[str, Any] | None = None,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE poll_repost_run
            SET status=?,
                result_json=COALESCE(?, result_json),
                winner_option_id=COALESCE(?, winner_option_id),
                winner_text=COALESCE(?, winner_text),
                chosen_event_id=COALESCE(?, chosen_event_id),
                kldevents_chat_id=COALESCE(?, kldevents_chat_id),
                kldevents_message_id=COALESCE(?, kldevents_message_id),
                kldevents_post_url=COALESCE(?, kldevents_post_url),
                reply_message_id=COALESCE(?, reply_message_id),
                forwarded_message_id=COALESCE(?, forwarded_message_id),
                error_json=COALESCE(?, error_json),
                updated_at=?
            WHERE id=?
            """,
            (
                status,
                json.dumps(result, ensure_ascii=False) if result is not None else None,
                winner_option_id,
                winner_text,
                int(chosen_event_id) if chosen_event_id else None,
                kldevents_chat_id,
                int(kldevents_message_id) if kldevents_message_id else None,
                kldevents_post_url,
                int(reply_message_id) if reply_message_id else None,
                int(forwarded_message_id) if forwarded_message_id else None,
                json.dumps(error, ensure_ascii=False) if error is not None else None,
                _iso_utc(),
                int(run_id),
            ),
        )
        await conn.commit()


async def resolve_due_debug_polls(
    db: Database,
    bot: Any,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    if not _env_enabled("ENABLE_POLL_TO_FORWARD_DEBUG", False):
        return {"resolved": 0, "reason": "disabled"}
    if bot is None:
        logger.warning("poll_to_forward.debug_resolve skipped: missing bot")
        return {"resolved": 0, "reason": "missing_bot"}
    now_value = now_utc or _now_utc()
    runs = await _load_open_due_runs(db, now_utc=now_value)
    resolved = 0
    for run in runs:
        run_id = int(run["id"])
        options = _decode_options(run.get("options_json"))
        try:
            poll = await bot.stop_poll(
                chat_id=run["poll_chat_id"],
                message_id=int(run["poll_message_id"]),
            )
            snapshot = _poll_result_snapshot(poll, options)
            total_votes = int(snapshot.get("total_voter_count", 0) or 0)
            min_votes = min_vote_threshold_for_profile(PROFILE_DEBUG, now_value)
            if total_votes < min_votes:
                await _update_run(
                    db,
                    run_id,
                    status=STATUS_SKIPPED_NO_VOTES,
                    result=snapshot,
                    error={"total_votes": total_votes, "min_votes": min_votes},
                )
                logger.info(
                    "poll_to_forward.debug_resolve skipped low_votes run_id=%s run_key=%s total_votes=%s min_votes=%s",
                    run_id,
                    run.get("run_key"),
                    total_votes,
                    min_votes,
                )
                resolved += 1
                continue
            result_options = list(snapshot.get("options") or [])
            max_votes = max((int(item.get("voter_count", 0) or 0) for item in result_options), default=0)
            tied_indexes = [
                int(item.get("index", 0) or 0)
                for item in result_options
                if int(item.get("voter_count", 0) or 0) == max_votes
            ]
            tied_options = [
                options[idx]
                for idx in tied_indexes
                if 0 <= idx < len(options)
            ]
            if not tied_options:
                await _update_run(db, run_id, status=STATUS_SKIPPED_NO_CANDIDATE, result=snapshot)
                logger.info(
                    "poll_to_forward.debug_resolve skipped no_tied_options run_id=%s run_key=%s total_votes=%s",
                    run_id,
                    run.get("run_key"),
                    total_votes,
                )
                resolved += 1
                continue
            target_date = _parse_date(run.get("target_event_date")) or now_value.astimezone(_local_tz()).date()
            feedback_won = len(tied_options) == 1 and _is_feedback_option(tied_options[0])
            if feedback_won:
                target_chat = _env_str("POLL_TO_FORWARD_DEBUG_TARGET_CHAT", "@keniggpt")
                reply = await bot.send_message(
                    chat_id=target_chat,
                    text=_feedback_other_reply_text(),
                    reply_to_message_id=int(run["poll_message_id"]),
                    disable_web_page_preview=True,
                )
                await _update_run(
                    db,
                    run_id,
                    status=STATUS_SKIPPED_FEEDBACK_OTHER,
                    result={
                        **snapshot,
                        "selection_trace": {
                            "reason": "feedback_other_won",
                            "feedback_option": tied_options[0].text,
                        },
                    },
                    winner_option_id=tied_options[0].key,
                    winner_text=tied_options[0].text,
                    reply_message_id=int(getattr(reply, "message_id", 0) or 0) or None,
                    error={"reason": "feedback_other_won"},
                )
                logger.info(
                    "poll_to_forward.debug_resolve skipped feedback_other run_id=%s run_key=%s total_votes=%s",
                    run_id,
                    run.get("run_key"),
                    total_votes,
                )
                resolved += 1
                continue
            events = await load_eligible_events(db, target_date=target_date, now_utc=now_value)
            events, popularity_diag = await _apply_popularity_preflight(
                db,
                events,
                target_date=target_date,
                now_utc=now_value,
            )
            winner_option, chosen_id, llm_reason = await _choose_winner_with_llm(
                tied_options=tied_options,
                events=events,
                target_date=target_date,
            )
            chosen = next((event for event in events if event.id == chosen_id), None)
            selection_trace = {
                "reason": llm_reason,
                "popularity": popularity_diag,
                "chosen_event": chosen.popularity_trace if chosen else None,
            }
            snapshot_with_trace = {**snapshot, "selection_trace": selection_trace}
            if not winner_option or not chosen:
                await _update_run(
                    db,
                    run_id,
                    status=STATUS_SKIPPED_NO_CANDIDATE,
                    result=snapshot_with_trace,
                    winner_option_id=winner_option.key if winner_option else None,
                    winner_text=winner_option.text if winner_option else None,
                    error={
                        "reason": llm_reason or "winner_candidate_unavailable",
                        "popularity": popularity_diag,
                    },
                )
                logger.info(
                    "poll_to_forward.debug_resolve skipped llm_or_candidate_unavailable run_id=%s run_key=%s reason=%s tied_options=%s eligible_now=%s",
                    run_id,
                    run.get("run_key"),
                    llm_reason,
                    [option.text for option in tied_options],
                    len(events),
                )
                resolved += 1
                continue
            llm_reply = await _compose_repost_reply_with_llm(
                winner_option=winner_option,
                chosen=chosen,
                reason=llm_reason,
                tied_options=tied_options,
                total_votes=total_votes,
                target_date=target_date,
            )
            target_chat = _env_str("POLL_TO_FORWARD_DEBUG_TARGET_CHAT", "@keniggpt")
            source_chat = _env_str("POLL_TO_FORWARD_SOURCE_CHAT", "@kldevents")
            reply = await bot.send_message(
                chat_id=target_chat,
                text=_repost_intro_text(
                    winner_option.text,
                    llm_reason,
                    total_votes=total_votes,
                    event_title=chosen.title,
                    telegraph_url=chosen.telegraph_url,
                    tied_texts=[option.text for option in tied_options],
                    reply_template=llm_reply.reply_text if llm_reply else None,
                    event_link_text=llm_reply.event_link_text if llm_reply else None,
                    fact_context=_event_reply_fact_context(chosen, llm_reason),
                ),
                reply_to_message_id=int(run["poll_message_id"]),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            forwarded = await bot.forward_message(
                chat_id=target_chat,
                from_chat_id=source_chat,
                message_id=int(chosen.tg_event_post_id),
            )
            await _update_run(
                db,
                run_id,
                status=STATUS_FORWARDED,
                result=snapshot_with_trace,
                winner_option_id=winner_option.key,
                winner_text=winner_option.text,
                chosen_event_id=chosen.id,
                kldevents_chat_id=source_chat,
                kldevents_message_id=chosen.tg_event_post_id,
                kldevents_post_url=chosen.tg_event_post_url,
                reply_message_id=int(getattr(reply, "message_id", 0) or 0) or None,
                forwarded_message_id=int(getattr(forwarded, "message_id", 0) or 0) or None,
                error={"llm_reason": llm_reason, "popularity": popularity_diag},
            )
            logger.info(
                "poll_to_forward.debug_resolve forwarded run_id=%s run_key=%s winner=%s event_id=%s source_message_id=%s target_forward_id=%s reason=%s",
                run_id,
                run.get("run_key"),
                winner_option.text,
                chosen.id,
                chosen.tg_event_post_id,
                getattr(forwarded, "message_id", None),
                llm_reason,
            )
            resolved += 1
        except Exception as exc:
            logger.exception("poll_to_forward: failed to resolve debug run id=%s", run_id)
            await _update_run(
                db,
                run_id,
                status=STATUS_FAILED,
                error={"error": str(exc) or type(exc).__name__},
            )
            resolved += 1
    return {"resolved": resolved, "due": len(runs)}


async def run_debug_tick(db: Database, bot: Any, *, now_utc: datetime | None = None) -> dict[str, Any]:
    now_value = now_utc or _now_utc()
    resolved = await resolve_due_debug_polls(db, bot, now_utc=now_value)
    created = await create_debug_poll_if_due(db, bot, now_utc=now_value)
    return {"resolved": resolved, "created": created}
