from __future__ import annotations

import asyncio
import colorsys
import hashlib
import io
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence

from sqlalchemy import or_, select

from db import Database
from models import Event, PromoActivity, PromoCampaign, PromoExposure, PromoTarget

logger = logging.getLogger("afishaengagement")

PROMO_SURFACE_AFISHA_ENGAGEMENT = "afishaengagement"
DEFAULT_DEBUG_MARKER = "#afishaengagement_shadow"
DEFAULT_BUILD_TAG_PREFIX = "#aeg_b"
DEFAULT_TARGET_GROUP_SHORT = "klgdevents"

_CLEANUP_DONE: set[tuple[str, str]] = set()


@dataclass(frozen=True)
class EngagementCandidate:
    campaign: PromoCampaign
    activity: PromoActivity
    target: PromoTarget
    config: dict[str, Any]


@dataclass(frozen=True)
class DiceDecision:
    seed: str
    value: float
    apply_rate: float
    applies: bool


@dataclass(frozen=True)
class PosterVisionSummary:
    provider: str
    confidence: float
    text: str = ""
    right_third_clean: bool | None = None
    reason: str | None = None


@dataclass(frozen=True)
class EngagementPlan:
    mechanic: str
    template_id: str
    palette_id: str
    cta_text: str
    hook_text: str | None
    event_type: str
    has_persona: bool
    has_festival: bool
    seed: str


@dataclass(frozen=True)
class TextFit:
    font_px: int
    lines: list[str]
    width: int
    height: int


@dataclass(frozen=True)
class RenderedImage:
    data: bytes
    filename: str
    template_id: str
    palette_id: str
    cta_text_lines: list[str]
    cta_text_font_px: int
    dimensions: tuple[int, int]
    render_ms: int


@dataclass
class StageLog:
    stage: str
    decision: str
    reason: str
    event_id: int | None = None
    campaign_id: int | None = None
    activity_id: int | None = None
    vk_owner_id: int | None = None
    vk_group_short: str | None = None
    event_type: str | None = None
    has_persona: bool | None = None
    has_festival: bool | None = None
    seed: str | None = None
    apply_rate: float | None = None
    dice_value: float | None = None
    mechanic: str | None = None
    template_id: str | None = None
    palette_id: str | None = None
    cta_text_final: str | None = None
    cta_text_len: int | None = None
    cta_text_lines: int | None = None
    cta_text_font_px: int | None = None
    vision_conf_right_third: float | None = None
    vision_conf_poster_text_zones: list[float] | None = None
    render_ms: int | None = None
    llm_ms_text: int | None = None
    llm_ms_vision: int | None = None
    total_ms: int | None = None
    shadow_mode: bool | None = None
    shadow_marker: str | None = None
    shadow_scheduled_for_ts: str | None = None
    original_post_ts: str | None = None
    vk_post_id: str | None = None
    vk_post_url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


PALETTES: dict[str, dict[str, str]] = {
    "deep_wine_ivory": {
        "background": "#4A0F1E",
        "text": "#FFF4DF",
        "accent": "#FFB000",
        "accent_text": "#000000",
    },
    "prussian_cream": {
        "background": "#102A43",
        "text": "#FFF3D6",
        "accent": "#F26B38",
        "accent_text": "#000000",
    },
    "baltic_navy_sand": {
        "background": "#071B33",
        "text": "#F5E6C8",
        "accent": "#3DD6D0",
        "accent_text": "#000000",
    },
    "black_lime": {
        "background": "#101010",
        "text": "#F5F5F0",
        "accent": "#B6FF3B",
        "accent_text": "#000000",
    },
    "green_black_cream": {
        "background": "#0C3B2E",
        "text": "#FFF4D6",
        "accent": "#FFBA08",
        "accent_text": "#000000",
    },
    "yellow_violet": {
        "background": "#F6D743",
        "text": "#24103F",
        "accent": "#8A2BE2",
        "accent_text": "#FFFFFF",
    },
    "midnight_gold": {
        "background": "#111827",
        "text": "#FFF7D6",
        "accent": "#D6A84F",
        "accent_text": "#000000",
    },
    "museum_green_ivory": {
        "background": "#1E4033",
        "text": "#FFF6E6",
        "accent": "#D4AF37",
        "accent_text": "#000000",
    },
    "petrol_champagne": {
        "background": "#064B5B",
        "text": "#FFF0C8",
        "accent": "#F2C078",
        "accent_text": "#000000",
    },
    "slate_warm_white": {
        "background": "#263238",
        "text": "#FFF8EF",
        "accent": "#FF8A65",
        "accent_text": "#000000",
    },
    "charcoal_apricot": {
        "background": "#202124",
        "text": "#FFF3E8",
        "accent": "#FFB38A",
        "accent_text": "#000000",
    },
}

MECHANIC_WEIGHTS = {"comments": 40, "likes": 40, "reposts": 20}
MECHANIC_BADGES = {
    "comments": "НАПИШИ КОММЕНТАРИЙ",
    "likes": "ЛАЙК",
    "reposts": "РЕПОСТ",
}
SUPPORTED_TEMPLATE_IDS = {"right_extension", "bottom_overlay", "bottom_extension", "hook_swipe_cta"}


def _json_log(stage_log: StageLog) -> None:
    payload = {
        "event": "afishaengagement.decision",
        "stage": stage_log.stage,
        "decision": stage_log.decision,
        "reason": stage_log.reason,
        "event_id": stage_log.event_id,
        "campaign_id": stage_log.campaign_id,
        "activity_id": stage_log.activity_id,
        "vk_owner_id": stage_log.vk_owner_id,
        "vk_group_short": stage_log.vk_group_short,
        "event_type": stage_log.event_type,
        "has_persona": stage_log.has_persona,
        "has_festival": stage_log.has_festival,
        "seed": stage_log.seed,
        "apply_rate": stage_log.apply_rate,
        "dice_value": stage_log.dice_value,
        "mechanic": stage_log.mechanic,
        "template_id": stage_log.template_id,
        "palette_id": stage_log.palette_id,
        "card_template": stage_log.template_id or "none",
        "cta_text_final": (stage_log.cta_text_final or "")[:200] or None,
        "cta_text_len": stage_log.cta_text_len,
        "cta_text_lines": stage_log.cta_text_lines,
        "cta_text_font_px": stage_log.cta_text_font_px,
        "vision_conf_right_third": stage_log.vision_conf_right_third,
        "vision_conf_poster_text_zones": stage_log.vision_conf_poster_text_zones,
        "render_ms": stage_log.render_ms,
        "llm_ms_text": stage_log.llm_ms_text,
        "llm_ms_vision": stage_log.llm_ms_vision,
        "total_ms": stage_log.total_ms,
        "shadow_mode": stage_log.shadow_mode,
        "shadow_marker": stage_log.shadow_marker,
        "shadow_scheduled_for_ts": stage_log.shadow_scheduled_for_ts,
        "original_post_ts": stage_log.original_post_ts,
        "vk_post_id": stage_log.vk_post_id,
        "vk_post_url": stage_log.vk_post_url,
    }
    payload.update(stage_log.extra or {})
    logger.info(json.dumps({k: v for k, v in payload.items() if v is not None}, ensure_ascii=False))


def stable_unit_interval(seed: str) -> float:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12)


def media_hash(urls: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for url in urls:
        digest.update(str(url).strip().encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "да"}


def _apply_rate_from_config(config: dict[str, Any]) -> float:
    raw = config.get("apply_rate", config.get("rate", 1.0))
    try:
        value = float(raw)
    except Exception:
        value = 1.0
    if value > 1:
        value = value / 100.0
    return max(0.0, min(1.0, value))


def should_apply_rate(
    *,
    event_id: int | None,
    campaign_id: int | None,
    activity_id: int | None,
    apply_rate: float,
    salt: str = "",
    media_digest: str = "",
) -> DiceDecision:
    seed = f"{event_id or 0}:{campaign_id or 0}:{activity_id or 0}:{salt}:{media_digest}"
    value = stable_unit_interval(seed)
    return DiceDecision(seed=seed, value=value, apply_rate=apply_rate, applies=value < apply_rate)


def _target_matches(event: Event, target: PromoTarget) -> bool:
    target_type = (target.target_type or "").strip().lower()
    if target_type == "all":
        return True
    if target_type == "event":
        return target.event_id is not None and event.id is not None and int(target.event_id) == int(event.id)
    if target_type == "festival":
        return bool(target.festival_name and event.festival and target.festival_name == event.festival)
    return False


def _config_matches_event(event: Event, config: dict[str, Any]) -> bool:
    keys = config.get("event_type_keys") or config.get("event_type_filter") or []
    if isinstance(keys, str):
        keys = [keys]
    wanted = {str(key).strip().lower() for key in keys if str(key).strip()}
    if not wanted:
        return True
    actual = _event_type_key(event)
    return actual in wanted


def _group_matches(config: dict[str, Any], group_id: str) -> bool:
    expected = str(
        config.get("target_group")
        or config.get("target_group_id")
        or os.getenv("AFISHAENGAGEMENT_TARGET_GROUP_ID")
        or ""
    ).strip()
    if not expected:
        return True
    return expected.lstrip("-") == str(group_id).lstrip("-")


async def resolve_candidate(
    db: Database | None,
    event: Event,
    *,
    target_group_id: str,
    now_utc: datetime | None = None,
) -> EngagementCandidate | None:
    if db is None:
        return None
    now_utc = now_utc or datetime.now(timezone.utc)
    async with db.get_session() as session:
        query = (
            select(PromoCampaign, PromoActivity, PromoTarget)
            .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoCampaign.status == "active")
            .where(PromoCampaign.starts_at <= now_utc)
            .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
            .where(PromoActivity.enabled.is_(True))
            .where(PromoActivity.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT)
            .order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id, PromoTarget.id)
        )
        rows = list((await session.execute(query)).all())
    for campaign, activity, target in rows:
        config = dict(activity.config_json or {})
        if not _group_matches(config, target_group_id):
            continue
        if not _config_matches_event(event, config):
            continue
        if _target_matches(event, target):
            return EngagementCandidate(campaign=campaign, activity=activity, target=target, config=config)
    return None


def _event_type_key(event: Event) -> str:
    explicit_type = str(event.event_type or "").casefold()
    if any(word in explicit_type for word in ("маркет", "ярмарк")):
        return "market"
    if "фестив" in explicit_type:
        return "festival"
    raw = " ".join(
        [
            explicit_type,
            str(event.title or ""),
            str(event.description or ""),
            str(event.search_digest or ""),
        ]
    ).casefold()
    if any(word in raw for word in ("маркет", "ярмарк")):
        return "market"
    if any(word in raw for word in ("лекц", "спикер", "дискусс", "встреч")):
        return "lecture"
    if any(word in raw for word in ("концерт", "музык", "оркестр", "джаз", "трек")):
        return "concert"
    if any(word in raw for word in ("мастер-класс", "мастер класс", "воркшоп", "практик")):
        return "workshop"
    if any(word in raw for word in ("спектак", "театр", "сцен")):
        return "theatre"
    if event.festival or any(word in raw for word in ("фестив", "маркет", "ярмарк")):
        return "festival"
    return "other"


def _extract_persona(event: Event) -> str | None:
    text = f"{event.title or ''}\n{event.description or ''}\n{event.search_digest or ''}"
    patterns = [
        r"(?:лекци[яю]|встреч[ау]|концерт|спектакль)\s+(?:с|от)\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){0,2})",
        r"(?:спикер|лектор|артист|ведущ[аи]й)[:\s]+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){0,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = match.group(1).strip()
            if len(value) <= 60:
                return value
    return None


def _topic_label(event: Event, event_type: str) -> str:
    if event_type == "lecture":
        return "лекций"
    if event_type == "concert":
        return "концертов"
    if event_type == "workshop":
        return "мастер-классов"
    if event_type == "theatre":
        return "спектаклей"
    if event_type == "festival":
        return "фестивалей"
    if event_type == "market":
        return "ярмарок"
    return "событий"


def _topic_accusative_plural(event_type: str) -> str:
    if event_type == "lecture":
        return "лекции"
    if event_type == "concert":
        return "концерты"
    if event_type == "workshop":
        return "мастер-классы"
    if event_type == "theatre":
        return "спектакли"
    if event_type == "festival":
        return "фестивали"
    if event_type == "market":
        return "ярмарки"
    return "события"


def _topic_prepositional_plural(event_type: str) -> str:
    if event_type == "lecture":
        return "лекциях"
    if event_type == "concert":
        return "концертах"
    if event_type == "workshop":
        return "мастер-классах"
    if event_type == "theatre":
        return "спектаклях"
    if event_type == "festival":
        return "фестивалях"
    if event_type == "market":
        return "ярмарках"
    return "событиях"


def _templates_for(event_type: str, persona: str | None, festival: str | None) -> dict[str, list[str]]:
    topic = "{T}"
    topic_acc = "{TA}"
    festival_from = "{FFROM}"
    festival_on = "{FON}"
    templates: dict[str, list[str]] = {
        "comments": [
            f"Расскажите в комментариях, что для вас главное в таких {topic}.",
            "Были на похожем событии? Поделитесь впечатлениями.",
            "Что ждёте от этого события? Напишите в комментариях.",
        ],
        "likes": [
            f"Лайк, если хочешь чаще таких {topic}.",
            f"Поставь лайк, если хочешь чаще таких {topic}.",
            f"Поддержи лайком формат таких {topic}.",
            "Лайк, если такие события — твоё.",
            "Поставь лайк, если добавил событие в планы.",
        ],
        "reposts": [
            "Поделись с теми, кому близки такие события.",
            "Перешли тому, с кем пошёл бы вместе.",
            f"Поделись с другом, который любит такие {topic_acc}.",
        ],
    }
    if event_type == "lecture" and persona:
        templates["comments"].extend(
            [
                "Были на лекции {N}? Расскажите, что унесли с собой.",
                "Что ещё вы бы спросили у {N}? Напишите в комментариях.",
                "Если уже слушали {N} — поделитесь, стоит ли идти впервые.",
            ]
        )
        templates["likes"].append("Лайк, если нравится, как {N} ведёт лекции.")
        templates["likes"].append("Поставь лайк, если уже слушал {N}.")
        templates["reposts"].append("Перешли другу, кто точно хочет на {N}.")
    elif event_type == "concert" and persona:
        templates["comments"].extend(
            [
                "Были на концертах {N}? Какой запомнился?",
                "Какой трек {N} ждёте живьём? Делитесь.",
            ]
        )
        templates["likes"].append("Лайк, если нравится творчество {N}.")
        templates["reposts"].append("Перешли другу, кто слушает {N}.")
    elif event_type == "workshop":
        templates["comments"].extend(
            [
                "Что хотели бы освоить на мастер-классе? Напишите в комментариях.",
                "Какой формат интереснее: теория или практика? Делитесь.",
            ]
        )
    elif event_type == "theatre":
        templates["comments"].extend(
            [
                "Какой спектакль в Калининграде вы пересмотрели бы?",
                "Что важнее: режиссура или актёрская игра?",
            ]
        )
    if festival:
        templates["comments"].extend(
            [
                f"Были на {festival_on} раньше? Расскажите, как впечатления.",
                f"Что ждёте от {festival_from}? Напишите в комментариях.",
            ]
        )
        templates["likes"].extend(
            [
                f"Лайк, если ждёшь {festival_from}.",
                f"Поставь лайк, если уже был на {festival_on}.",
            ]
        )
        templates["reposts"].append(f"Поделись с друзьями, если ждёшь {festival_from}.")
    return templates


def _sanitize_cta_text(text: str) -> str:
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    replacements = {
        "сохранил(а)": "добавил",
        "был(а)": "был",
        "позвал(а)": "позвал",
        "пошёл(а)": "пошёл",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(r"\s+([,.!?;:])", r"\1", text)
    text = re.sub(r"\bтакие,\s+([А-Яа-яЁё-]+)", r"такие \1", text)
    return text


def _festival_is_annual(event: Event) -> bool:
    text = "\n".join(
        [
            str(event.title or ""),
            str(event.description or ""),
            str(event.source_text or ""),
            str(event.search_digest or ""),
        ]
    ).casefold()
    if any(word in text for word in ("ежегод", "каждый год", "в этом году")):
        return True
    return bool(re.search(r"\b(?:[ivxlcdm]{2,}|[2-9]\d*)\s+(?:международн\w+\s+)?фестив", text))


def _festival_from_phrase(event: Event, festival: str | None) -> str | None:
    if not festival:
        return None
    phrase = f"фестиваля {festival}"
    if _festival_is_annual(event):
        phrase += " в этом году"
    return phrase


def _festival_on_phrase(festival: str | None) -> str | None:
    if not festival:
        return None
    return f"фестивале {festival}"


def _resolve_template(
    template: str,
    *,
    persona: str | None,
    festival: str | None,
    topic: str,
    topic_accusative_plural: str,
    festival_from: str | None = None,
    festival_on: str | None = None,
) -> str | None:
    slots = {
        "N": persona,
        "F": festival,
        "FFROM": festival_from,
        "FON": festival_on,
        "T": topic,
        "TA": topic_accusative_plural,
    }
    for key, value in slots.items():
        if "{" + key + "}" in template and not value:
            return None
        template = template.replace("{" + key + "}", str(value or ""))
    if re.search(r"\{[^}]+\}", template):
        return None
    return _sanitize_cta_text(template)


def _configured_formats(config: dict[str, Any]) -> list[str]:
    raw = config.get("formats") or config.get("template_ids") or []
    if isinstance(raw, str):
        raw = [part.strip() for part in raw.split(",")]
    formats = [str(item).strip() for item in raw if str(item or "").strip()]
    formats = [item for item in formats if item in SUPPORTED_TEMPLATE_IDS]
    if not formats:
        template_id = str(config.get("template_id") or "right_extension").strip()
        formats = [template_id if template_id in SUPPORTED_TEMPLATE_IDS else "right_extension"]
    return formats


def build_engagement_plan(
    event: Event,
    *,
    seed: str,
    config: dict[str, Any] | None = None,
    vision: PosterVisionSummary | None = None,
) -> EngagementPlan:
    config = config or {}
    rnd = random.Random(hashlib.sha256(seed.encode("utf-8")).hexdigest())
    event_type = _event_type_key(event)
    persona = _extract_persona(event)
    festival = (event.festival or "").strip() or None
    festival_from = _festival_from_phrase(event, festival)
    festival_on = _festival_on_phrase(festival)
    topic = _topic_label(event, event_type)
    topic_accusative_plural = _topic_accusative_plural(event_type)
    topic_prepositional_plural = _topic_prepositional_plural(event_type)

    weights = dict(MECHANIC_WEIGHTS)
    weights.update(config.get("mechanic_weights") or {})
    bag: list[str] = []
    for mechanic, weight in weights.items():
        try:
            count = max(0, int(weight))
        except Exception:
            count = 0
        bag.extend([mechanic] * count)
    if not bag:
        bag = ["comments", "likes", "reposts"]
    rnd.shuffle(bag)

    templates = _templates_for(event_type, persona, festival)
    cta_text = ""
    mechanic = "comments"
    for candidate_mechanic in bag[:20]:
        options = list(templates.get(candidate_mechanic) or [])
        rnd.shuffle(options)
        for template in options:
            resolved = _resolve_template(
                template,
                persona=persona,
                festival=festival,
                festival_from=festival_from,
                festival_on=festival_on,
                topic=topic,
                topic_accusative_plural=topic_accusative_plural,
            )
            if resolved and len(resolved) <= 95:
                cta_text = resolved
                mechanic = candidate_mechanic
                break
        if cta_text:
            break
    if not cta_text:
        mechanic = "likes"
        cta_text = "Лайк, если такие события — твоё."

    palette_ids = list(config.get("palette_ids") or PALETTES.keys())
    palette_ids = [pid for pid in palette_ids if pid in PALETTES] or ["slate_warm_white"]
    palette_id = palette_ids[int(stable_unit_interval(seed + ":palette") * len(palette_ids)) % len(palette_ids)]
    formats = _configured_formats(config)
    template_id = formats[int(stable_unit_interval(seed + ":format") * len(formats)) % len(formats)]
    if vision and vision.confidence < float(config.get("right_extension_confidence_min", 0.55)):
        if "right_extension" in formats:
            template_id = "right_extension"
        elif "hook_swipe_cta" in formats:
            template_id = "hook_swipe_cta"
    hook_text = f"Есть вопрос к тем, кто уже был на таких {topic_prepositional_plural}"
    if festival:
        hook_text = f"Кто уже был на {festival_on}?"
    elif persona:
        hook_text = f"Кто уже слушал {persona}?"
    return EngagementPlan(
        mechanic=mechanic,
        template_id=template_id,
        palette_id=palette_id,
        cta_text=cta_text,
        hook_text=hook_text,
        event_type=event_type,
        has_persona=persona is not None,
        has_festival=festival is not None,
        seed=seed,
    )


def _extract_json_object(text: str) -> dict[str, Any] | None:
    value = (text or "").strip()
    if not value:
        return None
    if value.startswith("```"):
        value = re.sub(r"^```(?:json)?\s*", "", value, flags=re.IGNORECASE).strip()
        value = re.sub(r"\s*```$", "", value).strip()
    try:
        parsed = json.loads(value)
    except Exception:
        match = re.search(r"\{.*\}", value, flags=re.S)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return None
    return parsed if isinstance(parsed, dict) else None


def _sanitize_llm_plan(
    event: Event,
    *,
    seed: str,
    config: dict[str, Any],
    vision: PosterVisionSummary | None,
    payload: dict[str, Any],
) -> EngagementPlan | None:
    fallback = build_engagement_plan(event, seed=seed, config=config, vision=vision)
    mechanic = str(payload.get("mechanic") or fallback.mechanic).strip().lower()
    if mechanic not in MECHANIC_BADGES:
        mechanic = fallback.mechanic
    template_id = str(payload.get("template_id") or fallback.template_id).strip()
    if template_id not in SUPPORTED_TEMPLATE_IDS:
        template_id = fallback.template_id
    palette_id = str(payload.get("palette_id") or fallback.palette_id).strip()
    if palette_id not in PALETTES:
        palette_id = fallback.palette_id
    cta_text = _sanitize_cta_text(str(payload.get("cta_text") or ""))
    if not cta_text or len(cta_text) > 95 or re.search(r"\{[^}]+\}", cta_text):
        return None
    hook_text = _sanitize_cta_text(str(payload.get("hook_text") or "")) or fallback.hook_text
    return EngagementPlan(
        mechanic=mechanic,
        template_id=template_id,
        palette_id=palette_id,
        cta_text=cta_text,
        hook_text=hook_text[:95] if hook_text else None,
        event_type=fallback.event_type,
        has_persona=fallback.has_persona,
        has_festival=fallback.has_festival,
        seed=seed,
    )


async def build_llm_engagement_plan(
    event: Event,
    *,
    seed: str,
    config: dict[str, Any],
    vision: PosterVisionSummary | None,
) -> tuple[EngagementPlan, int, str]:
    started = time.monotonic()
    fallback = build_engagement_plan(event, seed=seed, config=config, vision=vision)
    try:
        import main as main_mod

        ask_4o = getattr(main_mod, "ask_4o", None)
        if ask_4o is None:
            return fallback, 0, "fallback_no_ask_4o"
        palette_ids = list(config.get("palette_ids") or PALETTES.keys())
        palette_ids = [pid for pid in palette_ids if pid in PALETTES] or list(PALETTES.keys())
        prompt = (
            "Ты выбираешь план CTA-мотиватора для VK-афиши события. "
            "Верни только JSON без markdown: "
            "{\"mechanic\":\"comments|likes|reposts\",\"template_id\":\"right_extension|bottom_overlay|bottom_extension|hook_swipe_cta\","
            "\"palette_id\":\"...\",\"cta_text\":\"...\",\"hook_text\":\"...\",\"reason\":\"...\"}.\n"
            "Правила: русский текст, обращение на ты допустимо для лайков/репостов; "
            "cta_text максимум 95 символов; без плейсхолдеров, без ФИО если ФИО нет в данных; "
            "не обещай факт, которого нет; для плотной/неуверенной афиши предпочитай right_extension или hook_swipe_cta; "
            "финальный layout рисует код, не описывай графику.\n"
            f"Доступные palette_id: {', '.join(palette_ids[:16])}.\n"
            f"Seed: {seed}\n"
            f"Событие: title={event.title!r}; type={event.event_type!r}; festival={event.festival!r}; "
            f"date={event.date!r}; place={event.location_name!r}; description={(event.description or '')[:800]!r}; "
            f"search_digest={(event.search_digest or '')[:500]!r}.\n"
            f"Vision/OCR confidence={vision.confidence if vision else None}; "
            f"poster_text={(vision.text if vision else '')[:900]!r}."
        )
        raw = await ask_4o(prompt, max_tokens=220)
        payload = _extract_json_object(str(raw or ""))
        if not payload:
            return fallback, int((time.monotonic() - started) * 1000), "fallback_bad_json"
        plan = _sanitize_llm_plan(event, seed=seed, config=config, vision=vision, payload=payload)
        if plan is None:
            return fallback, int((time.monotonic() - started) * 1000), "fallback_invalid_plan"
        return plan, int((time.monotonic() - started) * 1000), "llm_plan"
    except Exception as exc:
        logger.warning("afishaengagement llm plan failed: %s", exc)
        return fallback, int((time.monotonic() - started) * 1000), "fallback_llm_error"


def _font_path(name: str) -> Path:
    root = Path(__file__).resolve().parent
    return root / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / name


def _load_font(name: str, size: int):
    from PIL import ImageFont

    path = _font_path(name)
    if not path.exists():
        raise FileNotFoundError(str(path))
    return ImageFont.truetype(str(path), size=size)


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    value = value.strip().lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def _relative_luminance(rgb: tuple[int, int, int]) -> float:
    values = []
    for channel in rgb:
        c = channel / 255.0
        values.append(c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4)
    return 0.2126 * values[0] + 0.7152 * values[1] + 0.0722 * values[2]


def _contrast_ratio(a: tuple[int, int, int], b: tuple[int, int, int]) -> float:
    l1 = _relative_luminance(a)
    l2 = _relative_luminance(b)
    lighter = max(l1, l2)
    darker = min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def _rgb_hls(rgb: tuple[int, int, int]) -> tuple[float, float, float]:
    r, g, b = [channel / 255.0 for channel in rgb]
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    return h * 360.0, l, s


def _hue_distance(a: float, b: float) -> float:
    diff = abs(a - b) % 360.0
    return min(diff, 360.0 - diff)


def _harmony_score(bg_hue: float, bg_sat: float, dominant_hues: Sequence[float]) -> float:
    if bg_sat < 0.18 or not dominant_hues:
        return 1.0
    best = 0.0
    for hue in dominant_hues:
        dist = _hue_distance(bg_hue, hue)
        if dist <= 35:
            score = 1.0 - dist / 70.0
        elif 145 <= dist <= 215:
            score = 0.95 - abs(180 - dist) / 140.0
        elif 95 <= dist <= 135 or 225 <= dist <= 265:
            score = 0.78
        elif 55 <= dist <= 95:
            score = 0.25
        else:
            score = 0.5
        best = max(best, score)
    return best


def _poster_color_profile(poster: Any) -> dict[str, Any]:
    from PIL import Image

    sample = poster.convert("RGB")
    sample.thumbnail((96, 96), Image.Resampling.LANCZOS)
    quantized = sample.quantize(colors=8, method=Image.Quantize.MEDIANCUT).convert("RGB")
    colors = quantized.getcolors(maxcolors=96 * 96) or []
    colors.sort(reverse=True, key=lambda item: item[0])
    dominant: list[tuple[int, int, int]] = [rgb for _count, rgb in colors[:5]]
    saturated_hues = []
    for rgb in dominant:
        hue, _lightness, saturation = _rgb_hls(rgb)
        if saturation >= 0.2:
            saturated_hues.append(hue)
    edge_w = max(1, int(poster.width * 0.08))
    edge = poster.crop((poster.width - edge_w, 0, poster.width, poster.height)).resize((1, 1), Image.Resampling.BOX)
    edge_rgb = edge.getpixel((0, 0))
    return {
        "dominant": dominant,
        "dominant_hues": saturated_hues,
        "edge_rgb": edge_rgb,
    }


def _average_region_rgb(poster: Any, box: tuple[int, int, int, int]) -> tuple[int, int, int]:
    from PIL import Image

    width, height = poster.size
    left, top, right, bottom = box
    left = max(0, min(width - 1, left))
    top = max(0, min(height - 1, top))
    right = max(left + 1, min(width, right))
    bottom = max(top + 1, min(height, bottom))
    sample = poster.convert("RGB").crop((left, top, right, bottom)).resize((1, 1), Image.Resampling.BOX)
    return sample.getpixel((0, 0))


def _inverted_surface_palette(palette: dict[str, str]) -> dict[str, str]:
    return {
        "background": palette["text"],
        "text": palette["background"],
        "accent": palette["background"],
        "accent_text": palette["text"],
    }


def _cta_surface_palette_for_region(
    poster: Any,
    palette: dict[str, str],
    *,
    seed: str,
    region_box: tuple[int, int, int, int],
) -> tuple[dict[str, str], bool]:
    bg = _hex_to_rgb(palette["background"])
    avg = _average_region_rgb(poster, region_box)
    bg_luma = _relative_luminance(bg)
    avg_luma = _relative_luminance(avg)
    contrast = _contrast_ratio(bg, avg)
    should_invert = (bg_luma < 0.24 and avg_luma < 0.48) or contrast < 1.8
    # Keep some deterministic variety for borderline cases so the CTA surface
    # does not always repeat the same visual temperature across similar posters.
    if not should_invert and contrast < 2.5:
        should_invert = stable_unit_interval(f"{seed}:surface_invert") < 0.35
    if not should_invert:
        return palette, False
    inverted = _inverted_surface_palette(palette)
    inv_bg = _hex_to_rgb(inverted["background"])
    inv_text = _hex_to_rgb(inverted["text"])
    if _contrast_ratio(inv_bg, inv_text) < 4.5:
        return palette, False
    return inverted, True


def _choose_compatible_palette_id(poster: Any, preferred_id: str, seed: str) -> str:
    profile = _poster_color_profile(poster)
    dominant_hues = list(profile.get("dominant_hues") or [])
    edge_rgb = profile.get("edge_rgb") or (128, 128, 128)
    scored: list[tuple[float, str]] = []
    for palette_id, palette in PALETTES.items():
        bg = _hex_to_rgb(palette["background"])
        text = _hex_to_rgb(palette["text"])
        accent = _hex_to_rgb(palette["accent"])
        text_contrast = _contrast_ratio(bg, text)
        accent_contrast = _contrast_ratio(bg, accent)
        if text_contrast < 4.5:
            continue
        bg_hue, _bg_lightness, bg_sat = _rgb_hls(bg)
        harmony = _harmony_score(bg_hue, bg_sat, dominant_hues)
        edge_contrast = _contrast_ratio(bg, edge_rgb)
        edge_score = 1.0 - min(abs(edge_contrast - 2.8), 2.8) / 2.8
        jitter = stable_unit_interval(f"{seed}:palette_compat:{palette_id}") * 0.08
        preferred_bonus = 0.12 if palette_id == preferred_id else 0.0
        score = (
            min(text_contrast, 10.0) * 0.32
            + min(accent_contrast, 8.0) * 0.12
            + harmony * 1.7
            + edge_score * 0.75
            + preferred_bonus
            + jitter
        )
        scored.append((score, palette_id))
    if not scored:
        return preferred_id if preferred_id in PALETTES else "slate_warm_white"
    scored.sort(reverse=True)
    top = scored[: min(4, len(scored))]
    index = int(stable_unit_interval(f"{seed}:palette_compat_pick") * len(top)) % len(top)
    return top[index][1]


def _text_bbox(draw: Any, xy: tuple[int, int], text: str, font: Any) -> tuple[int, int, int, int]:
    return draw.textbbox(xy, text, font=font)


def _wrap_words(draw: Any, text: str, font: Any, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        width = _text_bbox(draw, (0, 0), candidate, font)[2]
        if current and width > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


def fit_text(
    text: str,
    *,
    box_width: int,
    box_height: int,
    preferred_px: int = 78,
    min_px: int = 52,
    max_lines: int = 5,
    font_name: str = "Cygre-Bold.ttf",
) -> TextFit | None:
    from PIL import Image, ImageDraw

    probe = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(probe)
    for size in range(preferred_px, min_px - 1, -4):
        font = _load_font(font_name, size)
        lines = _wrap_words(draw, text, font, box_width)
        if len(lines) > max_lines:
            continue
        if any(_text_bbox(draw, (0, 0), line, font)[2] > box_width for line in lines):
            continue
        line_heights = []
        max_w = 0
        for line in lines:
            bbox = _text_bbox(draw, (0, 0), line, font)
            max_w = max(max_w, bbox[2] - bbox[0])
            line_heights.append(bbox[3] - bbox[1])
        total_h = int(sum(line_heights) + max(0, len(lines) - 1) * size * 0.08)
        if total_h <= box_height:
            return TextFit(font_px=size, lines=lines, width=max_w, height=total_h)
    return None


def _layout_scale(reference_px: int, base_px: int = 1080) -> float:
    return max(0.55, min(2.2, reference_px / base_px))


def _aa_overlay(
    *,
    size: tuple[int, int],
    polygon: list[tuple[int, int]],
    fill: tuple[int, int, int, int],
    line: list[tuple[int, int]] | None = None,
    line_fill: tuple[int, int, int, int] | None = None,
    line_width: int = 4,
    aa_scale: int = 4,
) -> Any:
    from PIL import Image, ImageDraw

    aa_scale = max(2, int(aa_scale))
    width, height = size
    hi = Image.new("RGBA", (width * aa_scale, height * aa_scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(hi)

    def scale_points(points: list[tuple[int, int]]) -> list[tuple[int, int]]:
        return [(int(x * aa_scale), int(y * aa_scale)) for x, y in points]

    draw.polygon(scale_points(polygon), fill=fill)
    if line and line_fill:
        draw.line(scale_points(line), fill=line_fill, width=max(1, int(line_width * aa_scale)))
    return hi.resize((width, height), Image.Resampling.LANCZOS)


def render_right_extension(source_image: bytes, plan: EngagementPlan) -> RenderedImage:
    from PIL import Image, ImageDraw, ImageOps

    started = time.monotonic()
    if not source_image:
        raise ValueError("source poster is empty")
    with Image.open(io.BytesIO(source_image)) as opened:
        poster = ImageOps.exif_transpose(opened).convert("RGB")
    palette_id = _choose_compatible_palette_id(poster, plan.palette_id, plan.seed)
    palette = PALETTES.get(palette_id) or PALETTES["slate_warm_white"]
    bg = _hex_to_rgb(palette["background"])
    text_color = _hex_to_rgb(palette["text"])
    accent = _hex_to_rgb(palette["accent"])
    accent_text = _hex_to_rgb(palette["accent_text"])

    if poster.width > poster.height * 1.12:
        return _render_bottom_extension(poster, plan, palette_id, palette, started)

    poster_w, height = poster.size
    scale = _layout_scale(height)
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_font = _load_font("Cygre-Medium.ttf", max(18, int(30 * scale)))
    diagonal = max(int(24 * scale), min(int(86 * scale), int(poster_w * 0.085)))

    fit: TextFit | None = None
    width = 0
    safe_x = 0
    safe_y = int(84 * scale)
    safe_w = 0
    text_box_y = 0
    text_box_h = 0
    start_w = max(int(460 * scale), int(height * 0.43))
    end_w = max(start_w + int(80 * scale), int(860 * scale))
    step_w = max(18, int(40 * scale))
    for cta_w in range(start_w, end_w + 1, step_w):
        width = poster_w + cta_w
        safe_x = poster_w + int(52 * scale)
        safe_w = width - safe_x - int(60 * scale)
        text_box_y = safe_y + int(48 * scale) + int(72 * scale)
        text_box_h = height - text_box_y - int(150 * scale)
        fit = fit_text(
            plan.cta_text,
            box_width=safe_w,
            box_height=text_box_h,
            preferred_px=max(30, int(78 * scale)),
            min_px=max(24, int(46 * scale)),
        )
        if fit is not None:
            break
    if fit is None:
        raise ValueError("text_overflow")

    canvas = Image.new("RGB", (width, height), bg)
    canvas.paste(poster, (0, 0))

    cut_top = poster_w - int(diagonal * 0.35)
    cut_bottom = poster_w - diagonal
    overlay = _aa_overlay(
        size=(width, height),
        polygon=[(cut_top, 0), (width, 0), (width, height), (cut_bottom, height)],
        fill=(*bg, 255),
        line=[(cut_top, 0), (cut_bottom, height)],
        line_fill=(*accent, 255),
        line_width=max(2, int(4 * scale)),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(canvas)

    badge_bbox = _text_bbox(draw, (0, 0), badge, badge_font)
    badge_w = badge_bbox[2] - badge_bbox[0] + int(34 * scale)
    badge_h = int(48 * scale)
    draw.rounded_rectangle(
        (safe_x, safe_y, safe_x + badge_w, safe_y + badge_h),
        radius=int(18 * scale),
        fill=accent,
    )
    draw.text((safe_x + int(17 * scale), safe_y + int(8 * scale)), badge, font=badge_font, fill=accent_text)

    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.08)
    total_h = fit.height
    y = text_box_y + max(0, int((text_box_h - total_h) / 2))
    for line in fit.lines:
        draw.text((safe_x, y), line, font=main_font, fill=text_color)
        bbox = _text_bbox(draw, (safe_x, y), line, main_font)
        y += (bbox[3] - bbox[1]) + line_gap
    draw.line(
        (safe_x, height - int(80 * scale), width - int(64 * scale), height - int(80 * scale)),
        fill=accent,
        width=max(2, int(4 * scale)),
    )

    out = io.BytesIO()
    canvas.save(out, format="PNG", optimize=True)
    data = out.getvalue()
    if len(data) < 1_000:
        raise ValueError("rendered_png_too_small")
    return RenderedImage(
        data=data,
        filename="afishaengagement-right-extension.png",
        template_id="right_extension",
        palette_id=palette_id,
        cta_text_lines=fit.lines,
        cta_text_font_px=fit.font_px,
        dimensions=(width, height),
        render_ms=int((time.monotonic() - started) * 1000),
    )


def _render_bottom_extension(
    poster: Any,
    plan: EngagementPlan,
    palette_id: str,
    palette: dict[str, str],
    started: float,
) -> RenderedImage:
    from PIL import Image, ImageDraw

    bg = _hex_to_rgb(palette["background"])
    text_color = _hex_to_rgb(palette["text"])
    accent = _hex_to_rgb(palette["accent"])
    accent_text = _hex_to_rgb(palette["accent_text"])

    width, poster_h = poster.size
    scale = _layout_scale(width)
    overlap = max(int(42 * scale), min(int(82 * scale), int(poster_h * 0.12)))
    diagonal = max(int(22 * scale), min(int(46 * scale), int(width * 0.035)))
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_font = _load_font("Cygre-Medium.ttf", max(18, int(28 * scale)))

    fit: TextFit | None = None
    block_h = 0
    safe_x = int(72 * scale)
    safe_w = width - safe_x * 2
    start_h = max(int(330 * scale), int(poster_h * 0.54))
    end_h = max(start_h + int(72 * scale), int(590 * scale))
    step_h = max(18, int(36 * scale))
    for candidate_h in range(start_h, end_h + 1, step_h):
        text_box_h = candidate_h - int(142 * scale)
        fit = fit_text(
            plan.cta_text,
            box_width=safe_w,
            box_height=text_box_h,
            preferred_px=max(30, int(66 * scale)),
            min_px=max(24, int(44 * scale)),
            max_lines=4,
        )
        if fit is not None:
            block_h = candidate_h
            break
    if fit is None:
        raise ValueError("text_overflow")

    height = poster_h + block_h - overlap
    block_top = poster_h - overlap
    canvas = Image.new("RGB", (width, height), bg)
    canvas.paste(poster, (0, 0))

    overlay = _aa_overlay(
        size=(width, height),
        polygon=[(0, block_top + diagonal), (width, block_top), (width, height), (0, height)],
        fill=(*bg, 255),
        line=[(0, block_top + diagonal), (width, block_top)],
        line_fill=(*accent, 255),
        line_width=max(2, int(4 * scale)),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(canvas)

    badge_bbox = _text_bbox(draw, (0, 0), badge, badge_font)
    badge_w = badge_bbox[2] - badge_bbox[0] + int(32 * scale)
    badge_h = int(46 * scale)
    safe_y = block_top + int(46 * scale)
    draw.rounded_rectangle(
        (safe_x, safe_y, safe_x + badge_w, safe_y + badge_h),
        radius=int(17 * scale),
        fill=accent,
    )
    draw.text((safe_x + int(16 * scale), safe_y + int(8 * scale)), badge, font=badge_font, fill=accent_text)

    text_box_y = safe_y + badge_h + int(32 * scale)
    text_box_h = height - text_box_y - int(58 * scale)
    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.08)
    y = text_box_y + max(0, int((text_box_h - fit.height) / 2))
    for line in fit.lines:
        draw.text((safe_x, y), line, font=main_font, fill=text_color)
        bbox = _text_bbox(draw, (safe_x, y), line, main_font)
        y += (bbox[3] - bbox[1]) + line_gap
    draw.line(
        (safe_x, height - int(42 * scale), width - safe_x, height - int(42 * scale)),
        fill=accent,
        width=max(2, int(4 * scale)),
    )

    out = io.BytesIO()
    canvas.save(out, format="PNG", optimize=True)
    data = out.getvalue()
    if len(data) < 1_000:
        raise ValueError("rendered_png_too_small")
    return RenderedImage(
        data=data,
        filename="afishaengagement-bottom-extension.png",
        template_id="bottom_extension",
        palette_id=palette_id,
        cta_text_lines=fit.lines,
        cta_text_font_px=fit.font_px,
        dimensions=(width, height),
        render_ms=int((time.monotonic() - started) * 1000),
    )


def _render_bottom_overlay(
    poster: Any,
    plan: EngagementPlan,
    palette_id: str,
    palette: dict[str, str],
    started: float,
) -> RenderedImage:
    from PIL import Image, ImageDraw

    width, height = poster.size
    scale = _layout_scale(width)
    block_h = max(int(260 * scale), min(int(height * 0.38), int(420 * scale)))
    block_top = height - block_h
    diagonal = max(int(24 * scale), min(int(54 * scale), int(width * 0.05)))
    palette, surface_inverted = _cta_surface_palette_for_region(
        poster,
        palette,
        seed=plan.seed,
        region_box=(0, max(0, block_top - int(48 * scale)), width, height),
    )
    bg = _hex_to_rgb(palette["background"])
    text_color = _hex_to_rgb(palette["text"])
    accent = _hex_to_rgb(palette["accent"])
    accent_text = _hex_to_rgb(palette["accent_text"])

    safe_x = int(64 * scale)
    safe_w = width - safe_x * 2
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_font = _load_font("Cygre-Medium.ttf", max(18, int(28 * scale)))
    badge_h = int(46 * scale)
    safe_y = block_top + diagonal + int(34 * scale)
    text_box_y = safe_y + badge_h + int(30 * scale)
    text_box_h = height - text_box_y - int(40 * scale)

    fit = fit_text(
        plan.cta_text,
        box_width=safe_w,
        box_height=text_box_h,
        preferred_px=max(30, int(62 * scale)),
        min_px=max(22, int(38 * scale)),
        max_lines=4,
    )
    if fit is None:
        raise ValueError("text_overflow")

    canvas = poster.copy()
    overlay = _aa_overlay(
        size=(width, height),
        polygon=[(0, block_top + diagonal), (width, block_top), (width, height), (0, height)],
        fill=(*bg, 248 if surface_inverted else 238),
        line=[(0, block_top + diagonal), (width, block_top)],
        line_fill=(*accent, 255),
        line_width=max(2, int(4 * scale)),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(canvas)

    badge_bbox = _text_bbox(draw, (0, 0), badge, badge_font)
    badge_w = min(safe_w, badge_bbox[2] - badge_bbox[0] + int(32 * scale))
    draw.rounded_rectangle(
        (safe_x, safe_y, safe_x + badge_w, safe_y + badge_h),
        radius=int(17 * scale),
        fill=accent,
    )
    draw.text((safe_x + int(16 * scale), safe_y + int(8 * scale)), badge, font=badge_font, fill=accent_text)

    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.08)
    y = text_box_y + max(0, int((text_box_h - fit.height) / 2))
    for line in fit.lines:
        draw.text((safe_x, y), line, font=main_font, fill=text_color)
        bbox = _text_bbox(draw, (safe_x, y), line, main_font)
        y += (bbox[3] - bbox[1]) + line_gap

    out = io.BytesIO()
    canvas.save(out, format="PNG", optimize=True)
    data = out.getvalue()
    if len(data) < 1_000:
        raise ValueError("rendered_png_too_small")
    return RenderedImage(
        data=data,
        filename="afishaengagement-bottom-overlay.png",
        template_id="bottom_overlay",
        palette_id=palette_id,
        cta_text_lines=fit.lines,
        cta_text_font_px=fit.font_px,
        dimensions=(width, height),
        render_ms=int((time.monotonic() - started) * 1000),
    )


def _render_hook_swipe_cta(
    poster: Any,
    plan: EngagementPlan,
    palette_id: str,
    palette: dict[str, str],
    started: float,
) -> list[RenderedImage]:
    from PIL import Image, ImageDraw, ImageOps

    bg = _hex_to_rgb(palette["background"])
    text_color = _hex_to_rgb(palette["text"])
    accent = _hex_to_rgb(palette["accent"])
    accent_text = _hex_to_rgb(palette["accent_text"])
    card_w, card_h = 1080, 1350
    scale = card_w / 1080

    def cover_image() -> Image.Image:
        return ImageOps.fit(poster, (card_w, card_h), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))

    def save_card(image: Image.Image, filename: str, template_id: str, lines: list[str], font_px: int) -> RenderedImage:
        out = io.BytesIO()
        image.save(out, format="PNG", optimize=True)
        data = out.getvalue()
        return RenderedImage(
            data=data,
            filename=filename,
            template_id=template_id,
            palette_id=palette_id,
            cta_text_lines=lines,
            cta_text_font_px=font_px,
            dimensions=(card_w, card_h),
            render_ms=int((time.monotonic() - started) * 1000),
        )

    hook_text = _sanitize_cta_text(plan.hook_text or "Есть вопрос к тем, кто уже был?")
    hook = cover_image().convert("RGBA")
    scrim = Image.new("RGBA", (card_w, card_h), (*bg, 188))
    hook = Image.alpha_composite(hook, scrim).convert("RGB")
    draw = ImageDraw.Draw(hook)
    hook_fit = fit_text(
        hook_text,
        box_width=int(card_w * 0.78),
        box_height=int(card_h * 0.42),
        preferred_px=92,
        min_px=54,
        max_lines=4,
    )
    if hook_fit is None:
        raise ValueError("hook_text_overflow")
    hook_font = _load_font("Cygre-Bold.ttf", hook_fit.font_px)
    x = int(86 * scale)
    y = int(410 * scale)
    for line in hook_fit.lines:
        draw.text((x, y), line, font=hook_font, fill=text_color)
        bbox = _text_bbox(draw, (x, y), line, hook_font)
        y += (bbox[3] - bbox[1]) + int(hook_fit.font_px * 0.1)
    swipe_font = _load_font("Cygre-Medium.ttf", 46)
    draw.rounded_rectangle((x, card_h - 188, x + 220, card_h - 124), radius=26, fill=accent)
    draw.text((x + 34, card_h - 176), "ЛИСТАЙ", font=swipe_font, fill=accent_text)
    draw.line((x, card_h - 92, card_w - x, card_h - 92), fill=accent, width=5)

    cta = Image.new("RGB", (card_w, card_h), bg)
    draw = ImageDraw.Draw(cta)
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_font = _load_font("Cygre-Medium.ttf", 42)
    badge_bbox = _text_bbox(draw, (0, 0), badge, badge_font)
    badge_w = badge_bbox[2] - badge_bbox[0] + 52
    draw.rounded_rectangle((86, 96, 86 + badge_w, 162), radius=28, fill=accent)
    draw.text((112, 111), badge, font=badge_font, fill=accent_text)

    cta_fit = fit_text(
        plan.cta_text,
        box_width=int(card_w * 0.78),
        box_height=int(card_h * 0.48),
        preferred_px=94,
        min_px=54,
        max_lines=5,
    )
    if cta_fit is None:
        raise ValueError("cta_text_overflow")
    cta_font = _load_font("Cygre-Bold.ttf", cta_fit.font_px)
    y = int(390 * scale)
    for line in cta_fit.lines:
        draw.text((86, y), line, font=cta_font, fill=text_color)
        bbox = _text_bbox(draw, (86, y), line, cta_font)
        y += (bbox[3] - bbox[1]) + int(cta_fit.font_px * 0.08)
    arrow_x = card_w // 2
    arrow_top = card_h - 330
    draw.line((arrow_x, arrow_top, arrow_x, arrow_top + 170), fill=accent, width=10)
    draw.polygon(
        [
            (arrow_x - 48, arrow_top + 132),
            (arrow_x + 48, arrow_top + 132),
            (arrow_x, arrow_top + 204),
        ],
        fill=accent,
    )
    draw.line((86, card_h - 92, card_w - 86, card_h - 92), fill=accent, width=5)

    return [
        save_card(hook, "afishaengagement-hook-swipe.png", "hook_swipe", hook_fit.lines, hook_fit.font_px),
        save_card(cta, "afishaengagement-cta-card.png", "hook_swipe_cta", cta_fit.lines, cta_fit.font_px),
    ]


def render_plan_images(source_image: bytes, plan: EngagementPlan) -> list[RenderedImage]:
    from PIL import Image, ImageOps

    started = time.monotonic()
    if not source_image:
        raise ValueError("source poster is empty")
    if plan.template_id == "right_extension":
        return [render_right_extension(source_image, plan)]

    with Image.open(io.BytesIO(source_image)) as opened:
        poster = ImageOps.exif_transpose(opened).convert("RGB")
    palette_id = _choose_compatible_palette_id(poster, plan.palette_id, plan.seed)
    palette = PALETTES.get(palette_id) or PALETTES["slate_warm_white"]
    if plan.template_id == "bottom_overlay":
        return [_render_bottom_overlay(poster, plan, palette_id, palette, started)]
    if plan.template_id == "bottom_extension":
        return [_render_bottom_extension(poster, plan, palette_id, palette, started)]
    if plan.template_id == "hook_swipe_cta":
        return _render_hook_swipe_cta(poster, plan, palette_id, palette, started)
    return [render_right_extension(source_image, plan)]


async def _default_fetch_image(url: str) -> bytes:
    import main as main_mod

    session = main_mod.get_http_session()
    semaphore = main_mod.HTTP_SEMAPHORE
    async with semaphore:
        async with session.get(url, timeout=30) as response:
            if response.status != 200:
                raise RuntimeError(f"image_download_status_{response.status}")
            return await response.read()


async def analyze_poster_vision(
    db: Database | None,
    image_bytes: bytes,
    *,
    enabled: bool = True,
) -> PosterVisionSummary:
    if not enabled or db is None:
        return PosterVisionSummary(provider="disabled", confidence=0.0, reason="vision_disabled")
    started = time.monotonic()
    try:
        import poster_ocr

        results, _, _ = await poster_ocr.recognize_posters(
            db,
            [(image_bytes, "afishaengagement.png")],
            detail="auto",
            log_context={"feature": PROMO_SURFACE_AFISHA_ENGAGEMENT},
        )
        text = (results[0].text if results else "") or ""
        dense = len(text) > 260 or text.count("\n") > 12
        confidence = 0.45 if dense else 0.62
        return PosterVisionSummary(
            provider="poster_ocr",
            confidence=confidence,
            text=text[:1000],
            right_third_clean=None,
            reason=f"ocr_text_len={len(text)} ms={int((time.monotonic() - started) * 1000)}",
        )
    except Exception as exc:
        logger.warning("afishaengagement vision failed: %s", exc)
        return PosterVisionSummary(provider="poster_ocr", confidence=0.0, reason="vision_error")


def _env_debug_enabled(config: dict[str, Any]) -> bool:
    raw = os.getenv("AFISHAENGAGEMENT_DEBUG_SHADOW_ENABLED")
    if raw is not None:
        return _parse_bool(raw)
    return _parse_bool(config.get("debug_shadow"), default=False)


def _debug_marker(config: dict[str, Any], now_utc: datetime) -> tuple[str, str]:
    marker = str(config.get("debug_marker") or os.getenv("AFISHAENGAGEMENT_DEBUG_MARKER") or DEFAULT_DEBUG_MARKER)
    build_tag = f"{DEFAULT_BUILD_TAG_PREFIX}{now_utc.strftime('%Y%m%d')}"
    return marker, build_tag


def _scheduled_shadow_ts(config: dict[str, Any], now_utc: datetime) -> int:
    days_raw = config.get("debug_publish_delay_days", os.getenv("AFISHAENGAGEMENT_DEBUG_PUBLISH_DELAY_DAYS", "3"))
    try:
        days = max(1, int(days_raw))
    except Exception:
        days = 3
    scheduled = now_utc + timedelta(days=days)
    scheduled = scheduled.replace(second=0, microsecond=0)
    minute = scheduled.minute
    if minute % 5:
        scheduled += timedelta(minutes=(5 - minute % 5))
    return int(scheduled.timestamp())


def _debug_slot_spacing_seconds(config: dict[str, Any]) -> int:
    raw = config.get("debug_slot_spacing_minutes", os.getenv("AFISHAENGAGEMENT_DEBUG_SLOT_SPACING_MINUTES", "5"))
    try:
        minutes = int(raw)
    except Exception:
        minutes = 5
    return max(1, minutes) * 60


def _debug_slot_search_limit(config: dict[str, Any]) -> int:
    raw = config.get("debug_slot_search_limit", os.getenv("AFISHAENGAGEMENT_DEBUG_SLOT_SEARCH_LIMIT", "96"))
    try:
        limit = int(raw)
    except Exception:
        limit = 96
    return max(1, min(limit, 288))


def _timestamp_from_any(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    try:
        return int(float(value))
    except Exception:
        pass
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except Exception:
            return None
    return None


async def _vk_postponed_dates(
    *,
    owner_id: int,
    vk_api_fn: Callable[..., Awaitable[dict[str, Any]]],
    db: Database | None,
    bot: Any,
) -> set[int]:
    response = await vk_api_fn(
        "wall.get",
        {"owner_id": owner_id, "filter": "postponed", "count": 100},
        db,
        bot,
    )
    data = response.get("response", response) if isinstance(response, dict) else {}
    items = data.get("items", []) if isinstance(data, dict) else []
    dates: set[int] = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        ts = _timestamp_from_any(item.get("date"))
        if ts is not None:
            dates.add(ts)
    return dates


async def _db_shadow_scheduled_dates(
    db: Database | None,
    *,
    since_ts: int,
) -> set[int]:
    if db is None:
        return set()
    since = datetime.fromtimestamp(max(0, since_ts - 3600), tz=timezone.utc)
    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(PromoExposure)
                    .where(PromoExposure.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT)
                    .where(PromoExposure.publish_status == "VK_SCHEDULED_DEBUG")
                    .where(PromoExposure.created_at >= since - timedelta(days=7))
                )
            )
            .scalars()
            .all()
        )
    dates: set[int] = set()
    for row in rows:
        details = dict(row.details_json or {})
        ts = _timestamp_from_any(details.get("scheduled_ts"))
        if ts is None:
            ts = _timestamp_from_any(row.published_at)
        if ts is not None and ts >= since_ts:
            dates.add(ts)
    return dates


async def _next_shadow_schedule(
    *,
    config: dict[str, Any],
    now_utc: datetime,
    owner_id: int,
    vk_api_fn: Callable[..., Awaitable[dict[str, Any]]],
    db: Database | None,
    bot: Any,
) -> tuple[int, dict[str, Any]]:
    base_ts = _scheduled_shadow_ts(config, now_utc)
    spacing = _debug_slot_spacing_seconds(config)
    search_limit = _debug_slot_search_limit(config)
    occupied: set[int] = set()
    meta: dict[str, Any] = {
        "base_ts": base_ts,
        "slot_spacing_seconds": spacing,
        "slot_search_limit": search_limit,
        "vk_postponed_count": 0,
        "db_shadow_scheduled_count": 0,
    }
    try:
        vk_dates = await _vk_postponed_dates(owner_id=owner_id, vk_api_fn=vk_api_fn, db=db, bot=bot)
        occupied.update(vk_dates)
        meta["vk_postponed_count"] = len(vk_dates)
    except Exception as exc:
        logger.warning("afishaengagement: postponed slot lookup failed: %s", exc)
        meta["vk_postponed_lookup_error"] = str(exc)
    try:
        db_dates = await _db_shadow_scheduled_dates(db, since_ts=base_ts)
        occupied.update(db_dates)
        meta["db_shadow_scheduled_count"] = len(db_dates)
    except Exception as exc:
        logger.warning("afishaengagement: shadow exposure slot lookup failed: %s", exc)
        meta["db_shadow_lookup_error"] = str(exc)

    for slot in range(search_limit):
        candidate = base_ts + (slot * spacing)
        if candidate not in occupied:
            meta["selected_slot_index"] = slot
            meta["selected_ts"] = candidate
            return candidate, meta

    candidate = base_ts + (search_limit * spacing)
    meta["selected_slot_index"] = search_limit
    meta["selected_ts"] = candidate
    meta["slot_search_exhausted"] = True
    return candidate, meta


def _looks_like_vk_schedule_collision(exc: Exception) -> bool:
    text = str(exc).lower()
    return "scheduled for this time" in text or "уже заплан" in text or "на это время" in text


def _vk_post_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"wall-?\d+_(\d+)", url)
    return match.group(1) if match else None


async def _shadow_already_exists(
    db: Database | None,
    *,
    event_id: int | None,
    activity_id: int | None,
    marker: str,
    build_tag: str,
) -> bool:
    if db is None or event_id is None:
        return False
    async with db.get_session() as session:
        query = (
            select(PromoExposure)
            .where(PromoExposure.event_id == int(event_id))
            .where(PromoExposure.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT)
            .where(PromoExposure.publish_status == "VK_SCHEDULED_DEBUG")
        )
        if activity_id is not None:
            query = query.where(PromoExposure.activity_id == int(activity_id))
        rows = list((await session.execute(query)).scalars().all())
    for row in rows:
        details = dict(row.details_json or {})
        if details.get("shadow_marker") == marker and details.get("build_tag") == build_tag:
            return True
    return False


async def _debug_cap_reached(
    db: Database | None,
    *,
    activity_id: int | None,
    config: dict[str, Any],
    now_utc: datetime,
) -> bool:
    if db is None:
        return False
    raw = config.get("debug_cap", os.getenv("AFISHAENGAGEMENT_DEBUG_CAP", "20"))
    try:
        cap = int(raw)
    except Exception:
        cap = 20
    if cap <= 0:
        return True
    since = now_utc - timedelta(hours=24)
    async with db.get_session() as session:
        query = (
            select(PromoExposure)
            .where(PromoExposure.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT)
            .where(PromoExposure.publish_status == "VK_SCHEDULED_DEBUG")
            .where(PromoExposure.created_at >= since)
        )
        if activity_id is not None:
            query = query.where(PromoExposure.activity_id == int(activity_id))
        count = len(list((await session.execute(query)).scalars().all()))
    return count >= cap


async def record_shadow_exposure(
    db: Database | None,
    *,
    candidate: EngagementCandidate,
    event: Event,
    vk_url: str,
    scheduled_ts: int,
    marker: str,
    build_tag: str,
    media_digest: str,
    plan: EngagementPlan,
    rendered: RenderedImage,
    dice: DiceDecision,
    vision: PosterVisionSummary,
) -> None:
    if db is None or event.id is None or candidate.campaign.id is None:
        return
    async with db.get_session() as session:
        exposure = PromoExposure(
            campaign_id=int(candidate.campaign.id),
            activity_id=int(candidate.activity.id) if candidate.activity.id is not None else None,
            event_id=int(event.id),
            surface=PROMO_SURFACE_AFISHA_ENGAGEMENT,
            placement_kind="vk_shadow_debug",
            publish_status="VK_SCHEDULED_DEBUG",
            public_target_count=1,
            public_targets_json=[{"type": "vk_wall_debug", "url": vk_url}],
            published_at=datetime.fromtimestamp(scheduled_ts, tz=timezone.utc),
            details_json={
                "target_url": vk_url,
                "vk_post_id": _vk_post_id_from_url(vk_url),
                "scheduled_ts": scheduled_ts,
                "shadow_marker": marker,
                "build_tag": build_tag,
                "media_hash": media_digest,
                "apply_rate": dice.apply_rate,
                "dice_value": dice.value,
                "seed": dice.seed,
                "mechanic": plan.mechanic,
                "template_id": plan.template_id,
                "palette_id": plan.palette_id,
                "cta_text": plan.cta_text,
                "cta_text_lines": rendered.cta_text_lines,
                "cta_text_font_px": rendered.cta_text_font_px,
                "dimensions": list(rendered.dimensions),
                "vision_provider": vision.provider,
                "vision_confidence": vision.confidence,
                "vision_reason": vision.reason,
            },
        )
        session.add(exposure)
        await session.commit()


async def cleanup_debug_posts(
    *,
    group_id: str,
    marker: str = DEFAULT_DEBUG_MARKER,
    vk_api_fn: Callable[..., Awaitable[dict[str, Any]]],
    db: Database | None = None,
    bot: Any = None,
    dry_run: bool = False,
) -> dict[str, int]:
    owner_id = -int(str(group_id).lstrip("-"))
    deleted = 0
    matched = 0
    try:
        response = await vk_api_fn(
            "wall.get",
            {"owner_id": owner_id, "filter": "postponed", "count": 100},
            db,
            bot,
        )
        data = response.get("response", response) if isinstance(response, dict) else {}
        items = data.get("items", []) if isinstance(data, dict) else []
        for item in items or []:
            text = str(item.get("text") or "")
            post_id = item.get("id")
            if not post_id or marker not in text:
                continue
            matched += 1
            if dry_run:
                continue
            await vk_api_fn(
                "wall.delete",
                {"owner_id": owner_id, "post_id": post_id},
                db,
                bot,
            )
            deleted += 1
    except Exception as exc:
        _json_log(
            StageLog(
                stage="cleanup",
                decision="error",
                reason="cleanup_error",
                vk_owner_id=owner_id,
                shadow_marker=marker,
                extra={"error": str(exc)},
            )
        )
        return {"matched": matched, "deleted": deleted, "errors": 1}
    _json_log(
        StageLog(
            stage="cleanup",
            decision="apply",
            reason="cleanup_complete",
            vk_owner_id=owner_id,
            shadow_marker=marker,
            extra={"matched": matched, "deleted": deleted, "dry_run": dry_run},
        )
    )
    return {"matched": matched, "deleted": deleted, "errors": 0}


async def maybe_cleanup_once(
    *,
    group_id: str,
    marker: str,
    config: dict[str, Any],
    vk_api_fn: Callable[..., Awaitable[dict[str, Any]]],
    db: Database | None,
    bot: Any,
) -> None:
    cleanup_enabled = _parse_bool(
        config.get(
            "debug_cleanup_before",
            os.getenv("AFISHAENGAGEMENT_DEBUG_CLEANUP_BEFORE", "1"),
        ),
        default=True,
    )
    if not cleanup_enabled:
        return
    key = (str(group_id).lstrip("-"), marker)
    if key in _CLEANUP_DONE:
        return
    _CLEANUP_DONE.add(key)
    await cleanup_debug_posts(group_id=group_id, marker=marker, vk_api_fn=vk_api_fn, db=db, bot=bot)


async def maybe_publish_shadow_debug_copy(
    *,
    event: Event,
    db: Database | None,
    bot: Any,
    target_group_id: str,
    message: str,
    photo_urls: Sequence[str],
    post_to_vk_fn: Callable[..., Awaitable[str | None]],
    upload_vk_photo_fn: Callable[..., Awaitable[str | None]],
    upload_images_fn: Callable[..., Awaitable[tuple[list[str], str]]],
    vk_api_fn: Callable[..., Awaitable[dict[str, Any]]],
    fetch_image_fn: Callable[[str], Awaitable[bytes]] | None = None,
    now_utc: datetime | None = None,
) -> str | None:
    started = time.monotonic()
    now_utc = now_utc or datetime.now(timezone.utc)
    event_id = int(event.id) if event.id is not None else None
    owner_id = -int(str(target_group_id).lstrip("-"))
    group_short = os.getenv("AFISHAENGAGEMENT_TARGET_GROUP_SHORT", DEFAULT_TARGET_GROUP_SHORT)
    if not photo_urls:
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="no_illustration",
                event_id=event_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
            )
        )
        return None
    candidate = await resolve_candidate(db, event, target_group_id=target_group_id, now_utc=now_utc)
    if candidate is None:
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="no_active_activity",
                event_id=event_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
            )
        )
        return None
    campaign_id = int(candidate.campaign.id) if candidate.campaign.id is not None else None
    activity_id = int(candidate.activity.id) if candidate.activity.id is not None else None
    config = candidate.config
    marker, build_tag = _debug_marker(config, now_utc)
    shadow_mode = _env_debug_enabled(config)
    if not shadow_mode:
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="shadow_disabled",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                shadow_mode=False,
            )
        )
        return None
    await maybe_cleanup_once(
        group_id=target_group_id,
        marker=marker,
        config=config,
        vk_api_fn=vk_api_fn,
        db=db,
        bot=bot,
    )
    if await _shadow_already_exists(db, event_id=event_id, activity_id=activity_id, marker=marker, build_tag=build_tag):
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="shadow_duplicate",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                shadow_mode=True,
                shadow_marker=marker,
            )
        )
        return None
    if await _debug_cap_reached(db, activity_id=activity_id, config=config, now_utc=now_utc):
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="debug_cap_reached",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                shadow_mode=True,
                shadow_marker=marker,
            )
        )
        return None
    digest = media_hash(photo_urls)
    dice = should_apply_rate(
        event_id=event_id,
        campaign_id=campaign_id,
        activity_id=activity_id,
        apply_rate=_apply_rate_from_config(config),
        salt=str(config.get("apply_salt") or ""),
        media_digest=digest,
    )
    if not dice.applies:
        _json_log(
            StageLog(
                stage="dice",
                decision="skip",
                reason="dice_miss",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                seed=dice.seed,
                apply_rate=dice.apply_rate,
                dice_value=dice.value,
                shadow_mode=True,
                shadow_marker=marker,
            )
        )
        return None
    fetch_image_fn = fetch_image_fn or _default_fetch_image
    try:
        source_image = await fetch_image_fn(photo_urls[0])
    except Exception as exc:
        _json_log(
            StageLog(
                stage="eligibility",
                decision="skip",
                reason="source_image_download_failed",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                extra={"error": str(exc), "media_hash": digest},
            )
        )
        return None
    vision_enabled = _parse_bool(config.get("vision_enabled", os.getenv("AFISHAENGAGEMENT_VISION_ENABLED", "1")), True)
    vision_started = time.monotonic()
    vision = await analyze_poster_vision(db, source_image, enabled=vision_enabled)
    vision_ms = int((time.monotonic() - vision_started) * 1000)
    llm_enabled = _parse_bool(
        config.get("llm_plan_enabled", os.getenv("AFISHAENGAGEMENT_LLM_PLAN_ENABLED", "0")),
        default=False,
    )
    if llm_enabled:
        plan, llm_ms, plan_provider = await build_llm_engagement_plan(
            event,
            seed=dice.seed,
            config=config,
            vision=vision,
        )
    else:
        plan = build_engagement_plan(event, seed=dice.seed, config=config, vision=vision)
        llm_ms = 0
        plan_provider = "deterministic_fallback"
    if "{" in plan.cta_text or len(plan.cta_text) > 95:
        _json_log(
            StageLog(
                stage="text_resolve",
                decision="skip",
                reason="unresolved_or_too_long_text",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                event_type=plan.event_type,
                has_persona=plan.has_persona,
                has_festival=plan.has_festival,
                seed=dice.seed,
                apply_rate=dice.apply_rate,
                dice_value=dice.value,
                mechanic=plan.mechanic,
                template_id=plan.template_id,
                palette_id=plan.palette_id,
                cta_text_final=plan.cta_text,
                cta_text_len=len(plan.cta_text),
                llm_ms_text=llm_ms,
                shadow_mode=True,
                shadow_marker=marker,
                extra={"plan_provider": plan_provider},
            )
        )
        return None
    try:
        rendered_images = await asyncio.to_thread(render_plan_images, source_image, plan)
        rendered = rendered_images[-1]
    except FileNotFoundError as exc:
        _json_log(
            StageLog(
                stage="render",
                decision="skip",
                reason="font_missing",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                extra={"error": str(exc)},
            )
        )
        return None
    except Exception as exc:
        _json_log(
            StageLog(
                stage="render",
                decision="skip",
                reason="render_exception",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                extra={"error": str(exc)},
            )
        )
        return None
    try:
        schedule_meta: dict[str, Any] = {}
        generated_urls, _ = await upload_images_fn(
            [(image.data, image.filename) for image in rendered_images],
            limit=len(rendered_images),
            force=True,
            event_hint=f"afishaengagement:{event_id or 'unknown'}",
        )
        if not generated_urls:
            raise RuntimeError("upload_images_returned_empty")
        generated_attachments = []
        for generated_url in generated_urls:
            attachment = await upload_vk_photo_fn(target_group_id, generated_url, db, bot)
            if not attachment:
                raise RuntimeError("upload_vk_photo_returned_empty")
            generated_attachments.append(attachment)
        debug_message = (
            f"{message.rstrip()}\n\n"
            f"[AFISHAENGAGEMENT DEBUG COPY — DELETE BEFORE PUBLISH]\n"
            f"{marker} {build_tag}"
        )
        scheduled_ts, schedule_meta = await _next_shadow_schedule(
            config=config,
            now_utc=now_utc,
            owner_id=owner_id,
            vk_api_fn=vk_api_fn,
            db=db,
            bot=bot,
        )
        vk_url = None
        publish_attempts: list[int] = []
        spacing = int(schedule_meta.get("slot_spacing_seconds") or _debug_slot_spacing_seconds(config))
        max_publish_attempts = min(6, 1 + _debug_slot_search_limit(config))
        last_publish_exc: Exception | None = None
        for attempt in range(max_publish_attempts):
            publish_attempts.append(scheduled_ts)
            try:
                vk_url = await post_to_vk_fn(
                    target_group_id,
                    debug_message,
                    db,
                    bot,
                    generated_attachments,
                    carousel=True,
                    publish_date=scheduled_ts,
                )
                last_publish_exc = None
                break
            except Exception as exc:
                last_publish_exc = exc
                if not _looks_like_vk_schedule_collision(exc):
                    raise
                if attempt >= max_publish_attempts - 1:
                    break
                scheduled_ts += spacing
                logger.info(
                    "afishaengagement: debug shadow slot collision; retrying event_id=%s next_ts=%s",
                    event_id,
                    scheduled_ts,
                )
        schedule_meta["publish_attempts"] = publish_attempts
        schedule_meta["publish_attempt_count"] = len(publish_attempts)
        if last_publish_exc is not None:
            raise last_publish_exc
        if not vk_url:
            raise RuntimeError("post_to_vk_returned_empty")
        await record_shadow_exposure(
            db,
            candidate=candidate,
            event=event,
            vk_url=vk_url,
            scheduled_ts=scheduled_ts,
            marker=marker,
            build_tag=build_tag,
            media_digest=digest,
            plan=plan,
            rendered=rendered,
            dice=dice,
            vision=vision,
        )
        _json_log(
            StageLog(
                stage="vk_schedule",
                decision="apply",
                reason="selected",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                event_type=plan.event_type,
                has_persona=plan.has_persona,
                has_festival=plan.has_festival,
                seed=dice.seed,
                apply_rate=dice.apply_rate,
                dice_value=dice.value,
                mechanic=plan.mechanic,
                template_id=plan.template_id,
                palette_id=plan.palette_id,
                cta_text_final=plan.cta_text,
                cta_text_len=len(plan.cta_text),
                cta_text_lines=len(rendered.cta_text_lines),
                cta_text_font_px=rendered.cta_text_font_px,
                vision_conf_right_third=vision.confidence,
                vision_conf_poster_text_zones=[vision.confidence],
                render_ms=rendered.render_ms,
                llm_ms_text=llm_ms,
                llm_ms_vision=vision_ms,
                total_ms=int((time.monotonic() - started) * 1000),
                shadow_mode=True,
                shadow_marker=marker,
                shadow_scheduled_for_ts=datetime.fromtimestamp(scheduled_ts, timezone.utc).isoformat(),
                vk_post_id=_vk_post_id_from_url(vk_url),
                vk_post_url=vk_url,
                extra={
                    "media_hash": digest,
                    "build_tag": build_tag,
                    "uploaded_url_count": len(generated_urls),
                    "generated_attachment_count": len(generated_attachments),
                    "rendered_template_ids": [image.template_id for image in rendered_images],
                    "rendered_dimensions": [list(image.dimensions) for image in rendered_images],
                    "vision_provider": vision.provider,
                    "vision_reason": vision.reason,
                    "plan_provider": plan_provider,
                    "schedule": schedule_meta,
                },
            )
        )
        return vk_url
    except Exception as exc:
        _json_log(
            StageLog(
                stage="error",
                decision="error",
                reason="vk_api_error",
                event_id=event_id,
                campaign_id=campaign_id,
                activity_id=activity_id,
                vk_owner_id=owner_id,
                vk_group_short=group_short,
                seed=dice.seed,
                apply_rate=dice.apply_rate,
                dice_value=dice.value,
                mechanic=plan.mechanic,
                template_id=plan.template_id,
                palette_id=plan.palette_id,
                cta_text_final=plan.cta_text,
                llm_ms_text=llm_ms,
                shadow_mode=True,
                shadow_marker=marker,
                total_ms=int((time.monotonic() - started) * 1000),
                extra={
                    "error": str(exc),
                    "media_hash": digest,
                    "plan_provider": plan_provider,
                    "schedule": schedule_meta if "schedule_meta" in locals() else {},
                },
            )
        )
        return None
