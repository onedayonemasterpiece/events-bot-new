from __future__ import annotations

import asyncio
import colorsys
import hashlib
import io
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field, replace
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
MAX_VK_FEED_PHOTO_ASPECT = 1.45
CTA_LAYER_SHADOW_ALPHA = 170

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


@dataclass(frozen=True)
class PaletteScore:
    palette_id: str
    score: float
    text_contrast: float
    signal_contrast: float
    poster_contrast: float
    luma_sep: float
    hue_sep: float
    family_fit: float
    tone_fit: float
    failsafe_level: str = "none"


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

CTA_EDITORIAL_PALETTES: dict[str, dict[str, str]] = {
    "ivory_charcoal_oxblood": {
        "surface": "#F4ECDB",
        "ink": "#1A1A1A",
        "signal": "#8B1A1A",
        "signal_ink": "#F4ECDB",
        "seam": "#8B1A1A",
        "rim": "#1A1A1A",
        "family": "ivory",
        "tone": "editorial",
    },
    "ivory_navy_ochre": {
        "surface": "#F2EAD3",
        "ink": "#0F1E3D",
        "signal": "#C77D2A",
        "signal_ink": "#0F1E3D",
        "seam": "#0F1E3D",
        "rim": "#C77D2A",
        "family": "ivory",
        "tone": "lecture",
    },
    "ivory_forest_brass": {
        "surface": "#EFE7D2",
        "ink": "#1B3A2C",
        "signal": "#C9A24A",
        "signal_ink": "#1B3A2C",
        "seam": "#1B3A2C",
        "rim": "#C9A24A",
        "family": "ivory",
        "tone": "editorial",
    },
    "ink_amber": {
        "surface": "#0F0F12",
        "ink": "#F4ECDB",
        "signal": "#FFB000",
        "signal_ink": "#0F0F12",
        "seam": "#FFB000",
        "rim": "#F4ECDB",
        "family": "warm_dark",
        "tone": "noir",
    },
    "oxblood_cream": {
        "surface": "#3B0A14",
        "ink": "#FFF1D8",
        "signal": "#E8B864",
        "signal_ink": "#3B0A14",
        "seam": "#E8B864",
        "rim": "#FFF1D8",
        "family": "warm_dark",
        "tone": "editorial",
    },
    "prussian_lime": {
        "surface": "#0B1E33",
        "ink": "#F0F5DC",
        "signal": "#C8FF3B",
        "signal_ink": "#0B1E33",
        "seam": "#C8FF3B",
        "rim": "#F0F5DC",
        "family": "cool_dark",
        "tone": "festival",
    },
    "petrol_pearl": {
        "surface": "#06303A",
        "ink": "#E7F4F2",
        "signal": "#3DD6D0",
        "signal_ink": "#06303A",
        "seam": "#3DD6D0",
        "rim": "#E7F4F2",
        "family": "cool_dark",
        "tone": "concert",
    },
    "violet_lemon": {
        "surface": "#3A1E5C",
        "ink": "#FFF7C2",
        "signal": "#F6D743",
        "signal_ink": "#3A1E5C",
        "seam": "#F6D743",
        "rim": "#FFF7C2",
        "family": "saturated_pop",
        "tone": "festival",
    },
    "magenta_mint": {
        "surface": "#4A0F3F",
        "ink": "#E8FFF1",
        "signal": "#6CE3B2",
        "signal_ink": "#4A0F3F",
        "seam": "#6CE3B2",
        "rim": "#E8FFF1",
        "family": "saturated_pop",
        "tone": "festival",
    },
    "graphite_signal_red": {
        "surface": "#1A1C20",
        "ink": "#F2EFE8",
        "signal": "#E14B3A",
        "signal_ink": "#F2EFE8",
        "seam": "#E14B3A",
        "rim": "#F2EFE8",
        "family": "graphite",
        "tone": "editorial",
    },
    "graphite_signal_chartreuse": {
        "surface": "#1A1C20",
        "ink": "#F2EFE8",
        "signal": "#C8FF3B",
        "signal_ink": "#1A1C20",
        "seam": "#C8FF3B",
        "rim": "#F2EFE8",
        "family": "graphite",
        "tone": "concert",
    },
    "cloud_plum_wasabi": {
        "surface": "#F7F5EF",
        "ink": "#25152E",
        "signal": "#A8C83A",
        "signal_ink": "#25152E",
        "seam": "#4B164C",
        "rim": "#A8C83A",
        "family": "ivory",
        "tone": "festival",
    },
    "cobalt_clay_ivory": {
        "surface": "#F5EFE2",
        "ink": "#142A5E",
        "signal": "#C65F3D",
        "signal_ink": "#F5EFE2",
        "seam": "#142A5E",
        "rim": "#C65F3D",
        "family": "ivory",
        "tone": "editorial",
    },
    "clay_cobalt_noir": {
        "surface": "#142A5E",
        "ink": "#F7F4EA",
        "signal": "#D07A55",
        "signal_ink": "#142A5E",
        "seam": "#D07A55",
        "rim": "#F7F4EA",
        "family": "cool_dark",
        "tone": "editorial",
    },
    "botanical_citron": {
        "surface": "#163D2B",
        "ink": "#F4F1E5",
        "signal": "#D6F04A",
        "signal_ink": "#163D2B",
        "seam": "#D6F04A",
        "rim": "#F4F1E5",
        "family": "cool_dark",
        "tone": "festival",
    },
    "plum_noir_cloud": {
        "surface": "#221326",
        "ink": "#F6F2E8",
        "signal": "#B7D34B",
        "signal_ink": "#221326",
        "seam": "#B7D34B",
        "rim": "#F6F2E8",
        "family": "warm_dark",
        "tone": "noir",
    },
    "smoky_jade_terracotta": {
        "surface": "#DDE8DE",
        "ink": "#18201D",
        "signal": "#A94F35",
        "signal_ink": "#F7F2EA",
        "seam": "#A94F35",
        "rim": "#18201D",
        "family": "ivory",
        "tone": "lecture",
    },
    "future_dusk_lime": {
        "surface": "#15142F",
        "ink": "#F5F1E8",
        "signal": "#B7FF2A",
        "signal_ink": "#15142F",
        "seam": "#B7FF2A",
        "rim": "#F5F1E8",
        "family": "cool_dark",
        "tone": "festival",
    },
    "transform_teal_persimmon": {
        "surface": "#063B3F",
        "ink": "#F1F7EA",
        "signal": "#FF6B4A",
        "signal_ink": "#101A1A",
        "seam": "#FF6B4A",
        "rim": "#F1F7EA",
        "family": "cool_dark",
        "tone": "editorial",
    },
    "mocha_aqua_ivory": {
        "surface": "#4A332C",
        "ink": "#F8EAD2",
        "signal": "#45D6D0",
        "signal_ink": "#1B1410",
        "seam": "#45D6D0",
        "rim": "#F8EAD2",
        "family": "warm_dark",
        "tone": "lecture",
    },
    "espresso_sky_cherry": {
        "surface": "#2A1B17",
        "ink": "#F4E7D3",
        "signal": "#64C7F2",
        "signal_ink": "#111214",
        "seam": "#C0182D",
        "rim": "#64C7F2",
        "family": "warm_dark",
        "tone": "editorial",
    },
    "butter_ink_cherry": {
        "surface": "#F4D35E",
        "ink": "#17121F",
        "signal": "#B9122A",
        "signal_ink": "#FFF7E3",
        "seam": "#B9122A",
        "rim": "#17121F",
        "family": "saturated_pop",
        "tone": "festival",
    },
    "cool_blue_jade_plum": {
        "surface": "#EAF1F5",
        "ink": "#24132E",
        "signal": "#16866F",
        "signal_ink": "#F7F4EA",
        "seam": "#24132E",
        "rim": "#16866F",
        "family": "ivory",
        "tone": "family",
    },
    "thermal_cobalt_tomato": {
        "surface": "#1137A6",
        "ink": "#F7F2E8",
        "signal": "#FF553E",
        "signal_ink": "#111214",
        "seam": "#FF553E",
        "rim": "#F7F2E8",
        "family": "cool_dark",
        "tone": "concert",
    },
    "ink_fuchsia_mint": {
        "surface": "#101018",
        "ink": "#F6F1E7",
        "signal": "#E949A8",
        "signal_ink": "#F6F1E7",
        "seam": "#76E7B2",
        "rim": "#E949A8",
        "family": "graphite",
        "tone": "party",
    },
    "sage_black_lilac": {
        "surface": "#DDE8D5",
        "ink": "#141414",
        "signal": "#7657D8",
        "signal_ink": "#F7F2EA",
        "seam": "#7657D8",
        "rim": "#141414",
        "family": "ivory",
        "tone": "lecture",
    },
    "oxide_blue_citron": {
        "surface": "#B94F36",
        "ink": "#FFF4E2",
        "signal": "#C9FF3B",
        "signal_ink": "#172028",
        "seam": "#172028",
        "rim": "#C9FF3B",
        "family": "warm_dark",
        "tone": "festival",
    },
}

for _palette_id, _palette in CTA_EDITORIAL_PALETTES.items():
    PALETTES[_palette_id] = {
        **_palette,
        "background": _palette["surface"],
        "text": _palette["ink"],
        "accent": _palette["signal"],
        "accent_text": _palette["signal_ink"],
    }

MECHANIC_WEIGHTS = {"comments": 40, "likes": 40, "reposts": 20}
MECHANIC_BADGES = {
    "comments": "НАПИШИ КОММЕНТАРИЙ",
    "likes": "ЛАЙК",
    "reposts": "РЕПОСТ",
}
MECHANIC_BADGE_ICONS = {
    "comments": "down_arrow",
    "likes": "heart",
    "reposts": "right_arrow",
}
MECHANIC_EYEBROWS = {
    "comments": "К ВАМ ВОПРОС",
    "likes": "ОТМЕТЬТЕ ЛАЙКОМ",
    "reposts": "ПОДЕЛИТЕСЬ",
}
SUPPORTED_TEMPLATE_IDS = {"right_extension", "bottom_overlay", "bottom_extension", "hook_swipe_cta"}
HOLIDAY_FESTIVAL_NAMES = {
    "день россии",
    "дню россии",
    "дня россии",
    "день победы",
    "дню победы",
    "дня победы",
}


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


async def resolve_candidates(
    db: Database | None,
    event: Event,
    *,
    target_group_id: str,
    now_utc: datetime | None = None,
) -> list[EngagementCandidate]:
    if db is None:
        return []
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
    candidates: list[EngagementCandidate] = []
    for campaign, activity, target in rows:
        config = dict(activity.config_json or {})
        if not _group_matches(config, target_group_id):
            continue
        if not _config_matches_event(event, config):
            continue
        if _target_matches(event, target):
            candidates.append(EngagementCandidate(campaign=campaign, activity=activity, target=target, config=config))
    return candidates


async def resolve_candidate(
    db: Database | None,
    event: Event,
    *,
    target_group_id: str,
    now_utc: datetime | None = None,
) -> EngagementCandidate | None:
    candidates = await resolve_candidates(db, event, target_group_id=target_group_id, now_utc=now_utc)
    return candidates[0] if candidates else None


def _explicit_event_type_key(value: str | None) -> str | None:
    explicit_type = str(value or "").strip().casefold()
    if not explicit_type:
        return None
    if any(word in explicit_type for word in ("экскурс", "прогулк")):
        return "excursion"
    if any(word in explicit_type for word in ("вечерин", "party", "пати", "тусов")):
        return "party"
    if any(word in explicit_type for word in ("выстав", "экспозиц")):
        return "exhibition"
    if any(word in explicit_type for word in ("волонт", "добровол")):
        return "volunteer"
    if any(word in explicit_type for word in ("праздник", "праздничн")):
        return "holiday"
    if any(word in explicit_type for word in ("маркет", "ярмарк")):
        return "market"
    if any(word in explicit_type for word in ("кинопоказ", "киносеанс", "кинолекторий", "кино")):
        return "cinema"
    if "фестив" in explicit_type:
        return "festival"
    if any(word in explicit_type for word in ("концерт", "музык")):
        return "concert"
    if any(word in explicit_type for word in ("мастер-класс", "мастер класс", "воркшоп")):
        return "workshop"
    if any(word in explicit_type for word in ("спектак", "театр", "постановк", "пьес")):
        return "theatre"
    if any(word in explicit_type for word in ("лекц", "семинар", "лектор", "спикер", "дискусс")):
        return "lecture"
    if any(word in explicit_type for word in ("дет", "семейн")):
        return "family"
    if any(word in explicit_type for word in ("встреч", "клуб", "игр", "квиз")):
        return "other"
    return None


def _looks_family_event(raw: str) -> bool:
    return any(
        word in raw
        for word in (
            "дет",
            "семейн",
            "малыш",
            "ребят",
            "сказоч",
            "сказк",
            "геро",
            "праздник",
            "аниматор",
        )
    )


def _looks_family_audience_event(raw: str) -> bool:
    return any(
        word in raw
        for word in (
            "дет",
            "семейн",
            "малыш",
            "ребят",
            "ребен",
            "ребён",
            "сказоч",
            "сказк",
            "геро",
            "аниматор",
        )
    )


def _looks_zoo_event(raw: str) -> bool:
    if not raw:
        return False
    return any(
        word in raw
        for word in (
            "зоопарк",
            "зоолог",
            "ветеринар",
            "вольер",
            "кормокух",
            "животн",
        )
    )


def _looks_excursion_event(raw: str) -> bool:
    if not raw:
        return False
    if "экскурс" in raw:
        return True
    return "зоопарк" in raw and _looks_zoo_event(raw) and any(
        word in raw
        for word in (
            "зоолог",
            "ветеринар",
            "вольер",
            "кормокух",
            "животн",
            "закулис",
            "уход",
        )
    )


def _looks_meeting_event(raw: str) -> bool:
    return any(word in raw for word in ("творческ", "встреч", "диалог", "разговор", "обсужден"))


def _looks_theatre_event(raw: str) -> bool:
    return any(word in raw for word in ("спектак", "театр", "постановк", "пьес"))


def _looks_cinema_event(raw: str) -> bool:
    return any(word in raw for word in ("кинопоказ", "киносеанс", "кинолекторий", "кинопросмотр", "фильм"))


def _looks_recycling_event(raw: str) -> bool:
    return any(
        word in raw
        for word in (
            "прием шин",
            "приём шин",
            "отработанных шин",
            "утилизац",
            "переработк",
            "раздельн",
            "экосбор",
        )
    )


def _looks_holiday_program(event: Event, raw: str, explicit_key: str | None) -> bool:
    holiday_markers = (
        "день россии",
        "дня россии",
        "дню россии",
        "день победы",
        "дня победы",
        "дню победы",
    )
    if _is_holiday_name(getattr(event, "festival", None)):
        return True
    title = str(event.title or "").casefold()
    if any(marker in title for marker in holiday_markers) and (
        explicit_key in {"festival", "holiday"} or re.match(r"\s*(?:празднован|день\s+(?:россии|победы))", title)
    ):
        return True
    if explicit_key == "festival" and any(marker in raw for marker in holiday_markers):
        return True
    return False


def _event_type_key(event: Event) -> str:
    explicit_type = str(event.event_type or "").casefold()
    explicit_key = _explicit_event_type_key(explicit_type)
    semantic_raw = " ".join(
        [
            str(event.title or ""),
            str(event.description or ""),
            str(event.source_text or ""),
            str(event.search_digest or ""),
            str(event.location_name or ""),
        ]
    ).casefold()
    raw = " ".join(
        [
            explicit_type,
            semantic_raw,
        ]
    ).casefold()
    if _looks_excursion_event(raw):
        return "excursion"
    if _looks_holiday_program(event, raw, explicit_key):
        return "holiday"
    title_raw = str(event.title or "").casefold()
    location_raw = str(event.location_name or "").casefold()
    if explicit_key == "cinema" and _looks_theatre_event(semantic_raw) and (
        not _looks_cinema_event(semantic_raw)
        or re.match(r"\s*спектак", title_raw)
        or "театр" in location_raw
    ):
        return "theatre"
    if explicit_key == "market" and _looks_recycling_event(semantic_raw):
        return "other"
    if explicit_key == "market" and _looks_family_audience_event(semantic_raw):
        return "family"
    if explicit_key and explicit_key != "other":
        return explicit_key
    if explicit_key == "other" and _looks_family_event(raw):
        return "family"
    if any(word in raw for word in ("маркет", "ярмарк")):
        return "market"
    if any(word in raw for word in ("волонт", "добровол", "добрыми новостями", "доброцентр", "добро.центр")):
        return "volunteer"
    if any(word in raw for word in ("вечерин", "party", "пати", "тусов", "диджей", "dj-сет", "dj сет")):
        return "party"
    if explicit_key == "other" and _looks_meeting_event(raw):
        return "meeting"
    if any(word in raw for word in ("выстав", "экспозиц", "музейн")):
        return "exhibition"
    if any(word in raw for word in ("кинопоказ", "киносеанс", "кинолекторий", "кинопросмотр")):
        return "cinema"
    if any(word in raw for word in ("фильм", "cinema", "movie", "film")) and "сценар" not in raw:
        return "cinema"
    if re.search(r"(^|\s)кино(\s|$|-)", raw) and "сценар" not in raw:
        return "cinema"
    if _looks_family_event(raw):
        return "family"
    if any(word in raw for word in ("лекц", "лектор", "спикер", "дискусс", "семинар", "паблик-ток")):
        return "lecture"
    if any(word in raw for word in ("концерт", "музык", "оркестр", "джаз", "трек")):
        return "concert"
    if any(word in raw for word in ("мастер-класс", "мастер класс", "воркшоп", "практик")):
        return "workshop"
    if _looks_theatre_event(raw):
        return "theatre"
    if event.festival or any(word in raw for word in ("фестив", "маркет", "ярмарк")):
        return "festival"
    return "other"


def _clean_persona_value(value: str) -> str | None:
    value = re.sub(r"\s+", " ", str(value or "").strip(" .,!?:;—-"))
    value = re.sub(
        r"\s+(?:и|или|на|в|с|от|для|по|про|где|котор[а-яё]+|что|как)\b.*$",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip(" .,!?:;—-")
    if 2 <= len(value) <= 60:
        return value
    return None


def _extract_persona(event: Event) -> str | None:
    text = f"{event.title or ''}\n{event.description or ''}\n{event.search_digest or ''}"
    patterns = [
        r"(?:лекци[яю]|встреч[ау]|концерт|спектакль)[ \t]+(?:с|от)[ \t]+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+(?:[ \t]+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+){0,2})",
        r"(?:спикер|лектор|артист|ведущ[аи]й)[: \t]+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+(?:[ \t]+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+){0,2})",
        r"(?:творчеств[оа]|песн[ияи]|музык[аи])[ \t]+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+(?:[ \t]+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё.'-]+){0,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = _clean_persona_value(match.group(1))
            if not value:
                continue
            before = text[max(0, match.start(1) - 4) : match.start(1)].casefold()
            if re.search(r"\bс\s*$", before) and _looks_instrumental_persona(value):
                continue
            return value
    return None


def _looks_instrumental_persona(value: str) -> bool:
    tokens = [token for token in re.split(r"\s+", str(value or "").strip()) if token]
    if not tokens:
        return False
    endings = ("ом", "ем", "ым", "им", "ой", "ою", "ей", "ею")
    return any(token.casefold().endswith(endings) for token in tokens)


def _topic_label(event: Event, event_type: str) -> str:
    if event_type == "excursion":
        return "экскурсий"
    if event_type == "meeting":
        return "встреч"
    if event_type == "lecture":
        return "лекций"
    if event_type == "concert":
        return "концертов"
    if event_type == "workshop":
        return "мастер-классов"
    if event_type == "theatre":
        return "спектаклей"
    if event_type == "cinema":
        return "кинопоказов"
    if event_type == "festival":
        return "фестивалей"
    if event_type == "market":
        return "ярмарок"
    if event_type == "family":
        return "детских событий"
    if event_type == "exhibition":
        return "выставок"
    if event_type == "party":
        return "вечеринок"
    if event_type == "volunteer":
        return "волонтёрских событий"
    if event_type == "holiday":
        return "праздников"
    return "событий"


def _topic_accusative_plural(event_type: str) -> str:
    if event_type == "excursion":
        return "экскурсии"
    if event_type == "meeting":
        return "встречи"
    if event_type == "lecture":
        return "лекции"
    if event_type == "concert":
        return "концерты"
    if event_type == "workshop":
        return "мастер-классы"
    if event_type == "theatre":
        return "спектакли"
    if event_type == "cinema":
        return "кинопоказы"
    if event_type == "festival":
        return "фестивали"
    if event_type == "market":
        return "ярмарки"
    if event_type == "family":
        return "детские события"
    if event_type == "exhibition":
        return "выставки"
    if event_type == "party":
        return "вечеринки"
    if event_type == "volunteer":
        return "волонтёрские события"
    if event_type == "holiday":
        return "праздники"
    return "события"


def _topic_prepositional_plural(event_type: str) -> str:
    if event_type == "excursion":
        return "экскурсиях"
    if event_type == "meeting":
        return "встречах"
    if event_type == "lecture":
        return "лекциях"
    if event_type == "concert":
        return "концертах"
    if event_type == "workshop":
        return "мастер-классах"
    if event_type == "theatre":
        return "спектаклях"
    if event_type == "cinema":
        return "кинопоказах"
    if event_type == "festival":
        return "фестивалях"
    if event_type == "market":
        return "ярмарках"
    if event_type == "family":
        return "детских событиях"
    if event_type == "exhibition":
        return "выставках"
    if event_type == "party":
        return "вечеринках"
    if event_type == "volunteer":
        return "волонтёрских событиях"
    if event_type == "holiday":
        return "праздниках"
    return "событиях"


def _this_event_accusative(event_type: str) -> str:
    if event_type == "excursion":
        return "эту экскурсию"
    if event_type == "meeting":
        return "эту встречу"
    if event_type == "lecture":
        return "эту лекцию"
    if event_type == "concert":
        return "этот концерт"
    if event_type == "workshop":
        return "этот мастер-класс"
    if event_type == "theatre":
        return "этот спектакль"
    if event_type == "cinema":
        return "этот кинопоказ"
    if event_type == "festival":
        return "это событие фестиваля"
    if event_type == "market":
        return "эту ярмарку"
    if event_type == "family":
        return "это семейное событие"
    if event_type == "exhibition":
        return "эту выставку"
    if event_type == "party":
        return "эту вечеринку"
    if event_type == "volunteer":
        return "это волонтёрское событие"
    if event_type == "holiday":
        return "этот праздник"
    return "это событие"


THEME_KEYWORDS: dict[str, dict[str, str]] = {
    "concert": {
        "казач": "казачьи песни",
        "народн": "народную музыку",
        "хор": "хоровую музыку",
        "патриотическ": "патриотические песни",
        "симфоническ": "симфоническую музыку",
        "акустическ": "акустические концерты",
        "акустика": "акустические концерты",
        "джаз": "джаз",
        "рок ": "рок-музыку",
        "инди": "инди-музыку",
        "фолк": "фолк",
        "электрон": "электронную музыку",
        "классическ": "классическую музыку",
        "орган": "органную музыку",
        "фортепиан": "фортепианную музыку",
        "оркестр": "оркестровую музыку",
    },
    "cinema": {
        "документал": "документальное кино",
        "авторск": "авторское кино",
        "короткометр": "короткометражки",
        "анимац": "анимацию",
        "артхаус": "артхаус",
    },
    "lecture": {
        "истори": "лекции по истории",
        "архитектур": "лекции про архитектуру",
        "искусств": "лекции про искусство",
        "наук": "научные лекции",
        "город": "лекции про город",
    },
    "excursion": {
        "зоопарк": "зоопарк изнутри",
        "зоолог": "встречи с зоологами",
        "ветеринар": "ветеринарный уход",
        "животн": "животных",
        "закулис": "закулисье",
    },
    "exhibition": {
        "маринист": "морскую живопись",
        "айвазов": "морскую живопись",
        "истори": "историю",
        "живопис": "живопись",
        "искусств": "искусство",
    },
    "family": {
        "сказоч": "сказочные события",
        "сказк": "сказочные события",
        "геро": "сказочных героев",
        "дет": "детские события",
        "семейн": "семейные события",
        "праздник": "детские праздники",
    },
}


def _theme_key_matches(raw: str, key: str) -> bool:
    if key == "народн":
        return bool(
            re.search(
                r"(?<![а-яё])(?:народн[а-яё]*\s+(?:песн|музык|хор)|фолк)(?![а-яё])",
                raw,
            )
        )
    if key == "орган":
        return bool(
            re.search(
                r"(?<![а-яё])орган(?:н[а-яё]*|а|е|у|ом|ов|ы)?(?![а-яё])",
                raw,
            )
        )
    if key == "хор":
        return bool(re.search(r"(?<![а-яё])хор(?:а|е|ом|ов|ы)?(?![а-яё])", raw))
    return key in raw


def _extract_theme(event: Event, event_type: str) -> str | None:
    raw = " ".join(
        [
            str(event.title or ""),
            str(event.description or ""),
            str(event.source_text or ""),
            str(event.search_digest or ""),
            str(event.location_name or ""),
        ]
    ).casefold()
    zoo_context = _looks_zoo_event(raw)
    for key, label in THEME_KEYWORDS.get(event_type, {}).items():
        if _theme_key_matches(raw, key):
            if event_type == "excursion" and key in {"зоолог", "ветеринар", "животн"} and not zoo_context:
                continue
            return label
    return None


def _event_text_blob(event: Event) -> str:
    return " ".join(
        [
            str(event.title or ""),
            str(event.description or ""),
            str(event.source_text or ""),
            str(event.search_digest or ""),
            str(event.location_name or ""),
        ]
    ).casefold()


def _extract_idea_phrase(event: Event, event_type: str) -> str | None:
    raw = _event_text_blob(event)

    if re.search(r"\bфестивал\w*\s+музык\w*\s+на\s+воде\b", raw):
        return "фестиваля музыки на воде"
    if re.search(r"\b(?:концерт\w*|музык\w*)[^.\n]{0,80}\bна\s+воде\b", raw) and event_type in {
        "concert",
        "festival",
    }:
        return "концерта на воде"
    if re.search(r"\bпри\s+свечах\b", raw) and event_type == "concert":
        return "концерта при свечах"
    if re.search(r"\bпод\s+открытым\s+небом\b|\bopen[- ]air\b|\bопен[ -]?эйр\b", raw):
        if event_type == "concert":
            return "концерта под открытым небом"
        if event_type == "cinema":
            return "кинопоказа под открытым небом"
        return "события под открытым небом"
    if re.search(r"\bарт[- ]завтрак\w*\b", raw):
        return "арт-завтрака"
    if re.search(r"\bмузыкальн\w+\s+путешеств\w*\b", raw):
        return "музыкального путешествия"
    if re.search(r"\bв\s+кирхе\b|\bкирх\w*\b", raw) and event_type == "concert":
        return "концерта в кирхе"
    if re.search(r"\bв\s+замке\b|\bзамк\w*\b", raw) and event_type == "concert":
        return "концерта в замке"
    return None


def _clean_organizer_name(value: str) -> str | None:
    value = re.sub(r"\s+", " ", str(value or "").strip(" .,!?:;—-"))
    value = re.sub(
        r"\s+(?:приглашает|показывает|present|представляет|организует|устраивает|и|на|в|с)\b.*$",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip(" .,!?:;—-")
    if 3 <= len(value) <= 42:
        return value
    return None


def _extract_cinema_club(event: Event) -> str | None:
    text = "\n".join(
        [
            str(event.title or ""),
            str(event.description or ""),
            str(event.source_text or ""),
            str(event.search_digest or ""),
            str(event.location_name or ""),
        ]
    )
    if re.search(r"\bwestside\s+movie\b", text, flags=re.IGNORECASE):
        return "Westside Movie"
    patterns = [
        r"(?:киноклуб[а-яё]*|кино\s*клуб[а-яё]*)\s+[«\"]?([A-ZА-ЯЁ0-9][A-Za-zА-Яа-яЁё0-9&.' -]{2,42})",
        r"(?:клуб[а-яё]*)\s+[«\"]?([A-ZА-ЯЁ0-9][A-Za-zА-Яа-яЁё0-9&.' -]{2,42})\s+(?:приглашает|показывает|представляет)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = _clean_organizer_name(match.group(1))
        if value:
            return value
    return None


def _templates_for(
    event_type: str,
    persona: str | None,
    festival: str | None,
    theme: str | None = None,
    idea: str | None = None,
    cinema_club: str | None = None,
) -> dict[str, list[str]]:
    topic = "{T}"
    topic_acc = "{TA}"
    festival_from = "{FFROM}"
    festival_on = "{FON}"
    festival_context = "{FCONTEXT}"
    templates: dict[str, list[str]] = {
        "comments": [
            "Расскажите в комментариях, что для вас главное в таких {TP}.",
            "Что вам ближе в таких {TP}? Напишите в комментариях.",
        ],
        "likes": [
            f"Лайк, если тебе интересны такие {topic_acc}.",
            f"Поставь лайк, если тебе близки такие {topic_acc}.",
            f"Поставь лайк, если любишь такие {topic_acc}.",
            f"Отметь лайком, если любишь такие {topic_acc}.",
            f"Поставь лайк, если такие {topic_acc} тебе близки.",
        ],
        "reposts": [
            f"Поделись с теми, кому близки такие {topic_acc}.",
            f"Поделись с другом, которому близки такие {topic_acc}.",
            f"Поделись с подругой, которой близки такие {topic_acc}.",
            f"Поделись с другом, который любит такие {topic_acc}.",
            f"Поделись с подругой, которая любит такие {topic_acc}.",
        ],
    }
    if event_type == "lecture" and persona:
        templates["comments"].extend(
            [
                "Были на лекции {N}? Расскажите, что унесли с собой.",
                "Что ещё вы бы спросили у {N}? Напишите в комментариях.",
                "Если уже слушали {N} — поделитесь, что запомнилось.",
            ]
        )
        templates["likes"].append("Лайк, если нравится, как {N} ведёт лекции.")
        templates["likes"].append("Поставь лайк, если уже слушал {N}.")
        templates["reposts"].append("Поделись с другом, который уже слушал {N}.")
        templates["reposts"].append("Поделись с подругой, которая уже слушала {N}.")
    elif event_type == "concert" and persona:
        templates["comments"].extend(
            [
                "Были на концертах {N}? Какой запомнился?",
                "Какой трек {N} ждёте живьём? Делитесь.",
            ]
        )
        templates["likes"].append("Лайк, если нравится творчество {N}.")
        templates["reposts"].append("Поделись с другом, который слушает {N}.")
    elif event_type == "workshop":
        templates["comments"].extend(
            [
                "Что хотели бы освоить на мастер-классе? Напишите в комментариях.",
                "Что больше всего хочется попробовать на мастер-классе? Делитесь.",
                "Уже бывали на мастер-классах в этой технике? Поделитесь.",
            ]
        )
    elif event_type == "theatre":
        templates["comments"].extend(
            [
                "Какой спектакль в Калининграде вы пересмотрели бы?",
                "Что важнее: режиссура или актёрская игра?",
            ]
        )
    elif event_type == "cinema":
        templates["comments"].extend(
            [
                "Какой кинопоказ запомнился больше всего? Делитесь.",
                "Какой фильм пересмотрели бы на большом экране? Напишите.",
            ]
        )
        templates["likes"].extend(
            [
                "Лайк, если любишь кинопоказы.",
                "Поставь лайк, если ходишь в киноклубы.",
            ]
        )
        templates["reposts"].append("Поделись с тем, с кем смотришь фильмы.")
        templates["reposts"].append("Поделись с подругой, с которой обсуждаешь фильмы.")
        if cinema_club:
            templates["comments"] = [
                "Были на показах киноклуба {ORG}? Расскажите.",
                "Что запомнилось на показах киноклуба {ORG}?",
            ] + templates["comments"]
            templates["likes"] = [
                "Лайк, если нравится киноклуб {ORG}.",
            ] + templates["likes"]
    elif event_type == "exhibition":
        templates["comments"] = [
            "Какие выставки вас вдохновляют? Напишите в комментариях.",
            "Что цепляет вас в таких выставках? Поделитесь.",
        ] + templates["comments"]
        templates["likes"] = [
            "Поставь лайк, если любишь выставки.",
            "Отметь лайком, если интересна такая живопись.",
        ] + templates["likes"]
        templates["reposts"] = ["Поделись с тем, кому интересны выставки."] + templates["reposts"]
    elif event_type == "volunteer":
        templates["comments"] = [
            "Вы когда-нибудь занимались волонтёрством? Напишите в комментариях.",
            "Что вам ближе в добровольчестве? Поделитесь.",
            "Любите волонтёрские проекты? Расскажите в комментариях.",
            "Что для вас главное в таких волонтёрских событиях? Напишите в комментариях.",
        ]
        templates["likes"] = [
            "Поставь лайк, если тебе близко волонтёрство.",
            "Лайк, если любишь добровольческие проекты.",
            "Отметь лайком, если тебе интересны волонтёрские события.",
        ]
        templates["reposts"] = [
            "Поделись с тем, кому близко волонтёрство.",
            "Поделись с другом, которому интересны волонтёрские события.",
            "Поделись с подругой, которой близко волонтёрство.",
        ]
    elif event_type == "party":
        templates["comments"] = [
            "С кем обсуждаешь такие тусовки? Напиши в комментариях.",
            "Какой трек хочешь услышать на вечеринке?",
            "Какая музыка делает вечеринку твоей? Напиши в комментариях.",
        ]
        templates["likes"] = [
            "Лайк, если за такие вечеринки.",
            "Поставь лайк, если любишь такие тусовки.",
            "Отметь лайком, если тебе близки такие вечеринки.",
        ]
        templates["reposts"] = [
            "Поделись с тем, кто любит такие тусовки.",
            "Поделись с другом, который любит такие вечеринки.",
            "Поделись с подругой, которая любит такие вечеринки.",
        ]
    elif event_type == "holiday":
        templates["comments"] = [
            "Как любите отмечать такие праздники? Напишите в комментариях.",
            "Что для вас главное в таком празднике? Поделитесь.",
        ] + templates["comments"]
        templates["likes"] = [
            "Поставь лайк, если любишь городские праздники.",
            "Отметь лайком, если тебе близки городские праздники.",
        ] + templates["likes"]
        templates["reposts"] = ["Поделись с тем, кто любит праздничные программы."] + templates["reposts"]
    elif event_type == "excursion":
        templates["comments"] = [
            "Что больше всего хочется узнать на такой экскурсии?",
            "Что вам интереснее увидеть на экскурсии?",
        ] + templates["comments"]
        templates["likes"] = [
            "Отметь лайком, если любишь экскурсии.",
            "Поставь лайк, если любишь такие экскурсии.",
        ] + templates["likes"]
        templates["reposts"] = [
            "Поделись с тем, кто любит экскурсии.",
            "Поделись с подругой, которая любит экскурсии.",
        ] + templates["reposts"]
        if theme and any(word in theme.casefold() for word in ("зоопарк", "зоолог", "ветеринар", "животн")):
            templates["comments"] = ["Какие закулисные истории зоопарка вам интересны?"] + templates["comments"]
            templates["likes"] = [
                "Поставь лайк, если интересен зоопарк изнутри.",
                "Лайк, если интересны встречи с зоологами.",
            ] + templates["likes"]
            templates["reposts"] = ["Поделись с тем, кому интересен зоопарк изнутри."] + templates["reposts"]
    elif event_type == "meeting":
        templates["comments"] = [
            "Что бы спросили на такой встрече?",
            "Какие темы таких встреч вам ближе?",
            "Что ждёте от этой встречи? Напишите в комментариях.",
        ] + templates["comments"]
        templates["likes"] = [
            "Поставь лайк, если интересны такие встречи.",
            "Отметь лайком, если любишь живые встречи.",
        ] + templates["likes"]
        templates["reposts"] = [
            "Поделись с тем, кому интересны такие встречи.",
            "Поделись с другом, который любит живые встречи.",
            "Поделись с подругой, которой близки живые встречи.",
        ] + templates["reposts"]
    elif event_type == "family":
        templates["comments"].extend(
            [
                "Кому из близких нравятся такие семейные события?",
                "Какие детские события вам особенно нравятся?",
            ]
        )
        templates["likes"].extend(
            [
                "Отметь лайком, если любишь семейные события.",
                "Поставь лайк, если интересны детские события.",
            ]
        )
        templates["reposts"].extend(
            [
                "Поделись с теми, кому близки семейные события.",
                "Поделись с подругой, которой интересны детские события.",
                "Поделись с подругой-мамой, которой близки семейные события.",
                "Поделись с мамой-подругой, которой интересны детские выходные.",
            ]
        )
        if theme and ("сказоч" in theme or "геро" in theme):
            templates["comments"].append("Кого из сказочных героев дети ждут больше всего?")
            templates["reposts"].append("Поделись с другом, чьи дети любят сказочных героев.")
            templates["reposts"].append("Поделись с подругой, чьи дети любят сказочных героев.")
            templates["reposts"].append("Поделись с подругой-мамой, чьи дети любят сказочные истории.")
    if theme:
        templates["likes"].extend(
            [
                "Поставь лайк, если любишь {THEME}.",
                "Поддержи лайком, если любишь {THEME}.",
                "Отметь лайком, если выбираешь {THEME}.",
            ]
        )
        templates["comments"].append("Расскажите, за что любите {THEME}.")
    if festival:
        templates["comments"] = [
            f"Были на {festival_on} раньше? Расскажите, как впечатления.",
            f"Что в {festival_context} вам ближе? Напишите.",
        ] + templates["comments"]
        templates["likes"] = [
            "Поставь лайк, если {FINTEREST}.",
            "Лайк, если следите за {FFOLLOW}.",
            f"Поставь лайк, если уже был на {festival_on}.",
        ] + templates["likes"]
        templates["reposts"] = [
            "Поделись с теми, кому {FINTEREST}.",
            "Поделись с теми, кто следит за {FFOLLOW}.",
        ] + templates["reposts"]
    if idea:
        templates["comments"] = [
            "Как вам идея {IDEA}? Напишите в комментариях.",
            "Вам близка идея {IDEA}? Расскажите.",
        ] + templates["comments"]
        templates["likes"] = [
            "Поставь лайк, если нравится идея {IDEA}.",
            "Лайк, если за идею {IDEA}.",
        ] + templates["likes"]
        templates["reposts"] = [
            "Поделись с тем, кому понравится идея {IDEA}.",
            "Поделись с другом, которому близка идея {IDEA}.",
            "Поделись с подругой, которой близка идея {IDEA}.",
        ] + templates["reposts"]
    return templates


def _configured_templates_for(config: dict[str, Any], event_type: str) -> dict[str, list[str]]:
    raw = config.get("cta_templates") or config.get("cta_templates_by_mechanic") or {}
    if not isinstance(raw, dict):
        return {}

    merged: dict[str, list[str]] = {}

    def add_templates(source: Any) -> None:
        if not isinstance(source, dict):
            return
        for mechanic, values in source.items():
            mechanic_key = str(mechanic).strip().lower()
            if mechanic_key not in MECHANIC_BADGES:
                continue
            if isinstance(values, str):
                values = [values]
            if not isinstance(values, list):
                continue
            clean = [str(value).strip() for value in values if str(value or "").strip()]
            if clean:
                merged.setdefault(mechanic_key, []).extend(clean)

    add_templates(raw)
    by_type = raw.get("by_event_type") or raw.get("event_types") or {}
    if isinstance(by_type, dict):
        add_templates(by_type.get(event_type))
        add_templates(by_type.get("*"))
    return merged


def _sanitize_cta_text(text: str) -> str:
    text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(re.sub(r"[ \t\f\v]+", " ", part).strip() for part in text.split("\n"))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
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


def _is_holiday_name(value: str | None) -> bool:
    normalized = re.sub(r"\s+", " ", str(value or "").strip().casefold())
    return normalized in HOLIDAY_FESTIVAL_NAMES


def _effective_festival(event: Event) -> str | None:
    value = (event.festival or "").strip()
    if not value or _is_holiday_name(value):
        return None
    return value


def _festival_display_name(festival: str | None) -> str | None:
    if not festival:
        return None
    value = re.sub(r"\s+", " ", str(festival).strip())
    if not value:
        return None
    if "фестив" not in value.casefold():
        return value
    quoted = re.search(r"[«\"]([^»\"]{2,80})[»\"]", value)
    if quoted:
        return f"«{quoted.group(1).strip()}»"
    stripped = re.sub(
        r"^\s*(?:\d+\s+|[ivxlcdm]+\s+)?(?:(?:международн|областн|городск|региональн|ежегодн)\w+\s+)*фестивал[ья]?\s+",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip()
    return stripped or value


def _festival_from_phrase(event: Event, festival: str | None) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    phrase = f"фестиваля {display}"
    if _festival_is_annual(event):
        phrase += " в этом году"
    return phrase


def _festival_on_phrase(festival: str | None) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    return f"фестивале {display}"


def _festival_context_phrase(event: Event, festival: str | None, event_type: str) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    raw = _event_text_blob(event)
    value = display.casefold()
    if "кантата" in value and "образователь" in raw:
        return "образовательной программе фестиваля Кантата"
    if "80 истор" in value:
        return "проекте «80 историй о главном»"
    if "другой зоопарк" in value or _looks_zoo_event(raw):
        return "проекте «Другой зоопарк»"
    return f"программе {_festival_from_phrase(event, festival) or f'фестиваля {display}'}"


def _festival_interest_clause(event: Event, festival: str | None, event_type: str) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    raw = _event_text_blob(event)
    value = display.casefold()
    if "кантата" in value and "образователь" in raw:
        return "интересна образовательная программа фестиваля Кантата"
    if "80 истор" in value:
        return "интересны истории Калининградской области"
    if "другой зоопарк" in value or _looks_zoo_event(raw):
        return "интересен зоопарк изнутри"
    return f"интересны события {_festival_from_phrase(event, festival) or display}"


def _festival_follow_phrase(event: Event, festival: str | None) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    raw = _event_text_blob(event)
    value = display.casefold()
    if "кантата" in value and "образователь" in raw:
        return "образовательной программой фестиваля Кантата"
    if "80 истор" in value:
        return "проектом «80 историй о главном»"
    if "другой зоопарк" in value:
        return "проектом «Другой зоопарк»"
    return f"фестивалем {display}"


def _festival_hook_text(event: Event, festival: str | None, event_type: str) -> str | None:
    display = _festival_display_name(festival)
    if not display:
        return None
    value = display.casefold()
    raw = _event_text_blob(event)
    if "кантата" in value and "образователь" in raw:
        return "Кому близка образовательная программа Кантаты?"
    if "80 истор" in value:
        return "Кому близки истории Калининградской области?"
    if "другой зоопарк" in value or _looks_zoo_event(raw):
        return "Кому интересен зоопарк изнутри?"
    annual = " в этом году" if _festival_is_annual(event) else ""
    return f"Кто следит за фестивалем {display}{annual}?"


def _resolve_template(
    template: str,
    *,
    event_type: str,
    persona: str | None,
    festival: str | None,
    theme: str | None = None,
    topic: str,
    topic_accusative_plural: str,
    festival_from: str | None = None,
    festival_on: str | None = None,
    festival_context: str | None = None,
    festival_interest: str | None = None,
    festival_follow: str | None = None,
    idea: str | None = None,
    cinema_club: str | None = None,
) -> str | None:
    slots = {
        "N": persona,
        "ORG": cinema_club,
        "F": festival,
        "FFROM": festival_from,
        "FON": festival_on,
        "FCONTEXT": festival_context,
        "FINTEREST": festival_interest,
        "FFOLLOW": festival_follow,
        "IDEA": idea,
        "THEME": theme,
        "T": topic,
        "TA": topic_accusative_plural,
        "TP": _topic_prepositional_plural(event_type),
        "THIS_EVENT": _this_event_accusative(event_type),
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


DEFAULT_FORMAT_WEIGHTS = {
    "right_extension": 42,
    "bottom_extension": 30,
    "bottom_overlay": 18,
    "hook_swipe_cta": 10,
}


def _select_template_id(formats: Sequence[str], config: dict[str, Any], seed: str) -> str:
    if not formats:
        return "right_extension"
    if len(formats) == 1:
        return formats[0]
    raw_weights = config.get("format_weights") or config.get("template_weights") or {}
    weights: dict[str, float] = {}
    if isinstance(raw_weights, dict):
        for template_id, value in raw_weights.items():
            key = str(template_id).strip()
            if key not in SUPPORTED_TEMPLATE_IDS:
                continue
            try:
                weight = float(value)
            except Exception:
                continue
            if weight > 0:
                weights[key] = weight
    weighted: list[tuple[str, float]] = []
    for template_id in formats:
        weight = weights.get(template_id)
        if weight is None:
            weight = float(DEFAULT_FORMAT_WEIGHTS.get(template_id, 1))
        if weight > 0:
            weighted.append((template_id, weight))
    if not weighted:
        return formats[int(stable_unit_interval(seed + ":format") * len(formats)) % len(formats)]
    total = sum(weight for _template_id, weight in weighted)
    roll = stable_unit_interval(seed + ":format") * total
    upto = 0.0
    for template_id, weight in weighted:
        upto += weight
        if roll < upto:
            return template_id
    return weighted[-1][0]


def _vision_has_sparse_poster_text(vision: PosterVisionSummary | None) -> bool:
    if vision is None:
        return False
    text = re.sub(r"\s+", "", str(vision.text or ""))
    return len(text) < 20


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
    festival = _effective_festival(event)
    theme = _extract_theme(event, event_type)
    idea = _extract_idea_phrase(event, event_type)
    cinema_club = _extract_cinema_club(event) if event_type == "cinema" else None
    festival_from = _festival_from_phrase(event, festival)
    festival_on = _festival_on_phrase(festival)
    festival_context = _festival_context_phrase(event, festival, event_type)
    festival_interest = _festival_interest_clause(event, festival, event_type)
    festival_follow = _festival_follow_phrase(event, festival)
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

    templates = _templates_for(event_type, persona, festival, theme, idea, cinema_club)
    configured_templates = _configured_templates_for(config, event_type)
    for configured_mechanic, configured_values in configured_templates.items():
        templates.setdefault(configured_mechanic, [])
        templates[configured_mechanic] = configured_values + templates[configured_mechanic]
    cta_text = ""
    mechanic = "comments"
    for candidate_mechanic in bag[:20]:
        options = list(templates.get(candidate_mechanic) or [])
        configured_options = list(configured_templates.get(candidate_mechanic) or [])
        if configured_options and _parse_bool(config.get("prefer_configured_cta_templates"), default=True):
            rnd.shuffle(configured_options)
            generic_options = [option for option in options if option not in configured_options]
            rnd.shuffle(generic_options)
            options = configured_options + generic_options
        else:
            rnd.shuffle(options)
        for template in options:
            resolved = _resolve_template(
                template,
                event_type=event_type,
                persona=persona,
                festival=festival,
                theme=theme,
                festival_from=festival_from,
                festival_on=festival_on,
                festival_context=festival_context,
                topic=topic,
                topic_accusative_plural=topic_accusative_plural,
                festival_interest=festival_interest,
                festival_follow=festival_follow,
                idea=idea,
                cinema_club=cinema_club,
            )
            if resolved and len(resolved) <= 95 and not _cta_text_has_forbidden_copy(resolved, event_type):
                cta_text = resolved
                mechanic = candidate_mechanic
                break
        if cta_text:
            break
    if not cta_text:
        mechanic = "likes"
        cta_text = "Поставь лайк, если интересно."

    palette_ids = list(config.get("palette_ids") or PALETTES.keys())
    palette_ids = [pid for pid in palette_ids if pid in PALETTES] or ["slate_warm_white"]
    palette_id = palette_ids[int(stable_unit_interval(seed + ":palette") * len(palette_ids)) % len(palette_ids)]
    formats = _configured_formats(config)
    template_id = _select_template_id(formats, config, seed)
    if vision and vision.confidence < float(config.get("right_extension_confidence_min", 0.55)):
        if "right_extension" in formats:
            template_id = "right_extension"
        elif "hook_swipe_cta" in formats:
            template_id = "hook_swipe_cta"
    if template_id == "bottom_overlay" and _vision_has_sparse_poster_text(vision):
        if "right_extension" in formats:
            template_id = "right_extension"
        elif "hook_swipe_cta" in formats:
            template_id = "hook_swipe_cta"
    hook_text = f"Есть вопрос к тем, кто уже был на таких {topic_prepositional_plural}"
    if festival:
        hook_text = _festival_hook_text(event, festival, event_type) or f"Кто уже был на {festival_on}?"
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
    if (
        not cta_text
        or len(cta_text) > 95
        or re.search(r"\{[^}]+\}", cta_text)
        or _cta_text_has_forbidden_copy(cta_text, fallback.event_type)
        or _text_has_unsupported_event_reference(event, cta_text)
    ):
        return None
    hook_text = _sanitize_cta_text(str(payload.get("hook_text") or "")) or fallback.hook_text
    if _text_has_unsupported_event_reference(event, hook_text):
        hook_text = fallback.hook_text
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


def _cta_text_has_forbidden_copy(text: str, event_type: str | None = None) -> bool:
    lower = str(text or "").casefold()
    if not lower:
        return True
    forbidden_fragments = (
        "были на похожем событии",
        "похожем событии",
        "поддержи лайком формат",
        "поддержка формата",
        "поддержать формат",
        "из темы",
        "хочешь чаще",
        "хочется видеть чаще",
        "видеть чаще",
        "почаще",
        "чаще провод",
        "нужны такие",
        "нужно больше таких",
        "больше таких событий",
    )
    if any(fragment in lower for fragment in forbidden_fragments):
        return True
    if _cta_text_has_non_social_action(lower):
        return True
    if event_type == "cinema" and "спектак" in lower:
        return True
    if event_type == "theatre" and "кинопоказ" in lower:
        return True
    if event_type != "family" and any(fragment in lower for fragment in ("сказоч", "сказк", "геро")):
        return True
    if "фестивал" in lower and re.search(r"\b(?:жд[её](?:шь|те|т|м|ем)|жду|ждут|ждать)\b", lower):
        return True
    if "фестивал" in lower and any(name in lower for name in HOLIDAY_FESTIVAL_NAMES):
        return True
    return False


def _cta_text_has_non_social_action(lower_text: str) -> bool:
    """Reject CTA copy that acts as attendance/commerce prompt, not VK engagement."""

    direct_fragments = (
        "зарегистр",
        "запиш",
        "куп",
        "билет",
        "приход",
        "приди",
        "посети",
        "сходи",
        "успей",
        "заброни",
        "планиру",
        "в планы",
        "куда сходить",
        "куда пойти",
        "хочет на",
        "хочешь на",
        "пошёл бы на",
        "пошла бы на",
        "пойти на",
        "пойдешь на",
        "пойдёшь на",
        "пойдете на",
        "пойдёте на",
        "заглянуть",
        "загляни",
        "присоедин",
    )
    return any(fragment in lower_text for fragment in direct_fragments)


def _text_has_unsupported_event_reference(event: Event, text: str | None) -> bool:
    lower = str(text or "").casefold()
    if not lower:
        return False
    event_blob = _event_text_blob(event)
    has_zoo_place = "зоопарк" in event_blob or "зоолог" in event_blob
    if any(word in lower for word in ("зоопарк", "зоолог")) and not has_zoo_place:
        return True
    if any(word in lower for word in ("ветеринар", "животн")) and not _looks_zoo_event(event_blob):
        return True
    if "закулисье зоопарка" in lower and "зоопарк" not in event_blob:
        return True
    return False


def _cta_text_is_generic_comment_question(text: str, mechanic: str) -> bool:
    if mechanic != "comments":
        return False
    lower = str(text or "").casefold()
    generic_fragments = (
        "что для вас главное",
        "что вам ближе в таких",
        "что ждёте от",
        "что цепляет вас в таких",
        "какие темы таких",
    )
    return any(fragment in lower for fragment in generic_fragments)


def _safe_generic_hook(event_type: str) -> str:
    if event_type == "cinema":
        return "Кто любит кинопоказы?"
    if event_type == "theatre":
        return "Кто любит спектакли?"
    if event_type == "concert":
        return "Кто любит концерты?"
    if event_type == "lecture":
        return "Кто любит лекции?"
    if event_type == "excursion":
        return "Кто любит экскурсии?"
    if event_type == "exhibition":
        return "Кто любит выставки?"
    if event_type == "holiday":
        return "Кто любит городские праздники?"
    return "Кому интересно такое событие?"


def _safe_generic_cta(event_type: str, mechanic: str) -> str:
    if mechanic == "reposts":
        return "Поделись с другом, которому это может понравиться."
    if mechanic == "comments":
        if event_type == "cinema":
            return "Напишите в комментариях, что ждёте от кинопоказа."
        if event_type == "theatre":
            return "Напишите в комментариях, что ждёте от спектакля."
        if event_type == "concert":
            return "Напишите в комментариях, что ждёте от концерта."
        if event_type == "lecture":
            return "Напишите в комментариях, что ждёте от лекции."
        if event_type == "excursion":
            return "Напишите в комментариях, что ждёте от экскурсии."
        if event_type == "meeting":
            return "Напишите в комментариях, что ждёте от встречи."
        if event_type == "exhibition":
            return "Напишите в комментариях, что ждёте от выставки."
        if event_type == "volunteer":
            return "Напишите в комментариях, близко ли вам волонтёрство."
        if event_type == "party":
            return "Напиши в комментариях, кого позвал бы на вечеринку."
        if event_type == "holiday":
            return "Напишите в комментариях, любите ли такие праздники."
        return "Напишите в комментариях, что ждёте от события."
    if event_type == "cinema":
        return "Поставь лайк, если любишь кинопоказы."
    if event_type == "theatre":
        return "Поставь лайк, если любишь спектакли."
    if event_type == "concert":
        return "Поставь лайк, если любишь концерты."
    if event_type == "lecture":
        return "Поставь лайк, если любишь лекции."
    if event_type == "excursion":
        return "Поставь лайк, если любишь экскурсии."
    if event_type == "meeting":
        return "Поставь лайк, если интересны такие встречи."
    if event_type == "exhibition":
        return "Поставь лайк, если любишь выставки."
    if event_type == "volunteer":
        return "Поставь лайк, если тебе близко волонтёрство."
    if event_type == "party":
        return "Лайк, если за такие вечеринки."
    if event_type == "holiday":
        return "Поставь лайк, если любишь городские праздники."
    return "Поставь лайк, если интересно."


def _ultra_safe_cta(mechanic: str) -> str:
    if mechanic == "comments":
        return "Напишите в комментариях, что думаете."
    if mechanic == "reposts":
        return "Поделись с другом."
    return "Поставь лайк, если интересно."


def _llm_text_mode(config: dict[str, Any]) -> str:
    raw = str(config.get("llm_text_mode") or os.getenv("AFISHAENGAGEMENT_LLM_TEXT_MODE", "auto"))
    raw = raw.strip().casefold()
    if raw in {"0", "false", "off", "disabled", "none"}:
        return "off"
    if raw in {"1", "true", "on", "always"}:
        return "always"
    return "auto"


def _should_run_llm_text(event: Event, plan: EngagementPlan, config: dict[str, Any]) -> bool:
    mode = _llm_text_mode(config)
    if mode == "off":
        return False
    if mode == "always":
        return True
    if config.get("cta_templates") and not _parse_bool(config.get("llm_text_rewrite_configured"), False):
        return _cta_text_has_forbidden_copy(plan.cta_text, plan.event_type) or _text_has_unsupported_event_reference(
            event, plan.cta_text
        )
    if _cta_text_has_forbidden_copy(plan.cta_text, plan.event_type) or _text_has_unsupported_event_reference(
        event, plan.cta_text
    ):
        return True
    if _text_has_unsupported_event_reference(event, plan.hook_text):
        return True
    if _cta_text_is_generic_comment_question(plan.cta_text, plan.mechanic):
        return True
    theme = _extract_theme(event, plan.event_type)
    if theme and plan.mechanic == "comments" and theme.casefold() in plan.cta_text.casefold():
        return True
    return False


async def build_llm_cta_text(
    event: Event,
    *,
    plan: EngagementPlan,
    config: dict[str, Any],
    vision: PosterVisionSummary | None,
) -> tuple[EngagementPlan, int, str]:
    started = time.monotonic()
    try:
        import main as main_mod

        ask_4o = getattr(main_mod, "ask_4o", None)
        if ask_4o is None:
            return plan, 0, "fallback_no_ask_4o"
        theme = _extract_theme(event, plan.event_type)
        cinema_club = _extract_cinema_club(event) if plan.event_type == "cinema" else None
        prompt = (
            "Ты пишешь финальный CTA-текст для карточки афиши VK. "
            "Layout уже выбран кодом; не описывай дизайн и не меняй механику. "
            "Верни только JSON без markdown: {\"cta_text\":\"...\",\"hook_text\":\"...\"}.\n"
            "Правила: cta_text максимум 95 символов; живой естественный русский; "
            "не используй канцелярит, не склоняй машинно готовые куски; обращение на ты допустимо. "
            "CTA должен призывать только к доступному действию во VK: написать комментарий, поставить лайк или поделиться/сделать репост. "
            "Не зови идти на событие, присоединяться к событию/празднику, покупать билеты, регистрироваться, записываться, планировать визит, сохранять в планы или искать, куда сходить. "
            "Для механики comments задай цепляющий, но конкретный вопрос по этому событию: "
            "про тему афиши, формат, организатора, артиста, произведение или идею события, если они явно есть в данных. "
            "Не оставляй общий вопрос вида «что цепляет вас в таких событиях» без конкретики. "
            "Если кинопоказ делает киноклуб, можно спросить, были ли уже на показах этого киноклуба. "
            "Если событие связано с конкретным артистом или его творчеством, можно спросить про отношение к его творчеству, "
            "включая давно умерших известных артистов, но только если имя явно есть в данных. "
            "Запрещены фразы: «Были на похожем событии», «поддержи формат», «поддержка формата», "
            "«из темы ...». Для кинопоказа не пиши про спектакль; для спектакля не пиши про кинопоказ. "
            "День России, День Победы и другие праздники не называй фестивалями. "
            "Про сказочных героев пиши только если событие действительно детское/семейное со сказочными героями. "
            "Для волонтёрских событий уместны формулировки про волонтёрство и добровольчество. "
            "Если событие внутри фестиваля или проекта, можно сохранять зонтичный контекст, "
            "но привязывай CTA к программе, проекту или теме конкретного события, а не к абстрактному ожиданию фестиваля. "
            "Не пиши «ждёшь/ждёте фестиваля»: если фестиваль уже идёт, это звучит неверно. "
            "Если в данных явно есть концепт события, например концерт на воде или кинопоказ под открытым небом, "
            "можно использовать CTA про идею этого концепта; не выдумывай концепт без прямого сигнала. "
            "Зоопарк, зоологов, животных или ветеринарный уход упоминай только если это явно есть в данных события. "
            "Для экскурсий по зоопарку не называй событие лекцией. "
            "Если используешь тему, впиши её грамотно: например «Поддержи лайком, если любишь симфоническую музыку» "
            "или «Что вам ближе в органной музыке?», но не «из темы органную музыку».\n"
            f"Seed: {plan.seed}\n"
            f"Механика: {plan.mechanic}; текущий черновик: {plan.cta_text!r}; hook: {plan.hook_text!r}.\n"
            f"Событие: title={event.title!r}; normalized_type={plan.event_type!r}; stored_type={event.event_type!r}; "
            f"festival={event.festival!r}; theme={theme!r}; cinema_club={cinema_club!r}; "
            f"date={event.date!r}; place={event.location_name!r}; "
            f"description={(event.description or '')[:700]!r}; search_digest={(event.search_digest or '')[:400]!r}; "
            f"poster_text={(vision.text if vision else '')[:500]!r}."
        )
        raw = await ask_4o(
            prompt,
            max_tokens=140,
            temperature=0.2,
            meta={"feature": "afishaengagement", "stage": "cta_text", "event_id": getattr(event, "id", None)},
        )
        payload = _extract_json_object(str(raw or ""))
        if not payload:
            return plan, int((time.monotonic() - started) * 1000), "fallback_text_bad_json"
        cta_text = _sanitize_cta_text(str(payload.get("cta_text") or ""))
        if (
            not cta_text
            or len(cta_text) > 95
            or re.search(r"\{[^}]+\}", cta_text)
            or _cta_text_has_forbidden_copy(cta_text, plan.event_type)
            or _text_has_unsupported_event_reference(event, cta_text)
        ):
            return plan, int((time.monotonic() - started) * 1000), "fallback_text_invalid"
        hook_text = _sanitize_cta_text(str(payload.get("hook_text") or ""))
        if _text_has_unsupported_event_reference(event, hook_text):
            hook_text = ""
        return (
            replace(plan, cta_text=cta_text, hook_text=(hook_text[:95] if hook_text else plan.hook_text)),
            int((time.monotonic() - started) * 1000),
            "llm_text",
        )
    except Exception as exc:
        logger.warning("afishaengagement llm text failed: %s", exc)
        return plan, int((time.monotonic() - started) * 1000), "fallback_text_error"


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
            "CTA должен быть только про действие во VK: комментарий, лайк или репост/поделиться. "
            "Не зови идти на событие, присоединяться к событию/празднику, покупать билеты, регистрироваться, записываться, планировать визит, сохранять в планы или искать, куда сходить; "
            "для comments лучше задать конкретный вопрос по событию, организатору, артисту, теме афиши или идее события, "
            "а не общий вопрос про «такие события»; "
            "если кинопоказ делает киноклуб, можно спросить, были ли уже на его показах; "
            "если явно указан артист/автор, можно спросить про отношение к его творчеству; "
            "фестивальный зонтик можно сохранять, но текст должен быть про программу/проект/тему события; "
            "не пиши «ждёшь/ждёте фестиваля», особенно для уже идущего фестиваля; "
            "идея события вроде концерта на воде или кинопоказа под открытым небом допустима только при прямом сигнале; "
            "экскурсии по зоопарку не называй лекциями; "
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


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02X}{:02X}{:02X}".format(*rgb)


def _mix_rgb(a: tuple[int, int, int], b: tuple[int, int, int], weight_b: float) -> tuple[int, int, int]:
    weight_b = max(0.0, min(1.0, float(weight_b)))
    weight_a = 1.0 - weight_b
    return (
        int(a[0] * weight_a + b[0] * weight_b),
        int(a[1] * weight_a + b[1] * weight_b),
        int(a[2] * weight_a + b[2] * weight_b),
    )


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


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _palette_roles(palette: dict[str, str]) -> dict[str, str]:
    surface = palette.get("surface") or palette.get("background") or "#1A1C20"
    ink = palette.get("ink") or palette.get("text") or "#F2EFE8"
    signal = palette.get("signal") or palette.get("accent") or "#E14B3A"
    signal_ink = palette.get("signal_ink") or palette.get("accent_text") or ink
    surface_rgb = _hex_to_rgb(surface)
    return {
        "surface": surface,
        "surface_hi": palette.get("surface_hi") or _rgb_to_hex(_mix_rgb(surface_rgb, (255, 255, 255), 0.30)),
        "surface_lo": palette.get("surface_lo") or _rgb_to_hex(_mix_rgb(surface_rgb, (0, 0, 0), 0.35)),
        "ink": ink,
        "signal": signal,
        "signal_ink": signal_ink,
        "seam": palette.get("seam") or signal,
        "rim": palette.get("rim") or ink,
        "family": palette.get("family") or "legacy",
        "tone": palette.get("tone") or "editorial",
        "background": surface,
        "text": ink,
        "accent": signal,
        "accent_text": signal_ink,
    }


def _rgb_chroma(rgb: tuple[int, int, int]) -> float:
    return (max(rgb) - min(rgb)) / 255.0


def _rgb_temperature(rgb: tuple[int, int, int]) -> str:
    hue, _lightness, saturation = _rgb_hls(rgb)
    if saturation < 0.14:
        return "neutral"
    if hue <= 75 or hue >= 315:
        return "warm"
    if 160 <= hue <= 280:
        return "cool"
    return "neutral"


def _hue_band_score(surface_hue: float | None, poster_hue: float | None, luma_sep: float) -> float:
    if surface_hue is None or poster_hue is None:
        return 0.72
    dist = _hue_distance(surface_hue, poster_hue)
    if dist <= 25:
        return 0.48 if luma_sep >= 0.45 else 0.08
    if dist <= 55:
        return 0.12
    if dist <= 105:
        return 0.95
    if dist <= 165:
        return 0.86
    if dist <= 195:
        return 0.70
    if dist <= 255:
        return 0.90
    return 0.45 if luma_sep >= 0.45 else 0.12


def _seam_region_box(template_id: str, width: int, height: int) -> tuple[int, int, int, int]:
    strip = max(1, int(min(width, height) * 0.08))
    if template_id == "right_extension":
        return (width - strip, 0, width, height)
    return (0, height - strip, width, height)


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


def _poster_color_profile(poster: Any, region_box: tuple[int, int, int, int] | None = None) -> dict[str, Any]:
    from PIL import Image

    source = poster.convert("RGB")
    if region_box is not None:
        width, height = source.size
        left, top, right, bottom = region_box
        left = max(0, min(width - 1, left))
        top = max(0, min(height - 1, top))
        right = max(left + 1, min(width, right))
        bottom = max(top + 1, min(height, bottom))
        source = source.crop((left, top, right, bottom))

    sample = source.copy()
    sample.thumbnail((96, 96), Image.Resampling.LANCZOS)
    quantized = sample.quantize(colors=8, method=Image.Quantize.MEDIANCUT).convert("RGB")
    colors = quantized.getcolors(maxcolors=96 * 96) or []
    colors.sort(reverse=True, key=lambda item: item[0])
    top_colors = colors[:5]
    total = max(1, sum(count for count, _rgb in top_colors))
    dominant: list[tuple[int, int, int]] = [rgb for _count, rgb in top_colors]
    weights = [count / total for count, _rgb in top_colors]
    saturated_hues = []
    avg_luma = 0.0
    avg_chroma = 0.0
    temp_weights = {"warm": 0.0, "cool": 0.0, "neutral": 0.0}
    for rgb, weight in zip(dominant, weights):
        hue, _lightness, saturation = _rgb_hls(rgb)
        if saturation >= 0.2:
            saturated_hues.append(hue)
        avg_luma += _relative_luminance(rgb) * weight
        avg_chroma += _rgb_chroma(rgb) * weight
        temp_weights[_rgb_temperature(rgb)] += weight
    edge = source.resize((1, 1), Image.Resampling.BOX)
    edge_rgb = edge.getpixel((0, 0))
    edge_hue, _edge_lightness, edge_saturation = _rgb_hls(edge_rgb)
    if avg_luma < 0.18:
        bucket_luma = "deep"
    elif avg_luma < 0.35:
        bucket_luma = "dark"
    elif avg_luma > 0.85:
        bucket_luma = "blown"
    elif avg_luma > 0.65:
        bucket_luma = "light"
    else:
        bucket_luma = "mid"
    if avg_chroma < 0.18:
        bucket_chroma = "muted"
    elif avg_chroma > 0.70:
        bucket_chroma = "neon"
    elif avg_chroma > 0.45:
        bucket_chroma = "saturated"
    else:
        bucket_chroma = "balanced"
    temperature = max(temp_weights.items(), key=lambda item: item[1])[0]
    is_monochrome = avg_chroma < 0.10
    if len(saturated_hues) >= 2:
        first = saturated_hues[0]
        is_monochrome = is_monochrome or all(_hue_distance(first, hue) <= 25 for hue in saturated_hues[1:])
    return {
        "dominant": dominant,
        "weights": weights,
        "dominant_hues": saturated_hues,
        "edge_rgb": edge_rgb,
        "edge_luma": _relative_luminance(edge_rgb),
        "edge_hue": edge_hue if edge_saturation >= 0.14 else None,
        "avg_luma": avg_luma,
        "avg_chroma": avg_chroma,
        "bucket_luma": bucket_luma,
        "bucket_chroma": bucket_chroma,
        "temperature": temperature,
        "is_monochrome": is_monochrome,
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


def _family_score(family: str, profile: dict[str, Any]) -> float:
    luma = str(profile.get("bucket_luma") or "mid")
    chroma = str(profile.get("bucket_chroma") or "balanced")
    if family == "legacy":
        family = "graphite"
    table = {
        "deep": {"ivory": 1.0, "warm_dark": 0.3, "cool_dark": 0.3, "saturated_pop": 0.3, "graphite": 0.6},
        "dark": {"ivory": 1.0 if chroma in {"saturated", "neon"} else 0.9, "warm_dark": 0.5, "cool_dark": 0.6, "saturated_pop": 0.1 if chroma in {"saturated", "neon"} else 0.3, "graphite": 0.7},
        "mid": {"ivory": 0.8 if chroma in {"saturated", "neon"} else 0.7, "warm_dark": 0.7, "cool_dark": 0.7, "saturated_pop": 0.2 if chroma in {"saturated", "neon"} else 0.5, "graphite": 0.8},
        "light": {"ivory": 0.2, "warm_dark": 1.0 if chroma in {"saturated", "neon"} else 0.9, "cool_dark": 1.0 if chroma in {"saturated", "neon"} else 0.9, "saturated_pop": 0.4 if chroma in {"saturated", "neon"} else 0.6, "graphite": 0.85},
        "blown": {"ivory": 0.0, "warm_dark": 1.0, "cool_dark": 0.9, "saturated_pop": 0.5, "graphite": 0.85},
    }
    return table.get(luma, table["mid"]).get(family, 0.55)


def _tone_score(tone: str, event_type: str | None) -> float:
    event_type = event_type or "other"
    if tone == "festival" and event_type in {"festival", "market"}:
        return 1.0
    if tone == "concert" and event_type in {"concert", "cinema"}:
        return 1.0
    if tone == "lecture" and event_type == "lecture":
        return 1.0
    if tone == "lecture" and event_type == "excursion":
        return 0.90
    if tone == "lecture" and event_type == "meeting":
        return 0.90
    if tone == "family" and event_type in {"family", "excursion"}:
        return 1.0
    if tone == "party" and event_type == "party":
        return 1.0
    if tone in {"editorial", "noir"}:
        return 0.82
    return 0.62


def _score_palette(
    *,
    palette_id: str,
    palette: dict[str, str],
    profile: dict[str, Any],
    event_type: str | None,
    seed: str,
    preferred_id: str,
) -> PaletteScore | None:
    roles = _palette_roles(palette)
    surface = _hex_to_rgb(roles["surface"])
    ink = _hex_to_rgb(roles["ink"])
    signal = _hex_to_rgb(roles["signal"])
    edge_rgb = profile.get("edge_rgb") or (128, 128, 128)
    text_contrast = _contrast_ratio(surface, ink)
    signal_contrast = _contrast_ratio(surface, signal)
    poster_contrast = _contrast_ratio(surface, edge_rgb)
    surface_luma = _relative_luminance(surface)
    edge_luma = float(profile.get("edge_luma") or _relative_luminance(edge_rgb))
    luma_sep = abs(surface_luma - edge_luma)
    surface_hue, _surface_lightness, surface_sat = _rgb_hls(surface)
    surface_hue_value = surface_hue if surface_sat >= 0.14 else None
    hue_sep = _hue_band_score(surface_hue_value, profile.get("edge_hue"), luma_sep)
    readability = min(text_contrast / 7.0, 1.0)
    signal_pop = min(signal_contrast / 3.0, 1.0)
    separation = _clip((poster_contrast - 1.6) / 3.0)
    luma_score = _clip(luma_sep / 0.55)
    family_fit = _family_score(roles["family"], profile)
    tone_fit = _tone_score(roles["tone"], event_type)
    chroma_sep = abs(_rgb_chroma(surface) - float(profile.get("avg_chroma") or 0.0))
    chroma_logic = _clip(chroma_sep / 0.55)
    surface_temp = _rgb_temperature(surface)
    poster_temp = str(profile.get("temperature") or "neutral")
    temperature_bonus = 0.20 if surface_temp != "neutral" and poster_temp != "neutral" and surface_temp != poster_temp else 0.0
    premium_penalty = 0.0
    if palette_id == "yellow_violet":
        premium_penalty += 0.50
        if event_type not in {"festival", "market", "family", "party"}:
            premium_penalty += 0.30
    if roles["family"] == "saturated_pop" and profile.get("bucket_chroma") in {"saturated", "neon"}:
        premium_penalty += 0.75
    if roles["family"] == "saturated_pop" and event_type in {"lecture", "exhibition", "excursion"}:
        premium_penalty += 0.25
    if surface_temp == poster_temp and luma_sep < 0.45 and surface_temp != "neutral":
        premium_penalty += 0.45
    if signal_contrast < 2.4:
        premium_penalty += 0.35
    if text_contrast < 5.0 or poster_contrast < 1.95 or luma_sep < 0.28:
        return None
    preferred_bonus = 0.12 if palette_id == preferred_id else 0.0
    jitter = stable_unit_interval(f"{seed}:palette_separation:{palette_id}") * 0.08
    score = (
        1.80 * separation
        + 1.40 * luma_score
        + 1.10 * readability
        + 0.85 * hue_sep
        + 0.70 * family_fit
        + 0.55 * tone_fit
        + 0.45 * signal_pop
        + 0.35 * chroma_logic
        + temperature_bonus
        + preferred_bonus
        + jitter
        - 1.20 * min(premium_penalty, 1.0)
    )
    return PaletteScore(
        palette_id=palette_id,
        score=score,
        text_contrast=text_contrast,
        signal_contrast=signal_contrast,
        poster_contrast=poster_contrast,
        luma_sep=luma_sep,
        hue_sep=hue_sep,
        family_fit=family_fit,
        tone_fit=tone_fit,
    )


def _choose_compatible_palette_id(
    poster: Any,
    preferred_id: str,
    seed: str,
    *,
    template_id: str = "right_extension",
    event_type: str | None = None,
) -> str:
    region_box = _seam_region_box(template_id, poster.width, poster.height)
    profile = _poster_color_profile(poster, region_box=region_box)
    scored: list[PaletteScore] = []
    for palette_id, palette in PALETTES.items():
        score = _score_palette(
            palette_id=palette_id,
            palette=palette,
            profile=profile,
            event_type=event_type,
            seed=seed,
            preferred_id=preferred_id,
        )
        if score is not None:
            scored.append(score)
    if not scored:
        edge_luma = float(profile.get("edge_luma") or 0.5)
        poster_temp = str(profile.get("temperature") or "neutral")
        if edge_luma < 0.18:
            return "ivory_charcoal_oxblood"
        if poster_temp == "warm":
            return "graphite_signal_chartreuse"
        return "graphite_signal_red"
    scored.sort(key=lambda item: item.score, reverse=True)
    top = scored[: min(3, len(scored))]
    index = int(stable_unit_interval(f"{seed}:palette_separation_pick:{template_id}") * len(top)) % len(top)
    return top[index].palette_id


def _force_right_extension_for_bottom_template(poster: Any, template_id: str) -> bool:
    if template_id not in {"bottom_overlay", "bottom_extension"}:
        return False
    if _prefers_bottom_extension_for_horizontal(poster):
        return False
    if poster.width < 720:
        return True
    aspect = poster.height / max(1, poster.width)
    if template_id == "bottom_overlay" and aspect > 1.18:
        return True
    if template_id == "bottom_extension" and aspect > 1.02:
        return True
    return False


def _prefers_bottom_extension_for_horizontal(poster: Any) -> bool:
    return poster.width > poster.height * 1.02


def _text_bbox(draw: Any, xy: tuple[int, int], text: str, font: Any) -> tuple[int, int, int, int]:
    return draw.textbbox(xy, text, font=font)


def _wrap_words(
    draw: Any,
    text: str,
    font: Any,
    max_width: int,
    *,
    allow_hyphen_break: bool = False,
) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        width = _text_bbox(draw, (0, 0), candidate, font)[2]
        if current and width > max_width:
            lines.append(current)
            if allow_hyphen_break and "-" in word and _text_bbox(draw, (0, 0), word, font)[2] > max_width:
                parts = [part for part in word.split("-") if part]
                if len(parts) == 2:
                    first = f"{parts[0]}-"
                    second = parts[1]
                    if (
                        _text_bbox(draw, (0, 0), first, font)[2] <= max_width
                        and _text_bbox(draw, (0, 0), second, font)[2] <= max_width
                    ):
                        lines.append(first)
                        current = second
                        continue
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
    avoid_orphan_lines: bool = False,
    allow_hyphen_break: bool = False,
) -> TextFit | None:
    from PIL import Image, ImageDraw

    probe = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(probe)
    for size in range(preferred_px, min_px - 1, -4):
        font = _load_font(font_name, size)
        lines: list[str] = []
        for paragraph in str(text or "").splitlines() or [""]:
            wrapped = _wrap_words(draw, paragraph, font, box_width, allow_hyphen_break=allow_hyphen_break)
            lines.extend(wrapped or [""])
        if len(lines) > max_lines:
            continue
        if avoid_orphan_lines and _has_orphan_cta_line(lines):
            continue
        if any(_text_bbox(draw, (0, 0), line, font)[2] > box_width for line in lines):
            continue
        line_heights = []
        max_w = 0
        for line in lines:
            bbox = _text_bbox(draw, (0, 0), line, font)
            max_w = max(max_w, bbox[2] - bbox[0])
            line_heights.append(bbox[3] - bbox[1])
        total_h = int(sum(line_heights) + max(0, len(lines) - 1) * size * 0.18)
        if total_h <= box_height:
            return TextFit(font_px=size, lines=lines, width=max_w, height=total_h)
    return None


_CTA_ORPHAN_WORDS = {
    "а",
    "в",
    "и",
    "к",
    "о",
    "с",
    "у",
    "бы",
    "до",
    "за",
    "из",
    "ли",
    "на",
    "не",
    "об",
    "от",
    "по",
}


def _has_orphan_cta_line(lines: Sequence[str]) -> bool:
    for line in lines:
        normalized = re.sub(r"[^0-9A-Za-zА-Яа-яЁё]+", "", line).casefold()
        if normalized in _CTA_ORPHAN_WORDS:
            return True
    return False


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


def _composite_aa_line(
    image: Any,
    points: list[tuple[float, float]],
    *,
    fill: tuple[int, int, int],
    width: int,
    aa_scale: int = 8,
) -> Any:
    from PIL import Image, ImageDraw

    aa_scale = max(2, int(aa_scale))
    image_width, image_height = image.size
    pad = max(4, int(width * 3))
    min_x = max(0, int(min(x for x, _ in points)) - pad)
    min_y = max(0, int(min(y for _, y in points)) - pad)
    max_x = min(image_width, int(max(x for x, _ in points)) + pad)
    max_y = min(image_height, int(max(y for _, y in points)) + pad)
    crop_w = max(1, max_x - min_x)
    crop_h = max(1, max_y - min_y)
    hi = Image.new("RGBA", (crop_w * aa_scale, crop_h * aa_scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(hi)
    scaled = [(int((x - min_x) * aa_scale), int((y - min_y) * aa_scale)) for x, y in points]
    draw.line(scaled, fill=(*fill, 255), width=max(1, int(width * aa_scale)))
    overlay = Image.new("RGBA", (image_width, image_height), (0, 0, 0, 0))
    overlay.paste(hi.resize((crop_w, crop_h), Image.Resampling.LANCZOS), (min_x, min_y))
    return Image.alpha_composite(image.convert("RGBA"), overlay).convert("RGB")


def _composite_aa_rounded_rect(
    image: Any,
    box: tuple[int, int, int, int],
    *,
    radius: int,
    fill: tuple[int, int, int],
    outline: tuple[int, int, int] | None = None,
    outline_width: int = 0,
    aa_scale: int = 4,
) -> Any:
    from PIL import Image, ImageDraw

    aa_scale = max(2, int(aa_scale))
    x0, y0, x1, y1 = box
    width = max(1, x1 - x0)
    height = max(1, y1 - y0)
    hi = Image.new("RGBA", (width * aa_scale, height * aa_scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(hi)
    draw.rounded_rectangle(
        (0, 0, width * aa_scale, height * aa_scale),
        radius=max(1, int(radius * aa_scale)),
        fill=(*fill, 255),
        outline=(*outline, 255) if outline else None,
        width=max(1, int(outline_width * aa_scale)) if outline and outline_width > 0 else 1,
    )
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    overlay.paste(hi.resize((width, height), Image.Resampling.LANCZOS), (x0, y0))
    return Image.alpha_composite(image.convert("RGBA"), overlay).convert("RGB")


def _apply_cta_grain(
    image: Any,
    *,
    polygon: list[tuple[int, int]],
    seed: str,
    amount: int = 4,
    step: int = 3,
) -> Any:
    from PIL import Image, ImageDraw

    if not polygon:
        return image
    width, height = image.size
    mask = Image.new("L", (width, height), 0)
    ImageDraw.Draw(mask).polygon(polygon, fill=255)
    px = image.load()
    mask_px = mask.load()
    rng = random.Random(seed)
    min_x = max(0, min(x for x, _ in polygon))
    max_x = min(width - 1, max(x for x, _ in polygon))
    min_y = max(0, min(y for _, y in polygon))
    max_y = min(height - 1, max(y for _, y in polygon))
    amount = max(1, min(6, int(amount)))
    step = max(2, int(step))
    for y in range(min_y, max_y + 1, step):
        for x in range(min_x, max_x + 1, step):
            if not mask_px[x, y]:
                continue
            delta = rng.randint(-amount, amount)
            r, g, b = px[x, y]
            px[x, y] = (
                max(0, min(255, r + delta)),
                max(0, min(255, g + delta)),
                max(0, min(255, b + delta)),
            )
    return image


def _offset_segment(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    dx: float,
    dy: float,
) -> tuple[tuple[float, float], tuple[float, float]]:
    return (start[0] + dx, start[1] + dy), (end[0] + dx, end[1] + dy)


def _diagonal_shadow_offset(
    seam_start: tuple[float, float],
    seam_end: tuple[float, float],
    *,
    toward: tuple[float, float],
    distance: float,
) -> tuple[int, int]:
    sx, sy = seam_start
    ex, ey = seam_end
    vx = ex - sx
    vy = ey - sy
    length = math.hypot(vx, vy)
    if length <= 0:
        tx, ty = toward
        fallback_len = math.hypot(tx, ty) or 1.0
        return int(round(distance * tx / fallback_len)), int(round(distance * ty / fallback_len))
    candidates = ((-vy / length, vx / length), (vy / length, -vx / length))
    tx, ty = toward
    nx, ny = max(candidates, key=lambda item: item[0] * tx + item[1] * ty)
    return int(round(nx * distance)), int(round(ny * distance))


def _compose_cta_edge(
    image: Any,
    *,
    seam_start: tuple[float, float],
    seam_end: tuple[float, float],
    cta_normal: tuple[float, float],
    surface: tuple[int, int, int],
    ink: tuple[int, int, int],
    seam: tuple[int, int, int],
    accent: tuple[int, int, int],
    rim: tuple[int, int, int],
    scale: float,
    include_accent_stripe: bool = True,
) -> Any:
    nx, ny = cta_normal
    light = _mix_rgb(surface, (255, 255, 255), 0.34)
    accent_color = _mix_rgb(accent, ink, 0.25)

    light_start, light_end = _offset_segment(seam_start, seam_end, dx=1 * nx, dy=1 * ny)
    accent_start, accent_end = _offset_segment(
        seam_start,
        seam_end,
        dx=max(8, int(10 * scale)) * nx,
        dy=max(8, int(10 * scale)) * ny,
    )
    edge_layers = [
        (light_start, light_end, light, 1),
    ]
    if include_accent_stripe:
        edge_layers.append((accent_start, accent_end, accent_color, max(2, int(2 * scale))))
    for start, end, color, width in edge_layers:
        image = _composite_aa_line(image, [start, end], fill=color, width=width, aa_scale=8)
    return image


def _drop_shadow_overlay(
    *,
    size: tuple[int, int],
    polygon: list[tuple[int, int]],
    offset: tuple[int, int],
    alpha: int,
    blur: int,
    aa_scale: int = 4,
) -> Any:
    from PIL import Image, ImageDraw, ImageFilter

    width, height = size
    aa_scale = max(2, int(aa_scale))
    hi = Image.new("RGBA", (width * aa_scale, height * aa_scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(hi)
    dx, dy = offset
    points = [
        (int((x + dx) * aa_scale), int((y + dy) * aa_scale))
        for x, y in polygon
    ]
    draw.polygon(points, fill=(0, 0, 0, max(0, min(255, int(alpha)))))
    if blur > 0:
        hi = hi.filter(ImageFilter.GaussianBlur(radius=max(1, int(blur * aa_scale))))
    return hi.resize((width, height), Image.Resampling.LANCZOS)


def _composite_inset_rule(
    image: Any,
    *,
    start: tuple[float, float],
    end: tuple[float, float],
    color: tuple[int, int, int],
    width: int,
    aa_scale: int = 8,
) -> Any:
    return _composite_aa_line(
        image,
        [start, end],
        fill=color,
        width=max(1, int(width)),
        aa_scale=aa_scale,
    )


def _round_line(draw: Any, pts: list[tuple[float, float]], color: tuple[int, int, int], width: int) -> None:
    draw.line(pts, fill=color, width=width, joint="curve")
    radius = width / 2
    for px, py in (pts[0], pts[-1]):
        draw.ellipse((px - radius, py - radius, px + radius, py + radius), fill=color)


def _draw_right_arrow(
    draw: Any,
    x0: float,
    x1: float,
    y: float,
    color: tuple[int, int, int],
    *,
    width: int = 11,
    head: int = 22,
) -> None:
    _round_line(draw, [(x0, y), (x1, y)], color, width)
    _round_line(draw, [(x1 - head, y - head), (x1, y)], color, width)
    _round_line(draw, [(x1 - head, y + head), (x1, y)], color, width)


def _draw_down_arrow(
    draw: Any,
    cx: float,
    y0: float,
    y1: float,
    color: tuple[int, int, int],
    *,
    width: int = 14,
    head: int = 44,
) -> None:
    _round_line(draw, [(cx, y0), (cx, y1)], color, width)
    _round_line(draw, [(cx - head, y1 - head), (cx, y1)], color, width)
    _round_line(draw, [(cx + head, y1 - head), (cx, y1)], color, width)


def _badge_trailing_icon(mechanic: str) -> str:
    return MECHANIC_BADGE_ICONS.get(mechanic, "right_arrow")


def _badge_icon_reserve(icon: str | None, scale: float) -> int:
    if not icon:
        return 0
    if icon == "heart":
        return int(44 * scale)
    if icon == "down_arrow":
        return int(38 * scale)
    return int(46 * scale)


def _draw_tracking_text(
    draw: Any,
    xy: tuple[int, int],
    text: str,
    *,
    font: Any,
    fill: tuple[int, int, int],
    tracking: int,
) -> None:
    x, y = xy
    for ch in text:
        draw.text((x, y), ch, font=font, fill=fill)
        x += int(draw.textlength(ch, font=font)) + tracking


def _cta_eyebrow_metrics(scale: float) -> tuple[Any, int, int]:
    font_px = max(16, int(21 * scale))
    font = _load_font("Cygre-Medium.ttf", font_px)
    ascent, descent = font.getmetrics()
    return font, max(1, int(2 * scale)), ascent + descent


def _draw_cta_eyebrow(
    draw: Any,
    *,
    x: int,
    y: int,
    mechanic: str,
    scale: float,
    fill: tuple[int, int, int],
) -> int:
    font, tracking, height = _cta_eyebrow_metrics(scale)
    label = MECHANIC_EYEBROWS.get(mechanic, "ОТ РЕДАКЦИИ")
    _draw_tracking_text(draw, (x, y), label, font=font, fill=fill, tracking=tracking)
    return height


def _draw_badge(
    draw: Any,
    *,
    x: int,
    y: int,
    label: str,
    font: Any,
    bg: tuple[int, int, int],
    fg: tuple[int, int, int],
    accent: tuple[int, int, int],
    scale: float,
    max_width: int | None = None,
) -> tuple[int, int]:
    bbox = _text_bbox(draw, (0, 0), label, font)
    label_w = bbox[2] - bbox[0]
    pad_x = int(28 * scale)
    badge_h = int(58 * scale)
    badge_w = label_w + pad_x * 2
    if max_width:
        badge_w = min(max_width, badge_w)
    radius = badge_h // 2
    outline = _mix_rgb(bg, accent, 0.55)
    draw.rounded_rectangle(
        (x, y, x + badge_w, y + badge_h),
        radius=radius,
        fill=bg,
        outline=outline,
        width=max(1, int(2 * scale)),
    )
    text_x = x + max(0, int((badge_w - label_w) / 2))
    draw.text((text_x, y + int(11 * scale)), label, font=font, fill=fg)
    return badge_w, badge_h


def _fit_badge_font(
    label: str,
    *,
    scale: float,
    max_width: int,
    preferred_px: int | None = None,
    trailing_arrow: bool = False,
    trailing_icon: str | None = None,
) -> Any:
    from PIL import Image, ImageDraw

    preferred = preferred_px or max(20, int(34 * scale))
    minimum = max(14, int(20 * scale))
    pad_x = int(28 * scale)
    icon = trailing_icon or ("right_arrow" if trailing_arrow else None)
    icon_reserve = _badge_icon_reserve(icon, scale)
    probe = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(probe)
    for size in range(preferred, minimum - 1, -2):
        font = _load_font("Cygre-Bold.ttf", size)
        bbox = _text_bbox(draw, (0, 0), label, font)
        if (bbox[2] - bbox[0]) + pad_x * 2 + icon_reserve <= max_width:
            return font
    return _load_font("Cygre-Bold.ttf", minimum)


def _draw_badge_on_image(
    image: Any,
    *,
    x: int,
    y: int,
    label: str,
    font: Any,
    bg: tuple[int, int, int],
    fg: tuple[int, int, int],
    accent: tuple[int, int, int],
    scale: float,
    max_width: int | None = None,
    button: bool = False,
    trailing_arrow: bool = False,
    trailing_icon: str | None = None,
) -> tuple[Any, int, int]:
    from PIL import ImageDraw

    draw = ImageDraw.Draw(image)
    bbox = _text_bbox(draw, (0, 0), label, font)
    label_w = bbox[2] - bbox[0]
    pad_x = int(28 * scale)
    icon = trailing_icon or ("right_arrow" if trailing_arrow else None)
    icon_reserve = _badge_icon_reserve(icon, scale)
    badge_h = int(58 * scale)
    badge_w = label_w + pad_x * 2 + icon_reserve
    if max_width:
        badge_w = min(max_width, badge_w)
        if label_w + pad_x * 2 > badge_w:
            label_w = max(1, badge_w - pad_x * 2)
    image = _composite_aa_rounded_rect(
        image,
        (x, y, x + badge_w, y + badge_h),
        radius=badge_h // 2,
        fill=accent if button else bg,
        outline=None if button else accent,
        outline_width=0 if button else max(2, int(3 * scale)),
        aa_scale=4,
    )
    if not button:
        image = _composite_inset_rule(
            image,
            start=(x + int(9 * scale), y + badge_h - int(2 * scale)),
            end=(x + badge_w - int(9 * scale), y + badge_h - int(2 * scale)),
            color=_mix_rgb(bg, accent, 0.70),
            width=max(1, int(2 * scale)),
            aa_scale=4,
        )
    draw = ImageDraw.Draw(image)
    text_area_w = badge_w - icon_reserve
    text_x = x + max(0, int((text_area_w - label_w) / 2))
    draw.text((text_x, y + int(11 * scale)), label, font=font, fill=fg)
    if icon == "right_arrow":
        arrow_gap = int(10 * scale)
        arrow_x0 = x + text_area_w - int(2 * scale)
        arrow_x1 = x + badge_w - pad_x + int(3 * scale)
        arrow_y = y + badge_h / 2
        _draw_right_arrow(
            draw,
            arrow_x0 + arrow_gap,
            arrow_x1,
            arrow_y,
            fg,
            width=max(3, int(4 * scale)),
            head=max(7, int(8 * scale)),
        )
    elif icon == "down_arrow":
        arrow_x = x + badge_w - pad_x + int(2 * scale)
        arrow_top = y + int(16 * scale)
        arrow_bottom = y + badge_h - int(14 * scale)
        _draw_down_arrow(
            draw,
            arrow_x,
            arrow_top,
            arrow_bottom,
            fg,
            width=max(3, int(4 * scale)),
            head=max(7, int(7 * scale)),
        )
    elif icon == "heart":
        heart_x = x + badge_w - pad_x - int(25 * scale)
        heart_y = y + int(6 * scale)
        _draw_heart_icon(draw, heart_x, heart_y, int(42 * scale))
    return image, badge_w, badge_h


def _draw_heart_icon(draw: Any, x: float, y: float, font_px: int) -> int:
    size = max(16, min(int(font_px * 0.62), int(font_px * 0.54 + 8)))
    top = y + int(font_px * 0.18)
    left = x + int(font_px * 0.03)
    red = (226, 24, 54)
    r = size * 0.28
    draw.ellipse((left + size * 0.04, top, left + size * 0.04 + r * 2, top + r * 2), fill=red)
    draw.ellipse((left + size * 0.40, top, left + size * 0.40 + r * 2, top + r * 2), fill=red)
    draw.polygon(
        [
            (left + size * 0.02, top + size * 0.35),
            (left + size * 0.98, top + size * 0.35),
            (left + size * 0.50, top + size * 1.05),
        ],
        fill=red,
    )
    return size + int(font_px * 0.14)


def _draw_text_line(
    draw: Any,
    xy: tuple[int, int],
    text: str,
    *,
    font: Any,
    fill: tuple[int, int, int],
    font_px: int,
) -> None:
    x, y = xy
    parts = re.split(r"(?:❤️|❤)", text)
    hearts = len(re.findall(r"(?:❤️|❤)", text))
    for index, part in enumerate(parts):
        if part:
            draw.text((x, y), part, font=font, fill=fill)
            x += int(draw.textlength(part, font=font))
        if index < hearts:
            x += _draw_heart_icon(draw, x, y, font_px)


def render_right_extension(
    source_image: bytes,
    plan: EngagementPlan,
    *,
    prefer_bottom_for_horizontal: bool = True,
) -> RenderedImage:
    from PIL import Image, ImageDraw, ImageOps

    started = time.monotonic()
    if not source_image:
        raise ValueError("source poster is empty")
    with Image.open(io.BytesIO(source_image)) as opened:
        poster = ImageOps.exif_transpose(opened).convert("RGB")
    if prefer_bottom_for_horizontal and _prefers_bottom_extension_for_horizontal(poster):
        palette_id = _choose_compatible_palette_id(
            poster,
            plan.palette_id,
            plan.seed,
            template_id="bottom_extension",
            event_type=plan.event_type,
        )
        palette = PALETTES.get(palette_id) or PALETTES["graphite_signal_red"]
        return _render_bottom_extension(poster, plan, palette_id, palette, started)

    palette_id = _choose_compatible_palette_id(
        poster,
        plan.palette_id,
        plan.seed,
        template_id="right_extension",
        event_type=plan.event_type,
    )
    palette = PALETTES.get(palette_id) or PALETTES["graphite_signal_red"]
    roles = _palette_roles(palette)
    bg = _hex_to_rgb(roles["surface"])
    text_color = _hex_to_rgb(roles["ink"])
    accent = _hex_to_rgb(roles["signal"])
    signal_ink = _hex_to_rgb(roles.get("signal_ink") or "#FFFFFF")
    seam = _hex_to_rgb(roles["seam"])
    rim = _hex_to_rgb(roles["rim"])

    poster_w, height = poster.size
    scale = _layout_scale(height)
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    diagonal = max(int(10 * scale), min(int(42 * scale), int(poster_w * 0.035)))

    fit: TextFit | None = None
    width = 0
    safe_x = 0
    safe_y = int(108 * scale)
    safe_w = 0
    text_box_y = 0
    text_box_h = 0
    badge_h = int(58 * scale)
    badge_gap = max(int(54 * scale), int(height * 0.055))
    bottom_pad = max(int(36 * scale), int(height * 0.045))
    badge_y = height - bottom_pad - badge_h
    _, _, eyebrow_h = _cta_eyebrow_metrics(scale)
    eyebrow_gap = max(int(18 * scale), int(height * 0.014))
    max_cta_w = max(1, int(poster_w * 0.5))
    start_w = min(max_cta_w, max(140, int(220 * scale), int(poster_w * 0.30)))
    end_w = max_cta_w
    step_w = 4 if end_w - start_w <= 260 else max(6, int(8 * scale))
    candidates = list(range(start_w, end_w + 1, step_w))
    if not candidates or candidates[-1] != end_w:
        candidates.append(end_w)
    best_fit: tuple[int, int, int, int, int, TextFit] | None = None
    relaxed_fit: tuple[int, int, int, int, int, TextFit] | None = None
    for cta_w in candidates:
        width = poster_w + cta_w
        left_pad = max(12, min(int(34 * scale), int(cta_w * 0.07)))
        right_pad = max(14, min(int(42 * scale), int(cta_w * 0.07)))
        candidate_safe_x = poster_w + left_pad
        candidate_safe_w = width - candidate_safe_x - right_pad
        if candidate_safe_w <= int(130 * scale):
            continue
        text_box_y = safe_y + eyebrow_h + eyebrow_gap
        text_box_h = badge_y - badge_gap - text_box_y
        candidate_fit = fit_text(
            plan.cta_text,
            box_width=candidate_safe_w,
            box_height=text_box_h,
            preferred_px=max(34, int(84 * scale)),
            min_px=max(18, int(32 * scale)),
            max_lines=10,
            avoid_orphan_lines=True,
            allow_hyphen_break=True,
        )
        if candidate_fit is None:
            fallback_candidate = fit_text(
                plan.cta_text,
                box_width=candidate_safe_w,
                box_height=text_box_h,
                preferred_px=max(34, int(84 * scale)),
                min_px=max(18, int(32 * scale)),
                max_lines=10,
                allow_hyphen_break=True,
            )
            if fallback_candidate is not None:
                fallback_score = (
                    fallback_candidate.font_px,
                    -cta_w,
                    width,
                    candidate_safe_x,
                    candidate_safe_w,
                    fallback_candidate,
                )
                if relaxed_fit is None or fallback_score[:2] > relaxed_fit[:2]:
                    relaxed_fit = fallback_score
            continue
        score = (
            candidate_fit.font_px,
            -cta_w,
            width,
            candidate_safe_x,
            candidate_safe_w,
            candidate_fit,
        )
        if _has_orphan_cta_line(candidate_fit.lines):
            if relaxed_fit is None or score[:2] > relaxed_fit[:2]:
                relaxed_fit = score
            continue
        if best_fit is None or score[:2] > best_fit[:2]:
            best_fit = score
    if best_fit is None and relaxed_fit is not None:
        best_fit = relaxed_fit
    if best_fit is None:
        raise ValueError("text_overflow")
    _font_px, _neg_cta_w, width, safe_x, safe_w, fit = best_fit
    badge_icon = _badge_trailing_icon(plan.mechanic)
    badge_font = _fit_badge_font(badge, scale=scale, max_width=safe_w, trailing_icon=badge_icon)

    canvas = Image.new("RGB", (width, height), bg)
    canvas.paste(poster, (0, 0))

    cut_top = poster_w - int(diagonal * 0.35)
    cut_bottom = poster_w - diagonal
    cta_polygon = [(cut_top, 0), (width, 0), (width, height), (cut_bottom, height)]
    seam_start = (cut_top, 0)
    seam_end = (cut_bottom, height)
    shadow = _drop_shadow_overlay(
        size=(width, height),
        polygon=cta_polygon,
        offset=_diagonal_shadow_offset(
            seam_start,
            seam_end,
            toward=(-1.0, 0.0),
            distance=int(18 * scale),
        ),
        alpha=120,
        blur=max(10, int(26 * scale)),
        aa_scale=4,
    )
    preserve_x = int(poster_w * 0.95)
    if preserve_x > 0:
        shadow_alpha = shadow.getchannel("A")
        ImageDraw.Draw(shadow_alpha).rectangle((0, 0, preserve_x - 1, height), fill=0)
        shadow.putalpha(shadow_alpha)
    canvas = Image.alpha_composite(canvas.convert("RGBA"), shadow).convert("RGB")
    overlay = _aa_overlay(
        size=(width, height),
        polygon=cta_polygon,
        fill=(*bg, 255),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    canvas = _apply_cta_grain(
        canvas,
        polygon=cta_polygon,
        seed=f"{plan.seed}:right_extension:grain",
    )
    canvas = _compose_cta_edge(
        canvas,
        seam_start=seam_start,
        seam_end=seam_end,
        cta_normal=(1.0, 0.0),
        surface=bg,
        ink=text_color,
        seam=seam,
        accent=accent,
        rim=rim,
        scale=scale,
        include_accent_stripe=True,
    )
    canvas, _badge_w, badge_h = _draw_badge_on_image(
        canvas,
        x=safe_x,
        y=badge_y,
        label=badge,
        font=badge_font,
        bg=bg,
        fg=signal_ink,
        accent=accent,
        scale=scale,
        max_width=safe_w,
        button=True,
        trailing_icon=badge_icon,
    )
    draw = ImageDraw.Draw(canvas)

    eyebrow_color = _mix_rgb(text_color, bg, 0.45)
    _draw_cta_eyebrow(
        draw,
        x=safe_x,
        y=safe_y,
        mechanic=plan.mechanic,
        scale=scale,
        fill=eyebrow_color,
    )

    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.18)
    total_h = fit.height
    slack = text_box_h - total_h
    y = text_box_y + max(0, int(slack / 2))
    text_x = safe_x
    for line in fit.lines:
        _draw_text_line(draw, (text_x, y), line, font=main_font, fill=text_color, font_px=fit.font_px)
        bbox = _text_bbox(draw, (text_x, y), line, main_font)
        y += (bbox[3] - bbox[1]) + line_gap

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

    roles = _palette_roles(palette)
    bg = _hex_to_rgb(roles["surface"])
    text_color = _hex_to_rgb(roles["ink"])
    accent = _hex_to_rgb(roles["signal"])
    signal_ink = _hex_to_rgb(roles.get("signal_ink") or "#FFFFFF")
    seam = _hex_to_rgb(roles["seam"])
    rim = _hex_to_rgb(roles["rim"])

    width, poster_h = poster.size
    scale = _layout_scale(width)
    overlap = max(int(28 * scale), min(int(54 * scale), int(poster_h * 0.07)))
    block_top = poster_h - overlap
    diagonal = max(int(22 * scale), min(int(46 * scale), int(width * 0.035)))
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")

    fit: TextFit | None = None
    block_h = 0
    safe_x = max(36, int(54 * scale))
    safe_w = width - safe_x * 2
    badge_icon = _badge_trailing_icon(plan.mechanic)
    badge_font = _fit_badge_font(badge, scale=scale, max_width=safe_w, trailing_icon=badge_icon)
    badge_h = int(58 * scale)
    rail_w = 0
    rail_gap = 0
    content_top_pad = diagonal + max(int(32 * scale), int(width * 0.034))
    badge_gap = max(int(14 * scale), int(width * 0.016))
    bottom_pad = max(int(36 * scale), int(width * 0.034))
    badge_clear_from_seam = max(int(160 * scale), int(width * 0.14))
    _, _, eyebrow_h = _cta_eyebrow_metrics(scale)
    eyebrow_gap = max(int(16 * scale), int(width * 0.014))
    max_height = int(width * MAX_VK_FEED_PHOTO_ASPECT)
    max_block_h = min(max_height - poster_h + overlap, int(poster_h * 0.50 + overlap))
    min_block_h = max(int(220 * scale), int(width * 0.20), int(poster_h * 0.16))
    if max_block_h < min_block_h:
        raise ValueError("bottom_extension_aspect_unsafe")
    start_h = max(min_block_h, min(int(360 * scale), max_block_h))
    end_h = min(max_block_h, max(start_h + int(180 * scale), int(540 * scale)))
    step_h = max(8, int(18 * scale))
    candidates = list(range(start_h, end_h + 1, step_h))
    if not candidates or candidates[-1] != end_h:
        candidates.append(end_h)
    for candidate_h in reversed(candidates):
        candidate_height = poster_h + candidate_h - overlap
        candidate_badge_y = candidate_height - bottom_pad - badge_h
        if candidate_badge_y < block_top + diagonal + badge_clear_from_seam:
            continue
        text_box_y_candidate = block_top + content_top_pad + eyebrow_h + eyebrow_gap
        text_box_h = candidate_badge_y - badge_gap - text_box_y_candidate
        fit = fit_text(
            plan.cta_text,
            box_width=safe_w - rail_w - rail_gap,
            box_height=text_box_h,
            preferred_px=max(30, int(66 * scale)),
            min_px=max(22, int(34 * scale), int(width * 0.030)),
            max_lines=7,
            avoid_orphan_lines=True,
            allow_hyphen_break=True,
        )
        if fit is not None:
            block_h = candidate_h
            break
    if fit is None:
        raise ValueError("text_overflow")

    height = poster_h + block_h - overlap
    canvas = Image.new("RGB", (width, height), bg)
    canvas.paste(poster, (0, 0))

    cta_polygon = [(0, block_top + diagonal), (width, block_top), (width, height), (0, height)]
    seam_start = (0, block_top + diagonal)
    seam_end = (width, block_top)
    shadow = _drop_shadow_overlay(
        size=(width, height),
        polygon=cta_polygon,
        offset=_diagonal_shadow_offset(
            seam_start,
            seam_end,
            toward=(0.0, -1.0),
            distance=int(12 * scale),
        ),
        alpha=120,
        blur=max(10, int(20 * scale)),
        aa_scale=4,
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), shadow).convert("RGB")
    overlay = _aa_overlay(
        size=(width, height),
        polygon=cta_polygon,
        fill=(*bg, 255),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    canvas = _apply_cta_grain(
        canvas,
        polygon=cta_polygon,
        seed=f"{plan.seed}:bottom_extension:grain",
    )
    canvas = _compose_cta_edge(
        canvas,
        seam_start=seam_start,
        seam_end=seam_end,
        cta_normal=(0.0, 1.0),
        surface=bg,
        ink=text_color,
        seam=seam,
        accent=accent,
        rim=rim,
        scale=scale,
        include_accent_stripe=True,
    )
    badge_y = height - bottom_pad - badge_h
    if badge_y < block_top + diagonal + badge_clear_from_seam:
        raise ValueError("badge_clearance_overflow")
    draw = ImageDraw.Draw(canvas)
    badge_probe = ImageDraw.Draw(canvas)
    badge_label_bbox = _text_bbox(badge_probe, (0, 0), badge, badge_font)
    badge_w = min(
        safe_w,
        (badge_label_bbox[2] - badge_label_bbox[0]) + int(28 * scale) * 2 + _badge_icon_reserve(badge_icon, scale),
    )
    badge_x = safe_x + max(0, safe_w - badge_w)
    canvas, _, _ = _draw_badge_on_image(
        canvas,
        x=badge_x,
        y=badge_y,
        label=badge,
        font=badge_font,
        bg=bg,
        fg=signal_ink,
        accent=accent,
        scale=scale,
        max_width=safe_w,
        button=True,
        trailing_icon=badge_icon,
    )
    draw = ImageDraw.Draw(canvas)

    eyebrow_y = block_top + content_top_pad
    eyebrow_color = _mix_rgb(text_color, bg, 0.45)
    _draw_cta_eyebrow(
        draw,
        x=safe_x,
        y=eyebrow_y,
        mechanic=plan.mechanic,
        scale=scale,
        fill=eyebrow_color,
    )

    text_box_y = eyebrow_y + eyebrow_h + eyebrow_gap
    text_box_h = max(1, badge_y - badge_gap - text_box_y)
    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.18)
    slack = text_box_h - fit.height
    y = text_box_y + max(0, int(slack / 2))
    y = min(y, max(text_box_y, badge_y - badge_gap - fit.height - int(fit.font_px * 0.22)))
    if rail_w > 0:
        rail_h = max(int(72 * scale), min(int(text_box_h * 0.78), fit.height + int(22 * scale)))
        rail_y = y + max(0, int((fit.height - rail_h) / 2))
        canvas = _composite_aa_rounded_rect(
            canvas,
            (safe_x, rail_y, safe_x + rail_w, rail_y + rail_h),
            radius=max(3, int(5 * scale)),
            fill=accent,
            aa_scale=4,
        )
    draw = ImageDraw.Draw(canvas)
    text_x = safe_x + rail_w + rail_gap
    text_bottom = y
    for line in fit.lines:
        _draw_text_line(draw, (text_x, y), line, font=main_font, fill=text_color, font_px=fit.font_px)
        bbox = _text_bbox(draw, (text_x, y), line, main_font)
        text_bottom = max(text_bottom, bbox[3])
        y += (bbox[3] - bbox[1]) + line_gap
    if text_bottom + badge_gap > badge_y + int(fit.font_px * 0.24):
        raise ValueError("text_overflow")

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
    block_h = max(int(320 * scale), min(int(height * 0.42), int(480 * scale)))
    block_top = height - block_h
    diagonal = max(int(24 * scale), min(int(54 * scale), int(width * 0.05)))
    roles = _palette_roles(palette)
    bg = _hex_to_rgb(roles["surface"])
    text_color = _hex_to_rgb(roles["ink"])
    accent = _hex_to_rgb(roles["signal"])
    signal_ink = _hex_to_rgb(roles.get("signal_ink") or "#FFFFFF")
    seam = _hex_to_rgb(roles["seam"])
    rim = _hex_to_rgb(roles["rim"])

    safe_x = max(56, int(72 * scale))
    safe_w = width - safe_x * 2
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_icon = _badge_trailing_icon(plan.mechanic)
    badge_font = _fit_badge_font(badge, scale=scale, max_width=safe_w, trailing_icon=badge_icon)
    badge_h = int(58 * scale)
    rail_w = max(4, int(6 * scale))
    rail_gap = int(24 * scale)
    content_top_pad = diagonal + max(int(54 * scale), int(width * 0.060))
    badge_gap = max(int(22 * scale), int(width * 0.024))
    bottom_pad = max(int(36 * scale), int(width * 0.040))
    badge_clear_from_seam = max(int(250 * scale), int(width * 0.22))
    _, _, eyebrow_h = _cta_eyebrow_metrics(scale)
    eyebrow_gap = max(int(16 * scale), int(width * 0.014))
    badge_y = height - bottom_pad - badge_h
    if badge_y < block_top + diagonal + badge_clear_from_seam:
        raise ValueError("badge_clearance_overflow")
    text_box_y = block_top + content_top_pad + eyebrow_h + eyebrow_gap
    text_box_h = max(1, badge_y - badge_gap - text_box_y)

    fit = fit_text(
        plan.cta_text,
        box_width=safe_w - rail_w - rail_gap,
        box_height=text_box_h,
        preferred_px=max(30, int(62 * scale)),
        min_px=max(24, int(38 * scale), int(width * 0.038)),
        max_lines=5,
    )
    if fit is None:
        raise ValueError("text_overflow")

    canvas = poster.copy()
    cta_polygon = [(0, block_top + diagonal), (width, block_top), (width, height), (0, height)]
    seam_start = (0, block_top + diagonal)
    seam_end = (width, block_top)
    shadow = _drop_shadow_overlay(
        size=(width, height),
        polygon=cta_polygon,
        offset=_diagonal_shadow_offset(
            seam_start,
            seam_end,
            toward=(0.0, -1.0),
            distance=int(12 * scale),
        ),
        alpha=120,
        blur=max(10, int(20 * scale)),
        aa_scale=4,
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), shadow).convert("RGB")
    overlay = _aa_overlay(
        size=(width, height),
        polygon=cta_polygon,
        fill=(*bg, 255),
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")
    canvas = _apply_cta_grain(
        canvas,
        polygon=cta_polygon,
        seed=f"{plan.seed}:bottom_overlay:grain",
    )
    canvas = _compose_cta_edge(
        canvas,
        seam_start=seam_start,
        seam_end=seam_end,
        cta_normal=(0.0, 1.0),
        surface=bg,
        ink=text_color,
        seam=seam,
        accent=accent,
        rim=rim,
        scale=scale,
    )
    draw = ImageDraw.Draw(canvas)
    badge_bbox = _text_bbox(draw, (0, 0), badge, badge_font)
    badge_w = min(
        safe_w,
        (badge_bbox[2] - badge_bbox[0]) + int(28 * scale) * 2 + _badge_icon_reserve(badge_icon, scale),
    )
    badge_x = safe_x + max(0, safe_w - badge_w)
    canvas, _, _ = _draw_badge_on_image(
        canvas,
        x=badge_x,
        y=badge_y,
        label=badge,
        font=badge_font,
        bg=bg,
        fg=signal_ink,
        accent=accent,
        scale=scale,
        max_width=safe_w,
        button=True,
        trailing_icon=badge_icon,
    )
    draw = ImageDraw.Draw(canvas)

    eyebrow_y = block_top + content_top_pad
    eyebrow_color = _mix_rgb(text_color, bg, 0.45)
    _draw_cta_eyebrow(
        draw,
        x=safe_x,
        y=eyebrow_y,
        mechanic=plan.mechanic,
        scale=scale,
        fill=eyebrow_color,
    )

    main_font = _load_font("Cygre-Bold.ttf", fit.font_px)
    line_gap = int(fit.font_px * 0.18)
    slack = text_box_h - fit.height
    y = text_box_y if slack > int(fit.font_px * 1.5) else text_box_y + max(0, int(slack / 2))
    rail_h = max(int(72 * scale), min(int(text_box_h * 0.78), fit.height + int(22 * scale)))
    rail_y = y + max(0, int((fit.height - rail_h) / 2))
    canvas = _composite_aa_rounded_rect(
        canvas,
        (safe_x, rail_y, safe_x + rail_w, rail_y + rail_h),
        radius=max(3, int(5 * scale)),
        fill=accent,
        aa_scale=4,
    )
    draw = ImageDraw.Draw(canvas)
    text_x = safe_x + rail_w + rail_gap
    text_bottom = y
    for line in fit.lines:
        _draw_text_line(draw, (text_x, y), line, font=main_font, fill=text_color, font_px=fit.font_px)
        bbox = _text_bbox(draw, (text_x, y), line, main_font)
        text_bottom = max(text_bottom, bbox[3])
        y += (bbox[3] - bbox[1]) + line_gap
    if text_bottom + badge_gap > badge_y:
        raise ValueError("text_overflow")

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

    roles = _palette_roles(palette)
    bg = _hex_to_rgb(roles["surface"])
    text_color = _hex_to_rgb(roles["ink"])
    accent = _hex_to_rgb(roles["signal"])
    signal_ink = _hex_to_rgb(roles.get("signal_ink") or "#FFFFFF")
    card_w, card_h = 1080, 1350
    scale = card_w / 1080

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

    def poster_band_preserve_full_image() -> Image.Image:
        from PIL import ImageFilter

        band_size = (card_w, band_h)
        background = ImageOps.fit(
            poster,
            band_size,
            method=Image.Resampling.LANCZOS,
            centering=(0.5, 0.5),
        ).filter(ImageFilter.GaussianBlur(radius=24))
        tint = Image.new("RGB", band_size, bg)
        band = Image.blend(background.convert("RGB"), tint, 0.42)
        resize_scale = min(card_w / max(1, poster.width), band_h / max(1, poster.height))
        contained = poster.resize(
            (max(1, int(poster.width * resize_scale)), max(1, int(poster.height * resize_scale))),
            Image.Resampling.LANCZOS,
        )
        poster_x = int((card_w - contained.width) / 2)
        poster_y = int((band_h - contained.height) / 2)
        shadow_pad = max(6, int(12 * scale))
        shadow_box = (
            poster_x - shadow_pad,
            poster_y - shadow_pad,
            poster_x + contained.width + shadow_pad,
            poster_y + contained.height + shadow_pad,
        )
        shadow = Image.new("RGBA", band_size, (0, 0, 0, 0))
        shadow_draw = ImageDraw.Draw(shadow)
        shadow_draw.rounded_rectangle(shadow_box, radius=max(2, int(8 * scale)), fill=(0, 0, 0, 76))
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=max(5, int(10 * scale))))
        band = Image.alpha_composite(band.convert("RGBA"), shadow).convert("RGB")
        band.paste(contained, (poster_x, poster_y))
        return band

    hook_text = _sanitize_cta_text(plan.hook_text or "Есть вопрос к тем, кто уже был?")
    hook = Image.new("RGB", (card_w, card_h), bg)
    draw = ImageDraw.Draw(hook)
    safe_x = int(86 * scale)
    safe_w = card_w - safe_x * 2
    band_h = int(card_h * 0.58)
    poster_band = poster_band_preserve_full_image()
    hook.paste(poster_band, (0, 0))

    block_top = band_h
    block_polygon = [(0, block_top), (card_w, block_top), (card_w, card_h), (0, card_h)]
    shadow = _drop_shadow_overlay(
        size=(card_w, card_h),
        polygon=block_polygon,
        offset=(0, -int(10 * scale)),
        alpha=CTA_LAYER_SHADOW_ALPHA,
        blur=max(12, int(24 * scale)),
        aa_scale=4,
    )
    hook = Image.alpha_composite(hook.convert("RGBA"), shadow).convert("RGB")
    block_overlay = _aa_overlay(size=(card_w, card_h), polygon=block_polygon, fill=(*bg, 255))
    hook = Image.alpha_composite(hook.convert("RGBA"), block_overlay).convert("RGB")
    hook = _apply_cta_grain(hook, polygon=block_polygon, seed=f"{plan.seed}:hook_swipe:grain")
    hook = _compose_cta_edge(
        hook,
        seam_start=(0, block_top),
        seam_end=(card_w, block_top),
        cta_normal=(0.0, 1.0),
        surface=bg,
        ink=text_color,
        seam=accent,
        accent=accent,
        rim=text_color,
        scale=scale,
    )
    draw = ImageDraw.Draw(hook)

    swipe_font = _load_font("Cygre-Medium.ttf", int(36 * scale))
    swipe_label = "листай"
    swipe_w = draw.textlength(swipe_label, font=swipe_font)
    fa, fd = swipe_font.getmetrics()
    right_x = card_w - safe_x
    y0 = card_h - int(96 * scale)
    arrow_w = int(66 * scale)
    x0 = right_x - (swipe_w + int(18 * scale) + arrow_w)
    draw.text((x0, y0 - (fa + fd) / 2), swipe_label, font=swipe_font, fill=accent)
    _draw_right_arrow(
        draw,
        x0 + swipe_w + int(18 * scale),
        right_x,
        y0,
        accent,
        width=max(8, int(10 * scale)),
        head=max(14, int(18 * scale)),
    )

    hook_fit = fit_text(
        hook_text,
        box_width=safe_w - int(38 * scale),
        box_height=max(1, int(y0 - block_top - 170 * scale)),
        preferred_px=86,
        min_px=52,
        max_lines=3,
    )
    if hook_fit is None:
        raise ValueError("hook_text_overflow")
    hook_font = _load_font("Cygre-Bold.ttf", hook_fit.font_px)
    hook_x = safe_x
    hook_y = block_top + int(102 * scale)
    hook = _composite_aa_rounded_rect(
        hook,
        (safe_x, hook_y - int(34 * scale), safe_x + int(56 * scale), hook_y - int(28 * scale)),
        radius=int(3 * scale),
        fill=accent,
        aa_scale=4,
    )
    draw = ImageDraw.Draw(hook)
    y = hook_y
    for line in hook_fit.lines:
        _draw_text_line(draw, (hook_x, y), line, font=hook_font, fill=text_color, font_px=hook_fit.font_px)
        bbox = _text_bbox(draw, (hook_x, y), line, hook_font)
        y += (bbox[3] - bbox[1]) + int(hook_fit.font_px * 0.18)
    rule_y = card_h - int(92 * scale)
    rule_right = max(safe_x, int(x0 - 24 * scale))
    draw.line((safe_x, rule_y, rule_right, rule_y), fill=accent, width=5)

    cta = Image.new("RGB", (card_w, card_h), bg)
    badge = MECHANIC_BADGES.get(plan.mechanic, "CTA")
    badge_icon = _badge_trailing_icon(plan.mechanic)
    badge_font = _fit_badge_font(
        badge,
        scale=1.0,
        max_width=card_w - 172,
        preferred_px=42,
        trailing_icon=badge_icon,
    )
    cta, _, _ = _draw_badge_on_image(
        cta,
        x=86,
        y=96,
        label=badge,
        font=badge_font,
        bg=bg,
        fg=signal_ink,
        accent=accent,
        scale=1.0,
        max_width=card_w - 172,
        button=True,
        trailing_icon=badge_icon,
    )
    draw = ImageDraw.Draw(cta)

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
        _draw_text_line(draw, (86, y), line, font=cta_font, fill=text_color, font_px=cta_fit.font_px)
        bbox = _text_bbox(draw, (86, y), line, cta_font)
        y += (bbox[3] - bbox[1]) + int(cta_fit.font_px * 0.18)
    arrow_x = card_w // 2
    arrow_top = card_h - 330
    _draw_down_arrow(draw, arrow_x, arrow_top, arrow_top + 204, accent, width=14, head=48)
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

    with Image.open(io.BytesIO(source_image)) as opened:
        poster = ImageOps.exif_transpose(opened).convert("RGB")
    if plan.template_id == "right_extension":
        if _prefers_bottom_extension_for_horizontal(poster):
            promoted = replace(plan, template_id="bottom_extension")
            palette_id = _choose_compatible_palette_id(
                poster,
                promoted.palette_id,
                promoted.seed,
                template_id="bottom_extension",
                event_type=promoted.event_type,
            )
            palette = PALETTES.get(palette_id) or PALETTES["graphite_signal_red"]
            try:
                return [_render_bottom_extension(poster, promoted, palette_id, palette, started)]
            except ValueError as exc:
                logger.info(
                    "afishaengagement render fallback: horizontal_right_extension_bottom_failed width=%s height=%s reason=%s",
                    poster.width,
                    poster.height,
                    exc,
                )
                raise
        return [render_right_extension(source_image, plan, prefer_bottom_for_horizontal=False)]
    palette_id = _choose_compatible_palette_id(
        poster,
        plan.palette_id,
        plan.seed,
        template_id=plan.template_id,
        event_type=plan.event_type,
    )
    palette = PALETTES.get(palette_id) or PALETTES["graphite_signal_red"]
    if _force_right_extension_for_bottom_template(poster, plan.template_id):
        forced = replace(plan, template_id="right_extension")
        logger.info(
            "afishaengagement render fallback: forced_right_extension_unsafe_bottom_template template=%s width=%s height=%s",
            plan.template_id,
            poster.width,
            poster.height,
        )
        return [render_right_extension(source_image, forced)]
    if plan.template_id == "bottom_overlay" and _prefers_bottom_extension_for_horizontal(poster):
        promoted = replace(plan, template_id="bottom_extension")
        palette_id = _choose_compatible_palette_id(
            poster,
            promoted.palette_id,
            promoted.seed,
            template_id="bottom_extension",
            event_type=promoted.event_type,
        )
        palette = PALETTES.get(palette_id) or PALETTES["graphite_signal_red"]
        logger.info(
            "afishaengagement render fallback: promoted_horizontal_bottom_overlay_to_bottom_extension width=%s height=%s",
            poster.width,
            poster.height,
        )
        try:
            return [_render_bottom_extension(poster, promoted, palette_id, palette, started)]
        except ValueError as exc:
            if "overflow" not in str(exc) and "aspect_unsafe" not in str(exc):
                raise
            logger.info(
                "afishaengagement render fallback: horizontal_bottom_extension_rejected_after_overlay_promote width=%s height=%s reason=%s",
                poster.width,
                poster.height,
                exc,
            )
            raise
    try:
        if plan.template_id == "bottom_overlay":
            return [_render_bottom_overlay(poster, plan, palette_id, palette, started)]
        if plan.template_id == "bottom_extension":
            return [_render_bottom_extension(poster, plan, palette_id, palette, started)]
        if plan.template_id == "hook_swipe_cta":
            return _render_hook_swipe_cta(poster, plan, palette_id, palette, started)
    except ValueError as exc:
        if "overflow" in str(exc) or "aspect_unsafe" in str(exc):
            if _prefers_bottom_extension_for_horizontal(poster):
                logger.info(
                    "afishaengagement render fallback: horizontal_bottom_template_rejected template=%s width=%s height=%s reason=%s",
                    plan.template_id,
                    poster.width,
                    poster.height,
                    exc,
                )
                raise
            fallback = replace(plan, template_id="right_extension")
            logger.info(
                "afishaengagement render fallback: forced_right_extension_after_render_reject template=%s width=%s height=%s reason=%s",
                plan.template_id,
                poster.width,
                poster.height,
                exc,
            )
            return [render_right_extension(source_image, fallback, prefer_bottom_for_horizontal=False)]
        raise
    return [render_right_extension(source_image, plan)]


def render_plan_images_for_publish(
    source_image: bytes,
    plan: EngagementPlan,
) -> tuple[list[RenderedImage], EngagementPlan, str | None]:
    from PIL import Image, ImageOps

    try:
        rendered = render_plan_images(source_image, plan)
        actual = rendered[-1] if rendered else None
        if actual and (actual.template_id != plan.template_id or actual.palette_id != plan.palette_id):
            plan = replace(plan, template_id=actual.template_id, palette_id=actual.palette_id)
        return rendered, plan, None
    except Exception as exc:
        if "overflow" not in str(exc) and "aspect_unsafe" not in str(exc):
            raise
        poster = None
        try:
            with Image.open(io.BytesIO(source_image)) as opened:
                poster = ImageOps.exif_transpose(opened).convert("RGB")
        except Exception:
            poster = None
        if poster is not None and _prefers_bottom_extension_for_horizontal(poster):
            fallback = replace(
                plan,
                template_id="bottom_extension",
                cta_text=_safe_generic_cta(plan.event_type, plan.mechanic),
            )
            try:
                rendered = render_plan_images(source_image, fallback)
                actual = rendered[-1] if rendered else None
                if actual and (actual.template_id != fallback.template_id or actual.palette_id != fallback.palette_id):
                    fallback = replace(fallback, template_id=actual.template_id, palette_id=actual.palette_id)
                return rendered, fallback, f"safe_bottom_extension_after_{type(exc).__name__}:{exc}"
            except Exception as second_exc:
                if "overflow" not in str(second_exc) and "aspect_unsafe" not in str(second_exc):
                    raise
                fallback = replace(
                    fallback,
                    template_id="bottom_extension",
                    cta_text=_ultra_safe_cta(plan.mechanic),
                )
                rendered = render_plan_images(source_image, fallback)
                actual = rendered[-1] if rendered else None
                if actual and (actual.template_id != fallback.template_id or actual.palette_id != fallback.palette_id):
                    fallback = replace(fallback, template_id=actual.template_id, palette_id=actual.palette_id)
                return rendered, fallback, f"ultra_safe_bottom_extension_after_{type(second_exc).__name__}:{second_exc}"
        fallback = replace(
            plan,
            template_id="right_extension",
            cta_text=_safe_generic_cta(plan.event_type, plan.mechanic),
        )
        rendered = render_plan_images(source_image, fallback)
        actual = rendered[-1] if rendered else None
        if actual and (actual.template_id != fallback.template_id or actual.palette_id != fallback.palette_id):
            fallback = replace(fallback, template_id=actual.template_id, palette_id=actual.palette_id)
        return rendered, fallback, f"safe_right_extension_after_{type(exc).__name__}:{exc}"


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
    source_photo_urls: Sequence[str] | None = None,
) -> None:
    if db is None or event.id is None or candidate.campaign.id is None:
        return
    source_photo_urls = [str(url or "").strip() for url in (source_photo_urls or []) if str(url or "").strip()]
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
                "campaign_title": str(getattr(candidate.campaign, "title", "") or "")[:160],
                "activity_profile_key": str(getattr(candidate.activity, "profile_key", "") or "")[:120],
                "activity_surface": str(getattr(candidate.activity, "surface", "") or "")[:80],
                "event_title": str(getattr(event, "title", "") or "")[:160],
                "event_type": plan.event_type,
                "stored_event_type": str(getattr(event, "event_type", "") or "")[:80],
                "event_festival": str(getattr(event, "festival", "") or "")[:160],
                "source_post_url": str(getattr(event, "source_post_url", "") or "")[:240],
                "event_source_vk_post_url": str(getattr(event, "source_vk_post_url", "") or "")[:240],
                "source_first_photo_url": source_photo_urls[0] if source_photo_urls else None,
                "source_photo_urls_count": len(source_photo_urls),
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
    upload_vk_photo_bytes_fn: Callable[..., Awaitable[str | None]] | None = None,
    fetch_image_fn: Callable[[str], Awaitable[bytes]] | None = None,
    now_utc: datetime | None = None,
) -> str | None:
    started = time.monotonic()
    now_utc = now_utc or datetime.now(timezone.utc)
    event_id = int(event.id) if event.id is not None else None
    owner_id = -int(str(target_group_id).lstrip("-"))
    group_short = os.getenv("AFISHAENGAGEMENT_TARGET_GROUP_SHORT", DEFAULT_TARGET_GROUP_SHORT)
    photo_urls = [str(url or "").strip() for url in (photo_urls or []) if str(url or "").strip()]
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
    candidates = await resolve_candidates(db, event, target_group_id=target_group_id, now_utc=now_utc)
    digest = media_hash(photo_urls)
    _json_log(
        StageLog(
            stage="eligibility",
            decision="inspect",
            reason="source_media_bound",
            event_id=event_id,
            vk_owner_id=owner_id,
            vk_group_short=group_short,
            extra={
                "event_title": str(getattr(event, "title", "") or "")[:160],
                "source_post_url": str(getattr(event, "source_post_url", "") or "")[:240],
                "event_source_vk_post_url": str(getattr(event, "source_vk_post_url", "") or "")[:240],
                "photo_urls_count": len(photo_urls),
                "first_photo_url": photo_urls[0],
                "media_hash": digest,
            },
        )
    )
    if not candidates:
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
    candidate: EngagementCandidate | None = None
    config: dict[str, Any] = {}
    marker = ""
    build_tag = ""
    dice: DiceDecision | None = None
    campaign_id: int | None = None
    activity_id: int | None = None
    for candidate_item in candidates:
        item_campaign_id = int(candidate_item.campaign.id) if candidate_item.campaign.id is not None else None
        item_activity_id = int(candidate_item.activity.id) if candidate_item.activity.id is not None else None
        item_config = candidate_item.config
        item_marker, item_build_tag = _debug_marker(item_config, now_utc)
        shadow_mode = _env_debug_enabled(item_config)
        if not shadow_mode:
            _json_log(
                StageLog(
                    stage="eligibility",
                    decision="skip",
                    reason="shadow_disabled",
                    event_id=event_id,
                    campaign_id=item_campaign_id,
                    activity_id=item_activity_id,
                    vk_owner_id=owner_id,
                    vk_group_short=group_short,
                    shadow_mode=False,
                )
            )
            continue
        await maybe_cleanup_once(
            group_id=target_group_id,
            marker=item_marker,
            config=item_config,
            vk_api_fn=vk_api_fn,
            db=db,
            bot=bot,
        )
        if await _shadow_already_exists(
            db,
            event_id=event_id,
            activity_id=item_activity_id,
            marker=item_marker,
            build_tag=item_build_tag,
        ):
            _json_log(
                StageLog(
                    stage="eligibility",
                    decision="skip",
                    reason="shadow_duplicate",
                    event_id=event_id,
                    campaign_id=item_campaign_id,
                    activity_id=item_activity_id,
                    vk_owner_id=owner_id,
                    vk_group_short=group_short,
                    shadow_mode=True,
                    shadow_marker=item_marker,
                )
            )
            continue
        if await _debug_cap_reached(db, activity_id=item_activity_id, config=item_config, now_utc=now_utc):
            _json_log(
                StageLog(
                    stage="eligibility",
                    decision="skip",
                    reason="debug_cap_reached",
                    event_id=event_id,
                    campaign_id=item_campaign_id,
                    activity_id=item_activity_id,
                    vk_owner_id=owner_id,
                    vk_group_short=group_short,
                    shadow_mode=True,
                    shadow_marker=item_marker,
                )
            )
            continue
        item_dice = should_apply_rate(
            event_id=event_id,
            campaign_id=item_campaign_id,
            activity_id=item_activity_id,
            apply_rate=_apply_rate_from_config(item_config),
            salt=str(item_config.get("apply_salt") or ""),
            media_digest=digest,
        )
        if not item_dice.applies:
            _json_log(
                StageLog(
                    stage="dice",
                    decision="skip",
                    reason="dice_miss",
                    event_id=event_id,
                    campaign_id=item_campaign_id,
                    activity_id=item_activity_id,
                    vk_owner_id=owner_id,
                    vk_group_short=group_short,
                    seed=item_dice.seed,
                    apply_rate=item_dice.apply_rate,
                    dice_value=item_dice.value,
                    shadow_mode=True,
                    shadow_marker=item_marker,
                    extra={"candidate_fallback": True, "candidate_count": len(candidates)},
                )
            )
            continue
        candidate = candidate_item
        campaign_id = item_campaign_id
        activity_id = item_activity_id
        config = item_config
        marker = item_marker
        build_tag = item_build_tag
        dice = item_dice
        break
    if candidate is None or dice is None:
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
    if _should_run_llm_text(event, plan, config):
        plan, text_ms, text_provider = await build_llm_cta_text(
            event,
            plan=plan,
            config=config,
            vision=vision,
        )
        llm_ms += text_ms
        if text_provider == "llm_text":
            plan_provider = f"{plan_provider}+llm_text"
        else:
            plan_provider = f"{plan_provider}+{text_provider}"
    if _cta_text_has_forbidden_copy(plan.cta_text, plan.event_type) or _text_has_unsupported_event_reference(
        event, plan.cta_text
    ):
        plan = replace(plan, cta_text=_safe_generic_cta(plan.event_type, plan.mechanic))
        plan_provider = f"{plan_provider}+safe_text_fallback"
    if _text_has_unsupported_event_reference(event, plan.hook_text):
        plan = replace(plan, hook_text=_safe_generic_hook(plan.event_type))
        plan_provider = f"{plan_provider}+safe_hook_fallback"
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
        rendered_images, plan, render_fallback_reason = await asyncio.to_thread(
            render_plan_images_for_publish,
            source_image,
            plan,
        )
        if render_fallback_reason:
            plan_provider = f"{plan_provider}+render_safe_fallback"
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
        for idx, image in enumerate(rendered_images):
            generated_url = generated_urls[idx] if idx < len(generated_urls) else None
            attachment = None
            if generated_url:
                attachment = await upload_vk_photo_fn(target_group_id, generated_url, db, bot)
            if not attachment and upload_vk_photo_bytes_fn is not None:
                attachment = await upload_vk_photo_bytes_fn(
                    target_group_id,
                    image.data,
                    db,
                    bot,
                    filename=image.filename,
                )
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
            source_photo_urls=photo_urls,
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
                    "render_fallback_reason": render_fallback_reason,
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
