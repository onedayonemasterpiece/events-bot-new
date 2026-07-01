from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from .config import AcqConfig, load_config


def _age_decay(created_at: datetime | None, *, half_life_h: int, now: datetime | None = None) -> float:
    if not created_at:
        return 1.0
    now = now or datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - created_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
    return 0.5 ** (age_h / max(1, half_life_h))


def conservative_reach_low(
    *,
    platform: str,
    surface_type: str | None = None,
    post_views: int | None = None,
    recent_views_p10: int | None = None,
    context_created_at: datetime | None = None,
    config: AcqConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    cfg = config or load_config()
    platform = (platform or "").lower()
    stype = (surface_type or "").lower()
    decay = _age_decay(context_created_at, half_life_h=cfg.reach_age_decay_half_life_h, now=now)
    confidence = "low"
    basis = "unknown"
    base = 0.0
    factor = 1.0
    if platform == "tg" and (post_views or recent_views_p10):
        base = float(recent_views_p10 or post_views or 0)
        factor = cfg.tg_comment_readthrough_factor
        confidence = "medium"
        basis = "tg_post_views_p10" if recent_views_p10 else "tg_post_views"
    elif platform == "vk" and post_views:
        base = float(post_views or 0)
        factor = cfg.vk_comment_readthrough_factor
        confidence = "medium"
        basis = "vk_post_views"
    elif "group" in stype:
        low = max(1, int(cfg.reach_unknown_group_low))
        return {
            "low": low,
            "confidence": "low",
            "formula": "unknown_group_low",
            "basis": "group_activity_unknown",
            "age_decay": 1.0,
        }
    low = max(1, int(math.floor(base * factor * decay))) if base > 0 else max(1, min(3, cfg.reach_unknown_group_low))
    return {
        "low": low,
        "confidence": confidence,
        "formula": "lower_bound_comment_readthrough" if base > 0 else "unknown_low",
        "basis": basis,
        "age_decay": round(decay, 4),
    }


def priority_score(*, relevance: float, reach_low: int, spam_risk: str = "low", safety_risk: str = "low") -> float:
    if spam_risk == "high" or safety_risk == "high":
        return 0.0
    risk_multiplier = 0.6 if "medium" in {spam_risk, safety_risk} else 1.0
    reach_component = min(1.0, math.log1p(max(0, reach_low)) / math.log(101))
    return round(max(0.0, min(1.0, (0.7 * relevance + 0.3 * reach_component) * risk_multiplier)), 4)
