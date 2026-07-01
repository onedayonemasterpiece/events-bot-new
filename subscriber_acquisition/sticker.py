from __future__ import annotations

from typing import Any


def sticker_fit_from_observation(payload: dict[str, Any] | None) -> str:
    p = payload or {}
    if str(p.get("spam_risk") or "").lower() == "high":
        return "no"
    naturalness = float(p.get("naturalness_score") or 0)
    future30 = int(p.get("eligible_future_events_count_30d") or 0)
    good_posters = str(p.get("poster_image_quality") or "").lower() == "good"
    sticker_norm = str(p.get("audience_sticker_norm") or "").lower()
    if future30 >= 5 and good_posters and sticker_norm in {"medium", "high"} and naturalness >= 0.7:
        return "strong"
    if future30 >= 2 and naturalness >= 0.45:
        return "possible"
    if future30 > 0 and naturalness >= 0.2:
        return "weak"
    return "no"
