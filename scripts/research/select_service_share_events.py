#!/usr/bin/env python3
"""Build an auditable popular/promoted/stable-daily event mix."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


TZ = ZoneInfo("Europe/Kaliningrad")


def _median(values: list[float]) -> float:
    positive = [float(value) for value in values if math.isfinite(float(value)) and float(value) > 0]
    return statistics.median(positive) if positive else 0.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def popularity_scores(events: list[dict]) -> dict[int, float]:
    med_likes = _median([event.get("source_likes_count") or 0 for event in events])
    med_views = _median([event.get("source_views_count") or 0 for event in events])
    scores: dict[int, float] = {}
    for event in events:
        likes = float(event.get("source_likes_count") or 0)
        views = float(event.get("source_views_count") or 0)
        sources = float(event.get("source_engagement_sources_count") or 0)
        like_score = _clamp(likes / med_likes, 0, 4) if med_likes else math.log1p(likes)
        view_score = _clamp(views / med_views, 0, 4) if med_views else (1 if views else 0)
        scores[int(event["id"])] = round(like_score * .54 + view_score * .36 + min(2, sources) / 2 * .10, 4)
    return scores


def stable_key(local_date: str, event_id: int) -> str:
    return hashlib.sha256(f"service_share_card_v1|{local_date}|{event_id}".encode()).hexdigest()


def _eligible(event: dict, local_date: str) -> bool:
    start = str(event.get("start_date") or "")
    end = str(event.get("end_date") or start)
    return bool(event.get("id") and event.get("image_url") and event.get("lifecycle_status", "active") == "active" and start and end >= local_date)


def _row(event: dict, *, group: str, reason: str, score: float, local_date: str) -> dict:
    return {
        "event_id": int(event["id"]), "title": event.get("title"),
        "start_date": event.get("start_date"), "end_date": event.get("end_date"),
        "start_time": event.get("start_time"), "image_url": event.get("image_url"),
        "image_text_mode": event.get("image_text_mode"), "safe_crop": bool(event.get("safe_crop")),
        "image_has_ocr_text": bool(event.get("image_has_ocr_text")),
        "image_object_position": event.get("image_object_position"), "festival": event.get("festival"),
        "selection_group": group, "selection_reason": reason, "popularity_score": score,
        "metrics": {"likes": event.get("source_likes_count") or 0, "views": event.get("source_views_count") or 0,
                    "shares": 0, "sources": event.get("source_engagement_sources_count") or 0},
        "stable_daily_key": stable_key(local_date, int(event["id"])),
    }


def select(
    events: list[dict], *, local_date: str,
    promo_candidates: list[dict] | None = None,
    promo_festivals: set[str] | None = None,
    popular_count: int = 3, promoted_count: int = 2, random_count: int = 3,
    strict_promo: bool = False,
) -> dict:
    """Reserve real promotions before popularity and stable-daily rotation.

    When active exact-surface promo inventory is short, the result remains full
    unless ``strict_promo`` is set, but fallback rows retain their actual
    ``popular``/``random`` labels.  They are never misrepresented as promoted.
    """
    eligible = [event for event in events if _eligible(event, local_date)]
    by_id = {int(event["id"]): event for event in eligible}
    scores = popularity_scores(eligible)
    promo_candidates = list(promo_candidates or [])
    # Backwards-compatible research input; production calls pass resolved IDs.
    if not promo_candidates and promo_festivals:
        promo_candidates = [
            {"event_id": int(event["id"]), "priority": 0, "legacy_festival": event.get("festival")}
            for event in eligible if str(event.get("festival") or "") in promo_festivals
        ]
    used: set[int] = set()
    groups: dict[str, list[dict]] = {"popular": [], "promoted": [], "random": []}

    for candidate in promo_candidates:
        event_id = int(candidate.get("event_id") or 0)
        if len(groups["promoted"]) >= promoted_count:
            break
        event = by_id.get(event_id)
        if not event or event_id in used:
            continue
        used.add(event_id)
        selected_row = _row(
            event, group="promoted", score=scores.get(event_id, 0), local_date=local_date,
            reason="active configured promo campaign target for service_share_card",
        )
        selected_row["promo_audit"] = {key: candidate.get(key) for key in ("campaign_id", "activity_id", "target_id", "provenance")}
        groups["promoted"].append(selected_row)
    promo_missing = max(0, promoted_count - len(groups["promoted"]))
    if strict_promo and promo_missing:
        raise RuntimeError(f"promoted selection underfilled: requested={promoted_count} selected={len(groups['promoted'])}")

    popular = sorted(eligible, key=lambda event: (-scores[int(event["id"])], event["start_date"], int(event["id"])))
    for event in popular:
        if len(groups["popular"]) >= popular_count:
            break
        event_id = int(event["id"])
        if event_id not in used:
            used.add(event_id)
            groups["popular"].append(_row(event, group="popular", score=scores[event_id], local_date=local_date,
                                                   reason="current source reaction/view popularity score"))

    random_pool = sorted((event for event in eligible if int(event["id"]) not in used),
                         key=lambda event: stable_key(local_date, int(event["id"])))
    for event in random_pool[:random_count]:
        event_id = int(event["id"]); used.add(event_id)
        groups["random"].append(_row(event, group="random", score=scores[event_id], local_date=local_date,
                                              reason="stable daily SHA-256 rotation"))

    # Preserve eight required textures during promo underfill, with honest labels.
    target_total = popular_count + promoted_count + random_count
    fallback_pool = [event for event in popular if int(event["id"]) not in used]
    for event in fallback_pool:
        if sum(map(len, groups.values())) >= target_total:
            break
        event_id = int(event["id"]); used.add(event_id)
        group = "popular" if len(groups["popular"]) <= len(groups["random"]) else "random"
        reason = "explicit promo underfill fallback; actual group preserved"
        groups[group].append(_row(event, group=group, score=scores[event_id], local_date=local_date, reason=reason))
    if sum(map(len, groups.values())) != target_total:
        raise RuntimeError(f"selection underfilled: expected={target_total} selected={sum(map(len, groups.values()))}")

    # Visibility-aware texture assignment: promoted inventory reaches BRIDGE
    # front/HERO side instead of being consumed only by distant cube faces.
    desired = ["popular", "promoted", "random", "popular", "random", "promoted", "popular", "random"]
    ordered: list[dict] = []
    queues = {name: list(rows) for name, rows in groups.items()}
    for name in desired:
        if queues.get(name):
            ordered.append(queues[name].pop(0))
    for name in ("popular", "promoted", "random"):
        ordered.extend(queues[name])
    for index, row in enumerate(ordered):
        row["slot_index"] = index
    actual = dict(Counter(row["selection_group"] for row in ordered))
    promo_status = {"requested": promoted_count, "selected": len(groups["promoted"]),
                    "underfilled": bool(promo_missing), "missing": promo_missing,
                    "fallback_mislabeled_as_promo": False,
                    "reason": "active_explicit_promo_targets_with_approved_posters_exhausted" if promo_missing else None}
    return {
        "schema_version": "service_share_selection_v2", "generated_at": datetime.now(TZ).isoformat(),
        "local_date": local_date, "timezone": str(TZ), "eligible_count": len(eligible),
        "requested_mix": {"popular": popular_count, "promoted": promoted_count, "random": random_count},
        "actual_mix": {name: actual.get(name, 0) for name in ("popular", "promoted", "random")},
        "promo_status": promo_status,
        "promo_shortfall": ({key: promo_status[key] for key in ("requested", "selected", "missing", "reason")}
                            if promo_missing else None),
        "events": ordered,
    }


def media_available(url: str) -> bool:
    request = urllib.request.Request(url, headers={"User-Agent": "events-bot-service-share/1.0", "Range": "bytes=0-1023"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return 200 <= int(response.status) < 400 and bool(response.read(32))
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-json", required=True); parser.add_argument("--output", required=True)
    parser.add_argument("--promo-json"); parser.add_argument("--local-date", default=datetime.now(TZ).date().isoformat())
    parser.add_argument("--popular-count", type=int, default=3); parser.add_argument("--promoted-count", type=int, default=2)
    parser.add_argument("--random-count", type=int, default=3); parser.add_argument("--strict-promo", action="store_true")
    parser.add_argument("--preflight-media", action="store_true")
    args = parser.parse_args()
    raw = json.loads(Path(args.events_json).read_text()); events = raw.get("events", raw) if isinstance(raw, dict) else raw
    promo = json.loads(Path(args.promo_json).read_text()) if args.promo_json else []
    blocked: set[int] = set()
    while True:
        result = select([event for event in events if int(event.get("id") or 0) not in blocked], local_date=args.local_date,
                        promo_candidates=promo, popular_count=args.popular_count, promoted_count=args.promoted_count,
                        random_count=args.random_count, strict_promo=args.strict_promo)
        if not args.preflight_media:
            break
        failed = [row for row in result["events"] if not media_available(str(row["image_url"]))]
        if not failed:
            result["media_preflight"] = {"checked": len(result["events"]), "failed_event_ids": sorted(blocked)}; break
        blocked.update(int(row["event_id"]) for row in failed)
    output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "output": str(output), "ids": [row["event_id"] for row in result["events"]]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
