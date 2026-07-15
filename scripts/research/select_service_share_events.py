#!/usr/bin/env python3
"""Build an auditable popular/promoted/stable-random event mix for research cards."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import urllib.request
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
    med_likes = _median([event.get("source_likes_count") or event.get("likes_count") or 0 for event in events])
    med_shares = _median([event.get("shares_count") or 0 for event in events])
    med_views = _median([event.get("source_views_count") or 0 for event in events])
    scores: dict[int, float] = {}
    for event in events:
        likes = float(event.get("source_likes_count") or event.get("likes_count") or 0)
        shares = float(event.get("shares_count") or 0)
        views = float(event.get("source_views_count") or 0)
        service_likes = float(event.get("service_likes_count") or 0)
        sources = float(event.get("source_engagement_sources_count") or 0)
        like_score = _clamp(likes / med_likes, 0, 4) if med_likes else math.log1p(likes)
        share_score = _clamp(shares / med_shares, 0, 4) if med_shares else math.log1p(shares)
        view_score = _clamp(views / med_views, 0, 4) if med_views else (1 if views else 0)
        source_score = min(2, sources) / 2
        scores[int(event["id"])] = round(
            like_score * 0.38
            + share_score * 0.24
            + view_score * 0.24
            + source_score * 0.10
            + math.log1p(service_likes) * 0.04,
            4,
        )
    return scores


def stable_key(local_date: str, event_id: int) -> str:
    return hashlib.sha1(f"service_share_card|{local_date}|{event_id}".encode()).hexdigest()


def _eligible(event: dict, local_date: str) -> bool:
    start = str(event.get("start_date") or "")
    end = str(event.get("end_date") or start)
    return bool(
        event.get("id")
        and event.get("image_url")
        and event.get("lifecycle_status", "active") == "active"
        and start
        and end >= local_date
    )


def select(
    events: list[dict],
    *,
    local_date: str,
    promo_festivals: set[str],
    popular_count: int,
    promoted_count: int,
    random_count: int,
) -> dict:
    eligible = [event for event in events if _eligible(event, local_date)]
    scores = popularity_scores(eligible)
    used: set[int] = set()
    rows: list[dict] = []

    def add(event: dict, group: str, reason: str) -> None:
        event_id = int(event["id"])
        used.add(event_id)
        rows.append({
            "event_id": event_id,
            "title": event.get("title"),
            "start_date": event.get("start_date"),
            "end_date": event.get("end_date"),
            "start_time": event.get("start_time"),
            "image_url": event.get("image_url"),
            "image_text_mode": event.get("image_text_mode"),
            "safe_crop": bool(event.get("safe_crop")),
            "image_object_position": event.get("image_object_position"),
            "festival": event.get("festival"),
            "selection_group": group,
            "selection_reason": reason,
            "popularity_score": scores.get(event_id, 0),
            "metrics": {
                "likes": event.get("source_likes_count") or event.get("likes_count") or 0,
                "views": event.get("source_views_count") or 0,
                "shares": event.get("shares_count") or 0,
                "sources": event.get("source_engagement_sources_count") or 0,
            },
            "stable_daily_key": stable_key(local_date, event_id),
        })

    popular = sorted(eligible, key=lambda event: (-scores[int(event["id"])], event["start_date"], int(event["id"])))
    for event in popular:
        if len([row for row in rows if row["selection_group"] == "popular"]) >= popular_count:
            break
        add(event, "popular", "static-site popularity score from current source reactions/views")

    promoted = [
        event for event in eligible
        if int(event["id"]) not in used and str(event.get("festival") or "") in promo_festivals
    ]
    promoted.sort(key=lambda event: (-scores[int(event["id"])], event["start_date"], int(event["id"])))
    for event in promoted[:promoted_count]:
        add(event, "promoted", f"active caller-supplied promo festival: {event.get('festival')}")

    random_pool = [event for event in eligible if int(event["id"]) not in used]
    random_pool.sort(key=lambda event: stable_key(local_date, int(event["id"])))
    for event in random_pool[:random_count]:
        add(event, "random", "stable daily SHA-1 rotation; not nondeterministic random")

    expected = popular_count + promoted_count + random_count
    if len(rows) != expected:
        raise RuntimeError(f"selection underfilled: expected={expected} selected={len(rows)}")
    return {
        "schema_version": 1,
        "research_only": True,
        "generated_at": datetime.now(TZ).isoformat(),
        "local_date": local_date,
        "timezone": str(TZ),
        "eligible_count": len(eligible),
        "requested_mix": {"popular": popular_count, "promoted": promoted_count, "random": random_count},
        "promo_contract": {
            "source": "explicit caller input; selector never mutates promo campaigns",
            "festivals": sorted(promo_festivals),
        },
        "events": rows,
    }


def media_available(url: str) -> bool:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "events-bot-service-share-research/1.0", "Range": "bytes=0-1023"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return 200 <= int(response.status) < 400 and bool(response.read(32))
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-json", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--local-date", default=datetime.now(TZ).date().isoformat())
    parser.add_argument("--promo-festival", action="append", default=[])
    parser.add_argument("--popular-count", type=int, default=3)
    parser.add_argument("--promoted-count", type=int, default=2)
    parser.add_argument("--random-count", type=int, default=3)
    parser.add_argument("--preflight-media", action="store_true")
    args = parser.parse_args()
    raw = json.loads(Path(args.events_json).read_text())
    events = raw.get("events", raw) if isinstance(raw, dict) else raw
    blocked: set[int] = set()
    while True:
        candidates = [event for event in events if int(event.get("id") or 0) not in blocked]
        result = select(
            candidates,
            local_date=args.local_date,
            promo_festivals=set(args.promo_festival),
            popular_count=args.popular_count,
            promoted_count=args.promoted_count,
            random_count=args.random_count,
        )
        if not args.preflight_media:
            break
        failed = [row for row in result["events"] if not media_available(str(row["image_url"]))]
        if not failed:
            result["media_preflight"] = {"checked": len(result["events"]), "failed_event_ids": sorted(blocked)}
            break
        blocked.update(int(row["event_id"]) for row in failed)
        print(f"media preflight rejected ids={sorted(int(row['event_id']) for row in failed)}", flush=True)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "output": str(output), "ids": [row["event_id"] for row in result["events"]]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
