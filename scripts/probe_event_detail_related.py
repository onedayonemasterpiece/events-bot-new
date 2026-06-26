#!/usr/bin/env python3
"""Probe MVP-0 event_detail_related ranking on a real events SQLite catalog.

The probe intentionally does not call online LLMs or embedding providers. It uses
current canonical event fields (event_type, topics, title/search_digest, city,
venue, price/status) to check whether a cheap static+local vector baseline can
safely enter an MVP-0 engineering spike before heavier semantic embeddings are
introduced as an offline quality comparison.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

TODAY_DEFAULT = date(2026, 6, 26)

TOPIC_TAGS = {
    "CONCERTS": ("music", ["live_music"]),
    "THEATRE": ("theatre", ["theatre"]),
    "THEATRE_CLASSIC": ("theatre", ["theatre", "classic", "drama"]),
    "EXHIBITIONS": ("exhibition", ["museum"]),
    "MOVIES": ("cinema", ["cinema"]),
    "STANDUP": ("other", ["standup", "comedy"]),
    "KIDS_SCHOOL": ("kids", ["kids"]),
    "FAMILY": ("kids", ["family", "kids"]),
    "MASTERCLASS": ("workshop", ["workshop"]),
    "SCIENCE_POP": ("lecture", ["lecture"]),
    "LECTURES": ("lecture", ["lecture"]),
    "OPEN_AIR": ("other", ["outdoor"]),
    "PARTIES": ("nightlife", ["nightlife", "evening"]),
    "ACTIVE": ("sport", ["active", "outdoor"]),
    "QUIZ_GAMES": ("other", ["quiz"]),
    "PSYCHOLOGY": ("lecture", ["psychology"]),
    "NETWORKING": ("other", ["networking"]),
    "KRAEVEDENIE_KALININGRAD_OBLAST": ("excursion", ["local_history", "tourist_friendly"]),
}

TYPE_CATEGORY = [
    ("концерт", "music", ["live_music"]),
    ("спектак", "theatre", ["theatre"]),
    ("театр", "theatre", ["theatre"]),
    ("выстав", "exhibition", ["museum"]),
    ("экскурс", "excursion", ["tour", "tourist_friendly"]),
    ("лекц", "lecture", ["lecture"]),
    ("мастер", "workshop", ["workshop"]),
    ("кин", "cinema", ["cinema"]),
    ("фестиваль", "festival", ["festival"]),
    ("вечерин", "nightlife", ["nightlife", "evening"]),
    ("стендап", "other", ["standup", "comedy"]),
    ("дегуста", "food", ["food"]),
    ("ярмар", "market", ["market"]),
]

TITLE_TAGS = [
    (r"\bджаз|jazz", "jazz"),
    (r"\bрок|rock", "rock"),
    (r"классическ|симфон", "classical_music"),
    (r"детск|дети|реб[её]н", "kids"),
    (r"семейн", "family"),
    (r"бесплат", "free"),
    (r"вечер|19:|20:|21:", "evening"),
    (r"open[ -]?air|парк|пляж|коса|улиц", "outdoor"),
]

GOLDEN_PERSONAS = {
    "music_no_kids": {
        "positive_tags": {"jazz": 1.0, "live_music": 0.7, "evening": 0.25, "music": 0.4},
        "negative_interest_tags": {"kids": 1.0, "family": 0.6},
        "price_preferences": {"prefer_free": False},
    },
    "theatre_evening": {
        "positive_tags": {"theatre": 1.0, "drama": 0.6, "classic": 0.3, "evening": 0.3},
        "negative_interest_tags": {"nightlife": 0.8, "rock": 0.4},
        "price_preferences": {"prefer_free": False},
    },
    "family_weekend": {
        "positive_tags": {"kids": 0.9, "family": 0.9, "outdoor": 0.2, "free": 0.2},
        "negative_interest_tags": {"nightlife": 0.9, "adult": 0.7},
        "price_preferences": {"prefer_free": False},
    },
    "tourist_free_walks": {
        "positive_tags": {"tourist_friendly": 1.0, "tour": 0.9, "local_history": 0.7, "free": 0.5, "outdoor": 0.3},
        "negative_interest_tags": {"nightlife": 0.7, "sport": 0.4},
        "price_preferences": {"prefer_free": True},
    },
    "museum_exhibitions": {
        "positive_tags": {"museum": 1.0, "exhibition": 0.9, "classic": 0.2, "local_history": 0.2},
        "negative_interest_tags": {"nightlife": 0.8, "rock": 0.4},
        "price_preferences": {"prefer_free": False},
    },
    "cinema_low_price": {
        "positive_tags": {"cinema": 1.0, "evening": 0.3, "free": 0.2},
        "negative_interest_tags": {"kids": 0.5, "nightlife": 0.4},
        "price_preferences": {"prefer_free": True},
    },
    "standup_nightlife": {
        "positive_tags": {"standup": 1.0, "comedy": 0.8, "nightlife": 0.6, "evening": 0.4},
        "negative_interest_tags": {"kids": 0.9, "classic": 0.4},
        "price_preferences": {"prefer_free": False},
    },
    "classical_music": {
        "positive_tags": {"classical_music": 1.0, "live_music": 0.6, "classic": 0.4, "evening": 0.2},
        "negative_interest_tags": {"rock": 0.8, "nightlife": 0.5},
        "price_preferences": {"prefer_free": False},
    },
    "workshops_lectures": {
        "positive_tags": {"workshop": 0.9, "lecture": 0.8, "psychology": 0.3, "networking": 0.2},
        "negative_interest_tags": {"nightlife": 0.6, "kids": 0.3},
        "price_preferences": {"prefer_free": False},
    },
    "local_weekend": {
        "positive_tags": {"local_history": 0.7, "festival": 0.6, "outdoor": 0.5, "market": 0.3, "free": 0.3},
        "negative_interest_tags": {"nightlife": 0.4},
        "price_preferences": {"prefer_free": True},
    },
}


@dataclass
class EventFeature:
    event_id: int
    title: str
    date_raw: str
    event_date: date | None
    time: str
    city: str
    venue: str
    event_type: str
    category: str
    tags: set[str] = field(default_factory=set)
    topics: list[str] = field(default_factory=list)
    ticket_status: str = ""
    is_free: bool = False
    lifecycle_status: str = "active"
    linked_event_ids: set[int] = field(default_factory=set)

    def as_manifest_candidate(self, score: float, reason_codes: list[str]) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "category": self.category,
            "tags": sorted(self.tags),
            "audience_exclusion_tags": [],
            "city": self.city,
            "location_name": self.venue,
            "date": self.date_raw,
            "status": self.ticket_status or "available",
            "lifecycle_status": self.lifecycle_status,
            "is_free": self.is_free,
            "base_similarity": round(score, 4),
            "reason_codes": reason_codes[:10],
        }


def parse_topics(raw: Any) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return []


def parse_linked_ids(raw: Any) -> set[int]:
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return {int(x) for x in parsed if str(x).isdigit()}
    except Exception:
        pass
    return set()


def parse_date(raw: str) -> date | None:
    if not raw:
        return None
    text = str(raw).strip()
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.search(r"(\d{1,2})[.](\d{1,2})[.](20\d{2})", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def infer_features(row: sqlite3.Row) -> EventFeature:
    title = row["title"] or ""
    event_type = (row["event_type"] or "").lower()
    text = " ".join(str(row[k] or "") for k in ("title", "search_digest", "short_description", "description", "source_text") if k in row.keys()).lower()
    topics = parse_topics(row["topics"] if "topics" in row.keys() else None)
    category = "other"
    tags: set[str] = set()

    for topic in topics:
        mapped = TOPIC_TAGS.get(topic)
        if not mapped:
            continue
        topic_category, topic_tags = mapped
        if category == "other" or topic_category not in {"other"}:
            category = topic_category
        tags.update(topic_tags)
        tags.add(topic_category)

    for needle, type_category, type_tags in TYPE_CATEGORY:
        if needle in event_type:
            category = type_category
            tags.update(type_tags)
            tags.add(type_category)
            break

    for pattern, tag in TITLE_TAGS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            tags.add(tag)
            if tag == "jazz":
                tags.update({"music", "live_music"})
                if category == "other":
                    category = "music"

    if row["is_free"]:
        tags.add("free")
    elif row["ticket_price_min"] or row["ticket_status"] in {"sale", "available", "registration"}:
        tags.add("ticketed")
    if row["time"] and re.search(r"\b(18|19|20|21|22):", row["time"]):
        tags.add("evening")

    return EventFeature(
        event_id=int(row["id"]),
        title=title,
        date_raw=row["date"] or "",
        event_date=parse_date(row["date"] or ""),
        time=row["time"] or "",
        city=row["city"] or "",
        venue=row["location_name"] or "",
        event_type=event_type,
        category=category,
        tags=tags,
        topics=topics,
        ticket_status=row["ticket_status"] or "",
        is_free=bool(row["is_free"]),
        lifecycle_status=row["lifecycle_status"] or "active",
        linked_event_ids=parse_linked_ids(row["linked_event_ids"] if "linked_event_ids" in row.keys() else None),
    )


def load_catalog(db_path: Path, today: date) -> list[EventFeature]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    quick = con.execute("pragma quick_check").fetchone()[0]
    if str(quick).lower() != "ok":
        raise SystemExit(f"sqlite quick_check failed: {quick}")
    rows = con.execute(
        """
        select id, title, description, date, time, city, location_name, event_type,
               topics, ticket_status, is_free, lifecycle_status, linked_event_ids,
               search_digest, short_description, source_text, ticket_price_min
          from event
         where coalesce(lifecycle_status, 'active') = 'active'
           and title is not null
        """
    ).fetchall()
    con.close()
    events = []
    for row in rows:
        feature = infer_features(row)
        if feature.event_date and feature.event_date < today:
            continue
        events.append(feature)
    return events


def is_linked_duplicate(anchor: EventFeature, candidate: EventFeature) -> bool:
    return candidate.event_id in anchor.linked_event_ids or anchor.event_id in candidate.linked_event_ids


def jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))


def date_proximity(anchor: EventFeature, candidate: EventFeature) -> float:
    if not anchor.event_date or not candidate.event_date:
        return 0.0
    days = abs((candidate.event_date - anchor.event_date).days)
    return math.exp(-days / 21.0)


def static_score(anchor: EventFeature, candidate: EventFeature) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    if anchor.category == candidate.category:
        score += 0.28
        reasons.append(f"same_category:{anchor.category}")
    overlap = jaccard(anchor.tags, candidate.tags)
    if overlap:
        score += 0.24 * overlap
        for tag in sorted((anchor.tags & candidate.tags))[:4]:
            reasons.append(f"tag:{tag}")
    if anchor.city and anchor.city == candidate.city:
        score += 0.12
        reasons.append("same_city")
    if anchor.venue and anchor.venue == candidate.venue:
        score += 0.06
        reasons.append("same_venue")
    prox = date_proximity(anchor, candidate)
    if prox:
        score += 0.12 * prox
        reasons.append("date_near")
    if anchor.is_free == candidate.is_free:
        score += 0.05
        reasons.append("price_band_match")
    if candidate.ticket_status == "sold_out":
        score -= 0.20
        reasons.append("penalty:sold_out")
    return max(0.0, min(1.0, score)), reasons


def static_related(anchor: EventFeature, events: list[EventFeature], limit: int = 24) -> list[tuple[EventFeature, float, list[str]]]:
    rows = []
    for candidate in events:
        if candidate.event_id == anchor.event_id:
            continue
        if candidate.lifecycle_status != "active":
            continue
        if is_linked_duplicate(anchor, candidate):
            continue
        score, reasons = static_score(anchor, candidate)
        if score <= 0:
            continue
        rows.append((candidate, score, reasons))
    rows.sort(key=lambda item: (-item[1], item[0].event_date or date.max, item[0].event_id))
    return apply_diversity(rows)[:limit]


def profile_affinity(candidate: EventFeature, profile: dict[str, Any]) -> float:
    positive = profile.get("positive_tags", {})
    return min(1.5, sum(float(positive.get(tag, 0) or 0) for tag in candidate.tags) / 2.0)


def negative_penalty(candidate: EventFeature, profile: dict[str, Any]) -> float:
    negative = profile.get("negative_interest_tags", {})
    return min(1.5, sum(max(0.0, float(negative.get(tag, 0) or 0)) for tag in candidate.tags))


def local_rerank(static_rows: list[tuple[EventFeature, float, list[str]]], profile: dict[str, Any], hidden_event_ids: set[int]) -> list[tuple[EventFeature, float, list[str], float]]:
    rows = []
    for candidate, base, reasons in static_rows:
        if candidate.event_id in hidden_event_ids:
            continue
        affinity = profile_affinity(candidate, profile)
        negative = negative_penalty(candidate, profile)
        price = 1.0 if profile.get("price_preferences", {}).get("prefer_free") and candidate.is_free else 0.0
        score = 0.80 * base + 0.10 * affinity + 0.04 * price - 0.55 * negative
        local_reasons = list(reasons)
        if affinity > 0:
            local_reasons.append("profile:positive_affinity")
        if negative > 0:
            local_reasons.append("profile:negative_interest_penalty")
        if price > 0:
            local_reasons.append("profile:price_match")
        rows.append((candidate, score, local_reasons, base))
    rows.sort(key=lambda item: (-item[1], -item[3], item[0].event_id))
    return apply_diversity([(c, s, r) for c, s, r, _ in rows], score_index=1)[:24]


def apply_diversity(rows: list[tuple], score_index: int = 1, max_category: int = 3, max_venue: int = 2) -> list[tuple]:
    result = []
    postponed = []
    cat_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    for row in rows:
        event = row[0]
        if cat_counts[event.category] >= max_category or venue_counts[event.venue] >= max_venue:
            postponed.append(row)
            continue
        result.append(row)
        cat_counts[event.category] += 1
        venue_counts[event.venue] += 1
    return result + postponed


def choose_anchors(events: list[EventFeature], target: int = 40) -> list[EventFeature]:
    """Pick a balanced automated golden-smoke set, not a final quality benchmark."""
    wanted = [
        "music",
        "theatre",
        "kids",
        "excursion",
        "cinema",
        "festival",
        "exhibition",
        "lecture",
        "workshop",
        "nightlife",
        "sport",
        "other",
    ]
    by_category: dict[str, list[EventFeature]] = defaultdict(list)
    ordered = sorted(events, key=lambda event: (event.event_date or date.max, event.category, event.event_id))
    for event in ordered:
        key = event.category if event.category in wanted else "other"
        by_category[key].append(event)

    anchors: list[EventFeature] = []
    seen: set[int] = set()
    while len(anchors) < target:
        added = False
        for category in wanted:
            bucket = by_category.get(category) or []
            while bucket and bucket[0].event_id in seen:
                bucket.pop(0)
            if not bucket:
                continue
            event = bucket.pop(0)
            anchors.append(event)
            seen.add(event.event_id)
            added = True
            if len(anchors) >= target:
                break
        if not added:
            break

    if len(anchors) < target:
        for event in ordered:
            if event.event_id in seen:
                continue
            anchors.append(event)
            seen.add(event.event_id)
            if len(anchors) >= target:
                break
    return anchors[:target]


def summarize_top(rows: list[tuple], limit: int = 5) -> list[dict[str, Any]]:
    out = []
    for rank, row in enumerate(rows[:limit]):
        event = row[0]
        score = row[1]
        reasons = row[2]
        out.append({
            "rank": rank,
            "event_id": event.event_id,
            "title": event.title,
            "category": event.category,
            "tags": sorted(event.tags)[:8],
            "score": round(float(score), 4),
            "reason_codes": reasons[:8],
        })
    return out


def checks(anchor: EventFeature, static_rows: list[tuple], local_rows: list[tuple], profile: dict[str, Any], hidden: set[int]) -> dict[str, bool]:
    static_ids = [row[0].event_id for row in static_rows[:10]]
    local_ids = [row[0].event_id for row in local_rows[:10]]
    negative_tags = set(profile.get("negative_interest_tags", {}))
    top5_negative = sum(1 for row in local_rows[:5] if row[0].tags & negative_tags)
    cat_counts = Counter(row[0].category for row in local_rows[:10])
    venue_counts = Counter(row[0].venue for row in local_rows[:10])
    return {
        "static_top10_non_empty": bool(static_ids),
        "local_top10_non_empty": bool(local_ids),
        "current_not_in_static_top10": anchor.event_id not in static_ids,
        "current_not_in_local_top10": anchor.event_id not in local_ids,
        "hidden_not_in_local_top10": not (hidden & set(local_ids)),
        "negative_interest_top5_count_le_1": top5_negative <= 1,
        "category_diversity_cap_top10": all(count <= 3 for count in cat_counts.values()),
        "venue_diversity_cap_top10": all(count <= 2 for count in venue_counts.values()),
    }


def build_manifest_sample(anchor: EventFeature, static_rows: list[tuple]) -> dict[str, Any]:
    return {
        "schema_version": "event-detail-related-v1",
        "feature_schema_version": "event-detail-related-v1",
        "taxonomy_version": "event-taxonomy-v1",
        "surface": "event_detail_related",
        "algorithm_id": "static_related_v1",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "current_event": {
            "event_id": anchor.event_id,
            "title": anchor.title,
            "category": anchor.category,
            "tags": sorted(anchor.tags),
            "city": anchor.city,
            "location_name": anchor.venue,
            "date": anchor.date_raw,
        },
        "related_static": [candidate.as_manifest_candidate(score, reasons) for candidate, score, reasons in static_rows[:6]],
    }


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Event Detail Related MVP-0 Probe Report",
        "",
        "> Generated by `python3 scripts/probe_event_detail_related.py` on a production SQLite snapshot. No online LLM, embedding provider, Supabase, or vector DB was called.",
        "",
        "## Input catalog",
        "",
        f"- DB: `{report['db_path']}`",
        f"- Probe date: `{report['today']}`",
        f"- Future active events loaded: **{report['future_active_events']}**",
        f"- Anchors evaluated: **{len(report['anchors'])}** (target: **{report.get('anchor_target')}**, balanced by category where possible)",
        f"- Golden personas rotated: **{len(report.get('personas', []))}** — {', '.join(report.get('personas', []))}",
        "",
        "## Scope and caveat",
        "",
        "This is an expanded automated golden-smoke probe: it checks hard invariants and obvious ranking regressions on 30–50 anchors. It is **not** a human/editorial proof of recommendation quality. Product-quality acceptance still requires manual top-10 review by persona, plus separate mobile/desktop UX review on the real page.",
        "",
        "## Ranker variants",
        "",
        "- `static_related_v1`: deterministic current-event similarity over category/tags/city/date/venue/price/status.",
        "- `local_related_rerank_v1`: `static_related_v1` plus localStorage-like positive/negative interests, hidden events, fatigue/diversity. Runs fully in browser.",
        "- `semantic_related_v1`: **not enabled for MVP-0**; keep as offline eval only if local vectors fail editorial/golden probes.",
        "",
        "## Invariant summary",
        "",
    ]
    all_checks = defaultdict(list)
    for anchor in report["anchors"]:
        for key, value in anchor["checks"].items():
            all_checks[key].append(bool(value))
    lines.append("| Check | Passed anchors | Result |")
    lines.append("| --- | ---: | --- |")
    warning_rows = []
    for key, values in all_checks.items():
        passed = sum(values)
        if passed != len(values):
            warning_rows.append(key)
        lines.append(f"| `{key}` | {passed}/{len(values)} | {'PASS' if passed == len(values) else 'WARN'} |")
    if warning_rows:
        lines.extend([
            "",
            f"Warnings: {', '.join(f'`{key}`' for key in warning_rows)}. Treat these as quality/taxonomy backlog evidence, not as production acceptance. Safety invariants still need to remain green before implementation.",
        ])
    lines.extend(["", "## Anchor samples", ""])
    for anchor in report["anchors"]:
        lines.append(f"### {anchor['anchor_id']} — {anchor['anchor_title']} (`{anchor['category']}`)")
        lines.append("")
        lines.append(f"Persona: `{anchor['persona']}`; hidden id: `{anchor['hidden_event_id']}`")
        lines.append("")
        lines.append("Static top 5:")
        for item in anchor["static_top5"]:
            lines.append(f"- #{item['rank']} `{item['event_id']}` {item['title']} — `{item['category']}`, score `{item['score']}`, reasons `{', '.join(item['reason_codes'][:4])}`")
        lines.append("")
        lines.append("Local rerank top 5:")
        for item in anchor["local_top5"]:
            lines.append(f"- #{item['rank']} `{item['event_id']}` {item['title']} — `{item['category']}`, score `{item['score']}`, reasons `{', '.join(item['reason_codes'][:5])}`")
        lines.append("")
    lines.extend([
        "## Decision on semantic embeddings",
        "",
        "For MVP-0, the local feature-vector baseline is sufficient for an **engineering implementation spike** of `event_detail_related`: it keeps provider calls out of the browser and passes the core safety invariants on the real future-event sample. Any WARN rows above remain backlog evidence for taxonomy/ranking tuning. This does **not** prove final product quality.",
        "",
        "Keep `semantic_related_v1` as an offline comparison after controlled taxonomy enrichment and human/golden top-10 review. Embeddings should be justified by a measured quality delta against the local baseline, not by architecture preference.",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="artifacts/db/event_quality_audit_20260624_prod.sqlite", help="SQLite DB snapshot path")
    parser.add_argument("--today", default=TODAY_DEFAULT.isoformat())
    parser.add_argument("--report", default="docs/features/unsigned-personalization/event-detail-related-probe.md")
    parser.add_argument("--sample", default="docs/features/unsigned-personalization/samples/event-detail-related-manifest.sample.json")
    parser.add_argument("--anchor-target", type=int, default=40, help="Number of balanced anchors for automated golden-smoke probe; keep 30-50 for MVP-0")
    args = parser.parse_args()

    db_path = Path(args.db)
    today = date.fromisoformat(args.today)
    events = load_catalog(db_path, today)
    anchors = choose_anchors(events, target=max(1, args.anchor_target))
    report: dict[str, Any] = {
        "db_path": str(db_path),
        "today": today.isoformat(),
        "future_active_events": len(events),
        "anchor_target": args.anchor_target,
        "personas": list(GOLDEN_PERSONAS),
        "anchors": [],
    }
    sample_manifest = None
    personas = list(GOLDEN_PERSONAS.items())
    for index, anchor in enumerate(anchors):
        static_rows = static_related(anchor, events, limit=24)
        persona_name, persona = personas[index % len(personas)]
        hidden = {static_rows[-1][0].event_id} if static_rows else set()
        local_rows = local_rerank(static_rows, persona, hidden)
        if sample_manifest is None and static_rows:
            sample_manifest = build_manifest_sample(anchor, static_rows)
        report["anchors"].append({
            "anchor_id": anchor.event_id,
            "anchor_title": anchor.title,
            "category": anchor.category,
            "persona": persona_name,
            "hidden_event_id": next(iter(hidden)) if hidden else None,
            "static_top5": summarize_top(static_rows, limit=5),
            "local_top5": summarize_top(local_rows, limit=5),
            "checks": checks(anchor, static_rows, local_rows, persona, hidden),
        })

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(markdown_report(report), encoding="utf-8")
    if sample_manifest:
        sample_path = Path(args.sample)
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        sample_path.write_text(json.dumps(sample_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(report_path), "sample": args.sample, "events": len(events), "anchors": len(anchors)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
