#!/usr/bin/env python3
"""Build an offline MVP-0 event_detail_related probe report.

The script is intentionally cheap and provider-free. It accepts a real catalog
sample (list or {sample: [...]}) and produces deterministic artifacts that can be
stored under artifacts/codex/static-personalization/probe-YYYY-MM-DD/:

- event_sample.json
- related_static_candidates.json
- persona_eval_report.md
- taxonomy_mapping_report.md
- cost_latency_report.md
- probe_report.json

It does not call LLMs or embeddings. Semantic embeddings can be evaluated later
against the same output as `semantic_related_v1`.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import html
import json
from pathlib import Path
import re
import sqlite3
import time
from typing import Any

TAG_SYNONYMS: dict[str, tuple[str, ...]] = {
    "concert": ("концерт", "оркестр", "вокал", "гитара", "орган", "музык", "рок"),
    "jazz": ("джаз",),
    "theatre": ("театр", "спектакль", "сцен", "постанов", "драма", "балет"),
    "kids": ("детск", "дети", "ребён", "ребен", "мульт", "игруш"),
    "workshop": ("мастер-класс", "занят", "роспис", "создани", "воркшоп"),
    "lecture": ("лекция", "семинар", "обсуждени", "встреча"),
    "cinema": ("кино", "кинопоказ", "фильм"),
    "excursion": ("экскурс", "маршрут", "закулись", "прогулк"),
    "festival": ("фестиваль", "ярмарка", "праздник"),
    "exhibition": ("выстав", "экспозици"),
    "free": ("бесплат", "свободный вход"),
    "evening": ("18:", "19:", "20:", "21:", "вечер"),
    "tourist": ("музей", "замок", "коса", "истори", "светлогорск", "зеленоградск"),
}

PERSONAS: list[dict[str, Any]] = [
    {
        "persona_key": "mobile_jazz_concerts_negative_kids",
        "viewport_class": "mobile",
        "presentation": "mobile_related",
        "positive_tags": {"concert": 0.8, "jazz": 0.7, "evening": 0.3},
        "negative_interest_tags": {"kids": 0.9, "workshop": 0.4},
    },
    {
        "persona_key": "desktop_theatre_related",
        "viewport_class": "desktop",
        "presentation": "desktop_related",
        "positive_tags": {"theatre": 0.85, "evening": 0.25},
        "negative_interest_tags": {"kids": 0.6, "workshop": 0.3},
    },
    {
        "persona_key": "mobile_free_tourist",
        "viewport_class": "mobile",
        "presentation": "mobile_related",
        "positive_tags": {"free": 0.8, "tourist": 0.55, "excursion": 0.45},
        "negative_interest_tags": {"sold_out": 1.0},
    },
]

TYPE_TO_TAG = {
    "концерт": "concert",
    "спектакль": "theatre",
    "балет": "theatre",
    "лекция": "lecture",
    "встреча": "lecture",
    "мастер-класс": "workshop",
    "кинопоказ": "cinema",
    "экскурсия": "excursion",
    "фестиваль": "festival",
    "ярмарка": "festival",
    "выставка": "exhibition",
}


@dataclass(frozen=True)
class EventFeature:
    event_id: int
    title: str
    event_type: str
    city: str
    venue_name: str
    date: str
    time: str
    ticket_status: str
    is_free: bool
    status: str
    tags: frozenset[str]
    quality_warnings: tuple[str, ...]


SQLITE_EVENT_COLUMNS = (
    "id",
    "title",
    "description",
    "date",
    "time",
    "location_name",
    "city",
    "event_type",
    "is_free",
    "search_digest",
    "ticket_status",
)


def _text(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "")
        for key in ("title", "event_type", "location_name", "venue_name", "digest", "search_digest", "description", "time", "ticket_status")
    ).lower()


def _event_id(row: dict[str, Any]) -> int | None:
    raw = row.get("event_id", row.get("id"))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def infer_feature(row: dict[str, Any]) -> EventFeature | None:
    eid = _event_id(row)
    if eid is None:
        return None
    text = _text(row)
    tags: set[str] = set()
    for tag, needles in TAG_SYNONYMS.items():
        if any(needle in text for needle in needles):
            tags.add(tag)
    event_type = str(row.get("event_type") or row.get("type") or "").lower()
    for needle, tag in TYPE_TO_TAG.items():
        if needle in event_type:
            tags.add(tag)
    if row.get("is_free"):
        tags.add("free")
    ticket_status = str(row.get("ticket_status") or "").lower()
    if ticket_status == "sold_out":
        tags.add("sold_out")
    warnings: list[str] = []
    digest = str(row.get("digest") or row.get("search_digest") or row.get("description") or "").strip()
    if len(digest) < 40:
        warnings.append("weak_description")
    if not (row.get("city") or row.get("location_city")):
        warnings.append("location_ambiguous")
    if event_type and "кин" in event_type and {"concert", "theatre"} & tags:
        warnings.append("type_description_mismatch")
    if not (row.get("time") or row.get("starts_at")):
        warnings.append("missing_time")
    return EventFeature(
        event_id=eid,
        title=str(row.get("title") or "")[:180],
        event_type=str(row.get("event_type") or row.get("type") or ""),
        city=str(row.get("city") or row.get("location_city") or ""),
        venue_name=str(row.get("venue_name") or row.get("location_name") or ""),
        date=str(row.get("date") or row.get("starts_at") or ""),
        time=str(row.get("time") or ""),
        ticket_status=ticket_status,
        is_free=bool(row.get("is_free")),
        status=str(row.get("status") or "active"),
        tags=frozenset(tags),
        quality_warnings=tuple(warnings),
    )


def load_input_rows(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("sample") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        raise ValueError("input must be a JSON list or object with sample[]")
    return [dict(row) for row in rows[:limit] if isinstance(row, dict)]


def load_sqlite_rows(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        columns = {row[1] for row in con.execute("PRAGMA table_info(event)")}
        if not columns:
            raise ValueError("SQLite DB must contain event table")
        selected = [column for column in SQLITE_EVENT_COLUMNS if column in columns]
        if "id" not in selected or "title" not in selected:
            raise ValueError("event table must contain at least id and title")
        sql = f"SELECT {', '.join(selected)} FROM event ORDER BY date, time, id"
        if limit is not None:
            sql += " LIMIT ?"
            rows = con.execute(sql, (limit,)).fetchall()
        else:
            rows = con.execute(sql).fetchall()
    finally:
        con.close()
    result = [dict(row) for row in rows]
    for row in result:
        row.setdefault("status", "active")
    return result


def load_events(path: Path, limit: int | None = None) -> list[EventFeature]:
    events = events_from_rows(load_input_rows(path, limit))
    return events


def events_from_rows(rows: list[dict[str, Any]]) -> list[EventFeature]:
    return [feature for row in rows if isinstance(row, dict) for feature in [infer_feature(row)] if feature]


def jaccard(left: set[str] | frozenset[str], right: set[str] | frozenset[str]) -> float:
    if not left and not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))


def same_time_bucket(left: EventFeature, right: EventFeature) -> bool:
    def bucket(value: str) -> str:
        if re.search(r"\b(18|19|20|21|22)[:.]", value):
            return "evening"
        if re.search(r"\b(10|11|12|13|14|15|16|17)[:.]", value):
            return "day"
        return ""
    return bool(bucket(left.time) and bucket(left.time) == bucket(right.time))


def base_similarity(current: EventFeature, candidate: EventFeature) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    tag_score = jaccard(current.tags, candidate.tags)
    score += 0.50 * tag_score
    if current.event_type and current.event_type == candidate.event_type:
        score += 0.18
        reasons.append(f"same_type:{candidate.event_type}")
    if current.city and current.city == candidate.city:
        score += 0.14
        reasons.append("same_city")
    if current.venue_name and current.venue_name == candidate.venue_name:
        score += 0.06
        reasons.append("same_venue")
    if same_time_bucket(current, candidate):
        score += 0.04
        reasons.append("same_time_bucket")
    for tag in sorted(current.tags & candidate.tags)[:4]:
        reasons.append(f"tag:{tag}")
    if candidate.ticket_status == "sold_out" or "sold_out" in candidate.tags:
        score -= 0.12
        reasons.append("sold_out_downrank")
    return round(max(0.0, min(1.0, score)), 6), reasons


def event_to_public(event: EventFeature) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "title": event.title,
        "city": event.city,
        "venue_name": event.venue_name,
        "date": event.date,
        "time": event.time,
        "event_type": event.event_type,
        "normalized_tags": sorted(event.tags),
        "status": event.status,
        "ticket_status": event.ticket_status,
        "is_free": event.is_free,
    }


def apply_event_type_diversity(rows: list[dict[str, Any]], *, window: int = 10, max_per_type: int = 5) -> list[dict[str, Any]]:
    """Keep the first visible window from being dominated by one event type.

    This is a deterministic probe guardrail, not a semantic repair. If enough
    alternatives exist after relevance sorting, overflow items of the same type
    are delayed until after the first window.
    """
    selected: list[dict[str, Any]] = []
    delayed: list[dict[str, Any]] = []
    type_counts: Counter[str] = Counter()
    for row in rows:
        event_type = str(row.get("event_type") or "unknown")
        if len(selected) < window and type_counts[event_type] >= max_per_type:
            delayed.append(row)
            continue
        selected.append(row)
        if len(selected) <= window:
            type_counts[event_type] += 1
    return selected + delayed


def static_related_for(current: EventFeature, events: list[EventFeature], top_k: int = 12) -> dict[str, Any]:
    rows = []
    for candidate in events:
        if candidate.event_id == current.event_id:
            continue
        if candidate.status == "cancelled":
            continue
        score, reasons = base_similarity(current, candidate)
        if score <= 0:
            continue
        rows.append({
            "event_id": candidate.event_id,
            "base_similarity": score,
            "reason_codes": reasons,
            "event_type": candidate.event_type,
            "city": candidate.city,
            "tags": sorted(candidate.tags),
            "ticket_status": candidate.ticket_status,
            "event": event_to_public(candidate),
        })
    rows.sort(key=lambda row: (-row["base_similarity"], row["event_id"]))
    rows = apply_event_type_diversity(rows)
    return {"event_id": current.event_id, "related_static": rows[:top_k]}


def local_score(item: dict[str, Any], persona: dict[str, Any]) -> float:
    tags = set(item.get("tags") or [])
    score = 0.45 * float(item.get("base_similarity") or 0)
    score += 0.20 * min(1.0, sum(float(persona["positive_tags"].get(tag, 0)) for tag in tags))
    score -= 0.90 * min(1.0, sum(float(persona["negative_interest_tags"].get(tag, 0)) for tag in tags))
    if item.get("ticket_status") == "sold_out" or "sold_out" in tags:
        score -= 0.25
    return round(score, 6)


def eval_persona(related: dict[str, Any], persona: dict[str, Any], top_k: int) -> dict[str, Any]:
    ranked = []
    for item in related["related_static"]:
        scored = dict(item)
        scored["personal_score"] = local_score(item, persona)
        ranked.append(scored)
    ranked.sort(key=lambda item: (-item["personal_score"], -item["base_similarity"], item["event_id"]))
    ranked = apply_event_type_diversity(ranked)
    top = ranked[:top_k]
    failures: list[str] = []
    if any(item["event_id"] == related["event_id"] for item in top):
        failures.append("current_event_in_top")
    if any(set(item.get("tags") or []) & set(persona["negative_interest_tags"]) for item in top[:5]):
        failures.append("negative_interest_in_top_5")
    type_counts = Counter(str(item.get("event_type") or "unknown") for item in top[:10])
    if type_counts and max(type_counts.values()) > 5:
        failures.append("event_type_diversity_cap_failed")
    return {
        "persona_key": persona["persona_key"],
        "viewport_class": persona["viewport_class"],
        "presentation": persona["presentation"],
        "pass": not failures,
        "failed_checks": failures,
        "top_k": top,
    }


def taxonomy_report(events: list[EventFeature]) -> dict[str, Any]:
    warning_counts = Counter(w for event in events for w in event.quality_warnings)
    tag_counts = Counter(tag for event in events for tag in event.tags)
    unmapped = [event.event_id for event in events if not event.tags]
    return {
        "events_total": len(events),
        "schema_valid_events": len(events),
        "unmapped_events": unmapped,
        "unmapped_count": len(unmapped),
        "quality_warning_counts": dict(sorted(warning_counts.items())),
        "top_tags": dict(tag_counts.most_common(30)),
    }


def markdown_persona_report(results: list[dict[str, Any]]) -> str:
    lines = ["# Persona Eval Report", "", "| Persona | Viewport | Pass | Failed checks | Top event IDs |", "| --- | --- | --- | --- | --- |"]
    for result in results:
        top_ids = ", ".join(str(item["event_id"]) for item in result["top_k"])
        failed = ", ".join(result["failed_checks"]) or "—"
        lines.append(f"| {result['persona_key']} | {result['viewport_class']} | {result['pass']} | {failed} | {top_ids} |")
    lines.append("")
    return "\n".join(lines)


def markdown_taxonomy_report(report: dict[str, Any]) -> str:
    lines = ["# Taxonomy Mapping Report", "", f"Events total: {report['events_total']}", f"Schema-valid events: {report['schema_valid_events']}", f"Unmapped events: {report['unmapped_count']}", "", "## Quality warnings", ""]
    for key, value in report["quality_warning_counts"].items():
        lines.append(f"- {key}: {value}")
    if not report["quality_warning_counts"]:
        lines.append("—")
    lines.extend(["", "## Top tags", ""])
    for key, value in report["top_tags"].items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def build_probe_from_rows(rows: list[dict[str, Any]], *, current_event_id: int | None = None, top_k: int = 12) -> dict[str, Any]:
    start = time.perf_counter()
    events = events_from_rows(rows)
    if not events:
        raise ValueError("no usable events in sample")
    current = next((event for event in events if event.event_id == current_event_id), events[0])
    related = static_related_for(current, events, top_k=top_k)
    persona_results = [eval_persona(related, persona, min(10, top_k)) for persona in PERSONAS]
    taxonomy = taxonomy_report(events)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 3)
    cancelled_ids = {event.event_id for event in events if event.status == "cancelled"}
    deterministic_checks = {
        "current_event_not_in_related": all(item["event_id"] != current.event_id for item in related["related_static"]),
        "cancelled_not_in_related": all(item["event_id"] not in cancelled_ids for item in related["related_static"]),
        "has_related_candidates": bool(related["related_static"]),
        "personas_pass": all(result["pass"] for result in persona_results),
    }
    return {
        "surface": "event_detail_related",
        "layout_mode": "module",
        "current_event_id": current.event_id,
        "current_event": event_to_public(current),
        "rankers_compared": ["static_related_v1", "local_related_rerank_v1"],
        "semantic_related_v1": "planned_eval_only_not_hot_path",
        "events_total": len(events),
        "related_static_candidates": related,
        "taxonomy_report": taxonomy,
        "persona_eval": persona_results,
        "deterministic_checks": deterministic_checks,
        "ok": all(deterministic_checks.values()),
        "cost_latency_report": {
            "provider_calls": 0,
            "estimated_provider_cost": 0,
            "elapsed_ms": elapsed_ms,
            "async_safe": True,
        },
    }


def build_probe(input_path: Path, *, current_event_id: int | None = None, limit: int | None = None, top_k: int = 12) -> dict[str, Any]:
    return build_probe_from_rows(load_input_rows(input_path, limit), current_event_id=current_event_id, top_k=top_k)


def static_page_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "surface": "event_detail_related",
        "layout_mode": "module",
        "algorithm_id": "static_related_v1",
        "current_event": report.get("current_event"),
        "related_static": report["related_static_candidates"]["related_static"],
    }


def render_static_related_html(report: dict[str, Any]) -> str:
    payload = static_page_payload(report)
    cards: list[str] = []
    for item in payload["related_static"]:
        event = item.get("event") or {}
        event_id = html.escape(str(event.get("event_id") or item.get("event_id") or ""))
        title = html.escape(str(event.get("title") or f"Событие {event_id}"))
        city = html.escape(str(event.get("city") or ""))
        venue = html.escape(str(event.get("venue_name") or ""))
        date = html.escape(" ".join(str(part or "") for part in (event.get("date"), event.get("time"))).strip())
        reasons = html.escape(", ".join(str(reason) for reason in item.get("reason_codes") or []))
        meta = " · ".join(part for part in (city, date, venue) if part)
        cards.append(
            f'    <article class="related-card" data-event-id="{event_id}">\n'
            f"      <h3>{title}</h3>\n"
            f'      <p class="related-meta">{meta}</p>\n'
            f'      <p class="reason-codes">{reasons}</p>\n'
            "    </article>"
        )
    if not cards:
        cards.append('    <p class="related-empty">Похожих событий пока нет.</p>')
    return (
        '<section class="event-detail-related" data-surface="event_detail_related" '
        'data-layout-mode="module" data-algorithm-id="static_related_v1">\n'
        "  <h2>Похожие события</h2>\n"
        '  <div class="related-block">\n'
        + "\n".join(cards)
        + "\n  </div>\n"
        "</section>\n"
    )


def write_outputs(report: dict[str, Any], output_dir: Path, sample_data: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "event_sample.json").write_text(json.dumps(sample_data, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "related_static_candidates.json").write_text(json.dumps(report["related_static_candidates"], ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "event_detail_related_payload.json").write_text(json.dumps(static_page_payload(report), ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "event_detail_related_static.html").write_text(render_static_related_html(report), encoding="utf-8")
    (output_dir / "probe_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "persona_eval_report.md").write_text(markdown_persona_report(report["persona_eval"]), encoding="utf-8")
    (output_dir / "taxonomy_mapping_report.md").write_text(markdown_taxonomy_report(report["taxonomy_report"]), encoding="utf-8")
    (output_dir / "cost_latency_report.md").write_text(
        "# Cost / Latency Report\n\n"
        f"Provider calls: {report['cost_latency_report']['provider_calls']}\n\n"
        f"Estimated provider cost: {report['cost_latency_report']['estimated_provider_cost']}\n\n"
        f"Elapsed: {report['cost_latency_report']['elapsed_ms']} ms\n\n"
        "The MVP-0 deterministic probe is async-safe and has no online LLM dependency.\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path)
    parser.add_argument("--sqlite-db", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--current-event-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--top-k", type=int, default=12)
    args = parser.parse_args()
    if bool(args.input) == bool(args.sqlite_db):
        parser.error("provide exactly one of --input or --sqlite-db")
    rows = load_sqlite_rows(args.sqlite_db, args.limit) if args.sqlite_db else load_input_rows(args.input, args.limit)
    report = build_probe_from_rows(rows, current_event_id=args.current_event_id, top_k=args.top_k)
    write_outputs(report, args.output, {"sample": rows})
    print(json.dumps({"ok": report["ok"], "events_total": report["events_total"], "current_event_id": report["current_event_id"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
