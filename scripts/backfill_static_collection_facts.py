#!/usr/bin/env python3
"""Bounded, resumable backfill for static-collection factual decisions.

The script is deliberately source-bound: it reuses persisted EventSource rows,
the production Smart Update adjudicator, and the same atomic apply contract.
Planning is read-only.  Applying requires an explicit flag and never rewrites
event prose or identity.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from models import Event, EventSource
from smart_event_update import (
    EventCandidate,
    adjudicate_collection_candidate,
    apply_collection_decisions,
    collection_adjudication_input_hash,
)

REASONS = frozenset({"admission", "audience", "people"})
TRUST_RANK = {"official": 4, "high": 3, "medium": 2, "low": 1}


@dataclass(frozen=True)
class PlannedEvent:
    event_id: int
    reasons: tuple[str, ...]
    source_ids: tuple[int, ...]


def _topics(event: Event) -> set[str]:
    raw = event.topics or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = []
    return {str(value or "").strip().upper() for value in raw if str(value or "").strip()}


def event_is_current(event: Event, *, current_date: date) -> bool:
    if str(event.identity_status or "canonical") != "canonical":
        return False
    if event.merged_into_event_id is not None:
        return False
    if str(event.lifecycle_status or "active") != "active" or bool(event.silent):
        return False
    effective_end = str(event.end_date or event.date or "")
    try:
        return date.fromisoformat(effective_end) >= current_date
    except ValueError:
        return False


def route_backfill_reasons(
    event: Event,
    *,
    enabled_reasons: Iterable[str],
    forced: bool = False,
) -> tuple[str, ...]:
    """Return high-recall reasons without making the semantic decision."""

    enabled = REASONS & {str(value).strip().lower() for value in enabled_reasons}
    topics = _topics(event)
    result: set[str] = set()
    decisions = event.collection_decisions if isinstance(event.collection_decisions, dict) else {}
    if "admission" in enabled and (
        forced or bool(event.is_free) or "admission_decision" in decisions
    ):
        result.add("admission")
    if "audience" in enabled and (
        forced
        or bool(topics & {"FAMILY", "KIDS_SCHOOL"})
        or "audience_decision" in decisions
    ):
        result.add("audience")
    if "people" in enabled and (
        forced or "PERSONALITIES" in topics or "people_appearances" in decisions
    ):
        result.add("people")
    return tuple(sorted(result))


def select_sources(
    sources: Iterable[EventSource],
    *,
    maximum: int,
) -> tuple[EventSource, ...]:
    usable = [
        source
        for source in sources
        if source.id and str(source.source_url or "").strip() and str(source.source_text or "").strip()
    ]

    def score(source: EventSource) -> tuple[int, datetime, int, int]:
        imported = source.imported_at or datetime.min.replace(tzinfo=timezone.utc)
        if imported.tzinfo is None:
            imported = imported.replace(tzinfo=timezone.utc)
        return (
            TRUST_RANK.get(str(source.trust_level or "").strip().lower(), 0),
            imported,
            len(str(source.source_text or "")),
            int(source.id or 0),
        )

    return tuple(sorted(usable, key=score, reverse=True)[: max(1, int(maximum))])


def build_candidate(event: Event, source: EventSource, reasons: Iterable[str]) -> EventCandidate:
    return EventCandidate(
        source_type=str(source.source_type or "backfill"),
        source_url=str(source.source_url or ""),
        source_text=str(source.source_text or ""),
        title=event.title,
        date=event.date,
        time=event.time,
        end_date=event.end_date,
        festival=event.festival,
        location_name=event.location_name,
        location_address=event.location_address,
        city=event.city,
        ticket_link=event.ticket_link,
        ticket_price_min=event.ticket_price_min,
        ticket_price_max=event.ticket_price_max,
        ticket_status=getattr(event, "ticket_status", None),
        age_restriction=event.age_restriction,
        event_type=event.event_type,
        is_free=event.is_free,
        search_digest=event.search_digest,
        source_chat_username=source.source_chat_username,
        source_chat_id=source.source_chat_id,
        source_message_id=source.source_message_id,
        trust_level=source.trust_level,
        topics=sorted(_topics(event)),
        collection_adjudication_reasons=[*reasons, "backfill"],
    )


def decision_hash_covers(event: Event, *, reasons: Iterable[str], input_hash: str) -> bool:
    """Return true when every requested decision already accepted this source hash."""

    decisions = event.collection_decisions if isinstance(event.collection_decisions, dict) else {}
    for reason in reasons:
        if reason == "admission":
            item = decisions.get("admission_decision")
            if not isinstance(item, dict) or item.get("input_hash") != input_hash:
                return False
        elif reason == "audience":
            item = decisions.get("audience_decision")
            if not isinstance(item, dict) or item.get("input_hash") != input_hash:
                return False
        elif reason == "people":
            people = decisions.get("people_appearances")
            if not isinstance(people, list) or not people:
                return False
            if any(not isinstance(item, dict) or item.get("input_hash") != input_hash for item in people):
                return False
    return True


async def build_plan(
    db: Database,
    *,
    current_date: date,
    enabled_reasons: Iterable[str],
    event_ids: set[int],
    limit: int,
    max_sources_per_event: int,
) -> list[PlannedEvent]:
    async with db.get_session() as session:
        statement = select(Event).order_by(Event.id)
        if event_ids:
            statement = statement.where(Event.id.in_(event_ids))
        events = list((await session.execute(statement)).scalars())
        plan: list[PlannedEvent] = []
        for event in events:
            if not event.id or not event_is_current(event, current_date=current_date):
                continue
            reasons = route_backfill_reasons(
                event,
                enabled_reasons=enabled_reasons,
                forced=int(event.id) in event_ids,
            )
            if not reasons:
                continue
            sources = list(
                (
                    await session.execute(
                        select(EventSource).where(EventSource.event_id == int(event.id))
                    )
                ).scalars()
            )
            selected = select_sources(sources, maximum=max_sources_per_event)
            if not selected:
                continue
            plan.append(
                PlannedEvent(
                    event_id=int(event.id),
                    reasons=reasons,
                    source_ids=tuple(int(source.id or 0) for source in selected),
                )
            )
            if limit and len(plan) >= limit:
                break
        return plan


async def apply_plan(db: Database, plan: Iterable[PlannedEvent]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "static-collection-facts-backfill-v1",
        "attempted_sources": 0,
        "applied_sources": 0,
        "unchanged_sources": 0,
        "deferred_sources": 0,
        "events": [],
    }
    for item in plan:
        event_report = {"event_id": item.event_id, "reasons": list(item.reasons), "sources": []}
        for source_id in reversed(item.source_ids):
            # Apply selected evidence in chronological priority order so the
            # strongest/newest accepted source is considered last.
            async with db.get_session() as session:
                event = await session.get(Event, item.event_id)
                source = await session.get(EventSource, source_id)
            if event is None or source is None or int(source.event_id) != item.event_id:
                event_report["sources"].append({"source_id": source_id, "status": "missing"})
                continue
            candidate = build_candidate(event, source, item.reasons)
            input_hash = collection_adjudication_input_hash(candidate)
            if decision_hash_covers(event, reasons=item.reasons, input_hash=input_hash):
                report["unchanged_sources"] += 1
                event_report["sources"].append(
                    {"source_id": source_id, "input_hash": input_hash, "status": "cached"}
                )
                continue
            report["attempted_sources"] += 1
            payload = await adjudicate_collection_candidate(candidate)
            if payload is None:
                report["deferred_sources"] += 1
                event_report["sources"].append(
                    {"source_id": source_id, "input_hash": input_hash, "status": "deferred"}
                )
                continue
            async with db.get_session() as session:
                persisted_event = await session.get(Event, item.event_id)
                persisted_source = await session.get(EventSource, source_id)
                if persisted_event is None or persisted_source is None:
                    changed = False
                else:
                    changed = apply_collection_decisions(
                        persisted_event,
                        payload,
                        source=persisted_source,
                        source_corpus=str(source.source_text or ""),
                        input_hash=input_hash,
                    )
                    if changed:
                        session.add(persisted_event)
                        await session.commit()
            key = "applied_sources" if changed else "unchanged_sources"
            report[key] += 1
            event_report["sources"].append(
                {
                    "source_id": source_id,
                    "input_hash": input_hash,
                    "status": "applied" if changed else "unchanged",
                }
            )
        report["events"].append(event_report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--apply", action="store_true", help="perform LLM calls and DB writes")
    parser.add_argument("--current-date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--reason", action="append", choices=sorted(REASONS), default=[])
    parser.add_argument("--event-id", action="append", type=int, default=[])
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-sources-per-event", type=int, default=1)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> dict[str, Any]:
    enabled_reasons = args.reason or sorted(REASONS)
    db = Database(args.db)
    if args.apply:
        await db.init()
    try:
        plan = await build_plan(
            db,
            current_date=date.fromisoformat(args.current_date),
            enabled_reasons=enabled_reasons,
            event_ids={int(value) for value in args.event_id},
            limit=max(0, int(args.limit)),
            max_sources_per_event=max(1, min(int(args.max_sources_per_event), 4)),
        )
        result: dict[str, Any] = {
            "schema_version": "static-collection-facts-backfill-plan-v1",
            "mode": "apply" if args.apply else "plan",
            "current_date": args.current_date,
            "event_count": len(plan),
            "source_count": sum(len(item.source_ids) for item in plan),
            "plan": [asdict(item) for item in plan],
        }
        if args.apply:
            result["apply"] = await apply_plan(db, plan)
        return result
    finally:
        await db.close()


def main() -> int:
    args = parse_args()
    result = asyncio.run(run(args))
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
