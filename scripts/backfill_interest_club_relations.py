#!/usr/bin/env python3
"""Plan or enqueue a bounded six-month interest-club relation catch-up."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from interest_clubs import (
    build_evidence_packet,
    event_is_relation_eligible,
    schedule_interest_club_evaluation,
)
from models import Event, EventSource, InterestClub


@dataclass(frozen=True)
class Candidate:
    event_id: int
    club_ids: tuple[int, ...]
    lanes: tuple[str, ...]


def event_overlaps_cutoff(event: Event, cutoff: date) -> bool:
    effective_end = str(event.end_date or event.date or "")
    try:
        return date.fromisoformat(effective_end) >= cutoff
    except ValueError:
        return False


async def build_plan(db: Database, *, cutoff: date, limit: int) -> list[Candidate]:
    async with db.get_session() as session:
        clubs = list(
            (
                await session.execute(
                    select(InterestClub)
                    .where(InterestClub.public_status.in_(["approved", "shadow"]))
                    .order_by(InterestClub.id)
                )
            ).scalars()
        )
        events = list((await session.execute(select(Event).order_by(Event.id))).scalars())
        result: list[Candidate] = []
        for event in events:
            if not event.id or not event_is_relation_eligible(event) or not event_overlaps_cutoff(event, cutoff):
                continue
            sources = list(
                (
                    await session.execute(
                        select(EventSource).where(EventSource.event_id == int(event.id))
                    )
                ).scalars()
            )
            matches = [
                (int(club.id or 0), packet.lane)
                for club in clubs
                if (packet := build_evidence_packet(event, sources, club)) is not None
            ]
            if not matches:
                continue
            result.append(
                Candidate(
                    event_id=int(event.id),
                    club_ids=tuple(club_id for club_id, _lane in matches),
                    lanes=tuple(lane for _club_id, lane in matches),
                )
            )
            if limit and len(result) >= limit:
                break
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--cutoff", required=True, help="inclusive YYYY-MM-DD activity cutoff")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--apply", action="store_true", help="enqueue durable relation jobs")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> dict[str, Any]:
    db = Database(args.db)
    if args.apply:
        await db.init()
    try:
        plan = await build_plan(db, cutoff=date.fromisoformat(args.cutoff), limit=max(0, int(args.limit)))
        actions: list[dict[str, Any]] = []
        if args.apply:
            for candidate in plan:
                action = await schedule_interest_club_evaluation(
                    db,
                    candidate.event_id,
                    schedule_projection=False,
                )
                actions.append({"event_id": candidate.event_id, "action": action})
        return {
            "schema_version": "interest-club-relation-backfill-v1",
            "mode": "apply" if args.apply else "plan",
            "cutoff": args.cutoff,
            "candidate_count": len(plan),
            "plan": [asdict(item) for item in plan],
            "actions": actions,
        }
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
