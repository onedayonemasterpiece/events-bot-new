#!/usr/bin/env python3
"""Enqueue bounded event-media derivative/semantic enrichment for static pages.

Dry-run is the default. ``--apply`` only schedules the existing audited
``event_media_review`` outbox task; workers remain responsible for CDN
materialisation, LLM classification and gallery projection.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
import sys

from sqlalchemy import and_, func, or_, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from event_media import MEDIA_ROLE_PROMPT_VERSION, enqueue_event_media_review_job
from models import Event, EventPoster


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="db.sqlite")
    parser.add_argument("--from-date", default=date.today().isoformat())
    parser.add_argument("--event-id", type=int)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    db = Database(str(args.db))
    await db.init()
    async with db.get_session() as session:
        role_statuses = [None, "pending", "stale"]
        if args.retry_errors:
            role_statuses.append("error")
        needs_semantics = or_(
            EventPoster.media_semantic_status.in_([value for value in role_statuses if value is not None]),
            EventPoster.media_semantic_status.is_(None),
            EventPoster.media_semantic_prompt_version != MEDIA_ROLE_PROMPT_VERSION,
            EventPoster.media_semantic_prompt_version.is_(None),
        )
        needs_derivative = or_(
            EventPoster.thumbnail_256_url.is_(None),
            EventPoster.thumbnail_512_url.is_(None),
            EventPoster.width.is_(None),
            EventPoster.height.is_(None),
        )
        stmt = (
            select(Event.id, func.count(EventPoster.id).label("media_count"))
            .join(EventPoster, EventPoster.event_id == Event.id)
            .where(
                EventPoster.review_status == "approved",
                or_(needs_derivative, needs_semantics),
                or_(Event.end_date >= str(args.from_date), and_(Event.end_date.is_(None), Event.date >= str(args.from_date))),
            )
            .group_by(Event.id)
            .order_by(Event.date.asc(), Event.id.asc())
            .limit(max(1, int(args.limit)))
        )
        if args.event_id:
            stmt = stmt.where(Event.id == int(args.event_id))
        rows = (await session.execute(stmt)).all()
        print(f"candidates={len(rows)} apply={int(args.apply)} from_date={args.from_date}")
        if not args.apply:
            for event_id, media_count in rows[:25]:
                print(f"event_id={event_id} media={media_count}")
            return 0

        enqueued = 0
        for event_id, _media_count in rows:
            if args.retry_errors:
                error_rows = (
                    await session.execute(
                        select(EventPoster).where(
                            EventPoster.event_id == int(event_id),
                            EventPoster.review_status == "approved",
                            EventPoster.media_semantic_status == "error",
                        )
                    )
                ).scalars().all()
                for poster in error_rows:
                    poster.media_semantic_status = "pending"
                    poster.media_semantic_reason_code = "operator_retry_requested"
                    poster.updated_at = datetime.now(timezone.utc)
                    session.add(poster)
            if await enqueue_event_media_review_job(session, int(event_id)):
                enqueued += 1
        await session.commit()
        print(f"enqueued={enqueued} already_pending={len(rows) - enqueued}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
