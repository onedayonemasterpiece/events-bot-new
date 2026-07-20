#!/usr/bin/env python3
"""Enqueue bounded event-media derivative/semantic enrichment for static pages.

Dry-run is the default. ``--apply`` only schedules the existing audited
``event_media_review`` outbox task; workers remain responsible for CDN
materialisation, LLM classification and gallery projection.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys

from sqlalchemy import and_, func, or_, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from event_media import (
    IMAGE_GEOMETRY_PROMPT_VERSION,
    MEDIA_ROLE_PROMPT_VERSION,
    enqueue_event_media_review_job,
    image_geometry_model,
)
from models import Event, EventImageGeometry, EventPoster


def static_event_eligibility(from_date: str):
    """Match the static export's active/non-silent current-or-ongoing scope."""

    return and_(
        func.coalesce(Event.silent, False).is_(False),
        func.coalesce(func.nullif(func.trim(Event.lifecycle_status), ""), "active")
        == "active",
        or_(
            Event.date >= str(from_date),
            Event.end_date >= str(from_date),
        ),
    )


def static_media_enrichment_statement(
    *,
    from_date: str,
    limit: int,
    event_id: int | None = None,
    retry_errors: bool = False,
    now: datetime | None = None,
):
    role_statuses = [None, "pending", "stale"]
    if retry_errors:
        role_statuses.append("error")
    needs_semantics = or_(
        EventPoster.media_semantic_status.in_(
            [value for value in role_statuses if value is not None]
        ),
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
    retry_before = (now or datetime.now(timezone.utc)) - timedelta(hours=20)
    geometry_model = image_geometry_model()
    needs_geometry = or_(
        EventPoster.image_geometry_id.is_(None),
        EventImageGeometry.id.is_(None),
        EventPoster.pixel_sha256.is_(None),
        EventImageGeometry.pixel_sha256.is_(None),
        EventPoster.pixel_sha256 != EventImageGeometry.pixel_sha256,
        EventImageGeometry.model.is_(None),
        EventImageGeometry.model != geometry_model,
        EventImageGeometry.prompt_version.is_(None),
        EventImageGeometry.prompt_version != IMAGE_GEOMETRY_PROMPT_VERSION,
        EventImageGeometry.status.is_(None),
        and_(
            EventImageGeometry.status != "classified",
            EventImageGeometry.status != "error",
        ),
        and_(
            EventImageGeometry.status == "error",
            EventImageGeometry.updated_at <= retry_before,
        ),
    )
    stmt = (
        select(Event.id, func.count(EventPoster.id).label("media_count"))
        .join(EventPoster, EventPoster.event_id == Event.id)
        .outerjoin(
            EventImageGeometry,
            EventPoster.image_geometry_id == EventImageGeometry.id,
        )
        .where(
            EventPoster.review_status == "approved",
            or_(needs_derivative, needs_semantics, needs_geometry),
            static_event_eligibility(str(from_date)),
        )
        .group_by(Event.id)
        .order_by(Event.date.asc(), Event.id.asc())
        .limit(max(1, int(limit)))
    )
    if event_id:
        stmt = stmt.where(Event.id == int(event_id))
    return stmt


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
        stmt = static_media_enrichment_statement(
            from_date=str(args.from_date),
            limit=int(args.limit),
            event_id=args.event_id,
            retry_errors=bool(args.retry_errors),
        )
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
