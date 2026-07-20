from __future__ import annotations

import pytest
from sqlalchemy import select

from db import Database
import event_media
from models import Event, EventImageGeometry, EventPoster
from scripts.enqueue_static_event_media_enrichment import (
    static_event_eligibility,
    static_media_enrichment_statement,
)


def _event(
    title: str,
    *,
    date: str = "2026-07-20",
    end_date: str | None = None,
    silent: bool = False,
    lifecycle_status: str = "active",
) -> Event:
    return Event(
        title=title,
        description="Описание",
        date=date,
        end_date=end_date,
        time="18:00",
        location_name="Площадка",
        source_text="Источник",
        silent=silent,
        lifecycle_status=lifecycle_status,
    )


@pytest.mark.asyncio
async def test_static_media_enrichment_excludes_non_public_rows(tmp_path) -> None:
    db = Database(str(tmp_path / "scope.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add_all(
            [
                _event("future active"),
                _event(
                    "ongoing active",
                    date="2026-06-01",
                    end_date="2026-08-01",
                ),
                _event("silent tombstone", silent=True),
                _event("cancelled", lifecycle_status="cancelled"),
                _event("postponed", lifecycle_status="postponed"),
                _event("already ended", date="2026-06-01", end_date="2026-07-14"),
            ]
        )
        await session.commit()

        titles = (
            await session.execute(
                select(Event.title)
                .where(static_event_eligibility("2026-07-15"))
                .order_by(Event.title)
            )
        ).scalars().all()

    assert titles == ["future active", "ongoing active"]


@pytest.mark.asyncio
async def test_static_media_enrichment_selects_stale_pixel_geometry(tmp_path) -> None:
    db = Database(str(tmp_path / "geometry-scope.sqlite"))
    await db.init()
    async with db.get_session() as session:
        stale_event = _event("stale geometry")
        current_event = _event("current geometry")
        session.add(stale_event)
        session.add(current_event)
        await session.flush()
        stale_geometry = EventImageGeometry(
            pixel_sha256="a" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        current_geometry = EventImageGeometry(
            pixel_sha256="c" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(stale_geometry)
        session.add(current_geometry)
        await session.flush()
        common = {
            "review_status": "approved",
            "width": 1200,
            "height": 800,
            "thumbnail_256_url": "https://static.example/256.webp",
            "thumbnail_512_url": "https://static.example/512.webp",
            "media_semantic_status": "classified",
            "media_semantic_prompt_version": event_media.MEDIA_ROLE_PROMPT_VERSION,
        }
        session.add(
            EventPoster(
                event_id=int(stale_event.id),
                poster_hash="stale",
                pixel_sha256="b" * 64,
                image_geometry_id=int(stale_geometry.id),
                **common,
            )
        )
        session.add(
            EventPoster(
                event_id=int(current_event.id),
                poster_hash="current",
                pixel_sha256="c" * 64,
                image_geometry_id=int(current_geometry.id),
                **common,
            )
        )
        await session.commit()

        rows = (
            await session.execute(
                static_media_enrichment_statement(
                    from_date="2026-07-20",
                    limit=100,
                )
            )
        ).all()

    await db.engine.dispose()
    assert [(event_id, count) for event_id, count in rows] == [
        (int(stale_event.id), 1)
    ]
