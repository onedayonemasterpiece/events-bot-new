from __future__ import annotations

import pytest
from sqlalchemy import select

from db import Database
from models import Event
from scripts.enqueue_static_event_media_enrichment import static_event_eligibility


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
