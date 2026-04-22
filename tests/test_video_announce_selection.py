from datetime import date, timezone
from types import SimpleNamespace

import pytest

from db import Database
from models import Event
import main
from video_announce import selection
from video_announce.custom_types import SelectionContext


@pytest.mark.asyncio
async def test_fetch_candidates_includes_fair_and_schedule_text(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        fair = Event(
            title="Fair",
            description="d",
            source_text="s",
            date="2025-12-25",
            end_date="2026-01-10",
            time="10:00..17:30",
            location_name="Market",
            event_type="ярмарка",
            photo_urls=["http://example.com/a.jpg"],
            photo_count=1,
        )
        session.add(fair)
        await session.commit()
        await session.refresh(fair)
        fair_id = fair.id

    ctx = SelectionContext(
        tz=timezone.utc,
        target_date=date(2026, 1, 3),
    )
    events, schedule_map, _ = await selection.fetch_candidates(db, ctx)
    assert any(e.id == fair_id for e in events)
    expected = f"по {main.format_day_pretty(date(2026, 1, 10))} с 10:00 до 17:30"
    assert schedule_map[fair_id] == expected


@pytest.mark.asyncio
async def test_fill_missing_about_can_be_disabled_by_env(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VIDEO_ANNOUNCE_DISABLE_ABOUT_FILL", "1")

    async def _unexpected_ask_4o(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("ask_4o must not be called when about fill is disabled")

    monkeypatch.setattr(selection, "ask_4o", _unexpected_ask_4o)

    result = await selection.fill_missing_about(
        db,
        session_id=176,
        items=[SimpleNamespace(final_about=None)],
        events={},
    )

    assert result == {}
