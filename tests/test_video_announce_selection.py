import json
from datetime import date, timezone
from types import SimpleNamespace

import pytest

from db import Database
from models import Event, VideoAnnounceItem, VideoAnnounceSession
import main
from video_announce import selection
from video_announce.custom_types import RenderPayload, SelectionContext


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


def test_payload_as_json_includes_festival_line_for_cherryflash() -> None:
    event = Event(
        id=4759,
        title="Лекция про влияние планировочных решений",
        description="d",
        date="2026-05-15",
        time="19:00",
        location_name="Дом молодежи",
        city="Калининград",
        festival="80 историй о главном",
        photo_urls=["https://example.com/poster.jpg"],
        photo_count=1,
    )
    session = VideoAnnounceSession(
        id=304,
        selection_params={"mode": "popular_review", "render_order": [4759]},
    )
    item = VideoAnnounceItem(
        session_id=304,
        event_id=4759,
        position=1,
        final_about="Лекция про город",
        final_description="Почему планировка влияет на жизнь.",
    )
    payload = RenderPayload(session=session, items=[item], events=[event])

    data = json.loads(selection.payload_as_json(payload, timezone.utc))

    assert data["scenes"][0]["festival"] == "80 историй о главном"


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


def test_payload_as_json_inserts_guide_excursion_promo_scene() -> None:
    events = []
    items = []
    for event_id, pos in ((1, 1), (2, 2), (3, 3)):
        events.append(
            Event(
                id=event_id,
                title=f"Event {event_id}",
                description="d",
                date="2026-07-09",
                time="19:00",
                location_name="Place",
                city="Калининград",
                photo_urls=[f"https://example.com/{event_id}.jpg"],
                photo_count=1,
            )
        )
        items.append(
            VideoAnnounceItem(
                session_id=700,
                event_id=event_id,
                position=pos,
                final_about=f"Event {event_id}",
                final_description="desc",
            )
        )
    session = VideoAnnounceSession(
        id=700,
        selection_params={
            "mode": "popular_review",
            "guide_excursion_promo": {
                "occurrence_id": 42,
                "title": "История в переплётах: экскурсия по библиотеке БФУ",
                "date_iso": "2026-07-10",
                "time": "11:00",
                "avatar_images": ["assets/guide_avatars/amber_fringilla.jpg"],
                "contact": "@amber_fringilla",
                "contact_label": "запись",
                "palette": "museum_green_ivory",
                "icon_kind": "building",
                "insert_position": 3,
            },
        },
    )
    payload = RenderPayload(session=session, items=items, events=events)

    data = json.loads(selection.payload_as_json(payload, timezone.utc))

    assert data["scenes"][2]["scene_variant"] == "guide_excursion_promo"
    assert data["scenes"][2]["date"] == "10.07 11:00"
    assert data["scenes"][2]["guide_excursion"]["occurrence_id"] == 42
    assert data["scenes"][2]["images"] == ["assets/guide_avatars/amber_fringilla.jpg"]
