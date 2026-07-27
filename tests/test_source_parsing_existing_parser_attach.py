from datetime import date, timedelta

import pytest
from sqlmodel import select

import source_parsing.handlers as handlers
from db import Database
from models import Event, EventSource
from source_parsing.parser import TheatreEvent


@pytest.mark.asyncio
async def test_exact_existing_event_attaches_parser_without_llm(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()

    async with db.get_session() as session:
        stored = Event(
            title="Вне подозрения",
            description="Existing canonical description",
            date=event_date,
            time="19:00",
            location_name="Калининградский театр эстрады (Дом искусств)",
            source_text="Telegram source",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def find_existing(*_args, **_kwargs):
        return event_id, False

    async def true_result(*_args, **_kwargs):
        return True

    async def no_result(*_args, **_kwargs):
        return None

    async def forbidden_llm(*_args, **_kwargs):
        raise AssertionError("exact existing parser occurrence must not call Smart Update")

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_event_ticket_status", true_result)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "schedule_existing_event_update", no_result)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", forbidden_llm)

    candidate = TheatreEvent(
        title="Вне подозрения",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url="https://domiskusstv.edinoepole.ru/widget/events/922/event_seats",
        location="Калининградский театр эстрады (Дом искусств)",
        source_type="estrada",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="estrada",
        start_index=0,
        total_count=1,
    )

    async with db.get_session() as session:
        sources = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    await db.engine.dispose()

    assert stats.ticket_updated == 1
    assert stats.failed == 0
    assert [(row.source_type, row.source_url) for row in sources] == [
        (
            "parser:estrada",
            "https://domiskusstv.edinoepole.ru/widget/events/922/event_seats",
        )
    ]
