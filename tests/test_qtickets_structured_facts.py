from pathlib import Path

import pytest
from sqlalchemy import select

from db import Database
from models import Event, EventSource
import smart_event_update as su
from smart_event_update import EventCandidate, smart_event_update
from source_parsing.handlers import _build_parser_source_text
from source_parsing.qtickets import parse_qtickets_output


def _fixture_path() -> str:
    return str(
        Path(__file__).parent
        / "replays"
        / "INC-2026-06-29-qtickets-structured-facts-lost"
        / "qtickets_events.json"
    )


def test_qtickets_replay_preserves_structured_address_and_end_date():
    events = parse_qtickets_output([_fixture_path()])

    flava = events[0]
    assert flava.title == "FLAVA INTENSIVE (VALERA & LERA VOYNITS)"
    assert flava.parsed_date == "2026-07-04"
    assert flava.parsed_time == "14:10"
    assert flava.end_date == "2026-07-07"
    assert flava.location == "КОНЦЕПТ"
    assert flava.location_address == "ул. Ленинский проспект, 42Б, Калининград, Россия"
    assert flava.ticket_price_min == 1800


def test_qtickets_llm_source_text_keeps_page_facts_ahead_of_ocr_fragments():
    event = parse_qtickets_output([_fixture_path()])[0]

    text = _build_parser_source_text(
        event,
        full_description="Возраст: 0+",
        location_name=event.location,
    )

    assert "Название: FLAVA INTENSIVE (VALERA & LERA VOYNITS)" in text
    assert "Дата: 2026-07-04" in text
    assert "Дата окончания: 2026-07-07" in text
    assert "Время: 14:10" in text
    assert "Площадка: КОНЦЕПТ" in text
    assert "Адрес: ул. Ленинский проспект, 42Б, Калининград, Россия" in text
    assert "Контракт источника:" in text
    assert "не заменяй название страницы отдельными словами с афиши" in text


def test_qtickets_negative_control_does_not_require_end_date_or_address():
    event = parse_qtickets_output([_fixture_path()])[1]

    text = _build_parser_source_text(
        event,
        full_description=event.description,
        location_name=event.location,
    )

    assert "Название: Контрольное событие без афишного конфликта" in text
    assert "Дата окончания:" not in text
    assert "Адрес:" not in text


@pytest.mark.asyncio
async def test_qtickets_replay_keeps_structured_text_through_smart_update(tmp_path, monkeypatch):
    event = parse_qtickets_output([_fixture_path()])[0]
    source_text = _build_parser_source_text(
        event,
        full_description="Возраст: 0+",
        location_name=event.location,
    )

    async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
        return None

    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    # This is an immutable historical incident replay, not a current ingestion
    # eligibility test.
    monkeypatch.setattr(
        su, "_should_skip_past_smart_update_candidate", lambda _candidate: False
    )

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        candidate = EventCandidate(
            source_type="parser:qtickets",
            source_url=event.url,
            source_text=source_text,
            title=event.title,
            date=event.parsed_date,
            time=event.parsed_time,
            end_date=event.end_date,
            location_name=event.location,
            location_address=event.location_address,
            city="Калининград",
            ticket_link=event.url,
            ticket_price_min=event.ticket_price_min,
            ticket_status=event.ticket_status,
            event_type="интенсив",
            raw_excerpt="Интенсив FLAVA INTENSIVE с VALERA и LERA VOYNITS.",
            trust_level="high",
        )

        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )
        assert result.status == "created"
        assert result.event_id

        async with db.get_session() as session:
            saved = await session.get(Event, int(result.event_id))
            assert saved is not None
            assert saved.title == "FLAVA INTENSIVE (VALERA & LERA VOYNITS)"
            assert saved.end_date == "2026-07-07"
            assert saved.location_address == "ул. Ленинский проспект, 42Б, Калининград, Россия"
            assert "Название: FLAVA INTENSIVE" in (saved.source_text or "")
            assert "Дата окончания: 2026-07-07" in (saved.source_text or "")
            assert "Адрес: ул. Ленинский проспект" in (saved.source_text or "")
            source_rows = (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == int(result.event_id))
                )
            ).scalars().all()
            assert len(source_rows) == 1
            assert "не заменяй название страницы" in (source_rows[0].source_text or "")
    finally:
        await db.close()
