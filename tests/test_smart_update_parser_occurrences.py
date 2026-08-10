import pytest
from types import SimpleNamespace

from db import Database
from models import Event, EventSource
import smart_event_update as su
from smart_event_update import (
    EventCandidate,
    _filter_same_parser_source_occurrence_conflicts,
    smart_event_update,
)
from source_parsing.handlers import _parser_occurrence_key
from source_parsing.commands import (
    _claim_source_parser_recovery_requests,
    _settle_source_parser_recovery_requests,
)


def test_official_parser_occurrence_key_splits_same_page_sessions_and_is_stable():
    common = {
        "source_type": "muzteatr",
        "source_url": "https://muzteatr39.ru/action/brodskiy?utm_source=test",
        "date_value": "2026-10-25",
        "end_date_value": None,
    }
    first = _parser_occurrence_key(
        **common,
        time_value="14:00",
        producer_ordinal=0,
    )
    replay = _parser_occurrence_key(
        **common,
        time_value="14.00",
        producer_ordinal=0,
    )
    later_session = _parser_occurrence_key(
        **common,
        time_value="17:00",
        producer_ordinal=1,
    )
    same_slot_sibling = _parser_occurrence_key(
        **common,
        time_value="14:00",
        producer_ordinal=1,
    )

    assert first == replay
    assert len({first, later_session, same_slot_sibling}) == 3


@pytest.mark.asyncio
async def test_parser_recovery_request_is_claimed_and_only_resolved_source_completes(
    tmp_path,
) -> None:
    db = Database(str(tmp_path / "parser-recovery.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            await conn.executemany(
                "INSERT INTO source_parser_recovery_request("
                "source_type,requested_since,status,next_run_at) "
                "VALUES(?,?,'pending',CURRENT_TIMESTAMP)",
                [
                    ("dramteatr", "2026-08-04 00:00:00"),
                    ("muzteatr", "2026-08-04 00:00:00"),
                ],
            )
            await conn.commit()

        claimed = await _claim_source_parser_recovery_requests(db)
        assert claimed == ["dramteatr", "muzteatr"]
        result = SimpleNamespace(
            stats_by_source={
                "dramteatr": SimpleNamespace(failed=0, retry_scheduled=0),
                "muzteatr": SimpleNamespace(failed=1, retry_scheduled=2),
            }
        )
        await _settle_source_parser_recovery_requests(db, claimed, result)

        async with db.raw_conn() as conn:
            cursor = await conn.execute(
                "SELECT source_type,status,attempts,last_error "
                "FROM source_parser_recovery_request ORDER BY source_type"
            )
            rows = await cursor.fetchall()
            await cursor.close()
        assert rows[0][0:3] == ("dramteatr", "done", 1)
        assert rows[0][3] is None
        assert rows[1][0:3] == ("muzteatr", "pending", 1)
        assert "retry_scheduled=2" in str(rows[1][3])
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_same_parser_explicit_second_session_is_not_collapsed(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            parser_event = Event(
                title="Бродский. Обещание любви",
                description="Вечерний сеанс.",
                date="2026-10-25",
                time="17:00",
                location_name="Музыкальный театр",
                city="Калининград",
                source_text="Официальная афиша: сеанс 17:00.",
            )
            social_event = Event(
                title="Другая карточка",
                description="Социальный источник с неверным временем.",
                date="2026-10-25",
                time="18:00",
                location_name="Музыкальный театр",
                city="Калининград",
                source_text="Социальная карточка: сеанс 18:00.",
            )
            session.add(parser_event)
            session.add(social_event)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(parser_event.id),
                    source_type="parser:muzteatr",
                    source_url="https://muzteatr39.ru/action/brodskiy",
                )
            )
            await session.commit()
            parser_id = int(parser_event.id)
            social_id = int(social_event.id)

        candidate = EventCandidate(
            source_type="parser:muzteatr",
            source_url="https://muzteatr39.ru/action/brodskiy",
            source_text="25 октября: 14:00, Бродский. Обещание любви",
            title="Бродский. Обещание любви",
            date="2026-10-25",
            time="14:00",
            location_name="Музыкальный театр",
            city="Калининград",
        )
        async with db.get_session() as session:
            events = [
                await session.get(Event, parser_id),
                await session.get(Event, social_id),
            ]
        filtered, excluded = await _filter_same_parser_source_occurrence_conflicts(
            db,
            candidate,
            [event for event in events if event is not None],
        )

        assert excluded == [parser_id]
        assert [int(event.id) for event in filtered] == [social_id]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_creates_second_official_session(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            existing = Event(
                title="Спектакль «Бродский. Обещание любви»",
                description="Вечерний сеанс.",
                source_text="Официальная афиша: сеанс 17:00.",
                date="2026-10-25",
                time="17:00",
                location_name="Музыкальный театр",
                city="Калининград",
                ticket_link="https://muzteatr39.ru/action/brodskiy",
            )
            session.add(existing)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(existing.id),
                    source_type="parser:muzteatr",
                    source_url="https://muzteatr39.ru/action/brodskiy",
                )
            )
            await session.commit()
            existing_id = int(existing.id)

        async def no_topics(*_args, **_kwargs):
            return None

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "_classify_topics", no_topics)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        candidate = EventCandidate(
            source_type="parser:muzteatr",
            source_url="https://muzteatr39.ru/action/brodskiy",
            source_text=(
                "Название: Бродский. Обещание любви\n"
                "Дата: 2026-10-25\nВремя: 14:00\n"
                "Площадка: Музыкальный театр"
            ),
            raw_excerpt="Спектакль в 14:00.",
            title="Спектакль «Бродский. Обещание любви»",
            date="2026-10-25",
            time="14:00",
            location_name="Музыкальный театр",
            city="Калининград",
            trust_level="high",
            ticket_link="https://muzteatr39.ru/action/brodskiy",
        )
        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.created is True
        assert result.event_id is not None
        assert int(result.event_id) != existing_id
        async with db.get_session() as session:
            created = await session.get(Event, int(result.event_id))
        assert created is not None
        assert created.date == "2026-10-25"
        assert created.time == "14:00"
    finally:
        await db.close()
