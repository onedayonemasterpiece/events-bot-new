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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_ordinal,new_ordinal,old_time,new_time,native,ambiguous,expected_match",
    [
        (10, 15, "20:00", "20:00", False, False, True),
        (4, 3, "18:00", "18:00", False, False, True),
        (0, 1, "12:00", "19:00", False, False, False),
        (0, 1, "20:00", "20:00", True, False, False),
        (0, 1, "20:00", "20:00", False, True, False),
    ],
)
async def test_parser_ordinal_drift_recalls_owner_without_conflating_sessions(
    tmp_path, old_ordinal, new_ordinal, old_time, new_time, native, ambiguous,
    expected_match,
):
    url = "https://kaliningrad.tretyakovgallery.ru/tickets"
    if old_ordinal == 10:
        url += "#/buy/event/52226/2026-11-21/20:00:00"
    date_value = "2026-11-10" if old_ordinal == 4 else "2026-11-21"
    def key(ordinal, clock):
        if native:
            return f"source-native:ticket-{ordinal}"
        return _parser_occurrence_key(
            source_type="tretyakov", source_url=url, date_value=date_value,
            end_date_value=None, time_value=clock, producer_ordinal=ordinal,
        )
    db = Database(str(tmp_path / "ordinal.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            owner_ids = []
            for title in (["Русская фортепианная школа", "Средневековый концерт"]
                          if ambiguous else ["Русская фортепианная школа"]):
                event = Event(
                    title=title, description=title, source_text=title,
                    date=date_value, time=old_time,
                    location_name="Филиал Третьяковской галереи", city="Калининград",
                )
                session.add(event)
                await session.flush()
                owner_ids.append(int(event.id))
                session.add(EventSource(
                    event_id=int(event.id), source_type="parser:tretyakov",
                    source_url=url, canonical_source_url=url,
                    source_role="identity_bearing",
                    candidate_key=f"ordinal-fixture-{len(owner_ids)}",
                    occurrence_key=key(old_ordinal + len(owner_ids) - 1, old_time),
                ))
            await session.commit()
        candidate = EventCandidate(
            source_type="parser:tretyakov", source_url=url,
            source_text="21 ноября, " + new_time,
            title="Концерт" if ambiguous else "Русская фортепианная школа",
            date=date_value, time=new_time,
            location_name="Филиал Третьяковской галереи", city="Калининград",
            occurrence_key=key(new_ordinal, new_time),
        )
        conflicts = await su._explicit_occurrence_conflict_ids(db, candidate)
        candidate.explicit_occurrence_conflict_event_ids = conflicts
        match = await su._match_existing_event_by_event_source_url(db, candidate)
        assert bool(conflicts) is native
        assert (match is not None) is expected_match
        if match is not None:
            assert int(match.id) == owner_ids[0]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_parser_ordinal_drift_updates_existing_event_and_source_binding(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    from sqlalchemy import func, select
    from smart_update_identity import IdentityGateMode

    db = Database(str(tmp_path / "ordinal-pipeline.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_classify_topics", AsyncMock(return_value=None))
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 0.99, "fixture")))
        url = "https://kaliningrad.tretyakovgallery.ru/tickets#/buy/event/52226/2099-11-21/20:00:00"
        results = []
        for ordinal in (10, 15):
            candidate = EventCandidate(
                source_type="parser:tretyakov", source_url=url, ticket_link=url,
                title="Русская фортепианная школа", date="2099-11-21", time="20:00",
                location_name="Филиал Третьяковской галереи", city="Калининград",
                source_text="Концерт 21 ноября 2099 в 20:00, Филиал Третьяковской галереи.",
                occurrence_key=_parser_occurrence_key(
                    source_type="tretyakov", source_url=url, date_value="2099-11-21",
                    end_date_value=None, time_value="20:00", producer_ordinal=ordinal,
                ),
                producer_ordinal=ordinal,
            )
            results.append(await su.smart_event_update(
                db, candidate, check_source_url=False, schedule_tasks=False,
            ))
        assert results[0].outcome is su.SmartUpdateTerminalOutcome.CREATED
        assert results[1].outcome is su.SmartUpdateTerminalOutcome.MERGED
        assert results[1].event_id == results[0].event_id
        async with db.get_session() as session:
            assert (await session.execute(select(func.count(Event.id)))).scalar_one() == 1
            source_ids = (await session.execute(select(EventSource.event_id))).scalars().all()
            assert set(source_ids) == {results[0].event_id}
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("native,same_programme", [(False, False), (False, True), (True, True)])
async def test_one_parser_owner_same_slot_keeps_semantic_gate_for_generated_keys(
    tmp_path, monkeypatch, native, same_programme,
):
    from unittest.mock import AsyncMock
    from sqlalchemy import func, select
    from smart_update_identity import IdentityGateMode, stable_candidate_identity
    from models import JobOutbox

    db = Database(str(tmp_path / "one-owner.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_classify_topics", AsyncMock(return_value=None))
        url = "https://kaliningrad.tretyakovgallery.ru/tickets"
        occurrence = "source-native:concert-42" if native else _parser_occurrence_key(
            source_type="tretyakov", source_url=url, date_value="2099-11-20",
            end_date_value=None, time_value="20:00", producer_ordinal=10,
        )
        old = EventCandidate(
            source_type="parser:tretyakov", source_url=url,
            title="Русская фортепианная школа", date="2099-11-20", time="20:00",
            location_name="Филиал Третьяковской галереи", city="Калининград",
            source_text="Концерт Чайковского и Рахманинова.", occurrence_key=occurrence,
        )
        key, _ = stable_candidate_identity(old)
        async with db.get_session() as session:
            owner = Event(title=old.title, description=old.source_text,
                          source_text=old.source_text, date=old.date, time=old.time,
                          location_name=old.location_name, city=old.city)
            session.add(owner)
            await session.flush()
            owner_id = int(owner.id)
            session.add(EventSource(event_id=owner_id, source_type=old.source_type,
                                    source_url=url, canonical_source_url=url,
                                    source_role="identity_bearing", candidate_key=key,
                                    occurrence_key=occurrence))
            await session.commit()
        gate = AsyncMock(return_value={
            "action": "allow_merge" if same_programme else "skip_merge_side_effects",
            "relation": "source_update" if same_programme else "unknown",
            "confidence": 0.99, "reason_code": "same_programme" if same_programme else "unrelated_programme",
            "reason": "isolated semantic verdict", "source_grounded_evidence": [],
            "blocking_conflicts": [] if same_programme else ["different programme"],
            "allowed_fields": ["title", "description", "source_text"] if same_programme else [],
        })
        monkeypatch.setattr(su, "_llm_merge_identity_gate", gate)
        candidate = EventCandidate(
            source_type=old.source_type, source_url=url,
            title="Школа русского фортепиано" if same_programme else "Роботы исследуют Марс",
            date=old.date, time=old.time, location_name=old.location_name, city=old.city,
            source_text="Концерт Чайковского и Рахманинова: обновлённая программа." if same_programme else "Лекция о марсоходах и космической робототехнике.",
            occurrence_key=occurrence,
        )
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert gate.await_count == (0 if native else 1)
        if same_programme:
            assert result.outcome is su.SmartUpdateTerminalOutcome.MERGED
            assert result.event_id == owner_id
        else:
            assert result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED
        async with db.get_session() as session:
            owner = await session.get(Event, owner_id)
            assert owner.title in ({old.title, candidate.title} if same_programme else {old.title})
            assert owner.source_text == (candidate.source_text if same_programme else old.source_text)
            assert (await session.execute(select(func.count(Event.id)))).scalar_one() == 1
            assert (await session.execute(select(func.count(JobOutbox.id)))).scalar_one() == 0
    finally:
        await db.close()
