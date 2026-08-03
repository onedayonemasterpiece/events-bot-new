from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from db import Database
from models import Event, EventSource
from scripts import backfill_static_collection_facts as facts
from scripts import build_static_collection_audience_coverage as coverage
from smart_event_update import collection_adjudication_input_hash


def _event(title: str, *, candidate: bool = True) -> Event:
    return Event(
        title=title,
        description="Описание",
        date="2026-08-10",
        time="18:00",
        location_name="Зал",
        location_address="Улица, 1",
        city="Калининград",
        source_text="Приглашаем родителей с детьми" if candidate else "Обычный концерт",
        topics=["FAMILY"] if candidate else [],
    )


def _source(event_id: int, suffix: int) -> EventSource:
    return EventSource(
        event_id=event_id,
        source_type="telegram",
        source_url=f"https://t.me/example/{suffix}",
        source_text="Приглашаем родителей с детьми",
        trust_level="official",
        imported_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_coverage_separates_evaluated_deferred_and_unprocessed(tmp_path):
    db = Database(str(tmp_path / "coverage.sqlite"))
    await db.init()
    async with db.get_session() as session:
        evaluated = _event("Оценено")
        unprocessed = _event("Не обработано")
        no_source = _event("Нет источника")
        ordinary = _event("Не кандидат", candidate=False)
        session.add_all([evaluated, unprocessed, no_source, ordinary])
        await session.flush()
        evaluated_source = _source(int(evaluated.id), 1)
        unprocessed_source = _source(int(unprocessed.id), 2)
        session.add_all([evaluated_source, unprocessed_source])
        await session.flush()
        candidate = facts.build_candidate(evaluated, evaluated_source, ("audience",))
        input_hash = collection_adjudication_input_hash(candidate)
        evaluated.collection_decisions = {
            key: {
                "value": "unknown",
                "input_hash": input_hash,
                "source_id": int(evaluated_source.id),
            }
            for key in facts.AUDIENCE_FACT_KEYS
        }
        evaluated_id = int(evaluated.id)
        unprocessed_id = int(unprocessed.id)
        no_source_id = int(no_source.id)
        await session.commit()

    first = await coverage.build_coverage(
        db,
        current_date=date(2026, 8, 3),
        generator_command="coverage-test",
    )
    assert first["status"] == "partial"
    assert first["candidate_event_ids"] == sorted(
        [evaluated_id, unprocessed_id, no_source_id]
    )
    assert first["evaluated_event_ids"] == [evaluated_id]
    assert first["deferred_event_ids"] == [no_source_id]
    assert first["unprocessed_event_ids"] == [unprocessed_id]
    assert first["provider_calls"] == 0

    completed = await coverage.build_coverage(
        db,
        current_date=date(2026, 8, 3),
        explicit_deferred_event_ids={unprocessed_id},
        generator_command="coverage-test --deferred",
    )
    assert completed["status"] == "complete"
    assert completed["evaluated_event_count"] == 1
    assert completed["deferred_event_count"] == 2
    assert completed["unprocessed_event_count"] == 0
    await db.close()
