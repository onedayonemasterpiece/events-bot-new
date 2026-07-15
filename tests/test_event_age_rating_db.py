from __future__ import annotations

import pytest
import importlib.util
import sys
from pathlib import Path
from sqlalchemy import text

from db import Database
from models import Event
from source_parsing.handlers import reconcile_existing_event_age
from source_parsing.parser import TheatreEvent
from smart_event_update import EventCandidate, smart_event_update
import smart_event_update as smart_update_module


@pytest.mark.asyncio
async def test_event_age_columns_and_roundtrip(tmp_path):
    db = Database(str(tmp_path / "age.sqlite"))
    await db.init()
    async with db.engine.connect() as conn:
        columns = {row[1]: row for row in (await conn.execute(text("pragma table_info(event)"))).fetchall()}
    required = {
        "age_restriction",
        "age_restriction_status",
        "age_restriction_provenance",
        "age_restriction_evidence",
        "age_assessment",
        "age_assessment_evidence",
    }
    assert required <= columns.keys()
    assert columns["age_restriction"][4] is None
    assert columns["age_restriction_status"][4] == "'unknown'"
    async with db.get_session() as session:
        event = Event(
            title="T",
            description="D",
            date="2026-08-01",
            time="12:00",
            location_name="L",
            source_text="S",
            age_restriction="12+",
            age_restriction_status="declared",
            age_restriction_provenance="official_structured",
            age_restriction_evidence={"kind": "structured", "quote": "12+"},
        )
        session.add(event)
        await session.commit()
        event_id = event.id
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored is not None
        assert stored.age_restriction == "12+"
        assert stored.age_restriction_evidence["quote"] == "12+"
    await db.close()


def test_universal_festival_uds_preserves_global_and_program_age():
    uds_path = Path(__file__).resolve().parents[1] / "kaggle" / "UniversalFestivalParser" / "src" / "uds.py"
    spec = importlib.util.spec_from_file_location("universal_festival_uds", uds_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    UDSOutput = module.UDSOutput

    payload = UDSOutput(
        source_url="https://festival.test",
        extracted_at="2026-07-15T00:00:00Z",
        parser_version="test",
        run_id="test",
        festival={"title_short": "Фестиваль", "age_restriction": "12+"},
        program=[{"title": "Фильм", "age_restriction": "18+"}],
    ).model_dump()
    assert payload["festival"]["age_restriction"] == "12+"
    assert payload["program"][0]["age_restriction"] == "18+"


@pytest.mark.asyncio
async def test_repeated_parser_fast_path_reconciles_age_without_llm(tmp_path):
    db = Database(str(tmp_path / "fast-path.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="T",
            description="D",
            date="2026-08-01",
            time="12:00",
            location_name="L",
            source_text="S",
        )
        session.add(event)
        await session.commit()
        event_id = int(event.id or 0)
    parser_event = TheatreEvent(
        title="T",
        date_raw="1 августа 12:00",
        ticket_status="available",
        url="https://tickets.test/1",
        photos=[],
        description="D",
        pushkin_card=False,
        location="L",
        age_restriction="6+",
        source_type="qtickets",
        parsed_date="2026-08-01",
        parsed_time="12:00",
    )
    assert await reconcile_existing_event_age(db, event_id, parser_event)
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored and stored.age_restriction == "6+"
        assert stored.age_restriction_provenance == "official_structured"
    assert not await reconcile_existing_event_age(db, event_id, parser_event)
    await db.close()


@pytest.mark.asyncio
async def test_smart_update_create_persists_structured_age_and_marks_effectful(tmp_path, monkeypatch):
    async def no_topics(*_args, **_kwargs):
        return None

    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(smart_update_module, "_classify_topics", no_topics)
    db = Database(str(tmp_path / "smart-create.sqlite"))
    await db.init()
    candidate = EventCandidate(
        source_type="parser:qtickets",
        source_url="https://tickets.test/future-age",
        source_text="Официальная билетная карточка. Возраст: 12+.",
        title="Будущий спектакль",
        date="2026-12-01",
        time="19:00",
        location_name="Тестовый зал",
        city="Калининград",
        age_restriction="12+",
        age_restriction_is_structured=True,
        raw_excerpt="Официальная аннотация будущего спектакля.",
        trust_level="high",
    )
    result = await smart_event_update(
        db, candidate, check_source_url=False, schedule_tasks=False
    )
    assert result.status == "created"
    async with db.get_session() as session:
        event = await session.get(Event, result.event_id)
        assert event and event.age_restriction == "12+"
        assert event.age_restriction_status == "declared"
        assert event.age_restriction_input_hash
    await db.close()
