from __future__ import annotations

import pytest
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import Event, EventIdentityDecisionLog
from smart_event_update import EventCandidate


@pytest.mark.asyncio
async def test_identity_gate_decision_is_persisted(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/1",
        source_text="10 июля концерт",
        title="Концерт",
        date="2026-07-10",
        time="19:00",
        location_name="Дом искусств",
    )

    await su._record_identity_gate_decision(
        db,
        candidate,
        decision="allow_create",
        reason="no_identity_veto",
        confidence=0.0,
        payload={"mode": "shadow"},
    )

    async with db.get_session() as session:
        rows = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
    assert len(rows) == 1
    assert rows[0].source_url == "https://t.me/example/1"
    assert rows[0].decision == "allow_create"
    assert rows[0].decision_payload["mode"] == "shadow"


@pytest.mark.asyncio
async def test_created_event_populates_date_provenance_fields(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_classify_topics", lambda *args, **kwargs: None)

    async def _no_topics(*args, **kwargs):
        return None

    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/2",
        source_text="Концерт 10 июля в Доме искусств",
        title="Концерт",
        date="2026-07-10",
        time="19:00",
        location_name="Дом искусств",
        city="Калининград",
        event_type="концерт",
    )

    result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

    assert result.status == "created"
    async with db.get_session() as session:
        event = (await session.execute(select(Event))).scalars().first()
    assert event is not None
    assert event.date_provenance == su.DATE_PROVENANCE_SOURCE_TEXT
    assert event.date_is_inferred is False
    assert event.date_confidence and event.date_confidence >= 0.8
