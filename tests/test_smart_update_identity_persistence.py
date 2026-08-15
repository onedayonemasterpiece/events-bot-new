from __future__ import annotations

from datetime import date

import pytest
import pytest_asyncio
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import Event, EventIdentityDecisionLog
from smart_event_update import EventCandidate


@pytest_asyncio.fixture(autouse=True)
async def _dispose_test_databases(monkeypatch):
    """Close SQLAlchemy/aiosqlite workers created by every test in this module."""

    instances: list[Database] = []
    original_init = Database.__init__

    def tracked_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    monkeypatch.setattr(Database, "__init__", tracked_init)
    yield
    for instance in instances:
        await instance.close()


@pytest.fixture(autouse=True)
def _allow_historical_identity_fixtures(monkeypatch):
    """Keep fixed provenance fixtures focused on identity persistence."""
    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")


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


@pytest.mark.asyncio
async def test_identity_gate_exception_records_single_fail_safe_row(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)

        async def _no_topics(*args, **kwargs):
            return None

        async def _no_vector(*args, **kwargs):
            return None

        def _boom(*args, **kwargs):
            raise RuntimeError("gate boom")

        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _no_vector)
        monkeypatch.setattr(su, "build_identity_gate_verdict", _boom)
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/example/failsafe",
            source_text="Концерт 10 июля в Доме искусств",
            title="Концерт",
            date="2026-07-10",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            event_type="концерт",
        )

        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.outcome is su.SmartUpdateTerminalOutcome.FAILED_TECHNICAL
        assert result.event_id is None
        assert result.reason == "identity_gate_uncertain:identity_gate_error"
        async with db.get_session() as session:
            rows = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.decision == "veto_create"
        assert row.decision_reason == "identity_gate_error"
        assert row.decision_payload["mode"] == "enforce"
        assert row.decision_payload["fail_safe"] is True
        assert "gate boom" in row.decision_payload["reasons"][0]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_vector_error_is_technical_terminal_and_is_persisted_for_rollout_metrics(tmp_path, monkeypatch):
    from scripts.inspect.audit_identity_gate_rollout import build_rollout_payload

    db = Database(str(tmp_path / "db.sqlite"))
    try:
        await db.init()
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)

        async def _no_topics(*args, **kwargs):
            return None

        async def _vector_error(*args, **kwargs):
            return su.IdentityVectorEvidence(
                available=False,
                reason="supabase_timeout",
                error="vector_recall_error:TimeoutError:boom",
            )

        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _vector_error)
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/example/vector-low-risk",
            source_text="Концерт 10 июля в Доме искусств",
            title="Концерт",
            date="2026-07-10",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            event_type="концерт",
        )

        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.outcome is su.SmartUpdateTerminalOutcome.FAILED_TECHNICAL
        assert result.event_id is None
        assert result.reason == "identity_gate_uncertain:identity_gate_error"
        async with db.get_session() as session:
            rows = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert len(rows) == 1
        payload = rows[0].decision_payload
        assert rows[0].decision == "veto_create"
        assert payload["fail_safe"] is True
        assert payload["vector"]["error"].startswith("vector_recall_error")
        rollout = build_rollout_payload(db.path, current=date.today(), since_days=14)
        assert rollout["identity_gate_vector_error_count"] == 1
    finally:
        await db.close()
