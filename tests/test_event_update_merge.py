from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

import smart_event_update as sut
from db import Database
from models import Event, EventSource
from smart_event_update import (
    STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
    EventCandidate,
    apply_collection_decisions,
    deep_merge_collection_decisions,
    smart_event_update,
)


def _event(*, event_id: int = 1, is_free: bool = False, decisions=None) -> Event:
    return Event(
        id=event_id,
        title="T",
        description="D",
        date="2026-08-10",
        time="18:00",
        location_name="L",
        source_text="S",
        is_free=is_free,
        collection_decisions=decisions,
    )


def _source(*, event_id: int = 1, source_id: int = 10, trust: str = "medium") -> EventSource:
    return EventSource(
        id=source_id,
        event_id=event_id,
        source_type="telegram",
        source_url=f"https://t.me/example/{source_id}",
        source_text="",
        trust_level=trust,
    )


def _payload(
    value: str,
    quote: str,
    reason: str,
    *,
    audience_value: str = "unknown",
    audience_quote: str = "",
    audience_reason: str = "insufficient_evidence",
    people=None,
) -> dict:
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": value,
            "evidence_quote": quote,
            "reason_code": reason,
        },
        "audience_decision": {
            "value": audience_value,
            "confidence": 0.9 if audience_value != "unknown" else 0.0,
            "evidence_quote": audience_quote,
            "reason_code": audience_reason,
        },
        "people_appearances": people or [],
    }


def _apply(
    event: Event,
    payload: dict,
    *,
    source: EventSource | None = None,
    corpus: str,
    digest: str,
    when: str,
    lock: bool = False,
) -> bool:
    return apply_collection_decisions(
        event,
        payload,
        source=source or _source(event_id=int(event.id or 0)),
        source_corpus=corpus,
        input_hash=digest,
        decided_at=datetime.fromisoformat(when).replace(tzinfo=timezone.utc),
        manual_lock=lock,
    )


def test_admission_materialization_is_bidirectional_and_unknown_preserves_prior():
    event = _event(is_free=True)
    free = _payload("confirmed_free", "Вход бесплатный", "explicit_free_admission")
    assert _apply(event, free, corpus="Вход бесплатный.", digest="free", when="2026-08-01T10:00:00")
    assert event.is_free is True

    paid = _payload("confirmed_paid", "Вход 500 рублей", "explicit_price")
    assert _apply(
        event,
        paid,
        source=_source(source_id=11, trust="high"),
        corpus="Вход 500 рублей.",
        digest="paid",
        when="2026-08-01T11:00:00",
    )
    assert event.is_free is False

    unknown = _payload("unknown", "", "insufficient_evidence")
    before = event.collection_decisions
    assert not _apply(
        event,
        unknown,
        source=_source(source_id=12, trust="high"),
        corpus="Билеты уже в продаже.",
        digest="unknown",
        when="2026-08-01T12:00:00",
    )
    assert event.collection_decisions == before
    assert event.is_free is False

    false_event = _event(event_id=2, is_free=False)
    assert _apply(
        false_event,
        free,
        source=_source(event_id=2, source_id=20, trust="high"),
        corpus="Вход бесплатный.",
        digest="free-2",
        when="2026-08-01T10:00:00",
    )
    assert false_event.is_free is True


def test_admission_conflict_uses_trust_recency_lock_and_same_hash_noop():
    event = _event(is_free=True)
    free = _payload("confirmed_free", "Вход бесплатный", "explicit_free_admission")
    assert _apply(
        event, free, source=_source(source_id=1, trust="high"), corpus="Вход бесплатный.",
        digest="same", when="2026-08-01T10:00:00",
    )
    # Same input hash is an exact no-op even if the timestamp differs.
    assert not _apply(
        event, free, source=_source(source_id=1, trust="high"), corpus="Вход бесплатный.",
        digest="same", when="2026-08-01T11:00:00",
    )
    paid = _payload("confirmed_paid", "Вход 500 рублей", "explicit_price")
    # Lower trust cannot replace a confirmed value.
    assert not _apply(
        event, paid, source=_source(source_id=2, trust="low"), corpus="Вход 500 рублей.",
        digest="low-paid", when="2026-08-01T12:00:00",
    )
    # Equal trust must be strictly newer.
    assert not _apply(
        event, paid, source=_source(source_id=3, trust="high"), corpus="Вход 500 рублей.",
        digest="old-paid", when="2026-08-01T09:00:00",
    )
    assert _apply(
        event, paid, source=_source(source_id=4, trust="high"), corpus="Вход 500 рублей.",
        digest="new-paid", when="2026-08-01T13:00:00",
    )
    assert event.is_free is False

    locked = _event(event_id=2, is_free=True)
    assert _apply(
        locked, free, source=_source(event_id=2, source_id=5, trust="low"),
        corpus="Вход бесплатный.", digest="locked", when="2026-08-01T10:00:00", lock=True,
    )
    assert not _apply(
        locked, paid, source=_source(event_id=2, source_id=6, trust="high"),
        corpus="Вход 500 рублей.", digest="paid-after-lock", when="2026-08-02T10:00:00",
    )
    assert locked.is_free is True


def test_apply_rejects_sibling_or_unpersisted_source():
    event = _event(event_id=7)
    free = _payload("confirmed_free", "Вход бесплатный", "explicit_free_admission")
    assert not _apply(
        event,
        free,
        source=_source(event_id=8, source_id=80),
        corpus="Вход бесплатный.",
        digest="sibling",
        when="2026-08-01T10:00:00",
    )
    assert not _apply(
        event,
        free,
        source=_source(event_id=7, source_id=0),
        corpus="Вход бесплатный.",
        digest="unpersisted",
        when="2026-08-01T10:00:00",
    )
    assert event.collection_decisions is None


def test_people_mention_never_retracts_confirmed_appearance():
    common = {
        "name": "Анна Смирнова",
        "role": "speaker",
        "origin_scope": "unknown",
        "evidence_quote": "Анна Смирнова",
        "origin_evidence_quote": "",
        "reason_code": "explicit_future_participation",
        "source_id": 1,
        "source_url": "https://example/1",
        "source_type": "site",
        "source_trust": "medium",
        "input_hash": "confirmed",
        "policy_version": "static-collection-facts-v1",
        "decided_at": "2026-08-01T10:00:00Z",
        "manual_lock": False,
    }
    existing = {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "people_appearances": [{**common, "appearance": "confirmed"}],
    }
    mention = {
        "people_appearances": [
            {
                **common,
                "appearance": "mentioned",
                "reason_code": "report_only",
                "input_hash": "mention",
                "decided_at": "2026-08-02T10:00:00Z",
            }
        ]
    }
    assert deep_merge_collection_decisions(existing, mention) == existing


@pytest.mark.asyncio
async def test_collection_decisions_db_roundtrip_and_json_reassignment(tmp_path):
    db = Database(str(tmp_path / "facts.sqlite"))
    await db.init()
    try:
        async with db.engine.connect() as conn:
            columns = {row[1] for row in (await conn.execute(text("PRAGMA table_info(event)"))).fetchall()}
        assert "collection_decisions" in columns

        async with db.get_session() as session:
            event = _event(event_id=0)
            event.id = None
            session.add(event)
            await session.commit()
            event_id = int(event.id or 0)
        async with db.get_session() as session:
            event = await session.get(Event, event_id)
            assert event is not None and event.collection_decisions is None
            event.collection_decisions = {
                "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
                "audience_decision": {"value": "family"},
            }
            session.add(event)
            await session.commit()
        async with db.get_session() as session:
            stored = await session.get(Event, event_id)
            assert stored is not None
            assert stored.collection_decisions["audience_decision"]["value"] == "family"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_unknown_keeps_legacy_free_then_ordinary_merge_corrects_paid(
    tmp_path, monkeypatch
):
    async def no_topics(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sut, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(sut, "_classify_topics", no_topics)
    db = Database(str(tmp_path / "smart-facts.sqlite"))
    await db.init()
    try:
        unknown = _payload("unknown", "", "insufficient_evidence")
        created = await smart_event_update(
            db,
            EventCandidate(
                source_type="parser:test",
                source_url="https://example.test/free",
                source_text="Регистрация открыта.",
                title="Точное событие",
                date="2026-12-10",
                time="18:00",
                location_name="Точный зал",
                city="Калининград",
                is_free=True,
                trust_level="medium",
                collection_semantic_decisions=unknown,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert created.status == "created"
        async with db.get_session() as session:
            stored = await session.get(Event, created.event_id)
            assert stored is not None
            assert stored.is_free is True
            assert stored.collection_decisions is None

        paid = _payload("confirmed_paid", "Вход 500 рублей", "explicit_price")
        merged = await smart_event_update(
            db,
            EventCandidate(
                source_type="parser:test",
                source_url="https://example.test/paid",
                source_text="Вход 500 рублей.",
                title="Точное событие",
                date="2026-12-10",
                time="18:00",
                location_name="Точный зал",
                city="Калининград",
                is_free=False,
                ticket_price_min=500,
                ticket_price_max=500,
                trust_level="high",
                collection_semantic_decisions=paid,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert merged.status == "merged"
        assert merged.event_id == created.event_id
        async with db.get_session() as session:
            stored = await session.get(Event, created.event_id)
            assert stored is not None
            assert stored.is_free is False
            decision = stored.collection_decisions["admission_decision"]
            assert decision["value"] == "confirmed_paid"
            assert decision["source_url"] == "https://example.test/paid"
    finally:
        await db.close()


def test_alembic_revision_adds_nullable_json_column(monkeypatch):
    calls = []
    fake_op = types.SimpleNamespace(
        add_column=lambda table, column: calls.append((table, column)),
        drop_column=lambda *_args, **_kwargs: None,
    )
    fake_alembic = types.ModuleType("alembic")
    fake_alembic.op = fake_op
    monkeypatch.setitem(sys.modules, "alembic", fake_alembic)
    revision_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260801_static_collection_facts.py"
    )
    spec = importlib.util.spec_from_file_location("static_collection_facts_revision", revision_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.down_revision == "20260731_festival_web_research"
    module.upgrade()
    assert len(calls) == 1
    table, column = calls[0]
    assert table == "event"
    assert column.name == "collection_decisions"
    assert column.nullable is True
    assert column.type.__class__.__name__.upper() == "JSON"
