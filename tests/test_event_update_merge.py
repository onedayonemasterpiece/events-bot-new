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
    collection_adjudication_cached_payload,
    collection_decision_hash_covers,
    deep_merge_collection_decisions,
    project_legacy_audience_decision,
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


def _source(
    *,
    event_id: int = 1,
    source_id: int = 10,
    trust: str = "medium",
    source_text: str = "",
) -> EventSource:
    return EventSource(
        id=source_id,
        event_id=event_id,
        source_type="telegram",
        source_url=f"https://t.me/example/{source_id}",
        source_text=source_text,
        trust_level=trust,
    )


def _payload(
    value: str,
    quote: str,
    reason: str,
    *,
    child_value: str = "unknown",
    child_quote: str = "",
    child_reason: str = "insufficient_evidence",
    family_value: str = "unknown",
    family_quote: str = "",
    family_reason: str = "insufficient_evidence",
    joint_value: str = "unknown",
    joint_quote: str = "",
    joint_reason: str = "insufficient_evidence",
    people=None,
) -> dict:
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": value,
            "evidence_quote": quote,
            "reason_code": reason,
        },
        "child_directed_decision": {
            "value": child_value,
            "confidence": 0.9 if child_value != "unknown" else 0.0,
            "evidence_quote": child_quote,
            "reason_code": child_reason,
        },
        "family_suitable_decision": {
            "value": family_value,
            "confidence": 0.9 if family_value != "unknown" else 0.0,
            "evidence_quote": family_quote,
            "reason_code": family_reason,
        },
        "joint_family_activity_decision": {
            "value": joint_value,
            "confidence": 0.9 if joint_value != "unknown" else 0.0,
            "evidence_quote": joint_quote,
            "reason_code": joint_reason,
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
    assert _apply(
        event,
        unknown,
        source=_source(source_id=12, trust="high"),
        corpus="Билеты уже в продаже.",
        digest="unknown",
        when="2026-08-01T12:00:00",
    )
    assert event.collection_decisions["admission_decision"]["value"] == "confirmed_paid"
    assert event.collection_decisions["evaluation_receipts"][-1]["input_hash"] == "unknown"
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
    assert _apply(
        event, paid, source=_source(source_id=2, trust="low"), corpus="Вход 500 рублей.",
        digest="low-paid", when="2026-08-01T12:00:00",
    )
    assert event.is_free is True
    # Equal trust must be strictly newer.
    assert _apply(
        event, paid, source=_source(source_id=3, trust="high"), corpus="Вход 500 рублей.",
        digest="old-paid", when="2026-08-01T09:00:00",
    )
    assert event.is_free is True
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
    assert _apply(
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


def test_legacy_projection_is_deterministic_and_never_requires_a_second_call():
    family = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        family_value="confirmed",
        family_quote="для всей семьи",
        family_reason="explicit_family_invitation",
    )
    child = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="confirmed",
        child_quote="для детей",
        child_reason="explicit_child_audience",
    )
    adults = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="denied",
        child_quote="только для взрослых",
        child_reason="explicit_adults_only",
        family_value="denied",
        family_quote="только для взрослых",
        family_reason="explicit_adults_only",
    )
    assert project_legacy_audience_decision(family)["value"] == "family"
    assert project_legacy_audience_decision(child)["value"] == "kids"
    assert project_legacy_audience_decision(adults)["value"] == "none"
    assert project_legacy_audience_decision(_payload("unknown", "", "insufficient_evidence"))[
        "value"
    ] == "unknown"
    assert project_legacy_audience_decision(family)["derived_from_facts_v3"] is True


def test_audience_only_apply_keeps_admission_people_and_is_free_untouched():
    corpus = "Вход бесплатный. Приглашаем детей на спектакль. Выступит Анна."
    payload = _payload(
        "confirmed_free",
        "Вход бесплатный",
        "explicit_free_admission",
        child_value="confirmed",
        child_quote="Приглашаем детей",
        child_reason="explicit_child_audience",
        people=[
            {
                "name": "Анна",
                "role": "performer",
                "appearance": "confirmed",
                "origin_scope": "unknown",
                "evidence_quote": "Выступит Анна",
                "origin_evidence_quote": "",
                "reason_code": "explicit_future_participation",
            }
        ],
    )
    event = _event(
        is_free=False,
        decisions={
            "admission_decision": {
                "value": "confirmed_free",
                "input_hash": "older-admission",
            }
        },
    )
    source = _source(source_text=corpus)
    assert apply_collection_decisions(
        event,
        payload,
        source=source,
        source_corpus=corpus,
        input_hash="audience-only",
        reasons=["audience"],
    )
    assert event.is_free is False
    assert event.collection_decisions["admission_decision"]["input_hash"] == "older-admission"
    assert "people_appearances" not in event.collection_decisions
    assert event.collection_decisions["child_directed_decision"]["value"] == "confirmed"
    assert event.collection_decisions["audience_decision"]["value"] == "kids"
    assert event.collection_decisions["audience_decision"]["derived_from_facts_v3"] is True


def test_apply_revalidates_v3_quote_against_persisted_event_source_text():
    candidate_corpus = "Приглашаем детей на спектакль."
    payload = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="confirmed",
        child_quote="Приглашаем детей",
        child_reason="explicit_child_audience",
    )
    event = _event()
    assert not apply_collection_decisions(
        event,
        payload,
        source=_source(source_text="Другая сохранённая версия источника."),
        source_corpus=candidate_corpus,
        input_hash="source-mismatch",
        reasons=["audience"],
    )
    assert event.collection_decisions is None


def test_all_unknown_receipt_is_bounded_cache_and_same_hash_is_noop():
    payload = _payload("unknown", "", "insufficient_evidence")
    event = _event()
    source = _source(source_text="Нейтральное описание события.")
    assert apply_collection_decisions(
        event,
        payload,
        source=source,
        source_corpus=source.source_text or "",
        input_hash="same-all-unknown",
        reasons=["audience"],
    )
    assert collection_decision_hash_covers(
        event.collection_decisions,
        reasons=["audience"],
        input_hash="same-all-unknown",
        source_id=source.id,
    )
    assert collection_adjudication_cached_payload(
        event.collection_decisions,
        input_hash="same-all-unknown",
        source_id=source.id,
    ) == payload
    before = event.collection_decisions
    assert not apply_collection_decisions(
        event,
        payload,
        source=source,
        source_corpus=source.source_text or "",
        input_hash="same-all-unknown",
        reasons=["audience"],
    )
    assert event.collection_decisions == before


def test_official_trust_and_manual_lock_apply_independently_per_v3_key():
    event = _event()
    child_corpus = "Приглашаем детей на спектакль."
    child = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="confirmed",
        child_quote="Приглашаем детей",
        child_reason="explicit_child_audience",
    )
    assert _apply(
        event,
        child,
        source=_source(source_id=11, trust="official", source_text=child_corpus),
        corpus=child_corpus,
        digest="official-child",
        when="2026-08-01T10:00:00",
    )

    mixed_corpus = "Только для взрослых. Приходите всей семьёй."
    mixed = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="denied",
        child_quote="Только для взрослых",
        child_reason="explicit_adults_only",
        family_value="confirmed",
        family_quote="Приходите всей семьёй",
        family_reason="explicit_family_invitation",
    )
    assert _apply(
        event,
        mixed,
        source=_source(source_id=12, trust="high", source_text=mixed_corpus),
        corpus=mixed_corpus,
        digest="high-mixed",
        when="2026-08-02T10:00:00",
    )
    assert event.collection_decisions["child_directed_decision"]["value"] == "confirmed"
    assert event.collection_decisions["family_suitable_decision"]["value"] == "confirmed"

    locked_event = _event(event_id=2)
    assert _apply(
        locked_event,
        child,
        source=_source(event_id=2, source_id=21, trust="low", source_text=child_corpus),
        corpus=child_corpus,
        digest="locked-child",
        when="2026-08-01T10:00:00",
        lock=True,
    )
    denied_corpus = "Только для взрослых."
    denied = _payload(
        "unknown",
        "",
        "insufficient_evidence",
        child_value="denied",
        child_quote="Только для взрослых",
        child_reason="explicit_adults_only",
        family_value="denied",
        family_quote="Только для взрослых",
        family_reason="explicit_adults_only",
    )
    assert _apply(
        locked_event,
        denied,
        source=_source(event_id=2, source_id=22, trust="official", source_text=denied_corpus),
        corpus=denied_corpus,
        digest="official-denied",
        when="2026-08-03T10:00:00",
    )
    assert locked_event.collection_decisions["child_directed_decision"]["value"] == "confirmed"


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
            assert stored.collection_decisions is not None
            assert stored.collection_decisions["evaluation_receipts"][0]["payload"][
                "admission_decision"
            ]["value"] == "unknown"

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
