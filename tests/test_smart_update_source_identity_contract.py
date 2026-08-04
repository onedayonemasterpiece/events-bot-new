from __future__ import annotations

import sqlite3
import sys
import types

import pytest
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import Event, EventSource, FestivalQueueItem
from smart_event_update import EventCandidate
from smart_update_identity import (
    IdentityGateMode,
    MergeIdentityAction,
    canonicalize_identity_url,
    build_merge_identity_gate_verdict,
    input_packet_fingerprint,
)


async def _no_topics(*_args, **_kwargs):
    return None


def test_identity_url_canonicalizer_normalizes_social_variants_and_preserves_ticket_fragment() -> None:
    assert canonicalize_identity_url(
        "https://telegram.me/s/KldEvents/42?utm_source=x&single=1"
    ) == "https://t.me/kldevents/42"
    assert canonicalize_identity_url(
        "https://m.vk.com/feed?w=wall-123_456&utm_campaign=x"
    ) == "https://vk.com/wall-123_456"
    assert canonicalize_identity_url(
        "https://vk.ru/wall-123_456?from=feed&yclid=tracker"
    ) == "https://vk.com/wall-123_456"
    assert canonicalize_identity_url(
        "https://tickets.tretyakovgallery.ru/events/abc?utm_medium=social#/buy",
        preserve_ticket_fragment=True,
    ) == "https://tickets.tretyakovgallery.ru/events/abc#/buy"


def test_merge_gate_fails_closed_without_valid_llm_and_context_cannot_assert_same_event() -> None:
    candidate = {
        "title": "Лекция",
        "date": "2026-09-01",
        "time": "19:00",
        "source_url": "https://t.me/context/2",
        "source_role": "context_only",
    }
    existing = {
        "id": 10,
        "title": "Лекция",
        "date": "2026-09-01",
        "time": "19:00",
        "source_url": "https://t.me/primary/1",
    }

    unavailable = build_merge_identity_gate_verdict(
        candidate,
        existing,
        mode=IdentityGateMode.ENFORCE,
        llm_data=None,
    )
    assert unavailable.action is MergeIdentityAction.REVIEW_REQUIRED
    assert unavailable.should_skip_side_effects

    context_claim = build_merge_identity_gate_verdict(
        candidate,
        existing,
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
        },
    )
    assert context_claim.action is MergeIdentityAction.REVIEW_REQUIRED
    assert context_claim.should_skip_side_effects
    assert context_claim.candidate is not None
    assert context_claim.candidate.source_url is None

    blocked = build_merge_identity_gate_verdict(
        {**candidate, "source_role": "identity_bearing"},
        existing,
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
        },
        blocking_conflicts=["ticket_link"],
    )
    assert blocked.action is MergeIdentityAction.REVIEW_REQUIRED
    assert blocked.should_skip_side_effects


@pytest.mark.asyncio
async def test_event_source_schema_is_additive_and_unique_index_is_conflict_safe(tmp_path) -> None:
    fresh = Database(str(tmp_path / "fresh.sqlite"))
    await fresh.init()
    try:
        async with fresh.raw_conn() as conn:
            cols = {
                row[1]
                for row in await (await conn.execute("PRAGMA table_info(event_source)")).fetchall()
            }
            indexes = {
                row[1]
                for row in await (await conn.execute("PRAGMA index_list(event_source)")).fetchall()
            }
        assert {"canonical_source_url", "source_role", "source_fingerprint"} <= cols
        assert "ux_event_source_event_canonical" in indexes
        assert "ux_event_source_identity_canonical" in indexes
    finally:
        await fresh.close()

    conflict_path = tmp_path / "conflict.sqlite"
    raw = sqlite3.connect(conflict_path)
    raw.executescript(
        """
        CREATE TABLE event_source(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            source_url TEXT NOT NULL,
            canonical_source_url TEXT,
            source_role TEXT,
            source_fingerprint TEXT,
            source_chat_username TEXT,
            source_chat_id INTEGER,
            source_message_id INTEGER,
            source_text TEXT,
            imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            trust_level TEXT,
            UNIQUE(event_id, source_url)
        );
        INSERT INTO event_source(event_id,source_type,source_url,canonical_source_url,source_role)
        VALUES
          (1,'telegram','https://t.me/x/1','https://t.me/x/1','identity_bearing'),
          (2,'telegram','https://telegram.me/x/1','https://t.me/x/1','identity_bearing');
        """
    )
    raw.commit()
    raw.close()

    conflicted = Database(str(conflict_path))
    await conflicted.init()
    try:
        async with conflicted.raw_conn() as conn:
            indexes = {
                row[1]
                for row in await (await conn.execute("PRAGMA index_list(event_source)")).fetchall()
            }
            roles = [
                row[0]
                for row in await (
                    await conn.execute("SELECT source_role FROM event_source ORDER BY id")
                ).fetchall()
            ]
        assert "ux_event_source_identity_canonical" not in indexes
        assert roles == ["identity_bearing", "identity_bearing"]
    finally:
        await conflicted.close()


@pytest.mark.asyncio
async def test_exact_input_packet_returns_noop_before_llm_or_writes(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "noop.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        candidate_values = dict(
            source_type="telegram",
            source_url="https://telegram.me/noop_test/44?utm_source=test",
            source_text="Концерт 1 сентября в 19:00 в Доме искусств.",
            title="Концерт",
            date="2026-09-01",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            event_type="концерт",
        )
        first = await su.smart_event_update(
            db,
            EventCandidate(**candidate_values),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert first.status == "created"

        async def _llm_must_not_run(*_args, **_kwargs):
            raise AssertionError("noop must return before LLM")

        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _llm_must_not_run)
        second = await su.smart_event_update(
            db,
            EventCandidate(**candidate_values),
            check_source_url=False,
            schedule_tasks=True,
        )
        assert second.status == "noop_exact_source_replay"
        assert second.event_id == first.event_id

        async with db.get_session() as session:
            source = (await session.execute(select(EventSource))).scalar_one()
            event_count = int((await session.execute(select(func.count(Event.id)))).scalar_one())
        assert source.canonical_source_url == "https://t.me/noop_test/44"
        assert source.source_role == "identity_bearing"
        assert source.source_fingerprint
        assert event_count == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exact_context_replay_with_multiple_bindings_requires_review(tmp_path) -> None:
    db = Database(str(tmp_path / "ambiguous-replay.sqlite"))
    await db.init()
    try:
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/shared_roundup/9",
            source_text="Общий дайджест",
            source_role="context_only",
        )
        fingerprint = input_packet_fingerprint(candidate)
        canonical = canonicalize_identity_url(candidate.source_url)
        async with db.get_session() as session:
            for idx in (1, 2):
                event = Event(
                    title=f"Событие {idx}",
                    description="Описание",
                    source_text="Источник",
                    date="2026-09-01",
                    time="19:00",
                    location_name="Музей",
                )
                session.add(event)
                await session.flush()
                session.add(
                    EventSource(
                        event_id=int(event.id),
                        source_type="telegram",
                        source_url=f"{candidate.source_url}?item={idx}",
                        canonical_source_url=canonical,
                        source_role="context_only",
                        source_fingerprint=fingerprint,
                    )
                )
            await session.commit()

        result = await su.smart_event_update(db, candidate, schedule_tasks=False)
        assert result.status == "review_required"
        assert result.reason == "source_binding_conflict"
        assert result.event_id is None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_rejected_merge_does_not_enqueue_festival_side_effect(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "festival-gate.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            event = Event(
                title="Лекция",
                description="Описание лекции",
                source_text="Исходный текст лекции",
                date="2026-09-01",
                time="19:00",
                location_name="Музей",
                city="Калининград",
                event_type="лекция",
            )
            session.add(event)
            await session.commit()
            await session.refresh(event)

        async def _anchor(*_args, **_kwargs):
            return event

        async def _gate(*_args, **_kwargs):
            return {
                "action": "skip_merge_side_effects",
                "relation": "festival_context_sibling",
                "confidence": 0.98,
                "reason_code": "festival_sibling_not_same_event",
            }

        async def _eventness(*_args, **_kwargs):
            return "event", 0.99, "test"

        class _Decision:
            context = "event_with_festival"
            festival = "Фестиваль"
            festival_full = "Фестиваль"
            dedup_links = []
            signals = ["festival"]

        async def _enqueue(*_args, **_kwargs):
            raise AssertionError("festival queue write happened before identity acceptance")

        fake_queue = types.ModuleType("festival_queue")
        fake_queue.detect_festival_context = lambda **_kwargs: _Decision()
        fake_queue.enqueue_festival_source = _enqueue
        monkeypatch.setitem(sys.modules, "festival_queue", fake_queue)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/festival/5",
            source_text="Лекция в программе фестиваля",
            title="Другая лекция",
            date="2026-09-01",
            time="19:00",
            location_name="Музей",
            city="Калининград",
            event_type="лекция",
            festival="Фестиваль",
            festival_context="event_with_festival",
        )
        result = await su.smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )
        assert result.status == "skipped_identity_gate"
        async with db.get_session() as session:
            assert int(
                (await session.execute(select(func.count(FestivalQueueItem.id)))).scalar_one()
            ) == 0
    finally:
        await db.close()
