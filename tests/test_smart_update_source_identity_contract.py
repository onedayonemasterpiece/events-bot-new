from __future__ import annotations

import sqlite3
import sys
import types

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

import smart_event_update as su
from db import Database
from models import Event, EventSource, FestivalQueueItem
from smart_event_update import (
    EventCandidate,
    SmartUpdateIntent,
    SmartUpdateTerminalOutcome,
)
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
    ticket_48801 = canonicalize_identity_url(
        "https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/48801/2026-08-09/14:00:00",
        preserve_ticket_fragment=True,
    )
    assert ticket_48801 == canonicalize_identity_url(
        "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/48801/2026-08-09/14:00:00",
        preserve_ticket_fragment=True,
    )
    ticket_48636 = canonicalize_identity_url(
        "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/48636/2026-08-09/17:00:00",
        preserve_ticket_fragment=True,
    )
    assert ticket_48801 != ticket_48636


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
    assert unavailable.action is MergeIdentityAction.AUTOMATIC_RESOLUTION_REQUIRED
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
    assert context_claim.action is MergeIdentityAction.AUTOMATIC_RESOLUTION_REQUIRED
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
    assert blocked.action is MergeIdentityAction.AUTOMATIC_RESOLUTION_REQUIRED
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
        assert {
            "canonical_source_url",
            "source_role",
            "source_fingerprint",
            "candidate_key",
            "occurrence_key",
            "smart_update_candidate_id",
        } <= cols
        assert "ux_event_source_identity_occurrence" in indexes
        assert "ux_event_source_identity_canonical_legacy" in indexes
        assert "ux_event_source_smart_candidate" in indexes
        async with fresh.raw_conn() as conn:
            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(
                    "INSERT INTO event_source(event_id,source_type,source_url,source_role) "
                    "VALUES(1,'telegram','https://t.me/x/1','invalid')"
                )
            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute(
                    "INSERT INTO event_source(event_id,source_type,source_url,source_role) "
                    "VALUES(1,'telegram','https://t.me/x/1','identity_bearing')"
                )
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
    try:
        with pytest.raises(RuntimeError, match="event_source_legacy_identity_conflict"):
            await conflicted.init()
    finally:
        await conflicted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_type", "source_url"),
    [
        ("telegram", "https://telegram.me/noop_test/44?utm_source=test"),
        ("vk", "https://m.vk.com/feed?w=wall-123_456&utm_source=test"),
    ],
)
async def test_exact_input_packet_returns_noop_before_llm_or_writes(
    tmp_path, monkeypatch, source_type: str, source_url: str
) -> None:
    db = Database(str(tmp_path / "noop.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        candidate_values = dict(
            source_type=source_type,
            source_url=source_url,
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
        assert source.canonical_source_url == canonicalize_identity_url(source_url)
        assert source.source_role == "identity_bearing"
        assert source.source_fingerprint
        assert event_count == 1
    finally:
        await db.close()


def test_packet_fingerprint_ignores_provider_metrics_but_detects_real_edit() -> None:
    base = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/edited_packet/7?single=1",
        source_text="Анонс события\r\nНачало в 19:00",
        title="Событие",
        date="2026-09-02",
        time="19:00",
        metrics={"request_id": "first", "prompt_tokens": 99},
        collection_semantic_decisions={"provider_output": "first"},
    )
    retry = EventCandidate(
        source_type="telegram",
        source_url="https://telegram.me/s/EDITED_PACKET/7?utm_source=x",
        source_text="Анонс события\nНачало в 19:00",
        title="Событие",
        date="2026-09-02",
        time="19:00",
        metrics={"request_id": "second", "prompt_tokens": 501},
        collection_semantic_decisions={"provider_output": "second"},
    )
    edited = EventCandidate(
        source_type="telegram",
        source_url=base.source_url,
        source_text="Анонс события\nНачало перенесено на 20:00",
        title="Событие",
        date="2026-09-02",
        time="20:00",
    )
    assert input_packet_fingerprint(base) == input_packet_fingerprint(retry)
    assert input_packet_fingerprint(base) != input_packet_fingerprint(edited)


@pytest.mark.asyncio
async def test_edited_packet_same_url_continues_real_update(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "edited-packet.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        source_url = "https://t.me/edited_packet/7?single=1"
        first_packet = EventCandidate(
            source_type="telegram",
            source_url=source_url,
            source_text="Концерт 2 сентября в 19:00 в Доме искусств.",
            title="Событие с редактируемым анонсом",
            date="2026-09-02",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            event_type="концерт",
        )
        first = await su.smart_event_update(db, first_packet, schedule_tasks=False)
        assert first.status == "created"

        edited_packet = EventCandidate(
            source_type="telegram",
            source_url="https://telegram.me/s/EDITED_PACKET/7?utm_source=edit",
            source_text="Концерт 2 сентября в 19:00 в Доме искусств. Добавлена программа вечера.",
            title="Событие с редактируемым анонсом",
            date="2026-09-02",
            time="19:00",
            location_name="Дом искусств",
            city="Калининград",
            event_type="концерт",
        )
        edited_fingerprint = input_packet_fingerprint(edited_packet)
        second = await su.smart_event_update(db, edited_packet, schedule_tasks=False)

        assert second.status == "merged"
        assert second.event_id == first.event_id
        assert second.status not in {"noop_exact_source_replay", "skipped_same_source_url"}
        async with db.get_session() as session:
            source = (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == int(first.event_id or 0))
                )
            ).scalar_one()
        assert source.source_fingerprint == edited_fingerprint
        assert source.source_text == edited_packet.source_text
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_tretyakov_direct_ticket_identity_cannot_cross_bind_screenings(tmp_path) -> None:
    db = Database(str(tmp_path / "tretyakov-bindings.sqlite"))
    await db.init()
    try:
        url_14 = canonicalize_identity_url(
            "https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/48801/2026-08-09/14:00:00"
        )
        url_17 = canonicalize_identity_url(
            "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/48636/2026-08-09/17:00:00"
        )
        assert url_14 and url_17 and url_14 != url_17
        async with db.get_session() as session:
            films = []
            for title, time in (("Право женщин на море", "14:00"), ("Кагуя", "17:00")):
                event = Event(
                    title=title,
                    description="Кинопоказ",
                    source_text="Официальный билет",
                    date="2026-08-09",
                    time=time,
                    location_name="Третьяковская галерея",
                )
                session.add(event)
                await session.flush()
                films.append(event)
            session.add(
                EventSource(
                    event_id=int(films[0].id), source_type="parser:tretyakov",
                    source_url=url_14, canonical_source_url=url_14,
                    source_role="identity_bearing",
                )
            )
            session.add(
                EventSource(
                    event_id=int(films[1].id), source_type="parser:tretyakov",
                    source_url=url_17, canonical_source_url=url_17,
                    source_role="identity_bearing",
                )
            )
            await session.commit()

        async with db.get_session() as session:
            session.add(
                EventSource(
                    event_id=int(films[1].id), source_type="parser:tretyakov",
                    source_url=url_14 + "?variant=wrong", canonical_source_url=url_14,
                    source_role="identity_bearing",
                )
            )
            with pytest.raises(IntegrityError):
                await session.commit()
            await session.rollback()
    finally:
        await db.close()


def test_pianissimo_concert_and_long_exhibition_are_review_not_merge() -> None:
    verdict = build_merge_identity_gate_verdict(
        {
            "title": "Фестиваль Pianissimo: Константин Хачикян",
            "date": "2026-08-07", "time": "20:00", "event_type": "концерт",
            "ticket_link": "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-08-07/20:00:00",
        },
        {
            "id": 3216, "title": "Великие учителя", "date": "2026-04-09",
            "end_date": "2026-09-27", "time": "", "event_type": "выставка",
        },
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge", "relation": "same_event", "confidence": 0.99,
            "reason_code": "same_venue",
        },
        blocking_conflicts=["title", "occurrence", "event_type", "ticket_link"],
    )
    assert verdict.action is MergeIdentityAction.AUTOMATIC_RESOLUTION_REQUIRED
    assert verdict.should_skip_side_effects


@pytest.mark.asyncio
async def test_explicit_context_intent_supports_multi_event_carrier_and_exact_replay(tmp_path) -> None:
    db = Database(str(tmp_path / "ambiguous-replay.sqlite"))
    await db.init()
    try:
        event_ids: list[int] = []
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
                event_ids.append(int(event.id))
            await session.commit()

        results = []
        for event_id in event_ids:
            results.append(
                await su.smart_event_update(
                    db,
                    EventCandidate(
                        intent=SmartUpdateIntent.ATTACH_CONTEXT,
                        target_event_id=event_id,
                        source_type="telegram",
                        source_url="https://t.me/shared_roundup/9",
                        source_text="Общий дайджест",
                        occurrence_key=f"context:{event_id}",
                    ),
                    schedule_tasks=False,
                )
            )
        assert all(
            result.outcome is SmartUpdateTerminalOutcome.MERGED
            for result in results
        )
        replay = await su.smart_event_update(
            db,
            EventCandidate(
                intent=SmartUpdateIntent.ATTACH_CONTEXT,
                target_event_id=event_ids[0],
                source_type="telegram",
                source_url="https://t.me/shared_roundup/9",
                source_text="Общий дайджест",
                occurrence_key=f"context:{event_ids[0]}",
            ),
            schedule_tasks=False,
        )
        assert replay.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        async with db.get_session() as session:
            assert int(
                await session.scalar(select(func.count()).select_from(EventSource))
            ) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_distinct_create_enqueues_festival_only_after_identity_acceptance(
    tmp_path, monkeypatch
) -> None:
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

        enqueue_event_counts: list[int] = []

        async def _enqueue(*_args, **_kwargs):
            async with db.get_session() as session:
                enqueue_event_counts.append(
                    int(await session.scalar(select(func.count()).select_from(Event)))
                )
            return types.SimpleNamespace(id=1)

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
        assert result.outcome is SmartUpdateTerminalOutcome.CREATED
        assert result.event_id is not None
        assert result.event_id != int(event.id)
        assert result.diagnostic_event_id == int(event.id)
        assert result.reason and result.reason.startswith("create_distinct:")
        assert enqueue_event_counts == [2]
        async with db.get_session() as session:
            original = await session.get(Event, int(event.id))
            assert original is not None
            assert original.title == "Лекция"
            assert int(
                (await session.execute(select(func.count(FestivalQueueItem.id)))).scalar_one()
            ) == 0
    finally:
        await db.close()
