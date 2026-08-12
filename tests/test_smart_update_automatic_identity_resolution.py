from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import Event, EventSource
from smart_event_update import EventCandidate, SmartUpdateTerminalOutcome
from smart_update_identity import IdentityGateMode
from smart_update_state import (
    IdentityDistinctReason,
    RetryReason,
    smart_update_funnel_counts,
)


REPLAY_FIXTURE = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-08-10-smart-update-identity-terminal-loss"
    / "multi_event_carrier.json"
)


async def _no_topics(*_args, **_kwargs):
    return None


async def _eventness(*_args, **_kwargs):
    return "event", 0.99, "fixture"


async def _seed_event(db: Database, *, ticket_link: str | None = None) -> int:
    async with db.get_session() as session:
        event = Event(
            title="Каноническое событие",
            description="Исходное описание",
            source_text="Исходный источник",
            date="2099-09-10",
            time="19:00",
            location_name="Музей",
            city="Калининград",
            event_type="лекция",
            ticket_link=ticket_link,
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)
        return int(event.id)


def _candidate(*, ticket_link: str | None = None) -> EventCandidate:
    return EventCandidate(
        source_type="telegram",
        source_url="https://t.me/automatic_identity/10",
        source_text="Отдельная программа 10 сентября в 19:00.",
        raw_excerpt="Отдельная программа.",
        title="Каноническое событие",
        date="2099-09-10",
        time="19:00",
        location_name="Музей",
        city="Калининград",
        event_type="лекция",
        ticket_link=ticket_link,
        producer_ordinal=0,
    )


async def _make_due(db: Database) -> dict[str, int]:
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE smart_update_candidate_state SET next_retry_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    return await su.retry_due_smart_update_candidates(db, limit=5)


@pytest.mark.asyncio
async def test_sanitized_carrier_replay_covers_exact_edited_and_child_identity(
    tmp_path,
    monkeypatch,
) -> None:
    payload = json.loads(REPLAY_FIXTURE.read_text(encoding="utf-8"))
    db = Database(str(tmp_path / "carrier-replay.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        candidates = [
            EventCandidate(
                source_type=payload["source_type"],
                source_url=payload["source_url"],
                source_text=item["source_text"],
                title=item["title"],
                date=item["date"],
                time=item["time"],
                location_name="Музей",
                city="Калининград",
                event_type="экскурсия",
                producer_ordinal=int(item["producer_ordinal"]),
            )
            for item in payload["occurrences"]
        ]
        first = await su.smart_event_update(
            db,
            candidates[0],
            check_source_url=False,
            schedule_tasks=False,
        )
        second = await su.smart_event_update(
            db,
            candidates[1],
            check_source_url=False,
            schedule_tasks=False,
        )
        assert first.outcome is SmartUpdateTerminalOutcome.CREATED
        assert second.outcome is SmartUpdateTerminalOutcome.CREATED
        assert first.event_id != second.event_id

        exact = await su.smart_event_update(
            db,
            EventCandidate(
                source_type=payload["source_type"],
                source_url=payload["source_url"],
                source_text=payload["occurrences"][0]["source_text"],
                title=payload["occurrences"][0]["title"],
                date=payload["occurrences"][0]["date"],
                time=payload["occurrences"][0]["time"],
                location_name="Музей",
                city="Калининград",
                event_type="экскурсия",
                producer_ordinal=0,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert exact.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        assert exact.event_id == first.event_id

        edited = await su.smart_event_update(
            db,
            EventCandidate(
                source_type=payload["source_type"],
                source_url=payload["source_url"],
                source_text=(
                    payload["occurrences"][0]["source_text"]
                    + " Добавлена подтвержденная деталь программы."
                ),
                title=payload["occurrences"][0]["title"],
                date=payload["occurrences"][0]["date"],
                time=payload["occurrences"][0]["time"],
                location_name="Музей",
                city="Калининград",
                event_type="экскурсия",
                producer_ordinal=0,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert edited.outcome is SmartUpdateTerminalOutcome.MERGED
        assert edited.event_id == first.event_id
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation",
    ["related_but_distinct", "festival_context_sibling", "unsafe_to_merge"],
)
async def test_known_nonmerge_identity_relations_create_distinct_automatically(
    tmp_path,
    monkeypatch,
    relation: str,
) -> None:
    db = Database(str(tmp_path / f"{relation}.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)

        async def _anchor(db_arg, _candidate_value):
            async with db_arg.get_session() as session:
                return await session.get(Event, existing_id)

        calls = {"eventness": 0, "merge_gate": 0}

        async def _gate(*_args, **_kwargs):
            calls["merge_gate"] += 1
            return {
                "action": "skip_merge_side_effects",
                "relation": relation,
                "confidence": 0.99,
                "reason_code": f"test_{relation}",
                "reason": "known separate event",
                "blocking_conflicts": ["identity"],
                "allowed_fields": [],
            }

        async def _counted_eventness(*_args, **_kwargs):
            calls["eventness"] += 1
            return await _eventness(*_args, **_kwargs)

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.ENFORCE,
        )
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(
            su,
            "_candidate_needs_llm_eventness_review",
            lambda *_args, **_kwargs: True,
        )
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _counted_eventness)

        result = await su.smart_event_update(
            db,
            _candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert result.outcome is SmartUpdateTerminalOutcome.CREATED
        assert result.event_id is not None
        assert result.event_id != existing_id
        assert result.diagnostic_event_id == existing_id
        assert result.reason and result.reason.startswith(f"create_distinct:{relation}:")
        assert calls == {"eventness": 1, "merge_gate": 1}
        async with db.get_session() as session:
            events = (await session.execute(select(Event).order_by(Event.id))).scalars().all()
        assert len(events) == 2
        assert events[0].id == existing_id
        assert events[0].title == "Каноническое событие"
        assert events[1].title == "Каноническое событие"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_specific_ticket_occurrence_conflict_creates_distinct(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "ticket-occurrence.sqlite"))
    await db.init()
    try:
        existing_ticket = (
            "https://kaliningrad.tretyakovgallery.ru/tickets/"
            "#/buy/event/48636/2099-09-10/19:00:00"
        )
        candidate_ticket = (
            "https://kaliningrad.tretyakovgallery.ru/tickets/"
            "#/buy/event/48801/2099-09-10/19:00:00"
        )
        existing_id = await _seed_event(db, ticket_link=existing_ticket)

        async def _anchor(db_arg, _candidate_value):
            async with db_arg.get_session() as session:
                return await session.get(Event, existing_id)

        async def _false_same(*_args, **_kwargs):
            return {
                "action": "allow_merge",
                "relation": "same_event",
                "confidence": 0.99,
                "reason_code": "same_event_update",
                "reason": "synthetic false same",
                "blocking_conflicts": [],
                "allowed_fields": ["source"],
            }

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.ENFORCE,
        )
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _false_same)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        result = await su.smart_event_update(
            db,
            _candidate(ticket_link=candidate_ticket),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert result.outcome is SmartUpdateTerminalOutcome.CREATED
        assert result.event_id is not None
        assert result.event_id != existing_id
        assert result.diagnostic_event_id == existing_id
        assert result.reason and "specific_ticket_occurrence_conflict" in result.reason
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_late_incoherent_merge_rolls_back_and_creates_distinct(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "late-incoherent.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)
        merge_calls = 0

        async def _anchor(db_arg, _candidate_value):
            async with db_arg.get_session() as session:
                return await session.get(Event, existing_id)

        async def _incoherent_merge(*_args, **_kwargs):
            nonlocal merge_calls
            merge_calls += 1
            return {
                "title": "Совершенно чужой фестиваль",
                "description": "Чужое описание",
                "added_facts": [],
                "duplicate_facts": [],
                "conflict_facts": [],
                "skipped_conflicts": [],
            }

        candidate = _candidate()
        candidate.source_url = "https://t.me/automatic_identity/incoherent"
        candidate.title = "Независимая новая лекция"
        candidate.source_text += " Дополнительные сведения."
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF
        )
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_single_candidate_auto_match_ok", lambda *_a, **_k: True)
        monkeypatch.setattr(su, "_llm_merge_event", _incoherent_merge)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        result = await su.smart_event_update(
            db, candidate, check_source_url=False, schedule_tasks=False
        )

        assert result.outcome is SmartUpdateTerminalOutcome.CREATED
        assert result.event_id not in {None, existing_id}
        assert result.diagnostic_event_id == existing_id
        assert result.identity_distinct_reason is IdentityDistinctReason.INCOHERENT_MERGE
        assert merge_calls == 1
        async with db.get_session() as session:
            original = await session.get(Event, existing_id)
            assert original is not None
            assert original.title == "Каноническое событие"
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_identity_llm_unavailable_remains_technical_retry_past_semantic_budget(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "llm-unavailable.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)

        async def _anchor(db_arg, _candidate_value):
            async with db_arg.get_session() as session:
                return await session.get(Event, existing_id)

        async def _unavailable(*_args, **_kwargs):
            return None

        monkeypatch.setenv("SMART_UPDATE_MAX_ATTEMPTS", "3")
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.ENFORCE,
        )
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _unavailable)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        first = await su.smart_event_update(
            db,
            _candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert first.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
        assert first.reason == "merge_identity_llm_unavailable"
        assert first.retry_reason is RetryReason.IDENTITY_TECHNICAL_FAILURE
        for _ in range(4):
            assert (await _make_due(db))["RETRY_SCHEDULED"] == 1
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_typed_semantic_unknown_creates_distinct_after_bounded_adjudication(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "semantic-unknown.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)
        calls = 0

        async def _anchor(db_arg, _candidate_value):
            async with db_arg.get_session() as session:
                return await session.get(Event, existing_id)

        async def _unknown(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            return {
                "action": "automatic_resolution_required",
                "relation": "unknown",
                "confidence": 0.4,
                "reason_code": "semantic_abstention",
                "reason": "identity cannot be proven",
                "blocking_conflicts": [],
                "allowed_fields": [],
            }

        monkeypatch.setenv("SMART_UPDATE_MAX_ATTEMPTS", "3")
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.ENFORCE
        )
        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _anchor)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _unknown)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        first = await su.smart_event_update(
            db, _candidate(), check_source_url=False, schedule_tasks=False
        )
        assert first.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
        assert first.retry_reason is RetryReason.IDENTITY_SEMANTIC_UNKNOWN
        assert (await _make_due(db))["RETRY_SCHEDULED"] == 1
        final = await _make_due(db)
        assert final["CREATED"] == 1
        assert calls == 2
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 2
        async with db.raw_conn() as conn:
            row = await (
                await conn.execute(
                    "SELECT reason FROM smart_update_candidate_state ORDER BY id DESC LIMIT 1"
                )
            ).fetchone()
        assert row == (IdentityDistinctReason.UNKNOWN_AFTER_BOUNDED_ADJUDICATION.value,)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_widened_dedup_invalid_schema_is_durable_retry_not_create(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "dedup-invalid-schema.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)

        async def _invalid_schema(*_args, **_kwargs):
            return None

        def _blocked(_candidate_value, events, _posters):
            return list(events[:1])

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_dedup_adjudicator_block_candidates", _blocked)
        monkeypatch.setattr(su, "_llm_dedup_adjudicator", _invalid_schema)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        candidate = _candidate()
        candidate.source_url = "https://t.me/automatic_identity/invalid-schema"
        candidate.location_name = "Другая подтвержденная площадка"
        result = await su.smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
        assert result.event_id is None
        assert result.reason == "dedup_adjudicator_unavailable"
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 1
        counts = await smart_update_funnel_counts(db)
        assert counts["RETRY_SCHEDULED"] == 1
        assert counts["attempt_starts"] == counts["attempt_terminals"] == 1
        assert counts["terminal_unresolved"] == 0
        assert existing_id > 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_facade_db_exception_persists_durable_retry_candidate(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "facade-db-exception.sqlite"))
    await db.init()
    try:
        async def _db_failure(*_args, **_kwargs):
            raise RuntimeError("synthetic database write failure")

        monkeypatch.setattr(su, "_smart_event_update_impl", _db_failure)
        result = await su.smart_event_update(
            db,
            _candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
        assert result.event_id is None
        assert result.reason == "smart_update_processing_error"
        counts = await smart_update_funnel_counts(db)
        assert counts["candidates_total"] == 1
        assert counts["RETRY_SCHEDULED"] == 1
        assert counts["attempt_starts"] == counts["attempt_terminals"] == 1
        assert counts["attempt_unresolved"] == 0
        async with db.raw_conn() as conn:
            cursor = await conn.execute(
                "SELECT current_outcome,reason,candidate_payload "
                "FROM smart_update_candidate_state"
            )
            row = await cursor.fetchone()
            await cursor.close()
        assert row is not None
        assert row[0:2] == ("RETRY_SCHEDULED", "smart_update_processing_error")
        assert "automatic_identity" in str(row[2])
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_legacy_unclassified_source_is_upgraded_after_accepted_match(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "legacy-source.sqlite"))
    await db.init()
    try:
        source_url = "https://t.me/legacy_source/5"
        async with db.get_session() as session:
            event = Event(
                title="Лекция",
                description="Старое описание",
                source_text="Старый текст",
                source_post_url=source_url,
                date="2099-09-10",
                time="19:00",
                location_name="Музей",
                city="Калининград",
                event_type="лекция",
            )
            session.add(event)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(event.id),
                    source_type="telegram",
                    source_url=source_url,
                    source_role=None,
                    canonical_source_url=None,
                )
            )
            await session.commit()
            event_id = int(event.id)

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)
        result = await su.smart_event_update(
            db,
            EventCandidate(
                source_type="telegram",
                source_url=source_url,
                source_text="Новый подтвержденный текст лекции.",
                title="Лекция",
                date="2099-09-10",
                time="19:00",
                location_name="Музей",
                city="Калининград",
                event_type="лекция",
                producer_ordinal=0,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert result.outcome is SmartUpdateTerminalOutcome.MERGED
        assert result.event_id == event_id
        async with db.get_session() as session:
            sources = (await session.execute(select(EventSource))).scalars().all()
        assert len(sources) == 1
        assert sources[0].source_role == "identity_bearing"
        assert sources[0].candidate_key
        assert sources[0].occurrence_key.startswith("structured:")
        assert sources[0].occurrence_key.endswith(":ordinal:0")
        assert sources[0].smart_update_candidate_id is not None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_final_transaction_probe_merges_authoritative_race_in_same_operation(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "final-probe-race.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)
        async with db.get_session() as session:
            authoritative = await session.get(Event, existing_id)
        assert authoritative is not None

        probe_calls = 0

        def _race_probe(_candidate_value, _events):
            nonlocal probe_calls
            probe_calls += 1
            # The first shortlist read missed the row; the transaction-local
            # read and its authoritative reload both identify the same Event.
            return None if probe_calls == 1 else authoritative

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_pre_create_duplicate_probe", _race_probe)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        result = await su.smart_event_update(
            db,
            EventCandidate(
                source_type="telegram",
                source_url="https://t.me/automatic_identity/99",
                source_text="Новый источник для события, обнаруженного в финальной транзакции.",
                title="Независимо извлечённое название",
                date="2099-09-10",
                time="19:00",
                location_name="Музей",
                city="Калининград",
                event_type="лекция",
                producer_ordinal=0,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.outcome is SmartUpdateTerminalOutcome.MERGED
        assert result.event_id == existing_id
        assert result.reason == "final_transaction_duplicate_probe"
        assert probe_calls == 3
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 1
            sources = (await session.execute(select(EventSource))).scalars().all()
        assert len(sources) == 1
        assert sources[0].event_id == existing_id
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_final_transaction_probe_creates_distinct_when_reload_disproves_match(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "final-probe-distinct.sqlite"))
    await db.init()
    try:
        existing_id = await _seed_event(db)
        async with db.get_session() as session:
            stale_match = await session.get(Event, existing_id)
        assert stale_match is not None

        probe_calls = 0

        def _changing_probe(_candidate_value, _events):
            nonlocal probe_calls
            probe_calls += 1
            if probe_calls == 1:
                return None
            if probe_calls == 2:
                return stale_match
            return None

        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_pre_create_duplicate_probe", _changing_probe)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        result = await su.smart_event_update(
            db,
            EventCandidate(
                source_type="telegram",
                source_url="https://t.me/automatic_identity/100",
                source_text="Отдельная программа для перепроверки финального матча.",
                title="Отдельное событие после reload",
                date="2099-09-10",
                time="19:00",
                location_name="Музей",
                city="Калининград",
                event_type="лекция",
                producer_ordinal=0,
            ),
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.outcome is SmartUpdateTerminalOutcome.CREATED
        assert result.event_id != existing_id
        assert probe_calls == 3
        async with db.get_session() as session:
            assert int(await session.scalar(select(func.count()).select_from(Event))) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_accepted_domain_write_survives_attempt_ack_failure_and_exact_replay_recovers(
    tmp_path,
    monkeypatch,
) -> None:
    db = Database(str(tmp_path / "accepted-ack-recovery.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su,
            "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
            IdentityGateMode.OFF,
        )
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)
        real_finish = su.finish_candidate_attempt

        async def _ack_failure(*_args, **_kwargs):
            raise RuntimeError("synthetic acknowledgement loss")

        monkeypatch.setattr(su, "finish_candidate_attempt", _ack_failure)
        created = await su.smart_event_update(
            db,
            _candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert created.outcome is SmartUpdateTerminalOutcome.CREATED
        assert created.event_id is not None

        monkeypatch.setattr(su, "finish_candidate_attempt", real_finish)
        replay = await su.smart_event_update(
            db,
            _candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )
        assert replay.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        assert replay.event_id == created.event_id
        counts = await smart_update_funnel_counts(db)
        assert counts["NOOP_EXACT_REPLAY"] == 1
        assert counts["terminal_unresolved"] == 0
        assert counts["attempt_starts"] == counts["attempt_terminals"] == 2
        assert counts["attempt_unresolved"] == 0
    finally:
        await db.close()
