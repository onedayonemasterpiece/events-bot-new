from __future__ import annotations

import pytest
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import EventSource
from smart_event_update import EventCandidate, SmartUpdateTerminalOutcome
from smart_update_identity import IdentityGateMode, stable_candidate_identity


def _candidate(
    occurrence_id: str,
    *,
    ordinal: int,
    source_url: str = "https://t.me/stable_occurrences/501",
) -> EventCandidate:
    return EventCandidate(
        source_type="telegram",
        source_url=source_url,
        source_text="Один слот программы 20 сентября в 19:00.",
        title="Один слот программы",
        date="2099-09-20",
        time="19:00",
        location_name="Музей",
        city="Калининград",
        event_type="лекция",
        source_native_occurrence_id=occurrence_id,
        producer_ordinal=ordinal,
    )


def test_occurrence_precedence_is_native_vendor_ticket_schedule_then_ordinal() -> None:
    candidate = _candidate("native-7", ordinal=19)
    candidate.vendor_occurrence_id = "vendor-8"
    candidate.ticket_link = (
        "https://kaliningrad.tretyakovgallery.ru/tickets/"
        "#/buy/event/48636/2099-09-20/19:00:00"
    )
    _, key = stable_candidate_identity(candidate)
    assert key == "source-native:native-7"

    candidate.source_native_occurrence_id = None
    candidate.occurrence_key = None
    _, key = stable_candidate_identity(candidate)
    assert key == "vendor:vendor-8"

    candidate.vendor_occurrence_id = None
    _, key = stable_candidate_identity(candidate)
    assert key.startswith("ticket:")
    assert "ordinal" not in key

    candidate.ticket_link = None
    _, key = stable_candidate_identity(candidate)
    assert key.startswith("structured:")
    assert key.endswith(":ordinal:19")


async def _no_topics(*_args, **_kwargs):
    return None


async def _eventness(*_args, **_kwargs):
    return "event", 0.99, "fixture"


async def _bindings(db: Database) -> dict[str, int]:
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventSource.occurrence_key, EventSource.event_id).where(
                    EventSource.source_type == "telegram",
                    EventSource.canonical_source_url
                    == "https://t.me/stable_occurrences/501",
                )
            )
        ).all()
    return {str(key): int(event_id) for key, event_id in rows}


@pytest.mark.asyncio
async def test_reorder_insertion_and_same_slot_explicit_ids_do_not_shift_bindings(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "occurrence-stability.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
        monkeypatch.setattr(
            su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF
        )
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)

        first_a = await su.smart_event_update(
            db, _candidate("A", ordinal=0), check_source_url=False, schedule_tasks=False
        )
        first_b = await su.smart_event_update(
            db, _candidate("B", ordinal=1), check_source_url=False, schedule_tasks=False
        )
        assert first_a.outcome is SmartUpdateTerminalOutcome.CREATED
        assert first_b.outcome is SmartUpdateTerminalOutcome.CREATED
        assert first_a.event_id != first_b.event_id
        original = await _bindings(db)
        assert original == {
            "source-native:A": first_a.event_id,
            "source-native:B": first_b.event_id,
        }

        # Reorder changes ordinals but native IDs retain the authoritative binding.
        reordered_b = await su.smart_event_update(
            db, _candidate("B", ordinal=0), check_source_url=False, schedule_tasks=False
        )
        reordered_a = await su.smart_event_update(
            db, _candidate("A", ordinal=1), check_source_url=False, schedule_tasks=False
        )
        assert reordered_b.event_id == first_b.event_id
        assert reordered_a.event_id == first_a.event_id
        assert await _bindings(db) == original

        # Inserting a new first sibling does not renumber or rebind A/B.
        inserted = await su.smart_event_update(
            db, _candidate("X", ordinal=0), check_source_url=False, schedule_tasks=False
        )
        shifted_a = await su.smart_event_update(
            db, _candidate("A", ordinal=1), check_source_url=False, schedule_tasks=False
        )
        shifted_b = await su.smart_event_update(
            db, _candidate("B", ordinal=2), check_source_url=False, schedule_tasks=False
        )
        assert inserted.outcome is SmartUpdateTerminalOutcome.CREATED
        assert shifted_a.event_id == first_a.event_id
        assert shifted_b.event_id == first_b.event_id
        final = await _bindings(db)
        assert final["source-native:A"] == first_a.event_id
        assert final["source-native:B"] == first_b.event_id
        assert final["source-native:X"] == inserted.event_id
    finally:
        await db.close()
