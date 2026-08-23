from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import Event, EventSource, EventSourceFact
from smart_event_update import EventCandidate, SmartUpdateTerminalOutcome
from smart_update_identity import IdentityGateMode
from source_parsing.handlers import _build_parser_source_text, _parser_occurrence_key
from source_parsing.qtickets import parse_qtickets_output


QTICKETS_SERIES_CASES = (
    (7604, 7707, "251797-svetlogorsk-i-yantarnyy", "2026-08-12", "09:15", "2026-08-17", "09:00"),
    (7580, 7833, "247858-kosa-koty-i-syr", "2026-08-16", "08:00", "2026-08-23", "08:00"),
    (7805, 8253, "251834-konigsberg-8-centuries", "2026-08-19", "10:00", "2026-08-24", "10:00"),
)


def _occurrence_key(url: str, event_date: str, event_time: str, *, ordinal: int = 0, end_date: str | None = None) -> str:
    return _parser_occurrence_key(
        source_type="qtickets",
        source_url=url,
        date_value=event_date,
        end_date_value=end_date,
        time_value=event_time,
        producer_ordinal=ordinal,
    )


@pytest.mark.parametrize(
    "left_id,right_id,slug,left_date,left_time,right_date,right_time",
    QTICKETS_SERIES_CASES,
)
def test_august_qtickets_product_pairs_are_distinct_occurrences(
    left_id: int,
    right_id: int,
    slug: str,
    left_date: str,
    left_time: str,
    right_date: str,
    right_time: str,
) -> None:
    url = f"https://kaliningrad.qtickets.events/{slug}"

    assert _occurrence_key(url, left_date, left_time) != _occurrence_key(
        url, right_date, right_time
    ), f"{left_id}/{right_id} must be recorded as FINAL_DISTINCT"


def test_qtickets_occurrence_key_is_slot_stable_not_schedule_or_order_stable() -> None:
    url = "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy"

    original = _occurrence_key(
        url, "2026-08-12", "09:15", ordinal=0, end_date="2026-10-25"
    )
    reordered_schedule_refresh = _occurrence_key(
        url, "2026-08-12", "09:15", ordinal=19, end_date="2026-11-01"
    )
    another_date = _occurrence_key(url, "2026-08-17", "09:15")
    another_slot = _occurrence_key(url, "2026-08-12", "09:00")

    assert reordered_schedule_refresh == original
    assert another_date != original
    assert another_slot != original


def test_qtickets_vendor_schedule_end_is_not_occurrence_end_date(tmp_path: Path) -> None:
    fixture = tmp_path / "qtickets_events.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Экскурсия «Светлогорск и Янтарный»",
                    "date_raw": "2026-08-12T09:15:00+02:00",
                    "parsed_date": "2026-08-12",
                    "parsed_time": "09:15",
                    "end_date": "2026-08-17",
                    "vendor_schedule_end_date": "2026-08-17",
                    "location": "Центральная площадь",
                    "url": "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    event = parse_qtickets_output([str(fixture)])[0]
    source_text = _build_parser_source_text(
        event,
        full_description="Однодневная экскурсия.",
        location_name=event.location,
    )

    assert event.end_date is None
    assert event.vendor_schedule_end_date == "2026-08-17"
    assert "Окно расписания/продаж Qtickets до: 2026-08-17" in source_text
    assert "Дата окончания: 2026-08-17" not in source_text


@pytest.mark.asyncio
async def test_qtickets_product_url_recall_never_matches_a_different_date(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "qtickets-series.sqlite"))
    await db.init()
    try:
        url = "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy"
        async with db.get_session() as session:
            owner = Event(
                title="Экскурсия «Светлогорск и Янтарный»",
                description="Экскурсия по побережью.",
                source_text="Расписание экскурсий.",
                date="2026-08-12",
                end_date="2026-10-25",
                time="09:15",
                location_name="Центральная площадь",
                city="Калининград",
            )
            session.add(owner)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(owner.id),
                    source_type="parser:qtickets",
                    source_url=url,
                    canonical_source_url=url,
                    source_role="identity_bearing",
                )
            )
            await session.commit()

        candidate = EventCandidate(
            source_type="parser:qtickets",
            source_url=url,
            source_text="17 августа — экскурсия Светлогорск и Янтарный.",
            title="Экскурсия «Светлогорск и Янтарный»",
            date="2026-08-17",
            time="09:00",
            location_name="Центральная площадь",
            city="Калининград",
        )

        assert await su._match_existing_event_by_event_source_url(db, candidate) is None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_qtickets_product_url_same_date_and_time_can_match(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "qtickets-repeat.sqlite"))
    await db.init()
    try:
        url = "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy"
        async with db.get_session() as session:
            owner = Event(
                title="Экскурсия «Светлогорск и Янтарный»",
                description="Экскурсия по побережью.",
                source_text="Расписание экскурсий.",
                date="2026-08-12",
                time="09:15",
                location_name="Центральная площадь",
                city="Калининград",
            )
            session.add(owner)
            await session.flush()
            owner_id = int(owner.id)
            session.add(
                EventSource(
                    event_id=owner_id,
                    source_type="parser:qtickets",
                    source_url=url,
                    canonical_source_url=url,
                    source_role="identity_bearing",
                )
            )
            await session.commit()

        candidate = EventCandidate(
            source_type="parser:qtickets",
            source_url=url,
            source_text="12 августа — экскурсия Светлогорск и Янтарный.",
            title="Экскурсия «Светлогорск и Янтарный»",
            date="2026-08-12",
            time="09:15",
            location_name="Центральная площадь",
            city="Калининград",
        )

        matched = await su._match_existing_event_by_event_source_url(db, candidate)
        assert matched is not None
        assert int(matched.id) == owner_id
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "left_id,right_id,slug,left_date,left_time,right_date,right_time",
    QTICKETS_SERIES_CASES,
)
async def test_qtickets_distinct_occurrences_and_exact_repeats_are_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    left_id: int,
    right_id: int,
    slug: str,
    left_date: str,
    left_time: str,
    right_date: str,
    right_time: str,
) -> None:
    db = Database(str(tmp_path / f"qtickets-{left_id}-{right_id}.sqlite"))
    await db.init()
    url = f"https://kaliningrad.qtickets.events/{slug}"
    provider_calls = 0

    async def forbidden_provider(*_args, **_kwargs):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("occurrence identity added an unexpected LLM call")

    async def no_topics(*_args, **_kwargs):
        return None

    monkeypatch.setattr(su, "_ask_gemma_json", forbidden_provider)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    monkeypatch.setattr(su, "_should_skip_past_smart_update_candidate", lambda _candidate: False)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", IdentityGateMode.OFF)

    def candidate(event_date: str, event_time: str, ordinal: int) -> EventCandidate:
        return EventCandidate(
            source_type="parser:qtickets",
            source_url=url,
            source_text=f"Экскурсия {event_date} в {event_time}.",
            raw_excerpt=f"Экскурсия {event_date} в {event_time}.",
            title=f"Qtickets product {slug}",
            date=event_date,
            time=event_time,
            location_name="Калининград",
            city="Калининград",
            event_type="экскурсия",
            ticket_link=url,
            vendor_schedule_end_date="2026-10-25",
            producer_ordinal=ordinal,
            occurrence_key=_occurrence_key(
                url, event_date, event_time, ordinal=ordinal
            ),
        )

    try:
        first = await su.smart_event_update(
            db, candidate(left_date, left_time, 0), check_source_url=False, schedule_tasks=False
        )
        first_replay = await su.smart_event_update(
            db, candidate(left_date, left_time, 71), check_source_url=True, schedule_tasks=False
        )

        second = await su.smart_event_update(
            db, candidate(right_date, right_time, 1), check_source_url=False, schedule_tasks=False
        )
        second_replay = await su.smart_event_update(
            db, candidate(right_date, right_time, 99), check_source_url=True, schedule_tasks=False
        )

        assert first.outcome is SmartUpdateTerminalOutcome.CREATED
        assert second.outcome is SmartUpdateTerminalOutcome.CREATED
        assert first.event_id != second.event_id
        assert first_replay.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        assert second_replay.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY
        assert provider_calls == 0
        async with db.get_session() as session:
            assert await session.scalar(select(func.count()).select_from(Event)) == 2
            facts = (
                await session.execute(select(EventSourceFact.fact))
            ).scalars().all()
        assert facts.count("Окно расписания/продаж Qtickets до: 2026-10-25") == 2
    finally:
        await db.close()
