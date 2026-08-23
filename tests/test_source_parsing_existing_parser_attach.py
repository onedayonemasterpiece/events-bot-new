from datetime import date, timedelta

import pytest
from sqlmodel import select

import source_parsing.handlers as handlers
import source_parsing.philharmonia as philharmonia
import source_parsing.qtickets as qtickets
from db import Database
from models import Event, EventSource, EventSourceFact
from source_parsing.parser import TheatreEvent
from smart_event_update import SmartUpdateTerminalOutcome


@pytest.mark.asyncio
async def test_exact_attach_prefers_same_event_canonical_row_over_legacy_duplicate(
    tmp_path,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    source_url = "https://sobor39.ru/afisha/bach-night/"
    canonical_url = source_url.rstrip("/")
    async with db.get_session() as session:
        stored = Event(
            title="(Не)известный Бах",
            description="Описание",
            source_text="Источник",
            date=event_date,
            time="19:00",
            location_name="Кафедральный собор",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)
        session.add_all(
            [
                EventSource(
                    event_id=event_id,
                    source_type="parser:sobor",
                    source_url=source_url,
                    trust_level="high",
                ),
                EventSource(
                    event_id=event_id,
                    source_type="parser:sobor",
                    source_url=canonical_url,
                    canonical_source_url=canonical_url,
                    source_role="identity_bearing",
                    trust_level="high",
                ),
            ]
        )
        await session.commit()

    candidate = TheatreEvent(
        title="(Не)известный Бах",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url=source_url,
        source_type="sobor",
    )
    assert await handlers.attach_parser_source_to_exact_existing(
        db, event_id, "sobor", candidate
    )
    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    assert len(rows) == 2
    assert sum(row.canonical_source_url == canonical_url for row in rows) == 1
    await db.close()


@pytest.mark.asyncio
async def test_exact_attached_ticket_url_survives_presentation_title_prefix(tmp_path):
    """An authoritative same-event URL must not fall back to another LLM pass.

    Production Sobor rows already owned the exact ticket URL, date and time,
    while the canonical title had gained the presentation prefix ``Концерт``.
    Rejecting that row as a title mismatch made the parser re-enter extraction,
    where an empty draft became a false technical terminal.
    """

    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    source_url = "https://tickets.sobor-kaliningrad.ru/scheme/EXACT-SLOT"
    async with db.get_session() as session:
        stored = Event(
            title="Концерт «(Нео)Органика 3.0»",
            description="Описание",
            source_text="Источник",
            date=event_date,
            time="20:00",
            location_name="Кафедральный собор",
            ticket_link="https://sobor39.ru/events/concerts/night/",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)
        session.add(
            EventSource(
                event_id=event_id,
                source_type="parser:sobor",
                source_url=source_url,
                canonical_source_url=source_url,
                source_role="identity_bearing",
                candidate_key="sobor-owned-child",
                occurrence_key="parser-slot:sobor-owned-child",
                trust_level="high",
            )
        )
        await session.commit()

    candidate = TheatreEvent(
        title="(Нео)Органика 3.0",
        date_raw=f"{event_date} 20:00",
        parsed_date=event_date,
        parsed_time="20:00",
        ticket_status="available",
        url=source_url,
        source_type="sobor",
    )

    assert await handlers.attach_parser_source_to_exact_existing(
        db, event_id, "sobor", candidate
    )
    await db.close()


@pytest.mark.asyncio
async def test_exact_shared_catalogue_url_attaches_supporting_terminal_noop(tmp_path):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    source_url = "https://sobor39.ru/afisha/night"
    async with db.get_session() as session:
        owner = Event(
            title="Первый концерт",
            description="Описание",
            source_text="Источник",
            date=event_date,
            time="17:00",
            location_name="Кафедральный собор",
        )
        target = Event(
            title="Второй концерт",
            description="Описание",
            source_text="Источник",
            date=event_date,
            time="19:00",
            location_name="Кафедральный собор",
        )
        session.add_all([owner, target])
        await session.commit()
        await session.refresh(owner)
        await session.refresh(target)
        session.add(
            EventSource(
                event_id=int(owner.id),
                source_type="parser:sobor",
                source_url=source_url,
                canonical_source_url=source_url,
                source_role="identity_bearing",
                trust_level="high",
            )
        )
        await session.commit()
        target_id = int(target.id)

    candidate = TheatreEvent(
        title="Второй концерт",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url=source_url,
        source_type="sobor",
    )
    assert await handlers.attach_parser_source_to_exact_existing(
        db, target_id, "sobor", candidate
    )
    async with db.get_session() as session:
        row = (
            await session.execute(
                select(EventSource).where(EventSource.event_id == target_id)
            )
        ).scalar_one()
    assert row.canonical_source_url == source_url
    assert row.source_role == "context_only"
    await db.close()


@pytest.mark.asyncio
async def test_qtickets_exact_attach_persists_slot_identity_and_schedule_fact(tmp_path):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    source_url = "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy"
    async with db.get_session() as session:
        stored = Event(
            title="Светлогорск и Янтарный",
            description="Однодневная экскурсия",
            source_text="Источник",
            date=event_date,
            time="09:15",
            location_name="Центральная площадь",
            ticket_link=source_url,
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    candidate = TheatreEvent(
        title="Светлогорск и Янтарный",
        description="Однодневная экскурсия.",
        date_raw=f"{event_date}T09:15:00+02:00",
        parsed_date=event_date,
        parsed_time="09:15",
        end_date=None,
        vendor_schedule_end_date=(date.today() + timedelta(days=70)).isoformat(),
        location="Центральная площадь",
        ticket_status="available",
        url=source_url,
        source_type="qtickets",
    )

    assert await handlers.attach_parser_source_to_exact_existing(
        db, event_id, "qtickets", candidate
    )
    # Exact refresh is deterministic and idempotent: no second source/fact row.
    assert await handlers.attach_parser_source_to_exact_existing(
        db, event_id, "qtickets", candidate
    )
    async with db.get_session() as session:
        source = (
            await session.execute(
                select(EventSource).where(EventSource.event_id == event_id)
            )
        ).scalar_one()
        facts = list(
            (
                await session.execute(
                    select(EventSourceFact).where(
                        EventSourceFact.event_id == event_id,
                        EventSourceFact.source_id == int(source.id),
                    )
                )
            ).scalars()
        )
    assert source.source_role == "identity_bearing"
    assert source.candidate_key
    assert source.occurrence_key == handlers._parser_occurrence_key(
        source_type="qtickets",
        source_url=source_url,
        date_value=event_date,
        end_date_value=None,
        time_value="09:15",
        producer_ordinal=0,
    )
    assert "Окно расписания/продаж Qtickets до:" in (source.source_text or "")
    assert "Дата окончания:" not in (source.source_text or "")
    assert len(facts) == 1
    assert facts[0].status == "note"
    assert facts[0].fact.startswith("Окно расписания/продаж Qtickets до:")
    await db.close()


@pytest.mark.asyncio
async def test_parser_smart_retry_is_visible_terminal_not_durable_recovery(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()

    async def no_existing(*_args, **_kwargs):
        return None, False

    async def no_ticket_slot(*_args, **_kwargs):
        return None

    async def retry_result(*_args, **_kwargs):
        return None, False, SmartUpdateTerminalOutcome.RETRY_SCHEDULED

    async def forbidden_recovery(*_args, **_kwargs):
        raise AssertionError("Smart terminal must not enqueue parser recovery")

    monkeypatch.setattr(handlers, "find_existing_event", no_existing)
    monkeypatch.setattr(handlers, "find_exact_parser_ticket_slot", no_ticket_slot)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", retry_result)
    monkeypatch.setattr(
        handlers, "_schedule_source_parser_recovery_request", forbidden_recovery
    )

    candidate = TheatreEvent(
        title="Решение требует оператора",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url="https://sobor39.ru/afisha/operator",
        location="Кафедральный собор",
        source_type="sobor",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="sobor",
        start_index=0,
        total_count=1,
    )

    assert stats.failed == 1
    assert stats.retry_scheduled == 0
    assert stats.terminal_errors == [
        "Решение требует оператора:FAILED_TECHNICAL"
    ]
    await db.close()


@pytest.mark.asyncio
async def test_exact_existing_event_attaches_parser_without_llm(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()

    async with db.get_session() as session:
        stored = Event(
            title="Вне подозрения",
            description="Existing canonical description",
            date=event_date,
            time="19:00",
            location_name="Калининградский театр эстрады (Дом искусств)",
            source_text="Telegram source",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def find_existing(*_args, **_kwargs):
        return event_id, False

    async def true_result(*_args, **_kwargs):
        return True

    async def no_result(*_args, **_kwargs):
        return None

    async def forbidden_llm(*_args, **_kwargs):
        raise AssertionError("exact existing parser occurrence must not call Smart Update")

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_event_ticket_status", true_result)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "schedule_existing_event_update", no_result)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", forbidden_llm)

    candidate = TheatreEvent(
        title="Вне подозрения",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url="https://domiskusstv.edinoepole.ru/widget/events/922/event_seats",
        location="Калининградский театр эстрады (Дом искусств)",
        source_type="estrada",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="estrada",
        start_index=0,
        total_count=1,
    )

    async with db.get_session() as session:
        sources = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    await db.engine.dispose()

    assert stats.ticket_updated == 1
    assert stats.failed == 0
    assert [
        (row.source_type, row.source_url, row.canonical_source_url, row.source_role)
        for row in sources
    ] == [
        (
            "parser:estrada",
            "https://domiskusstv.edinoepole.ru/widget/events/922/event_seats",
            "https://domiskusstv.edinoepole.ru/widget/events/922/event_seats",
            "identity_bearing",
        )
    ]


@pytest.mark.asyncio
async def test_exact_ticket_slot_allows_presentation_title_without_llm(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    official_url = "https://example.org/afisha/grozd-26/"

    async with db.get_session() as session:
        stored = Event(
            title="Эногастрономический фестиваль «Гроздь»",
            description="Existing canonical description",
            date=event_date,
            time="11:00",
            location_name="Янтарь холл",
            source_text="Parser source",
            ticket_link=official_url,
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def find_existing(*_args, **_kwargs):
        # Presentation title no longer matches the terse official title.  The
        # exact-ticket-slot fallback must discover the stored occurrence.
        return None, False

    async def true_result(*_args, **_kwargs):
        return True

    async def no_result(*_args, **_kwargs):
        return None

    async def forbidden_llm(*_args, **_kwargs):
        raise AssertionError("exact official ticket slot must be replay-idempotent")

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_event_ticket_status", true_result)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "schedule_existing_event_update", no_result)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", forbidden_llm)

    candidate = TheatreEvent(
        title='"Гроздь"',
        date_raw=f"{event_date} 11:00",
        parsed_date=event_date,
        parsed_time="11:00",
        ticket_status="available",
        url=official_url,
        location="Янтарь холл",
        source_type="yantarhall",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="yantarhall",
        start_index=0,
        total_count=1,
    )

    async with db.get_session() as session:
        sources = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    await db.engine.dispose()

    assert stats.ticket_updated == 1
    assert stats.failed == 0
    assert [
        (row.source_type, row.source_url, row.canonical_source_url, row.source_role)
        for row in sources
    ] == [
        ("parser:yantarhall", official_url, official_url.rstrip("/"), "identity_bearing")
    ]


@pytest.mark.asyncio
async def test_unchanged_exact_ticket_replay_skips_page_rebuild_and_schedule(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    official_url = "https://example.org/afisha/unchanged-slot"

    async with db.get_session() as session:
        stored = Event(
            title="Неизменившийся концерт",
            description="Existing canonical description",
            date=event_date,
            time="19:00",
            location_name="Янтарь холл",
            source_text="Parser source",
            ticket_status="available",
            ticket_link=official_url,
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def find_existing(*_args, **_kwargs):
        return event_id, False

    async def no_result(*_args, **_kwargs):
        return None

    async def forbidden_effect(*_args, **_kwargs):
        raise AssertionError("unchanged replay must not rebuild or schedule pages")

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "schedule_existing_event_update", forbidden_effect)
    monkeypatch.setattr(handlers, "_ensure_telegraph_url", forbidden_effect)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", forbidden_effect)

    candidate = TheatreEvent(
        title="Неизменившийся концерт",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url=official_url,
        location="Янтарь холл",
        source_type="yantarhall",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="yantarhall",
        start_index=0,
        total_count=1,
    )
    await db.engine.dispose()

    assert stats.ticket_updated == 0
    assert stats.already_exists == 1
    assert stats.failed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("module", "processor", "source_type"),
    [
        (philharmonia, philharmonia.process_philharmonia_events, "philharmonia"),
        (qtickets, qtickets.process_qtickets_events, "qtickets"),
    ],
)
async def test_specialized_site_processor_uses_exact_ticket_slot_guard(
    tmp_path,
    monkeypatch,
    module,
    processor,
    source_type,
):
    db = Database(str(tmp_path / f"{source_type}.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    official_url = f"https://example.org/{source_type}/exact-slot"

    async with db.get_session() as session:
        stored = Event(
            title="Расширенный presentation title",
            description="Existing canonical description",
            date=event_date,
            time="19:00",
            location_name="Филармония им. Светланова",
            source_text="Existing source",
            ticket_status="available",
            ticket_link=official_url,
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def no_title_match(*_args, **_kwargs):
        return None, False

    async def no_result(*_args, **_kwargs):
        return None

    async def forbidden_llm(*_args, **_kwargs):
        raise AssertionError("exact specialized replay must not call Smart Update")

    monkeypatch.setattr(module, "find_existing_event", no_title_match)
    monkeypatch.setattr(module, "update_linked_events", no_result)
    monkeypatch.setattr(module, "add_new_event_via_queue", forbidden_llm)

    candidate = TheatreEvent(
        title="Короткий официальный title",
        date_raw=f"{event_date} 19:00",
        parsed_date=event_date,
        parsed_time="19:00",
        ticket_status="available",
        url=official_url,
        location="Филармония им. Светланова",
        source_type=source_type,
    )
    stats = await processor(db, None, [candidate])

    async with db.get_session() as session:
        sources = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id)
                )
            ).scalars()
        )
    await db.engine.dispose()

    assert stats.ticket_updated == 1
    assert stats.failed == 0
    assert [(row.source_type, row.source_url) for row in sources] == [
        (f"parser:{source_type}", official_url)
    ]


@pytest.mark.asyncio
async def test_title_mismatch_without_explicit_slot_still_reaches_llm(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()
    official_url = "https://example.org/festival-programme"

    async with db.get_session() as session:
        stored = Event(
            title="Концерт на главной сцене",
            description="Existing occurrence",
            date=event_date,
            time="19:00",
            location_name="Фестивальная площадка",
            source_text="Parser source",
            ticket_link=official_url,
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    async def find_existing(*_args, **_kwargs):
        return event_id, False

    async def no_result(*_args, **_kwargs):
        return None

    smart_update_calls = 0

    async def fake_smart_update(*_args, **_kwargs):
        nonlocal smart_update_calls
        smart_update_calls += 1
        return 999, True, "created"

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", fake_smart_update)
    monkeypatch.setattr(handlers.asyncio, "sleep", no_result)

    candidate = TheatreEvent(
        title="Другое событие общей программы",
        date_raw=event_date,
        parsed_date=event_date,
        parsed_time="00:00",
        ticket_status="available",
        url=official_url,
        location="Фестивальная площадка",
        source_type="yantarhall",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="yantarhall",
        start_index=0,
        total_count=1,
    )
    await db.engine.dispose()

    assert smart_update_calls == 1
    assert stats.new_added == 1
    assert stats.failed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("candidate_title", "candidate_time"),
    [
        ("Бродский. Обещание любви", "14:00"),
        ("Фестиваль Pianissimo: Моён Юн", "17:00"),
    ],
)
async def test_non_exact_parser_occurrence_reaches_smart_update(
    tmp_path,
    monkeypatch,
    candidate_title,
    candidate_time,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    event_date = (date.today() + timedelta(days=7)).isoformat()

    async with db.get_session() as session:
        stored = Event(
            title="Бродский. Обещание любви",
            description="Existing parser-backed occurrence",
            date=event_date,
            time="17:00",
            location_name="Музыкальный театр",
            source_text="Parser source",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)
        session.add(
            EventSource(
                event_id=event_id,
                source_type="parser:muzteatr",
                source_url="https://muzteatr39.ru/performance",
                trust_level="high",
            )
        )
        await session.commit()

    async def find_existing(*_args, **_kwargs):
        return event_id, False

    async def no_result(*_args, **_kwargs):
        return None

    smart_update_calls = 0

    async def fake_smart_update(*_args, **_kwargs):
        nonlocal smart_update_calls
        smart_update_calls += 1
        return 999, True, "created"

    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "add_new_event_via_queue", fake_smart_update)
    monkeypatch.setattr(handlers.asyncio, "sleep", no_result)

    candidate = TheatreEvent(
        title=candidate_title,
        date_raw=f"{event_date} {candidate_time}",
        parsed_date=event_date,
        parsed_time=candidate_time,
        ticket_status="available",
        url="https://muzteatr39.ru/performance",
        location="Музыкальный театр",
        source_type="muzteatr",
    )
    stats, _ = await handlers.process_source_events(
        db,
        None,
        [candidate],
        source="muzteatr",
        start_index=0,
        total_count=1,
    )

    await db.engine.dispose()
    assert smart_update_calls == 1
    assert stats.new_added == 1
    assert stats.failed == 0


@pytest.mark.asyncio
async def test_current_official_available_occurrence_reactivates_stale_postponement(
    tmp_path,
):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    async with db.get_session() as session:
        stored = Event(
            title="Фестиваль Pianissimo: Моён Юн",
            description="Legacy aggregate",
            date="2026-08-06",
            time="20:00",
            location_name="Филиал Третьяковской галереи",
            source_text="Historical source",
            lifecycle_status="postponed",
            ticket_status="available",
        )
        session.add(stored)
        await session.commit()
        await session.refresh(stored)
        event_id = int(stored.id)

    candidate = TheatreEvent(
        title="ФЕСТИВАЛЬ PIANISSIMO: МОЁН ЮН (ЮЖНАЯ КОРЕЯ)",
        date_raw="2026-08-06 20:00",
        parsed_date="2026-08-06",
        parsed_time="20:00",
        ticket_status="available",
        url="https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46315/2026-08-06/20:00:00",
        location="Филиал Третьяковской галереи",
        source_type="tretyakov",
    )
    changed = await handlers.reconcile_existing_event_lifecycle(
        db,
        event_id,
        candidate,
    )

    async with db.get_session() as session:
        refreshed = await session.get(Event, event_id)
    await db.engine.dispose()

    assert changed is True
    assert refreshed is not None
    assert refreshed.lifecycle_status == "active"
