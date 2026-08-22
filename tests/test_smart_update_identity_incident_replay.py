from __future__ import annotations

import json
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import Event, EventIdentityDecisionLog
from smart_event_update import EventCandidate, smart_event_update
from smart_update_identity import IdentityVectorEvidence


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
def _allow_historical_incident_replays(monkeypatch):
    """The fixtures preserve incident dates and must exercise identity, not age."""
    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")


async def _no_topics(*_args, **_kwargs):
    return None


async def _seed_event(
    db: Database,
    *,
    event_id: int,
    title: str,
    date: str,
    end_date: str | None = None,
    time: str = "",
    location_name: str = "Музей",
    event_type: str = "выставка",
    source_url: str | None = None,
    ticket_link: str | None = None,
    festival: str | None = None,
) -> int:
    async with db.get_session() as session:
        ev = Event(
            id=event_id,
            title=title,
            description=f"Существующая карточка: {title}",
            date=date,
            end_date=end_date,
            time=time,
            location_name=location_name,
            city="Калининград",
            event_type=event_type,
            ticket_link=ticket_link,
            festival=festival,
            source_text=f"{title} {date}",
            source_post_url=source_url,
            telegraph_url=f"https://telegra.ph/event-{event_id}",
            telegraph_path=f"event-{event_id}",
            identity_status="canonical",
        )
        session.add(ev)
        await session.commit()
    return event_id


def _exhibition_candidate(title: str, *, date: str = "2026-07-02", end_date: str = "2026-08-02") -> EventCandidate:
    return EventCandidate(
        source_type="telegram",
        source_url=f"https://t.me/incident_replay/{abs(hash(title)) % 100000}",
        source_text=f"Выставка «{title}» проходит с 2 июля по 2 августа в музее.",
        raw_excerpt=f"Выставка «{title}» с 2 июля по 2 августа.",
        title=title,
        date=date,
        end_date=end_date,
        time="",
        location_name="Музей",
        city="Калининград",
        event_type="выставка",
    )


@pytest.mark.parametrize(
    ("cluster_title", "canonical_id", "candidate_title", "score"),
    [
        ("Билетёры 2.0", 5765, "Билетеры 2.0", 1.0),
        ("С чего начинается Родина", 4512, "С чего начинается родина", 0.916),
        ("Точка и линия", 5370, "Линия и точка", 0.928),
        ("Альбрехт Дюрер", 5703, "Дюрер: графика", 0.923),
        ("Розовый натюрморт", 6080, "Розовый натюрморт", 0.956),
    ],
)
@pytest.mark.asyncio
async def test_incident_exhibition_replay_vector_gate_prevents_public_duplicate(
    tmp_path,
    monkeypatch,
    cluster_title: str,
    canonical_id: int,
    candidate_title: str,
    score: float,
):
    db = Database(str(tmp_path / f"replay-{canonical_id}.sqlite"))
    await db.init()
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)

    await _seed_event(
        db,
        event_id=canonical_id,
        title=cluster_title,
        date="2026-07-01",
        end_date="2026-08-10",
        location_name="Музей",
    )

    async def _vector_evidence(_candidate):
        return IdentityVectorEvidence(
            available=True,
            nearest_event_id=canonical_id,
            score=score,
            reason=f"incident replay vector hit: {cluster_title}",
        )

    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _vector_evidence)

    result = await smart_event_update(
        db,
        _exhibition_candidate(candidate_title),
        check_source_url=False,
        schedule_tasks=False,
    )

    assert result.outcome in {
        su.SmartUpdateTerminalOutcome.MERGED,
        su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
    }
    if result.outcome is su.SmartUpdateTerminalOutcome.MERGED:
        assert result.event_id == canonical_id
        assert result.diagnostic_event_id is None
    else:
        assert result.event_id is None
        assert result.diagnostic_event_id == canonical_id
        assert result.reason == (
            "identity_gate_adjudicator_unavailable:vector_nearest_identity"
        )
    async with db.get_session() as session:
        public_count = await session.scalar(
            select(func.count()).select_from(Event).where(Event.identity_status == "canonical")
        )
        logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
    assert public_count == 1
    if result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED:
        assert logs
        assert logs[-1].event_id == canonical_id
        assert logs[-1].decision == "FINAL_RETRY"
        assert logs[-1].decision_payload["stage"] == "final_identity_adjudicator"
    await db.close()


@pytest.mark.asyncio
async def test_recurring_high_similarity_different_explicit_date_creates_distinct_occurrence(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "recurring.sqlite"))
    await db.init()
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    await _seed_event(
        db,
        event_id=6405,
        title="Стендап: Гассан Джабер",
        date="2026-07-10",
        time="20:00",
        location_name="Клуб",
        event_type="стендап",
    )

    async def _vector_evidence(_candidate):
        return IdentityVectorEvidence(
            available=True,
            nearest_event_id=6405,
            score=0.986,
            reason="known recurring negative control",
        )

    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _vector_evidence)
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/standup/6406",
        source_text="Стендап: Гассан Джабер 20 июля в 20:00 в клубе.",
        title="Стендап: Гассан Джабер",
        date="2026-07-20",
        time="20:00",
        location_name="Клуб",
        city="Калининград",
        event_type="стендап",
    )

    result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

    assert result.status == "created"
    async with db.get_session() as session:
        total = await session.scalar(select(func.count()).select_from(Event))
        created = await session.get(Event, result.event_id)
    assert total == 2
    assert created is not None
    assert created.date == "2026-07-20"


@pytest.mark.asyncio
async def test_multi_session_same_source_different_explicit_time_is_not_vector_merged(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "multi-session.sqlite"))
    await db.init()
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    source_url = "https://t.me/museum/4509"
    await _seed_event(
        db,
        event_id=5426,
        title="Кураторская экскурсия по выставке",
        date="2026-07-12",
        time="12:00",
        location_name="Музей",
        event_type="экскурсия",
        source_url=source_url,
    )

    async def _vector_evidence(_candidate):
        return IdentityVectorEvidence(
            available=True,
            nearest_event_id=5426,
            score=0.97,
            reason="same source multi-session control",
        )

    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _vector_evidence)
    candidate = EventCandidate(
        source_type="telegram",
        source_url=source_url,
        source_text="12 июля: экскурсия в 12:00 и отдельная экскурсия в 15:00.",
        title="Кураторская экскурсия по выставке",
        date="2026-07-12",
        time="15:00",
        location_name="Музей",
        city="Калининград",
        event_type="экскурсия",
    )

    result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

    assert result.status == "created"
    async with db.get_session() as session:
        total = await session.scalar(select(func.count()).select_from(Event))
    assert total == 2


@pytest.mark.asyncio
async def test_same_source_same_slot_distinct_titles_create_sibling_events(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "same-slot-siblings.sqlite"))
    await db.init()
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    source_url = "https://vk.com/wall-53460968_11826"
    await _seed_event(
        db,
        event_id=7709,
        title="Кинопоказ «Чебурашка 2»",
        date="2026-08-28",
        time="19:00",
        location_name="Гусевский музей",
        event_type="кинопоказ",
        source_url=source_url,
    )

    async def _no_vector_match(_candidate):
        return IdentityVectorEvidence(
            available=True,
            nearest_event_id=None,
            score=None,
            reason="same-source sibling negative control",
        )

    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _no_vector_match)

    candidate = EventCandidate(
        source_type="vk",
        source_url=source_url,
        source_text=(
            "28 августа в 19:00 в Гусевском музее состоятся отдельные "
            "кинопоказы «Чебурашка 2» и «Ангелы Ладоги»."
        ),
        title="Кинопоказ «Ангелы Ладоги»",
        date="2026-08-28",
        time="19:00",
        location_name="Гусевский музей",
        city="Гусев",
        event_type="кинопоказ",
        producer_ordinal=1,
        source_disposition="EVENTS_FOUND",
    )

    result = await smart_event_update(
        db, candidate, check_source_url=False, schedule_tasks=False
    )

    assert result.status == "created"
    async with db.get_session() as session:
        total = await session.scalar(select(func.count()).select_from(Event))
        created = await session.get(Event, result.event_id)
    assert total == 2
    assert created is not None
    assert created.title == "Кинопоказ «Ангелы Ладоги»"
    await db.close()


def _typed_adjudicator_decision(
    *,
    action: str,
    relation: str,
    reason_code: str,
    confidence: float,
    match_event_id: int | None,
    evidence: list[str] | None = None,
    conflicts: list[str] | None = None,
) -> dict[str, object]:
    return {
        "action": action,
        "match_event_id": match_event_id,
        "confidence": confidence,
        "reason_code": reason_code,
        "reason": "sanitized incident replay",
        "relation": relation,
        "source_grounded_evidence": list(evidence or []),
        "blocking_conflicts": list(conflicts or []),
    }


async def _run_vector_veto_adjudicator_replay(
    tmp_path,
    monkeypatch,
    *,
    suffix: str,
    decision: dict[str, object] | None,
    candidate_title: str = "Дюрер: графика",
    candidate_type: str = "выставка",
    candidate_time: str = "",
    owner_id: int = 5703,
    owner_title: str = "Альбрехт Дюрер",
    owner_date: str = "2026-07-01",
    candidate_date: str = "2026-07-02",
    location_name: str = "Музей",
    ticket_link: str | None = None,
    expect_dedup: bool = True,
    owner_type: str | None = None,
    owner_time: str | None = None,
    owner_end_date: str | None = "__auto__",
    candidate_end_date: str | None = "__auto__",
    owner_source_url: str | None = None,
    candidate_source_url: str | None = None,
    owner_festival: str | None = None,
    candidate_festival: str | None = None,
    candidate_festival_context: str | None = None,
    candidate_occurrence_key: str | None = None,
    replay_twice: bool = False,
) -> tuple[Database, su.SmartUpdateResult, int]:
    db = Database(str(tmp_path / f"typed-final-{suffix}.sqlite"))
    await db.init()
    await _seed_event(
        db,
        event_id=owner_id,
        title=owner_title,
        date=owner_date,
        end_date=(
            "2026-08-30"
            if owner_end_date == "__auto__" and candidate_type == "выставка"
            else None if owner_end_date == "__auto__" else owner_end_date
        ),
        time=candidate_time if owner_time is None else owner_time,
        location_name=location_name,
        event_type=owner_type or candidate_type,
        source_url=owner_source_url,
        ticket_link=ticket_link,
        festival=owner_festival,
    )
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    async def _eventness(*_args, **_kwargs):
        return "event", 0.99, "sanitized fixture"

    monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)
    async def _no_city_rescue(*_args, **_kwargs):
        return None, None

    monkeypatch.setattr(su, "_match_existing_event_by_city_noise_rescue", _no_city_rescue)
    async def _ordinary_bundle_no_match(*_args, **_kwargs):
        return None

    async def _ordinary_match_no_match(*_args, **_kwargs):
        return None, 0.0, "fixture_no_match"

    # These replays exercise the widened vector handoff and its one existing
    # adjudicator call.  Never let the earlier ordinary shortlist call a live
    # provider merely because a realistic long-running exhibition overlaps.
    monkeypatch.setattr(su, "_llm_match_or_create_bundle", _ordinary_bundle_no_match)
    monkeypatch.setattr(su, "_llm_match_event", _ordinary_match_no_match)
    monkeypatch.setattr(su, "_single_candidate_auto_match_ok", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(su, "_deterministic_exact_title_match", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        su,
        "_deterministic_related_title_anchor_match",
        lambda *_args, **_kwargs: None,
    )
    calls = {"dedup": 0}

    async def _vector_evidence(_candidate):
        return IdentityVectorEvidence(
            available=True,
            nearest_event_id=owner_id,
            score=0.97,
            reason="sanitized rank owner",
        )

    async def _adjudicate(*_args, **_kwargs):
        calls["dedup"] += 1
        # No later provider call is part of this identity replay; accepted
        # create/merge uses the existing deterministic fallback machinery.
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        return decision

    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", _vector_evidence)
    monkeypatch.setattr(su, "_llm_dedup_adjudicator", _adjudicate)
    candidate = EventCandidate(
        source_type="telegram",
        source_url=candidate_source_url or f"https://t.me/incident_replay/{suffix}",
        source_text=(
            f"{candidate_title}. Отдельное описание из источника; "
            f"время {candidate_time or 'не указано'}."
        ),
        raw_excerpt=candidate_title,
        title=candidate_title,
        date=candidate_date,
        end_date=(
            "2026-08-20"
            if candidate_end_date == "__auto__" and candidate_type == "выставка"
            else None if candidate_end_date == "__auto__" else candidate_end_date
        ),
        time=candidate_time,
        location_name=location_name,
        # Deliberate metadata drift keeps the vector owner out of the ordinary
        # exact city shortlist; vector handoff must still reach the same call.
        city="Балтийск",
        event_type=candidate_type,
        ticket_link=ticket_link,
        festival=candidate_festival,
        festival_context=candidate_festival_context,
        occurrence_key=candidate_occurrence_key,
    )
    result = await smart_event_update(
        db,
        candidate,
        check_source_url=False,
        schedule_tasks=False,
    )
    assert calls["dedup"] == (1 if expect_dedup else 0)
    if replay_twice:
        async with db.get_session() as session:
            events_before = await session.scalar(select(func.count()).select_from(Event))
        calls_before = dict(calls)
        replay_results = []
        for _ in range(2):
            replay_results.append(
                await smart_event_update(
                    db,
                    candidate,
                    check_source_url=True,
                    schedule_tasks=False,
                )
            )
        async with db.get_session() as session:
            events_after = await session.scalar(select(func.count()).select_from(Event))
        assert events_after == events_before
        assert calls == calls_before
        assert all(
            replay.outcome is not su.SmartUpdateTerminalOutcome.CREATED
            for replay in replay_results
        )
    return db, result, owner_id


@pytest.mark.asyncio
async def test_sos_shaped_veto_create_no_candidate_match_is_durable_retry(
    tmp_path,
    monkeypatch,
):
    db, result, owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix="sos-8117-8242",
        decision=_typed_adjudicator_decision(
            action="create",
            relation="unknown",
            reason_code="no_candidate_match",
            confidence=0.99,
            match_event_id=None,
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED
    assert result.event_id is None
    assert result.diagnostic_event_id == owner_id
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1
        logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
    assert logs[-1].decision == "FINAL_RETRY"
    assert logs[-1].decision_payload["candidate_state_id"] is not None
    assert logs[-1].decision_payload["attempt_no"] == 1


@pytest.mark.asyncio
async def test_typed_accepted_match_reuses_existing_owner(tmp_path, monkeypatch):
    db, result, owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix="accepted-match",
        candidate_title="Альбрехт Дюрер",
        decision=_typed_adjudicator_decision(
            action="match",
            relation="same_event",
            reason_code="identical_anchors_dup",
            confidence=0.99,
            match_event_id=5703,
            evidence=["Альбрехт Дюрер"],
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.MERGED
    assert result.event_id == owner_id
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1
        logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
    final = next(row for row in logs if row.decision == "FINAL_MATCH")
    assert final.event_id == owner_id


_AUGUST_POSITIVE_CASES = [
    ("SOS", 8117, "Тройной день рождения: Барн, Chipi Clo и SOS", "Праздничный SOS", "2026-08-22", "21:00", "Барн", "вечеринка", "https://barn.timepad.ru/event/4147114"),
    ("qTickets-247858", 7580, "Коса, коты и сыр", "Коса, коты и сыр", "2026-08-18", "", "Королевские ворота", "экскурсия", "https://qtickets.ru/event/247858-kosa-koty-i-syr"),
    ("qTickets-251796", 7603, "Малые средневековые города", "Малые (средневековые) города", "2026-08-18", "", "Калининградская область", "экскурсия", "https://qtickets.ru/event/251796-malye-srednevekovye-goroda"),
    ("Baltic Odyssey", 8055, "Балтийская Одиссея", "Балтийская Одиссея", "2026-08-22", "", "Побережье", "фестиваль", "https://balticodyssey.qtickets.ru/"),
    ("Great Teachers reminder", 3216, "Великие учителя", "Великие учителя", "2026-08-21", "", "Третьяковская галерея", "выставка", None),
    ("Durer exhibition reminder", 5703, "Альбрехт Дюрер. Секретный код", "Выставка «Секретный код Альбрехта Дюрера»", "2026-08-16", "", "Музей изобразительных искусств", "выставка", None),
    ("Living Thread of Traditions", 7609, "Живая нить традиций", "Живая нить традиций", "2026-08-15", "11:00", "ОКЦ ТеплоСеть", "выставка", None),
]

_POSITIVE_ANCHORS = {
    "SOS": dict(owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date=None, candidate_end_date=None),
    "qTickets-247858": dict(owner_date="2026-08-15", candidate_date="2026-08-18", owner_end_date="2026-10-30", candidate_end_date="2026-10-30"),
    "qTickets-251796": dict(owner_date="2026-08-15", candidate_date="2026-08-18", owner_end_date="2026-10-31", candidate_end_date="2026-10-31"),
    "Baltic Odyssey": dict(owner_date="2025-08-22", candidate_date="2026-08-22", owner_end_date="2026-08-24", candidate_end_date="2026-08-24"),
    "Great Teachers reminder": dict(owner_date="2026-04-09", candidate_date="2026-08-21", owner_end_date="2026-09-27", candidate_end_date="2026-09-06"),
    "Durer exhibition reminder": dict(owner_date="2026-06-06", candidate_date="2026-08-16", owner_end_date="2026-08-30", candidate_end_date="2026-08-30", owner_time="12:00"),
    "Living Thread of Traditions": dict(owner_date="2026-08-15", candidate_date="2026-08-15", owner_end_date="2026-09-05", candidate_end_date="2026-09-05", owner_time="11:00..18:00"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name,owner_id,owner_title,candidate_title,event_date,event_time,location,event_type,ticket",
    _AUGUST_POSITIVE_CASES,
)
async def test_named_august_positive_corpus_reuses_owner(
    tmp_path, monkeypatch, case_name, owner_id, owner_title, candidate_title,
    event_date, event_time, location, event_type, ticket,
):
    anchors = _POSITIVE_ANCHORS[case_name]
    db, result, expected_owner = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix=f"positive-{owner_id}",
        owner_id=owner_id,
        owner_title=owner_title,
        **anchors,
        candidate_title=candidate_title,
        candidate_type=event_type,
        candidate_time=event_time,
        location_name=location,
        ticket_link=ticket,
        expect_dedup=case_name != "SOS",
        replay_twice=True,
        decision=_typed_adjudicator_decision(
            action="match",
            relation="same_event",
            reason_code="venue_variant",
            confidence=0.99,
            match_event_id=owner_id,
            evidence=[candidate_title],
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.MERGED, case_name
    assert result.event_id == expected_owner
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1


_AUGUST_HARD_NEGATIVES = [
    ("exhibition_vs_excursion", "Экскурсия по выставке", "экскурсия", "15:00", "distinct_event"),
    ("exhibition_vs_lecture_or_closing", "Лекция об искусстве", "лекция", "18:00", "distinct_event"),
    ("same_day_distinct_sessions", "Дневной сеанс в 13:00", "спектакль", "13:00", "distinct_occurrence"),
    ("recurring_series_distinct_dates", "Следующая дата серии", "концерт", "19:00", "distinct_occurrence"),
    ("festival_parent_vs_independent_child", "Йога на фестивале", "занятие", "10:00", "distinct_event"),
    ("one_source_multiple_children", "Второй самостоятельный child", "спектакль", "17:00", "distinct_event"),
    ("same_venue_distinct_exhibitions", "Другая выставка", "выставка", "", "distinct_event"),
]

_NEGATIVE_ANCHORS = {
    "exhibition_vs_excursion": dict(owner_title="Великие учителя", owner_type="выставка", owner_time="", owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date="2026-09-27", candidate_end_date=None),
    "exhibition_vs_lecture_or_closing": dict(owner_title="Великие учителя", owner_type="выставка", owner_time="", owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date="2026-09-27", candidate_end_date=None),
    "same_day_distinct_sessions": dict(owner_title="Спектакль", owner_type="спектакль", owner_time="11:00", owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date=None, candidate_end_date=None, owner_source_url="https://t.me/gusmuseum/4509", candidate_source_url="https://t.me/gusmuseum/4509"),
    "recurring_series_distinct_dates": dict(owner_title="Концертная серия", owner_type="концерт", owner_time="19:00", owner_date="2026-08-22", candidate_date="2026-08-23", owner_end_date=None, candidate_end_date=None),
    "festival_parent_vs_independent_child": dict(owner_title="Фестиваль на Козьей горке", owner_type="фестиваль", owner_time="10:00", owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date="2026-08-23", candidate_end_date=None, owner_source_url="https://t.me/kozia_gorka/1556", candidate_source_url="https://t.me/kozia_gorka/1556", owner_festival="Фестиваль на Козьей горке", candidate_festival="Фестиваль на Козьей горке", candidate_festival_context="independent_child"),
    "one_source_multiple_children": dict(owner_title="Первый самостоятельный child", owner_type="спектакль", owner_time="17:00", owner_date="2026-08-22", candidate_date="2026-08-22", owner_end_date=None, candidate_end_date=None, owner_source_url="https://vk.com/wall-53460968_11826", candidate_source_url="https://vk.com/wall-53460968_11826", candidate_occurrence_key="child:1"),
    "same_venue_distinct_exhibitions": dict(owner_title="Альбрехт Дюрер", owner_type="выставка", owner_time="", owner_date="2026-08-01", candidate_date="2026-08-01", owner_end_date="2026-09-30", candidate_end_date="2026-09-30"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("case_name,title,event_type,event_time,relation", _AUGUST_HARD_NEGATIVES)
async def test_named_august_hard_negative_corpus_stays_distinct(
    tmp_path, monkeypatch, case_name, title, event_type, event_time, relation,
):
    anchors = _NEGATIVE_ANCHORS[case_name]
    db, result, owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix=f"negative-{case_name}",
        candidate_title=title,
        candidate_type=event_type,
        candidate_time=event_time,
        **anchors,
        replay_twice=True,
        decision=_typed_adjudicator_decision(
            action="create",
            relation=relation,
            reason_code="distinct_show_keep",
            confidence=0.99,
            match_event_id=None,
            evidence=[title],
            conflicts=[f"{case_name}: explicit independent occurrence/event"],
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.CREATED, case_name
    assert result.event_id != owner_id
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name,title,event_type,event_time,relation",
    [
        row
        for row in _AUGUST_HARD_NEGATIVES
        if row[0] in {"same_day_distinct_sessions", "recurring_series_distinct_dates"}
    ],
)
async def test_structural_hard_negatives_reject_even_adversarial_match(
    tmp_path, monkeypatch, case_name, title, event_type, event_time, relation,
):
    """Hard occurrence conflicts override even an erroneous model match."""

    anchors = _NEGATIVE_ANCHORS[case_name]
    db, result, owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix=f"negative-bad-match-{case_name}",
        candidate_title=title,
        candidate_type=event_type,
        candidate_time=event_time,
        **anchors,
        decision=_typed_adjudicator_decision(
            action="match",
            relation="same_event",
            reason_code="identical_anchors_dup",
            confidence=0.99,
            match_event_id=5703,
            evidence=[title],
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED, case_name
    assert result.event_id is None
    assert result.diagnostic_event_id == owner_id
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1


def test_named_corpus_manifest_and_executable_cases_are_complete():
    path = Path(__file__).parent / "replays" / "INC-2026-08-22-sos-dedup-veto-location-tyunin-farm" / "dedup_cases.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert {row[0] for row in _AUGUST_POSITIVE_CASES} == {row["name"] for row in payload["positives"]}
    assert {row[0] for row in _AUGUST_HARD_NEGATIVES} == {
        row["name"] for row in payload["hard_negatives"]
    }
    assert payload["replay_count"] == 2
    required_anchors = {"title", "date", "end_date", "time", "event_type"}
    for row in payload["positives"] + payload["hard_negatives"]:
        assert required_anchors <= set(row["owner"])
        assert required_anchors <= set(row["candidate"])
    negative_by_name = {row["name"]: row for row in payload["hard_negatives"]}
    sessions = negative_by_name["same_day_distinct_sessions"]
    assert sessions["owner"]["time"] != sessions["candidate"]["time"]
    assert sessions["owner"]["source_url"] == sessions["candidate"]["source_url"]
    parent_child = negative_by_name["festival_parent_vs_independent_child"]
    assert parent_child["owner"]["role"] == "festival_parent"
    assert parent_child["candidate"]["role"] == "independent_child"
    siblings = negative_by_name["one_source_multiple_children"]
    assert siblings["owner"]["source_url"] == siblings["candidate"]["source_url"]
    assert siblings["owner"]["occurrence_key"] != siblings["candidate"]["occurrence_key"]
    # 14/14 executable semantic fixtures is 100%, above both 99% gates; each
    # named test also executes two post-owner exact packet replays.
    assert len(_AUGUST_POSITIVE_CASES) == len(_AUGUST_HARD_NEGATIVES) == 7


@pytest.mark.asyncio
async def test_explicit_grounded_hard_negative_creates_distinct_event(
    tmp_path,
    monkeypatch,
):
    db, result, owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix="exhibition-vs-excursion",
        candidate_title="Экскурсия по выставке Альбрехта Дюрера",
        candidate_type="экскурсия",
        candidate_time="15:00",
        decision=_typed_adjudicator_decision(
            action="create",
            relation="distinct_event",
            reason_code="distinct_show_keep",
            confidence=0.98,
            match_event_id=None,
            evidence=["время 15:00"],
            conflicts=["event 5703 is the exhibition, not the excursion"],
        ),
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.CREATED
    assert result.event_id != owner_id
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 2
        logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
    assert any(row.decision == "FINAL_DISTINCT" for row in logs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [
        _typed_adjudicator_decision(
            action="match",
            relation="same_event",
            reason_code="identical_anchors_dup",
            confidence=0.2,
            match_event_id=5703,
            evidence=["Дюрер: графика"],
        ),
        None,
    ],
)
async def test_rejected_match_and_provider_abstention_are_retry(
    tmp_path,
    monkeypatch,
    decision,
):
    db, result, _owner_id = await _run_vector_veto_adjudicator_replay(
        tmp_path,
        monkeypatch,
        suffix=f"retry-{decision is None}",
        decision=decision,
    )
    assert result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED
    async with db.get_session() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1
