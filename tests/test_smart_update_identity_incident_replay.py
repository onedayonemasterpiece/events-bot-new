from __future__ import annotations

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
) -> tuple[Database, su.SmartUpdateResult, int]:
    db = Database(str(tmp_path / f"typed-final-{suffix}.sqlite"))
    await db.init()
    owner_id = 5703
    await _seed_event(
        db,
        event_id=owner_id,
        title="Альбрехт Дюрер",
        date="2026-07-01",
        end_date="2026-08-30",
        time="",
        location_name="Музей",
        event_type="выставка",
    )
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_classify_topics", _no_topics)
    async def _eventness(*_args, **_kwargs):
        return "event", 0.99, "sanitized fixture"

    monkeypatch.setattr(su, "_llm_review_candidate_eventness", _eventness)
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
        source_url=f"https://t.me/incident_replay/{suffix}",
        source_text=(
            f"{candidate_title}. Отдельное описание из источника; "
            f"время {candidate_time or 'не указано'}."
        ),
        raw_excerpt=candidate_title,
        title=candidate_title,
        date="2026-07-02",
        end_date="2026-08-20" if candidate_type == "выставка" else None,
        time=candidate_time,
        location_name="Музей",
        # Deliberate metadata drift keeps the vector owner out of the ordinary
        # exact city shortlist; vector handoff must still reach the same call.
        city="Балтийск",
        event_type=candidate_type,
    )
    result = await smart_event_update(
        db,
        candidate,
        check_source_url=False,
        schedule_tasks=False,
    )
    assert calls["dedup"] == 1
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
