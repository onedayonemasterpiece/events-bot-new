from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import (
    Event,
    EventIdentityDecisionLog,
    EventPoster,
    EventSource,
    EventSourceFact,
    FestivalQueueItem,
    JobOutbox,
)
from smart_event_update import EventCandidate, smart_event_update
from smart_update_identity import (
    IdentityGateAction,
    IdentityGateMode,
    MergeIdentityAction,
    MergeIdentityRelation,
    build_identity_gate_verdict,
    build_merge_identity_gate_verdict,
)


@dataclass
class _Obj:
    title: str
    date: str
    time: str | None = None
    end_date: str | None = None
    location_name: str | None = None
    city: str | None = "Калининград"
    event_type: str | None = None
    id: int | None = None
    source_type: str | None = None
    source_url: str | None = None
    ticket_link: str | None = None


async def _no_topics(*_args, **_kwargs):
    return None


async def _seed_boyko_lecture(db: Database, event_id: int = 5077, event_date: str = "2026-07-03") -> None:
    async with db.get_session() as session:
        session.add(
            Event(
                id=event_id,
                title="Калининград и область как кинодекорация",
                description="Лекция Андрея Бойко о Калининграде и области как кинодекорации.",
                date=event_date,
                time="19:00",
                location_name="Музей изобразительных искусств",
                location_address="Ленинский проспект, 83",
                city="Калининград",
                event_type="лекция",
                source_text="3 июля Андрей Бойко прочитает лекцию «Калининград и область как кинодекорация».",
                source_post_url="https://t.me/source/5077",
                identity_status="canonical",
            )
        )
        await session.commit()


async def _seed_zhenitba_performance(db: Database, event_id: int = 5756) -> None:
    async with db.get_session() as session:
        session.add(
            Event(
                id=event_id,
                title="Женитьба",
                description="Спектакль по пьесе Николая Гоголя.",
                date="2026-08-09",
                time="18:00",
                location_name="Драматический театр",
                location_address="проспект Мира, 4",
                city="Калининград",
                event_type="спектакль",
                ticket_link="https://dramteatr39.ru/spektakli/jenitba",
                source_text="09.08 в 18:00 — спектакль «Женитьба».",
                source_post_url="https://dramteatr39.ru/spektakli/jenitba",
                identity_status="canonical",
            )
        )
        await session.commit()


def _theatre_tour_candidate() -> EventCandidate:
    return EventCandidate(
        source_type="parser:dramteatr",
        source_url='https://dramteatr39.ru/spektakli/ekskursiya-"zakulise-teatra"',
        source_text=(
            "Название: Экскурсия «Закулисье театра»\n"
            "Дата: 2026-08-09\n"
            "Время: 14:30\n"
            "Площадка: Драматический театр\n"
            "Описание: экскурсия по сцене и закулисным помещениям театра."
        ),
        raw_excerpt="9 августа в 14:30 — экскурсия «Закулисье театра».",
        title="Экскурсия «Закулисье театра»",
        date="2026-08-09",
        time="14:30",
        location_name="Драматический театр",
        location_address="проспект Мира, 4",
        city="Калининград",
        event_type="экскурсия",
        ticket_link='https://dramteatr39.ru/spektakli/ekskursiya-"zakulise-teatra"',
        trust_level="high",
    )


def _zhenitba_source_update_candidate() -> EventCandidate:
    return EventCandidate(
        source_type="parser:dramteatr",
        source_url="https://dramteatr39.ru/spektakli/jenitba?source=august-update",
        source_text="9 августа в 18:00 — спектакль «Женитьба». Обновлено описание постановки.",
        raw_excerpt="9 августа в 18:00 — спектакль «Женитьба».",
        title="Женитьба",
        date="2026-08-09",
        time="18:00",
        location_name="Драматический театр",
        location_address="проспект Мира, 4",
        city="Калининград",
        event_type="спектакль",
        ticket_link="https://dramteatr39.ru/spektakli/jenitba",
        trust_level="high",
    )


def _boyko_exhibition_candidate() -> EventCandidate:
    return EventCandidate(
        source_type="telegram",
        source_url="https://t.me/museum/7001",
        source_text=(
            "Открытие выставки «Калининградская область. История любви». "
            "Экспозиция работает с 3 июля по 3 августа в музее. "
            "В программе упоминается встреча с авторами проекта."
        ),
        raw_excerpt="Выставка «Калининградская область. История любви» с 3 июля по 3 августа.",
        title="Калининград и область как кинодекорация: выставка «Калининградская область. История любви»",
        date="2026-07-03",
        end_date="2026-08-03",
        time="",
        time_is_default=True,
        location_name="Музей изобразительных искусств",
        location_address="Ленинский проспект, 83",
        city="Калининград",
        event_type="выставка",
    )


def test_merge_gate_off_allows_even_suspicious_candidate() -> None:
    existing = _Obj(
        id=5077,
        title="Калининград и область как кинодекорация",
        date="2026-07-03",
        time="19:00",
        location_name="Музей",
        event_type="лекция",
    )
    candidate = _Obj(
        title="Выставка «Калининградская область. История любви»",
        date="2026-07-03",
        end_date="2026-08-03",
        location_name="Музей",
        event_type="выставка",
    )

    verdict = build_merge_identity_gate_verdict(candidate, existing, mode=IdentityGateMode.OFF)

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects


def test_merge_gate_enforce_skips_llm_distinct_sibling() -> None:
    existing = _Obj(
        id=5077,
        title="Калининград и область как кинодекорация",
        date="2026-07-03",
        time="19:00",
        location_name="Музей",
        event_type="лекция",
    )
    candidate = _Obj(
        title="Выставка «Калининградская область. История любви»",
        date="2026-07-03",
        end_date="2026-08-03",
        location_name="Музей",
        event_type="выставка",
    )

    verdict = build_merge_identity_gate_verdict(
        candidate,
        existing,
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "skip_merge_side_effects",
            "relation": "festival_context_sibling",
            "confidence": 0.91,
            "reason_code": "festival_sibling_not_same_event",
            "reason": "выставка и лекция связаны контекстом, но это разные события",
            "blocking_conflicts": ["event_type"],
            "allowed_fields": [],
        },
    )

    assert verdict.action is MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS
    assert verdict.relation is MergeIdentityRelation.FESTIVAL_CONTEXT_SIBLING
    assert verdict.should_skip_side_effects


def test_merge_gate_shadow_reports_skip_without_enforcing() -> None:
    verdict = build_merge_identity_gate_verdict(
        _Obj(title="Выставка", date="2026-07-03", end_date="2026-08-03", event_type="выставка"),
        _Obj(id=1, title="Лекция", date="2026-07-03", time="19:00", event_type="лекция"),
        mode=IdentityGateMode.SHADOW,
        llm_data={
            "action": "review_required",
            "relation": "unsafe_to_merge",
            "confidence": 0.8,
            "reason_code": "insufficient_identity_evidence",
            "reason": "нет доказательства тождества",
            "blocking_conflicts": [],
            "allowed_fields": [],
        },
    )

    assert verdict.would_skip_side_effects
    assert not verdict.should_skip_side_effects


def test_merge_gate_positive_same_event_update_allowed() -> None:
    verdict = build_merge_identity_gate_verdict(
        _Obj(title="Лекция Андрея Бойко", date="2026-07-03", time="19:00", event_type="лекция"),
        _Obj(id=5077, title="Лекция Андрея Бойко", date="2026-07-03", time="19:00", event_type="лекция"),
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.96,
            "reason_code": "same_event_update",
            "reason": "совпадают название, дата, время и тип события",
            "blocking_conflicts": [],
            "allowed_fields": ["source", "description"],
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects


def test_theatre_tour_vs_performance_explicit_slot_conflict_is_structural_veto() -> None:
    """INC replay: same date/venue must not glue a 14:30 tour into an 18:00 play."""

    verdict = build_merge_identity_gate_verdict(
        _Obj(
            title="Экскурсия «Закулисье театра»",
            date="2026-08-09",
            time="14:30",
            location_name="Драматический театр",
            event_type="экскурсия",
            ticket_link='https://dramteatr39.ru/spektakli/ekskursiya-"zakulise-teatra"',
        ),
        _Obj(
            id=5756,
            title="Женитьба",
            date="2026-08-09",
            time="18:00",
            location_name="Драматический театр",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/spektakli/jenitba",
        ),
        mode=IdentityGateMode.ENFORCE,
        # Even an erroneous high-confidence allow may not override four exact
        # structural conflicts with no shared source/ticket/poster identity.
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
            "reason": "same date and venue",
            "blocking_conflicts": [],
            "allowed_fields": ["source", "description"],
        },
    )

    assert verdict.action is MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS
    assert verdict.should_skip_side_effects
    assert verdict.deterministic
    assert verdict.reason_code == "same_place_date_unrelated_type_time_conflict"


def test_same_performance_time_correction_with_specific_ticket_anchor_stays_allowed() -> None:
    """Positive control: explicit time drift alone must not block a grounded correction."""

    verdict = build_merge_identity_gate_verdict(
        _Obj(
            title="Женитьба",
            date="2026-08-09",
            time="18:30",
            location_name="Драматический театр",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/spektakli/jenitba",
        ),
        _Obj(
            id=5756,
            title="Женитьба",
            date="2026-08-09",
            time="18:00",
            location_name="Драматический театр",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/spektakli/jenitba",
        ),
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.98,
            "reason_code": "same_ticket_source_update",
            "reason": "same title and specific ticket page; source corrects start time",
            "blocking_conflicts": [],
            "allowed_fields": ["time", "source"],
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects


def test_same_date_venue_without_two_explicit_times_is_not_a_structural_veto() -> None:
    """The narrow rail must never degrade into a date-plus-venue rule."""

    verdict = build_merge_identity_gate_verdict(
        _Obj(
            title="Экскурсия «Закулисье театра»",
            date="2026-08-09",
            time=None,
            location_name="Драматический театр",
            event_type="экскурсия",
            ticket_link="https://dramteatr39.ru/tour",
        ),
        _Obj(
            id=5756,
            title="Женитьба",
            date="2026-08-09",
            time="18:00",
            location_name="Драматический театр",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/jenitba",
        ),
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.99,
            "reason_code": "same_event_update",
            "reason": "approved semantic positive control",
            "blocking_conflicts": [],
            "allowed_fields": ["source"],
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects
    assert not verdict.deterministic


def test_same_programme_multi_session_same_place_date_stays_allowed() -> None:
    """Legitimate same-date, same-venue sessions remain merge-eligible."""

    verdict = build_merge_identity_gate_verdict(
        _Obj(
            title="Кураторская экскурсия по выставке",
            date="2026-08-09",
            time="15:00",
            location_name="Музей",
            event_type="экскурсия",
            ticket_link="https://museum.example/tour",
        ),
        _Obj(
            id=5426,
            title="Кураторская экскурсия по выставке",
            date="2026-08-09",
            time="12:00",
            location_name="Музей",
            event_type="экскурсия",
            ticket_link="https://museum.example/tour",
        ),
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "source_update",
            "confidence": 0.97,
            "reason_code": "same_programme_source_update",
            "reason": "same programme and specific source anchor",
            "blocking_conflicts": [],
            "allowed_fields": ["source"],
        },
    )

    assert verdict.action is MergeIdentityAction.ALLOW_MERGE
    assert not verdict.should_skip_side_effects
    assert not verdict.deterministic


def test_merge_gate_blocks_single_occurrence_into_recurring_series_even_with_ticket_anchor() -> None:
    """INC-2026-07-09: a fresh exact occurrence must not mutate the stale season row."""

    recurring = _Obj(
        id=3980,
        title="Рыцарский турнир",
        date="2026-05-01",
        time="20:00",
        end_date="2026-09-30",
        location_name="Замок Нойхаузен, Заречная 2А, Гурьевск",
        event_type="турнир",
        ticket_link="https://turnir39.ru",
    )
    occurrence = _Obj(
        title="Рыцарский турнир",
        date="2026-07-10",
        time="20:00",
        location_name="Замок Нойхаузен, Заречная 2А, Гурьевск",
        event_type="турнир",
        ticket_link="https://turnir39.ru",
        source_url="https://vk.com/wall-222073295_9296",
    )

    verdict = build_merge_identity_gate_verdict(
        occurrence,
        recurring,
        mode=IdentityGateMode.ENFORCE,
        llm_data={
            "action": "allow_merge",
            "relation": "same_event",
            "confidence": 0.97,
            "reason_code": "same_event_update",
            "reason": "same title, place and ticket",
            "blocking_conflicts": [],
            "allowed_fields": ["source", "poster"],
        },
    )

    assert verdict.action is MergeIdentityAction.SKIP_MERGE_SIDE_EFFECTS
    assert verdict.should_skip_side_effects
    assert verdict.reason_code == "single_occurrence_vs_recurring_series"


def test_create_gate_allows_single_occurrence_next_to_recurring_series_same_ticket() -> None:
    """The create-path identity gate must not veto a real dated occurrence."""

    recurring = _Obj(
        id=3980,
        title="Рыцарский турнир",
        date="2026-05-01",
        time="20:00",
        end_date="2026-09-30",
        location_name="Замок Нойхаузен, Заречная 2А, Гурьевск",
        event_type="турнир",
        ticket_link="https://turnir39.ru",
    )
    occurrence = _Obj(
        title="Рыцарский турнир",
        date="2026-07-10",
        time="20:00",
        location_name="Замок Нойхаузен, Заречная 2А, Гурьевск",
        event_type="турнир",
        ticket_link="https://turnir39.ru",
        source_url="https://vk.com/wall-222073295_9296",
    )

    verdict = build_identity_gate_verdict(
        occurrence,
        [recurring],
        mode=IdentityGateMode.ENFORCE,
    )

    assert verdict.action is IdentityGateAction.ALLOW_CREATE
    assert not verdict.should_veto_create


@pytest.mark.asyncio
async def test_boyko_exhibition_regression_merge_gate_blocks_side_effects(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "boyko.sqlite"))
    await db.init()
    try:
        await _seed_boyko_lecture(db)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _gate(*_args, **_kwargs):
            return {
                "action": "skip_merge_side_effects",
                "relation": "festival_context_sibling",
                "confidence": 0.93,
                "reason_code": "festival_sibling_not_same_event",
                "reason": "выставка и лекция связаны одной площадкой, но это разные события",
                "blocking_conflicts": ["event_type", "end_date"],
                "allowed_fields": [],
            }

        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)

        result = await smart_event_update(
            db,
            _boyko_exhibition_candidate(),
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.status == "skipped_identity_gate"
        assert result.event_id == 5077
        assert result.reason == "festival_sibling_not_same_event"
        async with db.get_session() as session:
            ev = await session.get(Event, 5077)
            sources_count = await session.scalar(select(func.count()).select_from(EventSource))
            posters_count = await session.scalar(select(func.count()).select_from(EventPoster))
            logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert ev is not None
        assert ev.title == "Калининград и область как кинодекорация"
        assert ev.event_type == "лекция"
        assert ev.end_date is None
        assert "История любви" not in (ev.description or "")
        assert sources_count == 0
        assert posters_count == 0
        assert logs
        assert logs[-1].event_id == 5077
        assert logs[-1].decision == "skip_merge_side_effects"
        assert logs[-1].decision_payload["stage"] == "merge_identity_gate"
        assert logs[-1].decision_payload["would_skip_side_effects"] is True
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_merge_gate_internal_error_enforce_is_zero_side_effect_fail_closed(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "gate-error.sqlite"))
    await db.init()
    try:
        await _seed_boyko_lecture(db)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _gate_error(*_args, **_kwargs):
            raise RuntimeError("synthetic identity gate failure")

        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate_error)
        async with db.get_session() as session:
            event_before = await session.get(Event, 5077)
            assert event_before is not None
            event_snapshot = (event_before.title, event_before.description, event_before.date, event_before.time, event_before.event_type)

        result = await smart_event_update(
            db, _boyko_exhibition_candidate(), check_source_url=False, schedule_tasks=True
        )
        assert result.status == "skipped_identity_gate"
        assert result.created is False and result.merged is False

        async with db.get_session() as session:
            event_after = await session.get(Event, 5077)
            assert event_after is not None
            assert (event_after.title, event_after.description, event_after.date, event_after.time, event_after.event_type) == event_snapshot
            for model in (EventSource, EventSourceFact, EventPoster, FestivalQueueItem, JobOutbox):
                assert int(await session.scalar(select(func.count()).select_from(model))) == 0
            decisions = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
            assert len(decisions) == 1
            assert decisions[0].decision_payload["fail_safe"] is True
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_source_delta"),
    [
        (IdentityGateMode.SHADOW, "merged_or_nochange", 1),
        (IdentityGateMode.ENFORCE, "skipped_identity_gate", 0),
    ],
)
async def test_theatre_tour_performance_incident_replay_requires_enforce(
    tmp_path,
    monkeypatch,
    mode: IdentityGateMode,
    expected_status: str,
    expected_source_delta: int,
) -> None:
    """Replay the 5756 pollution path and prove shadow is observability-only."""

    db = Database(str(tmp_path / f"theatre-{mode.value}.sqlite"))
    await db.init()
    try:
        await _seed_zhenitba_performance(db)
        candidate = _theatre_tour_candidate()
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", mode)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _force_replayed_final_match(db_arg, _candidate):
            async with db_arg.get_session() as session:
                return await session.get(Event, 5756)

        async def _gate(*_args, **_kwargs):
            return {
                "action": "skip_merge_side_effects",
                "relation": "related_but_distinct",
                "confidence": 0.99,
                "reason_code": "theatre_tour_vs_performance_slot_conflict",
                "reason": "14:30 theatre tour and 18:00 performance are separate occurrences",
                "blocking_conflicts": ["title", "time", "event_type", "ticket_link"],
                "allowed_fields": [],
            }

        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _force_replayed_final_match)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)

        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )

        if expected_status == "merged_or_nochange":
            assert result.status in {"merged", "skipped_nochange"}
            assert result.status != "skipped_identity_gate"
        else:
            assert result.status == expected_status
        async with db.get_session() as session:
            event = await session.get(Event, 5756)
            source_count = await session.scalar(
                select(func.count()).select_from(EventSource).where(
                    EventSource.event_id == 5756,
                    EventSource.source_url == candidate.source_url,
                )
            )
            logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert event is not None
        assert source_count == expected_source_delta
        assert logs
        assert logs[-1].decision == "skip_merge_side_effects"
        assert logs[-1].decision_payload["mode"] == mode.value
        assert logs[-1].decision_payload["would_skip_side_effects"] is True
        if mode is IdentityGateMode.ENFORCE:
            assert event.title == "Женитьба"
            assert event.event_type == "спектакль"
            assert event.time == "18:00"
            assert "экскурс" not in (event.description or "").lower()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_theatre_same_performance_source_update_survives_enforce(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "theatre-positive.sqlite"))
    await db.init()
    try:
        await _seed_zhenitba_performance(db)
        candidate = _zhenitba_source_update_candidate()
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _force_replayed_final_match(db_arg, _candidate):
            async with db_arg.get_session() as session:
                return await session.get(Event, 5756)

        async def _gate(*_args, **_kwargs):
            return {
                "action": "allow_merge",
                "relation": "same_event",
                "confidence": 0.99,
                "reason_code": "same_ticket_source_update",
                "reason": "same title, slot and specific ticket page",
                "blocking_conflicts": [],
                "allowed_fields": ["source", "description"],
            }

        monkeypatch.setattr(su, "_match_existing_event_by_source_anchor", _force_replayed_final_match)
        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)

        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.status in {"merged", "skipped_nochange"}
        assert result.status != "skipped_identity_gate"
        async with db.get_session() as session:
            source_count = await session.scalar(
                select(func.count()).select_from(EventSource).where(
                    EventSource.event_id == 5756,
                    EventSource.source_url == candidate.source_url,
                )
            )
            logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert source_count == 1
        assert logs[-1].decision == "allow_merge"
        assert logs[-1].decision_payload["mode"] == "enforce"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_merge_gate_allows_same_event_source_update(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "same-event.sqlite"))
    await db.init()
    try:
        await _seed_boyko_lecture(db, event_date="2099-07-13")
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _gate(*_args, **_kwargs):
            return {
                "action": "allow_merge",
                "relation": "same_event",
                "confidence": 0.97,
                "reason_code": "same_event_update",
                "reason": "совпадает лекция, дата, время и площадка",
                "blocking_conflicts": [],
                "allowed_fields": ["source"],
            }

        monkeypatch.setattr(su, "_llm_merge_identity_gate", _gate)
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/museum/5078",
            source_text="13 июля 2099 года в 19:00 Андрей Бойко прочитает лекцию «Калининград и область как кинодекорация».",
            title="Калининград и область как кинодекорация",
            date="2099-07-13",
            time="19:00",
            location_name="Музей изобразительных искусств",
            location_address="Ленинский проспект, 83",
            city="Калининград",
            event_type="лекция",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status in {"merged", "skipped_nochange"}
        assert result.status != "skipped_identity_gate"
        async with db.get_session() as session:
            logs = (await session.execute(select(EventIdentityDecisionLog))).scalars().all()
        assert logs
        assert logs[-1].decision == "allow_merge"
    finally:
        await db.close()
