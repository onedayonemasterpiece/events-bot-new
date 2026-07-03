from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy import func, select

import smart_event_update as su
from db import Database
from models import Event, EventIdentityDecisionLog, EventPoster, EventSource
from smart_event_update import EventCandidate, smart_event_update
from smart_update_identity import (
    IdentityGateMode,
    MergeIdentityAction,
    MergeIdentityRelation,
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


async def _no_topics(*_args, **_kwargs):
    return None


async def _seed_boyko_lecture(db: Database, event_id: int = 5077) -> None:
    async with db.get_session() as session:
        session.add(
            Event(
                id=event_id,
                title="Калининград и область как кинодекорация",
                description="Лекция Андрея Бойко о Калининграде и области как кинодекорации.",
                date="2026-07-03",
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


@pytest.mark.asyncio
async def test_boyko_exhibition_regression_merge_gate_blocks_side_effects(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "boyko.sqlite"))
    await db.init()
    try:
        await _seed_boyko_lecture(db)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
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
async def test_merge_gate_allows_same_event_source_update(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "same-event.sqlite"))
    await db.init()
    try:
        await _seed_boyko_lecture(db)
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
            source_text="3 июля в 19:00 Андрей Бойко прочитает лекцию «Калининград и область как кинодекорация».",
            title="Калининград и область как кинодекорация",
            date="2026-07-03",
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
