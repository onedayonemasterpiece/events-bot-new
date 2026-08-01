from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from db import Database
from models import Event, EventSource, InterestClub
from scripts import backfill_interest_club_relations as club_backfill
from scripts import backfill_static_collection_facts as fact_backfill
from smart_event_update import STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION


def _event(**values) -> Event:
    defaults = {
        "title": "Семейная встреча",
        "description": "Описание",
        "date": "2026-08-10",
        "time": "18:00",
        "location_name": "Зал",
        "source_text": "Вход свободный. Событие для всей семьи.",
        "topics": ["FAMILY"],
        "is_free": True,
    }
    defaults.update(values)
    return Event(**defaults)


def _source(**values) -> EventSource:
    defaults = {
        "event_id": 1,
        "source_type": "telegram",
        "source_url": "https://t.me/example/1",
        "source_text": "Вход свободный. Событие для всей семьи.",
        "trust_level": "official",
        "imported_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }
    defaults.update(values)
    return EventSource(**defaults)


def test_fact_backfill_routes_only_current_high_recall_candidates():
    event = _event(id=5)
    assert fact_backfill.event_is_current(event, current_date=date(2026, 8, 1))
    assert fact_backfill.route_backfill_reasons(
        event,
        enabled_reasons={"admission", "audience", "people"},
    ) == ("admission", "audience")
    assert fact_backfill.route_backfill_reasons(
        _event(id=6, topics=[], is_free=False),
        enabled_reasons={"admission", "audience", "people"},
    ) == ()


def test_fact_backfill_prefers_trust_then_recency_and_builds_source_bound_candidate():
    older_official = _source(id=1, imported_at=datetime(2026, 7, 1, tzinfo=timezone.utc))
    newer_medium = _source(
        id=2,
        trust_level="medium",
        imported_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
    )
    selected = fact_backfill.select_sources([newer_medium, older_official], maximum=1)
    assert [row.id for row in selected] == [1]
    candidate = fact_backfill.build_candidate(_event(id=1), selected[0], ["admission"])
    assert candidate.source_url == older_official.source_url
    assert candidate.collection_adjudication_reasons == ["admission", "backfill"]


@pytest.mark.asyncio
async def test_fact_backfill_plan_and_apply_are_bounded_and_resumable(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        source = _source(event_id=int(event.id or 0))
        session.add(source)
        await session.commit()

    plan = await fact_backfill.build_plan(
        db,
        current_date=date(2026, 8, 1),
        enabled_reasons={"admission", "audience"},
        event_ids=set(),
        limit=1,
        max_sources_per_event=1,
    )
    assert len(plan) == 1

    async def fake_adjudicator(candidate):
        return {
            "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
            "admission_decision": {
                "value": "confirmed_free",
                "evidence_quote": "Вход свободный",
                "reason_code": "explicit_free_admission",
            },
            "audience_decision": {
                "value": "family",
                "confidence": 0.95,
                "evidence_quote": "для всей семьи",
                "reason_code": "explicit_family_format",
            },
            "people_appearances": [],
        }

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", fake_adjudicator)
    first = await fact_backfill.apply_plan(db, plan)
    assert first["applied_sources"] == 1
    second = await fact_backfill.apply_plan(db, plan)
    assert second["attempted_sources"] == 0
    assert second["unchanged_sources"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_club_backfill_plans_exact_registry_candidates_and_only_enqueues_on_apply(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "clubs.sqlite"))
    await db.init()
    async with db.get_session() as session:
        club = InterestClub(
            slug="game-vibes",
            canonical_name="Game Vibes",
            topic="настольные игры",
            public_status="approved",
            aliases_json=["Game Vibes"],
            source_anchors_json=["gamevibes"],
        )
        event = _event(title="Встреча Game Vibes", is_free=False, topics=[])
        session.add(club)
        session.add(event)
        await session.flush()
        session.add(
            _source(
                event_id=int(event.id or 0),
                source_url="https://t.me/gamevibes/10",
                source_chat_username="gamevibes",
                source_text="Game Vibes проводит встречу клуба.",
            )
        )
        await session.commit()

    plan = await club_backfill.build_plan(db, cutoff=date(2026, 2, 1), limit=10)
    assert len(plan) == 1
    assert plan[0].lanes == ("source",)

    calls = []

    async def fake_schedule(_db, event_id, *, schedule_projection):
        calls.append((event_id, schedule_projection))
        return "created"

    monkeypatch.setattr(club_backfill, "schedule_interest_club_evaluation", fake_schedule)
    args = type(
        "Args",
        (),
        {
            "db": str(tmp_path / "clubs.sqlite"),
            "apply": True,
            "cutoff": "2026-02-01",
            "limit": 10,
        },
    )()
    result = await club_backfill.run(args)
    assert result["candidate_count"] == 1
    assert result["actions"][0]["action"] == "created"
    assert calls and calls[0][1] is False
    await db.close()
