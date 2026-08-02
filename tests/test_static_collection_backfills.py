from __future__ import annotations

import copy
import os
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

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
        _event(id=6, topics=[], is_free=False, source_text="Обычный концерт"),
        enabled_reasons={"admission", "audience", "people"},
    ) == ()
    # Text is a recall-only route; an age rating on its own is deliberately not.
    assert fact_backfill.route_backfill_reasons(
        _event(id=7, topics=[], is_free=False, age_restriction="6+", source_text="6+"),
        enabled_reasons={"audience"},
    ) == ()
    assert fact_backfill.route_backfill_reasons(
        _event(
            id=8,
            topics=[],
            is_free=False,
            source_text="Приглашаем родителей с детьми на занятие",
        ),
        enabled_reasons={"audience"},
    ) == ("audience",)


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


def _args(path, **values):
    defaults = {
        "db": str(path),
        "mode": "plan",
        "primary_only": False,
        "current_date": "2026-08-01",
        "reason": ["audience"],
        "event_id": [],
        "event_id_file": None,
        "source_id": [],
        "source_id_file": None,
        "limit": 100,
        "max_sources_per_event": 1,
        "output": None,
        "generator_command": "python3 scripts/backfill_static_collection_facts.py --plan",
    }
    defaults.update(values)
    return SimpleNamespace(**defaults)


def _v3_payload(*, value="unknown"):
    def decision(fact):
        if value == "unknown":
            return {
                "value": "unknown",
                "confidence": 0.0,
                "evidence_quote": "",
                "reason_code": "insufficient_evidence",
            }
        return {
            "value": "confirmed",
            "confidence": 0.9,
            "evidence_quote": "для всей семьи",
            "reason_code": {
                "child": "explicit_child_audience",
                "family": "explicit_family_invitation",
                "joint": "explicit_joint_task",
            }[fact],
        }

    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": "unknown",
            "evidence_quote": "",
            "reason_code": "insufficient_evidence",
        },
        "child_directed_decision": decision("child"),
        "family_suitable_decision": decision("family"),
        "joint_family_activity_decision": decision("joint"),
        "people_appearances": [],
    }


async def _seed_fact_db(path, *, sources=1, decisions=None):
    db = Database(str(path))
    await db.init()
    async with db.get_session() as session:
        event = _event(collection_decisions=decisions)
        session.add(event)
        await session.flush()
        for index in range(sources):
            session.add(
                _source(
                    event_id=int(event.id or 0),
                    source_url=f"https://t.me/example/{index + 1}",
                    imported_at=datetime(2026, 8, index + 1, tzinfo=timezone.utc),
                )
            )
        await session.commit()
        event_id = int(event.id or 0)
    await db.close()
    return event_id


def test_fact_backfill_cli_modes_and_bounds(tmp_path):
    assert fact_backfill.parse_args([]).mode == "plan"
    assert fact_backfill.parse_args(["--evaluate", "--event-id", "1"]).mode == "evaluate"
    assert fact_backfill.parse_args(["--apply", "--event-id", "1"]).mode == "apply"
    with pytest.raises(SystemExit):
        fact_backfill.parse_args(["--evaluate", "--apply", "--event-id", "1"])
    with pytest.raises(SystemExit):
        fact_backfill.parse_args(["--evaluate"])
    with pytest.raises(SystemExit):
        fact_backfill.parse_args(["--apply", "--event-id", "1", "--event-id", "1"])
    with pytest.raises(SystemExit):
        fact_backfill.parse_args(["--apply", "--event-id", "1", "--max-sources-per-event", "5"])
    event_file = tmp_path / "events.json"
    source_file = tmp_path / "sources.json"
    event_file.write_text('{"event_ids":[10]}', encoding="utf-8")
    source_file.write_text('{"source_ids":[20]}', encoding="utf-8")
    parsed = fact_backfill.parse_args(
        [
            "--evaluate",
            "--event-id-file",
            str(event_file),
            "--source-id-file",
            str(source_file),
        ]
    )
    assert fact_backfill.validate_args(parsed) == ([10], [20])


@pytest.mark.asyncio
async def test_fact_backfill_plan_is_no_call_read_only_and_reports_truncation(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    first_id = await _seed_fact_db(path)
    db = Database(str(path))
    async with db.get_session() as session:
        second = _event(title="Другая семейная встреча")
        session.add(second)
        await session.flush()
        session.add(_source(event_id=int(second.id or 0), source_url="https://t.me/example/20"))
        await session.commit()
        second_id = int(second.id or 0)
    await db.close()
    before = fact_backfill._sha256_file(path)

    async def should_not_call(_candidate):
        raise AssertionError("plan must not call provider")

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", should_not_call)
    report = await fact_backfill.run(_args(path, limit=1))
    assert report["mode"] == "plan"
    assert report["execution"] is None
    assert report["selection"]["eligible_event_count"] == 2
    assert report["selection"]["selection_truncated"] is True
    assert report["selection"]["omitted_event_ids"] == [second_id]
    assert report["selection"]["resolved_event_ids"] == [first_id]
    assert report["logical_diff"]["selected_event_allowlist_ok"] is True
    assert fact_backfill._sha256_file(path) == before


@pytest.mark.asyncio
async def test_fact_backfill_evaluate_calls_provider_but_never_writes(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path)
    before = fact_backfill._sha256_file(path)
    calls = []

    async def fake_adjudicator(candidate):
        calls.append(candidate.source_url)
        return _v3_payload()

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", fake_adjudicator)
    report = await fact_backfill.run(
        _args(path, mode="evaluate", event_id=[event_id], primary_only=True)
    )
    assert len(calls) == 1
    assert report["execution"]["provider_calls"] == 1
    assert report["execution"]["writes"] == 0
    assert report["execution"]["events"][0]["sources"][0]["status"] == "evaluated"
    assert report["logical_diff"]["sha256_before"] == report["logical_diff"]["sha256_after"]
    assert fact_backfill._sha256_file(path) == before


@pytest.mark.asyncio
async def test_fact_backfill_primary_only_is_scoped(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path)
    previous = os.environ.get("SMART_UPDATE_4O_FALLBACK")
    os.environ["SMART_UPDATE_4O_FALLBACK"] = "yes"

    async def fake_adjudicator(_candidate):
        assert os.environ["SMART_UPDATE_4O_FALLBACK"] == "0"
        assert os.environ["SMART_UPDATE_MODEL"] == "gemma-4-31b-it"
        assert os.environ["GOOGLE_AI_FALLBACK_MODELS"] == ""
        assert os.environ["SMART_UPDATE_GEMMA_RETRIES"] == "1"
        return _v3_payload()

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", fake_adjudicator)
    await fact_backfill.run(
        _args(path, mode="evaluate", event_id=[event_id], primary_only=True)
    )
    assert os.environ["SMART_UPDATE_4O_FALLBACK"] == "yes"
    if previous is None:
        os.environ.pop("SMART_UPDATE_4O_FALLBACK", None)
    else:
        os.environ["SMART_UPDATE_4O_FALLBACK"] = previous


@pytest.mark.asyncio
async def test_fact_backfill_audience_only_rejects_is_free_or_unrelated_mutation(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path)

    async def fake_adjudicator(_candidate):
        return _v3_payload(value="confirmed")

    def malicious_apply(event, _payload, **kwargs):
        assert kwargs["allowed_reasons"] == {"audience"}
        event.collection_decisions = {"receipt": "new"}
        event.is_free = False
        return True

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", fake_adjudicator)
    monkeypatch.setattr(fact_backfill, "apply_collection_decisions", malicious_apply)
    report = await fact_backfill.run(_args(path, mode="apply", event_id=[event_id]))
    source_report = report["execution"]["events"][0]["sources"][0]
    assert source_report["write_status"] == "rejected_forbidden_diff"
    assert "is_free" in source_report["changed_keys"]
    assert report["execution"]["writes"] == 0
    db = Database(str(path))
    async with db.get_session() as session:
        event = await session.get(Event, event_id)
        assert event.is_free is True
        assert event.collection_decisions is None
    await db.close()


@pytest.mark.asyncio
async def test_fact_backfill_provider_failure_preserves_existing_truth(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    truth = {"child_directed_decision": {"value": "confirmed", "manual_lock": True}}
    event_id = await _seed_fact_db(path, decisions=truth)

    async def failed(_candidate):
        return None

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", failed)
    report = await fact_backfill.run(_args(path, mode="apply", event_id=[event_id]))
    assert report["execution"]["deferred_sources"] == 1
    assert report["execution"]["writes"] == 0
    db = Database(str(path))
    async with db.get_session() as session:
        event = await session.get(Event, event_id)
        assert event.collection_decisions == truth
    await db.close()


@pytest.mark.asyncio
async def test_fact_backfill_warm_unknown_receipts_cover_multiple_sources(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path, sources=2)
    calls = []

    async def fake_adjudicator(candidate):
        calls.append(candidate.source_url)
        return _v3_payload()

    def receipt_covers(event, *, input_hash, source_id, **_kwargs):
        receipts = (event.collection_decisions or {}).get("evaluation_receipts", [])
        return any(
            row.get("source_id") == source_id and row.get("input_hash") == input_hash
            for row in receipts
        )

    def receipt_apply(event, _payload, *, source, input_hash, allowed_reasons, **_kwargs):
        assert allowed_reasons == {"audience"}
        decisions = copy.deepcopy(event.collection_decisions or {})
        receipts = decisions.setdefault("evaluation_receipts", [])
        receipts.append({"source_id": source.id, "input_hash": input_hash})
        event.collection_decisions = decisions
        return True

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", fake_adjudicator)
    monkeypatch.setattr(fact_backfill, "evaluation_receipt_covers", receipt_covers)
    monkeypatch.setattr(fact_backfill, "apply_collection_decisions", receipt_apply)
    args = _args(path, mode="apply", event_id=[event_id], max_sources_per_event=2)
    first = await fact_backfill.run(args)
    assert first["execution"]["provider_calls"] == 2
    assert first["execution"]["writes"] == 2
    assert len(calls) == 2
    second = await fact_backfill.run(args)
    assert second["execution"]["provider_calls"] == 0
    assert second["execution"]["writes"] == 0
    assert second["execution"]["cached_sources"] == 2
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_fact_backfill_plan_lists_unselected_sources_instead_of_silent_drop(tmp_path):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path, sources=2)
    report = await fact_backfill.run(_args(path, event_id=[event_id], max_sources_per_event=1))
    row = report["plan"][0]
    assert len(row["source_ids"]) == 1
    assert len(row["unselected_source_ids"]) == 1
    assert set(row["source_ids"]) | set(row["unselected_source_ids"])


@pytest.mark.asyncio
async def test_fact_backfill_explicit_source_binding_mismatch_fails_before_provider(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    first_event_id = await _seed_fact_db(path)
    db = Database(str(path))
    async with db.get_session() as session:
        second = _event(title="Second")
        session.add(second)
        await session.flush()
        source = _source(event_id=int(second.id or 0), source_url="https://t.me/example/other")
        session.add(source)
        await session.commit()
        wrong_source_id = int(source.id or 0)
    await db.close()

    async def should_not_call(_candidate):
        raise AssertionError("binding mismatch must fail before provider")

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", should_not_call)
    with pytest.raises(ValueError, match="binding mismatch"):
        await fact_backfill.run(
            _args(
                path,
                mode="evaluate",
                event_id=[first_event_id],
                source_id=[wrong_source_id],
            )
        )


@pytest.mark.asyncio
async def test_fact_backfill_evaluate_fails_when_requested_event_is_unresolved(tmp_path, monkeypatch):
    path = tmp_path / "events.sqlite"
    await _seed_fact_db(path)

    async def should_not_call(_candidate):
        raise AssertionError("unresolved allowlist must fail before provider")

    monkeypatch.setattr(fact_backfill, "adjudicate_collection_candidate", should_not_call)
    with pytest.raises(ValueError, match="missing, ineligible"):
        await fact_backfill.run(_args(path, mode="evaluate", event_id=[999999]))


@pytest.mark.asyncio
async def test_fact_backfill_explicit_source_replays_exact_source_not_ranked_default(tmp_path):
    path = tmp_path / "events.sqlite"
    event_id = await _seed_fact_db(path, sources=2)
    db = Database(str(path))
    async with db.get_session() as session:
        sources = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == event_id).order_by(EventSource.id)
                )
            ).scalars()
        )
        exact_source_id = int(sources[0].id or 0)
    await db.close()
    report = await fact_backfill.run(
        _args(
            path,
            event_id=[event_id],
            source_id=[exact_source_id],
            max_sources_per_event=1,
        )
    )
    assert report["plan"][0]["source_ids"] == [exact_source_id]
    assert report["selection"]["requested_source_ids"] == [exact_source_id]
    assert report["selection"]["requested_source_bindings"] == [
        {"event_id": event_id, "source_id": exact_source_id}
    ]


@pytest.mark.asyncio
async def test_fact_backfill_legacy_v2_audience_does_not_cover_v3(tmp_path):
    event = _event(
        id=1,
        collection_decisions={
            "audience_decision": {
                "value": "family",
                "input_hash": "a" * 64,
                "source_id": 1,
            }
        },
    )
    assert not fact_backfill.evaluation_receipt_covers(
        event,
        reasons={"audience"},
        input_hash="a" * 64,
        source_id=1,
    )


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
