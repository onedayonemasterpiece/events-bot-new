from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

import main
import smart_event_update as su
from db import Database
from models import Event, EventSource, TelegramSource
from source_parsing.telegram import handlers as tg_handlers


FIXTURE = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-08-15-ingestion-retry-stall-and-wal-growth"
    / "telegram_terminal_children.json"
)


def _fixture_child(label: str) -> dict:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return next(item for item in payload["children"] if item["label"] == label)[
        "candidate"
    ]


def _fixture_opposite() -> dict:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return dict(payload["opposite_control"]["candidate"])


async def _seed_source(db: Database, candidate: dict) -> None:
    username = str(candidate["source_chat_username"])
    metrics = dict(candidate.get("metrics") or {})
    async with db.get_session() as session:
        source = (
            await session.execute(
                select(TelegramSource).where(TelegramSource.username == username)
            )
        ).scalar_one_or_none()
        if source is None:
            source = TelegramSource(username=username, enabled=True)
            session.add(source)
        source.enabled = True
        source.trust_level = str(candidate.get("trust_level") or "high")
        source.default_location = metrics.get("tg_default_location")
        await session.commit()


def _result_payload(candidate: dict, *, run_id: str) -> dict:
    event = {
        key: candidate.get(key)
        for key in (
            "title",
            "date",
            "time",
            "end_date",
            "festival",
            "location_name",
            "location_address",
            "city",
            "ticket_link",
            "event_type",
            "is_free",
            "raw_excerpt",
        )
        if candidate.get(key) is not None
    }
    if candidate.get("producer_ordinal") is not None:
        event["_telegram_result_index"] = int(candidate["producer_ordinal"])
        event["_telegram_result_indexes"] = [int(candidate["producer_ordinal"])]
    message = {
        "source_username": candidate["source_chat_username"],
        "source_title": candidate["source_chat_username"],
        "message_id": candidate["source_message_id"],
        "message_date": "2026-08-16T21:40:00+00:00",
        "source_link": candidate["source_url"],
        "text": candidate["source_text"],
        "events": [event],
        "posters": candidate.get("posters") or [],
    }
    return {
        "schema_version": 2,
        "run_id": run_id,
        "generated_at": "2026-08-17T00:18:05+00:00",
        "stats": {
            "sources_total": 1,
            "messages_scanned": 1,
            "messages_with_events": 1,
            "events_extracted": 1,
        },
        "messages": [message],
    }


@pytest.fixture
def _linear_smart_stubs(monkeypatch):
    async def fake_bundle(*_args, **_kwargs):
        return {
            "title": None,
            "description": "Source-grounded incident replay event.",
            "short_description": "Source-grounded event.",
            "search_digest": "incident replay event",
            "facts": [],
        }

    async def grounded_bundle(*_args, **_kwargs):
        return True, "llm_grounded", []

    async def no_topics(*_args, **_kwargs):
        return None

    async def no_vector(*_args, **_kwargs):
        return None

    async def no_jobs(*_args, **_kwargs):
        return {}

    async def no_public_posters(*_args, **_kwargs):
        return []

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", False)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", False)
    monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", True)
    monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.ENFORCE)
    monkeypatch.setattr(
        su,
        "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE",
        su.IdentityGateMode.OFF,
    )
    monkeypatch.setattr(
        su, "_candidate_needs_llm_occurrence_scope_review", lambda _candidate: False
    )
    monkeypatch.setattr(
        su,
        "_candidate_needs_llm_anchor_role_review",
        lambda _candidate: (False, "fixture"),
    )
    monkeypatch.setattr(
        su, "_candidate_needs_llm_eventness_review", lambda *_args, **_kwargs: False
    )
    monkeypatch.setattr(su, "_llm_create_description_facts_and_digest", fake_bundle)
    monkeypatch.setattr(su, "_llm_review_create_bundle_grounding", grounded_bundle)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    monkeypatch.setattr(su, "_smart_update_identity_vector_evidence", no_vector)
    monkeypatch.setattr(main, "schedule_event_update_tasks", no_jobs)
    monkeypatch.setattr(
        tg_handlers,
        "_fallback_fetch_posters_from_public_tg_page",
        no_public_posters,
    )
    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")


@pytest.mark.asyncio
async def test_tretyakovka_profile_child_crosses_telegram_smart_db_boundary(
    tmp_path,
    monkeypatch,
    _linear_smart_stubs,
) -> None:
    candidate = _fixture_child("tretyakovka_festival")
    result_path = tmp_path / "tretyakovka-results.json"
    result_path.write_text(
        json.dumps(
            _result_payload(candidate, run_id="inc-20260815-tretyakovka-replay"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    async def fake_json(prompt, _schema, *, max_tokens, label):  # noqa: ANN001
        assert max_tokens > 0
        if label == "location_grounding_review":
            assert "configured_source_profile_location" in prompt
            return {
                "decision": "keep",
                "confidence": 0.99,
                "location_name": None,
                "location_address": None,
                "city": "Калининград",
                "evidence_quote": "в кинозале музея пройдут показы",
                "reason_short": "configured gallery cinema",
            }
        raise AssertionError(f"unexpected JSON stage {label}")

    monkeypatch.setattr(su, "_ask_gemma_json", fake_json)

    db = Database(str(tmp_path / "tretyakovka-shadow.sqlite"))
    await db.init()
    try:
        await _seed_source(db, candidate)
        report = await tg_handlers.process_telegram_results(result_path, db)

        assert report.events_created == 1
        assert report.events_merged == 0
        assert report.messages_terminal_errors == 0
        async with db.get_session() as session:
            saved = (
                await session.execute(
                    select(Event).where(Event.source_post_url == candidate["source_url"])
                )
            ).scalar_one()
        assert saved.title == candidate["title"]
        assert saved.location_name == candidate["location_name"]
        assert saved.location_address == candidate["location_address"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_explicit_venue_opposite_control_crosses_same_boundary_without_profile_override(
    tmp_path,
    monkeypatch,
    _linear_smart_stubs,
) -> None:
    candidate = _fixture_opposite()
    candidate.update(
        {
            "source_chat_username": "fixture_explicit_venue",
            "source_message_id": 1,
            "trust_level": "high",
        }
    )
    result_path = tmp_path / "opposite-results.json"
    result_path.write_text(
        json.dumps(
            _result_payload(candidate, run_id="inc-20260815-opposite-replay"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    async def fake_json(prompt, _schema, *, max_tokens, label):  # noqa: ANN001
        assert max_tokens > 0
        assert label == "location_grounding_review"
        assert "Другой зал" in prompt
        return {
            "decision": "repair",
            "confidence": 0.99,
            "location_name": "Другой зал",
            "location_address": "Другая 5",
            "city": "Калининград",
            "evidence_quote": "Другой зал, Другая 5",
            "reason_short": "explicit source venue overrides configured profile",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_json)

    db = Database(str(tmp_path / "opposite-shadow.sqlite"))
    await db.init()
    try:
        await _seed_source(db, candidate)
        report = await tg_handlers.process_telegram_results(result_path, db)

        assert report.events_created == 1
        assert report.messages_terminal_errors == 0
        async with db.get_session() as session:
            saved = (
                await session.execute(
                    select(Event).where(Event.source_post_url == candidate["source_url"])
                )
            ).scalar_one()
        assert saved.location_name == "Другой зал"
        assert saved.location_address == "Другая 5"
    finally:
        await db.close()


@pytest.mark.parametrize(
    ("label", "expected_name", "expected_address"),
    [
        ("kldevents_short_venue", "Заря", "Мира 41-43"),
        (
            "dramteatr_profile_alias",
            "Драматический театр",
            "Мира 4",
        ),
    ],
)
@pytest.mark.asyncio
async def test_missing_location_children_cross_telegram_smart_db_boundary(
    tmp_path,
    monkeypatch,
    _linear_smart_stubs,
    label: str,
    expected_name: str,
    expected_address: str,
) -> None:
    """Exact Aug-17 product-loss shapes must create, not merely terminalize."""

    candidate = _fixture_child(label)
    result_path = tmp_path / f"{label}-results.json"
    result_path.write_text(
        json.dumps(
            _result_payload(candidate, run_id=f"inc-20260815-{label}-replay"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    async def unexpected_location_review(*_args, **_kwargs):
        raise AssertionError("source/profile-grounded venue must not be discarded")

    monkeypatch.setattr(su, "_ask_gemma_json", unexpected_location_review)

    db = Database(str(tmp_path / f"{label}-shadow.sqlite"))
    await db.init()
    try:
        await _seed_source(db, candidate)
        report = await tg_handlers.process_telegram_results(result_path, db)

        assert report.events_created == 1
        assert report.events_merged == 0
        assert report.messages_terminal_errors == 0
        async with db.get_session() as session:
            saved = (
                await session.execute(
                    select(Event).where(Event.source_post_url == candidate["source_url"])
                )
            ).scalar_one()
        assert saved.location_name == expected_name
        assert saved.location_address == expected_address
        assert saved.city == "Калининград"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_kozia_yoga_child_gets_typed_distinct_decision_and_is_created(
    tmp_path,
    monkeypatch,
    _linear_smart_stubs,
) -> None:
    candidate = _fixture_child("kozia_yoga")
    result_path = tmp_path / "kozia-results.json"
    result_path.write_text(
        json.dumps(
            _result_payload(candidate, run_id="inc-20260815-kozia-replay"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    async def fake_match_bundle(*_args, **_kwargs):
        return {
            "action": "create",
            "match_event_id": None,
            "confidence": 0.99,
            "reason_short": "separate programme child",
            "bundle": await fake_create_bundle(),
        }

    async def fake_create_bundle():
        return {
            "title": None,
            "description": "Йога с козами на фоне подсолнухов.",
            "short_description": "Йога с козами.",
            "search_digest": "йога козы экоферма",
            "facts": [],
        }

    async def fake_adjudicator(candidate_value, events, **_kwargs):
        assert candidate_value.title == candidate["title"]
        assert any(event.title == "Праздник сыра и сидра" for event in events)
        return {
            "action": "create",
            "match_event_id": None,
            "confidence": 0.99,
            "reason_code": "different_programme",
            "reason": "отдельная активность в программе",
        }

    monkeypatch.setattr(su, "_llm_match_or_create_bundle", fake_match_bundle)
    monkeypatch.setattr(su, "_llm_dedup_adjudicator", fake_adjudicator)

    db = Database(str(tmp_path / "kozia-shadow.sqlite"))
    await db.init()
    try:
        await _seed_source(db, candidate)
        async with db.get_session() as session:
            owner = Event(
                title="Праздник сыра и сидра",
                description="Общий праздник.",
                source_text=candidate["source_text"],
                source_post_url=candidate["source_url"],
                date=candidate["date"],
                time=candidate["time"],
                location_name=candidate["location_name"],
                location_address=candidate["location_address"],
                city=candidate["city"],
                event_type="фестиваль",
                ticket_link=candidate["ticket_link"],
            )
            session.add(owner)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(owner.id),
                    source_type="telegram",
                    source_url=candidate["source_url"],
                    canonical_source_url=candidate["source_url"],
                    source_role="identity_bearing",
                    source_chat_username=candidate["source_chat_username"],
                    source_message_id=int(candidate["source_message_id"]),
                    candidate_key="parent-festival-owner",
                    occurrence_key="parent-festival-owner",
                    source_text=candidate["source_text"],
                    trust_level="high",
                )
            )
            await session.commit()
            owner_id = int(owner.id)

        report = await tg_handlers.process_telegram_results(result_path, db)

        assert report.events_created == 1
        assert report.events_merged == 0
        assert report.messages_terminal_errors == 0
        async with db.get_session() as session:
            events = (await session.execute(select(Event).order_by(Event.id))).scalars().all()
            event_count = int(await session.scalar(select(func.count()).select_from(Event)))
        async with db.raw_conn() as conn:
            identity_row = await (
                await conn.execute(
                    "SELECT occurrence_key FROM smart_update_candidate_state "
                    "WHERE canonical_source_url=? ORDER BY id DESC LIMIT 1",
                    (candidate["source_url"],),
                )
            ).fetchone()
        assert event_count == 2
        assert identity_row is not None
        assert str(identity_row[0]).endswith(":ordinal:1")
        created = next(event for event in events if int(event.id) != owner_id)
        assert created.title == candidate["title"]
        assert created.source_post_url == candidate["source_url"]
    finally:
        await db.close()
