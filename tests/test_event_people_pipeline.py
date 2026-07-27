from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select

from db import Database
from event_people.service import (
    CATALOG_PATH,
    ensure_kgd80_registry,
    grounded_people_decisions,
    load_kgd80_catalog,
    registry_candidates_in_text,
    sync_event_people,
)
from models import ArtistRegistryEntity, Event, EventArtistAppearance
import smart_event_update as su


ROOT = Path(__file__).resolve().parents[1]


def event() -> Event:
    return Event(
        title="Лекция Андрея Левченкова",
        description="Андрей Викторович Левченков расскажет об истории региона.",
        date="2026-08-15",
        time="19:00",
        location_name="Библиотека",
        city="Калининград",
        source_text=(
            "Главный спикер — Андрей Викторович Левченков. "
            "Он выступит очно с лекцией об истории региона."
        ),
        source_texts=[],
        photo_urls=[],
    )


def decision_payload(*, people: list[dict] | None = None, complete: bool = True) -> dict:
    return {
        "participants": people
        if people is not None
        else [
            {
                "name": "Андрей Викторович Левченков",
                "role": "speaker",
                "billing": "headliner",
                "presence": "in_person",
                "evidence_quote": (
                    "Главный спикер — Андрей Викторович Левченков. "
                    "Он выступит очно с лекцией об истории региона."
                ),
                "confidence": 0.98,
            }
        ],
        "roster_complete": complete,
    }


def test_checked_in_kgd80_catalog_contains_every_public_portrait() -> None:
    payload = load_kgd80_catalog()
    assets = {
        path.name
        for path in (ROOT / "site/public/assets/participants").glob("*.webp")
    }
    catalog_files = {
        filename
        for person in payload["people"]
        for filename in person.get("photo_files") or []
    }
    assert CATALOG_PATH.exists()
    assert len(payload["people"]) == 38
    assert len(assets) == 40
    assert catalog_files == assets
    assert {
        "Татьяна Удовенко",
        "Андрей Викторович Левченков",
        "Владимир Андреевич Чечко",
        "Шахноза Мухитдиновна Усманова",
    } <= {
        person["display_name"] for person in payload["people"]
    }


def test_every_catalog_person_is_seeded_as_a_global_like_subject() -> None:
    payload = load_kgd80_catalog()
    migration = (
        ROOT
        / "supabase/migrations/20260727191852_person_like_counter_v1.sql"
    ).read_text(encoding="utf-8")
    for person in payload["people"]:
        assert f'"{person["artist_id"]}"' in migration
    assert "personalization_person_like_state" in migration
    assert "get_person_like_snapshot_v1" in migration
    assert "set_person_like_v1" in migration
    assert "is_anonymous" in migration


def test_semantic_roster_requires_exact_grounded_quote_and_preserves_headliner() -> None:
    corpus = event().source_text
    decisions, complete = grounded_people_decisions(
        decision_payload(),
        source_corpus=corpus,
    )
    assert complete is True
    assert len(decisions) == 1
    assert decisions[0].public_role == "headliner"

    short_name = decision_payload()
    short_name["participants"][0]["name"] = "Андрей Левченков"
    assert len(
        grounded_people_decisions(short_name, source_corpus=corpus)[0]
    ) == 1

    invalid = decision_payload()
    invalid["participants"][0]["evidence_quote"] = "Андрей Левченков будет главным"
    assert grounded_people_decisions(invalid, source_corpus=corpus) == ([], True)


@pytest.mark.asyncio
async def test_smart_update_fact_pass_extracts_people_and_headliner_without_extra_call(
    monkeypatch,
) -> None:
    source = (
        "Главный спикер — Андрей Викторович Левченков. "
        "Он выступит очно с лекцией об истории региона."
    )

    async def fake_json(*_args, **_kwargs):
        return {
            "facts": ["Главный спикер — Андрей Викторович Левченков"],
            "event_people": decision_payload(),
        }

    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", False)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "_ask_gemma_json", fake_json)
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://example.org/events/levchenkov",
        source_text=source,
        title="Лекция Андрея Левченкова",
    )
    facts = await su._llm_extract_candidate_facts(candidate)
    assert facts == ["Главный спикер — Андрей Викторович Левченков"]
    assert candidate.people_roster_complete is True
    assert len(candidate.people_semantic_decisions) == 1
    assert candidate.people_semantic_decisions[0].public_role == "headliner"


@pytest.mark.asyncio
async def test_seed_and_smart_update_relation_are_idempotent(tmp_path) -> None:
    db = Database(str(tmp_path / "people.sqlite"))
    await db.init()
    try:
        first = await ensure_kgd80_registry(db)
        second = await ensure_kgd80_registry(db)
        assert first["catalog_people"] == second["catalog_people"] == 38
        assert first["created"] == 38
        assert second["created"] == 0

        async with db.get_session() as session:
            row = event()
            session.add(row)
            await session.commit()
            await session.refresh(row)
            event_id = int(row.id or 0)
        decisions, complete = grounded_people_decisions(
            decision_payload(),
            source_corpus=event().source_text,
        )
        created = await sync_event_people(
            db,
            event_id,
            decisions,
            roster_complete=complete,
            source_url="https://example.org/events/levchenkov",
        )
        repeated = await sync_event_people(
            db,
            event_id,
            decisions,
            roster_complete=complete,
            source_url="https://example.org/events/levchenkov",
        )
        assert created["confirmed"] == 1
        assert created["changed"] == 1
        assert repeated["changed"] == 0

        async with db.get_session() as session:
            rows = list(
                (
                    await session.execute(
                        select(EventArtistAppearance).where(
                            EventArtistAppearance.event_id == event_id
                        )
                    )
                ).scalars()
            )
        assert len(rows) == 1
        assert rows[0].role == "headliner"
        assert rows[0].status == "confirmed"
        assert rows[0].eligibility_status == "eligible"
        assert rows[0].media_identity_status == "verified"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exact_cpu_recall_is_candidate_only_and_complete_roster_can_cancel(tmp_path) -> None:
    db = Database(str(tmp_path / "people-cancel.sqlite"))
    await db.init()
    try:
        await ensure_kgd80_registry(db)
        async with db.get_session() as session:
            people = list((await session.execute(select(ArtistRegistryEntity))).scalars())
            candidates = registry_candidates_in_text(
                people,
                "Новая встреча: выступит Татьяна Удовенко.",
            )
            assert [person.display_name for person in candidates] == ["Татьяна Удовенко"]
            # Candidate recall alone has not created a public relation.
            assert list(
                (await session.execute(select(EventArtistAppearance))).scalars()
            ) == []
            row = event()
            session.add(row)
            await session.commit()
            await session.refresh(row)
            event_id = int(row.id or 0)

        decisions, complete = grounded_people_decisions(
            decision_payload(),
            source_corpus=event().source_text,
        )
        await sync_event_people(
            db,
            event_id,
            decisions,
            roster_complete=complete,
            source_url="https://example.org/events/levchenkov",
        )
        cancelled = await sync_event_people(
            db,
            event_id,
            [],
            roster_complete=True,
            source_url="https://example.org/events/levchenkov",
        )
        assert cancelled["cancelled"] == 1
        async with db.get_session() as session:
            row = (
                await session.execute(
                    select(EventArtistAppearance).where(
                        EventArtistAppearance.event_id == event_id
                    )
                )
            ).scalar_one()
        assert row.status == "cancelled"
        assert row.eligibility_status == "ineligible"
    finally:
        await db.close()
