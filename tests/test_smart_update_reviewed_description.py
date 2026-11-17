from __future__ import annotations

import pytest
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import Event, EventSourceFact


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rejected", "confidence", "quote", "source_type", "keep"),
    [
        (["facts"], 0.97, "Ансамбль «Акцент»", "vk", True),
        (["facts", "search_digest"], 0.97, "Ансамбль «Акцент»", "telegram", True),
        (["facts", "description"], 0.97, "Ансамбль «Акцент»", "vk", False),
        (["facts"], 0.6, "Ансамбль «Акцент»", "vk", False),
        (["facts"], 0.97, "Несуществующая цитата", "vk", False),
        (["facts"], 0.97, "...", "vk", False),
        ([], 0.97, "Ансамбль «Акцент»", "vk", False),
        ([], 0.97, "Ансамбль «Акцент»", "parser", False),
    ],
)
async def test_split_create_uses_field_review_for_description(
    tmp_path, monkeypatch, rejected, confidence, quote, source_type, keep
):
    # Original Latin Jazz import retained the public description after field review,
    # then lost it because a different field (facts) had been stripped.
    seed = "Концерт «Латино джаз»."
    body = "Ансамбль «Акцент» сыграет босса-нову, сальсу и мамбо."
    bad_fact = "Специальный гость — саксофонист из Гаваны."
    candidate = su.EventCandidate(
        source_type=source_type,
        source_url={"vk": "https://vk.com/wall-29891284_15004",
                    "telegram": "https://t.me/example/17", "parser": "https://example.org/17"}[source_type],
        source_text=f"{seed}\n28 ноября 2026 года в 16:00. Историко-художественный музей. {body}",
        raw_excerpt=seed,
        title="Латино джаз", date="2026-11-28", time="16:00",
        location_name="Историко-художественный музей", city="Калининград", event_type="концерт",
    )

    async def generated(*args, **kwargs):
        return {"_split_create": True, "description": body,
                "facts": [] if source_type == "parser" else [bad_fact]}

    async def provider(prompt, schema, *, max_tokens, label):
        if label == "create_bundle_grounding":
            return {"decision": "ungrounded", "confidence": confidence,
                    "unsupported_fields": rejected, "evidence_quotes": [quote]}
        return None

    async def no_topics(*args, **kwargs):
        return None

    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE", False)
    monkeypatch.setattr(su, "_llm_g4_split_create_bundle", generated)
    monkeypatch.setattr(su, "_ask_gemma_json", provider)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    try:
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.status == "created", result
        async with db.get_session() as session:
            event = await session.get(Event, result.event_id)
            assert event.description == (body if keep else seed)
            logged = (await session.execute(select(EventSourceFact.fact).where(
                EventSourceFact.event_id == result.event_id
            ))).scalars().all()
            assert bad_fact not in logged
    finally:
        await db.close()

