from datetime import date, timedelta
from unittest.mock import AsyncMock

import pytest

import smart_event_update as su
from db import Database
from models import Event, EventSource, EventSourceFact


@pytest.mark.parametrize("low,high", [(1500, 1500), (1000, 1500), (1500, None), (None, 1500)])
def test_canonical_price_receipt_is_not_a_narrative_fact(low, high):
    price = f"Цена: {su._format_ticket_price(low, high)}"
    programme = "Ведущие покажут участникам, как самостоятельно делать массаж стоп."
    assert su._facts_text_clean_from_facts([price, programme]) == [programme]


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_network")
@pytest.mark.parametrize("case", ["price_ledger", "price_output", "programme_update", "old_price", "valid_merge_draft"])
async def test_price_update_does_not_replace_the_programme(tmp_path, monkeypatch, case):
    title = "Здоровье начинается со стоп"
    original = "На практикуме два ведущих сравнивают подходы Индии и Китая к здоровью стоп."
    if case == "old_price":
        original = "Цена: 1000 ₽"
    programme = "После лекции участники осмотрят музей с экскурсоводом."
    price = "Цена: 1500 ₽"
    day = (date.today() + timedelta(days=14)).isoformat()
    previous_url = "https://vk.com/wall-1_30"
    candidate = su.EventCandidate(
        title=title, date=day, time="12:00", location_name="Музей", city="Калининград",
        event_type="лекция", source_type="telegram", source_url="https://t.me/testmuseum/5761",
        source_text=f"{title}. {programme} Стоимость участия 1500 рублей.",
        ticket_price_min=1500, ticket_price_max=1500, trust_level="high",
    )
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", True)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 1.0, "fixture")))
    monkeypatch.setattr(su, "_ensure_legacy_description_fact", AsyncMock(return_value=[]))
    draft = original + " " + programme if case in {"valid_merge_draft", "programme_update"} else price
    monkeypatch.setattr(su, "_llm_merge_event", AsyncMock(return_value={
        "title": title, "description": draft, "ticket_price_min": 1500, "ticket_price_max": 1500,
        "added_facts": [programme] if case == "programme_update" else [],
    }))
    accepted = case in {"programme_update", "valid_merge_draft", "old_price"}
    expected = draft if accepted else original
    monkeypatch.setattr(su, "_ask_gemma_json", AsyncMock(return_value={
        "missing": [] if accepted else [original], "extra": [],
    }))
    writer = AsyncMock(return_value=programme if case == "programme_update" else price)
    monkeypatch.setattr(su, "_llm_fact_first_description_md_bounded", writer)
    for name in ("_llm_build_short_description", "_llm_build_search_digest", "_classify_topics"):
        monkeypatch.setattr(su, name, AsyncMock(return_value=None))

    db = Database(str(tmp_path / "events.sqlite"))
    try:
        await db.init()
        async with db.get_session() as session:
            event = Event(title=title, date=day, time="12:00", city=candidate.city,
                          location_name=candidate.location_name, event_type="лекция",
                          description=original, short_description=original, source_text=original,
                          ticket_price_min=1000, ticket_price_max=1000)
            session.add(event)
            await session.flush()
            source = EventSource(event_id=event.id, source_type="vk", source_url=previous_url, source_text=original)
            session.add(source)
            await session.flush()
            for fact in ["Цена: 1000 ₽"] + ([] if case == "price_ledger" else [original]):
                session.add(EventSourceFact(event_id=event.id, source_id=source.id, fact=fact, status="added"))
            await session.commit()
            eid = event.id
        monkeypatch.setattr(su, "_llm_match_event", AsyncMock(return_value=(eid, 1.0, "fixture")))
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.merged and result.event_id == eid
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            assert saved.ticket_price_min == saved.ticket_price_max == 1500
            assert saved.description == expected
            if case != "old_price":
                assert saved.short_description != price
        writer.assert_not_awaited()  # The merge draft is reviewed, not overwritten by a price-only writer.
    finally:
        await db.close()
