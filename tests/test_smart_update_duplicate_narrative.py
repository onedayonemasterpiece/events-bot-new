"""Duplicate semantic evidence must not regenerate an existing public narrative."""
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import main
import smart_event_update as su
import vk_event_relations
from db import Database
from models import Event, EventSource, EventSourceFact


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["duplicate", "added_fact", "ticket", "empty", "age_provenance", "age_value", "cleanup"])
async def test_duplicate_source_preserves_narrative_and_managed_vk(tmp_path, monkeypatch, change):
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", True)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 1.0, "fixture")))
    monkeypatch.setattr(su, "_classify_topics", AsyncMock(return_value=None))
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    original = "Участники рисуют морской пейзаж акрилом на грунтованном картоне и знакомятся с техникой живописи."
    if change == "empty":
        original = ""
    fact = "Участники рисуют морской пейзаж акрилом на грунтованном картоне."
    date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()
    try:
        async with db.get_session() as session:
            event = Event(title="Прибрежный покой", description=original, short_description=original,
                          search_digest=original, date=date, time="14:00", location_name="Музей",
                          city="Калининград", event_type="мастер-класс", source_text=fact,
                          source_texts=[fact], ticket_price_min=800, ticket_price_max=800,
                          age_restriction="12+", age_restriction_status="declared",
                          source_vk_post_url="https://vk.com/wall-1_10")
            event.vk_source_hash = main.content_hash("\n".join([
                main.VK_SOURCE_POST_FORMAT_VERSION, event.title, date, "14:00", "Музей", "",
                "Калининград", "", "ics:<none>", json.dumps([], ensure_ascii=False), original,
            ]))
            session.add(event)
            await session.flush()
            source = EventSource(event_id=event.id, source_type="telegram",
                                 source_url="https://t.me/museum/8362", source_text=fact)
            session.add(source)
            await session.flush()
            session.add(EventSourceFact(event_id=event.id, source_id=source.id, fact=fact, status="added"))
            await session.commit()
            eid = event.id
        if change == "cleanup":
            monkeypatch.setattr(su, "_description_has_foreign_schedule_noise", lambda *_a, **_kw: True)
        added = ["После занятия можно забрать картину домой."] if change == "added_fact" else []
        monkeypatch.setattr(su, "_llm_merge_event", AsyncMock(return_value={
            "title": "Прибрежный покой", "description": "Альтернативная редакция прежнего текста.",
            "added_facts": added, "duplicate_facts": [fact], "conflict_facts": [],
            "skipped_conflicts": [], "ticket_price_min": 900 if change == "ticket" else 800,
            "ticket_price_max": 900 if change == "ticket" else 800,
        }))
        narrative = AsyncMock(return_value="Новая редакция: участники рисуют картину акрилом на картоне.")
        monkeypatch.setattr(su, "_llm_fact_first_description_md_bounded", narrative)
        monkeypatch.setattr(su, "_llm_build_short_description", AsyncMock(return_value="Иной краткий текст о рисовании картины акрилом на картоне для гостей музея."))
        monkeypatch.setattr(su, "_llm_build_search_digest", AsyncMock(return_value="Иной поисковый текст."))
        if change.startswith("age_"):
            from event_age_rating import declared_structured_decision
            decision = declared_structured_decision("16+" if change == "age_value" else "12+",
                source_url="https://t.me/museum/8363", source_type="telegram", input_hash="new-proof")
            monkeypatch.setattr(su, "_candidate_age_decision", lambda *_a, **_kw: decision)
        candidate = su.EventCandidate(source_type="telegram", source_url="https://t.me/museum/8363",
            source_text=fact + f" Стоимость участия {900 if change == 'ticket' else 800} рублей. " + " ".join(added),
            title="Прибрежный покой", date=date, time="14:00", location_name="Музей",
            city="Калининград", event_type="мастер-класс", trust_level="high",
            ticket_price_min=900 if change == "ticket" else 800,
            ticket_price_max=900 if change == "ticket" else 800)
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.event_id == eid
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            sources = (await session.execute(select(EventSource).where(EventSource.event_id == eid))).scalars().all()
        assert any(source.source_url == candidate.source_url for source in sources)
        sync = AsyncMock(return_value="https://vk.com/wall-1_10")
        monkeypatch.setattr(main, "sync_vk_source_post", sync)
        monkeypatch.setattr(main, "_prepare_same_day_linked_publish_event", AsyncMock(side_effect=lambda _db, ev: (ev, [ev])))
        monkeypatch.setattr(main, "_filter_vk_event_gallery_against_ledger", AsyncMock())
        monkeypatch.setattr(main, "_recover_managed_vk_live_url", AsyncMock(return_value=None))
        monkeypatch.setattr(main, "_managed_vk_post_state", AsyncMock(return_value=(True, True)))
        monkeypatch.setattr(vk_event_relations, "presentation", AsyncMock(return_value={}))
        monkeypatch.setattr(vk_event_relations, "ensure_comment", AsyncMock())
        if change in {"duplicate", "age_provenance"}:
            assert (saved.description, saved.short_description, saved.search_digest) == (original,) * 3
            narrative.assert_not_awaited()
            if change == "age_provenance":
                assert saved.age_restriction_source_url == candidate.source_url
                assert saved.age_restriction == "12+"
        else:
            assert saved.description != original
            if change == "age_value":
                assert saved.age_restriction != "12+"
            if change == "ticket":
                assert saved.ticket_price_min == 900
        await main.job_sync_vk_source_post(eid, db, None)
        if change in {"duplicate", "age_provenance"}:
            sync.assert_not_awaited()  # The actual consumer never reaches uploads or wall.edit.
        else:
            sync.assert_awaited_once()
    finally:
        await db.close()
