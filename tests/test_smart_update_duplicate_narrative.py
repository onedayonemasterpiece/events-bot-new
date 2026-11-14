"""Duplicate semantic evidence must not regenerate an existing public narrative."""
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import main
import smart_event_update as su
import vk_event_relations
from db import Database
from models import Event, EventPoster, EventSource, EventSourceFact


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["duplicate", "added_fact", "ticket", "empty", "age_provenance", "age_value", "cleanup", "duplicate_gallery", "restored_gallery", "expanded_title", "generic_title", "time", "date", "added_fact_title"])
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
    canonical_photos = [f"https://static.kenigevents.ru/p/source-{i}.webp" for i in range(10)] if change in {"duplicate_gallery", "restored_gallery"} else []
    try:
        async with db.get_session() as session:
            event = Event(title="мастер-класс — Музей" if change == "generic_title" else "Прибрежный покой", description=original, short_description=original,
                          search_digest=original, date=date, time="" if change == "time" else "14:00", location_name="Музей",
                          city="Калининград", event_type="мастер-класс", source_text=fact,
                          source_texts=[fact], ticket_price_min=800, ticket_price_max=800,
                          age_restriction="12+", age_restriction_status="declared",
                          source_vk_post_url="https://vk.com/wall-1_10")
            event.photo_urls = canonical_photos[:8] if change == "restored_gallery" else canonical_photos
            event.photo_count = len(event.photo_urls)
            event.vk_source_hash = main.content_hash("\n".join([
                main.VK_SOURCE_POST_FORMAT_VERSION, event.title, date, "14:00", "Музей", "",
                "Калининград", "", "ics:<none>", json.dumps(canonical_photos, ensure_ascii=False), original,
            ]))
            applied_hash = event.vk_source_hash  # Existing successful public snapshot retains all ten sources.
            session.add(event)
            await session.flush()
            for i, url in enumerate(canonical_photos):
                session.add(EventPoster(event_id=event.id, poster_hash=f"source-{i}", supabase_url=url,
                                        review_status="approved", display_order=i, width=1600, height=900))
            source = EventSource(event_id=event.id, source_type="parser:fixture" if change in {"time", "date"} else "telegram",
                                 source_url="https://t.me/museum/8362", source_text=fact,
                                 canonical_source_url="https://t.me/museum/8362",
                                 candidate_key="known-child" if change in {"generic_title", "time", "date"} else None,
                                 source_role="identity_bearing",
                                 occurrence_key="child:0" if change in {"generic_title", "time", "date"} else None)
            session.add(source)
            await session.flush()
            session.add(EventSourceFact(event_id=event.id, source_id=source.id, fact=fact, status="added"))
            await session.commit()
            eid = event.id
        if change == "cleanup":
            monkeypatch.setattr(su, "_description_has_foreign_schedule_noise", lambda *_a, **_kw: True)
        added = ["После занятия можно забрать картину домой."] if change in {"added_fact", "added_fact_title"} else []
        monkeypatch.setattr(su, "_llm_merge_event", AsyncMock(return_value={
            "title": "Мастер-класс «Прибрежный покой»" if change in {"expanded_title", "added_fact_title"} else "Прибрежный покой", "description": "Альтернативная редакция прежнего текста.",
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
        candidate = su.EventCandidate(source_type="parser:fixture" if change in {"time", "date"} else "telegram", source_url="https://t.me/museum/8362" if change in {"generic_title", "time", "date"} else "https://t.me/museum/8363",
            source_text=("Прибрежный покой. " if change == "generic_title" else "") + fact + f" Стоимость участия {900 if change == 'ticket' else 800} рублей. " + " ".join(added),
            title="Мастер-класс «Прибрежный покой»" if change in {"expanded_title", "added_fact_title"} else "Прибрежный покой", date=(main.datetime.fromisoformat(date) + main.timedelta(days=1)).date().isoformat() if change == "date" else date, time="15:00" if change == "time" else "14:00", location_name="Музей",
            city="Калининград", event_type="мастер-класс", trust_level="high",
            ticket_price_min=900 if change == "ticket" else 800,
            ticket_price_max=900 if change == "ticket" else 800)
        if change in {"generic_title", "time", "date"}:
            candidate.candidate_key = "known-child"
            candidate.occurrence_key = "child:0"
        assert candidate.posters == []
        real_schedule = main.schedule_event_update_tasks
        schedule = AsyncMock()
        monkeypatch.setattr(main, "schedule_event_update_tasks", schedule)
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=True)
        assert result.event_id == eid
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            sources = (await session.execute(select(EventSource).where(EventSource.event_id == eid))).scalars().all()
        assert any(source.source_url == candidate.source_url for source in sources)
        if canonical_photos:
            assert result.added_facts == [] and result.added_posters == 0
            assert saved.photo_urls == canonical_photos
            assert saved.photo_count == 10
            assert saved.vk_source_hash == applied_hash
        sync = AsyncMock(return_value="https://vk.com/wall-1_10")
        monkeypatch.setattr(main, "sync_vk_source_post", sync)
        monkeypatch.setattr(main, "_prepare_same_day_linked_publish_event", AsyncMock(side_effect=lambda _db, ev: (ev, [ev])))
        monkeypatch.setattr(main, "_filter_vk_event_gallery_against_ledger", AsyncMock())
        monkeypatch.setattr(main, "_recover_managed_vk_live_url", AsyncMock(return_value=None))
        monkeypatch.setattr(main, "_managed_vk_post_state", AsyncMock(return_value=(True, True)))
        monkeypatch.setattr(vk_event_relations, "presentation", AsyncMock(return_value={}))
        monkeypatch.setattr(vk_event_relations, "ensure_comment", AsyncMock())
        if change in {"duplicate", "age_provenance", "duplicate_gallery", "restored_gallery", "expanded_title"}:
            assert (saved.description, saved.short_description, saved.search_digest) == (original,) * 3
            assert saved.title == "Прибрежный покой"
            assert main.content_hash("\n".join([
                main.VK_SOURCE_POST_FORMAT_VERSION, saved.title, saved.date, saved.time, saved.location_name, "",
                saved.city, "", "ics:<none>", json.dumps(saved.photo_urls or [], ensure_ascii=False), saved.description,
            ])) == applied_hash
            assert schedule.await_args.kwargs["refresh_existing_vk"] is (change == "restored_gallery")
            if change == "expanded_title":
                enqueue = AsyncMock(return_value="queued")
                monkeypatch.setattr(main, "enqueue_job", enqueue)
                monkeypatch.setattr(main, "_event_has_existing_managed_vk_post", AsyncMock(return_value=True))
                await real_schedule(db, saved, **schedule.await_args.kwargs)
                assert all(call.args[2] != main.JobTask.vk_sync for call in enqueue.await_args_list)
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
            if change == "time":
                assert saved.time == "15:00"
            if change == "generic_title":
                assert saved.title == "Прибрежный покой"
            if change == "added_fact_title":
                assert saved.title == "Мастер-класс «Прибрежный покой»"
            if change == "date":
                assert saved.date != date
        await main.job_sync_vk_source_post(eid, db, None)
        if change in {"duplicate", "age_provenance", "duplicate_gallery", "restored_gallery", "expanded_title"}:
            sync.assert_not_awaited()  # The actual consumer never reaches uploads or wall.edit.
        else:
            sync.assert_awaited_once()
    finally:
        await db.close()
