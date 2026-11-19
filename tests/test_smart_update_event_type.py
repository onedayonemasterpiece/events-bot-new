"""Source-cited type choice uses the existing semantic merge and transaction."""
import json
import logging
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import main
import smart_event_update as su
from db import Database
from models import Event, EventSource, EventSourceFact


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_network")
@pytest.mark.parametrize("decision", ["cited", "absent", "null", "forged", "theatre", "source_tail", "ocr", "ocr_tail"])
async def test_source_cited_type_survives_normal_merge(tmp_path, monkeypatch, caplog, decision):
    theatre = decision == "theatre"
    old_type = "кинопоказ" if theatre else "лекция"
    chosen_type = "спектакль" if theatre else "экскурсия"
    title = "Любовь и голуби" if theatre else "Экологическая экспедиция"
    source = (
        "В спектакле заняты три поколения звёзд российского театра и кино. Актеры играют и поют."
        if theatre else "Экологическая экспедиция в дендропарк. Участники берут пробы воды и почвы."
    )
    rewritten = (
        "Театральная постановка с актёрами разных поколений и музыкальными номерами."
        if theatre else "Участники экологической экспедиции берут пробы воды и почвы в дендропарке."
    )
    old_fact = f"Тип: {old_type}"
    old_narrative_fact = "Кинопоказ истории о любви и семье." if theatre else "Лекция об экологических исследованиях воды и почвы."
    original = f"Встреча «{title}». Ранее указанный формат: {old_type}."
    monkeypatch.setenv("DB_INIT_SKIP_VK_SOURCES_SEED", "1")
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", True)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 1.0, "fixture")))
    monkeypatch.setattr(su, "_classify_topics", AsyncMock())
    monkeypatch.setattr(su, "_ensure_legacy_description_fact", AsyncMock(return_value=[]))
    monkeypatch.setattr(su, "_llm_extract_candidate_facts", AsyncMock(return_value=[]))
    monkeypatch.setattr(su, "_llm_build_short_description", AsyncMock(return_value=rewritten))
    monkeypatch.setattr(su, "_llm_build_search_digest", AsyncMock(return_value=rewritten))
    import interest_clubs
    monkeypatch.setattr(interest_clubs, "schedule_interest_club_evaluation", AsyncMock())
    narrative = AsyncMock(return_value=rewritten)
    monkeypatch.setattr(su, "_llm_fact_first_description_md_bounded", narrative)
    schedule = AsyncMock()
    monkeypatch.setattr(main, "schedule_event_update_tasks", schedule)
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()
    date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()
    try:
        async with db.get_session() as session:
            event = Event(title=title, description=original, short_description=original,
                          search_digest=original, date=date, time="14:00", city="Калининград",
                          location_name="Музей", event_type=old_type, source_text=original,
                          source_texts=[original], source_vk_post_url="https://vk.com/wall-1_10")
            session.add(event)
            await session.flush()
            eid = event.id
            old_source = EventSource(event_id=eid, source_type="telegram",
                                    source_url="https://t.me/type_fixture/1", source_text=original)
            session.add(old_source)
            await session.flush()
            old_source_id = old_source.id
            session.add(EventSourceFact(event_id=eid, source_id=old_source_id,
                                        fact=old_fact, status="added"))
            session.add(EventSourceFact(event_id=eid, source_id=old_source_id,
                                        fact=old_narrative_fact, status="added"))
            await session.commit()
        monkeypatch.setattr(su, "_llm_match_event", AsyncMock(return_value=(eid, 1.0, "fixture")))
        merge_response = {"title": title, "description": rewritten, "added_facts": [],
                          "duplicate_facts": [], "conflict_facts": [], "skipped_conflicts": []}
        if decision != "absent":
            merge_response["event_type_decision"] = None if decision == "null" else {
                "value": chosen_type,
                "evidence_quote": "Неприсутствующая в источнике цитата" if decision == "forged" else source,
            }
        posters = []
        if decision in {"source_tail", "ocr", "ocr_tail"}:
            quote = "Экспедиция с исследованием почвы и отбором проб."
            merge_response["event_type_decision"]["evidence_quote"] = quote
            if decision == "source_tail":
                monkeypatch.setattr(su, "SMART_UPDATE_MERGE_CANDIDATE_TEXT_MAX_CHARS", 100)
                source += " " * 200 + quote
            else:
                posters = [su.PosterCandidate(ocr_text="") for _ in range(3)]
                posters.append(su.PosterCandidate(ocr_text=("x" * 401 if decision == "ocr_tail" else "") + quote))
        merge_calls = []
        coverage_calls = []

        async def ask(prompt, schema, **kwargs):
            if kwargs.get("label", "").endswith(":fact_first_cov"):
                coverage_calls.append(json.loads(prompt.split("Данные:\n", 1)[1]))
                return {"missing": [], "extra": []}
            assert kwargs.get("label") == "merge", "Unexpected unstubbed semantic provider stage"
            merge_calls.append((prompt, schema))
            return merge_response

        monkeypatch.setattr(su, "_ask_gemma_json", ask)
        candidate = su.EventCandidate(source_type="telegram", source_url="https://t.me/type_fixture/2",
                                      source_text=source, title=title, date=date, time="14:00",
                                      location_name="Музей", city="Калининград", trust_level="high",
                                      event_type=chosen_type, posters=posters)
        caplog.set_level(logging.INFO, logger=su.logger.name)
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.event_id == eid and result.merged
        assert len(merge_calls) == 1  # Type decision piggybacks on the real _llm_merge_event.
        prompt, schema = merge_calls[0]
        accepted = decision in {"cited", "theatre", "ocr"}
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            facts = (await session.execute(select(EventSourceFact, EventSource.source_url).join(
                EventSource, EventSourceFact.source_id == EventSource.id
            ).where(EventSourceFact.event_id == eid))).all()
        assert saved.event_type == (chosen_type if accepted else old_type)
        if accepted:
            assert "event_type_decision" in schema["properties"]
            assert old_type in prompt and chosen_type in prompt
        assert any(fact.source_id == old_source_id and fact.fact == old_fact for fact, _ in facts)
        assert any(fact.source_id == old_source_id and fact.fact == old_narrative_fact for fact, _ in facts)
        assert any(fact.fact == f"Тип: {chosen_type}" and fact.status == "added"
                   and url == candidate.source_url for fact, url in facts) is accepted
        merge_logs = [r for r in caplog.records if str(r.msg).startswith("smart_update.merge event_id=")]
        assert len(merge_logs) == 1
        assert ("event_type" in merge_logs[0].args[5]) is accepted
        if accepted:
            narrative.assert_not_awaited()
            assert len(coverage_calls) == 1
            assert old_narrative_fact in coverage_calls[0]["facts_text_clean"]
            assert coverage_calls[0]["merge_context"]["previous_description"] == original
            # Captures the real conflict seam: corrected type + historical narrative fact.
            # Stubbed verdict proves persistence of the merge draft, not semantic precedence.
            assert saved.description == rewritten
        else:
            narrative.assert_not_awaited()
        schedule.assert_not_awaited()  # No publication path is invoked by this canonical proof.
    finally:
        await db.close()
