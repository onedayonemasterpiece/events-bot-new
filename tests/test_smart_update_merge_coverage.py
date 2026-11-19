"""Persistence controls; model verdicts are fixtures, not a semantic quality score."""
import json
from datetime import date, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import Event, EventSource, EventSourceFact


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_network")
@pytest.mark.parametrize("case", ["complete", "missing", "extra", "none", "malformed", "exception", "replace", "cancel", "correct"])
async def test_merge_coverage_preserves_accepted_programme_and_allows_corrections(tmp_path, monkeypatch, case):
    # Miniature of 10973: the accepted programme is richer than the repeated source/ledger.
    title = "ЭкоГЭ"
    original = "Участники посетят оранжерею, возьмут пробы воды и почвы, исследуют образцы в лаборатории."
    added = "Марафон охватывает путь от оранжереи до научной лаборатории."
    draft = original + " " + added
    source_text = added
    conflicts = []
    if case in {"missing", "extra", "none", "malformed", "exception"}:
        draft = added
    if case == "extra":
        draft += " Каждый получит сертификат."
    if case in {"replace", "cancel", "correct"}:
        added, draft = {
            "replace": ("Вместо отбора проб воды и почвы пройдёт мастер-класс по переработке бумаги.",
                        "Участники посетят оранжерею и мастер-класс по переработке бумаги, исследуют образцы в лаборатории."),
            "cancel": ("Посещение оранжереи отменено; отбор проб воды и почвы и лабораторная работа состоятся.",
                       "Участники возьмут пробы воды и почвы и исследуют образцы в лаборатории. Посещение оранжереи отменено."),
            "correct": ("Исправление: участники не берут пробы воды и почвы, а исследуют готовые образцы в лаборатории. Посещение оранжереи состоится.",
                        "Участники посетят оранжерею и исследуют готовые образцы в лаборатории."),
        }[case]
        source_text = added
        conflicts = [f"Прежняя программа -> {added} (выбран: candidate)"]
    accepted = case in {"complete", "replace", "cancel", "correct"}
    response = {"missing": [], "extra": []}
    if case == "missing":
        response["missing"] = ["возьмут пробы воды и почвы"]
    elif case == "extra":
        response["extra"] = ["Каждый получит сертификат"]
    elif case == "none":
        response = None
    elif case == "malformed":
        response = {"missing": None, "extra": []}
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", True)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 1.0, "fixture")))
    monkeypatch.setattr(su, "_ensure_legacy_description_fact", AsyncMock(return_value=[]))
    monkeypatch.setattr(su, "_llm_extract_candidate_facts", AsyncMock(return_value=[]))
    monkeypatch.setattr(su, "_llm_merge_event", AsyncMock(return_value={
        "title": title, "description": draft, "search_digest": "Непроверенный новый дайджест.",
        "added_facts": [{"fact": added, "evidence_quote": added}], "conflict_facts": conflicts,
        "ticket_price_min": 1500, "ticket_price_max": 1500,
    }))
    calls = []

    async def review(prompt, schema, **kwargs):
        assert kwargs["label"].endswith(":fact_first_cov")
        calls.append(json.loads(prompt.split("Данные:\n", 1)[1]))
        if case == "exception":
            raise RuntimeError("coverage unavailable")
        return response

    monkeypatch.setattr(su, "_ask_gemma_json", review)
    second_writer = AsyncMock(return_value=added)
    late_rewrite = AsyncMock(return_value="Поздняя перезапись не должна обойти coverage.")
    monkeypatch.setattr(su, "_llm_fact_first_description_md_bounded", second_writer)
    monkeypatch.setattr(su, "_rewrite_description_full_from_sources", late_rewrite)
    monkeypatch.setattr(su, "_min_expected_description_len_from_sources", lambda *_a: 5000)
    for name in ("_llm_build_short_description", "_llm_build_search_digest", "_classify_topics"):
        monkeypatch.setattr(su, name, AsyncMock(return_value=None))
    day = (date.today() + timedelta(days=14)).isoformat()
    candidate = su.EventCandidate(title=title, date=day, time="12:00", event_type="лекция",
        location_name="ИЦАЭ", city="Калининград", source_type="telegram", trust_level="high",
        source_url="https://t.me/test_eco/5773", source_text=source_text + " Стоимость участия 1500 рублей.",
        search_digest="Непроверенный дайджест кандидата.", raw_excerpt="Непроверенный краткий текст кандидата.",
        ticket_price_min=1500, ticket_price_max=1500)
    db = Database(str(tmp_path / "events.sqlite"))
    try:
        await db.init()
        async with db.get_session() as session:
            event = Event(title=title, date=day, time="12:00", event_type="лекция", city=candidate.city,
                location_name=candidate.location_name, description=original, short_description="Прежний короткий текст.",
                search_digest="Прежний поисковый текст.", source_text=original, ticket_price_min=1000, ticket_price_max=1000)
            session.add(event)
            await session.flush()
            source = EventSource(event_id=event.id, source_type="telegram", source_url="https://t.me/test_eco/509", source_text=original)
            session.add(source)
            await session.flush()
            # The missing programme is deliberately absent from this incomplete ledger.
            session.add(EventSourceFact(event_id=event.id, source_id=source.id, fact="Тип: лекция", status="added"))
            await session.commit()
            eid = event.id
        monkeypatch.setattr(su, "_llm_match_event", AsyncMock(return_value=(eid, 1.0, "fixture")))
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.merged and result.event_id == eid
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            facts = (await session.execute(select(EventSourceFact).where(EventSourceFact.event_id == eid))).scalars().all()
            assert saved.description == (draft if accepted else original)
            assert saved.ticket_price_min == saved.ticket_price_max == 1500
            assert any(f.fact == added and f.status == "added" for f in facts)
            if not accepted:
                assert saved.short_description == "Прежний короткий текст."
                assert saved.search_digest == "Прежний поисковый текст."
        assert len(calls) == 1
        assert calls[0]["merge_context"]["previous_description"] == original
        assert calls[0]["merge_context"]["candidate_text"] == candidate.source_text
        assert calls[0]["description_md"] == draft
        assert original not in calls[0]["facts_text_clean"]
        second_writer.assert_not_awaited()
        late_rewrite.assert_not_awaited()
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_network")
@pytest.mark.parametrize("coverage", [None, {}, {"missing": []}, {"missing": None, "extra": []},
    {"missing": "", "extra": []}, {"missing": [], "extra": [None]}, {"missing": [], "extra": [""]},
    {"missing": [], "extra": [], "accepted": True}, {"missing": [], "extra": []}])
async def test_fact_first_invalid_coverage_cannot_approve_description(monkeypatch, coverage):
    text = "Участники собирают деревянную игрушку и забирают её домой."
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    writer = AsyncMock(return_value=text)
    monkeypatch.setattr(su, "_ask_gemma_text", writer)
    monkeypatch.setattr(su, "_ask_gemma_json", AsyncMock(return_value=coverage))
    result = await su._llm_fact_first_description_md(title="Игрушка", event_type="мастер-класс",
        facts_text_clean=[text], anchors=[], label="create")
    assert result == (text if coverage == {"missing": [], "extra": []} else None)
    writer.assert_awaited_once()  # A broken review cannot silently trigger a revision then PASS.


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_network")
@pytest.mark.parametrize("case", ["repeat_pass", "repeat_none", "repeat_extra", "legacy_exception", "legacy_none", "legacy_missing", "dedupe", "accepted_writer_failure"])
async def test_merge_review_recovery_cleanup_and_derived_fields(tmp_path, monkeypatch, case):
    original = "Участники посетят оранжерею, возьмут пробы воды и почвы, исследуют образцы в лаборатории."
    cancellation = "Посещение оранжереи отменено; отбор проб воды и почвы и лабораторная работа состоятся."
    draft = "Участники возьмут пробы воды и почвы и исследуют образцы в лаборатории. Посещение оранжереи отменено."
    short = "Участники возьмут пробы воды и почвы и исследуют образцы в лаборатории; оранжерея отменена."
    digest = "Отбор проб и лабораторная работа. Посещение оранжереи отменено."
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST", True)
    monkeypatch.setattr(su, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, "_llm_review_candidate_eventness", AsyncMock(return_value=("event", 1.0, "fixture")))
    for name in ("_ensure_legacy_description_fact", "_llm_extract_candidate_facts"):
        monkeypatch.setattr(su, name, AsyncMock(return_value=[]))
    monkeypatch.setattr(su, "_ask_gemma_text", AsyncMock(return_value=None))
    monkeypatch.setattr(su, "_classify_topics", AsyncMock(return_value=None))
    short_writer = AsyncMock(return_value=None if case == "accepted_writer_failure" else short)
    digest_writer = AsyncMock(return_value=None if case == "accepted_writer_failure" else digest)
    monkeypatch.setattr(su, "_llm_build_short_description", short_writer)
    monkeypatch.setattr(su, "_llm_build_search_digest", digest_writer)
    late_writer = AsyncMock(return_value="Непроверенная поздняя перезапись.")
    monkeypatch.setattr(su, "_rewrite_description_full_from_sources", late_writer)
    monkeypatch.setattr(su, "_min_expected_description_len_from_sources", lambda *_a: 5000)
    second_writer = AsyncMock(return_value="Второй писатель не нужен.")
    monkeypatch.setattr(su, "_llm_fact_first_description_md_bounded", second_writer)
    merge_calls = []

    async def merge(*args, **kwargs):
        merge_calls.append(kwargs)
        fact = {"fact": cancellation, "evidence_quote": cancellation}
        return {"title": "ЭкоГЭ", "description": draft + ("\n\n" + draft if case == "dedupe" else ""),
            "added_facts": [fact] if len(merge_calls) == 1 else [],
            "duplicate_facts": [] if len(merge_calls) == 1 else [fact],
            "conflict_facts": ["Прежняя программа -> отмена (выбран: candidate)"] if len(merge_calls) == 1 else []}

    monkeypatch.setattr(su, "_llm_merge_event", merge)
    reviews = []

    async def review(prompt, schema, **kwargs):
        assert kwargs["label"].endswith(":fact_first_cov")
        reviews.append(json.loads(prompt.split("Данные:\n", 1)[1]))
        if (case.startswith("repeat_") and len(reviews) == 1) or case == "legacy_exception":
            raise RuntimeError("temporary coverage failure")
        if case in {"legacy_none", "repeat_none"}:
            return None
        if case == "legacy_missing":
            return {"missing": ["лабораторная работа"], "extra": []}
        if case == "repeat_extra":
            return {"missing": [], "extra": ["неподтверждённая деталь"]}
        return {"missing": [], "extra": []}

    monkeypatch.setattr(su, "_ask_gemma_json", review)
    day = (date.today() + timedelta(days=14)).isoformat()
    packet = dict(title="ЭкоГЭ", date=day, time="12:00", event_type="лекция", location_name="ИЦАЭ",
        city="Калининград", source_type="telegram", trust_level="high", source_text=cancellation,
        raw_excerpt="Все участники получат сертификат победителя и бесплатные сувениры после завершения большой образовательной программы.",
        search_digest="Всем участникам выдадут бесплатные сертификаты и сувениры.")
    db = Database(str(tmp_path / "events.sqlite"))
    try:
        await db.init()
        async with db.get_session() as session:
            event = Event(title="ЭкоГЭ", date=day, time="12:00", event_type="лекция", city="Калининград", location_name="ИЦАЭ",
                description=original + ("\n\n> Текст до Smart Update: служебная вставка legacy snapshot" if case.startswith("legacy_") else ""),
                short_description="Прежний короткий текст.", search_digest="Прежний поисковый текст.", source_text=original)
            session.add(event)
            await session.flush()
            source = EventSource(event_id=event.id, source_type="telegram", source_url="https://t.me/review_eco/1",
                source_text=original, trust_level="high")
            session.add(source)
            await session.flush()
            session.add(EventSourceFact(event_id=event.id, source_id=source.id, fact="Тип: лекция", status="added"))
            await session.commit()
            eid = event.id
        monkeypatch.setattr(su, "_llm_match_event", AsyncMock(return_value=(eid, 1.0, "fixture")))
        first = await su.smart_event_update(db, su.EventCandidate(**packet, source_url="https://t.me/review_eco/2"),
            check_source_url=False, schedule_tasks=False)
        assert first.merged and first.event_id == eid
        if case.startswith("repeat_"):
            async with db.get_session() as session:
                saved = await session.get(Event, eid)
                assert saved.description == original
                facts = (await session.execute(select(EventSourceFact).where(EventSourceFact.event_id == eid))).scalars().all()
                assert any(f.fact == cancellation and f.status == "added" for f in facts)
            short_writer.assert_not_awaited()
            digest_writer.assert_not_awaited()
            # An ordinary follow-up source repeats the accepted correction. No exact-packet/noop bypass.
            second = await su.smart_event_update(db, su.EventCandidate(**packet, source_url="https://t.me/review_eco/3"),
                check_source_url=False, schedule_tasks=False)
            assert second.event_id == eid
        accepted = case in {"repeat_pass", "dedupe", "accepted_writer_failure"}
        async with db.get_session() as session:
            saved = await session.get(Event, eid)
            assert saved.description == (draft if accepted else original)
            if case == "accepted_writer_failure":
                for summary in (saved.short_description, saved.search_digest):
                    assert "пробы" in summary and "лаборатории" in summary
                    assert "сертификат" not in summary and "сувениры" not in summary
            else:
                assert saved.short_description == (short if accepted else "Прежний короткий текст.")
                assert saved.search_digest == (digest if accepted else "Прежний поисковый текст.")
        assert len(reviews) == (2 if case.startswith("repeat_") else 1)
        assert all(r["description_md"] == draft for r in reviews)  # The reviewed bytes are the saved bytes, deduped first.
        assert all(r["merge_context"]["previous_description"] == original for r in reviews)
        assert short_writer.await_count == digest_writer.await_count == int(accepted)
        second_writer.assert_not_awaited()
        late_writer.assert_not_awaited()
    finally:
        await db.close()
