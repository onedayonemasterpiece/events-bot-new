from __future__ import annotations

import pytest

import geo_region
import smart_event_update as su
from db import Database
from models import Event, EventSource
from smart_event_update import EventCandidate


def test_grounded_raw_excerpt_closes_digest_occurrence_scope() -> None:
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kulturnaya_chaika/8201",
        source_text=(
            "Обзор на уикенд.\n"
            "MAGGOTS FEST 2026 — Ape On The Rocket и Conquester\n"
            "Суббота, #15_августа\n"
            "Правая Набережная, 25\n"
            "Другой концерт 16 августа в Светлогорске."
        ),
        raw_excerpt=(
            "MAGGOTS FEST 2026 — Ape On The Rocket и Conquester. "
            "Суббота, 15 августа, Правая Набережная, 25."
        ),
        title="MAGGOTS FEST 2026 — Ape On The Rocket и Conquester",
        date="2026-08-15",
        location_name="Правая Набережная, 25",
        city="Калининград",
    )

    assert su._apply_grounded_occurrence_scope_fallback(candidate) is True
    assert "15 августа" in str(candidate.occurrence_scope_text)
    assert "Правая Набережная" in str(candidate.occurrence_scope_text)
    assert "Другой концерт" not in str(candidate.occurrence_scope_text)


@pytest.mark.asyncio
async def test_complete_vk_digest_child_uses_unique_verbatim_source_anchor_on_llm_abstention(
    monkeypatch,
) -> None:
    source_text = (
        "Планы на выходные — семь мероприятий.\n"
        "14 августа (пятница):\n"
        "20:00 опен-эйр вечеринка «Руки Вверх! И друзья», живой вокал и DJs.\n"
        "21:00 Хиты рока от NEW VERSION в баре Бастион.\n"
        "15 августа (суббота):\n"
        "12:00 Фестиваль народного единства.\n"
        "Бастион, ул. Судостроительная 6/1 (Культурный квартал «Понарт»)"
    )
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-149955604_24253",
        source_text=source_text,
        raw_excerpt="Трибьют-вечеринка с хитами 90-х и живым вокалом.",
        source_disposition="EVENTS_FOUND",
        source_evidence_complete=True,
        producer_ordinal=0,
        title="Вечеринка «Руки Вверх! И друзья»",
        date="2026-08-14",
        time="20:00",
        location_name="Понарт",
        location_address="Судостроительная 6",
        city="Калининград",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "uncertain",
            "confidence": 0.61,
            "selected_excerpts": [],
            "reason_short": "abstain",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_scope_candidate_occurrence(candidate) == (
        True,
        "grounded_source_anchor",
    )
    scoped = str(candidate.occurrence_scope_text)
    assert "14 августа" in scoped
    assert "20:00" in scoped
    assert "Судостроительная 6/1" in scoped
    assert "NEW VERSION" not in scoped
    assert "Фестиваль народного единства" not in scoped


def test_source_anchor_scope_fails_closed_for_two_matching_lines() -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text=(
            "14 августа:\n"
            "20:00 Концерт группы Север на первой сцене.\n"
            "20:00 Концерт группы Север на второй сцене.\n"
            "Адрес: Центральная 1."
        ),
        source_disposition="EVENTS_FOUND",
        source_evidence_complete=True,
        producer_ordinal=0,
        title="Концерт группы Север",
        date="2026-08-14",
        time="20:00",
        location_address="Центральная 1",
    )

    assert su._apply_source_anchored_occurrence_scope_fallback(candidate) is False
    assert candidate.occurrence_scope_text is None


def test_configured_telegram_location_repairs_bad_child_location() -> None:
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/zaryakinoteatr/964",
        source_text="Большое кино будет каждый день в большом зале «Зари».",
        raw_excerpt="17 августа 19:00 Волк с Уолл-стрит.",
        title="Волк с Уолл-стрит",
        date="2026-08-17",
        location_name="Интерстеллар",
        city="Калининград",
        metrics={
            "tg_default_location": "Заря, Мира 41-43, Калининград",
            "tg_extracted_location_name": "Заря",
            "tg_extracted_location_address": "Мира 41-43",
            "tg_extracted_city": "Калининград",
        },
    )

    assert su._restore_configured_telegram_location(candidate) is True
    assert candidate.location_name == "Заря"
    assert candidate.location_address == "Мира 41-43"
    assert candidate.city == "Калининград"


def test_source_grounded_allowlist_place_recovers_missing_city() -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-226785590_1140",
        source_text="16 августа в замке Бранденбург — День посёлка Ушаково.",
        raw_excerpt="День посёлка Ушаково, 16 августа.",
        title="День посёлка Ушаково",
        date="2026-08-16",
        location_name="Замок Бранденбург",
    )

    assert su._source_grounded_region_place_hint(candidate) == "Ушаково"
    assert geo_region.is_allowlisted_kaliningrad_place("Ушаково") is True


@pytest.mark.asyncio
async def test_invalid_bundle_review_drops_generated_prose_without_losing_event(
    monkeypatch,
) -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-226740803_6741",
        source_text="16 августа в 09:00 забег «Утренник на Балтике».",
        title="Утренник на Балтике",
        date="2026-08-16",
        time="09:00",
        location_name="Стадион «Балтика»",
        city="Калининград",
    )
    bundle = {
        "title": "Непроверенный заголовок",
        "description": "Непроверенное описание",
        "facts": ["Непроверенный факт"],
    }

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "grounded",
            "confidence": 0.99,
            "unsupported_fields": [],
            "evidence_quotes": ["цитата, которой нет в источнике"],
            "reason_short": "bad quote",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    ok, reason, fields = await su._llm_review_create_bundle_grounding(
        bundle, candidate
    )
    assert (ok, reason) == (False, "llm_evidence_not_verbatim")
    assert fields == ["title", "description", "facts"]
    assert su._remove_llm_rejected_bundle_fields(bundle, fields) == {}


@pytest.mark.asyncio
async def test_empty_source_bundle_review_keeps_typed_conservative_fallback() -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_1",
        source_text="",
        title="Анонс",
        date="2026-08-20",
        location_name="Онлайн",
    )

    assert await su._llm_review_create_bundle_grounding(
        {"description": "Непроверенное описание"}, candidate
    ) == (False, "empty_source", ["description"])


@pytest.mark.asyncio
async def test_anchor_review_repairs_invalid_quote_in_same_call(monkeypatch) -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-9118984_24806",
        source_text=(
            "16 августа в 13:00 — экскурсия по выставке «Секретный код "
            "Альбрехта Дюрера». Выставка работает до 30 сентября."
        ),
        title="Экскурсия по выставке «Секретный код Альбрехта Дюрера»",
        date="2026-08-16",
        time="13:00",
        location_name="Музей Изобразительных искусств",
        city="Калининград",
    )
    answers = iter(
        [
            {
                "decision": "keep",
                "confidence": 0.99,
                "date": "2026-08-16",
                "end_date": None,
                "time": "13:00",
                "evidence_quotes": ["пересказ вместо цитаты"],
                "reason_short": "keep",
            },
            {
                "decision": "keep",
                "confidence": 0.99,
                "date": "2026-08-16",
                "end_date": None,
                "time": "13:00",
                "evidence_quotes": ["16 августа в 13:00"],
                "reason_short": "keep",
            },
        ]
    )

    async def fake_ask(*_args, **_kwargs):
        return next(answers)

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_review_candidate_anchor_roles(
        candidate, trigger_reason="explicit_range"
    ) == (True, "llm_keep")


@pytest.mark.asyncio
async def test_parser_exact_source_url_matches_existing_recurring_range(tmp_path) -> None:
    db = Database(str(tmp_path / "parser-range.sqlite"))
    await db.init()
    try:
        url = "https://kaliningrad.qtickets.events/251797-svetlogorsk-i-yantarnyy"
        async with db.get_session() as session:
            event = Event(
                title="Экскурсия «Светлогорск и Янтарный»",
                description="Экскурсия по побережью.",
                source_text="Расписание экскурсий.",
                date="2026-08-12",
                end_date="2026-10-25",
                time="09:15",
                location_name="Центральная площадь",
                location_address="Шевченко 11",
                city="Калининград",
            )
            session.add(event)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(event.id),
                    source_type="parser:qtickets",
                    source_url=url,
                    canonical_source_url=url,
                    source_role="identity_bearing",
                )
            )
            await session.commit()
            event_id = int(event.id)

        candidate = EventCandidate(
            source_type="parser:qtickets",
            source_url=url,
            source_text="17 августа — экскурсия Светлогорск и Янтарный.",
            raw_excerpt="Однодневная экскурсия по побережью.",
            title="Экскурсия «Светлогорск и Янтарный»",
            date="2026-08-17",
            time="09:15",
            location_name="Центральная площадь",
            location_address="Шевченко 11",
            city="Калининград",
        )

        matched = await su._match_existing_event_by_event_source_url(db, candidate)

        assert matched is not None
        assert int(matched.id) == event_id
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_parser_same_source_different_session_is_not_collapsed(tmp_path) -> None:
    db = Database(str(tmp_path / "parser-session.sqlite"))
    await db.init()
    try:
        url = "https://muzteatr39.ru/action/show"
        async with db.get_session() as session:
            event = Event(
                title="Спектакль",
                description="Вечерний сеанс.",
                source_text="Сеанс 17:00.",
                date="2026-10-25",
                time="17:00",
                location_name="Музыкальный театр",
                city="Калининград",
            )
            session.add(event)
            await session.flush()
            session.add(
                EventSource(
                    event_id=int(event.id),
                    source_type="parser:muzteatr",
                    source_url=url,
                    canonical_source_url=url,
                    source_role="identity_bearing",
                )
            )
            await session.commit()

        candidate = EventCandidate(
            source_type="parser:muzteatr",
            source_url=url,
            source_text="Отдельный дневной сеанс в 14:00.",
            title="Спектакль",
            date="2026-10-25",
            time="14:00",
            location_name="Музыкальный театр",
            city="Калининград",
        )

        assert await su._match_existing_event_by_event_source_url(db, candidate) is None
    finally:
        await db.close()
