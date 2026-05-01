from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_tg_build_candidate_replaces_unsupported_extracted_location_from_poster_ocr():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)

    message = {
        "source_username": "signalkld",
        "message_id": 10431,
        "source_link": "https://t.me/signalkld/10431",
        "text": (
            "ЭкоКёниг приглашает на Весенний Экодвор.\n"
            "Вход в Железнодорожные ворота — со стороны Парка Победы."
        ),
        "events": [
            {
                "title": "Весенний Экодвор",
                "date": "2026-04-19",
                "time": "12:00-15:00",
                "location_name": "Фридландские ворота",
            }
        ],
        "posters": [
            {
                "sha256": "poster1",
                "ocr_text": (
                    "ВЕСЕННИЙ ЭКОДВОР\n"
                    "19 АПРЕЛЯ, 12:00-15:00\n"
                    "КАЛИНИНГРАД, ЖЕЛЕЗНОДОРОЖНЫЕ ВОРОТА"
                ),
            }
        ],
    }

    cand = _build_candidate(src, message, message["events"][0])

    assert (cand.location_name or "").casefold() != "фридландские ворота"
    assert "железнодорож" in (cand.location_name or "").casefold()


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_prose_location_and_uses_address_reference():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "terkatalk",
        "message_id": 4672,
        "source_link": "https://t.me/terkatalk/4672",
        "text": (
            "26.04 | в 17:00 \"Виниссимо\" на ул. Яналова, 2. "
            "\"Под солнцем Италии\". Арт-Дегустация."
        ),
    }
    event_data = {
        "title": "Виниссимо: Под солнцем Италии",
        "date": "2026-04-26",
        "time": "19:00",
        "location_name": (
            '17/04 в 19:00 "Виниссимо" на Яналова, 2 Битва Чемпионов - '
            "открываем вина с рейтингом и наградами."
        ),
        "location_address": "Яналова 2",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Виниссимо"
    assert cand.location_address == "Яналова 2"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_prose_location_and_finds_known_venue_in_text():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "minkultturism_39",
        "message_id": 4650,
        "source_link": "https://t.me/minkultturism_39/4650",
        "text": (
            "В Третьяковской галерее – программа «Музыкальные сказки русских композиторов». "
            "26 апреля в 14:00 пройдет лекция «Учителя и ученики»."
        ),
    }
    event_data = {
        "title": "Учителя и ученики",
        "date": "2026-04-26",
        "time": "14:00",
        "location_name": (
            "известный пианист, телерадиоведущий и Юлия Куликова, "
            "пианистка, композитор, представят новую дуэтную программу."
        ),
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Филиал Третьяковской галереи"
    assert cand.location_address == "Парадная наб. 3"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_section_label_location_and_uses_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Филиал Третьяковской галереи, Парадная наб. 3, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "tretyakovka_kaliningrad",
        "message_id": 2839,
        "source_link": "https://t.me/tretyakovka_kaliningrad/2839",
        "text": (
            "Дайджест событий в музее 28 апреля – 3 мая:\n"
            "📍Кинозал:\n"
            "📍Мастерские:\n"
            "1 мая в 14:00 – столярный мастер-класс «Солнечный круг»."
        ),
    }
    event_data = {
        "title": "Столярный мастер-класс «Солнечный круг»",
        "date": "2026-05-01",
        "time": "14:00",
        "location_name": "Кинозал:",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Филиал Третьяковской галереи"
    assert cand.location_address == "Парадная наб. 3"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_marks_unsupported_time_as_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Драматический театр, Мира 4, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "dramteatr39",
        "message_id": 4126,
        "source_link": "https://t.me/dramteatr39/4126",
        "text": "01.05 | Женитьба",
    }
    event_data = {
        "title": "Женитьба",
        "date": "2026-05-01",
        "time": "18:00",
        "location_name": "Драматический театр",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.time == "18:00"
    assert cand.time_is_default is True


@pytest.mark.asyncio
async def test_tg_build_candidate_normalizes_camember_reference_location():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="medium")
    message = {
        "source_username": "kulturnaya_chaika",
        "message_id": 7615,
        "source_link": "https://t.me/kulturnaya_chaika/7615",
        "text": '📍 сырный магазин "Камамбер", в Зеленоградске. Ул. Потемкина, 20Б',
    }
    event_data = {
        "title": "Винные дегустации с сомелье Ольгой Скобовой",
        "date": "2026-05-01",
        "time": "19:00",
        "location_name": 'сырный магазин "Камамбер"',
        "location_address": "Потемкина 20Б",
        "city": "Зеленоградск",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Сырный магазин Камамбер"
    assert cand.location_address == "Потемкина 20Б"
    assert cand.city == "Зеленоградск"
