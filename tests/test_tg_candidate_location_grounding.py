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

    assert cand.location_name == "Виниссимо, Яналова 2, Калининград"
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

    assert cand.location_name == "Филиал Третьяковской галереи, Парадная наб. 3, Калининград"
    assert cand.location_address == "Парадная наб. 3"
    assert cand.city == "Калининград"
