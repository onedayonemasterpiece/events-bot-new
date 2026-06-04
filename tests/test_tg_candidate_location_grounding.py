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
async def test_tg_build_candidate_drops_reaction_text_location_without_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    message = {
        "source_username": "molod_kld",
        "message_id": 3709,
        "source_link": "https://t.me/molod_kld/3709",
        "text": "🏠 Дайджест, мы его очень ждали",
        "posters": [
            {
                "sha256": "poster-digest",
                "ocr_text": "1-7 июня",
                "ocr_title": "Дайджест мероприятий",
            }
        ],
    }
    event_data = {
        "title": "Дайджест",
        "date": "2026-06-07",
        "time": "",
        "location_name": "мы его очень ждали",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name is None
    assert cand.location_address is None
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_does_not_replace_unsupported_offsite_location_with_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Калининград Сити Джаз Клуб, Грекова 3, Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "regional_events",
        "message_id": 4520,
        "source_link": "https://t.me/regional_events/4520",
        "text": "11 мая спортивные игры пройдут на площадке Зеленоградский городской стадион.",
    }
    event_data = {
        "title": "Спортивные игры",
        "date": "2026-05-11",
        "time": "12:00",
        "location_name": "Зеленоградский городской стадион",
        "city": "Зеленоградск",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name != "Калининград Сити Джаз Клуб"
    assert cand.city == "Зеленоградск"


@pytest.mark.asyncio
async def test_tg_build_candidate_future_quality_recovers_pure_from_text():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "meowafisha",
        "message_id": 7223,
        "source_link": "https://t.me/meowafisha/7223",
        "text": (
            "ELECTRODVOR празднуют свою первую дату. "
            "2 мая в Pure, Каштановая аллея 1а."
        ),
    }
    event_data = {
        "title": "ТУСОВЩИКИ",
        "date": "2026-05-02",
        "time": "",
        "location_name": (
            "ELECTRODVOR празднуют свою первую дату. Вспомнят и другие проекты - на Ялтинской"
        ),
        "location_address": "Мусорского в Бастионе и остальные рейвы за 5 лет.",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Pure"
    assert cand.location_address == "Каштановая аллея 1а"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_future_quality_recovers_1255_from_text():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "meowafisha",
        "message_id": 7224,
        "source_link": "https://t.me/meowafisha/7224",
        "text": (
            "Вечер настольных игр. "
            "Творческое пространство 12|55, Чкалова 1а, 4 этаж."
        ),
    }
    event_data = {
        "title": "Вечер настольных игр в творческом пространстве 12|55",
        "date": "2026-05-07",
        "time": "18:30",
        "location_name": (
            "ламповая атмосфера, приятная компания, чай-кофе-вкусняшки. "
            "Играют в «Бункер», «Уно», «Мафию»."
        ),
        "location_address": "Чкалова 1а 4 этаж",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Творческое пространство 12|55"
    assert cand.location_address == "Чкалова 1а, 4 этаж"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_future_quality_recovers_zoo_schedule_location():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "kldzoo",
        "message_id": 7189,
        "source_link": "https://t.me/kldzoo/7189",
        "text": (
            "В Калининградском зоопарке продолжаются музыкальные вечера у фонтана. "
            "Концерты проходят каждую субботу в 17:00 на сцене у фонтана."
        ),
    }
    event_data = {
        "title": "Группа «Париж»",
        "date": "2026-05-02",
        "time": "17:00",
        "location_name": "концерты проходят каждую субботу в 17.00 на сцене у фонтана,",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Калининградский зоопарк"
    assert cand.location_address == "пр-т Мира 26"
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


# --- INC-2026-05-16 regressions ----------------------------------------------


@pytest.mark.asyncio
async def test_tg_build_candidate_prose_inference_fallback_does_not_recreate_prose():
    """terkatalk/4818 replay: prose-drop fallback must not re-pick a prose
    sentence from `_infer_location_from_text` as the venue (INC-2026-05-16)."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="medium")
    message = {
        "source_username": "terkatalk",
        "message_id": 4818,
        "source_link": "https://t.me/terkatalk/4818",
        "text": (
            "16 мая | 18:00–21:00\n\n"
            "Это не практика.\n"
            "Это точка, после которой ты уже не тот же.\n\n"
            "Ты можешь не верить в звук."
        ),
    }
    event_data = {
        "title": "Шаманское путешествие",
        "date": "2026-05-16",
        "time": "18:00",
        "location_name": "после которой ты уже не тот же.",
        "city": "Калининград",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name is None
    assert cand.location_address is None


@pytest.mark.asyncio
async def test_tg_build_candidate_terkatalk_default_location_recovers_venue():
    """Once terkatalk has the Тёрка default_location seeded, prose-drop
    fallback should land on Пространство Тёрка (INC-2026-05-16)."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Пространство Тёрка, Пл. Победы 4 (1 под. 2 этаж), Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "terkatalk",
        "message_id": 4859,
        "source_link": "https://t.me/terkatalk/4859",
        "text": (
            "Приглашаю на терапевтическую встречу «Лабиринты Историй».\n\n"
            "Игровое поле, кубик и 1200 случайно-неслучайных вопросов.\n\n"
            "3300 рублей\n"
            "4 часа"
        ),
    }
    event_data = {
        "title": "Лабиринты Историй",
        "date": "2026-05-16",
        "time": "16:00",
        "location_name": "кубик и 1200 случайно-неслучайных вопросов.",
        "city": "Калининград",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name == "Пространство Тёрка"
    assert cand.location_address == "Пл. Победы 4 (1 под. 2 этаж)"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_rejects_ungrounded_known_venue_from_extraction():
    """festdir/4357 replay: extraction picks Калининград Сити Джаз Клуб from the
    reference list although the source post does not mention any venue. The
    grounding gate must drop the unsupported known venue (INC-2026-05-16,
    recurrence of INC-2026-04-29 bar-bastion-city-jazz)."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    message = {
        "source_username": "festdir",
        "message_id": 4357,
        "source_link": "https://t.me/festdir/4357",
        "text": (
            "Внимание, Калининград! Кино снимается на ваших глазах\n\n"
            "21 мая в городе пройдут съёмки масштабного исторического проекта.\n"
            "Команда ищет соавторов — тех, кто войдёт в кадр.\n"
            "Мы приглашаем актёров массовых сцен.\n"
            "Кроме того, потребуются костюмы 1940-х годов.\n"
            "🎬 Кинокомиссия Калининградской области"
        ),
    }
    event_data = {
        "title": "Съёмки исторического кинопроекта",
        "date": "2026-05-21",
        "time": "",
        "location_name": "Калининград Сити Джаз Клуб",
        "location_address": "Мира 33-35",
        "city": "Калининград",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name is None
    assert cand.location_address is None


# --- INC-2026-05-17 future quality prevention --------------------------------


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_person_name_location_and_uses_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Barn, ул. Литовский Вал 38, Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "barn_kaliningrad",
        "message_id": 1002,
        "source_link": "https://t.me/barn_kaliningrad/1002",
        "text": (
            "Кураторский тур по выставке «Общая кухня».\n"
            "17 мая 15:30-16:30. Куратор: ТАТЬЯНА БОРИСОВА."
        ),
    }
    event_data = {
        "title": "Кураторский тур по выставке «Общая кухня»",
        "date": "2026-05-17",
        "time": "15:30-16:30",
        "location_name": "ТАТЬЯНА БОРИСОВА",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Barn"
    assert cand.location_address == "Литовский Вал 38"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_date_marker_extracted_as_time():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Драматический театр, Мира 4, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "dramteatr39",
        "message_id": 4193,
        "source_link": "https://t.me/dramteatr39/4193",
        "text": "17.05 | GROZA\nНеделя в театре\nБилеты: https://dramteatr39.ru/spektakli/Groza",
    }
    event_data = {
        "title": "GROZA",
        "date": "2026-05-17",
        "time": "17:05",
        "location_name": "Драматический театр",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.time == ""
    assert cand.time_is_default is False
    assert cand.location_name == "Драматический театр"


def test_tg_zero_event_diagnostic_detects_kraftmarket235_shape():
    from source_parsing.telegram.handlers import _message_has_event_like_zero_extraction_signals

    message = {
        "text": (
            "Спектакль «8 женщин»\n"
            "22 мая в 19:00\n"
            "Городской центр культуры и искусства, Курортный проспект 11, Зеленоградск\n"
            "Билеты: https://voroh.ru/event/1022458/"
        )
    }

    assert _message_has_event_like_zero_extraction_signals(message) is True


def test_tg_zero_event_diagnostic_does_not_flag_plain_news():
    from source_parsing.telegram.handlers import _message_has_event_like_zero_extraction_signals

    message = {
        "text": (
            "Подводим итоги недели и делимся фотографиями с прошедшей встречи. "
            "Спасибо всем, кто был с нами."
        )
    }

    assert _message_has_event_like_zero_extraction_signals(message) is False
