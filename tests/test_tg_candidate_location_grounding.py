from types import SimpleNamespace

import pytest


def test_tg_build_candidate_ocr_only_phone_contact_beats_group_author_fallback():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    ocr_text = (
        "МАСТЕР КЛАСС\n"
        "ПО КАЛЛИГРАФИИ\n"
        "проводит Ламейко Светлана\n"
        "Дата: 1 июля\n"
        "Время: 19:00\n"
        "Место: музей «Восток на Западе», ул. Клиническая, 19А\n"
        "Стоимость: 1000 рублей\n"
        "Запись по телефону:\n"
        "+ 7 (931) 616 08 88"
    )
    message = {
        "source_username": "kraftmarket39",
        "source_type": "supergroup",
        "message_id": 317,
        "source_link": "https://t.me/kraftmarket39/317",
        "message_date": "2026-06-29T21:39:03+00:00",
        "text": "",
        "post_author": {
            "user_id": 1799336248,
            "username": "tasha9917",
            "is_user": True,
        },
        "posters": [{"sha256": "p1", "ocr_text": ocr_text, "ocr_title": "МАСТЕР КЛАСС ПО КАЛЛИГРАФИИ"}],
    }
    event = {
        "title": "Мастер-класс по каллиграфии",
        "date": "2026-07-01",
        "time": "19:00",
        "location_name": "музей «Восток на Западе»",
        "location_address": "ул. Клиническая, 19А",
        "city": "Калининград",
        "ticket_link": "",
        "ticket_price_min": 1000,
        "ticket_price_max": 1000,
        "event_type": "мастер-класс",
    }

    cand = _build_candidate(src, message, event)

    assert cand.ticket_link == "tel:+79316160888"
    assert cand.source_text and "Запись по телефону" in cand.source_text
    assert cand.metrics["tg_ticket_link_from_post_author"] is False
    assert cand.tg_source_author == "tasha9917"


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


def test_tg_build_candidate_never_uses_a_sibling_title_as_venue() -> None:
    """Regression for the eight-film poster at zaryakinoteatr/964."""

    from source_parsing.telegram.handlers import (
        _build_candidate,
        _location_is_sibling_event_title,
    )

    src = SimpleNamespace(
        default_location="Заря, Мира 41-43, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "zaryakinoteatr",
        "message_id": 964,
        "source_link": "https://t.me/zaryakinoteatr/964",
        "text": (
            "В программе: «Интерстеллар», «Волк с Уолл-стрит», «1+1».\n"
            "16–23 августа · каждый вечер · большой зал «Зари»"
        ),
        "events": [
            {
                "title": "Интерстеллар",
                "date": "2026-08-16",
                "time": "19:00",
                "location_name": "Заря",
            },
            {
                "title": "1+1",
                "date": "2026-08-18",
                "time": "19:00",
                "location_name": "Заря",
                "raw_excerpt": "18 августа 19:00 1+1. Большое кино в большом зале Зари.",
            },
        ],
        "posters": [
            {
                "sha256": "poster1",
                "ocr_title": "БОЛЬШОЕ КИНО",
                "ocr_text": (
                    "ИНТЕРСТЕЛЛАР\n18 АВГУСТА 19:00\n1+1\n"
                    "БОЛЬШОЙ ЗАЛ ЗАРИ"
                ),
            }
        ],
    }

    assert _location_is_sibling_event_title("Интерстеллар", message) is True
    assert _location_is_sibling_event_title("Заря", message) is False

    candidate = _build_candidate(src, message, message["events"][1])

    assert candidate.location_name == "Заря"
    assert candidate.location_address == "Мира 41-43"


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
async def test_tg_build_candidate_drops_program_item_location_and_uses_default():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Филиал Третьяковской галереи, Парадная наб. 3, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "tretyakovka_kaliningrad",
        "message_id": 3201,
        "source_link": "https://t.me/tretyakovka_kaliningrad/3201",
        "text": (
            "🎹 Открываем летний фестиваль Pianissimo!\n\n"
            "19 июня в 20:00 в атриуме музея прозвучит первый концерт нового сезона Pianissimo.\n\n"
            "В программе вечера — шедевры фортепианной музыки:\n"
            "🎵 И. С. Бах / Ф. Бузони – Чакона\n"
            "🎵 С. В. Рахманинов – Музыкальные моменты, соч. 16"
        ),
    }
    event_data = {
        "title": "Первый концерт нового сезона Pianissimo",
        "date": "2026-06-19",
        "time": "20:00",
        "location_name": "🎵 С. В. Рахманинов – Музыкальные моменты",
        "location_address": "соч. 16",
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
async def test_tg_build_candidate_drops_temporal_location_without_default_repair():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Барн, Каштановая аллея 1а, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "barn_kaliningrad",
        "message_id": 1058,
        "source_link": "https://t.me/barn_kaliningrad/1058",
        "text": (
            "Завтра, 14 июня, в 12:00 в рамках ОП!ФЕСТА "
            "состоится экспериментальный пленэр."
        ),
    }
    event_data = {
        "title": "Экспериментальный пленэр",
        "date": "2026-06-14",
        "time": "12:00",
        "location_name": "🤗Завтра",
        "location_address": "Каштановая аллея 1а",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name is None
    assert cand.location_address is None
    assert cand.city == "Калининград"
    assert cand.metrics["tg_location_temporal_rejected"] is True


@pytest.mark.asyncio
async def test_tg_build_candidate_does_not_infer_city_thanks_as_location():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level=None)
    message = {
        "source_username": "garazhka_kld",
        "message_id": 1505,
        "source_link": "https://t.me/garazhka_kld/1505",
        "text": (
            "Калининград, спасибо!\n"
            "Было классно.\n\n"
            "Следующий фестиваль: 5-6 сентября.\n"
            "Локация уточняется!"
        ),
    }
    event_data = {
        "title": "Гаражка",
        "date": "2026-09-05",
        "end_date": "2026-09-06",
        "time": "",
        "location_name": "",
        "city": "Калининград",
        "source_text": message["text"],
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
async def test_tg_build_candidate_does_not_keep_default_when_post_has_offsite_address():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Пространство Тёрка, пл. Победы 4, Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "terkatalk",
        "message_id": 5031,
        "source_link": "https://t.me/terkatalk/5031",
        "text": (
            "Спектакль «Женщины Мира. Война».\n"
            "🗓 Дата: 26 ИЮНЯ\n"
            "🕖 Время: 19:00\n"
            "📍 Место: г.Калининград Ул Кирпичная 7 (Центр города)"
        ),
    }
    event_data = {
        "title": "Женщины Мира. Война",
        "date": "2026-06-26",
        "time": "19:00",
        # The extractor missed the offsite venue/address; source default must
        # not silently become the public venue for this event-local address.
        "location_name": "",
        "location_address": "",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert "тёрк" not in (cand.location_name or "").casefold()
    assert "кирпич" in ((cand.location_name or "") + " " + (cand.location_address or "")).casefold()


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
async def test_tg_build_candidate_keeps_short_theatre_titles_from_llm():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Драматический театр, Мира 4, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "dramteatr39",
        "message_id": 4375,
        "source_link": "https://t.me/dramteatr39/4375",
        "text": (
            "завтра в театре\n"
            "🖤07.06 | Идиот\n"
            "🖤Пермский академический Театр-Театр (Пермь)"
        ),
    }
    event_data = {
        "title": "Идиот",
        "date": "2026-06-07",
        "time": "",
        "location_name": "Драматический театр",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.title == "Идиот"


@pytest.mark.asyncio
async def test_tg_build_candidate_keeps_numeric_theatre_title_from_repertoire_post():
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Драматический театр, Мира 4, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "dramteatr39",
        "message_id": 4361,
        "source_link": "https://t.me/dramteatr39/4361",
        "text": (
            "Появился в продаже репертуар АВГУСТА!\n"
            "30.08 | № 13\n"
            "Будет много ваших любимых комедий."
        ),
    }
    event_data = {
        "title": "№ 13",
        "date": "2026-08-30",
        "time": "",
        "location_name": "Драматический театр",
        "city": "Калининград",
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.title == "№ 13"


def test_tg_poster_pairs_do_not_treat_followup_dates_as_times():
    from source_parsing.telegram.handlers import _extract_poster_date_time_pairs

    ocr_text = (
        "7 июня 2026 г.\n"
        "НАЧАЛО в 16.00\n"
        "КИНОСЕАНС НЕМОГО КИНО\n"
        "СЛЕДУЮЩИЕ СЕАНСЫ: 28.06, 26.07, 9.08, 30.08"
    )

    pairs = _extract_poster_date_time_pairs(ocr_text)

    assert (6, 7, "16:00") in pairs
    assert (6, 28, "09:08") not in pairs
    assert (7, 26, "09:08") not in pairs


def test_tg_poster_multiday_expand_ignores_date_list_without_times():
    from source_parsing.telegram.handlers import _expand_events_from_poster_datetime_pairs

    message = {
        "source_username": "grezahutor",
        "message_id": 2169,
        "message_date": "2026-06-05T12:00:00+00:00",
        "posters": [
            {
                "ocr_text": (
                    "7 июня 2026 г.\n"
                    "НАЧАЛО в 16.00\n"
                    "КИНОСЕАНС НЕМОГО КИНО\n"
                    "СЛЕДУЮЩИЕ СЕАНСЫ: 28.06, 26.07, 9.08, 30.08"
                )
            }
        ],
    }
    events = [
        {
            "title": "Киносеанс немого кино",
            "date": "2026-06-07",
            "time": "16:00",
        }
    ]

    expanded = _expand_events_from_poster_datetime_pairs(
        message,
        events,
        username="grezahutor",
        message_id=2169,
    )

    assert expanded == events


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
async def test_tg_build_candidate_default_location_loses_to_event_local_venue():
    """INC-2026-06-12: Terka reposts of Westside schedule must not keep the
    channel default when the event-local block names ОКЦ/Сигнал."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Пространство Тёрка, Пл. Победы 4 (1 под. 2 этаж), Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "terkatalk",
        "message_id": 4990,
        "source_link": "https://t.me/terkatalk/4990",
        "text": (
            "12.06/13.06 | Women Power в киноклубе westside movieclub\n"
            "12 июня в 20:30 — «Род мужской» (2026)\n"
            "📍Новый ОКЦ, ул. Горького, 116\n"
            "13 июня в 20:00 — «Солнцестояние» (2017)\n"
            "📍Сигнал, Леонова 22"
        ),
    }
    event_data = {
        "title": "Род мужской",
        "date": "2026-06-12",
        "time": "20:30",
        "location_name": "",
        "location_address": "",
        "city": "Калининград",
        "ticket_link": "https://okts-na-gorkogo.timepad.ru/event/4024691/",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name == "ОКЦ на Горького"
    assert cand.location_address == "Горького 116"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_rejects_unsupported_city_jazz_default():
    """INC-2026-06-12: risky City Jazz defaults must be source-grounded, not
    silently applied to unrelated posts."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Калининград Сити Джаз Клуб, Мира 33-35, Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "terkatalk",
        "message_id": 4735,
        "source_link": "https://t.me/terkatalk/4735",
        "text": (
            "Женская арт-терапевтическая группа.\n"
            "Старт: 23 апреля.\n"
            "Место: очно в Калининграде (уютный кабинет в центре города).\n"
            "Вопросы, запись @lena_zaka"
        ),
    }
    event_data = {
        "title": "Женская арт-терапевтическая группа",
        "date": "2026-04-23",
        "time": "",
        "location_name": "",
        "location_address": "",
        "city": "Калининград",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name is None
    assert cand.location_address is None
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_keeps_source_grounded_city_jazz_default():
    """Negative control: City Jazz is valid when source/poster grounds it."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Калининград Сити Джаз Клуб, Мира 33-35, Калининград",
        default_ticket_link=None,
        trust_level="medium",
    )
    message = {
        "source_username": "qtickets",
        "message_id": 240069,
        "source_link": "https://kaliningrad.qtickets.events/240069-mariya-makarova-akustika",
        "text": "Мария Макарова акустика. 7 августа 20:00. пр-т Мира, 33 Калининград.",
    }
    event_data = {
        "title": "Мария Макарова акустика",
        "date": "2026-08-07",
        "time": "20:00",
        "location_name": "",
        "location_address": "",
        "city": "Калининград",
    }
    cand = _build_candidate(src, message, event_data)
    assert cand.location_name == "Калининград Сити Джаз Клуб"
    assert cand.location_address == "Мира 33-35"
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


@pytest.mark.asyncio
async def test_tg_build_candidate_does_not_replace_known_venue_with_city_prose_fragment():
    """Regression for kldzoo/7534 and sobor39/6000: comma prose like
    'Калининград, с приглашения...' must not overrule the LLM/default venue."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Калининградский зоопарк, пр-т Мира 26, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message = {
        "source_username": "kldzoo",
        "message_id": 7534,
        "source_link": "https://t.me/kldzoo/7534",
        "text": (
            "Сегодня начнём, пожалуй, с приглашения на концерт🎻🙃\n"
            "🎷20 июня 17:00\n"
            "🎷Музыкальный вечер: выступает ансамбль «Янтарь»\n"
            "Место проведения — сцена у фонтана."
        ),
    }
    event_data = {
        "title": "Музыкальный вечер: выступает ансамбль «Янтарь»",
        "date": "2026-06-20",
        "time": "17:00",
        "location_name": "Калининградский зоопарк",
        "location_address": "Калининградский зоопарк",
        "city": "Калининград",
        "source_text": message["text"],
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Калининградский зоопарк"
    assert cand.location_address == "пр-т Мира 26"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_non_location_bullet_fragment_and_uses_default():
    """Regression for @kldevents/913 / event 6162: a paragraph/list bullet from
    the source text must not survive as `location_name`; source default recovery
    is allowed as grounding metadata, while the semantic repair is LLM-owned
    upstream."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(
        default_location="Музей Изобразительных искусств, Ленинский проспект 83, Калининград",
        default_ticket_link=None,
        trust_level="high",
    )
    message_text = (
        "🐵Обезьянка Кики – обитательница Кёнигсбергского зоосада в 1920-х годах.\n\n"
        "📩 Зоосад с первого года (напомним, Кёнигсбергский зоосад открылся 21 мая "
        "1896 года) издавал открытки с видами зданий, памятников и животных.\n\n"
        "📌Выставка работает до 28 июня."
    )
    message = {
        "source_username": "kaliningradartmuseum",
        "message_id": 8017,
        "source_link": "https://t.me/kaliningradartmuseum/8017",
        "text": message_text,
    }
    event_data = {
        "title": "Ревущий лев, поющий лось",
        "date": "2026-06-18",
        "end_date": "2026-06-28",
        "time": "",
        "location_name": "📩 Зоосад с первого года (напомним",
        "city": "Калининград",
        "source_text": message_text,
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "Музей Изобразительных искусств"
    assert cand.location_address == "Ленинский проспект 83"
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_drops_topic_sentence_split_as_location():
    """Regression for @kldevents/914 / event 6163: a list topic split between
    `location_name` and `location_address` is not a venue and must fail closed
    instead of reaching public TG/VK/Telegraph surfaces as prose."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    message_text = (
        "19 июня в 14:30 проведём прямой эфир с министром по культуре и туризму "
        "Калининградской области Андреем Ермаком.\n\n"
        "Андрей Викторович расскажет:\n\n"
        "- о концертах, организованных в честь 80-летия Калининградской области;\n"
        "- об итогах фестиваля классической музыки «Кантата».\n\n"
        "Вопросы можно задавать в комментариях к этому посту."
    )
    message = {
        "source_username": "minkultturism_39",
        "message_id": 4826,
        "source_link": "https://t.me/minkultturism_39/4826",
        "text": message_text,
        "posters": [
            {
                "ocr_text": (
                    "прямой эфир\nАндрей Ермак\n19 июня, 14:30\n"
                    "Официальная страница правительства Калининградской области Вконтакте и в Одноклассниках"
                )
            }
        ],
    }
    event_data = {
        "title": "Прямой эфир с министром по культуре и туризму Калининградской области Андреем Ермаком",
        "date": "2026-06-19",
        "time": "14:30",
        "location_name": "о концертах",
        "location_address": "организованных в честь 80-летия Калининградской области;",
        "city": "Калининград",
        "source_text": message_text,
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name is None
    assert cand.location_address is None
    assert cand.city == "Калининград"


@pytest.mark.asyncio
async def test_tg_build_candidate_recovers_studio_from_address_ocr_over_wrong_known_venue():
    """Regression for meowafisha/7683: if OCR/source has explicit
    'Советский проспект 12, 809 студия', a wrong known venue name must not
    survive with that conflicting address."""
    from source_parsing.telegram.handlers import _build_candidate

    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="medium")
    message = {
        "source_username": "meowafisha",
        "message_id": 7683,
        "source_link": "https://t.me/meowafisha/7683",
        "text": "21.06 | Старт программы «Модный интенсив»\n📍Советский проспект 12, 809 студия",
        "posters": [
            {
                "sha256": "poster-fashion",
                "ocr_text": "Советский проспект 12, 809 студия 15:00 21 июня Кастинг",
                "ocr_title": "Кастинг в модельное агентство",
            }
        ],
    }
    event_data = {
        "title": "Модный интенсив",
        "date": "2026-06-21",
        "time": "15:00",
        "location_name": "ИЦАЭ (в КГТУ)",
        "location_address": "Советский проспект 12",
        "city": "Калининград",
        "source_text": message["text"],
    }

    cand = _build_candidate(src, message, event_data)

    assert cand.location_name == "809 студия"
    assert cand.location_address == "Советский проспект 12"
    assert cand.city == "Калининград"


def test_short_program_and_reminder_fragments_are_not_event_local_locations() -> None:
    from source_parsing.telegram import handlers as h

    assert h._event_local_location_candidate_ok(
        'В программе — бессмертные «Ave Maria» Ф. Шуберта', None
    ) is False
    assert h._event_local_location_candidate_ok('И не забывайте', None) is False


def test_ecodvor_activity_tbd_start_does_not_inherit_parent_hours() -> None:
    from source_parsing.telegram.handlers import _build_candidate

    source_text = (
        'Приглашаем на мастер-класс "Джанкбук: блокнот из случайных сокровищ".\n'
        "Продолжительность: около часа.\n"
        "Время начала уточняется. Программа Экодвора пока формируется.\n"
        "Летний Экодвор пройдёт 8 августа с 14:00 до 17:00 в Железнодорожных воротах."
    )
    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    candidate = _build_candidate(
        src,
        {
            "source_username": "ecodvor39",
            "message_id": 931,
            "source_link": "https://t.me/ecodvor39/931",
            "text": source_text,
        },
        {
            "title": "Джанкбук: блокнот из случайных сокровищ",
            "date": "2026-08-08",
            "time": "14:00",
            "end_date": "2026-08-08",
            "location_name": "Железнодорожные ворота",
            "city": "Калининград",
            "event_type": "мастер-класс",
            "source_text": source_text,
        },
    )

    assert candidate.time == ""
    assert candidate.time_is_default is False
    assert candidate.metrics["tg_time_explicitly_unknown"] is True


def test_unknown_gathering_time_does_not_erase_explicit_activity_start() -> None:
    from source_parsing.telegram.handlers import _build_candidate

    source_text = (
        "Время сбора участников уточняется. "
        "Мастер-класс начнётся 8 августа в 15:00. "
        "Экодвор работает с 14:00 до 17:00."
    )
    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    candidate = _build_candidate(
        src,
        {
            "source_username": "example",
            "message_id": 1,
            "source_link": "https://t.me/example/1",
            "text": source_text,
        },
        {
            "title": "Мастер-класс",
            "date": "2026-08-08",
            "time": "15:00",
            "location_name": "Железнодорожные ворота",
            "city": "Калининград",
            "source_text": source_text,
        },
    )

    assert candidate.time == "15:00"
    assert candidate.metrics["tg_time_explicitly_unknown"] is False


def test_sibling_tbd_activity_does_not_erase_other_activity_start() -> None:
    from source_parsing.telegram.handlers import _build_candidate

    source_text = (
        "Лекция о фриганстве — время начала уточняется.\n"
        "Мастер-класс по коллажу начнётся 8 августа в 15:00.\n"
        "Экодвор работает с 14:00 до 17:00."
    )
    src = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level="high")
    events = [
        {"title": "Лекция о фриганстве", "date": "2026-08-08", "time": ""},
        {"title": "Мастер-класс по коллажу", "date": "2026-08-08", "time": "15:00"},
    ]
    candidate = _build_candidate(
        src,
        {
            "source_username": "example",
            "message_id": 2,
            "source_link": "https://t.me/example/2",
            "text": source_text,
            "events": events,
        },
        {
            **events[1],
            "location_name": "Железнодорожные ворота",
            "city": "Калининград",
            "source_text": source_text,
        },
    )

    assert candidate.time == "15:00"
    assert candidate.metrics["tg_time_explicitly_unknown"] is False
