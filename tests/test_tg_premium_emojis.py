from telethon.helpers import add_surrogate, del_surrogate
from telethon.tl.types import MessageEntityBold, MessageEntityTextUrl, MessageEntityCustomEmoji

from tg_premium_emojis import (
    DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS,
    DEFAULT_FREE_EMOJI_DOCUMENT_IDS,
    DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS,
    apply_daily_free_premium_emojis,
)


def test_daily_free_labels_are_replaced_with_custom_emoji_entities():
    text = (
        "АНОНС на 29 июня 2026 #ежедневныйанонс\n\n"
        "👉 🚩 🟡 Заголовок до блока не компактится\n"
        "🟡 Бесплатно по регистрации\n"
        "+2 ДОБАВИЛИ В АНОНС\n\n"
        "КАЛИНИНГРАД\n"
        "01.07 🚩 🟡 🧦 Лекция «Чулки и носки»\n"
        "02.07 🚩 🎭 Спектакль"
    )
    title_offset = add_surrogate(text).index("Лекция")
    entities = [
        MessageEntityTextUrl(offset=title_offset, length=len("Лекция"), url="https://example.org"),
        MessageEntityBold(offset=0, length=5),
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert count == 4
    assert "🟡 Бесплатно" not in new_text
    assert "ДОБАВИЛИ В АНОНС\n\nКАЛИНИНГРАД\n01.07 🚩 🟡" not in new_text
    assert "👉 🚩 🟡 Заголовок до блока не компактится" in new_text
    assert "🆓🆓🆓🆓 по регистрации" in new_text
    assert "01.07 🆓🆓🆓🆓 🧦 Лекция" in new_text

    custom = [e for e in new_entities if isinstance(e, MessageEntityCustomEmoji)]
    assert custom[0].document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["👉"]
    free_entity_groups = [custom[1:5], custom[5:9]]
    assert all([entity.document_id for entity in group] == list(DEFAULT_FREE_EMOJI_DOCUMENT_IDS) for group in free_entity_groups)
    assert custom[-1].document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🎭"]
    assert len(custom) == 10

    assert any(isinstance(e, MessageEntityBold) and e.offset == 0 and e.length == 5 for e in new_entities)

    shifted_link = next(e for e in new_entities if isinstance(e, MessageEntityTextUrl))
    sur_new_text = add_surrogate(new_text)
    assert del_surrogate(sur_new_text[shifted_link.offset : shifted_link.offset + shifted_link.length]) == "Лекция"


def test_daily_free_replacement_is_idempotent():
    text = "🆓🆓🆓🆓 по регистрации\n+1 ДОБАВИЛИ В АНОНС\n01.07 🆓🆓🆓🆓 Лекция"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == text
    assert new_entities == []
    assert count == 0


def test_existing_single_custom_emoji_is_not_replaced_again():
    text = "👉 Заголовок"
    entities = [
        MessageEntityCustomEmoji(
            offset=0,
            length=2,
            document_id=DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["👉"],
        )
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert new_text == text
    assert count == 0
    assert new_entities == entities


def test_wrong_single_custom_emoji_document_id_is_corrected():
    text = "🤘 Трибьют группы «АРИЯ»"
    entities = [
        MessageEntityCustomEmoji(
            offset=0,
            length=len(add_surrogate("🤘")),
            document_id=5393556708398225048,
        )
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert new_text == text
    assert count == 1
    custom = [entity for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert len(custom) == 1
    assert custom[0].document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🤘"]
    assert custom[0].document_id == 5404517529362128309


def test_daily_price_source_ticket_gets_money_custom_emoji_and_keeps_link():
    text = "[ignored header]\nБилеты в источнике 2200\n26 июня 19:00 Другая площадка"
    ticket_offset = add_surrogate(text).index("Билеты в источнике")
    entities = [
        MessageEntityTextUrl(
            offset=ticket_offset,
            length=len("Билеты в источнике"),
            url="https://vk.cc/cYLKmN",
        )
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert "Билеты в источнике 💰 2200" in new_text
    assert count == 1
    money = [
        entity
        for entity in new_entities
        if isinstance(entity, MessageEntityCustomEmoji)
        and entity.document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["💰"]
    ]
    assert len(money) == 1
    shifted_link = next(e for e in new_entities if isinstance(e, MessageEntityTextUrl))
    sur_new_text = add_surrogate(new_text)
    assert del_surrogate(sur_new_text[shifted_link.offset : shifted_link.offset + shifted_link.length]) == "Билеты в источнике"
    assert shifted_link.url == "https://vk.cc/cYLKmN"


def test_daily_known_venues_get_custom_icons_in_location_lines():
    text = (
        "Лекция про Научная библиотека как институцию\n"
        "26 июня 19:00 Научная библиотека, Мира 9, #Калининград\n"
        "27 июня 18:00 Замок Ноухайзен, Гурьевский район"
    )

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert "Лекция про Научная библиотека как институцию" in new_text
    assert "26 июня 19:00 📗 Научная библиотека, Мира 9" in new_text
    assert "27 июня 18:00 🏰 Замок Ноухайзен" in new_text
    assert count == 2
    custom_by_id = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["📗"] in custom_by_id
    assert DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🏰"] in custom_by_id


def test_daily_inserted_money_and_venue_icons_are_idempotent_after_custom_entities():
    text = "Билеты в источнике 💰 2200\n26 июня 19:00 📗 Научная библиотека, Мира 9"
    entities = [
        MessageEntityCustomEmoji(
            offset=add_surrogate(text).index(add_surrogate("💰")),
            length=2,
            document_id=DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["💰"],
        ),
        MessageEntityCustomEmoji(
            offset=add_surrogate(text).index(add_surrogate("📗")),
            length=2,
            document_id=DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["📗"],
        ),
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert new_text == text
    assert new_entities == entities
    assert count == 0


def test_daily_venue_icon_insertion_extends_covering_format_entity():
    text = "26 июня 19:00 Научная библиотека, Мира 9"
    entities = [MessageEntityBold(offset=0, length=len(text))]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert count == 1
    assert new_text == "26 июня 19:00 📗 Научная библиотека, Мира 9"
    bold = next(entity for entity in new_entities if isinstance(entity, MessageEntityBold))
    assert bold.offset == 0
    assert bold.length == len(add_surrogate(new_text))


def test_daily_tretyakov_title_pair_is_cleaned_up_because_pair_is_venue_only():
    text = "👉 🖼🖼 Александр Дейнека. Картины советской эпохи"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == "👉 🖼️ Александр Дейнека. Картины советской эпохи"
    assert count == 2  # title cleanup plus regular `👉` premiumization
    custom_ids = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert custom_ids == [DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["👉"]]


def test_daily_tretyakov_added_row_does_not_infer_venue_from_title_text():
    text = "+1 ДОБАВИЛИ В АНОНС\n\n01.07 🚩 🖼️ Александр Дейнека"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == text
    assert new_entities == []
    assert count == 0


def test_daily_tretyakov_existing_added_row_pair_gets_composite_entities():
    text = "+1 ДОБАВИЛИ В АНОНС\n\n01.07 🖼🖼 Лекция в галерее"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == text
    assert count == 1
    assert DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS == (5188445640325099838, 5188470637034758005)
    custom_ids = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert custom_ids == list(DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS)


def test_daily_tretyakov_venue_line_gets_picture_pair_before_location():
    text = "26 июня 19:00 Филиал Третьяковской галереи, Парадная наб. 3"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == "26 июня 19:00 🖼🖼 Филиал Третьяковской галереи, Парадная наб. 3"
    assert count == 1
    custom_ids = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert custom_ids == list(DEFAULT_TRETYAKOV_EMOJI_DOCUMENT_IDS)


def test_tg_event_calendar_gets_custom_icon_and_ticket_row_uses_distinct_icon():
    text = "🎸 Концерт\n\n📅 30 июня 19:00\n\n🎟 Билеты 1000 руб.\n\n📅 Добавить в календарь"
    link_offset = add_surrogate(text).rindex(add_surrogate("📅"))
    entities = [
        MessageEntityTextUrl(
            offset=link_offset,
            length=len(add_surrogate("📅 Добавить в календарь")),
            url="https://example.org/calendar.ics",
        )
    ]

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, entities)

    assert "🎟 30 июня 19:00" in new_text
    assert "🎟 Добавить в календарь" in new_text
    assert "📅" not in new_text
    assert "🎫 Билеты 💰 1000" in new_text
    assert "руб" not in new_text
    assert count == 5  # two calendar icons, ticket icon migration, insert 💰, remove textual руб.
    ticket_entities = [
        entity
        for entity in new_entities
        if isinstance(entity, MessageEntityCustomEmoji)
        and entity.document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🎟"]
    ]
    money_entities = [
        entity
        for entity in new_entities
        if isinstance(entity, MessageEntityCustomEmoji)
        and entity.document_id == DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["💰"]
    ]
    assert len(ticket_entities) == 2
    assert len(money_entities) == 1
    shifted_link = next(e for e in new_entities if isinstance(e, MessageEntityTextUrl))
    sur_new_text = add_surrogate(new_text)
    assert (
        del_surrogate(sur_new_text[shifted_link.offset : shifted_link.offset + shifted_link.length])
        == "🎟 Добавить в календарь"
    )
    assert shifted_link.url == "https://example.org/calendar.ics"


def test_rock_concert_title_icon_becomes_horns_custom_emoji_from_body_signal():
    text = "🎸 Большой концерт\n\nЛучшие рок-хиты города\n\n📅 30 июня"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text.startswith("🤘 Большой концерт")
    assert count == 2  # title icon plus date/calendar premiumization
    custom_ids = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🤘"] in custom_ids
    assert DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🎟"] in custom_ids


def test_daily_added_rock_row_replaces_music_icon_not_recent_flag():
    text = "+1 ДОБАВИЛИ В АНОНС\n\n01.07 🚩 🎸 Рок-концерт во дворе"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert "01.07 🚩 🤘 Рок-концерт во дворе" in new_text
    assert count == 1
    custom_ids = [entity.document_id for entity in new_entities if isinstance(entity, MessageEntityCustomEmoji)]
    assert DEFAULT_DAILY_SINGLE_EMOJI_DOCUMENT_IDS["🤘"] in custom_ids
