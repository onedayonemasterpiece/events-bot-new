from telethon.helpers import add_surrogate, del_surrogate
from telethon.tl.types import MessageEntityBold, MessageEntityTextUrl, MessageEntityCustomEmoji

from tg_premium_emojis import (
    DEFAULT_FREE_EMOJI_DOCUMENT_IDS,
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

    assert count == 2
    assert "🟡 Бесплатно" not in new_text
    assert "ДОБАВИЛИ В АНОНС\n\nКАЛИНИНГРАД\n01.07 🚩 🟡" not in new_text
    assert "👉 🚩 🟡 Заголовок до блока не компактится" in new_text
    assert "🆓🆓🆓🆓 по регистрации" in new_text
    assert "01.07 🆓🆓🆓🆓 🧦 Лекция" in new_text

    custom = [e for e in new_entities if isinstance(e, MessageEntityCustomEmoji)]
    assert [e.document_id for e in custom[:4]] == list(DEFAULT_FREE_EMOJI_DOCUMENT_IDS)
    assert len(custom) == 8

    shifted_link = next(e for e in new_entities if isinstance(e, MessageEntityTextUrl))
    sur_new_text = add_surrogate(new_text)
    assert del_surrogate(sur_new_text[shifted_link.offset : shifted_link.offset + shifted_link.length]) == "Лекция"


def test_daily_free_replacement_is_idempotent():
    text = "🆓🆓🆓🆓 по регистрации\n+1 ДОБАВИЛИ В АНОНС\n01.07 🆓🆓🆓🆓 Лекция"

    new_text, new_entities, count = apply_daily_free_premium_emojis(text, [])

    assert new_text == text
    assert new_entities == []
    assert count == 0
