from __future__ import annotations

import pytest

import vk_intake
from poster_media import PosterMedia
from vk_intake import _budget_vk_parse_poster_texts


def test_long_vk_post_keeps_poster_ocr_logistics_lines(monkeypatch) -> None:
    """Regression for INC-2026-07-02.

    Long source captions should not cause us to drop the only poster evidence
    for time and venue.  The parser still owns semantics; this is just a compact
    evidence handoff into the LLM prompt.
    """

    monkeypatch.setenv("VK_PARSE_POSTER_TEXT_SKIP_MAIN_TEXT_CHARS", "1600")
    long_post = "Когда: 5 и 19 июля. Где: Пионерский, городской парк.\n" + (
        "Новостной контекст. " * 140
    )
    poster_ocr = """
    УЛИЧНЫЕ МЕЛОДИИ
    г. ПИОНЕРСКИЙ
    ГОРОДСКОЙ ПАРК
    05'ИЮЛЯ / 19'ИЮЛЯ
    14:00
    ВХОД СВОБОДНЫЙ
    """

    selected = _budget_vk_parse_poster_texts(long_post, [poster_ocr])

    assert selected
    compact = "\n".join(selected)
    assert "14:00" in compact
    assert "ПИОНЕРСКИЙ" in compact
    assert "ГОРОДСКОЙ ПАРК" in compact
    assert "05'ИЮЛЯ" in compact
    assert "19'ИЮЛЯ" in compact
    assert "ВХОД СВОБОДНЫЙ" in compact
    assert "УЛИЧНЫЕ МЕЛОДИИ" in compact


def test_long_vk_post_without_logistics_keeps_all_poster_ocr(monkeypatch) -> None:
    monkeypatch.setenv("VK_PARSE_POSTER_TEXT_SKIP_MAIN_TEXT_CHARS", "1600")
    long_post = "Подробный отчёт о городской жизни. " * 120
    poster_ocr = """
    УЛИЧНЫЕ МЕЛОДИИ
    ОРКЕСТР
    ЛЕТНИЙ НАСТРОЙ
    """

    assert _budget_vk_parse_poster_texts(long_post, [poster_ocr]) == [
        "УЛИЧНЫЕ МЕЛОДИИ\nОРКЕСТР\nЛЕТНИЙ НАСТРОЙ"
    ]


def test_explicit_schedule_cards_keep_complete_bounded_ocr_gallery(monkeypatch) -> None:
    """Regression for INC-2026-08-01 exact VK roundup transport."""

    monkeypatch.setenv("VK_PARSE_SCHEDULE_POSTER_TEXT_MAX_BLOCKS", "10")
    monkeypatch.setenv("VK_PARSE_SCHEDULE_POSTER_TEXT_MAX_BLOCK_CHARS", "1200")
    monkeypatch.setenv("VK_PARSE_SCHEDULE_POSTER_TEXT_MAX_TOTAL_CHARS", "9000")
    source = (
        "С 1 по 9 августа в Калининграде и области пройдут соревнования. "
        "Расписание и места проведения – в карточках."
    )
    cards = [
        "АФИША СПОРТИВНЫХ МЕРОПРИЯТИЙ\n1–9 АВГУСТА",
        "КУБОК ОБЛАСТИ ПО БАСКЕТБОЛУ 3x3\n1 АВГУСТА\nКалининград, Пространство МОСТ",
        "ДЕНЬ ФИЗКУЛЬТУРНИКА\n7 АВГУСТА\nКалининград, пл. Победы, 10",
        "ПЕРВЕНСТВО ОБЛАСТИ ПО ПЛЯЖНОМУ ФУТБОЛУ\n8–9 АВГУСТА\nСветлый, пляжный стадион",
        "ЧЕМПИОНАТ ОБЛАСТИ ПО ПЛЯЖНОМУ ВОЛЕЙБОЛУ\n9 АВГУСТА\nЗеленоградск, ФОК Янтарь",
        "ЧЕМПИОНАТ И ПЕРВЕНСТВО ОБЛАСТИ ПО ТРИАТЛОНУ\n1 АВГУСТА\nГусев, ул. Зворыкина",
        "ЧЕМПИОНАТ ОБЛАСТИ ПО СИЛОВОМУ ЭКСТРИМУ\n1 АВГУСТА\nКалининград, ул. Потёмкина, 18",
        "КУБОК ПАМЯТИ В. С. УСТИНОВА ПО РЕГБИ\n2–3 АВГУСТА\nЗеленоградск, ФОК Янтарь",
    ]

    selected = _budget_vk_parse_poster_texts(source, cards)

    assert selected == cards
    assert len(selected) == 8
    assert "ДЕНЬ ФИЗКУЛЬТУРНИКА" in selected[2]
    assert "КУБОК ПАМЯТИ" in selected[-1]


def test_explicit_schedule_cards_ignore_semantic_block_cap(monkeypatch) -> None:
    monkeypatch.setenv("VK_PARSE_SCHEDULE_POSTER_TEXT_MAX_BLOCKS", "3")
    source = "Расписание и места проведения — в карточках."

    selected = _budget_vk_parse_poster_texts(source, [f"КАРТОЧКА {idx}" for idx in range(8)])

    assert selected == [f"КАРТОЧКА {idx}" for idx in range(8)]


@pytest.mark.asyncio
async def test_vk_llm_budget_uses_raw_source_before_appended_policy(monkeypatch) -> None:
    source = "Расписание и места проведения — в карточках."
    cards = [f"СОРЕВНОВАНИЕ {idx}\n{idx + 1} АВГУСТА\nГОРОД {idx}" for idx in range(8)]
    media = [
        PosterMedia(data=str(idx).encode(), name=f"{idx}.jpg", ocr_text=card)
        for idx, card in enumerate(cards)
    ]
    captured: dict[str, object] = {}

    async def fake_parse(_prompt, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(vk_intake, "require_main_attr", lambda _name: fake_parse)

    await vk_intake.vk_intake_parse_llm(
        source + ("\nДлинная appended policy. " * 200),
        source_text=source,
        poster_media=media,
    )

    assert captured["poster_texts"] == cards
