from __future__ import annotations

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
    assert "УЛИЧНЫЕ МЕЛОДИИ" not in compact


def test_long_vk_post_without_logistics_still_skips_poster_ocr(monkeypatch) -> None:
    monkeypatch.setenv("VK_PARSE_POSTER_TEXT_SKIP_MAIN_TEXT_CHARS", "1600")
    long_post = "Подробный отчёт о городской жизни. " * 120
    poster_ocr = """
    УЛИЧНЫЕ МЕЛОДИИ
    ОРКЕСТР
    ЛЕТНИЙ НАСТРОЙ
    """

    assert _budget_vk_parse_poster_texts(long_post, [poster_ocr]) == []

