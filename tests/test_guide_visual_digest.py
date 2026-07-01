from __future__ import annotations

import pytest

from guide_excursions.visual_digest import (
    build_visual_digest_vk_text,
    render_visual_digest_cards,
    render_visual_digest_card,
)


SAMPLE_ROWS = [
    {
        "id": 1,
        "canonical_title": "Самые красивые виллы Амалиенау",
        "date": "2026-07-02",
        "time": "18:30",
        "city": "Калининград",
        "guide_names": ["Таня Бурдужан"],
        "route_summary": "Амалиенау",
        "seats_text": "осталось 3 места",
        "booking_url": "https://vk.com/wall-1_2",
        "source_post_url": "https://vk.com/wall-1_2",
    },
    {
        "id": 2,
        "canonical_title": "Вармийское кольцо",
        "date": "2026-07-03",
        "time": "09:00",
        "city": "Калининградская область",
        "guide_names": ["Татьяна Удовенко"],
        "route_summary": "Ушаково → Ладушкин → Багратионовск",
        "booking_url": "https://t.me/example/10",
        "source_post_url": "https://t.me/source/20",
    },
]


@pytest.mark.asyncio
async def test_visual_digest_vk_text_uses_vk_clickable_and_shortens_external_links():
    calls = []

    async def fake_vk(method, params, db=None, bot=None, **kwargs):
        calls.append((method, params["url"]))
        return {"response": {"short_url": f"https://vk.cc/{len(calls)}"}}

    text = await build_visual_digest_vk_text(SAMPLE_ROWS, issue_id=42, vk_api_fn=fake_vk)

    assert text.splitlines()[0] == "Дайджест экскурсий №42"
    assert "1. [https://vk.com/wall-1_2|Самые красивые виллы Амалиенау]" in text
    assert "2. Вармийское кольцо — vk.cc/1" in text
    assert calls == [("utils.getShortLink", "https://t.me/example/10")]


@pytest.mark.asyncio
async def test_visual_digest_vk_text_keeps_phone_without_shortener():
    calls = []

    async def fake_vk(method, params, db=None, bot=None, **kwargs):  # pragma: no cover - must not be called
        calls.append((method, params))
        return {"response": {"short_url": "https://vk.cc/nope"}}

    rows = [
        {
            "id": 3,
            "canonical_title": "Прогулка по Понарту",
            "date": "2026-07-04",
            "booking_text": "+7 (999) 111-22-33",
        }
    ]
    text = await build_visual_digest_vk_text(rows, issue_id=43, vk_api_fn=fake_vk)
    assert "Прогулка по Понарту — +7 999 111-22-33" in text
    assert calls == []


def test_render_visual_digest_card_smoke():
    payload = render_visual_digest_card(SAMPLE_ROWS, issue_id=42, all_rows=SAMPLE_ROWS)
    assert payload.startswith(b"\xff\xd8")
    assert len(payload) > 30_000


def test_render_visual_digest_cards_chunks_by_five():
    rows = []
    for idx in range(7):
        item = dict(SAMPLE_ROWS[idx % 2])
        item["id"] = idx + 1
        item["date"] = f"2026-07-{idx+2:02d}"
        item["canonical_title"] = f"Экскурсия {idx+1}"
        rows.append(item)
    cards = render_visual_digest_cards(rows, issue_id=44)
    assert len(cards) == 2
    assert all(card.startswith(b"\xff\xd8") for card in cards)
