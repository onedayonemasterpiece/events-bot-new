from __future__ import annotations

from datetime import date

import pytest

from guide_excursions.visual_digest import (
    _BRAND_LOCKUP_FILE,
    _avatar_key,
    _brand_lockup,
    _source_name,
    _visual_start_iso,
    _visual_item_state,
    _visual_selection_reason,
    build_visual_digest_telegram_text,
    build_visual_digest_vk_text,
    render_visual_digest_story_image,
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
async def test_visual_digest_telegram_text_uses_title_links_without_shortener():
    text = await build_visual_digest_telegram_text(SAMPLE_ROWS, issue_id=42)

    assert text.splitlines()[0] == "<b>Дайджест экскурсий №42</b>"
    assert '<a href="https://vk.com/wall-1_2">Самые красивые виллы Амалиенау</a>' in text
    assert '<a href="https://t.me/example/10">Вармийское кольцо</a>' in text
    assert "vk.cc" not in text
    assert "#Калининград" in text
    assert "#Амалиенау" in text


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


def test_render_visual_digest_story_image_keeps_full_width_card():
    payload = render_visual_digest_card(SAMPLE_ROWS, issue_id=42, all_rows=SAMPLE_ROWS)
    story = render_visual_digest_story_image(payload)
    assert story.startswith(b"\xff\xd8")
    from PIL import Image
    import io

    img = Image.open(io.BytesIO(story))
    assert img.size == (1080, 1920)


def test_brand_lockup_uses_committed_v18_asset():
    logo = _brand_lockup()
    assert _BRAND_LOCKUP_FILE.is_file()
    assert logo.size == (358, 141)


def test_visual_digest_candidate_window_starts_tomorrow():
    assert _visual_start_iso(date(2026, 7, 1)) == "2026-07-02"


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


def test_visual_digest_repeat_policy_requires_serious_change():
    published = {
        **SAMPLE_ROWS[0],
        "published_visual_digest_issue_id": 10,
    }
    state = _visual_item_state(published)

    unchanged = {**published, "updated_at": "2026-07-01 12:00:00", "published_visual_digest_state": state}
    assert _visual_selection_reason(unchanged) == ""

    last_seats = {
        **published,
        "seats_text": "осталось 2 места",
        "published_visual_digest_state": {**state, "seats_text": "12 мест", "seats_count": 12},
    }
    assert _visual_selection_reason(last_seats) == "low_seats"

    moved = {**published, "date": "2026-07-05", "published_visual_digest_state": state}
    assert _visual_selection_reason(moved) == "changed_date"


def test_visual_digest_katya_identity_uses_surname_and_avatar():
    row = {
        "guide_names": ["Катя"],
        "organizer_names": ["ПРОгулки с Катей"],
        "source_username": "progulki_s_katey",
        "booking_url": "https://t.me/katerinakostiugova",
    }
    assert _source_name(row) == "Катя Костюгова"
    assert _avatar_key(row) == "katya_kostyugova"
