import io
import json

import pytest
from PIL import Image

from guide_excursions import hook_cards as hc
from guide_excursions.hook_card_render import (
    CANVAS,
    load_palettes,
    palette_by_id,
    render_hook_card,
)


# --------------------------------------------------------------------------- #
# Sanitizers
# --------------------------------------------------------------------------- #
def test_sanitize_hook_accepts_clean_question():
    assert hc.sanitize_hook("Что скрывает немецкая вилла на тихой улице?") == (
        "Что скрывает немецкая вилла на тихой улице?"
    )


@pytest.mark.parametrize(
    "bad",
    [
        "Купить билет за 500 ₽ сейчас",  # price
        "Старт в 12:00 от Канта",  # time
        "Запись в t.me/guide",  # url
        "Пишите на @guidekln прямо сейчас",  # username
        "Экскурсия 6 июня по фортам",  # date
        "Да?",  # too short
        "Слово " * 20,  # too long
        "Старинный парк и его тайны",  # statement, no question mark
        "Что прячут форты? Узнайте это?",  # more than one question
    ],
)
def test_sanitize_hook_rejects_contract_violations(bad):
    assert hc.sanitize_hook(bad) is None


def test_sanitize_hook_strips_emoji_and_collapses_space():
    assert hc.sanitize_hook("Город 🌊 у моря, который вы не знали?") == (
        "Город у моря, который вы не знали?"
    )


def test_sanitize_subline_limits():
    assert hc.sanitize_subline("Экскурсия недели") == "Экскурсия недели"
    assert hc.sanitize_subline("Очень длинная подпись из множества лишних слов") is None


# --------------------------------------------------------------------------- #
# Palette selection: one palette per publication, rotates per seed/day
# --------------------------------------------------------------------------- #
def test_select_post_palette_deterministic_per_seed():
    assert hc.select_post_palette(seed=86).id == hc.select_post_palette(seed=86).id


def test_select_post_palette_varies_across_days():
    seen = {hc.select_post_palette(seed=s).id for s in range(12)}
    assert len(seen) > 1  # different publication days get different colours


def test_short_date_and_subline():
    row = {"date": "2026-06-07", "guide_names": ["Анна Иванова"]}
    assert hc._format_card_subline(row) == "7 июня · Анна Иванова"
    assert hc._format_card_subline({"date": "2026-06-07"}) == "7 июня"
    assert hc._format_card_subline({"guide_names": ["Игорь"]}) == "Игорь"
    assert hc._format_card_subline({}) is None


# --------------------------------------------------------------------------- #
# Pipeline (generate + select) with a stubbed LLM
# --------------------------------------------------------------------------- #
def _rows():
    return [
        {"occurrence_id": 101, "canonical_title": "Прогулка по Амалиенау",
         "summary_one_liner": "Виллы и легенды", "city": "Калининград",
         "date": "2026-06-07", "guide_names": ["Анна Иванова"],
         "fact_pack": {"main_hook": "немецкие виллы"}},
        {"occurrence_id": 102, "canonical_title": "Поездка к кирхам",
         "summary_one_liner": "Старые кирхи", "city": "Гвардейск", "fact_pack": {}},
        {"occurrence_id": 103, "canonical_title": "Джип-тур по косе",
         "summary_one_liner": "Песок и форты", "city": "Балтийск",
         "date": "2026-06-08", "guide_names": ["Игорь Селин"],
         "fact_pack": {"main_hook": "форты"}},
        {"occurrence_id": 104, "canonical_title": "Вечерний Кёнигсберг",
         "summary_one_liner": "Сумерки", "city": "Калининград",
         "date": "2026-06-09", "guide_names": ["Мария П."],
         "fact_pack": {}},
    ]


def _fake_ask_factory(cards):
    async def _fake_ask(prompt, *, system_prompt=None, response_format=None,
                        max_tokens=0, temperature=0.0, meta=None):
        assert response_format and response_format["json_schema"]["name"] == "GuideHookCards"
        assert "карточек" in prompt
        return json.dumps({"cards": cards})

    return _fake_ask


_LLM_CARDS = [
    {"occurrence_id": 103, "hook": "Что прячут заброшенные форты на косе?",
     "subline": "Джип-тур недели", "theme": "индустриальное", "strength": 92},
    {"occurrence_id": 101, "hook": "Какие легенды хранят виллы Амалиенау?",
     "subline": "Прогулка недели", "theme": "история", "strength": 88},
    {"occurrence_id": 104, "hook": "Каким бывает Кёнигсберг в сумерках?",
     "subline": "Вечерний маршрут", "theme": "город", "strength": 75},
    # invalid (contains time) -> dropped
    {"occurrence_id": 102, "hook": "Старт в 12:00 от кирхи",
     "subline": "", "theme": "арх", "strength": 40},
]


@pytest.mark.asyncio
async def test_generate_hook_cards_slot_math_and_filtering():
    # 5 afishas already -> cap = min(9-5, 3) = 4, but only 3 valid hooks survive
    cards = await hc.generate_hook_cards(
        _rows(), existing_image_count=5, seed=14, ask_fn=_fake_ask_factory(_LLM_CARDS)
    )
    assert [c.occurrence_id for c in cards] == [103, 101, 104]  # sorted by strength
    # one palette per publication (all cards share it)
    assert len({c.palette.id for c in cards}) == 1
    # footer is date + guide, not a slogan
    subs = {c.occurrence_id: c.sub_text for c in cards}
    assert subs[103] == "8 июня · Игорь Селин"
    assert subs[101] == "7 июня · Анна Иванова"
    assert all(c.main_text for c in cards)


@pytest.mark.asyncio
async def test_generate_hook_cards_caps_to_free_slots():
    one = await hc.generate_hook_cards(
        _rows(), existing_image_count=8, seed=1, ask_fn=_fake_ask_factory(_LLM_CARDS)
    )
    assert len(one) == 1  # only one free slot
    none = await hc.generate_hook_cards(
        _rows(), existing_image_count=9, seed=1, ask_fn=_fake_ask_factory(_LLM_CARDS)
    )
    assert none == []  # grid full


@pytest.mark.asyncio
async def test_generate_hook_cards_llm_failure_is_safe():
    async def boom(*a, **k):
        raise RuntimeError("llm down")

    assert await hc.generate_hook_cards(_rows(), existing_image_count=0, ask_fn=boom) == []


@pytest.mark.asyncio
async def test_generate_hook_cards_ignores_unknown_occurrence_ids():
    bogus = [{"occurrence_id": 999, "hook": "Чужой крючок про что-то?",
              "subline": "Недели", "theme": "x", "strength": 99}]
    cards = await hc.generate_hook_cards(
        _rows(), existing_image_count=0, ask_fn=_fake_ask_factory(bogus)
    )
    assert cards == []


# --------------------------------------------------------------------------- #
# Renderer
# --------------------------------------------------------------------------- #
def test_render_hook_card_smoke():
    png = render_hook_card(
        main_text="Что скрывает немецкая вилла на тихой улице?",
        sub_text="Экскурсия недели",
        palette=palette_by_id("deep_wine_ivory"),
    )
    img = Image.open(io.BytesIO(png))
    assert img.size == (CANVAS, CANVAS)
    assert img.mode == "RGB"


def test_render_every_palette_does_not_crash():
    for palette in load_palettes():
        png = render_hook_card(
            main_text="Город, который не показывают в первый день",
            sub_text=None,
            palette=palette,
        )
        assert png[:8] == b"\x89PNG\r\n\x1a\n"


# --------------------------------------------------------------------------- #
# Service integration helper (render + upload, best-effort, ordered)
# --------------------------------------------------------------------------- #
class _StubCard:
    def __init__(self, occ, png, palette_id="pal"):
        self.occurrence_id = occ
        self._png = png

        class _P:
            id = palette_id

        self.palette = _P()

    def render_png(self):
        if self._png is None:
            raise RuntimeError("render boom")
        return self._png


@pytest.mark.asyncio
async def test_build_vk_hook_card_attachments_orders_and_is_best_effort(monkeypatch):
    from guide_excursions import service

    cards = [_StubCard(1, b"png1"), _StubCard(2, None), _StubCard(3, b"png3")]

    async def fake_generate(rows, *, existing_image_count, seed=0):
        assert existing_image_count == 5
        return cards

    monkeypatch.setattr("guide_excursions.hook_cards.generate_hook_cards", fake_generate)

    uploaded = []

    async def fake_upload(group_id, image_bytes, db, bot, *, filename="x.png"):
        uploaded.append((image_bytes, filename))
        # second valid card fails to upload -> skipped
        return None if image_bytes == b"png3" else f"photo-{image_bytes.decode()}"

    out = await service._build_guide_vk_hook_card_attachments(
        group_id=123,
        rows=[{"occurrence_id": 1}],
        existing_image_count=5,
        db=None,
        bot=None,
        upload_vk_photo_bytes_fn=fake_upload,
        seed=77,
    )
    # card 2 raised on render, card 3 upload returned None -> only card 1 survives
    assert out == ["photo-png1"]
    assert uploaded[0][0] == b"png1"


@pytest.mark.asyncio
async def test_build_vk_hook_card_attachments_swallows_generate_failure(monkeypatch):
    from guide_excursions import service

    async def boom(*a, **k):
        raise RuntimeError("nope")

    monkeypatch.setattr("guide_excursions.hook_cards.generate_hook_cards", boom)

    async def fake_upload(*a, **k):  # pragma: no cover - should not be called
        raise AssertionError("upload should not run")

    out = await service._build_guide_vk_hook_card_attachments(
        group_id=1,
        rows=[{"occurrence_id": 1}],
        existing_image_count=0,
        db=None,
        bot=None,
        upload_vk_photo_bytes_fn=fake_upload,
    )
    assert out == []


# --------------------------------------------------------------------------- #
# Carousel builder (offline, stubbed vision + LLM)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_build_carousel_slides_mixes_photo_afisha_text_and_cta(tmp_path):
    import json as _json
    from guide_excursions.hook_carousel import build_carousel_slides

    # two on-disk media images: one "afisha" (reddish), one "photo" (greenish)
    afi = tmp_path / "afi.jpg"
    pho = tmp_path / "pho.jpg"
    Image.new("RGB", (900, 1200), (180, 60, 60)).save(afi)
    Image.new("RGB", (900, 1200), (80, 150, 90)).save(pho)
    media_items = [
        {"occurrence_id": 501, "media_asset": {"path": str(afi), "kind": "photo"}},
        {"occurrence_id": 502, "media_asset": {"path": str(pho), "kind": "photo"}},
    ]
    rows = [
        {"occurrence_id": 501, "canonical_title": "Фестиваль", "city": "Калининград",
         "date": "2026-06-06", "guide_names": ["Орг"], "fact_pack": {"main_hook": "лекции"}},
        {"occurrence_id": 502, "canonical_title": "Амалиенау", "city": "Калининград",
         "date": "2026-06-21", "guide_names": ["Таня"], "fact_pack": {"main_hook": "виллы"}},
        {"occurrence_id": 503, "canonical_title": "Байдарка", "city": "Калининград",
         "date": "2026-05-30", "guide_names": ["Синдикат"], "fact_pack": {"main_hook": "река"}},
    ]

    async def fake_ask(prompt, *, system_prompt=None, response_format=None, max_tokens=0, temperature=0.0, meta=None):
        ids = [int(x) for x in re.findall(r'"occurrence_id":\s*(\d+)', prompt)]
        return _json.dumps({"cards": [
            {"occurrence_id": i, "hook": f"Что скрывает место {i}?", "theme": "x", "strength": 90 - i}
            for i in ids
        ]})

    async def fake_classify(image_bytes):
        im = Image.open(io.BytesIO(image_bytes)).convert("RGB").resize((1, 1))
        r, g, b = im.getpixel((0, 0))
        return r > g  # reddish -> afisha

    slides = await build_carousel_slides(rows, media_items, seed=80, ask_fn=fake_ask, classify_fn=fake_classify)
    # photo+hook (502) + afisha (501) + text card (503) + CTA
    assert len(slides) >= 3
    for s in slides:
        img = Image.open(io.BytesIO(s))
        assert img.size == (1080, 1350)


@pytest.mark.asyncio
async def test_build_carousel_slides_empty_without_media_or_rows():
    from guide_excursions.hook_carousel import build_carousel_slides
    assert await build_carousel_slides([], [], seed=1) == []
