from __future__ import annotations

import io
from datetime import datetime, timezone

import pytest
from PIL import Image
from sqlalchemy import select

import afishaengagement as aeg
import main
from db import Database
from models import Event, PromoActivity, PromoCampaign, PromoExposure, PromoTarget


def _poster_bytes() -> bytes:
    image = Image.new("RGB", (420, 620))
    pixels = []
    for y in range(image.height):
        for x in range(image.width):
            pixels.append(((x * 13 + y * 7) % 255, (x * 3 + y * 17) % 255, (x * 19 + y * 5) % 255))
    image.putdata(pixels)
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=92)
    return out.getvalue()


def _horizontal_poster_bytes() -> bytes:
    image = Image.new("RGB", (800, 450))
    pixels = []
    for y in range(image.height):
        for x in range(image.width):
            pixels.append(((x * 5 + y * 11) % 255, (x * 17 + y * 2) % 255, (x * 3 + y * 13) % 255))
    image.putdata(pixels)
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=92)
    return out.getvalue()


def _large_poster_bytes() -> bytes:
    image = Image.new("RGB", (900, 1260))
    pixels = []
    for y in range(image.height):
        for x in range(image.width):
            pixels.append(((x * 7 + y * 3) % 255, (x * 11 + y * 5) % 255, (x * 2 + y * 13) % 255))
    image.putdata(pixels)
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=92)
    return out.getvalue()


def _square_poster_bytes() -> bytes:
    image = Image.new("RGB", (900, 900))
    pixels = []
    for y in range(image.height):
        for x in range(image.width):
            pixels.append(((x * 9 + y * 4) % 255, (x * 5 + y * 8) % 255, (x * 2 + y * 17) % 255))
    image.putdata(pixels)
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=92)
    return out.getvalue()


def test_apply_rate_is_stable_and_uses_bounds():
    first = aeg.should_apply_rate(
        event_id=10,
        campaign_id=20,
        activity_id=30,
        apply_rate=0.5,
        salt="same",
        media_digest="abc",
    )
    second = aeg.should_apply_rate(
        event_id=10,
        campaign_id=20,
        activity_id=30,
        apply_rate=0.5,
        salt="same",
        media_digest="abc",
    )

    assert first == second
    assert 0 <= first.value < 1
    assert aeg.should_apply_rate(event_id=1, campaign_id=1, activity_id=1, apply_rate=0).applies is False
    assert aeg.should_apply_rate(event_id=1, campaign_id=1, activity_id=1, apply_rate=1).applies is True


def test_target_all_matches_any_event():
    event = Event(
        title="Любое событие",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
    )
    target = PromoTarget(campaign_id=1, target_type="all")

    assert aeg._target_matches(event, target) is True


def test_event_type_config_filters_lecture_only():
    lecture = Event(
        title="Лекция о городе",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
    )
    concert = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
    )

    assert aeg._config_matches_event(lecture, {"event_type_keys": ["lecture"]}) is True
    assert aeg._config_matches_event(concert, {"event_type_keys": ["lecture"]}) is False


def test_text_fit_keeps_word_boundaries():
    fit = aeg.fit_text(
        "Лайк, если хочешь чаще таких мастер-классов.",
        box_width=520,
        box_height=360,
        preferred_px=72,
        min_px=52,
    )

    assert fit is not None
    assert fit.font_px >= 52
    assert " ".join(fit.lines) == "Лайк, если хочешь чаще таких мастер-классов."
    assert all("- " not in line for line in fit.lines)


def test_render_right_extension_outputs_png_with_fit_text():
    event = Event(
        title="Лекция с Иваном Петровым",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="render-test",
        config={"palette_ids": ["deep_wine_ivory"], "mechanic_weights": {"likes": 100}},
    )

    rendered = aeg.render_right_extension(_poster_bytes(), plan)

    assert rendered.template_id == "right_extension"
    assert rendered.palette_id in aeg.PALETTES
    assert rendered.dimensions[0] > 420
    assert rendered.dimensions[1] == 620
    assert rendered.cta_text_font_px >= 24
    assert rendered.data.startswith(b"\x89PNG")
    assert len(rendered.data) > 50_000
    with Image.open(io.BytesIO(rendered.data)) as image:
        assert image.size == rendered.dimensions


def test_right_extension_keeps_at_least_95_percent_of_small_poster_width():
    source = _poster_bytes()
    event = Event(
        title="Мария Макарова акустика",
        description="Акустический концерт",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="small-poster-preserve-right-edge",
        config={"palette_ids": ["deep_wine_ivory"], "mechanic_weights": {"likes": 100}},
    )

    rendered = aeg.render_right_extension(source, plan)

    with Image.open(io.BytesIO(source)) as original, Image.open(io.BytesIO(rendered.data)) as image:
        x = int(original.width * 0.95) - 1
        y = original.height - 2
        assert image.getpixel((x, y)) == original.convert("RGB").getpixel((x, y))


def test_render_horizontal_poster_uses_bottom_extension_without_resizing_width():
    event = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="horizontal-render-test",
        config={"palette_ids": ["deep_wine_ivory"], "mechanic_weights": {"likes": 100}},
    )

    rendered = aeg.render_right_extension(_horizontal_poster_bytes(), plan)

    assert rendered.template_id == "bottom_extension"
    assert rendered.dimensions[0] == 800
    assert rendered.dimensions[1] > 450
    assert rendered.cta_text_font_px >= 24


def test_cta_text_sanitizer_removes_parenthetical_gender_and_punctuation_spaces():
    assert aeg._sanitize_cta_text("Поделись с теми, кому близки такие , лекции.") == (
        "Поделись с теми, кому близки такие лекции."
    )
    assert aeg._sanitize_cta_text("Поставь лайк, если сохранил(а) в планы.") == (
        "Поставь лайк, если добавил в планы."
    )


def test_cta_text_sanitizer_preserves_intentional_line_breaks():
    text = aeg._sanitize_cta_text("Поставь лайк ❤️, если уже зарегистри-\nровался на эту лекцию.")

    assert "зарегистри-\nровался" in text
    fit = aeg.fit_text(text, box_width=420, box_height=260, preferred_px=56, min_px=32)
    assert fit is not None
    assert "зарегистри-" in fit.lines
    assert any(line.startswith("ровался") for line in fit.lines)


def test_repost_template_uses_event_type_noun():
    event = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"repost-template-{idx}",
            config={"mechanic_weights": {"reposts": 100}},
        ).cta_text
        for idx in range(40)
    }

    assert "Поделись с другом, который любит такие концерты." in seen
    assert all("такие афиши" not in text for text in seen)


def test_cinema_event_type_does_not_use_theatre_copy():
    event = Event(
        title="Кинопоказ фильма «Письма»",
        description="Обсуждение после фильма",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="кинопоказ",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"cinema-copy-{idx}",
            config={"mechanic_weights": {"comments": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert aeg._event_type_key(event) == "cinema"
    assert any("кинопоказ" in text.casefold() or "фильм" in text.casefold() for text in seen)
    assert all("спектак" not in text.casefold() for text in seen)


def test_forbidden_generic_phrases_are_not_generated():
    event = Event(
        title="Вечер симфонической музыки",
        description="Концерт камерного оркестра",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"forbidden-copy-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 60}},
        ).cta_text
        for idx in range(200)
    }

    assert all("похож" not in text.casefold() for text in seen)
    assert all("формат" not in text.casefold() for text in seen)
    assert all("в таких концертов" not in text.casefold() for text in seen)
    assert any("в таких концертах" in text.casefold() for text in seen)


def test_concert_like_copy_can_use_extracted_theme():
    event = Event(
        title="Вечер симфонической музыки",
        description="Концерт камерного оркестра",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"theme-copy-{idx}",
            config={"mechanic_weights": {"likes": 100}},
        ).cta_text
        for idx in range(200)
    }

    assert "Поддержи лайком, если любишь симфоническую музыку." in seen


def test_concert_theme_does_not_read_organ_from_organizers():
    event = Event(
        title="Праздничный концерт Балтийского казачьего хора ко Дню России",
        description="Организаторы приглашают на выступление казачьего хора.",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="Организаторы концерта подготовили народные песни.",
        event_type="концерт",
    )

    assert aeg._extract_theme(event, "concert") == "казачьи песни"

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"kazach-organizers-theme-{idx}",
            config={"mechanic_weights": {"comments": 50, "likes": 50}},
        ).cta_text
        for idx in range(200)
    }

    assert all("органн" not in text.casefold() for text in seen)
    assert any("казачьи песни" in text.casefold() for text in seen)


def test_concert_theme_still_detects_real_organ_music():
    event = Event(
        title="Русская музыка на органе",
        description="Органный концерт в кафедральном соборе.",
        date="2026-06-20",
        time="19:00",
        location_name="Кафедральный собор",
        source_text="В программе орган и камерный оркестр.",
        event_type="концерт",
    )

    assert aeg._extract_theme(event, "concert") == "органную музыку"


def test_configured_registration_cta_uses_event_type_phrase():
    event = Event(
        title="Лекция проекта 80 историй",
        description="Регистрация открыта",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
        festival="80 историй о главном",
    )

    plan = aeg.build_engagement_plan(
        event,
        seed="registration-like-cta",
        config={
            "mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0},
            "cta_templates": {
                "by_event_type": {
                    "*": {
                        "likes": [
                            "Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."
                        ]
                    }
                }
            },
        },
    )

    assert plan.mechanic == "likes"
    assert plan.cta_text == "Поставь лайк ❤️, если уже зарегистрировался на эту лекцию."


def test_configured_registration_cta_adapts_to_theatre():
    event = Event(
        title="Спектакль проекта 80 историй",
        description="Регистрация открыта",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="спектакль",
        festival="80 историй о главном",
    )

    plan = aeg.build_engagement_plan(
        event,
        seed="registration-like-cta-theatre",
        config={
            "mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0},
            "cta_templates": {
                "likes": ["Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."]
            },
        },
    )

    assert plan.cta_text == "Поставь лайк ❤️, если уже зарегистрировался на этот спектакль."


def test_festival_comment_template_names_festival_and_annual_context():
    event = Event(
        title="Кантата",
        description="VI Международный фестиваль классической музыки",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="фестиваль",
        festival="Кантата",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"festival-comment-{idx}",
            config={"mechanic_weights": {"comments": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert "Что ждёте от фестиваля Кантата в этом году? Напишите в комментариях." in seen
    assert all("от Кантата" not in text for text in seen)


def test_comments_badge_is_action_phrase():
    assert aeg.MECHANIC_BADGES["comments"] == "НАПИШИ КОММЕНТАРИЙ"


def test_hook_text_uses_prepositional_plural():
    event = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    plan = aeg.build_engagement_plan(event, seed="hook-case", config={"formats": ["hook_swipe_cta"]})

    assert plan.hook_text == "Есть вопрос к тем, кто уже был на таких концертах"


def test_market_event_type_is_not_overridden_by_meeting_words():
    event = Event(
        title="PUNK Market",
        description="Маркет, встречи и музыка",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="ярмарка",
    )

    plan = aeg.build_engagement_plan(
        event,
        seed="market-type",
        config={"mechanic_weights": {"likes": 100}},
    )

    assert plan.event_type == "market"
    assert "лекц" not in plan.cta_text.casefold()
    assert "фестивал" not in plan.cta_text.casefold()


def test_explicit_event_type_is_reused_before_title_heuristics():
    event = Event(
        title="Встреча после концерта",
        description="Обсуждаем музыку и планы сообщества",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    plan = aeg.build_engagement_plan(
        event,
        seed="explicit-type-wins",
        config={"mechanic_weights": {"reposts": 100}},
    )

    assert plan.event_type == "concert"
    assert "лекц" not in plan.cta_text.casefold()


def test_family_meeting_with_fairy_characters_does_not_get_lecture_copy():
    event = Event(
        title="Встреча со сказочными героями",
        description="Детский праздник в Холмогорье",
        date="2026-06-20",
        time="12:00",
        location_name="Холмогорье",
        source_text="",
        event_type="встреча",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"family-fairy-copy-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 40, "reposts": 20}},
        ).cta_text
        for idx in range(120)
    }

    assert aeg._event_type_key(event) == "family"
    assert all("лекц" not in text.casefold() for text in seen)
    assert any("дет" in text.casefold() or "семейн" in text.casefold() for text in seen)


def test_renderer_can_select_poster_compatible_palette():
    source = _poster_bytes()
    event = Event(
        title="Лекция с Иваном Петровым",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="palette-compatible-render-test",
        config={"palette_ids": ["yellow_violet"], "mechanic_weights": {"comments": 100}},
    )

    rendered = aeg.render_right_extension(source, plan)
    palette = aeg.PALETTES[rendered.palette_id]

    assert rendered.palette_id in aeg.PALETTES
    assert aeg._contrast_ratio(aeg._hex_to_rgb(palette["background"]), aeg._hex_to_rgb(palette["text"])) >= 4.5


def test_palette_selection_prioritizes_cta_separation_from_poster_edge():
    poster = Image.new("RGB", (900, 1200), (10, 30, 52))
    draw = Image.new("RGB", (90, 1200), (12, 32, 55))
    poster.paste(draw, (810, 0))

    palette_id = aeg._choose_compatible_palette_id(
        poster,
        preferred_id="prussian_cream",
        seed="separation-priority",
        template_id="right_extension",
        event_type="concert",
    )
    roles = aeg._palette_roles(aeg.PALETTES[palette_id])
    surface = aeg._hex_to_rgb(roles["surface"])
    profile = aeg._poster_color_profile(poster, region_box=aeg._seam_region_box("right_extension", 900, 1200))
    edge_rgb = profile["edge_rgb"]

    assert aeg._contrast_ratio(surface, edge_rgb) >= 1.6
    assert abs(aeg._relative_luminance(surface) - profile["edge_luma"]) >= 0.22
    assert palette_id != "prussian_cream"


def test_render_bottom_overlay_preserves_source_dimensions():
    event = Event(
        title="Лекция о городе",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="bottom-overlay-render-test",
        config={
            "formats": ["bottom_overlay"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"comments": 100},
        },
    )

    rendered = aeg.render_plan_images(_square_poster_bytes(), plan)[0]

    assert rendered.template_id == "bottom_overlay"
    assert rendered.dimensions == (900, 900)
    assert rendered.cta_text_font_px >= 22
    assert rendered.data.startswith(b"\x89PNG")


def test_bottom_overlay_can_invert_surface_on_dark_poster_region():
    poster = Image.new("RGB", (800, 800), (12, 12, 12))
    palette = aeg.PALETTES["black_lime"]

    surface, inverted = aeg._cta_surface_palette_for_region(
        poster,
        palette,
        seed="dark-overlay",
        region_box=(0, 520, 800, 800),
    )

    assert inverted is True
    assert surface["background"] == palette["text"]
    assert surface["text"] == palette["background"]


def test_render_explicit_bottom_extension_preserves_source_width():
    event = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="explicit-bottom-extension-render-test",
        config={
            "formats": ["bottom_extension"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"likes": 100},
        },
    )

    rendered = aeg.render_plan_images(_horizontal_poster_bytes(), plan)[0]

    assert rendered.template_id == "bottom_extension"
    assert rendered.dimensions[0] == 800
    assert rendered.dimensions[1] > 450
    assert rendered.dimensions[1] <= int(rendered.dimensions[0] * aeg.MAX_VK_FEED_PHOTO_ASPECT)


def test_horizontal_bottom_extension_overflow_falls_back_to_real_right_extension(monkeypatch):
    event = Event(
        title="Концерт камерной музыки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="horizontal-bottom-overflow-fallback-render-test",
        config={
            "formats": ["bottom_extension"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"likes": 100},
        },
    )

    def fake_bottom_extension(*args, **kwargs):
        raise ValueError("text_overflow")

    monkeypatch.setattr(aeg, "_render_bottom_extension", fake_bottom_extension)

    rendered = aeg.render_plan_images(_horizontal_poster_bytes(), plan)[0]

    assert rendered.template_id == "right_extension"
    assert rendered.dimensions[0] > 800
    assert rendered.dimensions[1] == 450


def test_vertical_poster_bottom_extension_falls_back_to_right_extension():
    event = Event(
        title="Лекция о городе",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="vertical-bottom-extension-render-test",
        config={
            "formats": ["bottom_extension"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"likes": 100},
        },
    )

    rendered = aeg.render_plan_images(_large_poster_bytes(), plan)[0]

    assert rendered.template_id == "right_extension"
    assert rendered.dimensions[1] == 1260


def test_small_poster_bottom_template_falls_back_to_right_extension():
    event = Event(
        title="Мария Макарова акустика",
        description="Акустический концерт",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="small-poster-fallback-render-test",
        config={
            "formats": ["bottom_extension"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"likes": 100},
        },
    )

    rendered = aeg.render_plan_images(_poster_bytes(), plan)[0]

    assert rendered.template_id == "right_extension"
    assert rendered.dimensions[0] > 420
    assert rendered.dimensions[1] == 620


def test_render_hook_swipe_cta_returns_two_cards():
    event = Event(
        title="Фестиваль Кантата",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="фестиваль",
        festival="Кантата",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="hook-swipe-render-test",
        config={
            "formats": ["hook_swipe_cta"],
            "palette_ids": ["midnight_gold"],
            "mechanic_weights": {"comments": 100},
        },
    )

    rendered = aeg.render_plan_images(_poster_bytes(), plan)

    assert [image.template_id for image in rendered] == ["hook_swipe", "hook_swipe_cta"]
    assert all(image.dimensions == (1080, 1350) for image in rendered)


@pytest.mark.asyncio
async def test_llm_plan_bad_json_falls_back(monkeypatch):
    event = Event(
        title="Концерт Анны Смирновой",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    async def fake_ask_4o(*args, **kwargs):
        return "не json"

    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    plan, _elapsed_ms, provider = await aeg.build_llm_engagement_plan(
        event,
        seed="bad-json",
        config={"palette_ids": ["black_lime"]},
        vision=aeg.PosterVisionSummary(provider="test", confidence=0.5),
    )

    assert provider == "fallback_bad_json"
    assert plan.palette_id == "black_lime"
    assert plan.cta_text


@pytest.mark.asyncio
async def test_shadow_debug_copy_schedules_generated_media_and_records_exposure(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Лекция с Иваном Петровым",
            description="",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            event_type="лекция",
            photo_urls=["https://example.test/poster.jpg"],
        )
        campaign = PromoCampaign(title="Мотивация", status="active", starts_at=now)
        session.add_all([event, campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(campaign)
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "apply_rate": 1,
                "debug_marker": "#aeg_test_shadow",
                "debug_cleanup_before": True,
                "palette_ids": ["prussian_cream"],
            },
        )
        target = PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id))
        session.add_all([activity, target])
        await session.commit()
        await session.refresh(activity)

    vk_calls = []
    posted = {}

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        vk_calls.append((method, params))
        if method == "wall.get":
            return {"response": {"items": []}}
        return {"response": 1}

    async def fake_upload_images(images, *args, **kwargs):
        assert len(images) == 1
        assert images[0][0].startswith(b"\x89PNG")
        assert kwargs["force"] is True
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        assert group_id == "231920894"
        assert url == "https://storage.test/generated.png"
        return "photo-231920894_777"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["group_id"] = group_id
        posted["message"] = message
        posted["attachments"] = attachments
        posted["kwargs"] = kwargs
        return "https://vk.com/wall-231920894_999"

    async def fake_fetch_image(_url):
        return _poster_bytes()

    url = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="231920894",
        message="Обычный пост",
        photo_urls=event.photo_urls,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_fn=fake_upload_vk_photo,
        upload_images_fn=fake_upload_images,
        vk_api_fn=fake_vk_api,
        fetch_image_fn=fake_fetch_image,
        now_utc=now,
    )

    assert url == "https://vk.com/wall-231920894_999"
    assert posted["attachments"] == ["photo-231920894_777"]
    assert posted["kwargs"]["carousel"] is True
    assert posted["kwargs"]["publish_date"] >= int(now.timestamp()) + 3 * 24 * 3600
    assert "#aeg_test_shadow #aeg_b20260609" in posted["message"]
    assert vk_calls[0][0] == "wall.get"

    async with db.get_session() as session:
        rows = list((await session.execute(select(PromoExposure))).scalars().all())
    assert len(rows) == 1
    assert rows[0].surface == aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT
    assert rows[0].publish_status == "VK_SCHEDULED_DEBUG"
    assert rows[0].details_json["shadow_marker"] == "#aeg_test_shadow"


@pytest.mark.asyncio
async def test_shadow_debug_copy_falls_through_after_candidate_dice_miss(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Лекция проекта 80 историй",
            description="Регистрация открыта",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            event_type="лекция",
            festival="80 историй о главном",
            photo_urls=["https://example.test/poster.jpg"],
        )
        special_campaign = PromoCampaign(title="80 stories motivation", status="active", starts_at=now, priority=1)
        fallback_campaign = PromoCampaign(title="all debug", status="active", starts_at=now, priority=2)
        session.add_all([event, special_campaign, fallback_campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(special_campaign)
        await session.refresh(fallback_campaign)
        special_activity = PromoActivity(
            campaign_id=int(special_campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "apply_rate": 0,
                "debug_marker": "#aeg_special",
                "debug_cleanup_before": False,
                "mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0},
                "cta_templates": {
                    "likes": ["Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."]
                },
            },
        )
        fallback_activity = PromoActivity(
            campaign_id=int(fallback_campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "apply_rate": 1,
                "debug_marker": "#aeg_all",
                "debug_cleanup_before": False,
                "palette_ids": ["prussian_cream"],
            },
        )
        session.add_all(
            [
                special_activity,
                fallback_activity,
                PromoTarget(
                    campaign_id=int(special_campaign.id),
                    target_type="festival",
                    festival_name="80 историй о главном",
                ),
                PromoTarget(campaign_id=int(fallback_campaign.id), target_type="all"),
            ]
        )
        await session.commit()
        await session.refresh(fallback_activity)

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": []}}
        return {"response": 1}

    async def fake_upload_images(images, *args, **kwargs):
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        return "photo-231920894_777"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        return "https://vk.com/wall-231920894_1002"

    async def fake_fetch_image(_url):
        return _poster_bytes()

    url = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="231920894",
        message="Обычный пост",
        photo_urls=event.photo_urls,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_fn=fake_upload_vk_photo,
        upload_images_fn=fake_upload_images,
        vk_api_fn=fake_vk_api,
        fetch_image_fn=fake_fetch_image,
        now_utc=now,
    )

    assert url == "https://vk.com/wall-231920894_1002"
    async with db.get_session() as session:
        rows = list((await session.execute(select(PromoExposure))).scalars().all())
    assert len(rows) == 1
    assert rows[0].activity_id == int(fallback_activity.id)
    assert rows[0].details_json["shadow_marker"] == "#aeg_all"


@pytest.mark.asyncio
async def test_shadow_debug_copy_falls_back_to_byte_vk_upload(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Концерт Анны Смирновой",
            description="",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            event_type="концерт",
            photo_urls=["https://example.test/poster.jpg"],
        )
        campaign = PromoCampaign(title="Мотивация", status="active", starts_at=now)
        session.add_all([event, campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(campaign)
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "apply_rate": 1,
                "debug_marker": "#aeg_test_shadow",
                "debug_cleanup_before": False,
                "palette_ids": ["prussian_cream"],
            },
        )
        target = PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id))
        session.add_all([activity, target])
        await session.commit()

    posted = {}

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": []}}
        return {"response": 1}

    async def fake_upload_images(images, *args, **kwargs):
        assert len(images) == 1
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        return None

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db_arg=None, bot_arg=None, **kwargs):
        assert group_id == "231920894"
        assert image_bytes.startswith(b"\x89PNG")
        assert kwargs["filename"].startswith("afishaengagement")
        assert kwargs["filename"].endswith(".png")
        return "photo-231920894_888"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["attachments"] = attachments
        return "https://vk.com/wall-231920894_1001"

    async def fake_fetch_image(_url):
        return _poster_bytes()

    url = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="231920894",
        message="Обычный пост",
        photo_urls=event.photo_urls,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_fn=fake_upload_vk_photo,
        upload_images_fn=fake_upload_images,
        upload_vk_photo_bytes_fn=fake_upload_vk_photo_bytes,
        vk_api_fn=fake_vk_api,
        fetch_image_fn=fake_fetch_image,
        now_utc=now,
    )

    assert url == "https://vk.com/wall-231920894_1001"
    assert posted["attachments"] == ["photo-231920894_888"]


@pytest.mark.asyncio
async def test_shadow_debug_copy_uses_next_free_vk_postponed_slot(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    base_ts = aeg._scheduled_shadow_ts({"debug_publish_delay_days": 3}, now)
    async with db.get_session() as session:
        event = Event(
            title="Мастер-класс по акварели",
            description="",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            event_type="мастер-класс",
            photo_urls=["https://example.test/poster.jpg"],
        )
        campaign = PromoCampaign(title="Мотивация", status="active", starts_at=now)
        session.add_all([event, campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(campaign)
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "debug_cleanup_before": False,
                "debug_publish_delay_days": 3,
                "debug_slot_spacing_minutes": 5,
                "apply_rate": 1,
                "debug_marker": "#aeg_test_shadow",
                "palette_ids": ["prussian_cream"],
            },
        )
        target = PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id))
        session.add_all([activity, target])
        await session.commit()

    posted = {}

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": [{"id": 10, "date": base_ts, "text": "regular postponed"}]}}
        return {"response": 1}

    async def fake_upload_images(images, *args, **kwargs):
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        return "photo-231920894_777"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["publish_date"] = kwargs["publish_date"]
        return "https://vk.com/wall-231920894_1000"

    async def fake_fetch_image(_url):
        return _poster_bytes()

    url = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="231920894",
        message="Обычный пост",
        photo_urls=event.photo_urls,
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_fn=fake_upload_vk_photo,
        upload_images_fn=fake_upload_images,
        vk_api_fn=fake_vk_api,
        fetch_image_fn=fake_fetch_image,
        now_utc=now,
    )

    assert url == "https://vk.com/wall-231920894_1000"
    assert posted["publish_date"] == base_ts + 300
    async with db.get_session() as session:
        row = (await session.execute(select(PromoExposure))).scalar_one()
    assert row.details_json["scheduled_ts"] == base_ts + 300


@pytest.mark.asyncio
async def test_shadow_debug_copy_skips_posts_without_images(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = Event(
        title="Без картинки",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
    )

    called = False

    async def fake_post_to_vk(*args, **kwargs):
        nonlocal called
        called = True
        return None

    result = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="1",
        message="",
        photo_urls=[],
        post_to_vk_fn=fake_post_to_vk,
        upload_vk_photo_fn=fake_post_to_vk,
        upload_images_fn=fake_post_to_vk,
        vk_api_fn=fake_post_to_vk,
    )

    assert result is None
    assert called is False


@pytest.mark.asyncio
async def test_shadow_debug_copy_dedupes_existing_build_tag(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Лекция",
            description="",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            photo_urls=["https://example.test/poster.jpg"],
        )
        campaign = PromoCampaign(title="Мотивация", status="active", starts_at=now)
        session.add_all([event, campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(campaign)
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={"debug_shadow": True, "apply_rate": 1, "debug_marker": "#aeg_test_shadow"},
        )
        target = PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id))
        session.add_all([activity, target])
        await session.commit()
        await session.refresh(activity)
        session.add(
            PromoExposure(
                campaign_id=int(campaign.id),
                activity_id=int(activity.id),
                event_id=int(event.id),
                surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
                placement_kind="vk_shadow_debug",
                publish_status="VK_SCHEDULED_DEBUG",
                details_json={"shadow_marker": "#aeg_test_shadow", "build_tag": "#aeg_b20260609"},
            )
        )
        await session.commit()

    called = False

    async def fake_call(*args, **kwargs):
        nonlocal called
        called = True
        return None

    result = await aeg.maybe_publish_shadow_debug_copy(
        event=event,
        db=db,
        bot=None,
        target_group_id="231920894",
        message="",
        photo_urls=event.photo_urls,
        post_to_vk_fn=fake_call,
        upload_vk_photo_fn=fake_call,
        upload_images_fn=fake_call,
        vk_api_fn=fake_call,
        now_utc=now,
    )

    assert result is None
    assert called is False


@pytest.mark.asyncio
async def test_cleanup_debug_posts_deletes_only_marker_matches():
    calls = []

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        calls.append((method, params))
        if method == "wall.get":
            return {
                "response": {
                    "items": [
                        {"id": 10, "text": "regular"},
                        {"id": 11, "text": "debug #afishaengagement_shadow"},
                    ]
                }
            }
        return {"response": 1}

    result = await aeg.cleanup_debug_posts(
        group_id="231920894",
        marker="#afishaengagement_shadow",
        vk_api_fn=fake_vk_api,
    )

    assert result == {"matched": 1, "deleted": 1, "errors": 0}
    assert calls[-1] == ("wall.delete", {"owner_id": -231920894, "post_id": 11})
