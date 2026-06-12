from __future__ import annotations

import io
import importlib.util
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from PIL import Image
from sqlalchemy import select

import afishaengagement as aeg
import main
from db import Database
from models import Event, PromoActivity, PromoCampaign, PromoExposure, PromoTarget


def _load_cleanup_script_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "cleanup_afishaengagement_debug_vk.py"
    spec = importlib.util.spec_from_file_location("cleanup_afishaengagement_debug_vk", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def test_holiday_festival_field_does_not_generate_festival_cta():
    event = Event(
        title="Мастер-класс по работе Н. Карякина «На причале»",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Музей",
        source_text="Мероприятия ко Дню России.",
        event_type="мастер-класс",
        festival="День России",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"holiday-not-festival-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 0, "reposts": 100}},
        ).cta_text
        for idx in range(60)
    }

    assert all("фестивал" not in text.casefold() for text in seen)
    assert all("День России" not in text for text in seen)


def test_day_russia_explicit_festival_type_uses_holiday_cta():
    event = Event(
        title="Празднование Дня России",
        description="Масштабное мероприятие посвящено Дню России. Гостей ждёт концерт и мастер-классы.",
        date="2026-06-12",
        time="12:00",
        location_name="Верхнее озеро",
        source_text="Приглашаем на масштабное празднование Дня России.",
        event_type="фестиваль",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"day-russia-not-festival-{idx}",
            config={"mechanic_weights": {"comments": 50, "likes": 25, "reposts": 25}},
        ).cta_text
        for idx in range(80)
    }

    assert aeg._event_type_key(event) == "holiday"
    assert all("фестивал" not in text.casefold() for text in seen)
    assert any("праздник" in text.casefold() or "праздничн" in text.casefold() for text in seen)


def test_volunteer_event_gets_volunteer_cta_copy():
    event = Event(
        title="пойдем гулять",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Добро.Центр",
        source_text="Следи за добрыми новостями.",
        event_type="встреча",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"volunteer-copy-{idx}",
            config={"mechanic_weights": {"comments": 100, "likes": 100, "reposts": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert all("волонт" in text.casefold() or "доброволь" in text.casefold() for text in seen)


def test_exhibition_rejects_family_fairy_copy():
    assert aeg._event_type_key(Event(title="Выставка «Адмирал маринистики»", event_type="выставка")) == "exhibition"
    assert aeg._cta_text_has_forbidden_copy(
        "Поделись, что тебя вдохновляет в сказочных героях!",
        "exhibition",
    )
    assert aeg._cta_text_has_forbidden_copy(
        "Поделись с друзьями, если ждёшь фестиваля «Кантата» в этом году.",
        "festival",
    )
    assert aeg._cta_text_has_forbidden_copy("Лайк, если ждёте фестиваль.", "festival")
    assert aeg._cta_text_has_forbidden_copy("Напишите, кто ждёт фестиваль.", "festival")
    assert aeg._cta_text_has_forbidden_copy("Присоединяйся к празднику в Баре Советов.", "holiday")


def test_party_event_uses_party_tone_not_concert_copy():
    event = Event(
        title="Кото пати в Грецком",
        description="",
        date="2026-06-20",
        time="21:00",
        location_name="Грецкий",
        source_text="Вечеринка, disco, funk, new wave, танцы под винил.",
        event_type="вечеринка",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"party-copy-{idx}",
            config={"mechanic_weights": {"comments": 100, "likes": 100, "reposts": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert any("тусов" in text.casefold() or "вечерин" in text.casefold() for text in seen)
    assert all("концерт" not in text.casefold() for text in seen)


def test_like_cta_does_not_promise_more_events_from_likes():
    event = Event(
        title="Мастер-класс по акварели",
        description="",
        date="2026-06-20",
        time="12:00",
        location_name="Мастерская",
        source_text="",
        event_type="мастер-класс",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"no-more-events-like-copy-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0}},
        ).cta_text.casefold()
        for idx in range(100)
    }

    assert all("хочешь чаще" not in text for text in seen)
    assert all("видеть чаще" not in text for text in seen)
    assert all("почаще" not in text for text in seen)
    assert all("чаще провод" not in text for text in seen)
    assert all("нужны такие" not in text for text in seen)
    assert all("больше таких" not in text for text in seen)


def test_family_like_copy_does_not_imply_event_demand():
    event = Event(
        title="Встреча со сказочными героями",
        description="Семейная программа для детей и родителей.",
        date="2026-06-20",
        time="12:00",
        location_name="Дом культуры",
        source_text="",
        event_type="семейное",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"family-like-demand-copy-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0}},
        ).cta_text.casefold()
        for idx in range(100)
    }

    assert all("нужны такие" not in text for text in seen)
    assert all("больше таких" not in text for text in seen)


def test_text_fit_keeps_word_boundaries():
    fit = aeg.fit_text(
        "Поставь лайк, если любишь такие мастер-классы.",
        box_width=520,
        box_height=360,
        preferred_px=72,
        min_px=52,
    )

    assert fit is not None
    assert fit.font_px >= 52
    assert " ".join(fit.lines) == "Поставь лайк, если любишь такие мастер-классы."
    assert all("- " not in line for line in fit.lines)


def test_text_fit_can_break_hyphenated_masterclass_for_larger_type():
    fit = aeg.fit_text(
        "Поставь лайк, если любишь такие мастер-классы.",
        box_width=170,
        box_height=420,
        preferred_px=42,
        min_px=24,
        max_lines=8,
        allow_hyphen_break=True,
    )

    assert fit is not None
    assert any(line == "мастер-" for line in fit.lines)
    assert any(line == "классы." for line in fit.lines)


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
    assert rendered.dimensions[0] - 420 <= 420 // 2
    assert rendered.cta_text_font_px >= 24
    assert rendered.data.startswith(b"\x89PNG")
    assert len(rendered.data) > 50_000
    with Image.open(io.BytesIO(rendered.data)) as image:
        assert image.size == rendered.dimensions


def test_render_right_extension_avoids_orphan_service_word_lines():
    plan = aeg.EngagementPlan(
        mechanic="reposts",
        template_id="right_extension",
        palette_id="deep_wine_ivory",
        cta_text="Поделись с друзьями, если ждёшь фестиваля «Кантата» в этом году.",
        hook_text=None,
        event_type="festival",
        has_persona=False,
        has_festival=True,
        seed="right-extension-orphan-lines",
    )

    rendered = aeg.render_right_extension(_poster_bytes(), plan)

    assert rendered.dimensions[0] - 420 <= 420 // 2
    assert not aeg._has_orphan_cta_line(rendered.cta_text_lines)


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
    assert rendered.dimensions[1] - 450 <= rendered.dimensions[1] // 3
    assert rendered.dimensions[1] - 450 <= rendered.dimensions[1] // 3
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
    assert "Поделись с подругой, которая любит такие концерты." in seen
    assert all("такие афиши" not in text for text in seen)


def test_generated_cta_stays_on_vk_social_action_not_attendance():
    event = Event(
        title="Семейный праздник",
        description="Детская программа и мастер-классы",
        date="2026-06-20",
        time="12:00",
        location_name="Парк",
        source_text="",
        event_type="семейное",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"social-action-only-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 40, "reposts": 20}},
        ).cta_text
        for idx in range(160)
    }

    assert all(not aeg._cta_text_has_forbidden_copy(text, "family") for text in seen)
    assert all("куда сходить" not in text.casefold() for text in seen)
    assert all("планиру" not in text.casefold() for text in seen)


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
    assert all("от таких концертах" not in text.casefold() for text in seen)
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


def test_deterministic_theme_copy_uses_safe_ready_templates():
    event = Event(
        title="Русская музыка на органе",
        description="Органный концерт в кафедральном соборе.",
        date="2026-06-20",
        time="19:00",
        location_name="Кафедральный собор",
        source_text="В программе орган и камерный оркестр.",
        event_type="концерт",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"theme-comment-copy-{idx}",
            config={"mechanic_weights": {"comments": 100}},
        ).cta_text
        for idx in range(300)
    }

    assert all("из темы" not in text.casefold() for text in seen)
    assert all("«органную музыку»" not in text.casefold() for text in seen)
    assert any("за что любите органную музыку" in text.casefold() for text in seen)


def test_theme_like_copy_does_not_use_bad_neuter_template():
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
            seed=f"theme-like-grammar-{idx}",
            config={"mechanic_weights": {"likes": 100}},
        ).cta_text
        for idx in range(300)
    }

    assert all("симфоническую музыку — твоё" not in text.casefold() for text in seen)
    assert any("Отметь лайком, если выбираешь симфоническую музыку." == text for text in seen)


@pytest.mark.asyncio
async def test_llm_cta_text_rewrites_risky_theme_comment(monkeypatch):
    event = Event(
        id=501,
        title="Русская музыка на органе",
        description="Органный концерт в кафедральном соборе.",
        date="2026-06-20",
        time="19:00",
        location_name="Кафедральный собор",
        source_text="В программе орган и камерный оркестр.",
        event_type="концерт",
    )
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="hook_swipe_cta",
        palette_id="black_lime",
        cta_text="Расскажите, что из темы «органную музыку» вам ближе всего.",
        hook_text="Кто уже был на таких концертах?",
        event_type="concert",
        has_persona=False,
        has_festival=False,
        seed="llm-text-rewrite",
    )

    async def fake_ask_4o(*args, **kwargs):
        return '{"cta_text":"Что вам ближе в органной музыке? Напишите в комментариях.","hook_text":"Кто уже слушал орган?"}'

    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    rewritten, _elapsed, provider = await aeg.build_llm_cta_text(
        event,
        plan=plan,
        config={},
        vision=aeg.PosterVisionSummary(provider="test", confidence=0.7, text="органный концерт"),
    )

    assert provider == "llm_text"
    assert rewritten.cta_text == "Что вам ближе в органной музыке? Напишите в комментариях."
    assert "из темы" not in rewritten.cta_text.casefold()


@pytest.mark.asyncio
async def test_llm_cta_text_rejects_forbidden_copy(monkeypatch):
    event = Event(title="Кинопоказ", event_type="кинопоказ")
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="right_extension",
        palette_id="black_lime",
        cta_text="Напишите в комментариях, что ждёте от кинопоказа.",
        hook_text=None,
        event_type="cinema",
        has_persona=False,
        has_festival=False,
        seed="llm-text-reject",
    )

    async def fake_ask_4o(*args, **kwargs):
        return '{"cta_text":"Какой спектакль ждёте больше всего?","hook_text":""}'

    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    rewritten, _elapsed, provider = await aeg.build_llm_cta_text(
        event,
        plan=plan,
        config={"llm_text_mode": "always"},
        vision=None,
    )

    assert provider == "fallback_text_invalid"
    assert rewritten.cta_text == plan.cta_text


@pytest.mark.asyncio
async def test_llm_cta_text_rejects_join_the_event_copy(monkeypatch):
    event = Event(
        title="Вечеринка в Баре Советов",
        event_type="вечеринка",
        location_name="Бар Советов",
        description="Праздничная программа в баре.",
    )
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="right_extension",
        palette_id="black_lime",
        cta_text="Какой трек делает праздник вашим? Напишите.",
        hook_text=None,
        event_type="party",
        has_persona=False,
        has_festival=False,
        seed="llm-text-reject-join",
    )

    async def fake_ask_4o(*args, **kwargs):
        return '{"cta_text":"Присоединяйся к празднику в Баре Советов.","hook_text":""}'

    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    rewritten, _elapsed, provider = await aeg.build_llm_cta_text(
        event,
        plan=plan,
        config={"llm_text_mode": "always"},
        vision=None,
    )

    assert provider == "fallback_text_invalid"
    assert rewritten.cta_text == plan.cta_text


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


def test_configured_registration_cta_is_rejected_as_attendance_copy():
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
    assert "зарегистр" not in plan.cta_text.casefold()
    assert not aeg._cta_text_has_forbidden_copy(plan.cta_text, plan.event_type)


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

    assert "зарегистр" not in plan.cta_text.casefold()
    assert not aeg._cta_text_has_forbidden_copy(plan.cta_text, plan.event_type)


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

    assert "Что в программе фестиваля Кантата в этом году вам ближе? Напишите." in seen
    assert all("от Кантата" not in text for text in seen)
    assert all("жд" not in text.casefold() or "фестивал" not in text.casefold() for text in seen)


def test_festival_cta_uses_short_display_name_without_duplicate_festival_word():
    event = Event(
        title="Творческая встреча",
        description="Образовательная программа фестиваля.",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="Фестиваль «Кантата» проходит каждый год.",
        event_type="встреча",
        festival="Фестиваль классической музыки «Кантата»",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"festival-short-name-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 0, "reposts": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert "Поделись с теми, кому интересна образовательная программа фестиваля Кантата." in seen
    assert "Поделись с теми, кто следит за образовательной программой фестиваля Кантата." in seen
    assert all("фестиваля Фестиваль" not in text for text in seen)


def test_festival_umbrella_cta_stays_but_names_project_topic():
    event = Event(
        title="80 историй о главном: встреча про послевоенный Калининград",
        description="Лекция проекта о людях и местах Калининградской области.",
        date="2026-06-20",
        time="19:00",
        location_name="Библиотека",
        source_text="",
        event_type="лекция",
        festival="80 историй о главном",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"80-stories-festival-umbrella-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 50, "reposts": 50}},
        ).cta_text
        for idx in range(120)
    }
    hook = aeg.build_engagement_plan(event, seed="80-stories-hook", config={"formats": ["hook_swipe_cta"]}).hook_text

    assert "Поставь лайк, если интересны истории Калининградской области." in seen
    assert "Поделись с теми, кто следит за проектом «80 историй о главном»." in seen
    assert all("ждёшь фестиваля" not in text.casefold() for text in seen)
    assert hook == "Кому близки истории Калининградской области?"


def test_zoo_excursion_does_not_get_lecture_copy_even_when_stored_as_lecture():
    event = Event(
        title="Экскурсия «Другой зоопарк»: кормокухня и ветеринарный уход",
        description="Участники узнают, как устроена работа зоологов и уход за животными.",
        date="2026-06-20",
        time="12:00",
        location_name="Калининградский зоопарк",
        source_text="",
        event_type="лекция",
        festival="Другой зоопарк",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"zoo-excursion-copy-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 40, "reposts": 20}},
        ).cta_text
        for idx in range(160)
    }
    hook = aeg.build_engagement_plan(event, seed="zoo-excursion-hook", config={"formats": ["hook_swipe_cta"]}).hook_text

    assert aeg._event_type_key(event) == "excursion"
    assert all("лекц" not in text.casefold() for text in seen)
    assert any("зоопарк изнутри" in text.casefold() or "экскурс" in text.casefold() for text in seen)
    assert hook == "Кому интересен зоопарк изнутри?"


def test_theatre_backstage_excursion_does_not_invent_zoo_copy():
    event = Event(
        title="Женитьба и экскурсия «Закулисье театра»",
        description="Атмосфера театрального закулисья открывается зрителям после спектакля.",
        date="2026-08-09",
        time="18:00",
        location_name="Калининградский драматический театр",
        source_text="Август уже в продаже. Экскурсия «Закулисье театра».",
        event_type="экскурсия",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"theatre-backstage-no-zoo-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 40, "reposts": 20}},
        ).cta_text
        for idx in range(180)
    }
    hook = aeg.build_engagement_plan(event, seed="theatre-backstage-hook", config={"formats": ["hook_swipe_cta"]}).hook_text

    assert aeg._event_type_key(event) == "excursion"
    assert aeg._extract_theme(event, "excursion") == "закулисье"
    assert all("зоопарк" not in text.casefold() for text in seen)
    assert all("зоолог" not in text.casefold() for text in seen)
    assert all("животн" not in text.casefold() for text in seen)
    assert "зоопарк" not in hook.casefold()


def test_llm_plan_rejects_zoo_copy_without_zoo_context():
    event = Event(
        title="Женитьба и экскурсия «Закулисье театра»",
        description="Атмосфера театрального закулисья открывается зрителям после спектакля.",
        date="2026-08-09",
        time="18:00",
        location_name="Калининградский драматический театр",
        source_text="Экскурсия «Закулисье театра».",
        event_type="экскурсия",
    )

    assert (
        aeg._sanitize_llm_plan(
            event,
            seed="llm-zoo-copy-rejected",
            config={},
            vision=None,
            payload={
                "mechanic": "likes",
                "template_id": "right_extension",
                "palette_id": "midnight_gold",
                "cta_text": "Поставь лайк, если любишь закулисье зоопарка.",
                "hook_text": "Кому интересен зоопарк изнутри?",
            },
        )
        is None
    )


def test_theatre_title_overrides_misstored_cinema_type():
    event = Event(
        title="Спектакль «Гараж»",
        description="Сатирическая история о собрании гаражно-строительного кооператива на сцене драмтеатра.",
        date="2026-08-01",
        time="18:00",
        location_name="Калининградский драматический театр",
        source_text="Спектакли драматического театра. Основная сцена.",
        event_type="кинопоказ",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"garage-theatre-not-cinema-{idx}",
            config={"mechanic_weights": {"comments": 50, "likes": 50, "reposts": 0}},
        ).cta_text
        for idx in range(120)
    }

    assert aeg._event_type_key(event) == "theatre"
    assert all("кинопоказ" not in text.casefold() for text in seen)
    assert any("спектак" in text.casefold() or "актёр" in text.casefold() for text in seen)


def test_recycling_collection_overrides_misstored_market_type():
    event = Event(
        title="Приём шин",
        description="Бесплатный приём отработанных шин от физических лиц на переработку.",
        date="2026-06-20",
        time="08:00",
        location_name="Правая набережная, 25",
        source_text="ЭКОИЮНЬ: бесплатный прием отработанных шин.",
        event_type="ярмарка",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"recycling-not-market-{idx}",
            config={"mechanic_weights": {"comments": 40, "likes": 40, "reposts": 20}},
        ).cta_text
        for idx in range(120)
    }

    assert aeg._event_type_key(event) == "other"
    assert all("ярмарк" not in text.casefold() for text in seen)
    assert all("маркет" not in text.casefold() for text in seen)


def test_family_market_uses_soft_mom_friend_repost_copy():
    event = Event(
        title="Путешествие в сказку в деревне Холмогорье",
        description="Семейная ярмарка, детская программа, аниматоры и сказочные герои.",
        date="2026-06-13",
        time="11:00",
        location_name="Холмогорье",
        source_text="",
        event_type="ярмарка",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"family-market-repost-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 0, "reposts": 100}},
        ).cta_text
        for idx in range(160)
    }

    assert aeg._event_type_key(event) == "family"
    assert any("подругой-мамой" in text.casefold() or "мамой-подругой" in text.casefold() for text in seen)
    assert all(
        "мам" in text.casefold() or "родител" in text.casefold() or "дет" in text.casefold()
        for text in seen
    )
    assert "Поделись с подругой, которая любит такие ярмарки." not in seen


def test_kantata_education_umbrella_uses_program_context():
    event = Event(
        title="Образовательная программа VI Международного фестиваля классической музыки Кантата",
        description="Диалоги, лекции и кинопоказы проходят в рамках образовательной программы.",
        date="2026-06-13",
        time="12:00",
        location_name="Филиал Третьяковской галереи",
        source_text="",
        event_type="фестиваль",
        festival="Кантата",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"kantata-education-context-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 50, "reposts": 50}},
        ).cta_text
        for idx in range(160)
    }
    hook = aeg.build_engagement_plan(event, seed="kantata-education-hook", config={"formats": ["hook_swipe_cta"]}).hook_text

    assert "Поставь лайк, если интересна образовательная программа фестиваля Кантата." in seen
    assert "Поделись с теми, кто следит за образовательной программой фестиваля Кантата." in seen
    assert all("кому может быть интересно" not in text.casefold() for text in seen)
    assert hook == "Кому близка образовательная программа Кантаты?"


def test_ongoing_festival_cta_does_not_wait_for_festival():
    event = Event(
        title="Образовательная программа фестиваля Кантата",
        description="Фестиваль уже идет: лекции, диалоги и кинопоказы проходят всю неделю.",
        date="2026-06-13",
        time="12:00",
        location_name="Филиал Третьяковской галереи",
        source_text="",
        event_type="фестиваль",
        festival="Кантата",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"ongoing-festival-no-wait-{idx}",
            config={"mechanic_weights": {"comments": 50, "likes": 25, "reposts": 25}},
        ).cta_text
        for idx in range(180)
    }

    assert "Что в образовательной программе фестиваля Кантата вам ближе? Напишите." in seen
    assert all("ждёшь фестивал" not in text.casefold() for text in seen)
    assert all("ждешь фестивал" not in text.casefold() for text in seen)
    assert all("ждёте от фестивал" not in text.casefold() for text in seen)
    assert all("ждете от фестивал" not in text.casefold() for text in seen)


def test_idea_cta_uses_explicit_event_concept_without_inventing_theme():
    event = Event(
        title="AIST FEST",
        description="Фестиваль музыки на воде: концерты, лекции, мастер-классы народного творчества и маркет.",
        date="2026-06-20",
        time="14:00",
        location_name="Парк",
        source_text="",
        event_type="концерт",
        festival="AIST FEST",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"aist-idea-cta-{idx}",
            config={"mechanic_weights": {"likes": 100}},
        ).cta_text
        for idx in range(180)
    }

    assert aeg._extract_theme(event, "concert") is None
    assert aeg._extract_idea_phrase(event, "concert") == "фестиваля музыки на воде"
    assert "Поставь лайк, если нравится идея фестиваля музыки на воде." in seen
    assert all("народную музыку" not in text.casefold() for text in seen)


def test_kantata_creative_meeting_uses_meeting_copy_not_instrumental_persona():
    event = Event(
        title="Творческая встреча с Евгением Князевым",
        description="Встреча проходит в рамках образовательной программы фестиваля Кантата.",
        date="2026-06-16",
        time="18:00",
        location_name="Филиал Третьяковской галереи",
        source_text="",
        event_type="встреча",
        festival="Кантата",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"kantata-meeting-copy-{idx}",
            config={"mechanic_weights": {"comments": 30, "likes": 40, "reposts": 30}},
        ).cta_text
        for idx in range(180)
    }

    assert aeg._event_type_key(event) == "meeting"
    assert aeg._extract_persona(event) is None
    assert all("Евгением" not in text for text in seen)
    assert all("спектак" not in text.casefold() for text in seen)
    assert any("встреч" in text.casefold() for text in seen)


def test_broad_festival_does_not_overread_folk_music_from_craft_program():
    event = Event(
        title="AIST FEST",
        description="Большой фестиваль: концерты, лекции, мастер-классы народного творчества и маркет.",
        date="2026-06-20",
        time="14:00",
        location_name="Парк",
        source_text="",
        event_type="концерт",
        festival="AIST FEST",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"aist-no-folk-overread-{idx}",
            config={"mechanic_weights": {"likes": 100}},
        ).cta_text
        for idx in range(160)
    }

    assert aeg._extract_theme(event, "concert") is None
    assert all("народную музыку" not in text.casefold() for text in seen)


def test_repost_templates_avoid_stilted_pereshli_copy():
    event = Event(
        title="Городская лекция",
        description="",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="лекция",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"repost-copy-{idx}",
            config={"mechanic_weights": {"comments": 0, "likes": 0, "reposts": 100}},
        ).cta_text
        for idx in range(80)
    }

    assert all("Перешли" not in text for text in seen)
    assert any(text.startswith("Поделись") for text in seen)


def test_comments_badge_is_action_phrase():
    assert aeg.MECHANIC_BADGES["comments"] == "НАПИШИ КОММЕНТАРИЙ"


def test_badge_icons_match_action_mechanics():
    assert aeg._badge_trailing_icon("likes") == "heart"
    assert aeg._badge_trailing_icon("comments") == "down_arrow"
    assert aeg._badge_trailing_icon("reposts") == "right_arrow"


def test_diagonal_shadow_offset_follows_seam_normal():
    right_offset = aeg._diagonal_shadow_offset(
        (790, 0),
        (772, 900),
        toward=(-1.0, 0.0),
        distance=18,
    )
    bottom_offset = aeg._diagonal_shadow_offset(
        (0, 520),
        (900, 480),
        toward=(0.0, -1.0),
        distance=18,
    )

    assert right_offset[0] < 0
    assert abs(right_offset[0]) >= 17
    assert bottom_offset[1] < 0
    assert bottom_offset[0] < 0


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


def test_hook_swipe_preserves_full_horizontal_poster_edges():
    poster = Image.new("RGB", (800, 450), (30, 30, 30))
    for x in range(0, 90):
        for y in range(poster.height):
            poster.putpixel((x, y), (240, 20, 30))
    for x in range(710, 800):
        for y in range(poster.height):
            poster.putpixel((x, y), (20, 80, 240))
    out = io.BytesIO()
    poster.save(out, format="PNG")
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="hook_swipe_cta",
        palette_id="midnight_gold",
        cta_text="Были на таких событиях? Напишите.",
        hook_text="Кто уже был?",
        event_type="other",
        has_persona=False,
        has_festival=False,
        seed="hook-preserve-poster-edges",
    )

    hook = aeg.render_plan_images(out.getvalue(), plan)[0]
    rendered = Image.open(io.BytesIO(hook.data)).convert("RGB")
    band_mid_y = int(1350 * 0.58 / 2)

    assert rendered.getpixel((18, band_mid_y))[0] > 180
    assert rendered.getpixel((1062, band_mid_y))[2] > 180


def test_hook_swipe_is_rare_when_formats_are_mixed():
    formats = ["right_extension", "bottom_overlay", "bottom_extension", "hook_swipe_cta"]
    selected = [
        aeg._select_template_id(formats, {}, f"format-weight-{idx}")
        for idx in range(500)
    ]

    assert selected.count("hook_swipe_cta") < 90
    assert aeg._select_template_id(["hook_swipe_cta"], {}, "single-format") == "hook_swipe_cta"


def test_cinema_club_comment_cta_can_name_westside_movie():
    event = Event(
        title="Киноклуб Westside Movie: специальный показ",
        description="Westside Movie приглашает на авторский кинопоказ.",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="кинопоказ",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"westside-movie-{idx}",
            config={"mechanic_weights": {"comments": 100}},
        ).cta_text
        for idx in range(120)
    }

    assert any("Westside Movie" in text and "показах" in text for text in seen)


def test_concert_persona_can_come_from_artist_creative_phrase():
    event = Event(
        title="Вечер музыки Элвиса Пресли",
        description="Разговор и концерт о творчестве Элвиса Пресли.",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )

    seen = {
        aeg.build_engagement_plan(
            event,
            seed=f"elvis-persona-{idx}",
            config={"mechanic_weights": {"likes": 100}},
        ).cta_text
        for idx in range(120)
    }

    assert aeg._extract_persona(event) == "Элвиса Пресли"
    assert any("творчество Элвиса Пресли" in text for text in seen)


@pytest.mark.asyncio
async def test_generic_comment_question_is_rewritten_to_event_specific(monkeypatch):
    event = Event(
        title="Краны и стаканы",
        description="Выставка о промышленном дизайне и городской среде.",
        date="2026-06-20",
        time="19:00",
        location_name="Галерея",
        source_text="",
        event_type="выставка",
    )
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="right_extension",
        palette_id="midnight_gold",
        cta_text="Что цепляет вас в таких выставках? Поделитесь.",
        hook_text="Кто любит выставки?",
        event_type="exhibition",
        has_persona=False,
        has_festival=False,
        seed="generic-comment-rewrite",
    )

    async def fake_ask_4o(prompt, **kwargs):
        assert "конкретный вопрос" in prompt
        return '{"cta_text":"Что в выставке «Краны и стаканы» цепляет сильнее?","hook_text":"Кому близок промышленный дизайн?"}'

    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)

    assert aeg._should_run_llm_text(event, plan, {}) is True
    rewritten, _elapsed, provider = await aeg.build_llm_cta_text(
        event,
        plan=plan,
        config={},
        vision=aeg.PosterVisionSummary(provider="test", confidence=0.8),
    )

    assert provider == "llm_text"
    assert rewritten.cta_text == "Что в выставке «Краны и стаканы» цепляет сильнее?"


def test_render_for_publish_retries_with_safe_right_extension(monkeypatch):
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="bottom_extension",
        palette_id="black_lime",
        cta_text="Очень длинный текст, который не влезает в выбранный шаблон",
        hook_text=None,
        event_type="other",
        has_persona=False,
        has_festival=False,
        seed="render-safe-fallback",
    )
    calls = []

    def fake_render(_source_image, current_plan):
        calls.append(current_plan)
        if len(calls) == 1:
            raise ValueError("text_overflow")
        assert current_plan.template_id == "right_extension"
        assert current_plan.cta_text == "Напишите в комментариях, что ждёте от события."
        return [
            aeg.RenderedImage(
                data=b"image",
                filename="fallback.png",
                template_id="right_extension",
                palette_id="black_lime",
                cta_text_lines=[current_plan.cta_text],
                cta_text_font_px=42,
                dimensions=(1200, 900),
                render_ms=1,
            )
        ]

    monkeypatch.setattr(aeg, "render_plan_images", fake_render)

    rendered, final_plan, reason = aeg.render_plan_images_for_publish(b"poster", plan)

    assert len(rendered) == 1
    assert final_plan.template_id == "right_extension"
    assert final_plan.cta_text == "Напишите в комментариях, что ждёте от события."
    assert reason and "text_overflow" in reason


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


def test_modern_palette_bank_has_accessible_roles():
    modern_ids = {
        "future_dusk_lime",
        "transform_teal_persimmon",
        "mocha_aqua_ivory",
        "espresso_sky_cherry",
        "butter_ink_cherry",
        "cool_blue_jade_plum",
        "thermal_cobalt_tomato",
        "ink_fuchsia_mint",
        "sage_black_lilac",
        "oxide_blue_citron",
    }

    assert modern_ids <= set(aeg.PALETTES)
    for palette_id in modern_ids:
        roles = aeg._palette_roles(aeg.PALETTES[palette_id])
        surface = aeg._hex_to_rgb(roles["surface"])
        ink = aeg._hex_to_rgb(roles["ink"])
        signal = aeg._hex_to_rgb(roles["signal"])
        signal_ink = aeg._hex_to_rgb(roles["signal_ink"])

        assert aeg._contrast_ratio(surface, ink) >= 4.5, palette_id
        assert aeg._contrast_ratio(signal, signal_ink) >= 3.0, palette_id


def test_yellow_violet_no_longer_beats_modern_editorial_palettes_for_lecture():
    poster = Image.new("RGB", (900, 1200), (12, 18, 26))
    profile = aeg._poster_color_profile(
        poster,
        region_box=aeg._seam_region_box("right_extension", 900, 1200),
    )
    yellow = aeg._score_palette(
        palette_id="yellow_violet",
        palette=aeg.PALETTES["yellow_violet"],
        profile=profile,
        event_type="lecture",
        seed="yellow-editorial-penalty",
        preferred_id="yellow_violet",
    )
    modern = [
        aeg._score_palette(
            palette_id=palette_id,
            palette=aeg.PALETTES[palette_id],
            profile=profile,
            event_type="lecture",
            seed="yellow-editorial-penalty",
            preferred_id="yellow_violet",
        )
        for palette_id in ("sage_black_lilac", "mocha_aqua_ivory", "smoky_jade_terracotta")
    ]
    modern = [score for score in modern if score is not None]

    assert yellow is not None
    assert modern
    assert max(score.score for score in modern) > yellow.score


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


def test_horizontal_bottom_overlay_promotes_to_bottom_extension():
    event = Event(
        title="Кинохиты. От Баха до Морриконе",
        description="Концертная программа",
        date="2026-06-20",
        time="19:00",
        location_name="Зал",
        source_text="",
        event_type="концерт",
    )
    plan = aeg.build_engagement_plan(
        event,
        seed="horizontal-bottom-overlay-promote",
        config={
            "formats": ["bottom_overlay"],
            "palette_ids": ["clay_cobalt_noir"],
            "mechanic_weights": {"reposts": 100},
        },
    )

    rendered = aeg.render_plan_images(_horizontal_poster_bytes(), plan)[0]

    assert rendered.template_id == "bottom_extension"
    assert rendered.dimensions[0] == 800
    assert rendered.dimensions[1] > 450


def test_horizontal_right_extension_plan_promotes_to_bottom_extension():
    plan = aeg.EngagementPlan(
        mechanic="comments",
        template_id="right_extension",
        palette_id="deep_wine_ivory",
        cta_text="Напишите в комментариях, что ждёте от концерта.",
        hook_text="Кто уже был на таких концертах?",
        event_type="concert",
        has_persona=False,
        has_festival=False,
        seed="horizontal-right-promote",
    )

    rendered = aeg.render_plan_images(_horizontal_poster_bytes(), plan)[0]

    assert rendered.template_id == "bottom_extension"
    assert rendered.dimensions[0] == 800
    assert rendered.dimensions[1] > 450
    assert rendered.dimensions[1] - 450 <= rendered.dimensions[1] // 3


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
    assert rendered.dimensions[1] - 450 <= rendered.dimensions[1] // 3
    assert rendered.dimensions[1] <= int(rendered.dimensions[0] * aeg.MAX_VK_FEED_PHOTO_ASPECT)


def test_horizontal_bottom_extension_overflow_retries_safe_bottom(monkeypatch):
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

    original_bottom_extension = aeg._render_bottom_extension
    calls = []

    def fake_bottom_extension(*args, **kwargs):
        calls.append(args[1])
        if len(calls) == 1:
            raise ValueError("text_overflow")
        return original_bottom_extension(*args, **kwargs)

    monkeypatch.setattr(aeg, "_render_bottom_extension", fake_bottom_extension)

    rendered, final_plan, reason = aeg.render_plan_images_for_publish(_horizontal_poster_bytes(), plan)

    assert reason and reason.startswith("safe_bottom_extension_after_")
    assert final_plan.template_id == "bottom_extension"
    assert final_plan.cta_text == aeg._safe_generic_cta(plan.event_type, plan.mechanic)
    assert rendered[0].template_id == "bottom_extension"
    assert rendered[0].dimensions[0] == 800


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
            source_post_url="https://vk.com/wall-1_2",
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
    assert rows[0].details_json["event_title"] == "Лекция с Иваном Петровым"
    assert rows[0].details_json["event_type"] == "lecture"
    assert rows[0].details_json["stored_event_type"] == "лекция"
    assert rows[0].details_json["source_post_url"] == "https://vk.com/wall-1_2"
    assert rows[0].details_json["source_first_photo_url"] == "https://example.test/poster.jpg"
    assert rows[0].details_json["source_photo_urls_count"] == 1
    assert rows[0].details_json["activity_surface"] == aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT
    assert rows[0].details_json["activity_profile_key"] == ""
    assert rows[0].details_json["cta_text"]


@pytest.mark.asyncio
async def test_public_engagement_copy_schedules_without_debug_marker(tmp_path):
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
            source_post_url="https://vk.com/wall-1_2",
            photo_urls=["https://example.test/poster.jpg"],
        )
        campaign = PromoCampaign(title="Мотивация public", status="active", starts_at=now)
        session.add_all([event, campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(campaign)
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "publish_mode": "public",
                "apply_rate": 1,
                "palette_ids": ["prussian_cream"],
            },
        )
        target = PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id))
        session.add_all([activity, target])
        await session.commit()

    posted = {}

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        raise AssertionError("public mode should not call shadow cleanup/slot lookup")

    async def fake_upload_images(images, *args, **kwargs):
        assert len(images) == 1
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        return "photo-231920894_777"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        posted["message"] = message
        posted["attachments"] = attachments
        posted["kwargs"] = kwargs
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
    assert posted["attachments"] == ["photo-231920894_777"]
    assert posted["kwargs"]["carousel"] is True
    assert posted["kwargs"]["publish_date"] is None
    assert "AFISHAENGAGEMENT DEBUG COPY" not in posted["message"]
    assert "#afishaengagement" not in posted["message"]

    async with db.get_session() as session:
        rows = list((await session.execute(select(PromoExposure))).scalars().all())
    assert len(rows) == 1
    assert rows[0].surface == aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT
    assert rows[0].publish_status == "VK_SCHEDULED"
    assert rows[0].placement_kind == "vk_engagement"
    assert rows[0].public_targets_json == [{"type": "vk_wall", "url": "https://vk.com/wall-231920894_1000"}]
    assert rows[0].details_json["publish_mode"] == "public"
    assert rows[0].details_json["debug_shadow"] is False
    assert "shadow_marker" not in rows[0].details_json


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
async def test_public_engagement_dice_miss_falls_through_to_shadow(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 9, 12, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Концерт",
            description="",
            date="2026-06-20",
            time="19:00",
            location_name="Зал",
            source_text="",
            event_type="концерт",
            photo_urls=["https://example.test/poster.jpg"],
        )
        public_campaign = PromoCampaign(title="public 10", status="active", starts_at=now, priority=0)
        shadow_campaign = PromoCampaign(title="shadow fallback", status="active", starts_at=now, priority=9)
        session.add_all([event, public_campaign, shadow_campaign])
        await session.commit()
        await session.refresh(event)
        await session.refresh(public_campaign)
        await session.refresh(shadow_campaign)
        public_activity = PromoActivity(
            campaign_id=int(public_campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "publish_mode": "public",
                "apply_rate": 0,
                "palette_ids": ["prussian_cream"],
            },
        )
        shadow_activity = PromoActivity(
            campaign_id=int(shadow_campaign.id),
            surface=aeg.PROMO_SURFACE_AFISHA_ENGAGEMENT,
            enabled=True,
            config_json={
                "debug_shadow": True,
                "apply_rate": 1,
                "debug_marker": "#aeg_shadow_fallback",
                "debug_cleanup_before": False,
                "palette_ids": ["prussian_cream"],
            },
        )
        session.add_all(
            [
                public_activity,
                shadow_activity,
                PromoTarget(campaign_id=int(public_campaign.id), target_type="all"),
                PromoTarget(campaign_id=int(shadow_campaign.id), target_type="all"),
            ]
        )
        await session.commit()
        await session.refresh(shadow_activity)

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": []}}
        return {"response": 1}

    async def fake_upload_images(images, *args, **kwargs):
        return ["https://storage.test/generated.png"], "ok"

    async def fake_upload_vk_photo(group_id, url, db_arg=None, bot_arg=None, **kwargs):
        return "photo-231920894_777"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, **kwargs):
        return "https://vk.com/wall-231920894_1003"

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

    assert url == "https://vk.com/wall-231920894_1003"
    async with db.get_session() as session:
        rows = list((await session.execute(select(PromoExposure))).scalars().all())
    assert len(rows) == 1
    assert rows[0].activity_id == int(shadow_activity.id)
    assert rows[0].publish_status == "VK_SCHEDULED_DEBUG"
    assert rows[0].details_json["publish_mode"] == "shadow"
    assert rows[0].details_json["shadow_marker"] == "#aeg_shadow_fallback"


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


def test_db_cleanup_selector_only_returns_stale_future_debug_rows(tmp_path):
    cleanup = _load_cleanup_script_module()
    db_path = tmp_path / "debug.sqlite"
    con = sqlite3.connect(db_path)
    con.execute(
        """
        CREATE TABLE promo_exposure (
            id INTEGER PRIMARY KEY,
            surface TEXT,
            publish_status TEXT,
            created_at TEXT,
            published_at TEXT,
            details_json TEXT,
            public_targets_json TEXT
        )
        """
    )

    def insert_row(
        row_id: int,
        *,
        surface: str = "afishaengagement",
        status: str = "VK_SCHEDULED_DEBUG",
        created_at: str,
        published_at: str = "2036-06-14 08:00:00+00:00",
        target_url: str = "",
    ) -> None:
        details = {"target_url": target_url, "cta_text": f"cta-{row_id}", "event_title": f"event-{row_id}"}
        targets = [{"type": "vk_wall_debug", "url": target_url}] if target_url else []
        con.execute(
            """
            INSERT INTO promo_exposure
                (id, surface, publish_status, created_at, published_at, details_json, public_targets_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (row_id, surface, status, created_at, published_at, json.dumps(details), json.dumps(targets)),
        )

    insert_row(1, created_at="2026-06-11 08:59:00+00:00", target_url="https://vk.com/wall-231920894_1001")
    insert_row(2, created_at="2026-06-11 09:19:00+00:00", target_url="https://vk.com/wall-231920894_1002")
    insert_row(
        3,
        status="VK_DELETED_DEBUG",
        created_at="2026-06-11 08:59:00+00:00",
        target_url="https://vk.com/wall-231920894_1003",
    )
    insert_row(
        4,
        surface="vk_post",
        created_at="2026-06-11 08:59:00+00:00",
        target_url="https://vk.com/wall-231920894_1004",
    )
    insert_row(
        5,
        created_at="2026-06-11 08:59:00+00:00",
        published_at="2026-06-10 08:00:00+00:00",
        target_url="https://vk.com/wall-231920894_1005",
    )
    con.commit()
    con.close()

    rows = cleanup._load_stale_debug_rows(
        db_path=str(db_path),
        stale_before=datetime(2026, 6, 11, 9, 18, tzinfo=timezone.utc),
        include_all=False,
    )

    assert [row["exposure_id"] for row in rows] == [1]
    assert rows[0]["post_id"] == 1001
