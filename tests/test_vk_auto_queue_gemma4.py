from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import geo_region
import main
import vk_auto_queue
import vk_intake
from db import Database
from poster_media import PosterMedia


class DummyBot:
    async def send_message(self, *_args, **_kwargs):
        return None

    async def get_me(self):
        return SimpleNamespace(username="eventsbotTestBot")


@pytest.mark.asyncio
async def test_vk_auto_queue_routes_draft_parse_to_gemma4_31b(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (1, "club1", "VK Source", None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "Анонс события 31 декабря в 19:00.",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    captured: dict[str, object] = {}

    async def fake_build_event_drafts(*_args, **kwargs):
        captured["parse_gemma_model"] = kwargs.get("parse_gemma_model")
        return [], None

    monkeypatch.delenv("VK_AUTO_IMPORT_PARSE_GEMMA_MODEL", raising=False)
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build_event_drafts)

    await vk_auto_queue.run_vk_auto_import(db, DummyBot(), chat_id=1, limit=1, operator_id=123)

    assert captured["parse_gemma_model"] == "models/gemma-4-31b-it"


@pytest.mark.asyncio
async def test_vk_auto_queue_parse_model_override_is_scoped_to_auto_queue(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (1, "club1", "VK Source", None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "Анонс события 31 декабря в 19:00.",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    captured: dict[str, object] = {}

    async def fake_build_event_drafts(*_args, **kwargs):
        captured["parse_gemma_model"] = kwargs.get("parse_gemma_model")
        return [], None

    monkeypatch.setenv("VK_AUTO_IMPORT_PARSE_GEMMA_MODEL", "models/gemma-4-26b-a4b-it")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build_event_drafts)

    await vk_auto_queue.run_vk_auto_import(db, DummyBot(), chat_id=1, limit=1, operator_id=123)

    assert captured["parse_gemma_model"] == "models/gemma-4-26b-a4b-it"


def test_vk_auto_queue_parse_model_blank_env_falls_back_to_31b(monkeypatch):
    monkeypatch.setenv("VK_AUTO_IMPORT_PARSE_GEMMA_MODEL", "   ")

    assert vk_auto_queue._vk_auto_parse_gemma_model() == "models/gemma-4-31b-it"


@pytest.mark.asyncio
async def test_vk_intake_parse_model_flows_to_event_parse_gemma(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_parse_event_via_llm(*_args, **kwargs):
        captured["gemma_model"] = kwargs.get("gemma_model")

        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "Событие",
                    "date": "2099-12-31",
                    "location_name": "Калининград",
                    "short_description": "Тестовое событие для проверки маршрутизации модели.",
                }
            ]
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        "Анонс события 31 декабря.",
        parse_gemma_model="models/gemma-4-31b-it",
    )

    assert drafts
    assert captured["gemma_model"] == "models/gemma-4-31b-it"


@pytest.mark.asyncio
async def test_vk_intake_prompt_treats_room_floor_as_non_venue(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_parse_event_via_llm(prompt_text, **kwargs):
        captured["prompt_text"] = prompt_text

        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "КардиоШкола",
                    "date": "2099-06-17",
                    "time": "18:30",
                    "location_name": "Научная библиотека",
                    "location_address": "Мира 9",
                    "city": "Калининград",
                    "short_description": "Встреча о профилактике заболеваний.",
                }
            ]
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        "Приглашаем на встречу научной библиотеки.\n📍 лекционный зал, 4 этаж",
        source_name="Калининградская областная научная библиотека",
    )

    assert drafts
    prompt = str(captured["prompt_text"])
    assert "Room/floor is not venue" in prompt
    assert "source_location/location hint" in prompt
    assert "location_name=Научная библиотека" in prompt


@pytest.mark.asyncio
async def test_vk_intake_prefers_exact_poster_datetime_over_relative_caption(monkeypatch):
    async def fake_parse_event_via_llm(*_args, **_kwargs):
        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "Встреча с Константином Бандуриным",
                    "date": "2026-06-18",
                    "time": "",
                    "location_name": "Музей Мирового океана",
                    "location_address": "наб. Петра Великого 1",
                    "city": "Калининград",
                    "event_type": "встреча",
                    "short_description": "Встреча о Большой Африканской экспедиции.",
                }
            ]
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)

    poster = PosterMedia(
        data=b"",
        name="poster.jpg",
        ocr_text=(
            "ЛЕКТОРИЙ «ОКЕАНиЯ»\n"
            "19 ИЮНЯ 16:00\n"
            "ПРЕДВАРИТЕЛЬНЫЕ ИТОГИ БОЛЬШОЙ АФРИКАНСКОЙ ЭКСПЕДИЦИИ\n"
            "Константин Бандурин – заместитель директора, руководитель Атлантического филиала ВНИРО\n"
            "ОБРАЗОВАТЕЛЬНЫЙ ЦЕНТР «ОКЕАНиЯ»\n"
            "наб. Петра Великого, 1Б"
        ),
        ocr_title="ЛЕКТОРИЙ «ОКЕАНиЯ»",
    )

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        (
            "Уже скоро мы узнаем какие открытия и результаты принесла масштабная Африканская экспедиция.\n\n"
            "В этот четверг приглашаем на встречу с Константином Бандуриным."
        ),
        source_name="Музей Мирового океана",
        publish_ts=datetime(2026, 6, 13, 10, 0, tzinfo=timezone.utc),
        poster_media=[poster],
    )

    assert len(drafts) == 1
    assert drafts[0].date == "2026-06-19"
    assert drafts[0].time == "16:00"
    assert drafts[0].venue == "ОБРАЗОВАТЕЛЬНЫЙ ЦЕНТР «ОКЕАНиЯ»"
    assert drafts[0].location_address == "наб. Петра Великого, 1Б"


@pytest.mark.asyncio
async def test_vk_intake_keeps_source_grounded_llm_title_instead_of_poster_or_generic_fallback(monkeypatch):
    async def fake_parse_event_via_llm(*_args, **_kwargs):
        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "💿 Виниловый вечер с DJ Switchoff",
                    "date": "2026-06-19",
                    "time": "21:00",
                    "location_name": "Бар Советов",
                    "location_address": "проспект Мира 118",
                    "city": "Калининград",
                    "event_type": "концерт",
                    "is_free": True,
                }
            ]
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)

    poster = PosterMedia(
        data=b"",
        name="poster.jpg",
        ocr_text="СОВЕТСКАЯ ЭЛЕКТРОНИКА\n19/06 21:00\nБАР SOVETOV\nDJ SWITCHOFF\nVINYL ONLY",
        ocr_title="СОВЕТСКАЯ ЭЛЕКТРОНИКА",
    )

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        (
            "В пятницу в “Баре Советов” слушаем винил, прокачиваем музыкальную эрудицию "
            "с DJ Switchoff.\n\n"
            "Путешествие по галактикам электронной музыки от композиторского авангарда "
            "и эмбиента, через легкую музыку космической эры к ритмичным диско, "
            "фанку и джазу.\n\n"
            "Звучание уникальных синтезаторов АНС и Синти-100, кастомных "
            "электроакустических гибридов Мещерина, импортных Moog и ARP, "
            "электроорганов и синт-аккордеонов экспериментальных радиозаводов СССР "
            "наполнит ваш незабываемый вечер неожиданными аудио впечатлениями!\n\n"
            "19 июня в 21:00\nБАР SOVETOV, проспект Мира 118\nВход свободный"
        ),
        source_name="Бар Советов",
        publish_ts=datetime(2026, 6, 18, 10, 0, tzinfo=timezone.utc),
        poster_media=[poster],
    )

    assert len(drafts) == 1
    assert drafts[0].title == "💿 Виниловый вечер с DJ Switchoff"
    assert drafts[0].title != "Концерт — Бар Советов"
    assert drafts[0].title != "СОВЕТСКАЯ ЭЛЕКТРОНИКА"


@pytest.mark.asyncio
async def test_vk_intake_resolves_clck_ticket_link_before_publication(monkeypatch):
    async def fake_parse_event_via_llm(*_args, **_kwargs):
        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "Акция «Набережная кораблей»",
                    "date": "2026-06-18",
                    "end_date": "2026-07-04",
                    "location_name": "Музей Мирового океана",
                    "location_address": "наб. Петра Великого 1",
                    "city": "Калининград",
                    "ticket_link": "https://clck.ru/3UEVYF",
                    "is_free": True,
                }
            ]
        )

    async def fake_resolve(url):
        assert url == "https://clck.ru/3UEVYF"
        return "https://world-ocean.ru/posetitelyam/vremya-raboty"

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)
    monkeypatch.setattr(vk_intake, "_resolve_external_short_ticket_link", fake_resolve)

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        "Оформить билет можно по ссылке: https://clck.ru/3UEVYF",
        source_name="Музей Мирового океана",
    )

    assert len(drafts) == 1
    assert drafts[0].links == ["https://world-ocean.ru/posetitelyam/vremya-raboty"]


@pytest.mark.asyncio
async def test_vk_intake_does_not_overwrite_explicit_text_date_with_poster(monkeypatch):
    async def fake_parse_event_via_llm(*_args, **_kwargs):
        class Parsed(list):
            festival = None

        return Parsed(
            [
                {
                    "title": "Встреча с Константином Бандуриным",
                    "date": "2026-06-18",
                    "time": "",
                    "location_name": "Музей Мирового океана",
                    "location_address": "наб. Петра Великого 1",
                    "city": "Калининград",
                    "event_type": "встреча",
                }
            ]
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse_event_via_llm)

    poster = PosterMedia(
        data=b"",
        name="poster.jpg",
        ocr_text=(
            "ЛЕКТОРИЙ «ОКЕАНиЯ»\n"
            "19 ИЮНЯ 16:00\n"
            "ОБРАЗОВАТЕЛЬНЫЙ ЦЕНТР «ОКЕАНиЯ»\n"
            "наб. Петра Великого, 1Б"
        ),
    )

    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        "18 июня приглашаем на встречу с Константином Бандуриным.",
        source_name="Музей Мирового океана",
        publish_ts=datetime(2026, 6, 13, 10, 0, tzinfo=timezone.utc),
        poster_media=[poster],
    )

    assert len(drafts) == 1
    assert drafts[0].date == "2026-06-18"
    assert drafts[0].time == ""
    assert drafts[0].venue == "Музей Мирового океана"


def test_vk_intake_prompt_mentions_poster_datetime_conflict_rule():
    source = vk_intake.build_event_drafts_from_vk.__code__.co_consts
    joined = "\n".join(str(item) for item in source if isinstance(item, str))
    assert "в этот четверг" in joined
    assert "OCR афиши более точным" in joined
    assert "не считай афишу автоматически сильнее" in joined
    assert "читальный зал" in joined
    assert "Room/floor is not venue" in joined


@pytest.mark.asyncio
async def test_event_parse_gemma_model_extra_overrides_global_env(monkeypatch):
    captured: dict[str, object] = {}

    class FakeClient:
        async def generate_content_async(self, *, model, prompt, generation_config, max_output_tokens):
            captured["model"] = model
            captured["generation_config"] = generation_config
            return "[]", SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2)

    async def noop_log(*_args, **_kwargs):
        return None

    monkeypatch.setenv("EVENT_PARSE_GEMMA_MODEL", "gemma-3-27b-it")
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop_log)

    parsed = await main._parse_event_via_gemma(
        "Нет событий.",
        gemma_model="models/gemma-4-31b-it",
    )

    assert list(parsed) == []
    assert captured["model"] == "models/gemma-4-31b-it"


@pytest.mark.asyncio
async def test_event_parse_gemma4_output_budget_fits_shared_15k_tpm(monkeypatch):
    captured: dict[str, object] = {}

    class FakeClient:
        DEFAULT_TPM_RESERVE_EXTRA = 1000

        @staticmethod
        def _estimate_prompt_tokens(_prompt):
            # Production replay 11019 was approximately this size after the
            # festival hint registry and poster OCR were included.
            return 10134

        async def generate_content_async(
            self, *, model, prompt, generation_config, max_output_tokens
        ):
            captured["model"] = model
            captured["max_output_tokens"] = max_output_tokens
            return "[]", SimpleNamespace(
                input_tokens=1, output_tokens=1, total_tokens=2
            )

    async def noop_log(*_args, **_kwargs):
        return None

    monkeypatch.delenv("EVENT_PARSE_GEMMA_TPM_RESERVATION_TARGET", raising=False)
    monkeypatch.delenv("EVENT_PARSE_GEMMA_MIN_OUTPUT_TOKENS", raising=False)
    monkeypatch.setenv("EVENT_PARSE_GEMMA_MAX_TOKENS", "4000")
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop_log)

    parsed = await main._parse_event_via_gemma(
        "Одно событие.",
        gemma_model="models/gemma-4-31b-it",
    )

    assert list(parsed) == []
    assert captured["model"] == "models/gemma-4-31b-it"
    assert captured["max_output_tokens"] == 3366
    assert 10134 + 1000 + int(captured["max_output_tokens"]) == 14500


@pytest.mark.asyncio
async def test_event_parse_gemma4_omits_venue_catalog_when_full_prompt_cannot_fit(monkeypatch):
    captured: dict[str, object] = {}

    class FakeClient:
        DEFAULT_TPM_RESERVE_EXTRA = 1000

        @staticmethod
        def _estimate_prompt_tokens(prompt):
            return 21743 if "\nKnown venues:\n" in prompt else 9000

        async def generate_content_async(
            self, *, model, prompt, generation_config, max_output_tokens
        ):
            captured["prompt"] = prompt
            captured["max_output_tokens"] = max_output_tokens
            return "[]", SimpleNamespace(
                input_tokens=1, output_tokens=1, total_tokens=2
            )

    async def noop_log(*_args, **_kwargs):
        return None

    monkeypatch.delenv("EVENT_PARSE_GEMMA_TPM_RESERVATION_TARGET", raising=False)
    monkeypatch.delenv("EVENT_PARSE_GEMMA_MIN_OUTPUT_TOKENS", raising=False)
    monkeypatch.setenv("EVENT_PARSE_GEMMA_MAX_TOKENS", "4000")
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop_log)

    parsed = await main._parse_event_via_gemma(
        "Завтра состоится встреча в библиотеке.",
        poster_texts=["1 августа, 11:00"],
        gemma_model="models/gemma-4-31b-it",
    )

    assert list(parsed) == []
    prompt = str(captured["prompt"])
    assert "\nKnown venues:\n" not in prompt
    assert "venue / city grounding rule" in prompt
    assert "Poster OCR:" in prompt
    assert captured["max_output_tokens"] == 4000


@pytest.mark.asyncio
async def test_event_parse_gemma4_fails_before_provider_when_compact_prompt_too_large(monkeypatch):
    called = False

    class FakeClient:
        DEFAULT_TPM_RESERVE_EXTRA = 1000

        @staticmethod
        def _estimate_prompt_tokens(_prompt):
            return 12000

        async def generate_content_async(self, **_kwargs):
            nonlocal called
            called = True
            return "[]", SimpleNamespace()

    monkeypatch.delenv("EVENT_PARSE_GEMMA_TPM_RESERVATION_TARGET", raising=False)
    monkeypatch.delenv("EVENT_PARSE_GEMMA_MIN_OUTPUT_TOKENS", raising=False)
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())

    with pytest.raises(RuntimeError, match="exceeds TPM cap"):
        await main._parse_event_via_gemma(
            "Очень большой вход.",
            gemma_model="models/gemma-4-31b-it",
        )

    assert called is False


def test_event_parse_output_budget_does_not_rewrite_non_gemma_models():
    class FakeClient:
        DEFAULT_TPM_RESERVE_EXTRA = 1000

        @staticmethod
        def _estimate_prompt_tokens(_prompt):
            return 200000

    assert (
        main._fit_event_parse_gemma_output_budget(
            FakeClient(),
            model="gemini-3.1-flash-lite",
            prompt="large",
            configured_max_tokens=4000,
        )
        == 4000
    )


@pytest.mark.asyncio
async def test_event_parse_gemma_default_is_gemma4_without_implicit_4o_fallback(monkeypatch):
    captured: dict[str, object] = {"models": [], "four_o_called": False}

    class FakeClient:
        async def generate_content_async(self, *, model, prompt, generation_config, max_output_tokens):
            captured["models"].append(model)
            return "not json", SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2)

    async def noop_log(*_args, **_kwargs):
        return None

    async def fake_4o(*_args, **_kwargs):
        captured["four_o_called"] = True
        return []

    monkeypatch.delenv("EVENT_PARSE_GEMMA_MODEL", raising=False)
    monkeypatch.delenv("EVENT_PARSE_ENABLE_4O_FALLBACK", raising=False)
    monkeypatch.setenv("FOUR_O_TOKEN", "test-token")
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop_log)
    monkeypatch.setattr(main, "_parse_event_via_4o", fake_4o)

    with pytest.raises(RuntimeError, match="bad gemma parse response"):
        await main._parse_event_via_gemma("Некорректный ответ модели.")

    assert captured["models"] == ["gemma-4-31b-it", "gemma-4-31b-it"]
    assert captured["four_o_called"] is False


@pytest.mark.asyncio
async def test_geo_region_llm_fallback_defaults_to_gemma4(monkeypatch):
    captured: dict[str, object] = {}

    class FakeClient:
        async def generate_content_async(self, *, model, prompt, generation_config, max_output_tokens):
            captured["model"] = model
            return '{"is_kaliningrad_oblast": true, "region_name": "Калининградская область", "confidence": 0.98}', SimpleNamespace()

    monkeypatch.delenv("GEO_REGION_GEMMA_MODEL", raising=False)

    decision = await geo_region._gemma_region_fallback(
        city="Калининград",
        gemma_client=FakeClient(),
    )

    assert captured["model"] == "gemma-4-31b-it"
    assert decision.allowed is True
