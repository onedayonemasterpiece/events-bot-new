from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import main
import vk_auto_queue
import vk_intake
from db import Database


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
