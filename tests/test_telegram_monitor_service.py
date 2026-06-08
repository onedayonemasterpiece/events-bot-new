from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from db import Database
from models import TelegramSource
from source_parsing.telegram.handlers import TelegramMonitorEventInfo
from source_parsing.telegram.service import (
    _build_config_payload,
    _build_secrets_payload,
    _format_event_block,
)


@pytest.mark.asyncio
async def test_build_config_payload_can_scope_to_single_source(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(TelegramSource(username="kraftmarket39", enabled=True, trust_level="high"))
        session.add(TelegramSource(username="otherchannel", enabled=True, trust_level="low"))
        session.add(TelegramSource(username="disabled", enabled=False, trust_level="high"))
        await session.commit()

    payload = await _build_config_payload(
        db,
        run_id="single_source_test",
        source_usernames=["@kraftmarket39", "disabled"],
    )

    assert payload["channels"] == ["kraftmarket39"]
    assert [item["username"] for item in payload["sources"]] == ["kraftmarket39"]
    assert payload["requested_source_usernames"] == ["disabled", "kraftmarket39"]


def test_build_secrets_payload_includes_yandex_storage_env(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("GOOGLE_API_KEY3", "google")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("YC_SA_BOT_STORAGE", "access")
    monkeypatch.setenv("YC_SA_BOT_STORAGE_KEY", "secret")
    monkeypatch.setenv("YC_STORAGE_BUCKET", "kenigevents")
    monkeypatch.setenv("YC_STORAGE_ENDPOINT", "https://storage.yandexcloud.net")

    payload = json.loads(_build_secrets_payload())

    assert payload["YC_SA_BOT_STORAGE"] == "access"
    assert payload["YC_SA_BOT_STORAGE_KEY"] == "secret"
    assert payload["YC_STORAGE_BUCKET"] == "kenigevents"
    assert payload["YC_STORAGE_ENDPOINT"] == "https://storage.yandexcloud.net"


def test_format_event_block_shows_vk_and_tg_posts_line():
    event = TelegramMonitorEventInfo(
        event_id=42,
        title="Лекция",
        date="2026-06-20",
        time="18:00",
        source_link="https://t.me/source/1",
        telegraph_url="https://telegra.ph/event",
        ics_url="https://example.com/event.ics",
        log_cmd="/log 42",
        fact_stats=None,
        photo_count=1,
        added_posters=1,
        vk_post_url="https://vk.com/wall-231920894_2403",
    )
    ctx = SimpleNamespace(
        event_posts_by_event_id={
            42: SimpleNamespace(
                vk_post_url="https://vk.com/wall-231920894_2403",
                tg_post_url="https://t.me/c/3954607218/7",
            )
        },
        sources_by_event_id={},
        video_count_by_event_id={},
        ticket_queue_by_event_id={},
        festival_queue_by_source_url={},
        tz=None,
    )

    lines = _format_event_block("Созданные события", [event], icon="✅", ctx=ctx)
    text = "\n".join(lines)

    assert 'Посты: VK <a href="https://vk.com/wall-231920894_2403">пост</a>' in text
    assert 'TG <a href="https://t.me/c/3954607218/7">пост</a>' in text


def test_format_event_block_marks_deferred_tg_post_pending():
    event = TelegramMonitorEventInfo(
        event_id=43,
        title="Спектакль",
        date="2026-06-21",
        time="19:00",
        source_link="https://t.me/source/2",
        telegraph_url="https://telegra.ph/event2",
        ics_url=None,
        log_cmd="/log 43",
        fact_stats=None,
        photo_count=0,
        added_posters=0,
        vk_post_url="https://vk.com/wall-231920894_2404",
    )
    ctx = SimpleNamespace(
        event_posts_by_event_id={
            43: SimpleNamespace(
                vk_post_url="https://vk.com/wall-231920894_2404",
                tg_post_url=None,
            )
        },
        sources_by_event_id={},
        video_count_by_event_id={},
        ticket_queue_by_event_id={},
        festival_queue_by_source_url={},
        tz=None,
    )

    lines = _format_event_block("Созданные события", [event], icon="✅", ctx=ctx)
    text = "\n".join(lines)

    assert 'Посты: VK <a href="https://vk.com/wall-231920894_2404">пост</a> · TG ⏳' in text
