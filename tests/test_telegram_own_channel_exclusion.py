import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import Database
from models import TelegramSource
from source_parsing.telegram.commands import add_source
from source_parsing.telegram.handlers import process_telegram_results
from source_parsing.telegram.service import _build_config_payload
from telegram_sources import is_excluded_tg_source


@pytest.mark.asyncio
@pytest.mark.parametrize("username", ["@KLDEVENTS", "https://telegram.me/s/kldevents/6237", "kenigevents"])
async def test_own_channel_cannot_be_added(username):
    assert is_excluded_tg_source(username)
    message = SimpleNamespace(answer=AsyncMock())
    await add_source(None, message, username)
    assert "исключён" in message.answer.call_args.args[0]
    assert not is_excluded_tg_source("agropark39")


@pytest.mark.asyncio
async def test_enabled_own_channel_is_excluded_from_normal_and_targeted_monitor(tmp_path):
    db = Database(str(tmp_path / "config.sqlite"))
    try:
        await db.init()
        async with db.get_session() as session:
            session.add(TelegramSource(username="kldevents", enabled=True))
            session.add(TelegramSource(username="kenigevents", enabled=True))
            session.add(TelegramSource(username="normalcontrol", enabled=True))
            await session.commit()
        for requested in (None, ["kldevents"], ["@KLDEVENTS", "normalcontrol"]):
            payload = await _build_config_payload(db, source_usernames=requested)
            assert not {"kldevents", "kenigevents"}.intersection(payload["channels"])
            assert not any(is_excluded_tg_source(s["username"]) for s in payload["sources"])
            if requested != ["kldevents"]:
                assert "normalcontrol" in payload["channels"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stale_monitor_results_cannot_register_own_channel_or_import_events(tmp_path):
    db = Database(str(tmp_path / "import.sqlite"))
    try:
        await db.init()
        path = tmp_path / "results.json"
        path.write_text(json.dumps({
            "run_id": "own-channel-stale-result",
            "sources_meta": [{"username": "kldevents", "title": "Own output"}],
            "messages": [{"source_username": "@KLDEVENTS", "message_id": 6237,
                          "text": "26 декабря 15:00", "events": [{"title": "Дед Мороз"}]}],
        }))
        report = await process_telegram_results(path, db)
        assert report.messages_skipped == 1
        assert report.events_created == report.events_merged == 0
        async with db.raw_conn() as conn:
            assert (await (await conn.execute(
                "SELECT count(*) FROM telegram_source WHERE username='kldevents'"
            )).fetchone())[0] == 0
            assert (await (await conn.execute("SELECT count(*) FROM joboutbox")).fetchone())[0] == 0
    finally:
        await db.close()
