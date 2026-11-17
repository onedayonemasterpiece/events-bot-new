from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telethon import errors

from guide_excursions import route_readers as readers


def channel(monkeypatch, messages=(), flood=False):
    import telethon
    import telethon.sessions

    resolve = NS(peer=NS(channel_id=198), chats=[NS(id=198, broadcast=True, access_hash=876)])
    client = AsyncMock(side_effect=[resolve, errors.FloodWaitError(None, capture=67)
                                    if flood else NS(messages=messages)])
    def new_client(*args, **kwargs):
        assert kwargs["request_retries"] == kwargs["flood_sleep_threshold"] == 0
        return client
    monkeypatch.setattr(telethon, "TelegramClient", new_client)
    monkeypatch.setattr(telethon.sessions, "StringSession", lambda value: object())
    monkeypatch.setattr(readers, "remote_telegram_auth_scope", lambda: "TELEGRAM_AUTH_BUNDLE_S22")
    monkeypatch.setattr(readers, "_parse_auth_bundle", lambda key: {"session": "fixture"})
    monkeypatch.setattr(readers, "_require_env_any", lambda *keys: "23")
    monkeypatch.setattr(readers, "raise_if_remote_telegram_session_busy", AsyncMock())
    return client


SOURCE = {"platform": "telegram", "username": "tour_calendar", "source_kind": "excursion_operator"}


@pytest.mark.asyncio
async def test_telegram_owned_originals_only(monkeypatch):
    msg = dict(id=2, post=True, peer_id=NS(channel_id=198), fwd_from=None, message="Описание",
               date=datetime(2026, 1, 5, tzinfo=timezone.utc))
    client = channel(monkeypatch, [NS(**msg), NS(**{**msg, "fwd_from": object()}),
                                   NS(**{**msg, "peer_id": NS(channel_id=77)})])
    posts = await readers.search_route_posts(SOURCE, "Мосты Преголи")
    assert posts == [{"url": "https://t.me/tour_calendar/2", "text": "Описание",
                      "post_date": "2026-01-05T00:00:00+00:00"}]
    assert client.await_count == 2
    assert client.await_args.args[0].limit == 6


@pytest.mark.asyncio
async def test_telegram_propagates_flood_without_retry(monkeypatch):
    client = channel(monkeypatch, flood=True)
    with pytest.raises(readers.RouteReadLimited) as exc:
        await readers.search_route_posts(SOURCE, "Мосты Преголи")
    assert exc.value.retry_after == 67
    assert client.await_count == 2
    client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_reregistered_username_and_aggregator_never_searched(monkeypatch):
    client = channel(monkeypatch)
    assert await readers.search_route_posts({**SOURCE, "source_kind": "aggregator"}, "Мосты Преголи") == []
    with pytest.raises(ValueError, match="identity has changed"):
        await readers.search_route_posts({**SOURCE, "flags_json": '{"avatar":{"peer_id":36}}'}, "Мосты Преголи")
    assert client.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("personal,owner", [(True, 54), (False, -54)])
async def test_vk_queries_only_registered_wall_and_originals(monkeypatch, personal, owner):
    monkeypatch.setenv("GUIDE_MONITORING_VK_TOKEN", "test")
    item = dict(id=11, owner_id=owner, from_id=owner, text="Экскурсия", date=1780000000)
    api = AsyncMock(return_value={"items": [item, {**item, "copy_history": [{}]},
                                           {**item, "from_id": 234}, {**item, "owner_id": -999}]})
    monkeypatch.setattr(readers, "_vk_api", api)
    posts = await readers.search_route_posts({**SOURCE, "platform": "vk", "flags_json": {
        "vk_personal_page": personal, "vk_id": 54}}, "Мосты Преголи")
    assert [p["url"] for p in posts] == [f"https://vk.com/wall{owner}_11"]
    assert api.await_count == 1
    assert api.await_args.args[1] == "wall.search"
    assert api.await_args.kwargs["owners_only"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status,payload,after", [(429, {}, "731"), (200, {"error": {"error_code": 6}}, None)])
async def test_vk_rate_limits_stop_immediately(status, payload, after):
    from unittest.mock import MagicMock

    session = MagicMock()
    response = NS(status=status, headers={"Retry-After": after}, json=AsyncMock(return_value=payload))
    session.post.return_value.__aenter__.return_value = response
    with pytest.raises(readers.RouteReadLimited) as exc:
        await readers._vk_api(session, "wall.search", "test")
    assert exc.value.retry_after == (731 if after else 3600)
    session.post.assert_called_once()


@pytest.mark.asyncio
async def test_telegram_does_not_open_contended_monitor_session(monkeypatch):
    client = channel(monkeypatch)
    guard = AsyncMock(side_effect=RuntimeError("busy"))
    monkeypatch.setattr(readers, "raise_if_remote_telegram_session_busy", guard)
    with pytest.raises(RuntimeError, match="busy"):
        await readers.search_route_posts(SOURCE, "Мосты Преголи")
    guard.assert_awaited_once_with(current_job_type="guide_monitoring",
                                   current_auth_scope="TELEGRAM_AUTH_BUNDLE_S22")
    client.connect.assert_not_awaited()
