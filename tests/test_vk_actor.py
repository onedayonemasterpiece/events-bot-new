import logging
import pytest
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import main


class DummyResp:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


@pytest.mark.asyncio
async def test_vk_actor_auto_fallback_and_circuit_breaker(monkeypatch):
    monkeypatch.setattr(main, "_vk_captcha_needed", False)
    monkeypatch.setattr(main, "VK_ACTOR_MODE", "auto")
    monkeypatch.setattr(main, "VK_TOKEN", "g")
    monkeypatch.setenv("VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "_vk_user_token_bad", None)
    monkeypatch.setattr(main, "BACKOFF_DELAYS", [0])
    main.vk_fallback_group_to_user_total = defaultdict(int)
    main.vk_group_blocked.clear()

    now = 0
    monkeypatch.setattr(main._time, "time", lambda: now)

    calls: list[str] = []
    attempt = 0

    async def fake_http_call(name, method, url, timeout, data, **kwargs):
        nonlocal attempt
        attempt += 1
        calls.append(data["access_token"])
        if attempt == 1:
            return DummyResp({"error": {"error_code": 15, "error_msg": "access denied"}})
        return DummyResp({"response": "ok"})

    monkeypatch.setattr(main, "http_call", fake_http_call)

    data = await main._vk_api("wall.post", {}, db=None, bot=None)
    assert data["response"] == "ok"
    assert calls == ["g", "u"]

    calls.clear()
    data = await main._vk_api("wall.post", {}, db=None, bot=None)
    assert data["response"] == "ok"
    assert calls == ["u"]

    now += main.VK_CB_TTL + 1
    calls.clear()
    data = await main._vk_api("wall.post", {}, db=None, bot=None)
    assert data["response"] == "ok"
    assert calls == ["g"]


@pytest.mark.asyncio
async def test_vk_actor_auto_no_fallback(monkeypatch, caplog):
    monkeypatch.setattr(main, "_vk_captcha_needed", False)
    monkeypatch.setattr(main, "VK_ACTOR_MODE", "auto")
    monkeypatch.setattr(main, "VK_TOKEN", "g")
    monkeypatch.setenv("VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "_vk_user_token_bad", None)
    monkeypatch.setattr(main, "BACKOFF_DELAYS", [0])
    main.vk_fallback_group_to_user_total = defaultdict(int)
    main.vk_group_blocked.clear()

    calls = []

    async def fake_http_call(name, method, url, timeout, data, **kwargs):
        calls.append(data["access_token"])
        return DummyResp({"error": {"error_code": 3, "error_msg": "bad"}})

    monkeypatch.setattr(main, "http_call", fake_http_call)

    caplog.set_level(logging.ERROR)
    with pytest.raises(main.VKAPIError) as e:
        await main._vk_api("wall.post", {}, db=None, bot=None)
    assert e.value.code == 3
    assert e.value.actor == "group"
    assert e.value.token == "<redacted>"
    assert calls == ["g"]
    assert main.vk_fallback_group_to_user_total["wall.post"] == 0
    assert any(
        "actor=group" in rec.getMessage() and "token=<redacted>" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_vk_actor_auto_fallbacks_blocked_user_shortlink_to_group(monkeypatch):
    monkeypatch.setattr(main, "_vk_captcha_needed", False)
    monkeypatch.setattr(main, "VK_ACTOR_MODE", "auto")
    monkeypatch.setattr(main, "VK_TOKEN", "g")
    monkeypatch.setenv("VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "_vk_user_token_bad", None)
    monkeypatch.setattr(main, "BACKOFF_DELAYS", [0])

    calls: list[str] = []

    async def fake_http_call(name, method, url, timeout, data, **kwargs):
        calls.append(data["access_token"])
        if data["access_token"] == "u":
            return DummyResp(
                {
                    "error": {
                        "error_code": 8,
                        "error_msg": "Invalid request: Application is blocked",
                    }
                }
            )
        return DummyResp({"response": {"short_url": "https://vk.cc/abcd"}})

    monkeypatch.setattr(main, "http_call", fake_http_call)

    data = await main._vk_api("utils.getShortLink", {"url": "https://example.com"}, db=None, bot=None)

    assert data["response"]["short_url"] == "https://vk.cc/abcd"
    assert calls == ["u", "g"]


@pytest.mark.asyncio
async def test_vk_actor_no_retry_edit_time_expired(monkeypatch):
    monkeypatch.setattr(main, "_vk_captcha_needed", False)
    monkeypatch.setattr(main, "VK_ACTOR_MODE", "group")
    monkeypatch.setattr(main, "VK_TOKEN", "g")
    monkeypatch.setattr(main, "_vk_user_token", lambda: None)
    monkeypatch.setattr(main, "BACKOFF_DELAYS", [0, 0, 0])

    attempts = 0

    async def fake_http_call(name, method, url, timeout, data, **kwargs):
        nonlocal attempts
        attempts += 1
        return DummyResp(
            {"error": {"error_code": 15, "error_msg": "Edit time expired"}}
        )

    monkeypatch.setattr(main, "http_call", fake_http_call)

    with pytest.raises(main.VKAPIError) as exc:
        await main._vk_api("wall.edit", {}, db=None, bot=None)

    assert exc.value.code == 15
    assert attempts == 1


def test_choose_vk_actor(monkeypatch):
    monkeypatch.setattr(main, "VK_MAIN_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "2")
    monkeypatch.setattr(main, "VK_TOKEN", "gm")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "ga")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "u")

    actors_main = main.choose_vk_actor(-1, "wall.post")
    assert [a.label for a in actors_main] == ["group:main", "user"]
    assert actors_main[0].token == "gm"

    actors_afisha = main.choose_vk_actor(-2, "wall.post")
    assert [a.label for a in actors_afisha] == ["group:afisha", "user"]
    assert actors_afisha[0].token == "ga"


@pytest.mark.asyncio
async def test_post_to_vk_group_actor_posts_from_group(monkeypatch):
    monkeypatch.setattr(main, "VK_MAIN_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "2")
    monkeypatch.setattr(main, "VK_TOKEN", "gm")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "ga")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "VK_POSTPONED_ENABLED", False)

    captured = {}

    async def fake_vk_api(
        method,
        params,
        db=None,
        bot=None,
        token=None,
        token_kind="group",
        skip_captcha=False,
    ):
        captured.update(
            {
                "method": method,
                "params": params,
                "token": token,
                "token_kind": token_kind,
                "skip_captcha": skip_captcha,
            }
        )
        return {"response": {"post_id": 123}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk("2", "hello")

    assert url == "https://vk.com/wall-2_123"
    assert captured["method"] == "wall.post"
    assert captured["token"] == "ga"
    assert captured["token_kind"] == "group"
    assert captured["skip_captcha"] is True
    assert captured["params"]["owner_id"] == "-2"
    assert captured["params"]["message"] == "hello"
    assert captured["params"]["from_group"] == 1
    assert captured["params"]["signed"] == 0


def test_vk_postponed_next_slot_uses_kaliningrad_morning_and_interval(monkeypatch):
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(main, "VK_POSTPONED_MIN_INTERVAL_SECONDS", 600)
    monkeypatch.setattr(main, "VK_POSTPONED_START_HOUR", 6)
    tz = ZoneInfo("Europe/Kaliningrad")

    early = datetime(2026, 5, 19, 5, 45, tzinfo=tz)
    assert main._vk_postponed_next_slot(early, None) == datetime(
        2026, 5, 19, 6, 0, tzinfo=tz
    )

    daytime = datetime(2026, 5, 19, 10, 0, tzinfo=tz)
    assert main._vk_postponed_next_slot(daytime, None) == datetime(
        2026, 5, 19, 10, 10, tzinfo=tz
    )

    latest = int(datetime(2026, 5, 19, 10, 30, tzinfo=tz).timestamp())
    assert main._vk_postponed_next_slot(daytime, latest) == datetime(
        2026, 5, 19, 10, 40, tzinfo=tz
    )

    late = int(datetime(2026, 5, 19, 23, 55, tzinfo=tz).timestamp())
    assert main._vk_postponed_next_slot(daytime, late) == datetime(
        2026, 5, 20, 6, 0, tzinfo=tz
    )


def test_vk_postponed_next_slot_uses_first_morning_gap_before_promo_anchors(monkeypatch):
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(main, "VK_POSTPONED_MIN_INTERVAL_SECONDS", 600)
    monkeypatch.setattr(main, "VK_POSTPONED_START_HOUR", 6)
    monkeypatch.setattr(main, "VK_POSTPONED_MAX_ANCHOR_AHEAD_SECONDS", 18 * 3600)
    tz = ZoneInfo("Europe/Kaliningrad")

    now = datetime(2026, 6, 13, 2, 9, tzinfo=tz)
    promo_anchors = [
        int(datetime(2026, 6, 13, 10, 40, tzinfo=tz).timestamp()),
        int(datetime(2026, 6, 13, 15, 20, tzinfo=tz).timestamp()),
    ]

    assert main._vk_postponed_next_slot(
        now,
        postponed_timestamps=promo_anchors,
    ) == datetime(2026, 6, 13, 6, 0, tzinfo=tz)


def test_vk_postponed_next_slot_steps_through_occupied_morning_slots(monkeypatch):
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(main, "VK_POSTPONED_MIN_INTERVAL_SECONDS", 600)
    monkeypatch.setattr(main, "VK_POSTPONED_START_HOUR", 6)
    tz = ZoneInfo("Europe/Kaliningrad")

    now = datetime(2026, 6, 13, 2, 9, tzinfo=tz)
    morning_anchors = [
        int(datetime(2026, 6, 13, 6, 0, tzinfo=tz).timestamp()),
        int(datetime(2026, 6, 13, 6, 10, tzinfo=tz).timestamp()),
        int(datetime(2026, 6, 13, 15, 20, tzinfo=tz).timestamp()),
    ]

    assert main._vk_postponed_next_slot(
        now,
        postponed_timestamps=morning_anchors,
    ) == datetime(2026, 6, 13, 6, 20, tzinfo=tz)


@pytest.mark.asyncio
async def test_post_to_vk_uses_postponed_publish_date(monkeypatch):
    monkeypatch.setattr(main, "VK_MAIN_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "2")
    monkeypatch.setattr(main, "VK_TOKEN", "gm")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "ga")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "VK_POSTPONED_ENABLED", True)
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(main, "VK_POSTPONED_MIN_INTERVAL_SECONDS", 600)
    monkeypatch.setattr(main, "VK_POSTPONED_START_HOUR", 6)
    main._vk_postponed_reserved_until_by_owner.clear()

    tz = ZoneInfo("Europe/Kaliningrad")
    expected = int(datetime(2026, 5, 19, 10, 40, tzinfo=tz).timestamp())
    captured_wall_post = {}
    calls = []

    async def fake_reserve(owner_id, actors, db, bot, *, now=None):
        assert owner_id == -2
        assert [actor.label for actor in actors] == ["group:afisha", "user"]
        return expected

    async def fake_vk_api(
        method,
        params,
        db=None,
        bot=None,
        token=None,
        token_kind="group",
        skip_captcha=False,
    ):
        calls.append(method)
        if method == "wall.post":
            captured_wall_post.update(
                {
                    "params": params,
                    "token": token,
                    "token_kind": token_kind,
                    "skip_captcha": skip_captcha,
                }
            )
            return {"response": {"post_id": 124}}
        if method == "wall.get":
            assert params["owner_id"] == "-2"
            assert params["filter"] == "postponed"
            return {"response": {"items": [{"id": 125, "postponed_id": 124, "date": expected}]}}
        raise AssertionError(method)

    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk("2", "hello")

    assert url == "https://vk.com/wall-2_125"
    assert calls == ["wall.post", "wall.get"]
    assert captured_wall_post["token"] == "ga"
    assert captured_wall_post["token_kind"] == "group"
    assert captured_wall_post["skip_captcha"] is True
    assert captured_wall_post["params"]["owner_id"] == "-2"
    assert captured_wall_post["params"]["from_group"] == 1
    assert captured_wall_post["params"]["signed"] == 0
    assert captured_wall_post["params"]["publish_date"] == expected


@pytest.mark.asyncio
async def test_post_to_vk_retries_postponed_id_resolution_with_user_actor(monkeypatch):
    monkeypatch.setattr(main, "VK_MAIN_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "2")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "ga")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "VK_POSTPONED_ENABLED", True)
    expected = int(datetime(2026, 5, 19, 10, 40, tzinfo=ZoneInfo("Europe/Kaliningrad")).timestamp())
    wall_get_calls = 0
    sleep_calls = []

    async def fake_reserve(owner_id, actors, db, bot, *, now=None):
        return expected

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    async def fake_vk_api(
        method,
        params,
        db=None,
        bot=None,
        token=None,
        token_kind="group",
        skip_captcha=False,
    ):
        nonlocal wall_get_calls
        if method == "wall.post":
            return {"response": {"post_id": 124}}
        if method == "wall.get":
            wall_get_calls += 1
            assert token == "u"
            assert token_kind == "user"
            if wall_get_calls == 1:
                return {"response": {"items": []}}
            return {"response": {"items": [{"id": 125, "postponed_id": 124, "date": expected}]}}
        raise AssertionError(method)

    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)
    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk("2", "hello")

    assert url == "https://vk.com/wall-2_125"
    assert wall_get_calls == 2
    assert sleep_calls == [0.8]


@pytest.mark.asyncio
async def test_fetch_vk_latest_postponed_prefers_user_actor(monkeypatch):
    monkeypatch.setattr(main, "VK_USER_TOKEN", "u")
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "Europe/Kaliningrad")
    tz = ZoneInfo("Europe/Kaliningrad")
    postponed_ts = int((datetime.now(tz) + timedelta(minutes=30)).timestamp())
    calls = []

    async def fake_vk_api(
        method,
        params,
        db=None,
        bot=None,
        token=None,
        token_kind="group",
        skip_captcha=False,
    ):
        calls.append((method, token, token_kind, skip_captcha))
        return {"response": {"items": [{"id": 1, "date": postponed_ts}]}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    latest = await main._fetch_vk_latest_postponed_ts(
        -2,
        [
            main.VkActor("group", "g", "group:afisha"),
            main.VkActor("user", None, "user"),
        ],
        db=None,
        bot=None,
    )

    assert latest == postponed_ts
    assert calls == [("wall.get", "u", "user", False)]
