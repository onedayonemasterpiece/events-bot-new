from __future__ import annotations

import pytest

from video_announce.scenario import VideoAnnounceScenario
from video_announce import story_publish
from telegram_business import cache_business_connection


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_story_session_payload_includes_optional_source_channel_id(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "12345")
    monkeypatch.setenv("TG_API_HASH", "hash-123")
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_SESSION_ENV", "TELEGRAM_SESSION")
    monkeypatch.setenv("TELEGRAM_SESSION", "session-abc")
    monkeypatch.setenv("SOURCE_CHANNEL_ID", "-100987654321")
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV", raising=False)

    payload = story_publish._story_session_payload()

    assert payload["api_id"] == 12345
    assert payload["api_hash"] == "hash-123"
    assert payload["session"] == "session-abc"
    assert payload["source_channel_id"] == -100987654321


def test_popular_review_selection_params_enable_story_publish_with_repost_target():
    scenario = VideoAnnounceScenario(db=None, bot=None, chat_id=0, user_id=0)

    params = scenario._popular_review_selection_params()

    assert params["story_publish_enabled"] is True
    assert params["story_publish_mode"] == "video"
    assert params["story_upload_profile"] == "telegram_story_native_hevc_720p_v1"
    assert params["story_targets_override"] == [
        {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
        {"peer": "@lovekenig", "delay_seconds": 600, "mode": "repost_previous"},
        {"peer": "@loving_guide39", "delay_seconds": 600, "mode": "repost_previous"},
    ]


@pytest.mark.asyncio
async def test_build_story_publish_config_prefers_selection_override_targets(monkeypatch):
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_TARGETS_JSON", raising=False)

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params={
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_targets_override": [
                {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
                {"peer": "@lovekenig", "delay_seconds": 600, "mode": "repost_previous"},
                {"peer": "@loving_guide39", "delay_seconds": 600, "mode": "repost_previous"},
            ],
        },
        selected_event_dates=["2026-04-16"],
    )

    assert config is not None
    assert config["upload_profile"] is None
    assert config["targets"] == [
        {
            "peer": "@kenigevents",
            "label": "@kenigevents",
            "delay_seconds": 0,
            "mode": "upload",
        },
        {
            "peer": "@lovekenig",
            "label": "@lovekenig",
            "delay_seconds": 600,
            "mode": "repost_previous",
        },
        {
            "peer": "@loving_guide39",
            "label": "@loving_guide39",
            "delay_seconds": 600,
            "mode": "repost_previous",
        },
    ]


@pytest.mark.asyncio
async def test_build_story_publish_config_keeps_native_upload_profile(monkeypatch):
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params={
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_upload_profile": "telegram_story_native_hevc_720p_v1",
            "story_targets_override": [
                {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
            ],
        },
        selected_event_dates=["2026-04-16"],
    )

    assert config is not None
    assert config["upload_profile"] == "telegram_story_native_hevc_720p_v1"


@pytest.mark.asyncio
async def test_build_story_publish_config_appends_encrypted_business_targets(
    monkeypatch,
    tmp_path,
):
    target = tmp_path / "business.enc.json"
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS", raising=False)
    monkeypatch.setenv("TELEGRAM_BUSINESS_CONNECTIONS_FILE", str(target))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "7910015203:test-token")
    cache_business_connection(
        Obj(
            id="biz-connection-secret",
            user=Obj(id=123456789, username="story_owner_fixture"),
            user_chat_id=987654321,
            date=1777194243,
            is_enabled=True,
            rights=Obj(can_manage_stories=True),
            can_reply=True,
        )
    )

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params={
            "mode": "popular_review",
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_targets_override": [
                {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
                {"peer": "@lovekenig", "delay_seconds": 600, "mode": "repost_previous"},
            ],
            "story_business_targets": ["@story_owner_fixture"],
        },
        selected_event_dates=["2026-04-16"],
    )

    assert config is not None
    business_target = config["targets"][-1]
    assert business_target["transport"] == "telegram_business"
    assert business_target["delay_seconds"] == 600
    assert business_target["label"].startswith("business:")
    serialized = str(config)
    assert "biz-connection-secret" not in serialized
    assert "story_owner_fixture" not in serialized


@pytest.mark.asyncio
async def test_business_story_targets_are_cherryflash_scoped(monkeypatch, tmp_path):
    target = tmp_path / "business.enc.json"
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS", raising=False)
    monkeypatch.setenv("TELEGRAM_BUSINESS_CONNECTIONS_FILE", str(target))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "7910015203:test-token")
    cache_business_connection(
        Obj(
            id="biz-connection-secret",
            user=Obj(id=123456789, username="story_owner_fixture"),
            user_chat_id=987654321,
            date=1777194243,
            is_enabled=True,
            rights=Obj(can_manage_stories=True),
        )
    )

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params={
            "mode": "default",
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_targets_override": [
                {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
            ],
            "story_business_targets": ["@story_owner_fixture"],
        },
    )

    assert config is not None
    assert all(target.get("transport") != "telegram_business" for target in config["targets"])


@pytest.mark.asyncio
async def test_build_story_publish_config_preserves_self_blocking_target(monkeypatch):
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_STORY_TARGETS_JSON",
        (
            '[{"peer":"me","delay_seconds":0,"mode":"upload"},'
            '{"peer":"@kenigevents","delay_seconds":0,"mode":"repost_previous"},'
            '{"peer":"@lovekenig","delay_seconds":600,"mode":"repost_previous"}]'
        ),
    )

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params={"story_publish_enabled": True, "story_publish_mode": "video"},
        selected_event_dates=["2026-04-25"],
    )

    assert config is not None
    assert config["targets"] == [
        {
            "peer": "me",
            "label": "me",
            "delay_seconds": 0,
            "mode": "upload",
        },
        {
            "peer": "@kenigevents",
            "label": "@kenigevents",
            "delay_seconds": 0,
            "mode": "repost_previous",
        },
        {
            "peer": "@lovekenig",
            "label": "@lovekenig",
            "delay_seconds": 600,
            "mode": "repost_previous",
        },
    ]
