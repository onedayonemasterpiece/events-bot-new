from __future__ import annotations

import pytest

from video_announce.scenario import VideoAnnounceScenario
from video_announce import story_publish
from video_announce.partner_tracks import PARTNER_KONB_LIBRARY
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
        {
            "peer": "@kenigevents",
            "delay_seconds": 0,
            "mode": "upload",
            "blocking": False,
        },
        {
            "peer": "club231828790",
            "label": "vk:club231828790:wall",
            "delay_seconds": 300,
            "mode": "upload",
            "transport": "vk_wall",
            "caption": "Видеоанонс",
        },
        {"peer": "@lovekenig", "delay_seconds": 300, "mode": "repost_previous"},
        {
            "peer": "club231828790",
            "label": "vk:club231828790:story",
            "delay_seconds": 0,
            "mode": "upload",
            "transport": "vk_wall_story",
        },
        {"peer": "@loving_guide39", "delay_seconds": 300, "mode": "repost_previous"},
        {
            "peer": "klgdevents",
            "label": "vk:klgdevents:story",
            "delay_seconds": 600,
            "mode": "upload",
            "transport": "vk_wall_story",
        },
        {
            "peer": "kenigeventsofficial",
            "label": "vk:kenigeventsofficial:wall",
            "delay_seconds": 300,
            "mode": "upload",
            "transport": "vk_wall",
            "caption_variant": "crumple_official",
            "blocking": False,
            "required": False,
        },
        {"peer": "@catwithbag", "delay_seconds": 300, "mode": "repost_previous"},
        {"peer": "@i_love_kaliningrad", "delay_seconds": 600, "mode": "repost_previous"},
    ]


@pytest.mark.asyncio
async def test_popular_review_story_config_keeps_vk_targets_and_nonblocking_primary(
    monkeypatch,
):
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    scenario = VideoAnnounceScenario(db=None, bot=None, chat_id=0, user_id=0)

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params=scenario._popular_review_selection_params(),
        selected_event_dates=["2026-05-17"],
        selected_event_cities=["Калининград", "Светлогорск"],
    )

    assert config is not None
    assert config["targets"][0]["blocking"] is False
    vk_wall_target = config["targets"][1]
    assert vk_wall_target["caption"].startswith("Видеоанонс\n\n")
    assert "#Калининград" in vk_wall_target["caption"]
    assert "#Светлогорск" in vk_wall_target["caption"]
    assert "#17мая" in vk_wall_target["caption"]
    assert "#17_мая" in vk_wall_target["caption"]
    official_wall_target = config["targets"][6]
    assert official_wall_target["peer"] == "kenigeventsofficial"
    assert official_wall_target["caption"] == (
        "События на 17 мая #Калининград #Светлогорск #17_мая #17мая"
    )
    assert [
        (target["peer"], target.get("transport", "telethon"))
        for target in config["targets"]
    ] == [
        ("@kenigevents", "telethon"),
        ("club231828790", "vk_wall"),
        ("@lovekenig", "telethon"),
        ("club231828790", "vk_wall_story"),
        ("@loving_guide39", "telethon"),
        ("klgdevents", "vk_wall_story"),
        ("kenigeventsofficial", "vk_wall"),
        ("@catwithbag", "telethon"),
        ("@i_love_kaliningrad", "telethon"),
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
    assert business_target["blocking"] is False
    assert "required" not in business_target
    assert business_target["label"].startswith("business:")
    serialized = str(config)
    assert "biz-connection-secret" not in serialized
    assert "story_owner_fixture" not in serialized


@pytest.mark.asyncio
async def test_empty_selection_override_blocks_global_channel_fanout(monkeypatch, tmp_path):
    target = tmp_path / "business.enc.json"
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_STORY_TARGETS_JSON",
        '[{"peer":"me"},{"peer":"@kenigevents"},{"peer":"@lovekenig"}]',
    )
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_BUSINESS_DELAY_SECONDS", "1")
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
            "mode": "popular_review",
            "story_publish_enabled": True,
            "story_targets_override": [],
            "story_business_targets": ["@story_owner_fixture"],
        },
        selected_event_dates=["2026-05-15"],
    )

    assert config is not None
    assert [target["peer"] for target in config["targets"]] == [
        config["targets"][0]["peer"]
    ]
    assert config["targets"][0]["transport"] == "telegram_business"


@pytest.mark.asyncio
async def test_konb_test_config_posts_to_channel_and_does_not_inherit_business(
    monkeypatch,
    tmp_path,
):
    target = tmp_path / "business.enc.json"
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
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

    scenario = VideoAnnounceScenario(db=None, bot=None, chat_id=0, user_id=0)
    params = scenario._partner_track_selection_params(
        PARTNER_KONB_LIBRARY,
        publish_mode="test",
    )

    config = await story_publish.build_story_publish_config(
        None,
        main_chat_id=None,
        selection_params=params,
        selected_event_dates=["2026-05-18"],
    )

    assert config is not None
    assert [(target["peer"], target.get("transport")) for target in config["targets"]] == [
        ("@keniggpt", "telegram_chat")
    ]
    assert all(target.get("transport") != "telegram_business" for target in config["targets"])


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
async def test_business_story_targets_are_allowed_for_kenigsberg(monkeypatch, tmp_path):
    target = tmp_path / "business.enc.json"
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS", raising=False)
    monkeypatch.delenv("VIDEO_ANNOUNCE_STORY_BUSINESS_MODES", raising=False)
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
            "mode": "kenigsberg_story",
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_targets_override": [
                {"peer": "@mostvkenig", "delay_seconds": 0, "mode": "upload"},
            ],
            "story_business_targets": ["@story_owner_fixture"],
        },
    )

    assert config is not None
    assert config["targets"][-1]["transport"] == "telegram_business"
    assert config["targets"][-1]["blocking"] is False
    assert "required" not in config["targets"][-1]


@pytest.mark.asyncio
async def test_build_story_publish_config_preserves_self_blocking_target(monkeypatch):
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_STORY_TARGETS_JSON",
        (
            '[{"peer":"me","delay_seconds":0,"mode":"upload"},'
            '{"peer":"@kenigevents","delay_seconds":0,"mode":"repost_previous","required":true},'
            '{"peer":"@lovekenig","delay_seconds":600,"mode":"repost_previous","required":true}]'
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
            "required": True,
        },
        {
            "peer": "@lovekenig",
            "label": "@lovekenig",
            "delay_seconds": 600,
            "mode": "repost_previous",
            "required": True,
        },
    ]
