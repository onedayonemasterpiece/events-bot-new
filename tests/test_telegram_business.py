import json

from telegram_business import (
    WEBHOOK_ALLOWED_UPDATES,
    business_connection_summary,
    cache_business_connection,
    load_business_story_targets,
    load_cached_business_connections,
)


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_webhook_allowed_updates_include_business_connection():
    assert "business_connection" in WEBHOOK_ALLOWED_UPDATES
    assert WEBHOOK_ALLOWED_UPDATES.count("business_connection") == 1


def test_business_connection_cache_encrypts_sensitive_ids(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "7910015203:test-token")
    connection = Obj(
        id="biz-connection-secret",
        user=Obj(id=123456789, username="story_owner_fixture"),
        user_chat_id=987654321,
        date=1777194243,
        is_enabled=True,
        rights=Obj(can_manage_stories=True),
        can_reply=True,
    )

    target = tmp_path / "connections.enc.json"
    record = cache_business_connection(connection, path=target)

    raw = target.read_text(encoding="utf-8")
    payload = json.loads(raw)
    assert payload["connections"]
    assert "biz-connection-secret" not in raw
    assert "123456789" not in raw
    assert "story_owner_fixture" not in raw
    assert record["can_manage_stories"] is True
    assert business_connection_summary(connection)["connection_hash"]

    restored = load_cached_business_connections(path=target)
    assert restored[0]["connection_id"] == "biz-connection-secret"
    assert restored[0]["user_id"] == 123456789
    assert restored[0]["username"] == "story_owner_fixture"


def test_business_story_targets_select_all_story_capable_connections(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "7910015203:test-token")
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_BUSINESS_TARGETS", "all")
    target = tmp_path / "connections.enc.json"
    cache_business_connection(
        Obj(
            id="biz-connection-secret",
            user=Obj(id=123456789, username="story_owner_fixture"),
            user_chat_id=987654321,
            date=1777194243,
            is_enabled=True,
            rights=Obj(can_manage_stories=True),
        ),
        path=target,
    )

    targets = load_business_story_targets(path=target)

    assert len(targets) == 1
    assert targets[0]["connection_id"] == "biz-connection-secret"
    assert targets[0]["connection_hash"]
    assert "story_owner_fixture" not in json.dumps(targets, ensure_ascii=False)
