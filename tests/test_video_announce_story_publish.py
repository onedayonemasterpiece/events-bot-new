from __future__ import annotations

from video_announce import story_publish


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
