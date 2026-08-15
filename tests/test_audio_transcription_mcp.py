from audio_transcription.mcp import build_audio_transcription_tools
from private_events_mcp.access_policy import social_scopes_authorized


def test_existing_telegram_publish_scope_authorizes_audio_tools() -> None:
    tools = build_audio_transcription_tools(object(), signing_key="s" * 32)
    legacy = frozenset({"telegram:publish"})
    dedicated = frozenset({"audio:transcribe"})

    assert {tool.name for tool in tools} == {
        "audio_transcription_start",
        "audio_transcription_status",
        "audio_transcription_get",
    }
    for tool in tools:
        assert tool.is_visible(legacy)
        assert tool.is_visible(dedicated)
        assert social_scopes_authorized(tool.required_scopes({}), legacy)
        assert not tool.is_visible(frozenset({"telegram:read"}))

