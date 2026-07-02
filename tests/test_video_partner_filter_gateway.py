from __future__ import annotations


def test_video_partner_filter_uses_supabase_backed_google_ai_client(monkeypatch):
    import google_ai
    import main
    from video_announce.scenario import VideoAnnounceScenario

    supabase_marker = object()
    calls: list[dict] = []

    class FakeGoogleAIClient:
        def __init__(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)
    monkeypatch.setattr(google_ai, "SecretsProvider", lambda: "secrets-provider")
    monkeypatch.setattr(main, "get_supabase_client", lambda: supabase_marker)
    monkeypatch.setattr(main, "notify_llm_incident", "incident-notifier")

    client = VideoAnnounceScenario(
        db=None,  # type: ignore[arg-type]
        bot=None,
        chat_id=1,
        user_id=1,
    )._gemma_client_for_partner_filters()

    assert isinstance(client, FakeGoogleAIClient)
    assert calls[0]["supabase_client"] is supabase_marker
    assert calls[0]["secrets_provider"] == "secrets-provider"
    assert calls[0]["consumer"] == "video_partner_filter"
    assert calls[0]["incident_notifier"] == "incident-notifier"
