from __future__ import annotations

import sys

import pytest


def _arg_after(cmd: list[str], name: str) -> str:
    return cmd[cmd.index(name) + 1]


def test_static_site_build_kaggle_command_includes_pgvector_handoff(monkeypatch: pytest.MonkeyPatch) -> None:
    import main

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "pgvector")
    monkeypatch.setenv("STATIC_SITE_SYNC_PGVECTOR_VECTORS", "1")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_EMBEDDING_MODEL", "gemini-embedding-2")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_EMBEDDING_KEY_ENV", "GOOGLE_API_KEY4")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_MAX_PROVIDER_CALLS", "123")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_VERIFY", "1")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS", "15")
    monkeypatch.setenv("STATIC_SITE_ASSET_BASE_URL", "https://static.kenigevents.ru")
    monkeypatch.setenv("STATIC_SITE_ASTRO_ASSET_BASE_URL", "https://static.kenigevents.ru/{buildId}")
    monkeypatch.setenv("STATIC_SITE_ICS_BASE_URL", "https://static.kenigevents.ru/ics")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY", "sb_publishable_test")

    cmd = main._static_site_build_kaggle_command(
        db_path="/data/db.sqlite",
        build_id="preview-test-pgvector",
        limit=70,
        current_date="2026-06-29",
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
    )

    assert cmd[0] == sys.executable
    assert _arg_after(cmd, "--db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-callback-url") == "https://events-bot.example/internal/kaggle/run-event"
    assert _arg_after(cmd, "--related-mode") == "pgvector"
    assert "--sync-pgvector-vectors" in cmd
    assert _arg_after(cmd, "--pgvector-embedding-model") == "gemini-embedding-2"
    assert _arg_after(cmd, "--pgvector-embedding-key-env") == "GOOGLE_API_KEY4"
    assert _arg_after(cmd, "--pgvector-max-provider-calls") == "123"
    assert "--gemma-related-verify" in cmd
    assert _arg_after(cmd, "--gemma-related-max-anchors") == "15"
    assert _arg_after(cmd, "--asset-base-url") == "https://static.kenigevents.ru"
    assert _arg_after(cmd, "--astro-asset-base-url") == "https://static.kenigevents.ru/{buildId}"
    assert _arg_after(cmd, "--ics-base-url") == "https://static.kenigevents.ru/ics"
    assert _arg_after(cmd, "--public-personalization-supabase-url") == "https://example.supabase.co"
    assert _arg_after(cmd, "--public-personalization-supabase-publishable-key") == "sb_publishable_test"
    assert _arg_after(cmd, "--public-yandex-auth-provider") == "custom:yandex"
    assert "--export-in-kaggle" in cmd
    assert "--download-output" in cmd


def test_static_site_build_kaggle_command_rejects_unknown_related_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import main

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "not-a-mode")

    with pytest.raises(ValueError):
        main._static_site_build_kaggle_command(
            db_path="/data/db.sqlite",
            build_id="preview-test",
            limit=1,
            current_date="2026-06-29",
            script_path="/repo/scripts/run_static_site_builder_kaggle.py",
            status_callback_url="https://events-bot.example/internal/kaggle/run-event",
        )
