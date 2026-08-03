from __future__ import annotations

from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260803074500_google_ai_rolling_windows_pacific_rpd.sql"
).read_text(encoding="utf-8")


def test_google_ai_admission_uses_rolling_window_and_pacific_day() -> None:
    assert "a.started_at > v_now - INTERVAL '60 seconds'" in MIGRATION
    assert "AT TIME ZONE 'America/Los_Angeles'" in MIGRATION
    assert "rolling_60s_pacific_day_v2" in MIGRATION
    assert "date_trunc('minute', v_now)" in MIGRATION  # audit counters only


def test_google_ai_provider_429_cooldown_is_scope_and_model_shared() -> None:
    assert "CREATE TABLE IF NOT EXISTS google_ai_provider_cooldowns" in MIGRATION
    assert "PRIMARY KEY (quota_scope, model)" in MIGRATION
    assert "CREATE OR REPLACE FUNCTION google_ai_report_provider_429(" in MIGRATION
    assert "WHERE quota_scope = v_quota_scope AND model = p_model" in MIGRATION
    assert "LEAST(93600000" in MIGRATION


def test_google_ai_reserve_locks_scope_model_not_individual_key() -> None:
    assert "google-ai-project-model-v1:" in MIGRATION
    assert "v_quota_scope || ':' || p_model" in MIGRATION
    assert "v_quota_scope = ANY(v_checked_scopes)" in MIGRATION


def test_migration_has_no_accidental_duplicate_upsert_clause() -> None:
    assert "DO UPDATE SET\n        DO UPDATE SET" not in MIGRATION
