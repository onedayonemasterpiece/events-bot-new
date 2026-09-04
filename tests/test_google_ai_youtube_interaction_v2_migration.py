from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260824170000_google_ai_youtube_interaction_v2.sql"
)
MIGRATION = MIGRATION_PATH.read_text(encoding="utf-8")

EXPECTED = {
    "gemini-3.1-flash-lite": (15, 250000, 500),
    "gemini-3.5-flash-lite": (15, 250000, 500),
    "gemma-4-31b-it": (30, 16000, 14400),
    "gemini-2.5-flash": (5, 250000, 20),
    "gemini-2.5-flash-lite": (10, 250000, 20),
    "gemini-2.5-flash-preview-tts": (3, 10000, 10),
    "gemini-3-flash-preview": (5, 250000, 20),
    "gemini-3.1-flash-tts-preview": (3, 10000, 10),
    "gemini-3.5-flash": (5, 250000, 20),
    "gemini-3.6-flash": (5, 250000, 20),
    "gemini-3.7-flash": (5, 250000, 20),
    "gemini-embedding-001": (100, 30000, 1000),
    "gemini-embedding-2-preview": (100, 30000, 1000),
    "gemini-robotics-er-1.6-preview": (5, 250000, 20),
    "gemini-robotics-er-2-preview": (5, 250000, 20),
    "gemma-4-26b-a4b-it": (30, 16000, 14400),
}


def parsed_rows() -> dict[str, tuple[int, int, int]]:
    return {
        model: (int(rpm), int(tpm), int(rpd))
        for model, rpm, tpm, rpd in re.findall(
            r"\('([^']+)',\s*(\d+),\s*(\d+),\s*(\d+)\)",
            MIGRATION,
        )
    }


def test_exact_owner_matrix_uses_canonical_positive_model_ids() -> None:
    assert parsed_rows() == EXPECTED
    assert all(min(limits) > 0 for limits in EXPECTED.values())
    assert "gemini-embedding-2" not in parsed_rows()
    assert "antigravity-preview-05-2026" not in parsed_rows()
    assert "live" not in {model.casefold() for model in parsed_rows()}
    assert not any("ground" in model.casefold() for model in parsed_rows())


def test_upsert_preserves_tpm_reserve_extra() -> None:
    upsert = MIGRATION.split("INSERT INTO google_ai_model_limits", 1)[1]
    assert "INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd)" in MIGRATION
    assert "tpm_reserve_extra = EXCLUDED.tpm_reserve_extra" not in upsert
    assert "rpm = EXCLUDED.rpm" in upsert
    assert "tpm = EXCLUDED.tpm" in upsert
    assert "rpd = EXCLUDED.rpd" in upsert


def test_interaction_v2_accounts_thought_tokens_and_actual_total() -> None:
    assert "ADD COLUMN IF NOT EXISTS usage_thought_tokens" in MIGRATION
    assert "CREATE OR REPLACE FUNCTION google_ai_finalize_interaction_v2(" in MIGRATION
    assert "p_usage_thought_tokens INT" in MIGRATION
    assert "usage_thought_tokens = p_usage_thought_tokens" in MIGRATION
    assert "ADD COLUMN IF NOT EXISTS initial_reserved_tpm" in MIGRATION
    assert "v_attempt.initial_reserved_tpm" in MIGRATION
    assert "tpm_used = GREATEST(0, tpm_used + v_delta)" in MIGRATION
    assert "reserved_tpm = COALESCE(p_usage_total_tokens, reserved_tpm)" in MIGRATION
    assert "google-ai-project-model-v1:" in MIGRATION


def test_unsent_release_is_fail_closed_and_removes_rolling_attempt() -> None:
    assert "CREATE OR REPLACE FUNCTION google_ai_release_unsent_v2(" in MIGRATION
    assert "v_request.sent_at IS NOT NULL" in MIGRATION
    assert "attempt_already_sent_or_finalized" in MIGRATION
    assert "DELETE FROM google_ai_request_attempts" in MIGRATION
    assert "rpm_used = GREATEST(0, rpm_used - 1)" in MIGRATION
    assert "rpd_used = GREATEST(0, rpd_used - 1)" in MIGRATION


def test_exact_capability_markers_are_published() -> None:
    for marker in (
        "google_ai_project_model_atomic_v1",
        "rolling_60s_pacific_day_v2",
        "quota_scope/model",
        "google_ai_interaction_usage_v2",
        "unsent_release_supported",
        "quota_scope_enforced",
    ):
        assert marker in MIGRATION


def test_migration_is_additive_and_does_not_rewrite_bootstrap() -> None:
    assert MIGRATION_PATH.name.startswith("20260824")
    assert "DROP TABLE" not in MIGRATION.upper()
    assert "API_KEY" not in MIGRATION.upper().replace("API_KEY_ID", "")
