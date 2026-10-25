from __future__ import annotations

from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "supabase"
    / "migrations"
    / "20260825120000_google_ai_interaction_started_v3.sql"
)
MIGRATION = MIGRATION_PATH.read_text(encoding="utf-8")


def test_v3_is_additive_and_does_not_rewrite_v2() -> None:
    assert MIGRATION_PATH.name == "20260825120000_google_ai_interaction_started_v3.sql"
    assert "CREATE OR REPLACE FUNCTION google_ai_mark_interaction_started_v1(" in MIGRATION
    assert "google_ai_finalize_interaction_v2" not in MIGRATION
    assert "DROP TABLE" not in MIGRATION.upper()
    assert "API_KEY" not in MIGRATION.upper().replace("API_KEY_ID", "")


def test_started_rpc_requires_sent_nonterminal_exact_attempt() -> None:
    for marker in (
        "v_request.sent_at IS NULL",
        "v_attempt.status NOT IN ('sent', 'in_progress')",
        "v_request.finalized_at IS NOT NULL",
        "v_attempt.completed_at IS NOT NULL",
        "interaction_started_attempt_not_sent",
        "interaction_started_attempt_terminal",
    ):
        assert marker in MIGRATION


def test_same_id_is_idempotent_and_different_id_requires_reconciliation() -> None:
    assert "v_request_id = p_provider_interaction_id" in MIGRATION
    assert "v_attempt_id = p_provider_interaction_id" in MIGRATION
    assert "COALESCE(" in MIGRATION
    assert "interaction_started_id_conflict_reconciliation_required" in MIGRATION
    assert "provider_interaction_id = p_provider_interaction_id" in MIGRATION


def test_started_rpc_does_not_touch_quota_or_terminal_accounting() -> None:
    function = MIGRATION.split(
        "CREATE OR REPLACE FUNCTION google_ai_mark_interaction_started_v1(", 1
    )[1].split("CREATE OR REPLACE FUNCTION google_ai_limiter_capabilities()", 1)[0]
    for forbidden in (
        "google_ai_usage_counters",
        "reserved_tpm =",
        "usage_input_tokens =",
        "usage_output_tokens =",
        "usage_total_tokens =",
        "finalized_at =",
        "completed_at =",
        "rpd_used",
        "rpm_used",
        "tpm_used",
    ):
        assert forbidden not in function


def test_v3_capability_markers_and_grants_are_exact() -> None:
    for marker in (
        "google_ai_project_model_atomic_v1",
        "rolling_60s_pacific_day_v2",
        "quota_scope/model",
        "google_ai_interaction_usage_v3",
        "interaction_started_supported",
        "google_ai_mark_interaction_started_v1",
        "unsent_release_supported",
    ):
        assert marker in MIGRATION
    assert (
        "GRANT EXECUTE ON FUNCTION google_ai_mark_interaction_started_v1(UUID, INT, TEXT, TEXT) "
        "TO service_role;"
    ) in MIGRATION
