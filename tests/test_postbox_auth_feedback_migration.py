from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase" / "migrations" / "20260804190000_postbox_auth_feedback_correlation_v1.sql"
CONTRACT = ROOT / "supabase" / "tests" / "email_postbox_auth_feedback_contract.sql"


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_migration_registers_all_postbox_send_sources_without_plain_email() -> None:
    sql = _sql()
    assert "create table email_control.postbox_message_correlation" in sql
    assert "source_kind = 'transactional_outbox'" in sql
    assert "source_kind = 'focus_auth'" in sql
    assert "source_kind = 'legacy_auth'" in sql
    assert "email_hmac text" in sql
    assert "normalized_email" not in sql
    assert "recipient_email" not in sql
    assert "email_address" not in sql


def test_auth_registration_happens_after_row_insert_and_unknown_ids_remain_pending() -> None:
    sql = _sql()
    assert "lock table email_control.email_outbox in share row exclusive mode" in sql
    assert "lock table personalization.focus_auth_delivery_attempt in share row exclusive mode" in sql
    assert "create trigger focus_auth_initialize_postbox_feedback_v1\n  before insert or update" in sql
    assert "create trigger focus_auth_register_postbox_correlation_v1\n  after insert or update" in sql
    assert "return 'correlation_pending';" in sql
    assert "email_register_legacy_postbox_auth_v1" in sql
    assert "The event itself never\n-- auto-registers an unknown MessageId" in sql


def test_v2_is_a_compatibility_wrapper_over_v3_and_health_is_unified() -> None:
    sql = _sql()
    assert "create or replace function public.email_record_postbox_event_v3" in sql
    wrapper = sql.split(
        "create or replace function public.email_record_postbox_event_v2", 1
    )[1].split("create or replace function public.email_postbox_health_v2", 1)[0]
    assert "select public.email_record_postbox_event_v3(" in wrapper
    assert "postbox_auth_submitted_count" in sql
    assert "postbox_missing_correlation_count" in sql
    assert "select public.email_postbox_health_v2()" in sql


def test_rollback_contract_covers_auth_outbox_legacy_and_suppression() -> None:
    contract = CONTRACT.read_text(encoding="utf-8")
    assert contract.startswith("-- Rollback-only contract")
    assert contract.rstrip().endswith("rollback;")
    for token in (
        "focus_auth_complete_delivery_batch_v1",
        "focus_auth_begin_delivery_batch_v1",
        "email_record_postbox_event_v2",
        "email_record_postbox_event_v3",
        "email_register_legacy_postbox_auth_v1",
        "transactional_outbox",
        "hard_bounce",
        "complaint",
        "correlation_pending",
        "postbox_missing_correlation_count",
        "recipient_suppressed",
    ):
        assert token in contract


def test_direct_auth_admission_is_exact_versioned_hmac_and_service_only() -> None:
    sql = _sql()
    assert "create or replace function public.focus_auth_begin_delivery_batch_v1" in sql
    assert "p_deliveries jsonb" in sql
    assert "s.email_hmac = v_email_hmac" in sql
    assert "s.hmac_key_version = v_hmac_key_version" in sql
    assert "s.scope in ('all', 'transactional')" in sql
    assert "'admission_status', 'recipient_suppressed'" in sql
    assert "pg_advisory_xact_lock(hashtextextended(v_email_hmac, 20260804))" in sql
    assert "recipient_hmac_key_version" in sql
    assert "network_claimed_at" in sql
    assert "to service_role" in sql
    assert "from public, anon, authenticated" in sql
    assert "normalized_email" not in sql


def test_migration_removes_the_suppression_free_legacy_admission_rpc() -> None:
    sql = _sql()
    assert "revoke execute on function public.focus_auth_begin_delivery_v1" in sql
    assert "from service_role" in sql
    assert "from public, anon, authenticated" in sql
