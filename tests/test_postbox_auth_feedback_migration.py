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
        "focus_auth_complete_delivery_v1",
        "email_record_postbox_event_v2",
        "email_record_postbox_event_v3",
        "email_register_legacy_postbox_auth_v1",
        "transactional_outbox",
        "hard_bounce",
        "complaint",
        "correlation_pending",
        "postbox_missing_correlation_count",
    ):
        assert token in contract
