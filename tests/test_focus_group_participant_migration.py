from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / 'supabase/migrations/20260731185118_focus_group_participant_cap_and_backfill_v2.sql').read_text()


def test_backfill_is_fixed_cutoff_and_never_infers_consent() -> None:
    assert "u.created_at < timestamptz '2026-08-01 00:00:00+00'" in SQL
    assert "'/focus-presentation-auth-backfill/'" in SQL
    backfill = SQL.split('create or replace function', 1)[0]
    assert 'focus_updates_consent' not in backfill
    assert "i.provider in ('email', 'custom:yandex')" in backfill


def test_admission_is_atomic_bounded_and_identity_owned() -> None:
    assert 'pg_advisory_xact_lock' in SQL
    assert 'v_active_count >= 200' in SQL
    assert 'auth.uid()' in SQL
    assert "v_email_confirmed_at is null" in SQL
    assert "i.provider in ('email', 'custom:yandex')" in SQL
    assert 'security definer' in SQL
    assert 'to authenticated, service_role' in SQL
