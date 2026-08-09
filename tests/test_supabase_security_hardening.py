from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAVED = ROOT / "supabase/migrations/20260731174310_harden_saved_event_mutations.sql"
SEARCH = ROOT / "supabase/migrations/20260731174313_harden_event_search_internal_rpc.sql"
SEARCH_CANARY = next(
    (ROOT / "supabase/migrations").glob("*event_search_canary_receipts*.sql")
)
EDGE = ROOT / "supabase/functions/event-search/index.ts"


def test_saved_event_mutations_are_rpc_only_owner_bound_and_capped() -> None:
    sql = SAVED.read_text(encoding="utf-8")
    assert "revoke insert, update, delete on table public.user_saved_event from authenticated" in sql
    assert "grant select on table public.user_saved_event to authenticated" in sql
    assert "security definer" in sql
    assert "set search_path = pg_catalog" in sql
    assert "v_user_id uuid := (select auth.uid())" in sql
    assert "where user_id = v_user_id" in sql
    assert "v_max_active constant integer := 500" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "saved_event_limit_exceeded" in sql
    assert "raw_user_meta_data" not in sql


def test_search_browser_roles_cannot_execute_expensive_primitives() -> None:
    sql = SEARCH.read_text(encoding="utf-8")
    for name in (
        "search_events_by_embedding_v1",
        "event_search_fallback_cards_v1",
        "reserve_event_search_quota_v1",
        "reserve_event_search_quota_v2",
        "reserve_event_search_quota_v3",
        "record_event_search_request_v1",
    ):
        assert f"'{name}'" in sql
    assert "from public, anon, authenticated" in sql
    assert "to service_role" in sql
    assert "get_event_search_quota_internal_v1" in sql
    assert "reserve_event_search_quota_internal_v1" in sql
    assert "search_events_by_embedding_internal_v1" in sql
    assert "event_search_fallback_cards_internal_v1" in sql
    assert "record_event_search_request_internal_v1" in sql
    assert "not exists (\n    select 1 from auth.users where id = p_user_id" in sql
    assert "raw_user_meta_data" not in sql


def test_quota_and_feedback_are_idempotent_bounded_and_retention_capped() -> None:
    sql = SEARCH.read_text(encoding="utf-8")
    assert "primary key (user_id, client_request_id)" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "created_at < pg_catalog.now() - interval '48 hours'" in sql
    assert "v_count >= 1000" in sql
    assert "event_search_feedback_user_operation_uidx" in sql
    assert "created_at >= pg_catalog.now() - interval '1 hour'" in sql
    assert "v_recent_count >= 30" in sql
    assert "created_at < pg_catalog.now() - interval '90 days'" in sql
    assert "limit 50" in sql
    assert "jsonb_strip_nulls" in sql


def test_edge_authenticates_before_constructing_service_client_and_caps_body() -> None:
    source = EDGE.read_text(encoding="utf-8")
    handler = source[source.index("async function runEventSearch"):]
    get_user = handler.index("supabase.auth.getUser(")
    service_client = handler.index("personalizationServiceClient(supabaseUrl)")
    assert get_user < service_client
    assert "MAX_REQUEST_BODY_BYTES = 16 * 1024" in source
    assert "request_too_large" in source
    assert '"reserve_event_search_quota_internal_v1"' in source
    assert '"search_events_by_embedding_internal_v1"' in source
    assert '"event_search_fallback_cards_internal_v1"' in source
    assert '"record_event_search_request_internal_v1"' in source
    assert '"reserve_event_search_quota_v3"' not in source
    assert '"search_events_by_embedding_v1"' not in source
    assert "p_client_request_id: quotaOperationId" in source
    assert "client_request_id: quotaOperationId" in source


def test_search_canary_receipts_are_owner_scoped_and_browser_immutable() -> None:
    sql = SEARCH_CANARY.read_text(encoding="utf-8")
    assert "alter table public.event_search_canary_receipts enable row level security" in sql
    assert (
        "revoke all on table public.event_search_canary_receipts from public, anon, authenticated"
        in sql
    )
    assert "get_event_search_receipt_v1" in sql
    assert "r.user_id = (select auth.uid())" in sql
    assert "record_event_search_canary_receipt_internal_v1" in sql
    assert "to service_role" in sql
    assert "raw_user_meta_data" not in sql
    assert "raw_app_meta_data" in sql


def test_search_canary_llm_budget_is_atomic_idempotent_and_server_enforced() -> None:
    sql = SEARCH_CANARY.read_text(encoding="utf-8")
    assert "event_search_canary_llm_budget_ledger" in sql
    assert "event_search_canary_llm_budget_operation" in sql
    assert "primary key (user_id, operation_id)" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "search_canary_llm_daily_budget_exhausted" in sql
    assert "attempts_used + p_attempts > v_ledger.budget_limit" in sql
    assert "reserve_event_search_canary_llm_budget_internal_v1" in sql


def test_search_receipt_contract_has_revisions_modes_and_attempt_counters() -> None:
    sql = SEARCH_CANARY.read_text(encoding="utf-8")
    source = EDGE.read_text(encoding="utf-8")
    for name in (
        "search_contract_version",
        "requested_execution_mode",
        "actual_execution_mode",
        "catalog_revision",
        "corpus_revision",
        "search_document_revision",
        "embedding_provider_attempts",
        "llm_provider_attempts",
        "vector_rpc_attempts",
        "result_cache_read_attempts",
        "result_cache_hit_count",
    ):
        assert name in sql
        assert name in source
    assert "degraded:deterministic_canary_failure" in source
    assert "attempts: []" in source


def test_broker_issue_claim_is_service_only_and_pii_free() -> None:
    sql = SEARCH_CANARY.read_text(encoding="utf-8")
    assert "claim_static_site_auth_session_issue_v1" in sql
    assert "static_site_auth_session_issue_claim" in sql
    assert "enable row level security" in sql
    assert "from public, anon, authenticated" in sql
    claim_table = sql[sql.index("create table public.static_site_auth_session_issue_claim") :]
    claim_table = claim_table[: claim_table.index(");")]
    for forbidden in ("email", "action_link", "access_token", "refresh_token", "session"):
        if forbidden == "session":
            # The canonical table name contains session; columns must not.
            continue
        assert forbidden not in claim_table

    replay_fix = (
        ROOT
        / "supabase"
        / "migrations"
        / "20260808094500_static_site_auth_session_claim_replay.sql"
    ).read_text(encoding="utf-8")
    duplicate_guard = replay_fix[
        replay_fix.index("if exists (") : replay_fix.index("delete from", replay_fix.index("if exists ("))
    ]
    assert "run_id = p_run_id" in duplicate_guard
    assert "run_attempt = p_run_attempt" in duplicate_guard
    assert "persona_id = p_persona_id" in duplicate_guard
    assert "return false;" in duplicate_guard
    assert "return true;" not in duplicate_guard

    platform_claim = (
        ROOT
        / "supabase"
        / "migrations"
        / "20260809143602_static_site_auth_broker_platform_claims.sql"
    ).read_text(encoding="utf-8")
    assert "claim_static_site_auth_session_issue_v2" in platform_claim
    assert "p_platform not in ('browser', 'android', 'ios')" in platform_claim
    for outcome in ("new", "duplicate_inflight", "persona_busy"):
        assert f"return '{outcome}';" in platform_claim
    assert "security definer" in platform_claim
    assert "set search_path = pg_catalog" in platform_claim
    assert "from public, anon, authenticated" in platform_claim
    assert "to service_role" in platform_claim
    for forbidden in ("email_otp", "action_link", "access_token", "refresh_token"):
        assert forbidden not in platform_claim
