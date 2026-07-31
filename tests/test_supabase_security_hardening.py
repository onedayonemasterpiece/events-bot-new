from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAVED = ROOT / "supabase/migrations/20260731174310_harden_saved_event_mutations.sql"
SEARCH = ROOT / "supabase/migrations/20260731174313_harden_event_search_internal_rpc.sql"
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
    get_user = handler.index("await supabase.auth.getUser(accessToken)")
    service_client = handler.index("const service = personalizationServiceClient(supabaseUrl)")
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
