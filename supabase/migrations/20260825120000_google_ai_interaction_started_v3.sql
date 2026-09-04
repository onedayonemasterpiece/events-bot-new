-- Additive early-interaction evidence for streamed Gemini Interactions API calls.
--
-- This migration is intentionally independent of the already-applied v2
-- interaction finalizer. It stores no API-key secret and changes no RPM, TPM or
-- RPD counter. A streamed provider attempt remains governed by:
-- reserve -> mark_sent -> mark_interaction_started_v1 -> finalize_interaction_v2.

BEGIN;

ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS interaction_started_at TIMESTAMPTZ NULL;
ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS provider_started_status TEXT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS interaction_started_at TIMESTAMPTZ NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS provider_started_status TEXT NULL;

CREATE OR REPLACE FUNCTION google_ai_mark_interaction_started_v1(
    p_request_uid UUID,
    p_attempt_no INT,
    p_provider_interaction_id TEXT,
    p_provider_status TEXT DEFAULT 'in_progress'
)
RETURNS JSONB
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    v_request RECORD;
    v_attempt RECORD;
    v_request_id TEXT;
    v_attempt_id TEXT;
BEGIN
    IF p_attempt_no < 1 THEN
        RAISE EXCEPTION 'interaction_started_attempt_invalid';
    END IF;
    IF p_provider_interaction_id IS NULL
       OR btrim(p_provider_interaction_id) = ''
       OR octet_length(p_provider_interaction_id) > 1024
       OR p_provider_interaction_id ~ '[[:space:][:cntrl:]]' THEN
        RAISE EXCEPTION 'interaction_started_id_invalid';
    END IF;
    IF p_provider_status NOT IN ('created', 'in_progress') THEN
        RAISE EXCEPTION 'interaction_started_status_invalid';
    END IF;

    SELECT * INTO v_request
    FROM google_ai_requests
    WHERE request_uid = p_request_uid
    FOR UPDATE;
    IF v_request IS NULL THEN
        RAISE EXCEPTION 'interaction_started_request_not_found';
    END IF;

    SELECT * INTO v_attempt
    FROM google_ai_request_attempts
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no
    FOR UPDATE;
    IF v_attempt IS NULL THEN
        RAISE EXCEPTION 'interaction_started_attempt_not_found';
    END IF;

    IF v_request.sent_at IS NULL OR v_attempt.status NOT IN ('sent', 'in_progress') THEN
        RAISE EXCEPTION 'interaction_started_attempt_not_sent';
    END IF;
    IF v_request.finalized_at IS NOT NULL OR v_attempt.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'interaction_started_attempt_terminal';
    END IF;

    v_request_id := NULLIF(v_request.provider_interaction_id, '');
    v_attempt_id := NULLIF(v_attempt.provider_interaction_id, '');
    IF (v_request_id IS NOT NULL AND v_request_id <> p_provider_interaction_id)
       OR (v_attempt_id IS NOT NULL AND v_attempt_id <> p_provider_interaction_id) THEN
        RAISE EXCEPTION 'interaction_started_id_conflict_reconciliation_required';
    END IF;

    UPDATE google_ai_requests
    SET provider_interaction_id = p_provider_interaction_id,
        interaction_started_at = COALESCE(interaction_started_at, NOW()),
        provider_started_status = COALESCE(provider_started_status, p_provider_status),
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    UPDATE google_ai_request_attempts
    SET provider_interaction_id = p_provider_interaction_id,
        interaction_started_at = COALESCE(interaction_started_at, NOW()),
        provider_started_status = COALESCE(provider_started_status, p_provider_status)
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;

    RETURN jsonb_build_object(
        'ok', true,
        'idempotent', COALESCE(
            v_request_id = p_provider_interaction_id
            OR v_attempt_id = p_provider_interaction_id,
            false
        ),
        'interaction_accounting', 'google_ai_interaction_usage_v3',
        'interaction_started_supported', true,
        'interaction_started_rpc', 'google_ai_mark_interaction_started_v1'
    );
END;
$$;

CREATE OR REPLACE FUNCTION google_ai_limiter_capabilities()
RETURNS JSONB
LANGUAGE sql
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT jsonb_build_object(
        'limiter_contract', 'google_ai_project_model_atomic_v1',
        'bucket_strategy', 'rolling_60s_pacific_day_v2',
        'quota_scope_dimension', 'google_cloud_project',
        'quota_dimension', 'quota_scope/model',
        'lock_dimension', 'quota_scope/model',
        'counter_aggregation', 'quota_scope/model/bucket',
        'quota_scope_enforced', true,
        'interaction_accounting', 'google_ai_interaction_usage_v3',
        'unsent_release_supported', true,
        'interaction_started_supported', true,
        'interaction_started_rpc', 'google_ai_mark_interaction_started_v1'
    );
$$;

REVOKE ALL ON FUNCTION google_ai_mark_interaction_started_v1(UUID, INT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_limiter_capabilities() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION google_ai_mark_interaction_started_v1(UUID, INT, TEXT, TEXT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_limiter_capabilities() TO service_role;

COMMIT;
