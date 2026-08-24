-- Additive canonical Google AI limiter support for stateless Gemini YouTube
-- interactions and owner-supplied model limits dated 2026-08-24.
--
-- This migration never stores API-key secrets. It preserves tpm_reserve_extra,
-- keeps quota aggregation at quota_scope/model, adds thought-token accounting,
-- and gives fail-closed clients an exact capability marker.
--
-- CUTOVER GATE: applying this migration is not permission to send traffic.
-- Operators must first verify that the owner-supplied matrix applies to every
-- candidate quota_scope. If scopes have different tiers, stop and add scoped
-- overrides rather than pretending this global table is correct.

BEGIN;

ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS usage_thought_tokens INT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS usage_thought_tokens INT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS initial_reserved_tpm INT NULL;

UPDATE google_ai_request_attempts
SET initial_reserved_tpm = reserved_tpm
WHERE initial_reserved_tpm IS NULL;

-- Preserve the currently deployed Pacific-day trigger behavior while clearing
-- the new usage field whenever a new physical attempt becomes current.
CREATE OR REPLACE FUNCTION google_ai_sync_attempt_context()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    NEW.initial_reserved_tpm := COALESCE(NEW.initial_reserved_tpm, NEW.reserved_tpm);
    NEW.minute_bucket := COALESCE(
        NEW.minute_bucket,
        date_trunc('minute', NEW.started_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    );
    NEW.day_bucket := COALESCE(
        NEW.day_bucket,
        (NEW.started_at AT TIME ZONE 'America/Los_Angeles')::DATE
    );

    UPDATE google_ai_requests
    SET
        api_key_id = NEW.api_key_id,
        quota_scope = NEW.quota_scope,
        minute_bucket = NEW.minute_bucket,
        day_bucket = NEW.day_bucket,
        reserved_tpm = NEW.reserved_tpm,
        status = NEW.status,
        attempts = GREATEST(attempts, NEW.attempt_no),
        sent_at = NULL,
        finalized_at = NULL,
        usage_input_tokens = NULL,
        usage_output_tokens = NULL,
        usage_thought_tokens = NULL,
        usage_total_tokens = NULL,
        provider_interaction_id = NULL,
        provider_terminal_status = NULL,
        semantic_status = NULL,
        semantic_error = NULL,
        last_error_kind = NULL,
        last_error_code = NULL,
        last_error_message = NULL,
        updated_at = NOW()
    WHERE request_uid = NEW.request_uid;
    RETURN NEW;
END;
$$;

-- Release only a lease that can still be proven unsent. The attempt row is
-- deleted so rolling-60-second admission no longer counts its RPM/TPM. The
-- request row remains as bounded technical audit and can be reused safely by a
-- later explicit attempt because the attempt identity no longer exists.
CREATE OR REPLACE FUNCTION google_ai_release_unsent_v2(
    p_request_uid UUID,
    p_attempt_no INT,
    p_reason TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    v_attempt RECORD;
    v_request RECORD;
BEGIN
    SELECT a.* INTO v_attempt
    FROM google_ai_request_attempts a
    WHERE a.request_uid = p_request_uid AND a.attempt_no = p_attempt_no
    FOR UPDATE;

    SELECT r.* INTO v_request
    FROM google_ai_requests r
    WHERE r.request_uid = p_request_uid
    FOR UPDATE;

    IF v_attempt IS NULL THEN
        IF v_request IS NOT NULL AND v_request.status = 'released_unsent' THEN
            RETURN jsonb_build_object('ok', true, 'idempotent', true, 'released', true);
        END IF;
        RAISE EXCEPTION 'unsent_attempt_not_found';
    END IF;
    IF v_request IS NULL THEN
        RAISE EXCEPTION 'unsent_request_not_found';
    END IF;
    IF v_request.sent_at IS NOT NULL
       OR v_attempt.completed_at IS NOT NULL
       OR v_attempt.status <> 'reserved' THEN
        RAISE EXCEPTION 'attempt_already_sent_or_finalized';
    END IF;
    IF btrim(COALESCE(v_attempt.quota_scope, '')) = '' THEN
        RAISE EXCEPTION 'unsent_attempt_quota_scope_missing';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            'google-ai-project-model-v1:' || v_attempt.quota_scope || ':' || v_request.model,
            0
        )
    );

    IF v_attempt.minute_bucket IS NOT NULL THEN
        UPDATE google_ai_usage_counters
        SET rpm_used = GREATEST(0, rpm_used - 1),
            tpm_used = GREATEST(0, tpm_used - v_attempt.reserved_tpm),
            updated_at = NOW()
        WHERE api_key_id = v_attempt.api_key_id
          AND model = v_request.model
          AND minute_bucket = v_attempt.minute_bucket;
    END IF;

    IF v_attempt.day_bucket IS NOT NULL THEN
        UPDATE google_ai_usage_counters
        SET rpd_used = GREATEST(0, rpd_used - 1),
            updated_at = NOW()
        WHERE api_key_id = v_attempt.api_key_id
          AND model = v_request.model
          AND minute_bucket IS NULL
          AND day_bucket = v_attempt.day_bucket;
    END IF;

    DELETE FROM google_ai_request_attempts
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;

    UPDATE google_ai_requests
    SET status = 'released_unsent',
        finalized_at = NOW(),
        last_error_kind = 'pre_send',
        last_error_code = left(COALESCE(NULLIF(p_reason, ''), 'unsent_release'), 120),
        last_error_message = 'Provider send was not recorded; reservation released.',
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    RETURN jsonb_build_object('ok', true, 'idempotent', false, 'released', true);
END;
$$;

CREATE OR REPLACE FUNCTION google_ai_finalize_interaction_v2(
    p_request_uid UUID,
    p_attempt_no INT,
    p_provider_interaction_id TEXT,
    p_provider_terminal_status TEXT,
    p_semantic_status TEXT,
    p_usage_input_tokens INT,
    p_usage_output_tokens INT,
    p_usage_thought_tokens INT,
    p_usage_total_tokens INT,
    p_duration_ms INT,
    p_error_type TEXT DEFAULT NULL,
    p_error_code TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    v_request RECORD;
    v_attempt RECORD;
    v_delta INT;
    v_request_status TEXT;
BEGIN
    IF p_provider_terminal_status NOT IN (
        'requires_action',
        'completed',
        'failed',
        'cancelled',
        'incomplete',
        'budget_exceeded'
    ) THEN
        RAISE EXCEPTION 'invalid provider terminal status: %', p_provider_terminal_status;
    END IF;
    IF p_semantic_status NOT IN ('not_evaluated', 'passed', 'failed') THEN
        RAISE EXCEPTION 'invalid semantic status: %', p_semantic_status;
    END IF;
    IF p_semantic_status = 'passed' AND p_provider_terminal_status <> 'completed' THEN
        RAISE EXCEPTION 'semantic pass requires provider completed status';
    END IF;
    IF p_usage_input_tokens IS NOT NULL AND p_usage_input_tokens < 0
       OR p_usage_output_tokens IS NOT NULL AND p_usage_output_tokens < 0
       OR p_usage_thought_tokens IS NOT NULL AND p_usage_thought_tokens < 0
       OR p_usage_total_tokens IS NOT NULL AND p_usage_total_tokens < 0 THEN
        RAISE EXCEPTION 'interaction usage tokens must be nonnegative';
    END IF;

    SELECT * INTO v_request
    FROM google_ai_requests
    WHERE request_uid = p_request_uid
    FOR UPDATE;
    IF v_request IS NULL THEN
        RAISE EXCEPTION 'interaction request not found: %', p_request_uid;
    END IF;
    IF v_request.finalized_at IS NOT NULL THEN
        RETURN;
    END IF;

    SELECT * INTO v_attempt
    FROM google_ai_request_attempts
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no
    FOR UPDATE;
    IF v_attempt IS NULL THEN
        RAISE EXCEPTION 'interaction attempt not found: %/%', p_request_uid, p_attempt_no;
    END IF;

    IF btrim(COALESCE(v_attempt.quota_scope, '')) = '' THEN
        RAISE EXCEPTION 'interaction attempt quota scope missing';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            'google-ai-project-model-v1:' || v_attempt.quota_scope || ':' || v_request.model,
            0
        )
    );

    IF p_usage_total_tokens IS NOT NULL THEN
        v_delta := p_usage_total_tokens - COALESCE(
            v_attempt.initial_reserved_tpm,
            v_attempt.reserved_tpm
        );
        IF v_delta <> 0 AND v_attempt.minute_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET tpm_used = GREATEST(0, tpm_used + v_delta),
                updated_at = NOW()
            WHERE api_key_id = v_attempt.api_key_id
              AND model = v_request.model
              AND minute_bucket = v_attempt.minute_bucket;
        END IF;
    END IF;

    v_request_status := CASE
        WHEN p_semantic_status = 'failed' THEN 'failed_semantic'
        WHEN p_provider_terminal_status = 'completed'
             AND p_semantic_status = 'passed' THEN 'succeeded'
        WHEN p_provider_terminal_status = 'completed' THEN 'provider_completed'
        WHEN p_provider_terminal_status = 'requires_action' THEN 'action_required'
        WHEN p_provider_terminal_status IN ('incomplete', 'budget_exceeded')
            THEN 'provider_incomplete'
        WHEN p_provider_terminal_status = 'cancelled' THEN 'cancelled'
        ELSE 'failed_provider'
    END;

    UPDATE google_ai_requests
    SET status = v_request_status,
        provider_interaction_id = p_provider_interaction_id,
        provider_terminal_status = p_provider_terminal_status,
        semantic_status = p_semantic_status,
        finalized_at = NOW(),
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_thought_tokens = p_usage_thought_tokens,
        usage_total_tokens = p_usage_total_tokens,
        last_error_kind = CASE
            WHEN p_semantic_status = 'failed' THEN 'semantic'
            WHEN p_error_type IS NOT NULL
                 OR p_provider_terminal_status IN ('failed', 'cancelled')
                THEN 'provider'
            ELSE NULL
        END,
        last_error_code = p_error_code,
        last_error_message = left(p_error_message, 500),
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    UPDATE google_ai_request_attempts
    SET status = v_request_status,
        provider_interaction_id = p_provider_interaction_id,
        provider_terminal_status = p_provider_terminal_status,
        semantic_status = p_semantic_status,
        -- rolling-60-second admission sums attempt.reserved_tpm. Once usage is
        -- terminal, replace the effective admission value with actual total
        -- while preserving the original lease in initial_reserved_tpm.
        reserved_tpm = COALESCE(p_usage_total_tokens, reserved_tpm),
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_thought_tokens = p_usage_thought_tokens,
        usage_total_tokens = p_usage_total_tokens,
        duration_ms = GREATEST(0, p_duration_ms),
        provider_status = p_provider_terminal_status,
        provider_error_type = p_error_type,
        provider_error_code = p_error_code,
        provider_error_message = left(p_error_message, 500),
        completed_at = NOW()
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
END;
$$;

-- Owner-supplied positive finite limits, verified/canonicalized on 2026-08-24.
-- Do not update tpm_reserve_extra: no new calibration was supplied for it.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd)
VALUES
    ('gemini-3.1-flash-lite', 15, 250000, 500),
    ('gemini-3.5-flash-lite', 15, 250000, 500),
    ('gemma-4-31b-it', 30, 16000, 14400),
    ('gemini-2.5-flash', 5, 250000, 20),
    ('gemini-2.5-flash-lite', 10, 250000, 20),
    ('gemini-2.5-flash-preview-tts', 3, 10000, 10),
    ('gemini-3-flash-preview', 5, 250000, 20),
    ('gemini-3.1-flash-tts-preview', 3, 10000, 10),
    ('gemini-3.5-flash', 5, 250000, 20),
    ('gemini-3.6-flash', 5, 250000, 20),
    ('gemini-3.7-flash', 5, 250000, 20),
    ('gemini-embedding-001', 100, 30000, 1000),
    ('gemini-embedding-2', 100, 30000, 1000),
    ('gemini-robotics-er-1.6-preview', 5, 250000, 20),
    ('gemini-robotics-er-2-preview', 5, 250000, 20),
    ('gemma-4-26b-a4b-it', 30, 16000, 14400)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    updated_at = NOW();

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
        'interaction_accounting', 'google_ai_interaction_usage_v2',
        'unsent_release_supported', true
    );
$$;

REVOKE ALL ON FUNCTION google_ai_release_unsent_v2(UUID, INT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_finalize_interaction_v2(
    UUID, INT, TEXT, TEXT, TEXT, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT
) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_limiter_capabilities() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION google_ai_release_unsent_v2(UUID, INT, TEXT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_finalize_interaction_v2(
    UUID, INT, TEXT, TEXT, TEXT, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT
) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_limiter_capabilities() TO service_role;

COMMIT;
