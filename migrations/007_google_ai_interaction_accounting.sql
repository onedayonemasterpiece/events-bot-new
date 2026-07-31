-- Strict Interactions API accounting for managed agents.
--
-- Additive only: existing GenerateContent reserve/mark_sent/finalize RPCs and
-- their callers are unchanged.  The new finalize RPC keeps provider terminal
-- state separate from the downstream semantic-quality verdict.
-- Antigravity limits remain owned by migration 006 (54 RPM / 96000 TPM /
-- 90 RPD per registered key, preserving the verified safety headroom).

BEGIN;

ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS provider_interaction_id TEXT NULL;
ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS provider_terminal_status TEXT NULL;
ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS semantic_status TEXT NULL;
ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS semantic_error TEXT NULL;

ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS provider_interaction_id TEXT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS provider_terminal_status TEXT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS semantic_status TEXT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS semantic_error TEXT NULL;

CREATE OR REPLACE FUNCTION google_ai_finalize_interaction(
    p_request_uid UUID,
    p_attempt_no INT,
    p_provider_interaction_id TEXT,
    p_provider_terminal_status TEXT,
    p_semantic_status TEXT,
    p_usage_input_tokens INT,
    p_usage_output_tokens INT,
    p_usage_total_tokens INT,
    p_duration_ms INT,
    p_error_type TEXT DEFAULT NULL,
    p_error_code TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_request RECORD;
    v_reserved_tpm INT;
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
    IF p_semantic_status = 'passed' AND p_provider_terminal_status != 'completed' THEN
        RAISE EXCEPTION 'semantic pass requires provider completed status';
    END IF;

    SELECT * INTO v_request
    FROM google_ai_requests
    WHERE request_uid = p_request_uid
    FOR UPDATE;
    IF v_request IS NULL OR v_request.finalized_at IS NOT NULL THEN
        RETURN;
    END IF;

    SELECT reserved_tpm INTO v_reserved_tpm
    FROM google_ai_request_attempts
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;

    IF p_usage_total_tokens IS NOT NULL AND v_reserved_tpm IS NOT NULL THEN
        v_delta := p_usage_total_tokens - v_reserved_tpm;
        IF v_delta != 0 AND v_request.minute_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET tpm_used = GREATEST(0, tpm_used + v_delta),
                updated_at = NOW()
            WHERE api_key_id = v_request.api_key_id
              AND model = v_request.model
              AND minute_bucket = v_request.minute_bucket;
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
        usage_total_tokens = p_usage_total_tokens,
        last_error_kind = CASE
            WHEN p_semantic_status = 'failed' THEN 'semantic'
            WHEN p_error_type IS NOT NULL
                 OR p_provider_terminal_status IN ('failed', 'cancelled')
                THEN 'provider'
            ELSE NULL
        END,
        last_error_code = p_error_code,
        last_error_message = p_error_message,
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    UPDATE google_ai_request_attempts
    SET status = v_request_status,
        provider_interaction_id = p_provider_interaction_id,
        provider_terminal_status = p_provider_terminal_status,
        semantic_status = p_semantic_status,
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_total_tokens = p_usage_total_tokens,
        duration_ms = GREATEST(0, p_duration_ms),
        provider_status = p_provider_terminal_status,
        provider_error_type = p_error_type,
        provider_error_code = p_error_code,
        provider_error_message = p_error_message,
        completed_at = NOW()
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
END;
$$;

CREATE OR REPLACE FUNCTION google_ai_record_interaction_semantic(
    p_request_uid UUID,
    p_attempt_no INT,
    p_semantic_status TEXT,
    p_semantic_error TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_terminal_status TEXT;
    v_status TEXT;
BEGIN
    IF p_semantic_status NOT IN ('passed', 'failed') THEN
        RAISE EXCEPTION 'invalid semantic status: %', p_semantic_status;
    END IF;

    SELECT provider_terminal_status INTO v_terminal_status
    FROM google_ai_requests
    WHERE request_uid = p_request_uid
    FOR UPDATE;
    IF v_terminal_status IS NULL THEN
        RAISE EXCEPTION 'interaction request is not provider-finalized: %', p_request_uid;
    END IF;
    IF p_semantic_status = 'passed' AND v_terminal_status != 'completed' THEN
        RAISE EXCEPTION 'semantic pass requires provider completed status';
    END IF;

    v_status := CASE
        WHEN p_semantic_status = 'failed' THEN 'failed_semantic'
        ELSE 'succeeded'
    END;

    UPDATE google_ai_requests
    SET semantic_status = p_semantic_status,
        semantic_error = p_semantic_error,
        status = v_status,
        last_error_kind = CASE
            WHEN p_semantic_status = 'failed' THEN 'semantic'
            ELSE NULL
        END,
        last_error_message = CASE
            WHEN p_semantic_status = 'failed' THEN p_semantic_error
            ELSE NULL
        END,
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    UPDATE google_ai_request_attempts
    SET semantic_status = p_semantic_status,
        semantic_error = p_semantic_error,
        status = v_status
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
END;
$$;

GRANT EXECUTE ON FUNCTION google_ai_finalize_interaction(
    UUID, INT, TEXT, TEXT, TEXT, INT, INT, INT, INT, TEXT, TEXT, TEXT
) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_record_interaction_semantic(
    UUID, INT, TEXT, TEXT
) TO service_role;

COMMIT;
