-- Make shared Google AI reservation check+increment atomic per key/model.
-- Additive rollout for databases that already applied migration 002.

BEGIN;

CREATE OR REPLACE FUNCTION google_ai_reserve(
    p_request_uid UUID,
    p_attempt_no INT,
    p_consumer TEXT,
    p_account_name TEXT,
    p_model TEXT,
    p_reserved_tpm INT,
    p_candidate_key_ids UUID[] DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := timezone('utc', now());
    v_minute_bucket TIMESTAMPTZ := date_trunc('minute', v_now);
    v_day_bucket DATE := v_now::date;
    v_limits RECORD;
    v_key RECORD;
    v_minute_used RECORD;
    v_day_used RECORD;
    v_retry_after_ms INT;
    v_blocked_reason TEXT;
BEGIN
    SELECT * INTO v_limits FROM google_ai_model_limits WHERE model = p_model;
    IF v_limits IS NULL THEN
        RETURN jsonb_build_object(
            'ok', false,
            'blocked_reason', 'model_not_found',
            'message', 'Model not found in google_ai_model_limits'
        );
    END IF;

    IF EXISTS (
        SELECT 1 FROM google_ai_request_attempts
        WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no
    ) THEN
        RETURN (
            SELECT jsonb_build_object(
                'ok', true,
                'api_key_id', r.api_key_id,
                'env_var_name', k.env_var_name,
                'key_alias', k.key_alias,
                'minute_bucket', r.minute_bucket,
                'day_bucket', r.day_bucket,
                'idempotent', true
            )
            FROM google_ai_requests r
            LEFT JOIN google_ai_api_keys k ON r.api_key_id = k.id
            WHERE r.request_uid = p_request_uid
        );
    END IF;

    FOR v_key IN
        SELECT * FROM google_ai_api_keys
        WHERE is_active = true
          AND (p_candidate_key_ids IS NULL OR id = ANY(p_candidate_key_ids))
        ORDER BY priority, id
    LOOP
        -- Serialize check+increment for one key/model across concurrent
        -- workers. Without this lock two transactions can both observe the
        -- same counters below the cap and oversubscribe RPM/TPM/RPD.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(v_key.id::text || ':' || p_model, 0)
        );

        -- A concurrent replay of the same request may have committed while we
        -- waited for the key lock. Re-check idempotency before incrementing.
        IF EXISTS (
            SELECT 1 FROM google_ai_request_attempts
            WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no
        ) THEN
            RETURN (
                SELECT jsonb_build_object(
                    'ok', true,
                    'api_key_id', r.api_key_id,
                    'env_var_name', k.env_var_name,
                    'key_alias', k.key_alias,
                    'minute_bucket', r.minute_bucket,
                    'day_bucket', r.day_bucket,
                    'idempotent', true
                )
                FROM google_ai_requests r
                LEFT JOIN google_ai_api_keys k ON r.api_key_id = k.id
                WHERE r.request_uid = p_request_uid
            );
        END IF;

        SELECT rpm_used, tpm_used INTO v_minute_used
        FROM google_ai_usage_counters
        WHERE api_key_id = v_key.id
          AND model = p_model
          AND minute_bucket = v_minute_bucket;

        IF v_minute_used IS NULL THEN
            v_minute_used.rpm_used := 0;
            v_minute_used.tpm_used := 0;
        END IF;

        SELECT rpd_used INTO v_day_used
        FROM google_ai_usage_counters
        WHERE api_key_id = v_key.id
          AND model = p_model
          AND day_bucket = v_day_bucket
          AND minute_bucket IS NULL;

        IF v_day_used IS NULL THEN
            v_day_used.rpd_used := 0;
        END IF;

        IF v_minute_used.rpm_used + 1 > v_limits.rpm THEN
            v_blocked_reason := 'rpm';
            v_retry_after_ms := (60 - EXTRACT(SECOND FROM v_now)::INT) * 1000;
            CONTINUE;
        END IF;

        IF v_minute_used.tpm_used + p_reserved_tpm > v_limits.tpm THEN
            v_blocked_reason := 'tpm';
            v_retry_after_ms := (60 - EXTRACT(SECOND FROM v_now)::INT) * 1000;
            CONTINUE;
        END IF;

        IF v_day_used.rpd_used + 1 > v_limits.rpd THEN
            v_blocked_reason := 'rpd';
            v_retry_after_ms := NULL;
            CONTINUE;
        END IF;

        INSERT INTO google_ai_usage_counters
            (api_key_id, model, minute_bucket, day_bucket, rpm_used, tpm_used)
        VALUES
            (v_key.id, p_model, v_minute_bucket, v_day_bucket, 1, p_reserved_tpm)
        ON CONFLICT (api_key_id, model, minute_bucket)
        WHERE minute_bucket IS NOT NULL
        DO UPDATE SET
            rpm_used = google_ai_usage_counters.rpm_used + 1,
            tpm_used = google_ai_usage_counters.tpm_used + p_reserved_tpm,
            updated_at = NOW();

        INSERT INTO google_ai_usage_counters
            (api_key_id, model, minute_bucket, day_bucket, rpd_used)
        VALUES
            (v_key.id, p_model, NULL, v_day_bucket, 1)
        ON CONFLICT (api_key_id, model, day_bucket)
        WHERE minute_bucket IS NULL
        DO UPDATE SET
            rpd_used = google_ai_usage_counters.rpd_used + 1,
            updated_at = NOW();

        INSERT INTO google_ai_requests (
            request_uid, consumer, account_name, model, api_key_id,
            minute_bucket, day_bucket, reserved_tpm, status
        ) VALUES (
            p_request_uid, p_consumer, p_account_name, p_model, v_key.id,
            v_minute_bucket, v_day_bucket, p_reserved_tpm, 'reserved'
        )
        ON CONFLICT (request_uid) DO NOTHING;

        INSERT INTO google_ai_request_attempts (
            request_uid, attempt_no, status, api_key_id, reserved_tpm
        ) VALUES (
            p_request_uid, p_attempt_no, 'reserved', v_key.id, p_reserved_tpm
        );

        RETURN jsonb_build_object(
            'ok', true,
            'api_key_id', v_key.id,
            'env_var_name', v_key.env_var_name,
            'key_alias', v_key.key_alias,
            'minute_bucket', v_minute_bucket,
            'day_bucket', v_day_bucket,
            'limits', jsonb_build_object('rpm', v_limits.rpm, 'tpm', v_limits.tpm, 'rpd', v_limits.rpd),
            'used_after', jsonb_build_object(
                'rpm', v_minute_used.rpm_used + 1,
                'tpm', v_minute_used.tpm_used + p_reserved_tpm,
                'rpd', v_day_used.rpd_used + 1
            )
        );
    END LOOP;

    RETURN jsonb_build_object(
        'ok', false,
        'blocked_reason', COALESCE(v_blocked_reason, 'no_keys'),
        'retry_after_ms', v_retry_after_ms,
        'minute_bucket', v_minute_bucket,
        'day_bucket', v_day_bucket
    );
END;
$$;

GRANT EXECUTE ON FUNCTION google_ai_reserve(
    UUID, INT, TEXT, TEXT, TEXT, INT, UUID[]
) TO service_role;

COMMIT;
