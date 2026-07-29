-- Register Gemini TTS in the shared Google AI limiter.
--
-- Operator-confirmed project guard on 2026-07-29:
--   * shared TTS family quota: 10 requests/day
--
-- Both provider model IDs share one local quota_scope, so switching model names
-- cannot double the daily allowance.  RPM=1 is an intentionally conservative
-- internal burst guard.  TPM is not used as a provider claim for audio output;
-- the provider model's own token limits still apply.

BEGIN;

ALTER TABLE google_ai_model_limits
    ADD COLUMN IF NOT EXISTS quota_scope TEXT;
UPDATE google_ai_model_limits
SET quota_scope = model
WHERE quota_scope IS NULL OR BTRIM(quota_scope) = '';
ALTER TABLE google_ai_model_limits
    ALTER COLUMN quota_scope SET NOT NULL;

ALTER TABLE google_ai_usage_counters
    ADD COLUMN IF NOT EXISTS quota_scope TEXT;
UPDATE google_ai_usage_counters
SET quota_scope = model
WHERE quota_scope IS NULL OR BTRIM(quota_scope) = '';
ALTER TABLE google_ai_usage_counters
    ALTER COLUMN quota_scope SET NOT NULL;

ALTER TABLE google_ai_requests
    ADD COLUMN IF NOT EXISTS quota_scope TEXT;
UPDATE google_ai_requests
SET quota_scope = model
WHERE quota_scope IS NULL OR BTRIM(quota_scope) = '';
ALTER TABLE google_ai_requests
    ALTER COLUMN quota_scope SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_google_ai_usage_scope_minute
    ON google_ai_usage_counters (api_key_id, quota_scope, minute_bucket)
    WHERE minute_bucket IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_google_ai_usage_scope_day
    ON google_ai_usage_counters (api_key_id, quota_scope, day_bucket)
    WHERE minute_bucket IS NULL;

-- One active registry lane per provider/env secret.  Duplicate metadata rows
-- would create multiple independent local counters for the same raw key.
CREATE UNIQUE INDEX IF NOT EXISTS idx_google_ai_api_keys_active_provider_env
    ON google_ai_api_keys (provider, env_var_name)
    WHERE is_active = true;

UPDATE google_ai_model_limits AS m
SET
    quota_scope = s.quota_scope,
    rpm = s.rpm,
    tpm = s.tpm,
    rpd = s.rpd,
    tpm_reserve_extra = s.tpm_reserve_extra,
    updated_at = NOW()
FROM (
    VALUES
        ('gemini-2.5-flash-preview-tts', 'google-tts', 1, 2147483647, 10, 0),
        ('gemini-3.1-flash-tts-preview', 'google-tts', 1, 2147483647, 10, 0)
) AS s(model, quota_scope, rpm, tpm, rpd, tpm_reserve_extra)
WHERE m.model = s.model;

INSERT INTO google_ai_model_limits
    (model, quota_scope, rpm, tpm, rpd, tpm_reserve_extra)
SELECT
    s.model, s.quota_scope, s.rpm, s.tpm, s.rpd, s.tpm_reserve_extra
FROM (
    VALUES
        ('gemini-2.5-flash-preview-tts', 'google-tts', 1, 2147483647, 10, 0),
        ('gemini-3.1-flash-tts-preview', 'google-tts', 1, 2147483647, 10, 0)
) AS s(model, quota_scope, rpm, tpm, rpd, tpm_reserve_extra)
WHERE NOT EXISTS (
    SELECT 1 FROM google_ai_model_limits m WHERE m.model = s.model
);

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
    v_quota_scope TEXT;
    v_key RECORD;
    v_minute_used RECORD;
    v_day_used RECORD;
    v_retry_after_ms INT;
    v_blocked_reason TEXT;
BEGIN
    SELECT * INTO v_limits
    FROM google_ai_model_limits
    WHERE model = p_model;
    IF v_limits IS NULL THEN
        RETURN jsonb_build_object(
            'ok', false,
            'blocked_reason', 'model_not_found',
            'message', 'Model not found in google_ai_model_limits'
        );
    END IF;
    v_quota_scope := COALESCE(NULLIF(BTRIM(v_limits.quota_scope), ''), p_model);

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
                'quota_scope', r.quota_scope,
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
        SELECT *
        FROM google_ai_api_keys
        WHERE is_active = true
          AND provider = 'google'
          AND (p_candidate_key_ids IS NULL OR id = ANY(p_candidate_key_ids))
        ORDER BY priority, id
    LOOP
        -- Serialize the check+increment boundary for this key and shared model
        -- family.  Without the lock, two transactions can both observe 9/10.
        PERFORM pg_advisory_xact_lock(
            hashtext(v_key.id::TEXT),
            hashtext(v_quota_scope)
        );

        SELECT rpm_used, tpm_used INTO v_minute_used
        FROM google_ai_usage_counters
        WHERE api_key_id = v_key.id
          AND quota_scope = v_quota_scope
          AND minute_bucket = v_minute_bucket;

        IF v_minute_used IS NULL THEN
            v_minute_used.rpm_used := 0;
            v_minute_used.tpm_used := 0;
        END IF;

        SELECT rpd_used INTO v_day_used
        FROM google_ai_usage_counters
        WHERE api_key_id = v_key.id
          AND quota_scope = v_quota_scope
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
            (api_key_id, model, quota_scope, minute_bucket, day_bucket, rpm_used, tpm_used)
        VALUES
            (v_key.id, p_model, v_quota_scope, v_minute_bucket, v_day_bucket, 1, p_reserved_tpm)
        ON CONFLICT (api_key_id, quota_scope, minute_bucket)
        WHERE minute_bucket IS NOT NULL
        DO UPDATE SET
            rpm_used = google_ai_usage_counters.rpm_used + 1,
            tpm_used = google_ai_usage_counters.tpm_used + p_reserved_tpm,
            updated_at = NOW();

        INSERT INTO google_ai_usage_counters
            (api_key_id, model, quota_scope, minute_bucket, day_bucket, rpd_used)
        VALUES
            (v_key.id, p_model, v_quota_scope, NULL, v_day_bucket, 1)
        ON CONFLICT (api_key_id, quota_scope, day_bucket)
        WHERE minute_bucket IS NULL
        DO UPDATE SET
            rpd_used = google_ai_usage_counters.rpd_used + 1,
            updated_at = NOW();

        INSERT INTO google_ai_requests (
            request_uid, consumer, account_name, model, quota_scope, api_key_id,
            minute_bucket, day_bucket, reserved_tpm, status
        ) VALUES (
            p_request_uid, p_consumer, p_account_name, p_model, v_quota_scope,
            v_key.id, v_minute_bucket, v_day_bucket, p_reserved_tpm, 'reserved'
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
            'quota_scope', v_quota_scope,
            'minute_bucket', v_minute_bucket,
            'day_bucket', v_day_bucket,
            'limits', jsonb_build_object(
                'rpm', v_limits.rpm,
                'tpm', v_limits.tpm,
                'rpd', v_limits.rpd
            ),
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
        'quota_scope', v_quota_scope,
        'minute_bucket', v_minute_bucket,
        'day_bucket', v_day_bucket
    );
END;
$$;

-- Finalize TPM reconciliation against the shared quota scope rather than the
-- exact provider alias used for the request.
CREATE OR REPLACE FUNCTION google_ai_finalize(
    p_request_uid UUID,
    p_attempt_no INT,
    p_usage_input_tokens INT,
    p_usage_output_tokens INT,
    p_usage_total_tokens INT,
    p_duration_ms INT,
    p_provider_status TEXT,
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
BEGIN
    SELECT * INTO v_request
    FROM google_ai_requests
    WHERE request_uid = p_request_uid;
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
            SET tpm_used = tpm_used + v_delta, updated_at = NOW()
            WHERE api_key_id = v_request.api_key_id
              AND quota_scope = v_request.quota_scope
              AND minute_bucket = v_request.minute_bucket;
        END IF;
    END IF;

    UPDATE google_ai_requests
    SET
        status = CASE WHEN p_error_type IS NULL THEN 'succeeded' ELSE 'failed_provider' END,
        finalized_at = NOW(),
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_total_tokens = p_usage_total_tokens,
        last_error_kind = CASE WHEN p_error_type IS NOT NULL THEN 'provider' ELSE NULL END,
        last_error_code = p_error_code,
        last_error_message = p_error_message,
        updated_at = NOW()
    WHERE request_uid = p_request_uid;

    UPDATE google_ai_request_attempts
    SET
        status = CASE WHEN p_error_type IS NULL THEN 'succeeded' ELSE 'failed_provider' END,
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_total_tokens = p_usage_total_tokens,
        duration_ms = p_duration_ms,
        provider_status = p_provider_status,
        provider_error_type = p_error_type,
        provider_error_code = p_error_code,
        provider_error_message = p_error_message,
        completed_at = NOW()
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
END;
$$;

GRANT EXECUTE ON FUNCTION google_ai_reserve(UUID, INT, TEXT, TEXT, TEXT, INT, UUID[])
    TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_finalize(UUID, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT, TEXT)
    TO service_role;

-- Stale reservations must refund the shared scope counter even when the exact
-- provider model alias differs from the alias that first created that row.
CREATE OR REPLACE FUNCTION google_ai_sweep_stale(
    p_older_than_minutes INT DEFAULT 30,
    p_limit INT DEFAULT 500
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := timezone('utc', now());
    v_cutoff TIMESTAMPTZ := v_now - make_interval(mins => GREATEST(1, p_older_than_minutes));
    v_limit INT := GREATEST(1, LEAST(p_limit, 5000));
    v_req RECORD;
    v_swept INT := 0;
    v_ids UUID[] := ARRAY[]::UUID[];
BEGIN
    FOR v_req IN
        SELECT
            r.request_uid,
            r.api_key_id,
            r.quota_scope,
            r.minute_bucket,
            r.day_bucket,
            COALESCE(r.reserved_rpm, 1) AS reserved_rpm,
            COALESCE(r.reserved_tpm, 0) AS reserved_tpm,
            COALESCE(r.reserved_rpd, 1) AS reserved_rpd
        FROM google_ai_requests r
        WHERE r.status = 'reserved'
          AND r.sent_at IS NULL
          AND r.finalized_at IS NULL
          AND r.created_at < v_cutoff
        ORDER BY r.created_at
        LIMIT v_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        IF v_req.minute_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET
                rpm_used = GREATEST(0, COALESCE(rpm_used, 0) - v_req.reserved_rpm),
                tpm_used = GREATEST(0, COALESCE(tpm_used, 0) - v_req.reserved_tpm),
                updated_at = NOW()
            WHERE api_key_id = v_req.api_key_id
              AND quota_scope = v_req.quota_scope
              AND minute_bucket = v_req.minute_bucket;
        END IF;

        IF v_req.day_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET
                rpd_used = GREATEST(0, COALESCE(rpd_used, 0) - v_req.reserved_rpd),
                updated_at = NOW()
            WHERE api_key_id = v_req.api_key_id
              AND quota_scope = v_req.quota_scope
              AND day_bucket = v_req.day_bucket
              AND minute_bucket IS NULL;
        END IF;

        UPDATE google_ai_requests
        SET
            status = 'stale',
            last_error_kind = 'stale',
            last_error_code = 'reserve_not_sent_timeout',
            last_error_message = 'swept stale reserved (sent_at is null)',
            finalized_at = COALESCE(finalized_at, NOW()),
            updated_at = NOW()
        WHERE request_uid = v_req.request_uid;

        UPDATE google_ai_request_attempts
        SET
            status = 'stale',
            provider_error_type = COALESCE(provider_error_type, 'stale'),
            provider_error_code = COALESCE(provider_error_code, 'reserve_not_sent_timeout'),
            provider_error_message = COALESCE(
                provider_error_message,
                'swept stale reserved (sent_at is null)'
            ),
            completed_at = COALESCE(completed_at, NOW())
        WHERE request_uid = v_req.request_uid
          AND status = 'reserved';

        v_swept := v_swept + 1;
        v_ids := array_append(v_ids, v_req.request_uid);
    END LOOP;

    RETURN jsonb_build_object(
        'ok', true,
        'swept', v_swept,
        'cutoff', v_cutoff,
        'request_uids', v_ids
    );
END;
$$;

GRANT EXECUTE ON FUNCTION google_ai_sweep_stale(INT, INT) TO service_role;

-- Idempotently import the one successful direct TTS request declared by the
-- operator.  A 503 and one success were observed, but the quota UI is the
-- accounting source of truth and reported one used request.  This synthetic
-- succeeded record is never eligible for stale-reservation compensation.
DO $$
DECLARE
    v_key_id UUID;
    v_key_count INT;
    v_inserted INT;
    v_request_uid UUID := '09115e9c-ad36-5ca1-af27-da29aad439c7';
BEGIN
    SELECT COUNT(*), (ARRAY_AGG(id ORDER BY id))[1]
    INTO v_key_count, v_key_id
    FROM google_ai_api_keys
    WHERE provider = 'google'
      AND env_var_name = 'GOOGLE_API_KEY'
      AND is_active = true;

    IF v_key_count != 1 THEN
        RAISE EXCEPTION
            'Expected exactly one active GOOGLE_API_KEY registry row, got %',
            v_key_count;
    END IF;

    INSERT INTO google_ai_requests (
        request_uid,
        consumer,
        provider,
        model,
        quota_scope,
        api_key_id,
        day_bucket,
        reserved_rpm,
        reserved_tpm,
        reserved_rpd,
        status,
        attempts,
        sent_at,
        finalized_at,
        meta,
        created_at,
        updated_at
    ) VALUES (
        v_request_uid,
        'codex_google_tts_manual_backfill',
        'google',
        'gemini-2.5-flash-preview-tts',
        'google-tts',
        v_key_id,
        DATE '2026-07-29',
        1,
        1,
        1,
        'succeeded',
        1,
        TIMESTAMPTZ '2026-07-29 17:27:41+00',
        TIMESTAMPTZ '2026-07-29 17:27:41+00',
        '{"manual_backfill":true,"source":"user_declared_usage","declared_rpd_delta":1}'::JSONB,
        TIMESTAMPTZ '2026-07-29 17:27:41+00',
        NOW()
    )
    ON CONFLICT (request_uid) DO NOTHING;
    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    IF v_inserted = 1 THEN
        INSERT INTO google_ai_request_attempts (
            request_uid,
            attempt_no,
            status,
            api_key_id,
            reserved_tpm,
            provider_status,
            started_at,
            completed_at,
            meta
        ) VALUES (
            v_request_uid,
            1,
            'succeeded',
            v_key_id,
            1,
            'succeeded',
            TIMESTAMPTZ '2026-07-29 17:27:41+00',
            TIMESTAMPTZ '2026-07-29 17:27:41+00',
            '{"manual_backfill":true}'::JSONB
        );

        INSERT INTO google_ai_usage_counters (
            api_key_id,
            model,
            quota_scope,
            minute_bucket,
            day_bucket,
            rpd_used
        ) VALUES (
            v_key_id,
            'gemini-2.5-flash-preview-tts',
            'google-tts',
            NULL,
            DATE '2026-07-29',
            1
        )
        ON CONFLICT (api_key_id, quota_scope, day_bucket)
        WHERE minute_bucket IS NULL
        DO UPDATE SET
            rpd_used = google_ai_usage_counters.rpd_used + 1,
            updated_at = NOW();
    END IF;
END;
$$;

COMMIT;
