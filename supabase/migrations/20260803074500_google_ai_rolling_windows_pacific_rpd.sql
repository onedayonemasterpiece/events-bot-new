-- Close the remaining cross-runtime quota gaps found after the 2026-07-31
-- limiter cutover: use Google's Pacific RPD boundary, rolling 60-second
-- admission, and a shared provider-429 cooldown per verified quota scope/model.

BEGIN;

CREATE TABLE IF NOT EXISTS google_ai_provider_cooldowns (
    quota_scope TEXT NOT NULL,
    model TEXT NOT NULL,
    blocked_until TIMESTAMPTZ NOT NULL,
    source_api_key_id UUID NULL REFERENCES google_ai_api_keys(id) ON DELETE SET NULL,
    reason TEXT NOT NULL DEFAULT 'provider_429',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (quota_scope, model)
);
CREATE INDEX IF NOT EXISTS idx_google_ai_attempt_scope_started
    ON google_ai_request_attempts (quota_scope, started_at DESC);

REVOKE ALL ON TABLE google_ai_provider_cooldowns FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_provider_cooldowns TO service_role;

-- Re-label attempt/request evidence to the provider's documented RPD day.
UPDATE google_ai_request_attempts
SET day_bucket = (started_at AT TIME ZONE 'America/Los_Angeles')::DATE
WHERE day_bucket IS DISTINCT FROM
      (started_at AT TIME ZONE 'America/Los_Angeles')::DATE;

UPDATE google_ai_requests r
SET day_bucket = latest.day_bucket
FROM (
    SELECT DISTINCT ON (request_uid) request_uid, day_bucket
    FROM google_ai_request_attempts
    ORDER BY request_uid, attempt_no DESC
) latest
WHERE latest.request_uid = r.request_uid
  AND r.day_bucket IS DISTINCT FROM latest.day_bucket;

-- Only recent daily rows affect current admission. Rebuild them from physical
-- attempts so the UTC -> Pacific cutover neither duplicates nor loses RPD.
DELETE FROM google_ai_usage_counters
WHERE minute_bucket IS NULL
  AND day_bucket >= ((clock_timestamp() AT TIME ZONE 'America/Los_Angeles')::DATE - 1);

INSERT INTO google_ai_usage_counters
    (api_key_id, model, minute_bucket, day_bucket, rpd_used)
SELECT
    a.api_key_id,
    r.model,
    NULL,
    a.day_bucket,
    COUNT(*)::INT
FROM google_ai_request_attempts a
JOIN google_ai_requests r ON r.request_uid = a.request_uid
WHERE a.day_bucket >= ((clock_timestamp() AT TIME ZONE 'America/Los_Angeles')::DATE - 1)
GROUP BY a.api_key_id, r.model, a.day_bucket
ON CONFLICT (api_key_id, model, day_bucket)
WHERE minute_bucket IS NULL
DO UPDATE SET rpd_used = EXCLUDED.rpd_used, updated_at = NOW();

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
SET search_path = public, pg_temp
AS $$
DECLARE
    v_contract CONSTANT TEXT := 'google_ai_project_model_atomic_v1';
    v_now TIMESTAMPTZ := clock_timestamp();
    v_minute_bucket TIMESTAMPTZ := date_trunc('minute', v_now);
    v_day_bucket DATE := (v_now AT TIME ZONE 'America/Los_Angeles')::DATE;
    v_limits RECORD;
    v_key RECORD;
    v_quota_scope TEXT;
    v_checked_scopes TEXT[] := ARRAY[]::TEXT[];
    v_rpm_used BIGINT;
    v_tpm_used BIGINT;
    v_rpd_used BIGINT;
    v_retry_after_ms INT;
    v_blocked_reason TEXT;
    v_oldest_in_window TIMESTAMPTZ;
    v_provider_cooldown_until TIMESTAMPTZ;
BEGIN
    SELECT * INTO v_limits
    FROM google_ai_model_limits
    WHERE model = p_model;

    IF v_limits IS NULL THEN
        RETURN jsonb_build_object(
            'ok', false,
            'blocked_reason', 'model_not_found',
            'message', 'Model not found in google_ai_model_limits',
            'limiter_contract', v_contract,
            'bucket_strategy', 'rolling_60s_pacific_day_v2'
        );
    END IF;

    IF p_reserved_tpm IS NULL OR p_reserved_tpm < 1 THEN
        RETURN jsonb_build_object(
            'ok', false,
            'blocked_reason', 'invalid_reserved_tpm',
            'limiter_contract', v_contract,
            'bucket_strategy', 'rolling_60s_pacific_day_v2'
        );
    END IF;

    -- Fast idempotency path.  The per-scope lock path repeats this check after
    -- waiting, which closes the concurrent replay race.
    IF EXISTS (
        SELECT 1
        FROM google_ai_request_attempts
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
                'quota_scope', COALESCE(r.quota_scope, k.quota_scope),
                'limiter_contract', v_contract,
                'bucket_strategy', 'rolling_60s_pacific_day_v2',
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
          AND (p_candidate_key_ids IS NULL OR id = ANY(p_candidate_key_ids))
        ORDER BY priority, id
    LOOP
        v_quota_scope := btrim(v_key.quota_scope);

        -- Trying another key in a scope already found exhausted cannot change a
        -- project-level verdict.  Different scopes remain independent pools.
        IF v_quota_scope = ANY(v_checked_scopes) THEN
            CONTINUE;
        END IF;

        -- One transaction at a time may inspect and increment a Cloud
        -- project/model ledger.  The stable prefix makes the lock contract
        -- explicit and avoids sharing the old key/model advisory namespace.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(
                'google-ai-project-model-v1:' || v_quota_scope || ':' || p_model,
                0
            )
        );
        v_checked_scopes := array_append(v_checked_scopes, v_quota_scope);

        IF EXISTS (
            SELECT 1
            FROM google_ai_request_attempts
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
                    'quota_scope', COALESCE(r.quota_scope, k.quota_scope),
                    'limiter_contract', v_contract,
                'bucket_strategy', 'rolling_60s_pacific_day_v2',
                    'idempotent', true
                )
                FROM google_ai_requests r
                LEFT JOIN google_ai_api_keys k ON r.api_key_id = k.id
                WHERE r.request_uid = p_request_uid
            );
        END IF;

        SELECT blocked_until INTO v_provider_cooldown_until
        FROM google_ai_provider_cooldowns
        WHERE quota_scope = v_quota_scope AND model = p_model;

        IF v_provider_cooldown_until IS NOT NULL
           AND v_provider_cooldown_until > v_now THEN
            v_blocked_reason := 'provider_429';
            v_retry_after_ms := GREATEST(
                1,
                CEIL(EXTRACT(EPOCH FROM (v_provider_cooldown_until - v_now)) * 1000)::INT
            );
            CONTINUE;
        END IF;

        -- Provider RPM/TPM are rolling 60-second limits.  Fixed calendar-minute
        -- buckets are retained for audit/reconciliation only; admission is
        -- calculated from physical attempt rows under the same scope/model lock.
        SELECT
            COUNT(*),
            COALESCE(SUM(a.reserved_tpm), 0),
            MIN(a.started_at)
        INTO v_rpm_used, v_tpm_used, v_oldest_in_window
        FROM google_ai_request_attempts a
        JOIN google_ai_requests r ON r.request_uid = a.request_uid
        WHERE a.quota_scope = v_quota_scope
          AND r.model = p_model
          AND a.started_at > v_now - INTERVAL '60 seconds';

        -- RPD is a Google Cloud project/model quota that resets at midnight
        -- Pacific Time, not UTC. Daily counters are rebuilt onto that boundary
        -- during this migration and remain attributed by key for audit.
        SELECT COALESCE(SUM(c.rpd_used), 0)
        INTO v_rpd_used
        FROM google_ai_usage_counters c
        JOIN google_ai_api_keys scope_key ON scope_key.id = c.api_key_id
        WHERE scope_key.provider = v_key.provider
          AND scope_key.quota_scope = v_quota_scope
          AND c.model = p_model
          AND c.day_bucket = v_day_bucket
          AND c.minute_bucket IS NULL;

        IF v_rpm_used + 1 > v_limits.rpm THEN
            v_blocked_reason := 'rpm';
            v_retry_after_ms := GREATEST(
                1,
                CEIL(EXTRACT(EPOCH FROM (v_oldest_in_window + INTERVAL '60 seconds' - v_now)) * 1000)::INT
            );
            CONTINUE;
        END IF;

        IF v_tpm_used + p_reserved_tpm > v_limits.tpm THEN
            v_blocked_reason := 'tpm';
            v_retry_after_ms := GREATEST(
                1,
                CEIL(EXTRACT(EPOCH FROM (v_oldest_in_window + INTERVAL '60 seconds' - v_now)) * 1000)::INT
            );
            CONTINUE;
        END IF;

        IF v_rpd_used + 1 > v_limits.rpd THEN
            v_blocked_reason := 'rpd';
            v_retry_after_ms := NULL;
            CONTINUE;
        END IF;

        -- Counters remain attributed to the selected key for audit/finalize.
        -- The next reservation sums all sibling-key rows under the scope lock.
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
            minute_bucket, day_bucket, reserved_tpm, status, quota_scope
        ) VALUES (
            p_request_uid, p_consumer, p_account_name, p_model, v_key.id,
            v_minute_bucket, v_day_bucket, p_reserved_tpm, 'reserved', v_quota_scope
        )
        ON CONFLICT (request_uid) DO NOTHING;

        INSERT INTO google_ai_request_attempts (
            request_uid, attempt_no, status, api_key_id, reserved_tpm, quota_scope
        ) VALUES (
            p_request_uid, p_attempt_no, 'reserved', v_key.id, p_reserved_tpm,
            v_quota_scope
        );

        RETURN jsonb_build_object(
            'ok', true,
            'api_key_id', v_key.id,
            'env_var_name', v_key.env_var_name,
            'key_alias', v_key.key_alias,
            'minute_bucket', v_minute_bucket,
            'day_bucket', v_day_bucket,
            'quota_scope', v_quota_scope,
            'limiter_contract', v_contract,
            'bucket_strategy', 'rolling_60s_pacific_day_v2',
            'limits', jsonb_build_object(
                'rpm', v_limits.rpm,
                'tpm', v_limits.tpm,
                'rpd', v_limits.rpd
            ),
            'used_after', jsonb_build_object(
                'rpm', v_rpm_used + 1,
                'tpm', v_tpm_used + p_reserved_tpm,
                'rpd', v_rpd_used + 1
            )
        );
    END LOOP;

    RETURN jsonb_build_object(
        'ok', false,
        'blocked_reason', COALESCE(v_blocked_reason, 'no_keys'),
        'retry_after_ms', v_retry_after_ms,
        'minute_bucket', v_minute_bucket,
        'day_bucket', v_day_bucket,
        'limiter_contract', v_contract,
        'bucket_strategy', 'rolling_60s_pacific_day_v2'
    );
END;
$$;


CREATE OR REPLACE FUNCTION google_ai_report_provider_429(
    p_request_uid UUID,
    p_attempt_no INT,
    p_retry_after_ms INT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    v_attempt RECORD;
    v_delay_ms INT;
BEGIN
    SELECT a.*, r.model INTO v_attempt
    FROM google_ai_request_attempts a
    JOIN google_ai_requests r ON r.request_uid = a.request_uid
    WHERE a.request_uid = p_request_uid AND a.attempt_no = p_attempt_no;

    IF v_attempt IS NULL OR btrim(COALESCE(v_attempt.quota_scope, '')) = '' THEN
        RAISE EXCEPTION 'provider_429_attempt_not_found';
    END IF;

    -- Preserve a provider-supplied long daily retry interval.  The upper bound
    -- covers the longest Pacific calendar day plus clock skew; truncating it to
    -- a few minutes would recreate an RPD retry storm.
    v_delay_ms := LEAST(93600000, GREATEST(5000, COALESCE(p_retry_after_ms, 60000)));
    INSERT INTO google_ai_provider_cooldowns (
        quota_scope, model, blocked_until, source_api_key_id, reason, updated_at
    ) VALUES (
        v_attempt.quota_scope,
        v_attempt.model,
        clock_timestamp() + make_interval(secs => v_delay_ms / 1000.0),
        v_attempt.api_key_id,
        'provider_429',
        NOW()
    )
    ON CONFLICT (quota_scope, model) DO UPDATE SET
        blocked_until = GREATEST(
            google_ai_provider_cooldowns.blocked_until,
            EXCLUDED.blocked_until
        ),
        source_api_key_id = EXCLUDED.source_api_key_id,
        reason = EXCLUDED.reason,
        updated_at = NOW();
END;
$$;

CREATE OR REPLACE FUNCTION google_ai_sync_attempt_context()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
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
        usage_total_tokens = NULL,
        last_error_kind = NULL,
        last_error_code = NULL,
        last_error_message = NULL,
        updated_at = NOW()
    WHERE request_uid = NEW.request_uid;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION google_ai_report_provider_429(UUID, INT, INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION google_ai_report_provider_429(UUID, INT, INT) TO service_role;

COMMIT;
