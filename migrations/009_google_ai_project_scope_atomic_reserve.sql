-- Aggregate Google AI quotas at the Google Cloud project/model boundary.
--
-- Google enforces Gemini/Gemma quotas per Cloud project, not per API key.  A
-- key-level check therefore permits N concurrent keys from the same project to
-- oversubscribe the same RPM/TPM/RPD allowance.  This migration gives each key
-- an explicit quota_scope, groups legacy Google keys conservatively into one
-- default project scope, and serializes reserve check+increment per
-- quota_scope/model.
--
-- The RPC response carries a versioned limiter_contract on every path.  New
-- clients accept successful reservations only when that exact contract is
-- present, so deploying client code before this migration fails closed.

BEGIN;

-- 2026-07-31 provider quota evidence corrected the earlier "unlimited TPM"
-- interpretation for both Gemma 4 lanes: the project/model TPM cap is 16K and
-- RPD is 14.4K.  Keep conservative headroom at 15K TPM / 14K RPD and retain
-- the already-conservative 15 RPM until a coordinated quota review.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemma-4-31b', 15, 15000, 14000, 1000),
    ('gemma-4-26b-a4b', 15, 15000, 14000, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();

ALTER TABLE IF EXISTS google_ai_api_keys
    ADD COLUMN IF NOT EXISTS quota_scope TEXT NOT NULL DEFAULT 'google:default-project';

COMMENT ON COLUMN google_ai_api_keys.quota_scope IS
    'Stable Google Cloud project quota identity. Keys in one Cloud project must share this value.';

DO $$
BEGIN
    ALTER TABLE google_ai_api_keys
        ADD CONSTRAINT google_ai_api_keys_quota_scope_nonempty
        CHECK (btrim(quota_scope) <> '');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_google_ai_api_keys_quota_scope
    ON google_ai_api_keys (provider, quota_scope, is_active, priority);

-- Persist the scope used for each reservation.  Keeping it on the audit rows
-- prevents a later registry correction from rewriting historical meaning.
ALTER TABLE IF EXISTS google_ai_requests
    ADD COLUMN IF NOT EXISTS quota_scope TEXT NULL;
ALTER TABLE IF EXISTS google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS quota_scope TEXT NULL;

CREATE OR REPLACE FUNCTION google_ai_limiter_capabilities()
RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    SELECT jsonb_build_object(
        'limiter_contract', 'google_ai_project_model_atomic_v1',
        'quota_scope_dimension', 'google_cloud_project',
        'lock_dimension', 'quota_scope/model',
        'counter_aggregation', 'quota_scope/model/bucket'
    );
$$;

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
    v_contract CONSTANT TEXT := 'google_ai_project_model_atomic_v1';
    v_now TIMESTAMPTZ := timezone('utc', now());
    v_minute_bucket TIMESTAMPTZ := date_trunc('minute', v_now);
    v_day_bucket DATE := v_now::date;
    v_limits RECORD;
    v_key RECORD;
    v_quota_scope TEXT;
    v_checked_scopes TEXT[] := ARRAY[]::TEXT[];
    v_rpm_used BIGINT;
    v_tpm_used BIGINT;
    v_rpd_used BIGINT;
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
            'message', 'Model not found in google_ai_model_limits',
            'limiter_contract', v_contract
        );
    END IF;

    IF p_reserved_tpm IS NULL OR p_reserved_tpm < 1 THEN
        RETURN jsonb_build_object(
            'ok', false,
            'blocked_reason', 'invalid_reserved_tpm',
            'limiter_contract', v_contract
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
                    'idempotent', true
                )
                FROM google_ai_requests r
                LEFT JOIN google_ai_api_keys k ON r.api_key_id = k.id
                WHERE r.request_uid = p_request_uid
            );
        END IF;

        -- Include historical counters from inactive sibling keys: disabling or
        -- rotating a key must not reset its Cloud project's current allowance.
        SELECT
            COALESCE(SUM(c.rpm_used), 0),
            COALESCE(SUM(c.tpm_used), 0)
        INTO v_rpm_used, v_tpm_used
        FROM google_ai_usage_counters c
        JOIN google_ai_api_keys scope_key ON scope_key.id = c.api_key_id
        WHERE scope_key.provider = v_key.provider
          AND scope_key.quota_scope = v_quota_scope
          AND c.model = p_model
          AND c.minute_bucket = v_minute_bucket;

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
            v_retry_after_ms := (60 - EXTRACT(SECOND FROM v_now)::INT) * 1000;
            CONTINUE;
        END IF;

        IF v_tpm_used + p_reserved_tpm > v_limits.tpm THEN
            v_blocked_reason := 'tpm';
            v_retry_after_ms := (60 - EXTRACT(SECOND FROM v_now)::INT) * 1000;
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
        'limiter_contract', v_contract
    );
END;
$$;

-- The limiter is an internal service-role API.  SECURITY INVOKER is the
-- default; revoke PostgreSQL's implicit PUBLIC execute grant explicitly.
REVOKE ALL ON FUNCTION google_ai_limiter_capabilities() FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_reserve(UUID, INT, TEXT, TEXT, TEXT, INT, UUID[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION google_ai_limiter_capabilities() TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_reserve(
    UUID, INT, TEXT, TEXT, TEXT, INT, UUID[]
) TO service_role;

COMMIT;
