-- Canonical Google AI limiter bootstrap for the personalization Supabase project.
--
-- This migration is self-contained and additive: the target project currently
-- has no google_ai_reserve schema.  It stores key metadata only (never secret
-- values), enforces project/model atomic admission, exposes a versioned
-- capability contract, and includes strict external/Antigravity accounting.
--
-- CUTOVER GATE (do not send provider traffic merely because this SQL applied):
--   1. Populate google_ai_api_keys with redacted env metadata and an audited
--      quota_scope shared by every key from the same Google Cloud project.
--   2. Verify google_ai_limiter_capabilities().limiter_contract equals
--      google_ai_project_model_atomic_v1 and perform a transactional reserve
--      smoke without calling Google.
--   3. Drain/finalize leases on the old ledger, then switch every Fly/Kaggle/
--      Edge/local concurrent client to this one project together. Split-ledger
--      traffic is not quota-safe; an old Antigravity lease must finalize where
--      it was reserved.
--   4. Keep direct Google callers disabled until they use reserve/mark/finalize.
--
-- No API-key secret is inserted by this migration.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS google_ai_model_limits (
    model TEXT PRIMARY KEY,
    rpm INT NOT NULL CHECK (rpm > 0),
    tpm INT NOT NULL CHECK (tpm > 0),
    rpd INT NOT NULL CHECK (rpd > 0),
    tpm_reserve_extra INT NOT NULL DEFAULT 1000 CHECK (tpm_reserve_extra >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS google_ai_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_alias TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT 'google',
    env_var_name TEXT NOT NULL,
    account_name TEXT NULL,
    quota_scope TEXT NOT NULL DEFAULT 'google:default-project',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INT NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NULL,
    CONSTRAINT google_ai_api_keys_alias_unique UNIQUE (provider, key_alias),
    CONSTRAINT google_ai_api_keys_quota_scope_nonempty CHECK (btrim(quota_scope) <> '')
);
COMMENT ON COLUMN google_ai_api_keys.quota_scope IS
    'Stable Google Cloud project quota identity. Keys in one Cloud project must share this value.';
CREATE INDEX IF NOT EXISTS idx_google_ai_api_keys_active
    ON google_ai_api_keys (is_active, priority);
CREATE INDEX IF NOT EXISTS idx_google_ai_api_keys_quota_scope
    ON google_ai_api_keys (provider, quota_scope, is_active, priority);

CREATE TABLE IF NOT EXISTS google_ai_usage_counters (
    id BIGSERIAL PRIMARY KEY,
    api_key_id UUID NOT NULL REFERENCES google_ai_api_keys(id),
    model TEXT NOT NULL,
    minute_bucket TIMESTAMPTZ NULL,
    day_bucket DATE NOT NULL,
    rpm_used INT NOT NULL DEFAULT 0 CHECK (rpm_used >= 0),
    tpm_used INT NOT NULL DEFAULT 0 CHECK (tpm_used >= 0),
    rpd_used INT NOT NULL DEFAULT 0 CHECK (rpd_used >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_google_ai_usage_counters_minute
    ON google_ai_usage_counters (api_key_id, model, minute_bucket)
    WHERE minute_bucket IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_google_ai_usage_counters_day
    ON google_ai_usage_counters (api_key_id, model, day_bucket)
    WHERE minute_bucket IS NULL;
CREATE INDEX IF NOT EXISTS idx_google_ai_usage_counters_minute_bucket
    ON google_ai_usage_counters (minute_bucket)
    WHERE minute_bucket IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_google_ai_usage_counters_day_bucket
    ON google_ai_usage_counters (day_bucket)
    WHERE minute_bucket IS NULL;

CREATE TABLE IF NOT EXISTS google_ai_requests (
    request_uid UUID PRIMARY KEY,
    consumer TEXT NOT NULL,
    account_name TEXT NULL,
    provider TEXT NOT NULL DEFAULT 'google',
    model TEXT NOT NULL,
    api_key_id UUID NULL REFERENCES google_ai_api_keys(id),
    quota_scope TEXT NULL,
    minute_bucket TIMESTAMPTZ NULL,
    day_bucket DATE NULL,
    reserved_rpm INT NOT NULL DEFAULT 1,
    reserved_tpm INT NOT NULL,
    reserved_rpd INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'reserved',
    attempts INT NOT NULL DEFAULT 1,
    last_error_kind TEXT NULL,
    last_error_code TEXT NULL,
    last_error_message TEXT NULL,
    sent_at TIMESTAMPTZ NULL,
    finalized_at TIMESTAMPTZ NULL,
    usage_input_tokens INT NULL,
    usage_output_tokens INT NULL,
    usage_total_tokens INT NULL,
    provider_interaction_id TEXT NULL,
    provider_terminal_status TEXT NULL,
    semantic_status TEXT NULL,
    semantic_error TEXT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_google_ai_requests_created
    ON google_ai_requests (created_at);
CREATE INDEX IF NOT EXISTS idx_google_ai_requests_consumer
    ON google_ai_requests (consumer, created_at);
CREATE INDEX IF NOT EXISTS idx_google_ai_requests_status
    ON google_ai_requests (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_google_ai_requests_quota_scope
    ON google_ai_requests (quota_scope, model, created_at);

CREATE TABLE IF NOT EXISTS google_ai_request_attempts (
    id BIGSERIAL PRIMARY KEY,
    request_uid UUID NOT NULL REFERENCES google_ai_requests(request_uid),
    attempt_no INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    blocked_reason TEXT NULL,
    retry_after_ms INT NULL,
    api_key_id UUID NULL REFERENCES google_ai_api_keys(id),
    quota_scope TEXT NULL,
    reserved_tpm INT NOT NULL,
    usage_input_tokens INT NULL,
    usage_output_tokens INT NULL,
    usage_total_tokens INT NULL,
    duration_ms INT NULL,
    provider_status TEXT NULL,
    provider_error_type TEXT NULL,
    provider_error_code TEXT NULL,
    provider_error_message TEXT NULL,
    provider_interaction_id TEXT NULL,
    provider_terminal_status TEXT NULL,
    semantic_status TEXT NULL,
    semantic_error TEXT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT google_ai_request_attempts_unique UNIQUE (request_uid, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_google_ai_request_attempts_started
    ON google_ai_request_attempts (started_at);

-- Conservative current model limits. The default quota_scope intentionally
-- aggregates all registry keys until the redacted Cloud-project inventory is
-- populated; over-throttling is safer than treating unknown keys as independent.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemma-3-27b', 30, 15000, 14400, 1000),
    ('gemini-2.5-flash', 5, 250000, 20, 1000),
    ('gemma-3-4b', 30, 15000, 14400, 1000),
    ('gemma-3-12b', 30, 15000, 14400, 1000),
    ('gemma-3-1b', 30, 15000, 14400, 1000),
    ('gemini-embedding-2', 10, 30000, 1000, 1000),
    -- 2026-07-31 provider UI: 16K TPM / 14.4K RPD.  These seeds retain
    -- conservative headroom and supersede the old unlimited/1.5K assumption.
    ('gemma-4-31b', 15, 15000, 14000, 1000),
    ('gemma-4-26b-a4b', 15, 15000, 14000, 1000),
    ('gemini-3.1-flash-lite', 13, 240000, 450, 1000),
    ('antigravity-preview-05-2026', 54, 96000, 90, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();
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

CREATE OR REPLACE FUNCTION google_ai_mark_sent(
    p_request_uid UUID,
    p_attempt_no INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE google_ai_requests
    SET sent_at = NOW(), status = 'sent', updated_at = NOW()
    WHERE request_uid = p_request_uid AND sent_at IS NULL;

    UPDATE google_ai_request_attempts
    SET status = 'sent'
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
END;
$$;

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
    SELECT * INTO v_request FROM google_ai_requests WHERE request_uid = p_request_uid;
    IF v_request IS NULL THEN
        RETURN;
    END IF;

    IF v_request.finalized_at IS NOT NULL THEN
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
              AND model = v_request.model
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
            r.model,
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
              AND model = v_req.model
              AND minute_bucket = v_req.minute_bucket;
        END IF;

        IF v_req.day_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET
                rpd_used = GREATEST(0, COALESCE(rpd_used, 0) - v_req.reserved_rpd),
                updated_at = NOW()
            WHERE api_key_id = v_req.api_key_id
              AND model = v_req.model
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
            provider_error_message = COALESCE(provider_error_message, 'swept stale reserved (sent_at is null)'),
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

-- Public-schema tables are internal service data. RLS with no anon/authenticated
-- policies provides defense in depth; service_role bypasses RLS.
ALTER TABLE google_ai_model_limits ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_ai_api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_ai_usage_counters ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_ai_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_ai_request_attempts ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE google_ai_model_limits FROM anon, authenticated;
REVOKE ALL ON TABLE google_ai_api_keys FROM anon, authenticated;
REVOKE ALL ON TABLE google_ai_usage_counters FROM anon, authenticated;
REVOKE ALL ON TABLE google_ai_requests FROM anon, authenticated;
REVOKE ALL ON TABLE google_ai_request_attempts FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_model_limits TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_api_keys TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_usage_counters TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_requests TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE google_ai_request_attempts TO service_role;
GRANT USAGE, SELECT ON SEQUENCE google_ai_usage_counters_id_seq TO service_role;
GRANT USAGE, SELECT ON SEQUENCE google_ai_request_attempts_id_seq TO service_role;

REVOKE ALL ON FUNCTION google_ai_limiter_capabilities() FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_reserve(UUID, INT, TEXT, TEXT, TEXT, INT, UUID[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_mark_sent(UUID, INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_finalize(UUID, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_sweep_stale(INT, INT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_finalize_interaction(UUID, INT, TEXT, TEXT, TEXT, INT, INT, INT, INT, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_record_interaction_semantic(UUID, INT, TEXT, TEXT) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION google_ai_limiter_capabilities() TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_reserve(UUID, INT, TEXT, TEXT, TEXT, INT, UUID[]) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_mark_sent(UUID, INT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_finalize(UUID, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT, TEXT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_sweep_stale(INT, INT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_finalize_interaction(UUID, INT, TEXT, TEXT, TEXT, INT, INT, INT, INT, TEXT, TEXT, TEXT) TO service_role;
GRANT EXECUTE ON FUNCTION google_ai_record_interaction_semantic(UUID, INT, TEXT, TEXT) TO service_role;

COMMIT;
