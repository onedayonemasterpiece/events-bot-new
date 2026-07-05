-- Shared runtime resource leases for cross-host/session coordination.
-- Used by Subscriber Acquisition Discovery to guard Telegram human sessions
-- before starting a Kaggle run. Secrets are not stored here.

CREATE TABLE IF NOT EXISTS runtime_resource_leases (
    resource_key TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    holder_kind TEXT NOT NULL DEFAULT 'runtime',
    status TEXT NOT NULL DEFAULT 'active',
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_runtime_resource_leases_status_expires
    ON runtime_resource_leases (status, expires_at);

ALTER TABLE runtime_resource_leases ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION runtime_resource_acquire(
    p_resource_key TEXT,
    p_holder_id TEXT,
    p_holder_kind TEXT DEFAULT 'runtime',
    p_ttl_seconds INT DEFAULT 10800,
    p_metadata JSONB DEFAULT '{}'::jsonb
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_now TIMESTAMPTZ := now();
    v_expires TIMESTAMPTZ := now() + make_interval(secs => GREATEST(60, COALESCE(p_ttl_seconds, 10800)));
    v_row runtime_resource_leases%ROWTYPE;
BEGIN
    IF COALESCE(BTRIM(p_resource_key), '') = '' OR COALESCE(BTRIM(p_holder_id), '') = '' THEN
        RETURN jsonb_build_object('ok', false, 'blocked_reason', 'bad_request');
    END IF;

    UPDATE runtime_resource_leases
    SET status = 'expired', released_at = v_now, updated_at = v_now
    WHERE resource_key = p_resource_key
      AND status = 'active'
      AND expires_at <= v_now;

    INSERT INTO runtime_resource_leases(
        resource_key, holder_id, holder_kind, status, acquired_at, expires_at, released_at, updated_at, metadata
    )
    VALUES(
        p_resource_key, p_holder_id, COALESCE(NULLIF(BTRIM(p_holder_kind), ''), 'runtime'),
        'active', v_now, v_expires, NULL, v_now, COALESCE(p_metadata, '{}'::jsonb)
    )
    ON CONFLICT (resource_key) DO UPDATE
    SET holder_id = excluded.holder_id,
        holder_kind = excluded.holder_kind,
        status = 'active',
        acquired_at = CASE
            WHEN runtime_resource_leases.holder_id = excluded.holder_id THEN runtime_resource_leases.acquired_at
            ELSE excluded.acquired_at
        END,
        expires_at = excluded.expires_at,
        released_at = NULL,
        updated_at = excluded.updated_at,
        metadata = excluded.metadata
    WHERE runtime_resource_leases.status IN ('released', 'expired')
       OR runtime_resource_leases.expires_at <= v_now
       OR runtime_resource_leases.holder_id = excluded.holder_id
    RETURNING * INTO v_row;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'ok', true,
            'resource_key', v_row.resource_key,
            'holder_id', v_row.holder_id,
            'holder_kind', v_row.holder_kind,
            'expires_at', v_row.expires_at,
            'status', v_row.status
        );
    END IF;

    SELECT * INTO v_row
    FROM runtime_resource_leases
    WHERE resource_key = p_resource_key;

    RETURN jsonb_build_object(
        'ok', false,
        'blocked_reason', 'busy',
        'resource_key', p_resource_key,
        'holder_id', v_row.holder_id,
        'holder_kind', v_row.holder_kind,
        'expires_at', v_row.expires_at,
        'status', v_row.status
    );
END;
$$;

CREATE OR REPLACE FUNCTION runtime_resource_release(
    p_resource_key TEXT,
    p_holder_id TEXT,
    p_status TEXT DEFAULT 'released'
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_now TIMESTAMPTZ := now();
    v_count INT := 0;
BEGIN
    UPDATE runtime_resource_leases
    SET status = CASE WHEN COALESCE(NULLIF(BTRIM(p_status), ''), 'released') IN ('released', 'complete', 'failed', 'error') THEN COALESCE(NULLIF(BTRIM(p_status), ''), 'released') ELSE 'released' END,
        released_at = v_now,
        updated_at = v_now
    WHERE resource_key = p_resource_key
      AND holder_id = p_holder_id
      AND status = 'active';
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN jsonb_build_object('ok', true, 'released', v_count, 'resource_key', p_resource_key, 'holder_id', p_holder_id);
END;
$$;

REVOKE ALL ON FUNCTION runtime_resource_acquire(TEXT, TEXT, TEXT, INT, JSONB) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION runtime_resource_release(TEXT, TEXT, TEXT) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION runtime_resource_acquire(TEXT, TEXT, TEXT, INT, JSONB) TO service_role;
GRANT EXECUTE ON FUNCTION runtime_resource_release(TEXT, TEXT, TEXT) TO service_role;
