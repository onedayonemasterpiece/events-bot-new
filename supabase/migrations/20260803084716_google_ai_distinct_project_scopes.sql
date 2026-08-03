-- Split the six Google API keys into operator-confirmed independent Cloud
-- project quota scopes. API key values and Cloud project identifiers remain
-- outside the database; env aliases are sufficient for stable redacted scope
-- identity. Correct historical request attribution so rolling admission sees
-- each project's own recent attempts immediately after cutover.

BEGIN;

-- Prevent a reserve statement from reading the old registry and inserting an
-- old-scope attempt after the attribution rewrite has completed.
LOCK TABLE google_ai_api_keys IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    v_rows INT;
BEGIN
    SELECT COUNT(*) INTO v_rows
    FROM google_ai_api_keys
    WHERE provider = 'google'
      AND env_var_name = ANY (ARRAY[
          'GOOGLE_API_KEY',
          'GOOGLE_API_KEY2',
          'GOOGLE_API_KEY3',
          'GOOGLE_API_KEY4',
          'GOOGLE_API_KEY5',
          'GOOGLE_API_KEY6'
      ]);
    IF v_rows <> 6 THEN
        RAISE EXCEPTION
            'Expected exactly six registered Google key aliases before scope split; found %',
            v_rows;
    END IF;
END;
$$;

WITH scope_map(env_var_name, quota_scope) AS (
    VALUES
        ('GOOGLE_API_KEY',  'google:operator-project-key1-20260803'),
        ('GOOGLE_API_KEY2', 'google:operator-project-key2-20260803'),
        ('GOOGLE_API_KEY3', 'google:operator-project-key3-20260803'),
        ('GOOGLE_API_KEY4', 'google:operator-project-key4-20260803'),
        ('GOOGLE_API_KEY5', 'google:operator-project-key5-20260803'),
        ('GOOGLE_API_KEY6', 'google:operator-project-key6-20260803')
)
UPDATE google_ai_api_keys k
SET quota_scope = m.quota_scope,
    notes = 'Operator confirmed this key belongs to a distinct Google Cloud project on 2026-08-03; secret and project identifier remain outside the ledger.',
    updated_at = NOW()
FROM scope_map m
WHERE k.provider = 'google'
  AND k.env_var_name = m.env_var_name;

-- Preserve any current or historical provider-429 attribution. The source key
-- identifies the only project which actually returned the 429.
UPDATE google_ai_provider_cooldowns c
SET quota_scope = k.quota_scope,
    updated_at = NOW()
FROM google_ai_api_keys k
WHERE c.source_api_key_id = k.id
  AND c.quota_scope IS DISTINCT FROM k.quota_scope;

-- Rolling RPM/TPM admission reads the scope stored on physical attempts. A
-- registry-only split would temporarily forget requests made in the preceding
-- 60 seconds and would leave the audit trail incorrectly grouped forever.
UPDATE google_ai_request_attempts a
SET quota_scope = k.quota_scope
FROM google_ai_api_keys k
WHERE a.api_key_id = k.id
  AND a.quota_scope IS DISTINCT FROM k.quota_scope;

UPDATE google_ai_requests r
SET quota_scope = k.quota_scope
FROM google_ai_api_keys k
WHERE r.api_key_id = k.id
  AND r.quota_scope IS DISTINCT FROM k.quota_scope;

DO $$
DECLARE
    v_rows INT;
    v_scopes INT;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT quota_scope)
    INTO v_rows, v_scopes
    FROM google_ai_api_keys
    WHERE provider = 'google'
      AND is_active
      AND env_var_name = ANY (ARRAY[
          'GOOGLE_API_KEY',
          'GOOGLE_API_KEY2',
          'GOOGLE_API_KEY3',
          'GOOGLE_API_KEY4',
          'GOOGLE_API_KEY5',
          'GOOGLE_API_KEY6'
      ]);
    IF v_rows <> 6 OR v_scopes <> 6 THEN
        RAISE EXCEPTION
            'Google project scope split failed: rows=%, distinct_scopes=%',
            v_rows, v_scopes;
    END IF;
END;
$$;

COMMIT;
