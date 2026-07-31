-- Register the operator-provided fresh Google AI lane without storing its
-- secret. The operator confirmed on 2026-07-31 that GOOGLE_API_KEY6 is a new
-- quota lane intended to keep key production workflows available while older
-- project lanes recover from their daily limits.

BEGIN;

INSERT INTO google_ai_api_keys (
    key_alias,
    provider,
    env_var_name,
    account_name,
    quota_scope,
    is_active,
    priority,
    notes
)
VALUES (
    'google-api-key6-fresh',
    'google',
    'GOOGLE_API_KEY6',
    'fresh6',
    'google:key6-operator-isolated-20260731',
    TRUE,
    1,
    'Operator-confirmed fresh quota lane added 2026-07-31; secret remains in runtime env only.'
)
ON CONFLICT (provider, key_alias) DO UPDATE SET
    env_var_name = EXCLUDED.env_var_name,
    account_name = EXCLUDED.account_name,
    quota_scope = EXCLUDED.quota_scope,
    is_active = EXCLUDED.is_active,
    priority = EXCLUDED.priority,
    notes = EXCLUDED.notes,
    updated_at = NOW();

COMMIT;
