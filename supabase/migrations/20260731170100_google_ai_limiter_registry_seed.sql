-- Redacted Google AI key registry for the canonical personalization limiter.
-- Secret values remain in Fly/Kaggle/Edge environment variables only.
--
-- All keys start in one deliberately conservative quota scope. This prevents
-- project-level overrun until an operator-verified key -> Google Cloud project
-- inventory can safely split the rows into independent scopes.

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
VALUES
    ('google-ai-key4', 'google', 'GOOGLE_API_KEY4', NULL,
     'google:unmapped-shared', true, 2,
     'Canonical limiter seed; Cloud project mapping pending operator verification.'),
    ('google-api-key3-reserve', 'google', 'GOOGLE_API_KEY3', 'reserve3',
     'google:unmapped-shared', true, 3,
     'Canonical limiter seed; Cloud project mapping pending operator verification.'),
    ('google-api-key5-reserve', 'google', 'GOOGLE_API_KEY5', 'reserve5',
     'google:unmapped-shared', true, 4,
     'Canonical limiter seed; Cloud project mapping pending operator verification.'),
    ('guide_key2', 'google', 'GOOGLE_API_KEY2', 'idontknow',
     'google:unmapped-shared', true, 5,
     'Canonical limiter seed; Cloud project mapping pending operator verification.'),
    ('key_local', 'google', 'GOOGLE_API_KEY', NULL,
     'google:unmapped-shared', true, 10,
     'Canonical limiter seed; Cloud project mapping pending operator verification.')
ON CONFLICT (provider, key_alias) DO UPDATE SET
    env_var_name = EXCLUDED.env_var_name,
    account_name = EXCLUDED.account_name,
    quota_scope = EXCLUDED.quota_scope,
    is_active = EXCLUDED.is_active,
    priority = EXCLUDED.priority,
    notes = EXCLUDED.notes,
    updated_at = NOW();

COMMIT;
