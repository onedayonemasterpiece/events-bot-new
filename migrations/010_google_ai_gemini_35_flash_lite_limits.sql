-- Compatibility migration for deployments that use the repository-level
-- migration runner. Canonical Supabase migration: 20260801191005.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemini-3.5-flash-lite', 13, 240000, 450, 1000),
    ('gemini-3.1-flash-lite', 13, 240000, 450, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra;
