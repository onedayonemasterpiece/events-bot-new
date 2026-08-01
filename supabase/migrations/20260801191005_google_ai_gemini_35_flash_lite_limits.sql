-- Register both stable Flash-Lite generations with a 10% operating margin
-- below the observed AI Studio project quotas (15 RPM / 250k TPM / 500 RPD).
-- The rows are model-scoped; quota_scope remains the Google Cloud project.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemini-3.5-flash-lite', 13, 240000, 450, 1000),
    ('gemini-3.1-flash-lite', 13, 240000, 450, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra;
