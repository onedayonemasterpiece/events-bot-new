-- Reconcile positive finite Google AI model caps with the owner's 2026-09-24 AI Studio readback.
-- Each configured key belongs to a separate Google project, so limits apply per quota_scope/model.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemini-2.5-flash', 5, 250000, 20, 1000),
    ('gemini-2.5-flash-lite', 10, 250000, 20, 1000),
    ('gemini-3-flash', 5, 250000, 20, 1000),
    ('gemini-3.1-flash-lite', 15, 250000, 500, 1000),
    ('gemini-3.5-flash-lite', 15, 250000, 500, 1000),
    ('gemini-3.5-flash', 5, 250000, 20, 1000),
    ('gemini-3.6-flash', 5, 250000, 20, 1000),
    ('gemini-3.7-flash', 5, 250000, 20, 1000),
    ('gemini-3.8-flash', 5, 250000, 20, 1000),
    ('gemma-4-31b', 30, 16000, 14400, 1000),
    ('gemma-4-26b-a4b', 30, 16000, 14400, 1000),
    ('gemini-embedding-001', 100, 30000, 1000, 1000),
    ('gemini-embedding-2', 100, 30000, 1000, 1000),
    ('antigravity-preview-05-2026', 60, 100000, 100, 1000),
    ('gemini-3.5-transcribe', 3, 10000, 25, 1000),
    ('gemini-3.1-flash-tts', 3, 10000, 10, 1000),
    ('gemini-3.8-flash-lite-tts', 3, 10000, 10, 1000),
    ('gemini-3.8-flash-tts', 3, 10000, 10, 1000),
    ('gemini-robotics-er-2-preview', 5, 250000, 20, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();
