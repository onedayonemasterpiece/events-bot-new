-- Reconcile positive finite model caps with the owner's 2026-09-24 AI Studio
-- readback. The owner verified that the six configured keys are in different
-- projects and that these displayed model limits apply to every project.
-- This registry is project/model-scoped by the canonical limiter RPC.
-- Models displayed with 0 capacity remain absent (fail closed). Live API
-- models with Unlimited RPM/RPD and Veo's non-numeric TPM are not represented
-- by this positive-integer RPM/TPM/RPD table.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemma-4-26b-a4b', 30, 16000, 14400, 1000),
    ('gemma-4-31b', 30, 16000, 14400, 1000),
    ('gemini-embedding-2', 100, 30000, 1000, 1000),
    ('antigravity-preview-05-2026', 60, 100000, 100, 1000),
    ('gemini-3.5-transcribe', 3, 10000, 25, 1000),
    ('gemini-3.8-flash-tts', 3, 10000, 10, 1000),
    ('gemini-3.8-flash-lite-tts', 3, 10000, 10, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();
