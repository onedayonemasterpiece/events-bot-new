-- Reconcile the canonical Google AI limiter with the owner's AI Studio
-- per-project quota readback on 2026-09-24.
--
-- All six configured GOOGLE_API_KEY* aliases are already mapped to distinct
-- quota_scope values, so these model rows apply independently to every key's
-- Google Cloud project. Models shown by AI Studio with zero capacity remain
-- absent/fail-closed.
--
-- Flash-Lite 3.1/3.5 retain the established operating margin below the
-- provider's 15 RPM / 250k TPM / 500 RPD limits. Other rows mirror the
-- finite positive provider caps from the owner readback.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemini-3.1-flash-lite', 13, 240000, 450, 1000),
    ('gemini-3.5-flash-lite', 13, 240000, 450, 1000),
    ('gemini-2.5-flash', 5, 250000, 20, 1000),
    ('gemini-2.5-flash-lite', 10, 250000, 20, 1000),
    ('gemini-3-flash', 5, 250000, 20, 1000),
    ('gemini-3.5-flash', 5, 250000, 20, 1000),
    ('gemini-3.6-flash', 5, 250000, 20, 1000),
    ('gemini-3.7-flash', 5, 250000, 20, 1000),
    ('gemini-3.8-flash', 5, 250000, 20, 1000),
    ('gemini-2.5-flash-tts', 3, 10000, 10, 1000),
    ('gemini-3.1-flash-tts', 3, 10000, 10, 1000),
    ('gemini-3.8-flash-lite-tts', 3, 10000, 10, 1000),
    ('gemini-3.8-flash-tts', 3, 10000, 10, 1000),
    ('gemini-3.5-transcribe', 3, 10000, 25, 1000),
    ('gemini-embedding-1.0', 100, 30000, 1000, 1000),
    ('gemini-embedding-2', 100, 30000, 1000, 1000),
    ('gemma-4-31b', 30, 16000, 14400, 1000),
    ('gemma-4-26b-a4b', 30, 16000, 14400, 1000),
    ('antigravity-preview-05-2026', 60, 100000, 100, 1000),
    ('gemini-robotics-er-2-preview', 5, 250000, 20, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();

-- Live API rows with Unlimited RPM/RPD, image/video/audio models with zero
-- capacity, and Veo rows with a non-numeric TPM are intentionally not inserted:
-- the current limiter schema represents only finite positive RPM/TPM/RPD caps.