-- Register the exact provider model id used by grounded Region Talk research.
--
-- The limiter already contained the same project quota snapshot under the
-- non-provider alias `gemini-3-flash`.  Google currently exposes the callable
-- model as `gemini-3-flash-preview`; keep the old row for historical usage
-- accounting and add the exact id for new reservations.

BEGIN;

INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES ('gemini-3-flash-preview', 5, 250000, 20, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();

COMMIT;
