-- Register the Antigravity managed agent in the shared Google AI limiter.
--
-- Verified against this project's Google AI Studio quota UI on 2026-07-29:
--   * antigravity-preview-05-2026 (Free Tier): 60 RPM, 100000 TPM, 100 RPD
--
-- Antigravity is an Interactions API agent code, not a GenerateContent model.
-- A caller must reserve/finalize the whole interaction explicitly; merely
-- adding this row does not route it through GoogleAIClient.generate_content.
--
-- Keep the same safety policy as other free-tier Google lanes:
--   * 10% headroom on RPM and RPD;
--   * 4% headroom on TPM.

BEGIN;

UPDATE google_ai_model_limits AS m
SET
    rpm = s.rpm,
    tpm = s.tpm,
    rpd = s.rpd,
    tpm_reserve_extra = s.tpm_reserve_extra,
    updated_at = NOW()
FROM (
    VALUES
        ('antigravity-preview-05-2026', 54, 96000, 90, 1000)
) AS s(model, rpm, tpm, rpd, tpm_reserve_extra)
WHERE m.model = s.model;

INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
SELECT s.model, s.rpm, s.tpm, s.rpd, s.tpm_reserve_extra
FROM (
    VALUES
        ('antigravity-preview-05-2026', 54, 96000, 90, 1000)
) AS s(model, rpm, tpm, rpd, tpm_reserve_extra)
WHERE NOT EXISTS (
    SELECT 1 FROM google_ai_model_limits m WHERE m.model = s.model
);

COMMIT;
