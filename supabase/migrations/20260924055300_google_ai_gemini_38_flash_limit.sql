-- Owner-requested shared-ledger cap for Gemini 3.8 Flash (2026-09-24).
-- 20 RPD is an operator limit per Google project/model, not a verified
-- provider-side quota. RPM/TPM match the existing Flash safety caps until
-- each project's actual Google quota can be reconciled.
INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES ('gemini-3.8-flash', 5, 250000, 20, 1000)
ON CONFLICT (model) DO NOTHING;
