-- Update stable Flash-Lite limits from the owner's Google AI Studio readback
-- on 2026-09-24. These limits are per Google Cloud project. The canonical
-- registry already maps the six active GOOGLE_API_KEY* aliases to six distinct
-- quota_scope values, so each key receives the full model budget independently.
--
-- Unlike the earlier conservative 13/240k/450 operating margin, this migration
-- intentionally mirrors the provider limits exactly: 15 RPM / 250k TPM /
-- 500 RPD for both stable Flash-Lite generations.
BEGIN;

INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd, tpm_reserve_extra)
VALUES
    ('gemini-3.1-flash-lite', 15, 250000, 500, 1000),
    ('gemini-3.5-flash-lite', 15, 250000, 500, 1000)
ON CONFLICT (model) DO UPDATE SET
    rpm = EXCLUDED.rpm,
    tpm = EXCLUDED.tpm,
    rpd = EXCLUDED.rpd,
    tpm_reserve_extra = EXCLUDED.tpm_reserve_extra,
    updated_at = NOW();

DO $$
DECLARE
    v_rows INT;
    v_scopes INT;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT quota_scope)
    INTO v_rows, v_scopes
    FROM google_ai_api_keys
    WHERE provider = 'google'
      AND is_active
      AND env_var_name = ANY (ARRAY[
          'GOOGLE_API_KEY',
          'GOOGLE_API_KEY2',
          'GOOGLE_API_KEY3',
          'GOOGLE_API_KEY4',
          'GOOGLE_API_KEY5',
          'GOOGLE_API_KEY6'
      ]);

    IF v_rows <> 6 OR v_scopes <> 6 THEN
        RAISE EXCEPTION
            'Expected six active Google keys in six distinct quota scopes; rows=%, scopes=%',
            v_rows, v_scopes;
    END IF;
END;
$$;

COMMIT;
