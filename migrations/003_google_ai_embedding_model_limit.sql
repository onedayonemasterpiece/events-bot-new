-- Add conservative limiter metadata for Smart Update identity embeddings.
-- Gemini API limits are enforced per Google Cloud project across RPM/TPM/RPD;
-- keep this cap intentionally conservative and tune upward only after live evidence.

INSERT INTO google_ai_model_limits (model, rpm, tpm, rpd)
SELECT 'gemini-embedding-2', 10, 30000, 1000
WHERE NOT EXISTS (
    SELECT 1 FROM google_ai_model_limits WHERE model = 'gemini-embedding-2'
);
