-- Preserve one auditable terminal row and the correct TPM bucket for every
-- physical provider retry.  The initial canonical limiter stored retry rows,
-- but its request-level finalized_at guard prevented attempts >1 from being
-- finalized and reconciled against their own minute.

BEGIN;

ALTER TABLE google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS minute_bucket TIMESTAMPTZ NULL;
ALTER TABLE google_ai_request_attempts
    ADD COLUMN IF NOT EXISTS day_bucket DATE NULL;

UPDATE google_ai_request_attempts
SET
    minute_bucket = COALESCE(
        minute_bucket,
        date_trunc('minute', started_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    ),
    day_bucket = COALESCE(day_bucket, (started_at AT TIME ZONE 'UTC')::DATE)
WHERE minute_bucket IS NULL OR day_bucket IS NULL;

UPDATE google_ai_requests r
SET
    attempts = latest.attempt_no,
    api_key_id = latest.api_key_id,
    quota_scope = latest.quota_scope,
    minute_bucket = latest.minute_bucket,
    day_bucket = latest.day_bucket,
    reserved_tpm = latest.reserved_tpm
FROM (
    SELECT DISTINCT ON (request_uid)
        request_uid,
        attempt_no,
        api_key_id,
        quota_scope,
        minute_bucket,
        day_bucket,
        reserved_tpm
    FROM google_ai_request_attempts
    ORDER BY request_uid, attempt_no DESC
) latest
WHERE latest.request_uid = r.request_uid
  AND (
      r.attempts IS DISTINCT FROM latest.attempt_no
      OR r.minute_bucket IS DISTINCT FROM latest.minute_bucket
      OR r.day_bucket IS DISTINCT FROM latest.day_bucket
  );

CREATE OR REPLACE FUNCTION google_ai_sync_attempt_context()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    NEW.minute_bucket := COALESCE(
        NEW.minute_bucket,
        date_trunc('minute', NEW.started_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    );
    NEW.day_bucket := COALESCE(NEW.day_bucket, (NEW.started_at AT TIME ZONE 'UTC')::DATE);

    UPDATE google_ai_requests
    SET
        api_key_id = NEW.api_key_id,
        quota_scope = NEW.quota_scope,
        minute_bucket = NEW.minute_bucket,
        day_bucket = NEW.day_bucket,
        reserved_tpm = NEW.reserved_tpm,
        status = NEW.status,
        attempts = GREATEST(attempts, NEW.attempt_no),
        sent_at = NULL,
        finalized_at = NULL,
        usage_input_tokens = NULL,
        usage_output_tokens = NULL,
        usage_total_tokens = NULL,
        last_error_kind = NULL,
        last_error_code = NULL,
        last_error_message = NULL,
        updated_at = NOW()
    WHERE request_uid = NEW.request_uid;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_google_ai_sync_attempt_context
    ON google_ai_request_attempts;
CREATE TRIGGER trg_google_ai_sync_attempt_context
BEFORE INSERT ON google_ai_request_attempts
FOR EACH ROW EXECUTE FUNCTION google_ai_sync_attempt_context();

CREATE OR REPLACE FUNCTION google_ai_finalize(
    p_request_uid UUID,
    p_attempt_no INT,
    p_usage_input_tokens INT,
    p_usage_output_tokens INT,
    p_usage_total_tokens INT,
    p_duration_ms INT,
    p_provider_status TEXT,
    p_error_type TEXT DEFAULT NULL,
    p_error_code TEXT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    v_request RECORD;
    v_attempt RECORD;
    v_delta INT;
BEGIN
    SELECT * INTO v_request FROM google_ai_requests WHERE request_uid = p_request_uid;
    IF v_request IS NULL THEN
        RETURN;
    END IF;

    SELECT * INTO v_attempt
    FROM google_ai_request_attempts
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;
    IF v_attempt IS NULL OR v_attempt.completed_at IS NOT NULL THEN
        RETURN;
    END IF;

    IF p_usage_total_tokens IS NOT NULL AND v_attempt.reserved_tpm IS NOT NULL THEN
        v_delta := p_usage_total_tokens - v_attempt.reserved_tpm;
        IF v_delta != 0 AND v_attempt.minute_bucket IS NOT NULL THEN
            UPDATE google_ai_usage_counters
            SET tpm_used = tpm_used + v_delta, updated_at = NOW()
            WHERE api_key_id = v_attempt.api_key_id
              AND model = v_request.model
              AND minute_bucket = v_attempt.minute_bucket;
        END IF;
    END IF;

    UPDATE google_ai_request_attempts
    SET
        status = CASE WHEN p_error_type IS NULL THEN 'succeeded' ELSE 'failed_provider' END,
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_total_tokens = p_usage_total_tokens,
        duration_ms = p_duration_ms,
        provider_status = p_provider_status,
        provider_error_type = p_error_type,
        provider_error_code = p_error_code,
        provider_error_message = p_error_message,
        completed_at = NOW()
    WHERE request_uid = p_request_uid AND attempt_no = p_attempt_no;

    UPDATE google_ai_requests
    SET
        status = CASE WHEN p_error_type IS NULL THEN 'succeeded' ELSE 'failed_provider' END,
        finalized_at = NOW(),
        usage_input_tokens = p_usage_input_tokens,
        usage_output_tokens = p_usage_output_tokens,
        usage_total_tokens = p_usage_total_tokens,
        last_error_kind = CASE WHEN p_error_type IS NOT NULL THEN 'provider' ELSE NULL END,
        last_error_code = p_error_code,
        last_error_message = p_error_message,
        updated_at = NOW()
    WHERE request_uid = p_request_uid
      AND attempts <= p_attempt_no;
END;
$$;

REVOKE ALL ON FUNCTION google_ai_sync_attempt_context() FROM PUBLIC;
REVOKE ALL ON FUNCTION google_ai_finalize(
    UUID, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION google_ai_finalize(
    UUID, INT, INT, INT, INT, INT, TEXT, TEXT, TEXT, TEXT
) TO service_role;

COMMIT;
