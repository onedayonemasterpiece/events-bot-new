# TPM-GATEWAY lane result

## Scope

- Lane: `TPM-GATEWAY`
- Requirements: `R15`, `R21` gateway portion; acceptance tests `T46`, `T47`, `T54`, `T55`
- Base SHA: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`
- Implementation head SHA: `7e0fb5c56f91cda80f1d671982e17e1e2da6cc8e`
- Branch: `agent/smart-update-llm-first/tpm-gateway`
- Effort/risk: high; cross-cutting gateway compatibility and quota-accounting behavior

## Delivered

- Preserved the public `(text, UsageInfo)` return contract and the original four
  positional `UsageInfo(input, output, total, model)` fields.
- Added finish reason, response ID, transport request ID, model version, thought
  tokens, reserved tokens, input-count source, actual-token and
  reservation/actual-ratio telemetry.
- `MAX_TOKENS`, unknown finish reasons, and other explicit non-success finish
  reasons now raise typed `ProviderError` values while retaining provider usage
  and identity evidence. Truncated/partial text is not returned as success.
- Added optional Google `countTokens` input counting with conservative heuristic
  fallback and explicit source/error telemetry. It is opt-in and no live calls
  were made in this lane.
- Added a persistable model/consumer/prompt-version p99 output+thought
  `TokenReservationCalibration`. Warm admission uses exact input plus observed
  p99 and safety margin; cold start retains the former conservative
  output-ceiling + 1000 behavior. The provider generation ceiling and prompt
  evidence are not modified.
- Added project/model quota-bucket primitives independent of API key identity.
  Typed limiter/provider errors retain quota scope, reason, model and
  `retry_after_ms` when present.
- Kept existing provider/model routing; no quota values were invented.

## Evidence and commands

Interpreter: `/home/dev/.venvs/events-bot-region-talk/bin/python`

1. Syntax:

   ```text
   python -m py_compile google_ai/client.py google_ai/exceptions.py \
     tests/test_google_ai_client.py tests/test_google_ai_tpm_calibration.py
   ```

   Result: pass.

2. Focused and adjacent gateway tests:

   ```text
   python -m pytest -q \
     tests/test_google_ai_client.py \
     tests/test_google_ai_tpm_calibration.py \
     tests/test_google_ai_interactions.py \
     tests/test_google_ai_limiter_supabase.py \
     tests/test_google_ai_antigravity_limits.py \
     tests/test_google_ai_flash_lite_limits.py
   ```

   Result: `109 passed in 1.23s`.

3. Diff hygiene: `git diff --check` passed.

4. Installed SDK contract inspection (no network):

   - `GenerateContentResponse`: `sdk_http_response`, `model_version`,
     `response_id`, `usage_metadata`.
   - `GenerateContentResponseUsageMetadata`: `prompt_token_count`,
     `candidates_token_count`, `thoughts_token_count`, `total_token_count`.
   - `CountTokensResponse`: `total_tokens`.
   - `Candidate`: `finish_reason`.

## Changed files

- `google_ai/client.py`
- `google_ai/exceptions.py`
- `tests/test_google_ai_client.py`
- `tests/test_google_ai_tpm_calibration.py`
- `.codex/lanes/TPM-GATEWAY/RESULTS.md` (evidence-only follow-up commit)

## Risks / integration notes

- The calibration object is deliberately persistence-ready, but selecting and
  storing production observations belongs to caller/DB lanes outside this
  writable scope. Without an exact matching calibration, behavior remains the
  conservative cold-start reservation.
- `countTokens` is opt-in because it is an additional provider endpoint call;
  callers may instead persist and supply an `InputTokenCount`.
- Existing finalization RPC signatures cannot receive new thought/reserved
  columns from this lane (DB/migrations are forbidden). Complete metadata is
  available on `UsageInfo`, typed errors, and structured gateway logs for the
  owning persistence lane.
- Documentation and `CHANGELOG.md` were forbidden and must be synchronized by
  the integrator.
- No live provider request, deploy, production mutation, push, or model/provider
  change was performed.
