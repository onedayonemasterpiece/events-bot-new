# INC-2026-06-03-smart-update-flash-lite-rpd Smart Update flash-lite RPD exhaustion → 4o bleed

Status: mitigated
Severity: sev2
Service: Smart Update LLM pipeline (`smart_update` consumer) on Fly app `events-bot-new-wngqia`
Opened: 2026-06-03
Closed: —
Owners: bot operator / incident owner
Related incidents: —
Related docs: `docs/llm/request-guide.md`, `google_ai/client.py`, `smart_event_update.py`

## Summary

At ~01:00 Europe/Kaliningrad (2026-06-03 ~23:00 UTC) the operator alert channel filled with
`🚨 LLM INCIDENT [WARNING] kind=smart_update_gemma_fallback_4o` messages carrying
`error=RateLimitError(blocked_reason='rpd', retry_after_ms=None, model='gemini-3.1-flash-lite', api_key_id=None, ...)`.
Every Smart Update stage that touched `gemini-3.1-flash-lite` (e.g. `telegraph_render_remove_logistics`,
fact extraction, writer) started cascading to `gpt-4o`, threatening the daily `gpt-4o` spend cap.
It was **not** an infinite hammer loop and **not** a real provider lockout: the daily request quota
(`rpd`) on the single key the `smart_update` lane was scoped to had been used up.

## User / Business Impact

- No user-facing data corruption: each affected stage is best-effort and either fell back to `gpt-4o`
  (successful output, higher cost) or, for `telegraph_render_remove_logistics`, kept the original text.
- Operational risk: sustained `gpt-4o` fallback would exhaust `FOUR_O_GPT4O_DAILY_TOKEN_LIMIT`
  (default 950k tokens) and degrade text quality / cost for the rest of the day.
- Alert noise: one warning per affected event (~1/min during the guide-monitor burst).

## Detection

- Operator noticed the repeated `smart_update_gemma_fallback_4o` warnings in the alert channel.
- Signals that worked: the per-fallback incident notifier surfaced the exact `blocked_reason='rpd'`
  and offending `model='gemini-3.1-flash-lite'`.
- Observability gap: nothing alerted that a *deployed* Google key (`GOOGLE_API_KEY3`) existed as a
  Fly secret but was never registered in `google_ai_api_keys`, so its quota sat idle while the lane starved.

## Timeline

- 2026-06-03 ~18:25 UTC — `GOOGLE_API_KEY` `gemini-3.1-flash-lite` `rpd_used` reaches 450/450 (day cap).
- 2026-06-03 22:59–23:06 UTC — repeated `smart_update_gemma_fallback_4o` warnings; stages bleed to `gpt-4o`.
- 2026-06-03 ~23:20 UTC — investigation: confirmed RPD enforced by Supabase `google_ai_reserve` ledger
  before any Google call (no provider abuse); found 2 registered keys, flash-lite limit 450/key.
- 2026-06-03 ~23:24 UTC — Fly secrets list revealed a THIRD key `GOOGLE_API_KEY3` (deployed, unused by limiter).
- 2026-06-03 ~23:25 UTC — registered `GOOGLE_API_KEY3` in `google_ai_api_keys` (priority 3, active).
- 2026-06-03 23:27:37 UTC — `flyctl secrets set GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV=0` → machine
  `48e42d5b714228` rolling-restart healthy.
- 2026-06-03 ~23:30 UTC — verified in the running machine `SCOPE=0`, `GOOGLE_API_KEY3` length 39 (valid format),
  and reproduced the RPC key-selection read-only: next flash-lite reservation routes to the empty KEY3.

## Root Cause

1. `gemini-3.1-flash-lite` has a small daily quota: `google_ai_model_limits.rpd = 450` per key.
2. `smart_update` routes fact-extraction + writer stages to `gemini-3.1-flash-lite`
   (`SMART_UPDATE_FACTS_MODEL` / `SMART_UPDATE_WRITER_MODEL`) and uses it as a fallback in the gemma chain.
3. `GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV=1` + `default_env_var_name="GOOGLE_API_KEY"` scoped the
   `smart_update` reservation candidate pool to the single `GOOGLE_API_KEY` key. Once that key hit 450/450,
   `google_ai_reserve` returned `ok=false blocked_reason='rpd' api_key_id=NULL` (no candidate had budget).
4. On `RateLimitError` the `GoogleAIClient` model chain does NOT fall to the next chain model (only `ProviderError`
   triggers in-chain fallback), so the wrapper jumped straight to `gpt-4o`.

## Contributing Factors

- A valid third Google key (`GOOGLE_API_KEY3`) was deployed as a Fly secret but never inserted into
  `google_ai_api_keys`, so the limiter could not use its idle quota.
- Key lanes are isolated by env name (main bot = `GOOGLE_API_KEY`, excursions = `GOOGLE_API_KEY2`), so
  spare capacity in one lane cannot be borrowed by another under the current code.

## Automation Contract

### Treat as regression guard when

- changing `smart_update` model routing (`SMART_UPDATE_FACTS_MODEL`, `SMART_UPDATE_WRITER_MODEL`,
  `GOOGLE_AI_FALLBACK_MODELS`);
- changing reservation scoping (`GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV`) or candidate-key resolution
  in `google_ai/client.py`;
- adding/removing Google API keys (Fly secrets and the `google_ai_api_keys` table must stay in sync).
- adding any Smart Update-adjacent normal key pool or bulk/backfill LLM consumer.

### Affected surfaces

- code paths: `google_ai/client.py` (`_reserve`, `_resolve_default_env_candidate_key_ids`, `generate`),
  `smart_event_update.py` (`_ask_gemma_text` / `_ask_gemma_json` 4o fallback).
- env/config: `GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV`, `GOOGLE_API_KEY{,2,3}`,
  `SMART_UPDATE_FACTS_MODEL`, `SMART_UPDATE_WRITER_MODEL`.
- external systems: Supabase tables `google_ai_api_keys`, `google_ai_model_limits`,
  `google_ai_usage_counters`, RPC `google_ai_reserve`.
- alerts: `smart_update_gemma_fallback_4o`, `provider_error_fallback`, `rate_limit_blocked`.

### Mandatory checks before closure or deploy

- Fly secrets and `google_ai_api_keys` rows agree on the active key set.
- For each active key + `gemini-3.1-flash-lite`: `rpd_used < 450` headroom check before peak windows.
- If the durable two-phase overflow lands: unit tests covering scoped-then-overflow reservation.
- For a normal pool: prove rotation starts on the first allocation, a blocked
  member advances within the same allocation, missing registry/limiter fails
  closed, and the feature never falls into an unrelated key lane.

### Required evidence

- deployed SHA / Fly machine id (`48e42d5b714228`).
- read-only RPC-selection reproduction showing KEY3 chosen for flash-lite.
- confirmation any code fix is reachable from `origin/main` before deploy.

## Immediate Mitigation

1. Registered `GOOGLE_API_KEY3` in `google_ai_api_keys` (`priority=3`, `is_active=true`) — +450 rpd flash-lite,
   +1500 rpd gemma of idle capacity.
2. Set Fly secret `GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV=0` (rolling restart) so `smart_update` reserves
   across all three keys by priority (KEY3 → KEY2 → KEY1), skipping the exhausted KEY1.
3. flash-lite intentionally kept (no Gemma downgrade) per operator quality requirement.

## Corrective Actions

- (done) Idle third key registered; scope opened to use full pooled flash-lite capacity (~886 rpd free today).
- (done, code — pending deploy) Two-phase emergency overflow in `google_ai/client.py`: `_reserve` reserves
  within the scoped lane first; only on a day-level block (`rpd`/`no_keys`) does it retry once with the
  scoped + reserve keys merged (RPC skips the exhausted scoped key, borrows the cheapest spare). Per-minute
  blocks (`rpm`/`tpm`) never overflow. New `GoogleAIClient(reserve_overflow_key_envs=...)` /
  `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS`; `smart_update` opts in via `SMART_UPDATE_RESERVE_OVERFLOW_KEY_ENVS`
  (default `GOOGLE_API_KEY3,GOOGLE_API_KEY2`). Successful borrows emit a `reserve_overflow_used` warning.
  Tests in `tests/test_google_ai_client.py`.
- (2026-07-17, code pending deploy) Image geometry uses a distinct normal
  KEY4+KEY5 pool from its first reservation, with no emergency/local/model
  fallback. Tests cover round-robin, blocked-member advance and fail-closed
  registry/limiter behavior. Production is capped at 100 new calls/UTC day;
  external backfill is paced and capped at 400 total calls/day because Google
  quota is per Cloud project, not automatically per API-key env.

## Follow-up Actions

- [x] operator — decided: restore `scope=1` + ship two-phase overflow (code landed, tests green).
- [ ] deploy the overflow fix, then set `GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV=1` (restore lane isolation)
      and confirm a real `reserve_overflow_used` event during the next flash-lite exhaustion window.
- [ ] add a check/alert when a `GOOGLE_API_KEY*` Fly secret has no matching active `google_ai_api_keys` row.
- [ ] consider per-day RPD headroom alert for `gemini-3.1-flash-lite` before peak windows.

## Release And Closure Evidence

- deploy path: `flyctl secrets set` config-only change (no image deploy); Fly machine `48e42d5b714228`
  restarted healthy 2026-06-03 23:27:37 UTC.
- regression checks: read-only `google_ai_reserve` selection reproduction (KEY3 first for flash-lite).
- post-deploy verification: `SCOPE=0` and `GOOGLE_API_KEY3` length 39 confirmed in the running container.

## Prevention

- Keep Fly Google-key secrets and `google_ai_api_keys` rows in sync (registration is required for the
  limiter to use a key).
- Treat `gemini-3.1-flash-lite`'s 450 rpd/key as a hard per-key ceiling — scale capacity by adding/registering
  keys, never by raising `google_ai_model_limits.rpd` above the provider's real quota (that converts a clean
  ledger refusal into real provider 429s).
