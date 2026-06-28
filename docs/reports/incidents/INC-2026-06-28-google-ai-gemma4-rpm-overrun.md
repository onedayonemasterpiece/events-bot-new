# INC-2026-06-28 Google AI Gemma 4 RPM Overrun

Status: monitoring
Severity: sev2
Service: events-bot Google AI / CherryFlash partner filters
Opened: 2026-06-28
Closed: —
Owners: events-bot maintainers
Related incidents: —
Related docs: `docs/features/llm-gateway/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Google AI Studio showed a Gemma 4 31B per-key RPM overrun (`18 / 15`) for one
Google key. Production runtime logs confirmed provider-side `429
RESOURCE_EXHAUSTED` for `gemma-4-31b` on `GOOGLE_API_KEY` during the scheduled
CherryFlash eco partner-track filter burst on 2026-06-27.

## User / Business Impact

- Google AI requests in that burst failed with provider `429`.
- The eco partner-track classifier could fall back or mark events for manual
  review, reducing deterministic quality for that scheduled video selection.
- Repeated bursts risk noisy operator incidents and cross-pipeline quota
  starvation on the primary Google key.

## Detection

- 2026-06-28: operator reported the Google AI Studio Rate Limit screen showing
  `Gemma 4 31B` peak RPM above the free-tier limit.
- Runtime file mirror was enabled and retained the incident window under
  `/data/runtime_logs/events-bot.log*`.

## Timeline

- 2026-06-27 10:30:00 UTC — scheduled `video_partner_track_eco` started.
- 2026-06-27 10:30-10:33 UTC — production emitted many
  `google_ai.reserve_ok` records for `consumer=bot`, `model=gemma-4-31b`,
  `env_var_name=GOOGLE_API_KEY`, but with `api_key_id=null`,
  `minute_bucket=null`, `used_after=null`.
- 2026-06-27 10:30:56 UTC onward — provider returned `429
  RESOURCE_EXHAUSTED` with quota metric
  `generate_content_free_tier_requests`, `limit: 15`, `model: gemma-4-31b`.
- 2026-06-28 — incident formalized and hotfix prepared.

## Root Cause

1. `VideoAnnounceScenario._gemma_client_for_partner_filters()` tried to import
   non-existent `google_ai.get_google_ai_client`.
2. The exception fallback instantiated plain `GoogleAIClient()` with no
   Supabase client, no secrets provider, and `consumer="bot"`.
3. `GoogleAIClient._reserve()` treated `supabase_client=None` as local-dev
   unlimited mode and returned `ok=True` without atomic counters or local RPM
   enforcement.
4. The eco partner filter called Gemma 4 once per candidate in a tight scheduled
   burst, so the primary key exceeded the provider's `15 RPM` limit.

## Contributing Factors

- The fallback path logged as normal `reserve_ok`, making it easy to miss that
  `api_key_id` / `used_after` were absent.
- There was no regression test that video partner filters use a
  Supabase-backed `GoogleAIClient`.
- The local limiter default RPM was above the Gemma 4 free-tier RPM.

## Automation Contract

### Treat as regression guard when

- Changing `GoogleAIClient` reserve/fallback behavior.
- Adding a new server-side Google AI consumer.
- Changing CherryFlash / video-announcement partner filter LLM wiring.
- Changing Google AI key overflow or local limiter defaults.

### Affected surfaces

- `google_ai/client.py`
- `video_announce/scenario.py`
- Google AI env and Supabase `google_ai_*` metadata/RPC
- scheduled `video_partner_track_eco`
- runtime log evidence for `google_ai.reserve_ok` and provider `429`

### Mandatory checks before closure or deploy

- Unit test proving `supabase_client=None` uses process-local RPM limiting by
  default and blocks the 16th Gemma 4 request in one minute.
- Unit test proving the video partner-filter client is constructed with
  `get_supabase_client()`, `SecretsProvider`, `consumer="video_partner_filter"`,
  and incident notifier.
- Runtime/log check after deploy: new `video_partner_filter` Gemma 4 reserves
  must have non-null `api_key_id`, `minute_bucket`, and `used_after`.
- Production health check after deploy.
- Release-governance check: deployed SHA must be reachable from `origin/main`
  before closure.

### Required evidence

- Targeted pytest output.
- Pre-fix runtime evidence from `/data/runtime_logs`.
- Post-deploy runtime evidence for the next relevant partner-filter run or a
  safe smoke that exercises the client construction without printing secrets.
- Deployed SHA and confirmation that it is reachable from `origin/main`.

## Immediate Mitigation

- Hotfix prepared to route video partner-filter Gemma calls through the shared
  Supabase-backed LLM gateway.
- Hotfix prepared to make no-Supabase `GoogleAIClient` instances use the
  process-local fail-fast limiter by default instead of unlimited direct calls.

## Corrective Actions

- Replace the nonexistent `get_google_ai_client` import/fallback with explicit
  `GoogleAIClient(supabase_client=get_supabase_client(), secrets_provider=...)`.
- Use `consumer="video_partner_filter"` for partner filter logs and quota
  attribution.
- Default no-Supabase local fallback RPM to `15`, matching Gemma 4 free-tier
  per-key RPM.
- Add regression tests for both guards.

## Follow-up Actions

- [ ] Add a lightweight production dashboard/query that alerts when
  `google_ai.reserve_ok` has `api_key_id=null` or `used_after=null` outside
  known local probes.
- [ ] Consider adding per-consumer concurrency limits for scheduled partner
  filter bursts, so even Supabase-backed calls spread load more smoothly.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- The incident record is now an active regression contract for Google AI client
  construction and local fallback behavior.
