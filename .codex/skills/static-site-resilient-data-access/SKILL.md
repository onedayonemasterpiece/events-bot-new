---
name: static-site-resilient-data-access
description: Use for KenigEvents static-site browser access to Supabase Auth, RPC, Search, personalization, feedback, focus membership, telemetry, or remote functions when diagnosing failures, adding fallback routing, limiting egress/local storage, or verifying a reliable thin-client flow.
---

# Static Site Resilient Data Access

Keep the static browser client thin. Improve delivery to the existing data
plane; do not turn Fly into the general application backend.

## Start with the user journey

1. Reproduce every earlier prerequisite first: install/launch, page state,
   identity and UI progression.
2. Do not claim the data-access contract was tested when an earlier step
   blocked the journey.
3. Use the diagnostic surface only to isolate routes; verify the real product
   action separately.

## Route browser operations

1. Locate the shared clients in `site/src/lib/resilientDataClient.ts` and
   `site/src/lib/resilientSupabaseTransport.ts`. The current v2 implementation
   is a rejected incident specimen until the v3 acceptance gates pass.
2. Reject new ad-hoc Supabase `fetch` calls outside an explicit diagnostic.
3. Require a closed central operation catalog. Components name a product
   operation; they never choose direct/relay or a retry policy. Classify:
   - **safe read**: one bounded alternate route is allowed;
   - **selected once**: do not replay OTP or a non-idempotent write after an
     ambiguous timeout;
   - **idempotent replay**: retry only with a stable idempotency key.
4. Treat a request as complete only after its bounded response body is fully
   consumed and decoded. A resolved `fetch()` with unread body is not success.
5. Keep route health per capability (`auth`, `data`, `functions`, small
   storage), not as one global flag. Use a single-flight first-use probe with a
   short staggered alternate, verified nonce/schema/body, compact last-known-
   good state and a per-route circuit breaker.
6. Use real traffic as passive health evidence. Revalidate on the next real use
   after TTL/failure; do not run a permanent background poll. Browser online and
   visibility signals are hints only.
7. Every operation returns its own structured success, definitive failure,
   not-dispatched or ambiguous result. Never infer one request from shared
   timestamp flags.
8. For ambiguous OTP issue, resolve an opaque Send Email Hook/provider receipt;
   never send `/otp` again through the alternate route. OTP/auth/session actions
   never enter the product outbox.
9. Product writes may use an outbox only when their server RPC atomically owns
   operation-id dedupe, payload consistency and device-sequence ordering.
10. Probe and selected-once submissions are single-flight. A disabled button is
   not sufficient protection against Enter/programmatic duplicate submits.
11. Keep the relay fixed-upstream and stateless. Permit only public credentials
   and an exact endpoint allowlist; never expose a service-role key.

## Preserve safety and compactness

- Treat server RLS and server rate limits as the security boundary. Browser
  cooldowns are usability controls only.
- Preserve the exact Supabase Auth storage key, its chunked fragments and PKCE
  verifier behavior.
- Bound local application storage and outboxes; keep Supabase Auth storage out
  of application eviction.
- Keep generated public configuration minimal. A candidate that needs Auth
  must fail before publication when its public relay URL is missing.
- Keep JSON response buffers and local route/outbox state bounded. Large and
  streaming Storage traffic requires a separate declared capability; it cannot
  silently pass through the small-JSON transport.
- Measure egress before and after changes. Avoid duplicate catalog/vector reads
  and repeated full payloads.

## Verify without overclaiming

Run focused unit tests, a non-root Astro candidate build and mobile Playwright.
Use a real fault-injection HTTP server for headers-then-stalled-body, partial
body/socket reset, invalid JSON, 429 and capability split; a ready-made mocked
`Response` is insufficient. For OTP, verify code and link journeys independently,
correlate Auth/provider/receipt evidence and prove upstream issue count is one.
Canary the actual affected-phone journey before any site-wide rollout. Record an
interrupted prerequisite as a blocker, not a pass.

Read `references/project-contract.md` for canonical documents, release gates
and the current acceptance status.
