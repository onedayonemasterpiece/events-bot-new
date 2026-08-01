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
   `site/src/lib/resilientSupabaseTransport.ts`.
2. Reject new ad-hoc Supabase `fetch` calls outside an explicit diagnostic.
3. Classify the operation before retrying:
   - **safe read**: one bounded alternate route is allowed;
   - **selected once**: do not replay OTP or a non-idempotent write after an
     ambiguous timeout;
   - **idempotent replay**: retry only with a stable idempotency key.
4. Probe direct and stateless relay routes in parallel with short budgets,
   cache the preferred route briefly, and invalidate it after a real failure.
5. Keep the relay fixed-upstream and stateless. Permit only public credentials
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
- Measure egress before and after changes. Avoid duplicate catalog/vector reads
  and repeated full payloads.

## Verify without overclaiming

Run focused unit tests, a non-root Astro candidate build and mobile Playwright.
Exercise direct and relay paths, slow/failure shapes, and the real user action.
For OTP, verify code and link journeys independently and prove that an
ambiguous request was not duplicated. Record an interrupted prerequisite as a
blocker, not a pass.

Read `references/project-contract.md` for canonical documents, release gates
and the current acceptance status.
