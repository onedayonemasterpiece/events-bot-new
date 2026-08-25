# W1 Results — resilient static-site client

- Lane: `W1`
- Requirements: `R01`, `R02`, `R05`, `R07`
- Base SHA: `cc7c213f`
- Implementation head SHA: `1b9a55f1399316c70f239faaf5e5264b2622dcb5`
- Branch: `agent/static-site-resilient-egress/W1`

## Delivered

- One Auth-independent, configuration-keyed `ResilientDataClient` singleton.
- Explicit `safe-read`, `selected-once`, and `idempotent-replay` policies.
- Parallel bounded direct/relay health probes, two-minute route reuse, and
  fail-closed `route=null` when neither path is healthy.
- No automatic duplicate Search or email-OTP POST after an ambiguous timeout.
- Shared bounded, channel-safe idempotent outbox for PWA lifecycle and transport
  experiment telemetry. Foreign channels return `skip` and retain attempt count.
- Static-site Auth uses the shared client while preserving the existing Supabase
  auth storage key. Added structured OTP request result, six-digit OTP verify,
  token-hash callback, onboarding reset, and focus participant registration API.
- Personal-feed RPC safe-read routing and compact path-independent ID hint instead
  of full per-preview manifests.
- Compact/capped feedback, profile and reconciliation state; application
  localStorage worst-case registry is below 64 KiB and excludes Supabase Auth.
- Source gate rejects browser Supabase runtime endpoints outside the shared client
  and explicit diagnostic pages.
- Canonical docs and CHANGELOG updated.

## Evidence / commands

- `npm run test:resilient-client` — PASS, 41/41 assertions across both TAP runs.
- `npm run test:focus-group-product` — PASS, 53/53.
- `npm run build` — PASS, Astro static build, 466 pages in 2m02s.
- `node --test tests/search-recovery.playwright.mjs` — clean SKIP because the
  ordinary build intentionally emitted `data-search-enabled=false` without the
  public Search environment. The test now correctly requires an enabled build.
- `git diff --check` — PASS.

## Integration notes / risks

- `FocusGroupInviteIntake.astro` and `FocusGroupLabPanel.astro` were explicitly
  forbidden and were not edited. Their integration lane must consume the new
  structured `signInWithEmailOtp()` return value rather than treating the object
  itself as a boolean.
- `registerFocusGroupParticipant()` intentionally calls the live v1 argument
  `p_communication_opt_in`; do not rename it to an undeployed v2 parameter.
- The build proves Astro/type integration. A live-enabled Search Playwright run
  remains an integration/release gate once public Search env is supplied.
- Client caps/cooldowns are UX and egress controls only. Server RLS, validation,
  idempotency and rate limits remain the abuse/DDoS boundary.

## Changed files

- `CHANGELOG.md`
- `docs/architecture/personalization-data-ownership.md`
- `docs/features/static-site-pages/listing-personal-feed.md`
- `docs/features/unsigned-personalization/authorized-event-search.md`
- `site/package.json`
- `site/src/components/AuthorizedEventSearch.astro`
- `site/src/components/KaupTransportSchedule.astro`
- `site/src/components/PwaTelemetry.astro`
- `site/src/components/auth/StaticSiteAuthRuntime.astro`
- `site/src/components/transport/TransportTimetableExperiment.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/browserStorage.ts`
- `site/src/lib/idempotentOutbox.ts`
- `site/src/lib/pwa-telemetry-controller.js`
- `site/src/lib/resilientDataClient.ts`
- `site/src/lib/resilientSupabaseTransport.test.ts`
- `site/src/lib/resilientSupabaseTransport.ts`
- `site/src/lib/savedEventRuntime.ts`
- `site/src/lib/savedEventRuntimeCore.mjs`
- `site/src/lib/staticSiteAuth.ts`
- `site/src/lib/transportExperimentClient.ts`
- `site/tests/resilient-data-storage.test.mjs`
- `site/tests/search-recovery.playwright.mjs`
- `site/tests/search-recovery.test.mjs`
- `site/tests/static-site-auth.test.mjs`
- `site/tests/supabase-runtime-gate.test.mjs`
