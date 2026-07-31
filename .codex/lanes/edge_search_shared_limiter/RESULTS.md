# R01 — Edge Search Shared Google Limiter

## Scope

- Requirement: R01 — route every `event-search` Google embedding and LLM provider attempt through the shared quota ledger; remove raw multi-key retry admission; fail closed without the limiter/key metadata.
- Writable scope used: `supabase/functions/event-search/**` and this results file only.
- No provider, network, or live Supabase API calls were made.
- No secrets or key values were printed, persisted, or included in accounting payloads.

## Git

- Base SHA: `86a0a8382f0dd9cbb644cd02540bf503e012332c`
- Implementation head SHA: `2afad8a8d725330fb7f01e8a70096f0d1ae6bc31`
- Branch: `agent/edge-search-shared-limiter`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/edge-search-shared-limiter`
- Note: the final lane head includes the later commit that adds this report; the implementation head above is the exact reviewed/tested code commit.

## Delivered

1. Added a strict shared quota wrapper for a single physical Google provider attempt:
   - resolves active `google_ai_api_keys` metadata for the explicitly configured key pool;
   - requires complete key ID/env metadata and a local secret for the leased env;
   - reserves with a fresh request UUID and a single candidate key ID;
   - marks sent before executing provider transport;
   - finalizes success, HTTP/provider error, timeout/network error, malformed provider payload, and unsent cleanup paths;
   - fails closed on backend, metadata, reserve RPC, mark RPC, key, or finalize RPC failure.
2. Removed `providerKeyAttempts`, seed-based raw secret rotation, and raw-secret-bearing key candidate objects.
3. Wrapped both direct Google endpoints in `index.ts`:
   - `:embedContent`;
   - `:generateContent`.
4. Preserved safe behavior:
   - cached embeddings/results still avoid provider calls and therefore require no reservation;
   - embedding inability fails the search closed before Google transport;
   - optional LLM limiter/provider failure degrades to the existing vector-result path;
   - retryable provider/quota outcomes can try another registered key, but every physical retry gets its own reserve/mark/finalize lifecycle.
5. Added redacted sent-event logging with request UUID, key row UUID/env metadata, model, and limiter buckets; no key value is logged.
6. Added local fake-backend JS tests that prove ordering, denial behavior, cleanup, alias metadata handling, finalization, and static wrapping of both provider endpoints.

## Changed Files

- `supabase/functions/event-search/index.ts`
- `supabase/functions/event-search/google-quota.ts`
- `supabase/functions/event-search/google-quota.test.mjs`
- `.codex/lanes/edge_search_shared_limiter/RESULTS.md`

## Commands And Evidence

### Tests

- `node --experimental-strip-types --test supabase/functions/event-search/google-quota.test.mjs`
  - PASS: 10 tests, 0 failures.
- `node --test supabase/functions/event-search/occurrence-families.test.mjs`
  - PASS: 5 tests, 0 failures.

### Type Check

A temporary declaration file outside the repository (`/tmp/event-search-deno-globals.d.ts`) supplied only the Deno global and pinned remote Supabase module shape for offline checking.

- `tsc --noEmit --skipLibCheck --strict --allowImportingTsExtensions --module esnext --moduleResolution bundler --target es2022 --lib es2022,dom /tmp/event-search-deno-globals.d.ts supabase/functions/event-search/google-quota.ts supabase/functions/event-search/occurrence-families.ts supabase/functions/event-search/index.ts`
  - PASS.

### Static Bypass Audit

- `rg -n 'generativelanguage\.googleapis\.com|withSharedGoogleQuotaAttempt|google_ai_(reserve|mark_sent|finalize)|providerKeyAttempts' supabase/functions/event-search`
  - Exactly two production Google endpoint call sites remain.
  - Exactly two `withSharedGoogleQuotaAttempt` production call sites wrap them.
  - `providerKeyAttempts` is absent from production source (its name remains only in the negative static test assertion).
  - Shared lifecycle RPC names are centralized in `google-quota.ts`.
- `git diff --check`
  - PASS.

## Risks / Release Gates

- The personalization Supabase project must expose the already-planned service-role `google_ai_api_keys` read and `google_ai_reserve`, `google_ai_mark_sent`, `google_ai_finalize` RPCs.
- Atomic migration 008 and complete active key registry metadata remain mandatory before concurrent production rollout. A configured but unregistered key intentionally fails the whole provider pool closed.
- A finalize outage after a sent provider request returns a fail-closed error and can leave the ledger row at `sent`; operations should reconcile those rows rather than retrying blindly.
- Live Supabase/provider behavior was not tested because this lane explicitly prohibited all network/provider/Supabase API calls.
- Canonical incident docs and `CHANGELOG.md` were outside this lane's writable scope and must be handled by the integration owner.
