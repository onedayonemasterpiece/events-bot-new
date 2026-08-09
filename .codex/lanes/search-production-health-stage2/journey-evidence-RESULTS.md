# Stage 2 journey/evidence lane — RESULTS

- Lane: `journey-evidence`
- Requirements: one normal vector-only UI Search POST; exact current accepted target pin/supersession; Auth/owner-RLS proof interface; response↔DOM/card/scroll/HTTP-200 acceptance; provider counters; Supabase byte meter; strict evidence redaction; production runner with preflight-before-issuance ordering.
- Base SHA: `dd1e0ad9072acdad46f01459ba4ab0ff171e0318`
- Implementation head SHA: `bc0b182542ef03d153f43c14f7e15995bf8b6c8d`
- Outcome: PASS (deterministic only; no browser, Appium, Supabase, Fly, deployment, or live Search executed).

## Changed files

- `site/e2e/search/acceptance.mjs`
- `site/e2e/search/adapters/playwright.mjs`
- `site/e2e/search/adapters/runtime-probe.mjs`
- `site/e2e/search/evidence.mjs`
- `site/e2e/search/production-health-journey.mjs`
- `site/e2e/search/production-health-meter.mjs`
- `site/e2e/search/production-health-run.mjs`
- `site/e2e/search/production-health-target.mjs`
- `site/e2e/search/provider-counters.mjs`
- `site/tests/search-production-health-journey.test.mjs`

## Evidence

- `npm --prefix site run test:search-production-health` — PASS, 40/40.
- `npm --prefix site run test:search-e2e-harness` — PASS, 30/30.
- `node --check site/e2e/search/production-health-run.mjs` — PASS.
- `node --check site/e2e/search/production-health-journey.mjs` — PASS.
- `node --check site/e2e/search/adapters/runtime-probe.mjs` — PASS.
- `git diff --check` — PASS.

Covered deterministic failures include duplicate POST, explicit canary execution mode, LLM attempt, result count >5, receipt RPC, Supabase Storage, cross-origin/failed event route, preflight side effects, release-not-active, pointer supersession without retry, 48/96 KiB meter thresholds, current `request_counters` parsing, and evidence omission of query/secret target/session/raw errors.

## Integration notes / risks

- Appium integration must implement `verifyAuthenticatedOwner()` by installing the runtime probe, executing exported `verifyAuthenticatedOwnerRuntimeProbe()`, and returning `{receipt, meter}`; its `waitForTerminal` must wait for `probe.meter.pending === 0`. This is intentionally left to the mobile-owned file lane.
- Broker integration must preserve `createAuthSessionFixture({ platform })` forwarding for browser and the finalized mobile `issuer.issue({personaId, platform, redirectTo, runId})` contract.
- The production CLI is built in for browser/Android/iOS; live infrastructure and credentials were deliberately not exercised in this lane.
- Initial `npm ci` hit host `ENOSPC`; tests used the already complete integration-worktree `site/node_modules` through a local ignored symlink. This does not affect committed code.
