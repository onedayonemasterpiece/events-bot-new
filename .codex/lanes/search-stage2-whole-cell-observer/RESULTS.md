# Search Stage 2 Whole-Cell Observer

- Lane ID: `search-stage2-whole-cell-observer`
- Requirements: `P1`–`P6`, including `B15`
- Base SHA: `e23001d210bd91384a0162aaf0d8fffc9eb522a0`
- Validated implementation SHA: `562a9b456`

## Outcome

- Added adapter-level physical observation from the pre-Auth/navigation boundary through final route completion for browser and Appium: physical Search POST count, direct/relay Supabase bytes, Storage, receipt RPC, pending request closure, and bounded quiet gates.
- Journey now waits for the physical observer before submit, requires zero absolute pre-submit Search/Storage/receipt traffic, requires zero runtime pending measurements, and blocks on an absolute pre-submit hard-cap breach.
- Journey evidence uses the physical new-page meter delta. Runner always merges that delta with the separately verified issued Auth meter, including the prior mobile cumulative case.
- Runtime probe now wraps the actual already-constructed `ResilientSupabaseTransport.rawFetch` physical boundary. High-level transformed Responses are not double-counted; probes and discarded direct/relay safe-read responses remain counted.
- Failure evidence prefers authoritative physical Search/forbidden counters. Missing observer/meter codes are platform `UNKNOWN_*`.
- Cache write status is bounded telemetry only (`store_failed`, `skipped`, and other bounded values remain healthy when the Search response/cards are valid).

## Validation

- Real `ResilientSupabaseTransport` regression: transformed final Response, physical functions probe, discarded 503 safe-read response, and alternate response counted exactly once.
- Delayed >96 KiB Playwright page-init response is observed after quiet waiting and blocks before Search.
- Appium whole-cell counter/meter spans independent driver-log drains.
- `npm run test:search-production-health`: PASS, 128/128.
- `npm run test:search-e2e-harness`: PASS, 31/31.
- `git diff --check`: PASS.

No live browser/device/Supabase/deploy run and no workflow, documentation, CHANGELOG, broker, reporter, or SQL edits were made.

## Changed files

- `site/e2e/mobile-web/appium-network-receipt.mjs`
- `site/e2e/search/adapters/appium-base.mjs`
- `site/e2e/search/adapters/playwright.mjs`
- `site/e2e/search/adapters/runtime-probe.mjs`
- `site/e2e/search/evidence.mjs`
- `site/e2e/search/production-health-contract.mjs`
- `site/e2e/search/production-health-journey.mjs`
- `site/e2e/search/production-health-meter.mjs`
- `site/e2e/search/production-health-run.mjs`
- `site/tests/search-production-health-contract.test.mjs`
- `site/tests/search-production-health-journey.test.mjs`
- `site/tests/search-production-health-mobile-preflight.test.mjs`

## Remaining risk

Deterministic real-transport and adapter boundary coverage is green. Actual hosted browser/device timing and provider logs still require the integration workflow.
