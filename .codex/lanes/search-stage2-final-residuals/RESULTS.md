# Search Stage 2 Final Residuals

- Lane ID: `search-stage2-final-residuals`
- Requirements: request-start terminal tracking; bounded Playwright/Appium quiet boundary; nonthrowing failed evidence; instrumentation UNKNOWN classification; `B15`, `B17`, `B18`, `B20`
- Base SHA: `cb289a9f7704590b70f746fa016d776351b7e7a0`
- Validated implementation SHA: `0628c5573`

## Outcome

- Appium CDP tracking now treats `requestWillBeSent` as pending across log drains until a valid Content-Length or `loadingFinished` closes it. `loadingFinished.encodedDataLength` remains a conservative on-wire byte count; no body fetch is performed.
- Mobile Auth closure is correlated to the actual `/auth/v1` request category rather than the same-origin token-hash callback URL.
- Playwright observes relevant request start/finish and uses a bounded quiet boundary. Appium drains to both terminal closure and a bounded quiet boundary.
- Failed journey evidence returns the preserved Search-page snapshot and best-known physical count without requiring successful final meter/network access.
- Instrumentation terminal/meter failures map to platform `UNKNOWN_*`.
- Runtime physical metering covers cold transport probes and discarded direct/relay retry responses without double-counting the final Search response.
- The pre-Search snapshot enforces the hard cap before submit.
- Relay-origin receipt/storage traffic is forbidden-counted and relay responses are metered.
- Appium target-open network drains now feed cumulative closed diagnostics.

## Validation

- Focused Search journey/mobile/harness tests: PASS.
- `npm run test:search-production-health`: PASS, 122/122.
- `npm run test:search-e2e-harness`: PASS, 31/31.
- `git diff --check`: PASS.

No live browser, Appium device, Supabase, deployment, workflow, reporter, SQL, canonical documentation, or CHANGELOG mutation was performed in this lane.

## Changed files

- `site/e2e/mobile-web/appium-network-receipt.mjs`
- `site/e2e/search/adapters/appium-base.mjs`
- `site/e2e/search/adapters/playwright.mjs`
- `site/e2e/search/adapters/runtime-probe.mjs`
- `site/e2e/search/production-health-journey.mjs`
- `site/e2e/search/production-health-run.mjs`
- `site/tests/search-production-health-journey.test.mjs`
- `site/tests/search-production-health-mobile-preflight.test.mjs`

## Remaining risk

Deterministic coverage is green; live browser/device timing and real provider logs still require the integration workflow.
