# F18 UI lane results

- Branch: `agent/f18-service-share/ui`
- Base: `c279f7db40b619474dff0c077fa95c7bace26562`
- Head: this handoff commit (reported to integrator after commit)

## Implemented

- One reusable footer-only `ServiceShareAction` with the approved Russian product copy, D0 default, configurable D1/D2 desktop modes, mobile Web Share and canonical KenigEvents URL.
- Manifest validation, asset integrity/MIME checks, prefetching that never consumes transient activation, safe rich clipboard candidates, deterministic D0 fallbacks, cancellation/error handling, and bounded telemetry.
- `/lab/service-share/` noindex harness with D0/D1/D2 controls, capability diagnostics, preview, controlled paste targets, elapsed time, and an in-memory bounded ledger.
- Preview static checks plus unit and Playwright contract suites.
- Preview deploy policy for immutable versioned service-share assets, uncached current manifest, public MIME/HEAD verification, and no writes to stable ICS objects.

## Validation

- PASS: `npm run test:service-share` (5/5)
- PASS: `node --check site/scripts/check-preview.mjs`
- PASS: `node --check site/scripts/deploy-preview-yc.mjs`
- PASS: `git diff --check`
- PARTIAL: preview Astro build reached route generation successfully, but was stopped to meet integration handoff timing before completing the repository's long full fixture build.
- BLOCKED locally: Playwright suite is discovered as 11 tests and web server starts, but installed Playwright 1.61.1 has no matching Chromium headless shell (`chromium_headless_shell-1228`). Run `cd site && npx playwright install chromium && npm run test:service-share:playwright` at integration gate.
