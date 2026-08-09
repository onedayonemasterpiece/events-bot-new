# R08 Results — Unusual Events Browser E2E

- Base SHA: `7598de224e64659c31325c9f5bc1c39f03c4e6ff`
- Implementation SHA: `948c526ba`
- Branch: `feat/unusual-browser-e2e`
- Status: complete

## Delivered

- Added a health-bound Playwright browser monitor for
  `schema_version=unusual-events-health-v1`.
- Added exact ordered event/concept projection parity, ready/empty and HTTP
  shell checks, bounded link probes, card/title/href/date/image checks,
  canonical/index checks, console/page/request/provider checks, and horizontal
  overflow checks.
- Added mandatory `390x844` and `1728x900` viewport receipts and full-page
  screenshots.
- Added bounded structured receipt output without the base/candidate URL and
  candidate-token redaction for browser diagnostics and stdout.
- Added generic ready/empty feed markers and explicit card event IDs to the
  unusual listing DOM.

## Changed files

- `site/src/components/UnusualListingSurface.astro`
- `site/tests/unusual-events.playwright.mjs`
- `site/tests/unusual-events-monitor.mjs`
- `site/tests/unusual-events-monitor.test.mjs`
- `.codex/lanes/R08/RESULTS.md` (lane evidence only)

## Commands and evidence

Passed:

```text
node --check site/tests/unusual-events-monitor.mjs
node --check site/tests/unusual-events.playwright.mjs
node --test site/tests/unusual-events-monitor.test.mjs site/tests/unusual-events.test.mjs
# 11 tests passed
git diff --check
```

Real Chromium smoke against a bounded local candidate fixture:

```text
UNUSUAL_EVENTS_HEALTH_FILE=/tmp/unusual-health.json \
UNUSUAL_EVENTS_BASE_URL=http://127.0.0.1:43119/_review/<redacted>/ \
UNUSUAL_EVENTS_SCREENSHOT_DIR=/tmp/unusual-shot-2 \
UNUSUAL_EVENTS_BROWSER_RECEIPT=/tmp/unusual-receipt-2.json \
node site/tests/unusual-events.playwright.mjs
```

Observed receipt: `status=READY`, `page_manifest_match=true`,
`browser_mechanics_passed=true`; both viewports passed with two ordered cards
and zero failures. Both PNG screenshots were present. A grep assertion found
neither the candidate token nor the local base URL in the JSON receipt.

Additional integration command:

```text
node --test site/tests/unusual-events-monitor.test.mjs \
  site/tests/unusual-events.test.mjs \
  site/tests/unusual-events-source-contract.test.mjs
```

The R08 and component tests passed. One pre-existing broad source-contract
assertion failed because the base branch's calendar implementation uses
`destinationAvailable` rather than the test's stale `!hasEvents` source regex.
R08 does not own that calendar component/test and did not modify it.

## Risks

- No immutable remote candidate was supplied in this lane, so the live smoke
  used a local HTTP fixture. The runner itself used real Chromium and its real
  network/image/screenshot paths.
- Existing legacy `product` and `lab` modes remain available when
  `UNUSUAL_EVENTS_HEALTH_FILE` is unset.
