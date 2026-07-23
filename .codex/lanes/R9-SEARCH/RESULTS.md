# R9-SEARCH results

Status: **complete**

## Scope and provenance

- Requirement: **R8 — Search CTA as the accepted mobile progress button**.
- Base SHA: `74bb254c4d20c1e488568fde445515131e64cbd5`.
- Implementation head SHA: `6e9d9df8377a2cd82aeff740515569b45952cad8`.
- Branch: `agent/mobile-acceptance-r9/search`.
- Accepted public donor: `integration/mobile-search-unified-v14-20260722` at
  `3f5b88f9d8b0c9835908c6b7cf924314deccfb6a`, published as
  `https://kenigevents.ru/preview-20260722-mobile-search-artifact-menu-v28/poisk/`.
- Donor source chain:
  - `c6d6faeb`: backend-owned NDJSON progress, monotonic stages, request epoch
    and `AbortController`;
  - `afa6e710`: progress fill inside the submit button;
  - `2a791e6b`: standalone `366×50px` dark mobile button at a `390px` viewport,
    `8px` radius;
  - `3f5b88f9`: visible `#98401f` fill and accepted indeterminate travel.
- Donor/current 55% computed comparison at `390×844`, DPR 2:
  `366×50px`, radius `8px`, shell `rgb(34, 26, 20)`, fill
  `rgb(152, 64, 31)`, no horizontal overflow.

## Implementation

- Preserved the accepted visual button and hidden adjacent semantic
  `role=progressbar` / live status.
- Closed an actual double-submit race before `getSession()` resolves with a
  synchronous `searchStartPending` guard; the existing disabled/busy state
  continues once the request starts.
- Added one abort/reset owner for logout and `pagehide`. It invalidates the
  request epoch before aborting, so queued stream frames and stale `finally`
  blocks cannot repaint the CTA.
- Success still reaches `100 / Готово` and performs the donor-owned delayed
  reset. Error and abort immediately restore `Искать`, clear progress/busy and
  leave the control retryable.

## Validation

- `PREVIEW_BUILD_ID=preview-r9-search-local ... npm run build:preview`
  — PASS, 389 pages, Authorized Search configured with browser-safe dummy
  public values.
- `PREVIEW_BUILD_ID=preview-r9-search-local npm run check:preview`
  — PASS (`288 events`, `strict_related=false`).
- `node --test tests/search-progress-button.test.mjs
  tests/search-learning.test.mjs tests/search-initial-state.test.mjs`
  — PASS, 16/16.
- `SEARCH_PROGRESS_PREVIEW_DIR=... SEARCH_PROGRESS_EVIDENCE_DIR=...
  node --test tests/search-progress-button.playwright.mjs`
  — PASS, 1/1 at `390×844`, DPR 2.
- Playwright covers indeterminate and 55% visible fill, accessible
  label/status, success/100/reset, error/retry, logout abort/reset, exact one
  request after two synchronous submits, and no horizontal overflow.
- `git diff --check` — PASS.
- Compact uncommitted evidence:
  `artifacts/codex/r9-search/{donor-v28-metrics.json,donor-v28-progress-55-390x844-dpr2.png,r9-search-progress-55-390x844-dpr2.png,node-tests.log,playwright.log,check-preview.log}`.
- Generated `site/dist` and installed `site/node_modules` were removed after
  evidence capture to release disk space.

## Changed files

- `site/src/components/AuthorizedEventSearch.astro`
- `site/tests/search-learning.test.mjs`
- `site/tests/search-progress-button.test.mjs`
- `site/tests/search-progress-button.playwright.mjs`
- `.codex/lanes/R9-SEARCH/RESULTS.md`

## Documentation delta and risks

- Canonical docs delta: **none**. Existing
  `docs/features/unsigned-personalization/authorized-event-search.md` already
  specifies the v24–v28 progress contract; this lane only enforces it.
- `CHANGELOG.md`: intentionally untouched for integrator reconciliation.
- No rail, event-detail or exhibition files were edited.
- Remaining external acceptance: a real authenticated Edge call/Yandex
  round-trip was not required for this focused mocked browser lane.
