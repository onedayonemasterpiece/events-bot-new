# R15 docs hardening final — results

## Lane contract

- Lane ID: `R15-DOCS-HARDENING-FINAL`
- Requirement IDs: final R15 rail, unusual semantics/state, daily service-share,
  production-candidate preview, Playwright separation and release-evidence
  documentation
- Base SHA: `b9560c111240b1ba46c4291c75402e8c526e0d0d`
- Implementation head SHA:
  `0d420b5a7d13b39e454113074785ff8765035e39`
- Writable files:
  - `docs/features/unusual-events/README.md`
  - `docs/features/static-site-pages/service-sharing.md`
  - `docs/features/static-site-pages/image-framing.md`
  - `docs/features/static-site-pages/astro-preview.md`
  - `docs/operations/e2e-scenarios.md`
  - `.codex/integration/unusual-static-site-r15-execution-matrix.md`
  - `CHANGELOG.md`
  - this required results record
- Forbidden files: all code, tests, configs and other documentation.

## Delivered

- Canonicalized every positively proven crop-safe `visual_only` mobile rail
  asset to horizontal `140×112` (`5:4`) cover regardless of source orientation
  or gallery position, while OCR/document/unknown/error/contradictory evidence
  remains fail-closed.
- Split packaged candidate product Playwright from the separately served local
  noindex ten-state red-dot lab and made candidate lab-route `404` part of the
  acceptance contract.
- Documented persistent ordinary-rebuild `notify_eligible` state and the
  migration/backfill rule: output stays false without erasing durable cached
  eligibility.
- Added the shared-BGE ordinary-corpus distance/receipt and explicit
  canonical-event eligibility projection as mandatory fail-closed semantic
  contracts; diversity caps can honestly underfill and cannot be bypassed.
- Documented the sole `00:00 Europe/Kaliningrad` calendar-rollover enqueue,
  startup catch-up and atomic local-day outbox marker.
- Documented the legacy `build:preview` + `check:preview` pre-gate before the
  production-root and secret-candidate checks.
- Recorded that exact SHA `11d8c984` did run but is superseded. No final
  integration SHA, Kaggle run ID or immutable URL was invented; every such
  field uses the exact placeholder `to be filled by integrator`.
- Synchronized `[Unreleased]` in `CHANGELOG.md`.

## Validation and evidence

```text
node --check site/tests/unusual-events.playwright.mjs
passed
```

```text
node --experimental-strip-types --test \
  site/tests/mobile-listing-rail-media.test.mjs \
  site/tests/mobile-listing-rails.test.mjs \
  site/tests/unusual-events.test.mjs
22 passed
```

```text
node --test site/tests/unusual-events-source-contract.test.mjs
3 passed
```

```text
PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/python \
  -m pytest -p no:cacheprovider -q \
  tests/test_static_site_build_debounce.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_daily_share_enqueue.py \
  tests/test_static_site_builder_preview_contract.py \
  tests/test_static_site_release.py
62 passed
```

Additional checks:

- `git diff --check`: passed.
- Every relative link added by this lane resolves.
- Placeholder assertion found 18 occurrences, all spelled exactly
  `to be filled by integrator`.
- Read-only worker evidence:
  - semantic hardening implementation `23ec1057`, final `be4fa59e`: 28 focused
    and 71 expanded tests passed;
  - rail/Playwright implementation `3346e3f`, final `12155299`: 22 tests,
    packaged product Playwright and separate local lab matrix passed;
  - daily/preview implementation `18779efc`, final `08c8711a`: 62 tests passed.

## Changed files

- `.codex/integration/unusual-static-site-r15-execution-matrix.md`
- `.codex/lanes/R15-DOCS-HARDENING-FINAL/RESULTS.md`
- `CHANGELOG.md`
- `docs/features/static-site-pages/astro-preview.md`
- `docs/features/static-site-pages/image-framing.md`
- `docs/features/static-site-pages/service-sharing.md`
- `docs/features/unusual-events/README.md`
- `docs/operations/e2e-scenarios.md`

## Risks and integration handoff

- This lane base includes the final rail and daily/preview integrations but
  predates the semantic hardening commits. The integrator must merge
  `23ec1057^..be4fa59e` before recording a final integration SHA.
- The superseded `11d8c984` canary is useful historical evidence only. A fresh
  exact-final-SHA Kaggle production-candidate run, immutable candidate URL,
  public HTTP/browser receipts and explicit owner decision remain required.
- No production deploy, root cutover or mutable external action was performed.
- This results metadata is committed immediately after the implementation
  commit; the final branch tip is reported to the integrator.
