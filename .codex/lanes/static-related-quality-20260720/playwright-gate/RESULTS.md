# R03/R04 Playwright release gate results

- Lane: `static-related-quality-20260720/playwright-gate`
- Branch: `agent/static-related-quality/playwright-gate`
- Base SHA: `288b56790ba0866fcbf3da827c499c421425b709`
- Implementation head SHA: `07d14744a8d3bcefd36ed8705ca3585801c5069a`
- Outcome: implemented and committed; no push/deploy.

## Delivered

- Pinned official `playwright@1.61.1` in the site lockfile.
- Added a library-based Chromium gate over generated event documents, including:
  - recommendation-row geometry and computed crop behavior for the dynamically chosen journey plus event `6408` when present;
  - canonical real-navigation `EventCard` Enter behavior in both related and `Ещё события` zones;
  - real gallery recommendation Enter navigation into a new document, followed from natural BODY/HTML focus by ArrowRight/ArrowLeft;
  - footer service P/S copy plus toast from BODY and offscreen event focus, without focusing footer buttons.
- Added candidate-prefix-aware local static serving and fail-closed `checks.browser_visual = "ok"` manifest recording only after all four browser checks pass.
- Kaggle production-candidate flow installs Chromium and runs the gate on root and secret-candidate trees before their respective archives.
- Trusted Fly-side result and candidate-manifest validators now require `browser_visual=ok`.
- CI installs Chromium with OS dependencies, builds real generated preview event pages, and blocks on the browser gate.

## Evidence and commands

Passed:

```text
node --check site/scripts/check-browser-release-gate.mjs
npm run test:browser-release-gate
# 3 tests, 3 passed
python3 -m py_compile static_site_release.py kaggle/StaticSiteBuilder/static_site_builder.py
python3 JSON parse of site/package.json and site/package-lock.json
git diff --check
npm ci --no-audit --no-fund
npx playwright install chromium
PREVIEW_BUILD_ID=preview-ci-browser-gate npm run build:preview
# generated 303 real event pages
```

Actual Chromium blocking evidence on the unintegrated `origin/main` base:

```text
npm run check:browser-release -- --root dist/preview-ci-browser-gate
Browser release gate failed: card 5830 crop mode contain != cover
```

This is expected release-gate evidence, not a gate implementation failure: event 5830 is marked `data-lab-media-treatment="visual-cover"` but resolves to `object-fit: contain` on the current base, demonstrating the existing related/continuation CSS divergence. The integration branch contains the visual fix (`655db32e`) and must rerun both production-root and secret-candidate gates after merge.

Targeted external-tool research after two similar computed-style failures confirmed that Playwright `locator.evaluateAll()` executes DOM APIs in-page and that `getComputedStyle()` returns the resolved post-cascade value. The assertion therefore remains blocking rather than trusting inline attributes.

Not run locally:

- Full `tests/test_static_site_release.py`: repository pytest environment was unavailable (`pytest` missing globally; isolated pytest then failed importing `aiogram` from repository `conftest.py`). CI installs `requirements.txt` and includes this test file.
- Real production/secret candidate generation requires the production snapshot/export identity. Candidate base-path serving and receipt mutation are covered by passing Node tests; combined integration must execute the two Kaggle-equivalent invocations.

## Changed files

- `.github/workflows/ci.yaml`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `site/package.json`
- `site/package-lock.json`
- `site/scripts/check-browser-release-gate.mjs`
- `site/scripts/browser-release-gate.behavior.test.mjs`
- `static_site_release.py`
- `tests/test_static_site_release.py`
- `.codex/lanes/static-related-quality-20260720/playwright-gate/RESULTS.md`

## Risks / integration notes

- The gate intentionally fails against the pre-fix base; do not weaken crop assertions. Rerun after merging the EventCard/related geometry lane.
- Production and candidate manifests are mutated only after browser success and are re-read before archiving; manifest files exclude themselves from their content inventories, preserving tree hashes.
- Documentation and `CHANGELOG.md` were forbidden in this lane and must be updated by the integration owner.
