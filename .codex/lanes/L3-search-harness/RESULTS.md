# Lane L3-search-harness Results

## Status

committed

## Requirement IDs

- R01
- R03
- R06

## Branch

`agent/search-live-automation/harness`

## Worktree

`/dev/shm/search-live-automation-20260807/harness`

## Base SHA

`ec09c011674eecddf9e9b8e154e3d102f9384b12`

## Head SHA

Implementation commit: `a1fdc3ec3` (`test(search): add platform-neutral live acceptance harness`).
The final lane SHA also contains this results record and is reported to the integrator because a file cannot contain the SHA of its own commit.

## Files changed

- `site/e2e/search/run.mjs`
- `site/e2e/search/journey.mjs`
- `site/e2e/search/acceptance.mjs`
- `site/e2e/search/canary-manifest.mjs`
- `site/e2e/search/evidence.mjs`
- `site/e2e/search/adapters/playwright.mjs`
- `site/e2e/search/adapters/appium-base.mjs`
- `site/e2e/search/adapters/appium-android.mjs`
- `site/e2e/search/adapters/appium-ios.mjs`
- `site/e2e/search/adapters/runtime-probe.mjs`
- `site/tests/search-e2e-acceptance.test.mjs`
- `site/tests/search-e2e-journey.test.mjs`
- `site/package.json`
- `.codex/lanes/L3-search-harness/RESULTS.md`

## Commands run

- `node --check site/e2e/search/run.mjs`
- `node --check site/e2e/search/journey.mjs`
- `node --check site/e2e/search/adapters/playwright.mjs`
- `node --check site/e2e/search/adapters/appium-base.mjs`
- `node --test tests/search-e2e-*.test.mjs`
- `npm_config_cache=/dev/shm/npm-cache-harness npm run test:search-e2e-harness`
- `node -e "Promise.all([import('./e2e/search/adapters/playwright.mjs'),import('./e2e/search/adapters/appium-android.mjs'),import('./e2e/search/adapters/appium-ios.mjs')])..."`
- `git diff --check`

The first plain `npm run test:search-e2e-harness` attempt could not write npm state because the root filesystem was full (`ENOSPC`). The same package script passed with npm cache redirected to `/dev/shm`; direct `node --test` also passed.

## Tests / verification

- Focused harness suite: **7/7 PASS**.
- Syntax checks: **PASS** for all new runner, journey, acceptance, evidence, manifest, probe and adapter modules.
- Adapter module import check with installed Playwright/WebdriverIO packages: **PASS**.
- Journey source contract test proves it contains no Playwright/Appium/WebDriver/mouse/touch mechanics.
- Exact secret-target identity contract test covers `/_review/<43-base64url>/poisk/`, `candidate-build.json`, schema `static_secret_candidate_build_v1`, and mandatory `E2E_EXPECTED_REPO_SHA`.
- Evidence tests reject session/JWT/email/raw secret-candidate paths and preserve only redacted target paths, IDs, counters and bounded receipts.
- Mocked semantic journey proves the three required stable regression queries, one-submit/one-POST, terminal cards, ID correspondence, pagination, duplicate rejection, real-scroll receipt, cache repeat, typed empty, invalid-query zero POST, and selected-once route receipts.

## Risks

- Terminal live execution was intentionally not claimed in this isolated lane: it requires an exact published candidate plus an ephemeral `auth.session_fixture` storage state or one-time broker action link from the auth lane.
- Android/iOS adapters require real Appium device jobs for terminal evidence; local tests verify their contract and native keyboard/touch command ownership only.
- Route acceptance deliberately fails closed if the shared resilient transport does not expose one `outcomeHistory()` selected-once receipt per Search POST.
- Pagination is blocking for the first incident regression query; a target that cannot expose a real `Показать ещё` page correctly fails acceptance rather than weakening the requirement.

## Merge notes

- Selectively ports the useful PR #284 Search concepts without merging the PR and without importing or modifying `site/e2e/focus-email/**`.
- Runner contract: `npm --prefix site run e2e:search-live` with required `E2E_SEARCH_TARGET_URL`, `E2E_EXPECTED_REPO_SHA`, `E2E_SEARCH_VARIANT`, and ephemeral auth bootstrap (`E2E_AUTH_STATE_PATH` for browser or `E2E_AUTH_ACTION_LINK` for any platform).
- Exact canary execution values are `cached_vector`, `cold_vector`, `cold_vector_llm`, and `degraded_vector_fallback`; adapters preserve the existing POST and add/retain UUID `client_request_id` plus the exact `execution_mode`.
- Secret-candidate navigation adds `?search_variant=<exact mode>` in memory, matching the L6 UI contract. The token is never written to evidence.
