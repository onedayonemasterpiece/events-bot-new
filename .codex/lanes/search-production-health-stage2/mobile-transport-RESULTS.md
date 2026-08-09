# Stage 2 mobile transport lane results

- Lane ID: `mobile-transport`
- Requirement IDs: `R04`, `R05`, `R06`, `R08`
- Base SHA: `dd1e0ad90e014f20fd63ed45fb36f20395610889`
- Implementation head SHA: `435a8cf850ee6a459143f13545b2285c63d4c31e`
- Status: `DONE`
- Effort/risk: `high` (cross-platform Appium lifecycle and fail-closed evidence)

## Delivered

- Shared side-effect-free Appium transport preflight for real Android Chrome/UiAutomator2 and iOS Mobile Safari/XCUITest/WDA.
- Allowlisted startup/device receipt with native viewport and context classes; no raw context IDs, session IDs, URLs, logs, hierarchy, screenshots, or credentials.
- Same-process continuation contract: `create*Adapter()` -> `adapter.preflight()` -> broker/auth -> journey on the same adapter.
- Explicit attempt-one retry proof requires zero broker/Auth/navigation/fetch/Search activity and confirmed cleanup of any failed WebDriver session.
- iOS preparation failure now deletes its Appium session; successful close purges local/session auth state and cookies before `deleteSession`.
- Existing native keyboard and real native touch scrolling paths are preserved.
- `openFirstResult()` captures the first result, opens it through the UI, and requires a same-origin main-document HTTP 200 from sanitized Android/Safari driver network events.
- Diagnostic CLI is explicitly not a production handoff: it closes the session and emits no serializable continuation handle.

## Changed files

- `site/e2e/mobile-web/appium-network-receipt.mjs`
- `site/e2e/mobile-web/appium-preflight.mjs`
- `site/e2e/mobile-web/appium-startup-receipt.mjs`
- `site/e2e/search/adapters/appium-base.mjs`
- `site/e2e/search/production-health-mobile-preflight.mjs`
- `site/tests/search-e2e-mobile-startup.test.mjs`
- `site/tests/search-production-health-mobile-preflight.test.mjs`

## Evidence

Commands run from the lane worktree:

```text
npm --prefix site ci --no-audit --no-fund
npm --prefix site run test:search-e2e-harness
# 31 tests, 31 pass, 0 fail
npm --prefix site run test:search-production-health
# 38 tests, 38 pass, 0 fail
node --check site/e2e/mobile-web/appium-preflight.mjs
node --check site/e2e/mobile-web/appium-network-receipt.mjs
node --check site/e2e/mobile-web/appium-startup-receipt.mjs
node --check site/e2e/search/adapters/appium-base.mjs
node --check site/e2e/search/production-health-mobile-preflight.mjs
git diff --check
```

No emulator, simulator, Appium server, broker, Auth callback, Search request, navigation, or other live operation was run in this lane.

## Integration contract

The production-health runner must not launch `production-health-mobile-preflight.mjs` as a separate workflow process. It must create the platform adapter, call `preflight()` before broker issuance, continue the exact same in-memory adapter through Auth and the journey, and call `close()` in `finally`. A failed attempt may be retried once only when `isSafeMobilePreflightRetryReceipt(error.searchReceipt)` is true.

## Residual risks / live gates

- Actual GitHub Android and macOS runner Appium capability shapes and driver log types still require the bounded live qualification requested for Stage 2.
- Safari/Chrome must expose a `Network.responseReceived` main-document event; absence fails closed rather than substituting a page fetch.
- If the web context is unavailable during terminal cleanup after Auth, cleanup still attempts `deleteSession` but reports `mobile_auth_local_purge_unconfirmed`.
