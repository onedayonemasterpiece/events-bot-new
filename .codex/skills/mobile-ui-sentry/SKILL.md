---
name: mobile-ui-sentry
description: "Observe, classify, and safely resolve native Android/iOS UI blockers around mobile-web, hybrid, emulator, simulator, Appium, XCUITest, UiAutomator2, Safari/Chrome first-run dialogs, permission prompts, choosers, keyboards, and stuck mobile E2E runs. Use whenever a mobile test cannot reach a DOM element, a screenshot shows system chrome or an overlay, an action reports success without visible progress, or Codex is designing a mobile browser preflight/release gate."
---

# Mobile UI Sentry

Treat every mobile action as an observation loop, not as proof of success:

`observe -> classify layer -> act once -> observe -> assert transition`

Use Appium with XCUITest/UiAutomator2 as the deterministic runner. Use
Playwright only after proving that native system UI is clear. Read
[`references/appium-system-ui.md`](references/appium-system-ui.md) before
changing an iOS alert locator or Appium capability.

## Start-of-session sentry

Before searching the page DOM or typing:

1. Capture a safe full-device screenshot immediately after browser launch.
2. Record foreground application, orientation, viewport, available driver
   contexts and pinned OS/browser/driver versions.
3. Switch to `NATIVE_APP` and classify the visible layer.
4. Run only an allowlisted known-blocker handler.
5. Verify stable disappearance before returning to a web context.

Do not wait for the product element timeout to inspect the device screen. If
the expected marker has not appeared within 15-20 seconds, take the second safe
screenshot and run the sentry again.

## UI-layer classification

Assign exactly one class before acting:

- `WEB_CONTENT` — product DOM is unobstructed;
- `BROWSER_FIRST_RUN` / `SEARCH_ENGINE_CHOICE`;
- `PERMISSION_PROMPT`;
- `SIGN_IN_PROMPT`;
- `INTENT_CHOOSER` / `SHARE_SHEET`;
- `KEYBOARD`;
- `BROWSER_CHROME`;
- `LOCK_SCREEN`;
- `UNKNOWN_SYSTEM_UI`.

If evidence is ambiguous, use `UNKNOWN_SYSTEM_UI` and block. Never infer
`WEB_CONTENT` merely because a DOM query finds an element underneath an
overlay.

## Action contract

- Prefer accessibility id/name, then exact native predicate/text, then a
  stable class-chain relation. Avoid XPath for XCUITest when native predicate
  or current-alert APIs express the contract.
- Use an image-template fallback only for a separately reviewed, non-sensitive
  diagnostic lane. Fixed coordinates are never release evidence.
- Never use generic `autoAcceptAlerts`, fuzzy button text, the first matching
  button, or a blind coordinate tap for a system dialog with multiple actions.
- Bind a known action to the same current dialog as the exact known title.
- Perform one action, then require the blocker to be absent for at least three
  bounded samples and the expected next-state marker to appear.
- A successful `click()` response is only `action_dispatched`, never PASS.

For the KenigEvents Safari search-engine choice, accept only one current alert
with exactly one full alert-text line `Выбор поисковой системы`, exactly one
button `Продолжить`, and one exact-label WDA alert action. `Настройки` is never
an acceptable fallback.

## No-progress watchdog

Stop blind work when either threshold is reached:

- two actions produce the same sanitized screenshot/state fingerprint; or
- 20 seconds pass without the expected state transition.

Then:

1. stop action retries;
2. capture a safe screenshot and safe state counters/fingerprint;
3. verify the active application and context;
4. inspect the exact known blocker contract;
5. classify as `BLOCKED_*` within 60 seconds if unresolved.

Do not try a third similar external-tool hypothesis until official
documentation/source has been compared with the current runtime, capabilities
and code.

## Keyboard boundary

Dismiss or block native overlays before issuing any keyboard verdict. Prove a
keyboard using all relevant signals: exact physical/native tap, native keyboard
presence, DOM focus/input mode and usable focused `visualViewport`. A dispatched
tap under an overlay is a system-UI block, not a keyboard failure.

## Sensitive journeys

For OTP, login and account flows:

- take the initial screenshot before inserting identity or OTP;
- mask/clear sensitive inputs before later screenshots;
- never retain raw native hierarchy, page source, video, Appium log, mail body,
  address, OTP, cookie, JWT or authorization headers;
- retain only allowlisted counts, booleans, failure classes, hashes and version
  provenance;
- never rerun after a side effect until request/mail/verification counts prove
  it is safe.

A raw native hierarchy may be inspected interactively only in an isolated
non-sensitive reproduction and must not be uploaded or committed. Prefer exact
queries and sanitized count evidence in CI.

## Terminal evidence

Record:

- state class before/after;
- known/action/blocking/unknown counts;
- action route and attempt count;
- stable-absence samples;
- safe screenshot names/hashes;
- active app/context and version provenance;
- side-effect counts;
- exact terminal `PASS`, `FAIL_*`, or `BLOCKED_*` domain.

Never promote a workflow conclusion to test PASS without reading its terminal
summary and evidence.
