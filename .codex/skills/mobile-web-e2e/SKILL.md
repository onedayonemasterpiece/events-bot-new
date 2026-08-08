---
name: mobile-web-e2e
description: Use in events-bot-new for any Android Emulator/Chrome or iOS Simulator/Mobile Safari browser E2E work, including Appium UiAutomator2/XCUITest setup, native keyboards/touch, Safari first-run UI, WebView attachment, auth callbacks, same-storage sessions, Search, OTP, and new mobile web scenarios. Requires reuse of the shared accepted mobile transport instead of feature-local Appium reinvention.
---

# Mobile Web E2E

Use this skill only in `/home/dev/projects/events-bot-new`.

## Mandatory first read

Before changing a workflow or adapter, inspect:

1. `site/e2e/mobile-web/` — shared transport, capabilities and Safari startup;
2. `site/e2e/focus-email/adapters/appium-ui.mjs` — accepted OTP consumer;
3. `.github/workflows/external-focus-email-otp.yml` — proven Android/iOS runner setup;
4. `docs/testing/external-focus-email-otp.md` — failure history and accepted mechanics;
5. `docs/operations/e2e-scenarios.md` — terminal receipts;
6. the feature journey/adapter being changed.

Run `rg -n "appium|UiAutomator2|XCUITest|Mobile Safari|webviewConnect" site/e2e .github/workflows docs/testing docs/operations` before creating any mobile infrastructure.

## Architecture rule

- Reuse `site/e2e/mobile-web/` for Appium capabilities, connection budgets, native-first Safari stabilization and WebView attachment.
- Feature journeys own only product selectors and assertions. They must not own a divergent copy of mobile session startup.
- Do not put a new feature under `site/e2e/focus-email/`; OTP is one consumer, not the shared layer.
- If the shared layer lacks a needed primitive, add it there with regression tests and verify both OTP and the new consumer.
- Never replace native Android/iOS acceptance with desktop Playwright device emulation.

## Proven baseline

- Android: real Chrome in Android Emulator, UiAutomator2, WebDriver Classic, software keyboard visible, Appium touch gestures.
- iOS: launch `com.apple.mobilesafari` natively, exact allowlisted first-run dialog handling, then attach WebKit; do not start with `browserName: Safari` on a clean simulator.
- iOS WebView discovery uses the shared bounded 60-second connection window and retry profile; do not fall back to XCUITest's five-second default.
- Focus a critical iOS web input through shared `focusIosSafariWebInput`: an exact allowlisted native accessibility match followed by a native tap and `observeNativeKeyboard`. A WebKit `click()` plus a web-context `isKeyboardShown()` is not keyboard evidence.
- Scroll browser documents only through shared `performNativeDocumentSwipe` in `NATIVE_APP`, resolving viewport dimensions after the context switch and restoring the original web context before reading `scrollY`. It routes Android Chrome to an absolute-coordinate W3C touch swipe and iOS Safari to XCUITest's application-level `mobile: swipe`; Safari's W3C pointer source can acknowledge without delivering the gesture to WebKit. Web-context/CSS dimensions are not native touch coordinates. Appium's `mobile: scrollGesture` (UiAutomator2) and `mobile: scroll` (XCUITest) are native control/table shortcuts and are not accepted as proof for browser page content.
- Dismiss an observed software keyboard through the shared native-context helper before measuring the scrolling baseline. XCUITest Safari may return its exact `Did not know how to dismiss the keyboard` response; only that exact iOS response may trigger a user-equivalent fallback. Prefer one caller-declared exact non-actionable `XCUIElementTypeStaticText` outside the field and tap its resolved native rect; use a downward application swipe only when the scenario has no safe static target. Then require `isKeyboardShown() == false`. Never use a generic coordinate that could hit submit/resend UI, treat the error itself as success, swallow other driver errors, or scroll while the IME is visible.
- If the exact safe target is missing or ambiguous, diagnose the native accessibility bridge only through closed exact-label counts for the allowlisted XCTest types (`StaticText`, `Other`, `Button`, `Link`), split into total and visible counts. Do not upload a hierarchy, labels, page source or nearby text, and do not broaden the actionable type until one live receipt proves the actual mapping.
- Prefer the product's ordinary submit lifecycle to release a validated mobile search input before its request/result state. A real native Search/Enter key followed by the product's `blur()` is a user-triggered IME transition; it is safer than adapter-only coordinate taps. The harness must still observe the native keyboard absent before accepting document scroll.
- Use one bounded WebDriver session creation attempt. Reuse the accepted OTP
  workflow pattern for at most one workflow-level Appium restart only when a
  sanitized receipt proves session creation failed before callback/product
  traffic. The retry must create a fresh WebDriver session attempt, reuse only
  the still-unconsumed callback, and never repeat an ambiguous callback/Search
  action.
- For authenticated scenarios, use a fresh persona-scoped credential/session per device, complete callback in the device browser, wait for the authorized UI, then reload the exact target to prove same-storage persistence.
- For broker-issued magic links, never open the default admin `action_link` directly: use the shared fail-closed converter to an allowlisted `token_hash/type` target callback so verification and persistence occur inside the device browser. Never extract or inject access/refresh tokens.

## Acceptance and evidence

Require real native keyboard observation and real touch scrolling. Native/web context switching must use the shared restore-on-finally helpers. Preserve feature-specific assertions such as request counts, terminal state, rendered IDs, pagination and duplicate checks.

Artifacts must be sanitized and allowlisted. A failed gesture may retain only route, native viewport/start/end/duration numbers, gesture count, DOM `scrollY` delta and booleans. Never upload raw Appium logs, page source/native hierarchy after sensitive input, action links, target bearer paths, tokens, email, OTP, storage state, screenshots containing credentials, HAR, trace or video. A `.redaction-ok` marker is required before upload.

## Debug workflow

1. Read the exact failed step and sanitized artifact; classify runner/session/browser/product failure. For a session-create timeout, retain only the shared closed Appium phase receipt (server ready, simulator/WDA phase booleans, elapsed time, attempt number and truncation flag); never upload the raw log. A truncated log can diagnose but can never authorize retry.
2. Compare the failed configuration with the shared profile and the latest terminal Android/iOS receipts.
3. Add the smallest failing regression test before changing code.
4. Change one transport variable at a time. After two similar external-tool failures, consult official Appium driver documentation before another attempt.
5. Run both the shared mobile/OTP contract tests and the feature harness locally.
6. Run one live device job on an already published exact target; do not trigger a full static build to debug adapter mechanics.
7. Only after terminal device PASS, integrate into strict release/post-deploy/scheduled gates.

Any new durable behavior must update the canonical docs, incident regression record when applicable, and `CHANGELOG.md`.
