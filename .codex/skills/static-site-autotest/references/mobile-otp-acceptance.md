# Mobile browser-tab OTP acceptance recipe

Use this recipe for a real Android Chrome or iOS Mobile Safari OTP scenario.
The canonical project implementation is `site/e2e/focus-email/`; extend it
instead of copying it.

## Architecture

1. Keep the product journey semantic and platform-neutral: open, focus email,
   enter email, issue once, receive once, focus OTP, type digits, verify once,
   register once, reload, confirm returning state.
2. Put Playwright/Appium mechanics behind adapters. Assert side-effect counts in
   the shared journey, not inside a device-specific script.
3. Validate the deployed target origin/path and the full SHA from immutable
   preview metadata before the first external side effect.
4. Run real-mail variants sequentially while they share a recipient.
5. Emit one schema-compatible, redaction-gated evidence bundle per platform.

## Android gate

- Use a pinned Android Emulator profile/API plus real Chrome and UiAutomator2.
- Verify `/dev/kvm` is readable and writable before emulator launch.
- Allow Chromedriver discovery only for the UiAutomator2 driver and retain the
  actual browser version in `device.json`.
- Prove the email and numeric keyboards through native keyboard presence, DOM
  focus/input mode and usable viewport geometry.

## iOS gate

- Pin macOS, Xcode, iPhone Simulator and iOS runtime. Create the exact simulator in `Shutdown`, pass its UDID, and let XCUITest/Appium boot and shut it down. Do not add external `simctl boot`, `open -a Simulator`, or global `defaults` mutations.
- Download the XCUITest driver's matching official prebuilt WDA; record Appium, driver, WDA and Xcode provenance.
- Start the iOS Safari session natively with
  `appium:bundleId=com.apple.mobilesafari`, no `browserName`, and
  `appium:settings[respectSystemAlerts]=true`. Remain in `NATIVE_APP` until the
  system sheet is proven absent; only then discover/attach the Safari WebView
  and navigate. Run `30767191144`, attempt 2, is the live proof for this order.
- Set `connectHardwareKeyboard=false` and `forceSimulatorSoftwareKeyboardPresence=true`; do not use Simulator menu or `Cmd-K` rescue gestures.
- Before product input, run a side-effect-free preflight: empty injected email and numeric controls first, then the empty product email field. A control failure is `BLOCKED_IOS_SIMULATOR_KEYBOARD`; a passing control plus failing product field is a product/browser-context failure. Require three terminal preflight passes before one full real-mail iOS OTP run.
- Stabilize Safari native UI through a bounded allowlist state machine. It may dismiss only one dialog containing the exact title `Выбор поисковой системы` and exact action `Продолжить`, then must observe stable disappearance. Unknown, ambiguous, missing-action or stuck dialogs are `BLOCKED_SAFARI_FIRST_RUN_UI`; never type through them and never use a generic alert accepter.
- Poll keyboard presence through a bounded animation window after one exact physical tap. Record activation attempts plus baseline/focused `visualViewport` geometry and an empty-field screenshot; never infer success from a dispatched gesture.
- Keep navigation in WebKit after the native-first boundary. For a keyboard-critical input, scroll it into view, switch to `NATIVE_APP`, locate exactly one labelled XCTest text field, and use `mobile: tap` at XCTest's own rect center. Never use JS value assignment, guessed coordinate transforms or blanket native web tap. A full hierarchy is allowed only in the clean simulator before candidate navigation/identity input and must be stored as the short-lived `ios-startup.raw.xml`; never capture it after identity or OTP input.
- Mask the derived recipient immediately at creation. After mailbox extraction, mask the OTP and register it with the UI adapter before any WebDriver command.
- A navigation-at-`about:blank` startup block may consume one pre-side-effect Appium retry. Retry only allowlisted zero-side-effect startup/Safari states; never retry an OTP attempt.
- Keep the sole competing issuance batch in the web context and still require exactly one issue.

## Evidence and failure rules

Require all of the following for PASS:

- exact target SHA observed;
- one delivered message and one unambiguous six-digit code;
- exactly one `/auth/v1/otp`, one `/auth/v1/verify`, and one participant RPC;
- successful registration status and returning state after reload;
- platform identity plus email/OTP keyboard acceptance;
- redaction audit success.

Classify failure before retrying. A failure before OTP issuance is safe to
repeat. After issuance, never resend blindly: first prove whether the message,
verify call or registration occurred. Stop trial-and-error after two similar
external-tool failures and compare the official Appium/driver contract with the
current capabilities and runtime.

Durable evidence must omit email, OTP, raw mail, cookie/JWT/auth headers, HAR,
trace, video and unsanitized native hierarchy. Keep only platform versions,
step outcomes, host classes, request paths/statuses, message count, latency,
OTP length and hashed message identity. The sole hierarchy exception is the
clean pre-navigation iOS startup artifact described above.

## Transport-failure OTP

- Use `docs/testing/transport-fault-profiles.v1.yml`; never invent a profile in
  workflow input or query parameters.
- Inject at the `ResilientSupabaseTransport` constructor `fetchImpl` boundary
  before `ResilientDataClient` and Auth singletons. A post-navigation
  `window.fetch` patch is evidence instrumentation only, never fault control.
- Build one immutable profile per preview and bind route-cache namespace,
  preview metadata and harness expectation to the profile plus registry digest.
- For `client_supabase_direct_unreachable`, fault the direct health probe so
  selected-once OTP is preselected to relay. Require direct OTP dispatch `0`,
  relay OTP dispatch `1`, mail `1`, verify `1`, registration durable effect `1`.
- Retain only allowlisted host class, route, method, pathname class, status and
  fault result. Never retain query, body, headers, email, OTP or token.
- Production and secret-candidate builds must reject activation variables and
  scan emitted text assets for the injector sentinel. Runtime-disable alone is
  not sufficient production exclusion.
