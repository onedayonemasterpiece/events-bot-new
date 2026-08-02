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

- Pin macOS runner, Xcode, iPhone Simulator and iOS runtime.
- Download the XCUITest driver's matching official prebuilt WDA and pass both
  `usePreinstalledWDA` and `prebuiltWDAPath`. Treat WDA startup failure as
  infrastructure failure before OTP issuance.
- Disable the hardware keyboard and require the simulator software keyboard.
- Treat Simulator's software-keyboard visibility toggle as distinct from its
  hardware-keyboard connection preference. If a verified physical input tap
  still reports no native keyboard on a hosted macOS runner, activate Simulator
  and click the exact `I/O → Keyboard → Toggle Software Keyboard` menu item once,
  then retap and recheck; a best-effort shortcut can be delivered to the wrong
  process, so never assume it succeeded.
- Poll keyboard presence through a bounded animation window after the physical
  tap. A single early `false` can race the keyboard animation; immediately
  toggling at that point may turn off the keyboard that was still appearing.
- If an exact menu click plus physical retap still leaves no keyboard after the
  second bounded check, the equivalent shortcut may be sent only after making
  the Simulator process explicitly frontmost. Retap once and require native
  keyboard evidence; never infer success from the shortcut itself.
- On a fresh Simulator, detect and close only the exact allowlisted Safari
  first-run prompt before journey interaction. Do not use a generic alert
  accepter: unrelated permission/security dialogs must still fail visibly.
- If a fresh Safari session acknowledges navigation but remains at
  `about:blank`, classify it as a pre-side-effect simulator/browser startup
  block. The workflow may consume its one bounded Appium restart; do not label
  it as a product failure or add an in-journey navigation retry.
- Keep navigation clicks in the Safari web context.
- Before a keyboard-critical tap, scroll the HTML input to the center, switch
  to `NATIVE_APP`, locate exactly one visible `XCUIElementTypeTextField` by the
  input's accessible label using `-ios predicate string`, and synthesize
  a screen-level `mobile: tap` at the absolute center of the rect returned by
  XCTest itself. Then restore the web context. This is not a guessed
  WebView-to-screen transform and must not be derived from DOM coordinates.
- Do not use a synthetic WebKit click as keyboard proof. Do not use global
  `nativeWebTap`, `nativeWebTapStrict`, coordinate translation calibration or a
  guessed screen offset. Do not persist native hierarchy or field values.
- For competing issuance gestures, prefer an ordinary WebKit element click and
  focused-field Return command started in one batch. Do not rely on XCUITest's
  web-context W3C touch source: it can acknowledge `performActions` without
  dispatching the pointer event to Safari. This batch is the sole issuance
  attempt, never a retry or fallback.
- Before destructive DOM masking, blur an active sensitive input, then allow
  Mobile Safari's native text overlay one paint frame before screenshot capture;
  a masked DOM alone does not prove the screenshot stopped displaying the prior
  field value.

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
OTP length and hashed message identity.
