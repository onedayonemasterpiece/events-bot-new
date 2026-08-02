# Appium system-UI contracts

## Tool selection

- Use Appium 3 + XCUITest for deterministic iOS native/hybrid/mobile-Safari
  release evidence.
- Use Appium 3 + UiAutomator2 for Android native/hybrid/mobile-Chrome evidence.
- Use Playwright for DOM/runtime/visual checks only after native UI is clear.
- Maestro can express a short setup smoke flow, but does not replace the
  project's evidence/redaction and one-side-effect contracts.
- Appium Inspector or a device MCP is useful for isolated diagnosis, not as the
  unattended release verdict.

## XCUITest locator rules

Official XCUITest driver guidance ranks native predicate string and class chain
ahead of XPath. XPath builds an XML accessibility snapshot and can be much
slower or behave differently from native XCTest lookup:

- <https://appium.github.io/appium-xcuitest-driver/latest/reference/locator-strategies/>
- <https://appium.github.io/appium-xcuitest-driver/latest/reference/ios-predicate/>
- <https://appium.github.io/appium-xcuitest-driver/latest/troubleshooting/wda-slowness/>

Use `-ios predicate string` for exact own-attribute matches. Use class chain
only when hierarchy is required. Do not use broad XPath as a first probe for a
visible system alert.

## Current-alert exact action

The documented `mobile: alert` command accepts `getButtons`, `accept`, or
`dismiss`, with an optional exact `buttonLabel`:

<https://appium.github.io/appium-xcuitest-driver/latest/reference/execute-methods/#mobile-alert>

The installed XCUITest/WDA source is also authoritative for the pinned version:

- `site/node_modules/appium-xcuitest-driver/lib/commands/alert.ts`;
- `site/node_modules/appium-xcuitest-driver/node_modules/appium-webdriveragent/WebDriverAgentLib/FBAlert.m`.

WDA's `/alert/text` enumerates alert `StaticText` descendants and joins them
with newlines. It does not define title-first order. Therefore require one
exact full title line anywhere in the current alert text, not a fuzzy substring
and not position zero. `buttonLabels` enumerates buttons within the same current
alert; require exactly one allowed label before exact-label acceptance.

## KenigEvents bounded handler

For the Russian Safari first-run dialog:

1. create a native-first Safari session with `com.apple.mobilesafari`, set
   `respectSystemAlerts=true`, and stay in `NATIVE_APP` until system UI is
   clear; do not make WebKit attachment part of initial session creation;
2. query exactly one visible StaticText named/labelled
   `Выбор поисковой системы` through XCTest predicate;
3. call current-alert text and button APIs;
4. require exactly one full title line and exactly one `Продолжить` button;
5. accept with `buttonLabel: 'Продолжить'`;
6. poll native inspection until three consecutive obstruction-free samples;
7. capture a safe post-transition screenshot before product input.

For a clean, side-effect-free simulator preflight only, a native source may be
captured before candidate navigation or identity input, retained as the
short-lived diagnostic artifact `native-ui/ios-startup.raw.xml`, and reduced
to allowlisted application/alert/sheet and known title/button type counts.
Never capture hierarchy after identity/OTP entry. Attach the Safari web context
only after the native blocker is proven absent.

Unknown title, duplicate title/action, missing action, alert API disagreement or
failure to disappear is `BLOCKED_SAFARI_FIRST_RUN_UI`. Never type through it.

## Privacy adaptation

Generic mobile-testing playbooks often recommend saving page source, hierarchy,
video and raw logs on every failure. Do not copy that advice into sensitive
KenigEvents flows. Email, OTP, notifications and autofill suggestions may enter
those artifacts. Use screenshots only while fields are empty/masked and keep
sanitized counts/fingerprints instead of raw hierarchy.
