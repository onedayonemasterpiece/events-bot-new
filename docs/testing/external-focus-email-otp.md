# External focus-group email OTP E2E

This is the black-box acceptance test for the real focus-group email journey.
The shared journey supports headless Chromium, Chrome Android in an Android
Emulator through Appium UiAutomator2, and Mobile Safari in an iOS Simulator
through Appium XCUITest. It opens the published onboarding page, requests an
ordinary real Supabase Auth OTP through the product UI, receives the delivered
message through a controlled adapter and types the code digit by digit without
Enter.

The test never receives a service-role key, provider key, fixed OTP or Auth
bypass. Local MIME fixtures verify only the parser and never count as delivery
evidence.

## Strategic status and mobile boundary

The adapters and protected jobs are implemented in the existing harness. Their
registry status may be changed from planned only after each protected job has a
terminal emulator/simulator artifact; configuration tests or a workflow file do
not count as mobile acceptance.

Canonical strategy and implementation handoff:

- [`../operations/static-site-autotest-strategy.md`](../operations/static-site-autotest-strategy.md);
- [`../features/static-site-pages/release-autotest-gates.md`](../features/static-site-pages/release-autotest-gates.md);
- [`static-site-autotest-scenarios.v1.yml`](static-site-autotest-scenarios.v1.yml);
- [`static-site-autotest-codex-prompt.md`](static-site-autotest-codex-prompt.md).

The first mobile scenario is `focus.otp.browser_tab`:

- Android Emulator + real Chrome Android + Appium UiAutomator2;
- iOS Simulator + real Mobile Safari + Appium XCUITest;
- real system email and OTP keyboards;
- the same exact-SHA, one-issue, one-verify, one-registration and returning-state
  assertions as the current Chromium path;
- the same restricted, redaction-gated evidence policy.

A desktop mobile viewport or Playwright WebKit does not close these mobile
gates. A workflow skeleton without a terminal emulator/simulator run also does
not count as acceptance.

### Live acceptance receipts (2026-08-02)

The immutable preview under test records repository SHA
`4a19fbe0b243d8a9a4652ff0c1e4fee9e895cf9c`.

- Chromium: [run 30745526613](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30745526613),
  real delivery, `issue=1`, `verify=1`, `registration=1`, returning state and
  redaction gate PASS.
- Canonical control issue browser smoke:
  [run 30755922643](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30755922643),
  launched by `/qa run` in
  [issue #253](https://github.com/onedayonemasterpiece/events-bot-new/issues/253),
  crossed Environment approval and independently repeated the same one-message,
  `1/1/1`, registration `200`, returning-state and redaction PASS assertions.
- Android 15 / Pixel 7 / real Chrome / UiAutomator2:
  [run 30747598046](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30747598046),
  email and numeric system-keyboard acceptance plus the same one-send and
  returning-state assertions PASS.
- iOS 18.5 / iPhone 16 / Mobile Safari / XCUITest:
  [run 30754894934](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30754894934)
  is the terminal **non-acceptance** receipt. Its screenshot visibly contains Safari's first-run «Выбор поисковой системы» dialog over the product page, so the corrected classification is `BLOCKED_SAFARI_FIRST_RUN_UI`, with `issue=0`, `verify=0`, `registration=0` and redaction PASS. It contains no valid email/OTP keyboard verdict and must never be presented as a product or keyboard failure.

The Ubuntu Android job explicitly enables and verifies `/dev/kvm` before
booting API 35. Unaccelerated x86 emulation is a blocked infrastructure result,
not a substitute for the Android acceptance run.

`focus.otp.installed_pwa` is a separate later scenario for Chrome install UI /
Safari Share Sheet, Launcher/SpringBoard, standalone and relaunch. It must not be
folded into the first browser-tab mobile PR.

The stable recipient is not a human mailbox. The protected workflow uses the
dedicated no-persistence Yandex Mail Trigger → API Gateway WebSocket boundary in
[`../../infra/yandex/focus-otp-e2e/README.md`](../../infra/yandex/focus-otp-e2e/README.md).
The runner connects outbound before OTP issuance and retains only the selected
code in memory. It has no access to the shared private inbound bucket. The IMAP
adapter remains supported for an independently controlled mailbox.

## Test identity policy

The default is one fixed mailbox, for example
`focus-e2e@kenigevents.ru`. Reusing it proves returning sign-in and idempotent
membership without growing `auth.users` on every run. Keep a small fixed set only
when separate stable personalization personas are genuinely needed.

Every fixed identity must be a returning Auth identity or be present in the
deployed Auth hook's `FOCUS_AUTH_NOTISEND_EMAILS` allowlist. The dedicated Mail
Trigger identity is pre-created once and tagged for E2E. While the Send Email
Hook is disabled, the journey verifies the project's currently configured
custom SMTP/Postbox path. If the hook is enabled, the same identity follows the
NotiSend route and later runs reuse the same recipient admission instead of
spending another one of the 200 unique-recipient slots **within the current
billing period**. In a new period its first send occupies one slot again. The aggregate
database report combines the latest real provider counter with admissions since
that reconciliation. `{run_id}` mode consumes a new admission and is therefore
reserved for deliberate fresh-user tests.

`E2E_RECIPIENT_TEMPLATE` without `{run_id}` is reported as
`returning_test_identity`. A template containing `{run_id}` is an explicit,
operator-approved fresh-user test and is reported as `fresh_unique_identity`.
Do not use unique mode for routine CI. Removing its disposable Auth user does
not release the unique-recipient slot already consumed at NotiSend; the private
admission row deliberately remains for accurate capacity accounting.

When the Send Email Hook is enabled, the protected E2E must not run while
`notisend_capacity.routing_ready=false`. First reconcile the provider dashboard's
actual used-recipient count and current period end through the service-only
procedure in `infra/yandex/focus-auth-email-hook/README.md`. Never assume that an
existing provider account has zero used recipients. This capacity gate does not
apply while Auth is explicitly using its existing custom SMTP/Postbox path.

While one fixed mailbox is shared, browser, Android and iOS real-mail variants
must run sequentially or be explicitly selected one at a time. Parallel mobile
runs are allowed only after separate stable identities/mailboxes are assigned.

## GitHub Environment

The protected Environment `external-e2e` uses:

- secrets `E2E_YANDEX_MAIL_WS_URL` and `E2E_RECIPIENT_TEMPLATE` — WSS domain,
  unguessable path and generated Mail Trigger recipient;
- variable `E2E_MAIL_ADAPTER=yandex-websocket`;
- sender/subject/timeout and network host-class variables listed below.

The environment may instead use controlled IMAP by setting
`E2E_MAIL_ADAPTER=imap`. For IMAP:

- require a reviewer;
- allow deployment from the default branch only;
- secrets: `E2E_IMAP_USERNAME`, `E2E_IMAP_PASSWORD`;
- variables:
  - `E2E_IMAP_HOST=imap.spaceweb.ru`
  - `E2E_IMAP_PORT=993`
  - `E2E_IMAP_SECURE=true`
  - secret `E2E_RECIPIENT_TEMPLATE=focus-e2e@kenigevents.ru`
  - `E2E_EXPECTED_FROM_PATTERN` — escaped trusted sender/domain pattern
  - `E2E_EXPECTED_SUBJECT_PATTERN` — anchored alternatives for the current SMTP
    subject and the staged Send Email Hook subject; update this contract whenever
    either provider template changes
  - `E2E_MAIL_TIMEOUT_MS=120000`
  - `E2E_SUPABASE_HOST` — direct host, used only for PII-free route labels
  - `E2E_RELAY_HOST` — relay host, used only for PII-free route labels

The mailbox password must be dedicated and must not be the hosting-panel
password. The adapter opens `INBOX` read-only, uses `BODY.PEEK` through ImapFlow,
starts from the pre-request `UIDNEXT` checkpoint and never retains raw mail.

## Run

Open **Actions → External focus email OTP → Run workflow** on the trusted default
branch. Supply:

1. the exact published onboarding URL on `https://kenigevents.ru`;
2. the full 40-character SHA recorded by that deployment's
   `preview-build.json`;
3. `platform=browser|android|ios|all`.

Only one run can execute at a time. There is no automatic resend. Missing
configuration produces a downloadable `BLOCKED_INFRASTRUCTURE` evidence bundle
instead of a false test success.

The platform selection is:

- `browser`;
- `android`;
- `ios`;
- `all` with sequential real-mail execution.

For iOS acceptance, post the following command to control issue `#253` three
times sequentially and require terminal PASS for each before using the full
scenario command once:

```text
/qa run scenario=focus.otp.ios_keyboard_preflight platform=ios target_url=<exact-preview-invite-url> expected_repo_sha=<40-hex> mode=blocking
```

Android/iOS verify native keyboard presence, active input and usable
viewport geometry before ordinary user input. Direct JS assignment of the email
or OTP value does not satisfy the mobile scenario.

On iOS Appium owns simulator boot/shutdown for the exact shutdown UDID. The harness sets `connectHardwareKeyboard=false` and `forceSimulatorSoftwareKeyboardPresence=true`; it does not mutate global Simulator defaults, open Simulator itself, or send menu/`Cmd-K` rescue gestures. A control email and control numeric field distinguish simulator keyboard failure (`BLOCKED_IOS_SIMULATOR_KEYBOARD`) from a product-field failure. Evidence records empty-field screenshots, baseline/focused `visualViewport` geometry and bounded activation attempts.
Safari native-modal inspection uses XCTest's native exact predicate for the
title and WDA's current-alert API for the text/buttons. Exactly one full
alert-text line must equal `Выбор поисковой системы` (WDA does not promise
title-first descendant ordering), and `Продолжить` must occur exactly once
among the buttons returned for that same current alert
before the exact-label accept action is allowed. XPath and WebdriverIO enhanced
`$$` collections are not used at this boundary. Protected preflight
`30763157336` exposed an enhanced-collection `object is not iterable` failure;
the first correction run `30764460049` then proved that XPath could return a
false absence while the screenshot still showed the modal. Both failed
preflights retained exact `0/0/0` side-effect counts and are not acceptance
passes.
Safari startup evidence also retains only the last inspection counts (known,
action, blocking and unknown); it never retains native alert text.
When XCTest predicate results disagree with a visible Safari alert, the same
evidence includes a fail-closed `contract_probe`: numeric result counts for
exact-visible, exact, containing and any-element title predicates; alert text
length/line/exact-title/substring counters; and exact known-button counters.
The probe never retains the alert text, button labels or native hierarchy and
never changes which dialog is actionable. It exists to choose the next single
fix from runner evidence instead of retrying locator guesses.
The iOS session starts with native Safari (`com.apple.mobilesafari`) rather
than attaching WebKit during session creation. XCUITest
`respectSystemAlerts=true` allows WDA to report SpringBoard while it owns a
system sheet. Only after the bounded handler proves the sheet absent does the
harness attach a Safari web context, with a 60-second CI discovery bound, and
navigate to the candidate. Before any identity or OTP input, one transient
native source is captured before candidate navigation or identity input, saved
as `native-ui/ios-startup.raw.xml` for the bounded clean-simulator diagnosis,
and also reduced to known title/button/container counts. No hierarchy is
captured after email/OTP entry.
The mobile sentry captures a safe screenshot immediately after Safari launch
and repeats observation after at most 20 seconds without the expected marker.
Every native action must produce a verified state transition. Two unchanged
actions stop blind retries and terminate through the classified system-UI
blocker path; a dispatched click alone is never success.
If Safari acknowledges the initial navigation command but stays on
`about:blank`, the harness records `BLOCKED_INFRASTRUCTURE` with zero side
effects and the workflow may use its one bounded Appium/WDA retry.

For selected-once issuance, Android uses simultaneous W3C touch and Return
sources. Mobile Safari uses the corresponding ordinary WebKit button click and
focused-field Return commands as one competing batch: XCUITest may acknowledge
a web-context touch action without delivering it to the page. Both paths must
still produce exactly one `/auth/v1/otp`; there is no fallback resend.
If the harness crashes before producing `.redaction-ok`, the workflow deletes
the incomplete directory and emits a static safe `BLOCKED_INFRASTRUCTURE`
receipt with side-effect counts `unknown`; it never starts the journey again.

Sanitized runtime diagnostics treat only an aborted losing `/auth/v1/health`
probe with a successful peer as expected cancellation. The exact best-effort
`focus_auth_record_verification_v1` HTTP 403 is a structured
`BEST_EFFORT_AUTH_TELEMETRY_403` warning; every other network/HTTP/console
failure remains blocking.

A heavy Android/iOS advisory run may be started without waiting only when it is
not the current release blocker. It must be reported as `STARTED_BACKGROUND`
with run ID/URL, exact SHA and target; it is never PASS until terminal evidence
exists. For an Auth/onboarding release, required mobile and OTP jobs are blocking.

## Evidence contract

Each platform artifact is named
`static-site-qa-focus-otp-<platform>-<run_id>-<attempt>`, is retained for seven
days and contains:

- `qa-summary.json` — open first; `PASS`, `FAIL` or `BLOCKED`;
- `result.json`;
- `steps.json`;
- `network.sanitized.jsonl` — method, host class, path, status, duration and
  failure class only;
- `console.sanitized.jsonl`;
- `mail-delivery.sanitized.json` — count, inbox placement, latency, code length
  and hashed message id;
- sanitized screenshots: destructive masking after values, plus explicitly
  empty keyboard controls/product fields and the safe Safari-blocker frame;
- `redaction-audit.json` and `.redaction-ok`.

- `device.json`, `scenarios.jsonl`, `junit.xml` and safe
  `native-ui/*-keyboard.json` evidence.

It never contains the address, OTP, message body, cookies, JWTs, authorization
headers, HAR, trace or video. Upload is denied unless the redaction gate passes.
Native hierarchy must also be omitted or sanitized when it can expose email,
OTP, notifications or autofill suggestions.

Simple prompt for a separate ChatGPT review:

> Проанализируй приложенный artifact external-focus-email-otp. Сначала открой
> qa-summary.json, если он есть, затем result.json, steps.json,
> network.sanitized.jsonl, mail-delivery.sanitized.json, device.json и
> redaction-audit.json. Дай итог PASS/FAIL/BLOCKED, platform, точный проваленный
> этап и отдельно оцени: одна ли была отправка OTP, одна ли проверка кода,
> зарегистрировался ли участник, сохранилось ли состояние после перезагрузки,
> доказана ли реальная mobile keyboard acceptance и безопасен ли artifact.

## Local gates

```bash
npm --prefix site run test:external-focus-email-otp
npm --prefix site run test:focus-group-product
npm --prefix site run test:resilient-client
```

A local run without mailbox configuration is expected to end `BLOCKED`, not
`PASS`. A real live result requires Environment approval and mailbox delivery.
Android/iOS configuration tests and mocked adapters also remain component
evidence until a protected emulator/simulator run reaches a terminal result.
