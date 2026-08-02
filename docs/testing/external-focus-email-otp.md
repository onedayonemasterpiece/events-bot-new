# External focus-group email OTP E2E

This is the black-box acceptance test for the real focus-group email journey.
Its currently implemented platform is headless Chromium on a GitHub-hosted Linux
runner. It opens the published onboarding page, requests an ordinary real
Supabase Auth OTP through the product UI, reads the delivered message from a
controlled IMAPS mailbox and types the code digit by digit without Enter.

The test never receives a service-role key, provider key, fixed OTP or Auth
bypass. Local MIME fixtures verify only the parser and never count as delivery
evidence.

## Strategic status and mobile boundary

The current Chromium workflow is a valid browser delivery E2E, but it is **not**
Android Chrome or Mobile Safari evidence. Android Emulator and iOS Simulator
variants are planned as the first mobile-autotest milestone and must extend this
existing harness rather than create a competing OTP implementation.

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

`focus.otp.installed_pwa` is a separate later scenario for Chrome install UI /
Safari Share Sheet, Launcher/SpringBoard, standalone and relaunch. It must not be
folded into the first browser-tab mobile PR.

The stable recipient does not have to be a provisioned human mailbox. The
existing Yandex Cloud Mail Trigger proves the automated-receipt route and is
operable through `.codex/skills/kenigevents-email-roundtrip/`. For unattended
GitHub execution, use a dedicated trigger and short-retention bucket (or a
narrowly signed one-time read endpoint); never give the workflow access to the
shared private bucket that also contains the read-only `info@kenigevents.ru`
copy. The current IMAP adapter remains supported for an independently controlled
mailbox.

## Test identity policy

The default is one fixed mailbox, for example
`focus-e2e@kenigevents.ru`. Reusing it proves returning sign-in and idempotent
membership without growing `auth.users` on every run. Keep a small fixed set only
when separate stable personalization personas are genuinely needed.

Every fixed mailbox must also be present in the deployed Auth hook's
`FOCUS_AUTH_NOTISEND_EMAILS` allowlist. The first and all later CI messages then
reuse the same NotiSend recipient admission instead of spending Postbox sends or
another one of the 200 unique-recipient slots **within the current billing
period**. In a new period its first send occupies one slot again. The aggregate
database report combines the latest real provider counter with admissions since
that reconciliation. `{run_id}` mode consumes a new admission and is therefore
reserved for deliberate fresh-user tests.

`E2E_RECIPIENT_TEMPLATE` without `{run_id}` is reported as
`returning_test_identity`. A template containing `{run_id}` is an explicit,
operator-approved fresh-user test and is reported as `fresh_unique_identity`.
Do not use unique mode for routine CI. Removing its disposable Auth user does
not release the unique-recipient slot already consumed at NotiSend; the private
admission row deliberately remains for accurate capacity accounting.

The protected E2E must not run while `notisend_capacity.routing_ready=false`.
First reconcile the provider dashboard's actual used-recipient count and current
period end through the service-only procedure in
`infra/yandex/focus-auth-email-hook/README.md`. Never assume that an existing
provider account has zero used recipients.

While one fixed mailbox is shared, browser, Android and iOS real-mail variants
must run sequentially or be explicitly selected one at a time. Parallel mobile
runs are allowed only after separate stable identities/mailboxes are assigned.

## GitHub Environment

Create the protected Environment `external-e2e`. Configure exactly one receive
adapter: controlled IMAP, or the dedicated Yandex Mail Trigger boundary described
above. For the current IMAP adapter:

- require a reviewer;
- allow deployment from the default branch only;
- secrets: `E2E_IMAP_USERNAME`, `E2E_IMAP_PASSWORD`;
- variables:
  - `E2E_IMAP_HOST=imap.spaceweb.ru`
  - `E2E_IMAP_PORT=993`
  - `E2E_IMAP_SECURE=true`
  - `E2E_RECIPIENT_TEMPLATE=focus-e2e@kenigevents.ru`
  - `E2E_EXPECTED_FROM_PATTERN` — escaped trusted sender/domain pattern
  - `E2E_EXPECTED_SUBJECT_PATTERN` — stable OTP subject fragment
  - `E2E_MAIL_TIMEOUT_MS=120000`
  - `E2E_SUPABASE_HOST` — direct host, used only for PII-free route labels
  - `E2E_RELAY_HOST` — relay host, used only for PII-free route labels

The mailbox password must be dedicated and must not be the hosting-panel
password. The adapter opens `INBOX` read-only, uses `BODY.PEEK` through ImapFlow,
starts from the pre-request `UIDNEXT` checkpoint and never retains raw mail.

## Run: currently implemented browser platform

Open **Actions → External focus email OTP → Run workflow** on the trusted default
branch. Supply:

1. the exact published onboarding URL on `https://kenigevents.ru`;
2. the full 40-character SHA recorded by that deployment's
   `preview-build.json`.

Only one run can execute at a time. There is no automatic resend. Missing
configuration produces a downloadable `BLOCKED_INFRASTRUCTURE` evidence bundle
instead of a false test success.

Until the mobile implementation PR is merged, the workflow does not provide a
real `android` or `ios` platform option. Do not describe the current Chromium
390×844 viewport as either platform.

## Future mobile run contract

The implementation must add an explicit `platform` selection:

- `browser`;
- `android`;
- `ios`;
- `all` with sequential real-mail execution.

Android/iOS must verify native keyboard presence, active input and usable
viewport geometry before ordinary user input. Direct JS assignment of the email
or OTP value does not satisfy the mobile scenario.

A heavy Android/iOS advisory run may be started without waiting only when it is
not the current release blocker. It must be reported as `STARTED_BACKGROUND`
with run ID/URL, exact SHA and target; it is never PASS until terminal evidence
exists. For an Auth/onboarding release, required mobile and OTP jobs are blocking.

## Evidence contract

The uploaded artifact is retained for seven days and contains:

- `result.json` — open first; `PASS`, `FAIL` or `BLOCKED`;
- `steps.json`;
- `network.sanitized.jsonl` — method, host class, path, status, duration and
  failure class only;
- `console.sanitized.jsonl`;
- `mail-delivery.sanitized.json` — count, inbox placement, latency, code length
  and hashed message id;
- masked screenshots;
- `redaction-audit.json` and `.redaction-ok`.

The mobile implementation additionally adds `qa-summary.json`, `device.json`,
`scenarios.jsonl`, `junit.xml` and safe native-keyboard/viewport evidence.

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
