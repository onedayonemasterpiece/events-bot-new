# External focus-group email OTP E2E

This is the black-box acceptance test for the real focus-group email journey.
It opens the published onboarding page in Chromium, requests an ordinary real
Supabase Auth OTP through the product UI, reads the delivered message from a
controlled IMAPS mailbox and types the code digit by digit without Enter.

The test never receives a service-role key, provider key, fixed OTP or Auth
bypass. Local MIME fixtures verify only the parser and never count as delivery
evidence.

## Test identity policy

The default is one fixed mailbox, for example
`focus-e2e@kenigevents.ru`. Reusing it proves returning sign-in and idempotent
membership without growing `auth.users` on every run. Keep a small fixed set only
when separate stable personalization personas are genuinely needed.

Every fixed mailbox must also be present in the deployed Auth hook's
`FOCUS_AUTH_NOTISEND_EMAILS` allowlist. The first and all later CI messages then
reuse the same NotiSend recipient admission instead of spending Postbox sends or
another one of the 200 unique-recipient slots. The database capacity report is
the source of truth for occupied/available slots. `{run_id}` mode consumes a new
recipient admission and is therefore reserved for deliberate fresh-user tests.

`E2E_RECIPIENT_TEMPLATE` without `{run_id}` is reported as
`returning_test_identity`. A template containing `{run_id}` is an explicit,
operator-approved fresh-user test and is reported as `fresh_unique_identity`.
Do not use unique mode for routine CI. Removing its disposable Auth user does
not release the unique-recipient slot already consumed at NotiSend; the private
admission row deliberately remains for accurate capacity accounting.

## GitHub Environment

Create the protected Environment `external-e2e`:

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

## Run

Open **Actions → External focus email OTP → Run workflow** on the trusted default
branch. Supply:

1. the exact published onboarding URL on `https://kenigevents.ru`;
2. the full 40-character SHA recorded by that deployment's
   `preview-build.json`.

Only one run can execute at a time. There is no automatic resend. Missing
configuration produces a downloadable `BLOCKED_INFRASTRUCTURE` evidence bundle
instead of a false test success.

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

It never contains the address, OTP, message body, cookies, JWTs, authorization
headers, HAR, trace or video. Upload is denied unless the redaction gate passes.

Simple prompt for a separate ChatGPT review:

> Проанализируй приложенный artifact external-focus-email-otp. Сначала открой
> result.json, затем steps.json, network.sanitized.jsonl,
> mail-delivery.sanitized.json и redaction-audit.json. Дай итог PASS/FAIL/BLOCKED,
> точный проваленный этап и отдельно оцени: одна ли была отправка OTP, одна ли
> проверка кода, зарегистрировался ли участник, сохранилось ли состояние после
> перезагрузки и безопасен ли artifact.

## Local gates

```bash
npm --prefix site run test:external-focus-email-otp
npm --prefix site run test:focus-group-product
npm --prefix site run test:resilient-client
```

A local run without mailbox configuration is expected to end `BLOCKED`, not
`PASS`. A real live result requires Environment approval and mailbox delivery.
