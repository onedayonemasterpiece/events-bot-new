---
name: kenigevents-email-roundtrip
description: Run, debug, and document controlled KenigEvents email send/receive tests using Yandex Cloud Mail Trigger, the retained SpaceWeb mailbox collector, Object Storage/YMQ evidence, or Yandex Cloud Postbox. Use for OTP delivery tests, inbound-email canaries, proving that a message reached Yandex Functions, extracting a test OTP from the private trigger envelope, checking email DLQs, or deciding whether a real mailbox is actually required.
---

# KenigEvents Email Roundtrip

Use the existing inbound and outbound infrastructure. Do not request or create a
new mailbox until the route decision below proves that a mailbox is necessary.

## Route decision

1. **Automated receipt, OTP, or provider canary:** use the existing Yandex Cloud
   Mail Trigger technical address. It invokes `kenigevents-email-intake` and
   stores the normalized body in private Object Storage. No mailbox is required.
2. **Human correspondence or stable reply address:** use the retained SpaceWeb
   `info@kenigevents.ru` mailbox. Its Yandex timer collector reads new UIDs with
   `BODY.PEEK[]` and does not mark mail read.
3. **Controlled outbound canary:** use `$kgd80-postbox-mailer` from
   `info@kgd80.ru` after explicit send consent. Do not rewrite Postbox sending.
4. **GitHub Actions OTP E2E:** use a dedicated Mail Trigger/bucket or a narrowly
   signed one-time read boundary. Never give CI read access to the shared
   production inbound bucket.
5. **Focus onboarding OTP:** request the message only through the deployed
   product UI, then receive it through the protected no-persistence Mail Trigger
   WebSocket adapter. Do not send a separate canary in the same run.

Read [references/architecture.md](references/architecture.md) before changing
resources, IAM, trigger destinations, retention, or the external OTP harness.

## Safe read-only commands

The helper discovers live resources by stable folder/resource names and uses
the existing local `yc` profile. It never sends mail or mutates infrastructure.

```bash
python3 .codex/skills/kenigevents-email-roundtrip/scripts/yandex_mail_trigger.py status
```

Reveal the generated technical recipient only when it is needed for a canary or
an environment variable:

```bash
python3 .codex/skills/kenigevents-email-roundtrip/scripts/yandex_mail_trigger.py \
  address --reveal-address
```

Create a checkpoint immediately before requesting an OTP:

```bash
python3 .codex/skills/kenigevents-email-roundtrip/scripts/yandex_mail_trigger.py checkpoint
```

Wait for a single matching OTP envelope. Supply tight sender and subject
patterns. `--emit-otp` is deliberately explicit; never paste its JSON into an
artifact or chat transcript.

```bash
python3 .codex/skills/kenigevents-email-roundtrip/scripts/yandex_mail_trigger.py \
  wait-otp \
  --since 2026-08-02T10:00:00Z \
  --from-pattern 'kenigevents|supabase' \
  --subject-pattern 'код|code' \
  --emit-otp
```

In GitHub Actions add the mask before any later output:

```text
::add-mask::<otp>
```

Prefer passing the OTP directly to the browser process rather than writing it
to disk. Evidence may retain only code length, latency, count, and hashed IDs.

For the protected focus test, reuse
`site/e2e/focus-email/adapters/yandex-websocket.mjs`; do not grant the workflow a
Yandex Cloud key or reimplement bucket polling. Open the WSS connection before
the UI issues OTP, require exactly one matching message after that checkpoint,
accept only the configured sender and anchored current/hook subject contract,
mask the OTP immediately, and keep it in memory until ordinary digit input.

## Send and receive a canary

1. Run `status`; require the Mail Trigger and processing trigger to be `ACTIVE`,
   the bucket to be private, and both DLQs to be empty.
2. Run `checkpoint` and record only its UTC timestamp.
3. Resolve the technical recipient with `address --reveal-address`.
4. Use `$kgd80-postbox-mailer` to dry-run the outbound message. Send only after
   current-turn user consent.
5. Use `wait-otp` for an OTP or inspect the metadata-only Supabase receipt for a
   generic canary. Do not dump the raw envelope.
6. Verify one receipt, one stored envelope, and empty intake/processing DLQs.
7. Store only redacted evidence under `artifacts/codex/<task>/`.

## Failure handling

- `trigger_not_found` or non-`ACTIVE`: inspect the exact folder and trigger;
  do not create a replacement by guessing.
- `mail_delivery_timeout`: check Function logs, intake DLQ, processing queue,
  processing DLQ, then provider evidence in that order.
- Multiple matching OTPs: fail the run. Do not select the newest code silently.
- No body in Supabase: expected. Supabase intentionally stores metadata only;
  the private Object Storage envelope owns the normalized body.
- Need raw MIME: Mail Trigger does not provide it. Use controlled IMAP with
  `BODY.PEEK[]`; do not pretend the trigger envelope is raw MIME.
- Sender/subject mismatch: retain only stage counters and a stable error class;
  update the anchored template contract only after comparing it with the actual
  configured provider template.
- WebSocket connected but no match: inspect Mail Trigger/Function status and
  safe stage counters before attempting another product send.

## Safety

- Never print mailbox passwords, IAM/OAuth tokens, static keys, raw messages,
  addresses from human mail, OTPs, cookies, or JWTs.
- Do not mark messages read, move them, delete them, or change SpaceWeb mailbox
  mode for a test.
- Do not grant GitHub access to the shared inbound bucket.
- Do not use a fixed public OTP or service-role Auth bypass as delivery evidence.
- Do not count local fixtures or a metadata receipt as real mailbox delivery.
