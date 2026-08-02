# KenigEvents email roundtrip architecture

## Existing live paths

```text
Yandex Mail Trigger technical address
  -> kenigevents-email-intake
  -> private KMS-encrypted Object Storage envelope
  -> Yandex Message Queue
  -> kenigevents-email-delivery
  -> HMAC-authenticated adapter
  -> metadata-only Supabase receipt

SpaceWeb info@kenigevents.ru
  -> read-only two-minute IMAP UID collector
  -> the same private envelope/queue/delivery path
```

Stable discovery values:

- folder ID: `b1g0v4ur96gis5kot6ku`;
- folder name: `kenigevents-email-prod`;
- mail trigger: `kenigevents-email-mail-trigger`;
- intake function: `kenigevents-email-intake`;
- processing trigger: `kenigevents-email-processing-trigger`;
- IMAP timer: `kenigevents-email-imap-poll`;
- bucket name prefix: `kenigevents-email-inbound-prod-`.

Always discover the generated trigger address and current bucket name from the
control plane. Do not hard-code them in docs or source.

Canonical repository contracts:

- `docs/operations/email-delivery.md`
- `infra/yandex/email-inbound/README.md`
- `serverless/email_inbound/README.md`
- `serverless/email_inbound/collector/index.py`
- `docs/testing/external-focus-email-otp.md`

## Why a new mailbox is usually unnecessary

Yandex Cloud automatically generates an email address for a Mail Trigger. A
message sent to that address invokes the target Function. The trigger event has
headers and a normalized message body; attachments may be stored in a private
bucket. It is therefore a valid controlled recipient for OTP and delivery
canaries without provisioning a human mailbox.

Official references:

- <https://yandex.cloud/ru/docs/functions/concepts/trigger/mail-trigger>
- <https://yandex.cloud/ru/docs/functions/operations/trigger/mail-trigger-create>

The service can take up to five minutes to activate a newly created trigger.
KenigEvents already has an active trigger, so ordinary tests must reuse it.

## Shared trigger versus CI trigger

The existing trigger writes to the same private bucket used by the retained
`info@kenigevents.ru` copy. Local operators may inspect a tightly filtered
envelope using their existing Yandex identity. GitHub Actions must not receive a
credential that can enumerate that bucket because it contains human inbound
mail.

For unattended focus OTP E2E, provision one of these boundaries:

1. **Preferred:** dedicated Mail Trigger -> dedicated short-retention bucket ->
   least-privilege reader available only to the protected `external-e2e`
   environment.
2. Dedicated Mail Trigger -> single-purpose Function -> signed one-time poll
   endpoint that returns only one six-digit code for the current run nonce.

Keep workflow concurrency at one. Correlate by a pre-request checkpoint, exact
recipient, allowlisted sender/subject, and an opaque run/attempt identifier.
Expire the object or receipt quickly and fail when zero or multiple codes match.

The fixed generated recipient belongs in the Auth hook's fixed-test allowlist so
repeated messages reuse the same NotiSend recipient admission for the current
billing period.

## Evidence boundaries

Allowed in durable evidence:

- provider/message ID hash;
- delivery latency;
- message count;
- OTP length;
- trigger/function/queue status;
- redacted stable error code.

Forbidden:

- raw or normalized body;
- OTP value;
- sender/recipient address from human mail;
- auth/session/provider secrets;
- shared bucket object keys when they reveal operational identifiers.

Supabase receipts are metadata-only by design. Reading the message body requires
the private envelope or IMAP; do not weaken the Supabase receipt boundary.
