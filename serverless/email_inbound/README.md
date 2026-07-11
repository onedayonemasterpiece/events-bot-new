# Inbound email serverless functions

This package implements only the Yandex Cloud ingress boundary:

```text
Mail Trigger → private Object Storage → intake → YMQ → delivery → signed adapter
```

It is intentionally independent from the Fly application. Importing this package has
no scheduler, database or network side effects.

## Runtime contracts

- Runtime: supported Yandex Cloud Functions `python312`.
- Mail Trigger batch size and YMQ trigger batch size: `1` in production. The code
  accepts up to ten entries so a retry of a small unexpected batch remains safe.
- The Mail Trigger body is capped at `220000` bytes, below the current 230 KB
  trigger-message limit after service metadata.
- The normalized envelope is stored as deterministic JSON. It is **not raw MIME**:
  Yandex Mail Trigger does not expose raw MIME.
- The YMQ body is a metadata/reference pointer capped at 32 KiB; it contains no
  plaintext sender, recipient, subject or body.
- Both triggers are at-least-once. `inbound_id` is a keyed HMAC over a canonical
  envelope identity and is the downstream idempotency key.

## Intake environment

All values are supplied to the Function version; secret values come from Lockbox.

| Name | Secret | Purpose |
|---|---:|---|
| `EMAIL_INBOUND_BUCKET` | no | Dedicated private bucket name. |
| `EMAIL_INBOUND_QUEUE_URL` | no | Processing queue URL. |
| `EMAIL_INBOUND_MAILBOX` | no | Must be `info@kenigevents.ru` in production. |
| `EMAIL_INBOUND_AWS_ACCESS_KEY_ID` | yes | Static key ID for the least-privilege intake SA. |
| `EMAIL_INBOUND_AWS_SECRET_ACCESS_KEY` | yes | Static key secret from Lockbox. |
| `EMAIL_INBOUND_IDEMPOTENCY_SECRET` | yes | At least 32 bytes, independent of AWS/signing secrets. |
| `EMAIL_INBOUND_RETENTION_DAYS` | no | Defaults to `30`; the bucket lifecycle is the enforcement layer. |
| `EMAIL_INBOUND_MAX_BODY_BYTES` | no | Defaults/maxes at `220000`. |
| `EMAIL_INBOUND_S3_ENDPOINT` | no | Defaults to Yandex Object Storage. |
| `EMAIL_INBOUND_YMQ_ENDPOINT` | no | Defaults to Yandex Message Queue. |
| `EMAIL_INBOUND_AWS_REGION` | no | Defaults to `ru-central1`. |

The intake SA needs only object upload, YMQ write, key-specific KMS encrypt and
payload access to its Lockbox secrets. Status commands must never create access
keys. Key creation and rotation are separate reviewed operations.

## Delivery environment

| Name | Secret | Purpose |
|---|---:|---|
| `EMAIL_INBOUND_ADAPTER_URL` | no | HTTPS endpoint owned by the control-plane lane. |
| `EMAIL_INBOUND_ADAPTER_KEY_ID` | no | Rotation identifier such as `current-2026-07`. |
| `EMAIL_INBOUND_ADAPTER_SECRET` | yes | At least 32 bytes from Lockbox. |
| `EMAIL_INBOUND_ADAPTER_TIMEOUT_SECONDS` | no | Defaults to 10, maximum 30. |

The adapter receives `kenigevents.email_inbound.adapter.v1`. Authentication:

```text
X-Kenig-Key-Id: <key id>
X-Kenig-Timestamp: <unix seconds>
X-Kenig-Content-SHA256: <lowercase SHA-256 hex>
X-Kenig-Signature: v1.<base64url HMAC-SHA256 without padding>
```

Canonical signing input:

```text
v1
POST
<exact URL path>
<timestamp>
<content sha256>
```

The adapter must allow only HTTPS POST JSON, verify the digest/signature in
constant time, accept current/previous keys for rotation, enforce a five-minute
clock window, and atomically insert by `inbound_id`. Only these acknowledgements
delete a YMQ message:

```json
{"ok":true,"status":"accepted","inbound_id":"<same id>"}
```

```json
{"ok":true,"status":"duplicate","inbound_id":"<same id>"}
```

Every other response, including all `4xx`, is retryable from the function's point
of view and eventually moves to the processing DLQ. Incoming email content can
never enable a test/failure mode.

## Build

From the repository root:

```bash
infra/yandex/email-inbound/build-functions.sh
```

Artifacts are written under ignored `artifacts/codex/`. Each ZIP has `index.py`,
`common/` and its own `requirements.txt`, so the Cloud Functions entry point is
`index.handler`.

No function version or trigger should use `$latest` in production. Create a new
version without the `prod` tag, invoke it with fixtures, then move `prod` only
after the smoke passes. Rollback means moving `prod` back to the prior version.

## Logging

The allowlist logger accepts only correlation IDs, queue message IDs, stable error
codes and integer sizes/counts. Do not add exception text, URLs, headers, object
keys, sender/recipient addresses, subjects or bodies to logs.
