# Postbox feedback correlation and DLQ recovery

> Status: implementation prepared; production migration, deploy and bounded replay remain gated.
>
> Incident: `INC-2026-08-04-postbox-feedback-dlq-correlation`.

## Purpose

This runbook closes the split between Postbox outbound acceptance and Postbox feedback processing. It covers both transactional rows sent by the Fly outbox worker and focus-group Auth mail sent directly by the Supabase Send Email Hook.

The queue `kenigevents-postbox-events-dlq` must never be purged merely because the monitor reports a stable non-zero count. A DLQ message may contain the only retained authenticated `Delivery`, `Bounce`, `Complaint` or `Rendering Failure` evidence for a provider receipt.

## Confirmed failure

The direct focus Auth hook persists an accepted Postbox `MessageId` in `personalization.focus_auth_delivery_attempt`. The feedback RPC deployed before this repair correlates only against `email_control.email_outbox`. An Auth `Send` or `Delivery` event therefore returns `correlation_pending`; the YDS trigger retries the invocation and eventually places the event in YMQ DLQ.

This is a feedback/correlation failure, not proof that 162 outbound messages failed to send. It does block authoritative delivery projection and can delay suppression from hard bounces or complaints.

## Corrected data contract

Migration `20260804190000_postbox_auth_feedback_correlation_v1.sql` introduces one PII-free receipt registry:

```text
email_control.postbox_message_correlation
```

Each Postbox `provider_message_id` belongs to exactly one source:

- `transactional_outbox` — a row in `email_control.email_outbox`; its HMAC is copied from the verified DB-owned identity;
- `focus_auth` — a row in `personalization.focus_auth_delivery_attempt`; its HMAC is initially unbound and is bound by the first authenticated Postbox YDS event;
- `legacy_auth` — a pre-ledger receipt registered only from an independently reviewed sanitized evidence manifest.

The registry stores no plaintext email, OTP, token, browser identifier, IP address, subject or body.

Unknown provider receipts remain `correlation_pending`. An incoming event is never allowed to create its own legacy correlation record.

### Registration boundaries

Database triggers register new receipts at the point where the existing ledgers persist the provider `MessageId`:

- after an outbox row receives a Postbox receipt;
- after an Auth attempt becomes `provider=postbox`, `provider_outcome=accepted` with a receipt.

A unique provider-message constraint fails closed on duplicate or cross-source receipts.

### Feedback application

`email_record_postbox_event_v3`:

1. validates the provider event envelope already normalized by the IAM-protected YDS consumer;
2. requires a pre-existing receipt correlation;
3. binds an unbound Auth/legacy HMAC once, or requires an exact versioned-HMAC match thereafter;
4. inserts an idempotent authenticated `provider_event`;
5. updates the outbox or Auth feedback projection;
6. applies `hard_bounce`, `complaint` and transactional `unsubscribe` suppression;
7. returns `applied`, `duplicate` or `correlation_pending`.

The old RPC name `email_record_postbox_event_v2` is retained as a compatibility wrapper over v3. Apply the migration first: the currently deployed consumer may continue calling v2 and immediately receives the unified semantics without a Function redeploy. A future consumer may call v3 explicitly only after the migration exists; deploying such a consumer before the migration is not safe.

## Auth feedback state

Postbox Auth attempts gain a delivery projection separate from provider API acceptance:

- `submitted`;
- `accepted`;
- `delivery_delay`;
- `delivered`;
- `terminal_failed`.

`Open`, `Click` and `Subscription` remain provider events but do not downgrade delivery state. Terminal failure is not overwritten by a later delivery event. Exact duplicate `eventId` values remain no-ops.

## Suppression boundary

The migration records authenticated `hard_bounce`, `complaint` and transactional `unsubscribe` suppressions by exact versioned recipient HMAC. This proves storage of provider suppression evidence, but it does not by itself prove that the direct focus Auth hook rejects a future send before provider network I/O.

Incident closure therefore requires one of two explicit outcomes:

- implement and test a PII-free, versioned recipient-HMAC admission boundary for direct Auth, including first-send, repeat-send and legitimate email-change cases; or
- retain direct Auth suppression enforcement as a named blocker and keep the incident open.

Never substitute user-ID history for email-identity suppression: account deletion, email change and profile switching make that unsafe.

## Health and notifications

`email_postbox_health_v1` becomes a compatibility wrapper over the unified v2 health projection. Existing counters now include both outbox and Auth sends. Additional counters expose:

- pending Auth feedback;
- Auth delivery and terminal failures in the last 24 hours;
- total and unbound correlations;
- registered legacy correlations;
- accepted sends missing a correlation row.

The Fly monitor persists a PII-free snapshot at:

```text
/data/email-postbox-monitor-state.json
```

A static DLQ backlog is notified immediately once, then only after the long reminder interval (default six hours). Growth, shrinkage or a changed alarm set is reported immediately. Clearing all alarms emits a recovery message. The notification includes `dlq delta`, Auth pending count and missing-correlation count.

## Production sequence

### 1. Freeze destructive queue actions

Until the sequence below passes:

- do not purge the DLQ;
- do not bulk-delete messages;
- do not start an unbounded replay;
- do not register legacy receipts from the DLQ event alone;
- do not disable complaint/bounce processing to make the alert disappear.

### 2. Establish immutable evidence

Record:

- exact repository SHA;
- current migration-history snapshot;
- Supabase logical backup;
- Postbox destination and trigger status;
- DLQ visible/in-flight count;
- sanitized distribution by event type, provider event time and stable error code;
- SHA-256 of the sanitized legacy-correlation manifest.

No artifact may contain plaintext recipient, message body, OTP, provider token or raw Supabase secret.

### 3. Validate and apply the migration

Use the personalization Supabase session pooler, not the transaction pooler.

Before commit, run the rollback-only contracts including:

```text
supabase/tests/email_postbox_auth_feedback_contract.sql
```

Required pre-commit results:

- all accepted Postbox outbox/Auth rows have exactly one correlation;
- `postbox_missing_correlation_count=0` for post-ledger sends;
- no provider-message collision;
- browser roles cannot execute v3 or legacy-registration RPCs;
- global/transactional/recommendation outbound switches remain in their reviewed state.

### 4. Deploy the Fly monitor change

Deploy the exact reviewed SHA. Verify:

- scheduler job is present and healthy;
- state file is created with mode `0600` where supported;
- unchanged backlog does not page every 15–20 minutes;
- a synthetic count change produces an immediate delta notification;
- recovery is emitted once when the test alarm clears.

Do not alter the Postbox destination or queue retention as part of this deploy.

### 5. Classify the retained backlog

Build a read-only inventory before replay:

- total unique YMQ messages;
- unique Postbox `eventId` and `messageId` counts;
- event-type histogram;
- oldest/newest provider event time;
- messages correlating to `transactional_outbox`;
- messages correlating to post-ledger `focus_auth`;
- messages requiring `legacy_auth` evidence;
- malformed, conflicting or unsupported messages.

The sum of all classifications must equal the immutable DLQ inventory. Approximate queue attributes alone are not sufficient.

### 6. Register legacy receipts

For each independently proven pre-ledger Hosted Auth receipt, call:

```text
email_register_legacy_postbox_auth_v1(message_id, manifest_sha256, sent_at)
```

The exact same registration is idempotent. A different source, evidence hash or send time for the same `MessageId` fails closed.

### 7. Replay in bounded batches

Replay no more than ten messages per batch initially. For every message:

- `applied` — delete that exact YMQ receipt only after the RPC transaction commits;
- `duplicate` — delete that exact YMQ receipt after proving the stored event is identical;
- `correlation_pending` — leave the message in DLQ and classify the missing evidence;
- validation/conflict/error — leave the message in DLQ, stop the batch and preserve sanitized evidence.

After each batch, verify queue delta, provider-event delta, suppression delta and correlation counts. Increase batch size only after two clean batches.

## Acceptance gates

The incident is not closed until all are true:

- new first-time Auth Postbox canary produces authenticated `Send` and `Delivery` without DLQ growth;
- ordinary transactional outbox canary still reaches `delivered`;
- exact event replay returns `duplicate` with no second state transition;
- controlled hard-bounce/complaint fixtures apply the expected suppression and are then removed according to the existing fixture procedure;
- direct Auth suppression is either proven before provider network I/O for the exact email identity or retained as an explicit blocker with the incident open;
- `postbox_missing_correlation_count=0`;
- no unexplained unbound correlation remains;
- retained DLQ is zero, or every remaining message has an explicit owner/evidence blocker recorded before retention expiry;
- monitor sends no static 15–20 minute alert storm and does send one recovery notification;
- production SHA, migration versions, Function version, Fly release and evidence hashes are recorded in the incident document.

## Rollback

The migration is additive but must still be backed by a verified logical backup. If v3 produces an unexpected conflict:

1. stop replay;
2. preserve the exact message and sanitized error evidence;
3. keep the Postbox destination enabled unless provider feedback itself is unsafe;
4. keep the compatibility v2 wrapper in place;
5. roll back the Fly monitor release independently if needed;
6. do not drop the correlation table while any accepted receipt or provider event refers to it.

A rollback must never convert an ambiguous provider outcome into a retry through another provider.
