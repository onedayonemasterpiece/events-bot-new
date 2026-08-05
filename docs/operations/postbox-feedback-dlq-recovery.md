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

Migration `20260804190000_postbox_auth_feedback_correlation_v1.sql` introduces one
receipt registry containing pseudonymous identifiers but no plaintext recipient:

```text
email_control.postbox_message_correlation
```

Each Postbox `provider_message_id` belongs to exactly one source:

- `transactional_outbox` — a row in `email_control.email_outbox`; its HMAC is copied from the verified DB-owned identity;
- `focus_auth` — a row in `personalization.focus_auth_delivery_attempt`; new
  hook admissions persist the exact HMAC/version and create an already-bound
  correlation, while only pre-migration attempts remain one-time-bindable;
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

The migration records authenticated `hard_bounce`, `complaint` and transactional
`unsubscribe` suppressions by exact versioned recipient HMAC. The reviewed hook
computes that pseudonymous proof inside the secret boundary and submits the
complete one- or two-recipient batch only after local rendering. The admission
RPC and suppression insertion take the same per-HMAC advisory locks; transaction
commit order therefore defines whether suppression or the network claim wins.
Admission persists the proof and `network_claimed_at` before returning, and the
next operation is provider I/O.

Secure Supabase `email_change` is one atomic two-recipient admission: current
address uses `token` with the reversed `token_hash_new`, and new address uses
`token_new` with `token_hash`. Insecure email change admits one new-address
delivery. Any exact active suppression blocks the complete batch without a
provider call. Unrelated historical user-ID suppression is never inferred.

HMAC key rotation is forbidden while active suppressions, correlations or
retained feedback use the current version unless a separately reviewed overlap
migration provides both keys/versions. A mismatched version fails closed; do not
change `EMAIL_ADDRESS_HMAC_KEY_VERSION` independently in either Function.

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

The current production Supabase Send Email Hook is intentionally disabled.
Reverify that fact and the absence of concurrent direct-hook traffic immediately
before apply; historic accepted rows are expected and are backfilled. Under that
required precondition,
`20260804190000_postbox_auth_feedback_correlation_v1.sql` atomically installs the
batch admission/completion RPCs **and revokes** the suppression-free v1 admission
from `service_role`; there is no ordinary migration state that leaves the bypass
callable. If the Hook is unexpectedly enabled or traffic exists, stop rather
than applying.

After migration/SQL-contract verification, deploy and smoke the reviewed focus
Auth Function that calls `focus_auth_begin_delivery_batch_v1` and
`focus_auth_complete_delivery_batch_v1`. Do not enable the Supabase Hook as part
of this incident without its separate product-release authorization. Re-granting
v1 is emergency rollback only and must be paired with the previous Function
version while the Hook remains disabled.

The modern `sb_secret_*` Supabase key is sent only as `apikey`; it is opaque and
must not be placed in a Bearer header. Legacy service-role JWTs retain the
Bearer header for compatibility.

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

Use the bounded operator tool from a private environment containing the existing
YMQ and Supabase secret variables. It drains only visibility for the snapshot,
deduplicates by queue/event/message identity, restores every receipt to visible
in `finally`, and writes hashes/classifications only:

```bash
python3 scripts/ops/postbox_dlq_recover.py inventory \
  --max-messages 500 \
  --output artifacts/codex/INC-2026-08-04/postbox-dlq-inventory.json
```

Inventory requires `inflight=0` at start, ten consecutive empty long polls, and
a reconciled hidden snapshot whose approximate `inflight` value equals the
unique queue receipts held by the tool. It fails rather than writing evidence
when the bound is exhausted or any visibility restore entry fails. YMQ documents
both queue-depth attributes as approximate; production may continue reporting a
received message as both visible and in-flight for the complete visibility
window. The manifest therefore preserves the raw approximate snapshot and an
explicit `hidden_reconciliation`: exact visible exhaustion is established by
ten empty long polls while the independent in-flight counter must match the
unique receipt inventory. Inspect the resulting `envelope_schema` classification
before replay; the tool supports only a raw Postbox event or the exact
`{"messages":[...]}` consumer envelope. It never writes the raw envelope.

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

The destructive mode has an incident-specific confirmation and stops at the
first retained item. It restores that receipt and all unprocessed receipts:

```bash
POSTBOX_DLQ_REPLAY_CONFIRM=INC-2026-08-04 \
python3 scripts/ops/postbox_dlq_recover.py replay \
  --batch-size 10 \
  --inventory artifacts/codex/INC-2026-08-04/postbox-dlq-inventory.json \
  --inventory-sha256 <reviewed-file-sha256> \
  --output artifacts/codex/INC-2026-08-04/replay-batch-001.json
```

`consumer.process_event` reconstructs the same validated Function input and v2
compatibility RPC. A queue receipt is deleted only after all records in that
queue message return `applied` or exact-field-verified `duplicate`. The exact
queue ID hash and body hash must also exist in the independently reviewed
inventory file whose full-file SHA-256 is supplied on the command line; an
unreviewed/new message stops the batch and remains visible.

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

The exact emergency admission rollback is:

```sql
grant execute on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean)
  to service_role;
notify pgrst, 'reload schema';
```

Use it only to pair an explicit previous-Function rollback. The additive registry
migration has no safe destructive rollback after receipts/events exist; restore
from the verified pre-migration logical backup only under a declared maintenance
decision, with feedback intake stopped and the DLQ preserved.
