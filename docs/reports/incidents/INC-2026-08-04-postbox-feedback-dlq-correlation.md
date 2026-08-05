# INC-2026-08-04 Postbox feedback DLQ correlation backlog

Status: open  
Severity: sev2  
Service: Yandex Postbox feedback / focus Auth email  
Opened: 2026-08-04  
Closed: —  
Owners: email delivery / focus Auth / production operations  
Related incidents: `INC-2026-07-30-focus-email-otp-false-success`, `INC-2026-08-03-yandex-cloud-reaper-service-suspension`  
Runbook: `docs/operations/postbox-feedback-dlq-recovery.md`

## Summary

The production monitor repeatedly reported `postbox_dlq_nonempty` with a stable DLQ count of 162. The backlog is not evidence of 162 outbound-send failures. It contains Postbox feedback events that the deployed consumer could not correlate to direct focus Auth sends.

The direct Auth hook stores accepted Postbox `MessageId` values in `personalization.focus_auth_delivery_attempt`. The feedback RPC correlated only against `email_control.email_outbox`, returned `correlation_pending`, and allowed the YDS trigger to exhaust its bounded retries into YMQ DLQ.

## Observed notifications

On 4 August 2026 the bot reported the same count at 12:42, 13:02, 13:22, 16:22 and 16:42 local message time:

```text
codes=postbox_dlq_nonempty
dlq=162 unknown=0 submitted_oldest_s=0
```

The `unknown` and `submitted` counters covered only the transactional outbox and therefore did not describe direct Auth sends. The in-memory 15-minute alert cooldown also treated the unchanged backlog as a fresh page after every cooldown window.

## Impact

- OTP sending is not blocked merely because feedback entered DLQ.
- Delivery projection for affected Auth mail is incomplete.
- Hard-bounce, complaint and rendering-failure handling may be delayed.
- Operational messages combine an Auth-capable DLQ count with outbox-only health counters.
- Repeated unchanged alerts obscure actual queue growth and recovery.
- Retained evidence can expire if the queue is not classified and replayed before its retention boundary.

## Root cause

1. Postbox outbound acceptance and Postbox feedback used two different persistence ledgers.
2. The provider-event RPC required an `email_control.email_outbox` row for every `MessageId`.
3. Direct focus Auth mail never created that outbox row.
4. Unit tests validated each half independently but did not cover direct Auth send → provider event → applied feedback → empty DLQ.
5. Monitor state was process-local and keyed only by alarm codes, not persisted queue count/delta.

## Corrective implementation

PR #333 on branch `agent/postbox-auth-feedback-correlation-20260804` is the only
implementation lane; no competing repair branch is authorized. It adds:

- one pseudonymous receipt registry, with no plaintext recipient, shared by
  outbox and Auth;
- automatic registration when either ledger persists a Postbox receipt;
- authenticated one-time HMAC binding for Auth and audited legacy receipts;
- `email_record_postbox_event_v3` with a v2 compatibility wrapper;
- Auth feedback state and unified health counters;
- an explicit service-only legacy-registration boundary;
- persisted DLQ alert state, delta reporting, six-hour static reminders and recovery notification;
- rollback-only SQL and Python regression contracts;
- a production recovery runbook.

Review on 2026-08-05 corrected additional closure-blocking defects before
production rollout:

- direct Auth originally stored suppression evidence but did not check the exact
  recipient before network I/O; batch admission now serializes the exact
  versioned HMAC against suppression insertion, persists the proof/network claim
  and blocks the complete secure-email-change pair atomically;
- modern opaque `sb_secret_*` API keys are no longer sent as Bearer JWTs;
- secure and insecure Supabase `email_change` payloads follow the documented
  current/new token and reversed token-hash mapping;
- new Auth correlations are bound at provider-receipt persistence; only
  pre-migration/legacy receipts remain authenticated one-time-bindable;
- receipt/event sizes, event-count range, trigger-install ordering and immutable
  outbox identity now align fail-closed;
- the old suppression-free v1 Auth admission is revoked by a separate cutover
  migration only after the new Function version passes its deployment smoke.

HMAC rotation remains deliberately prohibited while retained feedback,
correlations or active suppressions use the current key/version. A future
rotation requires an overlap/keyring migration and is not part of this incident.

## Validation evidence (pre-production)

- PostgreSQL 17 isolated apply: CI bootstrap plus migrations
  `20260711203940`, `20260712072912`, `20260712083037`, `20260801222242`,
  `20260804190000` and cutover `20260805071852` applied successfully.
- Rollback-only SQL contract: `BEGIN / DO / ROLLBACK`, green after both new
  migrations. It covers Auth/outbox/legacy correlation, cross-source collision,
  exact/conflicting duplicates, 512-character receipt, 300-character event key,
  event count above 1000, suppression scope/version, atomic email change,
  immutable outbox identity, health and role privileges.
- Focus/Postbox Python contract set: hook, consumer, infra and migration tests
  are included in the ordinary email CI gate. Exact commands and final CI URLs
  must be recorded here after the PR head is pushed.
- Read-only Yandex desired-state audit: active feedback consumer Function
  `d4enjcfg3h6nep4ij4fh`, version `d4ejof08mqck6sp1cn1h`, mounts both HMAC key
  and version from Lockbox secret `e6qeqbto7ticn9fsklgq`, immutable secret
  version `e6qi77mdnpmetpljf5qa`, keys `hmac_key` / `hmac_key_version`. The focus
  hook desired state now pins those exact same references; no secret value was
  read or recorded.

These are implementation gates, not production closure evidence.

## Production evidence ledger

Pending fields must be replaced with immutable values during the serialized
runbook; absence of a value is a blocker, never an implied success.

| Gate | Evidence |
|---|---|
| PR #333 reviewed head / merge SHA | pending |
| logical backup path, SHA-256, `pg_restore --list`, restore drill | pending |
| pre/post migration versions and queries | pending |
| focus Auth Function version and sanitized config parity | pending |
| Fly release and in-container SHA | pending |
| exact DLQ queue/event/message inventory and histogram | pending |
| sanitized legacy manifest SHA-256 and registrations | pending |
| replay applied/duplicate/pending/error/remaining | pending |
| direct Auth, transactional, duplicate, suppressive canaries | pending |
| monitor delta/static/recovery notification evidence | pending |

## Rollback ownership

- SQL cutover rollback: re-grant only
  `focus_auth_begin_delivery_v1(uuid,uuid,text,boolean)` to `service_role`, reload
  PostgREST, and pair it with a previous Function-version rollback.
- Registry migration rollback: stop feedback replay/intake and restore the
  verified logical backup only under an explicit maintenance decision; never
  drop the registry after new events/receipts exist.
- Fly monitor rollback: redeploy the preceding Fly release from its exact
  `origin/main` SHA; do not alter queue retention/destination.
- Queue replay rollback is not deletion recovery: therefore deletion is allowed
  only after `applied` or field-identical `duplicate` proof, and every batch has
  before/after counts.

## Immediate mitigation

- Keep the DLQ intact.
- Do not bulk replay before the migration and legacy classification.
- Treat unchanged `dlq=162` notifications as one open incident, not repeated new incidents.
- Escalate immediately if the count grows, a complaint/bounce subset is identified, or retention evidence approaches expiry.

## Closure gates

- reviewed migration applied after backup and rollback-only contracts;
- exact reviewed Fly release deployed;
- post-ledger Auth and outbox canaries reach `delivered` without DLQ growth;
- all 162 retained messages classified by exact identity, event type and outcome;
- independently supported legacy receipts registered from a sanitized manifest;
- bounded replay applies/de-duplicates messages before deletion;
- DLQ zero or every retained blocker explicitly owned;
- `postbox_missing_correlation_count=0`;
- no unexplained unbound correlation;
- one recovery notification and no unchanged 15–20 minute alert storm;
- production evidence and immutable identities recorded here before status changes to closed.
