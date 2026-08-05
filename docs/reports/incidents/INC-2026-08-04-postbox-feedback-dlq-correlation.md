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
- the old suppression-free v1 Auth admission is revoked atomically by the main
  migration under the verified production precondition that the Supabase Send
  Email Hook remains disabled; no normal migration ordering can leave a bypass.

HMAC rotation remains deliberately prohibited while retained feedback,
correlations or active suppressions use the current key/version. A future
rotation requires an overlap/keyring migration and is not part of this incident.

## Validation evidence (pre-production)

- PostgreSQL 17 isolated apply: CI bootstrap plus migrations
  `20260711203940`, `20260712072912`, `20260712083037`, `20260801222242`,
  `20260804190000` applied successfully, including the v1 privilege revocation.
- Rollback-only SQL contract: `BEGIN / DO / ROLLBACK`, green after the new
  migration apply and revocation cutover. It covers Auth/outbox/legacy
  correlation, cross-source collision,
  exact/conflicting duplicates, 512-character receipt, 300-character event key,
  event count above 1000, suppression scope/version, atomic email change,
  immutable outbox identity, health and role privileges.
- Focus/Postbox Python contract set: hook, consumer, infra, DLQ recovery and
  migration tests are included in the ordinary email CI gate. The final PR head
  passed 93 exact email tests and the full email/inbound set passed 94 tests.
- Final PR checks were green: [Postbox SQL contract](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30986778908/job/92243258576),
  [Python CI](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30986778912/job/92243258953)
  and [static browser release gate](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30986778912/job/92243258881).
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
| PR #333 reviewed head / merge SHA | merged 2026-08-05 07:56 UTC as `183326628b01d7f3d2762df5e0215af7540473f4`; the same implementation lane continued with fail-closed proof constraint `cd3f3d419cf97d7c2fcba6d52e8e37aef1bad385`, reachable from `origin/main` as `eecc93e451f32a66363218d6fe8352d5e5b6dada` |
| logical backup path, SHA-256, `pg_restore --list`, restore drill | `artifacts/codex/INC-2026-08-04-postbox-feedback-dlq-correlation/personalization-pre-postbox-20260805T074249Z.dump`, 6,188,260 bytes, SHA-256 `487ff4c0c5934e5c9e09e97d2c44403d1ca01e2b58299fd8665ed645641b23b2`; list SHA-256 `8757affc501f64ec8cd218b7b08b15e86082abeb8ef1f1745e4ee42e010ce31b`; vanilla PostgreSQL 17 restore is **blocked** because the provider extension `supabase_vault` is unavailable there, so restorable-backup proof is not yet satisfied |
| pre/post migration versions and queries | precheck SHA-256 `73e84abeaa9170f689096925c8a450397032e2fadb3fca144020cfa0d3d2a8f6`: no accepted outbox/Auth Postbox rows, duplicate receipts or cross-source collisions; `20260804190000` is absent in production. No post-state: migration intentionally not applied |
| focus Auth Function version and sanitized config parity | current feedback consumer `d4enjcfg3h6nep4ij4fh` / `d4ejof08mqck6sp1cn1h`; desired hook parity pins the same Lockbox version, but no new Function version was deployed because the database precondition is blocked |
| Fly release and in-container SHA | release `1909`, machine `48e419df93e078`, image digest `sha256:5989aa44b0d9b6bfb265fd5e9e409068d12587878301eac645554a336ab66870`, `/app/.static-site-repo-sha` = `eecc93e451f32a66363218d6fe8352d5e5b6dada`; `/healthz` ready with DB, scheduler, email worker and email monitor all `ok` |
| exact DLQ queue/event/message inventory and histogram | **blocked** until the classification RPC from the unapplied migration exists; no message was deleted |
| sanitized legacy manifest SHA-256 and registrations | **blocked** by exact inventory/classification; none registered |
| replay applied/duplicate/pending/error/remaining | not started; applied `0`, deleted `0`; migration/inventory gates remain blocked |
| direct Auth, transactional, duplicate, suppressive canaries | not run; no authorization to change product switches or send production canaries, and the migration/Function cutover is blocked |
| monitor delta/static/recovery notification evidence | Fly monitor implementation deployed; first scheduled run completed at 08:11:01 UTC in 1,935 ms. State file `/data/email-postbox-monitor-state.json` is mode `0600`, contains only the six documented keys, and records initialized `dlq_total=162` / `postbox_dlq_nonempty`. Delta/static/recovery transition evidence remains pending until a controlled queue transition |

Sanitized serialized-run summary:
`artifacts/codex/INC-2026-08-04-postbox-feedback-dlq-correlation/production-run-20260805T0805Z.json`,
SHA-256 `44794282847956049ca04eb641fea492040a8c1025bde16dca925d9bbb9f130c`.

## Production run status — 2026-08-05

The implementation is delivered through PR #333 and its same-lane continuation,
but the incident remains open. The serialized production sequence stopped before
any database, Function, queue or switch mutation for these fail-closed reasons:

1. The official Supabase Management Auth-config read returned HTTP `403` for
   both available redacted credential lanes. The required `auth_config_read`
   scope is unavailable, so the mandatory proof that the live Send Email Hook is
   disabled could not be obtained.
2. Standard Supabase migration dry-run is not isolated to this incident. The
   initial checkout lacked remote-history version `20260803143000`; after a
   temporary exact-history overlay it reported two unrelated Google AI
   migrations plus `20260804190000` as pending. The overlay was removed and no
   remote mutation was attempted.
3. The logical backup and restore list exist, but the isolated restore drill
   cannot complete on vanilla PostgreSQL 17 without the production provider's
   `supabase_vault` extension.
4. Exact DLQ classification depends on the unapplied service-only classifier
   RPC. Replay, deletion, legacy registration and canaries therefore remain
   prohibited.

Current read-only Yandex evidence remains healthy: trigger
`a1svvdcbe8pdoc8cv74a` is `ACTIVE`, the feedback consumer is the pinned version
above, and its YDB is `RUNNING`. No retention, destination, DLQ or unrelated
Yandex resource was changed.

## Rollback ownership

- SQL admission rollback: re-grant only
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
