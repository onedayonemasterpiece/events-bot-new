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
| logical backup path, SHA-256, `pg_restore --list`, restore drill | `artifacts/codex/INC-2026-08-04-postbox-feedback-dlq-correlation/personalization-pre-postbox-20260805T074249Z.dump`, 6,188,260 bytes, SHA-256 `487ff4c0c5934e5c9e09e97d2c44403d1ca01e2b58299fd8665ed645641b23b2`; list SHA-256 `8757affc501f64ec8cd218b7b08b15e86082abeb8ef1f1745e4ee42e010ce31b`; exact Supabase temporary-project restore including `auth.users` passed on PostgreSQL 17 with provider extensions. Validation SHA-256 `d5377bcb297a53738f556385fbf1f2b364e234bb723ed3f99d4a9e876d1ac0ba`, restore log `4c0d03cc4944066d0e7691bf6e6648959cdaadce1767e3c8cbfb3a5d17220056`, rollback contract `d57c25ea033bad7c2b63a3fe5055ddcc212616815d3cee3b6120f14c1bbb5103`; temporary project `znizhmnwabvkzhndaznn` deleted, evidence `ad0c3acf28bab4561f49bcf58709eb0074e17da5138cb8ea9f28aba15c331daf` |
| Supabase Send Email Hook precondition | verified through the official Management API at 08:39 UTC: `hook_send_email_enabled=false` and URI absent. Sanitized artifact `auth-config-precondition-20260805T083926Z.json`, SHA-256 `e1348fc5bccc26496d0180a7dd946430b670b9fe316c6e688196a6081b7ebc1d` |
| pre/post migration versions and queries | selectively applied only `20260804190000`; apply SHA-256 `1fbd89062abc50c150316a006821ad505681ac99f9e4ebd01870ac9ca6df3fec`, post-contract `b680c8b195713dfa322ff08e06e55c6039febb83d37f44842b296577f23f20fe`, assertions `c1d7050f8c3632a3d85c537648436831eaae016a09099b4e627b936ebe26a943`. Repaired only that migration-history version; repair `b5cfced2f535864f000269058f24f39f8d8060c41bf780e4725cf4f0e85dd5a7`, exact postcheck `b3e641f2a2f3e9a1d5a7b8a8ecfb3615eb4b1bc4bb21b4ea4eac769aad645886`. Unrelated Google history was not changed |
| focus Auth Function version and sanitized config parity | hook Function `d4euk47p8gv7qmgrtib4`, active version `d4ek3skja1m881culgd7`, deterministic zip SHA-256 `2f98728f04aeb09783e084d91dca2c8339e6d25b58cde0d0f85a0f8fad7fbe22`, deploy evidence `5c510314b92cf51310282add79370fbeca38b274cea53202e83f45f5fc79a8ba`; invalid-signature smoke returned `401`, evidence `68aa0b998925da793088f0db35af390b5e66f263f5befc2028b48d8abebf0af0`. It pins HMAC Lockbox version `e6qi77mdnpmetpljf5qa` and extracts only the documented Yandex `context.token.access_token` |
| Supabase Send Email Hook final state | enabled at exact URI `https://d5d17smc4tutrt316fjo.uvah0e6r.apigw.yandexcloud.net/v1/send-email`; final Management API evidence SHA-256 `a66edd52a5b0695289dd154ce719243ecda61afc250e6bf486bbe6b772504ad9` |
| Fly release and in-container SHA | release `1914`, machine `48e419df93e078`, image `deployment-01KZ8QQD590542C0CSA64J1FXY`, `/app/.static-site-repo-sha` = exact `origin/main@e7f02bf83f4b94d250be7cc6b792495de3be1984`; `/healthz` ready with DB and all issues empty. Same PR #333 implementation lane contains follow-up commits `f93df1e8f`, `d2d5f443c`, `3d2a8d387`, `e7f02bf83` |
| exact DLQ queue/event/message inventory and histogram | exact bounded inventory: 162 queue messages, 162 unique event IDs, 81 unique provider message IDs; `Send=81`, `Delivery=78`, `Bounce=3`, oldest `2026-07-29T07:53:33.005397822Z`, newest `2026-08-02T23:58:04.326672281Z`, malformed/unsupported `0`. All 162 classify `unproven:correlation_pending`. File SHA-256 `39b1ee6d93ed59596c7d96cb49d4b17e66109cb52025bdf443107947f5201a97`, internal manifest SHA-256 `7908b414a45b259887ba7951c2f548423f299cbd31be22d2b4ea03b6af945f7d`; visibility restored to 162/0, evidence `a6fe420ea8b2dfc45f9692d0b13f61bb80137fd0824412af48c56e66faa944e4` |
| sanitized legacy manifest SHA-256 and registrations | no independently verifiable sender-ledger evidence was found for any of the 81 legacy provider receipts, so fail-closed registration count is `0`. All 162 exact queue receipts are retained with owner `onedayonemasterpiece` and blocker `legacy_message_id_has_no_independent_sender_ledger_evidence`; blocker manifest SHA-256 `009ca3d1d7788337f3bb7d62bd5cb6a28054ea0ed74baa162550415ad40a4620` |
| replay applied/duplicate/pending/error/remaining | batch 001 stopped on its first retained message as required: `consumer_disabled`, applied `0`, duplicate `0`, deleted `0`, remaining `162`; replay evidence `3666f69f522f0806db023336bc7376e0509ea6e26e3fc97e604f92baa593146d`. DB pre/post evidence is byte-identical SHA-256 `355232526918b364d2967316157e13ffc71b2363d3b0e8e8d4bdfd21d288485b` (provider events 6, applied 6, active suppressions 0, correlations 1, legacy correlations 0). Replay now preflights the complete deployed consumer environment before receiving, deployed in release 1914; no second batch was attempted after the mandated first-error stop |
| direct Auth and duplicate canaries | fixed first-time controlled browser canary passed with exact immutable preview SHA `4a19fbe0b243d8a9a4652ff0c1e4fee9e895cf9c`: one issue, one mail, one verify and one registration. Backend: one `postbox/accepted`, feedback `delivered`, two authenticated/verified/applied events (`Send` and `Delivery`), HMAC version 1, message alias `0537ad09df67cf72`, missing/unbound correlations 0. Evidence: browser manifest `c284a2c04e355ce2c07efbfa6f4e22a2202b7279504734aab9006e487619f8d6`, backend `9ae5479f7a42d4098c5b2ab3e8d7408649c0d69630ff33858a5941b217f50088`, operator/health `916deb74ddf5217b6d5dc055d1be035b835beac2815a5e6e76290422e058b044`. Exact delivered-event replay returned `duplicate`; attempt/event counts stayed 2/2, evidence `ac7866de7885e2a1302d2da975be5240ed217f35345ab5a62387e05373ea5275` |
| transactional and suppressive canaries | not performed: the user-authorized live canary was explicitly the fixed first-time Auth mailbox, and no arbitrary recipient or synthetic production bounce/complaint was authorized |
| monitor delta/static/recovery notification evidence | state file remains PII-free mode `0600`. A real inventory in-flight delta notified once at 10:13:31 UTC; three unchanged scheduled runs through 10:30 UTC did not notify again, proving no 15–20 minute storm. Evidence SHA-256 `2672bc3caf5ad7d9928f6cf321963f9143fc036e54ae08c03bf8d377896e7f2a`. Recovery notification is not emitted because 162 evidence-blocked messages correctly remain in DLQ |

Sanitized serialized-run summary:
`artifacts/codex/INC-2026-08-04-postbox-feedback-dlq-correlation/production-run-20260805T0805Z.json`,
SHA-256 `44794282847956049ca04eb641fea492040a8c1025bde16dca925d9bbb9f130c`.

## Production run status — 2026-08-05

The implementation is delivered through PR #333 and its same-lane continuation,
but the incident remains open. The serialized production sequence stopped before
any database, Function, queue or switch mutation for the remaining fail-closed
reasons below.

The Auth-config blocker is resolved. The earlier HTTP `403` was Cloudflare error
`1010`: its bot filter rejected Python `urllib`'s default User-Agent before token
authorization. Both the cached CLI token and the replacement environment token
return `200` with an explicit operator User-Agent. This was not an
`auth_config_read` scope denial. The live Send Email Hook is verified disabled.

1. Standard Supabase migration dry-run is not isolated to this incident. The
   initial checkout lacked remote-history version `20260803143000`; after a
   temporary exact-history overlay it reported two unrelated Google AI
   migrations plus `20260804190000` as pending. The overlay was removed and no
   remote mutation was attempted. A read-only semantic check confirms the two
   Google migrations' table, limiter contract and six distinct scopes already
   exist in production while their versions are absent from migration history.
   Reapplying or history-repairing unrelated Google changes is outside this
   incident. Sanitized evidence: `migration-consistency-20260805T0841Z.json`,
   SHA-256 `a1675b6134669aef13041528f3ccd12cf52620f862a2eb06cd4082fc46cfe9b7`.
2. The logical backup and restore list exist, but the isolated restore drill
   cannot complete on vanilla PostgreSQL 17 without the production provider's
   `supabase_vault` extension.
3. Exact DLQ classification depends on the unapplied service-only classifier
   RPC. Replay, deletion, legacy registration and canaries therefore remain
   prohibited.

Current read-only Yandex evidence remains healthy: trigger
`a1svvdcbe8pdoc8cv74a` is `ACTIVE`, the feedback consumer is the pinned version
above, and its YDB is `RUNNING`. No retention, destination, DLQ or unrelated
Yandex resource was changed.

### Serialized continuation and direct Auth canary finding

The authorized continuation completed the compatible temporary-Supabase
restore drill, selectively applied only migration `20260804190000`, repaired
only that history version, reconciled the actual NotiSend provider counter and
deployed the reviewed Auth Function boundary. The first fresh controlled
Postbox canary then failed closed: the exact delivery ledger recorded one
`postbox / definitive_reject`, the Auth request returned `500`, no verify or
membership write occurred, no provider receipt/correlation was invented, and
the Hook was immediately disabled.

The failure exposed a separate runtime-contract defect in the reviewed hook.
Yandex Python Functions provide `context.token` as an authentication object
whose `access_token` field is the IAM token. The hook stringified the entire
object and sent that representation as `X-YaCloud-SubjectToken`; Postbox
correctly rejected it. The unit fixture had modeled `context.token` as a plain
string and therefore missed the production shape. The regression now supplies
the documented object form and asserts that only `access_token` reaches the
Postbox header. Hook reactivation, a fresh controlled Postbox canary and the
exact DLQ inventory then ran from the same PR #333 implementation lane; their
immutable results are in the production evidence ledger above.

### Final serialized outcome on 2026-08-05: incident remains open

The new product path is healthy: the Hook is enabled on Function version
`d4ek3skja1m881culgd7`, the fixed first-time Auth canary reached Postbox
`accepted` and authenticated `delivered`, its exact duplicate was idempotent,
and unified health reports no missing or unbound correlation.

The retained historical backlog cannot be deleted safely. All 162 queue
messages are structurally valid but all 81 unique legacy provider receipts lack
independent sender-ledger evidence outside the feedback envelope. Registering
them from the DLQ alone would violate the incident's fail-closed correlation
contract. The first bounded replay also exposed a missing process-local
consumer environment and stopped on `consumer_disabled` before deletion; the
tool now rejects that configuration before receiving a message, but the
mandatory stop-on-first-error rule prohibits continuing this serialized replay.

Therefore the product result for this run is `FAIL`, not a partial success. No
message was purged or deleted, all visibility was restored, and every retained
queue receipt has a hash-only blocker row owned by `onedayonemasterpiece`.
Incident closure still requires independently reviewed legacy sender evidence
or an explicit product decision before retention expiry, a new bounded replay
run, and a real monitor recovery notification after the backlog clears.

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
