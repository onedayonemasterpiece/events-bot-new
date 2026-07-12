# R03 — Postbox Data Streams consumer architecture/security audit

Date: 2026-07-12 UTC

Lane: `code-audit`

Branch: `agent/postbox-events-release/code-audit`
Base/head before this report: `d9ba3ad03288a923997c6626295b8a9016cf26ed` (`origin/main`)

## Decision

**A new additive Supabase migration is required before the Postbox event consumer may be enabled.**
The current `public.email_record_provider_event_v1` is not safe for authoritative
Postbox events: it accepts a caller-supplied `p_email_hmac`, can suppress that HMAC
without finding a corresponding outbox row, marks an unmatched event `applied`, treats
all unsubscribe events as recommendation suppressions, and cannot retry a previously
inserted-but-not-applied event because a duplicate returns before application.
The existing SQL contract test positively asserts the unsafe unmatched-suppression
behavior and must be replaced.

Implement a Python 3.12 Yandex Function plus a **Postbox-specific v2 RPC**. Keep all
outbound switches disabled until the migration, function, Data Streams subscription,
replay path, and canaries below pass.

## Primary-source contracts used

1. Yandex Postbox writes JSON notifications to Data Streams. The current event types
   are `Send`, `Rendering Failure`, `Delivery`, `Bounce`, `DeliveryDelay`,
   `Subscription`, `Complaint`, `Open`, and `Click`; the authoritative provider
   correlation field is `mail.messageId`; event-specific timestamps and recipient
   fields are documented per object:
   <https://yandex.cloud/en/docs/postbox/concepts/notification>.
2. Postbox notification QoS is **at least once**. Identical notifications retain the
   same `eventId`; therefore `(provider, eventId)` is the deduplication key, not a body
   hash or YDS sequence number: same source as above.
3. A Data Streams Function trigger calls the function with
   `{"messages": [<JSON object>, ...]}`. Current Yandex documentation does not define
   a per-record partial-failure response. A Python handler reports failure by raising:
   <https://yandex.cloud/en/docs/functions/concepts/trigger/data-streams-trigger>,
   <https://yandex.cloud/en/docs/functions/lang/python/handler>.
4. Trigger batch size is in **bytes** (1 B to 64 KB), retries are 1–5 with a 10–60 s
   interval, and a platform DLQ is optional:
   <https://yandex.cloud/en/docs/functions/operations/trigger/data-streams-trigger-create>.
5. A Postbox configuration subscription selects a Data Streams destination and event
   types, and the identity must be associated with the configuration:
   <https://yandex.cloud/en/docs/postbox/operations/create-configuration>.
6. `SendEmail` returns `MessageId`; the existing adapter correctly persists this
   value through the outbox attempt contract. The consumer must compare it exactly
   with `mail.messageId` (trim outer whitespace only; do not strip angle brackets,
   lowercase, or use MIME `commonHeaders.messageId`):
   <https://yandex.cloud/en/docs/postbox/aws-compatible-api/api-ref/send-email>.
7. Supabase `sb_secret_` keys are backend-only, map to `service_role`, and bypass RLS.
   Use a dedicated per-component secret from Lockbox; pass it only as `apikey` and
   never log it. `SECURITY DEFINER` functions require a fixed/empty search path and
   explicit grants:
   <https://supabase.com/docs/guides/getting-started/api-keys>,
   <https://supabase.com/docs/guides/database/functions>,
   <https://supabase.com/docs/guides/api/securing-your-api>.

The current Postbox subscription API reference lists only seven matching event names
and omits `COMPLAINT` and `RENDERING_FAILURE`, while the newer notification contract
includes both. Do not silently omit them. Create/update the destination using the
current console/control plane, read it back, and make an actual complaint/rendering
fixture acceptance check part of the release gate. If the control plane cannot select
complaints, transactional production remains blocked and this is a provider-contract
blocker, not a reason to infer a name.

## Existing repository findings

### Safe/reusable pieces

- `PostboxAdapter.send()` enforces `Stream.TRANSACTIONAL`, uses the configured
  `ConfigurationSetName`, sends one `ToAddress`, and rejects a 200 response without a
  real `MessageId`.
- `email_control.provider_event` already has `(provider, provider_event_key)` unique,
  payload hash, trust flags and an `applied` bit.
- `email_control.email_outbox` has a unique partial index on
  `(provider, provider_message_id)`.
- The inbound serverless code provides useful patterns: bounded validation,
  allowlisted structured logging, HTTPS-only Supabase endpoint validation,
  `sb_secret_` sent as `apikey`, generic external error codes, and no secret-bearing
  exception chaining.
- Raw email tables are RLS-enabled and browser roles have no raw grants.

### Unsafe assumptions/defects in v1

1. **Uncorrelated suppression:** `p_email_hmac` is accepted from the caller and used
   even when no `(provider, provider_message_id)` outbox exists. A malformed,
   compromised or incorrectly configured backend can suppress any known HMAC.
2. **False applied:** an unknown provider message ID still leads to
   `provider_event.applied=true`.
3. **Retry dead end:** an evidence row inserted before outbox correlation can never be
   applied later because the duplicate path returns immediately.
4. **Wrong unsubscribe scope:** v1 maps every unsubscribe to `recommendation`.
   Postbox is transactional-only; a Postbox `Subscription` must map to
   `transactional`. NotiSend unsubscribe remains `recommendation`.
5. **No immutable-conflict detection:** the same `eventId` with a different message
   ID/type/hash is treated as a harmless duplicate.
6. **Caller-controlled trust:** `authenticated` and `verified` are booleans supplied
   by the client. A Postbox-only RPC should hardcode these after correlation rather
   than expose them as parameters.
7. **State incompleteness:** `Rendering Failure` does not become a terminal failure;
   `DeliveryDelay` has no explicit mapping; a duplicate/past event can regress state
   if future code extends the current case expression naively.
8. **The SQL fixture encodes the defect:** it expects a verified NotiSend event with
   message ID `417` and arbitrary `repeat('f',43)` to create suppression without an
   outbox. Replace this with a correlated outbox fixture and explicit negative tests.

## Implementation-ready consumer contract

Suggested location:

- `serverless/email_postbox_events/index.py`
- `serverless/email_postbox_events/requirements.txt` (stdlib-only is sufficient)
- `tests/test_email_postbox_events.py`
- build/reconcile additions under a dedicated `infra/yandex/email-postbox-events/`
  or the existing isolated email folder reconciler, without coupling to Fly deploy.

Runtime: Python 3.12, entry point `index.handler`, 128 MB, timeout 15 s, production
function tag (not `$latest`).

### Input envelope and limits

- Require a mapping with `messages` list; reject empty or more than 100 records.
- Trigger deploy should use batch size **1 byte** and cutoff 1 s. Since a single
  record may exceed the configured byte threshold, this effectively isolates one
  provider record per invocation while the handler remains batch-correct.
- Bound each canonicalized message to 64 KiB and the invocation to 128 KiB before
  detailed parsing. Unknown object fields are allowed for forward compatibility;
  required fields/types are strict.
- Never serialize/log the incoming event, `commonHeaders`, diagnostic code, subject,
  URL, user-agent, IP, recipient, or exception text.

### Canonical event parsing

Required for every record:

- `eventId`: non-empty string, unchanged by trimming, no control characters,
  1–300 characters. Store exact value as `provider_event_key`; `provider='postbox'`
  already namespaces it.
- `eventType`: exact provider spelling from the mapping below.
- `mail`: mapping.
- `mail.messageId`: non-empty, 1–512 characters, no control characters. Compare the
  exact returned Postbox ID; do not use `mail.commonHeaders.messageId`.
- `mail.identityId`: exact expected Postbox identity ID from env/Lockbox. A mismatch
  is permanent/fail-closed.
- `mail.tags['ses:configuration-set']`: must contain the expected configuration
  identity/name or ID observed from the real provider event. Because examples show
  an ID while the send request supplies a name, capture the first live canary and
  pin the actual stable value; never guess it.
- `payload_sha256`: SHA-256 hex of canonical JSON (`ensure_ascii=False`, keys sorted,
  separators `(',', ':')`, UTF-8). It is evidence/conflict detection, not dedup.

Event time and canonical recipients:

| Postbox `eventType` | internal type | timestamp | recipient source | state/suppression |
|---|---|---|---|---|
| `Send` | `accepted` | `mail.timestamp` | exactly one parsed address in `mail.commonHeaders.to` | no state regression; confirms submitted |
| `Rendering Failure` | `rendering_failure` | `mail.timestamp` | `commonHeaders.to` | `terminal_failed`; no suppression |
| `Delivery` | `delivered` | `delivery.timestamp` | `delivery.recipients` | `delivered` |
| `Bounce` | `hard_bounce` | `bounce.timestamp` | `bounce.bouncedRecipients[*].emailAddress` | `terminal_failed`; suppression `all`, reason `hard_bounce` |
| `DeliveryDelay` | `delivery_delay` | `deliveryDelay.timestamp` | `deliveryDelay.delayedRecipients[*].emailAddress` | evidence only; **never resend** while provider is retrying |
| `Subscription` | `unsubscribe` | `subscription.timestamp` | `commonHeaders.to` (object has no recipient field) | keep delivery state; suppression `transactional` |
| `Complaint` | `complaint` | `complaint.timestamp` | `complaint.complainedRecipients[*].emailAddress` | `terminal_failed`; suppression `all` |
| `Open` | `open` | `open.timestamp` | `commonHeaders.to` | evidence only; do not subscribe initially |
| `Click` | `click` | `click.timestamp` | `commonHeaders.to` | evidence only; do not subscribe initially |

Postbox currently documents `Bounce` only for a non-retryable failure and documents
`Permanent` (the field table contains the typo `Permenent`). Accept either spelling,
case-insensitively, until a live event settles the provider output; any other
`bounceType` is an unknown schema and must not create hard suppression. A
`bounceSubType=Suppressed` is still a hard-bounce/all-scope suppression.

Parse RFC 3339 timestamps as timezone-aware datetimes. Preserve the represented
instant; convert to UTC for the RPC. Do not substitute function receipt time for a
missing/invalid provider timestamp.

The sender has a one-recipient contract. Require exactly one unique recipient in the
event-specific collection and exactly one address in `commonHeaders.to`; when an
event-specific collection exists, both must match after normalization. Multi-recipient
or mismatched events go to permanent-failure/replay review and make no DB mutation.

Important provider-doc discrepancy: the current `Subscription` example omits
`eventId`, although the main schema says it is required. **Never synthesize an
unsubscribe event ID.** A live unsubscribe canary must prove a stable `eventId`; until
then such an event fails closed and cannot suppress.

### Address normalization and HMAC

Use one shared helper for identity sync, sender and consumer:

1. Extract mailbox using `email.utils.getaddresses` (never split on comma manually).
2. Require exactly one syntactically parseable address, no CR/LF/NUL, length 3–320.
3. Normalize exactly as the current SQL identity contract: `lower(trim(address))`.
   Do not add Unicode NFKC or IDNA transformations only in this consumer; that would
   produce a different identity key.
4. Compute `base64url_no_padding(HMAC-SHA256(key_version_secret,
   normalized_email.encode('utf-8')))`; expected length is 43.
5. Pass only this HMAC and `EMAIL_ADDRESS_HMAC_KEY_VERSION` to Supabase. Zero logs and
   zero application persistence of the plaintext address.

The function needs the same current versioned address-HMAC secret used by verified
identity synchronization. During rotation, deploy `current` and `previous` secrets;
the RPC must compare the supplied version to the correlated identity row. Do not try
both keys and do not silently re-key an identity inside event ingestion.

### Supabase RPC v2 (minimal migration)

Create `public.email_record_postbox_event_v2` rather than widening generic v1.
Suggested parameters:

```text
p_provider_event_key text
p_provider_message_id text
p_event_type text
p_event_at timestamptz
p_recipient_hmac text
p_hmac_key_version integer
p_payload_sha256 text
```

Return one of `applied`, `duplicate`, or `correlation_pending`. Do not accept provider,
authenticated, verified, scope, reason, or a plaintext recipient from the caller.

Atomic behavior:

1. Validate lengths/event enum/hash/HMAC version.
2. Select and lock the exact `email_control.email_outbox` where
   `provider='postbox'` and `provider_message_id=p_provider_message_id`, joining its
   `recipient_identity`.
3. If not found, return `correlation_pending` **without inserting an event**. This
   handles the race where a provider notification arrives before the sender commits
   the returned `MessageId`; the function treats this result as retryable.
4. Require the outbox stream to be `transactional`, `dry_run=false`, and status in the
   explicit provider-accepted lifecycle. Compare both HMAC and key version to the
   correlated identity. Mismatch is a permanent exception and performs no insert.
5. Insert the event with hardcoded `provider='postbox'`, `authenticated=true`,
   `verified=true`, and the **DB-derived** identity HMAC. On unique conflict, lock and
   compare message ID, type, timestamp, payload hash and HMAC. Exact match returns
   `duplicate`; any difference raises `provider_event_conflict`.
6. Apply monotonic state transition and suppression in the same transaction. Set
   `applied=true` only after all effects succeed. Replaying an exact already-applied
   row returns `duplicate` without additional effects.
7. `accepted` and `delivery_delay` cannot move a terminal/delivered row backward.
   `delivered` moves `submitted` to `delivered`; `hard_bounce`, `complaint`, and
   `rendering_failure` move a non-dry-run submitted/delivered row to
   `terminal_failed`; `unsubscribe`, `open`, and `click` do not replace delivery
   state. Provider-event rows retain the complete state history.
8. Hard bounce/complaint insert active all-scope suppression. Postbox unsubscribe
   inserts active transactional-scope suppression. Existing unique suppression index
   makes the business effect idempotent.

Migration security:

- `SECURITY DEFINER SET search_path=''`; schema-qualify every object.
- Revoke v2 execute from `PUBLIC`, `anon`, and `authenticated`; grant only
  `service_role`.
- Revoke v1 from `service_role` until it is replaced with safe correlated semantics.
  There is no deployed caller today; do not leave the unsafe callable surface merely
  for compatibility.
- Keep internal tables RLS-enabled and without raw table grants.
- Add a provider-message ID length constraint/index validation if live data permits;
  the Python boundary must enforce it regardless.

### Function → Supabase call

- HTTPS URL only, no credentials/query/fragment, path
  `/rest/v1/rpc/email_record_postbox_event_v2`.
- Headers: `Content-Type: application/json`, `Accept: application/json`, dedicated
  `sb_secret_...` as `apikey`, and a fixed non-browser User-Agent. Do not add the
  secret as Bearer authorization.
- 10 s timeout, response body read cap 16 KiB.
- 2xx with `applied|duplicate` = record success.
- 2xx with `correlation_pending`, HTTP 408/409/425/429/5xx, timeout/TLS/connection
  error = retryable.
- Other 4xx or invalid response = permanent/schema/security failure.
- External exception messages/bodies must never enter logs.

## Batch failure, retry, DLQ and replay

Yandex documents no partial-record response for Data Streams triggers. Therefore:

- Process every record in the invocation, collecting only allowlisted status codes.
- Successfully committed records remain committed and are harmless on whole-batch
  retry because of `eventId` dedup.
- If any retryable/permanent record remains, raise one generic
  `PostboxEventBatchError('batch_failed')` after the loop; never return a list of
  failed IDs as if the trigger understood it.
- Deploy with 1-byte batch threshold to isolate records, retry attempts `5`, interval
  `30s`, and a dedicated DLQ. The handler still supports larger batches for provider
  or platform drift.

**Privacy caveat:** the provider source stream necessarily contains recipient
plaintext. A platform trigger DLQ also receives the original failed trigger message,
so it is not literally HMAC-only storage. If the release requirement means no new
plaintext-at-rest outside the provider stream, do not use a raw platform DLQ. Instead:

1. after valid parsing/HMAC, envelope-encrypt the canonical original with a dedicated
   KMS key and store ciphertext only in the existing private email bucket (short
   lifecycle, separate prefix); and
2. put only `{eventId, provider_message_id, recipient_hmac, payload_sha256,
   ciphertext_pointer}` into a private YMQ replay queue.

For malformed events where no safe recipient HMAC can be produced, store only payload
hash + YDS shard/sequence operational metadata and alert; inspect the original within
its short source-stream retention under break-glass IAM. Never copy malformed
plaintext to logs. If the platform DLQ is used as the pragmatic fallback, document it
as an explicit exception, use a dedicated least-privilege SA, minimum retention, and
prove who can read it before production.

Replay worker/tool requirements:

- global/transactional DB switch and separate env `POSTBOX_EVENT_CONSUMER_ENABLED`
  are independent kill switches;
- pausing the event consumer must not enable outbound; outbound should be disabled if
  event lag exceeds the release threshold;
- replay accepts only a minimized/KMS-pointer capsule, recomputes/validates hashes,
  calls the same v2 RPC, and is safe on duplicates;
- no ad-hoc SQL suppression and no manual deletion of dedup rows;
- dashboard/alert on oldest unprocessed age, retry count, DLQ depth,
  `correlation_pending`, conflict, schema mismatch and recipient mismatch, all keyed
  by event/message/outbox hashes rather than address.

## Required function environment / Lockbox

Non-secret environment:

- `POSTBOX_EVENT_CONSUMER_ENABLED=0` initially
- `POSTBOX_EXPECTED_IDENTITY_ID=<provider identity id>`
- `POSTBOX_EXPECTED_CONFIGURATION_TAG=<observed live tag value>`
- `PERSONALIZATION_SUPABASE_URL=https://<project>.supabase.co`
- `EMAIL_ADDRESS_HMAC_KEY_VERSION=<positive integer>`
- optional bounded timeout/body settings with safe defaults

Lockbox-injected secrets:

- dedicated per-function `PERSONALIZATION_SUPABASE_SECRET_KEY` (`sb_secret_...`), not
  the inbound adapter's key and not a legacy shared service-role JWT;
- `EMAIL_ADDRESS_HMAC_KEY_CURRENT` and, only during a planned rotation window,
  `EMAIL_ADDRESS_HMAC_KEY_PREVIOUS`;
- if strict ciphertext replay is selected, KMS/object credentials or, preferably,
  function service-account IAM with no static access key.

The runtime SA needs only invocation and secret payload access for its own secrets;
stream access belongs to the trigger SA. Replay storage permissions must be scoped to
its one bucket prefix/queue. Do not grant folder-wide `editor`, `admin`, or
`postbox.editor` to the runtime consumer.

## Tests required before merge/deploy

### Python unit/contract tests

- all nine provider event mappings and exact timestamp source;
- exact `mail.messageId` correlation; reject common-header-only or normalized IDs;
- address extraction with display name, case, whitespace; HMAC golden vector;
- recipient mismatch, zero/multiple recipients, mismatched common/event-specific
  recipient, malformed address;
- missing/wrong identity, configuration tag, eventId, timestamp, required object;
- `Permanent` and documented `Permenent` bounce accepted; unknown bounce type fails
  without suppression;
- `Subscription` without eventId fails; no synthesized key;
- canonical payload hash stable under object-key order;
- exact duplicate succeeds; eventId/hash/message/type conflict fails;
- no address/subject/URL/diagnostic/provider response/secret in captured logs;
- Supabase transient vs permanent response classification;
- all records attempted before a generic batch raise; prior successes are duplicate
  on retry; invalid top-level/oversized batches fail closed;
- kill switch prevents Supabase calls.

### SQL transactional contract tests

- unmatched message ID returns `correlation_pending`, inserts no event/suppression;
- arbitrary correct-length HMAC cannot suppress another identity;
- wrong HMAC version/mismatch makes no mutation;
- correlated delivered event updates only its outbox;
- hard bounce and complaint create all-scope suppression and block a second
  transactional claim;
- Postbox unsubscribe creates transactional (not recommendation) suppression;
- delivery delay never makes an outbox retryable and never triggers a second send;
- rendering failure becomes terminal;
- duplicate is idempotent; conflicting duplicate raises;
- out-of-order accepted/delay cannot regress delivered/terminal state;
- v1 no longer executable by `service_role`, browser roles cannot execute v2, and
  browser roles still have no raw table grants.

### Live gates

1. Destination read-back proves enabled event types, stream and associated identity.
2. One seed yields `Send` then `Delivery`, exact returned MessageId correlation, one
   applied row per event, zero plaintext DB/log evidence, no DLQ.
3. Duplicate injection of the exact live event produces `duplicate` and no state
   change.
4. Controlled unmatched-ID fixture retries then reaches the approved replay path
   without suppression.
5. Hard-bounce, complaint and unsubscribe **synthetic fixtures in staging** prove DB
   semantics. Do not deliberately spam/complain or send to arbitrary invalid users in
   production.
6. Exercise consumer kill switch, replay, stream lag alert and outbound fail-closed
   coupling before enabling transactional application sends.

## Commands and validation performed in this audit

Read-only commands included:

- `git status --short --branch`, `git rev-parse HEAD`
- `rg`/`sed` inspection of `email_control/`, `serverless/email_inbound/`,
  `supabase/migrations/20260711203940_email_control_plane_v1.sql`,
  `supabase/tests/email_control_plane_contract.sql`, tests, infra desired state,
  `.env.example`, and `docs/operations/email-delivery.md`
- official Yandex Cloud and Supabase documentation research listed above
- read-only inspection of the official Yandex Cloud example function at
  <https://github.com/yandex-cloud-examples/yc-postbox-events/blob/main/index.py>,
  confirming that trigger `messages` are already decoded Postbox JSON objects

No code, migration, tests, provider state, Supabase state, or Yandex resources were
changed. No runtime test was appropriate for this read-only architecture lane.

## Risks and merge notes

- **Release blocker:** do not enable Postbox transactional sending with v1 event RPC.
- **Provider-doc blocker to verify live:** complaint/rendering subscription names and
  actual `Subscription.eventId` contract.
- **Privacy decision:** raw platform DLQ versus strict KMS-ciphertext replay must be
  explicit; calling a raw DLQ “HMAC-only storage” would be false.
- **State-race risk:** event-before-outbox-commit is expected; `correlation_pending`
  must retry without inserting an applied tombstone.
- **Do not cherry-pick code from this lane.** This report is the only deliverable and
  should be used as the implementation/review contract by the writable consumer and
  migration lanes.
