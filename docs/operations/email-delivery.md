# Email infrastructure, delivery and deliverability

> Status: inbound/mailbox foundation live; outbound providers remain production-gated.

## Scope

Canonical operational contract for:

- human inbound/outbound correspondence through `info@kenigevents.ru`;
- automated processing of an inbound copy;
- transactional account and saved/followed-event email;
- personal recommendation emails containing exactly three events and linking to an already published personal page.

Product content, consent purpose and cadence remain separate. DNS, secrets, delivery evidence, suppression and incident/rollback controls are shared operational concerns.

## Role map and non-overlap

| Surface | Provider / owner | Contract |
|---|---|---|
| Human/inbound mailbox | SpaceWeb | MX target and durable mailbox for `info@kenigevents.ru`; manual webmail/IMAP/SMTP correspondence. |
| Automated inbound copy | Read-only SpaceWeb IMAP collector → Yandex Functions/YMQ | Polls only new IMAP UIDs without setting `Seen` or changing the mailbox purpose; private normalized envelopes, bounded batches and processing DLQ. |
| Direct inbound canary | Yandex Cloud Mail Trigger → intake Function | Keeps an isolated technical address for direct trigger/schema/DLQ canaries. It is not the SpaceWeb mailbox destination. |
| Transactional outbound | Yandex Cloud Postbox | Critical account and saved/followed-event lifecycle messages only; intended From `Kenig Events <notify@kenigevents.ru>`, `Reply-To: info@kenigevents.ru`. |
| Recommendation outbound | NotiSend | Opt-in personal recommendations/announcements only; intended From `Kenig Events <events@news.kenigevents.ru>`, `Reply-To: info@kenigevents.ru`. |
| Identity and send control | Personalization Supabase/Postgres | Verified email, purpose-specific consent/subscription, hard admission cap, preferences, suppression, outbox, send guard and provider evidence. |
| Analytics projection | YDB | De-identified asynchronous delivery/product aggregates only; never send eligibility or outbox state. |
| Published personal artifacts | Object Storage/CDN | Rendered personal HTML/JSON after validation; never recipient/control state. |

Providers are not interchangeable. Postbox must not be used as a hidden fallback for recommendations, and NotiSend must not send critical transactional messages. A provider contact/list is a projection from Supabase, not evidence of consent.

See [personalization data ownership](../architecture/personalization-data-ownership.md).

## Live disabled-by-default control-plane foundation

The additive foundation is applied to the live personalization project, but all
outbound switches remain disabled and dry-run-only. It provides:

- an additive private `email_control` Supabase schema for synchronized verified identities, purpose consent/audit, the atomic 200-user recommendation admission row, suppression, recommendation issue/items, outbox attempts and provider-event deduplication;
- authenticated preference RPCs plus service-only stage/publish/enqueue/claim/finalize/event-ingest RPCs with no raw table grants to browser roles;
- fixed provider routing (`transactional -> postbox`, `recommendation -> notisend`) enforced by both SQL and runtime adapters;
- Postbox API sending that requires the real returned `MessageId`;
- NotiSend individual-message API sending with `payment=subscriber` and the real returned message `id`;
- disabled DB/process switches and dry-run defaults;
- an idempotent metadata-only inbound receipt boundary; message bodies and open
  addresses remain in the private Yandex envelope/SpaceWeb mailbox, not Supabase.

The provider adapters are currently a tested library/control-plane foundation;
they are not wired to a deployed Fly scheduler/worker. Do not describe either
automated outbound stream as production-enabled until its worker, provider-event
consumer and stream-specific canary gates are deployed.

On 2026-07-11 the previous Supabase history drift was reconciled before applying
email migrations: a restorable logical backup was taken, all prior migration-owned
functions/indexes/RLS policies and semantic table columns were hash-compared with a
clean PostgreSQL 17 + pgvector replay, duplicate legacy filenames were assigned
unique CLI-compatible versions, four probe-only history rows were reverted, and a
session-pooler dry run proved that only the email migrations would apply. The live
history now matches all repository versions. Continue to use the session pooler on
port 5432 for migration operations; the transaction pooler produced a prepared
statement collision during the guarded first attempt.

Public NotiSend documentation does not document a webhook signature. A NotiSend webhook body is therefore only an untrusted signal: it may be size/schema/dedup checked and recorded, but it cannot update delivery state or suppression until an authenticated `GET /v1/email/messages/:id` lookup verifies the provider state. The repository now contains the Postbox-specific V2 boundary: an IAM-authenticated YDS consumer computes a versioned recipient HMAC and a service-only Supabase RPC correlates it to the exact persisted Postbox `messageId` and DB-owned identity before applying an idempotent transition or suppression. The isolated live YDS/Function/DLQ resources and event destination still have to pass the release gates below, so transactional application sending remains disabled.

## Live provider state (2026-07-12)

- SpaceWeb retains `info@kenigevents.ru` and `dmarc@kenigevents.ru`; public MX,
  combined root SPF, SpaceWeb DKIM and monitoring DMARC resolve without changing
  the authoritative Yandex nameservers or existing site/CDN records. One outbound
  webmail canary from `info@kenigevents.ru` to the controlled `info@kgd80.ru`
  address exists exactly once in `Sent` with no sender-side bounce. The mailbox
  owner confirmed that this exact canary arrived at `info@kgd80.ru`; folder
  placement and raw recipient-side authentication headers were not supplied and
  are not claimed. The current host cannot establish TLS to
  `smtp.spaceweb.ru:465`, so direct client SMTP was not claimed.
- The isolated Yandex inbound folder, KMS-encrypted private bucket, three YMQ
  queues, four production-tagged Functions and three triggers are active. Both a
  direct Mail Trigger canary and a retained-mailbox IMAP canary sent from
  `info@kgd80.ru` produced one idempotent Supabase receipt with empty DLQs; the
  retained SpaceWeb item remained present and unread.
- The Postbox `kenigevents.ru` identity and transactional configuration are
  verified. A seed from `notify@kenigevents.ru` returned a real provider message
  id and reached the SpaceWeb mailbox. Its two DKIM signatures, SPF and DMARC all
  passed; SpaceWeb still placed this single fresh-domain/self-domain seed in Spam
  without exposing a deterministic classifier reason. Postbox transport works,
  but the isolated folder has no YDS stream, event consumer or event destination.
  Keep transactional application sending disabled until consumer + stream +
  destination, suppression/alert evidence and a bounded cross-provider warm-up
  canary are completed atomically.
- NotiSend verifies `news.kenigevents.ru` and the
  `events@news.kenigevents.ru` sender. The account remains on the free 200-contact
  plan and API activation is complete (`200` total, `200` available). No contacts
  were imported and no campaign was sent. A diagnostic exposed the current key in
  internal tool output; because the panel has no documented rotation control,
  the recommended remediation remains support-led revoke/reissue followed by a
  Lockbox update. On 2026-07-12 the owner explicitly deferred that rotation and
  accepted the temporary risk; this does not waive the gate, so no recommendation
  canary or application enablement is allowed with the exposed key.
- The live Supabase control plane has a 200-user admission capacity with zero
  active recommendation users. Global, transactional and recommendation switches
  are off and `dry_run_only` is on.

## Address and DNS contract

- Authoritative DNS remains in Yandex Cloud DNS; do not move nameservers to SpaceWeb or NotiSend.
- `info@kenigevents.ru` is the human mailbox and Reply-To address.
- `dmarc@kenigevents.ru` receives aggregate DMARC reports and is not forwarded to Mail Trigger.
- The root domain has one combined SPF policy only; never publish multiple `v=spf1` records for the same name.
- SpaceWeb, Postbox and NotiSend DKIM selectors/verification records must be copied from the current provider control plane, not guessed from examples.
- Start DMARC in monitoring mode (`p=none`); move to `quarantine`/`reject` only after aligned traffic and aggregate reports are reviewed.
- Keep recommendation mail on the independently verifiable `news.kenigevents.ru` identity so reputation and policy can be measured separately from human/transactional mail.

Before a DNS change, store a sanitized before-snapshot in ignored `artifacts/codex/<task>/`; verify existing website/CDN/certificate records are unchanged. Never commit mailbox passwords, API keys, SMTP credentials or provider tokens.

## Inbound flow

1. Internet mail is delivered by SpaceWeb MX and retained in `info@kenigevents.ru`.
2. SpaceWeb exposes mailbox destination modes (`Mail`, `Forwarding`, `Distribution`)
   as mutually exclusive settings. Switching `info@` to `Forwarding` would remove
   the retained mailbox contract, so production deliberately keeps `Mail` and does
   **not** enable panel forwarding.
3. `kenigevents-email-imap-poll` invokes a read-only Python 3.12 IMAP collector every
   two minutes. It uses `BODY.PEEK[]`, never changes `Seen`, and advances a private
   UIDVALIDITY/UID cursor only after the envelope and queue pointer are stored.
4. Intake assigns a keyed-HMAC idempotency key, allowlists headers and stores a
   deterministic normalized envelope in a dedicated private KMS-encrypted bucket
   with 30-day retention. The SpaceWeb message remains the authoritative original,
   including attachments; the IMAP copy does not execute or expose attachments.
5. A small metadata/reference pointer passes through a standard YMQ queue. A
   delivery Function calls the public Yandex adapter with a timestamped HMAC; the
   adapter writes only an idempotent metadata receipt through a service-only
   Supabase RPC. Invalid/expired signatures fail before any Supabase request.
6. The direct Mail Trigger address remains active for isolated trigger and attachment
   canaries. Its normalized-envelope limit is 220 KB below the current 230 KB trigger
   limit; it is not configured as the SpaceWeb mailbox destination.
7. Processing retries use the YMQ redrive policy (five receives) and a dedicated
   processing DLQ. Direct Mail Trigger failures use a separate intake DLQ.

The current Cloud Functions trigger-message limit is 230 KB including service metadata. Intake therefore caps the trigger body at 220 KB and never copies it into YMQ; large-message behavior must be proven by live canary, while the SpaceWeb mailbox remains the loss-prevention fallback.

Loop guards are mandatory before any automatic response. Do not Bcc automated outbound mail to `info@kenigevents.ru`, and do not forward `dmarc@kenigevents.ru` into Mail Trigger.

## Outbound streams

### Transactional through Postbox

Examples include registration/address confirmation where the application owns that flow, preference/account changes, save/follow confirmation, reminder, cancellation and material reschedule notices. The server must derive current event/account facts and recheck the transactional send guard immediately before Postbox claim.

Transactional consent/legitimate-trigger rules are distinct from recommendation consent. A favorite, calendar save, auth session or previous transactional delivery never opts a user into recommendations.

Postbox notifications are QoS 1 / at-least-once. The consumer deduplicates by the
stable provider `eventId`; it never trusts a caller-provided suppression identity.
The event must match the pinned Postbox identity, configuration ID, From domain,
exact provider message ID and versioned recipient HMAC derived from the database
identity. `hard_bounce` and `complaint` suppress all mail, while a Postbox
one-click `Subscription` event suppresses only the transactional stream.

The provider documentation is currently inconsistent: the notification schema
includes `Complaint` and `Rendering Failure`, while the create-destination API
enumerates neither. Provisioning must probe the complete required set on a disabled
destination. If the provider rejects it, leave application transactional sending
disabled and record a provider blocker rather than silently claiming complete
feedback coverage from a reduced event list.

### Recommendations through NotiSend

Each logical recommendation issue contains **exactly three email events**; a hero is one of the three, never a fourth. The linked personal page may contain a larger ranked set, but it must already be published and validated before the issue becomes sendable.

The initial service has a hard ceiling of **200 actively consented recommendation users**:

1. Supabase transactionally admits a new active recommendation subscription only below 200.
2. At capacity, fail closed: do not mark the user active, synchronize a sendable provider contact or enqueue a message.
3. Before every build and final send claim, recheck verified identity, purpose-specific consent, active admission, suppression and the ceiling.
4. The usable canary may be lower if the current NotiSend plan counts seed/service contacts or imposes a lower operational limit.
5. Provider capacity/error responses are defense in depth, not the primary admission lock. Never change tariff or spill excess recipients to Postbox automatically.

## Mandatory production gates

### All outbound mail

- verified provider identity;
- SPF, DKIM and DMARC alignment for every From domain/subdomain;
- documented From, Reply-To and template version;
- warm-up and current provider/domain rate limits;
- durable Supabase outbox with atomic idempotency/send guard and bounded retry;
- authenticated callback/event ingestion and callback deduplication;
- immediate hard-bounce, complaint and unsubscribe suppression;
- documented soft-bounce/deferred threshold;
- delivery/failure/bounce/complaint/lag dashboards and alerts without plaintext recipient leakage;
- global and per-stream kill switches;
- no real user send before dry-run, seed-list canary and operator review.

### Recommendation-only

- explicit verified recommendation consent and active admission within the 200-user ceiling;
- one-click unsubscribe plus purpose-specific preference pause/unsubscribe;
- exactly three current, sendable events in the email;
- personal page published, reachable, `noindex` and validated before send;
- NotiSend domain/API/callback contract proven on a seed audience;
- no Postbox fallback.

### Inbound-only

- SpaceWeb webmail and encrypted IMAP/SMTP access proven;
- original retained and still unread after the read-only IMAP copy;
- IMAP UID cursor/idempotency plus direct Mail Trigger schema validation;
- private storage and lifecycle policy;
- idempotency under trigger retry/duplicate delivery;
- DLQ failure and controlled replay test;
- auto-reply/Bcc loop prevention.
- supported `python312` runtime, production function tags instead of `$latest`, and an isolated Yandex folder so any queue-trigger permission fallback cannot reach site/CDN resources.

## Live E2E and debugging order

Use a unique correlation marker such as `KE-MAIL-E2E-<UTC timestamp>-<random>` and verify one boundary at a time:

1. authoritative/public MX, SPF, DKIM and DMARC resolution;
2. SpaceWeb TLS webmail/IMAP/SMTP;
3. inbound test sent **from the existing `info@kgd80.ru` Postbox identity** to `info@kenigevents.ru` without modifying or deleting kgd80.ru resources;
4. one retained/unseen SpaceWeb mailbox copy and one IMAP collector invocation;
5. private attachment/object references, normalized envelope and one idempotent backend result;
6. forced test-only handler failure → bounded retry → DLQ → controlled replay without a duplicate business event;
7. Postbox transactional seed message, real provider message id and delivery event;
8. NotiSend seed issue with exactly three events and an already published personal page;
9. unsubscribe/hard-bounce/complaint fixtures update Supabase suppression and block a second claim;
10. kill-switch exercise for each outbound stream.

Store only redacted evidence under ignored `artifacts/codex/<task>/`. Correlate by run id, outbox id and provider message id; do not grep or report plaintext recipient addresses when a keyed identifier suffices.

## Safety invariants

- Never send to an unverified, unsubscribed, non-admitted or suppressed recipient.
- Never exceed 200 actively consented recommendation users at launch.
- Never infer recommendation consent from auth presence, calendar save, favorite or prior transactional mail.
- A personal page must be published and validated before its email becomes sendable.
- Sender retries never create a second logical message for the same idempotency key.
- YDB analytics failure never blocks or duplicates a send.
- Provider callback delay cannot make a known suppression disappear.
- Inbound processing failure never removes the retained human-mailbox copy.
- Provider or mailbox secrets never enter Git, application logs, test output or operator reports.

## Required evidence per canary

- sender identity and DNS verification;
- exact stream/provider, From/Reply-To and template version;
- recipient eligibility, active-admission and consent proof;
- outbox/send-guard record;
- provider message id and callback state;
- for recommendations, the exactly-three-event assertion and published-page check;
- unsubscribe/bounce/complaint suppression tests;
- aggregate delivery dashboard without plaintext recipient leakage;
- rollback/kill-switch test.
