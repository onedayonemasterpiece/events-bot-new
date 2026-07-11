# Email infrastructure, delivery and deliverability

> Status: accepted release architecture; provider provisioning and production sending remain gated.

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
| Automated inbound copy | Yandex Cloud Mail Trigger → Function/Container | Receives a forwarded copy without deleting the SpaceWeb original; private attachments, bounded retry and DLQ. |
| Transactional outbound | Yandex Cloud Postbox | Critical account and saved/followed-event lifecycle messages only; intended From `Kenig Events <notify@kenigevents.ru>`, `Reply-To: info@kenigevents.ru`. |
| Recommendation outbound | NotiSend | Opt-in personal recommendations/announcements only; intended From `Kenig Events <events@news.kenigevents.ru>`, `Reply-To: info@kenigevents.ru`. |
| Identity and send control | Personalization Supabase/Postgres | Verified email, purpose-specific consent/subscription, hard admission cap, preferences, suppression, outbox, send guard and provider evidence. |
| Analytics projection | YDB | De-identified asynchronous delivery/product aggregates only; never send eligibility or outbox state. |
| Published personal artifacts | Object Storage/CDN | Rendered personal HTML/JSON after validation; never recipient/control state. |

Providers are not interchangeable. Postbox must not be used as a hidden fallback for recommendations, and NotiSend must not send critical transactional messages. A provider contact/list is a projection from Supabase, not evidence of consent.

See [personalization data ownership](../architecture/personalization-data-ownership.md).

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
2. SpaceWeb forwards a copy to the technical address assigned by Yandex Cloud Mail Trigger. The original remains readable through SpaceWeb webmail/IMAP.
3. A minimal Python 3.12 Function validates the trigger envelope, assigns a keyed-HMAC correlation/idempotency key, allowlists headers and stores deterministic normalized JSON plus Yandex-managed attachment references in a dedicated private KMS-encrypted bucket with 30-day retention.
4. Yandex Mail Trigger exposes normalized headers/body and attachment object keys, not raw MIME. The retained SpaceWeb mailbox copy remains the authoritative original; if exact raw MIME processing becomes mandatory, this pipeline must be replaced or supplemented by an authenticated SpaceWeb IMAP puller.
5. The intake Function passes only a small metadata/reference pointer through a standard YMQ queue. A separate delivery Function sends the minimized receipt to the existing backend over HTTPS with timestamped HMAC authentication; plaintext addresses, subject, body and attachment keys never enter YMQ or ordinary logs.
6. Native Mail Trigger failure and processing failure use separate DLQs because their payloads require different replay logic. Retries are bounded, and replay must preserve the same idempotency key.

The current Cloud Functions trigger-message limit is 230 KB including service metadata. Intake therefore caps the trigger body at 220 KB and never copies it into YMQ; large-message behavior must be proven by live canary, while the SpaceWeb mailbox remains the loss-prevention fallback.

Loop guards are mandatory before any automatic response. Do not Bcc automated outbound mail to `info@kenigevents.ru`, and do not forward `dmarc@kenigevents.ru` into Mail Trigger.

## Outbound streams

### Transactional through Postbox

Examples include registration/address confirmation where the application owns that flow, preference/account changes, save/follow confirmation, reminder, cancellation and material reschedule notices. The server must derive current event/account facts and recheck the transactional send guard immediately before Postbox claim.

Transactional consent/legitimate-trigger rules are distinct from recommendation consent. A favorite, calendar save, auth session or previous transactional delivery never opts a user into recommendations.

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
- original retained after forwarding;
- Mail Trigger authentication/schema validation;
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
4. one retained SpaceWeb mailbox copy and one Mail Trigger invocation;
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
