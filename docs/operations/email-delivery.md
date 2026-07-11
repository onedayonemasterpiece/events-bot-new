# Email delivery and deliverability

> Status: release contract; production sending remains gated.

## Scope

Shared operational contract for:

- personal recommendation emails;
- transactional calendar/follow confirmations, reminders, cancellation and reschedule notices.

Product content, consent purpose and cadence remain separate. Delivery infrastructure, sender reputation, suppression and provider evidence are shared.

## Ownership

- Supabase/Postgres owns verified email, purpose-specific subscription/consent, preferences, suppressions, outbox, send guard, rate state and provider delivery evidence.
- YDB receives de-identified operational/product aggregates asynchronously; it is not send eligibility or outbox state.
- Yandex Cloud Postbox is the sender transport.
- Object Storage/CDN owns referenced public/personal artifacts, never recipient/control state.

See [personalization data ownership](../architecture/personalization-data-ownership.md).

## Stream separation

At minimum configure two sender streams/identities:

1. **Transactional:** user-requested calendar/follow lifecycle messages.
2. **Recommendations:** opt-in personal announcement/digest messages.

The streams require separate purpose consent, rate/fatigue policy and preferably separate subdomains or otherwise independently measurable sender identities. A calendar save is not recommendation-email consent.

## Mandatory production gates

- verified Postbox identity;
- SPF, DKIM and DMARC alignment for every From domain/subdomain;
- documented Reply-To and unsubscribe behavior;
- warm-up and provider/domain rate limits;
- durable outbox with idempotency/send guard and bounded retry;
- signed/provider-authenticated callback ingestion;
- deduplication of callback events;
- immediate hard-bounce, complaint and unsubscribe suppression;
- documented soft-bounce/deferred threshold;
- one-click unsubscribe for recommendation mail;
- preference center and purpose-specific pause/unsubscribe;
- delivery/failure/bounce/complaint/lag dashboards and alerts;
- global and per-stream kill switches;
- no real send before dry-run, seed-list canary and operator review pass.

## Safety invariants

- Never send to unverified, unsubscribed or suppressed recipients.
- Never infer email consent from auth presence, calendar save, favorite or previous transactional mail.
- A personal page must be published and validated before its email becomes sendable.
- Sender retries never create a second logical message for the same idempotency key.
- YDB analytics failure never blocks or duplicates a send.
- Provider callback delay cannot make a known suppression disappear.

## Required evidence per canary

- sender identity/DNS verification;
- exact stream, From/Reply-To and template version;
- recipient eligibility and consent proof;
- outbox/send-guard record;
- provider message id and callback state;
- unsubscribe/bounce/complaint suppression tests;
- aggregate delivery dashboard without plaintext recipient leakage;
- rollback/kill-switch test.
