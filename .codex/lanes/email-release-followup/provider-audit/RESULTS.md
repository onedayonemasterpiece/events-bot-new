# Email release follow-up: provider audit

Date: 2026-07-12 UTC
Lane: `provider-audit`
Requirements: R01, R02
Branch: `agent/email-release-followup/provider-audit`
Base: `origin/main@d09948130e26bea9f2294248f0b987940bc5b869`

## Outcome

| Requirement | Status | Result |
|---|---|---|
| R01 — NotiSend API activation | **Provider activation done; security reissue required before enablement** | The API is active on the unchanged free 200-subscriber plan. Activation itself requires no tariff change or further approval. The current key authenticated a read-only API call, but it was accidentally printed once in this audit's internal tool output; recommendation outbound must remain disabled until NotiSend support reissues it and Lockbox is updated. |
| R02 — Postbox production readiness | **Provider send path works; application production gate remains** | Identity/configuration/DKIM are healthy and a real message was delivered. The Spam placement is not an SPF/DKIM/DMARC failure. There is no event destination, YDS stream, or event consumer, so the safe closure is an atomic consumer + stream + destination follow-up rather than a provider-only half-configuration. |

No mass send, contact import, tariff change, DNS change, application-switch change, or real recipient send was performed in this lane.

## R01 — NotiSend live evidence

### Activation state

- The authenticated panel exposes `API и SMTP` and `Параметры подключения API и SMTP`.
- The panel key exists and matched the key stored in deletion-protected Lockbox secret `kenigevents-notisend-api` before the security issue below.
- No active `Ожидайте получения api ключа` banner or API/SMTP send-restriction warning was present.
- Authenticated, non-mutating `GET /v1/email/balance` returned HTTP `200` with subscriber capacity `total=200`, `available=200`, and no API errors.
- Authenticated `GET /v1/email/messages/<nonexistent-probe-id>` returned `404`, not `401/403`, independently proving that bearer authentication is accepted without creating a message.
- The panel still reports the free Starter capacity: 200 total, zero used. No contact was added.

The prior “wait for API key” state therefore cleared asynchronously. The account owner does **not** need to pay, change tariff, repeat activation, import contacts, or open an activation ticket.

Official provider contract checked: <https://notisend.ru/dev/email/api/>. It documents bearer authentication, `GET /email/balance`, the API/SMTP panel location, and that the key must remain secret.

### Security exception and exact action

A first diagnostic body dump accidentally included the current 32-character API key in internal tool output. Later probes redacted it, but this is still an exposure. The panel and public API documentation expose no rotate/regenerate/revoke control, so guessing an undocumented mutation would be unsafe.

Before recommendation outbound is enabled:

1. Ask NotiSend support (`support@notisend.ru`, authenticated-account context) to revoke and reissue the API key. This is a key-security request, **not** tariff activation.
2. Replace only the `api_key` entry in a new version of deletion-protected Lockbox secret `kenigevents-notisend-api`; preserve the base URL/account/sender entries.
3. Verify the new key with `GET /v1/email/balance` (`200`, total 200) and verify the old key no longer authenticates.
4. Keep all NotiSend/application switches off until that smoke passes.

No support request was opened automatically because an asynchronous provider rotation could invalidate the live Lockbox value without a coordinated secret update.

## R02 — Postbox live evidence

### Identity and configuration

Read-only AWS-compatible Postbox API audit using a temporary `postbox.editor` access key showed:

- identity `kenigevents.ru`: domain identity, sending enabled and verified for sending;
- EXTERNAL BYODKIM: signing enabled, selector `postbox2026`, status `SUCCESS`;
- configuration `kenigevents-transactional` is bound to the identity;
- delivery policy is `TLS REQUIRE`;
- `GetConfigurationSetEventDestinations` has no destinations.

Every temporary admin access key was deleted in a `finally` cleanup. Post-audit key count for `kenigevents-postbox-admin` is zero.

Public DNS still has one combined SPF record including SpaceWeb and Postbox, the `postbox2026` DKIM public key, and monitoring DMARC (`p=none`). No DNS mutation was made.

### Why the seed was routed to Spam

The retained seed was fetched read-only from SpaceWeb `Spam` with `BODY.PEEK[HEADER]`; it remained unmodified. Contrary to the earlier truncated observation, it contains **two** DKIM signatures:

1. `d=postbox.yandexcloud.net; s=sel`;
2. `d=kenigevents.ru; s=postbox2026`.

SpaceWeb/Kaspersky authentication evidence records:

- `dmarc=pass header.from=kenigevents.ru`;
- `spf=pass smtp.mailfrom=postbox.yandexcloud.net`;
- `dkim=pass` for both Postbox and `kenigevents.ru` signatures;
- anti-spam status `not_detected`, rate `0`, method `none`;
- nevertheless, the routing header is `X-IS-Spam: 1`.

Therefore:

- the seed did **not** land in Spam because BYODKIM was absent or broken;
- SPF, both DKIM signatures, and DMARC passed at the receiving provider;
- the receiving classifier did not expose a deterministic rejection reason. Its diagnostic hints include a fresh sender/reputation context and envelope-domain/header-domain tracking mismatch (`postbox.yandexcloud.net` return path versus `kenigevents.ru` From), but these are not proof of a single root cause;
- one self-domain seed at a new domain is insufficient to claim general Postbox deliverability failure.

Postbox transport is operational. The phrase “production-send forbidden” describes the project's fail-closed release policy, not a provider-side inability to send.

### Event destination decision

Official Yandex documentation says a Postbox configuration subscription targets a Yandex Data Streams stream and emits delivery/bounce/complaint/unsubscribe events in JSON with provider `eventId`:

- <https://yandex.cloud/en/docs/postbox/operations/create-configuration>
- <https://yandex.cloud/en/docs/postbox/concepts/notification>
- <https://yandex.cloud/en/docs/postbox/aws-compatible-api/api-ref/get-configuration-set-event-destinations>

The isolated email folder currently has:

- zero YDB databases / Data Streams streams;
- no YDS/YDB IAM bindings;
- no deployed Postbox event consumer;
- no configuration event destination.

Creating only a stream and destination would accumulate events without applying bounce/complaint suppression, deduplication, or alerts. It would not satisfy the documented production gates and would add chargeable state with no owned consumer. This lane therefore deliberately made no half-configuration.

Safe implementation order:

1. Implement and test an IAM-authenticated Postbox event consumer that validates the documented schema, deduplicates by `eventId`, correlates provider `MessageId`, and calls the existing service-only Supabase provider-event boundary without logging plaintext recipients.
2. Provision a dedicated low-retention YDS stream and least-privilege consumer identity in `kenigevents-email-prod`.
3. Attach an enabled event destination to `kenigevents-transactional` for send/delivery/delay/bounce/complaint/unsubscribe events.
4. Prove delivery-event ingestion, duplicate-event idempotency, hard-bounce/complaint suppression, consumer lag alerting, DLQ/replay, and kill switch on a controlled seed audience.
5. Run a small cross-provider inbox-placement canary and inspect full authentication headers before any gradual warm-up. Do not infer global reputation from the single SpaceWeb self-domain seed.

Only after those checks should the transactional application switch move from dry-run to a bounded canary.

## Commands and safety checks

Redacted checks performed:

- NotiSend headless authenticated panel inspection; all subsequent outputs masked key-like values.
- NotiSend authenticated `GET /email/balance` and nonexistent-message `GET`; no API mutation.
- `aws sesv2 list-email-identities`, `list-configuration-sets`, `get-configuration-set`, `get-email-identity`, and `get-configuration-set-event-destinations` against `https://postbox.cloud.yandex.net` using fail-closed temporary credentials.
- `yc iam access-key list` after both audits: admin key count `0`; sender key count stayed `0`.
- SpaceWeb encrypted IMAP read-only header fetch with `BODY.PEEK[HEADER]`.
- Public `dig` for root SPF, Postbox DKIM, and DMARC.
- `yc ydb database list` in the isolated email folder: empty.

No credential value, mailbox body, contact list, or raw provider request payload is committed here.
