# VK identity linking through a verification message

> Status: **post-release design / not implemented**. This is not an F1–F17 first-release blocker and must remain disabled until the legal texts, dedicated VK inbox and security canary are accepted.

## Decision

The proposed two-channel flow is technically workable:

1. an authenticated KenigEvents user starts VK linking on the static site;
2. the backend creates a short-lived one-time challenge;
3. the page shows a numeric code and a link to a dedicated VK inbox;
4. the user sends the exact code from their VK account;
5. a server-side VK message worker obtains the sender's VK ID, atomically consumes the challenge and links that VK identity to the current site subject in the Russian YDB personal-data contour;
6. the worker calls `friends.get` for the verified sender and intersects the currently public friend IDs with other opted-in linked identities;
7. the site may use only the permitted intersection to build friend-interest signals.

The official VK API schema declares `messages.getLongPollServer`, `messages.getConversations` and `messages.getHistory` for user/group access tokens, and `friends.get` for user/service tokens:

- <https://github.com/VKCOM/vk-api-schema/blob/master/messages/methods.json>
- <https://github.com/VKCOM/vk-api-schema/blob/master/friends/methods.json>

A read-only local capability probe on 2026-07-13 confirmed that two existing user-token lanes could call `messages.getLongPollServer` and `messages.getConversations`. No message bodies, peer IDs or profile data were printed or persisted. This proves the API path, but not that the eventual target profile accepts messages from strangers; that needs a separate live canary after the dedicated inbox is chosen.

## Three digits are not an acceptable production secret

A global three-digit code has only `1,000` values. An unrelated VK user can guess another user's active challenge and bind the wrong VK account before the intended sender. Binding the challenge to an authenticated site session does not remove that race because the sender is authenticated only after the message arrives.

Production requirement:

- use a cryptographically random **six-digit** code (`000000`–`999999`) at minimum;
- only one active challenge per site user;
- active codes must be unique;
- TTL: five minutes;
- one-time atomic consume;
- store only a keyed code hash, never the code itself;
- invalidate all older challenges when a new one is requested;
- rate-limit creation by user and IP, and invalid attempts by VK sender plus a global abuse guard;
- fail closed on an already-linked VK ID, an already-consumed code or concurrent consumption;
- never support operator/manual linking merely from a screenshot of the message.

For UX the page should provide `Скопировать код`, `Открыть VK` and a live `Ожидаем сообщение…` status. A prefilled-message deep link may be used only after a live desktop/mobile VK canary proves it; copying the exact code remains the fallback.

## Preferred inbox

Preferred production order:

1. a dedicated VK community inbox with a community token and Callback/Long Poll;
2. a dedicated service-only VK user profile with a user token that has messages access;
3. a shared human profile only for a bounded prototype, never as the default production inbox.

A personal-profile token can technically read the inbox, but a shared profile exposes unrelated correspondence to the integration and is operationally coupled to a human session. The worker must not scrape the VK web UI. It must process VK API events, accept only new incoming one-to-one messages matching the strict challenge syntax and discard unrelated text without persistence.

Before activation the chosen inbox must pass:

- a stranger-can-send desktop and mobile canary;
- sender VK ID correctness;
- duplicate/replayed event idempotency;
- token/session restart and revocation recovery;
- no outbound reply unless a separately reviewed acknowledgement is enabled;
- documented ownership and secret rotation.

## End-to-end protocol

### 1. Start

`POST /api/v1/identity/vk-link/challenges` requires a valid site session, current VK-link consent and accepted current legal-document versions. The same-origin backend, not the browser, resolves that session to the YDB-owned subject. A Supabase JWT may be accepted only as a transitional authentication assertion after the separate 152-FZ localization review proves that the account-identity flow is permissible; it is not the VK personal-data store. The backend returns:

```json
{
  "challengeId": "opaque-public-id",
  "displayCode": "482731",
  "expiresAt": "...",
  "messageUrl": "configured-vk-inbox-url"
}
```

The static page is only the UI. Challenge creation, message reception and binding are dynamic server operations.

### 2. Receive and consume

The Fly worker receives a new incoming VK message and:

1. rejects chats, outgoing messages, edited historical messages and non-matching text;
2. normalizes only the strict `KE <six digits>` syntax;
3. looks up the active keyed code hash;
4. atomically changes `pending → consumed` and creates one external-identity link;
5. stores minimal deduplication metadata, not the message body;
6. exposes success to the site status endpoint.

There must be a unique one-to-one relation between an active VK identity and an active YDB-owned site subject. Relinking an identity owned by another account requires an explicit recovery/manual-review flow; it must never silently merge accounts.

### 3. Fetch friends

After successful binding:

- call `friends.get(user_id=<verified sender>)`;
- treat private/deleted/unavailable profiles as an expected incomplete result;
- do not promise that the public list is complete or current;
- immediately intersect returned IDs against the keyed IDs of currently linked, opted-in users;
- do not persist the full friend list or IDs of unregistered/non-consenting people;
- persist only eligible pair edges, their observation time and refresh status;
- refresh with a bounded TTL and remove stale edges when no longer observed or when either user withdraws consent.

The initial research probe found that public friend lists are practically usable but incomplete: `144/165` sampled VK actors were readable and `21/165` were private/deleted; therefore absence of an edge is never proof that two people are not friends.

### 4. Produce friend signals

Friend signals require two independent permissions:

- viewer: `использовать активность друзей для моих рекомендаций`;
- actor/friend: `показывать мою активность подтверждённым друзьям`.

Without both permissions no named or aggregate friend signal is returned. The first product slice should return only a count such as `2 друга заинтересовались`. A name/avatar is a later separately approved capability.

Valid action semantics:

- VK like/comment/repost mapped to a canonical event;
- explicit KenigEvents like/favorite;
- explicit `Планирую пойти` state;
- a KGD80 registration linked to the same verified VK identity, if the cross-system consent permits it.

Downloading an ICS file is not equivalent to `пойдёт` and must not be described that way.

## Data ownership and minimization

For this feature, **Managed Service for YDB in Yandex Cloud `ru-central1` is the system of record for the personal-data contour**: site subject mapping, VK identity, purpose consent, challenge state, eligible friend edges, withdrawal and deletion audit. Fly may host the background VK receiver, but it is a stateless/service layer and must not place this graph in core SQLite. Supabase/Postgres may receive only genuinely de-identified aggregate event metrics with no stable site/VK/friend subject key; it must not own or mirror the VK link or graph.

KGD80 may project social actions through a service-only, purpose-limited YDB bridge using a new dedicated keyed external-identity identifier. It must not share raw email/FIO or reuse the registration `PII_FINGERPRINT_SECRET` as the cross-system key.

A read-only infrastructure check on 2026-07-13 found an existing running serverless YDB database in `ru-central1`, but its current tables belong to acquisition/Region Talk state and contain no site-identity/VK privacy schema. Its existence proves the Russian YDB contour is available; it does **not** authorize mixing personal data into the existing analytics tables. Before implementation, create an isolated personal-data database or a separately permissioned namespace with dedicated service accounts, KMS/secret policy, Audit Trails and approved retention.

Minimum private entities:

```text
vk_link_challenge(
  id, subject_id, code_hmac, state, expires_at,
  failed_attempts, created_at, consumed_at
)

user_external_identity(
  subject_id, provider='vk', provider_subject_ciphertext,
  provider_subject_hmac, linked_at, last_verified_at,
  consent_version, sharing_state, revoked_at
)

vk_friend_edge(
  identity_low_id, identity_high_id,
  observed_at, refresh_after, visibility_state
)
```

Security boundary:

- provider subject lookup uses a dedicated keyed HMAC; reversible VK ID, if operationally required, is encrypted server-side;
- the browser has no direct YDB credentials and cannot read raw identity links, challenges or edges;
- a same-origin service authorizes the session and returns only the current viewer's allowed aggregate signal;
- challenge/message content and complete public friend lists are not retained;
- consent evidence is versioned and auditable;
- unlink/account deletion removes the active identity and incident edges and creates the required downstream purge request;
- retention for consent audit, security evidence and irreversibly anonymized aggregates must be approved before implementation.

## Legal-document requirement

The product cannot truthfully claim “personal data is not stored, only anonymized information”. A VK ID linked to a site subject and a recoverable friendship edge identify or indirectly identify people. Encryption, HMAC and pseudonymous internal IDs reduce exposure but do not make the live link anonymous. The legal review must use the current definitions and purpose/minimization/consent requirements of Federal Law No. 152-FZ rather than treating technical pseudonymization as irreversible anonymization: <https://ips.pravo.gov.ru/api/ips/legislation/document?baseid=None&hash=98490812b3409e2a8d78a11ca9010f434ea3d9250a11dbbdb78690cd5551bdd6>.

YDB is the correct localization/technical protection contour, but using YDB alone does not complete compliance. The operator still owns purpose/legal-basis documentation, notification and organizational measures, access model, threat/protection model, processor terms, retention, incident response, subject requests and any cross-border transfer assessment. Yandex Cloud documents that its platform supports 152-FZ/УЗ-1 controls and that the customer remains the personal-data operator commissioning processing: <https://yandex.cloud/ru/docs/security/conform>.

Before the first canary the static site needs three separate surfaces:

1. **Personal-data processing/privacy policy** — operator/contact, purposes, data categories, sources (site and VK public API), operations, processors, cross-system KGD80 projection, retention, protection, withdrawal, deletion and complaint/contact path.
2. **Purpose-specific VK-link consent** — unchecked/explicit action, exact policy and consent version/hash/time, permission to receive the verification message, store the VK identity link, query the public friend list, retain only opted-in intersections and use friend activity for personalization. It must be independently revocable.
3. **User agreement** — service rules, voluntary linking, no guarantee of VK availability/completeness, acceptable use, account/link recovery, user controls, service suspension/change and contact/dispute terms.

Recommendation email remains a separate purpose-specific opt-in. Accepting the user agreement, linking VK, saving an event or registering with KGD80 must not subscribe a person to recommendation mail.

Draft legal texts must be reviewed by the product owner/legal reviewer. The current KGD80 registration consent covers registration purposes and must not be reused as evidence for this post-release purpose.

## Abuse and failure cases

- guessed code: six-digit entropy, TTL, attempt/rate limits, atomic consume;
- forwarded/screenshot code: possession alone is insufficient after consumption; never manually bind from a screenshot;
- two simultaneous senders: one database transaction wins, the other gets a generic failure;
- one VK ID linked elsewhere: fail closed and open recovery;
- private friends list: link succeeds, friend graph stays unavailable;
- VK API/token outage: existing site identity and static pages remain usable; retry with backoff;
- personal inbox contamination: strict new-event filter and no unrelated-message persistence;
- consent withdrawn: stop refresh/use, remove active edges and expose unlink/delete status;
- friend removes relationship: edge expires and is removed on refresh;
- VK public actions cannot be mapped to a canonical event: retain no product friend signal until mapping exists.

## Post-release delivery slices

1. Approve policy, VK consent, user agreement, retention and product copy.
2. Choose a dedicated VK inbox and complete read-only plus one controlled live-message canary.
3. Provision an isolated YDB personal-data namespace, IAM/KMS/audit boundary, atomic challenge queries and deletion flow.
4. Add Fly VK Long Poll/Callback receiver with idempotency and abuse controls.
5. Launch account-only VK linking without friends or event signals.
6. Add transient `friends.get` intersection and aggregate-only double-opt-in signals.
7. Map KGD80/VK/site event actions to canonical event IDs.
8. Add friend-aware ranking; only later evaluate friend-aware recommendation email under its separate email consent.

## Acceptance gates

- no three-digit production challenges;
- no raw message-body or full friend-list retention;
- personal data exists only in the isolated YDB personal-data contour; none in Supabase, core Fly SQLite, logs, YDB analytics tables or static artifacts;
- strict one-to-one/recovery semantics and concurrent-consume test;
- IAM/service-authorization negative tests for challenges, identities and edges, including proof that browsers cannot connect directly to YDB;
- public/private/deleted VK profile cases tested;
- unlink, consent withdrawal and account deletion verified end to end;
- aggregate friend signal requires both permission flags;
- static-site fallback works during VK/YDB/auth-service outage;
- legal-document versions and retention are owner-approved;
- production remains disabled until a canary report is attached.

## Related documentation

- [Site user identity](README.md)
- [Personalization data ownership](../../architecture/personalization-data-ownership.md)
- [Favorites and calendar](../event-favorites-calendar/README.md)
- [Personal email announcements](../personal-email-announcements/README.md)
- [Unsigned personalization](../unsigned-personalization/README.md)
