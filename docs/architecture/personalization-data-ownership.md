# Personalization data ownership

> Status: **accepted release architecture with a post-release VK/152-FZ correction** (2026-07-13). Existing Supabase account/email storage still requires a separate localization/data-flow audit before it may be represented as 152-FZ compliant.
> Scope: static-site identity, personalization, favorites/calendar, email recommendations, transactional event email, analytics and comment-feedback sidecars.

## Decision

The project has one current user/profile and email control-plane system of record:

- **Fly SQLite `/data/db.sqlite`** owns canonical events, sources, lifecycle, publication state, event-bound scheduler/outbox state and static-page generation metadata tied to an event.
- **Personalization Supabase/Postgres** owns identity, consent, durable user/profile state, favorites/follows, subscriptions, email send-control state, active recommendation issues and personal-page token metadata.
- **YDB** owns service-only high-volume/history/analytics projections and the independent event-comment-feedback sidecar. YDB is **not** a second user-profile or email-control-plane owner.
- **An isolated YDB personal-data contour in `ru-central1`** owns the post-release VK proof-of-control link, VK-purpose consent and eligible friend edges. It is separated by database/namespace and IAM from YDB analytics; this is a narrow compliance vault, not a competing personalization profile.
- **Object Storage/CDN** owns generated HTML/JSON/media artifacts. It does not own consent, profile, subscription, send state or token validity.

Email providers are transports and ingress surfaces, not additional systems of record:

- **SpaceWeb** owns the durable human/inbound mailbox `info@kenigevents.ru` and manual webmail/IMAP/SMTP access.
- **Yandex serverless inbound pipeline** polls the retained SpaceWeb mailbox read-only by UID; its direct Mail Trigger address is canary-only. Functions, private storage and DLQs do not become identity, consent or subscription owners.
- **Yandex Cloud Postbox** sends transactional account/event-lifecycle mail and
  remains the capacity route for first/new or over-capacity Auth recipients.
- **NotiSend** sends personal recommendations/announcements plus the narrow
  returning/repeated/fixed-test Auth route. Supabase enforces one shared hard
  ceiling of 200 unique NotiSend recipients.

This decision follows the implementation already present in `origin/main`: Supabase Auth/Yandex, pgvector search, reaction counters and the personalization project boundary. There is no production YDB user-profile write path to migrate.

## Entity ownership

| Entity | Canonical owner | Allowed projection |
|---|---|---|
| Yandex identity and sessions | Supabase Auth | Opaque/HMAC subject id in YDB analytics |
| Verified email-only identity | Supabase Auth email OTP/magic-link | Keyed email HMAC in YDB analytics |
| Post-release verified VK identity/link consent | Isolated YDB personal-data contour (`ru-central1`) | De-identified aggregate only; no stable subject/VK key in Supabase/YDB analytics |
| Post-release eligible VK friend edges | Isolated YDB personal-data contour (`ru-central1`) | Aggregate friend-signal metric through an authorized same-origin service; never the full public friend list |
| Anonymous device/identity link | Private Supabase schema after server materialization | HMAC subject id in YDB |
| Current anonymous/auth profile | Private Supabase schema | Sanitized immutable Kaggle export; de-identified YDB analytics |
| Browser `localStorage` profile | Local cache/offline projection, never SOR | None |
| Profile merge/link audit | Supabase | De-identified audit projection in YDB |
| Consent and consent evidence | Supabase | Aggregate consent metrics in YDB |
| Favorites, event follows and calendar-save state | Supabase | Aggregate event/action metrics in YDB |
| Transactional/recommendation subscriptions | Supabase, with separate purposes | Aggregate subscription metrics in YDB |
| Verified email, preferences and suppressions | Supabase | Keyed HMAC and aggregates only in YDB |
| Recommendation issue/cards | Supabase | De-identified issue metrics in YDB |
| Personal-page token hashes/metadata | Supabase | Rendered artifact in Object Storage/CDN |
| Email outbox, send guard, rate state | Supabase | Terminal/aggregate delivery projection in YDB |
| Provider delivery events | Supabase for send-critical evidence | Append-only analytics projection in YDB |
| Human mailbox and retained inbound correspondence | SpaceWeb | Read-only UID copy through Yandex Functions; direct Mail Trigger canary only |
| Normalized inbound trigger payload/attachments | Private Yandex Object Storage under an approved retention policy | Minimized signed metadata/reference receipt in the existing backend |
| Product/operational analytics aggregates | YDB | Current control counters in Supabase only when needed |
| Raw weak site telemetry | YDB with TTL, or do not collect | Never duplicate as a Supabase firehose |
| Strong-action current state/profile inputs | Supabase | Analytics event in YDB |
| Raw TG/VK comments, embeddings and matches | YDB comment-feedback sidecar | Sanitized static manifest in Object Storage |
| Canonical event data | Fly SQLite | Bounded card/vector projection in Supabase |

## Profile materialization rule

Before consent/server sync, browser state is device-local and not a durable identity claim. After materialization, the current durable profile belongs to Supabase. The project must not maintain competing `visitor_profile_snapshot`/`profile_revision` rows in Supabase and `pa_profile_snapshot` rows in YDB with unclear precedence.

The post-release VK privacy vault is a scoped exception: it stores only VK subject proof, VK consent, eligible friend edges and their lifecycle in YDB. It does not copy or independently rank the current personalization profile. A separate 152-FZ audit must decide whether the broader Supabase Auth/email/profile flow may remain as implemented or must migrate to a Russian primary store; the VK feature cannot be used to claim that unresolved broader flow is compliant.

## Required flows

### Views and actions

1. Static HTML remains useful without Supabase/YDB.
2. After consent, a same-origin endpoint validates actor, device credential, schema, payload and idempotency.
3. Supabase transactionally updates bounded strong-action/current state and a profile revision.
4. An asynchronous outbox projects de-identified analytics to YDB.
5. YDB failure never blocks CTA/navigation or rolls back a user action.

### Login and profile linking

1. Yandex or verified-email auth creates/recovers a Supabase Auth identity.
2. Anonymous-to-auth linking runs automatically after login under eligible personalization consent and is idempotent in storage; no additional merge-confirmation dialog is required.
3. Supabase stores `profile_identity_link` and merge audit state.
4. Merge compact snapshots/current state, not raw browsing history.
5. Authenticated explicit actions win conflicts.
6. Logout does not split the durable profile. Unlink/reset/delete are separate explicit operations.

### Personal email announcement

1. Planner reads due subscription and profile state from Supabase.
2. Kaggle receives only a sanitized immutable profile/event snapshot.
3. Recommendation issue/cards (including exactly three email events) and personal-page token metadata are persisted in Supabase.
4. HTML/JSON is published to Object Storage/CDN.
5. Only after artifact publication succeeds may the Supabase outbox row become sendable.
6. Sender rechecks the active-admission ceiling (`<= 200`), subscription, consent and suppression before NotiSend send.
7. Provider callbacks update delivery/suppression in Supabase; YDB receives asynchronous analytics.

The personal page itself is intentionally readable through a forwardable public secret URL without an auth session. Supabase still owns token hash/revocation/retention metadata; the artifact is `noindex` and excludes raw profile/private identity data.

### Transactional email

1. A server-owned account or saved/followed-event transition creates the transactional outbox row in Supabase.
2. Sender revalidates the current event/account state, purpose-specific consent where required and suppression immediately before claim.
3. Yandex Cloud Postbox sends from the verified transactional identity with `Reply-To: info@kenigevents.ru`.
4. Provider events update send-critical delivery/suppression evidence in Supabase; YDB receives only the asynchronous de-identified projection.

Postbox is not a recommendation fallback. NotiSend is not a generic
transactional fallback; its only transactional exception is the reviewed Auth
repeat/fixed-test route selected before dispatch under the shared capacity gate.

### Inbound email

1. Internet mail is accepted by SpaceWeb MX and retained in `info@kenigevents.ru` for human webmail/IMAP use.
2. SpaceWeb keeps `info@` in `Mail` mode. Its panel cannot combine retention with the mutually-exclusive `Forwarding` mode, so a timer Function reads only UIDs after a private cursor using `BODY.PEEK[]` and never changes `Seen`.
3. Intake stores an allowlisted normalized envelope only in private KMS-backed storage, then hands a minimized signed metadata/reference pointer through YMQ to an HMAC adapter and service-only Supabase receipt RPC. The retained SpaceWeb mailbox remains the authoritative original.
4. Keyed idempotency prevents IMAP/timer/queue duplicates; bounded batches, YMQ redrive and a DLQ retain failures for controlled replay. The separate Mail Trigger technical address remains available for direct trigger/attachment canaries.
5. Automation must not auto-reply or Bcc `info@kenigevents.ru` until explicit loop prevention exists. `dmarc@kenigevents.ru` is not forwarded into this pipeline.

### Event comment feedback

The comment pipeline reads canonical event/source snapshots from Fly SQLite, keeps raw comments and processing state in the YDB sidecar, and exports a safe static manifest. Comment-feedback state does not become a competing user profile and cannot directly rewrite Smart Update facts or user interests.

## Forbidden designs

- A competing YDB personalization profile alongside a Supabase-owned profile. The isolated purpose-specific VK personal-data vault is allowed and must not contain ranking/profile snapshots.
- Parallel YDB and Supabase subscription/suppression/outbox systems.
- Cross-database transactions in the send-critical path.
- YDB analytics used for send eligibility.
- Browser direct writes to YDB or raw/private Supabase profile tables.
- Plain email, bearer tokens or raw profile vectors in YDB analytics.
- Raw VK IDs, VK message bodies, complete public friend lists or friendship edges in Supabase, YDB analytics, core Fly SQLite or static artifacts. The encrypted/HMAC VK identity and eligible pair edge may exist only in the isolated YDB personal-data contour.
- Plain/unsalted SHA for email or bearer-token lookup.
- `anon_id` alone treated as proof of profile ownership.
- Full canonical event copies in Supabase/YDB.
- Comment sentiment directly applied to a user profile without a separate product/ranking contract.
- NotiSend treated as the consent, subscription, suppression or capacity source of truth.
- More than 200 unique NotiSend recipients at launch, or a provider-only
  over-limit check in place of an atomic Supabase admission gate.
- Postbox used as a hidden recommendation fallback, or NotiSend used for
  transactional mail outside the reviewed Auth repeat/test rule.
- Mail-trigger processing that removes the retained SpaceWeb mailbox copy, exposes attachments publicly or can create reply/Bcc loops.

## Static browser transport and storage boundary

All browser access to the personalization Supabase uses one configuration-keyed
`ResilientDataClient` singleton. The singleton owns route health/selection but
is independent of Auth; `StaticSiteAuth` consumes it rather than defining the
data route for the rest of the site. Direct and stateless-relay probes run in
parallel, a healthy choice is cached briefly in `sessionStorage`, and no route
is selected when both probes fail.

Every request declares one of three policies:

- `safe-read`: side-effect-free reads, including explicitly read-only RPC POSTs,
  may try the alternate healthy route once;
- `selected-once`: OTP, Search and other non-idempotent/cost-bearing writes are
  sent at most once; timeout after dispatch is ambiguous and never causes an
  automatic resend;
- `idempotent-replay`: retry/outbox is permitted only when the server contract
  has a stable idempotency key or state-upsert uniqueness guarantee.

The shared outbox is bounded (16 records, 12 KiB, 24-hour TTL, five attempts),
uses IndexedDB with a compact localStorage fallback, deduplicates by explicit
record id and preserves foreign channels without burning attempts. This is an
egress/reliability mechanism, not a security boundary. RLS, server-side rate
limits, schema validation, authentication and idempotency enforcement remain
authoritative; client probes/cooldowns must never be represented as DDoS
protection.

KenigEvents-owned browser state has both per-key limits and an enforced aggregate
budget of 64 KiB, excluding the Supabase-owned auth token. Profiles,
feedback/search queues, reconciliation markers and personal-feed hints are
capped and/or expiring. If many individually valid cache keys exceed the total,
disposable queues/caches are evicted first while the current focus participation
and compact personalization state are preserved. Full per-preview feed manifests
and obsolete continuation caches are removed. Cleanup never reads, compacts or
rewrites Supabase Auth storage.

## Security gates

- RLS on every exposed Supabase table; ownership predicates use `auth.uid()`.
- Browser cannot set `email_verified`, consent proof, suppression or send state.
- Authorization never trusts `user_metadata`.
- Emails are stored only where sending requires them; lookup uses keyed HMAC.
- Bearer tokens have at least 128 bits of entropy and are stored only as keyed hashes; page, click, unsubscribe and feedback tokens are separate.
- Both YDB analytics and the YDB personal-data contour are service-credential-only, but use separate least-privilege service accounts/namespaces; the browser never receives YDB credentials.
- Account/profile deletion emits a purge request for eligible YDB raw/history state; irreversibly anonymized aggregates follow a documented retention policy.
- VK message-link challenges use a keyed code hash, short TTL and atomic one-time consume; the full VK friend list is intersected transiently and is not persisted.
- The existing `ru-central1` YDB resource does not by itself establish compliance: privacy tables require an isolated IAM/KMS/audit/retention boundary, and broader Supabase PII flows remain a release/legal audit item.
- Recommendation admission and every send claim fail closed above the 200-user launch ceiling.
- Provider credentials and mailbox passwords stay in the approved secret manager and never enter Git, artifacts or application logs.
- The browser may read only owner-scoped saved-event state and must mutate it
  through the capped desired-state RPC; direct authenticated table DML is
  forbidden. Expensive vector search, quota reservation and search audit RPCs
  are service-role-only behind an Edge Function that first validates the caller
  JWT and passes the verified `auth.users.id` to fixed-`search_path` wrappers.
- The stateless relay is an explicit method/path allowlist, not a generic
  Supabase proxy. The only Storage exception is authenticated upload/delete in
  private bucket `focus-feedback`; Storage RLS remains authoritative. Unknown
  RPC/functions, Auth admin, Realtime and other buckets fail closed.
- Focus participant registration and page feedback are idempotent desired-state
  RPCs routed through the same thin client. The participant projection has a
  serialized 200-active-member ceiling; the one-time presentation backfill uses
  a fixed cutoff and never infers communication consent from Auth history.

## Consequences for existing branches

- `agent/personal-email-announcements-docs` is superseded as an ownership design: its YDB profile/subscription/outbox ownership must not be merged.
- `feature/event-email-notifications-static-20260702` is directionally closer, but must be ported to a fresh branch and hardened against legacy Supabase env fallback, ordinary SHA email lookup, client-trusted event snapshots and broad grants.
- Event-comment-feedback keeps YDB ownership for its independent sidecar.
- No production profile migration is required because the conflicting YDB profile path was never enabled.

## Related documentation

- [Anonymous/static-site personalization](../features/unsigned-personalization/README.md)
- [Personal email announcements](../features/personal-email-announcements/README.md)
- [Site user identity](../features/site-user-identity/README.md)
- [Favorites and calendar](../features/event-favorites-calendar/README.md)
- [Email delivery operations](../operations/email-delivery.md)
- [Event comment feedback](../features/event-comment-feedback/README.md)
