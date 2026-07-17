# Site user identity and profile linking

> Status: **partial/design**. Yandex login/logout exists in the authorized-search surface; verified-email identity and durable profile linking are not production-complete.

## Scope

Owns the global identity contract shared by search, personalization, favorites/calendar and email:

- device-local anonymous profile before server materialization;
- Yandex OAuth identity;
- verified-email identity by code/link;
- session restore and logout;
- anonymous-to-auth profile linking;
- unlink, reset, account/profile deletion and audit.

Post-release VK identity linking through a one-time direct-message challenge and privacy-preserving friend intersection is specified separately in [VK identity linking through a verification message](vk-message-link.md). It is not part of the F1–F17 first-release gate.

Search-specific UI remains in [authorized event search](../unsigned-personalization/authorized-event-search.md). Ranking/profile schemas remain in [unsigned personalization](../unsigned-personalization/README.md).

## Product rules

- Supabase Auth is the identity provider/system of record.
- A verified-email user is a real Supabase identity, not a second subscription-only account system. Verification supports both a six-digit code and a one-click link bound to the same transaction/identity.
- Local `anon_id` is not proof of ownership. Server materialization requires a device-bound credential and current consent.
- Linking is automatic after login, idempotent and auditable. No extra merge-confirmation dialog is required; repeating callback/login cannot duplicate profile/favorites/subscriptions.
- Authenticated explicit actions win merge conflicts over inferred/local state.
- Logout ends the session but does not split the durable profile.
- Reset/unlink/delete are explicit operations with clear scope.
- Identity/email presence never grants recommendation-email consent.

## Required states

- `anonymous_local`
- `anonymous_materialized`
- `email_verification_pending`
- `authenticated_email`
- `authenticated_yandex`
- `link_pending`
- `linked`
- `unlink_pending`
- `deleted_or_purging`

The exact schema belongs to a dedicated implementation task, but one identity must never own multiple competing current profiles.

## Linking acceptance

- current personalization consent required before importing local behavior;
- merge is transactionally idempotent;
- favorites/calendar saved state is deduplicated by event id;
- negative/explicit actions retain priority and timestamps;
- inferred interests are merged with confidence/recency decay and conflict checks; raw browsing history is not copied blindly;
- result is visible to the user and reset/unlink remains available;
- session expiry/callback failure restores a usable anonymous static page;
- deletion propagates to profile-owned Supabase rows and eligible YDB raw/history projections.

## Data ownership

See [personalization data ownership](../../architecture/personalization-data-ownership.md). Supabase owns identity/profile/linking; YDB may receive de-identified analytics only.

## Open product decisions

See the umbrella [global product decisions](../static-personal-announcements/global-product-decisions.md), especially verified-email UX and profile-link consent.

The VK message-link extension additionally requires approved retention, a dedicated VK inbox, a purpose-specific VK-link consent, a personal-data processing policy and a user agreement. Its live identity/friend graph is pseudonymous personal data, not anonymous data, and its canonical store is an isolated Managed Service for YDB personal-data contour in `ru-central1`, not Supabase.

## Shared controller and control plane (2026-07-17 foundation)

The layout-independent browser entry point is `site/src/lib/site-identity.js`. A page
family may subscribe to its state and render its own controls; this module does not
own the final header, cards, event-detail composition or `/izbrannoe/` layout.

Implemented contracts:

- Yandex PKCE login/logout with a cleaned return URL and persisted Supabase session;
- verified email through one Supabase OTP transaction whose email template exposes
  both `{{ .Token }}` and `{{ .ConfirmationURL }}`; the client supplies a 15-minute
  UX TTL, 60-second resend cooldown, five-attempt ceiling and replay marker, while
  Supabase Auth remains the authoritative one-time/rate-limit enforcement;
- a single remembered device email, exposed to UI only in masked form, plus
  `forgetEmailOnDevice()` independent of logout;
- reload and cross-tab synchronization through Supabase storage, `storage` events
  and `BroadcastChannel`; switching auth user clears account-dependent count state;
- random 256-bit device proof. `anon_id` alone is never accepted by the server;
- automatic consented merge through `identity-control`, with request-id replay,
  one profile per `auth.users.id`, unique active provider links and saved-occurrence
  deduplication. An already-linked device cannot be moved to another account.

The additive migration
`supabase/migrations/20260717170000_site_identity_saved_occurrence_v1.sql` creates
private `site_identity` and `saved_events` schemas. Browser roles have no raw table
privileges. Authenticated user actions are narrow RPCs; device materialization,
merge, unlink, purge, lifecycle sync and reminder dispatch are service-only RPCs
called through an explicitly authenticated Edge Function or backend worker.

### Conflict, unlink and delete policy

- authenticated explicit current state wins; anonymous rows are imported only when
  the durable `(profile,event,occurrence)` row does not already exist;
- repeated merge with the same request id returns its recorded result; a later
  request remains harmless because profile/link/save uniqueness is structural;
- logout preserves the durable profile; unlink revokes the current device proof and
  rotates the local device, without cloning/splitting the profile;
- deletion first marks the profile `deleting`, revokes reminders and creates a purge
  request. Actual Supabase Auth user deletion requires a separate recent-auth,
  audited service operation; eligible YDB history purge consumes that request;
- a provider identity can be explicitly unlinked only after proving another viable
  sign-in method. The foundation does not expose a browser RPC that could orphan an
  account.

### Required activation configuration

1. Apply the migration only after reconciling Supabase migration history.
2. Deploy `identity-control` with JWT verification disabled at the gateway; it
   validates bearer sessions itself and keeps the secret key server-only.
3. Configure the email template with both code and confirmation link, OTP expiry
   `900` seconds, and project resend/rate limits no weaker than the controller.
4. Use the existing `custom:yandex` provider and production redirect allow-list.
5. Inject only publishable personalization URL/key into static builds.

Production activation is **not** part of this branch: the live project contains
migration `20260717074903`, which is not present in this checkout, so applying a new
migration before history reconciliation would violate the repository migration
policy.
