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
