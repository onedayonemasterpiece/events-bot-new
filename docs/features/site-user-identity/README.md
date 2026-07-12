# Site user identity and profile linking

> Status: **partial/design**. Yandex login/logout exists only in the authorized-search surface; the required shared identity shell across every static HTML page, passwordless verified-email identity and durable profile linking are not production-complete.

## Scope

Owns the global identity contract shared by search, personalization, favorites/calendar and email:

- device-local anonymous profile before server materialization;
- Yandex OAuth identity;
- verified-email identity by code/link;
- session restore and logout;
- one shared account/identity control rendered by the common static-page shell on every HTML route;
- anonymous-to-auth profile linking;
- unlink, reset, account/profile deletion and audit.

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
- A manually entered address becomes authentication only after code/link verification has created or recovered a real passwordless Supabase Auth session. A cached or pending address is not an authenticated state.
- Verified email is the lightweight no-password authorization path: it asks for no profile data beyond the email and verification proof, and unlocks the same ordinary user-owned surfaces as Yandex unless a feature explicitly requires provider-specific Yandex data or a separate admin role.

## Global static-page identity shell (release requirement)

Identity is a site-wide capability, not a `/poisk/` component. Every generated **HTML** page must use one shared identity controller and compact account entry in the common header/navigation shell. Coverage includes root, listings/categories/tags, event detail, search, forwardable personal pages, transport-enabled pages and administrator surfaces. Machine artifacts (`.ics`, JSON, sitemap, robots and media) are excluded because they have no interactive HTML shell.

The control restores state before showing the final CTA and renders one of these consistent states everywhere:

- anonymous, with equal actions **«Войти через Яндекс»** and **«Добавить почту»**;
- email verification pending, with masked address, resume code/link flow, change address and **«Забыть почту»**;
- passwordless email-authenticated, with masked address and compact account menu;
- Yandex-authenticated with usable verified email;
- Yandex-authenticated without usable email, with a non-blocking **«Добавить почту»** completion action;
- restoring/expired/degraded, while the static page and public CTA remain usable.

Login or verification starts from the current page and returns to the same cleaned path/query/fragment after PKCE/code/link completion. Navigating to another static route or opening another same-origin tab must not revert the visible state; same-origin tabs synchronize through the authoritative Supabase session plus bounded browser-state events. The account control remains compact; logout/destructive actions live inside its menu rather than beside primary content actions.

### Action semantics

- **«Войти через Яндекс»** starts/restores the Yandex-backed Supabase session and automatically links eligible local personalization state idempotently.
- **«Добавить почту»** starts/reuses one code-or-link transaction. Before verification it is only a cached contact hint; after verification it creates/restores a lightweight passwordless Supabase session and the global shell immediately becomes authenticated on every page.
- **«Выйти»** ends the active Supabase session on this browser and returns every page to anonymous mode; it does not delete the durable profile, favorites, consent history or suppressions.
- **«Забыть почту на этом устройстве»** clears `ke_contact_email_v1`, pending verification UX and any email-only browser session. It does not delete the server identity, withdraw purpose consent or cancel already scheduled mail; those are separate explicit account/consent actions. If the address comes from an active Yandex session, the UI explains that logout or address/consent management is required rather than pretending the provider email was forgotten.
- **«Удалить аккаунт/данные»**, unlink and consent withdrawal remain separate, explicit operations with their own confirmation and propagation contracts.

No page may implement its own competing session store, email cache, callback handler or logout semantics. Search consumes this shared identity state; it no longer owns authentication UI.

## Email acquisition journey

Every surface that needs a deliverable email, including the calendar D-1 reminder, must offer two equal entry paths:

1. **«Войти через Яндекс».** If Yandex supplies an email that Supabase Auth treats as verified, synchronize that address to the email control plane and show it masked before consent. If Yandex does not return a usable verified email, keep the Yandex identity and ask the user to complete manual email verification; never invent an address or claim that reminders are enabled.
2. **«Ввести почту».** Create or recover the Supabase email identity through one verification transaction that can be completed by either the six-digit code or the one-click link. Both methods must converge on the same identity and must not create a parallel subscription-only account. The address is entered once on that browser: a versioned `ke_contact_email_v1` localStorage record keeps the normalized/masked address and `pending|verified` state so reloads and later calendar saves reuse it instead of asking again.

The user can start this journey from the global account entry on every static HTML page, calendar-save confirmation or an email-subscription surface. A verified address is reusable across those surfaces, but every purpose (`transactional_event`, recommendations, and any later purpose) has separate explicit consent. Address replacement requires verification of the new address and atomic migration/cancellation of pending reminder destinations.

localStorage is UX memory, not authorization: only the verified Supabase identity/control-plane record can make a reminder eligible. The cache stores no auth/provider token or consent grant, survives ordinary reload/navigation, and is cleared by explicit `Забыть почту`/profile reset, account deletion or incompatible schema migration. See [personalization E2E acceptance](../unsigned-personalization/e2e-acceptance.md#email-only-browser-persistence-contract).

## Required states

- `anonymous_local`
- `anonymous_materialized`
- `email_verification_pending`
- `authenticated_email`
- `authenticated_yandex`
- `authenticated_yandex_email_missing`
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
- Yandex-with-email, Yandex-without-email→manual verification, and email-only code/link paths converge without duplicate user/profile/favorite/reminder rows;
- deletion propagates to profile-owned Supabase rows and eligible YDB raw/history projections.

## Data ownership

See [personalization data ownership](../../architecture/personalization-data-ownership.md). Supabase owns identity/profile/linking; YDB may receive de-identified analytics only.

## Product decision status

The site-wide shell and passwordless verified-email semantics are accepted in the umbrella [global product decisions](../static-personal-announcements/global-product-decisions.md). Implementation details such as component boundaries and RPC names belong to dedicated feature tasks and may not weaken this release contract.
