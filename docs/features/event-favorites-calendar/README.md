# Event favorites and calendar

> Status: **design with ICS preview already implemented**. Durable favorite state and cross-device behavior are missing.

## Product contract

For the release, “Добавить в календарь” and “Избранное” share one durable **saved event** state:

- saving an event creates/updates one favorite/follow row;
- the user may also receive/download the event ICS;
- repeating the action is idempotent;
- removing the favorite does not delete an already imported external calendar entry;
- calendar/favorite does not imply email consent;
- a D-1 reminder/cancellation email is a separate explicit transactional opt-in;
- like, favorite and “не интересно” are different signals.

After a successful save, the UI must immediately make reminder state understandable:

- when verified email and transactional-event consent already exist, show the masked destination and the promise to remind 24 hours before the canonical start;
- when email or consent is missing, show the next action inline: Yandex login or manual verified-email entry, then explicit reminder opt-in; a manually entered address is cached once in `ke_contact_email_v1` and reused on later same-browser saves instead of being requested again;
- when less than 24 hours remain or the event start is not trustworthy, do not promise a D-1 message.

Calendar export must remain successful even when email verification, consent capture or the mail provider is unavailable. Conversely, an email/reminder failure must not roll back the durable saved-event state or ICS download.

## Surfaces

- event detail save/calendar CTA;
- global `Моё избранное` item in the shared mobile and desktop navigation/account shell;
- `/izbrannoe/` saved-events page/list;
- related/search/list cards where product UI allows save;
- personal recommendation page;
- transport-leg calendar action, which is not automatically an event favorite unless the product explicitly offers a separate saved trip.

## Global menu and saved-events page

This is a first-release requirement, not a post-release enhancement:

- every generated interactive HTML page uses the same `Моё избранное` destination; placement may adapt with the approved responsive navigation, but the label, meaning and state do not fork by page or breakpoint;
- after identity/state restore, show a numeric badge only when the resolved count is greater than zero; at zero, while anonymous, or while restoring state, do not flash a misleading `0`/previous-user count;
- accessible name includes the resolved count, for example `Моё избранное, 3 события`;
- the count is the number of distinct durable saved events after canonical merge resolution. It excludes likes, raw ICS downloads, reminder count and separately saved transport legs;
- `/izbrannoe/` is a `noindex` static shell that hydrates through authenticated Supabase/RLS reads. User-specific rows are never embedded into CDN HTML or a shared cache;
- direct navigation works. An anonymous visitor sees the common Yandex/manual-email identity choice without losing the public static fallback;
- the page returns all resolvable saved rows in a compact batch, grouped or clearly marked as upcoming/current, rescheduled, cancelled/merged and past/archived. A lifecycle change must not make a saved row disappear silently;
- the page joins saved ids to the versioned static catalog in one batch and does not perform one remote read per card;
- save, repeat, undo and remote reconciliation update the CTA, menu badge and list idempotently in the current tab and across same-origin tabs; authenticated cross-device state converges through the durable store;
- login/profile linking deduplicates local and remote saves. Logout/account switch immediately clears prior-user UI state, while failure to load favorites never blocks public navigation or ICS download.

## Ownership

Supabase/Postgres owns favorite/follow state with RLS by user identity. Local anonymous saves may stay device-local until a consented secure materialization/linking flow exists. Fly SQLite remains the event source of truth. ICS is generated from canonical event facts/static artifacts.

## Required behavior

- valid Europe/Kaliningrad ICS, stable UID and lifecycle metadata;
- one saved row per user+event;
- repeat/undo and concurrent-device safety;
- Yandex/email identity linking without duplicate favorites;
- one idempotent D-1 reminder schedule per user+canonical-event+start-version, cancelled/recomputed on undo, merge, cancellation or reschedule;
- cancelled/rescheduled event visibility and notification choice;
- deleted/merged event redirect or migration policy;
- export/delete/account purge;
- offline/static fallback that never blocks event navigation.

## Release E2E

- `FAV-MENU-001`: save → badge `1`; repeated save stays `1`; undo hides the badge at `0`; another tab converges.
- `FAV-PAGE-001`: `/izbrannoe/` shows every durable saved row once with correct lifecycle/status and no per-card network loop.
- `FAV-LINK-001`: local → Yandex and local → verified-email linking preserve/deduplicate saved events.
- `FAV-PRIVACY-001`: logout, account switch, direct URL and browser back/cache never expose the previous user's list/count.
- `FAV-DEGRADED-001`: Supabase/read/mutation failure is explicit, rolls optimistic UI back safely and does not break the ICS action or public page.

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)
