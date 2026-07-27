# Event favorites and calendar

> Status: **design with ICS preview already implemented**. Durable favorite state and cross-device behavior are missing.

## Product contract

For the release, “Добавить в календарь” and “Избранное” share one durable **saved event** state:

- saving an event creates/updates one favorite/follow row;
- the user may also receive/download the event ICS;
- repeating the action is idempotent;
- removing the favorite does not delete an already imported external calendar entry;
- calendar/favorite does not imply email consent;
- reminder/cancellation email is a separate explicit transactional opt-in;
- like, favorite and “не интересно” are different signals.

## Surfaces

- event detail save/calendar CTA;
- saved-events page/list;
- related/search/list cards where product UI allows save;
- personal recommendation page;
- transport-leg calendar action, which is not automatically an event favorite unless the product explicitly offers a separate saved trip.

## Ownership

Supabase/Postgres owns favorite/follow state with RLS by user identity. Local anonymous saves may stay device-local until a consented secure materialization/linking flow exists. Fly SQLite remains the event source of truth. ICS is generated from canonical event facts/static artifacts.

## Required behavior

- valid Europe/Kaliningrad ICS, stable UID and lifecycle metadata;
- one saved row per user+event;
- repeat/undo and concurrent-device safety;
- Yandex/email identity linking without duplicate favorites;
- cancelled/rescheduled event visibility and notification choice;
- deleted/merged event redirect or migration policy;
- export/delete/account purge;
- offline/static fallback that never blocks event navigation.

## R15 saved-events page and event-aware calendar

The saved-events route is an authenticated **noindex** utility surface. It
renders a stable static shell/skeleton first, then hydrates through the shared
origin-scoped Supabase auth runtime; it does not expose tokens in DOM state and
does not invent an anonymous server profile.

Within the future set, order is calendar-first:

1. events explicitly added through the calendar/save action;
2. then liked/favorited events not already represented above.

The same event/occurrence cannot appear twice merely because both actions were
used. Within a source tier the current implementation prefers the latest
recorded save action, then stable input order and an event-ID fallback; it does
not pretend that this is chronological event ordering. Past, cancelled, merged
or inaccessible rows follow the lifecycle policy above rather than being
silently mixed into the future list. Empty, signed-out and
backend-unavailable states remain honest and retain navigation.

The date calendar itself is event-aware: navigation extends through the
furthest month that contains a public event in the generated availability
manifest. Dates without events are not ordinary active targets—pointer events
and keyboard activation must not accidentally select them—while month
navigation, focus order and an explicit return path remain available.

## Public aggregate semantics

The current contract plans durable saved-event state but does not yet provide a
public `saved_event_count`. An ICS file request/download/click is a transport
event, not proof that a unique person saved or retained the event in an external
calendar. Listing pages therefore must not label ICS telemetry as “N people
added”. A future calendar social-proof count is eligible only when it is a
privacy-safe, deduplicated aggregate over durable saved-event rows, is non-zero,
and is labelled as saves rather than attendance. Zero or unavailable evidence
renders no icon and reserves no card width.

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)
