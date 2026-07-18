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
