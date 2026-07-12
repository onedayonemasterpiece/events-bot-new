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
- one idempotent D-1 reminder schedule per user+canonical-event+start-version, cancelled/recomputed on undo, merge, cancellation or reschedule;
- cancelled/rescheduled event visibility and notification choice;
- deleted/merged event redirect or migration policy;
- export/delete/account purge;
- offline/static fallback that never blocks event navigation.

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)
