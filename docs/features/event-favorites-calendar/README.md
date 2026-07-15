# Event favorites and calendar

> Status: **ICS + compact device-local acknowledgement implemented in static preview**. Durable account/cross-device favorite state is still missing.

## Current static-site behavior

Every eligible calendar control uses the same markup/runtime contract on event
details, listings, related cards, sticky CTA, mobile and desktop. JavaScript
first fetches the canonical `.ics`; only after a successful HTTP response it
starts the browser download and switches every control for that event to
`Добавлено`. Repeated activation downloads the ICS again and is idempotent.
Without JavaScript the control remains an ordinary `text/calendar` link.

`Добавлено` means **the site successfully fetched the ICS and initiated its
download**. A browser cannot prove that the user completed import into an OS or
third-party calendar, so this state must never be described as verified external
calendar membership.

The acknowledgement is shared by mobile and desktop on the same origin through
one intentionally small localStorage entry:

```json
{"v":1,"e":{"5658":20649,"5878":20653}}
```

- key: `ke_calendar_saved_v1`;
- payload: schema version plus `event_id → exclusive expiry epoch-day` only;
- no title, href, click log, timestamp, profile id, cookie or analytics copy;
- invalid and expired entries are removed on every read/boot/write;
- state survives through the event's Kaliningrad day and is removed the next day;
- the map is capped at `256` nearest upcoming events;
- corrupt, blocked or quota-limited storage fails open: the ICS link still works;
- browser `storage`/`pageshow` events re-sync visible controls and the explicit
  personalization reset also removes this technical local state.

This local projection is disposable and same-browser/same-origin only. It is not
the source of truth and is not used as a recommendation feature. Supabase remains
the future owner of authenticated durable favorite/follow state with RLS.

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

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)
