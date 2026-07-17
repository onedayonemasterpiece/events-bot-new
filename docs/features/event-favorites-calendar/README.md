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

## Related documentation

- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)

## Implemented shared foundation (2026-07-17)

`site_identity_saved_occurrence_v1` supplies a compact durable occurrence model and
layout-neutral RPCs:

- `personalization_save_occurrence_v1(event_id, occurrence_key, starts_at, saved)`
  is idempotent under the unique profile/event/occurrence constraint; undo is a
  soft removal and revokes an active reminder;
- `personalization_saved_count_v1()` returns `count(distinct event_id)` for the
  authenticated menu contract, including a real `N > 0` result;
- `event_signal` stores `like` and `not_interested` separately from calendar save;
- lifecycle is `upcoming | rescheduled | cancelled | completed`; only a service
  sync from canonical Fly event facts may change it;
- occurrence times submitted by the browser/device never become reminder facts;
  reminder consent stays unavailable until the service lifecycle sync validates the
  canonical Fly start and records its validation timestamp;
- favorite/save, `like`/`not_interested`, reminder consent evidence and reminder
  scheduling/delivery are separate relations; a save or like cannot authorize mail;
- browser roles have RPC execution only and no private-schema/table access; even a
  Supabase anonymous Auth token cannot use account-owned saved-event RPCs;
- the browser controller exposes `saveOccurrence`, `refreshSavedCount`, `setLike`
  and `setReminder` for parallel UI work.

ICS remains a static, Supabase-independent fallback. `site/src/lib/ics.ts` now emits
an occurrence UID, `X-KENIGEVENTS-OCCURRENCE-ID`, lifecycle `STATUS`, `TRANSP` and
`LAST-MODIFIED`; no failed dynamic save may suppress the `.ics` link/download.

The schema stores identifiers, occurrence time and state only. It intentionally does
not duplicate event title, description, poster, venue or ticket facts from Fly.
