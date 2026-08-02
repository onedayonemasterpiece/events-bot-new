# Event favorites and calendar

> Status: **R15 candidate implemented**. The owner-only Supabase saved-state
> schema is deployed; the static noindex page, shared auth hydration and
> calendar-first merge are implemented. Browser acceptance through a real
> Yandex session remains a release gate.

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

## R15 saved-events page: compact calendar above liked cards

The saved-events route is an authenticated **noindex** utility surface. It
renders a stable static shell/skeleton first, then hydrates through the shared
origin-scoped Supabase auth runtime; it does not expose tokens in DOM state and
does not invent an anonymous server profile.

The page is not one merged list. It has two consecutive product zones:

### Zone 1 — `Мой календарь`

The top of the page is a compact chronological agenda containing only rows with
`calendar_saved=true`:

```text
Дата
HH:MM–HH:MM | Мероприятие | локация
```

Dates and rows are sorted by event time. Reschedule moves the row; cancellation
is visible; unknown end time or location uses a typed fallback. This compact
section ends before the visual liked-event collection begins.

### Zone 2 — `Понравилось`

Below the calendar, events with `favorite_saved=true` render as the ordinary
large event cards used by the public/personalized site. This is the visual
collection of what the user liked, not a continuation of the compact agenda.

`calendar_saved` and `favorite_saved` are independent signals:

- the same event may appear once in the compact calendar **and** once as a large
  liked card when both states are true; that cross-zone reuse is intentional,
  not a duplicate bug;
- within either zone, one event/occurrence appears at most once;
- turning off calendar save removes only the agenda row;
- removing the like removes only the large card;
- turning Push off removes neither zone;
- repeating either action is idempotent.

Past, cancelled, merged or inaccessible rows follow the lifecycle policy above
rather than being silently mixed into the future surface. Empty, signed-out and
backend-unavailable states remain honest and retain navigation.

Durable storage is `public.user_saved_event`, introduced by
`supabase/migrations/20260727141820_durable_saved_events_v1.sql`. It has one
row per `auth.uid()` + event, separate `calendar_saved` and `favorite_saved`
sources, four owner-only RLS policies, a `security_invoker` view
`my_saved_events_v1`, and the authenticated RPC
`set_saved_event_state_v1(bigint,text,boolean)`. The migration was applied to
the personalization Supabase project on 2026-07-27 through the Management API
migration endpoint. Read-only postflight verified Postgres 17, RLS enabled,
four policies, the view/RPC present and zero pre-existing rows; the security
advisor reported no finding tied to these objects.

Migration `20260731174310_harden_saved_event_mutations.sql` is the reviewed
next security state (not applied by the static build): authenticated callers
retain owner-scoped `SELECT`, but direct `INSERT/UPDATE/DELETE` is revoked.
The same desired-state RPC becomes a fixed-`search_path` `SECURITY DEFINER`
boundary that derives ownership only from `auth.uid()`, serializes concurrent
tabs per owner, preserves idempotent repeats and enforces at most 500 active
saved-event rows per user. It accepts no caller-supplied `user_id` and never
uses `user_metadata` for authorization.

Optional date navigation remains event-aware: it extends through the furthest
month containing a public event in the generated availability manifest. Dates
without events are not ordinary active targets, while month navigation, focus
order and an explicit return path remain available. The month navigator does
not replace the compact agenda or the liked-card zone.

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
