# Transactional event email notifications

> Status: **production-disabled control-plane foundation in `origin/main`**. Verified-identity/consent/outbox guards and the `event_reminder_24h` message kind exist; calendar UX, reminder scheduling, templates, provider-event ingestion and live sending are not complete.

## Scope

User-requested transactional mail for a saved/followed event:

- save/follow confirmation;
- explicit reminder **24 hours before the canonical event start**;
- cancellation;
- reschedule or material date/time/location change.

This is distinct from [personal recommendation email](../personal-email-announcements/README.md). The streams share the Supabase email control plane and suppression evidence, but not provider, consent purpose, cadence or content rules.

## Ownership

Supabase owns authenticated email profile, follow/favorite relation, transactional consent, outbox, idempotency/send guard, suppression and provider delivery evidence. YDB may receive de-identified statistics only. Event facts/lifecycle come from Fly SQLite and must be revalidated server-side.

Yandex Cloud Postbox is the exclusive transactional transport, with the intended sender `Kenig Events <notify@kenigevents.ru>` and `Reply-To: info@kenigevents.ru` after identity/DNS verification. NotiSend is reserved for opt-in personal recommendations and must not be a transactional fallback.

## Calendar-to-reminder product flow

Adding an event to the calendar/favorites state does not silently grant email consent. The save result must show one truthful state:

- verified email plus active `transactional_event` consent: **«Событие сохранено. Напомним за день на a***@domain»**;
- verified email without consent: an explicit **«Напомнить за день по почте»** opt-in;
- no verified email: a choice between **«Войти через Яндекс»** and **«Ввести почту»**, followed by the same explicit reminder opt-in;
- event starts in less than 24 hours or has no trustworthy start: explain that a D-1 reminder cannot be promised; never show the success promise or enqueue a misleading reminder.

Changing/removing a saved event or changing/cancelling/merging the canonical event must cancel or recompute the pending reminder. Exactly one logical reminder may be sent per `user + canonical event + start-version`; retries reuse the same idempotency key.

## Current foundation and missing implementation

The shared Supabase email control plane in `origin/main` already accepts only verified synchronized identities, purpose-specific transactional consent, suppression checks and the fixed Postbox route for `event_reminder_24h`. All outbound switches remain disabled/dry-run-only.

Release still requires:

- calendar/favorite UI states and explicit consent capture described above;
- a scheduler that derives `send_at = canonical start - 24h`, handles timezone/reschedule/cancel/merge races and performs bounded catch-up without duplicates;
- server-owned event snapshots from Fly SQLite/static export, never client-trusted event facts;
- confirmation/reminder/reschedule/cancellation templates and accessible preference/undo surfaces;
- Postbox delivery-event ingestion, suppression/alert proof, seed canary and live E2E;
- retirement of the historical stale branch `feature/event-email-notifications-static-20260702` without merging it wholesale.

## Release gates

See [email delivery operations](../../operations/email-delivery.md), [favorites/calendar](../event-favorites-calendar/README.md) and [site identity](../site-user-identity/README.md).
