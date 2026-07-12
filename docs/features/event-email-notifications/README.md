# Transactional event email notifications

> Status: **shared production-safe control plane, live Postbox feedback path and Fly outbox worker implemented**. Event-specific calendar/reminder producers, templates and product UX remain disabled/missing until they enqueue server-validated facts and pass bounded warm-up.

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
- no verified email: a choice between **«Войти через Яндекс»** and **«Ввести почту»**, followed by the same explicit reminder opt-in; manual email entry is cached once in versioned localStorage and reused after reload/later saves, while server verification remains authoritative;
- event starts in less than 24 hours or has no trustworthy start: explain that a D-1 reminder cannot be promised; never show the success promise or enqueue a misleading reminder.

Changing/removing a saved event or changing/cancelling/merging the canonical event must cancel or recompute the pending reminder. Exactly one logical reminder may be sent per `user + canonical event + start-version`; retries reuse the same idempotency key.

## Runtime status

The historical branch `feature/event-email-notifications-static-20260702` remains
superseded and must not be merged wholesale. Its unsafe assumptions were replaced
on `main` by the shared `email_control` schema, correlated Postbox event consumer
and a transactional-only Fly worker.

The worker:

- claims only `transactional -> postbox` rows through a service-only RPC;
- rechecks verified identity, consent, suppression and database runtime switches;
- renders only the strict `transactional-plain-v1` payload contract;
- records the actual request hash before network access and the real Postbox
  MessageId after acceptance;
- quarantines ambiguous delivery instead of retrying and risking a duplicate;
- renews short-lived Yandex IAM tokens from a dedicated service-account authorized
  key rather than storing an expiring IAM token;
- exposes PII-free health counters and checks the private trigger DLQ, with bounded
  alerts to the Telegram superadmin.

Event-specific enqueue producers still have to derive the current event snapshot
from Fly SQLite and use the existing kind/consent constraints. Worker availability
does not authorize a client to supply event facts or enqueue arbitrary mail.

## Remaining release work

The controlled worker/feedback canaries prove the delivery runtime, IAM authorized-key parser, authenticated event correlation, suppression, alerting, DLQ/replay and ambiguity safety. They do **not** prove the calendar/reminder product flow.

Release still requires:

- calendar/favorite UI states, manual-email reuse and explicit consent capture described above;
- an event-specific producer that derives `send_at = canonical start - 24h`, handles timezone/reschedule/cancel/merge races and performs bounded catch-up without duplicates;
- server-owned event snapshots from Fly SQLite/static export, never client-trusted event facts;
- confirmation/reminder/reschedule/cancellation templates and accessible preference/undo surfaces;
- bounded cross-provider warm-up/placement evidence with application sends still behind the existing DB/process switches.

## Release gates

See [email delivery operations](../../operations/email-delivery.md), [favorites/calendar](../event-favorites-calendar/README.md) and [site identity](../site-user-identity/README.md).
