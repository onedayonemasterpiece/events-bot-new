# Transactional event email notifications

> Status: **shared production-safe control plane, Postbox feedback path and Fly outbox worker implemented**. Event-specific producers remain disabled until their product flows enqueue server-validated facts.

## Scope

User-requested transactional mail for a saved/followed event:

- save/follow confirmation;
- optional reminder;
- cancellation;
- reschedule or material date/time/location change.

This is distinct from [personal recommendation email](../personal-email-announcements/README.md). The streams share the Supabase email control plane and suppression evidence, but not provider, consent purpose, cadence or content rules.

## Ownership

Supabase owns authenticated email profile, follow/favorite relation, transactional consent, outbox, idempotency/send guard, suppression and provider delivery evidence. YDB may receive de-identified statistics only. Event facts/lifecycle come from Fly SQLite and must be revalidated server-side.

Yandex Cloud Postbox is the exclusive transactional transport, with the intended sender `Kenig Events <notify@kenigevents.ru>` and `Reply-To: info@kenigevents.ru` after identity/DNS verification. NotiSend is reserved for opt-in personal recommendations and must not be a transactional fallback.

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

## Release gates

See [email delivery operations](../../operations/email-delivery.md) and [favorites/calendar](../event-favorites-calendar/README.md).

## Saved-event D-1 producer foundation (2026-07-17)

The saved-occurrence migration adds explicit per-occurrence reminder consent. Save,
like and reminder consent are independent. Enabling requires a synchronized verified
`email_control.recipient_identity`, active `transactional_event` purpose consent and
returns only a masked address.

`saved_events.schedule_d1_v1` targets 24 hours before start, catches up when consent
arrives inside that window, and shifts a due time out of 22:00–08:00
`Europe/Kaliningrad` quiet hours. Completed/cancelled occurrences stop pending work;
reschedule increments the schedule revision and recalculates a not-yet-sent reminder.
A unique delivery/idempotency key plus `reminder_sent_at` guarantees one D-1 enqueue.
Reschedule/cancellation are separate lifecycle messages, not a second D-1.

`personalization_enqueue_due_reminders_v1` is service-only and writes strict
`transactional-plain-v1` payloads through the existing
`email_enqueue_transactional_v1` path. The existing Postbox worker therefore keeps
all claim-time verified-email, consent, suppression, hard-bounce, complaint,
unsubscribe, kill-switch and ambiguous-delivery protections. The migration defaults
producer calls to dry-run; activation needs a scheduled service call and existing
Postbox runtime switches.

The generic foundation message intentionally contains only server-owned event id and
occurrence timing. A later canonical Fly producer may enrich subject/body from a
server-validated event snapshot; the browser must never provide mail facts.
