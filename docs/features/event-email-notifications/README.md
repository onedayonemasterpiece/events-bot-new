# Transactional event email notifications

> Status: **prototype foundation in a stale side branch; clean port required**. Production sending is not enabled.

## Scope

User-requested transactional mail for a saved/followed event:

- save/follow confirmation;
- optional reminder;
- cancellation;
- reschedule or material date/time/location change.

This is distinct from [personal recommendation email](../personal-email-announcements/README.md). The two streams share delivery primitives but not consent purpose, cadence or content rules.

## Ownership

Supabase owns authenticated email profile, follow/favorite relation, transactional consent, outbox, idempotency/send guard, suppression and provider delivery evidence. YDB may receive de-identified statistics only. Event facts/lifecycle come from Fly SQLite and must be revalidated server-side.

## Porting status

The historical branch `feature/event-email-notifications-static-20260702` contains a dry-run foundation, migration, Edge Function, Postbox worker, smoke and tests, but is far behind `origin/main`. It must not be rebased or merged wholesale. A new main-based branch must selectively port and harden it.

Required corrections during port:

- personalization project env only; never silently fall back to legacy Supabase;
- keyed HMAC for email lookup, not ordinary SHA;
- server-owned event snapshot from canonical export, not client-trusted facts;
- least-privilege grants/RLS;
- one shared email control plane with personal announcements;
- provider callback/suppression and live dry-run evidence.

## Release gates

See [email delivery operations](../../operations/email-delivery.md) and [favorites/calendar](../event-favorites-calendar/README.md).
