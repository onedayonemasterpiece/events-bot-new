# Event email notifications

Status: requirements / implementation backlog
Owner surfaces: static event pages, auth/profile, calendar-follow flow, event lifecycle updates, Yandex Cloud Postbox, YDB analytics.

## Scope

This feature covers transactional email notifications for users who explicitly add an event to their calendar from the static site while authenticated.

Notifications are not marketing mailouts. They are user-requested transactional messages about a specific followed event.

## Product requirements

1. **Authenticated email is mandatory for notifications**
   - If a user signs in through Yandex OAuth, store the email returned by Yandex as the notification email.
   - If OAuth does not return an email, the UI must ask the user to provide an email before enabling calendar reminders/updates.
   - A user may update or remove the notification email.
   - Calendar-follow with email notifications requires explicit user consent.

2. **Calendar follow confirmation and reminders**
   - When an authenticated user adds an event to their calendar, enqueue a confirmation email.
   - Enqueue a reminder email approximately 24 hours before the event start.
   - If the event starts in less than 24 hours, skip the 24-hour reminder and record the skip reason.

3. **Reschedule notifications**
   - If a followed event is postponed/rescheduled or any anchor changes, enqueue an email to followers.
   - The email must include a diff of changed fields: date, time, location, address, city, and ticket/registration link when relevant.
   - The email must include the current event URL and the organizer/source URL when available.

4. **Cancellation notifications**
   - If a followed event becomes `lifecycle_status='cancelled'`, enqueue a cancellation email to followers.
   - The message must clearly say the event is cancelled and include the source-grounded cancellation note when available.
   - The email must include this disclaimer: `Полюбить Калининград Анонсы не является организатором события; информация об отмене получена из публичных источников/сообщений организатора.`

5. **Public event surfaces after cancellation/postponement**
   - Telegraph page remains available and must show `ОТМЕНЕНО` / `ПЕРЕНЕСЕНО` in title/summary.
   - Static event page remains available and must show the same lifecycle badge and source-grounded cancellation/reschedule note.
   - Existing Telegram/VK event posts must be edited in place when possible; non-active events must not create new first-time promo/public posts.
   - If a source poster clearly contains `ОТМЕНА` / cancellation text, that poster should become the primary event image.

## Transport: Yandex Cloud Postbox

Use **Yandex Cloud Postbox** for sending, not a personal mailbox SMTP. Postbox supports SMTP and AWS SES-compatible API, and is meant for transactional email such as event reminders and status-change notifications.

Official references checked 2026-07-02:

- [Yandex Mail: sending a large number of messages](https://yandex.com/support/yandex-360/customers/mail/en/web/letter/create/send-many-letters) warns that large daily mailouts from an ordinary mailbox can be treated as spam and restricts recipients; SMTP/mail-client single-message recipient limit is 300 and limits can be lowered by Anti-Spam. This is a reason not to use `info@kenigevents.ru` as a raw mailbox SMTP sender for queue bursts.
- [Yandex Cloud Postbox](https://yandex.cloud/ru/services/postbox) advertises managed email infrastructure, DKIM/SPF/DMARC alignment, analytics/monitoring integrations, and up to 10 million emails/day; it is the preferred transport for this feature.

## Queue and safety requirements

All emails must go through a durable outbox queue. Direct send from request handlers, lifecycle update handlers, or static-site callbacks is forbidden.

Recommended entities:

- `email_recipient` / user profile projection: `user_id`, `email`, `email_verified`, `source`, `created_at`, `updated_at`.
- `event_follow`: `user_id`, `event_id`, `calendar_added_at`, `notification_email`, `notification_consent_at`, `unsubscribed_at`.
- `email_outbox`: one row per message attempt target with `id`, `kind`, `event_id`, `user_id`, `recipient_email_hash`, encrypted/plain operational recipient field according to privacy policy, `payload_json`, `status`, `attempts`, `next_run_at`, `last_error`, `provider_message_id`, `idempotency_key`, timestamps.
- `email_delivery_event`: provider callbacks / internal state transitions: `queued`, `sent`, `failed`, `bounced`, `complained`, `suppressed`, `skipped`.
- `email_suppression`: bounced/unsubscribed/complaint addresses.

Queue controls:

- Per-kind idempotency key: do not send duplicate confirmation/reminder/cancel/reschedule for the same `(user_id, event_id, lifecycle/version)`.
- Backoff with jitter for provider/network failures.
- Hard stop on repeated bounce/complaint and add to suppression list.
- Unsubscribe/control link for non-essential reminders; cancellation/reschedule for an event the user followed may be transactional but still must offer notification preference management.
- Do not include short links in email bodies; use full `kenigevents.ru` or source URLs.

Initial rate limits:

- Postbox can scale much higher, but start conservatively until deliverability is proven:
  - per sender: max 100 emails/hour and 1,000 emails/day;
  - per recipient: max 6 event emails/day, max 2 per event/day;
  - emergency cancellation batch: drain at max 30 emails/minute with queue metrics visible.
- Limits must be config-driven and lowerable without deploy.
- If using ordinary Yandex Mail SMTP as a fallback, cap to max 100/day and 20/hour because Yandex Mail anti-spam can lower limits for similar bulk messages; fallback must be explicitly marked degraded.

## Statistics and YDB

Persist operational statistics to YDB (or the project analytics store backed by YDB) in addition to local queue rows:

- queue depth by status/kind;
- oldest pending age;
- sends per hour/day by sender/kind;
- success/failure/bounce/complaint counts;
- retries and last error classes;
- per-event notification counts for confirmation/reminder/reschedule/cancellation;
- provider message id correlation;
- suppression/unsubscribe counts.

An admin/status command or dashboard must expose current queue stats before production enablement.

## Open implementation tasks

- Add user email/profile persistence for Yandex OAuth result and fallback email capture UI.
- Add event-follow persistence when users add events to calendar on the static site.
- Add lifecycle/version diff detector for `date`, `time`, `location_name`, `location_address`, `city`, `ticket_link`, and `lifecycle_status`.
- Add Postbox sender adapter with idempotency, retry/backoff, suppression handling, and provider message-id persistence.
- Add YDB stats sink and admin queue status view.
- Add static-page lifecycle badge and notification preference UI.
- Add tests for confirmation, 24-hour reminder, reschedule diff, cancellation disclaimer, rate limits, idempotency, retry, and suppression.
