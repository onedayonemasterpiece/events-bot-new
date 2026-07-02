# Event email notifications

Status: **foundation implemented / production sending dry-run** (2026-07-02)
Owner surfaces: static event pages, auth/profile, calendar-follow flow, event lifecycle updates, Yandex Cloud Postbox, YDB analytics.

## Scope

This feature covers transactional email notifications for users who explicitly add an event to their calendar from the static site while authenticated.

Notifications are not marketing mailouts. They are user-requested transactional messages about a specific followed event.

## Implementation status

Implemented in this branch:

- Yandex OAuth email capture is supported through `supabase/functions/yandex-userinfo/index.ts`: Yandex `default_email` / `emails[0]` is mapped to Supabase `email`.
- Static calendar buttons in `site/src/components/CalendarLink.astro` ask an authenticated user for explicit notification consent before enqueueing email notifications; if Yandex/Supabase has no email, the UI asks for a fallback email.
- `supabase/functions/event-email-follow/index.ts` validates the Supabase session, stores `user_notification_profiles` + `event_follows`, and enqueues `calendar_confirmation` plus a 24-hour reminder or a persisted `skipped` event.
- `supabase/migrations/20260702_event_email_notifications.sql` creates durable personalization-DB tables: `user_notification_profiles`, `event_follows`, `email_outbox`, `email_delivery_events`, `email_suppressions`, and `email_rate_limit_ledger` with explicit grants/RLS.
- `email_notifications/` provides the worker-side foundation: templates, idempotency keys, lifecycle/reschedule diff detection, cancellation disclaimer, conservative rate limiter, Postbox SMTP sender, and YDB stats adapter contract.
- `scripts/email_notifications_smoke.py` renders a dry-run queue/Postbox smoke without sending real email.

Still gated before production sending:

- `POSTBOX_ENABLED=0` / `POSTBOX_DRY_RUN=1` remains the safe default.
- YDB stats table/credentials must be provisioned and `EMAIL_YDB_STATS_ENABLED=1` must pass a real write smoke before `POSTBOX_DRY_RUN=0`.
- Postbox identity/DKIM/SPF/DMARC for `info@kenigevents.ru` must be verified by operators before real sends.
- Provider callback ingestion for bounce/complaint notifications is not enabled yet; suppression primitives are in place.

## Yandex OAuth email contract

Official Yandex ID docs say that when the OAuth token has the email-address permission, JSON userinfo includes `default_email` and `emails`; the JWT form exposes `email`. Therefore the Supabase custom OAuth provider for `custom:yandex` must request `login:email login:info` and must keep the `yandex-userinfo` adapter as the userinfo endpoint.

If Yandex does not return email, Supabase can still allow sign-in with `email_optional=true`, but event email notifications must stay disabled until the user provides a valid fallback email and gives explicit consent.

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

Persist operational statistics to YDB in addition to local queue rows. This is mandatory; local-only logs/SQLite counters are not enough:

- queue depth by status/kind;
- oldest pending age;
- sends per hour/day by sender/kind;
- success/failure/bounce/complaint counts;
- retries and last error classes;
- per-event notification counts for confirmation/reminder/reschedule/cancellation;
- provider message id correlation;
- suppression/unsubscribe counts.

An admin/status command or dashboard must expose current queue stats before production enablement.

YDB statistics are mandatory before real sending. The adapter contract is in `email_notifications/ydb_stats.py`:

- `EMAIL_YDB_STATS_ENABLED=1`
- `EMAIL_YDB_ENDPOINT`
- `EMAIL_YDB_DATABASE`
- `EMAIL_YDB_STATS_TABLE` or `EMAIL_YDB_TABLE_PREFIX`
- optional `EMAIL_YDB_SERVICE_ACCOUNT_KEY_FILE`

Until this is configured and smoke-tested, `YDBStatsSink` fails visibly and Postbox must remain dry-run. Every queue state transition and delivery result should be projected as `EmailStatsEvent` with provider correlation, dry-run flag, recipient hash, event id, kind/status and metadata.

## Smoke / tests

Dry-run smoke:

```bash
python3 scripts/email_notifications_smoke.py --to operator@example.invalid
```

Targeted tests:

```bash
pytest tests/test_event_email_notifications.py
```

## Regression contract

This feature is a regression surface for `INC-2026-07-02-buynov-cancellation-lifecycle-gap.md`: followed cancelled/rescheduled events must have a queued transactional notification path; emails must stay durable/idempotent and must not bypass Postbox/YDB dry-run gates.

## Open implementation tasks

- Provision and verify real YDB stats tables/UPSERT wiring.
- Deploy `event-email-follow` after migration is applied to the personalization Supabase project.
- Wire the production worker to drain `email_outbox` with Postbox only after YDB stats write smoke passes.
- Add provider callback ingestion for bounce/complaint notifications.
- Add admin queue status view.
- Add static-page lifecycle badge and notification preference UI.
