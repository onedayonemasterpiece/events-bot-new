# Implementation prompt: Event email notifications via Yandex Cloud Postbox + YDB

Use this prompt in a fresh implementation window. This feature is **not implemented yet** as of 2026-07-02; only requirements are documented.

## Context

Repository: `events-bot-new`.

Incident source: cancellation of event `5988` (`Александр Буйнов: Лучшие песни`) showed that lifecycle changes must notify users who explicitly added an event to their calendar.

Canonical requirements: read first:

1. `AGENTS.md`
2. `docs/README.md`
3. `docs/features/event-email-notifications/README.md`
4. `docs/reports/incidents/INC-2026-07-02-buynov-cancellation-lifecycle-gap.md`
5. Existing auth/static/calendar docs/code paths before changing data model.

Important: do **not** treat this as a marketing-mail feature. These are transactional notifications for user-followed events.

## Goal

Implement the production-ready foundation for event email notifications:

- Yandex OAuth email capture / notification email persistence.
- Event follow / calendar-add persistence.
- Durable queued email outbox.
- Yandex Cloud Postbox serverless sender adapter.
- YDB-backed queue statistics and delivery statistics.
- Cancellation/reschedule/reminder/confirmation enqueue logic.
- Admin/status visibility.
- Tests and dry-run safety.

Sender: `info@kenigevents.ru`.

Transport: **Yandex Cloud Postbox only** for normal production sending. Do not implement ordinary Yandex Mail SMTP as the default path.

## Non-negotiable requirements

### 1. Email identity and consent

- If a user authenticates through Yandex OAuth, store the email returned by Yandex as notification email.
- If OAuth does not return email, UI/API must require the user to provide an email before enabling email notifications.
- User must explicitly consent to event email notifications when adding/following an event.
- User can unsubscribe/manage notification preferences.
- Store enough state to avoid emailing users who did not consent.

### 2. Event-follow lifecycle

When authenticated user adds an event to their calendar / follows event:

- Persist `event_follow` / equivalent durable row with `user_id`, `event_id`, notification email, consent timestamp, unsubscribe state.
- Enqueue confirmation email.
- Enqueue reminder approximately 24h before event start.
- If event is less than 24h away, do not send a 24h reminder; persist a `skipped` delivery event with reason.

When followed event changes:

- On date/time/location/address/city/ticket-or-registration link changes, enqueue reschedule/update email with a field-by-field diff.
- On `lifecycle_status='cancelled'`, enqueue cancellation email with cancellation note and disclaimer:
  `Полюбить Калининград Анонсы не является организатором события; информация об отмене получена из публичных источников/сообщений организатора.`
- Do not send duplicate notification for same `(user, event, lifecycle/version/change-set)`.

### 3. Durable queue

All sends must go through a durable queue. Forbidden:

- direct Postbox send from request handler;
- direct Postbox send from Smart Update/lifecycle handler;
- direct Postbox send from static-site callback.

Implement an outbox with at least:

- `id`
- `kind`: `calendar_confirmation`, `event_reminder_24h`, `event_rescheduled`, `event_cancelled`
- `event_id`
- `user_id`
- recipient email or encrypted operational field according to existing privacy conventions
- `recipient_email_hash`
- `payload_json`
- `status`: `pending`, `sending`, `sent`, `failed`, `bounced`, `complained`, `suppressed`, `skipped`
- `attempts`
- `next_run_at`
- `last_error`
- `provider_message_id`
- `idempotency_key`
- timestamps.

Implement delivery-event history:

- queued
- sending
- sent
- failed
- retry_scheduled
- bounced
- complained
- suppressed
- skipped
- unsubscribed.

Implement suppression table/list:

- bounced addresses
- complaints
- explicit unsubscribe
- repeated hard failures.

### 4. YDB statistics are mandatory

The user explicitly asked whether all email statistics are in YDB. The correct implementation target is: **yes, all operational email queue/delivery statistics must be persisted to YDB**.

Persist to YDB, not only SQLite/logs:

- queue depth by status and kind;
- oldest pending age;
- sends per hour/day by sender/kind;
- failures per hour/day by error class;
- retry counts;
- bounce/complaint/suppression counts;
- per-event notification counts by kind;
- provider message id correlation;
- per-recipient daily counters (hashed recipient is acceptable if raw email storage is not allowed);
- rate-limit decisions and skipped reasons.

If existing project YDB client/config is absent, implement a minimal adapter layer and config contract first, with tests/mocks. Do not silently replace YDB with local-only stats.

Local DB rows may still be used as the operational outbox if that matches current architecture, but YDB stats/event projections must be written for every queue state transition and delivery result. If YDB write fails, record the failure visibly and decide whether sending should pause; do not lose stats silently.

### 5. Yandex Cloud Postbox sender

Use Yandex Cloud Postbox via its intended API/SDK or SMTP-compatible endpoint only as a Postbox identity, not as a personal mailbox SMTP workaround.

Required sender config, names may be adjusted to project conventions:

- `POSTBOX_ENABLED`
- `POSTBOX_DRY_RUN`
- `POSTBOX_REGION` / endpoint
- `POSTBOX_ACCESS_KEY_ID` / secret source through existing secrets mechanism
- `POSTBOX_FROM_EMAIL=info@kenigevents.ru`
- `POSTBOX_FROM_NAME=Полюбить Калининград | Анонсы`
- rate limit envs:
  - `EMAIL_MAX_PER_HOUR=100`
  - `EMAIL_MAX_PER_DAY=1000`
  - `EMAIL_MAX_PER_RECIPIENT_PER_DAY=6`
  - `EMAIL_MAX_PER_RECIPIENT_EVENT_PER_DAY=2`
  - `EMAIL_CANCEL_BATCH_PER_MINUTE=30`

Do not commit secrets. Use `.env.example` only for variable names/placeholders.

### 6. Rate limiting and anti-spam safety

Start conservative even though Postbox can scale:

- max 100 emails/hour per sender;
- max 1,000 emails/day per sender;
- max 6 event emails/day per recipient;
- max 2 emails/day per recipient per event;
- cancellation emergency drain max 30/minute;
- configurable without code changes.

The worker must check rate limits before sending and persist both send and skip/defer decisions to queue rows and YDB stats.

### 7. Templates

Implement safe templates for:

- confirmation: event added/followed;
- 24h reminder;
- reschedule/update with diff;
- cancellation with disclaimer.

Email body requirements:

- include event title, date/time, location, current event URL;
- include organizer/source URL when available;
- use full `kenigevents.ru`/source URLs, not vk.cc/shortlinks;
- cancellation/reschedule templates must be factual and source-grounded;
- no marketing blasts or unrelated recommendations.

### 8. Integration points

Investigate existing code before implementation. Likely integration points:

- static event page calendar-add/follow path;
- auth/profile/Yandex OAuth path;
- `event.lifecycle_status`, Smart Update lifecycle changes;
- event anchor updates: `date`, `time`, `location_name`, `location_address`, `city`, `ticket_link`;
- job/outbox scheduler conventions;
- health/admin commands.

Add an admin/status command or dashboard endpoint showing current queue stats:

- pending/sending/sent/failed by kind;
- oldest pending;
- rate-limit counters;
- YDB stats write health;
- recent failures;
- suppression counts.

### 9. Safety and rollout

Default must be safe:

- `POSTBOX_ENABLED=0` or `POSTBOX_DRY_RUN=1` until identity/domain/DKIM/SPF/DMARC are verified.
- Dry-run should render templates, create outbox rows, write YDB stats with `dry_run=true`, but not send real email.
- Provide a narrow manual smoke command for one test recipient/event after explicit operator enablement.
- Do not send historical/catch-up emails to all existing calendar users unless a separate explicit backfill plan is approved.

### 10. Tests

Add tests for:

- OAuth email stored; missing email blocks notifications until supplied.
- Event follow creates outbox confirmation + reminder when consent exists.
- Reminder skipped for events less than 24h away and skip stats written.
- Cancellation creates one email per follower with disclaimer and idempotency.
- Reschedule diff includes changed fields only.
- Duplicate lifecycle/update processing does not duplicate emails.
- Rate limiter defers sends and writes YDB stats.
- Bounce/complaint suppression prevents future sends.
- Postbox dry-run does not call network sender.
- Postbox send success stores provider message id and YDB stat.
- YDB write failure is visible and tested.

### 11. Documentation and release evidence

Update:

- `docs/features/event-email-notifications/README.md`
- `docs/routes.yml` if new docs/scripts are added
- `.env.example`
- `CHANGELOG.md`
- incident record if implementation changes the Buynov cancellation contract.

Before final report:

- run targeted tests;
- run dry-run queue smoke;
- verify no real email was sent unless explicitly enabled;
- report exact YDB tables/paths used for stats;
- report exact Postbox identity/domain readiness status;
- report how to view current queue stats.

## Expected final answer from implementation agent

The final answer must explicitly say:

- whether real Postbox sending is enabled or still dry-run;
- where the durable outbox is stored;
- where YDB stats are stored;
- which templates are implemented;
- how cancellation/reschedule/reminder emails are enqueued;
- how rate limits are enforced;
- what tests passed;
- any blockers such as missing Postbox identity/DKIM/YDB credentials.
