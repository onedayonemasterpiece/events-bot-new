# INC-2026-06-16 tg_event_publish timeout duplicate

Status: mitigated
Severity: sev2
Service: events-bot Telegram event publishing (`@kldevents`)
Opened: 2026-06-16
Closed: —
Owners: events-bot
Related incidents: —
Related docs: `docs/features/tg-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/telegram-link-inspection.md`

## Summary

Two public Telegram event posts were created for the same event:

- `https://t.me/kldevents/625`
- `https://t.me/kldevents/626`

Both posts were `Depeche Mode – Devotional`, `16 июня 19:00`, `Сигнал, Леонова 22`. Production DB knew only message `625`. Runtime logs show the first Bot API send attempt timed out; retry sent the same event again and stored the retry result.

## User / Business Impact

- `@kldevents` subscribers saw duplicate event announcements for the same event.
- The duplicate could confuse users and weaken trust in the event channel.
- Production DB tracked only one of the public messages, so the untracked duplicate would not be updated/cleaned by normal idempotency.

## Detection

- Reported by operator from public Telegram links `625` and `626`.
- Telethon inspection confirmed both messages contained the same event content.
- Production DB and runtime-log probes localized the duplicate to `event.id=6066` and `tg_event_publish:6066`.

## Timeline

- 2026-06-16 00:10 UTC — event `6066` created from `https://t.me/meowafisha/7659`; `tg_event_publish` enqueued.
- 2026-06-16 00:13 UTC — `https://t.me/signalkld/10962` merged into the same event.
- 2026-06-16 09:35:51 UTC — `tg_event_publish:6066` picked for attempt 1.
- 2026-06-16 09:36:53 UTC — `publish_tg_event_announcement failed event_id=6066 desired_mode=photo_caption media=1` with Telegram `Request timeout error`; outbox run marked failed.
- 2026-06-16 09:38:27 UTC — automatic retry picked attempt 2.
- 2026-06-16 09:39:11 UTC — retry completed and stored `https://t.me/c/3954607218/625`.
- 2026-06-16 09:39:11 UTC — Telethon later confirmed public duplicate `626` with the same event text.
- 2026-06-16 09:52 UTC — duplicate message `626` deleted via Bot API; Telethon verified `625` remains and `626` is `message_not_found`.

## Root Cause

1. `tg_event_publish` treated Bot API send timeout as a normal retryable failure.
2. For `sendPhoto`/`sendMessage`/`sendMediaGroup`, a timeout after the request reaches Telegram is an uncertain write: Telegram can create the channel post while the bot loses the response containing `message_id`.
3. The automatic retry path had no pre-send reconciliation against public channel state, so it sent a second message and recorded only the retry response.

## Contributing Factors

- No dedicated uncertainty class for Telegram send timeouts.
- Outbox retry policy did not distinguish safe transient errors from "maybe already sent" Bot API write timeouts.
- Telegram link investigation instructions were not explicit enough: operator-provided `t.me` links should be read through Telethon first, not public HTML.

## Automation Contract

### Treat as regression guard when

- changing `publish_tg_event_announcement`;
- changing `JobTask.tg_event_publish` retry/backoff handling;
- changing Telegram Bot API send wrappers for event posts;
- changing Telegram incident/readback tooling for `t.me` links.

### Affected surfaces

- `main_part2.py`: Telegram event send path.
- `main.py`: `JobOutbox` retry decision.
- `@kldevents` public event channel.
- Production SQLite `event` / `joboutbox` rows for event post idempotency.

### Mandatory checks before closure or deploy

- Telethon verification of incident links.
- Production DB check for affected event/post id.
- Runtime log check for first timeout and retry.
- Unit coverage for uncertain send timeout suppressing automatic retry.
- `py_compile` for touched modules.
- Production `/healthz` after deploy, if code changed.
- Post-mitigation Telegram verification that duplicate public post is gone.

### Required evidence

- Telethon output for `625`/`626`.
- Bot API cleanup result for duplicate post.
- Runtime log lines around `tg_event_publish:6066`.
- Test command output.
- Deployed SHA reachable from `origin/main`, once corrective code is deployed.

## Immediate Mitigation

- Kept message `625` because production DB stored `tg_event_post_id=625`.
- Deleted untracked duplicate message `626` via Telegram Bot API.
- Re-checked through Telethon: `625` remains visible; `626` returns `message_not_found`.

## Corrective Actions

- Added `TelegramEventPublishUncertainSendError` for Bot API timeout/uncertain send results during new Telegram event post sends.
- Wrapped `sendMessage`, `sendPhoto`, and `sendMediaGroup` in the event publisher so uncertain write timeouts raise the dedicated error.
- Updated `JobOutbox` handling so this specific error is not automatically retried, preventing retry-created duplicates.
- Added project instruction and `telegram-link-inspection` skill requiring Telethon-first reading for operator-provided Telegram links.

## Follow-up Actions

- [ ] Consider an operator command/manual script to reconcile an uncertain `tg_event_publish` error by reading the latest `@kldevents` posts through Telethon and storing the found `message_id`.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Automatic retry is suppressed for uncertain Telegram send timeouts.
- `docs/features/tg-publishing/README.md` documents timeout uncertainty and manual reconciliation.
- `docs/operations/telegram-link-inspection.md` and project skill `telegram-link-inspection` document Telethon-first incident reads for Telegram links.
