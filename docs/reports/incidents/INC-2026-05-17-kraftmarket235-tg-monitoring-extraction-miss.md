# INC-2026-05-17 Kraftmarket235 Telegram Monitoring Extraction Miss

Status: open
Severity: sev2
Service: Telegram Monitoring producer / server import / daily recently-added inventory
Opened: 2026-05-17
Closed: —
Owners: Codex / Telegram Monitoring maintainers
Related incidents: `INC-2026-04-27-tg-monitoring-sticky-skipped-post`, `INC-2026-04-30-tg-monitoring-work-schedule-false-skips`, `INC-2026-05-05-80-stories-source-coverage`, `INC-2026-05-11-lecturer-name-and-title-dropped-from-description`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/prod-data.md`, `docs/llm/request-guide.md`, `docs/llm/prompts.md`

## Summary

Telegram post `https://t.me/kraftmarket39/235` is a clear future event announcement for спектакль `8 женщин` on 2026-05-22 19:00 at `Городской центр культуры и искусства, Курортный проспект 11, Зеленоградск`, with a ticket URL `https://voroh.ru/event/1022458/` and price from 350. It did not create an event and does not appear in the production DB as a scanned/imported event.

The fresh production evidence shows this is not a server-side skipped import after extraction. The 2026-05-16 Telegram Monitoring Kaggle output did scan the post and persisted it in `telegram_results.json`, but the producer returned `"events": []` for message `235`. Because no event payload existed, the server did not create `event`, `event_source`, or `telegram_scanned_message` rows for the message. The source cursor in production still shows `telegram_source.username='kraftmarket39'`, `last_scanned_message_id=232`, `last_scan_at=2026-05-15 01:18:47.476921`.

This incident was opened from investigation evidence. The first prevention patch now adds a producer single-event LLM rescue and a server-side zero-event diagnostic trail, but production catch-up/import and deploy have not been performed yet.

## User / Business Impact

- A valid future performance is missing from public announcement inventory and can be absent from daily/recently-added surfaces.
- Operators cannot see a durable skip reason in production DB because the message has no `telegram_scanned_message` row.
- This repeats the `kraftmarket39` source-coverage family, but at a different layer: producer false-negative `events=[]` instead of server-side `skipped`.

## Detection

- Operator reported that `https://t.me/kraftmarket39/235` is not in the current daily announcement.
- Fresh production snapshot: `artifacts/db/future_quality_audit_2026-05-17_070713.sqlite`, `PRAGMA quick_check=ok`.
- Public Telegram embed was inspected and confirms the source text and event facts for `8 женщин`.
- Runtime/Kaggle artifact evidence from run `fc3551b8f315477aa54c3b4a4d6205b1` was available on Fly under `/tmp/tg-monitor-fc3551b8f315477aa54c3b4a4d6205b1/`.

## Timeline

- 2026-05-15 01:18 UTC — production DB has the last successful durable `kraftmarket39` message at id `232`; `telegram_source.last_scanned_message_id=232`.
- 2026-05-16 06:29 UTC — `kraftmarket39/235` was published. Public embed text announces спектакль `8 женщин`, 2026-05-22 19:00, venue `Городской центр культуры и искусства, Курортный проспект 11, Зеленоградск`, ticket URL `voroh.ru/event/1022458`.
- 2026-05-16 21:40 UTC — scheduled `tg_monitoring` run `fc3551b8f315477aa54c3b4a4d6205b1` started, then was cancelled after about 519 seconds.
- 2026-05-16 23:38 UTC — recovery import downloaded the same Kaggle output and imported results successfully: `messages_processed=252`, `events_imported=19`, `events_created=10`, `events_merged=9`.
- 2026-05-16 23:25 UTC inside Kaggle log — source scan for `kraftmarket39` started with `last_id=232`, `latest_id=236`, then finished with `messages=4`, `processed=4`, `messages_with_events=0`, `events=0`.
- 2026-05-17 07:13 UTC — fresh production snapshot confirmed no `telegram_scanned_message`, `event_source`, or `event` row for `kraftmarket39/235`.
- 2026-05-17 UTC — this incident record was opened.

## Evidence

Production DB evidence from `artifacts/db/future_quality_audit_2026-05-17_070713.sqlite`:

- `telegram_source`: `username='kraftmarket39'`, `enabled=1`, `last_scanned_message_id=232`, `last_scan_at='2026-05-15 01:18:47.476921'`.
- No row in `telegram_scanned_message` for `(kraftmarket39, 235)`.
- No row in `event_source` for `source_chat_username='kraftmarket39'` and `source_message_id=235`.
- No event row with `source_post_url` or source text containing `kraftmarket39/235`.

Kaggle output evidence from `/tmp/tg-monitor-fc3551b8f315477aa54c3b4a4d6205b1/telegram_results.json`:

- message `235` is present with `source_link='https://t.me/kraftmarket39/235'`.
- text begins `22 мая состоится показ спектакля «8 женщин» по пьесе Робера Тома...`.
- links include `https://voroh.ru/event/1022458/`.
- posters include one uploaded image.
- `events` is an empty list.

Runtime log evidence from `/tmp/tg-monitor-fc3551b8f315477aa54c3b4a4d6205b1/telegram-monitor-bot.log`:

- `source.start username=kraftmarket39 type=supergroup last_id=232 latest_id=236 latest_date=2026-05-16T18:02:09+00:00`.
- `source.done username=kraftmarket39 messages=4 processed=4 messages_with_events=0 events=0 first_id=236 last_id=233 cutoff_hit=False`.

## Root Cause

The current evidence narrows the original miss to a TelegramMonitor producer false negative:

1. The post was scanned by the Kaggle producer and included in `telegram_results.json`.
2. The producer returned `events=[]` even though the post contains explicit date, time, venue, ticket link, and price anchors.
3. The server import had no event payload to process, so it could not create an event and did not persist a durable no-event diagnostic row for message `235`.

Prevention root causes fixed in the first patch:

1. The main extraction prompt did not have a general clear-single-event rescue outside lecture/exhibition special cases.
2. Server import silently ignored new messages with `events=[]`, so DB audits could not distinguish “not scanned” from “producer returned no events”.
3. Sticky-skip reprocessing refused rows with `error`, so a future diagnostic row would have risked blocking a fixed producer output unless `producer_zero_events` became an explicit reprocessable diagnostic class.

Still-open investigation areas:

- replay the exact producer call to confirm which Gemma decision path returned `[]`;
- source-level handling for `kraftmarket39` supergroup messages with `post_author` or forwarded-from metadata may still need more coverage;
- production catch-up/import remains pending.

## Contributing Factors

- The server has no durable `telegram_scanned_message` row for scanned messages with `events=[]`, so DB-only audits make the post look unscanned rather than scanned-and-false-negative.
- `kraftmarket39` has prior source-coverage incidents, but earlier fixes focused on server skips and festival/source coverage, not producer `events=[]` false negatives.
- The daily/recently-added surface does not have a guard that checks public source cursor drift against clear event posts.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/TelegramMonitor/telegram_monitor.py` extraction prompts, non-event guards, forwarding/repost handling, or source scan windows;
- changing `source_parsing/telegram/handlers.py` import of messages with zero extracted events;
- changing `telegram_scanned_message` idempotency, diagnostics, or source cursor updates;
- running catch-up/import-only flows for `kraftmarket39` or daily recently-added inventory.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- production `telegram_source`, `telegram_scanned_message`, `event`, `event_source`, `ops_run`
- Kaggle TelegramMonitor output artifacts
- `/daily` and recently-added inventory

### Mandatory checks before closure or deploy

- Save a minimal replay artifact for `kraftmarket39/235` under `tests/replays/INC-2026-05-17-kraftmarket235-tg-monitoring-extraction-miss/`.
- Replay `kraftmarket39/235` through the TelegramMonitor producer and server import boundary on a prod snapshot/shadow DB.
- The replay must produce one source-grounded candidate/event for `8 женщин`, 2026-05-22 19:00, `Городской центр культуры и искусства`, `Курортный проспект 11`, `Зеленоградск`, ticket URL `https://voroh.ru/event/1022458/`.
- Add at least one negative/opposite control so market/repost noise does not become blanket event creation.
- If code changes are needed, add producer-level regression coverage for the exact post shape.
- If production data is repaired, verify `event_source.source_url='https://t.me/kraftmarket39/235'` exists and the event appears in relevant public/recently-added surfaces or record why rerun is not applicable.
- Release-governance checks before deploy: clean task worktree, commit reachable from `origin/main`, docs/changelog synced.

### Required evidence

- Replay input and output for `kraftmarket39/235`.
- Pre/post DB query output showing the missing row and repaired row.
- Test output for targeted producer/import tests.
- Runtime/Kaggle evidence for the catch-up run if production data is repaired.
- Deployed SHA reachable from `origin/main` if code changes ship.

## Immediate Mitigation

- Incident record opened and production/Kaggle evidence preserved locally in `artifacts/codex/future-quality-2026-05-17/telegram_results_fc3551b8.json`.
- Prevention patch added:
  - `kaggle/TelegramMonitor/telegram_monitor.py` runs a narrow LLM single-event rescue when the structural detector sees a clear dated/timed event with ticket or venue evidence and the main extractor returned `[]`.
  - `source_parsing/telegram/handlers.py` persists `telegram_scanned_message.status='skipped'`, `events_extracted=0`, `error='producer_zero_events:clear_event_signals'` for structurally clear zero-event messages.
  - `_should_reprocess_incomplete_scan` allows `producer_zero_events` rows to be retried when a later payload contains importable events.
- Code prevention was deployed on 2026-05-17. Catch-up import, row repair for
  the exact `kraftmarket39/235` source attachment, and public surface rebuild
  are still pending.

## Corrective Actions

- [x] Producer clear-single-event LLM rescue added for date + time + ticket/venue posts.
- [x] Server zero-event diagnostic row added for clear event-shaped posts with `events=[]`.
- [x] `producer_zero_events` diagnostic rows are reprocessable when later producer output contains actual events.
- [x] Regression coverage added for the `kraftmarket39/235` structural shape and a plain-news negative control.
- [ ] Replay exact `kraftmarket39/235` producer call and run production catch-up/import.

## Follow-up Actions

- [ ] Owner: Telegram Monitoring / no due date / add replay fixture for `kraftmarket39/235`.
- [ ] Owner: Telegram Monitoring / no due date / identify why producer extraction returned `events=[]` for a clear dated event with venue and ticket URL.
- [ ] Owner: Telegram Monitoring / no due date / decide whether messages scanned with `events=[]` should persist a diagnostic `telegram_scanned_message` row or equivalent audit trail.
- [ ] Owner: operator / no due date / after prevention checks pass, run compensating catch-up/import for `kraftmarket39/235` and verify public surfaces.

## Release And Closure Evidence

- deployed SHA: `bba67b5aa78c4bd6c516348e4e5b4cfd26cd9c35`
- deploy path: clean linked worktree `hotfix/2026-05-17-cherryflash-eco-promo`, pushed to `origin/main`, deployed with `flyctl deploy -a events-bot-new-wngqia`
- regression checks: `/home/dev/projects/events-bot-new/.venv/bin/pytest tests/test_promo.py tests/test_video_announce_popular_review.py tests/test_vk_auto_queue_import.py tests/test_tg_candidate_location_grounding.py tests/test_tg_monitor_gemma4_contract.py -q` -> `91 passed`; `py_compile` for touched Python modules passed.
- post-deploy verification: Fly image `events-bot-new-wngqia:deployment-01KRTH9RXB7P1NV3X86S4CDWAT`; Fly machine `48e42d5b714228`, version `1100`, checks `1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`. Exact `kraftmarket39/235` catch-up remains a separate follow-up.

## Prevention

Producer false negatives need first-class replay coverage. The server should also retain enough diagnostic evidence for scanned zero-event messages so future DB audits can distinguish "not scanned" from "scanned but extractor returned no events."
