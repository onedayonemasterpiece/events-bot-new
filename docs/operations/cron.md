# Cron Schedule

The bot uses APScheduler to run periodic maintenance tasks on a fixed schedule.

## Routing

When you need to change or inspect a schedule, use this route first instead of searching the repo from scratch:

- canonical ops doc and schedule policy: `docs/operations/cron.md`
- APScheduler job registration and default times: `scheduling.py`
- production schedule overrides for Fly: `fly.toml` (`[env]`)
- local/dev env template: `.env.example`

Rule of thumb:

- if you need to understand *what* runs and *why*, start here in `docs/operations/cron.md`;
- if you need to change fallback/default times in code, edit `scheduling.py`;
- if you need to change current production timings, edit `fly.toml`;
- if you need to keep local setup examples in sync, update `.env.example`.

Some jobs are lightweight (seconds), but **Kaggle/LLM/rendering** jobs can take **minutes or hours** (e.g. Telegram monitoring via Kaggle, VK auto-import via Smart Update, `/parse`, `/3di`).

To avoid parallel long-running operations (especially **manual** starts overlapping with **scheduled** ones), the scheduler uses a shared “heavy ops” gate:

- by default, scheduled heavy jobs **skip** if another heavy operation is already running and notify `ADMIN_CHAT_ID` about the skip;
- if you prefer waiting/serialization (run later instead of skipping), set `SCHED_HEAVY_GUARD_MODE=wait` (or legacy `SCHED_SERIALIZE_HEAVY_JOBS=1`).

VK crawling runs six times per day by default at `05:15`, `09:15`, `13:15`, `17:15`, `21:15` and `22:45` Europe/Kaliningrad time (`VK_CRAWL_TIMES_LOCAL` / `VK_CRAWL_TZ`).

## Observed runtimes (local runs)

Numbers below are from `ops_run` snapshots + local `/parse` logs (p50/p90/max). Use them to spread heavy jobs across the day.

- `tg_monitoring`: ~37m / ~2h53m / ~3h17m
- `/parse` (source parsing): ~9m / ~19m / (rare outliers up to ~6h+ when Kaggle stalls)
- `vk_auto_import`: ~45m / (few samples) / (rare outliers up to ~6h+ when unbounded)

## Recommended spacing (Europe/Kaliningrad)

Defaults were adjusted to reduce overlaps between the most common heavy jobs:

- nightly source parsing: `SOURCE_PARSING_TIME_LOCAL=04:30` (was `02:15`)
- `/3di` morning run: `THREEDI_TIMES_LOCAL=07:15,15:15,17:15` (was `05:30,15:15,17:15`; older default `03:15,15:15,17:15`)
- VK auto-import: `VK_AUTO_IMPORT_TIMES_LOCAL=06:15,10:15,12:00,18:30` with `VK_AUTO_IMPORT_LIMIT=15` by default, so queue draining relies on cadence instead of oversized single runs and stays away from the `08:00` daily announcement window and late-evening monitoring.

If you see skip notifications in admin chat often, spread the schedules further instead of switching to “wait”: skipping is a safety net, not a planning tool.

Skipped heavy-job attempts are now also written to `ops_run.status='skipped'` (with a reason), so `/general_stats` can show that the scheduler tried to start a job but skipped it before the job body ran.
Scheduled `vk_auto_import` and `tg_monitoring` entrypoints also create a bootstrap `ops_run` before resolving superadmin / entering the inner runner, so a 1ms APScheduler fire can no longer disappear without either a real run row or an explicit `skipped/error` record.
Scheduled guide slots now also participate in the shared heavy-job guard at the scheduler layer: if another heavy job (for example a stuck `vk_auto_import`) already owns the gate, the guide slot records `ops_run(kind='guide_monitoring', status='skipped', skip_reason='heavy_busy')` instead of waiting invisibly before `run_guide_monitor()` can materialize its own run.
`tg_monitoring`, scheduled `guide_excursions_full`, and `vk_auto_import` are additionally protected by a critical-run catch-up path: their APScheduler misfire grace is longer than the generic 30s default, the scheduler performs startup catch-up for the last missed slot within the configured lookback window, and a live watchdog re-checks `ops_run` after the slot. If APScheduler emits `JOB_SUBMITTED`/`JOB_MISSED` but the entrypoint never writes a materialized run, the watchdog dispatches the same scheduled entrypoint with a catch-up `run_id` instead of waiting for the next day/slot.
For `guide_excursions_full`, the watchdog only treats a materialized `ops_run(kind='guide_monitoring', details.mode='full')` as delivery; a same-day `light` scan must not suppress recovery of the missed `full` auto-publish slot.
If a catch-up dispatch only materializes another resource-busy `guide_monitoring` skip (for example `remote_telegram_session_busy` while `tg_monitoring` still owns the shared Kaggle/Telegram session), the slot stays pending in the watchdog memory and is retried on the next watchdog tick instead of being marked "completed" for the day.
`tg_monitoring` and `vk_auto_import` use `wait` as their default heavy-job guard mode so a nearby critical run queues behind an existing heavy operation instead of silently skipping, unless `SCHED_HEAVY_GUARD_MODE` explicitly overrides it. `guide_excursions_full` still records the initial `heavy_busy` skip, but its catch-up dispatch uses the same `wait` semantics so the missed daily digest runs as soon as the blocking heavy job releases the gate.

For admin-facing scheduled reports, the bot now resolves the target chat from the superadmin row in SQLite first; `ADMIN_CHAT_ID` is only a bootstrap/legacy fallback.

## Jobs

- **partner reminders** – reminds inactive partners after 09:00 local time.
- **cleanup old events** – removes past events after 03:00 local time and notifies the superadmin.
- **general stats** – daily operational system report (`/general_stats`) for the previous 24 hours.
- **Telegram daily announcements** – posts `/daily` channel announcements after configured `daily_time`; scheduler has per-channel in-process dedup guard (inflight + sent-today cache) to prevent repeated sends while one run is still in progress.
  - Daily build must treat shortlink enrichment as best-effort: if VK `utils.getShortLink` fails for one actor/token path (including `code=8 / Application is blocked`), the run must fall back to the next token or keep the original URL instead of stalling the whole announcement.
- **VK daily posts and polls** – publishes daily announcements and festival polls when posting times are reached and a VK group is configured.
- **VK auto queue import** – imports queued VK posts (`vk_inbox`) via Smart Update on a fixed schedule when enabled.
- **Telegraph pages sync** – refreshes month and weekend Telegraph pages after 01:00 local time. Disabled by default; enable with `ENABLE_NIGHTLY_PAGE_SYNC=1`. Nightly runs update both page content and the month navigation block.
- **Telegraph cache sanitizer** – probes and warms Telegram web preview for Telegraph pages (via Kaggle/Telethon), tracks missing `cached_page` (Instant View) and warns on missing preview `photo`, and enqueues rebuilds for persistent “no cached_page” failures. Skips past pages (ended events / past weekends / past months). Manual `/telegraph_cache_sanitize` updates a single Kaggle status message while polling (like `/tg`), scheduled runs post a final summary to `ADMIN_CHAT_ID` when configured. Disabled by default; enable with `ENABLE_TELEGRAPH_CACHE_SANITIZER=1`.
- **festival navigation rebuild** – rebuilds festival navigation and landing page nightly.
- **festival queue processing** – processes the festival queue (VK/TG/site sources) on a fixed schedule when enabled.
- **ticket sites queue** – scans ticket-site URLs discovered in Telegram posts (pyramida.info / домискусств.рф / qtickets) via Kaggle and enriches events through Smart Update.
- **source parsing** – nightly + midday `/parse` runs when enabled (midday skips Kaggle if source pages did not change).
- **3D previews** – scheduled `/3di` run for “new” events:
  - events without `preview_3d_url` and with `photo_count >= 2`;
  - events whose 3D preview was invalidated because the illustration set changed (Smart Update clears `preview_3d_url` when `photo_urls` change).
- **Video announce `/v tomorrow`** – optional scheduled automatic `/v` run when `ENABLE_V_TOMORROW_SCHEDULED=1` (legacy alias: `ENABLE_V_TEST_TOMORROW_SCHEDULED=1`).
  - canonical mode is production: it uses `VideoAnnounceScenario.run_tomorrow_pipeline(... test_mode=False)`;
  - `V_TOMORROW_TEST_MODE=1` can temporarily switch the same slot back to the legacy test-render path;
  - when `VIDEO_ANNOUNCE_STORY_ENABLED=1`, the same Kaggle notebook can also publish the finished `/v` video to Telegram stories from inside Kaggle and attach `story_publish_report.json` to the kernel output;
  - for story fanout use explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` when order matters; `main` channel + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` remain only as fallback;
  - recommended default window: `16:00 Europe/Kaliningrad`, so even the worst-case `225` minute runtime plus a `10` minute second-target story delay still finishes before the `21:00` audience window.
- **Telegram monitoring** – scheduled daily import from Telegram sources (channels/groups) via Kaggle when enabled.
- перед `push` мониторинг проверяет shared remote Telegram session guard по `kaggle_registry`; если другой Telegram-based Kaggle job ещё жив или его status lookup не дал надёжного ответа, текущий run фиксируется как `skipped` вместо запуска второй удалённой Telethon session.
- **Guide excursions monitoring** – scheduled guide-only Kaggle scans when `ENABLE_GUIDE_EXCURSIONS_SCHEDULED=1`.
  - if `ENABLE_GUIDE_DIGEST_SCHEDULED=1`, the same successful `full` run immediately publishes `new_occurrences` after server-side import instead of using a separate cron slot.
  - the `full` slot is also part of the critical scheduler catch-up path: after a `heavy_busy` skip or missed APScheduler fire, startup catch-up and the live watchdog replay the same scheduled `full` path within the configured lookback window instead of dropping the day.
  - guide path использует тот же shared guard и при конфликте remote session завершает слот как `skipped` с явной диагностикой, а не падает в неявный `AuthKeyDuplicatedError`.
- **Video announce `/v tomorrow`** – optional scheduled automatic `/v` run when `ENABLE_V_TOMORROW_SCHEDULED=1` (legacy alias: `ENABLE_V_TEST_TOMORROW_SCHEDULED=1`).
  - canonical mode is production: it uses `VideoAnnounceScenario.run_tomorrow_pipeline(... test_mode=False)`;
  - `V_TOMORROW_TEST_MODE=1` can temporarily switch the same slot back to the legacy test-render path;
  - once `ENABLE_V_TOMORROW_SCHEDULED=1` is enabled, the runtime should resolve timing/profile only from `V_TOMORROW_*`; legacy `V_TEST_TOMORROW_*` remain backward-compatible only for older env sets that still use the legacy enable flag;
  - on app startup the scheduler now performs a same-day catch-up for a missed `video_tomorrow` slot, so a Fly restart after `16:45` local still dispatches the run once instead of silently waiting until tomorrow;
  - if that same-day scheduled run did start but its only matching session for the target date ended in a recoverable early `FAILED` state (currently `missing video output` or `kaggle push failed`), startup catch-up and the live watchdog must allow one automatic rerun instead of treating the earlier `ops_run=success` marker as final delivery;
  - this recovery is intentionally one-shot per local day/target-date/profile tuple: once there is more than one matching failed attempt, the scheduler stops auto-rerunning and leaves the incident for manual handling;
  - while the process is alive, a separate in-process watchdog now verifies that the same-day `video_tomorrow` dispatch really happened after the slot; if APScheduler silently misses the slot, the watchdog runs the same scheduled path once after its grace window instead of waiting for the next restart;
  - `/healthz` now also treats missing/stopped APScheduler state and a missing `video_tomorrow` job as unhealthy, so Fly can recycle a runtime that is “HTTP alive” but lost its cron layer;
  - when `VIDEO_ANNOUNCE_STORY_ENABLED=1`, the same Kaggle notebook can also publish the finished `/v` video to Telegram stories from inside Kaggle and attach `story_publish_report.json` to the kernel output;
  - when production also sets `VIDEO_ANNOUNCE_STORY_REQUIRED=1`, `/healthz` must fail closed if story publish is unexpectedly disabled or the required auth/target env path is broken, so stale deploy branches cannot silently downgrade `/v` to mp4-only delivery;
  - story-enabled exact reruns and regular cron runs must share the same dataset/story path: if `VIDEO_ANNOUNCE_STORY_ENABLED=1`, both paths should generate `story_publish.json` and the encrypted auth datasets;
  - `CrumpleVideo` keeps its main render at `1080x1572`, but story upload must use a story-safe `1080x1920` derivative with padding instead of sending the raw non-`9:16` mp4;
  - for story fanout use explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` when order matters; `main` channel + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` remain only as fallback;
  - recommended default window: `16:45 Europe/Kaliningrad`, which centers the historical GPU render window (`~1:45..2:40`) near `19:00` while still keeping buffer before the `20:10` guide full scan.
- **kaggle recovery** – resumes in-flight Kaggle jobs after restarts, including `tg_monitoring` and `guide_monitoring`.
  - `guide_monitoring` now keeps a persisted copy of the downloaded results bundle under `GUIDE_MONITORING_RESULTS_STORE_ROOT` (default `/data/guide_monitoring_results`), so a restart during server import or scheduled digest publish can resume from the saved `results_path` instead of depending on a second Kaggle download.
  - for scheduled `full` guide runs with `ENABLE_GUIDE_DIGEST_SCHEDULED=1`, recovery is responsible for finishing both the import and the same-job digest auto-publish if the process died in between.

## Health Checks

- Fly probes `GET /healthz` every 15 seconds.
- `/healthz` no longer returns a blind static `ok`: it verifies that startup completed, the runtime heartbeat is fresh, required background tasks (`daily_scheduler`, `add_event_watch`, and `job_outbox_worker` when enabled) are alive, the bot session is open, and SQLite answers `SELECT 1`.
- The same applies to scheduler watchdog hooks: if `video_tomorrow` or critical scheduler watchdog support is enabled in runtime, `create_app()` must import the matching `scheduler_*_watchdog_*` callables from `scheduling.py`; a missing import is a production defect because it turns `/healthz` into `500` and silently disables watchdog ticks instead of degrading to a normal `503` health report.
- `add_event_watch` is allowed to restart a stalled add-event worker in place; the watchdog now updates the shared dequeue timestamp correctly instead of tripping an `UnboundLocalError` during stall recovery and poisoning `/healthz`.
- If any of those checks fail, `/healthz` returns `503` with a JSON payload describing the failing component. This lets Fly recycle machines that are still serving HTTP but stopped processing Telegram webhooks or scheduler loops correctly.

## Environment variables

- `SCHED_HEAVY_GUARD_MODE` – scheduled heavy jobs gate mode: `skip` (default), `wait`, or `off`.
- `SCHED_HEAVY_TRY_TIMEOUT_SEC` – try-acquire timeout in seconds for `SCHED_HEAVY_GUARD_MODE=skip` (default: `0.2`).
- `SCHED_SERIALIZE_HEAVY_JOBS` – legacy flag: when enabled (`1|true|yes|on`) it implies `SCHED_HEAVY_GUARD_MODE=wait` + extra in-scheduler serialization.
- `VK_USER_TOKEN` – user token for VK posts (scopes: wall,groups,offline).
- `VK_TOKEN` – optional group token used as a fallback.
- `EVBOT_DEBUG` – enables extra logging and queue statistics.
- `ENABLE_SOURCE_PARSING` – enable nightly source parsing schedule.
- `SOURCE_PARSING_TIME_LOCAL` / `SOURCE_PARSING_TZ` – nightly parse time in local time zone.
- `ENABLE_SOURCE_PARSING_DAY` – enable midday source parsing schedule.
- `SOURCE_PARSING_DAY_TIME_LOCAL` / `SOURCE_PARSING_DAY_TZ` – midday parse time in local time zone.
- `ENABLE_3DI_SCHEDULED` – enable scheduled `/3di` runs.
- `THREEDI_TIMES_LOCAL` / `THREEDI_TZ` – `/3di` schedule times in local time zone.
- `ENABLE_GENERAL_STATS` – enable scheduled `/general_stats` report.
- `GENERAL_STATS_TIME_LOCAL` / `GENERAL_STATS_TZ` – `/general_stats` schedule time in local time zone.
- `ENABLE_TELEGRAPH_CACHE_SANITIZER` – enable scheduled Telegraph cache sanitizer.
- `TELEGRAPH_CACHE_TIME_LOCAL` / `TELEGRAPH_CACHE_TZ` – Telegraph cache sanitizer schedule time in local time zone.
- `TELEGRAPH_CACHE_DAYS_BACK` / `TELEGRAPH_CACHE_DAYS_FORWARD` – active window for collecting pages to probe.
- `TELEGRAPH_CACHE_LIMIT_EVENTS` / `TELEGRAPH_CACHE_LIMIT_FESTIVALS` – max number of event/festival pages to probe per run (defaults to safe values).
- `TELEGRAPH_CACHE_REGEN_AFTER_RUNS` – enqueue rebuilds after N consecutive failing sanitizer runs (default `2`).
- `ENABLE_TG_MONITORING` – enable daily Telegram monitoring job.
- `TG_MONITORING_TIME_LOCAL` / `TG_MONITORING_TZ` – Telegram monitoring schedule time in local time zone.
- `TG_MONITORING_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for Telegram monitoring (default: `1800`).
- `TG_MONITORING_CATCHUP_LOOKBACK_SECONDS` – startup/watchdog lookback for the last missed Telegram monitoring slot (default: `86400`).
- `ENABLE_GUIDE_EXCURSIONS_SCHEDULED` – enable guide-only scheduled scans.
- `GUIDE_EXCURSIONS_LIGHT_TIMES_LOCAL` / `GUIDE_EXCURSIONS_FULL_TIME_LOCAL` / `GUIDE_EXCURSIONS_TZ` – guide monitoring light/full schedule in local time zone.
- `GUIDE_MONITORING_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for the critical scheduled `full` guide slot (default: `1800`).
- `GUIDE_MONITORING_CATCHUP_LOOKBACK_SECONDS` – startup/watchdog lookback for the last missed critical `full` guide slot (default: `86400`).
- `ENABLE_GUIDE_DIGEST_SCHEDULED` – after a successful scheduled `full` guide scan, automatically publish the `new_occurrences` digest in the same job instead of a separate cron slot.
- `ENABLE_V_TOMORROW_SCHEDULED` – enable scheduled automatic `/v` run for tomorrow (`ENABLE_V_TEST_TOMORROW_SCHEDULED` remains a legacy alias).
- `V_TOMORROW_TIME_LOCAL` / `V_TOMORROW_TZ` – local schedule for automatic `/v` run. When `ENABLE_V_TOMORROW_SCHEDULED=1`, these canonical vars own the slot; `V_TEST_TOMORROW_*` remain legacy aliases only for legacy-enabled envs.
- `V_TOMORROW_PROFILE` – video profile key for the scheduled `/v` run (default: `default`).
- `V_TOMORROW_TEST_MODE` – when enabled, force the scheduled slot back into the legacy test-render path instead of the production `/v` path.
- `V_TOMORROW_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for `video_tomorrow` (default: `600`), so short loop stalls near the slot do not silently drop the run.
- `V_TOMORROW_WATCHDOG_GRACE_SECONDS` – same-day local-time grace window after the slot before the independent watchdog dispatches a missing `video_tomorrow` run (default: `720`).
- `V_TOMORROW_WATCHDOG_INTERVAL_SECONDS` – polling interval for the independent `video_tomorrow` watchdog task (default: `60`).
- `VIDEO_KAGGLE_TIMEOUT_MINUTES` – `/v` Kaggle timeout in minutes (default `225`).
- `VIDEO_ANNOUNCE_STORY_ENABLED` – enable Kaggle-side story publish for `/v`.
- `VIDEO_ANNOUNCE_STORY_REQUIRED` – optional prod guard: when enabled, `/healthz` fails if `/v` story publish is disabled or obviously misconfigured.
- `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` / `VIDEO_ANNOUNCE_STORY_SESSION_ENV` – explicit auth source passed into Kaggle for story publish; the same encrypted auth runtime is also reused by notebook-side Telegram poster-cache rescue when direct poster URLs are dead.
- `SOURCE_CHANNEL_ID` – optional Telegram channel id embedded into the encrypted story auth payload so Kaggle can search that channel by filename for poster rescue instead of defaulting to Saved Messages.
- `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` – explicit ordered story targets list; when set, it overrides `main`-channel-derived ordering and `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON`.
- `VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL` – use the profile `main` channel as the first story target (default `1`).
- `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` – optional extra story targets with per-target `delay_seconds`.
- `VIDEO_ANNOUNCE_STORY_PERIOD_SECONDS` – story TTL passed to Telegram (default `86400`).
- `ENABLE_FESTIVAL_QUEUE` – enable festival queue schedule (disabled by default; next release keep off).
- `FESTIVAL_QUEUE_TIMES_LOCAL` / `FESTIVAL_QUEUE_TZ` – festival queue schedule times (default `03:30,16:30` local).
- `FESTIVAL_QUEUE_LIMIT` – optional limit of queue items per run.
- `ENABLE_TICKET_SITES_QUEUE` – enable scheduled ticket-sites queue processing.
- `TICKET_SITES_QUEUE_TIME_LOCAL` / `TICKET_SITES_QUEUE_TZ` – ticket-sites queue schedule time (default `11:20` local).
- `TICKET_SITES_QUEUE_LIMIT` – optional limit of queue items per scheduled run.
- `TICKET_SITES_QUEUE_INTERVAL_HOURS` – how often to rescan each URL after a successful run (default `24`).
- `ENABLE_V_TOMORROW_SCHEDULED` – enable scheduled automatic `/v` run for tomorrow (`ENABLE_V_TEST_TOMORROW_SCHEDULED` remains a legacy alias).
- `V_TOMORROW_TIME_LOCAL` / `V_TOMORROW_TZ` – local schedule for automatic `/v` run (`V_TEST_TOMORROW_*` remain legacy aliases).
- `V_TOMORROW_PROFILE` – video profile key for the scheduled `/v` run (default: `default`).
- `V_TOMORROW_TEST_MODE` – when enabled, force the scheduled slot back into the legacy test-render path instead of the production `/v` path.
- `VIDEO_KAGGLE_TIMEOUT_MINUTES` – `/v` Kaggle timeout in minutes (default `225`).
- `VIDEO_ANNOUNCE_STORY_ENABLED` – enable Kaggle-side story publish for `/v`.
- `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` / `VIDEO_ANNOUNCE_STORY_SESSION_ENV` – explicit auth source passed into Kaggle for story publish.
- `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` – explicit ordered story targets list; when set, it overrides `main`-channel-derived ordering and `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON`.
- `VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL` – use the profile `main` channel as the first story target (default `1`).
- `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` – optional extra story targets with per-target `delay_seconds`.
- `VIDEO_ANNOUNCE_STORY_PERIOD_SECONDS` – story TTL passed to Telegram (default `86400`).
- `ENABLE_VK_AUTO_IMPORT` – enable VK inbox auto import job.
- `VK_AUTO_IMPORT_TIMES_LOCAL` / `VK_AUTO_IMPORT_TZ` – VK auto-import schedule times in local time zone.
- `VK_AUTO_IMPORT_LIMIT` – max number of VK inbox rows to process per scheduled run (default `15`).
- `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC` – max seconds per VK inbox row before auto-import marks that post as `failed` and continues with the next row (default `1800`; set `<=0` to disable).
- `VK_AUTO_IMPORT_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for VK auto-import (default: `1800`).
- `VK_AUTO_IMPORT_CATCHUP_LOOKBACK_SECONDS` – startup/watchdog lookback for the last missed VK auto-import slot (default: `86400`).
- `CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS` / `CRITICAL_SCHED_WATCHDOG_INTERVAL_SECONDS` – live watchdog grace and polling interval for critical scheduled jobs (`tg_monitoring`, `guide_excursions_full`, `vk_auto_import`; defaults: `300` / `60` seconds).
- `ENABLE_KAGGLE_RECOVERY` – enable background Kaggle recovery loop.
- `KAGGLE_RECOVERY_INTERVAL_MINUTES` – recovery interval in minutes (default: 5).
- `KAGGLE_JOBS_PATH` – path to Kaggle recovery registry JSON (default: `/data/kaggle_jobs.json`).
- `TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES` – how long `tg_monitoring` recovery should keep rechecking Kaggle jobs that temporarily report `failed/error/cancelled` before dropping them as irrecoverable (default: `360`).
- `RUNTIME_HEALTH_HEARTBEAT_SEC` – how often the in-process runtime heartbeat updates (default: `15` seconds).
- `RUNTIME_HEALTH_STALE_SEC` – max allowed heartbeat age before `/healthz` turns unhealthy (default: `45` seconds, minimum `2x` heartbeat interval).
- `RUNTIME_HEALTH_STARTUP_GRACE_SEC` – startup grace window before “not ready yet” becomes a failing `/healthz` condition (default: `120` seconds).

To monitor real job durations, use the daily `/general_stats` report: it prints per-run `took=...` for `vk_auto_import` and `tg_monitoring` (and other ops-run instrumented jobs).
