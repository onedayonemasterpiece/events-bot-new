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

Region Talk autonomous discovery is opt-in via
`ENABLE_REGION_TALK_SCHEDULED=1`. It runs at `06:20`, `13:20`, and `21:20`
Europe/Kaliningrad by default (`REGION_TALK_TIMES_LOCAL` / `REGION_TALK_TZ`).
Each slot invokes the bounded queue orchestrator chain through
`scripts/region_talk_scheduled_runner.py`; it has a cross-process lock, strict
non-interactive credential preflight, 14-day JSONL log retention and
`ops_run(kind='region_talk')` evidence. Scheduler health reports the earliest
next Region Talk slot. The job sends accepted candidates only to the prepared
operator chat and does not enable target-channel publishing. It is deliberately
outside the legacy global heavy-job gate: CandidateReport/ImageDiagnostic use
dedicated `DISCOVERY1`/`DISCOVERY2` sessions and every Region Talk resource has
its own kernel/lock guard, so an unrelated long render cannot starve all three
daily discovery slots.
An independent `region_talk_watchdog` runs every five minutes. It reads the
durable `ops_run` ledger for the latest due slot and, within a three-hour
lookback, resumes a missing/crashed/failed session through the same wrapper.
`running`/`success` rows, the wrapper's file lock and a six-attempt cap prevent
duplicate or unbounded recovery. This makes a Fly deploy/process replacement a
recoverable interruption instead of silently deferring discovery until the
next day's slot. Health reports the watchdog separately while
`region_talk_next_run` remains the next real daily slot.
After each bounded orchestrator session, the runner recalculates the next
14-day `1 article + 1 social post` selection plan in YDB. The planner uses
actual target-publication history plus BGE anti-vector ordering, overwrites only
future unlocked slots and does not connect any Telegram human session or call
public Telegram/VK publishing APIs.

## Observed runtimes (local runs)

Numbers below are from `ops_run` snapshots + local `/parse` logs (p50/p90/max). Use them to spread heavy jobs across the day.

- `tg_monitoring`: ~37m / ~2h53m / ~3h17m
- `/parse` (source parsing): ~9m / ~19m / (rare outliers up to ~6h+ when Kaggle stalls)
- `vk_auto_import`: ~45m / (few samples) / (rare outliers up to ~6h+ when unbounded)

## Recommended spacing (Europe/Kaliningrad)

Defaults were adjusted to reduce overlaps between the most common heavy jobs:

- nightly source parsing: `SOURCE_PARSING_TIME_LOCAL=04:30` (was `02:15`)
- `/3di` morning run: `THREEDI_TIMES_LOCAL=07:15,15:15,17:15` (was `05:30,15:15,17:15`; older default `03:15,15:15,17:15`)
- VK auto-import: `VK_AUTO_IMPORT_TIMES_LOCAL=06:15,10:15,12:00,15:30,18:30` with `VK_AUTO_IMPORT_LIMIT=15` by default, so queue draining relies on cadence instead of oversized single runs, picks up fresh daytime `pending` items after the `13:15` VK crawl, avoids the `/3di` `15:15` slot, and stays away from the `08:00` daily announcement window and late-evening monitoring.

If you see skip notifications in admin chat often, spread the schedules further instead of switching to “wait”: skipping is a safety net, not a planning tool.

Skipped heavy-job attempts are now also written to `ops_run.status='skipped'` (with a reason), so `/general_stats` can show that the scheduler tried to start a job but skipped it before the job body ran.
Scheduled `vk_auto_import` and `tg_monitoring` entrypoints also create a bootstrap `ops_run` before resolving superadmin / entering the inner runner, so a 1ms APScheduler fire can no longer disappear without either a real run row or an explicit `skipped/error` record.
Scheduled guide slots now also participate in the shared heavy-job guard at the scheduler layer: if another heavy job (for example a stuck `vk_auto_import`) already owns the gate, the guide slot records `ops_run(kind='guide_monitoring', status='skipped', skip_reason='heavy_busy')` instead of waiting invisibly before `run_guide_monitor()` can materialize its own run.
`tg_monitoring`, scheduled `guide_excursions_full`, and `vk_auto_import` are additionally protected by a critical-run catch-up path: their APScheduler misfire grace is longer than the generic 30s default, and a live `critical_scheduler_watchdog` interval job re-checks `ops_run` after the last local slot inside the configured lookback window. If APScheduler emits `JOB_SUBMITTED`/`JOB_MISSED` but the entrypoint never writes a materialized successful run, or the process is killed and startup cleanup marks the materialized run `crashed`, the watchdog dispatches the same scheduled entrypoint with a catch-up `run_id` instead of waiting for the next day/slot. The watchdog resolves the last local slot, not just "today", so a 23:40 slot remains recoverable after local midnight.
For `guide_excursions_full`, the watchdog only treats a materialized `ops_run(kind='guide_monitoring', details.mode='full')` as delivery; a same-day `light` scan must not suppress recovery of the missed `full` auto-publish slot.
If a catch-up dispatch only materializes another resource-busy `guide_monitoring` skip (for example `remote_telegram_session_busy` while another Kaggle run still owns the shared Telegram session or Kaggle status lookup is temporarily `UNKNOWN`), the slot stays pending in the watchdog memory and is deferred by `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS` instead of being marked "completed" for the day or retried every watchdog tick.
For `tg_monitoring`, the watchdog also checks the persistent Kaggle recovery registry before dispatching a catch-up. If a `tg_monitoring` kernel is already registered, the watchdog defers for `TG_MONITORING_REMOTE_BUSY_RETRY_SECONDS` (default `300`) and lets `kaggle_recovery` poll/import that kernel, avoiding a second `TELEGRAM_AUTH_BUNDLE_S22` push while the remote Telethon session may still be active. A materialized `remote_telegram_session_busy` skip gets the same short retry hold.
`tg_monitoring` and `vk_auto_import` use `wait` as their default heavy-job guard mode so a nearby critical run queues behind an existing heavy operation instead of silently skipping, unless `SCHED_HEAVY_GUARD_MODE` explicitly overrides it. `guide_excursions_full` still records the initial `heavy_busy` skip, but its catch-up dispatch uses the same `wait` semantics so the missed daily digest runs as soon as the blocking heavy job releases the gate.

For admin-facing scheduled reports, the bot now resolves the target chat from the superadmin row in SQLite first; `ADMIN_CHAT_ID` is only a bootstrap/legacy fallback.

## Jobs

- **Social metrics batch** — один лёгкий interval-job, включаемый
  `ENABLE_SOCIAL_METRICS_KAGGLE=1`, каждые
  `SOCIAL_METRICS_BATCH_INTERVAL_MINUTES` (default `30`) собирает все due
  публикации пакетами. VK запросы идут по издателю чанками до 100 ID; Telegram
  остаётся выключен, пока не задана отдельная role-scoped сессия
  `TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR`. Kaggle reader использует bounded human-like
  jitter, последовательные channel batches и не обращается к E2E/S22 sessions.
  Job не создаёт расписание на каждый пост и после простоя сам подбирает свежую
  допустимую точку `1h|6h|24h|72h`; postponed→live VK resolution также идёт
  пакетно в том же Kaggle run, а атомарный slot claim не допускает второй push.
  Каноника данных и флагов:
  `docs/features/post-metrics/README.md`.

- **Postbox transactional outbox worker** – every
  `EMAIL_OUTBOX_WORKER_INTERVAL_SECONDS` (default `60`) claims only eligible
  transactional Postbox rows from the personalization Supabase control plane.
  Enabled by `ENABLE_EMAIL_OUTBOX_WORKER`; database global/transactional switches
  and `dry_run_only` remain the authoritative send gate.
- **Postbox email health monitor** – every
  `EMAIL_OUTBOX_MONITOR_INTERVAL_SECONDS` (default `300`) checks PII-free outbox
  lag/failure counters and the dedicated YMQ trigger DLQ, then sends cooldown-bound
  warning/alarm codes to the Telegram superadmin. Enabled by
  `ENABLE_EMAIL_OUTBOX_MONITOR`.

- **partner reminders** – reminds inactive partners after 09:00 local time.
- **cleanup old events** – removes past events after 03:00 local time and notifies the superadmin.
- **general stats** – daily operational system report (`/general_stats`) for the previous 24 hours.
- **Telegram daily announcements** – posts `/daily` channel announcements after configured `daily_time`; scheduler has per-channel durable DB claims (`daily_announcement_guard`) plus an in-process guard to prevent repeated sends while one run is still in progress and to survive releases/restarts after the slot has already been claimed/sent.
  - Daily build must treat shortlink enrichment as best-effort: if VK `utils.getShortLink` fails for one actor/token path (including `code=8 / Application is blocked`), the run must fall back to the next token or keep the original URL instead of stalling the whole announcement.
  - Optional premium emoji editor: when `ENABLE_TG_PREMIUM_EMOJI_EDITOR=1`, successfully sent daily messages are edited via a dedicated Telethon session after `TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS` (default `150`) to replace free markers with custom emoji labels. Canonical feature doc: `docs/features/tg-premium-emojis-update/README.md`.
- **VK daily posts and polls** – publishes daily announcements and festival polls when posting times are reached and a VK group is configured.
  - VK daily announcements are split into multiple `wall.post` calls when the generated section exceeds `VK_DAILY_POST_MAX_CHARS` (default `12000`) so a busy day does not fail with VK `message_character_limit`. The split preserves event cards when possible and the slot is marked sent only after every chunk returns a VK post URL.
  - VK daily has two compact slots configured by `/vktime`: morning `today` publishes `НЕ ПРОПУСТИТЕ СЕГОДНЯ` plus a separate full-date line, evening `added` publishes `N ДОБАВИЛИ В АНОНС`. Event rows are one-line entries and link to the event VK post when one exists.
- **Promo VK runner** – every `PROMO_VK_INTERVAL_MINUTES` (default `30`) checks
  active promo activities with `surface IN ('vk_publication', 'vk_repost')`.
  It counts organic Smart Update posts and recorded promo exposures inside each
  activity's rolling window, schedules missing VK event posts, and reposts a
  recent source-community post when the repost activity is below its daily
  target. New actions start only inside the activity active window (default
  09:00-21:00 Europe/Kaliningrad) and are spread across even due-slots rather
  than published as one batch. The runner is enabled by default via
  `ENABLE_PROMO_VK_SCHEDULER`.
- **Poll to Repost debug runner** – when `ENABLE_POLL_TO_FORWARD_DEBUG=1`, runs
  at minutes `0,30`. It publishes at most one LLM-generated debug poll per local
  hour to `POLL_TO_FORWARD_DEBUG_TARGET_CHAT` (default `@keniggpt`) during the
  configured daytime window, then resolves due polls about 30 minutes later and
  forwards the chosen managed Telegram event post from
  `POLL_TO_FORWARD_SOURCE_CHAT` (default `@kldevents`). LLM is a hard
  dependency: if topic generation or winner/event choice fails, the slot is
  skipped and no deterministic fallback is published.
- **VK auto queue import** – imports queued VK posts (`vk_inbox`) via Smart Update on a fixed schedule when enabled.
- **VK past-event post prune** – twice a day deletes managed `klgdevents` event posts whose event is already in the past and that gained no reposts/story shares (`reposts.count == 0`) and no comments (`comments.count == 0`), so the community feed stops surfacing stale events. Default times `02:30,14:30` Europe/Kaliningrad (`VK_POST_PRUNE_TIMES_LOCAL`), enabled in production via `ENABLE_VK_POST_PRUNE`. Only posts whose `Event.source_vk_post_url` points at `-VK_EVENTS_GROUP_ID` are eligible; external VK-import source walls, pinned posts, future/ongoing events (`end_date`), and daily/poll/promo posts (not stored in `source_vk_post_url`) are never touched. The job is part of `_HEAVY_JOB_IDS`, so it skips (and notifies `ADMIN_CHAT_ID`) when another heavy job holds the gate instead of competing for the VK token. Canonical doc: `docs/features/vk-publishing/autodeletevkposts.md`.
- **Telegraph pages sync** – refreshes month and weekend Telegraph pages after 01:00 local time. Disabled by default; enable with `ENABLE_NIGHTLY_PAGE_SYNC=1`. Nightly runs update both page content and the month navigation block.
- **Telegraph cache sanitizer** – probes and warms Telegram web preview for Telegraph pages (via Kaggle/Telethon), tracks missing `cached_page` (Instant View) and warns on missing preview `photo`, and enqueues rebuilds for persistent “no cached_page” failures. Skips past pages (ended events / past weekends / past months). Manual `/telegraph_cache_sanitize` updates a single Kaggle status message while polling (like `/tg`), scheduled runs post a final summary to `ADMIN_CHAT_ID` when configured. Disabled by default; enable with `ENABLE_TELEGRAPH_CACHE_SANITIZER=1`.
- **Exhibition duplicate audit** – read-only daily acceptance gate for the Smart Update Vector Identity Gate rollout. It scans the file-backed SQLite DB with `scripts/inspect/audit_public_exhibition_duplicates.py` semantics, checks current/future canonical `/vystavki/` rows, joins `scripts/inspect/audit_identity_gate_rollout.py` counters from `event_identity_decision_log`, records `ops_run(kind='exhibition_duplicate_audit')`, and alerts the superadmin/admin chat on high-confidence duplicate pairs where at least one side was added inside the configured rollout window (`event.added_at >= EXHIBITION_DUPLICATE_AUDIT_SINCE_DATE` when set, otherwise `current_date - EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS`; schema fallback counts all pairs). Disabled by default; enable after `SMART_UPDATE_IDENTITY_GATE=enforce` with `ENABLE_EXHIBITION_DUPLICATE_AUDIT=1`.
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
  - for story fanout use explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` when order matters; production keeps `me` as the blocking upload target and marks channel reposts as `required=true`, so Telegram boost failures do not waste the render but still fail the final publish status; `main` channel + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` remain only as fallback;
  - recommended default window: `16:00 Europe/Kaliningrad`, so even the worst-case `225` minute runtime plus a `10` minute second-target story delay still finishes before the `21:00` audience window.
- **Telegram monitoring** – scheduled daily import from Telegram sources (channels/groups) via Kaggle when enabled.
- перед `push` мониторинг проверяет shared remote Telegram session guard по `kaggle_registry`; если другой Telegram-based Kaggle job с тем же `remote_telegram_auth_scope` ещё жив или его fresh status lookup не дал надёжного ответа, текущий run фиксируется как `skipped` вместо запуска второй удалённой Telethon session. Старые registry-записи с транзиентной ошибкой status lookup (`HTTP 5xx`, сеть, SSL, timeout) перестают блокировать после `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` (default `390`) и помечаются в job meta как `stale_transient_status_lookup_failure`. Jobs с разными explicit auth scopes могут идти параллельно; unknown scope считается конфликтующим.
- **Guide excursions monitoring** – scheduled guide-only Kaggle scans when `ENABLE_GUIDE_EXCURSIONS_SCHEDULED=1`.
  - if `ENABLE_GUIDE_DIGEST_SCHEDULED=1`, the same successful `full` run immediately publishes `new_occurrences` after server-side import instead of using a separate cron slot.
  - if `ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED=1`, a separate morning one-card visual schedule digest runs at `GUIDE_VISUAL_DIGEST_TIME_LOCAL` (production: `10:30 Europe/Kaliningrad`) from already-saved guide occurrences. It publishes to the guide Telegram targets immediately, schedules VK with `GUIDE_VISUAL_DIGEST_VK_DELAY_SECONDS=600`, and a lightweight due-job checks every 5 minutes to publish the VK Story after `GUIDE_VISUAL_DIGEST_VK_STORY_DELAY_SECONDS=900`.
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
  - story-enabled scheduled video runs assign one configured video lane from `VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS`; each run writes the selected auth env into its own session dataset, leases `telegram_session:env:<ENV>`, and can push the same local source notebook into a lane-specific Kaggle kernel target from `VIDEO_ANNOUNCE_CHERRYFLASH_KERNEL_REFS` or `VIDEO_ANNOUNCE_CRUMPLE_KERNEL_REFS`;
  - scheduled `/v tomorrow` must not finish `ops_run(kind='video_tomorrow')` as `success` until the run reaches a confirmed Kaggle handoff: `videoannounce_session.kaggle_dataset` is set and `kaggle_kernel_ref` is a non-local Kaggle slug, or the session is already terminal with durable artifact/publication evidence. A local-only `SELECTED` session is an attempted slot, not product delivery;
  - if all configured video lanes are busy, the scheduled `/v tomorrow` attempt is recorded as `ops_run.status='skipped'` with `skip_reason='video_lanes_busy'` and no new `SELECTED` session/dataset is created. A materialized `SELECTED` scheduled slot also suppresses same-day watchdog cloning so a stale pre-handoff attempt does not multiply while an operator investigates;
  - `CrumpleVideo` keeps its main render at `1080x1572`, but story upload must use a story-safe `1080x1920` derivative with padding instead of sending the raw non-`9:16` mp4;
  - for story fanout use explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` when order matters; production keeps `me` as the blocking upload target and marks channel reposts as `required=true`, so Telegram boost failures do not waste the render but still fail the final publish status; `main` channel + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` remain only as fallback;
  - recommended default window: `16:45 Europe/Kaliningrad`, which centers the historical GPU render window (`~1:45..2:40`) near `19:00` while still keeping buffer before the `20:10` guide full scan.
- **CherryFlash `popular_review`** – optional scheduled daily popularity story when `ENABLE_V_POPULAR_REVIEW_SCHEDULED=1`.
  - the scheduled path uses `VideoAnnounceScenario.run_popular_review_pipeline(wait_for_handoff=True)` and must not mark `ops_run(kind='video_popular_review')` as `success` until `videoannounce_session.kaggle_dataset` is set and `kaggle_kernel_ref` is a real Kaggle slug, not `local:CherryFlash`;
  - startup catch-up and the live watchdog retry the same local-day slot when the only matching CherryFlash session failed before Kaggle handoff;
  - duplicate prevention is based on remote handoff evidence: a matching session with a non-local kernel ref plus `cherryflash-session-*` dataset suppresses catch-up even if local status later drifts.
- **CherryFlash partner tracks** – always-registered daily partner story tracks with per-track defaults and watchdog retry until `22:00 Europe/Kaliningrad`.
  - `partner_eco_nature_001`: `12:30 Europe/Kaliningrad` (`V_PARTNER_TRACK_ECO_TIME_LOCAL`);
  - `partner_konb_library_001`: `12:37 Europe/Kaliningrad` (`V_PARTNER_TRACK_KONB_TIME_LOCAL`), defaulting to `prod` publish mode after the 2026-05-17 launch: Telegram channel story to `@kaliningradlibrary` and VK community story to `vk.com/konb39`, both best-effort and independent. `setting.partner_track_konb_publish_mode=test` is still available for preview runs as a normal Telegram channel post to `@keniggpt`;
  - `partner_region_east_001`: `18:30 Europe/Kaliningrad` (`V_PARTNER_TRACK_EAST_TIME_LOCAL`).
  - The КОНБ production fanout treats the Telegram channel story as best-effort, so `BOOSTS_REQUIRED` does not stop VK story publication.
  - The east-region track stops same-day watchdog launches after the scheduled attempt plus one retry when the Business story target is still missing from the encrypted cache; the next attempt is the next daily schedule.
  - Partner scheduled attempts use the same video-lane preflight as `/v tomorrow`: a busy lane pool is an explicit `skipped/video_lanes_busy` attempt with no new `SELECTED` session, while real created sessions must reach confirmed Kaggle handoff before the `ops_run` is marked `success`.
- **kaggle recovery** – resumes in-flight Kaggle jobs after restarts, including `tg_monitoring` and `guide_monitoring`.
  - `guide_monitoring` now keeps a persisted copy of the downloaded results bundle under `GUIDE_MONITORING_RESULTS_STORE_ROOT` (default `/data/guide_monitoring_results`), so a restart during server import or scheduled digest publish can resume from the saved `results_path` instead of depending on a second Kaggle download.
  - before and after copying a new guide output bundle, the server prunes old `guide-excursions-*` directories in that store by age/count/size/free-space guard. This is production-critical because the store shares Fly `/data` with SQLite; without retention, old recovery bundles can trigger `database or disk is full` and drop daily scheduler slots.
  - for scheduled `full` guide runs with `ENABLE_GUIDE_DIGEST_SCHEDULED=1`, recovery is responsible for finishing both the import and the same-job digest auto-publish if the process died in between.
  - for `tg_monitoring`, Kaggle status lookup HTTP 5xx must not leave a permanent shared remote-session lock. Recovery first tries the normal status path; if status lookup fails but `telegram_results.json` is already downloadable from the kernel output, that output is terminal evidence and must be imported before clearing `kaggle_registry`.

## Health Checks

- Fly service-level health checks must be visible in `flyctl config show` as `services.http_checks` for `GET /healthz`; legacy-looking `services.checks` entries in local `fly.toml` are not enough if the deployed config omits them.
- Fly probes `GET /healthz` every 15 seconds after startup grace.
- `/healthz` no longer returns a blind static `ok`: it verifies that startup completed, the runtime heartbeat is fresh, required background tasks (`daily_scheduler`, `add_event_watch`, and `job_outbox_worker` when enabled) are alive, the bot session is open, and SQLite answers `SELECT 1`. For `job_outbox_worker`, health also includes recent loop exceptions as `tasks.job_outbox_worker_loop`, so a task that stays alive while every cycle fails is unhealthy.
- The same applies to scheduler watchdog hooks: if `video_tomorrow` or critical scheduler watchdog support is enabled in runtime, `create_app()` must import the matching `scheduler_*_watchdog_*` callables from `scheduling.py`; a missing import is a production defect because it turns `/healthz` into `500` and silently disables watchdog ticks instead of degrading to a normal `503` health report.
- `/healthz` exposes `critical_scheduler_watchdog`, `tg_monitoring`, and `vk_auto_import` when those production jobs are enabled. Missing jobs or missing next-run timestamps are health evidence, not a benign omission from the payload.
- `add_event_watch` is allowed to restart a stalled add-event worker in place; the watchdog now updates the shared dequeue timestamp correctly instead of tripping an `UnboundLocalError` during stall recovery and poisoning `/healthz`.
- If any of those checks fail, `/healthz` returns `503` with a JSON payload describing the failing component. This lets Fly recycle machines that are still serving HTTP but stopped processing Telegram webhooks or scheduler loops correctly.

## Environment variables

- `SCHED_HEAVY_GUARD_MODE` – scheduled heavy jobs gate mode: `skip` (default), `wait`, or `off`.
- `SCHED_HEAVY_TRY_TIMEOUT_SEC` – try-acquire timeout in seconds for `SCHED_HEAVY_GUARD_MODE=skip` (default: `0.2`).
- `SCHED_SERIALIZE_HEAVY_JOBS` – legacy flag: when enabled (`1|true|yes|on`) it implies `SCHED_HEAVY_GUARD_MODE=wait` + extra in-scheduler serialization.
- `VK_AUTO_IMPORT_HEAVY_MODE` – guard mode inside VK auto-import itself:
  `off`, `try`, or `wait`. Default is `off` for manual `/vk_auto_import` so
  operators can debug/import while Telegram Monitoring is polling a remote
  Kaggle kernel, and `wait` for scheduled runs. If forced to `wait`, the bot
  reports which heavy operation it is waiting for.
- `VK_USER_TOKEN` – user token for VK posts (scopes: wall,groups,offline). Local/dev runs may also provide the same token as `VK_ACCESS_TOKEN4`.
- `VK_TOKEN` – optional group token used as a fallback.
- `VK_EVENTS_GROUP_ID` – target group id for Smart Update event posts with photos/video attachments; defaults to `VK_AFISHA_GROUP_ID`.
- `VK_PHOTOS_ENABLED_DEFAULT` – default for VK event-post media attachments before `/vkphotos` writes an explicit DB setting; default `true`.
- `VK_ACCESS_TOKEN5` – VK user token bundled into CherryFlash Kaggle story secrets for VK wall/story publication.
- `ENABLE_PROMO_VK_SCHEDULER` – enable the lightweight promo VK runner; default
  `true`.
- `PROMO_VK_INTERVAL_MINUTES` – promo VK runner interval; default `30`.
- `ENABLE_POLL_TO_FORWARD_DEBUG` – enable debug Poll to Repost scheduler; default
  `false` locally, enabled in production `fly.toml` for the debug rollout.
- `POLL_TO_FORWARD_DEBUG_TARGET_CHAT` – debug poll/repost target; default
  `@keniggpt`.
- `POLL_TO_FORWARD_SOURCE_CHAT` – Telegram source chat for `forward_message`;
  default `@kldevents`.
- `POLL_TO_FORWARD_DEBUG_START_HOUR` / `POLL_TO_FORWARD_DEBUG_END_HOUR` –
  local debug create window; production debug rollout uses `9` and `24`
  (end hour is exclusive, so the last create slot is `23:00` local; the
  quiet night window is `00:00-08:30`).
- `POLL_TO_FORWARD_DEBUG_RESOLVE_AFTER_MINUTES` – delay before resolving debug
  polls; default `30`.
- `POLL_TO_FORWARD_LLM_MODEL` – LLM model for topic and winner selection;
  default `gemini-3.1-flash-lite`.
- `EVBOT_DEBUG` – enables extra logging and queue statistics.
- `ENABLE_SOURCE_PARSING` – enable nightly source parsing schedule.
- `SOURCE_PARSING_TIME_LOCAL` / `SOURCE_PARSING_TZ` – nightly parse time in local time zone.
- `ENABLE_SOURCE_PARSING_DAY` – enable midday source parsing schedule.
- `SOURCE_PARSING_DAY_TIME_LOCAL` / `SOURCE_PARSING_DAY_TZ` – midday parse time in local time zone.
- `ENABLE_3DI_SCHEDULED` – enable scheduled `/3di` runs.
- `THREEDI_TIMES_LOCAL` / `THREEDI_TZ` – `/3di` schedule times in local time zone.
- `SMART_UPDATE_IDENTITY_GATE` – Smart Update create-path identity gate mode (`off`, `shadow`, `enforce`).
- `SMART_UPDATE_IDENTITY_VECTOR_RECALL` / `SMART_UPDATE_IDENTITY_VECTOR_TOP_K` / `SMART_UPDATE_IDENTITY_VECTOR_MIN_SIMILARITY` / `SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS` – vector-candidate recall knobs for the identity gate.
- `SMART_UPDATE_IDENTITY_EMBEDDING_MODEL` / `SMART_UPDATE_IDENTITY_EMBEDDING_DIM` / `SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV` – embedding generation config for vector recall; default key env is `GOOGLE_API_KEY4`.
- `PERSONALIZATION_SUPABASE_URL` / `PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY` – preferred Supabase access for the vector RPC used by the identity gate; if unset, the runtime falls back to existing `SUPABASE_URL` plus `SUPABASE_SERVICE_KEY`/`SUPABASE_KEY`.
- `ENABLE_EXHIBITION_DUPLICATE_AUDIT` – enable scheduled read-only `/vystavki/` duplicate audit after identity-gate enforce.
- `EXHIBITION_DUPLICATE_AUDIT_TIME_LOCAL` / `EXHIBITION_DUPLICATE_AUDIT_TZ` – audit schedule time in local time zone (default `07:45 Europe/Kaliningrad`).
- `EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS` – acceptance/reporting window for the Prometheus/ops metrics (default `14`); with `event.added_at` available, the `*_since_total` metrics include duplicate pairs where either row was added inside the window.
- `EXHIBITION_DUPLICATE_AUDIT_RAISE_ON_DUPLICATES` – when enabled (default), high-confidence pairs finish the ops run as `failed` and raise a scheduler job error.
- `EXHIBITION_DUPLICATE_AUDIT_SINCE_DATE` – optional explicit rollout start date (`YYYY-MM-DD`) for post-enforce acceptance; overrides the derived `current_date - EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS` start.
- `scripts/inspect/audit_identity_gate_rollout.py` – manual/read-only rollout report for `event_identity_decision_log`; Prometheus output includes `events_identity_gate_*_since_total` counters for decisions, vetoes, fail-safes, vector errors, final duplicate-probe vetoes, modes/reasons, plus `events_identity_gate_env_ready{check=...}` booleans for enforce/vector/Supabase/Google/audit env readiness without printing secret values.
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
- `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS` – cooldown before the critical guide watchdog retries a `full` catch-up whose latest materialized attempt was skipped only because the shared remote Telegram/Kaggle session was busy or status was `UNKNOWN` (default: `3600`, minimum: `300`).
- `/healthz` exposes `guide_excursions_light` and `guide_excursions_full`; missing guide job visibility is not acceptable evidence that the guide scheduler is healthy.
- `GUIDE_MONITORING_RESULTS_STORE_ROOT` – persistent store for downloaded Guide monitoring Kaggle output bundles (default: `/data/guide_monitoring_results` on Fly).
- `GUIDE_MONITORING_RESULTS_STORE_RETENTION_DAYS` / `GUIDE_MONITORING_RESULTS_STORE_MAX_RUNS` / `GUIDE_MONITORING_RESULTS_STORE_MAX_MB` / `GUIDE_MONITORING_RESULTS_STORE_MIN_FREE_MB` – retention guard for the persistent result store (defaults: `2` days, `6` runs including the current one, `256` MB total, and `256` MB free-space target). This guard runs before and after a new bundle is copied so old guide recovery artifacts cannot fill the SQLite volume.
- `ENABLE_GUIDE_DIGEST_SCHEDULED` – after a successful scheduled `full` guide scan, automatically publish the `new_occurrences` digest in the same job instead of a separate cron slot.
- `ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED` / `GUIDE_VISUAL_DIGEST_TIME_LOCAL` / `GUIDE_VISUAL_DIGEST_TZ` – separate morning visual guide digest slot (one 1080×1350 card, up to 5 excursions) in addition to the old guide digest.
- `GUIDE_VISUAL_DIGEST_TARGET_CHATS` – optional Telegram targets for the visual digest; defaults to `GUIDE_DIGEST_TARGET_CHATS`.
- `GUIDE_VISUAL_DIGEST_VK_DELAY_SECONDS` / `GUIDE_VISUAL_DIGEST_VK_STORY_DELAY_SECONDS` – VK wall postponed delay and follow-up VK Story delay (production: 600s and 900s).
- `ENABLE_V_TOMORROW_SCHEDULED` – enable scheduled automatic `/v` run for tomorrow (`ENABLE_V_TEST_TOMORROW_SCHEDULED` remains a legacy alias).
- `V_TOMORROW_TIME_LOCAL` / `V_TOMORROW_TZ` – local schedule for automatic `/v` run. When `ENABLE_V_TOMORROW_SCHEDULED=1`, these canonical vars own the slot; `V_TEST_TOMORROW_*` remain legacy aliases only for legacy-enabled envs.
- `V_TOMORROW_PROFILE` – video profile key for the scheduled `/v` run (default: `default`).
- `V_TOMORROW_TEST_MODE` – when enabled, force the scheduled slot back into the legacy test-render path instead of the production `/v` path.
- `V_TOMORROW_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for `video_tomorrow` (default: `600`), so short loop stalls near the slot do not silently drop the run.
- `V_TOMORROW_WATCHDOG_GRACE_SECONDS` – same-day local-time grace window after the slot before the independent watchdog dispatches a missing `video_tomorrow` run (default: `720`).
- `V_TOMORROW_WATCHDOG_INTERVAL_SECONDS` – polling interval for the independent `video_tomorrow` watchdog task (default: `60`).
- `ENABLE_V_POPULAR_REVIEW_SCHEDULED` – enable scheduled CherryFlash `popular_review`.
- `V_POPULAR_REVIEW_TIME_LOCAL` / `V_POPULAR_REVIEW_TZ` – local schedule for CherryFlash `popular_review` (default: `10:15 Europe/Kaliningrad`).
- `V_POPULAR_REVIEW_WATCHDOG_GRACE_SECONDS` – same-day local-time grace window after the CherryFlash slot before the independent watchdog dispatches a missing local-only pre-handoff run (default: `900`).
- `V_PARTNER_TRACK_ECO_TIME_LOCAL` / `V_PARTNER_TRACK_KONB_TIME_LOCAL` / `V_PARTNER_TRACK_EAST_TIME_LOCAL` – local schedule overrides for the always-registered CherryFlash partner tracks.
- `VIDEO_KAGGLE_TIMEOUT_MINUTES` – `/v` Kaggle timeout in minutes (default `225`).
- `VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES` – after the fixed timeout, keep waiting instead of failing if `kaggle_run_ledger.last_heartbeat_at` is fresher than this window (default `20`).
- `VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES` – bounded wait extension after a fresh heartbeat at timeout (default `30`).
- `VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES` – hard ceiling for a video poller even when heartbeats continue (default `720`).
- `VIDEO_ANNOUNCE_STORY_ENABLED` – enable Kaggle-side story publish for `/v`.
- `VIDEO_ANNOUNCE_STORY_REQUIRED` – optional prod guard: when enabled, `/healthz` fails if `/v` story publish is disabled or obviously misconfigured.
- `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` / `VIDEO_ANNOUNCE_STORY_SESSION_ENV` – explicit auth source passed into Kaggle for story publish; the same encrypted auth runtime is also reused by notebook-side Telegram poster-cache rescue when direct poster URLs are dead. For production parallelism with remote monitoring, prefer `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV=TELEGRAM_AUTH_BUNDLE_STORY`; keep `TELEGRAM_AUTH_BUNDLE_S22` reserved for monitoring unless a deliberate maintenance window serializes all remote Telegram jobs.
- `VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS` – comma-separated pool of Telethon auth bundle env names available for parallel story-enabled video renders. Example for two lanes: `TELEGRAM_AUTH_BUNDLE_STORY,TELEGRAM_AUTH_BUNDLE_S22_VIDEO1`. When unset, the legacy single `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` / `VIDEO_ANNOUNCE_STORY_SESSION_ENV` scope is used.
- `VIDEO_ANNOUNCE_CHERRYFLASH_KERNEL_REFS` / `VIDEO_ANNOUNCE_CRUMPLE_KERNEL_REFS` – optional comma-separated Kaggle kernel targets matching the video-lane order. They let one repo-local source notebook be pushed into isolated per-lane Kaggle slugs so two long CPU renders do not overwrite/poll the same remote kernel. Each configured target must already exist and be accessible before it is added to production env: normal `kernels_push` against a missing slug can fail with `Notebook not found` after the session dataset has already been created. The launcher preflights configured targets before dataset creation and marks the session `PUBLISH_BLOCKED` if a target is missing. Current two-lane production mapping is CherryFlash `zigomaro/cherryflash,zigomaro/cherryflash-video-lane-1` and CrumpleVideo `zigomaro/crumple-video,zigomaro/crumple-video-video1`. Lane choice must skip active `RENDERING` sessions and active/non-expired `kaggle_resource_lease` rows for `telegram_session:env:<ENV>` before creating a scheduled session or pushing a notebook; if no lane is free, the scheduler records `skipped/video_lanes_busy` without creating another `SELECTED` row or dataset. Without these vars, runs with the same real Kaggle kernel slug remain serialized by the existing rendering guard.
- `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` – explicit ordered story targets list; when set, it overrides `main`-channel-derived ordering and `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON`. Production should keep the first blocking target as `me` and put required channel reposts after it with `required=true`, so downstream `BOOSTS_REQUIRED` channel failures stay visible, do not prevent render delivery, and still fail the final publish status.
- `SOURCE_CHANNEL_ID` – optional Telegram channel id embedded into the encrypted story auth payload so Kaggle can search that channel by filename for poster rescue instead of defaulting to Saved Messages.
- `VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL` – use the profile `main` channel as the first story target (default `1`).
- `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` – optional extra story targets with per-target `delay_seconds`.
- `VIDEO_ANNOUNCE_STORY_PERIOD_SECONDS` – story TTL passed to Telegram (default `86400`).
- `KAGGLE_RESOURCE_LEASE_RENEW_TTL_SECONDS` – active resource-lease TTL written on live `alive` callbacks (default `10800`).
- `KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS` – minimum interval for storing another `alive` row in `kaggle_run_event` for the same phase while still updating the ledger every callback (default `300`).
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
- `VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES` / `VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES` / `VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES` – timeout safety for long CPU renders with fresh Kaggle heartbeats.
- `VIDEO_ANNOUNCE_STORY_ENABLED` – enable Kaggle-side story publish for `/v`.
- `VIDEO_ANNOUNCE_STORY_AUTH_BUNDLE_ENV` / `VIDEO_ANNOUNCE_STORY_SESSION_ENV` – explicit auth source passed into Kaggle for story publish. Prefer a dedicated `TELEGRAM_AUTH_BUNDLE_STORY` when story jobs may overlap with monitoring on `TELEGRAM_AUTH_BUNDLE_S22`.
- `VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS` – optional comma-separated pool of video Telegram auth bundle envs for parallel story-enabled renders; lane-specific kernel targets come from `VIDEO_ANNOUNCE_CHERRYFLASH_KERNEL_REFS` / `VIDEO_ANNOUNCE_CRUMPLE_KERNEL_REFS`.
- `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` – explicit ordered story targets list; when set, it overrides `main`-channel-derived ordering and `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON`. Production should keep the first blocking target as `me` and put required channel reposts after it with `required=true`, so downstream `BOOSTS_REQUIRED` channel failures stay visible, do not prevent render delivery, and still fail the final publish status.
- `VIDEO_ANNOUNCE_STORY_USE_MAIN_CHANNEL` – use the profile `main` channel as the first story target (default `1`).
- `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` – optional extra story targets with per-target `delay_seconds`.
- `VIDEO_ANNOUNCE_STORY_PERIOD_SECONDS` – story TTL passed to Telegram (default `86400`).
- `ENABLE_VK_AUTO_IMPORT` – enable VK inbox auto import job.
- `VK_AUTO_IMPORT_TIMES_LOCAL` / `VK_AUTO_IMPORT_TZ` – VK auto-import schedule times in local time zone.
- `VK_AUTO_IMPORT_LIMIT` – max number of VK inbox rows to process per scheduled run (default `15`).
- `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC` – max seconds per VK inbox row before auto-import marks that post as `failed` and continues with the next row (default `1800`; set `<=0` to disable).
- `VK_AUTO_IMPORT_MISFIRE_GRACE_SECONDS` – per-job APScheduler misfire window for VK auto-import (default: `1800`).
- `VK_AUTO_IMPORT_CATCHUP_LOOKBACK_SECONDS` – startup/watchdog lookback for the last missed VK auto-import slot (default: `86400`).
- `ENABLE_VK_POST_PRUNE` – enable twice-daily auto-deletion of past-event `klgdevents` VK posts (default: enabled in production).
- `VK_POST_PRUNE_TIMES_LOCAL` / `VK_POST_PRUNE_TZ` – prune schedule times in local time zone (default `02:30,14:30` Europe/Kaliningrad).
- `VK_POST_PRUNE_LIMIT` – max candidate posts checked/deleted per run (blast-radius cap, default `300`).
- `VK_POST_PRUNE_DRY_RUN` – when enabled, log what would be deleted without calling `wall.delete`.
- `CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS` / `CRITICAL_SCHED_WATCHDOG_INTERVAL_SECONDS` – live watchdog grace and polling interval for critical scheduled jobs (`tg_monitoring`, `guide_excursions_full`, `vk_auto_import`; defaults: `300` / `60` seconds).
- `ENABLE_KAGGLE_RECOVERY` – enable background Kaggle recovery loop.
- `KAGGLE_RECOVERY_INTERVAL_MINUTES` – recovery interval in minutes (default: 5).
- `KAGGLE_JOBS_PATH` – path to Kaggle recovery registry JSON (default: `/data/kaggle_jobs.json`).
- `TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES` – how long `tg_monitoring` recovery should keep rechecking Kaggle jobs that temporarily report `failed/error/cancelled` before dropping them as irrecoverable (default: `360`).
- `RUNTIME_HEALTH_HEARTBEAT_SEC` – how often the in-process runtime heartbeat updates (default: `15` seconds).
- `RUNTIME_HEALTH_STALE_SEC` – max allowed heartbeat age before `/healthz` turns unhealthy (default: `45` seconds, minimum `2x` heartbeat interval).
- `RUNTIME_HEALTH_STARTUP_GRACE_SEC` – startup grace window before “not ready yet” becomes a failing `/healthz` condition (default: `120` seconds). Fly service-level check grace is `60s` in production because Fly caps longer service check grace periods to one minute; if cold boot grows beyond that, treat it as a startup-performance/serving-readiness incident instead of hiding it behind a longer Fly grace.

To monitor real job durations, use the daily `/general_stats` report: it prints per-run `took=...` for `vk_auto_import` and `tg_monitoring` (and other ops-run instrumented jobs).
