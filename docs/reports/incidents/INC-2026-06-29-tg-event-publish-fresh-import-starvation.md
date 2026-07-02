# INC-2026-06-29 Telegram event publish fresh import starvation

Status: closed
Severity: sev2
Service: events-bot publication outbox / `@kldevents`
Opened: 2026-06-29
Closed: 2026-06-29
Owners: Codex / events-bot
Related incidents: `INC-2026-06-25-outbox-unknown-jobtask-publication-outage.md`, `INC-2026-06-16-tg-event-publish-timeout-duplicate.md`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

29 июня 2026 Smart Update успешно создал события `6491` и `6492`, построил Telegraph/ICS и опубликовал managed VK-посты, но `@kldevents` не получил event announcements. Причина была не в отсутствии `tg_event_publish` jobs: они существовали и имели выполненные зависимости, но попадали в один throttled backlog слот вместе со старыми catch-up rows и из-за сортировки `joboutbox.id` постоянно откладывались за более старыми pending jobs.

Вечером 29 июня обнаружена повторная форма того же класса: после очередного релиза/restart worker был жив (`/healthz` OK), VK продолжал публиковаться, но новые `tg_event_publish` jobs для событий `6500–6507` получили initial `next_run_at` на следующее утро (`2026-06-30 05:00 UTC`). Предыдущая freshness lane работала только для уже-due jobs; она не могла помочь, если `next_tg_event_publish_run_at()` ещё при enqueue посчитал старые pending catch-up rows полноценными anchors и вынес свежие импорты за текущий день.

## User / Business Impact

- Новые события из Smart Update выглядели опубликованными в отчёте и VK, но отсутствовали в основном Telegram-канале `@kldevents`.
- Пользователь видел последний `@kldevents` пост около `2026-06-29 08:20` Калининград / `06:20 UTC`, тогда как Smart Update импортировал новые события в `10:21–10:24` Калининград / `08:21–08:24 UTC`.
- Поверхности расходились: VK `klgdevents` был свежим, Telegram — отставал за старым backlog.

## Detection

- Обнаружено оператором по Smart Update отчёту:
  - `6491` `Литературно-патриотический фестиваль «Калининградцы памяти верны!»` — source `https://vk.com/wall-32547811_10951`, managed VK `https://vk.com/wall-231920894_4998`, Telegraph `https://telegra.ph/Literaturno-patrioticheskij-festival-Kaliningradcy-pamyati-verny-06-29`.
  - `6492` `Концерт Государственного академического Волжского русского народного хора имени Петра Милославова` — source `https://vk.com/wall-168966993_23326`, managed VK `https://vk.com/wall-231920894_4999`, ICS post `https://t.me/kenigeventscalendar/7126`, Telegraph `https://telegra.ph/Koncert-Gosudarstvennogo-akademicheskogo-Volzhskogo-russkogo-narodnogo-hora-imeni-Petra-Miloslavova-06-29`.
- Public fallback check of `https://t.me/s/kldevents` at `2026-06-29T10:00:30Z` showed latest public event posts `kldevents/1605` at `05:20 UTC` and `kldevents/1606` at `06:20 UTC`; no `6491/6492` post.
- Runtime file mirror was enabled and available: `/data/runtime_logs/events-bot.log*`, `ENABLE_RUNTIME_FILE_LOGGING=1`.

## Timeline

- `2026-06-29 08:21:25 UTC` — `ENQ [E6491] new` for `telegraph_build`, `vk_sync`, `tg_event_publish`.
- `2026-06-29 08:22:07 UTC` — `VK [E6491] event done` → `https://vk.com/wall-231920894_4998`.
- `2026-06-29 08:24:26 UTC` — `ENQ [E6492] new` for `ics_publish`, `telegraph_build`, `tg_ics_post`, `vk_sync`, `tg_event_publish`.
- `2026-06-29 08:27:24–08:27:26 UTC` — `E6492` dependencies complete: VK `wall-231920894_4999`, ICS post `https://t.me/kenigeventscalendar/7126`, Telegraph.
- `2026-06-29 08:49:05 UTC` — first observed `E6491` spacing defer: job `26400` → `08:51:26 UTC`.
- `2026-06-29 09:31:16 UTC` — first observed `E6492` spacing defer: job `26406` → `09:33:24 UTC`.
- `2026-06-29 09:53:41 UTC` — both jobs deferred again to `10:03:41 UTC` behind latest Telegram spacing anchor.
- `2026-06-29 10:00:17 UTC` — prod DB confirmed both events had `tg_event_post_url=NULL`, `tg_event_publish` pending with dependencies done.
- `2026-06-29 10:03:45 UTC` — old code deferred both again to `10:13:44 UTC`; no public post yet.
- `2026-06-29 10:05 UTC` — hotfix prepared: due `tg_event_publish` ordering now prioritizes fresh Smart Update imports over old catch-up backlog while preserving the 10-minute spacing gate.
- `2026-06-29 10:12 UTC` — deployed code SHA `1353511e` to Fly after narrowing the freshness lane to 3h and ordering newest fresh imports first.
- `2026-06-29 10:15 UTC` — `E6492` published to `@kldevents` as `https://t.me/kldevents/1608` (`tg_event_post_id=1608`, DB internal URL `https://t.me/c/3954607218/1608`).
- `2026-06-29 10:25 UTC` — `E6491` published to `@kldevents` as `https://t.me/kldevents/1609` (`tg_event_post_id=1609`, DB internal URL `https://t.me/c/3954607218/1609`).
- `2026-06-29 10:26 UTC` — public `https://t.me/s/kldevents` and production DB verified both posts; `/healthz` returned OK.

## Root Cause

1. `_run_due_jobs_once_locked()` sorted all due jobs by `(task_priority, joboutbox.id)`.
2. `tg_event_publish` catch-up/backlog rows from older imports and repair waves were repeatedly due at the same throttled timestamp as fresh Smart Update rows.
3. `_defer_tg_event_publish_if_spacing_blocked()` correctly enforces one Telegram event post per interval, but after each publish it defers every other due `tg_event_publish` row to the same next anchor. With `id` ordering, fresh high-id Smart Update jobs stayed behind older rows and were re-deferred every cycle.
4. VK publication has a separate postponed cadence and therefore continued successfully, creating the observed VK/TG divergence.
5. Evening recurrence: `schedule_event_update_tasks()` called `next_tg_event_publish_run_at()` while old catch-up rows already occupied same-day pending anchors.
6. `next_tg_event_publish_run_at()` considered those old pending anchors before the new job became due; once its search crossed the local publish window end it normalized fresh imports to next morning. The worker freshness sort only applies after `next_run_at <= now`, so the fresh lane was bypassed.
7. A deploy/restart made the failure visible because the queue resumed old catch-up/no-op rows and created dense pending anchors, while newly imported VK events remained scheduled for tomorrow.

## Contributing Factors

- A large residual `tg_event_publish` backlog existed from prior publication/outbox incidents and catch-up flows.
- The queue had no freshness lane for newly imported events; all pending rows were treated equally once due.
- The public report did not flag “VK done, Telegram still pending behind backlog” as a user-facing partial publication state.

## Automation Contract

### Treat as regression guard when

- touching `main.py::_run_due_jobs_once_locked`, `next_tg_event_publish_run_at`, `_defer_tg_event_publish_if_spacing_blocked`, `schedule_event_update_tasks`, or `JobOutbox` sorting/TTL/catch-up logic;
- changing Smart Update publication fanout or adding any bulk `tg_event_publish` catch-up/rearm;
- changing `TG_EVENT_PUBLISH_INTERVAL_MINUTES`, `TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS`, or publication queue health checks.

### Affected surfaces

- code paths: `main.py` job outbox worker, `tg_event_publish` handler, Smart Update `schedule_event_update_tasks` fanout;
- env/config: `TG_EVENT_PUBLISH_INTERVAL_MINUTES`, `TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS`;
- production surfaces: `@kldevents`, managed VK `klgdevents`, Telegraph pages, calendar channel;
- runtime evidence: `/data/runtime_logs/events-bot.log*`, prod SQLite `event`/`joboutbox`.

### Mandatory checks before closure or deploy

- Unit/regression: `tests/test_job_due_filter.py` must cover fresh `tg_event_publish` not starved by old backlog.
- Prod DB: confirm fresh imported events have `tg_event_publish` dependencies done and are no longer stuck behind older due rows.
- Public smoke: inspect `https://t.me/s/kldevents` (or Telethon if available) and verify `6491/6492` public posts or explicitly record blocker.
- `/healthz` production must be OK after deploy.
- Runtime logs must be checked from `/data/runtime_logs` before claiming inconclusive evidence.

### Required evidence

- deployed SHA and `origin/main` reachability;
- test command/result;
- DB before/after for `event.id IN (6491,6492)` and their `joboutbox` rows;
- public post URLs for repaired publications, or blocker with exact `next_run_at`/logs.

## Immediate Mitigation

- Built and tested a narrow queue ordering fix in a clean worktree `hotfix/inc-tg-publish-gap-20260629`.
- Deployed the fix and let the existing `tg_event_publish` jobs drain through the normal worker path; no direct Telegram post bypass and no bulk old backlog rewrite were used.

## Corrective Actions

- Added `TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS` (default 3h) and due-job sorting that ranks fresh `tg_event_publish` rows ahead of stale catch-up rows, with newest fresh imports first.
- Preserved Telegram spacing: even fresh rows still pass `_defer_tg_event_publish_if_spacing_blocked()`, so the fix changes ordering, not rate limits.
- Added regression coverage in `tests/test_job_due_filter.py` for a fresh Smart Update event behind an older due backlog row.
- Added fresh-aware initial scheduling: when a just-imported event is assigned `tg_event_publish.next_run_at`, stale pending catch-up anchors from old events are ignored unless they are from the same source. The execution-time spacing gate still enforces one Telegram post per interval. The default freshness horizon is now 8h so releases/restarts cannot age same-day imports out of the freshness lane before recovery.
- Added regression coverage in `tests/test_tg_event_publish.py` for old same-day pending anchors that previously pushed a fresh import to the next morning.

## Follow-up Actions

- [ ] Add queue-health/admin visibility for `tg_event_publish` backlog age and partial fanout (`VK done + TG pending`).
- [ ] Audit the old pending `tg_event_publish` backlog and either publish, pause, or expire rows with row-level evidence in a separate scoped task.

## Release And Closure Evidence

- deployed SHA: `1353511e` (`fix(tg): prioritize fresh event publish jobs`).
- recurrence deployed SHA: `44bb8ba8` (`fix(tg): keep fresh event publish slots ahead of backlog`), Fly image `deployment-01KWA9MPJ3BFFZ3RQ8KJAEBWVM`, machine version `1538`.
- deploy path: clean linked worktree `hotfix/inc-tg-publish-gap-20260629` → `fly deploy -a events-bot-new-wngqia --remote-only`.
- regression checks:
  - `uv run --with-requirements requirements.txt pytest -q tests/test_job_due_filter.py -q` → pass (`6 passed`).
  - `tests/test_tg_event_publish.py` full run was attempted but existing date-sensitive tests using `2026-06-20` now fail on 2026-06-29 because the events are treated as past; this is unrelated to the queue-ordering change and needs separate test-date maintenance.
  - recurrence targeted regression: `uv run --with-requirements requirements.txt python -m pytest -q tests/test_job_due_filter.py tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_defers_night_and_spaces_jobs tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_spreads_same_source_afisha tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_ignores_far_future_cancelled_backlog tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_ignores_next_day_pending_anchor_when_window_open tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_ignores_late_next_day_backlog_after_window tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_uses_open_gap_before_late_same_day_backlog tests/test_tg_event_publish.py::test_next_tg_event_publish_run_at_fresh_import_ignores_old_same_day_backlog tests/test_tg_event_publish.py::test_enqueue_tg_publish_rearm_replaces_stale_next_day_slot` → `14 passed`.
  - recurrence compile smoke: `python3 -m py_compile main.py main_part2.py` → pass.
- post-deploy verification:
  - production DB: `6492` `tg_event_post_url=https://t.me/c/3954607218/1608`, `tg_event_publish` done at `2026-06-29 10:15:21 UTC`; `6491` `tg_event_post_url=https://t.me/c/3954607218/1609`, `tg_event_publish` done at `2026-06-29 10:25:27 UTC`.
  - public Telegram fallback: `https://t.me/s/kldevents` showed `kldevents/1608` and `kldevents/1609` with full event facts/descriptions.
  - `/healthz`: OK at `2026-06-29 10:26 UTC`, including `job_outbox_worker=ok` and `job_outbox_worker_loop=ok`.
  - managed VK remains present: `6491` `https://vk.com/wall-231920894_4998`; `6492` `https://vk.com/wall-231920894_4999`.
  - recurrence `/healthz`: OK at `2026-06-29 18:19 UTC`, including `job_outbox_worker=ok` and `job_outbox_worker_loop=ok`.
  - recurrence queue repair: backed up touched rows to `codex_backup_tg_publish_requeue_20260629_6499_6507`, then rearmed only `tg_event_publish` rows for active no-TG events `6499–6507` whose dependencies were already done; no direct Telegram bypass was used.
  - recurrence public Telegram: normal worker published `6507` as `https://t.me/kldevents/1621` at `2026-06-29 18:28:42 UTC` and `6506` as `https://t.me/kldevents/1622` at `2026-06-29 18:38:49 UTC`; remaining rearmed rows moved to the next standard 10-minute slot (`2026-06-29 18:48:49 UTC`). Telethon reread showed `1621` was edited by the standard premium-emoji post-processor at `2026-06-29 18:31:47 UTC`.

## Prevention

- Fresh Smart Update imports now have a bounded freshness lane, preventing old bulk/catch-up rows from starving new user-visible announcements.
- Future closure must verify public Telegram, VK, DB, runtime logs, and `/healthz`, not just Smart Update admin output.
