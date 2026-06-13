# INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog

Status: mitigated
Severity: sev1
Service: Telegram Monitoring / critical scheduler watchdog / Kaggle handoff
Opened: 2026-06-12
Closed: —
Owners: Codex / operator
Related incidents: `INC-2026-06-11-tg-monitoring-recovery-after-deploy-cancel`, `INC-2026-06-07-guide-remote-session-stale-busy`, `INC-2026-06-12-kenigsberg-story-media-invalid-catchup-loop`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`

## Summary

The scheduled Telegram Monitoring slot for 2026-06-12 23:40 Europe/Kaliningrad started on time but was killed by a Fly deploy/restart before it could push/register the Kaggle kernel. The run was marked `crashed`, `/data/kaggle_jobs.json` stayed empty, and the promised critical scheduler watchdog did not replay the missed slot.

## User / Business Impact

- Telegram Monitoring did not import the scheduled 2026-06-12 source batch.
- No Kaggle monitoring run remained active for recovery because the process died while creating Kaggle datasets, before `register_job("tg_monitoring", ...)`.
- Future event intake from Telegram sources was delayed until manual/operator recovery.

## Detection

- Operator reported that Telegram Monitoring should have started at 23:40 and was not running in Kaggle.
- Production DB showed `ops_run.id=2354`, `kind=tg_monitoring`, `trigger=scheduled`, `started_at=2026-06-12 21:40:00 UTC`, `finished_at=2026-06-12 21:41:23 UTC`, `status=crashed`, `run_id=4ea4ccb80bb34788bed18243a6d99da8`.
- Runtime file mirror was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`) and showed the run creating Kaggle datasets at 21:40:00 UTC, followed by deploy startup logs at 21:41:14-21:41:25 UTC and `ops_run: startup cleanup marked=1 status=crashed`.

## Timeline

- 2026-06-12 21:40:00 UTC / 23:40 Europe/Kaliningrad: APScheduler submitted `tg_monitoring`; `tg_monitor.scheduler.entry` and `tg_monitor.start` logged `run_id=4ea4ccb80bb34788bed18243a6d99da8`.
- 2026-06-12 21:40:00 UTC: Telegram Monitoring built config for 51 sources, selected `TELEGRAM_AUTH_BUNDLE_S22`, and started creating Kaggle datasets.
- 2026-06-12 21:41:14 UTC: runtime log contains NUL gap followed by startup logs from the new deployed process.
- 2026-06-12 21:41:23 UTC: startup cleanup marked one running `ops_run` as `crashed`.
- 2026-06-12 23:41 UTC: production `/data/kaggle_jobs.json` was empty, confirming no recoverable Kaggle registry entry for this monitoring run.
- 2026-06-12 23:54 UTC: after deploying the watchdog registration fix, critical watchdog dispatched catch-up `run_id=catchup-tg-monitoring-cf57cdfbf6934f95a34424cba8e041dd`.
- 2026-06-12 23:55 UTC: catch-up pushed Kaggle kernel `zigomaro/telegram-monitor-bot` and registered it in `/data/kaggle_jobs.json` with `remote_telegram_auth_scope=TELEGRAM_AUTH_BUNDLE_S22`.
- 2026-06-13 00:19 UTC: a VK slot hotfix deploy cancelled the server-side poller; `ops_run #2355` ended as `error` with `errors=["cancelled"]`, but the Kaggle registry entry remained.
- 2026-06-13 00:20 UTC: pre-guard watchdog retry `#2358` was safely blocked as `skipped` with `remote_telegram_session_busy`.
- 2026-06-13 00:26 UTC: after deploying the registry-race guard, watchdog logged `deferring tg_monitoring catch-up while recovery registry exists` instead of creating another catch-up run.

## Root Cause

1. A production deploy/restart overlapped the 23:40 Telegram Monitoring slot and killed the process after dataset upload began but before the kernel was pushed and registered in `/data/kaggle_jobs.json`.
2. `docs/operations/cron.md` promised critical catch-up/watchdog coverage for `tg_monitoring`, `guide_excursions_full`, and `vk_auto_import`, but the implemented `maybe_dispatch_critical_scheduler_watchdog()` only checked guide full monitoring.
3. The critical watchdog function was imported by `main.py` but not registered as an APScheduler interval job, so it never ran in production.
4. The existing guide-only watchdog looked at the current local date, which would also miss a previous-day slot after midnight local time.

## Contributing Factors

- The previous Kenigsberg hotfix deploy happened inside the Telegram Monitoring launch minute.
- `/healthz` did not expose `critical_scheduler_watchdog`, `tg_monitoring`, or `vk_auto_import`, so a missing watchdog registration remained invisible.
- The run died before `register_job("tg_monitoring", ...)`, leaving Kaggle recovery with no kernel reference to resume.

## Automation Contract

### Treat as regression guard when

- Changing `scheduling.py` critical scheduler watchdog, startup/catch-up logic, health payload, or heavy-job guard behavior.
- Changing `source_parsing.telegram.service.telegram_monitor_scheduler` or Kaggle registry handoff timing.
- Changing `vk_auto_queue.vk_auto_import_scheduler` scheduled entrypoint behavior.
- Performing production deploys near critical scheduled slots.

### Affected surfaces

- `scheduling.py::maybe_dispatch_critical_scheduler_watchdog`
- `scheduling.py::runtime_health_status`
- APScheduler registration for `tg_monitoring`, `vk_auto_import_*`, and `critical_scheduler_watchdog`
- `ops_run` crash cleanup / delivery evidence
- Fly runtime logs and `/data/kaggle_jobs.json`

### Mandatory checks before closure or deploy

- `python3 -m py_compile scheduling.py tests/test_scheduling.py`
- Targeted pytest for:
  - `tests/test_scheduling.py::test_runtime_health_status_reports_critical_monitoring_jobs`
  - `tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_tg_monitoring_after_crash`
  - `tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_vk_auto_import_after_slot_crash`
  - existing guide critical watchdog tests in `tests/test_scheduling.py`
- Fly `/healthz` must show `critical_scheduler_watchdog=ok`, `tg_monitoring=ok`, and `vk_auto_import=ok` when the jobs are enabled.
- Post-deploy evidence must show the missed Telegram Monitoring slot either running/recovered or explicitly blocked by remote-session/Kaggle evidence.
- `/data/kaggle_jobs.json` must be inspected before claiming no Kaggle monitoring run exists.

### Required evidence

- deployed SHA: `664784a99c9bba3ef1ccd038e7acf78288164521`
- deploy path: `origin/main` -> `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks:
  - `python3 -m py_compile scheduling.py tests/test_scheduling.py main.py main_part2.py tests/test_vk_actor.py`
  - `pytest -q tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_tg_monitoring_after_crash tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_tg_monitoring_when_recovery_job_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_vk_auto_import_after_slot_crash tests/test_scheduling.py::test_runtime_health_status_reports_critical_monitoring_jobs tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_kaliningrad_morning_and_interval tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_first_morning_gap_before_promo_anchors tests/test_vk_actor.py::test_vk_postponed_next_slot_steps_through_occupied_morning_slots tests/test_vk_actor.py::test_fetch_vk_latest_postponed_prefers_user_actor` printed `8 passed in 0.80s`; the process then required Ctrl-C during Python thread shutdown after the pytest summary.
- runtime evidence:
  - `ops_run #2354` crashed with `run_id=4ea4ccb80bb34788bed18243a6d99da8`
  - runtime log file `events-bot.log.2026-06-12_21` shows deploy restart during the slot
  - `/data/kaggle_jobs.json` was `{"jobs": []}` after the original crash
- recovery evidence:
  - `ops_run #2355` catch-up pushed `zigomaro/telegram-monitor-bot` with `run_id=catchup-tg-monitoring-cf57cdfbf6934f95a34424cba8e041dd`.
  - `/data/kaggle_jobs.json` still has exactly one `tg_monitoring` entry for that kernel with `remote_telegram_auth_scope=TELEGRAM_AUTH_BUNDLE_S22`.
  - Kaggle status check after final deploy returned `{"status": "RUNNING"}`.
  - Post-final-deploy watchdog log shows defer on recovery registry rather than another catch-up push.

## Immediate Mitigation

- Stop relying on the next daily run; the missed 23:40 slot must be catch-up dispatched after deploy.
- Do not start a second Telegram session manually until `/data/kaggle_jobs.json` and remote-session guard state are checked.

## Corrective Actions

- Register a real `critical_scheduler_watchdog` APScheduler interval job when any critical job (`tg_monitoring`, `guide_excursions_full`, `vk_auto_import`) is enabled.
- Extend the watchdog to cover `tg_monitoring` and `vk_auto_import`, not only guide full monitoring.
- Make watchdog catch-up use the last local scheduled slot, so previous-day slots remain recoverable after local midnight.
- Extend `/healthz` to expose `critical_scheduler_watchdog`, `tg_monitoring`, and `vk_auto_import` scheduler health.
- Use critical-slot misfire grace values for `tg_monitoring` and `vk_auto_import` cron registration.

## Follow-up Actions

- [x] Codex: deploy the watchdog fix from `origin/main`.
- [x] Codex: verify the missed Telegram Monitoring run is running in Kaggle.
- [ ] Codex: verify final `recovery_import` evidence after Kaggle kernel completes.
- [x] Codex: verify video announcements and VK auto-import scheduler health after deploy.

## Release And Closure Evidence

- deployed SHA: `664784a99c9bba3ef1ccd038e7acf78288164521`
- deploy image: `registry.fly.io/events-bot-new-wngqia:deployment-01KTZ5VVRNGQYEV17ZJP997C86`
- deploy path: `origin/main` -> `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks: see required evidence above
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `critical_scheduler_watchdog=ok`, `tg_monitoring=ok`, `vk_auto_import=ok`, `video_tomorrow=ok`, and `video_popular_review=ok`.
  - `tg_monitoring_next_run=2026-06-13T21:40:00+00:00`; the missed 2026-06-12 slot is represented by the still-running recovery kernel.
  - `vk_auto_import_next_run=2026-06-13T04:15:00+00:00`; catch-up `ops_run #2360` is running after the final deploy.
  - No new `tg_monitoring` `ops_run` was created after final deploy; the latest remains pre-guard `#2358 skipped remote_telegram_session_busy`.

## Prevention

- `/healthz` now treats missing critical scheduler watchdog or missing critical jobs as visible health degradation.
- Critical scheduler tests cover deploy-killed `tg_monitoring` and per-slot `vk_auto_import` recovery.
- The watchdog now defers `tg_monitoring` catch-up while a `tg_monitoring` Kaggle recovery registry entry exists, preventing a second `TELEGRAM_AUTH_BUNDLE_S22` push while a remote kernel may still own the Telethon session.
- Incident closure requires catch-up evidence, not only a code deploy.
