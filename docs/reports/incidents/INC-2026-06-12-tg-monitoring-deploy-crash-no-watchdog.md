# INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog

Status: open
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

- deployed SHA: pending
- deploy path: `origin/main` -> Fly remote deploy
- regression checks: pending
- runtime evidence:
  - `ops_run #2354` crashed with `run_id=4ea4ccb80bb34788bed18243a6d99da8`
  - runtime log file `events-bot.log.2026-06-12_21` shows deploy restart during the slot
  - `/data/kaggle_jobs.json` was `{"jobs": []}` after the crash
- recovery evidence: pending

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

- [ ] Codex: deploy the watchdog fix from `origin/main`.
- [ ] Codex: verify the missed Telegram Monitoring run is running in Kaggle or has completed with import evidence.
- [ ] Codex: verify video announcements and VK auto-import scheduler health after deploy.

## Release And Closure Evidence

- deployed SHA: pending
- deploy image: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- `/healthz` now treats missing critical scheduler watchdog or missing critical jobs as visible health degradation.
- Critical scheduler tests cover deploy-killed `tg_monitoring` and per-slot `vk_auto_import` recovery.
- Incident closure requires catch-up evidence, not only a code deploy.
