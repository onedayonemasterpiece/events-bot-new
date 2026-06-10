# INC-2026-06-10 Guide Watchdog Kaggle Failure Retry Loop

Status: blocked-on-fresh-telegram-session
Severity: sev2
Service: Guide Excursions Monitoring / critical scheduler watchdog / Kaggle
Opened: 2026-06-10
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-06-guide-monitoring-missed-vk-festival-hashtag`, `INC-2026-06-07-guide-remote-session-stale-busy`
Related docs: `docs/operations/cron.md`, `docs/features/guide-excursions-monitoring/README.md`

## Summary

On 2026-06-10 the guide excursions critical watchdog repeatedly retried the
same missed `guide_excursions_full` slot after the Kaggle kernel reached
terminal `ERROR`. This created many admin notifications and repeated Kaggle
kernel pushes within the same hour.

The regular scheduler health still showed `guide_excursions_full_next_run` for
the next day, but the live watchdog was trying to compensate the same-day missed
full slot.

## Impact

- Admin chat received repeated guide monitoring failure notifications.
- Production pushed multiple `zigomaro/guide-excursions-monitor` kernel
  versions for the same failed catch-up window.
- The same-day guide full catch-up remained pending, but retries were too
  aggressive.

## Evidence

- `/healthz` showed `guide_excursions_light=ok`,
  `guide_excursions_full=ok`, and next regular full run on 2026-06-11.
- Production `ops_run` rows showed repeated scheduled full guide attempts:
  `2171`, `2172`, `2173`, `2176`, `2177` with status `error` and
  `Guide Kaggle kernel failed (failed)`.
- Runtime log evidence:
  - `SCHED critical watchdog dispatching missing guide_excursions_full slot scheduled_local=2026-06-10T20:10:00+02:00 ...`
  - `kaggle: kernels_push response ref=zigomaro/guide-excursions-monitor version=300`
  - `kaggle: kernel status kernel=zigomaro/guide-excursions-monitor status=ERROR`
  - `RuntimeError: Guide Kaggle kernel failed (failed)`

## Root Cause

### Retry loop

`maybe_dispatch_critical_scheduler_watchdog()` only treated materialized full
guide runs with status `running`, `success`, or `partial` as slot delivery. It
also had a cooldown for `remote_telegram_session_busy` skipped runs. A terminal
Kaggle failure materialized as `status='error'`, which was neither delivery nor
deferred. On the next watchdog tick, the same missed slot was dispatched again.

### Kaggle kernel failure

The guide Kaggle kernel itself is failing before source scanning at Telethon
startup:

`AuthKeyDuplicatedError: The authorization key (session file) was used under two different IP addresses simultaneously, and can no longer be used.`

On 2026-06-10 production `TELEGRAM_AUTH_BUNDLE_S22` was first rotated to the
available `events-bot-new/.env` `TELEGRAM_AUTH_BUNDLE_S22_2`, then rotated to
the separate `/home/dev/projects/kdg80/.env` `TELEGRAM_AUTH_BUNDLE_S22`.
Both smoke runs still failed with the same `AuthKeyDuplicatedError`, so the
available S22 bundles are not sufficient to restore guide monitoring.

## Corrective Action

- Add `GUIDE_MONITORING_FAILED_RETRY_SECONDS` cooldown for materialized
  `guide_monitoring` full runs whose latest attempt finished as `error` or
  `failed`.
- Keep the same-day slot pending for retry after the cooldown; do not mark it
  completed solely because the catch-up failed.
- Add regression coverage that a failed full catch-up suppresses immediate
  repeated watchdog dispatch and retries only after the cooldown.

## Regression Contract

When changing guide scheduling, `maybe_dispatch_critical_scheduler_watchdog`,
`ops_run` status handling, or Kaggle guide recovery:

- a same-day `light` run must not satisfy the `full` slot;
- a `running`, `success`, or `partial` full run must satisfy the slot;
- a `remote_telegram_session_busy` full skip must defer retry by
  `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS`;
- an `error`/`failed` full run must defer retry by
  `GUIDE_MONITORING_FAILED_RETRY_SECONDS`;
- the watchdog must not push a new guide Kaggle kernel every watchdog tick for
  the same failed full slot.

## Release And Closure Evidence

- retry-loop fix deployed SHA:
  `b987769393a8693f120fe4b2a5bfac32e3a53e88`
- deploy path: manual Fly deploy to `events-bot-new-wngqia`, release `v1289`
- secret rotation evidence:
  - release `v1290`: `TELEGRAM_AUTH_BUNDLE_S22` rotated to the available
    `events-bot-new/.env` spare S22 bundle; smoke still failed with
    `AuthKeyDuplicatedError`.
  - release `v1291`: `TELEGRAM_AUTH_BUNDLE_S22` rotated to the separate
    `/home/dev/projects/kdg80/.env` S22 bundle; smoke `authsmokef59e651b`
    still failed with `AuthKeyDuplicatedError`.
- regression checks:
  - `python3 -m py_compile scheduling.py main.py main_part2.py`
  - `pytest tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only tests/test_scheduling.py::test_critical_scheduler_watchdog_skips_guide_when_full_run_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_skips_guide_when_recovery_import_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_guide_after_remote_busy_skip tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_guide_after_failed_full_run`
- post-deploy verification:
  - `/healthz` ready after release.
  - Latest scheduled guide `ops_run` remained `2182`, finished at
    `2026-06-10 19:32:33` UTC; no new scheduled guide full `ops_run` rows were
    created during the immediate post-deploy window.
  - Runtime log confirmed:
    `SCHED critical watchdog deferring guide_excursions_full retry after failed catch-up ... retry_seconds=3600 failed_at=2026-06-10T19:32:33+00:00`.

## Remaining Blocker

Guide monitoring requires a fresh, exclusive `TELEGRAM_AUTH_BUNDLE_S22` for the
remote Kaggle monitoring role. Do not switch guide monitoring to
`TELEGRAM_SESSION` or `TELEGRAM_AUTH_BUNDLE_E2E`; those are separate roles.
