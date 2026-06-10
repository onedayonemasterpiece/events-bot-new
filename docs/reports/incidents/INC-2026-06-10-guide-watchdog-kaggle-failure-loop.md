# INC-2026-06-10 Guide Watchdog Kaggle Failure Retry Loop

Status: monitoring
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

`maybe_dispatch_critical_scheduler_watchdog()` only treated materialized full
guide runs with status `running`, `success`, or `partial` as slot delivery. It
also had a cooldown for `remote_telegram_session_busy` skipped runs. A terminal
Kaggle failure materialized as `status='error'`, which was neither delivery nor
deferred. On the next watchdog tick, the same missed slot was dispatched again.

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

- deployed SHA: pending
- deploy path: manual Fly deploy pending
- regression checks: pending
- post-deploy verification: pending
