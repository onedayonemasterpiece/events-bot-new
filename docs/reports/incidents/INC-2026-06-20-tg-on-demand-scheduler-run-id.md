# INC-2026-06-20 TG on-demand scheduler run_id crash

Status: mitigated
Severity: sev3
Service: Telegram Monitoring on demand scheduler
Opened: 2026-06-20
Closed: —
Owners: events-bot maintainer
Related incidents: —
Related docs: `docs/features/tg-monitoring-on-demand/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/release-governance.md`, `docs/operations/runtime-logs.md`

## Summary

After deploying TG monitoring on demand v1, the new APScheduler job `tg_monitoring_on_demand` started but failed on its first tick because the project scheduler wrapper injects `run_id=...` into wrapped job functions and the new dispatcher did not accept that keyword.

## User / Business Impact

- Existing scheduled Telegram Monitoring and `/healthz` stayed healthy.
- The newly deployed on-demand fast-path could not dispatch queued source-specific runs until fixed.
- Scheduled Telegram Monitoring remained the catch-up safety net, so no existing daily monitoring contract was lost.

## Detection

- Detected during post-deploy runtime log verification after release `v1463`.
- `/healthz` was ready, but `/data/runtime_logs/events-bot.log` showed `JOB_ERROR job_id=tg_monitoring_on_demand` and `TypeError: dispatch_due_on_demand_monitoring() got an unexpected keyword argument 'run_id'`.

## Timeline

- 2026-06-20 08:30 UTC — `a9dc0ce2` deployed to Fly release `v1463`.
- 2026-06-20 08:31 UTC — `/healthz` returned HTTP 200 ready.
- 2026-06-20 08:32 UTC — first `tg_monitoring_on_demand` tick failed with `TypeError` in runtime logs.
- 2026-06-20 08:33 UTC — root cause identified as scheduler-wrapper function signature mismatch.
- 2026-06-20 08:36 UTC — code fix prepared to accept optional scheduler `run_id`.

## Root Cause

1. `scheduling._job_wrapper` calls scheduled functions through `_execute(..., run_id=run_id)`.
2. `dispatch_due_on_demand_monitoring(db, bot)` was introduced without a `run_id` keyword-only parameter.
3. Unit tests covered direct dispatcher calls but did not call it with the scheduler wrapper contract.

## Contributing Factors

- The new job was not included in existing health payload details, so `/healthz` readiness did not expose the per-job exception directly.
- The first implementation reused the function as both direct service API and scheduler entrypoint without testing the scheduler-injected keyword.

## Automation Contract

### Treat as regression guard when

- Adding or changing APScheduler jobs registered through `_job_wrapper`.
- Changing `source_parsing.telegram.on_demand.dispatch_due_on_demand_monitoring` signature.
- Changing scheduler wrapper keyword injection semantics.

### Affected surfaces

- `scheduling.py` `_job_wrapper` dispatch contract.
- `source_parsing/telegram/on_demand.py` scheduler entrypoint.
- Runtime logs and post-deploy smoke checks for scheduler jobs.

### Mandatory checks before closure or deploy

- `python -m pytest tests/test_tg_monitoring_on_demand.py` must include a dispatcher call with `run_id=...`.
- `python -m py_compile source_parsing/telegram/on_demand.py handlers/channel_nav.py scheduling.py db.py models.py`.
- Post-deploy `/healthz` HTTP 200 ready.
- Production runtime logs show `tg_monitoring_on_demand` tick after fix without `unexpected keyword argument 'run_id'`.
- Fix SHA is reachable from `origin/main` before closure.

### Required evidence

- deployed SHA:
- Fly release version:
- test output:
- runtime log evidence:
- `origin/main` reachability:

## Immediate Mitigation

- Keep scheduled Telegram Monitoring as catch-up while fixing the on-demand scheduler entrypoint.

## Corrective Actions

- Updated `dispatch_due_on_demand_monitoring` to accept optional keyword-only `run_id` from `_job_wrapper`.
- Added unit coverage that calls the dispatcher with `run_id=...`.

## Follow-up Actions

- [ ] Consider adding `tg_monitoring_on_demand` to `/healthz` scheduler detail payload if the health framework supports interval-job status without noisy alerts.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Scheduler entrypoint tests should include the same keyword shape that `_job_wrapper` injects into production scheduled jobs.
