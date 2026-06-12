# INC-2026-06-11 Telegram Monitoring Recovery After Deploy Cancel

Status: monitoring
Severity: sev2
Service: Telegram Monitoring scheduled import / Kaggle recovery / Fly production deploy
Opened: 2026-06-11
Closed: —
Owners: events-bot operator + release agent
Related incidents: `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel.md`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

During an Afisha Engagement / VK auto-import production fix, the Fly deploy
restarted the bot while scheduled Telegram Monitoring was still waiting on its
remote Kaggle kernel. The original scheduled `ops_run.id=2273` was recorded as
`error` / `cancelled`, which looked like lost monitoring output in the bot
operator flow.

The existing Kaggle recovery mechanism was still valid and recovered the same
`run_id=7f4af7474db2421f9ee506d8157886be` without starting a new monitoring
run. Recovery import `ops_run.id=2275` finished successfully.

## User / Business Impact

- The operator saw the scheduled Telegram Monitoring run marked as `error` and
  reasonably treated it as lost monitoring work.
- VK auto-import debugging was delayed while recovery evidence was collected.
- No duplicate Telegram Monitoring run was started; the original Kaggle output
  was imported through the existing recovery path.

## Detection

- Detected by operator report during Afisha Engagement debug work.
- Confirmed in production DB `ops_run` rows and runtime file logs under
  `/data/runtime_logs/events-bot.log`.
- Runtime file mirror was enabled in production:
  `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`.

## Timeline

- 2026-06-11 21:40 UTC — scheduled Telegram Monitoring started as
  `ops_run.id=2273`, `run_id=7f4af7474db2421f9ee506d8157886be`; Kaggle kernel
  `zigomaro/telegram-monitor-bot` was pushed.
- 2026-06-11 22:27 UTC — operator launched `/vk_auto_import 3`; manual
  auto-import `ops_run.id=2274` waited behind the heavy-job gate.
- 2026-06-11 22:47 UTC — Fly deploy restarted the bot; `ops_run.id=2273`
  ended as `error` with `errors=["cancelled"]`; `ops_run.id=2274` ended
  `crashed`.
- 2026-06-11 22:52-23:02 UTC — `kaggle_recovery` saw the original kernel still
  `RUNNING`.
- 2026-06-11 23:12 UTC — recovery downloaded `telegram_results.json` and
  created import-only `ops_run.id=2275`.
- 2026-06-11 23:55 UTC — `ops_run.id=2275` finished `success`; registry
  `/data/kaggle_jobs.json` was cleaned to `{"jobs":[]}`.

## Root Cause

1. A production deploy was performed while a scheduled Telegram Monitoring
   Kaggle-backed job was still active.
2. The original scheduled `ops_run` status represented the local process
   cancellation, not the remote Kaggle kernel status.
3. The recovery mechanism was available, but the operator-visible status made
   the run look failed until recovery import evidence was checked.

## Contributing Factors

- Manual VK auto-import was blocked behind the heavy-job gate before the
  unblock fix, creating pressure to deploy during an active monitoring window.
- Recovery evidence required checking several sources: production DB, Kaggle
  registry, runtime file logs, and shadow publication counts.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring, Kaggle recovery, scheduler heavy-job gating, or
  Fly deploy procedure for scheduled import jobs.
- Deploying during an active `tg_monitoring` / `recovery_import` run.

### Affected surfaces

- `kaggle_recovery.py`
- `source_parsing.telegram.handlers`
- `ops_run` status/metrics rows
- `/data/kaggle_jobs.json`
- Fly production deploy/restart path
- Manual `/vk_auto_import` operator flow when scheduled jobs are active

### Mandatory checks before closure or deploy

- Check production `ops_run` for active `tg_monitoring` and
  `trigger='recovery_import'` rows.
- Check `/data/kaggle_jobs.json` for active Telegram Monitoring jobs.
- If a deploy interrupts a scheduled run, verify the recovery path reaches a
  terminal state and imports the same `run_id`; do not start a replacement run
  while the original Kaggle session is alive.
- Verify runtime logs by `run_id` and `kaggle_recovery` before declaring output
  lost.

### Required evidence

- `ops_run` terminal status and metrics for the recovery import.
- Kaggle registry cleanup or explicit active job state.
- Runtime log excerpts for download/import progress.
- Health/deploy evidence if code was deployed.

## Immediate Mitigation

- Did not start a compensating Telegram Monitoring run.
- Waited for the existing Kaggle recovery job to complete and verified
  `ops_run.id=2275` success.

## Corrective Actions

- Manual `/vk_auto_import` was already changed to bypass the heavy gate by
  default in commit `e9e6c146`, reducing future pressure to deploy/restart while
  scheduled Telegram Monitoring is active.
- This incident record documents the regression contract for future deploys and
  recovery checks.

## Follow-up Actions

- [ ] Consider an operator-facing message that distinguishes “local scheduled
  runner cancelled; remote Kaggle recovery pending” from a truly lost
  Telegram Monitoring run.

## Release And Closure Evidence

- deployed SHA: `31a44dfaf5b62b2f717369ac863e2400beaca33a` for the manual
  VK auto-import unblock fix; current Afisha Engagement CTA guard release TBD.
- deploy path: Fly production app `events-bot-new-wngqia`.
- regression checks: `ops_run.id=2275` success with
  `messages_processed=165`, `events_imported=29`, `events_created=16`,
  `events_merged=13`, `errors_count=0`; `/data/kaggle_jobs.json` cleaned to an
  empty jobs list.
- post-deploy verification: `/healthz` was OK after the unblock deploy; current
  CTA guard deploy evidence to be added if deployed.

## Prevention

- Treat active scheduled Kaggle-backed jobs as a deploy regression guard.
- When a local scheduled run is cancelled but the Kaggle registry still has the
  original job, verify recovery before launching any replacement monitoring
  work.
