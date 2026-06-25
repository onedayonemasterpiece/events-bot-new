# INC-2026-06-25 Outbox unknown JobTask publication outage

Status: monitoring
Severity: sev1
Service: events-bot-new publication outbox (`@kldevents`, `klgdevents`, Telegraph/calendar fanout)
Opened: 2026-06-25
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-10-event-outbox-fanout-deadlock`, `INC-2026-06-24-future-event-date-default-venue-regressions`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Publication fanout stopped after Telegram `@kldevents/1275` on 2026-06-24 10:16Z while VK also became stale. The outbox worker was alive according to `/healthz`, but every cycle crashed while ORM-materializing legacy `joboutbox.task` values (`telegraph_nav_month`, `telegraph_nav_week`, `telegraph_nav_weekend`) that were no longer present in the `JobTask` enum. After the queue was unblocked, many active future publication jobs had waited longer than `JOB_TTL` and were being expired instead of caught up.

## User / Business Impact

- Public event publication cadence was interrupted for roughly 23 hours.
- Fresh future events were imported/queued but did not reliably reach Telegram `@kldevents` and managed VK `klgdevents`.
- The public health check showed `job_outbox_worker: ok`, so the failure was not visible through the primary readiness surface.

## Detection

- Operator noticed that the last Telegram event post was `https://t.me/kldevents/1275` at 2026-06-24 10:16Z and that VK was also stale.
- Runtime file mirror `/data/runtime_logs/events-bot.log*` showed repeated `job_outbox_worker cycle failed` with `LookupError: 'telegraph_nav_month' is not among the defined enum values`.
- Production DB inspection found the blocking rows: `joboutbox.id=25577` (`telegraph_nav_month`), `25578` (`telegraph_nav_week`), `25579` (`telegraph_nav_weekend`).

## Timeline

- 2026-06-24 10:16Z — last known pre-incident Telegram event post: `@kldevents/1275`.
- 2026-06-25 09:20Z — runtime mirror still shows repeated `job_outbox_worker cycle failed` / unknown enum materialization errors.
- 2026-06-25 09:25Z — production backup `codex_backup_20260625_unknown_jobtask_outage` created; blocking `telegraph_nav_*` rows paused.
- 2026-06-25 09:32Z — code hotfix deployed to filter due/running outbox selects to known `JobTask` values.
- 2026-06-25 09:44Z — production backup `codex_backup_20260625_outage_expired_rearm` created; active future jobs that had been outage-expired were re-armed for catch-up.
- 2026-06-25 09:48Z — Telegram resumed: event `6386` published as `@kldevents/1276`.
- 2026-06-25 09:49Z–09:58Z — VK catch-up resumed, with managed VK posts `wall-231920894_4356` through `wall-231920894_4369` confirmed from DB job results.
- 2026-06-25 09:58Z — health check passing and no fresh unknown-enum crash observed in the sampled post-mitigation window; backlog still draining, so incident remains in monitoring.
- 2026-06-25 10:14Z — Telegram catch-up advanced to `@kldevents/1277`; VK catch-up advanced through `wall-231920894_4395`; deployed fix commit was fast-forwarded to `origin/main`.

## Root Cause

1. Legacy/experimental `joboutbox.task` strings were left in the production queue after the enum names changed or a previous feature was removed.
2. SQLAlchemy `SAEnum(JobTask)` raises during row materialization when any selected row contains an unknown enum value; the worker selected due rows before filtering to known tasks, so one bad row crashed the whole queue cycle.
3. The outbox stale/TTL path treated valid active future pending jobs as expired after the worker outage, instead of recognizing an outage catch-up case.

## Contributing Factors

- `/healthz` reported `job_outbox_worker: ok` even while the worker loop was repeatedly failing.
- The bad rows were for navigation/page jobs, but they shared the same global outbox table as event Telegram/VK publication jobs.
- Event publication dependencies mean Telegram catch-up can wait behind VK sync jobs, making recovery slower after a backlog.

## Automation Contract

### Treat as regression guard when

- Touching `JobTask`, `JobOutbox.task`, SQLAlchemy enum configuration, outbox row selection/materialization, or queue cleanup/migrations.
- Touching `_run_due_jobs_once_locked`, job TTL/stale handling, `job_outbox_worker`, or publication fanout dependency logic.
- Adding/removing page/navigation/festival jobs that write to the shared `joboutbox` table.

### Affected surfaces

- `main.py::_run_due_jobs_once_locked`
- `models.py::JobTask` / `JobOutbox.task`
- production SQLite `/data/db.sqlite.joboutbox`
- Fly health `/healthz` task status
- Telegram `@kldevents`, VK `klgdevents`, Telegraph/calendar fanout

### Mandatory checks before closure or deploy

- Unit test: unknown raw `joboutbox.task` values do not crash due/running job selection and a valid known job still runs.
- Unit or integration test: outage-stale pending active future event-pipeline jobs are caught up or re-deferred, not expired solely because the worker was blocked.
- Production smoke: `/healthz` passing, no fresh `job_outbox_worker cycle failed` / `LookupError` in `/data/runtime_logs/events-bot.log*` after deploy.
- Production smoke: a fresh or catch-up `tg_event_publish` reaches `done` and a visible `@kldevents` post newer than `@kldevents/1275` exists.
- Production smoke: managed VK publication catch-up creates/edits `klgdevents` wall posts or leaves only source-grounded blocked rows with explicit errors.
- Backlog check: due `pending`/`running` publication jobs are decreasing or have documented blockers; no unknown enum rows are runnable.

### Required evidence

- deployed SHA reachable from `origin/main`;
- Fly deployment image/version;
- focused pytest output;
- production DB/backlog query artifact;
- runtime mirror grep artifact;
- Telegram/VK public or API evidence for resumed publication.

## Immediate Mitigation

- Backed up affected rows in `codex_backup_20260625_unknown_jobtask_outage`.
- Paused the three blocking unknown `telegraph_nav_*` rows (`25577`, `25578`, `25579`) with a far-future `next_run_at` and outage-specific `last_error`.
- Restored non-target unknown rows that were accidentally paused during the first broad mitigation pass.
- Backed up/re-armed outage-expired active future publication jobs in `codex_backup_20260625_outage_expired_rearm`.

## Corrective Actions

- Filtered outbox running/due ORM selects with `JobOutbox.task.in_(list(JobTask))` before row materialization, so legacy unknown task strings cannot crash the worker.
- Added catch-up handling for active, non-silent current/future event-pipeline pending jobs that exceeded `JOB_TTL` only because the worker was blocked.
- Added regression coverage in `tests/test_job_running_stale.py` for unknown raw task values plus valid known jobs.

## Follow-up Actions

- [ ] Add health accounting for recent `job_outbox_worker` loop exceptions so `/healthz` cannot report the task as ok while every cycle is failing.
- [ ] Add a bounded queue cleanup/migration for obsolete `telegraph_nav_*` rows instead of leaving them as paused legacy data.
- [ ] Review whether `tg_event_publish` should hard-depend on `vk_sync` during outage catch-up, or whether Telegram should be allowed to continue while VK retries source/media-specific blockers.

## Release And Closure Evidence

- deployed SHA: `241994dab8a02f724529bcdcdf0ab51ac6244ca9` (fast-forwarded to `origin/main`)
- deploy path: manual `flyctl deploy` to `events-bot-new-wngqia` from clean `hotfix/outbox-unknown-jobtask-outage` worktree
- deployed image: `registry.fly.io/events-bot-new-wngqia:deployment-01KVZ3JNMKNKDVK58749Y7V181`
- regression checks:
  - `python3 -m py_compile main.py smart_event_update.py models.py` — passed
  - `/tmp/events-bot-test-venv/bin/python -m pytest -q tests/test_job_running_stale.py::test_due_jobs_ignore_unknown_task_values_without_crashing tests/test_job_running_stale.py::test_running_stale_marked_and_replaced tests/test_smart_event_update_duplicate_guards.py::test_match_create_prompt_distinguishes_time_conflict_from_multi_session tests/test_genai_dump_and_poster_dedup.py::test_sanitize_description_output_rejects_dump tests/test_genai_dump_and_poster_dedup.py::test_sanitize_description_output_strips_editor_meta_preamble` — passed, 5 tests
- post-deploy verification:
  - Fly checks passing for machine `683961db016e28`.
  - Telegram resumed with `@kldevents/1276` (`event_id=6386`) at 2026-06-25 09:48Z and advanced to `@kldevents/1277` (`event_id=6122`) at 2026-06-25 10:14Z.
  - VK catch-up produced managed posts including `https://vk.com/wall-231920894_4356` through `https://vk.com/wall-231920894_4395` from `vk_sync` job results.
  - Evidence artifacts under `artifacts/codex/publication-outage-20260625/` (not committed).

## Prevention

The outbox worker must be robust to stale DB task strings and must not let one unknown enum row block all unrelated publication fanout. Active future event-pipeline jobs must catch up after worker downtime rather than expiring silently. Health must eventually include recent worker-loop exception state so this class of outage is visible without waiting for an operator to notice missing public posts.
