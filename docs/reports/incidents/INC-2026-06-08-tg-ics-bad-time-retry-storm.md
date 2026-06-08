# INC-2026-06-08-tg-ics-bad-time-retry-storm

Status: monitoring
Severity: sev2
Service: `job_outbox` calendar fanout (`ics_publish`, `tg_ics_post`) and Telegram event publishing
Opened: 2026-06-08
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-08-festival-vk-aggregate-regression`
Related docs: `docs/features/tg-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

During a production VK auto-import E2E attempt, runtime log monitoring showed a retry storm in calendar publication jobs. `tg_ics_post` and `ics_publish` rebuilt ICS content for events whose `time` was non-empty but not parseable as a concrete start time, raised `ValueError: bad time`, and re-entered retry/error scheduling instead of becoming a terminal non-publishable calendar outcome.

## User / Business Impact

- Calendar posts and `.ics` files were unavailable for affected events with invalid schedule text.
- Repeated job failures added noise and backlog pressure to `job_outbox`.
- Telegram event publication could remain blocked when stale or newly scheduled dependencies included `tg_ics_post:<event_id>` for an event that does not have a valid calendar schedule.

## Detection

- Detected on 2026-06-08 while tailing production Fly logs for the live VK auto-import E2E run.
- Log signature: `job_outbox handler failed task=tg_ics_post event_id=... error=bad time` with stack trace ending in `build_ics_content`.
- Production DB snapshot during triage showed hundreds of `tg_ics_post` / `ics_publish` error rows and pending downstream Telegram event publish work.

## Timeline

- 2026-06-08 20:45 UTC: production log tail started for VK auto-import E2E.
- 2026-06-08 20:47 UTC: repeated `tg_ics_post` `ValueError: bad time` failures observed in Fly logs.
- 2026-06-08 20:50 UTC: production DB query confirmed large `tg_ics_post` / `ics_publish` error accumulation and `tg_event_publish` pending backlog.
- 2026-06-08 20:53 UTC: `/vk_auto_import --limit=1` reached production bot webhook but returned `Not authorized` for the current E2E Telegram user; import E2E remained blocked separately from this incident.
- 2026-06-08 21:02 UTC: hotfix branch added scheduler guard and execution-time terminal skip for invalid calendar schedules.
- 2026-06-08 21:06 UTC: deployed hotfix SHA `c7696ee52564deff5964e75fc6b5270adf562b50` to Fly app `events-bot-new-wngqia`, machine version `1255`.
- 2026-06-08 21:08 UTC: first due legacy `bad time` calendar jobs drained as `done/nochange` with `skipped invalid schedule` warnings instead of traceback failures; worker error count dropped from `2517` to `2512`.

## Root Cause

1. `schedule_event_update_tasks` treated any non-empty `event.time` as calendar-capable and enqueued `ics_publish` / `tg_ics_post`.
2. `build_ics_content` correctly rejected unparseable time strings with `ValueError("bad time")`, but callers treated that permanent data condition the same as transient infrastructure failures.
3. Existing queued jobs with invalid schedules had no terminal skip path, so worker retries could repeat indefinitely and keep downstream dependencies blocked.

## Contributing Factors

- Prior no-time guard handled empty time only, not non-empty prose schedule values.
- Calendar dependency checks were split between scheduler and builder instead of sharing the same parseability contract.
- Runtime logs exposed the retry storm, but job status alone did not distinguish permanent invalid schedule from transient calendar publication failures.

## Automation Contract

### Treat as regression guard when

- Changing `build_ics_content`, `ics_publish`, `tg_ics_post`, `schedule_event_update_tasks`, `enqueue_job` dependency behavior, or Telegram event publish dependency construction.
- Changing event date/time extraction semantics for Smart Update, VK auto-import, Telegram Monitoring, or `/parse`.

### Affected surfaces

- `main.py::schedule_event_update_tasks`
- `main.py::build_ics_content`
- `main.py::ics_publish`
- `main.py::tg_ics_post`
- `JobOutbox` dependency graph for `tg_event_publish`
- Production runtime logs and DB status queries for calendar fanout jobs

### Mandatory checks before closure or deploy

- Unit regression: invalid non-empty `event.time` must not enqueue `ics_publish`/`tg_ics_post`.
- Unit regression: already queued invalid-schedule `ics_publish`/`tg_ics_post` jobs must return `False` and mark `skipped_invalid_schedule`.
- Positive regression: valid calendar time still schedules `tg_ics_post` and keeps the calendar dependency for `tg_event_publish`.
- Production post-deploy logs must show no continuing `ValueError: bad time` retry storm for `tg_ics_post`/`ics_publish`.
- Production DB follow-up must show affected invalid-schedule jobs draining or no longer growing.

### Required evidence

- deployed SHA: `c7696ee52564deff5964e75fc6b5270adf562b50`
- deploy path: manual `flyctl deploy --app events-bot-new-wngqia` from clean `hotfix/tg-ics-bad-time-20260608`
- regression tests: targeted pytest and `py_compile` listed below
- production log window: `2026-06-08T21:06:25Z` boot through `2026-06-08T21:09:46Z` worker evidence
- DB query evidence: `joboutbox` counts and sample legacy rows listed below
- confirmation, что fix reachable from `origin/main`: `origin/main` contains `c7696ee52564deff5964e75fc6b5270adf562b50`

## Immediate Mitigation

- Added scheduler-time validation so only parseable `date` + `time` combinations create calendar jobs.
- Added execution-time terminal skip for already queued invalid schedule jobs so permanent data errors do not retry forever.

## Corrective Actions

- Centralized calendar schedule support check around the same `parse_time_range` / `parse_iso_date` contract used by ICS builder.
- Marked invalid schedule calendar jobs as `skipped_invalid_schedule` in progress reporting and logs.
- Updated Telegram publishing docs and regression tests.

## Follow-up Actions

- [x] Back-merge the deployed hotfix SHA to `origin/main` before incident closure.
- [ ] Decide separately whether stale `ics_hash` sharing between Supabase and Telegram ICS should be split; current targeted run exposed an existing failing unit test but it is outside this retry-storm hotfix.

## Release And Closure Evidence

- deployed SHA: `c7696ee52564deff5964e75fc6b5270adf562b50`
- deploy path: manual `flyctl deploy --app events-bot-new-wngqia`; image `events-bot-new-wngqia:deployment-01KTMGVBH8R21299T4M62V5108`; machine `48e42d5b714228`, version `1255`.
- regression checks:
  - `python -m py_compile main.py`
  - `pytest tests/test_ics_pipeline.py::test_ics_jobs_skip_invalid_schedule_without_retry tests/test_tg_event_publish.py::test_schedule_event_update_tasks_skips_calendar_dependency_without_time tests/test_tg_event_publish.py::test_schedule_event_update_tasks_skips_calendar_dependency_for_bad_time tests/test_tg_event_publish.py::test_schedule_event_update_tasks_enqueues_tg_publish -q` -> `4 passed`
  - Full targeted files `tests/test_ics_pipeline.py tests/test_tg_event_publish.py -q` had `27 passed, 2 failed`; the failures were pre-existing/stale-surface tests (`test_ics_updates_on_change`, `test_ics_coalesced_jobs_and_semaphore`) and are tracked as follow-up, not part of this retry-storm fix.
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `job_outbox_worker=ok`.
  - Fly `status` showed image `deployment-01KTMGVBH8R21299T4M62V5108`, machine version `1255`, `1 passing` check.
  - Runtime logs at `2026-06-08T21:08:27Z` showed legacy jobs `tg_ics_post:5657`, `ics_publish:5678`, `tg_ics_post:5723`, `tg_ics_post:5727`, `tg_ics_post:5734` logging `skipped invalid schedule` and completing as `done/nochange`.
  - Production DB `joboutbox` counts changed from `ics_publish error=1105`, `tg_ics_post error=746` to `ics_publish error=1104`, `tg_ics_post error=742`; sampled rows had `status=done`, `last_error=null`, `last_result=nochange`.

## Prevention

- Regression coverage now distinguishes empty/no calendar time from non-empty invalid schedule text.
- Incident index routes future calendar dependency and Telegram event publish changes through this record.
