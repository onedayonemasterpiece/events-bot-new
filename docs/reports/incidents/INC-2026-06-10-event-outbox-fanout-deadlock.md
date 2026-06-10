# INC-2026-06-10 Event Outbox Fanout Deadlock

Status: monitoring
Severity: sev1
Service: Smart Update event publishing / JobOutbox / VK and Telegram fanout
Opened: 2026-06-10
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-08-tg-ics-bad-time-retry-storm`, `INC-2026-06-09-social-video-tg-publishing`
Related docs: `docs/features/vk-publishing/README.md`, `docs/features/tg-publishing/README.md`, `docs/operations/cron.md`

## Summary

On 2026-06-10 ordinary Smart Update event fanout became uneven: VK auto-import
continued parsing and Telegraph pages were built, but some managed VK and
Telegram event posts did not appear. The issue was not missing VK auth. It was a
JobOutbox scheduling deadlock between calendar jobs and event fanout jobs.

## User / Business Impact

- Event imports did not reliably produce the expected trio:
  Telegraph page + managed `klgdevents` VK post + `@kldevents` Telegram post.
- Operators saw shadow/debug VK posts and some ordinary posts, but the public
  rhythm across VK and Telegram was inconsistent.
- Fresh events such as `5868`, `5869`, `5870`, `5871`, and `5834` reached
  `expired` `vk_sync` / `tg_event_publish` states instead of being published.

## Detection

- Detected by operator report on 2026-06-10: VK parsing was visible, but regular
  VK/Telegram event publications were missing or uneven.
- Production runtime logs showed repeated
  `RUN skip ... blocked_by_deps=tg_ics_post:error,vk_sync:error` followed by
  `OUTBOX_EXPIRED`.
- Production DB showed `VK_USER_TOKEN` and `VK_ACCESS_TOKEN4` present, proving
  the primary symptom was not absent VK token configuration.

## Timeline

- 2026-06-10 00:36 UTC: event `5868` enqueued `ics_publish`,
  `telegraph_build`, `tg_ics_post`, `vk_sync`, and `tg_event_publish`.
- 2026-06-10 00:36..01:40 UTC: `vk_sync:5868` repeatedly skipped because an
  earlier same-event `tg_ics_post` job was pending, even though VK has no
  dependency on the calendar post. `tg_ics_post` waited for retrying
  `ics_publish` and expired before `ics_publish` later succeeded.
- 2026-06-10 04:44..09:10 UTC: the same pattern affected event `5871`.
- 2026-06-10 16:36..16:44 UTC: `job_outbox_worker` also hit transient
  `sqlite3.OperationalError: database is locked`, worsening delays and
  visibility.
- 2026-06-10 18:00 UTC: production DB inspection confirmed recent successful
  posts for some events and `expired` fanout rows for others.

## Root Cause

1. `_run_due_jobs_once_locked` blocked any due job for an event behind any
   earlier pending/running job for the same event. This accidentally serialized
   independent fanout tasks: `vk_sync` could wait behind `tg_ics_post`, even
   though only `tg_event_publish` depends on VK/calendar completion.
2. Dependent jobs aged toward TTL while waiting for a dependency that was
   actively retrying with backoff. A `tg_ics_post` job could expire while
   `ics_publish` was still retrying and later succeeding.
3. Once `tg_ics_post` or `vk_sync` became `error/expired`, `tg_event_publish`
   kept skipping on dependency blockers until it also expired.

## Contributing Factors

- Runtime logs were noisy with old page-rebuild blockers, making the live
  failure look like a token or scheduler rhythm issue at first glance.
- `JobOutbox` dependency logs did not clearly distinguish "waiting for retrying
  dependency" from "terminal dependency failed".
- Manual debug shadow VK posts increased the amount of postponed VK activity
  operators had to visually separate from normal posts.

## Automation Contract

### Treat as regression guard when

- changing `_run_due_jobs_once_locked`, `enqueue_job`, `JobOutbox`, `JOB_TTL`,
  `JOB_MAX_RUNTIME`, or dependency handling;
- changing `schedule_event_update_tasks`;
- changing `vk_sync`, `tg_event_publish`, `ics_publish`, or `tg_ics_post`;
- changing Smart Update VK auto-import fanout or Telegram event publish rhythm.

### Affected surfaces

- `main.py` JobOutbox picker and dependency resolution;
- Smart Update post-import fanout;
- managed event VK posts in `https://vk.com/klgdevents`;
- Telegram event posts in `https://t.me/kldevents`;
- calendar/ICS support jobs that feed Telegram buttons.

### Mandatory checks before closure or deploy

- Unit test proves `vk_sync` is not blocked by unrelated same-event calendar
  backlog.
- Unit test proves a dependent job does not expire while its dependency is
  retrying with a bounded future `next_run_at`.
- Existing Telegram event dependency tests still pass.
- Existing VK event publish scheduling tests still pass.
- Post-deploy production evidence shows affected future events requeued or
  terminally explained, with normal Telegraph + VK + Telegram fanout restored.

### Required evidence

- Test command output.
- Deployed SHA and manual Fly deploy evidence.
- Production DB query showing no fresh future event fanout stuck in
  `expired`/`pending` because of calendar/VK dependency deadlock.
- VK and Telegram URLs for catch-up publications or explicit skip reasons for
  events that should not be republished.

## Immediate Mitigation

- Production queue was inspected and stale far-future VK postponed anchors were
  manually corrected earlier on 2026-06-10.
- This incident fix adds scheduler prevention before the next catch-up/requeue.

## Corrective Actions

- Event pipeline jobs (`telegraph_build`, `vk_sync`, `tg_event_publish`,
  `ics_publish`, `tg_ics_post`) no longer use the broad same-event "prior job"
  blocker; ordering is controlled through explicit `depends_on`.
- Jobs waiting for a dependency that is retrying with a bounded future
  `next_run_at` are deferred to that retry window and have their freshness
  renewed instead of expiring while blocked.
- Regression tests pin both behaviors.

## Follow-up Actions

- [ ] Add operator-facing outbox diagnostics that summarize dependency blockers
  by root dependency, not every 2-second skip line.
- [ ] Consider a soft-dependency policy for `tg_ics_post` so Telegram event
  posts can publish without a calendar button when calendar delivery is
  terminally unavailable.

## Release And Closure Evidence

- deployed SHA: `27b0b5ec28a67a24ff76ef1f353392abb0477b92`
- deploy path: manual Fly deploy to `events-bot-new-wngqia`, Fly release `v1287`
- regression checks:
  - `tests/test_job_due_filter.py tests/test_job_outbox_depends.py tests/test_tg_event_publish.py tests/test_vk_source.py::test_vk_wall_source_still_gets_event_vk_sync tests/test_vk_source.py::test_ongoing_vk_wall_source_still_gets_event_vk_sync` -> `32 passed`
  - wider ICS-adjacent check
    `tests/test_ics_pipeline.py tests/test_db_ics_fields.py tests/test_job_due_filter.py tests/test_job_outbox_depends.py tests/test_tg_event_publish.py`
    -> `36 passed, 3 failed`; failures are the pre-existing
    `ics_file_id`/coalesced-order family tracked by
    `INC-2026-06-08-tg-ics-bad-time-retry-storm`, not caused by this outbox
    dependency change.
- post-deploy verification:
  - `/healthz` after deploy: `ready=true`, `scheduler=ok`,
    `job_outbox_worker=ok`, no issues.
  - Production runtime env check: `VK_AUTH_TOKEN` absent, but
    `VK_USER_TOKEN`, `VK_ACCESS_TOKEN4`, and `VK_TOKEN` present; VK publishing
    continued through the existing user-token path.
  - Catch-up requeued future events `5834`, `5867`, `5868`, `5869`, `5870`,
    and `5871` through `schedule_event_update_tasks`.
  - Post-catch-up DB check: all six have `vk_sync=done`, `telegraph_build=done`,
    `ics_publish=done` when applicable, and `tg_ics_post=done` when applicable.
  - Confirmed public catch-up posts:
    - `5834`: VK `https://vk.com/wall-231920894_2812`, Telegram `https://t.me/c/3954607218/271`.
    - `5867`: VK `https://vk.com/wall-231920894_2805`, Telegram `https://t.me/c/3954607218/273`.
    - `5868`: VK `https://vk.com/wall-231920894_2807`, Telegram pending for `2026-06-10 18:43:28 UTC`.
    - `5869`: VK `https://vk.com/wall-231920894_2815`, Telegram pending for `2026-06-10 18:53:28 UTC`.
    - `5870`: VK `https://vk.com/wall-231920894_2809`, Telegram pending for `2026-06-10 19:03:28 UTC`.
    - `5871`: VK `https://vk.com/wall-231920894_2811`, Telegram pending for `2026-06-10 19:13:28 UTC`.
  - Event `5872` was left unrequeued: it was a same-day event already past its
    local start time during verification and had an old `vk_sync:error:stale`
    row from before this fix.

## Prevention

- Keep independent fanout tasks independent; add new dependencies only through
  explicit `depends_on`.
- Do not let waiting jobs age out while the dependency is still in normal
  retry/backoff.
