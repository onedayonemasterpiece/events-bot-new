# INC-2026-06-13-vk-postponed-event-slot-late-anchor

Status: closed
Severity: sev2
Service: VK event publishing / VK auto-import / postponed slot reservation
Opened: 2026-06-13
Closed: 2026-06-13
Owners: Codex / operator
Related incidents: `INC-2026-06-12-vk-partial-media-family-cta`, `INC-2026-06-12-raffle-source-publication-false-skip`
Related docs: `docs/features/vk-publishing/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/operations/runtime-logs.md`

## Summary

Manual `/vk_auto_import --limit=1` created event `5958` and a managed `klgdevents` VK postponed post `wall-231920894_3143`, but the post was scheduled for `2026-06-13 15:30 Europe/Kaliningrad` instead of the first free morning slot. The postponed queue only had same-day promo anchors at `10:40` and `15:20`; ordinary event posts should fill the first free slot from the morning cadence and then proceed every 10 minutes.

## User / Business Impact

- A fresh event imported from VK was queued late in the day even though morning slots were available.
- Operators could not rely on `/vk_auto_import --limit=1` to place new event announcements into the expected morning cadence.
- Future VK auto-import bursts could keep appending after late promo anchors and leave earlier publication slots unused.

## Detection

- Operator observed the newly created VK postponed post at `15:30` local.
- Production DB showed `ops_run #2357`, `kind='vk_auto_import'`, `trigger='manual'`, `status='success'`, `events_created=1`, `batch_id='auto:1781308789'`.
- Production DB linked inbox `8580` (`wall-32547811_10891`) to event `5958`, with `event.source_vk_post_url='https://vk.com/wall-231920894_3143'`.
- VK API `wall.get(filter=postponed)` for `owner_id=-231920894` showed same-day anchors at `10:40`, `15:20`, and the new event post at `15:30` Europe/Kaliningrad.

## Timeline

- 2026-06-12 23:59:49 UTC: manual `vk_auto_import` run `#2357` started.
- 2026-06-13 00:01:15 UTC: event `5958` was created.
- 2026-06-13 00:01:17 UTC: `vk_inbox_import_event` linked inbox `8580` to event `5958`.
- 2026-06-13 00:02:06 UTC: `vk_sync` for event `5958` completed with `last_result=https://vk.com/wall-231920894_3143`.
- 2026-06-13 02:09 Europe/Kaliningrad: production postponed queue inspection showed `wall-231920894_3143` scheduled for `15:30`.

## Root Cause

1. `_reserve_vk_postponed_publish_date()` fetched only the latest postponed timestamp for the target community.
2. `_vk_postponed_next_slot()` then reserved `latest + interval`, so any late same-day promo anchor forced ordinary event posts to the tail after that anchor.
3. The algorithm did not search for free gaps from the morning start hour and therefore ignored open slots before the latest anchor.

## Contributing Factors

- The original postponed queue contract was written as "after latest postponed post", which was too coarse once promo posts could occupy explicit later slots.
- Tests covered morning start and latest-anchor spacing, but not a late promo anchor with earlier free slots.

## Automation Contract

### Treat as regression guard when

- Changing `main_part2.py::_vk_postponed_next_slot`, `_reserve_vk_postponed_publish_date`, `_fetch_vk_postponed_anchor_timestamps`, or `post_to_vk`.
- Changing promo VK scheduling, VK auto-import publication, or `VK_POSTPONED_*` runtime knobs.

### Affected surfaces

- `main_part2.py::post_to_vk`
- VK postponed queue `wall.get(filter=postponed)`
- VK auto-import managed event `vk_sync`
- `docs/features/vk-publishing/README.md`

### Mandatory checks before closure or deploy

- `python3 -m py_compile main.py main_part2.py tests/test_vk_actor.py`
- `pytest -q tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_kaliningrad_morning_and_interval tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_first_morning_gap_before_promo_anchors tests/test_vk_actor.py::test_vk_postponed_next_slot_steps_through_occupied_morning_slots tests/test_vk_actor.py::test_fetch_vk_latest_postponed_prefers_user_actor`
- Production deploy must not interrupt a running `tg_monitoring` Kaggle job.
- Post-deploy smoke must inspect the postponed queue or run the slot helper against the observed anchors and confirm the next event slot is morning-first.

### Required evidence

- deployed SHA: `664784a99c9bba3ef1ccd038e7acf78288164521`
- regression checks:
  - `python3 -m py_compile scheduling.py tests/test_scheduling.py main.py main_part2.py tests/test_vk_actor.py`
  - `pytest -q tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_tg_monitoring_after_crash tests/test_scheduling.py::test_critical_scheduler_watchdog_defers_tg_monitoring_when_recovery_job_exists tests/test_scheduling.py::test_critical_scheduler_watchdog_dispatches_vk_auto_import_after_slot_crash tests/test_scheduling.py::test_runtime_health_status_reports_critical_monitoring_jobs tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_kaliningrad_morning_and_interval tests/test_vk_actor.py::test_vk_postponed_next_slot_uses_first_morning_gap_before_promo_anchors tests/test_vk_actor.py::test_vk_postponed_next_slot_steps_through_occupied_morning_slots tests/test_vk_actor.py::test_fetch_vk_latest_postponed_prefers_user_actor` printed `8 passed in 0.80s`; the process then required Ctrl-C during Python thread shutdown after the pytest summary.
- production evidence:
  - `ops_run #2357` created event `5958`
  - event `5958` stored `source_vk_post_url=https://vk.com/wall-231920894_3143`
  - VK postponed queue had `10:40`, `15:20`, `15:30` same-day anchors
- recovery evidence:
  - `wall-231920894_3143` was rescheduled with VK `wall.edit` to
    `2026-06-13 06:00 Europe/Kaliningrad`.

## Immediate Mitigation

- Do not run more VK event publication bursts expecting the old algorithm to fill morning gaps until the fix is deployed.
- Existing wrongly scheduled post `wall-231920894_3143` was rescheduled to the first safe free morning slot, `2026-06-13 06:00 Europe/Kaliningrad`, through VK `wall.edit`.

## Corrective Actions

- Fetch the active postponed timestamps list, not only the maximum timestamp.
- Reserve the first free slot from the normalized morning/current candidate using `VK_POSTPONED_MIN_INTERVAL_SECONDS`.
- Keep far-future and Afisha Engagement debug filtering before slot selection.
- Document the morning-first gap-search contract.

## Follow-up Actions

- [x] Codex: deploy from `origin/main` after adding Telegram Monitoring registry-race protection, so the deploy does not trigger a second `TELEGRAM_AUTH_BUNDLE_S22` push.
- [x] Codex: reschedule `wall-231920894_3143` away from `15:30` if VK accepts postponed `wall.edit`.
- [ ] Codex: inspect whether old 2026-06-14/15 postponed backlog should be cleaned separately; this incident fixes selection, not historical cleanup.

## Release And Closure Evidence

- deployed SHA: `664784a99c9bba3ef1ccd038e7acf78288164521`
- deploy image: `registry.fly.io/events-bot-new-wngqia:deployment-01KTZ5VVRNGQYEV17ZJP997C86`
- deploy path: `origin/main` -> `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks:
  - `python3 -m py_compile scheduling.py tests/test_scheduling.py main.py main_part2.py tests/test_vk_actor.py`
  - targeted pytest command above printed `8 passed in 0.80s`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `vk_auto_import=ok`, `critical_scheduler_watchdog=ok`, and `tg_monitoring=ok`.
  - Fly status shows image `events-bot-new-wngqia:deployment-01KTZ5VVRNGQYEV17ZJP997C86`, machine version `1370`, `1 passing` check.
  - VK postponed queue still has `wall-231920894_3143` at `2026-06-13T06:00:00+02:00`.
  - `vk_auto_import_next_run=2026-06-13T04:15:00+00:00` (`06:15 Europe/Kaliningrad`), so the next ordinary auto-import is still a morning run, not an evening-only fallback.

## Prevention

- Tests now cover a late promo anchor with free morning slots and consecutive occupied morning slots.
- The feature doc now states the first-free-slot contract explicitly.
