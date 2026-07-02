# INC-2026-06-24 VK actuals published past events

Status: mitigated
Severity: sev2
Service: Promo VK publication / Afisha Engagement / managed `klgdevents` VK wall
Opened: 2026-06-24
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-06-14-afishaengagement-shadow-fallback-regression`, `INC-2026-06-14-vk-publication-cta-plain-duplicate.md`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup.md`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/afishaengagement/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-24 the VK community `klgdevents` showed a fresh/actual wall post for the already-finished event `Калининград: Город-сад или микрорайон для жизни у моря` (`event_id=6244`, event time `2026-06-23 18:30`). Authenticated VK API showed live post `wall-231920894_4345` at `2026-06-24 11:15 UTC`; it was a promoted Afisha Engagement debug-shadow copy originally recorded as postponed `wall-231920894_4076`.

The same audit found two more stale future postponed items:

- `wall-231920894_4159`: Afisha Engagement debug-shadow copy for the same `2026-06-23 18:30` event, scheduled for `2026-06-25 16:15 UTC`.
- `wall-231920894_4149`: ordinary managed VK event post for `event_id=6288`, `Где заканчивается обычная усталость...`, event time `2026-06-22 15:00`, scheduled for `2026-06-24 16:30 UTC`.

## User / Business Impact

- Readers saw an event that happened yesterday as a fresh/current VK post.
- The managed VK postponed queue contained additional posts that would publish after their event dates.
- The failure undermines the core “what to attend now/future” contract even when the source event row itself has a correct date.

## Detection

- Detected by operator report on 2026-06-24: the VK Afisha community showed the `Калининград: город-сад...` post as the latest actual post.
- Evidence was collected through authenticated VK API, production SQLite, and `/data/runtime_logs` file mirror. Artifacts are under `artifacts/codex/INC-2026-06-24-vk-past-actuals/`.

## Timeline

- 2026-06-20 08:57 UTC — event `6244` imported from `https://t.me/kenigevents/4104` with correct event date `2026-06-23 18:30`.
- 2026-06-21 11:16 UTC — promo/Afisha Engagement recorded debug-shadow exposure `477`, scheduled for `2026-06-24 11:15 UTC`.
- 2026-06-22 16:16 UTC — promo/Afisha Engagement recorded debug-shadow exposure `490`, scheduled for `2026-06-25 16:15 UTC`.
- 2026-06-24 11:15 UTC — debug copy promoted to live VK post `wall-231920894_4345`, after the event had already finished.
- 2026-06-24 12:10-12:18 UTC — incident investigation confirmed the VK post and stale queue items; live bad post was already deleted by cleanup before the final verification, `wall-231920894_4149` was deleted, DB audit rows were reconciled, and the VK postponed queue was rechecked.

## Root Cause

1. Afisha Engagement debug-shadow scheduling used a fixed future delay (`debug_publish_delay_days`, typically 3 days) and did not compare the selected VK publish timestamp with the event start. A debug sample created before a valid future event could therefore become public after the event had happened if cleanup missed it.
2. Promo candidate selection used event date (`date >= today`) but not same-day start time, so timed events could remain eligible after their start time within the same local day.
3. Managed VK event fanout skipped only events whose end date was before today. It did not skip a one-day event whose start time had already passed earlier today, allowing late imports to create stale VK/TG fanout jobs.
4. VK postponed IDs can resolve differently after publication; cleanup must reconcile promoted live IDs instead of trusting the originally stored postponed URL alone.

## Contributing Factors

- Debug-shadow posts intentionally include a future publish delay to allow visual review, but this is unsafe for near-term events without an event-start cap.
- `event.lifecycle_status` remains `active` for historical rows, so public surfaces must apply time-aware eligibility rather than rely on lifecycle alone.
- Runtime logs are short-retention; the file mirror was available for current evidence, but the original 2026-06-21 scheduling log lines were not all retrievable by exact URL.

## Automation Contract

### Treat as regression guard when

- Changing Promo VK publication/repost/story/carousel selection, `promo.py::_events_for_target`, or `run_promo_vk_activities`.
- Changing Afisha Engagement debug-shadow scheduling or cleanup.
- Changing `schedule_event_update_tasks`, managed VK `vk_sync`, or Telegram event publishing job enqueue conditions.
- Repairing or pruning managed `klgdevents` VK wall/postponed posts.

### Affected surfaces

- `promo.py` campaign target selection and VK activity runner.
- `afishaengagement.py` debug-shadow schedule selection.
- `main.py::schedule_event_update_tasks` managed VK/TG fanout gating.
- Production SQLite: `event`, `promo_exposure`, `joboutbox`.
- VK API surfaces: `wall.getById`, `wall.get filter=postponed`, `wall.delete`.

### Mandatory checks before closure or deploy

- Authenticated VK API check proves no `klgdevents` postponed posts remain for already-started/past events from this incident family.
- Regression coverage proves:
  - Promo target selection excludes same-day timed events whose start time has passed.
  - Afisha Engagement debug-shadow copies are not scheduled at or after the event start (date-only events: not on the event day).
  - `schedule_event_update_tasks` does not enqueue `vk_sync` or `tg_event_publish` for same-day events that already started.
- `python3 -m py_compile` passes for changed modules/tests; pytest results are attached when dependencies are available.
- If production data is repaired, `PRAGMA quick_check` passes and deleted VK URLs are either removed from canonical event fields or marked as deleted exposure rows.
- If code is deployed, deployed SHA is reachable from `origin/main` and `/healthz` is ready after deploy.

### Required evidence

- VK API before/after artifacts for `wall-231920894_4345`, `4149`, `4159`, and current postponed queue.
- Prod DB evidence for `event_id=6244`, `event_id=6288`, `promo_exposure` rows `477`/`490`, and `PRAGMA quick_check` after repair.
- Runtime log mirror probe/search artifact.
- Test/compile output and deployed SHA.

## Immediate Mitigation

- Deleted stale managed VK postponed post `wall-231920894_4149` after owner/text/date verification.
- Verified `wall-231920894_4345` is deleted and `wall-231920894_4159` is absent.
- Verified current `klgdevents` postponed queue contains no `21/22/23 июня` or Afisha Engagement debug-shadow stale posts.
- Cleared deleted managed VK URL from production `event_id=6288.source_vk_post_url`.
- Marked Afisha Engagement debug exposure rows `477` and `490` as `VK_DELETED_DEBUG` (they had already been cleanup-reconciled) and captured `PRAGMA quick_check=ok`.

## Corrective Actions

- Added time-aware promo eligibility: same-day timed promo candidates are excluded once their start time has passed; local `Europe/Kaliningrad` date is used for the promo day.
- Added Afisha Engagement debug-shadow start cap: a shadow copy is skipped if the chosen publish timestamp is at or after event start; date-only events require the debug copy to publish before the event day.
- Added same-day start guard to `schedule_event_update_tasks` so managed VK/TG fanout is not enqueued for one-day timed events that have already started.
- Added focused regression tests for all three gates and documented the contract.

## Follow-up Actions

- [ ] Run the focused pytest tests in an environment with project test dependencies installed.
- [x] Deploy the prevention code from a clean worktree and record the reachable `origin/main` SHA.
- [ ] Add a reusable VK postponed audit command that parses event date lines and flags any managed postponed post whose event start is already past.

## Release And Closure Evidence

- deployed SHA: `3d7d504d9df5d25026b68ca09a3e501291b5eaf3` (reachable from `origin/main`)
- deploy path: clean linked worktree `/home/dev/projects/events-bot-inc-20260624-vk-past`, branch `hotfix/2026-06-24-vk-past-event-guard`, pushed to `origin/main`, `flyctl deploy -a events-bot-new-wngqia --remote-only`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KVWSA6XGPSX7DE3E11P875SX`, Fly machine `683961db016e28` version `1475`
- regression checks:
  - `python3 -m py_compile promo.py afishaengagement.py main.py tests/test_promo.py tests/test_afishaengagement.py tests/test_tg_event_publish.py` — passed locally.
  - `python3 -m pytest ...` — blocked locally: base Python had no `pytest`/`sqlmodel`; attempted ephemeral venv install from `requirements.txt`, but pip failed with `OSError: [Errno 28] No space left on device`.
- post-deploy verification: Fly status showed `1 total, 1 passing`; in-machine `http://127.0.0.1:8080/healthz` returned HTTP 200 with `ready=true`, `db=ok`, scheduler `promo_vk=ok`, and no issues. Post-deploy VK API verification showed `wall-231920894_4345` deleted, `wall-231920894_4149`/`4159` absent, and current postponed queue had no `21/22/23 июня` or Afisha Engagement debug-shadow stale posts.

## Prevention

This incident remains `mitigated`, not `closed`, until the focused tests or equivalent dependency-complete CI evidence are attached. The prevention code has been deployed from a SHA reachable from `origin/main`.
