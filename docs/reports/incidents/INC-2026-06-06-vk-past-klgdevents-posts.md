# INC-2026-06-06-vk-past-klgdevents-posts Past events published to klgdevents

Status: open
Severity: sev2
Service: VK outbound event publishing (`klgdevents`)
Opened: 2026-06-06
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-19-vk-posts-personal-author`, `INC-2026-06-05-vk-story-forward-wall-first`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Operator reported that fully past events are appearing in `https://vk.com/klgdevents`, with `https://vk.com/wall-231920894_2270` as an example. The inbound Smart Update path already skips many past automated candidates, and the VK prune job can delete some stale managed posts after the fact, but the outbound `vk_sync` path still allowed an active/non-silent event row that had already ended to create a new managed community post.

## User / Business Impact

- `klgdevents` followers can see stale event announcements in the community feed.
- VK recommendations can amplify already-ended events until the prune job removes eligible posts.
- Operators lose trust in the database-backed VK publication path because past rows can still produce fresh wall posts.

## Detection

- Detected by operator report on 2026-06-06 with a concrete VK post URL.
- Live VK API inspection was blocked in the local environment because the available VK token is IP-bound.
- Fly production runtime/DB evidence collection was blocked in the local environment because `flyctl` had no auth token.

## Timeline

- 2026-06-06 UTC — operator reported past events appearing in `klgdevents`, example `wall-231920894_2270`.
- 2026-06-06 UTC — code review found `schedule_event_update_tasks` only skipped `vk_sync` for non-active/silent rows or already-managed `klgdevents` URLs, not for fully past events.
- 2026-06-06 UTC — prevention fix prepared on `hotfix/vk-past-klgdevents-20260606`.

## Root Cause

1. `schedule_event_update_tasks` enqueued `vk_sync` for active, non-silent events even when their `date`/`end_date` was strictly before the current local day.
2. `job_sync_vk_source_post` had no second boundary check before calling `sync_vk_source_post`, so already-pending jobs could still reach `wall.post` after a deploy.
3. The existing `prune_past_event_vk_posts` cleanup was post-publication mitigation, not a prevention guard.

## Contributing Factors

- The VK publishing doc explicitly said imported VK events should still receive managed `klgdevents` posts, but did not qualify that with event freshness.
- Regression tests covered external VK source fanout and managed-post dedupe, but not the fully-past event case.
- Production runtime file logging is disabled by default, and this local environment lacked Fly auth for immediate live evidence collection.

## Automation Contract

### Treat as regression guard when

- Changing `schedule_event_update_tasks`, `enqueue_job` behavior for `JobTask.vk_sync`, `job_sync_vk_source_post`, `sync_vk_source_post`, or managed `klgdevents` URL detection.
- Changing `Event.date` / `Event.end_date` lifecycle semantics or cleanup/prune rules for past events.
- Changing VK post-prune behavior in a way that could be mistaken for publish prevention.

### Affected surfaces

- `main.py::schedule_event_update_tasks`
- `main.py::job_sync_vk_source_post`
- `main.py::_event_has_managed_vk_post`
- `main_part2.py::prune_past_event_vk_posts`
- `docs/features/vk-publishing/README.md`
- Production env: `VK_EVENTS_GROUP_ID`, `VK_AFISHA_GROUP_ID`, `ENABLE_VK_POST_PRUNE`
- External system: VK `wall.post` for `vk.com/klgdevents`

### Mandatory checks before closure or deploy

- Unit tests prove:
  - future VK-imported events still enqueue `vk_sync`;
  - fully past VK-imported events do not enqueue `vk_sync`;
  - already-pending `job_sync_vk_source_post` for a fully past event returns without calling `sync_vk_source_post`;
  - managed `klgdevents` posts still suppress duplicate `vk_sync`.
- Production config check confirms `VK_EVENTS_GROUP_ID=231920894` or the expected target group.
- Runtime evidence check follows `docs/operations/runtime-logs.md`: inspect file mirror env/dir first; if unavailable, record that fact and use Fly logs / production DB snapshot / ops rows.
- Post-deploy smoke or DB audit confirms no pending `vk_sync` jobs remain for fully past events and no new managed `klgdevents` posts are created for fully past rows.
- Release-governance check: deployed SHA must be reachable from `origin/main`.

### Required evidence

- Test command output.
- Commit SHA / branch / deploy path.
- Production query output for past events with pending `vk_sync` and managed `source_vk_post_url`.
- VK/API or DB evidence for `wall-231920894_2270`: event id, title, event date/end date, and whether cleanup/remediation deleted or retained the post.
- Confirmation that fix is reachable from `origin/main` before incident closure.

## Immediate Mitigation

- Code-side prevention prepared: fully past events no longer enqueue `vk_sync`, and the `vk_sync` job handler skips fully past events before external VK side effects.
- Existing VK prune remains the after-the-fact cleanup path for already-created eligible managed posts.

## Corrective Actions

- Added `_event_vk_publish_end_date` / `_event_has_ended_before_today` guard in `main.py`.
- Updated `schedule_event_update_tasks` to skip `vk_sync` for fully past events while leaving Telegraph/page cleanup available.
- Updated `job_sync_vk_source_post` to return before `sync_vk_source_post` for fully past events, covering already-pending jobs.
- Added regression tests in `tests/test_vk_source.py`.
- Updated canonical VK publishing docs and `CHANGELOG.md`.

## Follow-up Actions

- [ ] events-bot / after Fly auth is available / collect live production DB evidence for `wall-231920894_2270` and any other managed `klgdevents` posts created for fully past events.
- [ ] events-bot / after deploy / run a production audit for pending `vk_sync` jobs whose event ended before today and record the result here.
- [ ] events-bot / after deploy / confirm the VK prune job or manual remediation handled already-created stale managed posts that should not remain visible.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks:
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /home/dev/projects/events-bot-new/.venv/bin/pytest -q -p pytest_asyncio.plugin tests/test_vk_source.py tests/test_vk_post_prune.py` -> `39 passed in 7.32s` (pytest process printed the terminal result but did not exit; local hung process was killed after result collection).
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py tests/test_vk_source.py` -> passed.
- post-deploy verification: —

## Prevention

- The VK publish boundary now has prevention before `wall.post`, not only a cleanup job after stale posts appear.
- Regression tests pin the distinction between current/future VK source imports (still publish) and fully past rows (do not publish).
