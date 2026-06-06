# INC-2026-06-06-vk-past-klgdevents-posts Past events published to klgdevents

Status: monitoring
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
- Live VK API inspection was completed with the service VK token after release.
- Fly production runtime/DB evidence collection was completed with the service Fly token from `~/.fly/config.yml`; `flyctl auth whoami` alone was not a sufficient auth discovery check.

## Timeline

- 2026-06-06 UTC — operator reported past events appearing in `klgdevents`, example `wall-231920894_2270`.
- 2026-06-06 UTC — code review found `schedule_event_update_tasks` only skipped `vk_sync` for non-active/silent rows or already-managed `klgdevents` URLs, not for fully past events.
- 2026-06-06 UTC — prevention fix prepared on `hotfix/vk-past-klgdevents-20260606`.
- 2026-06-06 UTC — release auth discovery was corrected: Fly service token found in `~/.fly/config.yml`, and AGENTS/release-governance docs updated to require checking that location before declaring auth unavailable.
- 2026-06-06 UTC — PR #5 merged to `origin/main` as merge commit `428d59d33830f16d9aca0aec0cd0f132443ed9f7`.
- 2026-06-06 UTC — deployed to Fly app `events-bot-new-wngqia`, release version `1202`.
- 2026-06-06 UTC — post-deploy health and production DB audit confirmed no pending/running `vk_sync` jobs for fully past events.
- 2026-06-06 UTC — manual prune pass deleted 40 stale managed VK posts and found 10 already missing posts with zero API errors; scheduled prune remains responsible for draining the remaining backlog.

## Root Cause

1. `schedule_event_update_tasks` enqueued `vk_sync` for active, non-silent events even when their `date`/`end_date` was strictly before the current local day.
2. `job_sync_vk_source_post` had no second boundary check before calling `sync_vk_source_post`, so already-pending jobs could still reach `wall.post` after a deploy.
3. The existing `prune_past_event_vk_posts` cleanup was post-publication mitigation, not a prevention guard.

## Contributing Factors

- The VK publishing doc explicitly said imported VK events should still receive managed `klgdevents` posts, but did not qualify that with event freshness.
- Regression tests covered external VK source fanout and managed-post dedupe, but not the fully-past event case.
- Initial Fly auth discovery stopped too early at `flyctl auth whoami` instead of checking the service token in `~/.fly/config.yml`; release-governance docs now pin the required lookup.

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

- [x] events-bot / after Fly auth is available / collect live production DB evidence for `wall-231920894_2270` and any other managed `klgdevents` posts created for fully past events.
- [x] events-bot / after deploy / run a production audit for pending `vk_sync` jobs whose event ended before today and record the result here.
- [x] events-bot / after deploy / confirm manual remediation handled the first batch of already-created stale managed posts.
- [ ] events-bot / scheduled prune follow-up / confirm the remaining stale managed-post backlog continues to drain without VK API errors.

## Release And Closure Evidence

- deployed SHA:
  - PR branch head: `b884266352acbecdbee22726cf54b09f12b90d47`.
  - `origin/main` merge commit: `428d59d33830f16d9aca0aec0cd0f132443ed9f7`.
  - `git branch -r --contains b884266352acbecdbee22726cf54b09f12b90d47` includes `origin/main`.
- deploy path:
  - Manual Fly deploy from a clean worktree after PR #5 was merged to `origin/main`.
  - Command: `flyctl deploy --config fly.toml --app events-bot-new-wngqia --remote-only`.
  - Auth: process-local `FLY_ACCESS_TOKEN` loaded from `~/.fly/config.yml`; token value was not printed.
  - Fly release: version `1202`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KTDY1BM76DYWGFSB6G2TDYDV`, machine `48e42d5b714228`, `1/1` checks passing.
- regression checks:
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /home/dev/projects/events-bot-new/.venv/bin/pytest -q -p pytest_asyncio.plugin tests/test_vk_source.py tests/test_vk_post_prune.py` -> `39 passed in 7.32s` (pytest process printed the terminal result but did not exit; local hung process was killed after result collection).
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m py_compile main.py tests/test_vk_source.py` -> passed.
- runtime log evidence:
  - Production file mirror enabled: `ENABLE_RUNTIME_FILE_LOGGING=1`, `RUNTIME_LOG_DIR=/data/runtime_logs`, basename `events-bot.log`, retention `24h`.
  - Active `/data/runtime_logs/events-bot.log` and rotated files exist.
  - Post-deploy logs contained unrelated Kaggle recovery `GetKernelSessionStatus` 500 noise; `/healthz` stayed ready with no issues.
- post-deploy verification:
  - `https://events-bot-new-wngqia.fly.dev/healthz` returned `{"ok": true, "ready": true, ... "issues": []}` after deploy and again after the prune pass.
  - Production DB audit for fully past events with pending/running `vk_sync` returned `0` rows for `today_utc=2026-06-06`.
  - Existing old `vk_sync` rows for fully past events were only historical `error` rows with `next_run_at` in 2035/2036, so they will not execute.
  - VK API check for `wall-231920894_2270` returned text preview `Пост удалён`; no live `event` row matched that wall URL in `source_vk_post_url`, `source_post_url`, or `vk_repost_url`.
  - Manual prune dry-run found `853` stale managed-post candidates.
  - Manual prune real run with `limit=50` deleted `40`, found `10` missing, kept `0` pinned/reposts, and had `0` errors.
- residual risk:
  - The prevention fix is deployed; the remaining known risk is backlog drain for already-created stale managed posts. The scheduled prune job should continue cleanup, with a follow-up audit required before marking closed.
  - Branch drift audit found unrelated remote branch `origin/hotfix/google-ai-reserve-overflow` still ahead of `origin/main` by one commit.

## Prevention

- The VK publish boundary now has prevention before `wall.post`, not only a cleanup job after stale posts appear.
- Regression tests pin the distinction between current/future VK source imports (still publish) and fully past rows (do not publish).
