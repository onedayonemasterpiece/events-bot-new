# INC-2026-06-27 VK prune starvation kept past posts recommendable

Status: mitigated
Severity: sev2
Service: managed `klgdevents` VK wall / past-event cleanup
Opened: 2026-06-27
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-06-vk-past-klgdevents-posts`, `INC-2026-06-24-vk-past-actuals`
Related docs: `docs/features/vk-publishing/autodeletevkposts.md`, `docs/features/vk-publishing/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-27 the operator saw VK recommending the managed `klgdevents` post
for the already-finished event `Ведущий ребёнка` (`event_id=6129`, event date
`2026-06-21 14:00`, VK `wall-231920894_3678`). The twice-daily
`vk_post_prune` cleanup was enabled and running, but its capped batch was
starved by older historical `missing` / repost-protected candidates, so recent
past live posts could remain visible and recommendable.

## User / Business Impact

- VK users could see/recommend posts for events that had already passed.
- The operator-visible contract from `autodeletevkposts.md` was violated:
  recent past managed posts without reposts/comments should be removed.

## Detection

- Detected by operator report on 2026-06-27.
- Confirmed through authenticated VK API, production SQLite, and
  `/data/runtime_logs`.

## Timeline

- 2026-06-17 16:52 UTC — `event_id=6129` was imported from source VK
  `wall-29891284_13867`.
- 2026-06-17 18:00 UTC — VK exposed the live managed post as
  `wall-231920894_3678` with `postponed_id=3668`.
- 2026-06-21 14:00 Europe/Kaliningrad — event happened.
- 2026-06-27 00:30 UTC — scheduled `vk_post_prune` ran and logged
  `candidates=917 deleted=3 kept_reposts=33 missing=264 errors=0`; the reported
  post was not reached because the capped first batch was dominated by old rows.
- 2026-06-27 11:35-11:40 UTC — investigation confirmed `wall-231920894_3678`
  was live with `reposts=0`, `comments=0`, not pinned; recent-window audit found
  12 deletable live past posts and 235 already-missing managed URLs.
- 2026-06-27 11:39 UTC — immediate mitigation deleted the 12 verified
  recent-window live/deletable managed posts and cleared their exact
  `source_vk_post_url` values after backing up rows.

## Root Cause

1. `prune_past_event_vk_posts` selected all past/non-active managed URL rows
   without a deterministic order and then applied `VK_POST_PRUNE_LIMIT`.
2. Production had accumulated a large historical backlog of managed VK URLs
   whose posts were already missing or protected by reposts.
3. Missing rows were intentionally not cleared on lookup miss to allow retry, so
   they could keep occupying future capped batches until a separate cleanup
   removed the event row.

## Contributing Factors

- Previous cleanup fixes did not pin the invariant that recent past posts must
  be prioritized over old missing/protected rows.
- Runtime logs exposed aggregate counts, but did not alert when `deleted` stayed
  low while recent past live posts still existed.

## Automation Contract

### Treat as regression guard when

- Changing `main_part2.py::prune_past_event_vk_posts`, `vk_post_prune_scheduler`,
  `VK_POST_PRUNE_LIMIT`, or managed `klgdevents` URL cleanup rules.
- Changing event row retention / `cleanup_old_events` for past events.
- Repairing or auditing managed `klgdevents` posts for past events.

### Affected surfaces

- `main_part2.py::prune_past_event_vk_posts`
- `scheduling.py` `vk_post_prune` cron registration and heavy gate
- Production SQLite `event.source_vk_post_url`
- VK API `wall.getById`, `wall.get`, `wall.delete`

### Mandatory checks before closure or deploy

- Regression test proves a fresh past live post is processed under
  `VK_POST_PRUNE_LIMIT` even when more than the cap of older missing managed URLs
  exists.
- Authenticated VK API check proves the reported `wall-231920894_3678` is
  deleted and the event row no longer points at it.
- Recent-window VK audit (`date >= 2026-06-20`, `date < today`) has no remaining
  live/deletable managed posts with `reposts=0` and `comments=0`, excluding
  protected repost/comment/pinned posts.
- Runtime log mirror check records the latest scheduled `vk_post_prune` status.
- If deployed, deployed SHA must be reachable from `origin/main` and `/healthz`
  must be ready.

### Required evidence

- Production DB backup table name and post-repair `PRAGMA quick_check`.
- VK API before/after snippets for `wall-231920894_3678`.
- Test/compile output for changed code.
- Deploy SHA and post-deploy health, if code is deployed.

## Immediate Mitigation

- Backed up touched production event rows to
  `codex_backup_vk_prune_starvation_20260627_event`.
- Deleted 12 recent-window live/deletable managed VK posts with owner
  `-231920894`, `reposts=0`, `comments=0`, and not pinned:
  events `6172`, `6244`, `6138`, `5807`, `6249`, `6145`, `6129`, `6120`,
  `6091`, `6047`, `5804`, and `6133`.
- Cleared each deleted exact `event.source_vk_post_url`; `PRAGMA quick_check`
  returned `ok`.
- VK `wall.getById` after deletion returned `Пост удалён` for each deleted id,
  including `wall-231920894_3678`.

## Corrective Actions

- `prune_past_event_vk_posts` now orders candidates by `Event.date DESC,
  Event.id DESC` before applying `VK_POST_PRUNE_LIMIT`, so fresh past posts are
  handled before historical missing/protected backlog.
- Added a regression test for old missing backlog starvation.
- Updated VK publishing docs and `CHANGELOG.md`.

## Follow-up Actions

- [x] Deploy the code fix from a clean worktree and record reachable
  `origin/main` SHA.
- [ ] After the next natural scheduled prune tick, verify logs show recent past
  candidates are not starved.
- [ ] Consider a separate safe cleanup policy for long-term `missing` managed
  VK URLs so they do not inflate candidate counts indefinitely.

## Release And Closure Evidence

- deployed SHA: `e510e617f8fecee06f42c1033dcdca15719c626e`
  (`origin/main`, also `origin/hotfix/vk-past-cleanup-20260627`).
- deploy path: clean linked worktree
  `/home/dev/projects/events-bot-new-worktrees/vk-past-cleanup-20260627`,
  branch `hotfix/vk-past-cleanup-20260627`, `flyctl deploy --config fly.toml
  --app events-bot-new-wngqia --remote-only`.
- Fly evidence: image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KW4EA7RJZS628CC7SNWD1M6M`,
  machine `683961db016e28`, Fly version `1505`, checks `1 total, 1 passing`.
- regression checks:
  - `/home/dev/projects/events-bot-popular-tg-reposts-deploy/.venv/bin/python -m py_compile main_part2.py tests/test_vk_post_prune.py` — passed.
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /home/dev/projects/events-bot-popular-tg-reposts-deploy/.venv/bin/python -m pytest -q -p pytest_asyncio.plugin tests/test_vk_post_prune.py` — `17 passed in 12.07s`.
- production mitigation evidence:
  - backup table: `codex_backup_vk_prune_starvation_20260627_event`.
  - deleted 12 recent-window live/deletable managed posts; VK post-delete
    verification returned `Пост удалён` for all 12 ids.
  - `event_id=6129` after repair:
    `source_vk_post_url=NULL`, `vk_repost_url=NULL`.
  - `wall-231920894_3678` after repair returned `Пост удалён`.
  - recent-window audit after repair:
    `recent_deletable=[]`, `recent_missing=235`, protected repost rows:
    `event_id=5202` and `event_id=6094`.
  - `PRAGMA quick_check=ok`.
- post-deploy verification:
  - in-machine `http://127.0.0.1:8080/healthz` returned HTTP `200`,
    `ready=true`, `db=ok`, scheduler/task statuses `ok`, `issues=[]`.

## Prevention

The prevention is freshness-prioritized capped cleanup: when capacity is
limited, recent past posts that VK is still likely to recommend must be checked
before old historical rows whose posts are already gone or protected.
