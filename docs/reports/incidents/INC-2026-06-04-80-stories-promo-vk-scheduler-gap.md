# INC-2026-06-04 80 Stories Promo VK Scheduler Gap

Status: open
Severity: sev2
Service: Promo VK scheduler / `80 историй о главном` VK campaign
Opened: 2026-06-04
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-04-kraftmarket271-tg-monitoring-tpm-import-cancel`, `INC-2026-06-04-tg-monitoring-vk-fanout-llm-quota-storm`, `INC-2026-05-19-vk-posts-personal-author`, `INC-2026-05-05-80-stories-video-promo-gap`, `INC-2026-05-05-80-stories-source-coverage`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The operator expected the built-in `80 историй о главном` promo campaign to keep
VK visibility alive with two daily festival event posts in `vk.com/klgdevents`
and one repost into `vk.com/kenigeventsofficial`. On 2026-06-04, only one visible
festival post was observed and it was believed to be near-manual rather than a
normal promo delivery. The same request also expanded the required promo surface:
future festival/event promo campaigns must be able to publish VK community
stories from recent source-community event posts.

## User / Business Impact

- Festival visibility in VK was below the promised promo contract on
  2026-06-04.
- The main community lacked the expected automated repost evidence.
- There was no campaign-level `vk_story` activity, so the requested daily story
  surface could not be scheduled or audited through `/promo`.
- Because this is a daily/scheduled production surface, a missed same-day slot
  requires incident handling and explicit compensation decisions.

## Detection

- Detected by operator report on 2026-06-04: no new festival posts were visible
  except one post that was nearly manually pushed.
- Runtime `/healthz` at investigation start returned `ok=true`, `ready=true`,
  `db=ok`, scheduler/tasks healthy.
- Direct Fly SSH/file-log inspection was attempted with local `flyctl`, but the
  local environment had no Fly auth token (`no access token available`). This
  blocked direct `/data/runtime_logs` and live `/data/db.sqlite` inspection from
  this workstation.
- Local fallback snapshots available in the workspace were either stale
  pre-promo snapshots or empty/legacy DB files without current `promo_*`
  tables, so they were not sufficient live evidence for the 2026-06-04 slot.

## Timeline

- 2026-06-04, day: operator observed that the expected promo VK cadence had not
  produced the visible `80 историй` posts/reposts.
- 2026-06-04, investigation start: repo docs and active regression contracts
  were opened; current dirty checkout was isolated by switching work to a clean
  branch from `origin/main`.
- 2026-06-04, investigation: `/healthz` was healthy; Fly SSH was blocked by
  missing local auth; local DB snapshots were checked and found insufficient for
  live promo evidence.
- 2026-06-04, code fix: `vk_story` promo activity support was added and the
  built-in `80 историй о главном` campaign was updated to seed two story
  activities: `klgdevents -> klgdevents` and
  `klgdevents -> kenigeventsofficial`, each two stories per day.
- 2026-06-04 14:45 UTC: live `promo_exposure` showed the deployed promo runner
  had already created the intended main-community repost
  `https://vk.com/wall-231828790_984` from existing festival event post
  `https://vk.com/wall-231920894_1974`.
- 2026-06-04 21:44 UTC: a manual compensation repost
  `https://vk.com/wall-231828790_989` was briefly created before the live DB
  evidence was available. After discovering it duplicated `wall-231828790_984`,
  the manual duplicate was deleted; VK API reports `is_deleted=true` for `989`.
- 2026-06-10: recurrence observed for the `80 историй о главном` promo VK
  campaign. The scheduler ticks ran without hard errors, but no same-day local
  `promo_exposure` rows were created for `vk_publication`, `vk_repost`, or
  `vk_story` by the expected 15:00 Europe/Kaliningrad repost/story slot.
  Investigation found previous-evening exposure rows still inside the rolling
  24-hour window, so the runner considered today's local due slot already
  fulfilled.

## Root Cause

1. Confirmed product gap: `promo_activity` had `vk_publication` and `vk_repost`,
   but no `vk_story` surface, so the requested story contract could not run.
2. Confirmed delivery gap before the repost hotfix reached production:
   `vk_repost` attempted to use group authorization and failed with
   `Group authorization failed: method is unavailable with group auth`.
3. Confirmed publication cadence detail: after one public organic/promo source
   post, the second `vk_publication` attempt at 16:15 UTC created a VK wall post
   but failed to record `promo_exposure` due to a transient SQLite lock; the
   next tick at 16:45 UTC recorded `wall-231920894_2057`, scheduled by VK for
   2026-06-05 06:50 UTC because the shared postponed queue had later slots.

## Contributing Factors

- `promo_vk` is an interval runner without a dedicated `ops_run` row per tick,
  so scheduler evidence depends on runtime logs and `promo_exposure`.
- Before the 2026-06-10 follow-up, the same `window_hours` value was used both
  for source discovery/dedup and for daily fulfilment counts. That made
  yesterday evening's valid exposure suppress today's local-day slot.
- The current local workspace initially contained dirty parallel work and
  `flyctl` did not auto-read the saved token; passing the saved token through
  `FLY_API_TOKEN` was required before live logs/DB evidence could be collected.
- `vk_publication` posts use postponed VK wall publishing; without live URL
  reconciliation and recent wall checks, operator-visible timing can lag behind
  the scheduler action.

## Automation Contract

### Treat as regression guard when

- changing `promo.py::run_promo_vk_activities`;
- changing `ensure_initial_80_stories_campaign`;
- changing VK story/wall/repost actor selection or upload parameters;
- changing `/promo` report/activity rendering;
- changing scheduler registration for `promo_vk`;
- changing `80 историй о главном` source ingestion or VK fanout surfaces.

### Affected surfaces

- `promo.py` VK activity runner and story upload helpers;
- `promo_activity.config_json` for `vk_publication`, `vk_repost`, `vk_story`;
- `promo_exposure` reporting;
- `handlers/promo_cmd.py` and `handlers/partner_promo_cmd.py` campaign reports;
- VK API `wall.post`, `wall.repost`, `stories.getPhotoUploadServer`,
  `stories.save`;
- scheduler job `promo_vk`;
- production env: `ENABLE_PROMO_VK_SCHEDULER`, `PROMO_VK_INTERVAL_MINUTES`,
  `VK_USER_TOKEN` / `VK_ACCESS_TOKEN4`, `VK_EVENTS_GROUP_ID`, VK group tokens.

### Mandatory checks before closure or deploy

- Unit test proves the initial `80 историй` campaign seeds `vk_publication`,
  `vk_repost`, and two `vk_story` activities.
- Unit test proves `run_promo_vk_activities` can publish story exposures for
  both target communities from recent `klgdevents` source posts.
- Existing VK wall author regression (`INC-2026-05-19-vk-posts-personal-author`)
  still passes for `post_to_vk`.
- Production config check confirms `ENABLE_PROMO_VK_SCHEDULER` is enabled and a
  user VK token is present for story upload.
- Live evidence shows tomorrow's expected state:
  `klgdevents`: two festival event posts and two festival stories;
  `kenigeventsofficial`: one repost from `klgdevents` and two festival stories.
- Same-day compensation decision is recorded: one allowed VK post now; no extra
  night compensation posts into `klgdevents` beyond operator instruction.
- Release-governance check: deployed SHA must be reachable from `origin/main`.

### Required evidence

- Test command output.
- Deployed SHA and deploy path.
- `/healthz` after deploy.
- VK API/UI evidence for created story ids and wall/repost URLs.
- Production `promo_exposure` rows for `vk_publication`, `vk_repost`, and
  `vk_story`.
- Runtime log or fallback evidence explaining the 2026-06-04 missed cadence.

## Immediate Mitigation

- Implemented `vk_story` as a first-class promo activity and seeded it into the
  built-in `80 историй о главном` campaign.
- Kept compensation scope aligned with the operator request: no extra overnight
  `klgdevents` event-post compensation was launched.
- Verified that the automated repost compensation already existed:
  `https://vk.com/wall-231828790_984` from source
  `https://vk.com/wall-231920894_1974`; removed the brief manual duplicate
  `https://vk.com/wall-231828790_989`.

## Corrective Actions

- Added VK story delivery through `stories.getPhotoUploadServer` +
  `stories.save` with user actor.
- Added vertical event story image rendering from stored event poster, title,
  date/time and venue.
- Added normalized story evidence in `promo_exposure`.
- Added retries for transient SQLite locks while recording VK promo exposure, so
  a successful external wall/story side effect is not immediately repeated only
  because audit persistence was briefly locked.
- Follow-up fix: daily `vk_publication`, `vk_repost`, and `vk_story` fulfilment
  now counts already recorded promo exposures within the activity's current
  local calendar day, while source discovery and source dedup keep the rolling
  window behavior.
- Updated `/promo` and `/promo report` to show story activity/report rows.
- Added focused tests in `tests/test_promo.py`.

## Follow-up Actions

- [ ] Add durable per-tick `ops_run` or equivalent diagnostics for `promo_vk`.
- [ ] Add a small admin command/runbook for controlled `promo_vk` catch-up and
  target-specific dry-run reporting.

## Release And Closure Evidence

- deployed SHA:
  - `98e5e34ea8a513044d9f4cfcfac9a12ff34f0602`
- deploy path:
  - pushed `5b55316d` to `origin/main` for the feature delivery, then
    `98e5e34e` to `origin/main` for VK exposure SQLite-lock retry;
  - GitHub Actions deploy workflow was triggered manually while diagnosing the
    release path, but failed because repository secret `FLY_API_TOKEN` was empty;
  - manual Fly deploy succeeded from clean worktree using the saved Fly token
    passed as process-local `FLY_API_TOKEN`;
  - Fly image `registry.fly.io/events-bot-new-wngqia:deployment-01KTAACX8HNH15444T62PHK529`,
    machine `48e42d5b714228`, version `1191`.
- regression checks:
  - `python3 -m py_compile promo.py handlers/promo_cmd.py handlers/partner_promo_cmd.py tests/test_promo.py`
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q tests/test_promo.py tests/test_vk_actor.py` -> `26 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.
  - Fly status: app image `deployment-01KTAACX8HNH15444T62PHK529`, machine
    `48e42d5b714228`, version `1191`, `1 total, 1 passing`.
  - Runtime file mirror verified: `ENABLE_RUNTIME_FILE_LOGGING=1`,
    `RUNTIME_LOG_DIR=/data/runtime_logs`, active `events-bot.log` plus rotated
    2026-06-04 files.
  - Production campaign `#1` now has `vk_publication` activity `#8`
    (`klgdevents`, `max_per_publish=2`, `daily_cap=2`), `vk_repost` activity
    `#9` (`klgdevents->kenigeventsofficial`, `max_per_publish=1`,
    `daily_cap=1`), and `vk_story` activities `#11` (`klgdevents:story`) and
    `#12` (`klgdevents->kenigeventsofficial:story`), both enabled with
    `source_group=klgdevents`, `max_per_publish=2`, `daily_cap=2`.
- compensation evidence:
  - automated promo repost: `https://vk.com/wall-231828790_984`
  - source: `https://vk.com/wall-231920894_1974`
  - prod `promo_exposure.id=44`, `activity_id=9`, `surface='vk_repost'`,
    `publish_status='PUBLISHED_MAIN'`, `event_id=5656`
  - manual duplicate `https://vk.com/wall-231828790_989` was deleted;
    VK API reports `is_deleted=true`.

## Prevention

- Keep `80 историй о главном` promo surfaces as idempotent seeded activities so
  production campaigns gain missing activity rows automatically after deploy.
- Treat VK story upload as a two-step API operation; upload-only success is not
  enough without `stories.save` evidence.
