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
- 2026-06-04, compensation: one operator-approved immediate repost was created
  from existing festival event post `https://vk.com/wall-231920894_1974` into
  `https://vk.com/kenigeventsofficial` as
  `https://vk.com/wall-231828790_989`. VK API verification:
  `owner_id=-231828790`, `from_id=-231828790`,
  `copy_history=[wall-231920894_1974]`, `likes.can_publish=1`.

## Root Cause

1. Confirmed product gap: `promo_activity` had `vk_publication` and `vk_repost`,
   but no `vk_story` surface, so the requested story contract could not run.
2. Confirmed observability gap from this workstation: Fly SSH/file mirror and
   live production DB evidence were unavailable because local Fly auth was not
   configured.
3. Pending live evidence: the exact reason the 2026-06-04 `vk_publication`
   cadence produced only one visible festival post still needs live
   `promo_exposure`, scheduler logs, and VK wall evidence. Related June 4
   incidents already show upstream Telegram import/VK fanout/promo repost
   issues that could have reduced the eligible source-post pool.

## Contributing Factors

- `promo_vk` is an interval runner without a dedicated `ops_run` row per tick,
  so scheduler evidence depends on runtime logs and `promo_exposure`.
- The current local workspace contained dirty parallel work and no Fly auth,
  increasing the cost of live incident evidence collection.
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
- Kept compensation scope aligned with the operator request: prepare one
  immediate VK post/repost compensation only; do not launch extra overnight
  `klgdevents` event-post compensation.
- Published exactly one immediate compensation repost:
  `https://vk.com/wall-231828790_989` from source
  `https://vk.com/wall-231920894_1974`.

## Corrective Actions

- Added VK story delivery through `stories.getPhotoUploadServer` +
  `stories.save` with user actor.
- Added vertical event story image rendering from stored event poster, title,
  date/time and venue.
- Added normalized story evidence in `promo_exposure`.
- Updated `/promo` and `/promo report` to show story activity/report rows.
- Added focused tests in `tests/test_promo.py`.

## Follow-up Actions

- [ ] Add durable per-tick `ops_run` or equivalent diagnostics for `promo_vk`.
- [ ] Add a small admin command/runbook for controlled `promo_vk` catch-up and
  target-specific dry-run reporting.
- [ ] Complete live production evidence once Fly auth or another production DB
  path is available from the workstation.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
  - `python3 -m py_compile promo.py handlers/promo_cmd.py handlers/partner_promo_cmd.py tests/test_promo.py`
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m pytest -q tests/test_promo.py` -> `16 passed`
- post-deploy verification:
- compensation evidence:
  - `https://vk.com/wall-231828790_989`
  - source: `https://vk.com/wall-231920894_1974`
  - VK API: `owner_id=-231828790`, `from_id=-231828790`,
    `copy_history=[{"owner_id": -231920894, "id": 1974}]`,
    `likes.can_publish=1`

## Prevention

- Keep `80 историй о главном` promo surfaces as idempotent seeded activities so
  production campaigns gain missing activity rows automatically after deploy.
- Treat VK story upload as a two-step API operation; upload-only success is not
  enough without `stories.save` evidence.
