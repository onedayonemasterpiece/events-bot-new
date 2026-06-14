# INC-2026-06-14 CrumpleVideo VK transport drift

Status: closed
Severity: sev2
Service: CrumpleVideo scheduled `/v tomorrow` social fanout
Opened: 2026-06-14
Closed: 2026-06-14
Owners: events-bot maintainer
Related incidents: `INC-2026-06-09-social-video-tg-publishing`, `INC-2026-06-05-vk-story-forward-wall-first`, `INC-2026-06-13-kaggle-duplicate-videoannounce`, `INC-2026-04-26-crumple-story-required-channel-fanout`, `INC-2026-04-24-crumple-story-channel-boosts-required`
Related docs: `docs/features/crumple-video/README.md`, `docs/features/cherryflash/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/release-governance.md`

## Summary

On 2026-06-13 and again on 2026-06-14 scheduled CrumpleVideo `/v tomorrow`
renders completed, but VK fanout to `vk:kenigeventsofficial:wall` failed. The
Kaggle notebook treated the VK community screen name `kenigeventsofficial` as a
Telegram username and failed with `ValueError: No user has "kenigeventsofficial"
as username`.

This is a production incident because the daily scheduled video announcement did
not publish to the required VK wall surface, and the same regression repeated
after earlier fixes even though CherryFlash already had the correct VK transport
path.

## User / Business Impact

- `vk.com/kenigeventsofficial` did not receive the CrumpleVideo wall video post
  for the scheduled `/v tomorrow` slot.
- Operators saw repeated failure notifications for sessions `#668`, `#669`, and
  `#675` despite the server-side target config carrying `transport="vk_wall"`.
- The recurrence lowered trust in the daily scheduled video workflow: the same
  class was fixed on the server side, but the Kaggle notebook execution path was
  still stale.

## Detection

- Detected by operator report and bot notifications on 2026-06-14.
- Production DB `videoannounce_session` rows showed the repeated failures.
- Runtime file mirror was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`,
  `/data/runtime_logs/events-bot.log`), but the root cause was confirmed by
  comparing the deployed repo helper with the embedded notebook helper.

## Timeline

- 2026-06-13 14:45 UTC: session `#668` scheduled `video_tomorrow` started.
- 2026-06-13 17:01 UTC: session `#668` failed after render with
  `vk:kenigeventsofficial:wall (ValueError: No user has "kenigeventsofficial" as username)`.
- 2026-06-13 17:01 UTC: session `#669` catch-up started.
- 2026-06-13 19:20 UTC: session `#669` failed with the same VK-as-Telegram error.
- 2026-06-14 14:45 UTC: session `#675` scheduled `video_tomorrow` started.
- 2026-06-14 16:51 UTC: session `#675` failed with the same VK error; Telegram
  channel story reposts also reported `BOOSTS_REQUIRED`.
- 2026-06-14 16:52 UTC: session `#676` catch-up started while the incident was
  being investigated.
- 2026-06-14 18:59 UTC: session `#676` finished with the same VK-as-Telegram
  error because its Kaggle dataset had been created before the deployed
  helper-bundling fix and did not contain `kaggle_common/story_publish.py`.
- 2026-06-14 19:40 UTC: штатный `CrumpleStoryPublishOnly` publish-only
  recovery ran for session `#676` from the already-rendered mp4 without
  rerendering and published VK wall post `https://vk.com/wall-231828790_1140`.
- 2026-06-14 19:45 UTC: duplicate-recovery guard was deployed so a recovered
  session is skipped after restart/deploy if its durable DB status is already
  published.

## Root Cause

1. `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` and server-side
   `video_announce/story_publish.py` correctly produced the CrumpleVideo VK wall
   target with `transport="vk_wall"`.
2. Standalone `kaggle/CrumpleVideo/story_publish.py` already implemented the VK
   transports used by CherryFlash.
3. `kaggle/CrumpleVideo/crumple_video.ipynb` embedded an older copy of
   `story_publish.py` that did not contain the `vk_wall` / `vk_wall_story`
   branch. The notebook wrote this stale helper into `/kaggle/working` and then
   imported it, so the target fell through to Telethon `get_input_entity()`.

## Contributing Factors

- CherryFlash uses the shared helper from `kaggle_common/story_publish.py` in the
  session dataset, while CrumpleVideo relied primarily on an embedded notebook
  copy.
- Regression tests existed to compare embedded and repo helper sources, but they
  were not part of the targeted release gate for the prior VK fanout hotfixes.
- The process fixed server target generation without forcing a Kaggle notebook
  helper parity check before deploy.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/CrumpleVideo/story_publish.py`;
- changing `kaggle/CrumpleVideo/crumple_video.ipynb` or
  `kaggle/CrumpleVideo/build_notebook.py`;
- changing `video_announce/scenario.py` dataset assembly for CrumpleVideo;
- changing `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` or CrumpleVideo VK wall/story
  fanout;
- changing scheduled `video_tomorrow` catch-up logic.

### Affected surfaces

- `kaggle/CrumpleVideo/story_publish.py` VK transports;
- `kaggle/CrumpleVideo/crumple_video.ipynb` embedded fallback helper;
- CrumpleVideo session dataset assembly in `video_announce/scenario.py`;
- Fly env `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`;
- production sessions `#668`, `#669`, `#675`, and catch-up session `#676`;
- `vk.com/kenigeventsofficial` wall publication evidence.
- publish-only compensation path for already-rendered failed sessions.

### Mandatory checks before closure or deploy

- `tests/test_crumple_build_notebook.py` must prove embedded helper parity and
  VK transport branch presence.
- `tests/test_kaggle_story_publish.py` must prove VK transports do not fall
  through to Telethon and `vk_wall_story` uses VK story upload with wall link.
- `tests/test_video_announce_story_publish.py` must prove default CrumpleVideo
  config carries `vk:kenigeventsofficial:wall` with `transport="vk_wall"` and
  required fanout semantics.
- `tests/test_video_announce_v_pipeline.py` must prove CrumpleVideo story-enabled
  datasets bundle current `kaggle_common/story_publish.py`.
- Production preflight must show runtime `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`
  carries `transport="vk_wall"` for `kenigeventsofficial` and the deployed
  app is healthy.
- `tests/test_video_announce_v_pipeline.py` must prove publish-only recovery
  creates a Kaggle dataset from an existing mp4, filters to failed VK targets,
  and bundles current `kaggle_common/story_publish.py`.
- Closure requires compensating rerun/catch-up for the current day unless it is
  externally blocked by Kaggle/VK/Telegram capability evidence.

### Required evidence

- deployed SHA reachable from `origin/main`;
- focused pytest output;
- Fly deploy/status/health evidence;
- production DB session evidence after deploy;
- VK API/public URL evidence for a `kenigeventsofficial` wall video post from
  the compensating rerun or a documented external blocker.

## Immediate Mitigation

- Work moved to a clean hotfix worktree from `origin/main`.
- Current production evidence was captured from Fly status, runtime env, and
  read-only SQLite rows before changing code.

## Corrective Actions

- Regenerate `crumple_video.ipynb` embedded `story_publish.py` from the current
  repo helper so the fallback contains `vk_wall` / `vk_wall_story` handling.
- Bundle current `kaggle_common/story_publish.py` into story-enabled CrumpleVideo
  datasets.
- Make the CrumpleVideo notebook prefer the bundled helper from the session
  dataset before the embedded fallback, matching the CherryFlash pattern.
- Add regression coverage for notebook helper parity, explicit VK transport
  branch presence, and CrumpleVideo dataset helper bundling.
- Add a штатный publish-only recovery path for completed renders: when the
  final mp4 exists but story/VK fanout failed, create a separate Kaggle dataset
  with the existing mp4, fresh `story_publish.json`, and current helper, then
  run `CrumpleStoryPublishOnly` to publish only failed VK targets without
  rerendering.
- Treat `video-afisha-session-*` and
  `crumple-story-publish-session-*` as ephemeral Kaggle dataset sources for
  Crumple kernels, so each push drops stale per-run datasets before attaching
  the current compensation dataset.
- Guard publish-only recovery with a per-session file lock so watchdog and
  manual compensation cannot concurrently push the shared Kaggle kernel with
  different dataset sources.
- Put encrypted VK auth directly into the temporary private publish-only
  dataset, avoiding stale shared story-secret dataset versions during urgent
  compensation.
- Re-check durable session status immediately before publish-only dataset
  creation so a recovered session is not published twice after a deploy/restart
  drops the temporary filesystem lock.
- Let VK-only story configs skip Telethon client creation in the Kaggle helper,
  so publish-only VK wall recovery does not compete for the shared Telegram
  auth bundle.

## Follow-up Actions

- [ ] Add a release checklist item or CI grouping so any VK/CrumpleVideo fanout
  change always runs `tests/test_crumple_build_notebook.py` together with
  story-publish tests.
- [ ] Add operator-facing preflight evidence for Kaggle notebook helper version
  or helper source (`bundled` vs `embedded`) in `story_publish_report.json`.

## Release And Closure Evidence

- deployed SHA: `29d76209232e3da35ecd16b55d1efda08c9a4e30`, pushed to
  `origin/main`.
- deploy path: clean hotfix worktree from `origin/main`, `flyctl deploy
  -a events-bot-new-wngqia --local-only`, Fly image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KV3TM3KZCTGEAB0KSZB45E83`,
  machine `683961db016e28` version `1417`, `1 total, 1 passing`.
- regression checks: `76 passed in 7.38s` for
  `tests/test_video_announce_v_pipeline.py`,
  `tests/test_kaggle_notebook_status_instrumentation.py`,
  `tests/test_kaggle_client.py`, `tests/test_crumple_build_notebook.py`,
  `tests/test_kaggle_story_publish.py`, and
  `tests/test_video_announce_story_publish.py`; `py_compile` for the touched
  publish-only/runtime modules passed earlier in the release sequence.
- post-deploy verification: `/healthz` returned HTTP `200`, `ok=true`,
  `ready=true`, `video_tomorrow=ok`, next scheduled `video_tomorrow` is
  `2026-06-15T14:45:00+00:00`.
- compensation evidence: publish-only Kaggle kernel
  `zigomaro/crumple-story-publish-only` completed with
  `story_publish_report.json` `ok=true`, target
  `vk:kenigeventsofficial:wall` `transport=vk_wall`, `post_id=1140`, URL
  `https://vk.com/wall-231828790_1140`, attachment
  `video-231828790_456239091_488bcee4858915295d`.
- production DB evidence: `videoannounce_session #676` is
  `status=PUBLISHED_TEST`, `error=null`, `video_url=crumple_video_final.mp4`.
- duplicate cleanup evidence: earlier non-canonical VK wall posts
  `https://vk.com/wall-231828790_1138` and
  `https://vk.com/wall-231828790_1139` are deleted (`is_deleted=true` in
  `wall.getById`), and orphan duplicate VK video objects
  `video-231828790_456239089` and `video-231828790_456239090` were deleted
  with `video.delete`. Canonical remaining wall/video pair is
  `https://vk.com/wall-231828790_1140` with `video-231828790_456239091`.

## Prevention

CrumpleVideo must not depend solely on embedded notebook copies for production
social fanout. The scheduled dataset now carries the current shared helper, and
regression tests pin both the notebook fallback and dataset helper path to the
same VK transport contract CherryFlash uses.
