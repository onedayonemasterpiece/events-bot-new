# INC-2026-05-13 Kenigsberg Production Story BOOSTS_REQUIRED

Status: closed
Severity: sev2
Service: Kenigsberg Stories production story publishing
Opened: 2026-05-13
Closed: 2026-05-13
Owners: Codex
Related incidents: `INC-2026-04-26-crumple-story-required-channel-fanout`, `INC-2026-05-12-kenigsberg-music-range-overrun-into-vocals`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/features/telegram-business-stories/README.md`

## Summary

The first Kenigsberg production catch-up after enabling the daily `20:10 Europe/Kaliningrad` schedule failed before render. Kaggle story preflight rejected the direct Telethon target `@mostvkenig` with Telegram `BOOSTS_REQUIRED`. The immediate hotfix then incorrectly let Kenigsberg inherit the shared video-announcement Business story allowlist, publishing issue `#44` to unrelated Business accounts. Kenigsberg is a separate history-channel product and must not use that fanout.

## User / Business Impact

- The same-day compensation publication for `2026-05-13` did not publish on the first attempt.
- The scheduled job itself was installed and reached Kaggle, but the story preflight was too strict for the direct channel target.
- The first hotfix compensation published to unrelated Business story accounts; those mistaken Business stories were deleted through Bot API `deleteStory`.
- Existing `/a` source bans and issue history were not lost.

## Detection

- Production DB row `videoannounce_session.id=296`, `issue=43`, ended `FAILED`.
- Kaggle output `/tmp/videoannounce-logs-296/koenigsberg-stories.log` showed:
  - `Story preflight failed for @mostvkenig: ... BOOSTS_REQUIRED`;
  - Business story preflight passed for the selected Business target hashes.
- Production DB row `videoannounce_session.id=297`, `issue=44`, ended `PUBLISHED_TEST`; `/tmp/videoannounce-297/story_publish_report.json` showed direct `@mostvkenig` failed with `BOOSTS_REQUIRED`, then two unrelated Business story targets succeeded.

## Timeline

- 2026-05-13 20:47 UTC: production recency windows reset while preserving 5 source bans.
- 2026-05-13 20:49 UTC: startup catch-up launched Kenigsberg `issue #43`, session `#296`.
- 2026-05-13 20:51 UTC: Kaggle preflight failed before rendering because `@mostvkenig` direct upload was required/blocking.
- 2026-05-13 20:51 UTC: Kaggle output and `story_publish_report.json` were downloaded by the poller.
- 2026-05-13 21:xx UTC: fix changes direct `@mostvkenig` fanout to best-effort and keeps Business targets as required production gates.
- 2026-05-13 21:21 UTC: compensation `issue #44`, session `#297`, published to the shared Business allowlist, which was the wrong product surface.
- 2026-05-13 21:xx UTC: mistaken Business stories from session `#297` were deleted with Bot API `deleteStory`.
- 2026-05-13 21:xx UTC: fix changes Kenigsberg targets to required self-account upload plus best-effort `@mostvkenig` repost, with `story_business_targets=[]`.
- 2026-05-13 22:03 UTC: after the channel received an additional boost, compensation session `#299`, issue `#46`, published successfully both to the required self-account story and to `@mostvkenig` via `repost_previous`.

## Root Cause

1. Kenigsberg production story config marked direct `@mostvkenig` Telethon upload as required.
2. Telegram channel stories can return `BOOSTS_REQUIRED` independently of the user account being an admin.
3. The first mitigation reused the shared Business target selection path, which belongs to CherryFlash/video announcements, not Kenigsberg.

## Contributing Factors

- The earlier readiness check verified Business cache and webhook state, but did not run a direct `CanSendStoryRequest` against `@mostvkenig`.
- The config treated the direct channel target as primary hard gate instead of a best-effort fanout after a required self-account story.
- Kenigsberg had no product-specific guard forbidding shared Business fanout.

## Automation Contract

### Treat as regression guard when

- changing Kenigsberg `story_targets_override`;
- changing shared story preflight semantics for `blocking` / `required`;
- changing Business story target selection or Kenigsberg production schedule.
- changing Kenigsberg music ranges or music recency resets after a production catch-up.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`;
- shared Kaggle story helper `kaggle/CrumpleVideo/story_publish.py`;
- Kenigsberg `story_business_targets=[]` override;
- Kaggle preflight/publish output handling.

### Mandatory checks before closure or deploy

- `pytest tests/test_kenigsberg_stories.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_notebook.py -q`;
- compile changed Python files;
- confirm production `build_story_publish_config` emits `peer=me` as blocking/required, `@mostvkenig` as non-blocking/non-required `repost_previous`, and no Business targets;
- deploy SHA reachable from `origin/main`;
- run a compensation Kenigsberg publication and verify terminal status plus story publish report.

### Required evidence

- deployed SHA;
- Fly deploy version;
- production `/healthz` with `kenigsberg_story_daily=ok`;
- production DB row for the compensation rerun;
- Kaggle/story publish logs proving self-account story success plus best-effort `@mostvkenig` result or a clear external blocker.

## Immediate Mitigation

- Mistaken Business stories from issue `#44` were deleted.
- Kenigsberg no longer inherits the shared runtime DB Business allowlist: `story_business_targets=[]`.
- The first `The Promise` whitelist range (`3:44-4:26`) was removed after repeated vocalized selections; music recency is preserved while text/source usage can be reset.

## Corrective Actions

- Update production story config and docs.
- Add/adjust tests for the non-blocking direct target contract.
- Redeploy and run another same-day compensation launch.

## Follow-up Actions

- [x] `@mostvkenig` received a boost and passed live story repost during compensation session `#299`.

## Release And Closure Evidence

- deployed SHA: `f6f387284665c4c69a5ee1dcf2401a725d1ea705`
- deploy path: manual `flyctl deploy --remote-only` from `origin/main`
- Fly release: machine version `1089`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRHM6SPGCGC10KPH4Z24Y4WX`
- regression checks:
  - `.venv/bin/python -m compileall -q handlers/kenigsberg_stories_cmd.py scripts/render_kenigsberg_story.py kenigsberg_stories/state.py scheduling.py`
  - `.venv/bin/pytest tests/test_kenigsberg_stories.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_notebook.py -q` -> `61 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, `kenigsberg_story_daily=ok`, next run `2026-05-14T18:10:00+00:00`.
  - Production story config emits only `me` blocking/required and `@mostvkenig` best-effort `repost_previous`; no Business targets are selected.
  - Mistaken Business stories from issue `#44` were deleted via Bot API `deleteStory` with `ok=true` for both affected short-hash targets.
  - Compensation session `#298`, issue `#45`, completed `PUBLISHED_TEST`; `story_publish_report.json` has `required_ok=true`, self-account story publish `ok=true`, and `@mostvkenig` best-effort repost still blocked by Telegram `BOOSTS_REQUIRED`.
  - Issue `#45` selected `05 - Save Me.flac`; `The Promise` did not repeat, and the previous `The Promise` window remains in effective `recent_music`.
  - Post-boost compensation session `#299`, issue `#46`, completed `PUBLISHED_TEST`; `story_publish_report.json` has `ok=true`, `required_ok=true`, `fanout_ok=true`, self-account `story_id=20`, and `@mostvkenig` `repost_previous` `story_id=6`.
  - Issue `#46` selected `02 - Wyatt Earth.flac`, `43.365-67.135s`, so the compensation did not repeat `The Promise`.

## Prevention

- Keep direct channel stories out of the Kenigsberg render gate unless a live preflight proves the channel can send stories without `BOOSTS_REQUIRED`.
- Keep shared video-announcement Business fanout out of Kenigsberg.
