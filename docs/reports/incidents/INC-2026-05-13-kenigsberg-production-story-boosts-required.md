# INC-2026-05-13 Kenigsberg Production Story BOOSTS_REQUIRED

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories production story publishing
Opened: 2026-05-13
Closed: —
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

- [ ] Decide whether `@mostvkenig` needs Telegram boosts or another channel-native story mechanism; self-account story publishing remains the required fallback meanwhile.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep direct channel stories out of the Kenigsberg render gate unless a live preflight proves the channel can send stories without `BOOSTS_REQUIRED`.
- Keep shared video-announcement Business fanout out of Kenigsberg.
