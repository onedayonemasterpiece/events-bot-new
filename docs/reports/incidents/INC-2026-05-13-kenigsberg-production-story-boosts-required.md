# INC-2026-05-13 Kenigsberg Production Story BOOSTS_REQUIRED

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories production story publishing
Opened: 2026-05-13
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-26-crumple-story-required-channel-fanout`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/features/telegram-business-stories/README.md`

## Summary

The first Kenigsberg production catch-up after enabling the daily `20:10 Europe/Kaliningrad` schedule failed before render. Kaggle story preflight rejected the direct Telethon target `@mostvkenig` with Telegram `BOOSTS_REQUIRED`, even though the encrypted Business story targets passed preflight.

## User / Business Impact

- The same-day compensation publication for `2026-05-13` did not publish on the first attempt.
- The scheduled job itself was installed and reached Kaggle, but the story preflight was too strict for the direct channel target.
- Existing `/a` source bans and issue history were not lost.

## Detection

- Production DB row `videoannounce_session.id=296`, `issue=43`, ended `FAILED`.
- Kaggle output `/tmp/videoannounce-logs-296/koenigsberg-stories.log` showed:
  - `Story preflight failed for @mostvkenig: ... BOOSTS_REQUIRED`;
  - Business story preflight passed for the selected Business target hashes.

## Timeline

- 2026-05-13 20:47 UTC: production recency windows reset while preserving 5 source bans.
- 2026-05-13 20:49 UTC: startup catch-up launched Kenigsberg `issue #43`, session `#296`.
- 2026-05-13 20:51 UTC: Kaggle preflight failed before rendering because `@mostvkenig` direct upload was required/blocking.
- 2026-05-13 20:51 UTC: Kaggle output and `story_publish_report.json` were downloaded by the poller.
- 2026-05-13 21:xx UTC: fix changes direct `@mostvkenig` fanout to best-effort and keeps Business targets as required production gates.

## Root Cause

1. Kenigsberg production story config marked direct `@mostvkenig` Telethon upload as required.
2. Telegram channel stories can return `BOOSTS_REQUIRED` independently of the user account being an admin.
3. The existing Business story path was healthy, but the preflight contract required all required targets to pass before render.

## Contributing Factors

- The earlier readiness check verified Business cache and webhook state, but did not run a direct `CanSendStoryRequest` against `@mostvkenig`.
- The config treated the direct channel target as primary hard gate instead of a best-effort fanout next to the required Business targets.

## Automation Contract

### Treat as regression guard when

- changing Kenigsberg `story_targets_override`;
- changing shared story preflight semantics for `blocking` / `required`;
- changing Business story target selection or Kenigsberg production schedule.

### Affected surfaces

- `handlers/kenigsberg_stories_cmd.py`;
- shared Kaggle story helper `kaggle/CrumpleVideo/story_publish.py`;
- production DB setting `video_announce_story_business_targets`;
- Kaggle preflight/publish output handling.

### Mandatory checks before closure or deploy

- `pytest tests/test_kenigsberg_stories.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_notebook.py -q`;
- compile changed Python files;
- confirm production `build_story_publish_config` emits direct `@mostvkenig` as non-blocking/non-required and Business targets as required;
- deploy SHA reachable from `origin/main`;
- run a compensation Kenigsberg publication and verify terminal status plus story publish report.

### Required evidence

- deployed SHA;
- Fly deploy version;
- production `/healthz` with `kenigsberg_story_daily=ok`;
- production DB row for the compensation rerun;
- Kaggle/story publish logs proving Business target publish success or a clear external blocker.

## Immediate Mitigation

- Direct `@mostvkenig` remains in fanout but is marked `blocking=false`, `required=false`.
- Kenigsberg no longer has a Kenigsberg-specific ENV override for Business target selection; it uses the shared runtime DB allowlist.

## Corrective Actions

- Update production story config and docs.
- Add/adjust tests for the non-blocking direct target contract.
- Redeploy and run another same-day compensation launch.

## Follow-up Actions

- [ ] Decide whether `@mostvkenig` needs Telegram boosts or another channel-native story mechanism; Business story publishing remains the production path meanwhile.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep direct channel stories out of the Kenigsberg render gate unless a live preflight proves the channel can send stories without `BOOSTS_REQUIRED`.
