# INC-2026-04-24 CrumpleVideo Story Channel Boosts Required

Status: open
Severity: sev1
Service: production bot / scheduled `video_tomorrow` CrumpleVideo story publish
Opened: 2026-04-24
Closed: —
Owners: video announce / production runtime / operations
Related incidents: `INC-2026-04-10-crumple-story-prod-drift.md`, `INC-2026-04-20-video-tomorrow-stuck-rendering.md`, `INC-2026-04-26-crumple-story-required-channel-fanout.md`
Related docs: `docs/features/crumple-video/README.md`, `docs/operations/cron.md`, `docs/operations/release-governance.md`

## Summary

The scheduled production CrumpleVideo run for April 24, 2026 started session `#188` for the April 25, 2026 event set, pushed Kaggle dataset `zigomaro/video-afisha-session-188`, and Kaggle completed successfully from the platform point of view. Inside the notebook, the pipeline stopped before video render because story preflight failed with Telegram `BOOSTS_REQUIRED` for the configured channel targets, so the bot later reported `missing video output`.

## User / Business Impact

- the daily video announcement slot for Saturday, April 25, 2026 did not produce a video/story during the normal scheduled window;
- operators saw `Kaggle: COMPLETE` but no mp4 in output, which looked like a renderer/output defect instead of a story-target capability failure;
- this was reported as the second consecutive day of CrumpleVideo not delivering, increasing the chance of losing the daily audience window without manual catch-up.

## Detection

- operator Telegram messages for session `#188` reported `VideoAnnounceSessionStatus.FAILED`, `Kaggle: COMPLETE`, `Ошибка: missing video output`;
- Kaggle logs showed `Story preflight account: @The_day_of_kk premium=True`;
- Kaggle logs then showed `BOOSTS_REQUIRED` for `@kenigevents` and `@lovekenig`, followed by `FATAL: story publish preflight failed before video render`;
- screenshot evidence showed the Kaggle kernel and session dataset both completed successfully, confirming the failure was inside notebook business logic.

## Timeline

- 2026-04-24 16:45 Europe/Kaliningrad: session `#188` started for target date `2026-04-25`, selected 12 events, and prepared the Kaggle payload.
- 2026-04-24 16:45 Europe/Kaliningrad: Kaggle kernel `zigomaro/crumple-video` ran against dataset `zigomaro/video-afisha-session-188`.
- 2026-04-24 16:45-16:48 Europe/Kaliningrad: notebook completed poster preflight (`sources 18/18 ready`, `scenes 12/12 ready`) and appended the final scene.
- 2026-04-24 16:48 Europe/Kaliningrad: story preflight failed for both configured channel targets with `BOOSTS_REQUIRED`; notebook returned failure before render and produced no video output.
- 2026-04-24 16:48 Europe/Kaliningrad: bot marked session `#188` failed with `missing video output` and sent repeated missing-video log notifications.

## Root Cause

1. Production `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` used channel targets as the ordered story fanout, with `@kenigevents` as the first blocking target.
2. Telegram currently requires additional channel boosts for those targets, so `CanSendStoryRequest` returned `BOOSTS_REQUIRED`.
3. The notebook intentionally fails before expensive render when the first blocking story target cannot accept stories; with the channel as blocking target, this prevented even the mp4 from being generated.

## Contributing Factors

- the logged account `@The_day_of_kk` was Premium and capable as a user account, but production config did not use self-account story publish as the blocking target;
- `missing video output` in the bot status is technically correct but hides the upstream story preflight root cause unless operators open Kaggle logs;
- the current day slot needs compensating rerun after the config fix because the scheduled window already fired.

## Automation Contract

### Treat as regression guard when

- changing `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, story fanout order, or story target modes in `fly.toml` / env;
- changing `/v tomorrow`, `video_announce/`, `kaggle/CrumpleVideo/`, `story_publish.py`, or the notebook preflight/publish behavior;
- changing release or catch-up paths for daily scheduled CrumpleVideo.

### Affected surfaces

- `fly.toml`
- `.env.example`
- `video_announce/story_publish.py` config generation
- `kaggle/CrumpleVideo/story_publish.py` preflight/publish helper
- `kaggle/CrumpleVideo/crumple_video.ipynb`
- scheduled `video_tomorrow` catch-up and operator evidence
- Telegram channel story capability / boosts

### Mandatory checks before closure or deploy

- verify production story config keeps a Premium self-account target (`me`) as the first blocking upload target;
- verify channel targets are downstream `repost_previous` fanout targets and remain non-blocking for render preflight when Telegram returns `BOOSTS_REQUIRED`;
- verify production-required channel target misses do not finish as green story publish;
- run targeted unit tests for story config and Kaggle story helper partial fanout behavior;
- verify `/healthz` still treats story publish as required and does not silently downgrade to mp4-only;
- perform release-governance checks and confirm the fix is reachable from `origin/main`;
- after deploy, perform a compensating same-day `/v tomorrow` rerun/catch-up for the April 25, 2026 slot and verify a video/story output exists.

### Required evidence

- deployed SHA:
- tests / smoke covering story target order and partial fanout:
- production env/config evidence for `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`:
- Kaggle or Telegram evidence that the compensating rerun produced video/story output:
- confirmation that the delivered fix is reachable from `origin/main`:

## Immediate Mitigation

- identified the failure as story preflight `BOOSTS_REQUIRED`, not poster availability or Blender render failure;
- changed production default story target order so `me` is the blocking upload target and channel reposts are downstream fanout; on 2026-04-26 this was tightened so channel reposts remain render-non-blocking but are required for final publish status.

## Corrective Actions

- update `fly.toml` production story targets to `me -> @kenigevents -> @lovekenig`, with channel targets using `repost_previous`;
- update canonical CrumpleVideo/cron docs and env examples to document the self-account blocking target requirement;
- add a targeted config regression test for preserving the self-account first target.

## Follow-up Actions

- [x] Decide whether `@kenigevents` / `@lovekenig` should keep channel stories via boosts, stay best-effort repost targets, or be removed from the CrumpleVideo fanout. Decision on 2026-04-26: keep them in fanout and mark them required for final publish status; see `INC-2026-04-26-crumple-story-required-channel-fanout.md`.
- [ ] Improve operator status/error copy so `missing video output` caused by pre-render story preflight includes the blocking target error summary.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- keep `me` as the blocking target for scheduled CrumpleVideo while channel story capability depends on Telegram boosts;
- treat downstream channel story failures as visible render-non-blocking warnings during preflight, but as final publish failures when the target is production-required;
- use this incident as the regression contract for future story target ordering changes.
