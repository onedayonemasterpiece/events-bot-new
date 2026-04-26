# INC-2026-04-26 CrumpleVideo Story Required Channel Fanout

Status: open
Severity: sev1
Service: production bot / scheduled `video_tomorrow` CrumpleVideo story publish
Opened: 2026-04-26
Closed: —
Owners: video announce / production runtime / operations
Related incidents: `INC-2026-04-24-crumple-story-channel-boosts-required.md`, `INC-2026-04-10-crumple-story-prod-drift.md`
Related docs: `docs/features/crumple-video/README.md`, `docs/operations/cron.md`, `docs/operations/release-governance.md`

## Summary

The scheduled production CrumpleVideo run completed render and story publish from the Kaggle/runtime point of view, but `@kenigevents` did not receive the story. The current self-account-first mitigation from `INC-2026-04-24` kept the daily render alive, but it also let a required production channel miss finish as `Story publish status: OK`.

## User / Business Impact

- the daily CrumpleVideo story was visible only on part of the intended Telegram surface;
- `@kenigevents`, the primary production channel, missed the story for the current day;
- the operator had to inspect Kaggle logs manually because the notebook and Kaggle run both looked successful.

## Detection

- user reported that the previous day had no stories, and the April 26 run reached only `@lovekenig`;
- Kaggle log showed `Story preflight account: @The_day_of_kk premium=True`;
- Kaggle log showed `BOOSTS_REQUIRED` for `@kenigevents` during both preflight and publish;
- Kaggle log then showed successful publish to `me`, successful publish to `@lovekenig`, and final `Story publish status: OK`.

## Timeline

- 2026-04-24: `INC-2026-04-24` changed production story order to `me -> @kenigevents -> @lovekenig` so channel `BOOSTS_REQUIRED` would not block the expensive render.
- 2026-04-26: scheduled CrumpleVideo rendered successfully and published to `me`.
- 2026-04-26: `@kenigevents` returned `BOOSTS_REQUIRED` and did not receive the story.
- 2026-04-26: `@lovekenig` accepted the repost after the configured `600s` delay.
- 2026-04-26: the run ended green because only the first target was treated as blocking/required.

## Root Cause

1. Telegram still reports `BOOSTS_REQUIRED` for `@kenigevents`, so the current Telethon channel-story path cannot publish there until the channel capability/boost state changes.
2. The `INC-2026-04-24` mitigation intentionally made `me` the only blocking target to preserve render delivery.
3. The helper had no separate `required` fanout concept, so production channel delivery and render-gate success were coupled to the same first-target-only `ok` status.

## Contributing Factors

- `Story publish status: OK` was derived from `blocking_ok`, not from all production-required targets;
- `story_publish_report.json` captured the target failure, but the final status hid it;
- the compensating path for a same-day story miss still depends on Telegram channel capability, not only code.

## Automation Contract

### Treat as regression guard when

- changing `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, story target `required` / `blocking` flags, or story target modes in `fly.toml` / env;
- changing `kaggle/CrumpleVideo/story_publish.py`, `video_announce/story_publish.py`, or embedded `crumple_video.ipynb` story helper code;
- changing scheduled `video_tomorrow` catch-up / status handling.

### Affected surfaces

- `fly.toml`
- `.env.example`
- `video_announce/story_publish.py`
- `kaggle/CrumpleVideo/story_publish.py`
- `kaggle/CrumpleVideo/crumple_video.ipynb`
- `video_announce/poller.py` story report handling
- Telegram channel story capability / boosts

### Mandatory checks before closure or deploy

- verify production story config keeps `me` as the first blocking upload target;
- verify `@kenigevents` and `@lovekenig` are required channel fanout targets;
- verify required channel fanout failure does not block pre-render `Story preflight status: OK`, but does produce final `Story publish status: FAIL`;
- run targeted story config/helper tests and notebook embedding check;
- verify `/healthz` still treats story publish as required and does not silently downgrade to mp4-only;
- perform release-governance checks and confirm the fix is reachable from `origin/main`;
- after deploy, perform same-day compensating check/rerun if channel capability allows it, or record that Telegram `BOOSTS_REQUIRED` still blocks `@kenigevents`.

### Required evidence

- deployed SHA:
- tests / smoke covering required fanout:
- production env/config evidence for `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`:
- Kaggle or Telegram evidence for same-day story recovery, or explicit `BOOSTS_REQUIRED` blocker:
- confirmation that the delivered fix is reachable from `origin/main`:

## Immediate Mitigation

- keep `me` as the blocking render-gate so the daily video can still render;
- mark production channel repost targets as required for final story publish status;
- make required target failures visible as final publish failures instead of green partial success.

## Corrective Actions

- add `required=true` support to server-side story target config;
- add Kaggle-side `required_ok` reporting separate from `blocking_ok`;
- keep preflight render-gate based on blocking targets, but make publish-phase `ok` require all required targets;
- update production config/docs to mark `@kenigevents` and `@lovekenig` as required fanout targets.

## Follow-up Actions

- [ ] Restore or confirm Telegram story capability for `@kenigevents` so the same-day missed story can be compensated.
- [ ] Improve operator copy to say `required story target failed` instead of only listing generic story publish failure.

## Release And Closure Evidence

- deployed SHA: `401e9632e0f027f3f035d4bb75b907395951e0f4`
- deploy path: manual `/home/vscode/.fly/bin/flyctl deploy --app events-bot-new-wngqia --config fly.toml` from clean branch `hotfix/crumple-story-fanout-required-2026-04-26`; the same commit was pushed to `origin/main`.
- regression checks:
  - `pytest -q tests/test_kaggle_story_publish.py tests/test_video_announce_story_publish.py tests/test_crumple_build_notebook.py` -> `18 passed`
  - `python -m py_compile video_announce/story_publish.py kaggle/CrumpleVideo/story_publish.py`
  - `pytest -q tests/test_video_announce_v_pipeline.py::test_create_cherryflash_dataset_writes_story_publish_config_when_enabled` -> `1 passed`
  - `python kaggle/CrumpleVideo/build_notebook.py`
- post-deploy verification:
  - `flyctl status --app events-bot-new-wngqia` showed machine `48e42d5b714228` on version `1010`, image `deployment-01KQ5E1P3MG7D6P075W76XQXV8`, `1 total, 1 passing` check.
  - `curl https://events-bot-new-wngqia.fly.dev/healthz` returned `{"ok": true, "ready": true, ... "video_tomorrow": "ok", ... "issues": []}`.
  - startup log showed `SCHED startup catchup skip video_tomorrow: scheduled dispatch already recorded today`.
  - same-day `@kenigevents` compensation is not restored by code alone: the April 26 Kaggle evidence still has `BOOSTS_REQUIRED` for `@kenigevents`, so a rerun would be expected to miss the same required channel until Telegram channel story capability/boost state is restored.

## Prevention

- do not treat self-account story publish as sufficient production delivery evidence;
- keep render-gate (`blocking`) and channel-delivery contract (`required`) separate;
- when `BOOSTS_REQUIRED` is still present for a required channel, close only with explicit external-capability evidence or a documented product decision to remove that channel from required fanout.
