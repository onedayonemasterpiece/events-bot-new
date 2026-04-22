# INC-2026-04-19-cherryflash-story-media-invalid CherryFlash Story Media Invalid

Status: open
Severity: sev2
Service: CherryFlash / Telegram Stories publish
Opened: 2026-04-19
Closed: —
Owners: video announce runtime
Related incidents: `INC-2026-04-10-crumple-story-prod-drift`, `INC-2026-04-16-cherryflash-kaggle-save-kernel-drift`
Related docs: `docs/backlog/features/cherryflash/README.md`, `docs/features/crumple-video/README.md`, `docs/operations/release-governance.md`

## Summary

CherryFlash scheduled run `#170` rendered the final mp4 and passed story preflight, but Telegram rejected the first story upload to `@kenigevents` with `MEDIA_FILE_INVALID`. Because the second target is `repost_previous`, `@lovekenig` also failed by dependency and the daily story publication was lost.

## User / Business Impact

- the daily CherryFlash video was not published to Telegram Stories;
- the operator had to inspect Kaggle logs manually even though the render itself completed;
- the failure happened after a long Kaggle render, so retry cost was high.

## Detection

- user reported no story publication after the scheduled run;
- Kaggle Version 58 log showed `Story publish status: FAIL`;
- `story_publish_report.json` recorded `BadRequestError: RPCError 400: MEDIA_FILE_INVALID (caused by SendStoryRequest)` for `@kenigevents`.

## Timeline

- 2026-04-17 07:44 UTC: CherryFlash session `#166` / Kaggle Version 56 published stories successfully with `v16-story-native-720p`.
- 2026-04-19 07:44 UTC: CherryFlash session `#170` / Kaggle Version 58 rendered successfully but failed at `SendStoryRequest`.
- 2026-04-19: comparison showed both runs used the same `v16-story-native-720p` notebook family and the same `720x1280 h264/aac` story path; the failed render had a heavier video stream profile, and the helper still used a generic `crf=20` story upload copy instead of a fixed story-delivery bitrate.

## Root Cause

1. The shared story helper normalized geometry but still encoded the story upload copy using a generic/high-quality `CRF 20` profile without a bitrate cap.
2. The successful Version 56 did not prove that this profile was stable for all future CherryFlash selections; Version 58 reached Telegram with a media file that passed local canvas checks but was rejected by `SendStoryRequest`.
3. The report did not include enough exact media diagnostics for the story-safe file, so the first investigation had to infer bitrate/profile from render logs and partial output downloads.

## Contributing Factors

- CherryFlash and CrumpleVideo share the story helper source, while the Crumple notebook embedded helper had drifted from the repo helper.
- The story helper only guarded a 30 MB file-size ceiling and did not log size/duration/bitrate for the uploaded file.

## Automation Contract

### Treat as regression guard when

- touching `kaggle/CrumpleVideo/story_publish.py`;
- changing CherryFlash or CrumpleVideo story video dimensions, codec, bitrate, report schema, or `SendStoryRequest` media construction;
- rebuilding `kaggle/CrumpleVideo/crumple_video.ipynb`;
- changing CherryFlash final encode profile in `scripts/render_cherryflash_full.py`.

### Affected surfaces

- `kaggle/CrumpleVideo/story_publish.py`
- `kaggle/CrumpleVideo/crumple_video.ipynb`
- `kaggle/CherryFlash/cherryflash.ipynb`
- `video_announce/scenario.py` CherryFlash bundle assembly
- Telegram Stories publish via Telethon `SendStoryRequest`

### Mandatory checks before closure or deploy

- `pytest -q tests/test_kaggle_story_publish.py tests/test_crumple_build_notebook.py tests/test_video_announce_story_publish.py tests/test_cherryflash_notebook.py`;
- confirm the CrumpleVideo embedded helper matches `kaggle/CrumpleVideo/story_publish.py`;
- confirm the CherryFlash bundle ships `kaggle_common/story_publish.py`;
- run a live CherryFlash story publish and verify `story_publish_report.json` has `ok=true` plus media diagnostics for the uploaded file;
- verify the deployed SHA is reachable from `origin/main`.

### Required evidence

- deployed SHA;
- test output;
- Kaggle output/report showing `Story publish status: OK`;
- evidence that `@kenigevents` received the upload and `@lovekenig` received the repost.

## Immediate Mitigation

- switched the story upload copy to a fixed story-native delivery budget (`720x1280`, `H.264/AAC`, `b:v=900k`) with an explicit `1200k` maxrate / `2400k` buffer and no B-frames;
- added media diagnostics to the report and log line for the exact file passed to Telegram.

## Corrective Actions

- keep the shared helper’s story encode profile aligned with CherryFlash’s story-first delivery profile;
- CherryFlash now owns a stricter one-pass upload contract than the generic helper:
  - the final render must already be a Telegram-native `720x1280` `H.265/AAC` story artifact;
  - the helper must validate and report the uploaded CherryFlash file instead of applying a second lossy default transcode;
- keep `crumple_video.ipynb` embedded helper synchronized via `kaggle/CrumpleVideo/build_notebook.py`;
- make future `MEDIA_FILE_INVALID` reports self-contained enough to debug without downloading gigabytes of Kaggle output.

## Follow-up Actions

- [ ] Close this incident only after a live CherryFlash run publishes both story targets successfully.
- [ ] If Telegram still rejects the capped H.264 file, capture the report media diagnostics and compare against the last successful run before changing renderer math.

## Release And Closure Evidence

- deployed SHA: `78e8834a24797a3d1a2d641feb75af5ed921ed6a`
- deploy path: manual `~/.fly/bin/flyctl deploy --app events-bot-new-wngqia --config fly.toml` from clean worktree `hotfix/cherryflash-one-pass-story-2026-04-20`, then fast-forward push to `origin/main`
- regression checks:
  - `BLENDER_BIN=/usr/bin/true pytest -q tests/test_kaggle_story_publish.py tests/test_crumple_build_notebook.py tests/test_video_announce_story_publish.py tests/test_cherryflash_notebook.py tests/test_cherryflash_full_render.py` → `25 passed`
  - `python -m py_compile scripts/render_cherryflash_full.py kaggle/CrumpleVideo/story_publish.py video_announce/story_publish.py video_announce/scenario.py`
  - `python kaggle/CrumpleVideo/build_notebook.py`
- post-deploy verification:
  - `flyctl status` shows machine `48e42d5b714228` on version `971` with image `deployment-01KPN9WFKB3BW213QNCMRXBN92`
  - `curl https://events-bot-new-wngqia.fly.dev/healthz` returned `{"ok": true, "ready": true, ...}`
  - prod live CherryFlash run `#174` started on deployed code, generated `story_publish.json` with `upload_profile=telegram_story_native_hevc_720p_v1`, and reached Kaggle kernel `zigomaro/cherryflash` version `61`
  - the live run still failed before media upload because preflight for repost target `@lovekenig` returned `BOOSTS_REQUIRED`; `story_publish_report.json` therefore does not yet prove end-to-end one-pass media delivery, and this incident stays open

## Prevention

- story helper tests must assert the upload transcode profile, not only dimensions;
- CherryFlash tests must assert the final render profile itself (`libx265`, `hvc1`, `30fps`, `AAC 48kHz`) so quality regressions are caught before helper-level publish smoke;
- release checks for story surfaces must include both preflight and publish evidence;
- branch reconciliation must not leave embedded notebook helpers stale relative to repo helper files.
