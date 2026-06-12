# INC-2026-06-12-kenigsberg-story-media-invalid-catchup-loop

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories / Kaggle story publish / scheduler catch-up
Opened: 2026-06-12
Closed: —
Owners: Codex / operator
Related incidents: `INC-2026-06-12-kenigsberg-story-session-duplication`, `INC-2026-05-13-kenigsberg-production-story-boosts-required`, `INC-2026-04-19-cherryflash-story-media-invalid`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/cron.md`, `docs/operations/runtime-logs.md`

## Summary

Kenigsberg scheduled startup catch-up session `#661` failed after Kaggle rendered the MP4 and published VK targets, because Telegram rejected the required story upload with `MEDIA_FILE_INVALID`. The same deploy/restart window also created a burst of same-day failed Kenigsberg startup catch-up sessions (`#655`-`#661`) because the catch-up path retried after failed sessions without a cap.

## User / Business Impact

- The required Telegram story for `@mostvkenig` was not published.
- VK best-effort fanout did publish during `#661` (`vk_story` and `vk_wall`), creating a partial cross-platform state.
- Repeated startup catch-up attempts consumed Kaggle/API capacity during an already constrained quota window.

## Detection

- Operator reported bot messages at `2026-06-12 23:13 Europe/Kaliningrad`: session `#661` finished with Kaggle `error` and sent Kaggle error logs.
- Production DB showed `videoannounce_session.id=661`, `profile_key=kenigsberg_story`, `status=FAILED`, `trigger=startup_catchup`.
- Kaggle kernel output for `zigomaro/koenigsberg-stories` showed Telegram `MEDIA_FILE_INVALID` and media diagnostics with HEVC/`hvc1`.

## Timeline

- 2026-06-12 20:15-20:41 UTC: Kenigsberg startup catch-up sessions `#655`-`#660` failed quickly during restart/deploy churn.
- 2026-06-12 20:43 UTC: startup catch-up session `#661` was handed off to Kaggle.
- 2026-06-12 20:43 UTC: story preflight passed for `@mostvkenig` and `@loving_guide39`; `@jane_tour39` was best-effort and failed preflight with `PeerIdInvalidError`.
- 2026-06-12 20:47 UTC approx: Telegram publish of required primary target failed with `MEDIA_FILE_INVALID`; VK story and wall later succeeded.
- 2026-06-12 21:13 UTC: Kaggle status became `ERROR`; bot reported session `#661` failure.
- 2026-06-12: investigation downloaded Kaggle output from production and identified the HEVC-native Kenigsberg upload profile as the publish blocker.

## Root Cause

1. Kenigsberg production story config forced `story_upload_profile=telegram_story_native_hevc_720p_v1`.
2. That profile bypassed the shared helper's H.264 story-safe transcode path and uploaded `kenigsberg_story_final.mp4` directly as HEVC/`hvc1`.
3. Telegram rejected the rendered file during `SendStoryRequest` with `MEDIA_FILE_INVALID`.
4. Kenigsberg startup catch-up checked only for a confirmed handoff, not for repeated same-day failed scheduled/story sessions, so deploy restarts could relaunch the missed slot repeatedly.

## Contributing Factors

- GPU quota exhaustion made renders slower and raised the cost of each failed retry.
- `ops_run(kind=kenigsberg_story)` records the handoff as `success` while the terminal Kaggle publish result is tracked later by `videoannounce_session`, making the retry-loop pattern less obvious without joining both tables.
- Best-effort VK targets can succeed even when the required Telegram target fails, so partial fanout needs explicit operator review before compensation.

## Automation Contract

### Treat as regression guard when

- Changing `handlers/kenigsberg_stories_cmd.py` story config or target list.
- Changing `video_announce/story_publish.py` upload profile handling.
- Changing `kaggle/CrumpleVideo/story_publish.py` story-safe transcode/native upload behavior.
- Changing `scheduling.py` Kenigsberg startup catch-up logic.
- Changing Kenigsberg poller/error status semantics.

### Affected surfaces

- Kenigsberg scheduled startup catch-up.
- Kaggle `KoenigsbergStories` runtime bundle.
- Shared Kaggle story publish helper.
- Telegram Telethon `SendStoryRequest`.
- VK best-effort story/wall fanout for `mostvkenig`.

### Mandatory checks before closure or deploy

- `python -m py_compile` for changed modules.
- `pytest -q tests/test_scheduling.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_h264_profile`
- Verify generated Kenigsberg `story_publish.json` carries `upload_profile=legacy_h264_transcode`.
- Verify startup catch-up skips after at least two same-day failed scheduled/story sessions.
- Verify Fly `/healthz` is ready after deploy.

### Required evidence

- deployed SHA: `85ad5fc7f827e775d260c26b9601ba736eab9d34`
- deploy path: `origin/main` -> `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks:
  - `python3 -m py_compile scheduling.py handlers/kenigsberg_stories_cmd.py video_announce/story_publish.py tests/test_scheduling.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_stories.py`
  - `/tmp/events-bot-poll-venv2/bin/python -m pytest -q tests/test_scheduling.py tests/test_video_announce_story_publish.py tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_h264_profile` -> `38 passed, 1 warning`
- Kaggle/session evidence:
  - session `#661` Kaggle output: preflight passed for `@mostvkenig`, media diagnostics `video_codec=hevc`, `video_tag=hvc1`, Telegram publish failed with `BadRequestError: RPCError 400: MEDIA_FILE_INVALID`, VK story/wall targets succeeded.
  - post-deploy `/data/kaggle_jobs.json`: `{"jobs": []}`.
  - post-deploy startup catch-up log: `SCHED startup catchup skip kenigsberg_story: failed session retry cap reached count=9`.
- compensation decision/evidence: pending operator/Codex decision; no automatic rerun was launched after deploy because VK already partially published session `#661` and Kaggle quota is constrained.

## Immediate Mitigation

- Stop treating session `#661` as an auth-session duplication recurrence: its output proves the auth preflight used the dedicated story account and passed the required primary target.
- Do not manually replay the rejected HEVC artifact as a Telegram story; wait for the H.264 profile fix before any compensation run.

## Corrective Actions

- Kenigsberg production story config now requests `legacy_h264_transcode`, forcing the shared helper's H.264/`avc1` story-safe path.
- `build_story_publish_config` preserves the explicit legacy H.264 profile in `story_publish.json`.
- Kenigsberg startup catch-up now stops after two same-day failed scheduled/story publish sessions.

## Follow-up Actions

- [ ] Codex: deploy the fix from `origin/main`.
- [ ] Codex/operator: decide whether to run a compensation `/kenigsberg --poetry-today` or wait for the next slot, since VK already published for `#661` and Kaggle quota is constrained.
- [ ] Codex: after compensation or next scheduled run, attach terminal Telegram publish evidence and close/monitor this incident.

## Release And Closure Evidence

- deployed SHA: `85ad5fc7f827e775d260c26b9601ba736eab9d34`
- deploy image: `events-bot-new-wngqia:deployment-01KTYVGW573EGB2RTSGZQ28T78`
- deploy path: clean worktree at `origin/main`, then Fly remote deploy
- regression checks:
  - py_compile for changed code/test modules passed
  - targeted pytest suite passed: `38 passed, 1 warning`
- post-deploy verification:
  - Fly machine `48e42d5b714228`, version `1366`, checks `1 passing`
  - `/healthz`: `ok=true`, `ready=true`, `kenigsberg_story_daily=ok`
  - `/data/kaggle_jobs.json` empty
  - startup catch-up did not launch a new Kenigsberg Kaggle job; runtime log recorded the new failed-session retry cap.

## Prevention

- Kenigsberg no longer borrows the CherryFlash HEVC-native upload assumption.
- The startup catch-up retry cap prevents deploy/restart churn from repeatedly burning Kaggle sessions after terminal publish failures.
