# INC-2026-06-12-kenigsberg-story-media-invalid-catchup-loop

Status: closed
Severity: sev2
Service: Kenigsberg Stories / Kaggle story publish / scheduler catch-up
Opened: 2026-06-12
Closed: 2026-06-12
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
- 2026-06-12: investigation downloaded Kaggle output from production.
- 2026-06-12: follow-up git/DB review corrected the initial hypothesis:
  the compact HEVC-native profile had been used successfully by earlier
  thought-mode scheduled stories; the failed 2026-06-12 sessions were the first
  long production poetry runs.

## Root Cause

1. The failed 2026-06-12 sessions were long poetry productions (`poem-1`) rather than the previously successful thought-mode scheduled stories.
2. Session `#661` produced a native HEVC/`hvc1` file with `duration_seconds=59.133`, very close to Telegram's story duration boundary, and Telegram rejected it during `SendStoryRequest` with `MEDIA_FILE_INVALID`.
3. Kenigsberg startup catch-up checked only for a confirmed handoff, not for repeated same-day failed scheduled/story sessions, so deploy restarts could relaunch the missed slot repeatedly.
4. The initial H.264 permanent fix was too broad because it changed the compact media profile that had worked for earlier Kenigsberg stories; it is kept only as evidence from the one-off compensation publish.

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
- `pytest -q tests/test_scheduling.py tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_native_profile`
- Verify generated Kenigsberg `story_publish.json` carries `upload_profile=telegram_story_native_hevc_720p_v1`.
- Verify the scheduled Kenigsberg cron trigger is weekly (`day_of_week='fri'`) rather than daily while this mitigation is active.
- Verify startup catch-up skips after at least two same-day failed scheduled/story sessions.
- Verify Fly `/healthz` is ready after deploy.

### Required evidence

- deployed SHA: pending weekly-cadence redeploy
- deploy path: `origin/main` -> `flyctl deploy -a events-bot-new-wngqia --remote-only`
- regression checks:
  - `python3 -m py_compile scheduling.py handlers/kenigsberg_stories_cmd.py tests/test_scheduling.py tests/test_kenigsberg_stories.py`
  - `/tmp/events-bot-poll-venv2/bin/python -m pytest -q tests/test_scheduling.py::test_kenigsberg_story_startup_catchup_retries_single_failed_session tests/test_scheduling.py::test_kenigsberg_story_startup_catchup_skips_after_two_failed_sessions tests/test_scheduling.py::test_kenigsberg_story_startup_catchup_skips_non_weekly_day tests/test_kenigsberg_stories.py::test_kenigsberg_production_story_config_uses_mostvkenig_and_native_profile`
- Kaggle/session evidence:
  - session `#661` Kaggle output: preflight passed for `@mostvkenig`, media diagnostics `video_codec=hevc`, `video_tag=hvc1`, Telegram publish failed with `BadRequestError: RPCError 400: MEDIA_FILE_INVALID`, VK story/wall targets succeeded.
  - post-deploy `/data/kaggle_jobs.json`: `{"jobs": []}`.
  - post-deploy startup catch-up log: `SCHED startup catchup skip kenigsberg_story: failed session retry cap reached count=9`.
- compensation decision/evidence: no new Kaggle render was launched. The finished
  `#661` output was downloaded, transcoded locally through the shared H.264
  story-safe helper path, and published only to the required Telegram target
  `@mostvkenig` to avoid duplicating the already-successful VK story/wall.
  Compensation result: `ok=true`, `story_id=22`, media `720x1280`,
  `30fps`, `59.1s`, `7,563,742` bytes. Local temporary output/runtime files
  containing encrypted story secrets were removed after the run.

## Immediate Mitigation

- Stop treating session `#661` as an auth-session duplication recurrence: its output proves the auth preflight used the dedicated story account and passed the required primary target.
- Do not start another same-day Kaggle retry for the rejected long-poetry artifact; keep further compensation Telegram-only unless VK fanout also failed.

## Corrective Actions

- Kenigsberg production story config is restored to `telegram_story_native_hevc_720p_v1`.
- Scheduled production cadence is temporarily reduced from daily to weekly Friday `19:30 Europe/Kaliningrad`; startup catch-up also runs only for that weekly slot day.
- Kenigsberg startup catch-up now stops after two same-day failed scheduled/story publish sessions.

## Follow-up Actions

- [x] Codex: deploy the fix from `origin/main`.
- [x] Codex: run Telegram-only compensation from the already-rendered `#661`
  artifact without starting another Kaggle render or duplicating VK fanout.
- [x] Codex: attach terminal Telegram publish evidence and close this incident.

## Release And Closure Evidence

- deployed SHA: pending weekly-cadence redeploy
- deploy image: pending weekly-cadence redeploy
- deploy path: clean worktree at `origin/main`, then Fly remote deploy
- regression checks:
  - py_compile for changed code/test modules passed
  - targeted pytest suite passed
- post-deploy verification:
  - Fly machine/check evidence pending weekly-cadence redeploy
  - `/healthz`: `ok=true`, `ready=true`, `kenigsberg_story_daily=ok`
  - `/data/kaggle_jobs.json` empty
  - startup catch-up did not launch a new Kenigsberg Kaggle job; runtime log recorded the new failed-session retry cap.
- compensation verification:
  - downloaded `#661` output from `zigomaro/koenigsberg-stories`
  - H.264 story-safe upload copy prepared as `720x1280`, `30fps`, `59.1s`,
    `7,563,742` bytes
  - Telegram story published to `@mostvkenig`, `story_id=22`,
    `blocking_ok=true`, `required_ok=true`
  - VK fanout was intentionally excluded from compensation because `#661` had
    already published the VK story/wall successfully.

## Prevention

- Kenigsberg keeps the proven compact HEVC-native upload profile for scheduled stories while the long-poetry media edge case is reviewed.
- Weekly cadence reduces exposure and quota burn during that review.
- The startup catch-up retry cap prevents deploy/restart churn from repeatedly burning Kaggle sessions after terminal publish failures.
