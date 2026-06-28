# INC-2026-06-28 CrumpleVideo publish-only recovery storm

Status: mitigated
Severity: sev2
Service: CrumpleVideo / scheduled `/v tomorrow` video announcement
Opened: 2026-06-28
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce`, `INC-2026-06-16-cherryflash-duplicate-after-bot-send-failure`, `INC-2026-06-26-tg-story-message-forward`
Related docs: `docs/features/crumple-video/README.md`, `docs/features/kaggle-status-framework/README.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-28 the scheduled CrumpleVideo `/v tomorrow` session `#777` repeatedly
posted the same video announcement to the viewer-facing test/debug Telegram
channel `@keniggpt` (`https://t.me/keniggpt/2451` and following messages) every
about five minutes. This was not a full rerender storm: production DB and logs
showed one source render session and a loop of `CrumpleStoryPublishOnly`
publish-only recovery attempts.

## User / Business Impact

- `@keniggpt` subscribers saw a visible stream of duplicate `Видео-анонс #777`
  posts from `17:05` through `18:35 UTC`.
- The loop consumed Kaggle dataset/kernel operations and risked repeating
  downstream publish-only side effects.
- Operator state was misleading: the source video session had rendered an mp4,
  but fresh publish-only ledger rows made the source session look recoverable to
  the restart/resume path.

## Detection

- Detected by operator report with public links `https://t.me/keniggpt/2467`,
  `https://t.me/keniggpt/2466`, `https://t.me/keniggpt/2465`, and
  `https://t.me/keniggpt/2464`.
- Public Telegram HTML confirmed duplicate `Видео-анонс #777 на завтра 4 июня -
  30 июня` messages every about five minutes.
- Production DB confirmed session `#777`, `profile_key=default`,
  `scheduled_slot_key=crumple_video:default:2026-06-29:tomorrow`, one source
  kernel `zigomaro/crumple-video`, and repeated fresh ledgers whose `run_id`
  matched `videoannounce:777:publish-only:*`.
- Runtime mirror `/data/runtime_logs/events-bot.log*` confirmed repeated
  `publish-only dataset created` / `crumple-story-publish-only` runs.

## Timeline

All times are UTC.

- 2026-06-28 14:45:00 — scheduled `video_tomorrow` run created session `#777`.
- 2026-06-28 17:03:02 — source CrumpleVideo Kaggle run exited after publish
  phase and released its Telegram story resource lease.
- 2026-06-28 17:05:41 — first public `@keniggpt` duplicate wave message for
  session `#777` observed.
- 2026-06-28 17:08:58..18:39:04 — server repeatedly created
  `videoannounce:777:publish-only:*` ledger rows and publish-only datasets every
  about five minutes.
- 2026-06-28 18:35:34 — latest duplicate visible in public Telegram HTML during
  triage (`@keniggpt/2469`).
- 2026-06-28 18:39:25 — immediate containment terminalized 21 non-terminal
  `videoannounce:777:publish-only:*` ledger rows as `cancelled` to stop the
  resume loop.
- 2026-06-28 18:42 — production DB showed session `#777` with rendered
  `crumple_video_final.mp4`, no active publish-only ledger, and no public
  message after `18:35` yet visible.

## Root Cause

1. `resume_rendering_sessions()` includes sessions whose ids are returned by
   `_live_video_ledger_session_ids()` even when the source session is already
   terminal/published.
2. `_live_video_ledger_session_ids()` treated fresh `updated_at` on any
   `videoannounce:%` ledger as liveness. Publish-only recovery ledger rows are
   created with `status=created`, no heartbeat, no terminal timestamp, and a
   fresh `updated_at`, so they were classified as a live source-video session.
3. The resumed source poller downloaded the already-rendered source output,
   saw the old story-report failure, started another publish-only recovery, and
   then sent the mp4 to the test/debug Telegram channel again.

## Contributing Factors

- Source render runs and publish-only compensation runs shared the same
  `session_id` and `videoannounce:%` ledger namespace without a resume filter on
  `run_id`, `kind`, or `notebook`.
- The live-ledger helper name/comment promised heartbeat-based liveness, but the
  implementation fell back to `updated_at`, making just-created recovery ledger
  rows look live.
- The test channel `@keniggpt` is public/viewer-facing, so a delivery retry to a
  test target has user-visible impact.

## Automation Contract

### Treat as regression guard when

- changing `video_announce/poller.py` resume/restart recovery;
- changing Kaggle status ledger liveness logic;
- changing CrumpleVideo/CherryFlash publish-only recovery;
- changing scheduled video watchdog/catch-up behavior;
- changing test/notify chat delivery for video announcements.

### Affected surfaces

- `video_announce/poller.py` `_live_video_ledger_session_ids()` and
  `resume_rendering_sessions()`.
- Kaggle status tables `kaggle_run_ledger` and publish-only `run_id` shapes.
- CrumpleVideo source session `#777` and `CrumpleStoryPublishOnly` recovery.
- Public Telegram test/debug channel `@keniggpt`.

### Mandatory checks before closure or deploy

- Unit test proving `videoannounce:<id>:publish-only:*` ledgers do not make the
  source session resumable.
- Unit test proving terminal/published source sessions (`DONE`,
  `PUBLISHED_TEST`, `PUBLISHED_MAIN`) do not resume even if a source live ledger
  is fresh.
- Regression test proving a genuinely false-failed source session with a fresh
  source heartbeat is still revived.
- Production DB check after containment/deploy showing no new
  `videoannounce:777:publish-only:*` non-terminal rows.
- Public Telegram smoke showing no new `@keniggpt` `#777` post after the
  containment/deploy window.
- `/healthz` production OK after deploy.

### Required evidence

- Public Telegram links around `https://t.me/keniggpt/2464`..
  `https://t.me/keniggpt/2469`.
- Production DB rows for `videoannounce_session.id=777` and
  `kaggle_run_ledger.run_id LIKE 'videoannounce:777:publish-only:%'`.
- Runtime log lines for repeated `publish-only dataset created session=777` and
  `crumple-story-publish-only` output downloads.
- Test output for `tests/test_video_announce_poller.py`.
- Deployed SHA reachable from `origin/main` once corrective code is shipped.

## Immediate Mitigation

- Terminalized 21 non-terminal `videoannounce:777:publish-only:*` ledger rows in
  production at `2026-06-28T18:39:25Z` as `cancelled` with error
  `incident containment: stop duplicate publish-only recovery storm`.
- Left the source session and rendered video artifact intact; no full rerender
  was started.

## Corrective Actions

- Exclude publish-only ledger rows from source video liveness detection by
  `run_id`, `kind`, and `notebook`.
- Restrict restart/resume recovery from live ledgers to source sessions whose DB
  status is actually resumable (`RENDERING` or false-failed `FAILED`), not
  already terminal/published sessions.
- Add regression tests for the publish-only storm shape while preserving the
  false-failed live-ledger recovery behavior.

## Follow-up Actions

- [ ] Decide whether public `@keniggpt` duplicate cleanup is desired; if yes,
      keep one canonical message and delete only verified duplicates.
- [ ] Consider splitting publish-only recovery ledger namespace from source
      video sessions in operator UI/reporting.

## Release And Closure Evidence

- deployed SHA: `1004909a62f34d7f4138775c1af34179f3db8e49`, reachable from
  `origin/main`.
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only`
  from clean branch `hotfix/inc-crumple-video-publish-only-storm-20260628`.
  Fly image `registry.fly.io/events-bot-new-wngqia:deployment-01KW7RT48KM3ARG9ACV16CWR3T`,
  machine `683961db016e28` version `1516`, `1 total, 1 passing`.
- regression checks:
  - `python3 -m py_compile video_announce/poller.py` — passed.
  - `uv run --with-requirements requirements.txt --with pytest --with pytest-asyncio pytest -q tests/test_video_announce_poller.py` — `18 passed in 3.84s` locally before deploy.
  - `git diff --check` — passed.
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, DB `ok`, scheduler `ok`.
  - production code probe confirmed `/app/video_announce/poller.py` contains the
    publish-only ledger guard and `resumable_live_statuses` filter.
  - at `2026-06-28T18:50:40Z`, production DB had
    `nonterminal_publish_only_count=0` for
    `run_id LIKE 'videoannounce:777:publish-only:%'`; latest publish-only row
    remained the contained `18:39:04Z` row marked `cancelled`.
  - public Telegram HTML for `https://t.me/s/keniggpt` showed no new
    `Видео-анонс #777` after `@keniggpt/2469` at `2026-06-28T18:35:34Z`.

## Prevention

The invariant for video restart recovery is: publish-only compensation ledger
rows may describe a narrow downstream retry, but they must never resurrect the
source render poller or cause a source mp4/test-channel delivery to repeat.
