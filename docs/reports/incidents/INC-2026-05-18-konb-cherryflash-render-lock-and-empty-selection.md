# INC-2026-05-18 KONB CherryFlash Render Lock And Empty Selection

Status: monitoring
Severity: sev1
Service: CherryFlash partner story tracks / КОНБ scheduled production slot
Opened: 2026-05-18
Closed: —
Owners: bot/runtime
Related incidents: `INC-2026-05-17-konb-cherryflash-test-story-preflight`, `INC-2026-05-17-eco-cherryflash-underfilled-event-recall`, `INC-2026-05-15-cherryflash-partner-fanout-promo-filter`, `INC-2026-04-23-cherryflash-pre-handoff-loss`, `INC-2026-04-24-crumple-story-channel-boosts-required`
Related docs: `docs/features/cherryflash/partner-story-tracks.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-05-18 the first production `partner_konb_library_001` CherryFlash slot did not publish. The operator log showed repeated `Сессия #325 уже рендерится` messages at the scheduled KОНБ time and watchdog retries, followed by `CherryFlash popular review did not collect enough events (selected=0 min=1)`.

## User / Business Impact

- The KОНБ production video announcement missed its same-day scheduled publication.
- The intended 7-minute offset after the eco/nature slot did not isolate KОНБ from a slow eco render.
- A dry KОНБ fresh pool still failed closed instead of re-airing eligible future KОНБ events from prior issues; the first compensating rerun also showed that every reusable КОНБ event had already appeared earlier the same Kaliningrad calendar day, so the strict same-day cooldown still produced `selected=0`.

## Detection

The operator reported the production failure from bot messages:

- 2026-05-18 12:37 local: `Сессия #325 уже рендерится`
- 2026-05-18 12:43 local: `Сессия #325 уже рендерится`
- 2026-05-18 12:59 local: `Сессия #325 уже рендерится`
- 2026-05-18 13:09 local: `Сессия #325 уже рендерится`
- 2026-05-18 13:19 local: `selected=0 min=1`

## Timeline

- 2026-05-17: KОНБ was promoted to `prod` mode with Telegram story and VK story best-effort targets.
- 2026-05-18 12:30 Europe/Kaliningrad: eco/nature partner slot started and left session `#325` in `RENDERING`.
- 2026-05-18 12:37 Europe/Kaliningrad: KОНБ scheduled slot attempted to start, but the global render lock blocked it behind `#325`.
- 2026-05-18 12:43, 12:59, 13:09 Europe/Kaliningrad: KОНБ watchdog retries were blocked by the same global lock.
- 2026-05-18 13:19 Europe/Kaliningrad: after the lane freed, KОНБ selection underfilled to zero and failed instead of recycling older eligible future KОНБ events.
- 2026-05-18 after first fix deploy: manual compensating run reached the new recycle path but still skipped reusable KОНБ events (`5021`, `5038`, `5046`, `4412`, `5047`) because they had appeared in same-day test sessions.
- 2026-05-18 14:07 Europe/Kaliningrad: second fix deployed at `e2437dd1`.
- 2026-05-18 14:07-14:35 Europe/Kaliningrad: compensating run created session `#326`, selected five events through `konb_same_day_recycle`, rendered on Kaggle, and published the VK story (`vk:konb39:story`, `story_id=456239755`). Telegram channel-story target `tg:@kaliningradlibrary:story` was attempted but failed with `BOOSTS_REQUIRED`; this is best-effort by the KОНБ prod contract and is tracked as the channel-story boost prerequisite surface.

## Root Cause

1. `run_partner_track_pipeline()` called `has_rendering()` without a profile scope, so any in-flight video session blocked every partner track even when the DB and scheduler already scope partner runs by `profile_key`.
2. The KОНБ selector had a broad future fallback, but no last-resort re-air pool from previous KОНБ issues when the current fresh/future scan underfilled.
3. The previous anti-repeat fix allowed next-day repeats, but still depended on candidates reaching the selector through fresh/future collection.
4. During the production launch/test day, the same КОНБ pool had already been rendered in earlier same-day test sessions. Cross-day recycle was therefore insufficient for same-day compensation: the emergency contract needed a final same-day recycle layer that still prevents duplicates inside one issue.

## Contributing Factors

- The KОНБ slot was intentionally scheduled only 7 minutes after eco/nature, so any global lock regression has immediate production impact.
- KОНБ has a small event pool; a no-repeat or no-recycle policy is too brittle for daily scheduled publishing.
- The operator-facing message did not name the blocking profile, only the session id.

## Automation Contract

### Treat as regression guard when

- Changing `VideoAnnounceScenario.has_rendering()` or `run_partner_track_pipeline()`.
- Changing KОНБ selection fallback/repeat policy in `video_announce/popular_review.py`.
- Changing partner-track cron/watchdog scheduling or same-day catch-up behavior.
- Changing KОНБ publish mode or target config.

### Affected surfaces

- `video_announce/scenario.py`
- `video_announce/popular_review.py`
- `scheduling.py`
- `video_announce/partner_tracks.py`
- KОНБ Telegram/VK story publish via CherryFlash Kaggle handoff
- production `videoannounce_session` and `ops_run` evidence
- Telegram channel story boost prerequisite (`BOOSTS_REQUIRED`) for `@kaliningradlibrary`

### Mandatory checks before closure or deploy

- `py_compile` for changed CherryFlash/scheduler modules.
- `tests/test_video_announce_v_pipeline.py` must prove a `popular_review_eco` `RENDERING` session does not block `popular_review_konb` partner launch checks.
- `tests/test_video_announce_popular_review.py` must prove KОНБ can recycle a previously shown future event when fresh/future pools are empty.
- `tests/test_video_announce_popular_review.py` must prove same-calendar-day KОНБ repeats are still blocked by default.
- `tests/test_video_announce_popular_review.py` must prove the KОНБ partner pipeline can opt into same-day recycle as a last resort and marks it as `konb_same_day_recycle` / `same_day_recycle`.
- Existing KОНБ target checks from `INC-2026-05-17-konb-cherryflash-test-story-preflight` must still pass.
- Production `/healthz` after deploy.
- Same-day compensating KОНБ rerun/catch-up after deploy, with session id, Kaggle dataset/kernel, and story publish evidence.

### Required evidence

- deployed SHA: `e2437dd1` (with first incident fix `ebb6cf36` already in `origin/main`)
- tests: py_compile passed; focused KОНБ regression tests passed; broader relevant suite printed `129 passed`
- production health: `/healthz` ok/ready after deploy
- compensating rerun session id: `#326`
- Kaggle dataset/kernel: `zigomaro/cherryflash-session-326-1779106119` / `zigomaro/cherryflash`
- story publish evidence: VK story `vk:konb39:story` published as `story_id=456239755`; Telegram channel story `tg:@kaliningradlibrary:story` attempted and failed `BOOSTS_REQUIRED`
- fix reachable from `origin/main`: yes, `e2437dd1`

## Immediate Mitigation

Applied and deployed: partner render-lock checks are scoped by `partner_track.profile_key`, KОНБ has a recycle pool sourced from previous KОНБ video sessions, and the KОНБ partner pipeline has a same-day emergency recycle opt-in for missed daily-slot compensation.

## Corrective Actions

- `has_rendering(profile_key=...)` keeps the legacy global behavior by default but can now answer per-profile in-flight checks.
- `run_partner_track_pipeline()` uses the partner profile key for its render-lock check, so a slow eco/nature render no longer blocks the KОНБ slot.
- KОНБ selection now has a last-resort `konb_recycle` pool: future/current events shown in prior KОНБ issues can re-enter after at least one Europe/Kaliningrad calendar-day boundary.
- Same-calendar-day repeats remain blocked by default in the selector.
- `run_partner_track_pipeline()` passes `allow_same_day_recycle=True` only for the KОНБ selection policy, enabling a final `konb_same_day_recycle` layer when otherwise the production slot would be missed. Same-video duplicates remain blocked.

## Follow-up Actions

- [ ] Add operator-facing render-lock text that includes the blocking profile key.
- [ ] Revisit per-run CherryFlash Kaggle kernel isolation so concurrency is not tied to shared kernel metadata mutation.
- [ ] Resolve the Telegram channel-story prerequisite for `@kaliningradlibrary`: session `#326` attempted `tg:@kaliningradlibrary:story` and failed with `BOOSTS_REQUIRED`, while `vk:konb39:story` succeeded. Until the channel has enough boosts or a supported business/story target is configured, KОНБ Telegram story fanout remains best-effort.

## Release And Closure Evidence

- deployed SHAs:
  - `ebb6cf36e02776fedc47e9112338d169cc624405` (`fix(cherryflash): recover KONB partner slot`) — profile-scoped render guard, KОНБ recycle, VK personal/community source work.
  - `e2437dd10e0ee95d2fefbcbc80ef5ac8c99853e1` (`fix(cherryflash): allow KONB emergency recycle`) — KОНБ-only same-day emergency recycle.
- deploy path:
  - `flyctl deploy -a events-bot-new-wngqia` from clean linked worktree `/tmp/events-bot-new-deploy-konb`.
  - First deploy image `deployment-01KRXF3A6T1CCG7ZYF0Z0YCCKP`; second deploy image `deployment-01KRXFNQWG3S1HS1JXK9KTQH9B`.
- regression checks:
  - `py_compile` for changed CherryFlash/VK/scheduler modules passed.
  - `tests/test_video_announce_popular_review.py`, `tests/test_video_announce_v_pipeline.py`, `tests/test_video_announce_story_publish.py`, `tests/test_partner_track_isolation.py`, `tests/test_partner_track_scheduling.py`, `tests/test_partner_tracks.py`, selected VK publish/source tests: `129 passed` (pytest printed success; the runner process did not terminate cleanly and was stopped after success output).
  - Focused regression rerun after the second patch: `3 passed` for default same-day block, emergency same-day recycle, and profile-scoped render lock.
- post-deploy verification:
  - `/healthz` after second deploy: `ok=true`, `ready=true`, `db=ok`, `scheduler=ok`, no issues.
  - Runtime jobs registered include `video_partner_track_konb` and `video_partner_track_konb_watchdog`.
  - Runtime file logging was available at `/data/runtime_logs/events-bot.log`.
- compensating rerun:
  - command: `python scripts/run_cherryflash_live.py --partner-track partner_konb_library_001 --timeout-minutes 240 --poll-seconds 30` on Fly.
  - session `#326`, status `PUBLISHED_TEST` (the poller status used for rendered story/video sessions), `video_url=cherryflash_full_final.mp4`, `error=NULL`.
  - selected event ids: `5047`, `4412`, `5046`, `5038`, `5021`.
  - selection trace: all five were `source_window=konb_same_day_recycle`, `anti_repeat_status=same_day_recycle`.
  - Kaggle dataset/kernel: `zigomaro/cherryflash-session-326-1779106119`, `zigomaro/cherryflash`, kernel push version `104`.
  - story evidence: config targeted `tg:@kaliningradlibrary:story` and `vk:konb39:story`; Telegram channel story failed `BOOSTS_REQUIRED`; VK story published successfully to `vk:konb39:story` with `story_id=456239755`; overall story publish status `OK` because both KОНБ prod targets are independent best-effort.
  - evidence excerpt saved locally under `artifacts/codex/INC-2026-05-18-konb-cherryflash/session-326-evidence.log` (not committed).
- fix reachable from `origin/main`:
  - `e2437dd1` pushed to `origin/main`.

## Prevention

This record is the regression contract for KОНБ daily publication resilience: a slow eco/nature partner render must not make KОНБ miss its slot, and an underfilled KОНБ pool must prefer a controlled recycle, including same-day emergency recycle for the КОНБ partner pipeline, over no publication.
