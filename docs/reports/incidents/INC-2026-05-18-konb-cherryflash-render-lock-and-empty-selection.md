# INC-2026-05-18 KONB CherryFlash Render Lock And Empty Selection

Status: open
Severity: sev1
Service: CherryFlash partner story tracks / КОНБ scheduled production slot
Opened: 2026-05-18
Closed: —
Owners: bot/runtime
Related incidents: `INC-2026-05-17-konb-cherryflash-test-story-preflight`, `INC-2026-05-17-eco-cherryflash-underfilled-event-recall`, `INC-2026-05-15-cherryflash-partner-fanout-promo-filter`, `INC-2026-04-23-cherryflash-pre-handoff-loss`
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

- deployed SHA:
- tests:
- production health:
- compensating rerun session id:
- Kaggle dataset/kernel:
- story publish evidence:
- fix reachable from `origin/main`:

## Immediate Mitigation

Pending deploy: scope partner render-lock checks by `partner_track.profile_key`, add a KОНБ recycle pool sourced from previous KОНБ video sessions, and add a KОНБ-only same-day emergency recycle opt-in for missed daily-slot compensation.

## Corrective Actions

- `has_rendering(profile_key=...)` keeps the legacy global behavior by default but can now answer per-profile in-flight checks.
- `run_partner_track_pipeline()` uses the partner profile key for its render-lock check, so a slow eco/nature render no longer blocks the KОНБ slot.
- KОНБ selection now has a last-resort `konb_recycle` pool: future/current events shown in prior KОНБ issues can re-enter after at least one Europe/Kaliningrad calendar-day boundary.
- Same-calendar-day repeats remain blocked by default in the selector.
- `run_partner_track_pipeline()` passes `allow_same_day_recycle=True` only for the KОНБ selection policy, enabling a final `konb_same_day_recycle` layer when otherwise the production slot would be missed. Same-video duplicates remain blocked.

## Follow-up Actions

- [ ] Add operator-facing render-lock text that includes the blocking profile key.
- [ ] Revisit per-run CherryFlash Kaggle kernel isolation so concurrency is not tied to shared kernel metadata mutation.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:
- compensating rerun:

## Prevention

This record is the regression contract for KОНБ daily publication resilience: a slow eco/nature partner render must not make KОНБ miss its slot, and an underfilled KОНБ pool must prefer a controlled recycle, including same-day emergency recycle for the КОНБ partner pipeline, over no publication.
