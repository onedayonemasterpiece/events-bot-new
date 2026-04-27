# INC-2026-04-27 CherryFlash Missing Photo URLs Before Kaggle

Status: open
Severity: sev1
Service: scheduled CherryFlash `popular_review`
Opened: 2026-04-27
Closed: —
Owners: Codex / events bot operator
Related incidents: `INC-2026-04-23-cherryflash-pre-handoff-loss`, `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The 2026-04-27 scheduled CherryFlash slot repeatedly started local sessions but never reached Kaggle. The selected popular-review events included rows whose persisted `photo_urls` were empty, so render payload validation failed with `Ошибка: нет фото` before any `cherryflash-session-*` dataset or `zigomaro/cherryflash` kernel run appeared.

## User / Business Impact

- The daily CherryFlash story/video was not published in the expected first-half-of-day window.
- Operator notifications showed repeated failed sessions instead of one recoverable handoff.
- The story fanout chain for the current day stayed empty until a production fix and same-day catch-up rerun.

## Detection

- User reported the repeated Telegram operator messages on 2026-04-27.
- Visible failed sessions: `#194`, `#195`, `#196`, `#197`, and `#216`.
- All reported failures stopped before Kaggle with event IDs `4261` and `4262` missing `photo_urls`.
- Runtime file mirror must be checked for production incidents, but production currently has `ENABLE_RUNTIME_FILE_LOGGING=0`, so closure evidence uses prod SQLite rows, Fly logs, Kaggle/session rows, and operator notifications.

## Timeline

- 2026-04-27 09:43 Europe/Kaliningrad: session `#194` started, picked `4264:24h, 4261:24h, 4262:24h, 4254:3d, 4255:3d, 4047:3d`, then failed before Kaggle because `4261` and `4262` had no `photo_urls`.
- 2026-04-27 09:59 Europe/Kaliningrad: session `#195` repeated the same failure.
- 2026-04-27 10:09 Europe/Kaliningrad: session `#196` repeated the same failure.
- 2026-04-27 10:19 Europe/Kaliningrad: session `#197` repeated the same failure, with `4246` replacing `4047` in the 3-day window.
- 2026-04-27 13:29 Europe/Kaliningrad: session `#216` repeated the same failure.
- 2026-04-27: hotfix investigation found that selection could rehydrate poster URLs in memory but did not persist them before the render path reloaded events from SQLite.
- 2026-04-27: production SQLite evidence confirmed sessions `#194..#216` were all local-only failures (`kaggle_dataset=null`, `kaggle_kernel_ref=local:CherryFlash`) and `ops_run` rows `#885..#906` repeatedly failed with `CherryFlash did not reach confirmed Kaggle handoff`.
- 2026-04-27: production event rows confirmed event `4261` and `4262` had `photo_count=0` and serialized `photo_urls=[]`, while sibling picked events already had one persisted poster each.

## Root Cause

1. `build_popular_review_selection()` checked candidate renderability through `_ensure_renderable_photo_urls(event)`.
2. For events with empty stored `photo_urls`, `_ensure_renderable_photo_urls()` could fetch poster URLs from the public Telegram/VK source post and mutate the detached `Event` object in memory.
3. The rehydrated URLs were not persisted to SQLite.
4. `start_render()` later rebuilt session items and `payload.json` from freshly loaded event rows, so the same selected event IDs again had empty `photo_urls`.
5. `_render_and_notify()` correctly failed the pre-Kaggle payload validation, but the scheduler kept retrying the same invalid persisted rows.

## Contributing Factors

- Selection and render payload creation use separate database reads, so in-memory repairs are not durable across the handoff.
- The existing no-photo candidate guard proved selection-time intent but did not assert that selected events remain renderable after session persistence.
- Previous pre-handoff incidents covered local/Kaggle status drift, not this poster durability boundary.

## Automation Contract

### Treat as regression guard when

- Changing CherryFlash popular-review candidate selection, poster fallback/rehydration, session item persistence, or render payload validation.
- Changing scheduled CherryFlash catch-up logic for local-only failed sessions.

### Affected surfaces

- `video_announce/popular_review.py`
- `video_announce/scenario.py`
- `video_announce/selection.py`
- scheduled `popular_review` catch-up/watchdog path
- prod SQLite `event`, `videoannounce_session`, `videoannounce_item`, and `ops_run` rows
- Kaggle `cherryflash-session-*` dataset and `zigomaro/cherryflash` kernel handoff

### Mandatory checks before closure or deploy

- Unit coverage proving a selected event with rehydrated poster URLs persists those URLs to the event row before render handoff.
- Existing CherryFlash popular-review regression tests.
- Existing CherryFlash pre-handoff/catch-up regression checks from `INC-2026-04-23-cherryflash-pre-handoff-loss`.
- `python -m py_compile` for touched video announce/scheduler modules.
- Release-governance checks: clean worktree, fix reachable from `origin/main`, and deploy SHA recorded.
- Same-day production catch-up rerun after deploy, unless a newer remote CherryFlash handoff for 2026-04-27 already exists.

### Required evidence

- Prod session evidence showing pre-fix local-only failures with empty Kaggle dataset/kernel: captured from `/data/db.sqlite` on Fly; sessions `#194..#216` were `FAILED`, `kaggle_dataset=null`, `kaggle_kernel_ref=local:CherryFlash`.
- Prod event evidence showing pre-fix no-photo rows: events `4261` and `4262` had `photo_count=0`, serialized `photo_urls=[]`, and source post `https://t.me/locostandup/3361`.
- Runtime log mirror evidence: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory exists but is empty; fallback evidence is prod SQLite/Fly/Kaggle.
- Test command output for the poster persistence regression.
- Deployed SHA reachable from `origin/main`.
- Post-deploy CherryFlash session with non-local Kaggle dataset/kernel evidence.
- Story publication or terminal story publish report evidence for the 2026-04-27 catch-up.

## Immediate Mitigation

- No manual event deletion or selection override was applied before the code fix. The safe mitigation is to persist rehydrated poster URLs or skip no-photo candidates before session launch, then let the same-day catch-up path rerun.

## Corrective Actions

- `video_announce.popular_review._ensure_renderable_photo_urls()` now accepts a database handle and persists rehydrated Telegram/VK poster URLs to the canonical event row.
- `build_popular_review_selection()` passes the database handle into the poster guard, so any candidate accepted because of source-post poster rehydration remains renderable when the render payload reloads events from SQLite.
- Added regression coverage for the exact missing durability boundary.
- Updated the CherryFlash feature doc and incident index with the new contract.

## Follow-up Actions

- [ ] Confirm whether the scheduler should alert after repeated same-root local-only failures instead of waiting for operator observation.
- [ ] Consider adding a compact admin report row that shows selected event IDs with persisted poster counts before Kaggle launch.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Poster eligibility is now both selection-time and persistence-time guarded.
- This incident record is an active regression contract for future CherryFlash selection/render changes.
