# INC-2026-04-27 CherryFlash Missing Photo URLs Before Kaggle

Status: open
Severity: sev1
Service: scheduled CherryFlash `popular_review`
Opened: 2026-04-27
Closed: —
Owners: Codex / events bot operator
Related incidents: `INC-2026-04-27-prod-unresponsive-during-cherryflash-recovery`, `INC-2026-04-23-cherryflash-pre-handoff-loss`, `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The 2026-04-27 scheduled CherryFlash slot repeatedly started local sessions but never reached Kaggle. The selected popular-review events included rows whose persisted `photo_urls` were empty, so render payload validation failed with `Ошибка: нет фото` before any `cherryflash-session-*` dataset or `zigomaro/cherryflash` kernel run appeared.

## User / Business Impact

- The daily CherryFlash story/video was not published in the expected first-half-of-day window.
- Operator notifications showed repeated failed sessions instead of one recoverable handoff.
- The story fanout chain for the current day stayed empty until production fixes and a same-day catch-up rerun.
- A later catch-up appeared green in Kaggle logs but executed an older mounted `cherryflash-session-*` bundle, so it did not prove that the current-day CherryFlash selection or configured Business fanout were actually restored.

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
- 2026-04-27: first same-day catch-up after the durability fix exposed a second failure mode: a rehydrated poster write for event `3398` hit transient SQLite `database is locked`, aborting popularity selection before a valid set could be launched.
- 2026-04-27: a later catch-up created session `#218` and reached remote Kaggle handoff (`zigomaro/cherryflash-session-218-1777292942`, `zigomaro/cherryflash`), while the lock path remained open as a regression risk for future same-day attempts.
- 2026-04-27: Kaggle `#218` preflight then exposed a story fanout defect: configured Telegram Business targets were present in `story_publish.json`, but the mounted encrypted auth payload did not contain their `business_connection` secrets, so the notebook continued after primary-channel preflight while personal-account stories would be skipped.
- 2026-04-27: after the Business-secret fix, a later Kaggle run completed but logged `Using mounted CherryFlash bundle at /kaggle/input/cherryflash-session-192-1777189467` instead of the freshly launched session dataset. It published only the three channel targets shown by the stale bundle and did not include the configured Business targets.
- 2026-04-27: the operator reported that no visible stories appeared despite Kaggle reporting `Story publish status: OK`, so closure evidence must now require both fresh dataset binding and publication fanout evidence for the current session, not only a terminal notebook status.
- 2026-04-27: session `#221` used the fresh `cherryflash-session-221-*` dataset and Kaggle reported successful Business `postStory` responses, but the operator observed that Business stories were visible as active stories while missing from the expected profile/page story list. Investigation found that Business Bot API calls did not pass `post_to_chat_page=true`.

## Root Cause

1. `build_popular_review_selection()` checked candidate renderability through `_ensure_renderable_photo_urls(event)`.
2. For events with empty stored `photo_urls`, `_ensure_renderable_photo_urls()` could fetch poster URLs from the public Telegram/VK source post and mutate the detached `Event` object in memory.
3. The rehydrated URLs were not persisted to SQLite.
4. `start_render()` later rebuilt session items and `payload.json` from freshly loaded event rows, so the same selected event IDs again had empty `photo_urls`.
5. `_render_and_notify()` correctly failed the pre-Kaggle payload validation, but the scheduler kept retrying the same invalid persisted rows.
6. A later repair still allowed an older `cherryflash-session-*` dataset to remain attached to the shared `zigomaro/cherryflash` kernel metadata.
7. The notebook selected the stale mounted session bundle from Kaggle inputs, so it rendered and published according to old session config even though the server-side catch-up was for the current day.
8. The server persisted handoff metadata before verifying that Kaggle metadata exposed the fresh dataset binding, making stale-bundle execution look like a valid daily run.

## Contributing Factors

- Selection and render payload creation use separate database reads, so in-memory repairs are not durable across the handoff.
- The existing no-photo candidate guard proved selection-time intent but did not assert that selected events remain renderable after session persistence.
- The first durability hotfix made poster rehydrate writes mandatory but did not handle transient SQLite writer contention; a locked write could still crash the entire popularity selection instead of skipping only the non-durable candidate.
- CherryFlash Business story targets were still delivered through the shared static story-secrets dataset path, so Kaggle could mount a stale auth payload that did not match the freshly generated session `story_publish.json`; those targets were also treated as non-blocking fanout during preflight.
- The previous CherryFlash contract treated delayed `dataset_sources` metadata as non-fatal telemetry after a successful `kernels_push`; that was too permissive once old per-run datasets could coexist with the fresh session dataset on the same Kaggle kernel.
- Kaggle notebook logs did not print enough non-secret story runtime matching evidence to show which config and encrypted Business secrets were actually loaded before render/publish.
- Business Bot API success was treated as sufficient publication evidence, but the product requirement also needs account page/profile-list visibility; Telegram exposes this through the separate `post_to_chat_page` flag.
- Previous pre-handoff incidents covered local/Kaggle status drift, not this poster durability boundary.

## Automation Contract

### Treat as regression guard when

- Changing CherryFlash popular-review candidate selection, poster fallback/rehydration, session item persistence, or render payload validation.
- Changing scheduled CherryFlash catch-up logic for local-only failed sessions.
- Changing CherryFlash Kaggle kernel deploy, `dataset_sources` merge/prune behavior, handoff persistence, or notebook source-folder resolution.
- Changing CherryFlash story config/secret bundling or Kaggle-side story preflight/publish logging.
- Changing Telegram Business story publication, including `postStory` parameters and `/check_business` smoke behavior.

### Affected surfaces

- `video_announce/popular_review.py`
- `video_announce/scenario.py`
- `video_announce/selection.py`
- scheduled `popular_review` catch-up/watchdog path
- prod SQLite `event`, `videoannounce_session`, `videoannounce_item`, and `ops_run` rows
- Kaggle `cherryflash-session-*` dataset and `zigomaro/cherryflash` kernel handoff
- Kaggle story runtime config/secret loader and publish fanout logs

### Mandatory checks before closure or deploy

- Unit coverage proving a selected event with rehydrated poster URLs persists those URLs to the event row before render handoff.
- Unit coverage proving a rehydrated candidate is skipped, not selected or allowed to crash the run, when its poster repair cannot be persisted after SQLite lock handling.
- Unit coverage proving CherryFlash Business story targets are blocking/required and a missing Business secret fails preflight before render.
- Unit coverage proving CherryFlash kernel deploy prunes older `cherryflash-session-*` sources while preserving static inputs.
- Unit coverage proving CherryFlash fails before persisting Kaggle handoff if the fresh dataset bind wait does not confirm the expected `dataset_sources`.
- Unit coverage proving Business `postStory` calls include `post_to_chat_page=true`.
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
- Post-deploy kernel metadata evidence showing the current `cherryflash-session-*` source is attached and older CherryFlash session datasets are absent.
- Story publication evidence for the 2026-04-27 catch-up that includes the fresh session dataset, channel targets, configured Business target count, matching encrypted Business secret count, and terminal story publish report. A stale `cherryflash-session-192-*` run is not valid closure evidence.
- Business story visibility evidence must distinguish an active story ring from profile/page story-list visibility.

## Immediate Mitigation

- No manual event deletion or selection override was applied before the code fix. The safe mitigation is to persist rehydrated poster URLs or skip no-photo candidates before session launch, then let the same-day catch-up path rerun.

## Corrective Actions

- `video_announce.popular_review._ensure_renderable_photo_urls()` now accepts a database handle and persists rehydrated Telegram/VK poster URLs to the canonical event row.
- `build_popular_review_selection()` passes the database handle into the poster guard, so any candidate accepted because of source-post poster rehydration remains renderable when the render payload reloads events from SQLite.
- Rehydrated poster persistence now retries transient SQLite lock errors for a bounded window and treats non-durable repairs as candidate-ineligible, so one locked event write cannot crash the whole CherryFlash popularity set or produce a selected event that reloads without photos.
- Scheduled CherryFlash now treats a `None` session id from `run_popular_review_pipeline()` as a failed `ops_run`, so no-op catch-up attempts remain visible and retryable.
- CherryFlash now writes encrypted story secrets into the same per-run `cherryflash-session-*` dataset as `story_publish.json`, and configured Telegram Business targets are generated as `blocking=true` / `required=true`.
- CherryFlash kernel deploy now removes older `cherryflash-session-*` dataset sources before adding the fresh per-run dataset, while preserving static inputs.
- CherryFlash now waits for Kaggle metadata to confirm the fresh dataset binding before persisting local handoff metadata; if the bind wait fails, the session fails closed instead of letting a stale mounted bundle masquerade as a successful run.
- The Kaggle story runtime now logs non-secret config/secret matching diagnostics at startup, including target labels, Business target count, encrypted Business secret count, and missing Business hashes.
- Business `postStory` calls from CherryFlash and `/check_business` now pass `post_to_chat_page=true` so accepted stories also target account page/profile visibility.
- Added regression coverage for the exact missing durability boundary.
- Updated the CherryFlash feature doc and incident index with the new contract.

## Follow-up Actions

- [ ] Confirm whether the scheduler should alert after repeated same-root local-only failures instead of waiting for operator observation.
- [ ] Consider adding a compact admin report row that shows selected event IDs with persisted poster counts before Kaggle launch.
- [ ] Add an operator-facing story publication audit that compares configured channel/Business targets with observed publish attempts and terminal per-target results.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Poster eligibility is now both selection-time and persistence-time guarded.
- This incident record is an active regression contract for future CherryFlash selection/render changes.
