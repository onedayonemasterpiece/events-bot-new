# INC-2026-07-18 CherryFlash Missing True3D Runtime Bundle

Status: open
Severity: sev1
Service: CherryFlash scheduled `popular_review` / Telegram Stories / Kaggle
Opened: 2026-07-18
Closed: —
Owners: video announce runtime / operations
Related incidents: `INC-2026-07-11-cherryflash-eco-retry-storm.md`, `INC-2026-04-23-cherryflash-pre-handoff-loss.md`, `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/cron.md`, `docs/features/kaggle-status-framework/README.md`, `docs/operations/runtime-logs.md`

## Summary

The daily base CherryFlash stopped reaching Telegram Stories after the July 11
release of the approved guide True3D scene. The renderer began calling
`scripts/render_cherryflash_guide_true3d_v4.py`, but the per-session Kaggle
bundle continued shipping only `render_cherryflash_full.py` and the intro
scripts. Every base run from July 12 onward therefore exited during render
before the story publish/report phase.

The server then converted the failed notebook into false `PUBLISHED_TEST`
state because output recovery treated the intermediate
`mobilefeed_intro_scene1_final.mp4` as the completed CherryFlash product. This
suppressed the same-day watchdog catch-up and hid six consecutive lost daily
publications behind green session rows.

## User / Business Impact

- The base CherryFlash was absent from Telegram Stories on July 12–17, 2026.
- The scheduled July 18 run started on the same broken bundle and required a
  compensating rerun after repair.
- `@kenigevents` and `@lovekenig` pinned-story history confirms the last real
  CherryFlash was session `#871` on July 11 (`@kenigevents` story `357`,
  `2026-07-11 08:16:38 UTC`; `@lovekenig` story `686`, `08:27:15 UTC`).
- Later daily videos around `16:xx UTC` are a different 34.19-second story
  product, not CherryFlash; no `Видеоанонс #884/#888/#893/#897/#901/#906`
  story exists in the channel history.
- Production rows for sessions `#884`, `#888`, `#893`, `#897`, `#901` and
  `#906` incorrectly said `PUBLISHED_TEST` and stamped `published_at`.

## Detection

- User reported the multi-day absence and repeated daily errors.
- Telegram pinned-story inspection through the approved local E2E human session
  established the public-surface gap.
- Production SQLite, the Kaggle status ledger and the 48-hour rotated Fly log
  mirror established the false-green sequence.

## Timeline

- `2026-07-11 14:47 UTC`: commit `323cb1e4` enabled the approved guide True3D
  subprocess in `render_cherryflash_full.py`.
- `2026-07-12 07:44 UTC`: session `#884` launched with a guide promo; the
  notebook emitted `kernel_exited` in `phase=render`, with no `render_done` or
  `report_written`. Output recovery selected `mobilefeed_intro_scene1_final.mp4`
  and marked the session `PUBLISHED_TEST`.
- `2026-07-13`–`2026-07-17`: sessions `#888`, `#893`, `#897`, `#901`, `#906`
  repeated the same pattern once per day.
- `2026-07-16 08:08 UTC`: rotated logs explicitly recorded Kaggle `ERROR` for
  `#901`, followed by output probing and later a false green row.
- `2026-07-18 07:44 UTC`: broken scheduled session `#912` launched before the
  incident repair.
- `2026-07-18 07:45 UTC`: its uploaded dataset file inventory again omitted
  `scripts/render_cherryflash_guide_true3d_v4.py`.

## Root Cause

1. Commit `323cb1e4` added a hard runtime dependency on
   `scripts/render_cherryflash_guide_true3d_v4.py` for every injected guide
   promo scene.
2. `VideoAnnounceScenario._iter_cherryflash_bundle_files()` was not updated to
   include that new script in the per-session Kaggle dataset.
3. The bundle test asserted several long-standing assets but did not assert the
   transitive renderer dependency introduced by the same change.
4. `poller._find_video()` accepted any largest video whose filename contained
   `final`; the intro approval artifact matched that heuristic even though the
   required `cherryflash_full_final.mp4` did not exist.
5. Once the poller wrote `PUBLISHED_TEST`, the daily watchdog treated the slot
   as delivered and did not compensate.

## Contributing Factors

- Generic Kaggle instrumentation wrote cleanup `done`/100% after an abnormal
  `kernel_exited`, even though no domain `render_done`/`report_written` existed.
- Session state had no durable target-level story receipt, so `published_at`
  meant test-chat delivery rather than confirmed Stories fanout.
- The rotating log mirror was enabled with `48h` retention, enough to prove
  July 16–18 but not the first July 12 failure; SQLite and Telegram history
  were required for the full window.

## Automation Contract

### Treat as regression guard when

- adding or changing any CherryFlash renderer subprocess/import/runtime asset;
- changing `_iter_cherryflash_bundle_files()` or Kaggle dataset validation;
- changing poller output discovery, terminal-error recovery or
  `PUBLISHED_TEST` semantics;
- changing scheduled CherryFlash catch-up/watchdog closure evidence.

### Affected surfaces

- `video_announce/scenario.py::_iter_cherryflash_bundle_files`
- `scripts/render_cherryflash_full.py`
- `scripts/render_cherryflash_guide_true3d_v4.py`
- `video_announce/poller.py` output discovery and terminal recovery
- Kaggle `zigomaro/cherryflash`
- `videoannounce_session`, `kaggle_run_ledger`, `kaggle_run_event`
- Telegram Stories on `@kenigevents`, `@lovekenig` and configured fanout targets

### Mandatory checks before closure or deploy

- bundle inventory test must require the True3D script;
- a CherryFlash output containing only
  `mobilefeed_intro_scene1_final.mp4` must fail and never become
  `PUBLISHED_TEST`;
- focused scenario/poller/renderer tests and `py_compile`;
- clean main-reachable deploy plus `/healthz`, Fly checks, disk and SQLite
  `quick_check`;
- compensating July 18 base CherryFlash run;
- public Telegram verification of the resulting story on `@kenigevents` and
  `@lovekenig`, not only a local session status.

### Required evidence

- rotated-log excerpts for `#901/#906/#912`;
- Telegram pinned-story inventory covering July 11–18;
- production session/event timeline for `#871/#884/#888/#893/#897/#901/#906/#912`;
- deployed SHA/Fly release reachable from `origin/main`;
- successful catch-up dataset/kernel/report plus public story IDs/timestamps.

## Immediate Mitigation

- Prepared a hotfix to ship the missing True3D script in every CherryFlash
  session dataset.
- Tightened output recovery so CherryFlash requires the exact
  `cherryflash_full_final.mp4`; intro-only output is diagnostic and fails closed.

## Corrective Actions

- Add `scripts/render_cherryflash_guide_true3d_v4.py` to the canonical session
  bundle and regression-test its presence.
- Make poller product-aware for CherryFlash final output discovery.
- Preserve provider terminal failure details when the expected product artifact
  is absent.

## Follow-up Actions

- [ ] Persist target-level story receipts independently of test-chat delivery.
- [ ] Reconcile historical false-green sessions `#884/#888/#893/#897/#901/#906`
  so reporting does not count them as viewer-facing exposures.
- [ ] Make domain `render_done/report_written` required before generic cleanup
  can finalize a story-enabled video run as successful.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending
- compensating July 18 catch-up: pending

## Prevention

- Runtime dependencies introduced by a renderer change must be represented in
  the session-bundle manifest and asserted by tests in the same commit.
- Intermediate approval videos can never satisfy a product-final artifact
  contract.
- Scheduled story closure requires public-surface evidence, not a green local
  test-delivery state.
