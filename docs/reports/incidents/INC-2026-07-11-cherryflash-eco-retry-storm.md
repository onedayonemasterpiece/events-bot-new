# INC-2026-07-11 CherryFlash Eco Render Retry Storm

Status: monitoring
Severity: sev2
Service: CherryFlash partner track `partner_eco_nature_001` / Kaggle / scheduler watchdog
Opened: 2026-07-11
Closed: —
Owners: operations / video announce runtime
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce.md`, `INC-2026-07-08-prod-root-overlay-disk-full.md`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/cron.md`, `docs/features/kaggle-status-framework/README.md`

## Summary

The Eco/Nature CherryFlash partner slot repeatedly launched a new Kaggle run
approximately every ten minutes. Sessions `#872` and `#874`–`#880` all failed
before rendering a video, while the persisted partner watchdog continued to
consider the daily slot missing and retried until its 22:00 local deadline.

The actual first failure was not the visible LLM warning and not `missing video
output`. Kaggle failed while building the intro because the runtime selection
manifest was unavailable/empty at the renderer boundary; the renderer silently
switched to an obsolete static April design fixture and then raised
`FileNotFoundError` for fixture poster `event_id=3292` / `3292.jpg`.
`missing video output` was the downstream symptom after that render failure.

## User / Business Impact

- The Eco/Nature partner story for 2026-07-11 was not published.
- At least eight failed Eco/Nature sessions were created in production.
- Operators received repeated session, missing-output and provider-fallback notifications.
- Repeated Kaggle runs consumed quota and left several small `/tmp/videoannounce-*`
  result directories. Other CherryFlash tracks continued to work: KОНБ session
  `#873` reached `PUBLISHED_TEST` on the second video lane.

## Detection

- Operator reported the repeated CherryFlash errors from the bot Telegram UI.
- Production `videoannounce_session`, `ops_run`, Kaggle output and status-ledger
  evidence confirmed the loop and renderer exception.

## Timeline

- `2026-07-11 10:34 UTC`: initial Eco session `#872` created and later failed.
- `10:37 UTC`: KОНБ session `#873` launched on lane 1 and later succeeded.
- `10:41`–`11:42 UTC`: Eco sessions `#874`–`#880` launched at roughly ten-minute
  watchdog intervals and each failed in about 2.5 minutes.
- `11:44 UTC`: session `#880` logged `FileNotFoundError: CherryFlash poster
  asset is missing event_id=3292 file_name=3292.jpg`.
- `11:47 UTC`: another watchdog `ops_run #3578` started before containment.
- `2026-07-11`: temporary containment moved
  `V_PARTNER_TRACK_ECO_TIME_LOCAL` to `23:59`, preventing further same-day
  watchdog launches while the code fix was prepared.

## Root Cause

Two gaps combined into the incident:

1. The production intro renderer treated the derived
   `assets/cherryflash_selection.json` as its only live selection input. When
   that file was unavailable or empty at the runtime boundary, it silently
   loaded a local design-time April fixture rather than reconstructing the
   selection from required root-level `payload.json` or failing closed. The
   fixture referenced `3292.jpg`, which is not part of a live session bundle.
2. The partner-track watchdog had a time deadline but no persisted failure
   count. Every terminal `FAILED` session reopened the daily slot, so the
   ten-minute watchdog launched another deterministic failure until 22:00.

The exact reason the derived manifest was unavailable in the deleted private
session dataset can no longer be inspected: after terminal handling its dataset
API returns `403`. This does not block prevention because `payload.json` is the
canonical required render input and now supplies the recovery path.

## Contributing Factors

- `ops_run` recorded successful Kaggle handoff while the associated session
  later became `FAILED`; green handoff rows did not close the slot.
- Kaggle cleanup recorded `status=done` for resource release even when the
  notebook exited during render; those rows had no `terminal_at` and correctly
  did not close the scheduler slot, but the label was confusing.
- Gemma `500 INTERNAL` warnings from `video_partner_filter` were noisy but not
  causal: Gemini Flash-Lite fallback continued selection and reached Kaggle.
- Runtime file logging was intentionally disabled; evidence came from SQLite,
  Kaggle output and the Telegram transcript.

## Incident Control Block

- **Incident ID:** `INC-2026-07-11-cherryflash-eco-retry-storm`
- **Current status:** monitoring after containment; code fix pending release evidence.
- **Affected surfaces:** partner watchdog, CherryFlash session bundle and intro
  selection loader, Kaggle status/output handling, Fly `/tmp`.
- **Target behavior:** one scheduled attempt plus at most one persisted recovery
  attempt; live renderer uses session selection/canonical payload, never a fixture.
- **Mandatory checks:** watchdog-cap, payload-recovery and bundle-manifest tests;
  Kaggle status regressions; disk/health/DB checks; one deliberate Eco catch-up.
- **Release evidence to collect:** clean SHA reachable from `origin/main`, Fly
  version/image, post-deploy health, no retry storm, terminal catch-up evidence.
- **Follow-up actions:** clarify failed-kernel terminal semantics and add one
  operator alert when the persisted retry cap is reached.

## Automation Contract

### Treat as regression guard when

- changing partner-track schedules, startup catch-up or watchdog retry logic;
- changing CherryFlash dataset contents, selection manifest or `payload.json`;
- changing intro poster resolution or design-fixture fallback behavior;
- changing Kaggle terminal/status cleanup semantics.

### Affected surfaces

- `scheduling.py::maybe_dispatch_partner_track_watchdog`;
- `video_announce/scenario.py::_create_cherryflash_dataset`;
- `scripts/render_mobilefeed_intro_still.py`;
- `kaggle/CherryFlash/cherryflash.ipynb`;
- `videoannounce_session`, `ops_run`, `kaggle_run_ledger` and Fly `/tmp`.

### Mandatory checks before closure or deploy

- `python3 -m py_compile scheduling.py video_announce/scenario.py scripts/render_mobilefeed_intro_still.py`.
- Watchdog retries after one matching failure and stops after two persisted failures.
- With valid `payload.json` and no derived manifest, renderer reconstructs the
  selected event/poster and never references fixture event `3292`.
- Dataset build writes matching root and `assets/` manifests and rejects zero primary scenes.
- Run callback/ledger/resource-lease tests from `INC-2026-06-13-kaggle-duplicate-videoannounce`.
- Verify `/healthz`, `PRAGMA quick_check`, `ENABLE_RUNTIME_FILE_LOGGING=0`, disk
  free space and `/tmp` writes.
- Verify no repeated new Eco sessions after the cap.
- Perform one compensating Eco catch-up and verify terminal publication state.

### Required evidence

- session/ops timeline for `#872`–`#880` and Kaggle traceback for `3292.jpg`;
- focused regression-test output;
- before/after production session counts showing the storm stopped;
- deployed SHA/Fly release/health/disk/DB evidence;
- successful or explicitly blocked same-day Eco catch-up evidence.

## Immediate Mitigation

- Set Eco schedule override to `23:59` during repair so the watchdog cannot launch again.
- Confirmed production healthy, SQLite `quick_check=ok`, root overlay about
  `4.6G` free and `/data` about `247M` free.

## Corrective Actions

- Added a persisted cap of two failed partner sessions per profile/date.
- Added `payload.json` recovery for missing/empty derived selection manifests
  and prohibited live `CHERRYFLASH_ROOT` runs from silently using the fixture.
- Added a redundant root-level selection manifest and fail-fast empty-selection validation.
- Added focused retry-cap, payload-recovery and bundle-manifest tests.

## Follow-up Actions

- [ ] Persist a clear terminal `failed` ledger state for notebook failure rather
  than cleanup `done` without `terminal_at`.
- [ ] Emit one bounded operator alert when a partner retry cap is reached.
- [ ] Remove temporary `V_PARTNER_TRACK_ECO_TIME_LOCAL=23:59` after fixed release.

## Release And Closure Evidence

- temporary containment: Fly secret `V_PARTNER_TRACK_ECO_TIME_LOCAL=23:59`.
- code release: pending.
- compensating catch-up: pending.

## Prevention

- Production renderers derive from canonical session payloads and never silently
  substitute design fixtures.
- Scheduled retry loops require a persisted per-slot failure budget; a wall
  clock deadline alone is not sufficient containment.
