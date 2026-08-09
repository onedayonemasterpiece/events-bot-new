# packet-semantics-fix — RESULTS

## Identity

- Lane: `packet-semantics-fix`
- Base SHA: `05921d84d6ef77a9c46c923820990f042805f2c0`
- Implementation head SHA: `22769dc30`
- Exact decoded source SHA: `ef7aa62e45c60f7a12da6160f490719c0721ec03`
- Branch: `agent/current-ui-behavioral-v1-1/packet-semantics-fix`
- Foundation note: work began before PR #444 was merged; the assigned base already contains the required harness-copy commit. No rebase was performed per integrator instruction.

## Outcome

Corrected registry: **67 plans / 57 executable / 10 exact blockers / 124 planned rasters**. All records remain `NOT_MERGED`, production-state claims remain false, and normalization remains forbidden.

- All 13 `DYNAMIC_REGIONS` IDs have executable runtime packet refs or exact blockers.
- All 293 exact breakpoint/container probe IDs have explicit refs. The aggregate gap record carries every exact ID and `blocks_ready:true`; it is not an execution-coverage claim.
- `rail-keyboard-home-end` is an exact `blocks_ready:true` blocker: the actual focusable `.rail-window` remained at `scrollLeft=0` after native Chromium `End`; no scripted scroll was substituted.
- Three transport disclosures use exact components and a bounded fixture made only from trips returned by the exact resolver across controlled `PreviewEvent.start_time` variants. Browser smoke showed real closed/open geometry and six departures in all three treatments.
- Weekend sticky selects the visible desktop implementation at `1280×800`; all three phases have nonzero geometry and real scroll transitions.
- Actions require an existing visible target (or an explicit not-applicable controlled-runtime contract), and action phases require an observable DOM/geometry/scroll/focus delta.
- Plans and observations carry reachability, dynamic-region, breakpoint-probe and coverage refs. Media packets carry OCR/photo provenance.
- Added bounded per-plan logging, 4 s font settle, 20 s controlled route hold and 30 s screenshot budgets. Deferred static-CDN loading holds image resources only, removing the cancelled-run font deadlock.
- Added real controlled Weather and ListingEventCard wrappers and bounded dynamic packets for discovery, home rerank, popular personalized shelf, Weather and Exhibitions.

## Evidence and commands

PASS:

- `node --check scripts/current_ui_resource_graph/v1/behavioral/{registry,harness,capture,validate}.mjs`
- Registry deep validation via `assertBehaviorPacketRegistry`: `67/57/10/124`, 13 dynamic IDs, 293 breakpoint IDs, readiness blockers = rail keyboard + breakpoint matrix.
- `/home/dev/.codex/venvs/events-bot-new/bin/pytest -q tests/test_current_ui_behavioral_packet_semantics.py` → `2 passed`.
- Disposable exact-source Astro harness build → `459 page(s) built`, `build.ok=true`.
- Browser smoke, native Playwright/perceptual capture:
  - transport disclosures + `media-broken`: `8 observations`, `0 blockers`, all plans PASS;
  - corrected weekend sticky: `3 observations`, `0 blockers`, PASS;
  - Weather ready/unavailable: PASS;
  - Exhibitions load/loaded/close, error and no-preference motion + ListingEventCard media loaded/error: `12 observations`, `0 blockers`, PASS;
  - menu Tab/Shift+Tab and live resize: PASS;
  - home rerank and popular hidden/visible-five: PASS.
- `git diff --check` → PASS.

Expected integration failures (forbidden files were intentionally not edited):

- `tests/test_current_ui_behavioral_decoder.py`: `4 passed, 2 failed` because the legacy test hard-codes `50/99/5` and builds fake observations without the new required reachability/coverage/transition/font receipts. Integrator must update that existing test together with materializer/workflow count propagation.

## Human visual inspection

Opened at original resolution and checked manually (not hashes only):

- all six transport closed/open rasters: departures are visibly added in every treatment;
- weekend sticky static/pinned/group-collision rasters: correct desktop nav remains visible;
- Exhibitions loading/loaded/error rasters: distinct skeleton, poster and failure message;
- Weather ready raster: two locations, temperatures, water temperature and Open-Meteo provenance;
- `media-broken` loading/error and ListingEventCard loading/loaded rasters: bounded capture and distinct loaded photo state.

Smoke artifacts are ignored and remain under:
`artifacts/codex/current-ui-behavioral-v1-1/packet-semantics-smoke-20260809a/`.

## Risks / follow-up owned by integrator

1. Full 67-plan/124-raster capture was not run in this lane; the integrator requested running it after cherry-pick and materializer reconciliation.
2. The 293-row breakpoint/container gap and native rail keyboard gap intentionally block readiness.
3. Existing `materialize.mjs`, `behavioral.mjs`, legacy tests and workflow still contain old count/schema assumptions; they were outside this lane and must be updated in integration.
4. Full-resolution human review of all 124 final rasters and independent audit are still required. Targeted smoke review does not authorize READY or normalization.

## Changed files

- `CHANGELOG.md`
- `docs/features/static-site-pages/current-ui-resource-graph.md`
- `scripts/current_ui_resource_graph/v1/behavioral/registry.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/harness.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/capture.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/validate.mjs`
- `tests/test_current_ui_behavioral_packet_semantics.py`
- `.codex/lanes/packet-semantics-fix/RESULTS.md`
