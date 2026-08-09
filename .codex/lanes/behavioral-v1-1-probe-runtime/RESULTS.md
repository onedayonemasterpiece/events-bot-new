# Lane behavioral-v1-1-probe-runtime Results

## Status
committed

## Requirement IDs
- Exact-source breakpoint/container source parser and Playwright executor
- Exact 293 terminal probe closure across 32 source paths
- Marker-only negative validation and terminal schema
- Bounded mismatch/ambiguous raster index
- Exact 390x844 event-6767 rail keyboard packet
- Incremental append-only closure CLI/API
- Focused tests and deterministic two-run evidence

## Branch
`agent/current-ui-behavioral-v1-1/probe-runtime-closure`

## Worktree
`behavioral-v1-1-probe-runtime` linked worktree

## Base SHA
`1f449af361e586da509d0199cfe059d620fb42d6`

## Head SHA
The commit containing this result file is the lane head reported in the handoff.

## Files changed
- `scripts/current_ui_resource_graph/v1/behavioral/breakpoint-source.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/breakpoint-plan.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/breakpoint-runtime.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/probe-validate.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/rail-keyboard.mjs`
- `scripts/current_ui_resource_graph/v1/behavioral/probe-runtime.mjs`
- `tests/test_current_ui_behavioral_breakpoint_probes.py`
- `.codex/lanes/behavioral-v1-1-probe-runtime/RESULTS.md`

No production `site/src`, `site/public`, Astro, CSS, JS, tokens, Penpot, workflow, documentation, or changelog files were changed.

## Implementation summary
- Preserves all 293 matrix IDs and enriches them from the exact `ef7aa62` source with condition axes/features, offsets/ordinals, source hashes/fingerprints, selectors, and declarations.
- Plans width, height, combined-query controls, reduced-motion, hover/pointer, and named container content-box environments.
- Executes real Chromium consumers with native media matching, exact compiled CSSOM fingerprints, affected selector resolution, target-specific cascade reconciliation, geometry/styles/overflow/visibility evidence, and explicit terminal reasons.
- Holds image requests only when the exact affected selector requires `[data-image-state="loading"]`; this pins the source-authored transient state without mutating the DOM.
- Rejects marker-only PASS and validates only `PASS`, `MISMATCH`, or `UNREACHABLE_WITH_REASON`.
- Selects only bounded unique mismatch/ambiguous rasters and writes an indexed raster ledger.
- Captures the ordinary `div.rail-window[tabindex=0]` via Tab/Shift+Tab only, real Space/Enter like toggles, native ArrowLeft/Right start/middle/end/boundaries, non-required Home/End observations, link skip, and the drag-only hidden negative-control gap.
- Supports append-only closure over a hash-verified immutable reviewed supplement, without regenerating its prior 124 rasters.

## Exact full-run evidence
Two sequential full Chromium runs completed with the same 293 terminal identities and classifications:

- terminal: **293/293**
- PASS: **236**
- MISMATCH: **39**
- UNREACHABLE_WITH_REASON: **18**
- source paths: **32**
- media/container: **272 / 21**
- numeric/nonnumeric: **273 / 20**
- planned/unconfirmed: **0**
- `blocks_ready`: **false** (terminal mismatches/unreachable are nonblocking evidence dispositions)
- bounded breakpoint rasters: **8** (limit 12), all indexed

The two exact-run record digests were `5ef0d1a05fd2e81521b72beee6a59e59de0018b8852fb6f1b6026c2c6482c669` and `de413d820536fd671fbae544198455b82c134eb53c0c0eee5043b3aaaa7eb907`. Their only raw variability was two aggregate affected-selector counts and one PNG byte/SHA value; there were zero differences in ID, terminal status, expected branch, actual branch, source proof, or terminal reason. The semantic terminal digest (explicitly excluding raster bytes/SHA and aggregate target counts while retaining selector-existence proof) is identical for both: `22ba86265b7fd64fc2bc86f3811a171bc745ed1b1086313c55fce7bbb8371200`.

Selected breakpoint raster IDs:
- `breakpoint.017c006b1fb4a73e`
- `breakpoint.092a8f35e91d132c`
- `breakpoint.18e09a553f004c97`
- `breakpoint.29a74ab638026ed1`
- `breakpoint.4bf84601f6f0d7c0`
- `breakpoint.4ded4f210c76bdbd`
- `breakpoint.59de3e4fb92b6f11`
- `breakpoint.657255de26bb8525`

## Rail evidence
Two repeated rail runs were byte-identical, including both selected full-resolution rasters:

- packet SHA-256: `5aaef129d6b0bfffcce6d1e2f971f4a2f83dae9629dd200236b0f071b50a10f0`
- rail-focus raster: 28,585 bytes; SHA-256 `abd87684f330b06530efdb8aafed97b81b282c701b9e13c80dff9f88c243c8b4`; dHash `0282828292828202`
- like-focus raster: 47,859 bytes; SHA-256 `6f474ec7e0d36b674ba85a3a29856bdbf76f6d01c260a57cb02915c170be1900`; dHash `4040808180c050c0`
- selected/viewed: **2/2** rail rasters
- observed sequential order: rail -> like (display-contents link skipped)
- like: Space toggled false -> true; Enter toggled true -> false
- native rail scroll: start 0, middle 320 -> 360, end/max 895; both boundaries held
- Home/End: no movement, non-required/nonblocking
- not-interested control: hidden, `tabIndex=-1`, `display:none`, no visible sequential-focusable equivalent; nonblocking evidence-complete conformance gap

## Commands run
- Node syntax checks for all six new runtime/parser/validator modules
- Focused pytest for the new probe suite and existing behavioral decoder suite
- Exact source matrix decode against the detached `ef7aa62` source
- Representative and full exact Chromium runs against the copied exact harness
- Two repeated rail-only Chromium runs
- A/B terminal and semantic digest comparisons
- sensitive-value scan
- `git diff --check`

## Tests / verification
- `14 passed in 28.81s` for the combined focused pytest invocation
- full closure validator: 293 unique terminal rows, 32 source paths
- marker-only PASS rejected
- two-run terminal semantic digest identical
- rail packet and two PNGs byte-identical across two runs
- no sensitive values, full HTML, or full URLs retained

## Risks
- The 39 MISMATCH and 18 UNREACHABLE_WITH_REASON rows remain explicit nonblocking conformance/coverage findings; they were not converted to PASS or hidden.
- Eight newly selected breakpoint rasters are indexed for later manual review; the two fresh rail rasters were visually opened and reviewed.
- No production UI remediation or experiment decision is included in this lane.

## Merge notes
Cherry-pick the lane head. The integrator owns workflow/docs/CHANGELOG wiring and any artifact materialization/review ledger integration.
