# Archetype specimen lane results

- Branch: `agent/ui-golden-event-corpus/archetype-specimen`
- Assigned base: `9d95276590342e2746f7f68debf61c59ab07a867`
- Scope: disposable Golden EventCard conformance harness only; production `site/src` was not changed.

## Requirement closure

| Requirement | Status | Evidence |
| --- | --- | --- |
| R07 — parent archetype context | Done | A v2 resolved case mounts the existing production `OptimizedEventCardGrid.astro` with the exact declared Golden input fixtures. The adapter rejects non-controlled placement claims, missing/invented siblings, and mismatched viewport/grid/card geometry. |
| R08 — 3-column/equal-height desktop evidence | Done | The desktop-1280 contract resolves to an 1180px parent, three 380px columns, 20px gaps and `align-items: stretch`. Browser validation records computed CSS grid geometry, output order, each card rect, placement, row mode/ratio, media state and equal-height peers. |
| R10 — selected plus parent capture | Done | `astro.png` remains the selected-card capture; `astro-archetype.png` captures the full grid. Both hashes and the archetype PASS result flow through `astro-capture-receipt.json` into the actual tuple. `ranking_supplied` and `production_route_placement_claimed` are explicitly false. |

## Real pinned-candidate proof

Validated with resolved case `event-card-large-landscape-crop-safe-7906-event-detail-related-desktop` against candidate Astro SHA `22ebe3c5e92b13684cca32c14357ef7b91834977` and immutable corpus `ui-reference-events.v1`.

- Exact inputs: `7906, 8156, 4327, 6628`
- Production packer output: `7906, 8156, 6628, 4327` (controlled layout only; not a ranking/route claim)
- Parent: 1180px; computed columns: `380px 380px 380px`; gaps: 20px
- Selected `7906`: row 0, column 0, width 380px, row ratio `0.83333`, mode `document-led-bounded-cover`
- First-row heights: `683.640625px` for all three peers
- Selected screenshot SHA-256: `db2343f77897780cebfe513a8459d84ae6eed83e3a1c5b40ee13c00558faeb8c`
- Parent screenshot SHA-256: `d0b7ea250b8ad5f59e61c48e13488fed2c70864b91dc55dab8fe7dd47203bcd3`
- All archetype checks: PASS; all 12 fixture assets hash-verified and locally
  bound (including derivative URL fallback); font preflight PASS.

The generated harness, captures and tuple were kept in `/dev/shm` and were not committed.

## Validation

- `node --check` on all changed `.mjs` scripts — PASS
- `node --test tests/ui-conformance-archetype-specimen.test.mjs` — 5/5 PASS
- `node --test tests/ui-surface-placement.test.mjs` — 2 PASS, optional real-corpus test SKIP without env
- `node --test tests/ui-conformance-routing.test.mjs` — PASS after materializing its tracked workflow fixture omitted by the sparse checkout
- Real `materialize-case.mjs` build — PASS, one Astro specimen page
- Real `capture-case.mjs` browser capture — PASS, exact/perceptual stability on both PNGs
- Real `build-actual-tuple.mjs` — PASS; tuple carries selected/parent hashes and archetype validation
- Legacy v1 single-card materialization and capture — PASS; parent fields remain null
- `git diff --name-only -- site/src` — empty
