# Static event compact rows + occurrence review — 2026-07-21

Base: `origin/main@71a9cb1e5b78c1e85098cfb70862daa1f07bd28f`

## Requirements

- **R01** — every non-final desktop recommendation row is full; only the last
  row in a section may contain fewer than three cards.
- **R02** — cards and media are equal-height inside their row, use no image
  fields, and use intrinsic row-local chrome instead of the rejected global
  `184px + 58px + 56px` reservation.
- **R03** — reciprocal explicit occurrence families collapse on family-based
  surfaces and show the complete compact label; no title/type/venue inference.
- **R04** — publish a new isolated `preview-*` noindex prefix and present only
  event `6408` for owner visual acceptance. Do not mutate stable root, stable
  ICS, the current `/_review` pointer, or request an all-pages production build.
- **R05** — preserve the OCR ≤20% crop and no-fields contracts, run occurrence,
  Astro/generated-output/browser gates and the
  `INC-2026-07-18-dramteatr-same-day-event-glue` regression.
- **R06** — update canonical docs, `CHANGELOG.md`, and the active static-event
  incident. Full rollout remains blocked until explicit owner visual approval.

## Execution map

| Lane | Type | Requirements | Ownership / result |
|---|---|---|---|
| row-packing audit | read-only | R01, R02, R05 | Root cause and gate/doc map returned; no writes. |
| occurrence audit | read-only | R03, R05 | Resolver is present; `6318`/`6586` lack reciprocal exported links. |
| focused review audit | read-only | R04 | Use unique legacy `preview-*` prefix; never run production request/publisher. |
| integration | serial write | R01–R06 | This worktree only; implementation, tests, docs, preview publication. |

## Status at focused handoff

- R01, R02, R03, R04 and R05: done for the isolated `6408` preview; generated
  rows are `3,3,3,1`, Romeo is one explicit-family card, all gates are green,
  and stable root/current-review/stable ICS were not changed.
- R06: partial by design. Canonical docs/changelog/incident are synchronized,
  but canonical DB repair and all-page generation remain blocked on explicit
  owner visual approval of the focused URL.

## Integration order

1. Enforce full rows plus at most one final remainder in the shared packer.
2. Restore intrinsic row-local card chrome on both static and hydrated surfaces.
3. Add explicit reciprocal review-fixture data for `6318`/`6586` and regression
   coverage without changing the family inference contract.
4. Build, gate, inspect event `6408`, then upload only a fresh isolated preview
   namespace and wait for owner acceptance before any broader rollout.
