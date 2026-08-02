# Lane static-collections-pr-a Results

## Status
committed

## Requirement IDs
- R01 semantic rows 4648/6871 and festival parent/child/extraction scope
- R02 role-specific repo SHAs, exact command/timestamps and snapshot serialization
- R03 seed/index/receipt/required-set/source-ref validator invariants
- R04 full/excerpt quote kind and explicit truncation metadata
- R05 named semantic and validator regression tests
- R06 PR-A-only boundary; no owner gold/scores/thresholds/Astro promotion
- R07 review PASS with warnings; strict expected FAIL until PR B

## Branch
`agent/static-collections-quality/pr-a-ontology`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-collections-analyst-review-20260801`

## Base SHA
`f1a732fce6003dc24bcf38b012072951be85e5e6` (existing integrated PR-A draft)

## Head SHA
`6dec4628c3291e9433291ed13fdf9cb912b385a0`

## Files changed
Review seed/receipts/index, deterministic source binder, validator/workflow,
regression tests, canonical docs and changelog. No PR-B or Astro implementation.

## Commands run
- read-only Fly SQLite schema and Event/EventSource probes
- canonical 126-event/389-EventSource evidence export validation
- `python3 scripts/validate_static_collections_quality.py --mode review ...`
- expected-fail `--mode strict`
- all `tests/test_static_collection_*.py`
- Node E2E checker behavior tests

## Tests / verification
Review gate PASS with zero errors and 11 warnings (six positive-supply
shortfalls plus five pending PR-B bindings). Strict FAIL is expected because
owner gold, scores, winners and frozen hashes do not exist. Semantic publication
stays blocked.

## Risks
Six labels remain below 15 independent positive families. Five receipts have
`needs_source_review` status. Owner gold, all-event scores, thresholds, winning
prototypes and browser candidate are deliberately absent.

## Merge notes
This is PR A only. Do not promote routes or weaken warnings in merge.
