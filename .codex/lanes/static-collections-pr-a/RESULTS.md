# Lane static-collections-pr-a Results

## Status
committed

## Requirement IDs
- R01 move provisional seed and delete misleading legacy gold fixture
- R02 ontology policy v2
- R03 production source recheck for named defects
- R04 source provenance and occurrence-family repair
- R05 fail-closed contract CI; no public routes

## Branch
`agent/static-collections-quality/pr-a-ontology`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-collections-analyst-review-20260801`

## Base SHA
`0131dc384aadf364f81089e41aacd086d114e3a1` plus merge of current `origin/main@4d5a8f3592b2b4808b3885b8dd07ea4c31fbdc36`

## Head SHA
`e2073976ef1d0f435ac3716a257e7bb4ab2e13d3`

## Files changed
Review data/receipts, ontology v2, validator/workflow/tests, canonical docs,
route-state contract and changelog. No Astro public route implementation.

## Commands run
- read-only Fly SQLite schema and Event/EventSource probes
- `python3 scripts/validate_static_collections_quality.py --mode review ...`
- three targeted unittest suites
- Node E2E checker behavior tests

## Tests / verification
Review gate PASS with zero errors. Short family supply and missing PR-B artifacts
remain explicit warnings. Semantic publication stays blocked.

## Risks
Five labels remain below 15 independent positive families. Four source-review
receipts still require canonical production follow-up. Owner gold, all-event
scores, winning prototypes and browser candidate are deliberately absent.

## Merge notes
This is PR A only. Do not promote routes or weaken warnings in merge.
