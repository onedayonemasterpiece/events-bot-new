# Lane static-collections-review Results

## Status
committed

## Requirement IDs
- R01 — provisional overinclusive production seed
- R02 — external critical-analysis dossier
- R03 — committed and pushed branch

## Branch
`agent/static-collections-review/curation`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-collections-analyst-review-20260801`

## Base SHA
`b5df4b261a582824058bba5458d57d4ba1b4a4f3`

## Implementation commit
`eb8cec0812876f1638b3fb153e09ff9d347208b3`

## Files changed
- provisional five-label production review seed and regression test;
- external analyst dossier;
- corrected canonical project and release-plan evidence;
- Unreleased changelog entry.

## Commands run
- read-only Fly SQLite production queries
- static batch inspection
- JSON fixture generation and validation

## Tests / verification
- `python3 -m json.tool tests/fixtures/static_collections_gold_v1.json`
- `PYTHONPATH=/tmp/events-bot-pytest python3 -m pytest --noconftest -q tests/test_static_collection_gold_seed.py tests/test_static_collection_export.py` — `10 passed`
- `git diff --check`

## Risks
- Seed is intentionally recall-oriented and must remain publication-blocked.
- Medieval supply does not yet satisfy the declared positive-family minimum.

## Merge notes
Review branch is a handoff for external analysis; it must not be treated as an approved rollout branch.
