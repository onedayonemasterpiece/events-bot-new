# Lane static-site-single-kaggle-contract Results

## Status

committed

## Requirement IDs

- R01 — one Kaggle-to-bucket rail
- R02 — same process and bucket for preview/production
- R03 — preview-only page-class slicing
- R04 — final slug/prefix is the mode boundary

## Branch

`agent/static-site-single-kaggle-contract`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-site-single-kaggle-contract`

## Base SHA

`0d73428dfafff2fd5450b74fd68e7bb40e92d2c5`

## Head SHA

Recorded by the commit that contains this file.

## Files changed

- Kaggle runner/kernel page-class and publication handoff
- trusted create-only preview publisher
- Astro route-class filter and focused check
- canonical operations/feature docs, env example, routes and changelog
- focused Python/Node regression tests

## Commands run

- Python compile for runner, kernel and release publisher
- `pytest` static-site handoff/release suites
- Node page-class, PWA and static-release suites
- focused `date` page-class Astro build/check
- `git diff --check`

## Tests / verification

- 69 Python handoff/release tests passed
- 11 combined Node page-class/PWA tests passed
- 18 Node static-release tests passed
- focused `date` build: 31 pages, 352 files, 32 MiB, 20.21 s

## Risks

- A real Kaggle build/publication is intentionally not run from uncommitted source.
- Production root activation remains governed by its existing atomic promotion gate.

## Merge notes

Cherry-pick the lane commit onto the current integration candidate, then use
that exact pushed SHA for the first Kaggle page-class preview transaction.
