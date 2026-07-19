# Lane INT Results

## Status
committed

## Requirement IDs
- R01
- R02
- R03
- R04
- R05
- R06
- R07
- R08

## Branch
`integration/region-talk-publications-20260719`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/region-talk-publications-20260719`

## Base SHA
`dff55929923f3e55e2ce8fe188b19b530ec56088`

## Head SHA
Implementation commit: `ffd794c5e1c556d0389bf90a73bf8616d3b4747a`

## Files changed
- Versioned external research Schema and Region Talk concept/prompt documentation.
- Schema-validating, idempotent YDB staging importer.
- Read-only compatible-BGE MMR/adjacency queue helper.
- Existing operator notifier and Region Talk canonical docs/tests/dependencies.

## Commands run
- Focused pytest with the Region Talk virtualenv and `--noconftest`.
- Full `tests/test_region_talk*.py` regression suite with `--noconftest`.
- JSON Schema Draft 2020-12 meta-schema check.
- Importer CLI dry run using a conforming fixture.
- `docs/routes.yml` YAML parse, Python compileall, notifier `--help`, and `git diff --check`.

## Tests / verification
- Focused final: `33 passed`.
- Full Region Talk suite before final formatter-only delta: `549 passed in 70.25s`; the final focused suite covers all files changed after that run.
- Importer dry run: one valid input, zero rejected, two planned staging rows, zero production writes.
- Normal pytest collection in the minimal Region Talk virtualenv is unavailable because that environment does not contain `aiogram`; `--noconftest` is the established isolated suite invocation here.

## Risks
- `--queue` is an explicit local operator request surface, not an inbound Telegram bot command.
- No live external research-model acceptance run or live operator-chat send was performed.
- Automatic staging promotion remains fail-closed until the editorial/academic semantic bank, source attestation, final verifier, rights propagation, and CandidateReport consumer exist.
- The Region Talk base branch is not in `origin/main`; at verification time `origin/main...base` was `1046 388`, so reconciliation must be intentional.

## Merge notes
- No read-only planner lane edited files.
- All implementation changes are contained in `ffd794c5e1c556d0389bf90a73bf8616d3b4747a`.
- Do not deploy or merge this branch directly over current `origin/main`; reconcile the existing Region Talk branch history first.
