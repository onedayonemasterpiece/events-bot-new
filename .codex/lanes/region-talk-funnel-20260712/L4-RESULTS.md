# Lane L4 Results

## Status
committed

## Requirement IDs
- R12

## Branch
agent/region-talk/bge-m3-enrichment-test

## Worktree
/home/dev/projects/events-bot-new-region-talk-bge-m3-test

## Base SHA
0e8f5e3be2a561bfa73deb55ac125ce01379c607

## Head SHA
23830691

## Files changed
- Region Talk CandidateReport, ImageDiagnostic, orchestrator/finalizer/notifier helpers
- Region Talk canonical docs and CHANGELOG
- Region Talk tests

## Commands run
- 17 controlled Candidate/BGE/Image/finalizer orchestration cycles in the current funnel audit series
- dry-run and execute state maintenance
- live Gemini finalization under the durable 100-call budget
- local E2E notifier to the pinned operator chat
- YDB root/table storage inspection with `yc` + `ydb`

## Tests / verification
- 299 Region Talk unittest cases passed after the final code change.
- Four false terminal `#media` rows were reopened; live ImageDiagnostic changed actual-scored photos 26 -> 30.
- `https://t.me/umka_blog/2118` was accepted by Gemini and delivered as Telegram message 31960.
- `https://t.me/krasivoorussia/6168` reached Gemini through the calibrated near-threshold visual lane; Gemini returned needs-review because the text was encyclopedic rather than firsthand.
- BGE-ready backlog changed 34 -> 25 at the start of the first local-rescore run, proving eight YDB-text rescoring rows progressed without Telegram requests.
- Full YDB root is approximately 37 MB; Region Talk compact table is 36.7 MB, down from the previously observed 690.19 MB database state.

## Risks
- This branch remains substantially diverged from `origin/main`; production integration needs a dedicated reconciliation, not a blind merge.
- Roughly 88% of source rows still have no scan evidence.
- Twenty-five historical BGE-ready exact posts remain; rows retaining active text now progress in batches of eight, older compacted rows retain one bounded human-like refetch slot.

## Merge notes
All changes were committed and pushed on the existing Region Talk integration branch. The live cycle launched at 2026-07-12T14:05Z was still running when this result file was drafted; final metrics are recorded in the parent response/artifacts.
