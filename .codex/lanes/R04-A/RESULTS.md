# Lane R04-A Results

## Status
committed

## Requirement IDs
- R01 — current FloodWait/cooldown audit
- R02 — persistent local-source leakage regression
- R03 — fail-closed keyword research harness
- R04 — cached-peer live A/B canaries
- R05 — production decision, implementation, documentation and validation

## Branch
`agent/region-talk/R04-A`

## Worktree
`/home/dev/projects/events-bot-new-region-talk-bge-m3-test`

## Base SHA
`2de9194753318f515eab7d1bfa327325a3484ecf`

## Head SHA
Filled by the integration commit containing this file.

## Files changed
- CandidateReport fast-check/source-locality implementation and tests
- Orchestrator environment contract
- Standalone research harness and tests
- Canonical Region Talk discovery/publication/risk documentation
- `CHANGELOG.md`

## Commands run
- `python3 -m py_compile ...`
- `uv run --with openpyxl python tests/test_region_talk_candidate_report.py`
- `uv run --with openpyxl python tests/test_region_talk_orchestrator.py`
- `python3 tests/test_region_talk_low_frequency_research.py`
- two live cached-peer canaries using DISCOVERY1/DISCOVERY2
- portable analytical report validation/package

## Tests / verification
- CandidateReport: 219 tests passed.
- Orchestrator: 65 tests passed.
- Research harness: 14 tests passed.
- Live reads: 70/70 RPC, zero resolve/error/timeout/FloodWait.
- Research report packaged with structural-only browser QA after the installed
  Chromium reader remained in fallback state; canonical artifact validation,
  payload equality and semantic fallback checks passed.

## Risks
- Positive-control evidence covers three confirmed sources and must be checked
  against downstream E5+BGE/media/Gemini conversion in a production run.
- Eighteen confirmed Telegram sources still need their one-at-a-time safe
  cache admission; this was scheduler starvation, not an active FloodWait.
- Numerous pre-existing untracked report outputs remain untouched.

## Merge notes
- `cea7b941` (R04-B) was cherry-picked and reviewed before this integration.
- Artifacts under `artifacts/` are intentionally not committed.
