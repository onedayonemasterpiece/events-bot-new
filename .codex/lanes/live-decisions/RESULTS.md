# Lane live-decisions Results

## Status
committed

## Requirement IDs
- R1
- R3
- R6
- R7
- R9
- Final publication idempotency guard

## Branch
`agent/region-talk-live-intake/live-decisions`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/region-talk-live-intake-live-decisions`

## Base SHA
`e404e9fa754704d1f6c9e38946d6afa4a884329f`

## Head SHA
Implementation commit: `c427dae0` (the following lane-results-only commit is reported in the handoff).

## Files changed
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `scripts/region_talk_orchestrator.py`
- `scripts/region_talk_publication_finalizer.py`
- `scripts/region_talk_publication_plan.py`
- `scripts/region_talk_scheduled_runner.py`
- `tests/test_region_talk_candidate_report.py`
- `tests/test_region_talk_orchestrator.py`
- `tests/test_region_talk_publication_finalizer.py`
- `tests/test_region_talk_publication_plan.py`
- `tests/test_region_talk_scheduled_runner.py`
- `.codex/lanes/live-decisions/RESULTS.md`

## Requirement evidence

### R1 — live rereads
- CandidateReport performs a narrow `SnapshotReadOnly`, limit+1-complete intake refresh immediately before the scoring selection cycle and clears the external lane on error/truncation.
- Orchestrator rereads YDB metrics before every executable action selection rather than selecting several actions from one snapshot.
- Per-selection snapshots expose current intake IDs and newly observed intake IDs/counts.

### R3 — normal funnel only
- Clean `review_status=unreviewed` / `publication_permission=not_granted` intake can enter the ordinary web CandidateReport scoring lane only when the exact routing/policy contract is clean.
- Arrival never creates publication/confirmation status.
- Explicit `manual_review_required` is excluded from projection and cannot reach Gemini confirmation in the finalizer.
- Request/input hash/evidence URL/time/title/authors/identity provenance survives CandidateReport and finalizer allowlists.

### R6 — prepared release identity
- Future prepared/reviewed slots are frozen by candidate identity.
- Late higher-scored intake is deferred to an unprepared cycle rather than silently replacing the slot.
- Changed external intake/operator review revision preserves the prepared identity but changes the slot to audited `manual_review_required`.
- Planner rereads the publication/schedule/intake revision immediately before writes and defers with zero writes if it changed.
- Scheduled runner does not recalculate the plan after failed/deferred reaction synchronization, preserving the existing durable plan.

### R7 — fail closed
- Decision-critical intake truncation/read/auth errors fail closed.
- Finalizer strongly rereads current image/memory/publication/source/intake evidence after the mocked/real provider work.
- A read/truncation failure raises `FinalDecisionRefreshError` and writes nothing.
- A successful reread finding changed/missing/manual evidence writes only explicit review/deferred state, never the stale acceptance; already target-published rows remain immutable.

### R9 — LLM-owned semantics
- New deterministic code is limited to exact workflow-state routing, normalization, fingerprinting, dedupe/identity preservation, completeness checks, and safety fences.
- No keyword/regex semantic promotion was added; ordinary scoring and final Gemini remain authoritative.

### Incident regression contract
- Raised `INC-2026-08-01-region-talk-empty-reaction-sync` because the scheduled runner/planner surface is affected.
- Added regression coverage proving a failed reaction sync does not launch/recalculate the publication plan.
- This worker did not deploy or run production reaction sync; integration/release owner must retain the incident record's clean-main deploy, live sync, planner receipt, and full-suite gates.

## Commands run
- `python3 -m py_compile kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py scripts/region_talk_orchestrator.py scripts/region_talk_publication_finalizer.py scripts/region_talk_publication_plan.py scripts/region_talk_review_queue.py scripts/region_talk_scheduled_runner.py`
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_region_talk_candidate_report.py tests/test_region_talk_orchestrator.py tests/test_region_talk_publication_finalizer.py tests/test_region_talk_publication_plan.py tests/test_region_talk_review_queue.py tests/test_region_talk_scheduled_runner.py`
- Targeted finalizer refresh selectors after the last audit adjustment.
- `git diff --check`

## Tests / verification
- Owned focused suite: **506 passed in 22.60s**.
- Last finalizer audit selectors: **2 passed in 0.28s**.
- `py_compile`: passed.
- `git diff --check`: passed.
- Generated root-level report artifacts from CandidateReport tests were deleted and not committed.

## Risks
- No live YDB/Kaggle/Telegram/production operation was performed in this worker lane.
- Canonical docs and `CHANGELOG.md` were forbidden to this lane and remain the integrator's responsibility.
- The incident record requires a full Region Talk suite and production reaction-sync/planner receipts before deploy/closure; this lane provides focused local regression evidence only.
- Intake compatibility is tolerant of the parallel lane's aliases, but integration must inspect the merged importer contract and rerun the focused suite.

## Merge notes
- Cherry-pick implementation commit `c427dae0` and the subsequent RESULTS-only commit.
- Merge after the intake-ledger lane, then rerun the owned suite against the combined importer contract.
- Update canonical Region Talk docs and `[Unreleased]` changelog on the integration branch.
