# Lane L1B Results

## Status

committed

## Requirement IDs

- **R01 — Done:** added a versioned typed/materialized work queue with a
  primary-key-prefix access path.
- **R02 — Done:** added a small validated counter/read model; counters are
  aggregated over full producer state, never a truncated work page.
- **R03 — Done:** added generation-first/pointer-last schema and offline cutover
  planning, with CandidateReport publication defaulting to shadow.
- **R04 — Done:** required mode removes broad start-state/orchestrator reads;
  legacy fallback needs two explicit flags and remains under the L1 budget.
- **R05 — Done:** 20k regression coverage proves exact counters, bounded work
  pages, point joins and absence of normal kind-population readers.
- **R06 — Done:** canonical YDB schema and incident regression docs are synced;
  scheduler/catch-up remain disabled and no live action was performed.

## Branch

`agent/static-unified/l1b-ydb-typed-read-model`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-site-unified-20260803/l1b-ydb`

## Base SHA

`67333baf3dd8bb4ea9e0f172f74b9bd32a67761f`

## Head SHA

Implementation commit: `f6007023a`. This RESULTS metadata commit follows it;
integrate the branch tip.

## Files changed

- `.codex/lanes/L1B/LANE_MAP.yml`
- `.codex/lanes/L1B/RESULTS.md`
- `scripts/region_talk_ydb_read_model.py`
- `scripts/region_talk_ydb_read_model_cutover.py`
- `scripts/region_talk_orchestrator.py`
- `scripts/region_talk_scheduled_runner.py`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `kaggle/execute_region_talk_candidate_report.py`
- `tests/test_region_talk_ydb_read_model.py`
- `docs/features/region-talk-channel/ydb-schema.md`
- `docs/reports/incidents/INC-2026-08-03-ydb-request-unit-billing.md`

## Commands run

- Focused five-module Region Talk pytest suite under
  `/opt/venvs/events-bot-modern/bin/pytest`.
- Narrow read-model pytest suite after final counter/overflow changes.
- `python3 -m py_compile` for every changed Python/test module.
- `git diff --check`.
- Explicit generated-report cleanup using `git ls-files --others` allowlisting;
  no runtime report/artifact was committed.

## Tests / verification

- `463 passed, 1 deselected in 20.88s` for read model, L1 cost, orchestrator,
  scheduled runner and CandidateReport suites.
- `10 passed in 1.41s` for the final narrow read-model suite.
- The single deselection is a pre-existing CandidateReport test whose optional
  `openpyxl` dependency is absent from the supplied shared environment; no
  package install was attempted under the lane disk constraint.
- Compilation and whitespace checks passed.
- Forbidden `site/src`, `CHANGELOG.md` and `docs/routes.yml` scopes are untouched.
- No YDB connection/read/write, DDL application, scheduler enablement, catch-up,
  RU throttle change, deploy or production action occurred.

## Risks

1. CandidateReport publishes `shadow` by default. `ready` remains explicitly
   disabled until BGE, ImageDiagnostic and finalizer writer coverage is reviewed
   and a bounded canary measures server-side RU.
2. Old work generations need an approved retention pass before live use.
3. The client cost ledger remains an I/O RU floor; exact CPU/billed RU and a
   billing alert remain incident follow-ups.
4. An active queue above its configured materialization ceiling becomes
   `blocked_overflow`; required readers fail closed rather than undercounting.

## Merge notes

Merge the complete branch. The integration lane owns `CHANGELOG.md`, route
coordination, final combined tests and any later live/canary decision. Do not
enable `REGION_TALK_YDB_READ_MODEL_PUBLISH_READY`, scheduling or YDB throttling
as part of this merge.
