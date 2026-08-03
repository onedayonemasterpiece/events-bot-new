# Lane L1C Results

## Status

committed

## Requirement IDs

- **R01 — Done:** CandidateReport projection now uses the exact shared typed
  work classification and status aliases for emitted rows and expected counts.
- **R02 — Done:** v2 work keys are due-first and normal pages apply an explicit
  `due_at <= cutoff` predicate with deterministic keyset tie-breaks.
- **R03 — Done:** one serializable generation/queue cursor lease records owner,
  token, expiry and page end; only exact unexpired token ACK advances the
  committed cursor, so expiry/crash replays rather than skips.
- **R04 — Done:** queue/model/cursor mismatch, active foreign leases, stale ACK,
  overflow and bounded-input republish all fail closed.
- **R05 — Done:** synthetic alias divergence, due pagination/no-starvation,
  expired replay, serializable claim, stale-owner ACK, mismatch, overflow and
  partial-input readiness regressions are covered.
- **R06 — Done:** canonical schema and incident regression docs describe v2;
  offline cutover output includes cursor DDL/seeds. No live action occurred.

## Branch

`agent/static-unified/l1c-ydb-projection-lease`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/static-site-unified-20260803/l1c-ydb`

## Base SHA

`f8dbe224ed8aded5d2067d377bea5387b5635a7e`

## Head SHA

Implementation commit: `c37dff17b`; official-YQL Uint64 literal hardening:
`dcb4f6dbd`. This RESULTS metadata commit follows them; integrate the branch
tip.

## Verification

- `14 passed in 0.65s` for the final targeted v2 read-model suite.
- `335 passed, 1 deselected in 17.06s` for the targeted read-model plus full
  CandidateReport suite. The deselected test is the pre-existing optional
  `openpyxl` case; the un-deselected run had `335 passed` and only that missing
  dependency failure.
- `py_compile` passed for both implementation modules, the offline planner and
  the targeted test module; `git diff --check` passed.
- CandidateReport-generated report files were removed using an explicit
  untracked allowlist; only lane-owned source/docs/tests are committed.

## Live actions

None. No YDB connection/read/write, DDL application, scheduler/RU change,
deploy, catch-up or production action was performed.
