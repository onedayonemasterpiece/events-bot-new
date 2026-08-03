# Lane L1C Results

## Status

in progress

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

Pending commit.

## Verification

Pending final targeted and CandidateReport regression runs.

## Live actions

None. No YDB connection/read/write, DDL application, scheduler/RU change,
deploy, catch-up or production action was performed.
