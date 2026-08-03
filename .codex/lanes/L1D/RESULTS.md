# Lane L1D Results

## Status

in progress

## Requirement IDs

- **R01 — Done:** partial materialized consumer saves ACK the existing
  generation only; they do not publish work rows, seed cursors or mutate the
  sole current pointer.
- **R02 — Done:** only complete producer input can publish/swap a generation;
  partial `PUBLISH_READY=1` is rejected before YDB connect or mutation.
- **R03 — Done:** deterministic two-run regression consumes page 1, ACKs, starts
  page 2 after the committed key and reaches final no-work with the same ready
  pointer. Expired-lease replay coverage remains active.
- **R04 — Done:** official YQL `Uint64` fallbacks use `0ul`; canonical schema and
  incident docs record the corrected lifecycle. No live action occurred.

## Branch

`agent/static-unified/l1d-ydb-consumer-lifecycle`

## Base SHA

`d0c3f7d7fc08edc475ae20217115b0b370f3f135`

## Head SHA

Pending commit.

## Verification

- `16 passed in 1.41s` for the final targeted typed read-model/lifecycle suite.
- `337 passed, 1 deselected in 19.33s` for the typed read-model plus full
  CandidateReport suite. The deselection is the pre-existing optional
  `openpyxl` test absent from the supplied environment.
- `py_compile` and `git diff --check` passed. CandidateReport-generated output
  files were removed; only lane-owned source/tests/docs/results remain.

## Live actions

None.
