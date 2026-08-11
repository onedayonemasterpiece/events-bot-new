# Lane SMART-CLOSURE Results

## Status
committed

## Requirement IDs
- R17
- T56
- T57
- T58
- T59
- T60
- T61
- T62
- T63 (core result boundary; caller AST enforcement is owned by the caller lane)
- T64
- T65

## Branch
`agent/smart-update-llm-first/smart-closure`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/smart-update-smart-closure`

## Base SHA
`8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`

## Head SHA
Implementation commit: `d7d896ffa9a7a14667609c5c8e39dc973845f788`.
The lane receipt is committed immediately after this implementation commit; use
`git rev-parse agent/smart-update-llm-first/smart-closure` for the receipt-inclusive lane head.

## Files changed
- `smart_event_update.py`
- `smart_update_state.py`
- `smart_update_identity.py`
- `tests/test_smart_update_typed_reasons.py`
- `tests/test_smart_update_occurrence_stability.py`
- `tests/test_smart_update_automatic_identity_resolution.py`
- `.codex/lanes/SMART-CLOSURE/RESULTS.md`

## Delivered
- Added closed `ProductExclusionReason`, `RetryReason`,
  `IdentityDistinctReason`, and `LifecycleReason` enums and structural fields on
  Smart Update results/identity verdicts.
- Removed substring-based legacy reason authority. Only exact closed legacy
  reasons can cross the compatibility boundary; unknown/untyped product reasons
  fail closed to `RETRY_SCHEDULED`.
- Separated typed semantic identity uncertainty from technical/provider/schema/
  DB/vector uncertainty. Only `IDENTITY_SEMANTIC_UNKNOWN` can consume the bounded
  identity budget and create distinct; technical failure remains durably retryable.
- Changed late incoherent merge rollback to reuse the prepared create path and
  return a typed distinct creation instead of product rejection.
- Implemented occurrence precedence: explicit/source-native, vendor, ticket,
  structured schedule anchor, then ordinal only as a same-anchor tie-breaker.
- Added a hard same-source rail excluding Events bound to a different explicit
  occurrence ID before source-anchor matching, shortlist matching, city rescue,
  and final duplicate probing.
- Preserved accepted versus diagnostic IDs, exact replay, known-distinct immediate
  creation, and a single merge-identity adjudicator call per adjudication attempt.

## Commands run
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m py_compile smart_event_update.py smart_update_state.py smart_update_identity.py tests/test_smart_update_typed_reasons.py tests/test_smart_update_occurrence_stability.py tests/test_smart_update_automatic_identity_resolution.py`
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_smart_update_typed_reasons.py tests/test_smart_update_occurrence_stability.py tests/test_smart_update_automatic_identity_resolution.py tests/test_smart_update_candidate_state_db.py`
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_smart_update_candidate_state_db.py tests/test_smart_update_candidate_state_keys.py tests/test_smart_update_identity_gate.py tests/test_smart_update_merge_identity_gate.py tests/test_smart_update_identity_persistence.py tests/test_smart_update_identity_replay_contracts.py tests/test_smart_update_source_identity_contract.py tests/test_smart_update_terminal_contract.py tests/test_smart_update_outcome_boundary_hotfix.py tests/test_smart_update_parser_occurrences.py tests/test_smart_update_typed_reasons.py tests/test_smart_update_occurrence_stability.py tests/test_smart_update_automatic_identity_resolution.py`
- `git diff --check`

## Tests / verification
- Focused owned/core suite: `25 passed`.
- Expanded Smart Update DB/facade/identity suite: `91 passed, 9 warnings`.
- `py_compile`: passed.
- `git diff --check`: passed.
- New coverage proves: typed unknown fail-closed behavior; technical retry beyond
  max semantic attempts; bounded typed semantic UNKNOWN distinct creation;
  incoherent merge rollback-to-create; explicit-ID same-slot hard distinct;
  reorder/insertion binding stability; and diagnostic ID isolation at the core
  result boundary.

## Risks
- Stable reorder/insertion requires producers to provide a source-native or vendor/
  ticket occurrence ID. When none exists, ordinal remains only the collision
  tie-breaker after the structured schedule anchor; no core algorithm can infer a
  missing native identity safely.
- Existing legacy `ordinal:*` EventSource bindings are intentionally not rewritten
  in this lane because schema/data migration and callers were forbidden.
- T63 caller-side AST proof and downstream adaptation remain owned by the separate
  caller lane; this lane guarantees that a retry/rejection never exposes a
  diagnostic ID as accepted `event_id`.
- Documentation and `CHANGELOG.md` are intentionally untouched because they are
  forbidden in this worker scope and must be handled by integration.

## Merge notes
Cherry-pick the implementation commit and this receipt commit in order. No push,
deploy, merge, or production write was performed.
