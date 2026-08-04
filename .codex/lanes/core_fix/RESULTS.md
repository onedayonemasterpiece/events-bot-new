# Lane core_fix Results

## Status
committed

## Requirement IDs
- CORE-1: enforce/fail-closed merge identity gate before domain side effects
- CORE-2: canonical source/ticket URLs
- CORE-3: role-aware EventSource schema and transactional binding guard
- CORE-4: exact input packet fingerprint/noop
- CORE-5: context-only exclusion from identity
- CORE-6: caller noop scheduling/attachment suppression
- CORE-7: targeted regressions

## Branch / worktree
- Branch: `agent/smart-update-identity-repair/core`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/smart-update-identity-core`

## Base SHA
`0d1848bc324ef8c44df146ec2a7126a116a94bf4`

## Head SHA
Reported in the lane handoff after the commit; embedding a commit's own SHA in
that commit is self-referential.

## Evidence
- `SMART_UPDATE_MERGE_IDENTITY_GATE="enforce"` is present in `fly.toml`.
- Invalid/unavailable merge decisions, context-only identity claims, and all
  nonempty blocking-conflict packets fail closed.
- Festival queue persistence occurs only after an accepted create/merge DB
  transaction commits; a rejected merge regression asserts zero queue writes.
- Exact accepted packet replay returns `noop_exact_source_replay` before LLM or
  domain writes; ambiguous multi-event context replay returns
  `review_required/source_binding_conflict`.
- EventSource evolution is additive and leaves legacy role/canonical fields
  unclassified. Conditional partial uniqueness is activated only when its
  existing rows have no relevant conflict.

## Commands run / tests
- `uv run --with-requirements requirements.txt pytest -q tests/test_smart_update_source_identity_contract.py`
  - `6 passed`
- `SMART_UPDATE_SKIP_PAST_EVENTS=0 uv run --with-requirements requirements.txt pytest -q tests/test_smart_update_source_identity_contract.py tests/test_smart_update_merge_identity_gate.py tests/test_smart_update_identity_persistence.py tests/test_smart_update_identity_gate.py tests/test_db.py`
  - `41 passed`
- `python3 -m py_compile smart_update_identity.py smart_event_update.py models.py db.py source_parsing/telegram/handlers.py vk_intake.py`
  - passed
- `git diff --check`
  - passed

## Risks / boundaries
- Legacy EventSource rows remain nullable/unclassified by design; this lane does
  not attempt unsafe history-wide classification or repair.
- Conditional unique indexes deliberately remain absent when a touched/explicit
  conflict exists; the binding guard still fails closed for new Smart Update
  identity-bearing writes.
- Whole-festival/context posts no longer persist FestivalQueueItem rows unless a
  concrete create/merge identity is accepted, matching the side-effect gate.
- No production, paid providers, audit/repair scripts, docs, or changelog were
  touched in this lane.

## Changed files
- `smart_event_update.py`
- `smart_update_identity.py`
- `models.py`
- `db.py`
- `fly.toml`
- `source_parsing/telegram/handlers.py`
- `vk_intake.py`
- `tests/test_smart_update_source_identity_contract.py`
- `.codex/lanes/core_fix/RESULTS.md`
