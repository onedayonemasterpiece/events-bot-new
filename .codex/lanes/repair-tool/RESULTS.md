# Lane R5 / repair-tool results

## Scope

- Lane ID: `repair-tool`
- Requirement IDs: R5 manifest repair CLI, guarded preservation transaction, verification/idempotency, receipt/cleanup handoff, compact SQLite tests.
- Writable scope used:
  - `scripts/ops/repair_august_dedup_regression_20260822.py`
  - `tests/test_repair_august_dedup_regression_20260822.py`
  - `.codex/lanes/repair-tool/RESULTS.md`
- Forbidden files/actions respected: no core Smart Update edits, docs/CHANGELOG edits, production/database/API actions, integration-worktree edits, pushes, or destructive Git operations.

## Git metadata

- Branch: `agent/dedup-regression/repair-tool`
- Base SHA: `ddd75b74105e0fea778ef25e3a1ff234d96d1e7a`
- Implementation head SHA: `d73726deb2779e7e257838c0a085cb7def29d79c`
- Results receipt is committed separately after the implementation commit; the parent integration report records the final branch head.

## Delivered behavior

- CLI requires explicit `--db` and `--manifest`; defaults to read-only dry-run and supports explicit `--apply`, `--verify`, `--rollback`, and optional sanitized `--receipt` output.
- Manifest v1 is incident/prevention/census anchored and accepts only manually adjudicated `SAME_EVENT`/`MERGE` or `KEEP_DISTINCT` clusters. It validates event anchors and row hashes, full incident graph hashes, exact candidate-state and source-occurrence projections, source/poster policies, public mapping, cross-cluster IDs, and running jobs before any write.
- Apply uses `BEGIN IMMEDIATE`, narrowly named backup/receipt tables, no Event deletion, and marks obsolete Events `cancelled + silent + identity_status=merged + merged_into_event_id` without modifying stored public URLs.
- Unique sources/facts move to the canonical Event; exact duplicate source bindings collapse only after facts and source-linked identity decisions are repointed. Facts and attempt history are never deleted.
- Posters move where unique; media hash collisions retain both evidence rows and connect the obsolete row to canonical media via the review graph. No poster evidence is deleted.
- Pending/paused obsolete jobs transition to the existing valid `error` state with an incident cancellation reason; terminal job and Smart Update attempt history are untouched. Accepted current candidate owners alone are reconciled.
- `linked_event_ids` are rewritten without obsolete/self/duplicate references. One append-only repair decision is emitted per obsolete Event.
- Verification covers touched fact/source consistency, source ownership, poster/decision-source graph orphans, candidate-owner contracts, pending obsolete jobs, linked IDs, FK/orphan baselines, and `quick_check`.
- Second apply returns `changed=false` with `diff=[]` plus verification. Receipt includes canonical/obsolete public/social mapping and explicitly reports that no social action was performed.
- CAS-guarded rollback restores all mutated/deleted pre-rows and removes only repair-created decision rows.

## Evidence and commands

Executed from `/home/dev/.codex/worktrees/events-bot-new/repair-tool` with the required integration virtualenv:

```text
/home/dev/.codex/worktrees/events-bot-new/integration-dedup-regression/.venv/bin/python -m py_compile scripts/ops/repair_august_dedup_regression_20260822.py tests/test_repair_august_dedup_regression_20260822.py
# exit 0

/home/dev/.codex/worktrees/events-bot-new/integration-dedup-regression/.venv/bin/python -m pytest -q tests/test_repair_august_dedup_regression_20260822.py
# 7 passed in 0.58s

git diff --check
# exit 0, no output

/home/dev/.codex/worktrees/events-bot-new/integration-dedup-regression/.venv/bin/python scripts/ops/repair_august_dedup_regression_20260822.py --help
# confirmed required --db/--manifest, dry-run default, explicit apply/verify/rollback, optional receipt
```

The fixture tests prove preservation/rebinding of sources and facts, poster evidence preservation, public URL retention, obsolete-job cancellation, append-only attempt preservation, accepted-owner reconciliation, linked-ID rewrite, KEEP_DISTINCT no-op, stale row-hash refusal, running-job refusal, exact candidate/occurrence refusal, second-apply zero diff, verify, rollback restore, rollback CAS refusal, cross-cluster refusal, and census-hash refusal.

## Risks / production assumptions

- The tool intentionally requires the current production schema. It fails closed if Event identity/lifecycle/link columns, EventSource candidate/occurrence fields, Smart Update candidate/attempt tables, EventPoster review graph, JobOutbox `last_error`, or identity-decision `source_id` are absent.
- `JobStatus` has no cancelled enum value. Therefore cancellation is represented by the existing terminal `error` status plus an incident-specific `last_error`; this avoids introducing schema/core changes in this lane.
- A production manifest must be built from one consistent post-prevention census snapshot. Any intervening incident-graph change causes graph/row/candidate/occurrence mismatch and blocks apply.
- The tool does not rebuild projections or perform Telegram/VK/Telegraph/ICS/static/vector actions. It emits only the cleanup mapping/receipt for the separately owned handoff, as required.
- No production manifest or production DB was supplied to this lane, so tests use only a compact local SQLite fixture and no production preservation claim is inferred beyond the checked schema contract.

## Changed files

- `scripts/ops/repair_august_dedup_regression_20260822.py`
- `tests/test_repair_august_dedup_regression_20260822.py`
- `.codex/lanes/repair-tool/RESULTS.md`
