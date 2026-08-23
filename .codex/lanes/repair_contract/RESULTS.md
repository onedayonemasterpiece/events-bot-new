# Lane repair_contract results

> Integration correction: the worker result below records its original lane
> output. Final integration deliberately replaced complete Cartesian
> `pair_verdicts` coverage with sparse explicit reviewed edges, and replaced
> full `event_publication` row CAS with an ownership projection. Reconcile
> metadata is allowed to drift and rollback does not overwrite it. See
> `.codex/integration/INTEGRATION_REPORT.md` for final evidence.

## Scope

- Lane ID: `repair_contract`
- Requirement IDs: `R1`, `R3`
- Base SHA: `0af3f0f8b4673417853a5cd2b6be13b67b21fded`
- Implementation head SHA: `91c1673790b77a432141b66c8f5cf988d58b31c4`
- Branch: `agent/august-repair-contracts/repair-contract`
- Production/content/social mutation: **none**
- Push: **not performed**

## Result

### R1 — repair CAS

- `cluster_graph_hash` now fingerprints the required stable job fields and excludes only `updated_at` / `next_run_at`.
- Apply performs the final job/publication reread after `BEGIN IMMEDIATE`.
- Stable job drift, running jobs, added/removed jobs, and `event_publication` row/ownership drift fail closed.
- Timestamp-only drift is returned by dry-run/apply and persisted in `observed_job_timestamp_drift_json` on the repair receipt.
- Post-apply receipt verification hashes jobs through the same stable projection, so later scheduler timestamp changes do not break the exact second-apply noop.
- Publication rows are included in the forensic backup/receipt CAS surface.

### R3 — mixed pairwise component

- Manifest clusters may now use `component_id`, `event_ids`, and complete `pair_verdicts` with `MERGE`, `KEEP_DISTINCT_RELATED`, and `PARENT_CHILD`.
- Validation enforces complete unordered-pair coverage, reverse-pair uniqueness, valid endpoints/canonicals, grounded non-merge decisions, and a safe merge order (no canonical may also be obsolete; no obsolete may have multiple owners).
- Merge pairs execute first in deterministic order. Only explicitly non-merge pairs receive append-only `FINAL_DISTINCT` ledger rows; merge pairs never receive blanket distinct rows.
- Admissions-style tests cover two duplicate department pairs plus related-distinct and parent-child edges.
- The public-exhibition audit accepts grounded `related_but_distinct` and `parent_child` final/manual rows as `KEEP_DISTINCT`, preventing reader-facing `UNRESOLVED` for those pairwise hard negatives.

## Test-first evidence

Before implementation, the new CAS tests reproduced:

- timestamp-only drift blocked by `cluster_graph_hash_mismatch`;
- six stable job-field drift cases surfaced only as generic graph mismatch;
- publication ownership drift was not blocked.

Result: `8 failed, 8 passed`.

Before mixed-component implementation, the new component tests reproduced unsupported schema behavior (`cluster_id_invalid`).

Result: `4 failed, 16 deselected`.

## Commands and validation

```text
python3 -m py_compile scripts/ops/repair_august_dedup_regression_20260822.py scripts/inspect/audit_public_exhibition_duplicates.py
/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q \
  tests/test_repair_august_dedup_regression_20260822.py \
  tests/test_exhibition_identity_duplicate_monitor.py \
  tests/test_exhibition_duplicate_audit_scheduler.py
# 36 passed in 2.37s

git diff --check
# clean
```

`ruff` was not installed in the provided test environment; syntax compilation, focused pytest suites, and `git diff --check` passed.

## Changed files

- `scripts/ops/repair_august_dedup_regression_20260822.py`
- `scripts/inspect/audit_public_exhibition_duplicates.py`
- `tests/test_repair_august_dedup_regression_20260822.py`
- `tests/test_exhibition_identity_duplicate_monitor.py`
- `.codex/lanes/repair_contract/RESULTS.md`

The audit file/test ownership was explicitly added by the parent after acceptance mapping found that its prior relation allowlist would otherwise leave the new grounded hard negatives unresolved.

## Risks / merge notes

- Existing manifests must be regenerated with the deployed implementation because `expected_graph_sha256` now uses stable job projection and includes `event_publication`; each unit must provide `expected_job_rows` and `expected_event_publications`.
- `event_publication` is deliberately pinned as a full row, not merely by `event_id`, so any concurrent reconciliation drift fails closed.
- `PARENT_CHILD` direction follows manifest `left_id` (parent) to `right_id` (child); pair uniqueness remains unordered for duplicate/reverse detection.
- The implementation preserves legacy homogeneous `MERGE` / `SAME_EVENT` / `KEEP_DISTINCT` manifests once they include the new CAS projections.
- No Qtickets, Smart Update, docs, changelog, or production-state files were changed in this lane.
