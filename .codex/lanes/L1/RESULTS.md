# Lane L1 Results — Region Talk YDB cost compaction

## Status

Committed. No live YDB writes, scheduler enablement, RU/s change, deploy, or
catch-up was performed.

- Branch: `agent/static-unified/l1-ydb-compaction`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-site-unified-20260803/l1-ydb`
- Base SHA: `0bc8482dcda5cf16a89f312f9791ecbb6d0e9a3a`
- Implementation SHA: `484db3b80d3c29cf2aa9c9f27d699b08ca8734ab`
- Results metadata: committed separately after the implementation SHA; use the
  branch tip when integrating.

## Requirement checklist

- **R01 — Done:** exact full YDB database-path/account guard, with redacted
  mismatch errors, is mandatory for the scheduled path and compactor.
- **R02 — Done:** application-side per-cycle/process hard ceilings cover query
  count, read/write rows and bytes, and the documented YQL I/O RU floor.
- **R03 — Done:** exact-post work uses a bounded keyset compatibility page and
  point reads for referenced rows rather than 20k population scans.
- **R04 — Done:** CandidateReport bulk writes deduplicate stable PKs and skip
  product-identical repeats within the same process; fingerprints are recorded
  only after successful chunks.
- **R05 — Done:** the one-time compactor now has explicit cost ceilings while
  retaining source-watermark, count, identity, and ordered-export validation.
- **R06 — Done:** incident regression documentation and deterministic fixtures
  cover the migration facts and a synthetic 20,000-row workload.

## Files

- `scripts/region_talk_ydb_cost.py`
- `scripts/region_talk_orchestrator.py`
- `scripts/region_talk_scheduled_runner.py`
- `scripts/region_talk_ydb_compact.py`
- `kaggle/execute_region_talk_candidate_report.py`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `tests/test_region_talk_ydb_cost.py`
- `tests/test_region_talk_candidate_report.py`
- `tests/fixtures/region_talk_ydb_cost_20k.json`
- `tests/fixtures/region_talk_ydb_incident_migration.json`
- `docs/reports/incidents/INC-2026-08-03-ydb-request-unit-billing.md`

## Validation

- `457 passed, 1 deselected in 115.42s`:
  `/opt/venvs/events-bot-modern/bin/pytest -q tests/test_region_talk_ydb_cost.py tests/test_region_talk_ydb_compact.py tests/test_region_talk_scheduled_runner.py tests/test_region_talk_orchestrator.py tests/test_region_talk_candidate_report.py -k 'not public_blogger_links_imports_frontier_only_and_dedupes'`
- Re-ran `tests/test_region_talk_ydb_cost.py` after the final scan-ceiling fix:
  passed.
- `python3 -m py_compile` passed for every changed Python/test module.
- `git diff --check` passed.
- Forbidden scopes `site/src`, `CHANGELOG.md`, and `docs/routes.yml` were not
  modified.

## Risks / follow-ups

1. One pre-existing CandidateReport test was deselected because the supplied
   shared environment lacks `openpyxl`; no dependency install was attempted due
   to the lane disk constraint.
2. Exact billed RU was not measured: YQL bills the greater of CPU and I/O, while
   this client ledger can only compute the documented I/O floor. Server-side RU
   telemetry and billing alerting remain follow-ups.
3. Start-state reconstruction and the orchestrator's complete metric population
   still contain broad reads. The strict 5,000-row ceiling is intended to abort
   them rather than silently recreate the incident; scheduler enablement remains
   blocked pending typed indexes/counters and a manually approved canary.
4. Changed-only bulk suppression is process-local. Cross-process suppression
   requires typed state/CDC or a stored content fingerprint.

## Integration notes

Merge the whole branch (implementation plus this metadata commit). The parent
lane owns `CHANGELOG.md`, route/index synchronization, release validation, and
any later live canary approval.
