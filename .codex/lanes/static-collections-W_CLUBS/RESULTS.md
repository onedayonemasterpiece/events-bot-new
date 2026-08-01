# W_CLUBS results

- Lane: `W_CLUBS`
- Requirement: `R01`
- Base SHA: `4e564392ff3348ef130ec270b5a155bd28833b24`
- Implementation SHA: `5271e8c7`
- Branch: `agent/static-collections-data-prep/W_CLUBS`

## Delivered

- Added durable `JobTask.interest_club_relation` enqueue/handler, seven-day TTL,
  bounded runtime, worker priority, retry/backoff/restart recovery and immutable
  running owner plus exactly one rearmed pending successor per event.
- Replaced Smart Update fire-and-forget task creation with awaited durable outbox
  enqueue using `interest_club_relation:<event_id>` and `requeue_done=True`.
- Added hash/policy-versioned `InterestClubEvaluation` history and additive SQLite
  table-copy migration `20260801_club_eval_history`, preserving existing rows.
- Provider failure and `unclear` now persist history without replacing an older
  accepted exact-hash active relation; explicit `no`, no-match and ineligible
  evidence can invalidate the relation. Provider deferrals raise a retryable
  marker from the outbox handler.
- Added exact accepted evaluation gate to static relation projection.
- Kept `interest-clubs.json` v1 filename/shape for Astro compatibility and added
  `interest-clubs-static-v2.json`: approved-only, exact six-calendar-month
  inclusive visibility, 6m/12m counts, last/next activity, dormant receipts,
  one-meeting eligibility, grounded festival relation support, current-catalog
  ID filtering and `data_updated_at`.
- Replaced unbounded club fingerprint rows with approved identity plus bounded
  six-month relation/evaluation/event truth; retry attempt/timestamp churn is
  excluded.
- Added a default-off, bounded, shadow-only discovery report CLI/helper. It does
  not approve identities, publish them or use BGE as relation truth.
- Added the read-only production control fixture: six approved identities and
  the 13 required grounded relation event IDs.

## Changed symbols / files

- `models.py`: `JobTask.interest_club_relation`, history unique constraint.
- `db.py`: fresh-schema history uniqueness.
- `alembic/versions/20260801_interest_club_evaluation_history.py`.
- `interest_clubs.py`: `InterestClubProviderDeferred`,
  `_active_grounded_relation`, durable `schedule_interest_club_evaluation`,
  history-aware evaluation semantics, `build_shadow_identity_discovery_report`.
- `smart_event_update.py`: durable awaited club enqueue at create/update seams.
- `main.py`: successor coalescing, TTL/runtime/priority/independent retry,
  `job_interest_club_relation`, `JOB_HANDLERS`.
- `site/scripts/export-production-preview-data.py`:
  `build_interest_clubs_projection_v2`, exact-hash v1 gate, v2 file writer.
- `static_site_release.py`: `_interest_club_projection_digest`.
- Focused tests and
  `tests/fixtures/interest_clubs_production_control_20260801.json`.

## Evidence and commands

1. Compile and diff hygiene:

   ```bash
   python3 -m py_compile interest_clubs.py models.py db.py main.py \
     smart_event_update.py static_site_release.py \
     site/scripts/export-production-preview-data.py \
     alembic/versions/20260801_interest_club_evaluation_history.py
   git diff --check
   ```

   Result: pass.

2. Club/projection/fingerprint tests:

   ```bash
   uv run --with-requirements requirements.txt pytest -q \
     tests/test_interest_clubs.py \
     tests/test_interest_clubs_static_export.py \
     tests/test_static_site_release.py \
     -k 'not static_site_debounce_has_maximum_wait_and_preserves_immediate_request'
   ```

   Result: `41 passed, 1 deselected in 4.77s`.

3. Outbox, Smart Update and merged semantic debounce regression tests:

   ```bash
   uv run --with-requirements requirements.txt pytest -q \
     tests/test_job_coalesce.py tests/test_job_dedup.py \
     tests/test_job_running_stale.py tests/test_job_due_filter.py \
     tests/test_static_site_build_debounce.py \
     tests/test_smart_event_update.py tests/test_event_update_merge.py
   ```

   Result: `62 passed in 12.85s`.

4. Migration upgrade preservation probe using an original named pair unique
   constraint and one existing evaluation, then inserting a second hash after
   upgrade:

   ```bash
   uv run --with 'alembic>=1.13,<2' --with 'sqlalchemy>=2,<3' \
     python3 /tmp/test_club_migration.py
   ```

   Result: `migration-upgrade-ok rows=2`.

5. Read-only Fly SQLite control audit (artifact ignored by git):
   `artifacts/codex/static-collections-W_CLUBS/prod-control-rows.json`.
   Compact committed fixture asserts six approved clubs and exactly relation
   events `2897,6929,6990; 2533,6662; 3032,3516,5806; 3488,3923;
   3265,6853; 3393` without hardcoding six in runtime code.

## Risks / integration notes

- The integration base contains one stale W_SEMANTIC-incompatible test:
  `tests/test_static_site_release.py::test_static_site_debounce_has_maximum_wait_and_preserves_immediate_request`
  still calls the retired `merged_payload`/maximum-cap API, while merged
  `main.py` intentionally implements strict trailing +15m. This existed before
  W_CLUBS and was not rewritten here; the root integrator was notified. The
  current semantic contract is covered by `tests/test_static_site_build_debounce.py`.
- No production writes, migration application, deploy, Astro route/navigation,
  cinema or festival extraction/page changes were performed.
