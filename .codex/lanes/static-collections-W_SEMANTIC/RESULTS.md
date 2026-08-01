# W_SEMANTIC results

## Lane contract

- Lane: `W_SEMANTIC`
- Requirements: `R03`, `R06`
- Base SHA: `23c1702bb565f693f7022f3d7ac2e3455d6d412c`
- Implementation head SHA: `14945d2216018cf64490e368ac01135576e3738d`
- Branch: `agent/static-collections-data-prep/W_SEMANTIC`
- Push: not performed

## Outcome

### R03 — strict trailing debounce

Done within `main.py` and focused tests.

- Every incoming automatic Smart Update resets the one pending build to the
  merged `latest_effect_at + 15 minutes`, including a row that was already due.
- The old first-effect/max-debounce cap no longer applies to static Smart Update
  builds.
- Scheduling uses the incoming trigger, not merged trigger history. A prior
  operator/calendar/startup trigger cannot make a later Smart Update immediate.
- Incoming operator/calendar/startup requests pull the existing pending row
  forward.
- Existing atomic `BEGIN IMMEDIATE`, one-running + one-pending follow-up,
  recoverable handoff, stale-owner, no-op fingerprint and lease paths remain in
  place.
- Tests cover concurrent writers, mixed immediate/automatic history, and
  update-during-running rearm behavior.

### R06 — collection semantic compute contract

Done for the owned semantic/orchestration surfaces; exporter glue remains an
explicit integrator-owned dependency below.

- Production-candidate command/config/staging/kernel/runner now require
  collection semantic compute independently of `related_mode=pgvector` and
  `STATIC_SITE_UNUSUAL_ENABLED`.
- Kernel validates `collection-batch-v1.json` before Astro build and checks item
  IDs against the frozen `production-catalog.json` eligible set.
- Runner validates the returned receipt and atomically persists batch/current
  and optional last-good outputs without requiring unusual publication files.
- Added evidence-only `collection_semantics_v1` documents. Generated `topics`,
  tags, audience regex properties and `related_v1` digests are excluded.
- Added a collection BGE artifact API whose event cache identity depends only on
  model/document/vector contract. Prototype-bank or head-only changes reuse
  unchanged event rows and re-encode only changed prototypes.
- Added physical float32 NPZ writer/validator and float32-bound artifact
  validation for the collection contract.
- Added `static_collection_batch.py`: canonical builder/writer/validator for
  compute, quality and publication states, sorted ID-only items, per-label
  hashes/failures, approved-empty supply evidence, last-good use, catalog
  membership and self hash.
- Existing Gemini `related_v1` / pgvector vectors are never imported into or
  treated as the BGE collection matrix.
- No public collection route or rollout flag was enabled.

## Exact exporter wiring required during integration

`site/scripts/export-production-preview-data.py` is integrator-owned and was not
edited. The integration commit must make these exact connections:

1. Add CLI flags already emitted by runner/kernel:
   - `--collection-semantic-compute`
   - `--collection-batch-output <site/src/data/collection-batch-v1.json>`
   - `--collection-batch-last-good <durable working path>`
2. When compute is required, call
   `static_event_bge.build_collection_bge_vector_artifact(...)`, never reuse
   Gemini/pgvector `related_v1` vectors, and validate with
   `validate_collection_bge_vector_artifact(...)`.
3. Persist that matrix with `write_collection_bge_cache(...)` and validate the
   NPZ/receipt with `validate_collection_bge_cache(...)`; do not route the new
   contract through the exporter's existing float64 `_write_bge_cache`.
4. Build per-label records with `static_collection_batch.build_collection_label`
   and the final artifact with `build_collection_batch`; call
   `validate_collection_batch(..., catalog_item_ids=..., require_compute=True)`
   before `write_collection_batch(...)`.
5. Write the current batch to the requested output path. Promote
   `collection-batch-last-good.json` only after its label quality/publication
   rules pass; a failed label may use a compatible, non-empty validated
   last-good while other labels continue independently. Approved empty requires
   its explicit reason and verified supply count.
6. Extend `static-semantic-build-result.json` with:
   - `collection_batch_sha256` = SHA-256 of the emitted file bytes;
   - `collection_last_good_sha256` only when a last-good file was emitted;
   - existing zero-provider-call/event/artifact/cache hashes expected by runner.
7. Keep unusual page publication controlled by its existing rollout decision;
   compute must still run and emit a fresh receipt when that publication flag is
   false.

Until that exporter-side call site is merged, a production-candidate correctly
fails closed at the new required collection receipt instead of silently
skipping semantic compute.

## Validation evidence

Test environment: temporary untracked venv under `artifacts/codex/` using the
project's installed dependency environment plus NumPy; artifacts are ignored.

Commands:

```text
PYTHONPATH=/opt/venvs/events-bot-modern/lib/python3.12/site-packages \
  artifacts/codex/static-collections-W_SEMANTIC-testenv-2/bin/python -m pytest -q \
  tests/test_static_collection_semantics.py \
  tests/test_static_site_build_debounce.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_unusual_builder_adapter.py \
  tests/test_static_site_builder_preview_contract.py \
  tests/test_unusual_event_semantics_r15.py \
  tests/test_unusual_event_semantic_regressions.py
# 92 passed in 10.91s

PYTHONPATH=/opt/venvs/events-bot-modern/lib/python3.12/site-packages \
  artifacts/codex/static-collections-W_SEMANTIC-testenv-2/bin/python -m pytest -q \
  tests/test_static_collection_semantics.py \
  tests/test_static_site_build_debounce.py \
  tests/test_static_site_build_handoff.py
# 60 passed in 7.46s

python3 -m py_compile \
  main.py \
  site/scripts/static_event_bge.py \
  site/scripts/static_collection_batch.py \
  scripts/run_static_site_builder_kaggle.py \
  kaggle/StaticSiteBuilder/static_site_builder.py
# passed

git diff --check
# passed
```

## Risks / integration notes

- The mandatory production-candidate receipt is deliberately fail-closed. Merge
  the exporter wiring in the same integration before any candidate run.
- Existing related/unusual artifact metadata gains explicit `document_kind` and
  cache identity fields. A legacy cache can cold-rebuild once; it is never
  accepted as the new collection contract.
- No live BGE/Kaggle run or public deployment was performed in this lane.
- No docs or `CHANGELOG.md` were changed because they are integrator-owned and
  forbidden by the lane map.

## Changed files

- `main.py`
- `site/scripts/static_event_bge.py`
- `site/scripts/static_collection_batch.py`
- `scripts/run_static_site_builder_kaggle.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `tests/test_static_site_build_debounce.py`
- `tests/test_static_site_build_handoff.py`
- `tests/test_static_collection_semantics.py`
- `.codex/lanes/static-collections-W_SEMANTIC/RESULTS.md`
