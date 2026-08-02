# Lane static-collection-product-loop-product Results

## Status
committed

## Requirement IDs
- R9 — import the exact PR #234 product-quality runner/workflow/test/document set without merging main.
- R10 — integrate one provider-free facts-v3 product snapshot adapter at the existing exporter/StaticSiteBuilder boundary.

## Branch
`agent/static-collection-product-loop/product-snapshot`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-collection-facts-v3-backfill`

## Base SHA
`dded795fe1dc87acf4f17fff1b4e3d67f093b000`

## Head SHA
Implementation commit: `153692926d4337c5f40563934946d57f5f733b00`.
The branch tip also contains the subsequent documentation-only lane-results commit.

## Files changed
- `.github/workflows/static-collections-product-quality.yml`
- `CHANGELOG.md`
- `docs/README.md`
- `docs/features/static-site-pages/podborki-to-be.md`
- `docs/features/static-site-pages/release-autotest-gates.md`
- `docs/operations/static-collection-facts-v3.md`
- `docs/routes.yml`
- `docs/testing/static-collections-product-quality-autotests.md`
- `docs/testing/static-site-autotest-scenarios.v1.yml`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `scripts/check_static_collections_product_quality.py`
- `scripts/run_static_site_builder_kaggle.py`
- `site/scripts/export-production-preview-data.py`
- `site/scripts/static_collection_product_snapshot.py`
- `tests/fixtures/static_collections_product_quality/baseline.json`
- `tests/fixtures/static_collections_product_quality/current.json`
- `tests/fixtures/static_collections_product_quality/regression.json`
- `tests/test_static_collection_export.py`
- `tests/test_static_collection_product_snapshot.py`
- `tests/test_static_collection_semantics.py`
- `tests/test_static_collections_product_quality.py`

## Commands run
- `git fetch origin pull/234/head`
- `git restore --source=FETCH_HEAD -- <PR #234 file set>`
- `python3 -m unittest discover -s tests -p 'test_static_collections_product_quality.py' -v`
- `UV_CACHE_DIR=/dev/shm/uv-facts-v3-cache-1785673946 uv run --no-project --with pytest --with numpy python -m pytest --noconftest tests/test_static_collection_product_snapshot.py tests/test_static_collection_export.py tests/test_static_collection_semantics.py -q`
- `UV_CACHE_DIR=/dev/shm/uv-facts-v3-cache-1785673946 uv run --no-project --with pyyaml python - ...`
- `python3 -m py_compile site/scripts/static_collection_product_snapshot.py site/scripts/export-production-preview-data.py kaggle/StaticSiteBuilder/static_site_builder.py scripts/run_static_site_builder_kaggle.py scripts/check_static_collections_product_quality.py`
- `git diff --check`

## Tests / verification
- PR #234 product-quality runner regressions: **10 passed**.
- Facts-v3 adapter/exporter/StaticSiteBuilder focused pytest set: **22 passed**.
- Python compile check: **PASS**.
- Workflow and both YAML registries parsed with PyYAML: **PASS**.
- `git diff --check`: **PASS**.
- Adapter CLI regression covers stage provenance `production-copy-after-apply`, independent evidence trust scope, exact EventSource binding, organizer projection and atomic valid output.
- Warm regression proves equal `input_fingerprint` plus PR #234 runner-compatible `normalized_output_sha256`, while `snapshot_sha256` may differ with `generated_at`.

## Risks
- No owner-accepted baseline or terminal real production/Kaggle product-quality artifact exists yet; absent baseline remains `WATCH` by design.
- No real Kaggle cold/warm canary, production apply, ingestion replay, deployment or schedule was performed in this lane.
- Semantic publication remains blocked; the adapter only emits `shadow`/`experimental` collections and performs zero provider calls.
- The standalone DB adapter uses existing `Event.organizer_names`; empty stored organizer data remains empty rather than being inferred.

## Merge notes
- Cherry-pick the implementation commit and this results commit; do not merge main into the lane.
- PR #234 content was copied at fetched head `8e9e0fe` and then only reconciled where R10 required adapter grounding/fail-closed behavior and current branch documentation/index integration.
- This lane intentionally contains no owner gold, scores, thresholds, provider calls, Astro routes, navigation, sitemap, schedule or public-mode activation.
