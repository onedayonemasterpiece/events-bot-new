# Lane results: static-collection-shadow-builder

## Scope

- Lane ID: `static-collection-shadow-builder`
- Requirement IDs: `R7-R10`
- Base SHA: `61b8d7dcf58a4299c2e9a7538fa55c3eeda9be79`
- Implementation head SHA: `2ab6b99b066a66dbddb564c7cea2ff7cecb276b2`
- Status: `DONE`

## Result

The existing StaticSiteBuilder pipeline now stages and invokes the existing
`check_static_collections_product_quality.py` contract immediately after it
validates and copies `static-collection-product-snapshot-v1.json`.

It writes these bounded evidence files beside the copied snapshot in Kaggle
working output:

- `static-collections-product-quality.json`
- `static-collections-product-quality.md`
- `qa-summary.json`

`WATCH` remains non-blocking. `FAIL` writes the reports/QA summary and then fails
the build. No baseline, second pipeline, schedule, required-live setting,
publication, route, ingestion, or Smart Update change was added.

The local runner validates the three downloaded hashes and atomically persists
them beside its configured durable product snapshot.

## Evidence and commands

```bash
python3 -m py_compile \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  scripts/run_static_site_builder_kaggle.py
```

Result: PASS.

```bash
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_collection_semantics.py \
  tests/test_static_collections_product_quality.py
```

Result: `16 passed`.

```bash
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_site_build_handoff.py \
  tests/test_static_collection_semantics.py \
  tests/test_static_collections_product_quality.py
```

Result: `52 passed`.

```bash
git diff --check
```

Result: PASS.

One exploratory pytest command referenced the nonexistent
`tests/test_static_site_kaggle_production.py` and exited before collection; it
was corrected to the 52-test command above.

## Changed files

- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `scripts/run_static_site_builder_kaggle.py`
- `tests/test_static_collection_semantics.py`
- `.codex/lanes/static-collection-shadow-builder/RESULTS.md`

## Risks / follow-up owned by integrator

- No paid/live Kaggle build was run in this lane; integration CI or the bounded
  shadow run must confirm the three files are present in downloaded kernel
  output.
- Preview builds without a configured repository SHA record `repo_sha: null` in
  QA summary; production-candidate builds retain their mandatory exact SHA.
- Shared canonical docs and `CHANGELOG.md` were deliberately not edited because
  they are integrator-owned.
