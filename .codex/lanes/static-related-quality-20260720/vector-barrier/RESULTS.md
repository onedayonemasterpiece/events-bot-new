# Vector barrier lane results

## Lane

- Lane ID: `static-related-quality-20260720/vector-barrier`
- Requirements: R01, R02, R04
- Base SHA: `288b56790ba0866fcbf3da827c499c421425b709` (`origin/main` at lane start)
- Implementation head SHA: `2d169abf` (`Gate static builds on vector corpus receipts`)
- Branch: `agent/static-related-quality/vector-barrier`

## Outcome

- Added deterministic `search_v3_hash` and `related_v1_hash` corpus revisions to every vector sync report. The canonical input binds ordered event/text hashes, embedding model, dimension, and document kind.
- The vector job now exports and derives event revisions from one consistent SQLite backup, then atomically writes an fsynced `event_vector_sync_receipt_v1` receipt. Default path: `/data/event_vector_sync_receipt.json`; override: `EVENT_VECTOR_SYNC_RECEIPT_PATH`.
- The static-site vector barrier now rejects wrong receipt schemas and missing/malformed corpus hashes, while preserving retryable behavior for absent/incomplete receipts and revision lag.
- Fly config enables the mandatory pgvector barrier and binds both writer and reader to `/data/event_vector_sync_receipt.json`.
- The validated `related_v1_hash` is included in the static input fingerprint and propagated as `--related-corpus-revision` through Fly runner config to the Kaggle exporter command.
- The per-anchor Gemma strict verifier remains explicitly disabled in Fly: `STATIC_SITE_GEMMA_RELATED_VERIFY=0`, `STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS=0`.

## Contract for integration

- Receipt/report fields: `search_v3_hash`, `related_v1_hash` (lowercase SHA-256 hex).
- Receipt schema: `event_vector_sync_receipt_v1`.
- CLI: `--related-corpus-revision`.
- Environment fallback: `STATIC_SITE_RELATED_CORPUS_REVISION`.
- Kaggle `build_config.json`: `related_corpus_revision`.
- Integration owns exporter cache validation/payload binding in `site/scripts/export-production-preview-data.py`; the root integrator confirmed that implementation is present on integration.

## Validation evidence

Commands run:

```text
PYTHONPATH="$PWD:/home/dev/projects/events-bot-new" /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q \
  tests/test_event_vector_sync.py \
  tests/test_static_site_release.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_build_debounce.py
```

Result: `57 passed in 9.74s`.

```text
PYTHONPATH="$PWD:/home/dev/projects/events-bot-new" /home/dev/.venvs/events-bot-image-geometry/bin/python -m py_compile \
  event_vector_sync.py static_site_release.py main.py \
  scripts/sync_event_search_vectors_to_supabase.py \
  scripts/run_static_site_builder_kaggle.py \
  kaggle/StaticSiteBuilder/static_site_builder.py
```

Result: passed.

```text
scripts/sync_event_search_vectors_to_supabase.py --preview-events-json <two-event-fixture> --report-json <report>
```

Result: planning report emitted distinct 64-character `search_v3_hash` and `related_v1_hash` fields.

## Changed files

- `scripts/sync_event_search_vectors_to_supabase.py`
- `event_vector_sync.py`
- `static_site_release.py`
- `main.py`
- `scripts/run_static_site_builder_kaggle.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `fly.toml`
- `tests/test_event_vector_sync.py`
- `tests/test_static_site_build_handoff.py`
- `tests/test_static_site_release.py`
- `.codex/lanes/static-related-quality-20260720/vector-barrier/RESULTS.md`

## Risks and integration notes

- `kaggle/StaticSiteBuilder/static_site_builder.py` has one localized insertion beside `--related-mode` (exporter command construction, approximately line 339). The playwright-gate lane also owns changes in this file; integrator must preserve both.
- `kaggle/StaticSiteBuilder/static_site_builder.ipynb` does not exist on `origin/main`; propagation was implemented in the tracked `static_site_builder.py` after root authorization.
- Canonical feature documentation and `CHANGELOG.md` are outside this lane's writable scope and remain an integration responsibility.
- No deploy or push was performed, per lane instructions.
