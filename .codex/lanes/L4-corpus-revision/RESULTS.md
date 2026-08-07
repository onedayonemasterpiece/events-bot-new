# L4-corpus-revision results

## Lane contract

- Lane ID: `L4-corpus-revision`
- Requirement: `R09`
- Base SHA: `ec09c011674eecddf9e9b8e154e3d102f9384b12`
- Implementation head SHA: `e24d4ace9feb33fa663021afb80be6f6b9543fa9`
- Branch: `agent/search-live-automation/corpus-revision`
- Effort/risk: high; cross-boundary Search catalog/pgvector revision contract

## Outcome

- Added the shared deterministic `event_search_catalog_revision_v1` algorithm to
  the static exporter and vector projector. It hashes every complete eligible
  exported event payload, rejects duplicate/non-positive IDs and is independent
  of catalog ordering.
- Full and slice static exports now publish `catalog_revision` plus its schema
  version in `preview-events.json`; the full production catalog ledger binds the
  same revision to exact eligible-ledger parity.
- Upgraded corpus identity to `event_search_corpus_revision_v2`. Each corpus
  revision now binds `catalog_revision`, document version, embedding model,
  dimension, document kind and the ordered event/text-hash manifest.
- Preserved the existing `search_v3_hash` / `related_v1_hash` compatibility
  fields while adding top-level `catalog_revision`, `corpus_revision`,
  `search_document_revision` and the structured `revision_contract` report.
- Persisted the revision identities in `event_search_documents.metadata` and
  `event_embeddings.metadata`. Unchanged embeddings receive a metadata-only
  PATCH, so a zero-provider-call reconciliation still advances the revision
  snapshot consumed by the server/cache contract.
- Authoritative `--prune-missing` sync now reads a fresh, deterministic complete
  inventory, removes exact stale/orphan/wrong-model/wrong-dimension rows, reads
  the inventory again and emits `event_search_projection_coverage_v1`.
- The terminal coverage receipt proves document and embedding percentages plus
  missing, stale, orphan, wrong-model/dimension and wrong-document-kind counts.
  `--require-complete` fails unless provider-call gaps are zero and the stored
  coverage receipt is terminal `complete`.
- No LLM was called and the configured embedding provider/model was not changed.

## Integration contract

The server-contract lane confirmed that its revision snapshot reads exactly:

- `event_search_documents.metadata.catalog_revision`;
- `event_search_documents.metadata.search_document_revision`;
- `event_embeddings.metadata.corpus_revision`, filtered by the configured
  active/public/searchable model/dimension/document-kind contract.

For Search, top-level `corpus_revision == search_v3_hash`. The related corpus has
its own `related_v1_hash` / `embedding_corpus_revision`.

## Verification

Focused regression suite:

```text
PYTHONPATH="$PWD:/home/dev/projects/events-bot-new" \
  /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q \
  tests/test_event_search_corpus_revision.py \
  tests/test_static_site_public_gate.py \
  tests/test_static_site_pgvector_export.py \
  tests/test_event_vector_sync.py
```

Result: `58 passed in 1.18s`.

Compilation and diff gate:

```text
PYTHONPATH="$PWD:/home/dev/projects/events-bot-new" \
  /home/dev/.venvs/events-bot-image-geometry/bin/python -m py_compile \
  scripts/sync_event_search_vectors_to_supabase.py \
  site/scripts/export-production-preview-data.py
git diff --check
```

Result: passed.

Current checked static fixture compatibility (no apply/provider call):

```text
scripts/sync_event_search_vectors_to_supabase.py \
  --preview-events-json site/src/data/preview-events.json \
  --max-provider-calls 0 --report-json /tmp/l4-corpus-plan.json
```

Result: `288` events; two repeated runs emitted identical revisions:

- catalog: `b6151fc8f8ca71e8dcfcf5dce7120a5278d2655e340452c8c5a5cc4337d78028`
- Search corpus: `e3e9411c7c18109a87eb9ff584c2a2461e3af1720340f614399104736952a8cf`
- related corpus: `749230afa9b0fe0813f39fcd102c41f1fed91d44d3f67bdef0e6938b0cd60a5f`
- model/dimension: `gemini-embedding-2` / `768`
- document versions: `event-search-doc-v3-search-facets` /
  `event-related-doc-v1`

Broader related suite result: `137 passed, 1 failed`. The single failure was
`tests/test_static_site_build_handoff.py::test_runner_adopts_exact_complete_output_without_push`;
it failed before its assertion because the unrelated host `runtime_scratch_health`
reported root scratch `status=critical`. No L4-owned call path was in the trace.

Supabase source-of-truth checks included the official changelog and official
Data REST/filter documentation; current `eq` filters remain valid for exact
filtered delete/PATCH operations.

## Risks / handoff

- A live authoritative `--apply --prune-missing --require-complete` run was not
  executed in this lane because it can invoke the embedding provider for stale
  documents. The parent/integrator owns that controlled production run and its
  sanitized receipt.
- The first authoritative run can delete legacy incompatible embedding rows.
  Deletes are exact on the composite identity, and the sync re-reads inventory
  before declaring completion.
- Canonical docs and `CHANGELOG.md` were explicitly outside this lane and remain
  an integration responsibility.

## Changed files

- `scripts/sync_event_search_vectors_to_supabase.py`
- `site/scripts/export-production-preview-data.py`
- `tests/test_event_search_corpus_revision.py`
- `.codex/lanes/L4-corpus-revision/RESULTS.md`
