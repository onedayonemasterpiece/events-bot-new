# W2 Results — compact static related egress

## Scope

- Lane: `W2`
- Requirement: `R04`
- Base SHA: `cc7c213f5e49173b24029b00dabb1359c1f1059b`
- Implementation head SHA: `ba3e6f53`
- Branch: `agent/static-site-resilient-egress/W2`

## Result

Implemented the fail-closed compact pgvector retrieval path for static related
rebuilds:

- added per-anchor RPC `event_related_candidates_compact_by_event_id_v1` with
  exact `event_id`/`vector_similarity` projection;
- revoked `PUBLIC`, `anon` and `authenticated`; granted only `service_role`;
- switched the exporter away from the wide legacy RPC with no fallback;
- bounded each response before JSON decode (default 256 KiB) and bounded full
  rebuild aggregate response bytes (default 16 MiB);
- recorded request, row, aggregate-byte and maximum-response counters in both
  `preview-related.json` and Kaggle `static_site_build_result.json`;
- kept valid cache hits at zero Supabase related retrieval calls;
- rejected duplicate anchor ids before retrieval, and preserved whole-graph
  recomputation (no unsafe changed-anchor-only update);
- kept the full Astro build unchanged.

## Evidence

Commands run from the W2 worktree:

```text
uv run --with-requirements requirements.txt --with numpy pytest -q \
  tests/test_static_site_pgvector_export.py \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_builder_preview_contract.py \
  tests/test_static_site_unusual_builder_adapter.py
# 55 passed in 3.67s

uv run --with-requirements requirements.txt --with numpy pytest -q \
  tests/test_static_site_content_projection.py \
  tests/test_event_vector_sync.py \
  tests/test_static_site_public_gate.py
# 39 passed in 1.32s

cd site && npm ci --no-audit --no-fund && npm run build
# 466 pages built; completed successfully in 2m36s

python3 -m py_compile \
  site/scripts/export-production-preview-data.py \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  scripts/run_static_site_builder_kaggle.py
# passed

uv run --with pglast python3 -c '<parse migration>'
# pglast-ok

git diff --check
# passed
```

Test coverage includes golden graph parity with the legacy wide row shape,
pre-decode oversized-body rejection, aggregate ceiling rejection, exact narrow
projection, counters, build-receipt propagation, zero-call cache hit and
pre-request duplicate-anchor rejection.

`npx supabase@2.111.0 migration list --local` could not connect because this
worktree has no running local Supabase/Postgres at `127.0.0.1:54322`. The SQL
was still parsed with PostgreSQL `pglast`; live migration application belongs to
integration/deploy.

## Risks / follow-up

- The migration must be applied before a pgvector static rebuild using this
  commit; the exporter intentionally does not fall back to the wide RPC.
- The 97.45% historical egress reduction remains an audit estimate until the
  first live full rebuild receipt records real response bytes.
- No batch RPC was introduced: per-anchor retrieval was retained to preserve
  exact HNSW top-K semantics safely.
- No production deploy or push was performed in this lane.

## Changed files

- `supabase/migrations/20260731174929_compact_static_related_candidates_v1.sql`
- `site/scripts/export-production-preview-data.py`
- `tests/test_static_site_pgvector_export.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `scripts/run_static_site_builder_kaggle.py`
- `docs/operations/kaggle-static-site-builder.md`
- `CHANGELOG.md`
- `.codex/lanes/W2/RESULTS.md`
