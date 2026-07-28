# R15-L03-BUILDER results

- **Status:** Done
- **Requirement IDs:** R03, R09
- **Branch:** `agent/unusual-r15/builder-share`
- **Base SHA:** `31b72b93153c094ca16cd564bfdc6b56c2031867`
- **Implementation head SHA:** `65ba1e7bfac615410717cff513c0fe5e6e4129f4`

## Scope contract

Writable scope was limited to the StaticSiteBuilder/Kaggle handoff, exporter,
release validation, service-share renderer and their focused tests/configuration.
Astro pages/components/menu/EventLayout, Supabase migrations, canonical docs and
`CHANGELOG.md` were forbidden and were not edited.

## Requirement outcome

| Requirement | Status | Evidence |
|---|---|---|
| R03 | Done | Deterministic Pillow-only 1080×1350 daily service-share renderer selects from the current exported catalog, writes content-addressed immutable PNG/WebP/version manifest, then atomically replaces `service-share/current/manifest.json`. The existing coalesced `JobTask.static_site_build` path calls it after export; the existing calendar-rollover/startup-catch-up path is made explicit as `STATIC_SITE_LOCAL_MIDNIGHT=00:00`. Manifest freshness is exactly 24 hours and release validation checks dimensions, hashes, readiness and local date. |
| R09 | Done | BGE candidate mode invokes one pinned CPU `related_v1` artifact build and reuses that same in-memory event/prototype vector artifact for BGE related graph and unusual scoring. NPZ/JSON vector receipt plus unusual cache/last-good state are atomic and fail closed on contract/hash/partial mismatch. Frozen quality fixture cases missing from the public catalog are included in that single encode, and `evaluate_unusual_quality_fixture` supplies derived gate evidence. Provider calls are fixed/validated at zero; migration notification eligibility is always false. |

## Implementation details

- Expected L02 semantic API is bound narrowly through:
  - `build_shared_bge_vector_artifact`
  - `validate_shared_bge_vector_artifact`
  - `load_unusual_prototype_bank`
  - `load_unusual_classifier`
  - `score_unusual_manifest`
  - `evaluate_unusual_quality_fixture`
- Persistent state paths are configurable and default to:
  `static_event_bge_vectors.npz`,
  `static_event_bge_vectors.receipt.json`,
  `unusual_events_cache.json`, and
  `unusual_events_last_good.json`.
- Required unusual stage logs are emitted, including vector reuse, prototype
  load, scoring, quality gate, concept dedup, manifest/cache writes, disabled
  feed, and compatible last-good fallback.
- `static-semantic-build-result.json`, builder result, release receipt and input
  fingerprint carry semantic/service-share provenance and validation evidence.
- Production governance is intentionally unchanged: `fly.toml` keeps
  `STATIC_SITE_RELATED_MODE=pgvector`, vector barrier/sync enabled, and unusual
  disabled. BGE/unusual settings are inert candidate configuration only. The
  independent Gemini `search_v3`/pgvector synchronization rail remains intact.

## Changed files

- `.env.example`
- `fly.toml`
- `kaggle/StaticSiteBuilder/service_share_card.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `main.py`
- `scripts/run_static_site_builder_kaggle.py`
- `site/scripts/export-production-preview-data.py`
- `static_site_release.py`
- `tests/test_static_site_service_share_daily.py`
- `tests/test_static_site_unusual_builder_adapter.py`
- `.codex/lanes/R15-L03-BUILDER/RESULTS.md`

## Commands and evidence

```text
python3 -m py_compile \
  kaggle/StaticSiteBuilder/service_share_card.py \
  kaggle/StaticSiteBuilder/static_site_builder.py \
  scripts/run_static_site_builder_kaggle.py \
  site/scripts/export-production-preview-data.py \
  main.py static_site_release.py

/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_static_site_build_handoff.py \
  tests/test_static_site_release.py \
  tests/test_static_site_pgvector_export.py \
  tests/test_static_site_service_share_daily.py \
  tests/test_static_site_unusual_builder_adapter.py

git diff --check
```

Result: **60 passed**, compile and whitespace checks passed.

A local deterministic renderer smoke produced manifest
`service_share_asset_manifest_v2`, a `1080×1350` PNG/WebP pair, immutable
version `20260727-a9f4e6e3cb279c3a`, and freshness window
`2026-07-27T08:00:00Z` → `2026-07-28T08:00:00Z`. The rendered image was
visually inspected for hierarchy, clipping and footer/card bounds.

## Risks and integration gates

- This lane did not execute the full real BGE-M3 CPU canary because L02 semantic
  modules and the L06 frozen fixture live on separate integration commits. The
  integrator must merge L02/L06 first, then run the Kaggle CPU candidate build;
  the adapter will fail closed rather than manufacture approval evidence.
- Production BGE/unusual cutover is explicitly out of scope and remains gated by
  the frozen real-BGE evaluation and owner approval.
- A full Astro catalog build was not run in this lane; the coalesced builder and
  release handoff are covered by focused Python contracts.
- Canonical documentation and `CHANGELOG.md` remain the integration/docs lane's
  responsibility under the declared forbidden-file scope.

## Merge notes

Merge implementation commit `65ba1e7bfac615410717cff513c0fe5e6e4129f4`
plus the following results-only metadata commit after L02/L06 to satisfy the
semantic imports and golden fixture path.
