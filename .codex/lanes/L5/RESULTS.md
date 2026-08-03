# L5 Collections results

Branch: `agent/static-unified/l5-collections`
Base: `0bc8482dc` (`origin/main`)

`origin/agent/static-collections-gastronomy-data-prep` was inspected before implementation. It resolved to the same SHA as the base and had an empty diff, so there was no separate patch to transplant.

## Requirement status

- **R1 Done** — added checked `static-collection-registry-v1` and `/podborki/` catalog.
- **R2 Done** — registry validates `public`, `repair`, `blocked`, and `deferred`; current unreviewed gastronomy is deliberately `repair`.
- **R3 Done** — catalog, mobile collection navigation, and sitemap consume registry projections; blocked entries are excluded, and dormant gastronomy is dynamically excluded.
- **R4 Done** — added `gastronomy_v1` checked decisions and exact-ID manifest with `active` (3+), `low_supply` (1–2), `recent_empty` (0 future/recent), and `dormant` states.
- **R5 Done** — generator deduplicates by explicit occurrence family, validates catalog/hash parity and zero provider calls, and retains only a valid catalog-compatible last-good manifest after partial/failed input.
- **R6 Done** — `/podborki/gastronomiya/` resolves event cards exclusively by manifest IDs, groups future cards by month, separates recent accepted cards, and has no prose/topic membership classifier.
- **R7 Done** — missing owner audit is a blocked technical state rather than an approved empty result; no remote writes or paid providers were used.
- **R8 Done** — policies, canonical feature docs, unit/contract tests, lane map, and this report were updated. `CHANGELOG.md`, `docs/routes.yml`, and `EventLayout.astro` were intentionally not touched because they are owned by the parent integration lane.

## Validation

- `node --test site/tests/static-collection-registry.test.mjs site/tests/reference4-collections-menu.test.mjs` — **4 passed**.
- `pytest -q tests/test_gastronomy_collection_manifest.py` — **6 passed**.
- `pytest -q tests/test_static_collection_export.py -k semantic_candidates_remain_blocked_in_valid_batch` — **1 passed, 7 deselected**.
- `python3 -m py_compile ...` and `git diff --check` — **passed**.
- Manifest CLI regeneration compared byte-for-byte with the checked blocked manifest — **reproducible**.
- `npm run build:preview` — **passed**, 460 pages built; both `/podborki/` and `/podborki/gastronomiya/` emitted.
- `npm run check:preview` — **passed**, 288-event fixture.
- Built HTML verification — gastronomy is `noindex`, `data-publication-status=blocked`, `data-collection-state=blocked`; blocked Kids is absent from catalog; repair gastronomy is absent from sitemap.

A wider `tests/test_static_collection_export.py` run reached 13 passes and one environment-only failure because that available Python venv did not include NumPy. The modified batch test was rerun alone and passed.
