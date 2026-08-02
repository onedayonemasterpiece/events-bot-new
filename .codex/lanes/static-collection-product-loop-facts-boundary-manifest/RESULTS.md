# Lane results: static-collection-product-loop-facts-boundary-manifest

## Identity

- Lane ID: `static-collection-product-loop-facts-boundary-manifest`
- Requirement ID: `R3-followup`
- Base SHA: `9cc8fbaa9da9c2c769c938e186f96c5b0b7ac2c1`
- Implementation head SHA: `b19b79c6d583a322a38435ba7e26e963658bb659`
- Branch: `agent/static-collection-product-loop/facts-gate-ci-router`
- Push: intentionally not performed

## Result

Gate-B now accepts an optional versioned `--boundary-manifest`. The manifest
binds the exact corrected seed file SHA and each named boundary's event ID,
EventSource ID and source-text SHA. The only accepted runtime cohort is exactly
the union of seed and manifest bindings; omitted and unlisted execution rows
remain blocking stale-cohort errors.

Boundary rows are excluded from the high/keep/sufficient family-recall
denominator. A `not_confirmed` row confirmed by runtime is a hard NO-GO.
Disagreement with `watch` or `confirmed_watch` is emitted as a categorized WATCH
warning and does not block copy gates. JSON and Markdown reports expose each
boundary outcome/classification and summary counts. Semantic publication remains
blocked.

## Verification

- Exact five-file suite: `137 passed in 10.44s`.
- Focused evaluator/report suite after final duplicate-contract tightening:
  `29 passed in 4.91s`.
- `python3 -m py_compile scripts/evaluate_static_collection_facts_v3_gate_b.py`: PASS.
- Both JSON schemas parse and pass Draft 2020-12 `check_schema`: PASS.
- Workflow YAML parse: PASS.
- `git diff --check`: PASS.

Regression coverage proves: bound extra accepted; unbound extra rejected; hard
boundary confirmation blocked; WATCH disagreement non-blocking and excluded
from recall.

## Changed files

- `.codex/lanes/static-collection-facts-v3/LANE_MAP.yml`
- `.codex/lanes/static-collection-product-loop-facts/RESULTS.md`
- `.github/workflows/static-collections-quality-e2e.yml`
- `CHANGELOG.md`
- `docs/features/static-site-pages/static-collections-smart-update-facts-v3-implementation.md`
- `docs/features/static-site-pages/static-collections-smart-update-facts-v3-real-data-acceptance.md`
- `docs/operations/static-collection-facts-v3.md`
- `docs/review-data/static_collection_facts_v3_boundary_manifest.schema.json`
- `docs/review-data/static_collection_facts_v3_gate_b_report.schema.json`
- `scripts/evaluate_static_collection_facts_v3_gate_b.py`
- `tests/test_static_collection_facts_backfill_report.py`
- `.codex/lanes/static-collection-product-loop-facts-boundary-manifest/RESULTS.md`

## Risks / boundary

No real provider, production snapshot, apply, deploy or live canary was run or
claimed. The integration lane must construct the manifest from real corrected
EventSource bindings and run the offline evaluator. No prompt, model route,
scoring, Astro/publication, cinema or festival work changed.
