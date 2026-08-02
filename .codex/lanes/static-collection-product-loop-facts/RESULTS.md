# Lane results: static-collection-product-loop-facts

Follow-up: the exact-cohort contract is extended by the optional hash-bound
boundary manifest documented in
`../static-collection-product-loop-facts-boundary-manifest/RESULTS.md`; seed-only
behavior remains unchanged.

## Identity

- Lane ID: `static-collection-product-loop-facts`
- Requirement IDs: `R3`, `R4`, `R5`, `R6`
- Base SHA: `dded795fe1dc87acf4f17fff1b4e3d67f093b000`
- Implementation head SHA: `4accbdb41aa3fb62a05d4e594b145bca7f14b6de`
- Branch: `agent/static-collection-product-loop/facts-gate-ci-router`
- Push: intentionally not performed

## Result

Implemented a pure offline Gate-B evaluator and versioned output schema. It
binds the primary-only evaluate report to the corrected provisional seed,
source-review index/receipts, immutable SQLite bytes and exact repository SHA;
checks source/event/text/quote provenance, zero writes, exact cohorts, one
logical/physical send maximum and no fallback; then reports occurrence-family
runtime outcomes and review classifications. Only high/keep/sufficient positive
families enter the `0.80` recall denominator. Confirmed hard negatives and
safety/provenance errors block copy gates. Semantic publication is always
`blocked`.

Smart Update and backfill recall routing now both recognize normalized
`всей семьей` / `всей семьёй`; generic `приходите` and age-only copy remain
unrouted. The static-collections workflow has affected-scope triggers for core,
evaluator/schema and the exact five tests, runs them without a real LLM, and the
final unified gate requires that job.

## Verification evidence

- Exact required suite:
  - command: `/home/dev/.venvs/events-bot-region-talk/bin/pytest -q tests/test_smart_event_update.py tests/test_event_update_merge.py tests/test_google_ai_client.py tests/test_static_collection_backfills.py tests/test_static_collection_facts_backfill_report.py`
  - result: `133 passed in 9.19s`
- Evaluator syntax:
  - command: `python3 -m py_compile scripts/evaluate_static_collection_facts_v3_gate_b.py`
  - result: PASS
- Versioned schema:
  - command: `python3 -m json.tool docs/review-data/static_collection_facts_v3_gate_b_report.schema.json`
  - result: PASS
  - Draft 2020-12 `check_schema`: PASS
- Workflow syntax:
  - PyYAML `safe_load(.github/workflows/static-collections-quality-e2e.yml)`
  - result: PASS
- Diff hygiene:
  - command: `git diff --check`
  - result: PASS

Regression coverage includes denominator eligibility/WATCH rows, confirmed hard
negatives, exact quote/source mismatch, provider-deferred vs validator-reject,
source-insufficient/seed-conflict/model-miss classifications, `4/5` PASS,
`3/5` BLOCKED, report/seed/index/receipt/source-ref/text/DB/repo/cohort hashes,
and missing generator provenance.

## Changed files

- `.codex/lanes/static-collection-facts-v3/LANE_MAP.yml`
- `.github/workflows/static-collections-quality-e2e.yml`
- `CHANGELOG.md`
- `docs/features/static-site-pages/static-collections-smart-update-facts-v3-implementation.md`
- `docs/features/static-site-pages/static-collections-smart-update-facts-v3-real-data-acceptance.md`
- `docs/operations/e2e-scenarios.md`
- `docs/operations/static-collection-facts-v3.md`
- `docs/review-data/static_collection_facts_v3_gate_b_report.schema.json`
- `scripts/backfill_static_collection_facts.py`
- `scripts/evaluate_static_collection_facts_v3_gate_b.py`
- `smart_event_update.py`
- `tests/test_smart_event_update.py`
- `tests/test_static_collection_backfills.py`
- `tests/test_static_collection_facts_backfill_report.py`
- `.codex/lanes/static-collection-product-loop-facts/RESULTS.md`

## Risks / boundaries

- No real provider call, production snapshot replay, copy apply, deploy or live
  canary was performed in this lane. The evaluator and CI contract are ready for
  the integration lane to consume real artifacts.
- No prompt/semantic inference, owner gold, scoring/threshold calibration,
  Astro/public routes, navigation, sitemap, cinema or festival work was added.
- Passing Gate B permits only subsequent bounded copy gates; it never permits
  semantic publication.
