# R01/R07 semantic metrics results

- **Status:** Done
- **Requirement IDs:** R01, R07
- **Branch:** `feat/unusual-semantic-metrics`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/unusual-semantic-metrics`
- **Base SHA:** `7598de224e64659c31325c9f5bc1c39f03c4e6ff`
- **Implementation head SHA:** `925ab9083e9ef8a858600d7422a64d2c00b1e5ce`

## Outcome

- Exact editorial precision@20 is now unavailable unless `k`, ranked count,
  and denominator are all exactly 20; partial ranked samples cannot satisfy
  the approval gate.
- Precision output now includes numerator, denominator, k, sample status, and
  a 95% Wilson interval for complete samples.
- Added expected-family top-1 accuracy, top-3 recall, a complete 15x15
  confusion matrix, taxonomy coverage, and label coverage. All rate metrics
  carry explicit numerators and denominators.
- Kept expected-family metrics observational because the canonical classifier
  config defines no expected-family thresholds. The existing configured
  family-diversity gate remains unchanged.
- Added regression coverage for the former partial-P@20 defect, exact sample
  evidence, Wilson bounds, family ranking metrics, coverage, confusion counts,
  and the absence of invented family gates.

## Changed files

- `site/scripts/unusual_event_semantics.py`
- `tests/test_unusual_event_semantics_r15.py`
- `.codex/lanes/impl_semantic_metrics/RESULTS.md` (lane evidence only)

## Commands and tests

Red-phase regression evidence:

```text
uv run --with-requirements requirements.txt python -m pytest -q \
  tests/test_unusual_event_semantics_r15.py \
  -k 'quality_fixture_deduplicates or exact_p20'
```

Result before implementation: `2 failed, 18 deselected`; the old evaluator
reported `1.0` from one ranked result and did not emit the new evidence fields.

Final focused verification:

```text
python3 -m py_compile \
  site/scripts/unusual_event_semantics.py \
  tests/test_unusual_event_semantics_r15.py

uv run --with-requirements requirements.txt python -m pytest -q \
  tests/test_unusual_event_semantic_regressions.py \
  tests/test_unusual_event_semantics_r15.py \
  tests/test_unusual_events_golden_contract.py

git diff --check
```

Result: `27 passed`, `0 failed`; compile and diff checks passed.

## Risks

- Expected-family rates intentionally include every positive fixture row with
  a canonical `expected_family`, including rows that are not publish-eligible;
  label coverage separately exposes unlabeled positive rows.
- A complete 15x15 zero-filled confusion matrix increases evaluation payload
  size modestly but keeps absent taxonomy cells explicit and deterministic.
- Canonical documentation and `CHANGELOG.md` were forbidden by lane scope and
  must be synchronized by the integration/documentation owner.
