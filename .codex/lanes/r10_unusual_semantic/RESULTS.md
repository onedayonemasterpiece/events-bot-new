# R10 unusual semantic regressions — results

## Lane contract

- Requirement: R10 / Telegram `#783–784`, `#788–789`.
- Base branch: `origin/integration/static-site-focus-r15-live-e2e-20260728`.
- Base SHA: `64dd872d795f6859179f80ca42c387da7b811504`.
- Branch: `agent/focus-group/unusual-semantic-r10-20260728`.
- Implementation SHA: `016f95346f3c2831921128793dd8d0480d16042e`.
- Scope stayed limited to unusual-event semantic fixtures and focused
  evaluator/shared-document tests. Runtime classifier code, canonical docs and
  `CHANGELOG.md` were not changed.

## Outcome

- Added a supplemental, source-bound hard-negative fixture for:
  - event `5376`, an ordinary civic/commemorative youth action;
  - event `4327`, an ordinary archival exhibition.
- Each case records the confusable unusual families, insufficient thematic
  signals, and the concrete experience mechanism that would need to be stated
  before an unusual classification was justified.
- The fixture is checked against the exact semantic input fields currently in
  `site/src/data/preview-events.json`, so future source drift requires explicit
  re-adjudication.
- Tests build the existing shared `event-related-doc-v1` documents rather than
  introducing keywords or regex rules.
- A deterministic, non-production semantic probe routes both full documents to
  existing prompt/prototype anchors and runs
  `evaluate_unusual_quality_fixture(...)`. Both cases are counted as eligible
  hard negatives, remain `ordinary`, have false-positive rate `0.0`, and
  produce no unusual ranked rows.

## Commands and evidence

Baseline:

```bash
/home/dev/projects/events-bot-new/artifacts/codex/unusual-r15/venv/bin/python \
  -m pytest -p no:cacheprovider -q \
  tests/test_unusual_events_golden_contract.py \
  tests/test_unusual_event_semantics_r15.py
```

Result: `23 passed`.

Focused regression:

```bash
/home/dev/projects/events-bot-new/artifacts/codex/unusual-r15/venv/bin/python \
  -m pytest -p no:cacheprovider -q \
  tests/test_unusual_event_semantic_regressions.py
```

Result: `3 passed`.

Combined semantic suite:

```bash
/home/dev/projects/events-bot-new/artifacts/codex/unusual-r15/venv/bin/python \
  -m pytest -p no:cacheprovider -q \
  tests/test_unusual_events_golden_contract.py \
  tests/test_unusual_event_semantics_r15.py \
  tests/test_unusual_event_semantic_regressions.py
```

Result: `26 passed`.

Additional checks:

```bash
/home/dev/projects/events-bot-new/artifacts/codex/unusual-r15/venv/bin/python \
  -m py_compile tests/test_unusual_event_semantic_regressions.py
python3 -m json.tool \
  tests/fixtures/unusual_events_semantic_regressions_v1.json >/dev/null
git diff --check
```

All passed.

## Changed files

- `tests/fixtures/unusual_events_semantic_regressions_v1.json`
- `tests/test_unusual_event_semantic_regressions.py`
- `.codex/lanes/r10_unusual_semantic/RESULTS.md` (evidence-only follow-up)

## Limitations and risks

- The new evaluator run is explicitly a `non_production_probe` with controlled
  vectors. It proves fixture wiring, shared-document construction, evaluator
  accounting and the intended semantic boundary; it is not fresh real-BGE
  evidence.
- The calibrated `unusual_events_golden_v1.json` was deliberately left
  unchanged because it is hash-bound to the existing classifier calibration.
  Promoting these cases into the release gate requires a new pinned-BGE canary
  and honest recalibration/approval rather than editing the calibration hash by
  hand.
- No provider call, production build, push or deployment was performed.
