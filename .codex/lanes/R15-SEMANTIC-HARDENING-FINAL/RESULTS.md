# R15 Semantic Hardening Final — Results

## Lane contract

- Lane ID: `R15-SEMANTIC-HARDENING-FINAL`
- Base SHA: `11d8c9846432414020cc5201eb650f5cfbf38eba`
- Implementation head SHA: `23ec1057b04bcadf795806700859375484f30508`
- Requirement IDs: `R15-NOTIFY`, `R15-ORDINARY-CORPUS`, `R15-ELIGIBILITY`, `R15-DIVERSITY`
- Writable implementation/test files:
  - `site/scripts/unusual_event_semantics.py`
  - `site/scripts/export-production-preview-data.py`
  - `tests/test_unusual_event_semantics_r15.py`
  - `tests/test_static_site_unusual_builder_adapter.py`
  - `tests/test_unusual_events_golden_contract.py`
- Forbidden: all other product, test, documentation, and changelog files. This result record is the required lane metadata exception.

## Delivered

- Preserved concept-level `notify_eligible` across unchanged normal rebuilds while keeping migration/backfill output silent and leaving per-user seen suppression to browser-local state.
- Added a mandatory, policy-hash-bound distance-to-ordinary-event-corpus feature using only the existing shared BGE event vectors. Receipts bind member IDs, text hashes, vector hashes, corpus hash, model/revision/dimension/document contract, and record zero provider calls.
- Made scorer eligibility fail closed on the exact structured semantic projection. The exporter now projects canonical Event identity, merge, silence, lifecycle, public/searchable, eventness, and publication facts; incomplete legacy rows become explicitly untrusted.
- Removed deferred-fill behavior that bypassed family/venue/type caps and exposed cap/deferred metrics.
- Added deterministic focused coverage for the ordinary-corpus receipt, missing structured fields, diversity caps, and notification persistence/migration behavior.

## Commands and evidence

```text
PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_unusual_event_semantics_r15.py \
  tests/test_static_site_unusual_builder_adapter.py \
  tests/test_unusual_events_golden_contract.py
28 passed in 2.15s
```

```text
PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -p no:cacheprovider -q \
  tests/test_static_site_content_projection.py \
  tests/test_static_site_public_gate.py \
  tests/test_static_site_release.py \
  tests/test_static_site_unusual_builder_adapter.py \
  tests/test_unusual_event_semantics_r15.py \
  tests/test_unusual_events_golden_contract.py
71 passed in 4.34s
```

```text
PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/python -m py_compile \
  site/scripts/unusual_event_semantics.py \
  site/scripts/export-production-preview-data.py
passed

git diff --check
passed
```

## Changed files

- `site/scripts/unusual_event_semantics.py`
- `site/scripts/export-production-preview-data.py`
- `tests/test_unusual_event_semantics_r15.py`
- `tests/test_static_site_unusual_builder_adapter.py`
- `.codex/lanes/R15-SEMANTIC-HARDENING-FINAL/RESULTS.md` (required lane record)

`tests/test_unusual_events_golden_contract.py` was exercised but did not require changes.

## Risks / follow-up

- The new policy intentionally invalidates prior quality evaluations: the quality gate now requires the ordinary-corpus policy hash and remains shadow-only until a fresh real-BGE canary evaluation is produced.
- An existing real-BGE artifact was inspected read-only, but its event document hashes do not fully match the current export, so it was not misrepresented as fresh acceptance evidence. A current canary requires the normal shared-vector builder to re-encode changed documents.
- No production deploy or mutable external operation was performed in this lane.
