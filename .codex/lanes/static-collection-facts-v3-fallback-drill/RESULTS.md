# static-collection-facts-v3 fallback/failure drill — results

## Identity

- Lane: `static-collection-facts-v3-fallback-drill`
- Requirement: R8 offline reproducible fallback/failure harness
- Base SHA: `c625f1809ee60eaf62c16f079f701adb6a9847f1`
- Branch: `agent/static-collection-facts-v3/fallback-drill`
- Production semantics changed: **no**

## Delivered

- Added `scripts/run_static_collection_facts_v3_fallback_drill.py`.
- Both provider boundaries are injected locally; the command cannot make a
  real provider request and reports `real_provider_calls=0`.
- Every scenario forces exactly one primary Gemma physical send/failure and
  permits at most one call through the existing `ask_4o` fallback boundary.
- The production trace records `models/gemma-4-31b-it -> gpt-4o` and the exact
  physical-send counts.
- A valid fallback result passes the unchanged production adjudicator, strict
  source-quote validator and `apply_collection_decisions`.
- Malformed JSON and exact-evidence mismatch are rejected before apply.
- Total provider unavailability abstains; accepted existing truth and the
  whole decision mapping stay byte-logically unchanged.
- The versioned JSON report explicitly carries
  `gate_claim=offline_harness_only_real_gate_c_not_claimed` and keeps semantic
  publication blocked.

## Deterministic verification

```bash
export PYTHONPATH=/home/dev/.codex/worktrees/events-bot-new/static-collection-facts-v3
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest \
  tests/test_static_collection_facts_fallback_drill.py -q
# 5 passed in 0.19s

TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python \
  scripts/run_static_collection_facts_v3_fallback_drill.py \
  --generated-at 2026-08-02T00:00:00Z \
  --output /dev/shm/static-collection-facts-v3-fallback-drill.json
# status=pass; 4/4 cases; max primary/fallback sends=1/1;
# real_provider_calls=0
```

`PYTHONPATH` was needed only because this lane used a disk-saving sparse
worktree and borrowed untouched dependency files from the full integration
worktree. The committed command needs no such override in a normal checkout.

Additional focused run:

```text
tests/test_static_collection_facts_fallback_drill.py +
tests/test_smart_event_update.py + tests/test_event_update_merge.py:
38 passed, 2 unrelated sparse-worktree failures.
```

The two failures were not caused by this lane: the sparse checkout omitted the
Alembic revision file, and an existing location-region integration fixture
resolved differently with dependency files borrowed from another worktree.
The new drill suite and the existing Smart Update fallback tests passed.

## Files

- `scripts/run_static_collection_facts_v3_fallback_drill.py`
- `tests/test_static_collection_facts_fallback_drill.py`
- `docs/operations/static-collection-facts-v3.md`
- `docs/features/static-site-pages/static-collections-smart-update-facts-v3-real-data-acceptance.md`
- `CHANGELOG.md`
- `.codex/lanes/static-collection-facts-v3-fallback-drill/RESULTS.md`

## Boundary / remaining gate

No real GPT-4o/Gemma call, production snapshot, database write, ingestion,
deploy, product adapter or Gate-B evaluator change was made. This harness is a
prerequisite regression check only. After a corrected final-SHA Gate B passes,
the separate 3–5 real-source Gate C is still mandatory.
