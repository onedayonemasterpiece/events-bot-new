# Lane RECOVERY-CENSUS Results

## Status

committed

## Requirement IDs

- R07
- R19
- R20
- R23
- T66
- T67
- T68
- T69
- T70

## Branch

`agent/smart-update-llm-first/recovery-census`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/smart-update-recovery-census`

## Base SHA

`8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`

## Head SHA

Implementation commit: `55c5e7496`.

The branch handoff tip also contains this RESULTS-only metadata commit; use
`git rev-parse agent/smart-update-llm-first/recovery-census` as the final lane
head (reported to the integrator at handoff).

## Files changed

- `scripts/ops/recover_smart_update_identity_losses.py`
- `scripts/ops/smart_update_prod_audit.py`
- `scripts/ops/smart_update_loss_census.py`
- `scripts/ops/rehearse_smart_update_migration.py`
- `tests/test_recover_smart_update_identity_losses.py`
- `tests/test_smart_update_loss_census.py`
- `tests/test_smart_update_migration_rehearsal.py`
- `.codex/lanes/RECOVERY-CENSUS/RESULTS.md`

## Delivered

- Pure deterministic A–T classifier with earliest-loss ordering, definitive
  success override, immutable carrier+revision aliases and safe class `T` for
  unavailable evidence.
- Carrier counts are distinct from extracted occurrence and lifecycle-action
  counts. Duplicate evidence for the same revision cannot double-count.
- Read-only census feature-detects planned `vk_source_packet`, append-only
  `vk_source_packet_attempt`, `vk_crawl_continuation`, legacy VK/Telegram,
  ticket/festival queues, Smart Update child state, parser observations and
  explicit offline Supabase/miss evidence without CI network access.
- Deterministic February–July source/month/loss-class sampling publishes exact
  denominators and coverage and explicitly forbids multiplying
  `vk_misses_sample`.
- Recovery supports half-open `--since`/`--until`, source/loss filters,
  `--include-discovery-misses`, offline evidence inputs and explicit
  `--read-only` mutually exclusive with `--apply`.
- Recovery plan order is raw payload → attachments/OCR → typed LLM decision →
  Smart Update. It contains no direct Event insert, and inventory/plan hashes
  exclude runtime timestamps/run IDs.
- Snapshot rehearsal byte-reads the original main/WAL/SHM bundle, clones via
  SQLite backup, proves query-only rejection, runs full `Database.init()`
  twice, checks counts/schema/indexes/triggers/conflicts, runs census/recovery
  read-only on the clone, rehearses rollback compatibility and ends with
  `quick_check=ok`. All mutations are clone-confined and conflicts fail closed.

## Commands run

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m py_compile \
  scripts/ops/recover_smart_update_identity_losses.py \
  scripts/ops/smart_update_loss_census.py \
  scripts/ops/rehearse_smart_update_migration.py \
  scripts/ops/smart_update_prod_audit.py

/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_recover_smart_update_identity_losses.py \
  tests/test_smart_update_loss_census.py \
  tests/test_smart_update_migration_rehearsal.py \
  tests/test_smart_update_prod_audit.py

PYTHONPATH=. /home/dev/.venvs/events-bot-region-talk/bin/python - <<'PY'
# Created an isolated minimal SQLite snapshot in TemporaryDirectory and called
# rehearse_smart_update_migration.run() with the real Database.init initializer.
PY

git diff --check
```

## Tests / verification

- Focused suite: `32 passed in 1.23s`.
- Real `Database.init()`-twice smoke on an isolated snapshot: `status=passed`,
  `database_init_runs=2`, original unchanged, final `quick_check=["ok"]`,
  count changes empty, all conflict counters zero.
- Static direct-write probe: no `INSERT INTO event` in census/recovery/rehearsal
  tooling.
- Tests prove discovery and prefilter misses, partial-child inventory,
  carrier/occurrence separation, deterministic dry-run hashes, half-open time
  boundary, source/loss filters, query-only write rejection, original DB byte
  and row preservation, and conflict fail-closed behavior.
- `git diff --check`: pass.
- No production or network access; no push.

## Risks

- Pre-migration parser OpsRun rows expose aggregate failure observations but no
  immutable raw carrier IDs. The census reports carrier inventory unavailable
  instead of fabricating replay counts; deployment/raw evidence is required.
- Supabase discovery misses require a supplied offline JSON export. No sampling
  extrapolation is performed.
- The RAW lane may adjust planned column names. Detection is additive and
  fail-safe, but the integrator should reconcile exact names after merging that
  lane.
- Existing `--apply` behavior remains limited to durable requeue/state
  transitions; it was exercised only against temporary test DBs. This lane did
  not perform production recovery or Event writes.
- Migration/snapshot work is high-risk and would merit extra-high/max effort in
  a runner exposing that tier; this bounded lane used high effort.

## Merge notes

- Cherry-pick implementation commit `55c5e7496`, then this RESULTS metadata
  commit.
- After the RAW/schema lane is merged, rerun the four focused test files and a
  real snapshot rehearsal. Do not convert unavailable pre-migration evidence
  into estimated carrier/event counts.
