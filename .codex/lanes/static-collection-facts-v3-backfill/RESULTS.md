# static-collection-facts-v3-backfill — results

Status: **DONE (bounded backfill/report lane)**

## Scope

- Lane ID: `static-collection-facts-v3-backfill`
- Base SHA: `5aa9958ad673f1d056f5192dde9ed19651fabf5c`
- Implementation SHA: `884bd4f8df17cf1fa04e5d09e0f5a4f0d60e9534`
- Branch: `agent/static-collection-facts-v3/backfill`

## Delivered requirements

- `plan` is the default; `--plan`, `--evaluate`, and `--apply` are mutually exclusive.
- `evaluate`/`apply` require a positive, duplicate-free, bounded explicit event allowlist; unresolved/ineligible IDs fail before provider use.
- Exact PR-A evidence replay is supported through bounded `--source-id` / `--source-id-file`; every source is checked against the explicit event allowlist, usable source text, and the per-event source cap before provider use. Missing/mismatched/truncated source allowlists fail closed.
- Plan/evaluate never call `db.init()` and use a SQLite online-backup temporary copy, so the requested source file is not opened by the application's writable ORM engine. Source file SHA-256 and `quick_check` are recorded before/after.
- `--primary-only` scopes and restores `SMART_UPDATE_MODEL=gemma-4-31b-it`, `SMART_UPDATE_4O_FALLBACK=0`, empty Google provider fallbacks, and one outer retry.
- Audience routing covers facts-v3 keys and broad family/child recall phrases but not age-rating-only text.
- Audience-only apply passes the core `reasons={"audience"}` allowlist (with compatibility for `allowed_reasons`) and transactionally rejects any `is_free` or other non-`collection_decisions` mutation.
- Warm replay prefers the core `collection_decision_hash_covers()` per-source receipt contract, including all-unknown evaluations and multiple EventSource rows; v2 legacy audience alone cannot cover v3.
- Per-source report captures binding/hash, logical provider invocation, physical sends when exposed, requested/actual model path, fallback, waits, nested token usage, latency, validated v3 outcomes, deterministic legacy projection, write status, and changed keys.
- Whole-DB logical Event/EventSource hashes prove plan/evaluate read-only behavior and selected/unselected apply allowlists.
- Added a versioned, strict, redacted Draft 2020-12 JSON report schema.

## Evidence and commands

```bash
python3 -m py_compile \
  scripts/backfill_static_collection_facts.py \
  tests/test_static_collection_backfills.py \
  tests/test_static_collection_facts_backfill_report.py

python3 - <<'PY'
import json
from pathlib import Path
import jsonschema
schema = json.loads(Path('docs/review-data/static_collection_facts_v3_real_data_report.schema.json').read_text())
jsonschema.Draft202012Validator.check_schema(schema)
PY

TMPDIR=/dev/shm \
UV_CACHE_DIR=/dev/shm/uv-facts-v3-cache-1785673946 \
uv run --no-project \
  --with pytest --with pytest-asyncio \
  --with sqlmodel==0.0.24 --with aiosqlite --with aiohttp \
  --with jsonschema --with pyyaml \
  python -m pytest --noconftest \
  tests/test_static_collection_backfills.py \
  tests/test_static_collection_facts_backfill_report.py -q
# 21 passed in 5.02s

git diff --check
```

`--noconftest` was intentional for this isolated lane run because the host lacks the full application environment and repository `tests/conftest.py` imports `main`/aiogram. The selected tests exercise the real SQLite/SQLModel runner and use mocked provider payloads only where the lane contract requires deterministic external-call behavior. The integrator must rerun these files in the full project CI environment after merging the core lane.

## Core reconciliation contract

The lane dynamically consumes these core APIs and was reconciled against the core worktree WIP:

- `collection_decision_hash_covers(decisions, reasons=..., input_hash=..., source_id=...)`
- `apply_collection_decisions(..., reasons=...)`
- trace keys `physical_sends`, `actual_models`, and nested `token_usage`

A legacy fallback remains only to keep the stacked branch importable before core cherry-pick; it requires all three facts-v3 keys and never treats old `audience_decision` as v3 coverage.

## Changed files

- `scripts/backfill_static_collection_facts.py`
- `docs/review-data/static_collection_facts_v3_real_data_report.schema.json`
- `tests/test_static_collection_backfills.py`
- `tests/test_static_collection_facts_backfill_report.py`
- `.codex/lanes/static-collection-facts-v3-backfill/RESULTS.md`

## Residual risks / integration checks

1. Rerun focused and incident-critical CI after core cherry-pick; the base SHA still has facts-v2 constants/schema.
2. Confirm core keeps `reasons=` and `collection_decision_hash_covers`; otherwise reconcile the narrowly isolated adapters.
3. Run production-copy evaluate/apply with `TMPDIR=/dev/shm` (or another volume with room), because read-only modes create a full temporary online-backup copy by design.
4. A live provider/copy/Fly canary is integration-owned and was not run in this implementation lane.
5. No PR-B, Astro, routes/navigation, thresholds, publication, festival, or cinema behavior was added.
