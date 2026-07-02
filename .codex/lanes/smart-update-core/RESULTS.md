# Lane smart-update-core Results

## Status
committed

## Requirement IDs
- R04
- R05
- R06
- R11
- R12
- R14

## Branch
agent/smart-update-vector-identity-gate/smart-update-core

## Worktree
/home/dev/.codex/worktrees/events-bot-new/smart-update-core

## Base SHA
f44a3f3db3112e03e2cbf6ba4e24fff44cd1afc8

## Head SHA
f6f504bd06001f10071eb73651d3fc188acb5237

## Files changed
- CHANGELOG.md
- docs/features/smart-event-update/README.md
- smart_event_update.py
- smart_update_identity.py
- tests/test_smart_update_identity_gate.py
- .codex/lanes/smart-update-core/RESULTS.md

## Commands run
- `cat /home/dev/.agents/skills/feature-fanout/SKILL.md`
- `cat AGENTS.md`
- `sed -n '1,180p' docs/README.md`
- `grep -n "_llm_dedup_adjudicator\|create\|Smart Update\|dedup" smart_event_update.py | head -n 80`
- `sed`/`grep` inspections of `smart_event_update.py`, `docs/features/smart-event-update/README.md`, and `tests/test_dedup_adjudicator.py`
- `pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` (failed: `pytest` not on PATH)
- `python -m pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` (failed: `python` not on PATH)
- `python3 -m pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` (failed: pytest not installed in system Python)
- `python3 -m compileall -q smart_update_identity.py smart_event_update.py tests/test_smart_update_identity_gate.py`
- `uv run pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` (failed: pytest executable unavailable in uv environment)
- `uv run --with pytest pytest -q tests/test_smart_update_identity_gate.py` (failed: project deps such as aiogram unavailable)
- `uv run --with-requirements requirements.txt pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` (first failed on missing import bug; rerun passed)

## Tests / verification
- `uv run --with-requirements requirements.txt pytest -q tests/test_smart_update_identity_gate.py tests/test_dedup_adjudicator.py` → `21 passed in 0.19s`
- `python3 -m compileall -q smart_update_identity.py smart_event_update.py tests/test_smart_update_identity_gate.py` → passed

## Risks
- Vector RPC/schema is intentionally not wired because the vector-rpc/schema lane is not merged; helper accepts optional vector evidence later.
- `SMART_UPDATE_IDENTITY_GATE=off` remains default. `shadow` only logs. `enforce` can return `skipped_identity_gate` instead of create on deterministic/vector/error vetoes, by design.
- Deterministic vetoes are intentionally narrow to avoid deterministic semantic overreach and never auto-merge.

## Merge notes
- Insertion point is after the existing widened-recall `_llm_dedup_adjudicator` block and before create-field assembly.
- New helper module is dependency-light and covered by guard-helper unit tests.
- Do not push per lane instruction.
