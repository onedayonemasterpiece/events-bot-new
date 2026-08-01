# REACTION-CODE results

- Lane: `REACTION-CODE`
- Requirements: `R03`, reaction-side of `R08`
- Base SHA: `66ce2a5ae2c175bae3aa2f968e7785089b731dc8`
- Implementation head SHA: `769de2ff`
- Status: complete, committed, not deployed

## Delivered

- Added `scripts/region_talk_reaction_sync.py`, fixed to
  `telethon_discovery2` and protected by the existing discovery-session lease
  plus remote ImageDiagnostic idle guard.
- Added required exact numeric reviewer allowlist, full
  `GetMessageReactionsList` pagination and count/offset fail-closed checks.
- Implemented the documented two-axis approve/reject/rewrite truth table.
- Added idempotent `publication_review_state_item` and transition-stable
  `publication_review_event_item` rows plus exact-current candidate projection.
- Bound delivery/review identity to exact draft plus ordered media/presentation
  manifest; stale reactions cannot approve changed copy/media.
- Added a visible reaction legend to candidate delivery.
- Added default-off `REGION_TALK_REACTION_GATE_ENABLED`; when enabled, the
  planner accepts only exact-current `approved + clean` candidates.
- Updated canonical publication queue/YDB schema docs and `CHANGELOG.md`.

## Evidence

Commands:

```text
python3 -m py_compile scripts/region_talk_goal_notify.py \
  scripts/region_talk_publication_plan.py scripts/region_talk_reaction_sync.py
PYTHONPATH=. /home/dev/.venvs/events-bot-region-talk/bin/pytest --noconftest -q \
  tests/test_region_talk_reaction_sync.py \
  tests/test_region_talk_goal_notify.py \
  tests/test_region_talk_publication_plan.py
git diff --check
```

Result: `38 passed in 0.93s`; compile and diff checks passed.

A broad `tests/test_region_talk_*.py` collection was also attempted. The shared
specialized environment lacks `aiosqlite`, imported by the unrelated scheduled
runner through `db.py`, so collection stopped before tests. Focused owned tests
are green. Pytest's official `--noconftest` behavior was verified before using
the focused environment because the repository conftest imports the full bot
dependency graph.

## Changed files

- `scripts/region_talk_reaction_sync.py`
- `scripts/region_talk_goal_notify.py`
- `scripts/region_talk_publication_plan.py`
- `tests/test_region_talk_reaction_sync.py`
- `tests/test_region_talk_publication_plan.py`
- `docs/features/region-talk-channel/publication-queue.md`
- `docs/features/region-talk-channel/ydb-schema.md`
- `CHANGELOG.md`

## Risks / integration notes

- The gate intentionally remains disabled until the operator reviewer allowlist
  is configured and legacy candidates have been redelivered/reviewed.
- Scheduling the sync belongs to the runner/orchestration lane; this lane did
  not edit forbidden scheduler files.
- No production write, session connection, push or deploy was performed.
