# RUNTIME-CODE lane results

- Lane ID: `RUNTIME-CODE`
- Requirement IDs: `R01`, `R02`
- Base SHA: `66ce2a5ae2c175bae3aa2f968e7785089b731dc8`
- Implementation head SHA: `016bed762c51e0e900359a728b5232ea7be3e2e1`
- Branch: `agent/region-talk-editorial/runtime-discovery`
- Production mutation/deploy: none

## Delivered

- Five 90-minute Region Talk canary slots: `06:20,09:50,13:50,17:50,21:50 Europe/Kaliningrad`. The `13:20` Guide slot is no longer shared; the last nominal Region Talk end is `23:20`, before Telegram monitoring at `23:40`.
- CandidateReport auth fails closed on explicitly selected `TELEGRAM_AUTH_BUNDLE_DISCOVERY1|2`; scheduled children strip E2E, `TELEGRAM_SESSION`, and `TG_SESSION`.
- Immutable source admission `queue_seq` now owns selection/cursor traversal and bounded handoff. A timestamp-newer canonical source cursor may rewind to the earliest low-sequence primary gap; retained per-run cursor history cannot replace the canonical row. Image cursor monotonicity is unchanged.
- Due confirmed-external/high-yield sources receive a configurable 20% revisit reserve while the remaining normal history capacity stays with primary discovery. Terminal local/spam/compliance rows are excluded.
- Telegram revisits resume from the durable highest numeric message id with a configurable ten-id overlap. First/deep scans remain recent-plus-anchor. Interrupted scans do not advance the durable cursor.
- Primary/delta attempted, completed, and deferred counters are distinct in governor observability, product summary, and run-funnel metrics.
- Scheduled runner invokes the fail-closed `region_talk_reaction_sync.py` hook after the orchestrator and before publication-plan recalculation. D2/ImageDiagnostic busy or unverified status is a non-fatal deferral recorded for the next scheduled slot. The command loads no `.env` and receives no generic/E2E session.

## Validation

Commands:

```text
python3 -m py_compile kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py scheduling.py scripts/region_talk_scheduled_runner.py
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest tests/test_region_talk_candidate_report.py tests/test_region_talk_scheduled_runner.py tests/test_scheduling.py -q --disable-warnings --maxfail=15
# 366 passed in 27.51s

git diff --check
```

Focused reaction/cursor/auth/reserve test subset also passed: `7 passed in 2.05s`.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `docs/features/region-talk-channel/README.md`
- `docs/features/region-talk-channel/mvp-candidate-report.md`
- `docs/features/region-talk-channel/orchestration-to-be.md`
- `docs/features/region-talk-channel/source-discovery.md`
- `docs/operations/cron.md`
- `fly.toml`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `scheduling.py`
- `scripts/region_talk_scheduled_runner.py`
- `tests/test_region_talk_candidate_report.py`
- `tests/test_region_talk_scheduled_runner.py`
- `tests/test_scheduling.py`

## Integration risks / follow-up

- This branch intentionally does not own `scripts/region_talk_reaction_sync.py`; integration must also merge the reaction-worker commit that provides it. The runner handles a missing script as a visible non-fatal deferral, so discovery itself remains shippable.
- The reaction sync hook is post-orchestrator. It runs before the runner-owned publication-plan step, but any notifier action already executed inside the orchestrator necessarily precedes this hook. Reordering an orchestrator-owned notifier would be a separate integration decision.
- No live Telegram, YDB, Fly deploy, or production schedule mutation was performed in this lane.
- Pytest created root-level report files; all generated/untracked outputs were removed and none were committed.
