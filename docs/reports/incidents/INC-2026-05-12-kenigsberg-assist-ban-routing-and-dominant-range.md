# INC-2026-05-12-kenigsberg-assist-ban-routing-and-dominant-range

Status: monitoring
Severity: sev2
Service: Kenigsberg Stories manual ban controls
Opened: 2026-05-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-12-kenigsberg-deterministic-text-fallback-quality`
Related docs: `docs/features/kenigsberg-stories/README.md`, `docs/operations/release-governance.md`

## Summary

The operator asked `/a Kenigsberg #4 бан 4-6`. Admin assistant treated `Kenigsberg` as a direct command token and executed `/kenigsberg #4 бан 4-6` instead of canonical `/kenigsberg ban #4 4-6`. The command showed help text and did not store any ban. The ban mapper also mapped one requested generated-time range to every overlapping scene, while operator input is whole-second approximate and should select one dominant segment.

## User / Business Impact

- The bad source fragment from issue `#4` was not banned.
- `/kenigsberg bans` reported no bans, contradicting the successful `/a` message.
- Future generations could reuse the unwanted video fragment.

## Detection

- Operator posted the Telegram transcript showing `/a` reported `✅ Выполнено: /kenigsberg #4 бан 4-6`, followed by `/kenigsberg` help text and an empty bans report.

## Timeline

- 2026-05-12 15:48 Europe/Kaliningrad — operator sent `/a Kenigsberg #4 бан 4-6`.
- 2026-05-12 15:48 Europe/Kaliningrad — bot executed malformed `/kenigsberg #4 бан 4-6`.
- 2026-05-12 15:48 Europe/Kaliningrad — `/kenigsberg bans` showed no bans.

## Root Cause

1. `_extract_direct_command_proposal` ran before the Kenigsberg natural-language heuristic and preserved tail args as `#4 бан 4-6`.
2. `_build_command_text` did not canonicalize Kenigsberg `args_text`.
3. `/kenigsberg` accepted only `ban ...` as the first arg token and rejected reordered forms.
4. `map_generated_range_to_source` returned every overlapping source segment instead of one dominant segment per operator range.

## Contributing Factors

- Tests covered `в выпуске kenigsberg #15 бан ...` but not direct-token input `Kenigsberg #4 бан ...`.
- The original mapping model assumed precise ranges rather than whole-second operator review ranges.

## Automation Contract

### Treat as regression guard when

- Changing `/a` direct command extraction or Kenigsberg allowlist action.
- Changing `/kenigsberg ban` argument parsing.
- Changing generated timeline to source ban mapping.

### Affected surfaces

- `handlers/admin_assist_cmd.py`
- `handlers/kenigsberg_stories_cmd.py`
- `kenigsberg_stories/state.py`
- `tests/test_kenigsberg_stories.py`

### Mandatory checks before closure or deploy

- `/a` heuristic/direct routing builds `/kenigsberg ban #4 4-6` from `Kenigsberg #4 бан 4-6`.
- `/kenigsberg #4 бан 4-6` canonicalizes to ban args instead of help text.
- One generated second range maps to one dominant source segment by overlap.
- `pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` passes.
- Production `/healthz` remains green after deploy.
- After deploy, issue `#4` range `4-6` is applied or the operator is told to rerun the now-fixed command.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Fly release/version evidence.
- Test output.
- Post-deploy `/healthz`.
- Bans report showing the intended issue `#4` ban, or explicit evidence that issue `#4` was not present in generation history.

## Immediate Mitigation

- Canonicalize Kenigsberg ban args in admin assistant command construction.
- Canonicalize reordered ban args in `/kenigsberg` itself.
- Map each operator range to only the dominant source segment.

## Corrective Actions

- Add tests for direct `/a` phrasing `Kenigsberg #4 бан 4-6`.
- Add tests for `/kenigsberg #4 бан 4-6` parser canonicalization.
- Add tests for edge-overlap dominant range mapping.

## Follow-up Actions

- [ ] Consider adding a `/kenigsberg ban preview #N ranges` command if manual review needs a dry-run before mutating bans.

## Release And Closure Evidence

- deployed SHA: `116625b40f20bc76e0aa0a17a7834993afd9acba`
- deploy path: manual `flyctl deploy --remote-only` from clean detached worktree at `origin/main`
- Fly release: `v1069`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KRE7PN25EWPMSWRY5ZK6W9BB`
- regression checks:
  - `python3 -m py_compile handlers/admin_assist_cmd.py handlers/kenigsberg_stories_cmd.py kenigsberg_stories/state.py`
  - `.venv/bin/pytest -q tests/test_kenigsberg_stories.py tests/test_kenigsberg_notebook.py` -> `25 passed`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, no issues.
  - Fly machine `48e42d5b714228` is `started`, release `v1069`, service check passing.
  - Production code contains Kenigsberg arg canonicalization and dominant source-segment mapping.
  - Production DB `/data/db.sqlite` had Kenigsberg issues `['2', '3', '4']` and zero bans before repair.
  - Applied the intended missed ban for issue `#4`, requested generated range `4.0-6.0`; stored one dominant source ban:
    `zigomaro/koenigsberg-winter`, source file `___ 9872354.mp4`, source `3.524-5.098s`, generated overlap `4.426-6.000s`, reason `operator_retry_after_assist_routing_fix`.
  - Post-repair production verification returned `ban_count=1` with that exact dominant-source ban as the latest stored row.

## Prevention

- Command assistants must canonicalize risky mutating args before execution.
- Human whole-second review ranges must be treated as approximate and mapped conservatively to one dominant segment.
