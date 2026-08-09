# Lane mcp-direct-user-authorized-actions Results

## Status
committed

## Requirement IDs
- R01: do not execute the stopped production preparation.
- R02: remove redundant second confirmation for explicitly requested outbound actions.
- R03: preserve edit/delete external approval and old-preparation isolation.
- R04: cover the ChatGPT PNG -> ready asset -> Saved prepare -> commit/read-back contract.
- R05: synchronize canonical runbook, E2E index, incident record and changelog.

## Branch
`hotfix/mcp-direct-user-authorized-actions-20260809`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/mcp-fileparams-deploy`

## Base SHA
`150358ed88b5239753bf3669a1f1e311bf3f63cc`

## Head SHA
`81c5da060`

## Files changed
Private Events MCP social contract/runtime/tool descriptions, OAuth consent copy,
media smoke, focused tests, canonical runbook/E2E/incident docs and changelog.

## Commands run
- `uv run --with-requirements requirements.txt python -m pytest -q tests/test_private_events_mcp_*.py`
- `python3 -m compileall -q private_events_mcp scripts/smoke_private_events_mcp_media.py tests/test_private_events_mcp_*.py`
- focused `uvx ruff check` for all changed Python files
- `git diff --check`

## Tests / verification
- Private MCP suite: 372 passed; 3 pre-existing aiohttp `NotAppKeyWarning` warnings.
- Compileall: pass.
- Ruff changed files: pass.
- Diff check: pass.
- No live provider call, approval or commit was performed.

## Risks
- The MCP server cannot independently read the originating ChatGPT prompt. The
  typed outbound action invocation is therefore the connector's delegated
  assertion that the current user explicitly requested the exact action.
- Edit/delete remain browser-approved. Existing `awaiting_human_approval` rows
  remain inert and are not upgraded.

## Merge notes
Preserve the existing endpoint/client/resource/signing identity. Merge to main,
run green PR CI, deploy exact main, then verify health/DB/webhook. Do not run a
live Saved Messages canary without a fresh explicit user request.
