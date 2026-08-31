# Lane docs-ops Results

## Status

committed

## Requirement IDs

- DOC-01 — create the new August 31 incident from the canonical template and keep it open.
- DOC-02 — preserve proven Telegram, VK, runtime, release and queue-count evidence without converting hypotheses into facts.
- DOC-03 — define the scheduled-list, provider-owned deadline, exact scheduled-delete, bounded retry and safe observability contracts plus mandatory release checks.
- DOC-04 — update the active incident indexes and machine-readable route.
- DOC-05 — update `[Unreleased]` without claiming production deployment or acceptance.
- DOC-06 — provide lane validation and merge evidence.

## Branch

`agent/eventsbot-scheduled-readback/docs-ops`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/eventsbot-readback-docs`

## Base SHA

`64f75d10f7aff33fa616cee212878bd9d03673b1`

## Head SHA

Implementation/docs head before this results-only evidence commit:
`43142a4e80532e0cffa66dc6e780b2778ffef691`.

## Files changed

- `CHANGELOG.md`
- `docs/README.md`
- `docs/operations/private-events-mcp.md`
- `docs/reports/incidents/INC-2026-08-31-mcp-scheduled-readback-reschedule.md`
- `docs/reports/incidents/README.md`
- `docs/routes.yml`
- `.codex/lanes/docs-ops/RESULTS.md` (this evidence-only follow-up)

## Commands run

- `git status --short --branch`
- `git rev-parse HEAD`
- fresh reads of `docs/README.md`, `docs/routes.yml`, `docs/operations/incident-management.md`, `docs/reports/incidents/TEMPLATE.md`, the incident index, canonical MCP documentation, changelog, and related August 24/25/27/30 incident records
- read-only inspection of the prior audit session/evidence for exact production facts
- scoped `rg`, `sed`, and Python assertions for incident sections, routes, indexes, target contract terms and false closure claims
- `git diff --check`
- `python3` + PyYAML parse of `docs/routes.yml`
- explicit `git add` of lane-owned files and `git commit`

## Tests / verification

- PASS — writable-scope gate: every changed file was lane-owned.
- PASS — `git diff --check` before and after staging.
- PASS — `docs/routes.yml` parsed with PyYAML; the new route resolves to an existing incident file.
- PASS — incident template/automation sections, four related incidents, proven production SHA/release/evidence, exact VK `0 / 0` counts, open status, release placeholders and `BLOCKED_OLD_TELEGRAM_OUTCOME` were asserted.
- PASS — MCP contract includes the confirmed implementation names/fields: `social_scheduled_items_list`, `social_action_retry(operation_ref)`, raw scheduled read/delete requests, exact reconciliation terminal codes, list limits and safe VK multipart diagnostics.
- PASS — false-closure gate found no closed/mitigated/monitoring status or filled deploy/test evidence.
- Not run — code tests/full Private Events MCP suite; this lane owns documentation only and makes no runtime changes.
- Note — one initial validation command referenced a nonexistent worktree-local `.venv/bin/python`; it made no changes and was immediately rerun successfully with system `python3` and installed PyYAML.

## Risks

- Incident remains open: implementation, merge, deploy, historical Telegram reconciliation, exact Telegram counts and refreshed ChatGPT action acceptance are intentionally pending.
- The audited VK live count covers the latest 100 exact-owner wall items, as stated; it is not generalized to an unbounded provider history.
- This lane performed no provider/social mutation and cannot establish the old Telegram outcome.
- Integration must retain the exact public names and receipt fields confirmed by the Telegram/tools/VK implementation lanes or reconcile the documentation if their final reviewed code changes.

## Merge notes

- Cherry-pick `43142a4e80532e0cffa66dc6e780b2778ffef691`, followed by the results-only commit containing this file.
- Do not mark the incident mitigated/monitoring/closed during merge. Fill release evidence only after exact-main deploy and operational acceptance.
- The changelog entry deliberately says the contract is defined and acceptance remains gated; it does not claim the fix is deployed.
