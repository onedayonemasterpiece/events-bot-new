# INC-2026-09-06-mcp-snapshot-integrity-budget

Status: open
Severity: sev2
Service: EventsBot MCP operations_snapshot
Opened: 2026-09-06
Closed: —
Owners: #643 implementation lane
Related docs: docs/operations/private-events-mcp.md

## Summary and impact

Authenticated production `operations_snapshot({})` returned `Tool time budget
exceeded` / `INVALID_ARGUMENT` during the R0 release baseline. Fly health remained
passing. No event/publication write was attempted; the owner lost queue/count
readback. This is a mechanical read-budget defect, not event semantics.

## Evidence and timeline (UTC)

- 2026-09-06 18:38:11: production MCP request correlation
  `b6eaa7887696437ab46babffe9ff4888`, OAuth, duration 1752ms.
- Before any #643 deploy, image SHA was
  `6fddf14aeb983f97bde96e5963e1c9a9ddf72590`.
- File mirror was enabled at `/data/runtime_logs`; active and seven rotated
  files were checked. Read-only production timing separated each repository step:
  schema 39ms, document index 84ms, ordinary count/status queries 6–27ms;
  quick_check alone 1354ms, ending `unavailable:QueryBudgetExceeded`;
  whole direct snapshot 1654ms, too close to HTTP's 1750ms budget.
- Evidence files are private local artifacts under
  `artifacts/codex/hero-talk-643/` (`r0-prod-probe.log`, `r0-timing-probe.log`).

## Root cause and containment

`operations_snapshot` ran `PRAGMA quick_check(1)` on every interactive call.
SQLite documents this as an O(N) scan, not a constant-time connection probe:
[SQLite PRAGMA quick_check](https://www.sqlite.org/pragma.html#pragma_quick_check).
The per-query progress-handler budget did not make the full scan cheap enough
for the whole-tool deadline on this production database.

Remove only that full scan from the interactive snapshot. Keep the existing
field as `not_run:interactive_budget`; do not report `ok`, raise the global
budget, cache fabricated integrity or start an untracked background scan.
`ReadOnlySQLite.quick_check()` remains available for explicit operator checks.

## Automation contract

Treat changes to `operations_snapshot`, owner queue readback or tool/query
budgets as regression triggers. Before closure/deploy:

- Repository and R0 tests prove count/queue reads, redaction, legacy schema and
  zero DB/provider writes.
- A forbidden quick_check test proves no full integrity scan is started.
- Exact-main production deploy through `scripts/deploy_fly_main.sh`.
- Authenticated live snapshot and bounded owner queue page succeed; no
  integrity PASS is inferred from `not_run`.
- `/healthz` and existing seven-tool Codex projection remain valid.

No scheduled publication was missed due to this read defect; no content catch-up
or partner notification is authorized/needed.

## Release evidence

Pending. Do not mark closed until deployed SHA and actual authenticated readback
are recorded. Rollback is previous exact-main image; no schema migration exists.
