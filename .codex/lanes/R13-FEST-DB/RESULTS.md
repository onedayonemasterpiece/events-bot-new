# Lane R13-FEST-DB Results

## Status
completed-read-only

## Requirement IDs
- R13-02
- R13-03

## Branch
integration/festivals-production-r13-20260726

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/r13-production`

## Base SHA
`7ba887a9`

## Head SHA
Read-only lane; integration owner commits the result.

## Files changed
None by mapper.

## Commands run
Production Fly SQLite schema/count probes and donor/source mapping.

## Tests / verification
Found 9/21 donor identities in legacy `festival`, mostly stale; 12 absent. Verified donor's 21 rows were hardcoded and production export had no festival query.

## Risks
Legacy `festival.name` consumers assume a unique match, so yearly duplicate rows there are unsafe.

## Merge notes
Integrator implemented a dedicated `festival_calendar_item` edition table and fail-closed exporter.
