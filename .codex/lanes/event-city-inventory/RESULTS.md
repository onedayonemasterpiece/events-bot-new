# Lane event-city-inventory Results

## Status
committed (read-only result integrated by root)

## Requirement IDs
- R01

## Branch
integration/event-transport-schedule

## Worktree
read-only production probe; no worker worktree writes

## Base SHA
08c06252614ad838fa93ef3992950ce422c5f7d2

## Head SHA
N/A (read-only)

## Files changed
None.

## Commands run
Schema-first Fly production SQLite read-only queries.

## Tests / verification
30 active canonical events, 14 logical localities, 21 logical venues; city/venue aliases and missing-coordinate blockers enumerated.

## Risks
Event rows do not store venue coordinates; city-centroid cache cannot be used for walking guidance.

## Merge notes
Inventory counts and alias findings were integrated into JSON/docs.
