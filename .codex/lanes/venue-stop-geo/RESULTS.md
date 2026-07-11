# Lane venue-stop-geo Results

## Status
committed (read-only result integrated by root)

## Requirement IDs
- R03
- R04

## Branch
integration/event-transport-schedule

## Worktree
read-only OSM/Valhalla research; no worker worktree writes

## Base SHA
08c06252614ad838fa93ef3992950ce422c5f7d2

## Head SHA
N/A (read-only)

## Files changed
None.

## Commands run
OSM stop/route inspection and Valhalla pedestrian calculations.

## Tests / verification
High-confidence and conditional venue-to-stop mappings produced; Ушаково homonym, Кауп detour, Territory Ya and missing venue points identified.

## Risks
Low-confidence entrance/platform proxies remain explicitly conditional or blocked.

## Merge notes
Measurements and safety states were integrated into busVenueAccess.json.
