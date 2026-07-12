# Lane Map — bus transport directory (2026-07-11)

| Lane | Requirements | Mode | Writes | Effort | Outcome |
|---|---|---|---|---|---|
| event-city-inventory | R01 | read-only parallel | none | high | production event/locality/venue inventory |
| official-bus-routes | R02 | read-only parallel | none | high | official route and timetable-readiness matrix |
| venue-stop-geo | R03/R04 | read-only parallel | none | high | stop/venue access measurements and JSON architecture |
| serial integrator | R04/R05 | serial | transport worktree only | high | JSON, validation, docs, changelog |

Dependency order: parallel discovery → source reconciliation → one serial JSON owner → validation/review.
