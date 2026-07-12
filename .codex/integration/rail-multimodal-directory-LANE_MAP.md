# Lane Map — official rail and multimodal directory (2026-07-12)

Mode: three read-only discovery lanes, then one serial integration owner.
Base/integration branch: `integration/event-transport-schedule`.

| Lane | Requirement IDs | Mode | Writes | Effort | Output |
|---|---|---|---|---|---|
| `official-bus-routes` | R01, R02, R04, R05, R06 | read-only parallel | none | high | coastal/Baltiysk/Mamonovo/Krasnolesye matrices and priority evidence |
| `event-city-inventory` | R03, R05, R06 | read-only parallel | none | high | eastern corridor, Tyunin farm access and mixed-mode constraints |
| `venue-stop-geo` | R04, R06 | read-only parallel | none | high | Bagrationovsk/Zheleznodorozhny, publication precedence and schema review |
| serial integrator | R01–R06 | serial after discovery | transport worktree only | high | JSON, validator, docs, changelog and release checklist |

Dependency order: official page/image discovery → cross-lane timetable reconciliation → static reference implementation → validation and closure audit.

Stop conditions: do not infer a stop from geography; do not invent a season end; do not partially replace the current public calendar snapshot when official/API rows differ.
