# Integration Report — bus transport directory

Base/integration branch: `integration/event-transport-schedule` at `08c06252`.

| Requirement | Status | Evidence |
|---|---|---|
| R01 current bus coverage and production inventory | Done | 30 active events / 14 localities / 21 venues committed in route/access metadata |
| R02 official route coverage | Done | official AVL timetable, station pages and registry URLs per corridor |
| R03 venue-to-stop access | Done | 21 active venue rows; exact/proxy/blocked confidence preserved |
| R04 JSON vs YDB and linked schema | Done | split static route/access/schedule JSON with stable cross-ids |
| R05 docs/checks/report | Done | canonical directory doc, changelog and `check:bus-directory` |

Read-only child lanes made no repository writes. The serial integrator owns all committed changes. Public schedule rendering remains intentionally unchanged outside the reviewed Romanovo example.
