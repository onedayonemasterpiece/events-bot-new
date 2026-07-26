# R13 Festivals Production Integration

## Fanout Decision
Broad cross-system work uses parallel read-only mapping; all writes remain serial in this clean integration worktree.

## Requirement matrix

| ID | Requirement | Owner | Dependency | Done when |
|---|---|---|---|---|
| R13-01 | Telegram comment: mobile Today completed events remain chronologically ordered | integrator after mapper | R12 rail | regression fixture/browser gate passes |
| R13-02 | Future festival information from donor exists in canonical Fly SQLite-backed data | integrator | donor audit + schema | idempotent backfill/migration verified against DB copy and prod |
| R13-03 | Festival page uses current DB projection without losing donor composition | integrator | R13-02 | export + Astro page retain expected festivals |
| R13-04 | Current R12 unified page set is part of production generation, not preview-only assembly | integrator | production mapper | production artifact contains all page types and working links |
| R13-05 | Production release through origin/main with clean build/deploy evidence | integrator | all above | root URLs and generated output gates pass |

## Lane map

- R13-FEST-DB: read-only mapper, medium effort, no writes.
- R13-PROD-GEN: read-only mapper, medium effort, no writes.
- R13-TODAY-SORT: read-only explorer, medium effort, no writes.
- R13-INTEGRATION: serial integrator, high effort; owns all writes, tests, docs, DB backfill and release.
