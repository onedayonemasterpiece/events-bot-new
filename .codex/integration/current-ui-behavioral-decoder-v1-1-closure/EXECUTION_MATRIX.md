# Current UI Behavioral Decoder v1.1 closure execution matrix

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Reconcile rail Home/End semantics and observed keyboard/focus/boundary behavior without fixing production | source/runtime evidence | behavioral registry/capture, unresolved/audit records | exact source and captured rail packet | medium | rail semantic map + closure integration | map only | semantic requirement is proven; evidence gap is closed; implementation gap is non-blocking |
| R02 | Execute all 293 breakpoint/container probes in real browser runtime with terminal records and bounded rasters | browser harness | behavioral capture/registry/validator/tests | existing exact-source harness and source matrix | high | probe runtime implementation | no after mapping | exactly 293 unique records are PASS, MISMATCH, or UNREACHABLE_WITH_REASON |
| R03 | Reconcile matrix, observations, automation/manual ledgers, unresolved, audit and durable receipt | materialization | behavioral materializer/validator/artifacts | R01 and R02 final evidence | high | closure integration | no | all hashes/counts/references validate; every new raster has a full-resolution review row |
| R04 | Verify R-07 publication in design-system main without rewriting R-01…R-06 | cross-repo docs | design research README/index/R-07 | merged design PR 28 | low | design publication verify | yes | direct paths and exact main commit/tree are recorded |
| R05 | Independent final audit | acceptance | immutable outputs, both repositories | R01–R04 | high | independent final audit | no | reviewer independently verifies all requested gates and final status |

Dependency order: `(R01 map || R02 map || R04 verify) -> R02 implementation/capture -> R03 materialization/manual review -> R05 independent audit -> Actions/Release/import/main handoff`.
