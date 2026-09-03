# Issue #621 execution matrix

| ID | Requirement | Area | Dependencies | Risk | Done when |
|---|---|---|---|---|---|
| R01 | Fresh-read issue #621 and contract v1.6.0 | governance | none | low | current heads and contract recorded |
| R02 | Census integration, F0/M0/A0/V0 and consumers | Git/source | R01 | low | current remote tips and gaps recorded |
| R03 | Recompute and execute highest-value reversible backlog | integration | R01-R02 | medium | latest safe candidate built |
| R04 | Freeze exact transaction SHAs and ancestry | Git | R03 | low | all selected refs are ancestors of candidate |
| R05 | Exact reachable normalized build URL | build/publish | R04 | medium | strict public verification passes |
| R06 | Reproducible fresh-production generation | data/build | R04 | medium | immutable snapshot/projection and generation evidence retained |
| R07 | Run available local browser smoke | QA | R05-R06 | medium | generic applicable smoke passes or factual harness defects recorded |
| R08 | Continue ready integrations while V0 audits | integration | R05 | medium | ready technical fixes processed without claiming V0 verdict |
