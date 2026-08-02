# Region Talk live intake execution matrix

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|---|
| R1 | Before each selection cycle and immediately before final decision reread current YDB; no fixed snapshot. | orchestration/finalizer/planner | orchestrator, finalizer, plan | importer contract | high | live-decisions | after mapping | tests prove two distinct reads and late row visibility |
| R2 | New record defaults unreviewed; arrival is not publication permission. | importer/YDB | external research importer | none | medium | intake-ledger | yes | persisted intake has explicit unreviewed gate and cannot be eligible directly |
| R3 | Route candidate through standard checking/scoring; never auto-promote manual_review_required. | pipeline admission | orchestrator/finalizer | R2 | high | live-decisions | yes | normal stages consume intake and manual status remains terminal/manual |
| R4 | Dedupe URL/DOI/title+authors; preserve request/SHA/external ID/evidence/time provenance. | importer/YDB | external research importer | none | medium | intake-ledger | yes | canonical keys and complete provenance survive updates |
| R5 | Reruns create no duplicate record or publication. | importer/finalizer | importer plus final gate | R4 | high | intake-ledger | yes | repeated identical/matching intake is an idempotent no-op/merge |
| R6 | Late intake cannot silently replace prepared issue; reevaluate safely or defer. | planner/schedule | publication plan/finalizer | R1 | high | live-decisions | yes | prepared/locked slot identity remains stable and late rows enter next evaluation |
| R7 | Conflicts/missing proof/data errors fail closed to manual review. | all gates | importer/finalizer | R2-R4 | high | live-decisions | yes | ambiguous rows never become auto-publishable |
| R8 | Log count and IDs of new intake records. | operator observability | importer output/audit rows | R4 | low | intake-ledger | yes | machine-readable result and logs expose count/IDs |
| R9 | Semantic decisions use LLM; deterministic code only normalization/dedupe/safety. | LLM policy | admission/finalizer prompts/gates | R3,R7 | high | live-decisions | yes | no keyword/regex semantic promotion; tests/docs enforce LLM decision requirement |
