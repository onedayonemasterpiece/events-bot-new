# Unusual Events Health — Execution Matrix

Base: `origin/main` at `bbc286c906de79f6f58b6315daec4558b508780c`.

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Primary lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Validate and repair the canonical unusual-event semantic methodology, fixtures, precision@20 and family metrics without a second semantic pipeline | semantic selection | existing unusual selector/build code; monitor config; fixtures/tests | current-pipeline mapping | high | semantics-quality | discovery only until contract is mapped | failing regressions pass against the one production BGE path |
| R02 | Emit one bounded, versioned machine-readable health contract at the existing BGE/build boundary | builder contract | StaticSiteBuilder/export scripts; schema; fetch/normalize scripts | R01 pipeline identity | high | contract-ops | no, shared builder boundary | real receipt validates and carries all required provenance/counts/statuses |
| R03 | Define and enforce target/minimum/card eligibility/dedup/freshness/page-manifest completeness | product readiness | selector config; manifest generation; page check | R01, R02 | high | semantics-quality | serial with R01 | zero/underfilled/stale/mismatched output cannot be READY/HEALTHY |
| R04 | Review the live catalog selections, near-threshold candidates, hard negatives, duplicates and family disputes | editorial evidence | review-pack generator; runtime artifacts | R01-R03 | medium | semantics-quality | runtime after implementation | current review pack is complete and explicitly separates agent review from owner acceptance |
| R05 | Add production-grade daily/PR/weekly-cold GitHub Actions orchestration without a second BGE source of truth | CI orchestration | `.github/workflows/`; fetch/wait helper | R02 | medium | contract-ops | after contract boundary | scheduled/manual/PR paths, bounded wait, concurrency, permissions, artifacts and summary work |
| R06 | Implement HEALTHY/WATCH/INCIDENT plus READY/NOT_READY/BLOCKED and persistent issue lifecycle | operational state | evaluator; workflow issue step; tests | R02, R03, R07 | medium | contract-ops | after evaluator contract | INCIDENT fails, WATCH succeeds with deduped issue, closure needs two consecutive healthy/ready runs |
| R07 | Compute independent quality/drift metrics, denominators/uncertainty, warm/cold ratios and accepted-baseline comparisons | quality evaluation | quality script; baseline config/storage; tests | R01, R02 | high | semantics-quality | serial with R01 | required metrics are truthful, data splits are leakage-safe, thresholds are explicit |
| R08 | Validate `/neobychnoe/` at 390x844 and 1728x900 against the publication manifest with screenshots and browser receipt | browser E2E | Playwright runner/tests; page/card markup if needed | R02, R03 | medium | browser-e2e | after manifest schema stable | live page IDs/order/content/SEO/images/console match and evidence is saved |
| R09 | Produce the full JSON/Markdown/candidate/review/diff/browser evidence set and useful Step Summary/docs link | reporting/docs | report builders; workflow; canonical docs; `CHANGELOG.md` | R02, R04-R08 | medium | contract-ops | after output contracts | every real run publishes the requested bounded artifact set and docs point to latest workflow |
| R10 | Run local tests, real warm/cold builds/workflows, review evidence, fix discovered defects, open ready PR, pass checks, merge to main and audit release | release | GitHub/Kaggle/CDN/Fly runtime; PR | R01-R09 | high | release-integrator | no | merged main SHA and real run/artifact/issue links satisfy the closure checklist |

## Dependency graph

`pipeline mapping -> R01 -> R02/R03/R07 -> R04/R05/R06/R08 -> R09 -> R10`.

The semantic selector, health contract, manifest and evaluator share a single production boundary, so implementation ownership remains serial until discovery fixes the exact file graph. Independent discovery and later browser/workflow-only writes may fan out if their writable scopes are proven disjoint.
