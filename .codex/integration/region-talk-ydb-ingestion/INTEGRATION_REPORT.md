# Region Talk GitHub Actions → YDB integration report

## Integration base

- Base: `origin/main` updated through `5a5d28cc` (merged into this integration branch)
- Integration branch: `integration/region-talk-ydb-ingestion`
- Payload audit was read-only; no historical research JSON was staged to YDB.

| Lane | Requirement IDs | Branch | Status | Head / merge | Evidence |
|---|---|---|---|---|---|
| audit | R01 | read-only | merged evidence | n/a | Three source payloads schema-valid; two require semantic corrections before staging. |
| importer | R03 | `agent/region-talk-ydb-ingestion/importer` | merged | `e55bf2bb` → `e6016ff7` | YDB-only `--no-publish-registry`; 19 targeted tests passed. |
| workflow | R02 | `agent/region-talk-ydb-ingestion/workflow` | merged | `a9c9d43e`, `17215ab5` → `500d7dd9`, `31922950` | Trusted-main, path-constrained manual import with OIDC and artifact receipt. |
| infra | R04 | external YC/GitHub config | completed | n/a | Dedicated OIDC-only SA, workload federation, exact environment subject, table-scoped YDB grants, protected Actions environment and non-secret variables. |
| docs | R05 | `agent/region-talk-ydb-ingestion/docs` | merged | `656595d6` → `98b7e821` | Canonical runbook, exact historic source packages, audit status and changelog. |

## Final verification

- `pytest -q tests/test_region_talk_external_publication_import.py` — 19 passed.
- `python -m py_compile scripts/region_talk_external_publication_import.py` — passed.
- Workflow static security/YAML/Bash contract checks — passed.
- Integrated dry-runs: first two historical payloads correctly stop before YDB; third completes dry validation. No `--execute` run occurred.
- `git diff --check origin/main..HEAD` — passed.

## Release note

The workflow becomes callable only after this branch is merged into `main`; its OIDC trust only accepts the `region-talk-ydb-import` GitHub Environment. Historical input staging remains deliberately unperformed.
