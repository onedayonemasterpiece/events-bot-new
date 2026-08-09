# Search production-health Stage 1 integration report

## Scope and safety boundary

- Initial audited base: `origin/main@65926e63b436efcc275c8e4094473ab9ef1fd9e8`.
- Final rebased base: `origin/main@48378542d13869447bacc6bfeea136d650b3d407`.
- Integration branch: `integration/search-production-health-stage1-20260809`.
- Prohibited operations performed: **none**.
- Live Search POSTs: `0`.
- Production Supabase/Fly/Kaggle/static publication calls: `0`.
- Browser/Android/iOS/Appium/WDA runs: `0`.
- Deploys and PR #436 merge/cherry-pick: `0`.

## Lane reconciliation

| Lane | Owned scope | Integrated commit | Receipt | Disposition |
|---|---|---|---|---|
| runtime discovery | AS-IS build/target identities | read-only | agent evidence map | accepted |
| workflow discovery | triggers/CI/issue reporter | read-only | agent evidence map | accepted |
| risk discovery | PR #436, issue #431, taxonomy/egress | read-only | agent evidence map | accepted |
| workflow policy | `.github/workflows`, workflow contract tests | `6b3a792a3` | `6471bdf48` | merged |
| core contracts | `site/e2e/search/production-health-*`, focused tests | `2d195957f` | `a40dd7e4f` | merged |
| integration/docs | canonical architecture, registry, handoff, package script, integration corrections | `960effec8` | this report | merged |

No worker change was dropped. Integration tightened the core target pin from a
synthetic release label to the repository's existing immutable candidate tuple
(`build_id`, `run_id`, `repo_sha`, `snapshot_id`, artifact hashes and required
`input_fingerprint`) and expanded relevant-path policy to the new workflows,
auth fixture, shared mobile transport, registry and canonical Search docs.

## Requirements

| ID | Status | Evidence |
|---|---|---|
| R01 actual runtime/data path audit | Done | canonical README §16.1 with exact symbols |
| R02 four identity boundary/reuse | Done | README §16.2; no new release DB/service |
| R03 three testing contours | Done | README §16.3; workflows/registry split |
| R04 disable automatic Search traffic/noise | Done | legacy workflow manual-only; 4 cron + repository dispatch + issue reporter removed |
| R05 resolver/pinning/superseded contract | Done for Stage 1 | pure interface/tests; live adapter deferred |
| R06 typed failure/incident/retry policy | Done | exact 13-result enum; only `BROKEN_*` product incident |
| R07 egress contract | Done for Stage 1 | Auth/Edge/REST/RPC byte-meter; 48/96 KiB tests; live wiring deferred |
| R08 workflows/default PR CI | Done | two manual dry workflows; deterministic CI step |
| R09 bounded PR #436 decision | Done | README §16.10; useful preclaim guard, not merged/deployed/dependency |
| R10 canonical docs + Stage 2 handoff | Done | README §16 and `stage-2-production-health-handoff.md` |
| R11 deterministic acceptance/zero live | Done | 25/25 architecture + 29/29 existing harness; safety counters above |
| R12 clean pushed PR | Pending publisher step | branch is clean; push/PR follows final audit |

## Deterministic validation

Commands executed from the clean integration worktree:

```bash
npm --prefix site run test:search-production-health
# 25 pass, 0 fail/skip/todo

npm --prefix site run test:search-e2e-harness
# 29 pass, 0 fail/skip/todo

node site/e2e/search/production-health-plan-cli.mjs \
  --plane production_health --trigger workflow_dispatch
node site/e2e/search/production-health-plan-cli.mjs \
  --plane release_qualification --trigger workflow_dispatch
# both stage_1_contract_only, dry_run=true, zero_live=true,
# live_calls target_resolver/browser/search_post/supabase = 0/0/0/0

# strict parse of the three Search workflows, default CI and scenario registry
# policy grep: no cron/repository_dispatch/issues:write/gh issue mutation
git diff --check origin/main...HEAD
```

All final commands passed. `docs/routes.yml` has a pre-existing duplicate
top-level key outside this change; its syntax was checked with the repository's
existing duplicate-key tolerance rather than claiming a new strict-parse pass.

## Architecture decisions

1. Smart Update builds use `/app/.static-site-repo-sha`, baked from the deployed
   clean Fly image. They do not follow moving GitHub `main`; no second
   `site_runtime_sha` mechanism is created.
2. Current accepted target remains
   `static_site_build_state.current_secret_candidate_receipt_json` through
   `resolve_current_secret_candidate`; running Kaggle and public root are not
   fallback targets.
3. `catalog_revision` and `corpus_revision` are telemetry for production
   health. `release_exact` stays only in manual/selective release qualification.
4. The old live journey remains available only through explicit manual legacy
   debug. New Stage-1 workflows cannot execute live work.
5. PR #436's preclaim vector guard is independently useful for candidate
   qualification but does not solve production-health orchestration and remains
   unmerged.

## Final Stage-1 state

```text
ARCHITECTURE_READY_FOR_LIVE_VALIDATION
PRODUCT_HEALTH_UNCONFIRMED
```
