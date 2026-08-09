# Lane search-production-health-stage1/workflow-policy Results

## Status

committed

## Requirement IDs

- R1 — convert the legacy Search canary to manual live-debug only and remove incident reporting
- R2 — add the manual, deterministic production-health planner workflow
- R3 — add the manual, default-off release-qualification planner workflow
- R4 — add the deterministic Search production-health suite to default pull-request CI
- R5 — replace unsafe schedule assertions with Stage-1 workflow policy contracts

## Branch

`feature/search-production-health-stage1-workflow-20260809`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/search-production-health-stage1-workflow`

## Base SHA

`dce9d0fa98d2d1c99f6a3c727f828de4c47bd70d`

## Head SHA

Implementation commit: `dd786d0aad3405afb3a576ed067edd47745be716`.
The lane receipt is committed immediately after that implementation commit; use the branch head when integrating.

## Files changed

- `.github/workflows/static-site-search-canary.yml`
- `.github/workflows/search-production-health.yml`
- `.github/workflows/search-release-qualification.yml`
- `.github/workflows/ci.yaml`
- `site/tests/search-e2e-workflow-contract.test.mjs`
- `.codex/lanes/search-production-health-stage1/workflow-policy-RESULTS.md`

## Commands run

- `node --test tests/search-e2e-workflow-contract.test.mjs`
- `npm run test:search-e2e-harness`
- local Node + `yaml` parse of the three Search workflows and `.github/workflows/ci.yaml`
- `git diff --check`

The worktree reused the main checkout's already-installed, lockfile-matching `site/node_modules` through an ignored local symlink. No dependency download or network access was performed.

## Tests / verification

- Focused workflow contract: PASS, 10/10 tests.
- Complete offline Search E2E harness contract suite: PASS, 29/29 tests.
- YAML parse: PASS; Search workflows expose only `workflow_dispatch`, CI exposes only `pull_request`.
- `git diff --check`: PASS.
- No workflow runs, live/prod calls, browser runs, deployment, or network activity were performed.

## Risks

- Both new workflows intentionally depend on `site/e2e/search/production-health-plan-cli.mjs`, owned by the core lane and expected to exist after integration.
- Pull-request CI intentionally references `npm run test:search-production-health`; the integration owner must merge the corresponding `site/package.json` script from its owning lane.
- The legacy workflow retains its live jobs, environment, secrets, and OIDC permission solely behind explicit `workflow_dispatch`, as required for manual investigation. The two new Stage-1 workflows have none of those capabilities.

## Merge notes

- Cherry-pick the implementation commit and this receipt commit.
- Integrate after (or together with) the core planner CLI and package-script owner so the new workflow commands and CI step resolve.
- The policy tests deliberately require all three Search workflows to remain manual-only and require the new planner workflows to stay single-job, secretless, and network-tool-free.
