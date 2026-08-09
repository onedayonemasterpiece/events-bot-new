# Stage 2 workflow/release lane result

- Lane ID: `workflow_release`
- Requirements: `R01`, `R02`
- Base SHA: `dd1e0ad9072acdad46f01459ba4ab0ff171e0318`
- Implementation SHA: `6b964e4353d009a21b2c8a9610ffa2c56dfdf1f5`
- Status: `DONE` (deterministic implementation only; no live dispatch, deploy, Search, emulator, or simulator run)

## Delivered

- Activated `search-production-health.yml` with only:
  - UTC `17 6 * * *` -> browser + Android;
  - UTC `17 18 * * *` -> browser + iOS;
  - manual `browser`, `browser_android`, `browser_ios`, `all`;
  - `repository_dispatch: search-runtime-deployed` with an exact five-field payload and explicit `standard|full` validation.
- Applied the constant `search-production-health` / `cancel-in-progress: false` concurrency group to production health and the legacy manual canary, preventing shared-persona overlap.
- Wired each platform job to the single in-process `production-health-run.mjs` contract; the workflows do not issue broker credentials or start a separate mobile preflight process.
- Left the terminal disposition hook exclusively on the pure reporter CLI; no inline conclusion-to-incident heuristic was added.
- Kept release qualification manual-only. A `full` deployment marker can request it exactly once after terminal health success.
- Added `none` (default), `standard`, and `full` marker parsing to the canonical Fly deploy script. Marker-only arguments are consumed and never included in Fly arguments. `none` and Fly failure send zero events; successful `standard|full` sends one post-success repository dispatch.
- Added a pure dispatch-envelope builder/validator. The client payload contains exactly `site_runtime_sha`, `search_backend_revision`, `validation_profile`, `changed_surfaces`, and `deployment_run_id`; it rejects URLs, extra keys, `none`, unsafe values, and severity words in changed-surface telemetry.

## Changed files

- `.github/workflows/search-production-health.yml`
- `.github/workflows/search-release-qualification.yml`
- `.github/workflows/static-site-search-canary.yml`
- `scripts/deploy_fly_main.sh`
- `scripts/search-runtime-deploy-dispatch.mjs`
- `site/e2e/search/production-health-plan-cli.mjs`
- `site/e2e/search/production-health-planner.mjs`
- `site/tests/search-e2e-workflow-contract.test.mjs`
- `site/tests/search-production-health-planner.test.mjs`
- `site/tests/search-production-health-deploy-marker.test.mjs`

## Evidence

Commands run from `/home/dev/.codex/worktrees/events-bot-new/search-stage2-workflow`:

- `npm --prefix site ci --no-audit --no-fund` -> PASS.
- `npm --prefix site run test:search-production-health` -> PASS, 33/33.
- `npm --prefix site run test:search-e2e-harness` -> PASS, 29/29.
- YAML parse with pinned project dependency `yaml@2.9.0` for all three changed Search workflows -> PASS.
- `bash -n scripts/deploy_fly_main.sh` -> PASS.
- `node --check scripts/search-runtime-deploy-dispatch.mjs` -> PASS.
- `node --check site/e2e/search/production-health-planner.mjs` -> PASS.
- Fake isolated Git/Fly/GitHub deploy tests prove default `none` and failed Fly deploy emit zero dispatches, while successful `standard` emits exactly one -> PASS.
- `git diff --check` -> PASS.

## Integration dependencies and residual risks

- The workflow intentionally references `site/e2e/search/production-health-run.mjs` and `site/e2e/search/production-health-report-plan-cli.mjs`, owned by sibling lanes. Integration must reconcile their final CLI/env contracts before merge.
- This lane did not run GitHub-hosted Android/iOS infrastructure or production Search. Deterministic workflow shape is green; live validity remains an integration/live gate.
- A requested marker requires authenticated `gh` after Fly succeeds. A marker-delivery failure returns non-zero after a successful deploy, visibly separating deployment success from validation activation.
- No generation, Smart Update, snapshot, static-site builder, or Kaggle emitter was added.
