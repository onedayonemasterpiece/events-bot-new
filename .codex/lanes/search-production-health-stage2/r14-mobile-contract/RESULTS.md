# R14 Mobile Contract Results

- Lane ID: `R14` / `search-production-health-stage2/r14-mobile-contract`
- Requirement IDs: `R14`
- Base SHA: `9642c7d56a34f647a6f725eba0d5d3b4b8256554`
- Implementation head SHA: `3a32cf03530425dc2f30d494097beb469a7b69c3`
- Branch: `agent/search-production-health-stage1/mobile-contract`
- Final branch head: the receipt-only commit immediately after the implementation head (report separately to the integrator because a committed file cannot contain its own commit SHA).

## Result

Implemented the Stage-1 dry contract correction required before Stage 2:

- deterministic `schedule_morning` selects browser + Android;
- deterministic `schedule_evening` selects browser + iOS;
- legacy ambiguous `twice_daily` is recognized but fail-closed/ineligible;
- manual profiles select exactly `browser`, `browser_android`, `browser_ios`, or `all`;
- `search_runtime_deploy` requires explicit `standard`/`full`; both select browser + Android + iOS and only `full` requests release qualification;
- missing, `none`, or invalid validation markers select no platforms, regardless of changed paths;
- generation/data/index triggers remain ineligible;
- all Stage-1 plans remain `dry_run=true`, `zero_live=true`, with zero live call counts;
- independent `product_health`, `execution_status`, and `failure_class` mapping is defined;
- added `UNKNOWN_ANDROID_INFRA` and `UNKNOWN_IOS_INFRA` and platform-first mobile infra evaluation;
- only typed `BROKEN_*` produces a product incident, with scope `search-product:<platform>:<failureClass>`;
- one Search POST, zero LLM, zero pagination/receipt/storage-image activity remains unchanged.

## Changed files

- `site/e2e/search/production-health-contract.mjs`
- `site/e2e/search/production-health-planner.mjs`
- `site/tests/search-production-health-contract.test.mjs`
- `site/tests/search-production-health-planner.test.mjs`
- `.codex/lanes/search-production-health-stage2/r14-mobile-contract/RESULTS.md`

## Commands and evidence

1. `node --test site/tests/search-production-health-contract.test.mjs site/tests/search-production-health-planner.test.mjs`
   - PASS: 19/19 tests.
2. `node --test site/tests/search-production-health-*.test.mjs`
   - PASS: 19/19 tests.
3. `git diff --check`
   - PASS.
4. `npm run test:search-production-health`
   - Environment-blocked only in the unrelated workflow-contract test: this isolated worktree has no `site/node_modules/yaml`; the 19 production-health tests in the same command passed. No dependency installation or out-of-scope file change was made.
5. `NODE_PATH=/home/dev/projects/events-bot-new/site/node_modules node --test site/tests/search-e2e-workflow-contract.test.mjs`
   - Same environment blocker: Node ESM package resolution did not use `NODE_PATH`; no further dependency trial was attempted.

## Risks / handoff

- This lane intentionally does not activate or edit workflows, live runners, CLI profile parsing, docs, or `CHANGELOG.md`; those files were forbidden for R14 and remain Stage-2 integration work.
- `DEGRADED` is retained only for backward compatibility and maps to `UNCONFIRMED/PASS` without a failure class or product incident; it is not a scheduled-health terminal state.
- No live Auth, Search, Supabase, browser, emulator, simulator, workflow, deploy, or generation operation was run.
