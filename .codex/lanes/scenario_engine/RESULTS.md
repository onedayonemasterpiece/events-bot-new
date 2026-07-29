# Lane scenario_engine Results

## Status
committed

## Requirement IDs
- R02
- R03
- R04

## Branch
`agent/autopresenter-pwa-three-scenes/scenario-engine`

## Worktree
`/home/dev/projects/events-bot-new-autopresenter-scenario-engine`

## Base SHA
`edce9daf9d5e5e6ea9e2041187ae2726db5c62a9`

## Head SHA
Implementation commit: `5c5dc2d63d61ae08a6a4b9348e79686b4dde81de`

The final results-only commit is returned to the integrator separately because a file cannot contain its own commit SHA.

## Files changed
- `tools/autopresenter/agent/README.md`
- `tools/autopresenter/agent/agent.mjs`
- `tools/autopresenter/agent/pacing.mjs`
- `tools/autopresenter/agent/scenario-contract.mjs`
- `tools/autopresenter/agent/test/pacing.test.mjs`
- `tools/autopresenter/agent/test/static-contract.test.mjs`
- `tools/autopresenter/relay/Dockerfile.internet-test`
- `.codex/lanes/scenario_engine/RESULTS.md`

## Implemented
- Exactly three explicit command scenario IDs with `tomorrow-mobile` fallback and rejection of unknown IDs.
- Shared readiness gates for complete document, fonts, optional mobile-v23 readiness, visible media, and sampled settling.
- Pure bounded wheel trajectory generation at the requested velocity/duration/sample pacing, real wheel input, small-only final correction, visible tap/swipe cues, hidden cursor, 12-second typical minimum, and 30-second abort ceiling.
- Improved `tomorrow-mobile` flow with natural vertical movement and 18x34ms rail drags.
- `tomorrow-rail-like` fresh-context flow using current `/zavtra/` canary 5296 (`Концерт «Фестиваль Pianissimo: Жуан Нету Виейра»`) per integration clarification: one real drag to maxScroll, distinct >=120px armed edge pull asserted before mouseup, visible consent acceptance, canonical feedback/storage/count assertions, and reload persistence.
- `weekend-amber-artifact` fresh-context flow through the mobile discovery drawer, DOM-derived marker checked against snapshot 7014, natural vertical movement, real rail drags, first-tap storage/ARIA/custom-event/URL assertions, reload, second tap, hash route, dialog, and found-count assertion.
- Internet-test Docker site build is preview-only with amber research enabled and a deterministic preview seed selecting snapshot event 7014. Production remains fail-closed in the existing site gate.

## Commands run
- `node --check tools/autopresenter/agent/agent.mjs`
- `cd tools/autopresenter/agent && npm test`
- `git diff --check`
- `cd site && npm ci --no-audit --no-fund`
- `cd site && PUBLIC_SITE_MODE=preview PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail PUBLIC_PREVIEW_BUILD_ID=autopresenter-54 npm run build`
- `cd site && node --test tests/mobile-listing-rails.test.mjs tests/artifacts.test.mjs tests/artifact-generated.test.mjs`

## Tests / verification
- PASS: agent Node suite, 16/16 tests.
- PASS: `node --check` for the scenario agent.
- PASS: `git diff --check`.
- PASS: site dependencies installed successfully for validation, then removed with generated output.
- PARTIAL: targeted site source tests passed 14 relevant tests; `artifact-generated.test.mjs` was invoked without its required `ARTIFACT_GENERATED_ROOT` and therefore failed its setup guard.
- BLOCKED: full Astro build progressed through entrypoint compilation and substantial route generation, then failed with `ENOSPC: no space left on device`; the partial `dist/` and installed `node_modules/` were removed. At cleanup the shared volume had only 289 MB free.

## Risks
- Full headed/live three-scenario E2E is deferred to integration E2E as directed by the integrator.
- The scenario agent intentionally fails on snapshot drift, pre-liked state, non-incrementing counts, missing normalized feedback, unarmed pulls, or hidden/oversized corrections rather than forcing DOM state.
- The one-release maxScroll gesture asserts that the rail can be covered by a bounded visible drag; a future materially wider rail will fail explicitly rather than using hidden scroll mutation.

## Merge notes
- Cherry-pick the final lane head returned with this handoff; it includes the implementation commit above plus this results record.
- No relay control/server, `tools/autopresenter/m0`, docs, `CHANGELOG.md`, or other forbidden files were edited.
- No push was performed.
