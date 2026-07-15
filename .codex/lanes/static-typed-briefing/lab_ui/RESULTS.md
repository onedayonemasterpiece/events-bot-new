# Lane lab_ui Results

## Status
committed

## Requirement IDs
- R03
- R04
- R05
- R06

## Branch
`agent/static-typed-briefing/ui`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/typed-briefing-ui`

## Base SHA
`81829363e3a15a4c66cf53d0cf178b3a9fad6f20`

## Head SHA
The branch-tip SHA is returned in the lane handoff (this file is part of that commit).

## Files changed
- `site/src/pages/lab/briefing/index.astro`
- `site/src/data/briefingLab.ts`
- `site/scripts/check-preview.mjs`
- `tests/playwright/static_briefing_lab.spec.ts`
- `.codex/lanes/static-typed-briefing/lab_ui/RESULTS.md`

## Commands run
- `npm ci` (from `site/`)
- `npm run build:preview`
- `PREVIEW_BUILD_ID=... npm --prefix site run check:preview`
- `npm run dev -- --port 4329`
- targeted Playwright/Chromium probes with global Playwright via `NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules`
- `NODE_PATH=... playwright test tests/playwright/static_briefing_lab.spec.ts --list`
- `node --check site/scripts/check-preview.mjs`
- `git diff --check`

## Tests / verification
- Current corrected lab source compiled and served successfully through Astro dev; latest HTML contained 9 scenario rows (8 canonical IDs plus `neutral_fallback`), four category chips, three local fixture cards, qualified visibility hooks, and no lab-authored external stylesheet/script/API request.
- 320x568 Chromium measurement for static B: briefing `85.19px` (`15svh`); first title top `307.89px`; stable decision region top/bottom `293.39/385.45px`, height `92.06px`, fully visible in the initial viewport. No-JS fallback kept the full static briefing and moved that decision region to `245.39–337.45px`.
- Qualified local telemetry probe passed: `eligible_session`, then `first_event_visible`/`briefing_impression` only after 250ms at required ratios, followed by static `briefing_complete`; `assignment_source=qa_query` was present.
- Reveal probe passed with `pointerdown` -> `briefing_interrupt` -> `briefing_complete(interrupt)`. Reduced-motion probe completed immediately with `completion_reason=reduced_motion` and never ran the reveal.
- Playwright discovered all 3 targeted specs. `node --check` and `git diff --check` passed.
- An earlier full preview build completed before the final audit corrections. The corrected full build successfully generated `/lab/briefing/` but later failed while generating unrelated event pages with `ENOSPC: no space left on device` (filesystem had about 104MB free). Therefore final full `check:preview` and the file-backed Playwright spec were not rerun; the integrator should rerun them after freeing space. Generated/installed ignored directories were removed after evidence capture.

## Risks
- Product desirability remains unvalidated; this is an isolated research lab only.
- Full preview regression is pending solely because the host filesystem filled during unrelated bulk page generation; current-page Astro compilation and targeted browser behavior are verified.
- The local debug sink is intentionally bounded to 24 in-memory records and uses session storage only for once-per-session deduplication/reveal replay protection. It performs no remote telemetry or persistence.

## Merge notes
- Cherry-pick the returned branch-tip commit.
- No production route, event-detail page, personalization, Gemini, API, deploy/publisher file, or wordmark behavior is changed.
- After freeing disk, run `cd site && npm ci && npm run build:preview && npm run check:preview`, then `NODE_PATH=/opt/node-v22.22.3-linux-x64/lib/node_modules playwright test tests/playwright/static_briefing_lab.spec.ts` from the repository root.
