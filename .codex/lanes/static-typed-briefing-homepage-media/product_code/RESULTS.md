# Lane product_code Results

## Status
committed

## Requirement IDs
- R01
- R02
- R03
- R04
- R05
- R07
- R08
- R12 (partial: grounded named-person scene delivered; unsupported comedian-fact template intentionally not asserted)

## Branch
agent/static-typed-briefing-homepage-media/product-code

## Worktree
/home/dev/.codex/worktrees/events-bot-new/product-code

## Base SHA
23cbef7ff358e310843dc8a8bd656cc6a9e386f4

## Head SHA
SELF — exact commit SHA is reported in the worker handoff because this file is part of that commit.

## Files changed
- site/src/data/briefingLab.ts
- site/src/pages/lab/briefing/index.astro
- site/public/brand/announcements-wide-o-ui.svg
- site/scripts/build-briefing-lab.mjs
- site/scripts/check-briefing-lab.mjs
- tests/playwright/static_briefing_lab.spec.ts
- .codex/lanes/static-typed-briefing-homepage-media/product_code/RESULTS.md

## Commands run
- `npm --prefix site run build:lab`
- `npm --prefix site run check:lab`
- `playwright test tests/playwright/static_briefing_lab.spec.ts --workers=1 --grep-invert 'geometry'`
- `playwright test tests/playwright/static_briefing_lab.spec.ts --workers=1 --grep 'local queue|education is|no-JS|lab remains|page is|selected media'`
- `playwright test tests/playwright/static_briefing_lab.spec.ts --workers=1 -g 'geometry'`

## Tests / verification
- Isolated lab build and allowlist check pass (5 emitted files).
- Playwright non-geometry coverage passes in split runs: reveal/static, 16-scenario deck, controls, memory/action semantics, exact O, no cursor linger, no-JS/reduced motion, no remote telemetry, public Next/chain, named-person link, desktop/mobile/reduced-motion media.
- Geometry test passes all 17 scenarios including fallback, variants B/C, and 320×568, 375×667, 390×844, 1440×900: <=3 lines, hero <=50svh, no stage/message/category/body overflow.
- Desktop media sources are resolved at build time only from fixture `image_assets` with safe-crop cover metadata. Mobile stores `data-src` but never assigns `src`.

## Risks
- R12 is partial by design: no humorous “likes to joke” celebrity claim was emitted because fixture facts do not ground that assertion.
- Weather and festival scenes are visibly labelled DEMO signals; they do not claim current live weather or festival state.
- Lab media uses fixture remote image URLs and falls back to text-only on load failure.

## Merge notes
- Cherry-pick the single lane commit.
- No docs or CHANGELOG were touched; integrator owns those files.
- Build ID during verification was `briefing-lab-23cbef7ff358` because the isolated builder keys its default ID to the pre-commit lane SHA.
