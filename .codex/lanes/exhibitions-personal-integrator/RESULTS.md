# Lane exhibitions-personal-integrator Results

## Status
committed

## Requirement IDs
- R01
- R02
- R03
- R04
- R05
- R06
- R07
- R08
- R09

## Branch
integration/exhibitions-personal-discovery-prototype-20260719

## Worktree
/home/dev/.codex/worktrees/events-bot-new/exhibitions-personal-discovery-prototype-20260719

## Base SHA
c587a0cf86e144a88c0457035866c8325ea59dc5

## Head SHA
baebee6272dd273fe7445f983e08344b8d5dcd9d

## Files changed
- site/src/pages/lab/exhibitions-personal/index.astro
- site/src/components/ExhibitionPrototypeRow.astro
- site/scripts/check-exhibitions-personal-prototype.mjs
- docs/features/static-site-pages/exhibitions-personal-prototype.md
- docs/features/static-site-pages/README.md
- docs/routes.yml
- CHANGELOG.md
- .codex/integration/exhibitions-personal-discovery-prototype-20260719-LANE_MAP.yml

## Commands run
- npm run build
- node scripts/check-exhibitions-personal-prototype.mjs
- Playwright CLI browser QA at 375×812, 768×1024 and 1440×1000
- a-gemini / Gemini 3.1 Pro (High) product design review, acceptance critique and final gate

## Tests / verification
- Astro build passed.
- Static contract: 17/17 passed; 12 unique exhibition rows and 3 new rows.
- Browser: zero horizontal overflow at all three viewports; no console errors; no visible button below 44×44.
- Interaction: ArrowDown roving focus, L like, X reject, undo focus return, input shortcut guard, six-image gallery, cold/relevant/soft badge states passed.
- Reject stub height delta is exactly 0.
- Gemini final gate: Accept; no P0 remains.
- `git diff origin/main -- site/src/pages/vystavki/index.astro` is empty.

## Risks
- Prototype social discussion/mention counts are presentation-only fixtures.
- Prototype state is local-only and intentionally isolated from the production personalization key.
- Production analytics must not compare stub-inflated scroll/time directly with later sessions where rejected rows are absent.

## Merge notes
- This is a separate lab route. Do not replace production `/vystavki/` without a subsequent product/data integration gate.
- `INC-2026-07-02-exhibition-duplicates-static-site` remains open; this lane performs only the prototype-scope curated identity/type/duplicate checks and does not claim incident closure.
