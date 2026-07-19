# Lane media Results

## Status
committed

## Requirement IDs
- R01
- R05

## Branch
agent/static-event-v11-regression-repair/media

## Worktree
/home/dev/.codex/worktrees/events-bot-new/static-event-v11-media-repair

## Base SHA
daf1527cfa574ab649801bd9e05e037d84242952

## Head SHA
a761564de5b39947b2ae55af2612e17a5b2e8b40

## Files changed
- `site/src/lib/eventMediaQuality.ts`
- `site/src/lib/desktopEventPresentation.ts`
- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventHero.astro`
- `site/tests/event-media-quality.test.mjs`

## Commands run
- `node --test site/tests/event-media-quality.test.mjs`
- `npm --prefix site ci`
- `npm --prefix site run build`
- `npm --prefix site run test:static-release`
- `npm --prefix site run dev -- --port 8766`
- Playwright CLI checks at 1440x1000 and 390x844 for events 4783 and 6815
- `git diff --check`

## Tests / verification
- Targeted media tests: 4/4 passed.
- Static release behavior tests: 5/5 passed.
- Astro full build: 375 pages built successfully.
- Event 4783 output exposes exactly source indexes `0,4,6,8,9,10,11` on desktop and 7 mobile gallery images; weak source indexes `1,2,3,5,7` are absent, and the mobile hero reports hidden count 5.
- Event 6815 routes to `split-low-resolution-portrait-viewer` with `viewport-contain`; Playwright measured a 720x943 contain frame, no crop, and verified the efficient viewer opens with one image plus the recommendation stop.
- Synthetic test proves all weak originals remain when no technically strong event-local alternative exists; classified documents remain admitted beside strong photos.
- The repository Python `pytest` executable was not available in this worktree/host, so the unrelated static Python source-contract test was not rerun.

## Risks
- Technical quality uses the existing production contract (`long edge >= 720`, area `>= 450000`, `quality_score >= 10`). Missing/weak-only families intentionally remain unfiltered.
- A single weak portrait cannot become sharper; the fix prevents destructive crop/oversized natural scrolling and offers the accepted height-fit viewer rather than inventing pixels.

## Merge notes
- Cherry-pick implementation commit `a761564de5b39947b2ae55af2612e17a5b2e8b40`.
- Canonical feature documentation and `CHANGELOG.md` are owned by the integrator lane and were intentionally not edited here.
