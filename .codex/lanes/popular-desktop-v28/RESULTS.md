# Lane popular-desktop-v28 Results

## Status
committed

## Requirement IDs
- R01–R09

## Branch
`integration/popular-desktop-v28-20260720`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/popular-desktop-v28-20260720`

## Base SHA
`d58119ba`

## Head SHA
Implementation commit: `6c191ad1`

## Files changed
- Desktop Popular data/grouping/rendering and shared optional card metadata.
- Bounded client-side 4+1 selector and its unit tests.
- Preview regression checks, canonical docs and changelog.

## Commands run
- `PREVIEW_BUILD_ID=preview-20260720-popular-desktop-v28 npm run build:preview`
- `PREVIEW_BUILD_ID=preview-20260720-popular-desktop-v28 npm run check:preview`
- `node --test tests/popular-desktop-listing.test.mjs`
- `npm run check:listing-desktop-geometry` against the local immutable build
- Playwright mobile checks at 360/390/430 and warm/cold profile DOM checks

## Tests / verification
- Static preview gate: pass, 220-event acceptance fixture.
- Personalization unit tests: 3/3 pass.
- Desktop geometry: 12/12 pass at 1366/1536/1920.
- Mobile large/adaptive: 25/25 identical order, both modes switch at all three widths, zero overflow.
- Cold profile: sixth shelf absent. Warm profile: 4 affinity + 1 anti-bubble, no global-family overlap.

## Risks
- The acceptance fixture was generated on 18 July 2026; the preview is not evidence of a new 20 July production export.
- This V28 lineage remains based on the pushed V27 hotfix chain, which is not yet reachable from `origin/main`.

## Merge notes
Single serial integration lane; no parallel writable branch and no lost changes.
