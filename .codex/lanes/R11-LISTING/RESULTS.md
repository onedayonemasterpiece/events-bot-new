# Lane R11-LISTING Results

## Status
committed

## Requirement IDs
- R11-01
- R11-02
- R11-04
- R11-07

## Branch
`agent/unified-r11/listing`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/r11-listing`

## Base SHA
`7c34d29a2ad65fc6532d934a49d4d48604f79e82`

## Head SHA
Implementation commit: `f9b348a2ec24774b58fe9b0371bca5d87071854d`

## Files changed
- `site/src/components/listings/DateListingSurface.astro`
- `site/src/components/listings/ListingDiscoveryRail.astro`
- `site/src/components/listings/MobileListingRailRow.astro`
- `site/src/components/listings/MobileListingRailSurface.astro`
- `site/src/styles/design-system.css`
- `site/tests/mobile-listing-rails.test.mjs`
- `site/tests/mobile-listing-rails.playwright.mjs`
- `.codex/lanes/R11-LISTING/RESULTS.md`

## Commands run
- `npm ci`
- `node --test tests/mobile-listing-rails.test.mjs`
- `npm run build:preview`
- `PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail npm run dev`
- `R9_RAIL_BASE_URL=http://127.0.0.1:4321 node tests/mobile-listing-rails.playwright.mjs`
- `git diff --check`
- `git diff --cached --check`

## Tests / verification
- Static mobile listing rail suite: 9/9 passed.
- Preview build: passed; 431 pages built.
- Mobile listing Playwright acceptance: passed at 320px and 390px, including:
  - first-swipe consent, cancel/no-store, committed consent, later direct negative swipe, and Undo;
  - exact `8–9 августа` range with full accessible schedule;
  - desktop pinned date context, filter geometry, and leather reservation;
  - today-only past/started-earlier main-media muting.
- `git diff --check`: passed.

## Risks
- A second optional artifact-enabled preview build exhausted the shared runner disk after the first successful full preview build. Generated `dist/` output was removed, and the artifact-enabled browser acceptance was completed successfully against Astro dev instead.
- The existing Vite warning about mixed JSON import attributes in `listingPresentation.ts` remained unchanged and did not fail the build.

## Merge notes
- No `docs/**` or `CHANGELOG.md` files were edited, per lane ownership.
- Negative-swipe consent is stored only after the canonical hidden control reaches `aria-pressed="true"`; storage exceptions fail closed.
- Desktop date context reserves its grid column before it becomes visible, so pinning does not reflow the city/time controls.
- Temporal styling targets only `.event-media > img` inside the mobile rail.
