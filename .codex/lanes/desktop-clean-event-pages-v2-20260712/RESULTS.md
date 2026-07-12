# Lane desktop-clean-event-pages-v2-20260712 Results

## Status
committed

## Requirement IDs
- R01–R12

## Branch
feature/event-page-desktop-clean-pages-v2-20260712

## Worktree
/home/dev/.codex/worktrees/events-bot-new/event-page-desktop-multimedia-analysis

## Base SHA
e9a50f61805dd2ae01222e3ddc1eb6c16126cbdb

## Head SHA
c1b3e0397c3736b026861d079beb5637599a1b7b

## Files changed
- clean desktop event page component and six scenario routes
- desktop lab overview links
- preview build checks
- canonical static-page docs and changelog
- removed the rejected full-flow prototype component fragments

## Commands run
- Gemini 3.1 Pro (High) consultation via the approved local wrapper
- `PREVIEW_BUILD_ID=preview-20260712t-desktop-clean-pages-v2 node site/scripts/build-preview.mjs`
- `PREVIEW_BUILD_ID=preview-20260712t-desktop-clean-pages-v2 npm --prefix site run check:preview`
- local and built-output Chromium acceptance (`qa.mjs`)

## Tests / verification
- Preview build generated successfully.
- `check:preview` passed.
- Local and built-output QA: 42 desktop layout runs, 6 mobile isolation runs and reduced-motion check, zero failures.
- Fullscreen gallery open/advance/Escape, sticky header, first-fold H1/date/venue/CTA, OCR crop/motion policy, parallax, related-card controls and horizontal overflow verified.
- Diff contains no production EventHero/EventLayout/mobile component or stylesheet change.

## Risks
- This is a review preview, not a production promotion.
- The six desktop examples are fixture-backed and are not an automatic production routing implementation.

## Merge notes
Publish the named preview from this branch; do not merge or deploy to production without explicit product approval.
