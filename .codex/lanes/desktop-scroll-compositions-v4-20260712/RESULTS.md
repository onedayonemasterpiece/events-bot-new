# Desktop scroll compositions v4 — integration results

## Status
local-gates-complete-pending-public-release

## Requirement IDs
- R01–R05

## Branch
`feature/event-page-desktop-scroll-compositions-v4-20260712`

## Base SHA
`09a21a1da544c51ccfbf1d56733b52150cd74c25`

## Files changed
- `site/src/components/lab/DesktopEventCleanPage.astro`
- `site/src/components/lab/DesktopEventActionPanel.astro`
- `site/src/pages/lab/event-desktop/examples/[scenario].astro`
- `site/src/pages/lab/event-desktop/index.astro`
- `site/scripts/check-preview.mjs`
- canonical desktop-preview docs and `CHANGELOG.md`

## Verification
- Astro build: 442 pages.
- `check:preview`: passed for `preview-20260712t-desktop-continuous-scroll-v4`.
- Chromium visual review: Editorial continuous slab, promoted event-4671 landscape, Split OCR slow poster, physical reading strip, adaptive Bento and related-card release.
- Corrected machine viewport gates: Editorial title and zero overflow at `1440×650`; Split/Bento zero overflow at `3440×1440`; reduced-motion Reading track has no transform.
- Production mobile diff gate: no `EventHero.astro`, `EventLayout.astro` or production mobile file changes; lab root remains hidden below `1024px`.
- Gemini 3.1 Pro High review completed; `a-opus` was unavailable with `Individual quota reached` and was not replaced by a lower-class model.
- Closure re-review: R01–R05 and desktop/mobile isolation are Done; no code blocker remains.

## Risks
- This is a noindex lab preview, not a production event-page rollout.
- Media metadata has known width/mode errors; v4 uses runtime natural ratios for Bento and explicit reviewed hero dimensions for event 4671.
