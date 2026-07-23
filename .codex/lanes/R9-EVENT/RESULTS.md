# R9-EVENT results

## Lane contract

- Lane: `R9-EVENT`
- Requirements: `R1`, Telegram `R9` (messages 624/625)
- Base branch / SHA: `integration/mobile-acceptance-r9-20260723` / `74bb254c4d20c1e488568fde445515131e64cbd5`
- Implementation head SHA: `07e1943ff2815334e5cbd81c6fb14312c8d722f5`
- Worktree branch: `agent/mobile-acceptance-r9/event`
- Result: complete

## Accepted donor provenance

R1 reuses the accepted Reference4 mobile leather stack rather than drawing or
generating another tag:

- `94833f10479aad38d833c25a66ced07f1274e8ca`
  (`fix(static-site): move leather brand tag with mobile drawer`) supplied
  `site/public/assets/ui/mobile-head-skinny-leather-3x.webp` and
  `site/public/assets/ui/mobile-head-skinny-leather-3x.metadata.json`.
- `3f5b88f9d8b0c9835908c6b7cf924314deccfb6a`,
  `ac110fb0c1879dd3da4bb13af0a7d4f9510d1c30`, and
  `15106c51c4267cf0254cf40a7a7e5a6a7e022fc3` are the accepted Reference4
  shell/stacking lineage. The final named donor adaption point is
  `site/src/components/Reference4MobileMenu.astro` at `15106c51`.

The runtime keeps that stitched WebP as the normal visible skin. CSS
`background-color:#98401f` now paints immediately below it, so the live white
lockup is readable during decode and on image failure. No pale scrim or old
flat production skin was added.

## Implementation

- `site/src/components/Reference4MobileMenu.astro`
  - keeps the accepted shared leather asset;
  - separates fallback colour from the background image so terracotta paints
    immediately and survives asset failure;
  - keeps the live wordmark, physical shadow, and accepted drawer ownership.
- `site/src/components/MobileEventProductionStyles.astro`
  - applies one shared event-detail identity scale,
    `clamp(84px, 23vw, 92px)`, to all non-pill mobile tokens;
  - organizer/Main and free/Secondary therefore share physical diameter and
    row baseline without slug/logo hacks;
  - preserves the Pushkin composite's intentionally wider wordmark geometry;
  - bounds the row/artwork to the mobile continuation width.
- `site/tests/mobile-event-chrome-contract.test.mjs`
  - locks the exact accepted leather asset/hash and terracotta fallback;
  - locks the shared mobile token scale, free badge source, and unchanged
    Main/Secondary role projection.

## Validation and evidence

Commands run from `site/` unless noted:

1. `node --test tests/mobile-event-chrome-contract.test.mjs`
   - PASS: 2/2.
2. `node --test --test-name-pattern='free admission uses the dedicated inline medallion' tests/event-detail-runtime-regressions.test.mjs`
   - PASS: 1/1.
3. `npm run build`
   - PASS after linking the base integration worktree's existing
     `site/node_modules`;
   - Astro built 386 pages in 3m 9s;
   - emitted only the pre-existing Vite warning about inconsistent JSON import
     attributes in `listingPresentation.ts`.
4. Headless Chromium at `390x844`, real built event `7007`
   (`Концерт VI Творческой школы «Камертон»`):
   - document/body scroll width: `390px` (no horizontal overflow);
   - computed tag image:
     `/assets/ui/mobile-head-skinny-leather-3x.webp`;
   - computed fallback: `rgb(152, 64, 31)`;
   - KONB Main venue medallion: `89.69 x 89.69px`;
   - free Secondary medallion: `89.69 x 89.69px`;
   - both bottom edges: `687.02px` (same baseline);
   - both images complete with `512 x 512` intrinsic size.

Uncommitted QA artifacts (intentionally ignored):

- `artifacts/codex/r9-event-mobile/event-7007-390-top.png`
- `artifacts/codex/r9-event-mobile/event-7007-390-medallions.png`
- `artifacts/codex/r9-event-mobile/event-7007-390-metrics.json`

## Known pre-existing test debt / risk

`node --test tests/mobile-shell-toast.test.mjs` passes 6/7 and fails the
`EventLayout is the single mobile shell owner...` assertion because that test
still requires a removed `.reference4-menu__brand::before` pale-scrim rule.
The identical focused assertion fails on untouched base
`74bb254c` in the integration worktree. It is unrelated to this lane and
conflicts with the current no-pale-scrim acceptance direction, so this lane did
not reintroduce it or edit the broad shell test.

## Required documentation delta (not edited by lane instruction)

The integrator should make these exact canonical updates:

1. `docs/features/static-site-pages/mobile-shell.md`
   - state that full mobile event detail uses the accepted
     `mobile-head-skinny-leather-3x.webp` Reference4 tag;
   - state that live white lettering has immediate `#98401f` CSS fallback and
     no pale scrim.
2. `docs/features/static-site-pages/event-token-medallions.md`
   - replace the mobile-detail size wording with the runtime contract
     `clamp(84px, 23vw, 92px)` (about `89.7px` at 390px);
   - state that Main/Secondary is semantic/placement priority, not visual size;
   - organizer/source/program/free circles share diameter and baseline, while
     the Pushkin composite may remain wider for its wordmark.
3. `CHANGELOG.md` `[Unreleased] / Fixed`
   - add: mobile event detail now preserves the accepted leather header tag
     with a terracotta failure fallback and normalizes organizer/free medallion
     diameter and baseline.

## Changed files

- `site/src/components/MobileEventProductionStyles.astro`
- `site/src/components/Reference4MobileMenu.astro`
- `site/tests/mobile-event-chrome-contract.test.mjs`
- `.codex/lanes/R9-EVENT/RESULTS.md`
