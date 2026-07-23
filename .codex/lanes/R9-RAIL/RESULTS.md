# R9-RAIL Results

## Lane

- Lane ID: `R9-RAIL`
- Requirements: `R2`, `R3`, `R4`, `R5`, `R6`, `R10`
- Base SHA: `74bb254c4d20c1e488568fde445515131e64cbd5`
- Implementation head SHA: `675be149f9732e25118a5d540141af84f73baba5`
- Branch: `agent/mobile-acceptance-r9/rail`

## Requirement outcome

| Requirement | Status | Evidence |
|---|---|---|
| R2 | Done | Fixed-size media skeleton with cached-image decode, load and neutral error states; browser gate verifies no geometry shift. No accepted skeleton donor was identified, so this is not described as donor parity. |
| R3 | Done | Only a single explicitly classified `visual_only` portrait can become a `140×112`/`5:4` cover. OCR, unknown/document roles and multi-image rows fail closed to authored contain geometry. |
| R4 | Done | Exact v28 structure: fixed `56px` date accessory above `64px` bottom nav, 42-day horizontal rail, centered selected date where scroll range permits, calendar trigger and bottom sheet. Only generated routes are links; unavailable dates are 52px disabled non-links. Playwright fetched every emitted href with HTTP 200. |
| R5 | Done | Accepted A-tail artifact is directly reachable on `/vyhodnye/`, after the large like, behind `!IS_PRODUCTION` plus `PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail`. Event `6939` is preferred with deterministic Saturday/weekend fallback. State remains localStorage-only; reduced motion is static. |
| R6 | Done | The page hero swaps into a compact fixed `.sticky-date` under `body.is-date-pinned`; `.feed-head` remains normal flow. Shelf headers physically stick at `top:64px`; Popular is `80px` high. `overflow-x:clip` avoids creating the unintended scrolling ancestor that broke sticky behavior. |
| R10 | Done | Inline straight arrow has a `48×23` box, horizontal shaft, and symmetric 45-degree head. |

## Donor provenance

- Accepted integration donor: commit `3f5b88f9`, branch recorded in the accepted Telegram thread as `integration/mobile-search-unified-v14-20260722`.
- Exact v28 public specimens inspected under `artifacts/codex/r8-telegram/v28-public/` for calendar structure, sticky hierarchy and rail CSS.
- R5 accepted artifact implementation donor: commit `2a791e6b`; canonical contract: `docs/features/static-site-pages/amber-artifact-easter-egg.md`.
- R10 defect evidence: accepted Telegram screenshot `artifacts/codex/r8-telegram/photo_2026-07-23_19-48-41.jpg` and thread message identifying the crooked arrow. The replacement uses explicit inline SVG geometry rather than a visual imitation.
- R2 and the narrow R3 guardrail had no accepted visual donor. They are recorded as requirement implementations, not “inspired by” or claimed donor parity.
- Existing R8 rail geometry from base ancestry is preserved: `112px` row/viewport height, `296×112` summary, full viewport scroll window.

## Commands and evidence

### Focused Node tests

```text
node --test site/tests/mobile-listing-rail-media.test.mjs \
  site/tests/mobile-listing-rails.test.mjs \
  site/tests/amber-artifact.test.mjs
```

Result: `10/10` passed.

### Browser acceptance on final source

Final-source Astro dev server:

```text
PUBLIC_SITE_MODE=preview \
PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail \
PUBLIC_PREVIEW_BUILD_ID=r9-rail-final \
npm run dev -- --port 4190
```

Gate:

```text
R9_RAIL_BASE_URL=http://127.0.0.1:4190 \
node tests/mobile-listing-rails.playwright.mjs
```

Result: passed at `320×700` and `390×844`.

The gate covers:

- `/segodnya/`, `/vyhodnye/`, `/populyarnoe/`;
- row/window/summary `112px` geometry and no horizontal page overflow;
- arrow `48×23` bounding box and literal path;
- sticky title `0..64`, shelf head `top:64`, Popular head height `80`;
- date accessory and bottom sheet geometry;
- all emitted date-accessory hrefs return HTTP 200;
- loading, cached/reload and error image states without size change;
- A-tail order, `94×112` control, 72% wake, collection/localStorage;
- static reduced-motion state.

### Static build

A complete `386`-page Astro build passed before the final unavailable-date
cells were changed from broken links to disabled non-links. On the final source,
the client/server compilation and focused route rendering passed through the
Astro dev server and the browser gate above.

Two subsequent full final-source build attempts were externally terminated
with exit `143` during static route generation. There was no Astro error in
the log; disk had about `4.5GB` free and memory about `2.4GB` available.
Per integration-owner direction, no further full-build retry was made in this
lane. The integration owner will run one full build after cherry-pick.

## Risks / integration notes

- Full final-source static generation remains an integration gate because of
  the external `SIGTERM` limitation above.
- The generic `/date-YYYY-MM-DD/` route does not exist and was outside this
  lane's writable scope. The calendar therefore renders those dates as
  accessible disabled cells rather than public 404 links.
- The Easter egg is absent unless the explicit research env is set, and is
  hard-blocked in production even if that env is accidentally present.
- No `dist/`, logs, images or generated artifacts remain in the worktree.

## Changed files

- `docs/features/static-site-pages/amber-artifact-easter-egg.md`
- `site/src/components/listings/AmberRailArtifact.astro`
- `site/src/components/listings/DateListingSurface.astro`
- `site/src/components/listings/MobileDateAccessory.astro`
- `site/src/components/listings/MobileListingRailRow.astro`
- `site/src/components/listings/MobileListingRailSurface.astro`
- `site/src/components/listings/WeekendListingSurface.astro`
- `site/src/lib/mobileListingRailMedia.mjs`
- `site/tests/mobile-listing-rail-media.test.mjs`
- `site/tests/mobile-listing-rails.playwright.mjs`
- `site/tests/mobile-listing-rails.test.mjs`
- `.codex/lanes/R9-RAIL/RESULTS.md`
