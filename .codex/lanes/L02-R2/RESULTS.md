# Lane L02-R2 Results — compact related cards and event-detail keyboard navigation

## Lane contract

- Requirements: `R02` (event 6686 related-card crop/height), `R04`
  (event-detail keyboard navigation, including 6529).
- Branch: `agent/static-unified-r2/cards-nav`.
- Owned implementation: `relatedCardLayout.mjs`, `EventCard.astro`,
  `OptimizedEventCardGrid.astro`, `KeyboardEventNavigationPrototype.astro`, the
  event route's preview keyboard gate, focused tests, and this record.
- `DesktopEventPage.astro`, `EventLayout.astro`, canonical docs and
  `CHANGELOG.md` were not edited.

## Outcome

### R02 — 6686 related-card row

The tall photographed `6764` card was not a row-optimizer inference problem.
Its exported 180×320 thumbnail is `unknown/error`, but the existing
`listingMediaOverrides.json` producer manifest already contains a human-reviewed,
canonical-source-URL-keyed replacement:

- source: `.../67139633...e9839.webp`;
- replacement: `.../67139633...e9839-1080.webp`, 1080×720;
- evidence: `source-video-frame-0-reviewed`;
- mode: `visual_only`.

Compact EventCard geometry and rendering now reuse that exact reviewed evidence.
No event-id exception and no new semantic inference was introduced. All other
unknown/error media remains fail-closed.

Photo-only rows now use one canonical horizontal `5/4` frame. Every card in the
row uses `cover`; the row shares one media height and CSS grid row height. The
existing DP still decides group membership globally, keeps all non-final rows
full, permits only the last remainder, and enforces unchanged OCR/document
intervals and the 20% very-tall-document crop ceiling.

Generated 6686 acceptance row:

- row 2: events `6764`, `5658`, `7023`;
- all `data-lab-media-kind=visual`;
- all `data-lab-row-media=visual-compact-5x4`;
- all `--lab-row-media-ratio:1.25000`;
- computed media heights: `302.390625px` each;
- computed total card heights: `555.75px` each;
- computed `object-fit: cover` for all three;
- 6764 renders the prefix-local reviewed 1080×720 WebP.

### R04 — keyboard navigation

The r1 preview omitted the router because it was gated to the production family
unless a force env was manually supplied. Every named immutable preview now
mounts the reviewed V7 router by default while retaining the explicit global
disable flag and production-family behavior. Local unnamed development remains
unchanged unless forced.

The existing router already derives arrow order from computed CSS geometry
(`visualCardRows`) rather than reordered DOM adjacency; this is now reachable in
preview artifacts. The K keycap keeps its reserved space but is visible only in
the focused card. Current-event shortcut badges are also hidden ambiently and
become visible only when their owning action/action panel has visible focus.

Generated 6529 Chromium evidence:

- `[data-keyboard-event-navigation-mounted]`: 1;
- `[data-keyboard-event-surface]`: 1;
- ambient shortcut badge: `visibility:hidden`, `opacity:0`;
- first `ArrowDown` focuses visual row 0 / column 0, event `7032`;
- `ArrowRight` focuses visual row 0 / column 1, event `6955`;
- x coordinates advance `130px -> 530px` with identical card height
  `725.859375px`;
- focused card K hint: `visibility:visible`, `opacity:0.58`.

## Validation

```text
node --experimental-strip-types --test \
  site/tests/visual-keyboard-regressions.test.mjs \
  site/tests/keyboard-event-navigation-production.test.mjs
# 30/30 PASS

PREVIEW_BUILD_ID=preview-20260723-r2-l02 npm --prefix site run build:preview
# PASS, 389 pages
# 6529 generated HTML contains data-keyboard-event-navigation-mounted

PREVIEW_BUILD_ID=preview-20260723-r2-l02 npm --prefix site run check:preview
# PASS, 288 events

PREVIEW_BUILD_ID=preview-20260723-r2-l02 npm --prefix site run check:unified-prototype
# PASS, 288 event pages, 373 checked related cards

git diff --check
# PASS
```

## Files changed

- `site/src/lib/relatedCardLayout.mjs`
- `site/src/components/EventCard.astro`
- `site/src/components/KeyboardEventNavigationPrototype.astro`
- `site/src/pages/sobytiya/[slug].astro`
- `site/tests/visual-keyboard-regressions.test.mjs`
- `site/tests/keyboard-event-navigation-production.test.mjs`
- `.codex/lanes/L02-R2/RESULTS.md`

## Integration notes / risks

- Integration must update canonical image-framing/keyboard documentation and
  `CHANGELOG.md`; this lane was explicitly forbidden to edit them.
- The reviewed replacement is intentionally source-keyed. If producer source
  bytes/URL changes, the override no longer applies and fail-closed behavior
  returns.
- This lane did not deploy. The integrator must rebuild the final combined
  preview because other R2 lanes change shared chrome/data.
