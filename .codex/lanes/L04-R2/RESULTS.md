# L04-R2 — desktop event chrome

## Status

Done.

## Delivered

- Desktop Editorial event breadcrumbs are now a semantic ordered navigation over
  the lower photograph, ending 4 px above the cream reading sheet.
- The trail is intentionally small and subordinate, with a soft lower glow and
  text shadow for changing-photo contrast.
- The Editorial reading sheet now has 28 px rounded top corners. Split event
  pages retain their existing hierarchy; mobile retains the parent-only
  breadcrumb and does not render the desktop overlay.
- The global desktop announcements tag and desktop full-screen gallery tag use
  `/assets/ui/desktop-head-leather.webp`.
- The WebP is a deterministic 30:11 crop of the supplied
  `docs/features/static-site-pages/references/head-desctop-skin.png`, matching
  the existing 240×88 tag ratio. It uses only the leather interior, not the
  stitched border, phone or page background.
- The existing solid terracotta colour remains the actual CSS background under
  the image. The 240×88 silhouette and white lockup paint immediately when the
  WebP is pending or fails.
- Crop coordinates, source/output hashes and the Pillow conversion recipe are
  recorded in `desktop-head-leather.metadata.json`.

## Validation

- `PREVIEW_BUILD_ID=preview-20260723-unified-corrections-r2-l04 npm --prefix site run build:preview`
  — PASS, 389 pages.
- Focused Node suite — PASS, 19/19:
  - `desktop-chrome-contract.test.mjs`
  - `breadcrumbs-contract.test.mjs`
  - `desktop-editorial-motion.test.mjs`
  - `event-detail-runtime-regressions.test.mjs`
- `git diff --check` — PASS.
- Chromium 1440×1000 on event 6529:
  - horizontal overflow: 0;
  - breadcrumb top/bottom: 679/723 px;
  - sheet top: 727 px;
  - sheet top radius: 28 px;
  - breadcrumb link colour: `rgb(255,250,242)`;
  - leather resource loaded.
- Chromium with the leather WebP forced to HTTP 404:
  - tag background: `rgb(152,64,31)`;
  - lockup colour: `rgb(255,250,242)`;
  - geometry remains 240×88.
- Chromium 390×844:
  - horizontal overflow: 0;
  - desktop event surface/overlay hidden;
  - mobile parent link remains visible.

Screenshots (ignored artifacts):

- `artifacts/codex/r2-l04/event-6529-desktop-final.png`
- `artifacts/codex/r2-l04/event-6529-brand-fallback.png`
- `artifacts/codex/r2-l04/event-6529-mobile.png`

## Integration notes

- The 4.9 MB reference PNG is currently an untracked user-supplied file in the
  main checkout; this lane intentionally commits only the optimized 19 KB WebP
  and complete provenance metadata.
- No search, card optimizer, keyboard-navigation, transport, canonical docs or
  CHANGELOG files were changed.
