# Lane media_polish results

## Scope

- Lane: `media_polish`
- Requirements: `R02`, `R03`
- Base SHA: `4542a7dfaedf3d86ea4b5e4618e06e717f0dc8cf`
- Tested implementation SHA: `5f161b8c267b95a4ca934257cdd3ced42fde9e08`
- Branch: `agent/static-event-preprod/media-polish`

## Outcome

- **R02 done:** the desktop title-panel medallion wrapper and token row expose
  visual overflow, so a venue medallion's 2px outer ring and 28px soft shadow
  are no longer clipped. `resolveEventMedallions` and its one-venue
  fail-closed/ambiguous-source behavior were not changed.
- **R03 done:** initial desktop `Смотрите дальше` HTML contains a visible
  reduced-motion-safe skeleton and server-declared row geometry for every image
  card. Each image removes `aria-busy` and the skeleton on `load` or `error`;
  errors reveal the existing fallback. Alt text, quality admission, media role,
  row packing and the <=20% document crop gate remain intact.
- Mobile retains the separate accepted renderer; its 390px QA surface has zero
  horizontal overflow.

## Evidence and commands

- `npm ci --prefix site` — dependencies installed; npm reported two unrelated
  low-severity audit findings.
- `npm --prefix site run test:content-media` — 6/6 pass, including the existing
  Unicode-boundary and single-venue fail-closed regressions.
- `node --test site/tests/event-media-quality.test.mjs site/tests/personal-feed-surface.test.mjs`
  — 9/9 pass.
- `PREVIEW_BUILD_ID=preview-media-polish-final npm --prefix site run build:preview`
  — pass; 303-event preview built in 2m34s.
- `PREVIEW_BUILD_ID=preview-media-polish-final npm --prefix site run check:preview`
  — pass (`303 events`, `strict_related=false`).
- `PREVIEW_BUILD_ID=preview-media-polish-final node --test site/tests/event-detail-runtime-regressions.test.mjs`
  — 10/10 pass, including built-HTML skeleton and medallion overflow contracts.
- Chromium Playwright against the final built preview at `1920x1080` and
  `390x844` — pass. Ten delayed continuation images all showed initial
  skeletons, retained non-empty alt text, and measured `0px` height and document
  top delta after load. Forced image failure removed busy/loading state, hid the
  skeleton and exposed fallback at unchanged height. Desktop medallion geometry
  measured `overflow: visible` on wrapper/row, `venueCount=1`, resolved identity
  and zero horizontal overflow. Mobile measured `scrollWidth=viewport=390`,
  desktop hidden and mobile visible.
- Playwright report and screenshots (ignored artifacts):
  `artifacts/codex/media-polish/playwright-final-report.json`,
  `artifacts/codex/media-polish/after/final-built-fhd-skeletons.png`,
  `artifacts/codex/media-polish/after/final-built-fhd-loaded.png`,
  `artifacts/codex/media-polish/after/final-built-fhd-medallion.png`,
  `artifacts/codex/media-polish/after/final-built-mobile.png`.

## Known risk / unrelated gate

`PREVIEW_BUILD_ID=preview-media-polish-final npm run check:production-desktop`
continues to fail one pre-existing routing expectation: event `5756` renders as
`editorial/editorial-replaces-non-identity-document-with-classified-photo`,
while the checker still expects
`split/split-resolution-constrained-landscape`. This lane did not change the
accepted presentation router, event data, or that checker; all R02/R03 focused
and preview gates pass. Integration should reconcile that stale v11/v12
expectation in its owning lane rather than broadening this media-polish lane.

## Changed files

- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventCard.astro`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `docs/features/static-site-pages/event-token-medallions.md`
- `docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md`
- `CHANGELOG.md`
- `.codex/lanes/media_polish/RESULTS.md`
