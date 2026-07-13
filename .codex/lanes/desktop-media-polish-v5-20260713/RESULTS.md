# Desktop event media polish v5 — integration results

## Status
in-progress

## Branch
`feature/event-page-desktop-media-polish-v5-20260713`

## Base SHA
`0d20185c1ae369fb8a65d140e9fa80cedffa9e19`

## Local verification

- `astro build`: `442` pages, exit `0`.
- `PREVIEW_BUILD_ID=preview-20260713t-desktop-media-polish-v5 npm run check:preview`: passed.
- Chromium source and packaged-preview runs: no page errors or horizontal overflow at `1024×768`, `1440×650`, `1920×600`, and `1920×1080`.
- Editorial internal image offset decreases with downward scroll (`32 → 20 → 7.2px` in the source run); the six thumbnails share one row, there is no duplicate photo-count pill, and thumbnail `2` opened gallery index `2`.
- Split media ratio is `0.5`, slow-track coefficient is `0.28`, and measured travel remains bounded to track overflow.
- Bento media ratio is `0.5`; square cells measured `162×162px`; one real `1.3289` visual image measured `338×162px` as `2×1`; tile index `5` opened gallery index `5`. A runtime mode probe additionally computed visual crop at `50% 50%` and OCR/unknown crop at `50% 0%`; `check:preview` has explicit center-vs-top selector guards.
- Related card article/action wrapper computed transparent on graphite, body/utility computed `rgb(255,250,242)`, share light and like red.
- The desktop surface computed `display:none` at `390×844`.
- `git diff -- site/src/components/EventHero.astro site/src/layouts/EventLayout.astro`: empty.

## Public verification

Pending commit, push and preview deploy.
