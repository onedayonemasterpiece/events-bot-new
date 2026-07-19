# Carousel contracts lane results

## Outcome

Implemented the event-detail media matrix as a shared contract rather than an
event-specific override:

- ordinary `visual_only` photographs use viewport-bounded `cover` in the
  desktop hero and fullscreen gallery even while semantic role enrichment is
  pending;
- OCR/unknown-text and positively classified non-photo documents stay
  `contain`;
- classified non-photo visual primaries no longer fall through to an Editorial
  photo route when no real event photo exists;
- mobile remains the accepted `accepted-v8` surface;
- added the named real-data portrait-carousel lab route
  `/lab/event-desktop/examples/portrait-carousel-production/` using event 4783.

The production portrait example contains seven quality-admitted height-fit
items, retains multiple simultaneously visible images, exposes symmetric group
navigation and discloses `Показаны 7 из 12 изображений в лучшем качестве`.

`docs/features/static-site-pages/test-scenarios.md` is absent on this branch, so
it was not duplicated. The canonical event-page docs note coordination with the
parallel design-system branch.

## Root cause

Event 5658 contains six wide, positively no-text (`visual_only`) photographs,
but the current export still carries `unknown_document/pending`,
`recommended_hero_fit=contain`, `safe_crop=false`. The fullscreen renderer
required completed `classified event_photo + safe_crop + cover`, and Continuous
Editorial independently forced `contain`, so photo display was governed by
asynchronous semantic hints rather than the direct no-OCR evidence.

## Build and static evidence

- build id: `preview-20260717t-static-event-v11-carousel-contracts`
- output: 374 pages total / 303 production event pages
- `check:preview`: pass (`303` events)
- `check:production-desktop`: pass for all `303` event pages
- `check:rail-directory`: pass (`13` pages, `9` routes, `17` locality policies,
  `10` service patterns)
- `check:bus-directory`: pass (`17` localities, `26` venues, `21` stops)
- `git diff --check`: pass

The full production gate now validates every fullscreen image against the
photo-cover/document-contain matrix and validates the selected hero fit.

## Focused Playwright evidence

Evidence JSON:
`/home/dev/projects/events-bot-new/artifacts/codex/static-event-v11-carousel-contracts/carousel-lane/playwright.json`

Screenshots:

- `garage-desktop-1536.png`
- `garage-gallery-cover-1536.png`
- `portrait-carousel-production-1536.png`
- `garage-mobile-390.png`

Measured at `1536×864` on production event 5658:

- hero frame: `1536×807`, starting below the `57px` header;
- hero image: `object-fit: cover`, `object-position: 50% 50%`;
- explicit contracts: `data-hero-render-fit=cover`,
  `data-editorial-crop=bounded-cover`;
- source: `1280×853`, therefore it scales and vertically crops inside the
  bounded `1536×807` hero instead of letterboxing;
- opened fullscreen slide: `visual_only`, semantic state `pending`,
  `data-desktop-gallery-fit=cover`, computed `object-fit: cover`;
- console/page errors: zero.

Measured portrait example at `1536×864`:

- opened viewport: `1536×800`;
- seven height-fit items, `770px` tall, computed `object-fit: contain`;
- first view shows three images (`Фото 1–3 из 7`), rather than one oversized
  portrait;
- `7 из 12` quality disclosure present;
- console/page errors: zero.

Measured mobile 5658 at `390×844`:

- accepted variant remains `accepted-v8`;
- hero computed `object-fit: cover`;
- horizontal overflow `0`;
- console/page errors: zero.
