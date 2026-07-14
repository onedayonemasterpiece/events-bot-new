# Desktop event focus v11 — integration result

- Branch: `feature/event-page-desktop-focus-v11-20260714`
- Base: `origin/feature/event-page-desktop-focus-v10-20260714` (`4f8b2de43fe8b4dfe3770f4bdd94ec29c3c77336`)
- Implementation commit: `a2176763eca0579b8783d7dc044c8c3b8efe0b1a`
- Public preview: <https://kenigevents.ru/preview-20260714t-desktop-focus-v11/lab/event-desktop/>

## Requirement closure

| Requirement | Result |
|---|---|
| R01 arrival CTA hold/join/dock/release | Done |
| R02 natural-ratio poster and adaptive preview packing | Done |
| R03 rejected immediate integrated route removed | Done |
| R04 low-resolution schedule thumbnail without side fields | Done |
| R05 responsive rail shows more real previews before `+N` | Done |
| R06 useful minimum width and centered crop for narrow thumbnails | Done |
| R07 aligned related rows and `<=12%` OCR crop gate | Done |
| R08 exact-source mixed-orientation multi viewer with no nested single-image mode | Done |
| R09 terminal one-event recommendation adapted from the prior mobile pattern | Done |

## Validation

- Static preview build: `448` pages.
- `PREVIEW_BUILD_ID=preview-20260714t-desktop-focus-v11 npm run check:preview`: passed.
- Public HTTP: overview, arrival, low-resolution, related-hybrid, generated JS and CSS return `200`; rejected integrated route returns `404`.
- Public Playwright at `1536x864`: zero horizontal overflow; arrival `hold` and `docked` geometry passed; thumbnail source `3` opened as source `3`; 12 mixed-orientation viewer items; zero nested single-image openers; terminal recommendation passed; related-row geometry and `10.69%`/`21.58%` crop gate passed.
- Fresh `390x844` load: desktop lab root hidden with a zero rectangle and no desktop motion state.
- Gemini 3.1 Pro (High) final gate: `SHIP`, no blocker/high/medium finding.
- Checklist reviewer: R01–R09 `Done`; forbidden production mobile/event files unchanged.

The deploy command was stopped only after the reviewed preview HTML/JS/CSS was public and browser-verified; the remaining output was the non-blocking `/ics/*.ics` mirror tail.

## Files

- `.codex/lanes/desktop-event-focus-v11-20260714/LANE_MAP.yml`
- `.codex/lanes/desktop-event-focus-v11-20260714/RESULTS.md`
- `CHANGELOG.md`
- `docs/features/static-site-pages/event-desktop-media-families-2026-07-12.md`
- `site/scripts/check-preview.mjs`
- `site/src/components/lab/DesktopEventCleanPage.astro`
- `site/src/pages/lab/event-desktop/examples/[scenario].astro`
- `site/src/pages/lab/event-desktop/index.astro`

Production files intentionally unchanged:

- `site/src/layouts/EventLayout.astro`
- `site/src/pages/sobytiya/**`
- `site/src/components/EventCard.astro`
