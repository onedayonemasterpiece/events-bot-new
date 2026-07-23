# R5 desktop presentation polish — integration report

Date: 2026-07-23  
Branch: `integration/desktop-polish-cleanup-r5-20260723`  
Implementation SHA: `b6e040b7461c79480fe7afe222b15b16d9d64911`

## Requirement closure

| ID | Status | Evidence |
| --- | --- | --- |
| R01 | Done | Central register in `docs/features/static-site-pages/presentation-release-checklist.md`; `TD-PRESENTATION-UI-001` records the clubs glow and is closed in R5. |
| R02 | Done | R5 leather WebP removes the offset lower/right backing and pale remnant while retaining the stitched face; public desktop screenshot `artifacts/codex/r5-public/event-desktop.png`. |
| R03 | Done | Desktop `Ближайших встреч` label has lower-directed shadow and radial glow; public desktop screenshot `artifacts/codex/r5-public/clubs-desktop.png`; browser computed-style assertion passed. |
| R04 | Done | Review URLs sent with the approved local `TELEGRAM_AUTH_BUNDLE_E2E` human session to `KenigEvents · UI review`, topic anchor `548`; verified message `608`. S22 was not used. Receipt: `artifacts/codex/r5-telegram/send-receipt.json`. |

## Published review object

- Hub: <https://kenigevents.ru/preview-20260723-unified-corrections-r5/__preview/>
- Clubs: <https://kenigevents.ru/preview-20260723-unified-corrections-r5/kluby-po-interesam/>
- Event 6529: <https://kenigevents.ru/preview-20260723-unified-corrections-r5/sobytiya/dekorativnoe-mini-panno-tkanye-uzory-zelenogradsk-6529/>

The R5 prefix is immutable and `noindex`; production and R4 were not overwritten.

## Validation

- `node --test site/tests/desktop-media-contract.test.mjs site/tests/interest-club-catalog.test.mjs` — 8/8 passed.
- `node --test site/tests/interest-club-catalog.browser.test.mjs` — 1/1 passed.
- `PREVIEW_BUILD_ID=preview-20260723-unified-corrections-r5 PUBLIC_INTEREST_CLUBS_ENABLED=1 npm --prefix site run build:preview` — 389 pages.
- `npm --prefix site run check:preview` — 288 events passed.
- `npm --prefix site run check:unified-prototype` — 18 primary routes, 39 hub links, 288 event pages and 373 related cards passed.
- Public Chromium at `1440×900` and `390×844` — HTTP 200, noindex, no overflow, broken images, console errors or page errors.
- Visual inspection confirmed a clean leather perimeter and a legible restrained glow below the club badge.

The 390 px result is a technical regression smoke, not mobile product sign-off.
