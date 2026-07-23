# Desktop polish R4 — integration report

Date: 2026-07-23  
Branch: `integration/desktop-polish-medallion-slots-r4-20260723`  
Base: `ecafccbc4670940763f102a597254fb043b1b844`  
Integrated implementation SHA before this report: `d7ca18920c3a2ee249bd3a645f9ce56f21d03d49`

## Requirement status

| ID | Status | Evidence |
|---|---|---|
| R01 desktop leather tag | Done | Blank leather source `(2)` plus live SVG/DOM wordmark; complete stitching, lower seam, contact shadow, and immediate color fallback. |
| R02 compact desktop breadcrumbs | Done | Public event 6686 breadcrumb height is 26 px; the event title ends at approximately 343 px in a 1440×900 viewport. |
| R03 club meeting badge | Done | Desktop badge is at the media/card upper-right (`top/right: 19.2px`) with a lower-directed warm shadow. |
| R04 Main/Secondary medallion slots | Done | Public event 6529 resolves `mumod` as the sole Main: one TopSlot, zero InlineSlots, aligned to the exact information-card center; breadcrumbs remain left. |
| R05 public build and acceptance | Done | Immutable noindex R4 prefix is live; local/generated gates pass; public Playwright smoke passes; Gemini 3.1 Pro High verdict is `SHIP`. |

## Integrated commits

- `1a2133ec` — desktop leather tag source and rendering
- `bc9a4bdb` — compact event chrome and prioritized medallion slots
- `f310a3b3` — desktop club meeting badge placement and glow
- `d7ca1892` — canonical docs, changelog, and final integration CSS

## Validation

- Targeted Node test set: **51/51 passed**.
- Preview build:
  - `PREVIEW_BUILD_ID=preview-20260723-unified-corrections-r4`
  - `PUBLIC_INTEREST_CLUBS_ENABLED=1`
  - **389 pages generated successfully**.
- Generated-output gates:
  - `check:preview`: passed for 288 events.
  - `check:unified-prototype`: passed for 18 primary routes, 39 hub links, 288 event pages, 373 related cards, occurrence 6686, bus 6365, and rail 6529.
- `git diff --check`: passed.
- Public desktop Playwright smoke at 1440×900:
  - all three acceptance URLs return HTTP 200;
  - `robots=noindex,nofollow,noarchive`;
  - no horizontal overflow;
  - no console or page errors;
  - public viewport screenshots are in `artifacts/codex/r4-public/`.

## Public preview

- Root: <https://kenigevents.ru/preview-20260723-unified-corrections-r4/__preview/>
- Event 6686: <https://kenigevents.ru/preview-20260723-unified-corrections-r4/sobytiya/ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad-6686/>
- Clubs: <https://kenigevents.ru/preview-20260723-unified-corrections-r4/kluby-po-interesam/>
- Event 6529: <https://kenigevents.ru/preview-20260723-unified-corrections-r4/sobytiya/dekorativnoe-mini-panno-tkanye-uzory-zelenogradsk-6529/>

The deploy command completed with `Public preview verification: ok`. R3, the production root, the current preview pointer, and stable `s3://<bucket>/ics/*` were not modified.

## External acceptance

Consultant: `agy` 1.1.5, `gemini-3.1-pro-high`, effort `high`.

- Design consultation: `artifacts/codex/r4-gemini/design-consultation.txt`
- Public acceptance: `artifacts/codex/r4-gemini/public-acceptance.txt`
- Result: R01 PASS, R02 PASS, R03 PASS, R04 PASS.
- Final verdict: **SHIP**.

Mobile acceptance is intentionally deferred per the user request; existing mobile behavior was preserved rather than redesigned in R4.
