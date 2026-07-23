# R10 results — mobile rail donor restoration

Base: `43a4b3e5`  
Branch: `integration/mobile-rail-donor-restore-r10-20260723`  
Donor contract: `integration/mobile-search-unified-v14-20260722@3f5b88f9`

| ID | Status | Evidence |
|---|---|---|
| R1 | Done | Rail proof, green underlay and terminal action reuse `Icon.astro`; generated DOM contains outline and solid paths; `aria-pressed` swaps them. |
| R2 | Done | Real event `5296`: `140×112`, `cover`, `single_safe_visual_landscape_5x4`; browser canary has no letterbox fields. |
| R3 | Done | Real event `6939`: reviewed alternate source, `90×112`, `cover`, `reviewed_multi_visual_portrait_4x5`; event-level OCR bypass additionally requires `listing_no_ocr_review=true`. |
| R4 | Done | Exact donor thresholds restored for start-edge negative and end-edge Like; pointer/touch/cancel, capture-phase click suppression, explicit confirm and Undo use canonical feedback state. |
| R5 | Done | Real event `4211` resolves external `more-vnutri.svg` through structured `festival` binding; OCR media remains contain. |

## Verification

- Node focused/regression suite: 25/25 passed.
- Occurrence tests: 10/10 included and passed.
- Astro noindex preview build: 431 pages.
- Generated-output gate: passed, 288 real events.
- Playwright: passed at 320px and 390px, including loading/error skeletons, exact canary geometry, external medallion, pointer negative/Like and touch negative.
- `git diff --check`: passed.
- External acceptance: `a-gemini` routed to Gemini 3.1 Pro (High). First review produced valid capture/race/OCR-scope concerns and several false source claims. Valid findings were fixed. A correction audit executed exact greps, withdrew the stale 80ms/modal claims and returned **GO** for R1–R5. Artifacts remain under ignored `artifacts/codex/mobile-rail-r10/`.

## Preview

Immutable noindex target: `preview-20260723-unified-corrections-r10`.
Production root was not modified.
