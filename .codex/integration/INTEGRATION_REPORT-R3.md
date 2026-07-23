# R3 unified static prototype integration report

Date: 2026-07-23  
Integration branch: `integration/medallion-duration-desktop-r3-20260723`  
Validated product SHA: `1b802de2ddb9c3fa88bd4f55514ad2fde93b8d5f`  
Requested donor/control: `feature/rzd-lastochka-medallion-20260723` at
`68576d5b70f57164c00386b05cff126586c3f700`

The integration merge preserves both the accepted R2 prototype and the exact
requested medallion control as ancestors. No production root was deployed.
Only the immutable noindex prefix below was published:

`https://kenigevents.ru/preview-20260723-unified-corrections-r3/__preview/`

## Requirement closure

| Requirement | Status | Integrated evidence |
|---|---|---|
| R01 Smart Update duration forecast | Done | Nullable persisted `duration_forecast_minutes`; LLM call only behind transport eligibility; explicit source timing wins; exporter/Astro perform no provider call; desktop/mobile/ICS share one projection. |
| R02–R04 real medallion coverage | Done | Production-backed 38-row audit, 28 current-use / 10 current-unused / 0 unreachable; runtime and source provenance checked; event-detail festival resolution and deterministic Telegram raster fallbacks repaired; fail-closed and Unicode boundaries preserved. |
| R05 controlled Search smoke | Done | Test-only PKCE/session boundary; no production bypass; mocked browser smoke passed and the lane's opt-in real-edge run passed all three incident queries. |
| R06 desktop leather tag | Done | 240×88 source-derived WebP keeps stitched sides, bottom seam and outer edge; terracotta fallback and bounded inset/exterior shadow verified publicly. |
| R07 Goblin/card framing | Done | Exact immutable source override to reviewed 5:4 WebP; public progressive continuation renders `object-fit:cover`, 1000×800 natural media, no fields. |
| R08–R09 Clubs | Done | Three current real-data cards; Game Vibes uses source-backed event 2897 image; other clubs use honest CSS fallbacks; geometry-scoped arrows/Home/End/Enter; desktop and mobile overflow gate passed. |
| R10 full responsive prototype | Done | 389-page build and generated-output gates passed; all requested page types are linked from one prefix. |
| R11 external acceptance | Done | `agy` with `gemini-3.1-pro-high`, effort high, inspected the public desktop/mobile artifact and returned final `SHIP` with all 11 contracts `PASS`. |

## Validation

- Astro `build:preview`: 389 pages.
- `check:preview`: pass, 288 events.
- `check:unified-prototype`: pass, 18 primary routes, 39 hub links,
  373 related cards.
- Duration/Smart Update Python regressions: 24/24.
- Public/merge/content/keyboard Python regressions: 43/43.
- Occurrence/crop/keyboard/medallion Node regressions: 52/52.
- Responsive event/transport/clubs Node/browser regressions: 20/20.
- Medallion Playwright: 4/4 at 1440×1100 and 390×844.
- Controlled Search smoke: pass; typed query restored after the mocked PKCE
  boundary and one authenticated search result rendered.
- Incident regression
  `INC-2026-07-18-dramteatr-same-day-event-glue`: merge-identity gate passed.
- Public deployment verification: main preview and website-origin fallback
  responded successfully; stable production `/ics/*` objects were not touched.

## Consultant correction trail

The first Gemini pass returned `CONDITIONAL` because it inspected the transparent
wordmark child rather than its leather parent, and did not trigger progressive
related-card hydration. A bounded recheck inspected
`a.site-header__brand-tag` and scrolled the event continuation. It confirmed:

- leather asset, terracotta fallback, inset seam/edge and two exterior shadows;
- three hydrated links to Goblin, visible first card, `object-fit:cover`, no
  media padding or bands;
- final verdict `SHIP`, no critical or important defects.

Ignored evidence:

- `artifacts/codex/r3-gemini-acceptance/review.txt`
- `artifacts/codex/r3-gemini-acceptance/recheck.txt`
- `artifacts/codex/r3-visual/public-hub-desktop.png`
- `artifacts/codex/r3-visual/6529-desktop-goblin-card.png`

