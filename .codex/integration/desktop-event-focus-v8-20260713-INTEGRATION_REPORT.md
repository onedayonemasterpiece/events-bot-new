# Desktop event focus v8 — integration report

## Scope

Noindex desktop event-page laboratory only (`min-width:1024px`). Production event detail/mobile files are explicitly forbidden and absent from the diff.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done locally | Pinned image starts at `top=73`, natural full-width height `1023.59`; faces visible in FHD/125% screenshot. |
| R02 | Done locally | Continuous media and image are both `1536×1023.59`; matching-ratio `contain`, no top/bottom crop. |
| R03 | Done locally | Computed order: rail `1`, CTA `2`; rail bottom `627.84`, CTA top `640.64`. |
| R04 | Done locally | OCR companion visible poster `276.47×388.8` at `1536×864`; fullscreen requested/active/source indexes are `1/1/0`. |
| R05 | Done locally | Split OCR and Split portrait render no duplicate-only rail; fullscreen source indexes remain stable. |
| R06 | Done locally | Real rail is `124.78px` tall; cells use `88/116/156px` portrait/square/landscape buckets. |
| R07 | Done locally | Low-resolution track bottom equals media bottom; remainder `0`; image scale about `1.11×`. |
| R08 | Done locally | Full-flow `related-cover`, `related-ambient`, and `related-hybrid` comparison routes. |
| R09 | Done locally | Hybrid default: visual cover plus document ambient contain; normalized complete card heights per row. |
| R10 | Partial | Gemini Pro pre-design completed; final public browser audit follows deploy. |
| R11 | Done locally | Changes are lab-only and desktop media-query scoped; at `390×844` the root is `display:none` with zero rectangle/overflow. |
| R12 | Partial | Build/check/docs/changelog done; public deploy, public QA, commit/push evidence pending. |

## Local gates

- `git diff --check`: passed.
- `PREVIEW_BUILD_ID=preview-20260713t-desktop-focus-v8 npm run build:preview`: passed, `443` pages.
- `PREVIEW_BUILD_ID=preview-20260713t-desktop-focus-v8 npm run check:preview`: passed.
- Playwright CLI against final `dist`: two desktop viewports, nine routes, decoded-media screenshots, companion and thumbnail gallery interactions, ordering and corrected mobile-isolation smoke; no console errors.

Ignored evidence: `artifacts/codex/desktop-event-focus-v8-20260713/`.

## Integration risks

- `duplicateSourceIndexes` is a lab fixture, not a production perceptual-dedup system. Promotion requires canonical upstream media identity.
- Ambient fill is intentionally scoped to noindex related-card comparison selectors; the production duplicate/backdrop guard remains active.
- The current top-safe policy is conservative. Production promotion should use source-grounded salient/focal metadata where available.
