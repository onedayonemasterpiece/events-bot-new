# Desktop event focus v8 — integration report

## Scope

Noindex desktop event-page laboratory only (`min-width:1024px`). Production event detail/mobile files are explicitly forbidden and absent from the diff.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Public decoded image starts at `top=73`, natural full-width height `1023.59`; both faces are visible at FHD/125%. |
| R02 | Done | Public Continuous media and image are both `1536×1023.59`; matching-ratio `contain`, no top/bottom crop. |
| R03 | Done | Public computed order: rail `1`, CTA `2`; rail bottom `627.84`, CTA top `640.64`. |
| R04 | Done | Public OCR companion poster is `276.47×388.8` at `1536×864`; fullscreen requested/active/source indexes are `1/1/0`. |
| R05 | Done | Public Split OCR and Split portrait render no duplicate-only rail; fullscreen source indexes remain stable. |
| R06 | Done | Public real rail is `124.78px` tall; cells use `88/116/156px` portrait/square/landscape buckets. |
| R07 | Done | Public low-resolution track bottom equals media bottom at both viewports; remainder `0`; scale about `1.11×`. |
| R08 | Done | Public full-flow `related-cover`, `related-ambient`, and `related-hybrid` comparison routes return `200`. |
| R09 | Done | Hybrid default keeps visual cover plus document ambient contain and normalized complete row heights; final Gemini ranked it first. |
| R10 | Done | Gemini 3.1 Pro (High) pre-design and final public-capture review completed; final verdict **SHIP**, no blocker. |
| R11 | Done | Changes are lab-only and desktop media-query scoped; at `390×844` the public root is `display:none` with zero rectangle/overflow. |
| R12 | Done | `443`-page build/check, docs/CHANGELOG, public upload/HTTP/Playwright, clean commits and pushes completed. |

## Local gates

- `git diff --check`: passed.
- `PREVIEW_BUILD_ID=preview-20260713t-desktop-focus-v8 npm run build:preview`: passed, `443` pages.
- `PREVIEW_BUILD_ID=preview-20260713t-desktop-focus-v8 npm run check:preview`: passed.
- Playwright CLI against final `dist`: two desktop viewports, nine routes, decoded-media screenshots, companion and thumbnail gallery interactions, ordering and corrected mobile-isolation smoke; no console errors.
- Targeted cascade regression: Continuous computes `object-fit:contain` at both desktop viewports while media/image geometry remains identical.
- Public HTTP: overview plus nine direct routes return `200`; current CSS carries the v8 enlarged-companion rule.
- Public Playwright CLI: `passed=true`, `failures=[]` for the same two desktop viewports and all exact-index/mobile-isolation interactions.
- Antigravity wrapper provenance: `/home/dev/.local/bin/gemini` resolves to `Gemini 3.1 Pro (High)`; final run exited `0` and returned **SHIP**.
- Git: pushed clean feature HEAD `460ae91541819842f003ba1d078bda0419960d37` before public deployment.
- Checklist re-audit: R01–R11 done; the sole R12 bookkeeping finding was corrected in the committed closure records. Its Continuous cascade fragility finding was also corrected and rerun locally.

Ignored evidence: `artifacts/codex/desktop-event-focus-v8-20260713/`.

## Integration risks

- `duplicateSourceIndexes` is a lab fixture, not a production perceptual-dedup system. Promotion requires canonical upstream media identity.
- Ambient fill is intentionally scoped to noindex related-card comparison selectors; the production duplicate/backdrop guard remains active.
- The current top-safe policy is conservative. Production promotion should use source-grounded salient/focal metadata where available.
- This closes the noindex v8 preview, not production promotion. Production merge still requires current-main reconciliation and upstream media-identity metadata.

## Publication note

The relevant preview HTML/CSS/JS/assets were uploaded and accepted publicly. The follow-up deploy's long stable-ICS mirror tail was interrupted after public acceptance; the preceding v8 deploy had already completed all `410` stable ICS objects and this follow-up changed only lab CSS/docs, not event or calendar data.
