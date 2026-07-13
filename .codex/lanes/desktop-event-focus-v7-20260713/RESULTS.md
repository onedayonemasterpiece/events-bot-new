# Desktop event focus v7 — integration results

Preview: `https://kenigevents.ru/preview-20260713t-desktop-focus-v7/lab/event-desktop/`

| ID | Status | Evidence |
| --- | --- | --- |
| R01 | Done | Public Playwright: native `position:sticky`, internal travel `133px`, release ratio `1.00`; `qa-public/results.json`. |
| R02 | Done | Separate continuous route computes `position:absolute`, monotonic upward movement and net ratio `0.65`; `qa-public/results.json`. |
| R03 | Done | `50% 80%` focal position and equal initial media/image lower edges; public top screenshot preserves the actors' legs. |
| R04 | Done | Split media is `720/1440` at the canonical viewport; real OCR and portrait tracks exceed the `827px` viewport and expose measured overflow. |
| R05 | Done | Compact poster rail retains gallery indexes, uses a final `+N`, and selected-index fullscreen clicks pass publicly. |
| R06 | Done | One H2 `О событии`, paragraph lead, repeated source heading removal and remaining heading demotion pass preview/source checks. |
| R07 | Done | Targeted public sample measured `64.28125px` between CTA and related section; `qa-public/targeted-contract.json` and screenshot. |
| R08 | Done | OCR companion DOM order is CTA → companion → rail; companion is `contain`, non-parallax, and opens gallery index `1`. |
| R09 | Done | Real OCR (`5077`, `955×1280`), portrait (`6550`, `1357×1920`) and low-resolution (`5761`, `800×602`) routes are public. |
| R10 | Done | Active overview/static paths/checks contain only Editorial and Split/Fallback scenarios; Gallery/Reading/Bento links were removed. |
| R11 | Done | Gemini 3.1 Pro (High) pre-contract, six-route browser audit and evidence reconciliation are saved under `artifacts/.../gemini/`; final recommendation is `SHIP`. |
| R12 | Done | Forbidden production/mobile files are absent from the diff; lab CSS/runtime is desktop-gated and public `390×844` smoke proves the root hidden and inert. |

## Validation

- Astro preview build: `440` pages.
- `npm run check:preview`: pass.
- Local Playwright: `57/57`.
- Public HTTP: overview plus six direct routes return `200`.
- Public Playwright: `57/57`, including `1440×650`, `1440×900`, `1920×600`, `1920×1080` and mobile-isolation smoke.
- Gemini 3.1 Pro (High): browser audit completed; two first-pass contract ambiguities were rechecked against exact geometry and the approved graphite action treatment, both revised to `PASS`; final `SHIP`.

The recursive preview upload completed for all versioned HTML/JS/CSS/media. The subsequent unrelated stable `/ics/*.ics` metadata-mirroring tail was intentionally interrupted after it began; it does not affect any desktop lab route or asset.

## Final checklist review

The read-only `checklist_reviewer` marked R01–R12 **Done**, found no forbidden production/mobile diff and no unrelated change, and declared the branch safe to commit and push. Its only non-blocking notes were the intentionally retained inactive legacy lab CSS/JS and that final `SHIP` follows Gemini's evidence-based reconciliation of its two first-pass contract misunderstandings.
