# Desktop Event Focus v9 Integration Report

## Scope
Desktop-only noindex event-page lab refinement. Production mobile event code and routes are explicitly out of scope and unchanged.

## Baseline and branch
- Base: `origin/feature/event-page-desktop-focus-v8-20260713` @ `6f3b610c1651d9ff07f6e456036862a782ea3d64`
- Integration branch: `feature/event-page-desktop-focus-v9-20260714`
- Implementation commit: `6a95b5cf`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Three real Continuous routes; exact/wide no-crop and near-square `19.97%` top-only crop with `0px` bottom mismatch. |
| R02 | Done | Responsive rail is a stage child visible in the initial top-right low-value hero area. |
| R03 | Done | Width-derived 72px cells, 8px gaps, failed-media collapse and truthful `+N`; wider viewport exposes more sources. |
| R04 | Done | Secondary actions measure 56×56px with ~11px gaps and remain subordinate to primary CTA. |
| R05 | Done | Separate and integrated OCR routes are copy-free, ratio-exact and scroll-safe; Gemini selects integrated B. |
| R06 | Done | `Бесплатно` is fully readable and non-overlapping at 1536×864 and 1440×900. |
| R07 | Done | Height-fit portrait viewer promotes the partial right item and opens the selected source index; safe-centred rail verified. |
| R08 | Done | Related documents use <=12% safe-cover or ambient contain; real examples verify both branches. |
| R09 | Done | Dedicated `split-ocr-with-photos` route shows one OCR primary plus real additional photos. |
| R10 | Done | Build/check/public HTTP/Playwright/mobile isolation/Gemini/preview/docs/changelog complete. |

## Public review surface
- Overview: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/>
- Continuous exact: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/editorial-photo-continuous/>
- Continuous wide: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/editorial-photo-continuous-wide/>
- Continuous near-square: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/editorial-photo-continuous-near-square/>
- OCR A: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/editorial-ocr-companion/>
- OCR B: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/editorial-ocr-companion-integrated/>
- Split OCR: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/split-ocr/>
- Split low-resolution: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/split-low-resolution/>
- Multi-portrait: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/split-multi-portrait/>
- OCR + photos: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/split-ocr-with-photos/>
- Related hybrid: <https://kenigevents.ru/preview-20260714t-desktop-focus-v9/lab/event-desktop/examples/related-hybrid/>

## Verification summary
- Static build: 448 pages.
- Preview checker: pass.
- Public Playwright: ten routes × three desktop viewports, `passed=true`, no failures or browser errors.
- Mobile isolation: pass at 390×844; desktop prototype hidden and no horizontal overflow.
- Gemini 3.1 Pro High final public visual gate: **SHIP**, no release blocker, integrated OCR B preferred.

## Publication note
Relevant preview HTML/JS/CSS and generated hashed assets were uploaded and verified over public HTTP and Playwright. Only the long idempotent stable `/ics/*.ics` mirror tail was stopped after acceptance; it is unrelated to the desktop lab surface.
