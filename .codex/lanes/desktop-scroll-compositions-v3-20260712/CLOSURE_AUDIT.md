# Desktop scroll compositions v3 — closure audit

Independent reviewer: checklist agent `019f584d-423a-7310-b582-c61f5bf33683`.

| ID | Status | Closure evidence |
|---|---|---|
| R01 | Done | Diff contains only noindex desktop-lab routes/component, preview checks and docs. Production mobile/EventHero/EventLayout files are absent; `390×844` isolation passes. |
| R02 | Done | Editorial photo action lives in the sticky media layer; the normal-flow slab rises faster than media; rail/action do not overlap; manual selection stops autorotation. |
| R03 | Done | Editorial OCR frame is `100svh - 73px`, flush left, `contain`, zero measured crop/no parallax, with digest and first-viewport CTA across seven desktop viewports. |
| R04 | Done | Split OCR is `clamp(380px,45vw,620px)`, natural-ratio stage height and ordinary document flow; `1920×600` keeps title/venue/CTA visible. |
| R05 | Partial | Sticky `48%` reading media, long right story, deterministic reversible scroll image state and release before related all pass. Dedup is exact-URL only; neural crop/composite same-visual dedup remains a documented production follow-up. |
| R06 | Done | Measured rows use natural single-OCR ratio, geometric minimax for multiple OCR, `15%` crop budget/contain fallback and aligned media/card/action geometry. |

## Accepted preview limitation

R05's neural same-visual dedup is not represented as complete. It does not block publishing this desktop layout review because no destructive media selection or production routing is enabled here. It remains a prerequisite before promotion of automatic multi-image hero selection to production.

## Local release evidence

- Environment-backed Astro preview build: `441` pages.
- Preview checker: passed.
- Focused Chromium QA: `49` desktop layout runs plus interaction, row and phone-isolation checks; zero failures.
- `git diff --check`: passed.

