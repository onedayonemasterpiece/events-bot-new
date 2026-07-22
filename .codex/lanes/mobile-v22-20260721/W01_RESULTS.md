# W01 — mobile v22 research implementation

Status: **complete**

Owned output (ignored research artifact):
`artifacts/codex/mobile-calendar-city-popular-v22-research-20260721/`

## Implemented

- Event `6764` / poster `13792`: exact, user-verified `visual_only` + `safe_crop` exception with `crop_source=user-verified-visual-only-v22`; single-image presentation is 140×112 horizontal 5:4. Unknown media elsewhere continue to fail closed. Source is only 180×320, so generated 2x/3x variants do not constitute new source detail.
- Default calendar hero: parallax compensation increased to `0.28` (about 0.72× perceived viewport speed); static comparison retained at `/date-2026-07-24-static/`; reduced-motion disables transform.
- Parallax/hero lifecycle: random seed, render bases and exit schedule initialize once per page load; height-only resize cannot rerandomize them. Scroll fade is monotonic in both directions and scroll interruption settles the entry animation.
- Event `5511`: date-list-only time block `19:00 / 24 июля / 25 июля 17:00`, exact explicit ids `5511,5512`, full current/next aria label, no redundant post-media “Ещё даты”.
- `/poisk/`: anonymous query entry, inline point-of-intent auth after submit, preserved `q`, current Yandex method clearly distinguished from unapplied/non-sending email research state, ten visibly simulated public query chips, and separate personal-saved-search concept. Search bottom navigation is active; no date accessory.
- Accepted v21 calendar/Popular rails, cities, medallions, gestures, sticky geometry and ephemeral horizontal offsets remain in the output.

## Validation

Command: `node validate-v22.cjs`

Result: **106/106 PASS** (`v22-local-validation.json`). Coverage includes 320×667 and 390×844 DPR2 smoke, no horizontal overflow, eager image decode, non-distorted media, exact 6764 crop provenance/ratio, exact 5511 relation projection/aria, forward/reverse hero opacity monotonicity, height-resize seed/schedule/DOM stability, reload randomization, 0.28 transform, reduced-motion/static comparisons, Search auth timing/query preservation/demo truthfulness, city multiselect, medallions and ephemeral rail offsets.

Visually inspected evidence:

- `v22-date25-art-travel-crop-row-390x112-2x.png` — target 6764 row visible; 5:4 image is not stretched.
- `v22-date24-orpheus-occurrence-row-390x112-2x.png` — target 5511 row visible; all three time/date lines fit.
- `v22-search-auth-390x844-2x.png` — query and inline auth hierarchy are readable with bottom-nav Search active.
- `v22-date24-main-parallax-390x844-2x.png` — hero field, title protection and main calendar shell remain visually coherent.

## Material caveat

This remains a static `noindex` research prototype. It does not repair the production semantic enrichment row for poster 13792, activate email auth, execute search, or publish static query pages. Those require production implementation/data work outside this worker lane.
