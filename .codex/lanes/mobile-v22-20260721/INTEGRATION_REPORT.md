# Mobile calendar / Search v22 — integration report

Date: 2026-07-21
Integration branch: `integration/mobile-v22-20260721`
Public noindex preview: <https://kenigevents.ru/preview-20260721-mobile-calendar-city-search-v22/>

## Requirement closure

| ID | Status | Evidence |
| --- | --- | --- |
| R01 | Done | Event `6764`/poster `13792` is the sole `user-verified-visual-only-v22` exception and renders `140×112` horizontal `5:4`; unknown/OCR remains fail-closed. Exact row screenshot and local/public browser gate pass. |
| R02 | Done | Main calendar routes use parallax factor `.28`; random schedule is one-shot and height-resize safe; forward/reverse opacity gates are monotonic; reload randomizes the field; reduced motion removes transform. Static comparison remains `/date-2026-07-24-static/`. |
| R03 | Done | Date-list row `5511` renders `19:00 / 24 июля / 25 июля 17:00`, carries explicit family `5511,5512`, full aria, current-detail click semantics and no redundant `Ещё даты`. No title/venue inference was added. |
| R04 | Done | `/poisk/` uses anonymous query-first, inline auth after submit, preserved `q`, active Search nav, no date accessory, ten visibly simulated public-query chips, honest Yandex-active/email-unapplied copy and separate personal-saved-search semantics. |
| R05 | Done | Telegram forum topic `122` received one annotated four-link message; receipt `telegram-receipt-v22-annotated.json`, message `474`, exact text verified in topic. |
| R06 | Done | Gemini 3.1 Pro (High) reviewed the public prototype and returned PASS on all four product areas. Its initial false attribution of reload randomization to Search was challenged and formally retracted; corrected Search evidence is the Demo badge/copy, simulated data attribute and fill-only chip behavior. |

## Validation

- Local Playwright: **106/106 PASS** (`v22-local-validation.json`).
- Public Playwright: **106/106 PASS** (`v22-public-validation.json`).
- Viewports: `320×667` and `390×844`, DPR2; hero coverage also checks exact transform, resize/reload/reduced-motion states.
- Public security/indexing boundary: prefix `robots.txt` disallows all; page meta is `noindex,nofollow,noarchive`.
- Visual evidence inspected: exact event-6764 row, exact Orpheus row, main parallax page and Search inline-auth state.

## Critical consultant disposition

Accepted from the preliminary Gemini review: exact verified visual-only crop, explicit linked-performance block, point-of-intent auth, query-cloud/public-vs-personal separation and annotated Telegram links. The suggested blurred duplicate background for unknown images was intentionally not adopted: it would weaken the established fail-closed/monolithic rail treatment and was not required to fix the confirmed asset. Final review artifacts:

- `GEMINI_PRODUCT_REVIEW.md`
- `GEMINI_FINAL_ACCEPTANCE.md`
- `GEMINI_FINAL_ACCEPTANCE_CORRECTION.md`

## Honest production gaps

This release is a static noindex research prototype. It does not repair poster `13792` semantic metadata in the production database, create higher-resolution source detail, activate email auth, execute live search, or materialize the future normalized query tags. Production implementation must reuse the canonical crop/occurrence/auth components rather than copying the prototype generator wholesale.
