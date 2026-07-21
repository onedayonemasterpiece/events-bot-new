# Mobile v24 Search runtime — integration report

Date: 2026-07-21 UTC  
Branch: `integration/mobile-v24-search-runtime-20260721`  
Base: `origin/integration/mobile-v23-search-sticky-20260721`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Search runtime uses donor `9dced876ab4e8d2c69c79937d3b0186196c924db` through `packRelatedCardRows(..., { rowSize: 1, presentation: 'flow' })`; the snapshot carries semantic role, intrinsic geometry and focal Y; mocked authorized browser smoke checks a real photo is `cover` and no related-grid placement leaks into results. |
| R02 | Done | Synthetic progress timers were removed. The request epoch owns one AbortController/reset; backend NDJSON stage/value can only advance; the separate ARIA progressbar is indeterminate before the first backend frame and determinate afterward. The generated-browser smoke injects out-of-order frames and covers success, a delayed second request across the prior reset window, terminal failure and successful retry. |
| R03 | Done | One `MobileToastRegion` is mounted after the shared header. Global keyboard, share/copy and phone-copy producers use its bounded queue/dedupe/pause contract; info/success lasts 5s, error/action persists, and the terracotta underline retreats from right to left. Contextual Search/auth/quota and local transactional feedback stay inline. |
| R04 | Done | Canonical product contract makes explicit free intent a hard constraint. Child/family admission requires evidence-backed LLM output; topic and age labels are insufficient, so a decorative child medallion is not shipped. A noindex combined collection remains a research materialization, not claimed production truth. |
| R05 | Done | `EventLayout` owns the shared mobile header/drawer and bottom safe areas. Search and collection select exactly one `Поиск` item; event detail uses CTA bottom mode and has no global bottom nav. |
| R06 | Done | Three design consultations were completed with Gemini 3.1 Pro (High), with model evidence saved. The first final pass returned `CONDITIONAL GO` only for toast/brand overlap. After the overhang fix was deployed, Gemini 3.1 Pro (High) returned `GO` in `gemini-pro-final-recheck.md`, then reconfirmed comprehensive live `GO` after the corrected public Search build in `gemini-pro-final-live.md`; Flash probes remain invalid/supplementary and are not used as the gate. |
| R07 | Done with noted legacy-gate limitation | Public noindex preview, build/static gates, focused browser tests, high-DPR Playwright at canonical 320px and review 390px widths, and authorized Search smoke passed. The older cross-site `check-browser-release-gate.mjs` stalled after specimen selection and was terminated; focused generated/browser gates covering every touched surface passed. |

## Public preview

- Search: <https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/poisk/>
- Free + children research collection: <https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/podborki/besplatno-s-detmi/>
- Crop donor specimen 6408 in this build: <https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/sobytiya/spektakl-sobaka-na-sene-kaliningrad-6408/>
- Preview index: <https://kenigevents.ru/preview-20260721-mobile-search-runtime-v24/__preview/>

Deployment was prefix-only. The stable production root and `/ics/*` were not replaced.

## Verification

- Focused Node suites: 38/38 passed for Search, mobile shell/toast, keyboard and continuation behavior.
- Occurrence resolver/formatter: 9/9 passed.
- `tests/test_event_vector_sync.py`: 15/15 passed after adding the zero-call full-corpus regression.
- Browser release behavior unit tests: 10/10 passed.
- Astro preview build: 386 pages generated.
- Static preview check: 303 events, `strict_related=false`.
- Mocked authorized Search browser smoke: 2 cards; first event 6310; fallback section preserved; visual photo computed `object-fit: cover`; no grid placement. Four browser runs cover success/reset-race/error/retry; recorded progress was `[2,55,72,96,100]`, `[2,55,72,96,100]`, `[2,28]`, `[2,55,72,96,100]` with no rollback.
- Public Playwright at 390x844, DPR 3:
  - Search: one shell/nav/current item/toast region, zero horizontal overflow;
  - toast countdown pauses and resumes;
  - collection: 12 cards, one current Search nav item, zero overflow;
  - event 6408: CTA mode, zero bottom nav, one toast region, zero overflow.
- Public Search at 320x720 and 390x720, DPR 3: authorized-search shell is
  present, one bottom nav/current item, zero horizontal overflow, and a 7px
  gap between the brand handle and toast at both widths.
- The collection QA field `broken: 6` is not six broken assets: those were below-fold lazy images inspected before decode; visible media rendered. This counter is not used as a release gate.
- Full legacy cross-site browser script: partial/non-blocking for this noindex
  prototype. It stalled for more than six minutes after `static candidates=55`,
  specimen 6408 and target 6407, produced no report, and was terminated. Its
  touched-surface responsibilities are covered by the focused generated Search
  smoke plus public 320/390 DPR3 shell, toast and crop checks above; unrelated
  legacy journeys were not declared green.

## Search sidecar synchronization

The live personalization Supabase search projection was synchronized for 303 `search_v3` documents. A final zero-provider-call `--require-complete` audit passed with 303 unchanged embeddings, 0 provider calls and 0 missing rows. While closing the run, an early-exit bug in zero-call verification was found and fixed: provider caps now limit Gemini calls without stopping the remaining hash audit.

Evidence: `artifacts/codex/mobile-v24-search-runtime-20260721/search-vector-sync-verify-final.json`.

## Consultant artifacts

Valid Gemini Pro-class consultation:

- `gemini-pro-progress-toast-review.md`
- `gemini-pro-free-header-review.md`
- `gemini-pro-header-review.md`
- `gemini-pro-final-acceptance.md` (`CONDITIONAL GO`, blocker identified)
- `gemini-pro-final-recheck.md` (final `GO`)
- `gemini-pro-final-live.md` (comprehensive public-build `GO`)
- matching `*-model-evidence.txt` / final model logs

The files `gemini-consultation*.md`, `gemini-progress-toast-review.md` and `gemini-final-flash-invalid.md` are supplementary/invalid lower-class probes and do not satisfy R06.

## Handoff

The annotated preview links were delivered to Telegram Saved Messages and
verified as message `32524`; the redacted receipt is
`artifacts/codex/mobile-v24-search-runtime-20260721/telegram-receipt.json`.
