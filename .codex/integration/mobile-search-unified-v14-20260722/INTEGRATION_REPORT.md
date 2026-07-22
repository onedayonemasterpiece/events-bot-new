# Mobile Search large-card v25 — integration report

Date: 2026-07-22 UTC  
Branch: `integration/mobile-search-unified-v14-20260722`  
Base: `integration/mobile-menu-reference4-v13-20260722`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Search continues through `KenigEventsRenderEventCard(..., 'split-actions', resolveMobileEventCardMedia(item))`; the fake shell-lab `sample_rows` path is not used by the delivered Astro `/poisk/`. |
| R02 | Done | Initial fetch immediately shows two full structural large-card skeletons plus a third-card peek; each media slot reserves horizontal `5:4`. Provisional vector frames leave this surface in place. |
| R03 | Done | Exact section is `Результаты поиска`; zero exact state is `По вашему запросу ничего не найдено`. |
| R04 | Done | `fallback_items` are buffered while `has_more=true`; after exact exhaustion they appear under honest `Ещё можно посмотреть`, without a false personalization claim. |
| R05 | Done | Exact endcap is `Нашли то, что искали?`; `Да, нашёл`/`Нет, не нашёл` preserve RPC values `matched`/`missed` and the local fallback queue. |
| R06 | Done | Noindex preview is public, returns HTTP 200, exposes the real Search runtime and canonical card template, and contains no `Подходящие события` label. Mobile loading/result screenshots were captured from the generated build. |

## Verification before deployment

- Search/media/occurrence Node suites: 34/34 passed.
- Astro production build passed.
- Astro preview build passed for `preview-20260722-mobile-search-large-cards-v25`.
- `check:preview`: passed, 303 events, `strict_related=false`.
- Mocked authorized mobile browser smoke: passed. It observes the skeleton and in-button progress before final cards, then two canonical `split-actions` cards, exact heading, feedback endcap, discovery heading and no visible secondary progress bar. Progress stayed monotonic across success/reset-race/error/retry: `[2,55,72,96,100]`, `[2,55,72,96,100]`, `[2,28]`, `[2,55,72,96,100]`.

## Consultant

Gemini 3.1 Pro (High) returned `GO` for the ordered exact → feedback → discovery design, recommended keeping provisional vector results behind the skeleton, and approved an isolated noindex Search preview before global shell unification. Prompt and response are saved under ignored `artifacts/codex/mobile-search-unified-v14-20260722/`.

## Public preview

- Search: <https://kenigevents.ru/preview-20260722-mobile-search-large-cards-v25/poisk/>
- Prefix-only deploy; stable production root was not replaced.
- Public fetch: HTTP 200, 220,516-byte document; Search enabled, noindex,
  canonical `split-actions` runtime template present, structural skeleton present,
  and the rejected `Подходящие события` copy absent.
- Telegram UI-review topic `122`: verified message `585` with the URL and a
  concise list of what to inspect on the phone.

## Incident regression contract

`INC-2026-07-02-static-search-92-percent-no-cards` applies. The card-shaped loading gate and monotonic progress smoke pass locally; the production-root promotion and live three-query evidence remain outside this prefix-only design preview, so the incident is not closed by this task.
