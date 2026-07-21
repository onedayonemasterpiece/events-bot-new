# Mobile v23 — discovery report before implementation

Date: 2026-07-21. Scope: mobile only. No implementation started until the old tested Search donor was found.

## Search donor gate — passed

- Visual donor: `abbcf7a13d230a11932ecf2e7658c1ddc3303f66` (v58).
- Canonical latest Search revision: `2ef8dd834da584ef82be534dc3f1b296f87d0651` on `recovery/static-site-smart-search-full-20260701`.
- Current integration source already contains this donor plus newer necessary hardening: occurrence-family dedupe, canonical runtime EventCard renderer and preview URL normalization.
- Preserve `site/src/pages/poisk/index.astro`, `AuthorizedEventSearch.astro`, all Search CSS in `EventLayout.astro`, current `EventCard.astro`, PKCE/session/NDJSON/vector-first/stream-rescue logic and smokes.
- The standalone v22 Search must not be extended: its fake auth, arrow-inside-input and bespoke small results are rejected.
- Safe research publication uses a separate Astro Search prefix; the standalone calendar links to it. Two-root same-prefix assembly is rejected for this iteration.

## Calendar regressions

- Passed-start/no-end events become `.is-started`, but only `.is-ended` receives the accepted neutral/desaturated treatment. Use factual `Уже началось`; do not infer duration/end. All-day/day-program remains active and undimmed. Autoscroll targets the first future timed event.
- Orpheus's other-date recap disappeared because event `5511` was explicitly suppressed. User now accepts deliberate two-level repetition from the same reciprocal explicit family: three-line time block plus `Ещё даты / 25 июля · 17:00` after digest and before medallions. Date-list recap contains future siblings only; 25 July must not recap 24 July.

## Sticky header

Root cause is the unscoped `.sticky-date span` selector, which turns `20 событий` into two lines while the city is centered against both. Accepted Layout A keeps 64px height and the existing brand safe gap:

- row 1: `24 июля · 20 событий`;
- row 2: `Вся область ⌄` or compact selected state `Калининград +2 ⌄`;
- 18/18 strong date, 10.5/12 muted count/city, no pill/border, only arrow terracotta;
- atomic background/content pinning remains.

## Pinterest research

Collection: `/home/dev/projects/pinterest-idea-library/collections/20260721-kenigevents-mobile-event-search-query-education-v23/`.

Funnel: **collected 100 / 10 query axes / self-reviewed 100 / keep 12 / maybe 6 / reject 82**. Source of truth is `pins.json`; board `board.png`; detailed notes `SELF_REVIEW.md`.

Primary references:

- [#001 — saved-query and preference sections](https://www.pinterest.com/pin/153896512262861386/)
- [#008 — quiet search/history/category hierarchy](https://www.pinterest.com/pin/23432860601081984/)
- [#011 — search/filter to vertical results](https://www.pinterest.com/pin/251920172903447789/)
- [#012 — large event result cards](https://www.pinterest.com/pin/548524429637038155/)
- [#021 — full-phrase prompt starters](https://www.pinterest.com/pin/11118330334357443/)
- [#022 — minimal natural-language input](https://www.pinterest.com/pin/14073817581134508/)
- [#071 — saved searches separated from search](https://www.pinterest.com/pin/1096274734321274541/)
- [#073 — compact chip states](https://www.pinterest.com/pin/85427724178008677/)
- [#074 — recent/trending queries teach vocabulary](https://www.pinterest.com/pin/28991991346052805/)
- [#091 — large vertical event feed](https://www.pinterest.com/pin/330873903897167848/)
- [#093 — query refinements above a feed](https://www.pinterest.com/pin/5488830793317276/)
- [#100 — complete-phrase onboarding choices](https://www.pinterest.com/pin/2462974793196482/)

Product synthesis: donor progress and canonical large EventCard are stronger than generic Pinterest progress/auth patterns. A quiet `Готовые подборки` section teaches with full natural-language phrases. Materialized normalized queries are real links to static pages; non-materialized examples may only fill the input, never fake navigation or execute search. Personal saved searches remain a distinct signed-in section.

## Gemini gate

`gemini-3.1-pro-high` reviewed the public v22, Telegram screenshot, donor facts and Pinterest board/self-review. It returned **GO** with one required change: replace the current sticky header with Layout A. It accepted the factual started-state treatment, deliberate Orpheus repetition, exact donor transfer, large EventCard results and the static-query learning model. Full response: `GEMINI_PRODUCT_DECISION.md`.
