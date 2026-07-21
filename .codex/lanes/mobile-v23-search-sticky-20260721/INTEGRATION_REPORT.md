# Mobile calendar/Search v23 — integration report

Date: 2026-07-21  
Branch: `integration/mobile-v23-search-sticky-20260721`

## Public handoff

- Calendar: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/>
- Today passed-start state: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/segodnya/>
- 24 July / Orpheus / sticky: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/date-2026-07-24/>
- Proven Astro Search donor: <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/poisk/>
- Materialized query example: <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/podborki/dzhaz-na-vyhodnyh/>

All are noindex research previews.

## Requirement closure

- **R01 Done:** factual/desaturated `Уже началось`; no inferred end; day-program
  remains vivid; initial marker targets first future timed event.
- **R02 Done:** Orpheus keeps the three-line immediate time projection and the
  post-digest/pre-medallion future-date recap from the same reciprocal family.
- **R03 Done:** exact v58-derived separate submit/progress donor retained.
- **R04 Done:** Pinterest funnel 100 → 12 keep + 6 maybe; full-phrase query
  learning implemented as real materialized links or explicit fill-only examples.
- **R05 Done:** Search results and materialized pages use canonical large
  `EventCard`; bespoke small rows are absent.
- **R06 Done:** sticky Layout A is one-line date/count + second-line city inside
  64px at 320/390.
- **R07 Done:** Telegram screenshot 480 was treated as the sticky regression
  acceptance input.
- **R08 Done:** Gemini 3.1 Pro High first returned `GO WITH CHANGES`; its stale
  relative-date, clipped mobile header nav and missing bottom dock blockers were
  fixed. Public recheck returned `GO`, all three blockers PASS, no handoff
  regression.

## Validation

- Calendar public focused Playwright: **36/36 PASS**.
- Calendar inherited v22 acceptance: **106/106 PASS**.
- Search query-learning source tests: **6/6 PASS**.
- Occurrence tests: **9/9 PASS**.
- Astro preview build and `check:preview`: **PASS**, 303 events.
- Search public Chromium 390×844 DPR2: **10/10 PASS**; zero overflow/runtime
  errors, 3 materialized links, 4 fill-only examples, mobile dock current Search,
  and two 25–26 July canonical cards with no stale 18 July card.
- `git diff --check`: **PASS**.

## Honest boundaries

- Search auth is Yandex/Supabase PKCE from the donor; no email flow was invented.
- Personal saved-search history is not simulated.
- The source catalog still states its 17 July update date. The materialized
  research page separately displays its 21 July calculation date; production
  requires a regularly refreshed materialization job before indexing.
- Calendar and Search remain separate preview builds connected by explicit
  build-time public base URLs; no old labs were merged wholesale.

## Telegram receipt

Topic `122` (`Главная, Популярное, списки — wireframes`), message `481`.
Exact annotated text and all four URLs were re-read from the same topic after
send; `verified_in_topic=true`, `verified_exact_text=true`.
