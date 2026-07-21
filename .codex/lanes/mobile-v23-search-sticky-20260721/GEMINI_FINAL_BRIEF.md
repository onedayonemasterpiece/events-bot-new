# Gemini 3.1 Pro High — final public acceptance brief

Act as a **critical mobile product reviewer, interaction designer, and acceptance gate**. Do not be agreeable by default. Inspect the public pages and the named visual evidence. Return `GO`, `GO WITH CHANGES`, or `BLOCK`, followed by requirement-by-requirement PASS/FAIL and exact changes.

## Public surfaces

- Calendar root: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/>
- Today: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/segodnya/>
- 24 July / Orpheus / sticky: <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/date-2026-07-24/>
- Search donor: <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/poisk/>
- Materialized example: <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/podborki/dzhaz-na-vyhodnyh/>

## Visual evidence

- User-annotated sticky failure donor: `/home/dev/projects/events-bot-new/artifacts/codex/mobile-v23-telegram-feedback/photo_2026-07-21_12-28-15.jpg`
- Accepted sticky 320: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-sticky-layout-a-320x667-2x.png`
- Started event: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-today-started-row-390x112-2x.png`
- Orpheus recap after digest: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-v23-research-20260721/v23-date24-orpheus-recap-scrolled-390x112-2x.png`
- Anonymous Search/query learning: `/home/dev/.codex/worktrees/events-bot-new/mobile-v23-search/artifacts/codex/mobile-search-v23-20260721/search-anonymous-top-collections-390x844-dpr2.png`
- Separate submit/progress: `/home/dev/.codex/worktrees/events-bot-new/mobile-v23-search/artifacts/codex/mobile-search-v23-20260721/search-mocked-authorized-progress-button-390x844-dpr2.png`
- Materialized large EventCard: `/home/dev/.codex/worktrees/events-bot-new/mobile-v23-search/artifacts/codex/mobile-search-v23-20260721/materialized-jazz-collection-large-eventcard-390-dpr2.png`

## Acceptance contract

1. Today: started/no-end timed event is visibly neutral/desaturated with factual `Уже началось`; no invented end; day-program remains vivid; initial scroll targets first future timed event.
2. Orpheus: three-line time block `19:00 / 24 июля / 25 июля 17:00` plus a deliberate recap `Ещё даты / 25 июля · 17:00` after digest and before medallions; reciprocal explicit family only.
3. Sticky: replace annotated visual mess with 64px Layout A: row 1 `24 июля · 20 событий`, row 2 city; no wrap at 320/390, brand safe lane.
4. Search: preserve exact proven v58-derived donor mechanics: real Yandex/Supabase PKCE, separate submit whose background is the progress bar, canonical large EventCard results. No fake email or fake saved-search state.
5. Query learning: quiet `Готовые подборки`; full natural-language phrases; materialized queries are real static links; examples only fill input and never auto-submit; personal saved searches remain separate/auth-only.
6. Public tests: calendar focused 36/36 plus inherited v22 106/106; Search source 5/5 + occurrence 9/9; mocked authorized progress/card smoke; public Search 8/8.

## Explicit critical question

The Astro fixture metadata is dated `2026-07-17`, so the current published materialized `джаз на ближайших выходных` page displays an 18 July event while the review date is 21 July. It is labelled as data updated on 17 July and the whole Search build is noindex, but this can still undermine the meaning of “nearest”. Decide whether this is acceptable frozen prototype evidence or a **release blocker that must be corrected before user handoff**. Prefer semantic honesty over schedule convenience.

Also flag any visible mobile regression caused by preserving the proven donor rather than rebuilding it.
