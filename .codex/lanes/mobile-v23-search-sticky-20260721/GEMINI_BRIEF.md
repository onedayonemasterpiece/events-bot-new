# Gemini 3.1 Pro High — critical product/design brief for mobile v23

Act simultaneously as a critical consultant, product analyst, interaction designer and acceptance gate. Mobile only. Inspect evidence and do not rubber-stamp.

Public v22: https://kenigevents.ru/preview-20260721-mobile-calendar-city-search-v22/
Telegram screenshot: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v22-research-20260721/telegram-v22-feedback/message-480.jpg`.
Current Search screenshot: `/home/dev/.codex/worktrees/events-bot-new/calendar-occurrences-v21-mobile/artifacts/codex/mobile-calendar-city-popular-v22-research-20260721/v22-search-auth-390x844-2x.png`.
Pinterest board: `/home/dev/projects/pinterest-idea-library/collections/20260721-kenigevents-mobile-event-search-query-education-v23/board.png`.
Codex self-review: `/home/dev/projects/pinterest-idea-library/collections/20260721-kenigevents-mobile-event-search-query-education-v23/SELF_REVIEW.md` and `pins.json`.

User feedback from Telegram:
1. Today does not visibly show events whose start time is already past.
2. Orpheus's three-line time block was added, but the separate other-date recap before the medallion disappeared; user wants it back.
3. Search must reuse the older separate button that is also the progress bar.
4. Ready-made queries at the bottom should open centrally normalized static result pages and also teach users how to search; product/design reference research requested on Pinterest.
5. Search results must be the already accepted standard large cards. Do not proceed without finding the old tested Search.
6. Sticky header date/count/city shown in the screenshot is visually messy.

Discovery facts:
- Proven Search donor FOUND. Visual v58 commit `abbcf7a13d230a11932ecf2e7658c1ddc3303f66`; canonical latest Search file revision `2ef8dd834da584ef82be534dc3f1b296f87d0651` on `recovery/static-site-smart-search-full-20260701`. Reuse exact Astro `/poisk`, `AuthorizedEventSearch.astro`, Search CSS from `EventLayout.astro`, `EventCard.astro`/`window.KenigEventsRenderEventCard`, and smoke/readiness tests. It has separate full-width submit under the field, progress in that button, live Yandex/Supabase PKCE, restored sessions, NDJSON/vector-first/JSON rescue and standard large cards. v22 Search is a bespoke fake and must be removed, not developed further. Email does not exist in this donor.
- Today root cause: passed-start/no-end event 6804 is `.is-started`, but neutral/desaturated treatment applies only to `.is-ended`. Do not invent an end time. Proposed: factual `Уже началось`/`Время начала прошло`, same neutral summary/image desaturation as accepted past treatment, while all-day/day-program remains undimmed; auto marker targets first future timed event and ignores all-day for scroll targeting.
- Orpheus: the other-date block was deliberately suppressed. New accepted intent is deliberate two-level repetition from the same explicit reciprocal family DTO: immediate three-line orientation in the time block, plus end-of-rail `Ещё даты / 25 июля · 17:00` after digest immediately before medallions. Current date-list sibling filtering must show only future dates so the 25-Jul row does not recap 24-Jul. No title/venue inference; current rail href remains 5511.
- Sticky measured root cause: broad `.sticky-date span` forces `20` and `событий` onto separate lines while city is centered across both. Brand safe gap is already correct. Recommended layout A within same 64px header: row1 `24 июля · 20 событий`, row2 `Вся область ⌄`; date 18/18 w920, count 10.5/12 muted, city 10.5/12 muted, no pill/border, selected city `Калининград +2`, only arrow terracotta. Atomic pin visibility stays.
- Pinterest funnel: collected 100 / 10 query axes / self-reviewed 100 / keep 12 / maybe 6 / reject 82. Primary useful pins: #001, #008, #011, #012, #021, #022, #071, #073, #074, #091, #093, #100. Generic progress/auth/AI-neon references were rejected because the proven donor is better evidence. Current synthesis: keep donor progress + EventCard; under input place quiet `Попробуйте так` or `Готовые подборки` with full natural-language phrases. An approved/materialized normalized query is a real link to its static page; before materialization omit it or clearly make it an example that fills the field. Personal saved searches are a separate signed-in section.

Required output in Russian:
A. PASS/CHANGE/BLOCK for each of the six feedback areas.
B. Pick exact product copy and interaction for passed-start/no-end state, explaining semantic safety.
C. Decide whether the deliberate Orpheus repetition is useful or redundant and specify exact hierarchy.
D. Confirm donor transfer plan and forbid any pieces that should not survive from v22 Search.
E. From the Pinterest shortlist, cite specific pin numbers/mechanics worth adapting, identify misleading patterns, and design the exact query-learning/static-page section: heading, explanatory copy, link behavior, fallback before static page exists, relationship to personal saved searches.
F. Choose sticky header layout with exact visual hierarchy and 320/390 constraints.
G. Give implementation order and hard Playwright/functional gates. End with one implementation go/no-go verdict. State exact model class.
