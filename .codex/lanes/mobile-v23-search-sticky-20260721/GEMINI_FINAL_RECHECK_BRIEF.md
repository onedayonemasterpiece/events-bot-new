# Gemini 3.1 Pro High — blocker recheck

Re-open your prior final report in `GEMINI_FINAL_ACCEPTANCE.md`. You returned
`GO WITH CHANGES` and named three handoff blockers. They have now been addressed
without rewriting the proven `AuthorizedEventSearch` core:

1. `/podborki/dzhaz-na-vyhodnyh/` is re-materialized with the explicit research
   reference date `2026-07-21`. It now contains two canonical large EventCards
   for 25–26 July and no 18 July card. The page shows both source-data update
   date (17 July) and collection reference date (21 July), so staleness remains
   visible rather than hidden.
2. The clipped/occluded horizontal `.site-nav` is hidden on mobile only on pages
   that mount the Search mobile shell. The existing mobile brand/top-sheet
   component remains intact; `AuthorizedEventSearch`, its auth, and progress
   mechanics are unchanged.
3. `/poisk/` and `/podborki/*` now mount the same four-item bottom navigation
   grammar as calendar v23, with `Поиск` selected. Calendar/Search cross-links
   use the two explicit noindex preview prefixes.

Public pages (same URLs, overwritten):

- <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/poisk/>
- <https://kenigevents.ru/preview-20260721-mobile-search-donor-v23/podborki/dzhaz-na-vyhodnyh/>
- <https://kenigevents.ru/preview-20260721-mobile-calendar-v23/date-2026-07-24/>

New public evidence:

- `/home/dev/projects/events-bot-new/artifacts/codex/mobile-v23-public-search/public-search-390x844-dpr2.png`
- `/home/dev/projects/events-bot-new/artifacts/codex/mobile-v23-public-search/public-materialized-390x844-dpr2.png`
- `/home/dev/projects/events-bot-new/artifacts/codex/mobile-v23-public-search/public-search-validation.json` — 10/10.

Source gates after the patch: query-learning 6/6, occurrences 9/9, Astro build,
`check:preview`, and public browser 10/10. Calendar public gate remains 36/36
plus inherited 106/106.

Act as the same strict mobile product/design acceptance gate. Verify each of
your three blockers from the public pages/evidence. Return only:

- overall `GO`, `GO WITH CHANGES`, or `BLOCK`;
- blocker 1/2/3 PASS/FAIL with concise evidence;
- any genuinely handoff-blocking regression. Do not reopen accepted unrelated
  product direction unless the new patch caused a regression.
