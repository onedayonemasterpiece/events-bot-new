# W02 — Search donor + query-learning/static collections

Status: **complete**

Date: 2026-07-21

Branch: `agent/mobile-v23/search`

## Implemented

- Kept the existing v58-derived `AuthorizedEventSearch.astro` unchanged: live Yandex/Supabase PKCE, session recovery, vector-first NDJSON path, stream rescue, separate full-width mobile submit/progress button and canonical runtime `EventCard` renderer remain the source of truth.
- Replaced the technical `Поисковые теги` explanation on `/poisk/` with a quiet `Готовые подборки` query-learning section. The phrases teach natural-language formulation rather than presenting one-word taxonomy chips.
- Added three actual materialized Astro routes under `/podborki/<slug>/`:
  - `dzhaz-na-vyhodnyh`;
  - `besplatno-s-detmi`;
  - `stendap-na-etoy-nedele`.
- Each materialized page states its exact deterministic criteria, preserves empty truthfully instead of padding with inferred matches, collapses reciprocal explicit occurrence families `per-family`, is explicitly research/noindex, and renders standard large `EventCard` components.
- Added four visibly labelled `пример` phrases. They only write to the existing search input, emit an input event and (for an anonymous user) explain that Yandex sign-in is required. They do not navigate, submit or make a search request.
- Kept editorial/public collections semantically distinct from personal saved searches; no fake email auth, fake saved state or fake personal history was added.
- Adapted the Pinterest mechanics documented in the discovery decision (#021/#022 natural phrases, #001/#071 separation from personal saves, #100 complete-phrase onboarding) without copying their visual style.

## Owned files

- `site/src/pages/poisk/index.astro`
- `site/src/pages/podborki/[slug]/index.astro`
- `site/src/components/SearchCollectionLinks.astro`
- `site/src/data/searchCollections.ts`
- `site/tests/search-learning.test.mjs`
- this report

No changes were made to `AuthorizedEventSearch.astro`, `EventCard.astro`, `EventLayout.astro`, Supabase or database code.

## Validation

- `node --test tests/search-learning.test.mjs` — **5/5 PASS**.
- `npm run test:occurrences` — **9/9 PASS**.
- Preview build with ID `preview-20260721-mobile-search-donor-v23` and browser-safe public values mapped from the main repo `.env` (`PERSONALIZATION_*` → `PUBLIC_PERSONALIZATION_*`) — **PASS**, 303 events.
- `PREVIEW_BUILD_ID=preview-20260721-mobile-search-donor-v23 npm run check:preview` — **PASS**.
- Mocked authorized browser smoke, Chromium at `390×844`, DPR 2:
  - restored mock Supabase session;
  - example changed the input with **0** Search calls;
  - submit entered `aria-busy=true` progress state;
  - exactly one mocked Search call;
  - final result rendered as `.event-card--split-actions` from the canonical runtime EventCard template, not a bespoke small result row;
  - the real collection link navigated to its generated page and rendered a canonical EventCard.
- Responsive browser check at `320×844` and `390×844`, DPR 2 — **PASS**, no horizontal overflow.
- `git diff --check` — **PASS**.

## Build/tooling note

The main repo's local Astro package directory was incomplete (`astro/dist/cli/index.js` absent). Per the no-install constraint, this worktree used a symlink to an already installed, working project worktree dependency tree (Astro 6.4.8); no packages or browsers were installed. The first build also proved that the main `.env` intentionally stores private-name source variables rather than `PUBLIC_*`; the accepted preview was rebuilt with only the URL and publishable key mapped to their documented browser-safe public names.

## Handoff

- Preview artifact is local at `site/dist/preview-20260721-mobile-search-donor-v23/`.
- Per lane contract it was **not published** and nothing was sent to Telegram.
- Canonical docs, CHANGELOG, integration merge, publication and Telegram are root/integrator responsibilities.
