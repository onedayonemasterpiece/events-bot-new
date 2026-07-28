# R15-L05-FAVORITES-HOME results

- **Status:** Done, with integration gates noted below
- **Lane ID:** `R15-L05-FAVORITES-HOME`
- **Requirement IDs:** `R04`, `R06`
- **Branch:** `agent/unusual-r15/favorites-home`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/unusual-r15-favorites-home`
- **Base SHA:** `31b72b93153c094ca16cd564bfdc6b56c2031867`
- **Implementation head SHA:** `3fa49fc053dc15bc8c390fbcbe4c34314cf2f784`

## Requirement outcome

| Requirement | Status | Evidence |
|---|---|---|
| R04 — Favorites and durable saved state | Done | Added noindex `/izbrannoe/`, skeleton/auth/offline/empty states, future-only catalog join, stable dedupe with calendar-first priority, authenticated Supabase view/RPC, owner RLS, post-local-commit like/unlike and calendar add/remove persistence, plus one-time local→remote sign-in reconciliation. |
| R06 — Home | Done | Replaced the root splash with an `EventLayout` home page containing talk-like hero, quick routes, and at most 30 server-rendered canonical event cards. Local profile signals may progressively rerank the static cards; no client feed/provider/LLM request is used for cold start. |

## Saved-state behavior

- Existing global `[data-feedback-action="like"]` controls remain canonical. The runtime records their pre-action local state at capture time, waits for the existing card runtime to commit localStorage, and persists the observed **post-action** value. Like and unlike map to `favorite = true/false`.
- Calendar state uses the same post-commit path for `[data-calendar-action]`; an exact `kenigevents:local-saved-event-change` event supports canonical calendar removal/toggle surfaces without capture-time guessing.
- On sign-in, local calendar IDs are reconciled first, then local liked IDs as favorites. RPC writes are idempotent and a per-user local signature prevents redundant complete reconciliation. A live mutation waits for an in-flight reconciliation so an immediate unlike/remove wins.
- Remote rows store only user-owned references/source state. Event facts remain in the canonical static catalog. Favorites renders only events on or after the catalog's `current_date`.

## Supabase security and API review

Reviewed current official material before implementing:

- API keys: <https://supabase.com/docs/guides/getting-started/api-keys> — the browser receives only the publishable key; no secret/service-role key is referenced by the site implementation.
- RLS: <https://supabase.com/docs/guides/database/postgres/row-level-security> — exposed state has RLS, authenticated role policies, owner predicates, and a Postgres 15+ `security_invoker` view.
- Breaking-change changelog: <https://supabase.com/changelog?types=breaking-change> — explicit Data API grants are included rather than depending on default exposure/grants.
- CLI migrations: the official CLI contract was used to create `20260727141820_durable_saved_events_v1.sql` through `supabase migration new`.

The migration was **not applied to a live project**. No configured personalization Supabase URL/key or direct database connection was available in this worktree. Local `migration list --local` and `db lint --local` could not connect to Postgres at `127.0.0.1:54322`; this is recorded as an integration validation gate, not reported as a passing SQL execution.

## Commands and validation

```text
git diff --check
node --test \
  site/tests/favorites-home-contract.test.mjs \
  site/tests/pwa-install.test.mjs \
  site/tests/static-site-auth.test.mjs
```

Result: **14/14 tests passed**, `0` failed. Contracts cover future-only calendar-first dedupe, static/local home ranking, post-action like/unlike, calendar add/remove, same-event source merge, shared auth/card runtime use, and absence of browser secret-key paths.

Final-source Astro development smoke:

```text
cd site
npm run dev -- --port 4329
curl http://127.0.0.1:4330/
curl http://127.0.0.1:4330/izbrannoe/
```

Port `4329` was already owned by another lane, so Astro selected `4330`. Both routes returned HTTP `200`; root HTML contained exactly `30` `data-home-feed-item` nodes and all home selectors; Favorites contained the skeleton/grid selectors and `noindex,nofollow,noarchive`.

A full `npm run build` compiled Astro/Vite and all entrypoints, then failed during static route generation at `/data/discovery/6425.json/` with `ENOSPC`. The worktree had approximately 200–350 MB free during final validation. No generated `dist/` is retained. The integration owner must run the full static build once workspace disk pressure is resolved.

## Risks and integration notes

- The migration and SQL transaction fixture need execution against an integration/local Supabase database before deploy; no live migration apply was attempted.
- `Reference4MobileMenu.astro` is explicitly outside this lane. Its current Favorites navigation target must be reconciled by its owner to `/izbrannoe/`.
- Canonical docs and `CHANGELOG.md` are explicitly outside this lane and remain owned by the documentation/integration lane.
- Quick-nav destinations for Free and Unusual are owned by their respective lanes and must exist in the integrated tree.
- Cross-device behavior depends on deploying the migration and supplying `PUBLIC_PERSONALIZATION_SUPABASE_URL` plus `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`. The browser implementation contains no service-role/secret fallback.

## Changed files

- `site/src/components/FavoritesSurface.astro`
- `site/src/components/HomeColdStartFeed.astro`
- `site/src/components/HomeHeroTalk.astro`
- `site/src/components/HomeQuickNav.astro`
- `site/src/components/auth/StaticSiteAuthRuntime.astro`
- `site/src/env.d.ts`
- `site/src/lib/favorites.mjs`
- `site/src/lib/homeFeed.mjs`
- `site/src/lib/savedEventRuntime.ts`
- `site/src/lib/savedEventRuntimeCore.mjs`
- `site/src/lib/savedEvents.ts`
- `site/src/pages/index.astro`
- `site/src/pages/izbrannoe/index.astro`
- `site/tests/favorites-home-contract.test.mjs`
- `site/tests/pwa-install.test.mjs`
- `supabase/migrations/20260727141820_durable_saved_events_v1.sql`
- `supabase/tests/durable_saved_events_contract.sql`
- `.codex/lanes/R15-L05-FAVORITES-HOME/RESULTS.md` (lane evidence only)
