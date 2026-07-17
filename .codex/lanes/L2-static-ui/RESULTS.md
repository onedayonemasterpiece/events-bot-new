# Lane L2-static-ui Results

## Status
committed (recovered by integrator after worker turn ended)

## Requirement IDs
- R04

## Branch
`agent/interest-clubs/l2-static-ui`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/interest-clubs-l2-static-ui`

## Base SHA
`08f2deb881c42d9cb55d56900450685817c19450`

## Files changed
- `site/scripts/build-preview.mjs`
- `site/scripts/check-preview.mjs`
- `site/scripts/export-production-preview-data.py`
- `site/src/components/InterestClubCard.astro`
- `site/src/data/interest-clubs.json`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/clubs.ts`
- `site/src/lib/types.ts`
- `site/src/pages/kluby-po-interesam/index.astro`
- `site/src/pages/kluby-po-interesam/[slug]/index.astro`
- `site/src/pages/sitemap.xml.ts`
- `tests/test_interest_clubs_static_export.py`

## Delivered contract
- Versioned `interest-clubs-static-v1` export from approved clubs plus active relations only.
- Default-OFF projection gate `ENABLE_INTEREST_CLUB_STATIC_PROJECTION` and public promotion gate `PUBLIC_INTEREST_CLUBS_ENABLED`.
- Public recurrence/freshness gates: at least two distinct dates, plus future/current meeting or last activity within 90 days.
- Fail-closed handling for missing schema, shadow/deferred/invalid/stale rows and festival/program rows.
- Public index/detail pages, cards, future meetings, breadcrumbs, JSON-LD, sitemap/navigation gating, mobile/accessibility/no-JS semantics.

## Commands run
- `uv run --with-requirements requirements.txt python -m pytest tests/test_interest_clubs_static_export.py -q`
- `PREVIEW_BUILD_ID=preview-interest-clubs-local PUBLIC_INTEREST_CLUBS_ENABLED=1 ENABLE_INTEREST_CLUB_STATIC_PROJECTION=1 npm run build:preview`
- `npm run check:preview`
- `git diff --check`

## Tests / verification
- Static export tests: **3 passed**.
- Astro preview: **423 pages built**, including index and two evidence-backed fixture detail pages.
- Interest-club checks in `check-preview.mjs` passed after making H1 assertions Astro-scope-attribute-safe. The full check then stopped at the pre-existing authorized-search assertion because the local build intentionally had no public Supabase/Yandex env; this is unrelated to the club surface.
- Club navigation is hidden and detail paths are empty when the default-OFF public flag is absent.

## Risks
- Fixture detail pages are preview-only; production projection is empty until explicit schema/bootstrap/approval plus both build/public flags.
- V1 fails closed for festival-contained events even if future product review might accept separately evidenced co-hosting.
- No visual browser screenshots or live deployment were performed.

## Merge notes
- Core schema contract: `interest_club` + `interest_club_event` from L1.
- Integrator must add the three exact flags to `.env.example` and canonical release docs, then run combined tests after cherry-pick.
- No push performed from this lane.
