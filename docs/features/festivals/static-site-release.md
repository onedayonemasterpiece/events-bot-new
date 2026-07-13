# Separate post-release: static festival section

> Status: **planned separate release after the first public static-site presentation**. This document is the canonical release contract for the festival section; it is not an F1–F17 presentation GO gate.

## Honest baseline

The core repository already has festival series/edition concepts, Smart Update detection, `festival_queue`, a one-shot Universal Festival Parser and Telegraph festival pages/index. The new Astro static site does **not** yet have a festival index, festival detail route, festival card or a stable edition link from event cards/pages. Queue scheduling is disabled by default and permanent website monitoring is not implemented.

The existing [festival feature](README.md), [parser](../source-parsing/sources/festival-parser/README.md) and [monitoring debt](../../backlog/features/festival-monitoring-debt/README.md) remain canonical for their respective behavior. This release does not silently re-enable aggregate VK festival posts.

## Mandatory scope

| ID | Deliverable | Current stage | Release acceptance |
|---|---|---|---|
| FEST-1 | Public `/festivali/` section | Missing | index plus stable edition pages are in the checked static artifact, sitemap/link graph and CDN promotion |
| FEST-2 | Reliable festival queue parsing | Partial / open debt | idempotent queue state, retry/quarantine, full live VK/TG/site handoff E2E and no silently stuck items |
| FEST-3 | Universal festival monitoring, at least official websites | One-shot parser only | registered sources run on schedule, changed content is extracted LLM-first, unchanged content is skipped, stale/failure state is visible |
| FEST-4 | Unmistakable festival card | Missing | separate card design visibly says `Фестиваль` and passes owner, mobile, desktop, a11y and no-JS acceptance |
| FEST-5 | Stable event ↔ festival relation | Partial / name string | event card/detail links to the exact edition; festival page lists linked events; merge/rename does not break public links |
| FEST-6 | Stable static refresh | Missing | festival-only and linked-event changes trigger one coalesced checked build with last-good promotion, freshness evidence and rollback |

All six items are mandatory for this separate release. A collection of logos/medallions or the legacy Telegraph index is not a partial public substitute for the static festival section.

## Product and information architecture

- Canonical URL family: `/festivali/` plus one stable edition leaf page; default proposal is `/festivali/<edition-slug>/`.
- The primary index shows current and upcoming editions. An accessible archive exposes previous editions without mixing them into the primary upcoming feed.
- A festival card is not a recolored event card. It includes an explicit `Фестиваль` type label, series and edition/year, date range/status, city/area, cover/logo, number of linked events or programme items, and a canonical CTA.
- Event list cards and event details show a linked `В рамках фестиваля …` relationship. The festival detail page provides the reverse list of canonical event rows.
- Programme-only items may appear in a separately labelled programme block and must never look like fully specified event cards.
- Series/edition navigation, lifecycle, archive and superseded-edition behavior are explicit; a renamed or merged edition preserves redirects.

## Data and identity gate

Before public URLs are minted, replace the fragile `Event.festival == Festival.name` public relation with a stable edition identity. The implementation task may use a normalized relation table or a direct `festival_id`, but must support:

- one primary edition relation plus explicit additional relations where product-approved;
- source/evidence, confidence and timestamps;
- idempotent Smart Update linking and deterministic merge/redirect migration;
- compatibility projection for legacy Telegraph behavior during migration;
- a versioned festival projection consumed by the static builder.

Fly SQLite remains the canonical festival/event fact store. The static builder consumes immutable projections; browser code does not parse festival sources or query the core DB.

## Queue and monitoring gate

The [festival monitoring debt](../../backlog/features/festival-monitoring-debt/README.md) must be closed for the release:

1. queue claim/lease and state transitions are atomic and idempotent (`pending/running/done/retry/quarantine` or equivalent);
2. source identity and content fingerprints prevent duplicate work while allowing a genuinely changed programme to be reprocessed;
3. website sources have a registry, cadence, last attempt/success/change, effective snapshot and freshness state;
4. official websites are the minimum supported monitoring class; Telegram/VK adapters share the same normalized handoff but do not weaken source-specific safety;
5. rendered HTML/PDF/programme inputs use a bounded Playwright fetch and LLM-first extraction with schema/evidence validation, bounded diff and atomic last-known-good output;
6. the legacy Gemma 3 parser path is migrated/evaluated under the current project LLM policy before production enablement;
7. robots, rate, licensing and source terms are recorded per monitored website;
8. VK, Telegram and external-URL live E2E records source → queue → parser → edition/event relation → static artifact evidence.

## Static generation and reliability

- Any effective festival projection or event↔festival relation change updates a projection hash and schedules the standard coalesced static build.
- Multiple provider/queue changes in the coalescing window cause one build, not one build per source.
- Festival pages use the same immutable artifact, validation, unique staging prefix, atomic current pointer, CDN parity, last-good rollback and release manifest as other canonical static pages.
- The manifest records festival snapshot/hash, page and relation counts, source freshness and rejected/quarantined counts.
- Empty/partial/provider-failed output never overwrites last-good. After the approved staleness limit, affected content fails closed or is visibly stale according to the accepted product policy.
- Sitemap, canonical, JSON-LD and visible facts agree; archived/private/preview surfaces obey the shared SEO/GEO contract.

## Release stages

1. **Scope and identity freeze:** URL/archive/programme-only decisions; stable edition relation and migration plan.
2. **Queue burn-down:** reproduce current queue failures, close root causes, add missing queue E2E and production-safe scheduling.
3. **Website monitoring:** source registry, scheduled changed-only Kaggle runs, LLM/evidence validation, last-good/freshness/alerts.
4. **Static projection:** exporter, index/detail routes, reverse relations, coalesced rebuild and manifest/parity checks.
5. **UI/UX freeze:** distinct festival card, event-card/detail link, mobile/desktop/a11y/no-JS owner acceptance.
6. **RC:** live source-to-CDN E2E, failure/staleness/rollback drills and exact release evidence.
7. **Festival SEO/GEO rerun:** audit the new page family after its own UI/UX freeze; the earlier site release audit cannot cover pages that did not yet exist.

## Release-wide decisions still requiring owner approval

1. Confirm `/festivali/<edition-slug>/` and whether a series ever receives its own page.
2. Confirm current/upcoming primary index plus separate archive.
3. Approve stable edition-id migration before public linking.
4. Approve daily changed-only website monitoring as the default cadence and set the maximum stale age.
5. Confirm that programme-only rows are public only in a clearly separate programme block.

## Evidence pack

- clean `origin/main`-reachable SHA and immutable festival RC build;
- source registry and per-source freshness/terms evidence;
- queue backlog/retry/quarantine ledger and live VK/TG/site E2E;
- relation migration/merge/redirect tests;
- full `/festivali/` index/detail/event-link inventory and Playwright screenshots;
- a11y, no-JS, sitemap/canonical/JSON-LD/link-graph checks;
- changed-only rebuild, provider failure, stale fail-closed and last-good rollback drills;
- final festival-page SEO/GEO audit and owner sign-off.
