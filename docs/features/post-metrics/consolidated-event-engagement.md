# Consolidated event engagement: sources + KenigEvents site

> Status: **required release design / not implemented end to end**. Source TG/VK snapshots and a partial source-like counter sync exist; first-party site persistence, one shared event-level read function and a shared popular-event projection for all consumers do not. The current static `/populyarnoe/` page is a source-only preview, not release evidence for this contract.

## Product requirement

Likes, shares/reposts and views belong to an **event engagement aggregate**, regardless of whether they came from:

- Telegram source posts;
- VK source posts;
- managed KenigEvents Telegram/VK publications when separately identifiable;
- first-party actions and valid views on `kenigevents.ru`.

Public UI may show one simple total where semantics are compatible, but internal/audit data must retain the component split, source count and freshness. A consolidated view count is reach observations across surfaces, **not unique people**.

The aggregate is enriched as new source snapshots and site actions arrive. Consumers must not reimplement mappings, sums, source baselines or fallback rules independently.

The public static [`/populyarnoe/`](../static-site-pages/README.md) listing is a global popular-events product surface. Its main ordered list must be based on this aggregate and a shared versioned popular-event ranker that materially accounts for both source engagement and accepted KenigEvents site engagement. Reusing `EventListItem.astro` only reuses rendering and does **not** satisfy the shared popularity-component requirement.

## Current gap

The repository already has several pieces, but not one release-safe contract:

- `source_parsing/post_metrics.py` stores bounded TG/VK post snapshots and calculates source-relative baselines;
- `handlers/popular_posts_cmd.py` contains its own loading/mapping/baseline/report logic;
- `main_part2.py` has a separate daily event audience aggregation;
- `reaction_counter_sync.py` can project source likes/views into the personalization counter table;
- `site/scripts/export-production-preview-data.py` exports source likes/views/reposts but hardcodes `service_likes_count=0` and exports no accepted site views/shares;
- `site/src/pages/populyarnoe/index.astro` calls the local TypeScript `getPopularEvents()`, while `site/src/lib/events.ts::eventPopularityScore()` recomputes its own source-dominated score (site likes are only a small separate term; site views/shares are absent);
- static-site likes/shares remain partly local/preview and site view persistence is not a consolidated production signal;
- `personalization_event_reaction_counter` is directionally the compact event-level projection, but it is not yet the single complete read model for `/popular_posts`, daily, static pages and video selection.

Therefore “post metrics implemented” and the existence of the `/populyarnoe/` route do not mean “source + site event engagement consolidated”. At present the route is best described as **source-engagement preview / partial**, not as a release-ready combined popularity section.

## Single read contract

Introduce one canonical batch function in the post-metrics domain, with an implementation-equivalent interface such as:

```python
async def load_consolidated_event_engagement(
    db: Database,
    *,
    event_ids: Sequence[int] | None = None,
    as_of_ts: int | None = None,
    window: EngagementWindow | None = None,
    site_snapshot: Mapping[int, SiteEngagement] | None = None,
) -> dict[int, EventEngagement]:
    ...
```

The exact Python names may change in the implementation task, but there must be one domain API and one versioned output schema. It loads in batches; consumers never call Supabase once per event.

Required consumers:

- the public static `/populyarnoe/` ordered event projection;
- `/popular_posts`;
- `build_daily_posts` audience labels/ranking;
- CherryFlash/`/v` popular-event candidate selection;
- static-site counter export/manifest;
- related/personal ranking features when social popularity is an allowed input;
- operator/debug reports and later dashboards.

The consumers may choose different weights/windows, but they receive the same raw components, provenance, freshness and normalized source features. They may not maintain separate SQL joins or definitions of an event’s likes/shares/views.

## Shared popular-event projection

The common component has two layers, both owned by the post-metrics/domain side rather than by an Astro page:

1. `load_consolidated_event_engagement(...)` (or implementation-equivalent) produces the canonical source + site event aggregate.
2. One versioned batch ranker/projection such as `rank_popular_events(...)` applies an explicit `policy_version`, window and eligibility contract, and materializes the ordered static-build input.

Minimum conceptual build-time projection:

```text
PopularEventProjection
  event_id
  rank
  popularity_score
  popularity_score_version
  computed_at
  window
  completeness
  source_refreshed_at
  site_refreshed_at
  source_signal_summary
  site_signal_summary
```

Contract for `/populyarnoe/`:

- the static exporter receives the already ordered/versioned projection or the already computed per-event score; Astro does not calculate medians, join counters or maintain its own weight formula;
- a TypeScript helper may filter by public lifecycle or read a precomputed rank, but it must be a thin consumer. The current independent `eventPopularityScore()` implementation must be removed or reduced to that role before release;
- V1 inputs include distinct/latest accepted TG/VK source views, likes and reposts plus accepted KenigEvents valid views, current likes and shares. Favorite/calendar saves are not silently recast as likes; adding them later requires a new documented score version;
- source and site signals are normalized/calibrated so platform scale does not make either group decorative. Golden counterfactuals must prove that a source-only strong event and a site-only strong event can each enter or materially move within the list, while a blended event is combined once;
- a fresh zero is different from a missing component. The release page may claim combined popularity only when both source and site pipelines are fresh enough under the accepted SLO. On partial failure the build preserves the last-good full projection or exposes an explicitly approved degraded state; it never silently relabels a source-only result as combined popularity;
- only canonical public current/future occurrences are eligible; canceled, merged-away, invalid and already ended occurrences are excluded or redirected before ranking;
- the main `/populyarnoe/` order is global. `ListingPersonalFilter` may narrow visibility without recomputing the score, and `PersonalFeedSlot` remains a separately labelled personalized block that cannot insert into or reorder the popular list;
- `/popular_posts` and other consumers may have different documented policies/windows, but if they request the same policy/window/eligibility their event score/order must reproduce the static projection. Any difference is attributable to a recorded policy or presentation limit, not a private SQL/formula;
- the public copy changes to “популярно в источниках и на KenigEvents” only after the combined projection gate is green.

## Versioned result

Minimum conceptual output per event:

```text
EventEngagement
  event_id
  schema_version
  computed_at
  source_refreshed_at
  site_refreshed_at
  completeness = full | source_only | site_only | stale | unavailable

  source_components[]
    surface = telegram_source | vk_source | telegram_managed | vk_managed
    posts_count, views_count, likes_count, comments_count, reposts_count
    refreshed_at

  source_posts_count
  source_views_count
  source_likes_count
  source_comments_count
  source_reposts_count
  source_relative_views_score
  source_relative_likes_score

  site_valid_views_count
  site_likes_count
  site_shares_count

  views_count
  likes_count
  shares_count
  popularity_score_version
  popularity_score
```

Rules:

- `likes_count = source_likes_count + site_likes_count`;
- `shares_count = source_reposts_count + site_shares_count`, while the component split remains available;
- `views_count = source_views_count + site_valid_views_count` only after both counters meet the accepted view definition; it is never called unique audience;
- the function exposes compact per-surface TG/VK/managed components for audit and channel-aware ranking, while the long-lived Supabase current row does not duplicate every underlying source snapshot;
- source comments may enrich ranking/debug evidence but are not silently treated as site reviews;
- missing/stale components are explicit. A failed site projection is not converted to a fresh zero;
- raw totals and source-relative normalized scores remain separate because Telegram, VK and site traffic have different scale/distribution;
- downstream ranking records `popularity_score_version`; changing weights cannot silently reinterpret historical evidence.

## Source identity and deduplication

Source aggregation operates on distinct canonical post identities, not URL strings alone:

- Telegram: canonical `(source_id/chat, message_id)` plus normalized source URL;
- VK: canonical `(group_id, post_id)` plus inbox/event-source mapping;
- managed posts: count separately only when they are genuinely separate audience surfaces; do not attach the same physical post twice through legacy and current URLs;
- each source post contributes its latest valid/max monotonic snapshot for the requested maturity window, not the sum of all `age_day` observations;
- one post linked through several `EventSource` paths contributes once to one event aggregate;
- an event merge transfers/recomputes mappings idempotently; merged/inactive rows do not keep competing public aggregates.

Site actions are also idempotent:

- likes are current state, so like→unlike removes the service-like contribution rather than appending an eternal extra count;
- a share count uses one documented accepted action (`native_share_invoked`, successful copy or another selected contract), deduped by `client_event_id`/session bucket; it must not claim the external recipient opened anything;
- valid site views exclude bots, previews, reload storms and duplicate hydration/render events. The implementation task must define the bounded session/day dedup rule before publishing the total.

## Data ownership and projection

One read function does **not** mean one physical database or a cross-database transaction:

- Fly SQLite remains source of truth for TG/VK post snapshots, event/source mapping and canonical event lifecycle;
- personalization Supabase/Postgres remains source of truth for first-party current reaction state and compact current site counters;
- a best-effort batch projection enriches one compact current event-counter row and a same-origin CDN manifest;
- YDB may keep de-identified daily/historical analytics with TTL, not user-control state;
- source import and a site CTA never fail because the analytics/projector destination is unavailable.

The canonical read function composes source metrics with one preloaded last-good site snapshot/current projection. It must not make N remote calls for N events. Freshness/completeness travels with the result so `/popular_posts` and other consumers can degrade honestly.

## Ecological storage contract

The feature must minimize both Fly SQLite and Supabase/Postgres growth:

1. **Current aggregate:** one compact row per event, updated only when a component/version changes.
2. **Source snapshots:** retain only bounded `age_day` maturity buckets and existing finite horizon; never copy all source metric rows into Supabase.
3. **Site likes:** one current state row per actor+event plus the event counter; short bounded strong-action evidence only.
4. **Site shares:** compact deduped counters/current summaries; no permanent row per button click.
5. **Site views:** browser/session or edge-side compaction before durable storage. Do not store an unbounded raw page-view row stream in Supabase.
6. **History:** fold to daily aggregates and project de-identified history to YDB with TTL only when useful. Supabase keeps current totals plus bounded audit.
7. **No duplicated payloads:** no event descriptions, source text, reaction JSON, page URLs or device blobs in the long-lived counter row.
8. **Indexes:** only indexes required by event lookup, current-state idempotency, cleanup and RLS; table and index bytes are measured together.
9. **Lifecycle:** archive/delete compact projections for events outside the approved retention horizon, while preserving only aggregate history required by product policy.
10. **Manifest-first reads:** static pages fetch a small CDN-backed counter manifest after first paint; do not create a Supabase read per page/card view or rebuild the whole site for every counter delta.

Capacity release evidence includes relation/index size, bytes per event and active user-day, update rate, TTL/compaction result and launch/1k/10k forecast under the existing 500 MB budget.

## Reliability and enrichment

- Writes from source scanners and site actions are idempotent and monotonic where the upstream metric is monotonic; current-state unlike/undo changes are applied transactionally.
- Projection uses last-good state, bounded retry and a freshness alert. It does not zero counters on partial failure.
- Every aggregate records component timestamps and schema/score versions.
- New metrics enrich the same event row; consumers do not need to rebuild historical raw events to learn the current totals.
- Counter changes generate the small manifest/projection only, not a full static rebuild.
- A canonical event merge/reopen/cancel operation has an explicit aggregate recompute/redirect rule.

## Release acceptance

- [ ] One canonical batch function/output schema exists and has contract tests.
- [ ] One versioned popular-event ranker/projection exists; `/populyarnoe/` consumes its precomputed order/score and contains no independent median/weight formula.
- [ ] `/populyarnoe/`, `/popular_posts`, daily labels/ranking, CherryFlash popular selection and static counter export use the canonical aggregate; duplicate private aggregators are removed or become thin consumers.
- [ ] Golden fixture with TG + VK + site view/like/share proves distinct-post dedup, latest-age selection, source/site totals, freshness and score version.
- [ ] Counterfactual ranking fixture proves source-only, site-only and blended popularity can each affect the result materially; fresh zero, missing, stale and last-good inputs are not conflated.
- [ ] Like/unlike, repeated share/copy, reload/bot/preview views, event merge and source repost cases do not inflate counts.
- [ ] Source-only/site-only/stale/unavailable states degrade honestly and preserve last-good values.
- [ ] The public manifest and cards show compatible totals while internal evidence retains component split.
- [ ] A build check compares the `/populyarnoe/` event IDs/order with its SHA-bound projection manifest, and Playwright verifies the rendered order, lifecycle exclusions, combined-popularity copy and separately labelled personalized slot.
- [ ] No per-event remote read loop and no per-page Supabase read dependency.
- [ ] Storage test proves current-row cardinality, bounded evidence/TTL, relation+index budgets and near-cap shedding without breaking likes/unlikes or other durable controls.
- [ ] Reconciliation compares aggregate output with raw TG/VK snapshots and accepted site summaries for a production snapshot; drift is zero or explained.

## Related documentation

- [Post Metrics & Popularity](README.md)
- [Static event reaction counters](../static-site-pages/reaction-counters.md)
- [Personalization data ownership](../../architecture/personalization-data-ownership.md)
- [Personalization storage budget](../../operations/personalization-storage-budget.md)
- [Unsigned personalization telemetry](../unsigned-personalization/README.md)
- [Release readiness checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md)
