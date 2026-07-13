# Consolidated event engagement: sources + KenigEvents site

> Status: **required release design / not implemented end to end**. Source TG/VK snapshots and a partial source-like counter sync exist; first-party site persistence and one shared event-level read function for all popularity consumers do not.

## Product requirement

Likes, shares/reposts and views belong to an **event engagement aggregate**, regardless of whether they came from:

- Telegram source posts;
- VK source posts;
- managed KenigEvents Telegram/VK publications when separately identifiable;
- first-party actions and valid views on `kenigevents.ru`.

Public UI may show one simple total where semantics are compatible, but internal/audit data must retain the component split, source count and freshness. A consolidated view count is reach observations across surfaces, **not unique people**.

The aggregate is enriched as new source snapshots and site actions arrive. Consumers must not reimplement mappings, sums, source baselines or fallback rules independently.

## Current gap

The repository already has several pieces, but not one release-safe contract:

- `source_parsing/post_metrics.py` stores bounded TG/VK post snapshots and calculates source-relative baselines;
- `handlers/popular_posts_cmd.py` contains its own loading/mapping/baseline/report logic;
- `main_part2.py` has a separate daily event audience aggregation;
- `reaction_counter_sync.py` can project source likes/views into the personalization counter table;
- static-site likes/shares remain partly local/preview and site view persistence is not a consolidated production signal;
- `personalization_event_reaction_counter` is directionally the compact event-level projection, but it is not yet the single complete read model for `/popular_posts`, daily, static pages and video selection.

Therefore “post metrics implemented” does not mean “source + site event engagement consolidated”.

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

- `/popular_posts`;
- `build_daily_posts` audience labels/ranking;
- CherryFlash/`/v` popular-event candidate selection;
- static-site counter export/manifest;
- related/personal ranking features when social popularity is an allowed input;
- operator/debug reports and later dashboards.

The consumers may choose different weights/windows, but they receive the same raw components, provenance, freshness and normalized source features. They may not maintain separate SQL joins or definitions of an event’s likes/shares/views.

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
- [ ] `/popular_posts`, daily labels/ranking, CherryFlash popular selection and static counter export use it; duplicate private aggregators are removed or become thin consumers.
- [ ] Golden fixture with TG + VK + site view/like/share proves distinct-post dedup, latest-age selection, source/site totals, freshness and score version.
- [ ] Like/unlike, repeated share/copy, reload/bot/preview views, event merge and source repost cases do not inflate counts.
- [ ] Source-only/site-only/stale/unavailable states degrade honestly and preserve last-good values.
- [ ] The public manifest and cards show compatible totals while internal evidence retains component split.
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
