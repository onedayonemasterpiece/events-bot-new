# Единый агрегат вовлечённости события: источники и KenigEvents

> **Статус:** принятый TO-BE design; end-to-end runtime ещё не завершён.
> **Статистика сайта:**
> [`../static-site-pages/analytics/unified-statistics-runtime-architecture.md`](../static-site-pages/analytics/unified-statistics-runtime-architecture.md).

## Решение

Likes, shares/reposts и valid views относятся к одному event-level aggregate,
независимо от surface:

- Telegram/VK source posts;
- отдельно идентифицированные managed publications;
- accepted first-party KenigEvents actions/views.

Public UI может показывать совместимый total, но internal model сохраняет
components, source count, freshness и completeness. `views_count` — сумма
наблюдений reach, не unique people.

## Один domain API и один popular projection

Все consumers используют один batch API, implementation-equivalent:

```python
load_consolidated_event_engagement(event_ids, as_of, window)
rank_popular_events(engagement, eligibility, policy_version)
```

Required consumers:

- `/populyarnoe/`;
- Telegram `/popular_posts` и daily ranking;
- CherryFlash/video candidate selection;
- static counter manifest;
- related/personal ranking, когда popularity разрешена;
- operator/debug reports.

Consumers могут выбирать documented policy/window, но не реализуют собственные
joins, medians, source baselines или hidden weight formulas. Astro получает
already computed score/order and provenance.

## Versioned output

```text
EventEngagement
  event_id
  schema_version
  computed_at
  source_refreshed_at
  site_refreshed_at
  completeness = full | source_only | site_only | stale | unavailable

  source_components[]
    surface
    post_count
    views / likes / comments / reposts
    refreshed_at

  source_views / source_likes / source_reposts
  site_valid_views / site_likes / site_shares
  total_views / total_likes / total_shares
  normalized_source_features
  normalized_site_features
  popularity_score_version / popularity_score
```

Rules:

- one physical post contributes once through canonical source identity;
- latest valid maturity snapshot is used, not sum of all snapshots;
- like is current state: unlike removes contribution;
- accepted share does not claim recipient opened the link;
- site valid views exclude bots, previews, reload storms and duplicate render;
- missing/stale component is not converted to fresh zero;
- changed weight policy creates a new score version.

## Ownership and storage

- Fly SQLite remains SOR for source post snapshots and event/source mapping;
- authoritative site reactions/current counters stay in their product SOR;
- Unified Statistics Runtime delivers compact site observations/receipts and
  de-identified history;
- YDB stores bounded recent facts and aggregates, not current identity/control;
- Object Storage stores verified Parquet history;
- static pages read one small CDN manifest, not one remote request per card.

Storage invariants:

```text
one compact current aggregate per event
no raw page-view firehose
no event descriptions/source text/media payload copies
no permanent row per button click
bounded evidence + TTL
```

## Reliability

- source and site writes are idempotent;
- projector uses last-good state and never zeroes a component on partial failure;
- freshness/completeness travel with every result;
- event merge/cancel/reopen has explicit recompute/redirect behavior;
- counter manifest update does not require a full static rebuild;
- analytics failure cannot block source import or durable user-control action.

## Release acceptance

- [ ] One canonical batch API and output schema.
- [ ] One versioned popular ranker/projection.
- [ ] All consumers are thin consumers of the same components.
- [ ] TG/VK/site dedupe and latest-snapshot fixtures pass.
- [ ] Source-only, site-only, blended, missing and stale counterfactuals are
  distinguished.
- [ ] Like/unlike, repeated share, reload/bot/preview and event merge do not
  inflate counts.
- [ ] `/populyarnoe/` order is SHA-bound to the projection manifest.
- [ ] No per-event remote read loop and no raw browser analytics in Supabase.
- [ ] Relation/index/TTL/archive budgets pass launch/1k/10k forecast.
