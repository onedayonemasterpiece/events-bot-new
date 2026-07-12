# Personalization Supabase storage budget and compaction

> Status: **required release operations contract; monitoring/compaction automation is not production-complete**.
> Capacity assumption: personalization Supabase/Postgres has a hard plan limit of approximately **500 MB**, including tables and indexes. A fresh provider-plan check is required before release.

## Goal

Keep personalization resource-efficient and predictable as usage grows. Supabase is the durable current-state/control plane, not an unlimited telemetry lake. The design follows the same separation used for YDB:

- **browser localStorage** keeps the immediate compact device projection;
- **Supabase** keeps verified identity, consent, current profile/state, favorites, subscriptions, send control and only bounded evidence needed for product correctness;
- **YDB** receives asynchronous de-identified history/analytics with TTL when high-volume history is genuinely useful;
- **Object Storage/CDN** keeps generated manifests/pages/media and bulky immutable artifacts;
- **Fly SQLite** remains the canonical event source, so Supabase stores only bounded event/vector/card projections.

YDB is not an overflow database for identity, consent, favorites or email control. Approaching the Supabase cap must trigger compaction/admission controls, not an unsafe split-brain migration.

## Capacity envelope

The 500 MB plan limit is a ceiling, not a working target. Initial operating bands:

| Band | Approximate database size | Required action |
|---|---:|---|
| Green | `<300 MB` (`<60%`) | normal operation; weekly growth report |
| Yellow | `300–375 MB` (`60–75%`) | investigate top relations/index growth; increase compaction cadence |
| Orange | `375–425 MB` (`75–85%`) | stop debug sampling and nonessential history; product/ops owner action required |
| Red | `425–450 MB` (`85–90%`) | block new nonessential telemetry/tag/debug evidence; preserve control-plane writes only |
| Critical | `>=450 MB` (`>=90%`) | emergency kill switch for disposable writes, bounded cleanup/rollback plan, capacity decision |

The release target is Green with enough forecast headroom to remain below Orange through the approved launch/canary horizon. Limits must be recalculated from the current provider plan and actual `pg_database_size`, not assumed from an old snapshot.

## Storage classes

| Class | Examples | Policy |
|---|---|---|
| Durable control state | identity links, consent audit, favorites/current reactions, current profile, subscriptions, suppressions, active outbox/issues/token metadata | preserve correctness; compact columns/current state; delete only by lifecycle/legal policy |
| Bounded catalog projection | current event search documents/embeddings, event feature snapshot, accepted public-tag registry | one current row per key/model/version where possible; prune inactive/stale versions |
| Short-lived evidence | served-list/session summaries, search request audit, recommendation debug, quarantine, provider attempt details | strict TTL; fold to aggregate/profile; no indefinite JSON history |
| Analytics/history | de-identified daily/product/delivery aggregates and optionally sampled raw history | project asynchronously to YDB with TTL; Supabase keeps only current counters needed for decisions |
| Generated/bulky artifacts | public/personal/tag HTML/JSON, manifests, screenshots, full debug packs | Object Storage/CDN or ignored test artifacts, never ordinary DB rows |

## Compaction rules

1. **No raw impression firehose.** Ordinary scroll/hover/page-view noise is local/session-aggregated or dropped. Strong actions and served-list context are compact summaries.
2. **Current state over revision history.** Keep one current reaction/favorite/profile state plus bounded audit where required; fold old rows into daily aggregates and profile horizons.
3. **Typed compact fields over JSON copies.** Use ids, arrays, enums, small scores/bitmasks and hashes. Full reason payloads are sampled test/debug evidence only.
4. **No duplicated canonical event bodies.** Store current digest/card/vector projections, not descriptions, OCR, source posts or media blobs already owned elsewhere.
5. **No duplicated personal artifacts or provider payloads.** Store hashes/status/message ids and minimal send evidence; HTML, message bodies, attachments and debug exports belong outside Postgres.
6. **Bound indexes as well as tables.** Every index needs a real query/RLS/cleanup purpose; relation and index size are reviewed together because both consume the plan budget.
7. **TTL is executable.** Retention jobs are idempotent, observable, tested against interrupted/repeated runs and followed by a size/bloat/reuse report. Do not use a disruptive full-table rewrite as routine cleanup.
8. **Fail safely near capacity.** Drop/block disposable telemetry before durable user actions. Favorites, consent withdrawal, unsubscribe/suppression, reminder cancellation and send guards remain available.

## Proposed retention defaults for product approval

| Data | Proposed default |
|---|---:|
| served-list summaries | 21 days, then daily aggregate |
| session summaries | 30 days full, then current profile/aggregate |
| strong-action audit | 90 days, while current reaction/favorite state remains compact |
| search request/debug and recommendation result evidence | 21 days |
| quarantine | 7 days |
| daily product aggregates | 12 months |
| inactive compact profile | 365 days since last seen, subject to consent/delete policy |
| consent, suppression and send-critical evidence | separate legal/product policy; never deleted merely to satisfy telemetry budget |

These defaults become release truth only after product/legal approval. Until then, implementation must choose the shortest safe end of any documented range.

## Monitoring and forecast

At least daily during canary and weekly in steady state, record without personal data:

- `pg_database_size(current_database())` and percentage of the current provider limit;
- `pg_total_relation_size` for top tables/indexes and their week-over-week delta;
- rows/bytes per active user-day and per served list/session summary;
- index-to-table ratio, dead-row/compaction lag and TTL rows/bytes removed;
- event/vector/tag projection cardinality by active model/version;
- projected days to Yellow/Orange at current and peak growth;
- number of writes dropped/blocked by storage band, separated from user-control actions;
- YDB projection lag/failures and Object Storage artifact volume, without treating either as Supabase correctness evidence.

No single `pg_database_size` value is enough: launch evidence includes top-relation attribution and a volume model for at least the launch cohort, `1k` active users and `10k` active users over the approved retention horizon. This is a forecast/test model, not permission to increase launch scope automatically.

## Release gates

- [ ] Fresh plan-limit and database-size snapshot; historical `25 MB` evidence is not reused as current proof.
- [ ] Green-band launch baseline with measured table/index attribution and forecast headroom through canary/hypercare.
- [ ] Synthetic volume test proves row-size/cardinality budgets at launch, 1k and 10k active-user scenarios.
- [ ] TTL/fold/compaction job is idempotent and demonstrated on a restorable snapshot; current profile/favorite/consent/suppression/send state remains correct.
- [ ] Yellow/Orange/Red/Critical alerts and kill switches are tested; disposable writes stop before control-plane writes.
- [ ] E2E asserts normal collection/application plus near-cap behavior: static UX and durable control actions work while nonessential telemetry is rejected.
- [ ] Capacity dashboard and weekly owner are named; projected Orange date is visible.
- [ ] Retention defaults and legal exceptions are approved; account deletion and YDB purge projection are tested.
- [ ] Upgrade/architecture decision is made before Red, never during an outage.

## Related documentation

- [Personalization data ownership](../architecture/personalization-data-ownership.md)
- [Personalization database design](../features/unsigned-personalization/database.md)
- [Production integration](../features/unsigned-personalization/production-integration.md)
- [Personalization E2E acceptance](../features/unsigned-personalization/e2e-acceptance.md)
- [Static personal announcements release checklist](../reports/static-personal-announcements-release-readiness-2026-07-11.md)
