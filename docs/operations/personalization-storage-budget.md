# Personalization Supabase storage budget and compaction

> Status: **measured/unapplied foundation plus required release operations contract**. Identity/saved-event tables and monitoring activation are not production-complete.
> Capacity assumption: personalization Supabase/Postgres has a hard plan limit of approximately **500 MB**, including tables and indexes. A fresh provider-plan check is required before release.

## Goal

Keep personalization resource-efficient and predictable as usage grows. Supabase is the durable current-state/control plane, not an unlimited telemetry lake. The design follows the same separation used for YDB:

- **browser localStorage** keeps the immediate compact device projection;
- **Supabase** keeps verified identity, consent, current profile/state, favorites, subscriptions, send control and only bounded evidence needed for product correctness;
- **YDB** receives asynchronous de-identified history/analytics with TTL when high-volume history is genuinely useful;
- **Object Storage/CDN** keeps generated manifests/pages/media and bulky immutable artifacts;
- **Fly SQLite** remains the canonical event source, so Supabase stores only bounded event/vector/card projections.

YDB is not an overflow database for identity, consent, favorites or email control. Approaching the Supabase cap must trigger compaction/admission controls, not an unsafe split-brain migration.

## Measured foundation — 2026-07-17

A redacted dual-DB probe against the personalization project reported:

- current database: **38,759,571 bytes (~37 MB)**;
- headroom to 500,000,000 bytes: **461,240,429 bytes (~461 MB)**;
- largest relation: `public.event_embeddings`, approximately 17 MB;
- `email_control` is present; `site_identity`/`saved_events` are not live.

Reproduce without printing secrets:

```bash
python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py --env .env
```

Conservative implementation sizing above this baseline is 1.5 KiB per profile,
0.8 KiB per saved occurrence, 0.5 KiB per optional signal and 1.2 KiB per reminder
opt-in including terminal delivery evidence. The 10k-user/10-save scenario is about
112 MB total; 25k is about 226 MB; 50k is about 416 MB and outside the safe release
envelope after unrelated growth and vacuum slack.

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
9. **Engagement is current-state first.** Source+site views/likes/shares keep one compact current event aggregate in Supabase; TG/VK age-bucket snapshots stay in Fly SQLite, raw site views are compacted before Postgres, and optional history is folded/projected instead of duplicating every observation.

## Proposed retention defaults for product approval

In plain product terms, the only unresolved user-facing question is: **for how long should the service remember an inactive person's compact interests before forgetting/anonymizing that profile?** The recommended answer is one year after the last visit. Short technical logs are removed much earlier automatically, while consent/suppression evidence follows its separate safety/legal policy.

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

The product owner has not yet approved these retention periods. Until the simple inactive-profile question and legal exceptions are approved, implementation must choose the shortest safe end of any documented technical-log range and must not treat the table above as final release truth.

## Implemented identity-foundation retention and stricter launch guards

The unapplied service-only `personalization_retention_cleanup_v1` contract uses:

- device proof expiry: 180 days;
- soft-removed saves and inactive signals: 30 days;
- merge audit and reminder delivery/idempotency evidence: 400 days;
- completed purge requests: 90 days after completion;
- active saves, identity links, active consents and suppressions: retained until
  user deletion or their separate canonical retention policy.

Run cleanup daily from the service scheduler and record only aggregate counts. The
browser must never invoke it. Until measured production automation exists, stricter
launch guardrails override the broad planning bands: warn at 250 MB, freeze bulk
backfills at 325 MB, block release at 350 MB without approved capacity, and at
425 MB stop new disposable/materialization writes while preserving consent
withdrawal, unsubscribe/suppression, reminder cancellation and static/ICS access.

Before any user data exists, rollback may remove the new schemas/functions only
after confirming they are empty. After activation, disable Edge/scheduler producers
and grants, export/preserve user state and use a reviewed data-preserving migration;
never drop live schemas directly.

## Monitoring and forecast

At least daily during canary and weekly in steady state, record without personal data:

- `pg_database_size(current_database())` and percentage of the current provider limit;
- `pg_total_relation_size` for top tables/indexes and their week-over-week delta;
- rows/bytes per active user-day and per served list/session summary;
- index-to-table ratio, dead-row/compaction lag and TTL rows/bytes removed;
- event/vector/tag projection cardinality by active model/version;
- engagement aggregate rows/bytes/index bytes, accepted site-view summary volume and counter updates per active event/day;
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
