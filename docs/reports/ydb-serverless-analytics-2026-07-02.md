# Yandex Cloud Serverless YDB analytics/storage split audit — 2026-07-02

Status: architecture/report only; no runtime behavior changed.

## Executive summary

1. **We are not currently storing production data in Yandex Cloud Serverless YDB.**
   - Clean `origin/main` has no YDB write path.
   - The current local working copy has an in-progress email-notification YDB stats contract (`email_notifications/ydb_stats.py`, `EMAIL_YDB_*` env names), but the sink is gated/stubbed and does not perform an UPSERT/write.
   - `.env` checked redacted on 2026-07-02: no `YDB*`/`EMAIL_YDB_*` values were present.
2. **Yandex services currently in use/planned are mostly not YDB:** Object Storage/CDN for static site/media, Postbox for email transport, Yandex OAuth for auth.
3. **Supabase personalization DB is currently small:** live redacted check on 2026-07-02 reported `pg_database_size = 31 MB`; top public relations were `event_embeddings` (`14 MB`), `event_search_documents` (`5080 kB`), `event_search_requests` (`528 kB`), `personalization_event_reaction_counter` (`456 kB`).
4. **Production SQLite is not huge yet but is already carrying history/diagnostic weight:** live Fly `/data/db.sqlite` redacted read-only probe on 2026-07-02 reported `198.0 MiB`; largest relations were canonical `event` (`55.97 MiB`), `vk_inbox` (`21.82 MiB`), `event_source` (`17.05 MiB`), `ops_run` (`12.95 MiB`), `event_source_fact` (`11.41 MiB`). `codex_backup_*` tables alone consumed about `11.31 MiB`.
5. **Best first use of YDB:** append-only/TTL analytics and history projections, not canonical data ownership. Start with email delivery events, raw/static-site telemetry, served-list/session summaries, old search audit/feedback, post metric samples, Kaggle run events, terminal `ops_run`, terminal promo/video history.
6. **Keep SQLite thin by policy:** canonical hot state stays in SQLite; long-lived raw/diagnostic/backup/history tables are exported or TTL-archived to YDB/Object Storage and pruned from SQLite.
7. **Keep Supabase under 450 MB by policy:** Supabase keeps auth/RLS-coupled current state and pgvector/search working set; raw telemetry and historical audit streams go to YDB with TTL/rollups.

## Scope and evidence

Inspected areas:

- bot/core SQLite models and migrations: `models.py`, `db.py`, `main.py`, `main_part2.py`, `smart_event_update.py`, `source_parsing/`, `reaction_counter_sync.py`, `general_stats.py`, `kaggle_status.py`, `video_announce/`, `promo.py`;
- static site/export/personalization: `site/`, `site/scripts/export-production-preview-data.py`, static-site docs, unsigned-personalization docs;
- Supabase sidecar: `supabase/migrations/`, `supabase/functions/`, sync scripts;
- email notifications/Postbox/YDB contract in the current local working copy: `email_notifications/`, `supabase/functions/event-email-follow/`, `docs/features/event-email-notifications/README.md`;
- redacted live checks: personalization Supabase size and production Fly SQLite size/table inventory.

Important repository-state note:

- This report branch is based on clean `origin/main` to avoid committing unrelated dirty work from `/home/dev/projects/events-bot-new`.
- Some email-notification/YDB files referenced above were present in the local dirty working copy on 2026-07-02 and may not be visible in this branch until that feature branch is separately merged/pushed.

External docs checked on 2026-07-02:

- Yandex Managed Service for YDB Serverless pricing: <https://yandex.cloud/en/docs/ydb/pricing/serverless>
- Yandex YDB serverless/dedicated concepts and RU throttling: <https://yandex.cloud/en/docs/ydb/concepts/serverless-and-dedicated>
- YDB TTL concept: <https://ydb.tech/docs/en/concepts/ttl>
- Supabase database size/read-only behavior: <https://supabase.com/docs/guides/platform/database-size>
- Supabase egress usage/caching guidance: <https://supabase.com/docs/guides/platform/manage-your-usage/egress>

Key external constraints:

- YDB Serverless charges by request units and stored data; it can elastically handle workload but needs throttling to cap runaway RU spend.
- YDB TTL can delete/move rows after a TTL column age; deletion is background/asynchronous, so queries must still filter logically expired rows.
- Supabase Free projects enter read-only mode when actual Postgres database size exceeds 500 MB; keeping an operational guard at 430–450 MB is reasonable.
- Supabase database size includes data, indexes and materialized views; deletes may require vacuum/autovacuum time before size drops.

## Current database ownership map

| Domain | Current storage | Keep as source of truth? | YDB candidate? | Notes |
|---|---:|---:|---:|---|
| Canonical event rows (`event`) | Fly SQLite | Yes | No, only analytics projection | Many publication fields and local transaction assumptions. |
| Event provenance (`event_source`) | Fly SQLite | Yes | Archive/mirror old read-only copies only | Needed by Smart Update, source metrics, dedup. |
| Source facts (`event_source_fact`) | Fly SQLite | Hot/current yes | Old terminal facts maybe | Existing code replaces facts for event/source; not pure append-only. |
| Posters/media metadata | Fly SQLite + object storage URLs | Yes for metadata | Only audit history | Blobs belong in Object Storage/CDN, not DB. |
| Job outbox (`joboutbox`) | Fly SQLite | Yes | Not first | Queue semantics/coalescing/backoff are local-SQL coupled. |
| Static site generated HTML/JSON/media | Yandex Object Storage/CDN + repo fixtures | Yes (artifact) | No blobs; optional build manifest | Keep static payloads in bucket/CDN. |
| Post metrics (`telegram_post_metric`, `vk_post_metric`) | Fly SQLite | Rolling hot window | Yes | Excellent append/TTL/rollup candidate. |
| Reaction counters | SQLite source metrics → Supabase aggregate | Supabase aggregate current | Raw logs/archive yes | Current `personalization_event_reaction_counter` is compact and should stay. |
| Authorized search documents/embeddings | Supabase Postgres/pgvector | Yes for current search | Search audit/history yes | `event_embeddings` is current biggest Supabase relation. |
| Search requests/feedback | Supabase | Short recent window | Yes | Archive old/audit to YDB. |
| Anonymous personalization raw telemetry | mostly browser local; draft Supabase docs | No Supabase raw firehose | Yes | Do not insert weak raw events into Supabase. |
| Email notification profiles/follows | planned Supabase | Yes | No | Auth/RLS/user consent state belongs near Supabase Auth. |
| Email outbox | planned Supabase | Pending hot queue only | Terminal archive yes | Do not move queue until worker semantics mature. |
| Email delivery events/rate history | planned Supabase | Short operational window | Yes | First YDB implementation target. |
| Ops/Kaggle/video/promo history | Fly SQLite | Recent/current | Yes | Archive terminal rows and append-only diagnostic events. |
| Incident/Codex backup tables | Fly SQLite now | No long-term | Object Storage/YDB manifest | Should not accumulate in core DB. |

## Are we already saving anything to YDB?

### Answer

No active YDB storage path was found.

Evidence:

- Clean branch search: no `ydb`/YDB client dependency or active write code.
- Current local dirty branch: `email_notifications/ydb_stats.py` exports a YDB stats adapter contract, but `YDBStatsSink.record()` intentionally raises `NotImplementedError` after readiness checks.
- `requirements.txt` in the scanned state did not include the official `ydb` SDK.
- `.env.example` contains commented `EMAIL_YDB_*` placeholders in the local dirty branch; `.env` redacted check had no YDB-like keys.
- Supabase email-follow code in the local dirty branch writes `metadata: { ydb_projection_required: true }`; this is only a marker, not a YDB write.

### Distinguish from other Yandex services

The repo does use or plan these Yandex services separately:

- Yandex Object Storage/CDN: static site, media mirror, `static.kenigevents.ru`, `kenigevents.ru` bucket.
- Yandex Cloud Postbox: email transport through SMTP-compatible Postbox endpoint.
- Yandex OAuth: Supabase custom provider / Yandex login.

None of these imply YDB persistence.

## Current Supabase state and 450 MB strategy

### Live size snapshot

Redacted script check on 2026-07-02:

- Database size: `31 MB` (`32877715` bytes).
- Available vs 500 MB decimal: about `467.12 MB`.
- Public table counts/sizes:

| Public table | Rows | Size |
|---|---:|---:|
| `event_embeddings` | 955 | 14 MB |
| `event_search_documents` | 508 | 5080 kB |
| `event_search_requests` | 214 | 528 kB |
| `personalization_event_reaction_counter` | 2866 | 456 kB |
| `user_search_quota_ledger` | 127 | 88 kB |
| `event_search_tag_candidates` | 1 | 80 kB |
| `event_search_feedback` | 1 | 64 kB |
| `search_quota_plans` | 1 | 32 kB |

### Keep in Supabase

Keep data that needs Supabase Auth/RLS, public RPC/Data API, or pgvector:

- `auth.*`, custom Yandex/Supabase identity state;
- active `event_search_documents` and `event_embeddings` for current authorized search/semantic related;
- `search_quota_plans` and active `user_search_quota_ledger`;
- `personalization_event_reaction_counter` aggregate: one row per event, public narrow read columns;
- user notification profiles/follows/active suppressions if email feature ships;
- short operational window for search requests/feedback and email outbox while workers process them.

### Send away from Supabase

To keep under 450 MB:

1. **Do not write weak raw telemetry into Supabase by default.** Page views, impressions, dwell, card-visible events, served-list arrays and scroll/click debug should go to YDB or stay local/session summary.
2. **Archive search audit:** move old `event_search_requests` and `event_search_feedback` to YDB after 7–30 days, keeping compact per-day counters in Supabase if needed.
3. **Bound pgvector working set:** keep only active/future event embeddings and compact document snapshots; remove inactive/expired event vectors after static pages no longer need live search.
4. **Email history TTL:** keep only pending/recent outbox and current suppressions in Supabase; project `email_delivery_events` and old terminal `email_outbox` rows to YDB.
5. **Use same-origin static manifests:** reaction counters, discovery payloads and public card data should be emitted as Yandex Object Storage/CDN JSON, not fetched row-by-row from Supabase by every page view.
6. **Add a DB-size kill switch:** at 430 MB disable raw/audit inserts first; at 450 MB fail closed to local-only telemetry and keep only essential auth/search/counter writes.

## Current SQLite state and “thin SQLite” strategy

### Live production SQLite snapshot

Read-only Fly probe on 2026-07-02:

- `PRAGMA quick_check`: `ok`.
- DB size: `198.0 MiB`.
- Freelist: `0.01 MiB`.
- Selected row counts:

| Table | Rows |
|---|---:|
| `event` | 6206 |
| `eventposter` | 12144 |
| `event_source` | 11183 |
| `event_source_fact` | 87718 |
| `telegram_scanned_message` | 3852 |
| `telegram_post_metric` | 2600 |
| `vk_post_metric` | 4728 |
| `joboutbox` | 26947 |
| `ops_run` | 3186 |
| `posterocrcache` | 7268 |
| `vk_inbox` | 9558 |
| `videoannounce_session` | 795 |
| `videoannounce_item` | 6754 |
| `festival_queue` | 979 |
| `ticket_site_queue` | 257 |

Top relation sizes:

| Relation | Size |
|---|---:|
| `event` | 55.97 MiB |
| `vk_inbox` | 21.82 MiB |
| `event_source` | 17.05 MiB |
| `ops_run` | 12.95 MiB |
| `event_source_fact` | 11.41 MiB |
| `videoannounce_session` | 8.18 MiB |
| `kaggle_run_event` | 5.94 MiB |
| `eventposter` | 5.08 MiB |
| `joboutbox` | 4.10 MiB |
| `guide_fact_claim` | 2.98 MiB |
| `festival_queue` | 2.93 MiB |
| `posterocrcache` | 2.79 MiB |
| `videoannounce_item` | 2.41 MiB |
| `videoannounce_llm_trace` | 2.07 MiB |

Backup-like objects:

- `codex_backup_*`: 128 objects, about `11.31 MiB`.
- `incident_*` backup/incident-like: 34 objects, about `1.79 MiB`.

### Keep in SQLite

Keep hot canonical/control-plane data:

- `event` and active lifecycle/publication fields;
- `event_source`, active `event_source_fact`, `eventposter`, `event_media_asset`;
- `joboutbox` and scheduler recovery state;
- active `telegram_source`, `telegram_scanned_message`, `vk_source`, `vk_crawl_cursor`, `vk_inbox`, `festival_queue`, `ticket_site_queue`;
- active page state (`monthpage*`, `weekendpage`, `weekpage`, `tomorrowpage`);
- active promo/video campaign/session state where caps and anti-repeat need local joins;
- short hot windows of post metrics and ops/Kaggle status required by admin commands/incidents.

### Move/archive from SQLite

Priority order:

1. **Stop accumulating `codex_backup_*` tables inside production DB.** Export repair backups to `artifacts/`/Object Storage or a YDB audit table, then drop after verified incident closure.
2. **Archive append-only operational events:** `kaggle_run_event`, terminal/old `ops_run`, old `videoannounce_llm_trace`, old `promo_exposure`.
3. **Mirror/roll up post metrics:** `telegram_post_metric`, `vk_post_metric` can keep a SQLite hot window plus YDB historical samples/daily aggregates.
4. **Retention on `vk_inbox` and scanned messages:** keep pending/recent/import mappings locally; archive raw processed text after a policy window.
5. **Cache TTL:** `posterocrcache`, `telegraph_preview_probe`, page section cache and OCR usage should have explicit retention/rollups.
6. **Event-source facts:** keep current/public facts locally; archive older/source-iteration facts after they stop participating in Smart Update/operator UI.

## Static site analysis

### Current data flow

- Static export reads Fly SQLite events/posters/sources/metrics and emits `site/src/data/preview-events.json`, `site/src/data/preview-related.json`, plus built pages/JSON.
- Astro uses `site/src/lib/events.ts` to compute listings, related/discovery payloads, popularity fields and card snapshots.
- `/data/discovery/<event_id>.json` is a static same-origin discovery payload.
- Built static HTML/JS/CSS/JSON/media are deployed to Yandex Object Storage/CDN.
- Anonymous personalization is mostly local-first in browser `localStorage` (`ke_personalization_profile`, feedback logs, listing mode/cache, search feedback queue).
- Current production Supabase sidecar provides reaction counters and authorized search/pgvector; planned raw personalization tables in docs are not all applied.

### What should go to YDB from static site

Good candidates:

- `site_telemetry_event_v1`: sampled/filtered page views, valid impressions, ticket clicks, detail views, dwell checkpoints, not-interested/like/share events.
- `served_list_summary_v1`: one compact row per rendered feed/list, with hash, event ids, scores/reasons, surface and session metadata.
- `session_summary_v1`: one compact row per session/day with interest deltas and counters.
- `search_audit_v1`: old search request/feedback audit, query hashes or privacy-safe normalized query tokens, response counts, latency, model path.
- `reaction_action_log_v1`: strong explicit actions only; not every UI state change.
- `counter_manifest_audit_v1`: generated reaction-counter manifests and build ids, not the manifest blob itself.

Keep out of YDB/Supabase DB:

- static HTML/CSS/JS;
- event page JSON/discovery blobs;
- image/media assets;
- full `preview-events.json` blobs except as Object Storage artifacts.

### Static-site guardrails

- Static site must remain useful with Supabase/YDB unavailable.
- Browser writes should never use secrets; use Edge/Fly ingest or signed anonymous token/RPC.
- Batch telemetry locally and send compact summaries; avoid row-per-scroll/impression firehose.
- All anonymous ids should be scoped/rotated and HMACed before YDB if they leave the browser.

## Bot/event lifecycle analysis

### Current event path

1. Source parsers / Telegram monitoring / VK intake create candidates.
2. `smart_event_update.smart_event_update(...)` creates/merges `event`, `event_source`, `event_source_fact`, poster rows, ticket/festival queue rows.
3. `schedule_event_update_tasks(...)` enqueues `joboutbox` tasks: Telegraph, ICS, Telegram ICS post, month/weekend/festival pages, static-site build, VK sync.
4. `job_outbox_worker` executes local handlers and writes result URLs/errors/backoff to SQLite.
5. Publication state lands back on `event`: Telegraph URL/path/hash, ICS URLs/hashes, VK URL/hash, short-link fields.
6. Metrics/history tables accumulate post metrics, ops runs, Kaggle callbacks, promo/video events.

### Do not move first

- `event`, `event_source`, active `event_source_fact`, `joboutbox`, active source queues and publication fields.
- Moving these to YDB first would require reimplementing coalescing keys, stale-running recovery, dependency checks, local joins, uniqueness and transactional semantics.

### Move/mirror first

- best-effort YDB projections after the SQLite commit succeeds;
- old/terminal history rows;
- daily/hourly rollups for reports;
- static/reporting reads can optionally read YDB later with SQLite fallback.

## Statistics candidates

| Candidate | Current | Proposed YDB role | SQLite/Supabase retained role |
|---|---|---|---|
| `telegram_post_metric`, `vk_post_metric` | SQLite | append samples + daily aggregates + TTL | hot recent samples, source mapping joins |
| `reaction_counter_sync` outputs | Supabase aggregate | raw action/archive, manifest audit | `personalization_event_reaction_counter` current aggregate |
| `ops_run` | SQLite | terminal run history + metrics JSON archive | active/recent incident/debug rows |
| `kaggle_run_event` | SQLite | append event stream | `kaggle_run_ledger` current status; leases stay SQLite initially |
| `videoannounce_llm_trace` | SQLite | terminal traces after retention | recent debug traces |
| `promo_exposure` | SQLite | terminal exposure history | current campaign cap rollups |
| `general_stats` snapshots | computed from SQLite/Supabase/storage | persisted daily summaries | current source reads |
| source parser run stats | SQLite/admin messages | aggregate by source/day | active parse status |

## Mail and notification candidates

### Current/planned state

The current local dirty branch has a foundation for transactional email reminders via Supabase Edge Function + Postbox + YDB stats contract, but production sending is gated/dry-run. Key gap found by the read-only lane: docs describe static-site consent UI and Yandex userinfo adapter that were not fully present/wired in the scanned code.

### Keep in Supabase

- `user_notification_profiles`: user email/consent/unsubscribe state tied to Supabase Auth.
- `event_follows`: user-event follow state and current event snapshot for UI/unsubscribe.
- active `email_outbox` rows until the worker claims/sends them.
- active `email_suppressions` for worker checks/unsubscribe UI.

### Send/project to YDB

- `email_delivery_event_v1`: every queued/sent/failed/bounced/complained/suppressed/skipped transition.
- terminal `email_outbox_archive_v1`: payload metadata only, not full rendered body forever.
- `email_rate_limit_bucket_v1`: historical bucket counters with TTL.
- `email_daily_stats_v1`: by kind/status/provider/domain/event/day.
- provider webhook event raw envelope only if sanitized and TTL-bound.

### P0 before real email sending

- Keep `POSTBOX_DRY_RUN=1` until a real YDB write smoke succeeds and a worker drain path exists.
- Make the Edge Function fail closed if it is pointed at the legacy Supabase project instead of personalization Supabase.
- Validate event data server-side by `event_id`; do not trust client-supplied title/date/source URL for email payloads.
- Use keyed HMAC for recipient hashes; minimize plaintext email copies.
- Add bounce/complaint ingestion and unsubscribe flow.

## Recommended YDB table families

Use separate table families/prefixes so ownership and TTL are obvious:

### `analytics/site_events_v1`

Purpose: filtered static-site interaction events.

Suggested key shape:

- `day Date`
- `shard Uint32` — hash prefix to avoid hot partitions
- `event_ts Timestamp`
- `event_uid Utf8` — idempotency/client event id

Columns: `anon_hash`, `session_hash`, `surface`, `action`, `event_id`, `position`, `source`, compact JSON metadata, `created_at`.

Retention: raw 14–30 days; daily rollups 12–24 months.

### `analytics/served_list_summary_v1`

Key: `day`, `shard`, `served_list_id`.

Columns: surface, list hash, compact event ids/ranks/reason masks, anon/session hashes, generated_at.

Retention: 30–90 days raw; rollups longer.

### `analytics/search_audit_v1`

Key: `day`, `user_or_anon_hash`, `request_id`.

Columns: query hash/normalized safe query, result count, latency, embedding model, fallback path, feedback status.

Retention: raw 30–90 days; query text should be minimized or avoided.

### `analytics/email_delivery_events_v1`

Key: `day`, `shard`, `delivery_event_id`.

Columns: event_id, user hash, recipient HMAC, kind, status, provider_message_id, dry_run, error class, metadata.

Retention: raw 90–180 days; daily aggregates longer.

### `analytics/post_metric_samples_v1`

Key: `source_kind`, `source_id`, `message_or_post_id`, `age_day`, `collected_ts` or `day/shard` depending query pattern.

Columns: views, likes, reactions JSON compacted, comments/reposts when available, source URL, event_id if known.

Retention: raw 90–180 days; per-event/day aggregates longer.

### `ops/kaggle_run_events_v1` and `ops/ops_run_history_v1`

Purpose: incident/debug history without bloating SQLite.

Retention: raw 90–180 days, or longer if compact enough.

## Implementation plan

### Phase 0 — ownership and guardrails

- Add a canonical DB ownership matrix for every SQLite/Supabase/YDB table.
- Add size probes to scheduled/general stats:
  - SQLite file size/top tables;
  - Supabase `pg_database_size` and top relations;
  - YDB RU/storage metrics once enabled.
- Add Supabase kill switches:
  - disable raw/audit writes at 430 MB;
  - hard fail closed at 450 MB except essential auth/search/counter paths.
- Add policy: production repair/incident backup tables must not remain in `/data/db.sqlite` long-term.

### Phase 1 — YDB write-only projections

- Add official `ydb` SDK dependency and a small `ydb_projection` package.
- Implement idempotent write helpers with:
  - environment gates;
  - RU throttling/backoff;
  - bounded payload size;
  - no secret/browser exposure;
  - metrics/logging and fail-open behavior for non-critical projections.
- First writers:
  - email delivery events;
  - post metric samples;
  - Kaggle run events;
  - terminal ops_run archive.

### Phase 2 — retention and backfill

- Backfill old SQLite history to YDB in bounded batches.
- After verification, prune SQLite old rows/backup tables.
- Add object-storage exports for bulky incident backups and full JSON artifacts.
- Keep local hot windows and rollups for current admin commands.

### Phase 3 — reporting reads

- Optional: allow general stats/admin reports to read YDB rollups with SQLite fallback.
- Static-site builds may consume precomputed YDB/Supabase summaries only through server-side export scripts, not browser direct YDB.

### Phase 4 — reconsider queues only after maturity

- Only after YDB projections are stable, evaluate whether any queue moves to YDB.
- Do not move `joboutbox` or email active queue until lease/claim/backoff/dead-letter/idempotency semantics have production tests.

## Immediate action list

P0:

1. Create DB ownership matrix and enforce it in docs/tests.
2. Add Supabase 430/450 MB guard and report in `general_stats`.
3. Stop leaving `codex_backup_*` tables in production SQLite; create export/drop runbook.
4. Implement real YDB stats sink for email before Postbox real sends.
5. Keep static-site raw telemetry out of Supabase; batch to local/session summaries until YDB ingest exists.

P1:

1. Mirror TG/VK post metrics to YDB.
2. Archive old `ops_run`, `kaggle_run_event`, `videoannounce_llm_trace`, `promo_exposure`.
3. Define retention windows for search audit/feedback and move old rows to YDB.
4. Generate same-origin counter manifests instead of per-view Supabase reads.

P2:

1. Add YDB-based daily rollups for product analytics.
2. Cut over selected reports to YDB rollups with SQLite fallback.
3. Reassess pgvector footprint and expiration of inactive event embeddings.

## Open questions for external consultant

1. Is YDB Serverless the right first destination for this telemetry, or should any slice use Yandex Data Streams / ClickHouse-style analytics instead?
2. Are the proposed YDB primary keys sufficiently balanced for our expected static-site traffic and bot schedules?
3. Should email delivery events be a YDB table, YDB topic, or both?
4. What minimal YDB Terraform/schema should we add before application code?
5. What RU throttling defaults should be used to prevent runaway cost during client telemetry bugs?
6. Which queries/reports should remain in Supabase/SQLite to avoid YDB read complexity?
7. What retention windows satisfy product/debug needs while staying below Supabase 450 MB and keeping SQLite thin?

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---:|---|
| R01 | Current YDB saves? | Done | No active YDB write path; only local dirty email YDB stub/contract. |
| R02 | Analytics candidates for YDB | Done | Static-site telemetry, served-list/session summaries, search audit, reaction logs. |
| R03 | Event-work candidates for YDB | Done | Only projections/archive; canonical event/source/outbox stay SQLite. |
| R04 | Statistics candidates for YDB | Done | Post metrics, ops/Kaggle, video/promo, general stat rollups. |
| R05 | Mail/notification candidates for YDB | Done | Delivery events, terminal outbox archive, rate ledgers, aggregates. |
| R06 | Thin SQLite strategy | Done | Keep canonical hot state; archive/drop history/backups/caches. |
| R07 | Supabase <450 MB strategy | Done | Keep auth/current/vector/counters; archive raw/history; add kill switches. |
| R08 | Static-site analysis | Done | Data flow, localStorage, Supabase sidecar and YDB candidates mapped. |
| R09 | Bot analysis | Done | Smart Update, JobOutbox, publication, metrics and history mapped. |
| R10 | Branch/link for consultant | Done when branch is pushed | See final handoff message. |
