# Personalization Database Design

> **Status:** full personalization telemetry schema remains design/draft; public reaction counters applied 2026-06-27; authorized pgvector search tables/RPCs applied 2026-06-28; two-document `search_v3`/`related_v1` pgvector split applied 2026-06-30; compact PWA lifecycle aggregates applied 2026-07-27
> **Target DB:** separate Supabase/Postgres personalization project (`PERSONALIZATION_*`)
> **Do not store here:** canonical events, source facts, Telegram/VK/Telegraph state, Smart Update decisions or static rebuild queues.

## Current capacity snapshot

As of 2026-06-30 after the two-document vector backfill, the separate personalization Supabase database is still comfortably inside the free-tier budget:

- `pg_database_size(current_database())`: about `25 MB`;
- approximate free space vs 500 MB decimal free tier: about `473 MB`;
- PostgreSQL: `17.6`;
- installed extensions include `pgcrypto`, `uuid-ossp`, `pg_stat_statements` and `pgvector`;
- largest relations: `event_embeddings≈9.4 MiB`, `event_search_documents≈4.1 MiB`;
- vectors present in the current active/future sidecar: about `404` `search_v3` rows and `343` `related_v1` rows. Ordinary page views still use static manifests and do not call Supabase vector search.

## Architectural gate constraints

This schema is not a raw telemetry sink. It is shaped by `production-integration.md`:

- browser `anon_id` and `session_id` are UUID-compatible when SQL columns are `uuid`; legacy prefixed ids are rejected/reset by the client before trusted telemetry;
- browser payloads are never inserted directly as DB rows; the selected write path validates, annotates, compacts and may drop/quarantine before insert;
- accepted served-list rows target `<2 KB`; full reason-code JSON is sampled/debug-only;
- direct browser -> Supabase table writes are forbidden in production;
- browser -> Supabase RPC ingest is allowed only as `supabase_rpc_ingest_v1` with explicit execute grants, quota/dedupe/storage guards and no raw JSON persistence;
- retention/aggregation jobs run from backend/worker/SQL jobs, not from the web page.

## Supabase constraints that shape the schema

- New Supabase projects no longer expose new `public` tables to Data API automatically; explicit `GRANT` is required for `anon`/`authenticated` roles. RLS and grants are separate layers.
- Any table exposed to Data API must have RLS enabled.
- Browser code may use only `PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`; secret/service/direct DB credentials stay backend-only.
- Public RPCs are not a shortcut around table security. If `supabase_rpc_ingest_v1` is selected, keep the function append-only/minimal, revoke default `PUBLIC` execute, grant only the specific ingest function to `anon`, set a fixed `search_path` for `SECURITY DEFINER`, and add abuse/rate/storage controls.

Official references:

- Supabase changelog, Data API grants breaking change: <https://supabase.com/changelog/45329-breaking-change-tables-not-exposed-to-data-and-graphql-api-automatically>
- Securing Data API: <https://supabase.com/docs/guides/api/securing-your-api>
- RLS guide: <https://supabase.com/docs/guides/database/postgres/row-level-security>
- API keys: <https://supabase.com/docs/guides/getting-started/api-keys>
- pgvector: <https://supabase.com/docs/guides/database/extensions/pgvector>
- Database functions, function privileges and `security definer`/`search_path`: <https://supabase.com/docs/guides/database/functions>

## Applied compact PWA lifecycle analytics

PWA installation/use analytics is intentionally separate from the broader
personalization event design. It does **not** create an append-only row for
every opening:

- `personalization.pwa_installation_state` — one mutable row per random
  browser installation UUID. It keeps only confirmed-install date, first/last
  standalone date, last session UUID, active-day count and D1/D7 idempotency
  flags;
- `personalization.pwa_daily_metric` — one row per Kaliningrad calendar day
  with confirmed installs, standalone sessions, unique active installations,
  first standalone launches and exact D1/D7 cohort returns;
- `personalization.pwa_telemetry_maintenance` — one singleton row used to claim
  the daily self-prune;
- `public.record_pwa_lifecycle_v1(uuid, uuid, text)` — the only browser-facing
  write surface. Raw tables have RLS enabled and no `anon`/`authenticated`
  grants or policies.

Storage/privacy boundaries:

- no page-view/open event journal, fingerprint, contact id, URL, referrer,
  user-agent or IP is persisted in application tables;
- browser automation (`navigator.webdriver`) does not send metrics;
- if durable browser storage is blocked, the client drops the datapoint instead
  of generating a new undeduplicated id on every load;
- installation state older than 180 inactive days is deleted opportunistically
  once per day. Daily aggregates remain compact indefinitely (365 rows/year);
- the public RPC accepts at most 1,000 previously unseen installation UUIDs per
  day, limiting anonymous storage amplification;
- a reinstall after retained browser storage is counted as the same browser
  installation. Uninstall cannot be observed reliably by the web platform.

Operator queries use the direct/backend connection, never the publishable
browser key:

```sql
-- Daily product metrics and cohort retention.
select
  metric_date,
  confirmed_installs,
  standalone_sessions,
  active_installations,
  first_standalone_launches,
  round(100.0 * cohort_d1_returns / nullif(confirmed_installs, 0), 1) as d1_percent,
  round(100.0 * cohort_d7_returns / nullif(confirmed_installs, 0), 1) as d7_percent
from personalization.pwa_daily_metric
order by metric_date desc
limit 31;

-- Current observed active installation ids (not an uninstall counter).
select
  count(*) filter (where last_active_on >=
    (now() at time zone 'Europe/Kaliningrad')::date - 6) as active_7d,
  count(*) filter (where last_active_on >=
    (now() at time zone 'Europe/Kaliningrad')::date - 29) as active_30d
from personalization.pwa_installation_state;
```

The client/runtime contract is documented in
`docs/features/static-site-pages/mobile-shell.md`.

## Data ownership boundary

### Public/exposed table in `public`

The MVP write path should be **compact summaries first**, not a raw event firehose:

- `public.event_search_documents` / `public.event_embeddings` — **applied 2026-06-28, hardened 2026-06-30** closed Supabase pgvector sidecar for authorized search and semantic retrieval canary. Browser roles have no direct table SELECT; authenticated search goes through `event-search` Edge Function and RPC `search_events_by_embedding_v1`. Stores compact `search_digest` for `search_v3`, cleaner `related_digest` for `related_v1`, controlled facets, trusted `card_snapshot` and `vector(768)` embeddings (`gemini-embedding-2`, keyed by `embedding_doc_kind`), not raw OCR/source text.
- `public.search_quota_plans`, `public.user_search_quota_ledger`, `public.event_search_requests` — **applied 2026-06-28** quota/audit for authorized smart search. Raw queries are not stored: only hash, length, status and counts.
- `public.personalization_event_reaction_counter` — **applied 2026-06-27** compact public aggregate for event-card counters. It stores `source_likes_count` (TG/VK/source-post metrics imported by backend) and `service_likes_count` (first-party KenigEvents likes) in the same row, with generated `likes_count = source_likes_count + service_likes_count`. Browser/Data API roles may select only `event_id`, `likes_count`, `not_interested_count`, `share_count`, `updated_at`; source/service split, source views and metadata remain backend-only. UI copy must show only total counts. Long-lived first-party reactions should be folded from a bounded state table, not from an infinite raw log.
- `public.personalization_session_summary` — compact session/profile deltas after consent;
- `public.personalization_served_list_summary` — compact exposure context per feed/list/module chunk for future ranker labels;
- `public.personalization_interaction_event` — optional short-retention/debug/sampled strong raw actions, not the default path for every impression.

Preferred production path: same-origin endpoint validates/rate-limits payload, classifies `actor_class`/trust, maps the client contract to the server row contract, and inserts with service-role/direct DB credentials or calls a private DB RPC. Allowed lightweight path: browser calls only a dedicated append-only `supabase_rpc_ingest_v1` function that performs the same validation/compaction/dedupe/quota checks inside Postgres. Direct browser table insert/update/select with the Supabase publishable key is forbidden for telemetry/profile tables in production. Exposed telemetry tables have RLS and **no public SELECT**; the only current exception is the deliberately narrow aggregate counter `public.personalization_event_reaction_counter`, where anon/authenticated may select only total public columns. For MVP-0 `event_detail_related`, store `surface='event_detail_related'`, `layout_mode='module'`; `presentation_mode` and `current_event_id` are dedicated columns on served-list summaries and metadata fields elsewhere. Crawler/preview/monitor/bot-like payloads are dropped or quarantined and never feed profiles/training; see `bots-and-automation.md`.

### Private schema `personalization`

All derived and debug data lives outside exposed schemas:

- `personalization.anonymous_visitor` — first/last seen, consent version, coarse profile status;
- `personalization.anonymous_session` — session rollups built from compact summaries;
- `personalization.event_feature_snapshot` — event ranking features exported from Fly SQLite + offline enrichment;
- `personalization.visitor_profile_snapshot` — compact session/short/mid/long profile horizons, each with positive and negative vectors/maps;
- `personalization.recommendation_request` and `recommendation_result` — debug/eval evidence;
- `personalization.telemetry_quarantine` — compact rejected/suspicious payload evidence with `actor_class`, reason and size/hash only; no raw source texts;
- served-list summaries are kept in `public` only as append-only compact telemetry; backend jobs can fold them into private training/eval tables later;
- `personalization.daily_interaction_aggregate` — retention-safe analytics;
- `personalization.e2e_persona` — synthetic test personas and expected top-k.

The static site reads event catalog/recommendation manifests from same-origin JSON first. Supabase is not the default mass-read path.

## Retention policy

Default MVP retention and storage policy is intentionally stricter than the earlier broad plan because the Supabase free-tier budget is small:

| Data | Retention | Storage shape / reason |
| --- | ---: | --- |
| Raw weak `interaction_event` impressions/skips | off | avoid filling 500 MB with scroll noise |
| Raw strong actions (`ticket_click`, `hide`, `detail_view`, `share`) | 30-90 days | meaningful labels only; still compact |
| `personalization_session_summary` | 30-90 days full, then fold into profile/aggregate | compact profile signal without raw volume |
| `personalization_served_list_summary` | 14-30 days full, then daily aggregate | exposure context; accepted row target `<2 KB` |
| `daily_interaction_aggregate` | 12 months | product metrics without raw event volume |
| `visitor_profile_snapshot` | 365 days since last seen, compact only | returning visitor personalization |
| `recommendation_request/result` debug | 14-30 days | E2E/debug only |
| `telemetry_quarantine` compact evidence | 7-14 days | abuse/debug; excluded from training and metrics |
| `event_feature_snapshot` | while event is future + static retention window | ranking features tied to event lifecycle |

Retention should run from backend/cron/SQL jobs with direct DB credentials, not from browser. Raw telemetry must be deleted or aggregated before it can threaten the free-tier cap.

## Client payload -> accepted row mapping

The browser client may emit a convenient JSON payload for local/demo use, but the selected production write path maps it to compact SQL fields. For `same_origin_endpoint_v1` the mapping happens on Fly before DB insert/RPC. For `supabase_rpc_ingest_v1` the mapping happens inside the dedicated Postgres function before any accepted insert:

| Client field | Server handling |
| --- | --- |
| `anon_id`, `session_id` | must be UUIDs when SQL columns are `uuid`; invalid values drop/quarantine before DB |
| `served_list_id`, `served_list_hash` | opaque text ids; dedupe key with `(anon_id, served_list_id)` / repeated hash limits |
| `shown[].event_id` | `shown_event_ids bigint[]`, max 24 for `event_detail_related` MVP |
| `shown[].rank` | `shown_ranks smallint[]` |
| `shown[].personal_score/base_similarity` | compact `shown_score_0_1000 smallint[]` after normalization |
| `shown[].reason_codes` | `shown_reason_mask integer[]`; full reason JSON only when `debug_sample=true` |
| promo flags | `promo_event_ids bigint[]` and metadata flags; promo rows never train as organic interest by default |
| `presentation_mode`, `current_event_id` | dedicated columns on served-list summary |
| `consent_version`, `actor_class`, `trust_state`, `requested_at` | server-attached/validated, not trusted from browser alone |

## Index strategy

MVP indexes are write-friendly:

- compact summaries by `(anon_id, ended_at desc)` for profile aggregation;
- served-list summaries by `(anon_id, requested_at desc)` and `(session_id, requested_at desc)` for label building;
- raw telemetry by `occurred_at` only if debug/sampling is enabled;
- raw telemetry by `(anon_id, occurred_at desc)` only for short-retention profile/debug windows;
- raw telemetry by `(event_id, occurred_at desc)` only for sampled event analytics;
- partial index for strong actions: `ticket_click`, `share`, `copy_link`, `event_detail_view`, `hide_event`;
- GIN indexes on small JSONB feature maps only after query patterns are real;
- pgvector/HNSW only if server-side vector search is chosen after eval.

Avoid indexes on every JSON field before usage is known. Compact summaries/profile snapshots should dominate storage; raw telemetry volume must remain bounded and disposable.

## Abuse and rate-limit policy

RLS, grants and insert/RPC policies do not stop abuse by themselves. Before production
public writes, one of these modes must be explicitly selected:

1. **Default production mode:** same-origin endpoint
   `/api/personalization/summary` validates payloads, rate-limits by IP/session,
   and inserts with service-role/direct DB credentials or calls a private DB RPC.
   The browser never sees a secret key.
2. **Allowed lightweight mode:** browser calls only
   `public.ingest_personalization_summary_v1(...)` through Supabase RPC with the
   publishable key. The function is the backend: it validates, compacts, dedupes,
   enforces quota/storage caps, and writes only compact accepted rows or tiny
   quarantine evidence.

No mode allows direct browser table INSERT/UPDATE/SELECT, raw payload -> DB row,
or public SELECT by `anon_id` in the anonymous MVP. Server profile
snapshots are analytics/eval/post-MVP server-ranker evidence until a safe
same-origin recommendation endpoint exists. Accepted payloads must carry or be
annotated with `actor_class` (`human_likely`, `unknown`, `preview_bot`,
`crawler_verified`, `monitor`, `bot_likely`, `automation_suspected`) and
`trust_state` (`accepted`, `quarantined`, `dropped`, `diagnostic`). Only
`accepted` human/unknown consented summaries can update profiles or training
sets.

## Optional Supabase RPC ingest contract

`supabase_rpc_ingest_v1` is allowed only as a consciously selected write path, not as a generic public API. It is effectively a tiny backend running in Postgres.

Function contract options:

- preferred: typed parameters (`uuid`, `text`, `bigint[]`, `smallint[]`, `integer[]`) so PostgREST/Postgres rejects malformed shapes early;
- acceptable: one `jsonb` payload only if the function immediately validates and maps it to typed local variables, drops unknown fields and never persists raw JSON as an accepted row.

Minimum RPC gates:

- revoke execute on all functions in exposed schemas from `PUBLIC`, `anon`, `authenticated`; grant execute only on the specific ingest function to `anon`;
- keep telemetry/profile tables closed to `anon`/`authenticated`; the function writes with tightly reviewed privileges;
- use `security invoker` when possible; if `security definer` is necessary to insert into closed tables, set a fixed `search_path` and explicitly schema-qualify relations;
- ignore client `actor_class`, `trust_state`, `training_eligible`, quota/debug flags and server timestamps;
- enforce payload size, shown list length, enum/version checks, dedupe by `served_list_hash`/`client_summary_id`, per-anon/session time-bucket quota and emergency storage caps before accepted insert;
- return void/minimal success only; no profiles, recommendations or debug details.

## Draft SQL

The SQL below is a design artifact. Apply only after review against the current Supabase project settings.

```sql
-- Optional only if server-side vector search is selected later:
-- create extension if not exists vector with schema extensions;

create schema if not exists personalization;

-- Compact session summary. This is the MVP default telemetry payload.
create table if not exists public.personalization_session_summary (
  id uuid primary key default gen_random_uuid(),
  client_summary_id text not null,
  anon_id uuid not null,
  session_id uuid not null,
  started_at timestamptz not null,
  ended_at timestamptz not null,
  received_at timestamptz not null default now(),
  viewport_class text not null,
  layout_mode text not null,
  primary_surface text not null,
  algorithm_id text not null default 'local_rerank_v1',
  consent_version text not null,
  consent_state text not null default 'accepted',
  actor_class text not null default 'unknown',
  trust_state text not null default 'accepted',
  event_counts jsonb not null default '{}'::jsonb,
  positive_tag_delta jsonb not null default '{}'::jsonb,
  negative_interest_tag_delta jsonb not null default '{}'::jsonb,
  strong_event_ids jsonb not null default '{}'::jsonb,
  seen_event_ids_sample bigint[] not null default '{}',
  hidden_event_ids bigint[] not null default '{}',
  profile_delta_vector jsonb not null default '[]'::jsonb,
  client_summary_version text not null,
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_session_summary_consent_chk check (consent_state = 'accepted'),
  constraint personalization_session_summary_trust_chk check (trust_state = 'accepted'),
  constraint personalization_session_summary_actor_chk check (actor_class in ('human_likely', 'unknown')),
  constraint personalization_session_summary_time_chk check (ended_at >= started_at),
  constraint personalization_session_summary_payload_chk check (
    length(event_counts::text) <= 2048
    and length(positive_tag_delta::text) <= 4096
    and length(negative_interest_tag_delta::text) <= 4096
    and length(strong_event_ids::text) <= 4096
    and cardinality(seen_event_ids_sample) <= 200
    and cardinality(hidden_event_ids) <= 200
    and length(profile_delta_vector::text) <= 8192
    and length(metadata::text) <= 2048
  ),
  unique (anon_id, client_summary_id)
);

alter table public.personalization_session_summary enable row level security;
revoke all on public.personalization_session_summary from anon, authenticated;
grant select, insert, update, delete on public.personalization_session_summary to service_role;

-- Production writes come from the same-origin backend/service role or from a
-- dedicated ingest RPC. Do not grant direct table INSERT to anon/authenticated.

create index if not exists personalization_session_summary_anon_time_idx
  on public.personalization_session_summary (anon_id, ended_at desc);
create index if not exists personalization_session_summary_received_idx
  on public.personalization_session_summary (received_at desc);

-- Compact exposure context for future learning-to-rank labels. No public SELECT.
-- The endpoint maps client `shown[]` JSON into arrays/bitmasks before insert.
create table if not exists public.personalization_served_list_summary (
  id uuid primary key default gen_random_uuid(),
  served_list_id text not null,
  served_list_hash text,
  anon_id uuid not null,
  session_id uuid not null,
  requested_at timestamptz not null,
  received_at timestamptz not null default now(),
  viewport_class text not null,
  layout_mode text not null,
  presentation_mode text,
  surface text not null,
  current_event_id bigint,
  algorithm_id text not null default 'local_rerank_v1',
  event_pool_hash text,
  shown_event_ids bigint[] not null default '{}',
  shown_ranks smallint[] not null default '{}',
  shown_score_0_1000 smallint[] not null default '{}',
  shown_reason_mask integer[] not null default '{}',
  promo_event_ids bigint[] not null default '{}',
  debug_sample boolean not null default false,
  debug_shown jsonb not null default '[]'::jsonb,
  consent_version text not null,
  consent_state text not null default 'accepted',
  actor_class text not null default 'unknown',
  trust_state text not null default 'accepted',
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_served_list_consent_chk check (consent_state = 'accepted'),
  constraint personalization_served_list_trust_chk check (trust_state = 'accepted'),
  constraint personalization_served_list_actor_chk check (actor_class in ('human_likely', 'unknown')),
  constraint personalization_served_list_viewport_chk check (viewport_class in ('mobile', 'tablet', 'desktop')),
  constraint personalization_served_list_layout_chk check (layout_mode in ('feed', 'grid', 'list', 'module', 'detail')),
  constraint personalization_served_list_cardinality_chk check (
    cardinality(shown_event_ids) <= 100
    and cardinality(shown_event_ids) = cardinality(shown_ranks)
    and cardinality(shown_event_ids) = cardinality(shown_score_0_1000)
    and cardinality(shown_event_ids) = cardinality(shown_reason_mask)
    and cardinality(promo_event_ids) <= 20
    and jsonb_typeof(debug_shown) = 'array'
    and (debug_sample or jsonb_array_length(debug_shown) = 0)
    and length(debug_shown::text) <= 8192
    and length(metadata::text) <= 1024
  ),
  unique (anon_id, served_list_id)
);

alter table public.personalization_served_list_summary enable row level security;
revoke all on public.personalization_served_list_summary from anon, authenticated;
grant select, insert, update, delete on public.personalization_served_list_summary to service_role;

-- Production writes come from the same-origin backend/service role or from a
-- dedicated ingest RPC. Do not grant direct table INSERT to anon/authenticated.

create index if not exists personalization_served_list_anon_time_idx
  on public.personalization_served_list_summary (anon_id, requested_at desc);
create index if not exists personalization_served_list_session_time_idx
  on public.personalization_served_list_summary (session_id, requested_at desc);
create index if not exists personalization_served_list_received_idx
  on public.personalization_served_list_summary (received_at desc);

-- Optional RPC ingest shape. Keep table grants closed; grant only function execute.
-- Review and harden before applying; this is a contract sketch, not final SQL.
-- revoke execute on all functions in schema public from public;
-- revoke execute on all functions in schema public from anon, authenticated;
-- create or replace function public.ingest_personalization_summary_v1(
--   p_anon_id uuid,
--   p_session_id uuid,
--   p_served_list_id text,
--   p_served_list_hash text,
--   p_surface text,
--   p_current_event_id bigint,
--   p_algorithm_id text,
--   p_viewport_class text,
--   p_layout_mode text,
--   p_presentation_mode text,
--   p_shown_event_ids bigint[],
--   p_scores_0_1000 smallint[],
--   p_reason_masks integer[],
--   p_consent_version text,
--   p_client_created_at timestamptz
-- )
-- returns void
-- language plpgsql
-- security definer
-- set search_path = ''
-- as $$
-- begin
--   -- validate versions/enums/cardinality/quota/dedupe; ignore client trust fields;
--   -- insert only compact mapped fields into public.personalization_served_list_summary;
--   -- optionally write tiny public/private quarantine evidence without raw payload.
-- end;
-- $$;
-- revoke execute on function public.ingest_personalization_summary_v1(
--   uuid, uuid, text, text, text, bigint, text, text, text, text,
--   bigint[], smallint[], integer[], text, timestamptz
-- ) from public, authenticated;
-- grant execute on function public.ingest_personalization_summary_v1(
--   uuid, uuid, text, text, text, bigint, text, text, text, text,
--   bigint[], smallint[], integer[], text, timestamptz
-- ) to anon;

-- Compact quarantine evidence. Private schema only; never used for training/profile updates.
create table if not exists personalization.telemetry_quarantine (
  id uuid primary key default gen_random_uuid(),
  received_at timestamptz not null default now(),
  actor_class text not null,
  trust_state text not null default 'quarantined',
  surface text,
  event_kind text,
  reason_code text not null,
  anon_id_hash text,
  session_id_hash text,
  ip_prefix_hash text,
  payload_hash text not null,
  payload_size integer not null,
  metadata jsonb not null default '{}'::jsonb,
  constraint telemetry_quarantine_trust_chk check (trust_state in ('quarantined', 'dropped', 'diagnostic')),
  constraint telemetry_quarantine_size_chk check (payload_size between 0 and 16384 and length(metadata::text) <= 1024)
);

-- Optional browser-facing raw telemetry for short-retention debug/sampling. No public SELECT.
create table if not exists public.personalization_interaction_event (
  id uuid primary key default gen_random_uuid(),
  client_event_id text not null,
  anon_id uuid not null,
  session_id uuid not null,
  event_id bigint,
  event_slug text,
  event_kind text not null,
  occurred_at timestamptz not null default now(),
  received_at timestamptz not null default now(),
  page_url text not null,
  page_referrer text,
  viewport_class text not null,
  layout_mode text not null,
  surface text not null,
  position integer,
  page_cursor text,
  algorithm_id text not null default 'static_fallback',
  consent_version text not null,
  consent_state text not null default 'accepted',
  actor_class text not null default 'unknown',
  trust_state text not null default 'accepted',
  dwell_ms integer,
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_interaction_event_kind_chk check (event_kind in (
    'page_view', 'valid_impression', 'event_card_click', 'event_detail_view',
    'dwell_checkpoint', 'ticket_click', 'register_click', 'source_click',
    'calendar_add', 'ics_download', 'map_click', 'hide_event', 'not_interested',
    'like_event', 'unlike_event', 'undo_not_interested',
    'share', 'share_native', 'copy_link', 'share_copy_link',
    'recommendation_feed_loaded', 'recommendation_fallback_used'
  )),
  constraint personalization_viewport_chk check (viewport_class in ('mobile', 'tablet', 'desktop')),
  constraint personalization_layout_chk check (layout_mode in ('feed', 'grid', 'list', 'module', 'detail')),
  constraint personalization_consent_chk check (consent_state = 'accepted'),
  constraint personalization_interaction_trust_chk check (trust_state = 'accepted'),
  constraint personalization_interaction_actor_chk check (actor_class in ('human_likely', 'unknown')),
  constraint personalization_position_chk check (position is null or position >= 0),
  constraint personalization_dwell_chk check (dwell_ms is null or dwell_ms between 0 and 86400000),
  constraint personalization_metadata_size_chk check (length(metadata::text) <= 2048),
  unique (anon_id, client_event_id)
);

alter table public.personalization_interaction_event enable row level security;

revoke all on public.personalization_interaction_event from anon, authenticated;
-- Raw-event debug writes stay backend/RPC-only. Do not grant direct table INSERT
-- to anon/authenticated.
grant select, insert, update, delete on public.personalization_interaction_event to service_role;

create index if not exists personalization_interaction_event_occurred_idx
  on public.personalization_interaction_event (occurred_at desc);
create index if not exists personalization_interaction_event_anon_time_idx
  on public.personalization_interaction_event (anon_id, occurred_at desc);
create index if not exists personalization_interaction_event_session_time_idx
  on public.personalization_interaction_event (session_id, occurred_at desc);
create index if not exists personalization_interaction_event_event_time_idx
  on public.personalization_interaction_event (event_id, occurred_at desc)
  where event_id is not null;
create index if not exists personalization_interaction_event_strong_actions_idx
  on public.personalization_interaction_event (anon_id, occurred_at desc, event_kind)
  where event_kind in ('event_detail_view', 'ticket_click', 'share', 'copy_link', 'hide_event', 'not_interested', 'like_event', 'unlike_event', 'undo_not_interested');

-- Dedicated compact raw log for explicit feedback. This is intentionally much
-- smaller than generic interaction telemetry and powers per-anonymous-profile
-- reports: how many likes a visitor made, at which timestamps, and which
-- events were marked not interesting. Browser clients must not write this
-- table directly; same-origin endpoint or a hardened append-only RPC maps the
-- client payload to these rows.
create table if not exists public.personalization_event_reaction (
  id uuid primary key default gen_random_uuid(),
  client_event_id text not null,
  anon_id uuid not null,
  session_id uuid not null,
  event_id bigint not null,
  event_slug text,
  reaction_kind text not null,
  occurred_at timestamptz not null,
  received_at timestamptz not null default now(),
  surface text not null,
  layout_mode text not null,
  position integer,
  page_url text not null,
  algorithm_id text not null default 'static_fallback',
  actor_class text not null default 'unknown',
  trust_state text not null default 'accepted',
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_event_reaction_kind_chk check (reaction_kind in ('like_event', 'unlike_event', 'not_interested', 'undo_not_interested')),
  constraint personalization_event_reaction_layout_chk check (layout_mode in ('feed', 'grid', 'list', 'module', 'detail')),
  constraint personalization_event_reaction_actor_chk check (actor_class in ('human_likely', 'unknown')),
  constraint personalization_event_reaction_trust_chk check (trust_state = 'accepted'),
  constraint personalization_event_reaction_position_chk check (position is null or position >= 0),
  constraint personalization_event_reaction_metadata_size_chk check (length(metadata::text) <= 512),
  unique (anon_id, client_event_id)
);

alter table public.personalization_event_reaction enable row level security;

revoke all on public.personalization_event_reaction from anon, authenticated;
grant select, insert, update, delete on public.personalization_event_reaction to service_role;

create index if not exists personalization_event_reaction_anon_time_idx
  on public.personalization_event_reaction (anon_id, occurred_at desc);
create index if not exists personalization_event_reaction_event_kind_idx
  on public.personalization_event_reaction (event_id, reaction_kind, occurred_at desc);

-- Applied minimal public aggregate used by static export/build and optional
-- browser Data API reads to render card counters. Source metrics are imported
-- from production TG/VK/source-post metrics by backend jobs; first-party likes
-- are folded in from trusted reaction rows. UI must show only likes_count.
create table if not exists public.personalization_event_reaction_counter (
  event_id bigint primary key,
  source_likes_count integer not null default 0 check (source_likes_count >= 0),
  service_likes_count integer not null default 0 check (service_likes_count >= 0),
  likes_count integer generated always as (source_likes_count + service_likes_count) stored,
  not_interested_count integer not null default 0 check (not_interested_count >= 0),
  share_count integer not null default 0 check (share_count >= 0),
  source_views_count integer not null default 0 check (source_views_count >= 0),
  source_engagement_sources_count integer not null default 0 check (source_engagement_sources_count >= 0),
  source_refreshed_at timestamptz,
  service_refreshed_at timestamptz,
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_event_reaction_counter_metadata_size_chk check (length(metadata::text) <= 2048)
);

alter table public.personalization_event_reaction_counter enable row level security;
revoke all on public.personalization_event_reaction_counter from anon, authenticated;
grant select(event_id, likes_count, not_interested_count, share_count, updated_at)
  on public.personalization_event_reaction_counter to anon, authenticated;
grant all on public.personalization_event_reaction_counter to service_role;

create policy personalization_event_reaction_counter_public_read
  on public.personalization_event_reaction_counter
  for select
  to anon, authenticated
  using (true);

create index if not exists personalization_event_reaction_counter_updated_idx
  on public.personalization_event_reaction_counter (updated_at desc);

create table if not exists personalization.anonymous_visitor (
  anon_id uuid primary key,
  first_seen_at timestamptz not null,
  last_seen_at timestamptz not null,
  consent_version text,
  profile_state text not null default 'cold',
  profile_updated_at timestamptz,
  profile_quality numeric(5,4) not null default 0,
  metadata jsonb not null default '{}'::jsonb
);

create table if not exists personalization.anonymous_session (
  session_id uuid primary key,
  anon_id uuid not null references personalization.anonymous_visitor(anon_id) on delete cascade,
  started_at timestamptz not null,
  last_seen_at timestamptz not null,
  viewport_class text,
  first_page_url text,
  entry_surface text,
  event_count integer not null default 0,
  strong_action_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb
);

create table if not exists personalization.event_feature_snapshot (
  event_id bigint primary key,
  stable_slug text not null,
  taxonomy_version text not null,
  feature_schema_version text not null,
  source_hash text not null,
  event_date date not null,
  city text,
  event_type text,
  raw_tags text[] not null default '{}',
  normalized_tags text[] not null default '{}',
  unmapped_tags text[] not null default '{}',
  audience_tags text[] not null default '{}',
  audience_exclusion_tags text[] not null default '{}',
  venue_key text,
  is_free boolean,
  ticket_status text,
  popularity_baseline numeric(8,5) not null default 0,
  feature_vector jsonb not null default '[]'::jsonb,
  vector_dim integer,
  embedding_model text,
  embedding_text text,
  -- Optional after pgvector decision:
  -- embedding extensions.vector(1536),
  quality_warnings text[] not null default '{}',
  built_at timestamptz not null default now(),
  expires_at timestamptz
);

create table if not exists personalization.visitor_profile_snapshot (
  id uuid primary key default gen_random_uuid(),
  anon_id uuid not null references personalization.anonymous_visitor(anon_id) on delete cascade,
  profile_version text not null,
  horizon text not null check (horizon in ('session', 'short', 'mid', 'long')),
  positive_vector jsonb not null default '[]'::jsonb,
  negative_interest_vector jsonb not null default '[]'::jsonb,
  positive_tags jsonb not null default '{}'::jsonb,
  negative_interest_tags jsonb not null default '{}'::jsonb,
  city_affinity jsonb not null default '{}'::jsonb,
  venue_affinity jsonb not null default '{}'::jsonb,
  price_preference jsonb not null default '{}'::jsonb,
  time_preference jsonb not null default '{}'::jsonb,
  seen_event_ids bigint[] not null default '{}',
  hidden_event_ids bigint[] not null default '{}',
  built_from_event_count integer not null default 0,
  quality numeric(5,4) not null default 0,
  built_at timestamptz not null default now(),
  expires_at timestamptz,
  unique (anon_id, profile_version, horizon)
);

create table if not exists personalization.recommendation_request (
  id uuid primary key default gen_random_uuid(),
  anon_id uuid,
  session_id uuid,
  requested_at timestamptz not null default now(),
  viewport_class text not null,
  layout_mode text not null,
  surface text not null,
  algorithm_id text not null,
  event_pool_hash text,
  fallback_used boolean not null default false,
  latency_ms integer,
  debug_context jsonb not null default '{}'::jsonb
);

create table if not exists personalization.recommendation_result (
  request_id uuid not null references personalization.recommendation_request(id) on delete cascade,
  rank integer not null,
  event_id bigint not null,
  stable_slug text,
  score numeric(10,6) not null,
  score_parts jsonb not null default '{}'::jsonb,
  reason_codes text[] not null default '{}',
  primary key (request_id, rank)
);

create table if not exists personalization.daily_interaction_aggregate (
  day date not null,
  viewport_class text not null,
  layout_mode text not null,
  surface text not null,
  event_kind text not null,
  event_id bigint,
  count_events bigint not null default 0,
  unique (day, viewport_class, layout_mode, surface, event_kind, event_id)
);

create table if not exists personalization.e2e_persona (
  persona_key text primary key,
  description text not null,
  local_profile jsonb not null,
  expected_top_tags text[] not null default '{}',
  must_not_show_tags text[] not null default '{}',
  viewport_class text not null,
  layout_mode text not null,
  created_at timestamptz not null default now()
);

create index if not exists event_feature_snapshot_date_type_idx
  on personalization.event_feature_snapshot (event_date, event_type, city);
create index if not exists event_feature_snapshot_tags_gin_idx
  on personalization.event_feature_snapshot using gin (normalized_tags);
create index if not exists visitor_profile_snapshot_anon_built_idx
  on personalization.visitor_profile_snapshot (anon_id, built_at desc);
create index if not exists recommendation_request_anon_time_idx
  on personalization.recommendation_request (anon_id, requested_at desc);

revoke all on schema personalization from anon, authenticated;
grant usage on schema personalization to service_role;
grant select, insert, update, delete on all tables in schema personalization to service_role;
```

## Why no public SELECT in MVP

This rule applies to profile/telemetry/personalized reads by `anon_id`; the public aggregate counter table is a narrow exception because it exposes only event-level totals and no visitor/profile data.

Anonymous personalization without auth cannot strongly prove that a browser owns a given `anon_id`. Therefore public reads by `anon_id` are avoided in MVP. The safe flow is:

1. static site reads same-origin manifests;
2. browser stores local profile in localStorage;
3. browser sends compact append-only telemetry after consent through either the same-origin rate-limited endpoint or the dedicated `supabase_rpc_ingest_v1` function;
4. backend/SQL jobs aggregate profiles and can later publish coarse recommendation manifests or serve a rate-limited endpoint.

Future direct personalized read RPC is possible, but it must be treated as a new security design, not a default table SELECT.
