# Personalization Database Design

> **Status:** design/draft, SQL not applied yet  
> **Target DB:** separate Supabase/Postgres personalization project (`PERSONALIZATION_*`)  
> **Do not store here:** canonical events, source facts, Telegram/VK/Telegraph state, Smart Update decisions.

## Current capacity snapshot

As of 2026-06-24 the separate personalization Supabase database is effectively empty:

- `pg_database_size(current_database())`: `10211 kB`;
- approximate free space vs 500 MB decimal free tier: `~489.54 MB`;
- PostgreSQL: `17.6`;
- installed extensions include `pgcrypto`, `uuid-ossp`, `pg_stat_statements`; `pgvector` is not required for MVP and should be enabled only if vector search moves into Postgres.

## Supabase constraints that shape the schema

- New Supabase projects no longer expose new `public` tables to Data API automatically; explicit `GRANT` is required for `anon`/`authenticated` roles. RLS and grants are separate layers.
- Any table exposed to Data API must have RLS enabled.
- Browser code may use only `PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`; secret/service/direct DB credentials stay backend-only.
- MVP should avoid public `SECURITY DEFINER` RPCs. If a future RPC is needed, keep the function minimal, revoke default `PUBLIC` execute, grant only required roles, and add abuse/rate controls.

Official references:

- Supabase changelog, Data API grants breaking change: <https://supabase.com/changelog/45329-breaking-change-tables-not-exposed-to-data-and-graphql-api-automatically>
- Securing Data API: <https://supabase.com/docs/guides/api/securing-your-api>
- RLS guide: <https://supabase.com/docs/guides/database/postgres/row-level-security>
- API keys: <https://supabase.com/docs/guides/getting-started/api-keys>
- pgvector: <https://supabase.com/docs/guides/database/extensions/pgvector>

## Data ownership boundary

### Public/exposed table in `public`

The MVP browser-facing write should be **compact summaries first**, not a raw event firehose:

- `public.personalization_session_summary` — append-only compact session/profile deltas after consent;
- `public.personalization_interaction_event` — optional short-retention/debug/sampled raw telemetry, not the default path for every impression.

Both have insert-only RLS and **no public SELECT** if exposed. The default frontend path should upload one bounded summary per session/interval and only strong raw actions when needed for debugging/evaluation. This is required to protect the 500 MB free-tier database limit.

### Private schema `personalization`

All derived and debug data lives outside exposed schemas:

- `personalization.anonymous_visitor` — first/last seen, consent version, coarse profile status;
- `personalization.anonymous_session` — session rollups built from compact summaries;
- `personalization.event_feature_snapshot` — event ranking features exported from Fly SQLite + offline enrichment;
- `personalization.visitor_profile_snapshot` — compact session/short/mid/long profile horizons, each with positive and negative vectors/maps;
- `personalization.recommendation_request` and `recommendation_result` — debug/eval evidence;
- `personalization.daily_interaction_aggregate` — retention-safe analytics;
- `personalization.e2e_persona` — synthetic test personas and expected top-k.

The static site reads event catalog/recommendation manifests from same-origin JSON first. Supabase is not the default mass-read path.

## Retention policy

Default MVP retention and storage policy:

| Data | Retention | Reason |
| --- | ---: | --- |
| Raw weak `interaction_event` impressions/skips | off by default or sampled; 0-7 days | avoid filling 500 MB with scroll noise |
| Raw strong actions (`ticket_click`, `hide`, `detail_view`, `share`) | 14-30 days | debugging labels and position bias |
| `personalization_session_summary` | 90-180 days or until folded into profiles | compact training/eval signal without raw volume |
| `daily_interaction_aggregate` | 12 months | product metrics without raw event volume |
| `visitor_profile_snapshot` | 365 days since last seen, compact only | returning visitor personalization |
| `recommendation_request/result` debug | 14–30 days | E2E/debug only |
| `event_feature_snapshot` | while event is future + static retention window | ranking features tied to event lifecycle |

Retention should run from backend/cron with direct DB credentials or SQL job, not from browser. Raw telemetry must be deleted or aggregated before it can threaten the free-tier cap.

## Index strategy

MVP indexes are write-friendly:

- compact summaries by `(anon_id, ended_at desc)` for profile aggregation;
- raw telemetry by `occurred_at` only if debug/sampling is enabled;
- raw telemetry by `(anon_id, occurred_at desc)` only for short-retention profile/debug windows;
- raw telemetry by `(event_id, occurred_at desc)` only for sampled event analytics;
- partial index for strong actions: `ticket_click`, `share`, `copy_link`, `event_detail_view`, `hide_event`;
- GIN indexes on small JSONB feature maps only after query patterns are real;
- pgvector/HNSW only if server-side vector search is chosen after eval.

Avoid indexes on every JSON field before usage is known. Compact summaries/profile snapshots should dominate storage; raw telemetry volume must remain bounded and disposable.

## Draft SQL

The SQL below is a design artifact. Apply only after review against the current Supabase project settings.

```sql
-- Optional only if server-side vector search is selected later:
-- create extension if not exists vector with schema extensions;

create schema if not exists personalization;

-- Browser-facing compact session summary. This is the MVP default write path.
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
  event_counts jsonb not null default '{}'::jsonb,
  positive_tag_delta jsonb not null default '{}'::jsonb,
  negative_tag_delta jsonb not null default '{}'::jsonb,
  strong_event_ids jsonb not null default '{}'::jsonb,
  seen_event_ids_sample bigint[] not null default '{}',
  hidden_event_ids bigint[] not null default '{}',
  profile_delta_vector jsonb not null default '[]'::jsonb,
  client_summary_version text not null,
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_session_summary_consent_chk check (consent_state = 'accepted'),
  constraint personalization_session_summary_time_chk check (ended_at >= started_at),
  constraint personalization_session_summary_payload_chk check (
    length(event_counts::text) <= 2048
    and length(positive_tag_delta::text) <= 4096
    and length(negative_tag_delta::text) <= 4096
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
grant insert on public.personalization_session_summary to anon;
grant select, insert, update, delete on public.personalization_session_summary to service_role;

create policy "anon can append compact personalization summaries"
  on public.personalization_session_summary
  for insert
  to anon
  with check (
    consent_state = 'accepted'
    and anon_id is not null
    and session_id is not null
    and client_summary_id is not null
    and length(client_summary_id) <= 120
  );

create index if not exists personalization_session_summary_anon_time_idx
  on public.personalization_session_summary (anon_id, ended_at desc);
create index if not exists personalization_session_summary_received_idx
  on public.personalization_session_summary (received_at desc);

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
  dwell_ms integer,
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_interaction_event_kind_chk check (event_kind in (
    'page_view', 'event_impression', 'event_card_click', 'event_detail_view',
    'dwell_checkpoint', 'ticket_click', 'hide_event', 'not_interested',
    'share', 'copy_link', 'recommendation_feed_loaded', 'recommendation_fallback_used'
  )),
  constraint personalization_viewport_chk check (viewport_class in ('mobile', 'tablet', 'desktop')),
  constraint personalization_layout_chk check (layout_mode in ('feed', 'grid', 'list', 'module', 'detail')),
  constraint personalization_consent_chk check (consent_state = 'accepted'),
  constraint personalization_position_chk check (position is null or position >= 0),
  constraint personalization_dwell_chk check (dwell_ms is null or dwell_ms between 0 and 86400000),
  constraint personalization_metadata_size_chk check (length(metadata::text) <= 4096),
  unique (anon_id, client_event_id)
);

alter table public.personalization_interaction_event enable row level security;

revoke all on public.personalization_interaction_event from anon, authenticated;
-- Enable only if raw-event sampling/debug is intentionally used:
-- grant insert on public.personalization_interaction_event to anon;
grant select, insert, update, delete on public.personalization_interaction_event to service_role;

-- Optional raw-event insert policy. Keep disabled for MVP unless sampling/debug is approved.
-- create policy "anon can append accepted personalization telemetry"
--   on public.personalization_interaction_event
--   for insert
--   to anon
--   with check (
--     consent_state = 'accepted'
--     and anon_id is not null
--     and session_id is not null
--     and client_event_id is not null
--     and length(client_event_id) <= 120
--     and length(page_url) <= 2048
--   );

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
  where event_kind in ('event_detail_view', 'ticket_click', 'share', 'copy_link', 'hide_event', 'not_interested');

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
  feature_version text not null,
  source_hash text not null,
  event_date date not null,
  city text,
  event_type text,
  normalized_tags text[] not null default '{}',
  audience_tags text[] not null default '{}',
  negative_tags text[] not null default '{}',
  venue_key text,
  is_free boolean,
  ticket_status text,
  popularity_baseline numeric(8,5) not null default 0,
  feature_vector_schema text,
  feature_vector jsonb not null default '[]'::jsonb,
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
  negative_vector jsonb not null default '[]'::jsonb,
  positive_tags jsonb not null default '{}'::jsonb,
  negative_tags jsonb not null default '{}'::jsonb,
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

Anonymous personalization without auth cannot strongly prove that a browser owns a given `anon_id`. Therefore public reads by `anon_id` are avoided in MVP. The safe flow is:

1. static site reads same-origin manifests;
2. browser stores local profile in localStorage;
3. browser inserts append-only telemetry after consent;
4. backend aggregates profiles and can later publish coarse recommendation manifests or serve a rate-limited endpoint.

Future direct personalized RPC is possible, but it must be treated as a new security design, not a default table SELECT.
