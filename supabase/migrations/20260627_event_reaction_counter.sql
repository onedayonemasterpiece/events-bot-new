-- Personalization aggregate for card counters.
-- Public UI reads only total counters; technical source/service split is backend-only.

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

-- RLS allows browser roles to read only the columns granted above.
drop policy if exists personalization_event_reaction_counter_public_read
  on public.personalization_event_reaction_counter;
create policy personalization_event_reaction_counter_public_read
  on public.personalization_event_reaction_counter
  for select
  to anon, authenticated
  using (true);

create index if not exists personalization_event_reaction_counter_updated_idx
  on public.personalization_event_reaction_counter (updated_at desc);
