-- KenigEvents authorized event search / related retrieval sidecar.
-- Target: separate personalization Supabase project, not Fly SQLite source of truth.

create extension if not exists vector with schema extensions;
create extension if not exists pgcrypto with schema extensions;

create table if not exists public.event_search_documents (
  event_id bigint primary key,
  search_doc_version text not null default 'event-search-doc-v1',
  card_snapshot_version text not null default 'event-card-v1',
  text_hash text not null,
  title text not null,
  search_digest text not null,
  event_type text,
  category text,
  tags text[] not null default '{}',
  city text,
  venue_name text,
  start_date date,
  end_date date,
  starts_at timestamptz,
  lifecycle_status text not null default 'active',
  ticket_kind text,
  price_label text,
  is_free boolean not null default false,
  active boolean not null default true,
  card_snapshot jsonb not null,
  source_event_updated_at timestamptz,
  indexed_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint event_search_documents_search_digest_size_chk check (length(search_digest) <= 12000),
  constraint event_search_documents_title_size_chk check (length(title) <= 500),
  constraint event_search_documents_card_snapshot_size_chk check (length(card_snapshot::text) <= 20000),
  constraint event_search_documents_metadata_size_chk check (length(metadata::text) <= 4096)
);

create table if not exists public.event_embeddings (
  event_id bigint not null references public.event_search_documents(event_id) on delete cascade,
  embedding_model text not null,
  embedding_dim smallint not null default 768 check (embedding_dim = 768),
  embedding extensions.vector(768) not null,
  text_hash text not null,
  embedded_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  primary key (event_id, embedding_model, embedding_dim),
  constraint event_embeddings_metadata_size_chk check (length(metadata::text) <= 2048)
);

create table if not exists public.search_quota_plans (
  plan_id text primary key,
  daily_search_limit integer not null check (daily_search_limit >= 0),
  monthly_search_limit integer not null check (monthly_search_limit >= 0),
  daily_llm_rerank_limit integer not null check (daily_llm_rerank_limit >= 0),
  monthly_llm_rerank_limit integer not null check (monthly_llm_rerank_limit >= 0),
  enabled boolean not null default true,
  updated_at timestamptz not null default now()
);

insert into public.search_quota_plans (
  plan_id,
  daily_search_limit,
  monthly_search_limit,
  daily_llm_rerank_limit,
  monthly_llm_rerank_limit
) values
  ('registered', 5, 30, 2, 10)
on conflict (plan_id) do update set
  daily_search_limit = excluded.daily_search_limit,
  monthly_search_limit = excluded.monthly_search_limit,
  daily_llm_rerank_limit = excluded.daily_llm_rerank_limit,
  monthly_llm_rerank_limit = excluded.monthly_llm_rerank_limit,
  enabled = true,
  updated_at = now();

create table if not exists public.user_search_quota_ledger (
  user_id uuid not null,
  plan_id text not null references public.search_quota_plans(plan_id),
  bucket_kind text not null check (bucket_kind in ('day', 'month')),
  bucket_start date not null,
  request_count integer not null default 0 check (request_count >= 0),
  llm_request_count integer not null default 0 check (llm_request_count >= 0),
  first_used_at timestamptz not null default now(),
  last_used_at timestamptz not null default now(),
  primary key (user_id, bucket_kind, bucket_start)
);

create table if not exists public.event_search_requests (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null,
  request_kind text not null check (request_kind in ('vector_search', 'llm_rerank', 'fallback')),
  query_hash text not null,
  query_length smallint not null default 0 check (query_length >= 0),
  result_count integer not null default 0 check (result_count >= 0),
  llm_used boolean not null default false,
  status text not null check (status in ('ok', 'quota_exceeded', 'provider_error', 'db_error', 'invalid_request', 'fallback')),
  error_code text,
  created_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint event_search_requests_metadata_size_chk check (length(metadata::text) <= 4096)
);

create index if not exists event_search_documents_active_date_idx
  on public.event_search_documents (active, start_date, end_date);
create index if not exists event_search_documents_category_idx
  on public.event_search_documents (category) where active;
create index if not exists event_search_documents_city_idx
  on public.event_search_documents (city) where active;
create index if not exists event_search_documents_tags_gin_idx
  on public.event_search_documents using gin (tags);
create index if not exists event_search_requests_user_created_idx
  on public.event_search_requests (user_id, created_at desc);
create index if not exists event_search_requests_created_idx
  on public.event_search_requests (created_at desc);
create index if not exists user_search_quota_ledger_recent_idx
  on public.user_search_quota_ledger (bucket_kind, bucket_start desc);

create index if not exists event_embeddings_embedding_hnsw_idx
  on public.event_embeddings
  using hnsw (embedding extensions.vector_cosine_ops)
  with (m = 16, ef_construction = 128);

alter table public.event_search_documents enable row level security;
alter table public.event_embeddings enable row level security;
alter table public.search_quota_plans enable row level security;
alter table public.user_search_quota_ledger enable row level security;
alter table public.event_search_requests enable row level security;

revoke all on public.event_search_documents from anon, authenticated;
revoke all on public.event_embeddings from anon, authenticated;
revoke all on public.search_quota_plans from anon, authenticated;
revoke all on public.user_search_quota_ledger from anon, authenticated;
revoke all on public.event_search_requests from anon, authenticated;

grant select, insert, update, delete on public.event_search_documents to service_role;
grant select, insert, update, delete on public.event_embeddings to service_role;
grant select, insert, update, delete on public.search_quota_plans to service_role;
grant select, insert, update, delete on public.user_search_quota_ledger to service_role;
grant select, insert, update, delete on public.event_search_requests to service_role;

drop policy if exists event_search_documents_service_all on public.event_search_documents;
create policy event_search_documents_service_all
  on public.event_search_documents
  for all
  to service_role
  using (true)
  with check (true);

drop policy if exists event_embeddings_service_all on public.event_embeddings;
create policy event_embeddings_service_all
  on public.event_embeddings
  for all
  to service_role
  using (true)
  with check (true);

drop policy if exists search_quota_plans_service_all on public.search_quota_plans;
create policy search_quota_plans_service_all
  on public.search_quota_plans
  for all
  to service_role
  using (true)
  with check (true);

drop policy if exists user_search_quota_ledger_service_all on public.user_search_quota_ledger;
create policy user_search_quota_ledger_service_all
  on public.user_search_quota_ledger
  for all
  to service_role
  using (true)
  with check (true);

drop policy if exists event_search_requests_service_all on public.event_search_requests;
create policy event_search_requests_service_all
  on public.event_search_requests
  for all
  to service_role
  using (true)
  with check (true);

create or replace function public.search_events_by_embedding_v1(
  p_query_embedding extensions.vector(768),
  p_match_count integer default 24,
  p_offset_count integer default 0,
  p_date_from date default current_date,
  p_date_to date default null,
  p_city_filter text default null,
  p_category_filter text default null
)
returns table (
  event_id bigint,
  distance double precision,
  similarity double precision,
  title text,
  category text,
  tags text[],
  city text,
  start_date date,
  card_snapshot jsonb
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
begin
  if (select auth.uid()) is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  perform set_config('hnsw.ef_search', '80', true);

  return query
  select
    d.event_id,
    (e.embedding <=> p_query_embedding)::double precision as distance,
    (1 - (e.embedding <=> p_query_embedding))::double precision as similarity,
    d.title,
    d.category,
    d.tags,
    d.city,
    d.start_date,
    d.card_snapshot
  from public.event_embeddings e
  join public.event_search_documents d on d.event_id = e.event_id
  where d.active
    and d.lifecycle_status = 'active'
    and (p_date_from is null or coalesce(d.end_date, d.start_date) >= p_date_from)
    and (p_date_to is null or d.start_date <= p_date_to)
    and (p_city_filter is null or d.city = p_city_filter)
    and (p_category_filter is null or d.category = p_category_filter)
  order by e.embedding <=> p_query_embedding
  limit least(greatest(coalesce(p_match_count, 24), 1), 60)
  offset greatest(coalesce(p_offset_count, 0), 0);
end;
$$;

create or replace function public.get_event_search_quota_v1(
  p_plan_id text default 'registered',
  p_now timestamptz default now()
)
returns table (
  user_id uuid,
  plan_id text,
  daily_search_limit integer,
  monthly_search_limit integer,
  daily_llm_rerank_limit integer,
  monthly_llm_rerank_limit integer,
  day_used integer,
  month_used integer,
  llm_day_used integer,
  llm_month_used integer,
  day_remaining integer,
  month_remaining integer,
  llm_day_remaining integer,
  llm_month_remaining integer
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_plan public.search_quota_plans%rowtype;
  v_day_start date := (date_trunc('day', p_now at time zone 'UTC'))::date;
  v_month_start date := (date_trunc('month', p_now at time zone 'UTC'))::date;
  v_day_count integer := 0;
  v_month_count integer := 0;
  v_llm_day_count integer := 0;
  v_llm_month_count integer := 0;
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  select * into v_plan
  from public.search_quota_plans
  where search_quota_plans.plan_id = coalesce(nullif(p_plan_id, ''), 'registered')
    and enabled;

  if not found then
    raise exception 'search quota plan is disabled or missing' using errcode = 'P0001';
  end if;

  select coalesce(request_count, 0), coalesce(llm_request_count, 0)
    into v_day_count, v_llm_day_count
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start;

  select coalesce(request_count, 0), coalesce(llm_request_count, 0)
    into v_month_count, v_llm_month_count
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'month'
    and bucket_start = v_month_start;

  return query select
    v_user_id,
    v_plan.plan_id,
    v_plan.daily_search_limit,
    v_plan.monthly_search_limit,
    v_plan.daily_llm_rerank_limit,
    v_plan.monthly_llm_rerank_limit,
    coalesce(v_day_count, 0),
    coalesce(v_month_count, 0),
    coalesce(v_llm_day_count, 0),
    coalesce(v_llm_month_count, 0),
    greatest(v_plan.daily_search_limit - coalesce(v_day_count, 0), 0),
    greatest(v_plan.monthly_search_limit - coalesce(v_month_count, 0), 0),
    greatest(v_plan.daily_llm_rerank_limit - coalesce(v_llm_day_count, 0), 0),
    greatest(v_plan.monthly_llm_rerank_limit - coalesce(v_llm_month_count, 0), 0);
end;
$$;

create or replace function public.reserve_event_search_quota_v1(
  p_plan_id text default 'registered',
  p_use_llm boolean default false,
  p_now timestamptz default now()
)
returns table (
  user_id uuid,
  plan_id text,
  day_remaining integer,
  month_remaining integer,
  llm_day_remaining integer,
  llm_month_remaining integer
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_plan public.search_quota_plans%rowtype;
  v_day_start date := (date_trunc('day', p_now at time zone 'UTC'))::date;
  v_month_start date := (date_trunc('month', p_now at time zone 'UTC'))::date;
  v_day public.user_search_quota_ledger%rowtype;
  v_month public.user_search_quota_ledger%rowtype;
  v_plan_id text := coalesce(nullif(p_plan_id, ''), 'registered');
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  select * into v_plan
  from public.search_quota_plans
  where search_quota_plans.plan_id = v_plan_id
    and enabled;

  if not found then
    raise exception 'search quota plan is disabled or missing' using errcode = 'P0001';
  end if;

  insert into public.user_search_quota_ledger(user_id, plan_id, bucket_kind, bucket_start)
  values (v_user_id, v_plan.plan_id, 'day', v_day_start)
  on conflict on constraint user_search_quota_ledger_pkey do nothing;

  insert into public.user_search_quota_ledger(user_id, plan_id, bucket_kind, bucket_start)
  values (v_user_id, v_plan.plan_id, 'month', v_month_start)
  on conflict on constraint user_search_quota_ledger_pkey do nothing;

  select * into v_day
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  for update;

  select * into v_month
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'month'
    and bucket_start = v_month_start
  for update;

  if v_day.request_count >= v_plan.daily_search_limit or v_month.request_count >= v_plan.monthly_search_limit then
    raise exception 'search quota exceeded' using errcode = 'P0001';
  end if;

  if p_use_llm and (v_day.llm_request_count >= v_plan.daily_llm_rerank_limit or v_month.llm_request_count >= v_plan.monthly_llm_rerank_limit) then
    raise exception 'llm rerank quota exceeded' using errcode = 'P0001';
  end if;

  update public.user_search_quota_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when p_use_llm then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  returning * into v_day;

  update public.user_search_quota_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when p_use_llm then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'month'
    and bucket_start = v_month_start
  returning * into v_month;

  return query select
    v_user_id,
    v_plan.plan_id,
    greatest(v_plan.daily_search_limit - v_day.request_count, 0),
    greatest(v_plan.monthly_search_limit - v_month.request_count, 0),
    greatest(v_plan.daily_llm_rerank_limit - v_day.llm_request_count, 0),
    greatest(v_plan.monthly_llm_rerank_limit - v_month.llm_request_count, 0);
end;
$$;

create or replace function public.record_event_search_request_v1(
  p_request_kind text,
  p_query_hash text,
  p_query_length integer,
  p_result_count integer,
  p_llm_used boolean,
  p_status text,
  p_error_code text default null,
  p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_id uuid;
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  insert into public.event_search_requests(
    user_id,
    request_kind,
    query_hash,
    query_length,
    result_count,
    llm_used,
    status,
    error_code,
    metadata
  ) values (
    v_user_id,
    coalesce(nullif(p_request_kind, ''), 'vector_search'),
    left(coalesce(nullif(p_query_hash, ''), 'missing'), 128),
    least(greatest(coalesce(p_query_length, 0), 0), 4096),
    greatest(coalesce(p_result_count, 0), 0),
    coalesce(p_llm_used, false),
    coalesce(nullif(p_status, ''), 'ok'),
    nullif(left(coalesce(p_error_code, ''), 120), ''),
    coalesce(p_metadata, '{}'::jsonb)
  ) returning id into v_id;

  return v_id;
end;
$$;

revoke all on function public.search_events_by_embedding_v1(extensions.vector, integer, integer, date, date, text, text) from public, anon, authenticated;
revoke all on function public.get_event_search_quota_v1(text, timestamptz) from public, anon, authenticated;
revoke all on function public.reserve_event_search_quota_v1(text, boolean, timestamptz) from public, anon, authenticated;
revoke all on function public.record_event_search_request_v1(text, text, integer, integer, boolean, text, text, jsonb) from public, anon, authenticated;

grant execute on function public.search_events_by_embedding_v1(extensions.vector, integer, integer, date, date, text, text) to authenticated;
grant execute on function public.get_event_search_quota_v1(text, timestamptz) to authenticated;
grant execute on function public.reserve_event_search_quota_v1(text, boolean, timestamptz) to authenticated;
grant execute on function public.record_event_search_request_v1(text, text, integer, integer, boolean, text, text, jsonb) to authenticated;

create or replace function public.event_search_fallback_cards_v1(
  p_match_count integer default 24,
  p_offset_count integer default 0,
  p_date_from date default current_date
)
returns table (
  event_id bigint,
  title text,
  category text,
  tags text[],
  city text,
  start_date date,
  card_snapshot jsonb
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
begin
  if (select auth.uid()) is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  return query
  select d.event_id, d.title, d.category, d.tags, d.city, d.start_date, d.card_snapshot
  from public.event_search_documents d
  left join public.personalization_event_reaction_counter c on c.event_id = d.event_id
  where d.active
    and d.lifecycle_status = 'active'
    and (p_date_from is null or coalesce(d.end_date, d.start_date) >= p_date_from)
  order by coalesce(c.likes_count, 0) desc, d.start_date asc nulls last, d.event_id desc
  limit least(greatest(coalesce(p_match_count, 24), 1), 60)
  offset greatest(coalesce(p_offset_count, 0), 0);
end;
$$;

revoke all on function public.event_search_fallback_cards_v1(integer, integer, date) from public, anon, authenticated;
grant execute on function public.event_search_fallback_cards_v1(integer, integer, date) to authenticated;
