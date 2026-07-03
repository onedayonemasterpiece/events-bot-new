-- Product search quota/cache update for the authorized static-site smart search.
-- - Monthly quota is no longer part of the active product contract.
-- - A one-hour per-user window prevents one visitor from burning provider budget at once.
-- - Short-lived result cache is keyed by salted query hash + result-shaping params.
-- - Result cache is physically cleared when the event/vector corpus changes.

alter table public.search_quota_plans
  add column if not exists hourly_search_limit integer not null default 60 check (hourly_search_limit >= 0),
  add column if not exists hourly_llm_rerank_limit integer not null default 60 check (hourly_llm_rerank_limit >= 0);

update public.search_quota_plans
   set hourly_search_limit = least(greatest(coalesce(hourly_search_limit, 60), 10), greatest(daily_search_limit, 10)),
       hourly_llm_rerank_limit = least(greatest(coalesce(hourly_llm_rerank_limit, 60), 5), greatest(daily_llm_rerank_limit, 5)),
       updated_at = now()
 where plan_id = 'registered';

create table if not exists public.user_search_quota_hourly_ledger (
  user_id uuid not null,
  plan_id text not null references public.search_quota_plans(plan_id),
  bucket_start_at timestamptz not null,
  request_count integer not null default 0 check (request_count >= 0),
  llm_request_count integer not null default 0 check (llm_request_count >= 0),
  first_used_at timestamptz not null default now(),
  last_used_at timestamptz not null default now(),
  primary key (user_id, bucket_start_at)
);

create index if not exists user_search_quota_hourly_recent_idx
  on public.user_search_quota_hourly_ledger (bucket_start_at desc);

alter table public.user_search_quota_hourly_ledger enable row level security;
revoke all on public.user_search_quota_hourly_ledger from anon, authenticated;
grant select, insert, update, delete on public.user_search_quota_hourly_ledger to service_role;

drop policy if exists user_search_quota_hourly_service_all on public.user_search_quota_hourly_ledger;
create policy user_search_quota_hourly_service_all
  on public.user_search_quota_hourly_ledger
  for all
  to service_role
  using (true)
  with check (true);

create or replace function public.get_event_search_quota_v2(
  p_plan_id text default 'registered',
  p_now timestamptz default now()
)
returns table (
  user_id uuid,
  plan_id text,
  hourly_search_limit integer,
  daily_search_limit integer,
  hourly_llm_rerank_limit integer,
  daily_llm_rerank_limit integer,
  hour_used integer,
  day_used integer,
  llm_hour_used integer,
  llm_day_used integer,
  hour_remaining integer,
  day_remaining integer,
  llm_hour_remaining integer,
  llm_day_remaining integer,
  hour_reset_at timestamptz
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_plan public.search_quota_plans%rowtype;
  v_hour_start timestamptz := date_trunc('hour', p_now);
  v_day_start date := (date_trunc('day', p_now at time zone 'UTC'))::date;
  v_hour_count integer := 0;
  v_day_count integer := 0;
  v_llm_hour_count integer := 0;
  v_llm_day_count integer := 0;
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
    into v_hour_count, v_llm_hour_count
  from public.user_search_quota_hourly_ledger
  where user_search_quota_hourly_ledger.user_id = v_user_id
    and bucket_start_at = v_hour_start;

  select coalesce(request_count, 0), coalesce(llm_request_count, 0)
    into v_day_count, v_llm_day_count
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start;

  return query select
    v_user_id,
    v_plan.plan_id,
    v_plan.hourly_search_limit,
    v_plan.daily_search_limit,
    v_plan.hourly_llm_rerank_limit,
    v_plan.daily_llm_rerank_limit,
    coalesce(v_hour_count, 0),
    coalesce(v_day_count, 0),
    coalesce(v_llm_hour_count, 0),
    coalesce(v_llm_day_count, 0),
    greatest(v_plan.hourly_search_limit - coalesce(v_hour_count, 0), 0),
    greatest(v_plan.daily_search_limit - coalesce(v_day_count, 0), 0),
    greatest(v_plan.hourly_llm_rerank_limit - coalesce(v_llm_hour_count, 0), 0),
    greatest(v_plan.daily_llm_rerank_limit - coalesce(v_llm_day_count, 0), 0),
    v_hour_start + interval '1 hour';
end;
$$;

create or replace function public.reserve_event_search_quota_v3(
  p_plan_id text default 'registered',
  p_use_llm boolean default false,
  p_now timestamptz default now()
)
returns table (
  user_id uuid,
  plan_id text,
  hour_remaining integer,
  day_remaining integer,
  llm_hour_remaining integer,
  llm_day_remaining integer,
  hour_reset_at timestamptz,
  llm_reserved boolean
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_plan public.search_quota_plans%rowtype;
  v_hour_start timestamptz := date_trunc('hour', p_now);
  v_day_start date := (date_trunc('day', p_now at time zone 'UTC'))::date;
  v_hour public.user_search_quota_hourly_ledger%rowtype;
  v_day public.user_search_quota_ledger%rowtype;
  v_plan_id text := coalesce(nullif(p_plan_id, ''), 'registered');
  v_llm_reserved boolean := false;
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

  insert into public.user_search_quota_hourly_ledger(user_id, plan_id, bucket_start_at)
  values (v_user_id, v_plan.plan_id, v_hour_start)
  on conflict on constraint user_search_quota_hourly_ledger_pkey do nothing;

  insert into public.user_search_quota_ledger(user_id, plan_id, bucket_kind, bucket_start)
  values (v_user_id, v_plan.plan_id, 'day', v_day_start)
  on conflict on constraint user_search_quota_ledger_pkey do nothing;

  select * into v_hour
  from public.user_search_quota_hourly_ledger
  where user_search_quota_hourly_ledger.user_id = v_user_id
    and bucket_start_at = v_hour_start
  for update;

  select * into v_day
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  for update;

  if v_hour.request_count >= v_plan.hourly_search_limit then
    raise exception 'hourly search quota exceeded' using errcode = 'P0001';
  end if;

  if v_day.request_count >= v_plan.daily_search_limit then
    raise exception 'daily search quota exceeded' using errcode = 'P0001';
  end if;

  v_llm_reserved := p_use_llm
    and v_hour.llm_request_count < v_plan.hourly_llm_rerank_limit
    and v_day.llm_request_count < v_plan.daily_llm_rerank_limit;

  update public.user_search_quota_hourly_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when v_llm_reserved then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_hourly_ledger.user_id = v_user_id
    and bucket_start_at = v_hour_start
  returning * into v_hour;

  update public.user_search_quota_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when v_llm_reserved then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  returning * into v_day;

  return query select
    v_user_id,
    v_plan.plan_id,
    greatest(v_plan.hourly_search_limit - v_hour.request_count, 0),
    greatest(v_plan.daily_search_limit - v_day.request_count, 0),
    greatest(v_plan.hourly_llm_rerank_limit - v_hour.llm_request_count, 0),
    greatest(v_plan.daily_llm_rerank_limit - v_day.llm_request_count, 0),
    v_hour_start + interval '1 hour',
    v_llm_reserved;
end;
$$;

revoke all on function public.get_event_search_quota_v2(text, timestamptz) from public, anon, authenticated;
revoke all on function public.reserve_event_search_quota_v3(text, boolean, timestamptz) from public, anon, authenticated;
grant execute on function public.get_event_search_quota_v2(text, timestamptz) to authenticated;
grant execute on function public.reserve_event_search_quota_v3(text, boolean, timestamptz) to authenticated;

create table if not exists public.event_search_result_cache (
  cache_key text primary key,
  query_hash text not null,
  embedding_model text not null,
  embedding_dim smallint not null default 768 check (embedding_dim = 768),
  embedding_doc_kind text not null,
  request_signature text not null,
  response jsonb not null,
  hit_count integer not null default 0 check (hit_count >= 0),
  created_at timestamptz not null default now(),
  expires_at timestamptz not null,
  last_used_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint event_search_result_cache_key_size_chk check (length(cache_key) between 32 and 128),
  constraint event_search_result_cache_hash_size_chk check (length(query_hash) between 32 and 128),
  constraint event_search_result_cache_doc_kind_size_chk check (length(embedding_doc_kind) between 3 and 80),
  constraint event_search_result_cache_signature_size_chk check (length(request_signature) between 32 and 128),
  constraint event_search_result_cache_response_size_chk check (length(response::text) <= 500000),
  constraint event_search_result_cache_metadata_size_chk check (length(metadata::text) <= 4096)
);

create index if not exists event_search_result_cache_expires_idx
  on public.event_search_result_cache (expires_at);
create index if not exists event_search_result_cache_last_used_idx
  on public.event_search_result_cache (last_used_at desc);

alter table public.event_search_result_cache enable row level security;
revoke all on public.event_search_result_cache from anon, authenticated;
grant select, insert, update, delete on public.event_search_result_cache to service_role;

drop policy if exists event_search_result_cache_service_all on public.event_search_result_cache;
create policy event_search_result_cache_service_all
  on public.event_search_result_cache
  for all
  to service_role
  using (true)
  with check (true);

create or replace function public.get_event_search_result_cache_v1(
  p_cache_key text
)
returns table (
  response jsonb,
  hit_count integer,
  expires_at timestamptz,
  last_used_at timestamptz
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
begin
  delete from public.event_search_result_cache
   where event_search_result_cache.expires_at <= now();

  return query
  update public.event_search_result_cache c
     set hit_count = c.hit_count + 1,
         last_used_at = now()
   where c.cache_key = p_cache_key
     and c.expires_at > now()
  returning c.response, c.hit_count, c.expires_at, c.last_used_at;
end;
$$;

create or replace function public.upsert_event_search_result_cache_v1(
  p_cache_key text,
  p_query_hash text,
  p_embedding_model text,
  p_embedding_dim integer,
  p_embedding_doc_kind text,
  p_request_signature text,
  p_response jsonb,
  p_ttl_seconds integer default 10800,
  p_metadata jsonb default '{}'::jsonb
)
returns void
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_ttl integer := least(greatest(coalesce(p_ttl_seconds, 10800), 60), 21600);
begin
  delete from public.event_search_result_cache
   where event_search_result_cache.expires_at <= now();

  insert into public.event_search_result_cache (
    cache_key,
    query_hash,
    embedding_model,
    embedding_dim,
    embedding_doc_kind,
    request_signature,
    response,
    hit_count,
    created_at,
    expires_at,
    last_used_at,
    metadata
  ) values (
    p_cache_key,
    p_query_hash,
    p_embedding_model,
    p_embedding_dim,
    p_embedding_doc_kind,
    p_request_signature,
    p_response,
    0,
    now(),
    now() + make_interval(secs => v_ttl),
    now(),
    coalesce(p_metadata, '{}'::jsonb)
  )
  on conflict (cache_key) do update set
    response = excluded.response,
    expires_at = excluded.expires_at,
    last_used_at = now(),
    metadata = excluded.metadata;
end;
$$;

create or replace function public.purge_event_search_result_cache_v1()
returns integer
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_deleted integer;
begin
  delete from public.event_search_result_cache where true;
  get diagnostics v_deleted = row_count;
  return v_deleted;
end;
$$;

create or replace function public.clear_event_search_result_cache_on_corpus_change_v1()
returns trigger
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
begin
  delete from public.event_search_result_cache where true;
  return null;
end;
$$;

revoke all on function public.get_event_search_result_cache_v1(text) from public, anon, authenticated;
revoke all on function public.upsert_event_search_result_cache_v1(text, text, text, integer, text, text, jsonb, integer, jsonb) from public, anon, authenticated;
revoke all on function public.purge_event_search_result_cache_v1() from public, anon, authenticated;
revoke all on function public.clear_event_search_result_cache_on_corpus_change_v1() from public, anon, authenticated;
grant execute on function public.get_event_search_result_cache_v1(text) to service_role;
grant execute on function public.upsert_event_search_result_cache_v1(text, text, text, integer, text, text, jsonb, integer, jsonb) to service_role;
grant execute on function public.purge_event_search_result_cache_v1() to service_role;
grant execute on function public.clear_event_search_result_cache_on_corpus_change_v1() to service_role;

drop trigger if exists event_search_documents_clear_result_cache on public.event_search_documents;
create trigger event_search_documents_clear_result_cache
  after insert or update or delete or truncate on public.event_search_documents
  for each statement execute function public.clear_event_search_result_cache_on_corpus_change_v1();

drop trigger if exists event_embeddings_clear_result_cache on public.event_embeddings;
create trigger event_embeddings_clear_result_cache
  after insert or update or delete or truncate on public.event_embeddings
  for each statement execute function public.clear_event_search_result_cache_on_corpus_change_v1();
