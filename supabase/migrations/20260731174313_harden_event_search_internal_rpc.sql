-- Close expensive search primitives to browser roles. The authenticated Edge
-- Function verifies the user JWT first, then calls these service-role-only
-- wrappers with the verified auth.users id. No user_metadata claim is trusted.

create table if not exists public.event_search_quota_operation (
  user_id uuid not null references auth.users(id) on delete cascade,
  client_request_id uuid not null,
  plan_id text not null,
  hour_remaining integer not null check (hour_remaining >= 0),
  day_remaining integer not null check (day_remaining >= 0),
  llm_hour_remaining integer not null check (llm_hour_remaining >= 0),
  llm_day_remaining integer not null check (llm_day_remaining >= 0),
  hour_reset_at timestamptz not null,
  llm_reserved boolean not null,
  created_at timestamptz not null default now(),
  primary key (user_id, client_request_id)
);

create index if not exists event_search_quota_operation_created_idx
  on public.event_search_quota_operation (created_at);
create index if not exists event_search_quota_operation_user_created_idx
  on public.event_search_quota_operation (user_id, created_at desc);

alter table public.event_search_quota_operation enable row level security;
revoke all on table public.event_search_quota_operation from public, anon, authenticated;
grant select, insert, delete on table public.event_search_quota_operation to service_role;

create policy "event search quota operations are service-only"
  on public.event_search_quota_operation
  for all
  to service_role
  using (true)
  with check (true);

create or replace function public.get_event_search_quota_internal_v1(
  p_user_id uuid,
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
set search_path = pg_catalog
as $$
begin
  if p_user_id is null or not exists (
    select 1 from auth.users where id = p_user_id
  ) then
    raise exception 'verified_user_required' using errcode = '28000';
  end if;

  perform pg_catalog.set_config('request.jwt.claim.sub', p_user_id::text, true);
  return query select *
  from public.get_event_search_quota_v2(p_plan_id, p_now);
end;
$$;

create or replace function public.reserve_event_search_quota_internal_v1(
  p_user_id uuid,
  p_client_request_id uuid,
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
set search_path = pg_catalog
as $$
declare
  v_saved public.event_search_quota_operation%rowtype;
  v_count integer;
  v_reserved_user_id uuid;
  v_reserved_plan_id text;
  v_hour_remaining integer;
  v_day_remaining integer;
  v_llm_hour_remaining integer;
  v_llm_day_remaining integer;
  v_hour_reset_at timestamptz;
  v_llm_reserved boolean;
begin
  if p_user_id is null or p_client_request_id is null or not exists (
    select 1 from auth.users where id = p_user_id
  ) then
    raise exception 'verified_user_and_request_id_required' using errcode = '28000';
  end if;

  -- One owner lock makes duplicate request IDs and the bounded operation ledger
  -- deterministic under concurrent tabs/retries.
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(p_user_id::text, 1811704447)
  );

  select * into v_saved
  from public.event_search_quota_operation
  where event_search_quota_operation.user_id = p_user_id
    and client_request_id = p_client_request_id;

  if found then
    return query select
      v_saved.user_id,
      v_saved.plan_id,
      v_saved.hour_remaining,
      v_saved.day_remaining,
      v_saved.llm_hour_remaining,
      v_saved.llm_day_remaining,
      v_saved.hour_reset_at,
      v_saved.llm_reserved;
    return;
  end if;

  delete from public.event_search_quota_operation
  where event_search_quota_operation.user_id = p_user_id
    and created_at < pg_catalog.now() - interval '48 hours';

  select pg_catalog.count(*)::integer into v_count
  from public.event_search_quota_operation
  where event_search_quota_operation.user_id = p_user_id;
  if v_count >= 1000 then
    raise exception 'search_operation_ledger_limit_exceeded' using errcode = '54000';
  end if;

  perform pg_catalog.set_config('request.jwt.claim.sub', p_user_id::text, true);

  select * into
    v_reserved_user_id,
    v_reserved_plan_id,
    v_hour_remaining,
    v_day_remaining,
    v_llm_hour_remaining,
    v_llm_day_remaining,
    v_hour_reset_at,
    v_llm_reserved
  from public.reserve_event_search_quota_v3(p_plan_id, p_use_llm, p_now);

  insert into public.event_search_quota_operation (
    user_id,
    client_request_id,
    plan_id,
    hour_remaining,
    day_remaining,
    llm_hour_remaining,
    llm_day_remaining,
    hour_reset_at,
    llm_reserved
  ) values (
    p_user_id,
    p_client_request_id,
    v_reserved_plan_id,
    v_hour_remaining,
    v_day_remaining,
    v_llm_hour_remaining,
    v_llm_day_remaining,
    v_hour_reset_at,
    v_llm_reserved
  )
  returning * into v_saved;

  return query select
    v_saved.user_id,
    v_saved.plan_id,
    v_saved.hour_remaining,
    v_saved.day_remaining,
    v_saved.llm_hour_remaining,
    v_saved.llm_day_remaining,
    v_saved.hour_reset_at,
    v_saved.llm_reserved;
end;
$$;

create or replace function public.search_events_by_embedding_internal_v1(
  p_user_id uuid,
  p_query_embedding extensions.vector(768),
  p_match_count integer default 24,
  p_offset_count integer default 0,
  p_date_from date default current_date,
  p_date_to date default null,
  p_city_filter text default null,
  p_category_filter text default null,
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768,
  p_weekday_iso smallint default null,
  p_time_of_day_filter text default null,
  p_admission_filter text default null,
  p_embedding_doc_kind text default 'search_v3'
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
set search_path = pg_catalog
as $$
begin
  if p_user_id is null or not exists (
    select 1 from auth.users where id = p_user_id
  ) then
    raise exception 'verified_user_required' using errcode = '28000';
  end if;
  perform pg_catalog.set_config('request.jwt.claim.sub', p_user_id::text, true);
  return query select * from public.search_events_by_embedding_v1(
    p_query_embedding,
    p_match_count,
    p_offset_count,
    p_date_from,
    p_date_to,
    p_city_filter,
    p_category_filter,
    p_embedding_model,
    p_embedding_dim,
    p_weekday_iso,
    p_time_of_day_filter,
    p_admission_filter,
    p_embedding_doc_kind
  );
end;
$$;

create or replace function public.event_search_fallback_cards_internal_v1(
  p_user_id uuid,
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
set search_path = pg_catalog
as $$
begin
  if p_user_id is null or not exists (
    select 1 from auth.users where id = p_user_id
  ) then
    raise exception 'verified_user_required' using errcode = '28000';
  end if;
  perform pg_catalog.set_config('request.jwt.claim.sub', p_user_id::text, true);
  return query select * from public.event_search_fallback_cards_v1(
    p_match_count,
    p_offset_count,
    p_date_from
  );
end;
$$;

create or replace function public.record_event_search_request_internal_v1(
  p_user_id uuid,
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
set search_path = pg_catalog
as $$
begin
  if p_user_id is null or not exists (
    select 1 from auth.users where id = p_user_id
  ) then
    raise exception 'verified_user_required' using errcode = '28000';
  end if;
  perform pg_catalog.set_config('request.jwt.claim.sub', p_user_id::text, true);
  return public.record_event_search_request_v1(
    p_request_kind,
    p_query_hash,
    p_query_length,
    p_result_count,
    p_llm_used,
    p_status,
    p_error_code,
    p_metadata
  );
end;
$$;

-- Feedback remains a narrow authenticated browser RPC. It is now idempotent,
-- serialized per owner, rate-capped, metadata-compacted and retention-bounded.
alter table public.event_search_feedback
  add column if not exists operation_id uuid;
update public.event_search_feedback
set operation_id = extensions.gen_random_uuid()
where operation_id is null;
alter table public.event_search_feedback
  alter column operation_id set default extensions.gen_random_uuid(),
  alter column operation_id set not null;
create unique index if not exists event_search_feedback_user_operation_uidx
  on public.event_search_feedback (user_id, operation_id);

-- Replace the old five-argument overload so calls omitting p_operation_id use
-- the default on the single hardened function rather than an unsafe overload.
drop function if exists public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb);

create function public.record_event_search_feedback_v1(
  p_query text,
  p_verdict text,
  p_result_event_ids bigint[] default '{}'::bigint[],
  p_result_count integer default 0,
  p_metadata jsonb default '{}'::jsonb,
  p_operation_id uuid default null
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog, extensions
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_query text := pg_catalog.left(pg_catalog.regexp_replace(pg_catalog.btrim(coalesce(p_query, '')), '[[:cntrl:]<>]+', ' ', 'g'), 160);
  v_verdict text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_verdict, '')));
  v_query_hash text;
  v_operation_id uuid;
  v_existing_id uuid;
  v_id uuid;
  v_ids bigint[] := coalesce(p_result_event_ids, '{}'::bigint[]);
  v_metadata jsonb;
  v_recent_count integer;
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  v_query := pg_catalog.regexp_replace(v_query, '\s+', ' ', 'g');
  if pg_catalog.length(v_query) < 3 then
    raise exception 'query_too_short' using errcode = '22023';
  end if;
  if v_verdict not in ('matched', 'missed') then
    raise exception 'invalid_verdict' using errcode = '22023';
  end if;

  v_query_hash := pg_catalog.encode(extensions.digest(pg_catalog.lower(v_query), 'sha256'), 'hex');
  v_ids := (
    select coalesce(pg_catalog.array_agg(distinct item), '{}'::bigint[])
    from pg_catalog.unnest(v_ids[1:40]) as item
    where item is not null and item > 0
  );
  v_metadata := pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'surface', pg_catalog.left(p_metadata ->> 'surface', 80),
    'client_ts', pg_catalog.left(p_metadata ->> 'client_ts', 40)
  ));
  v_operation_id := coalesce(
    p_operation_id,
    (
      pg_catalog.substr(v_query_hash, 1, 8) || '-' ||
      pg_catalog.substr(v_query_hash, 9, 4) || '-4' ||
      pg_catalog.substr(v_query_hash, 14, 3) || '-8' ||
      pg_catalog.substr(v_query_hash, 18, 3) || '-' ||
      pg_catalog.substr(
        pg_catalog.encode(
          extensions.digest(
            v_user_id::text || ':' || v_verdict || ':' ||
            pg_catalog.date_trunc('hour', pg_catalog.now())::text,
            'sha256'
          ),
          'hex'
        ),
        1,
        12
      )
    )::uuid
  );

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(v_user_id::text, 1097259931)
  );

  select id into v_existing_id
  from public.event_search_feedback
  where user_id = v_user_id
    and operation_id = v_operation_id;
  if found then
    return v_existing_id;
  end if;

  delete from public.event_search_feedback
  where ctid in (
    select ctid
    from public.event_search_feedback
    where user_id = v_user_id
      and created_at < pg_catalog.now() - interval '90 days'
    order by created_at
    limit 50
  );

  select pg_catalog.count(*)::integer into v_recent_count
  from public.event_search_feedback
  where user_id = v_user_id
    and created_at >= pg_catalog.now() - interval '1 hour';
  if v_recent_count >= 30 then
    raise exception 'search_feedback_rate_limit_exceeded' using errcode = '54000';
  end if;

  insert into public.event_search_feedback(
    user_id,
    operation_id,
    query_hash,
    query_text,
    verdict,
    result_event_ids,
    result_count,
    metadata
  ) values (
    v_user_id,
    v_operation_id,
    v_query_hash,
    v_query,
    v_verdict,
    v_ids,
    least(greatest(coalesce(p_result_count, pg_catalog.array_length(v_ids, 1), 0), 0), 500),
    v_metadata
  ) returning id into v_id;

  if v_verdict = 'matched' then
    insert into public.event_search_tag_candidates(
      query_hash,
      canonical_query,
      slug,
      status,
      source_feedback_count,
      positive_feedback_count,
      negative_feedback_count,
      metadata
    ) values (
      v_query_hash,
      v_query,
      'search-' || pg_catalog.left(v_query_hash, 12),
      'candidate',
      1,
      1,
      0,
      pg_catalog.jsonb_build_object('source', 'event_search_feedback', 'needs_llm_canonicalization', true)
    )
    on conflict (query_hash) do update set
      source_feedback_count = public.event_search_tag_candidates.source_feedback_count + 1,
      positive_feedback_count = public.event_search_tag_candidates.positive_feedback_count + 1,
      updated_at = pg_catalog.now(),
      metadata = public.event_search_tag_candidates.metadata || pg_catalog.jsonb_build_object('last_feedback_id', v_id, 'needs_llm_canonicalization', true);
  else
    update public.event_search_tag_candidates
    set negative_feedback_count = negative_feedback_count + 1,
        source_feedback_count = source_feedback_count + 1,
        updated_at = pg_catalog.now(),
        metadata = metadata || pg_catalog.jsonb_build_object('last_negative_feedback_id', v_id)
    where query_hash = v_query_hash;
  end if;

  return v_id;
end;
$$;

-- Revoke every overload left by historical migrations, then expose only the
-- minimal status/feedback surface to authenticated browser clients.
do $$
declare
  v_function regprocedure;
begin
  for v_function in
    select p.oid::regprocedure
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.proname in (
        'search_events_by_embedding_v1',
        'event_search_fallback_cards_v1',
        'reserve_event_search_quota_v1',
        'reserve_event_search_quota_v2',
        'reserve_event_search_quota_v3',
        'record_event_search_request_v1',
        'get_event_search_quota_v1'
      )
  loop
    execute pg_catalog.format(
      'revoke all on function %s from public, anon, authenticated',
      v_function
    );
    execute pg_catalog.format(
      'grant execute on function %s to service_role',
      v_function
    );
  end loop;
end;
$$;

revoke all on function public.get_event_search_quota_internal_v1(uuid, text, timestamptz) from public, anon, authenticated;
revoke all on function public.reserve_event_search_quota_internal_v1(uuid, uuid, text, boolean, timestamptz) from public, anon, authenticated;
revoke all on function public.search_events_by_embedding_internal_v1(uuid, extensions.vector, integer, integer, date, date, text, text, text, integer, smallint, text, text, text) from public, anon, authenticated;
revoke all on function public.event_search_fallback_cards_internal_v1(uuid, integer, integer, date) from public, anon, authenticated;
revoke all on function public.record_event_search_request_internal_v1(uuid, text, text, integer, integer, boolean, text, text, jsonb) from public, anon, authenticated;

grant execute on function public.get_event_search_quota_internal_v1(uuid, text, timestamptz) to service_role;
grant execute on function public.reserve_event_search_quota_internal_v1(uuid, uuid, text, boolean, timestamptz) to service_role;
grant execute on function public.search_events_by_embedding_internal_v1(uuid, extensions.vector, integer, integer, date, date, text, text, text, integer, smallint, text, text, text) to service_role;
grant execute on function public.event_search_fallback_cards_internal_v1(uuid, integer, integer, date) to service_role;
grant execute on function public.record_event_search_request_internal_v1(uuid, text, text, integer, integer, boolean, text, text, jsonb) to service_role;

revoke all on function public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb, uuid) from public, anon, authenticated;
grant execute on function public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb, uuid) to authenticated;
grant execute on function public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb, uuid) to service_role;

-- Keep exactly one browser-visible read-only quota API.
revoke all on function public.get_event_search_quota_v2(text, timestamptz) from public, anon, authenticated;
grant execute on function public.get_event_search_quota_v2(text, timestamptz) to authenticated;
grant execute on function public.get_event_search_quota_v2(text, timestamptz) to service_role;

notify pgrst, 'reload schema';
