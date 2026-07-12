-- KenigEvents pgvector search hardening.
-- Adds compact public/search facets used by the authorized search endpoint and
-- makes the online vector-search RPC model/dimension scoped so embedding spaces
-- can never be mixed accidentally.

alter table public.event_search_documents
  add column if not exists slug text,
  add column if not exists canonical_path text,
  add column if not exists date_local date,
  add column if not exists ends_at timestamptz,
  add column if not exists timezone text not null default 'Europe/Kaliningrad',
  add column if not exists is_weekend boolean,
  add column if not exists time_of_day text check (time_of_day is null or time_of_day in ('morning', 'day', 'evening', 'night')),
  add column if not exists admission_type text,
  add column if not exists availability_status text not null default 'unknown',
  add column if not exists is_public boolean not null default true,
  add column if not exists is_searchable boolean not null default true;

create index if not exists event_search_documents_searchable_date_idx
  on public.event_search_documents (is_public, is_searchable, active, date_local)
  where active;

create index if not exists event_search_documents_time_of_day_idx
  on public.event_search_documents (time_of_day)
  where active and is_searchable;

create index if not exists event_search_documents_availability_idx
  on public.event_search_documents (availability_status)
  where active and is_searchable;

drop function if exists public.search_events_by_embedding_v1(extensions.vector, integer, integer, date, date, text, text);

create or replace function public.search_events_by_embedding_v1(
  p_query_embedding extensions.vector(768),
  p_match_count integer default 24,
  p_offset_count integer default 0,
  p_date_from date default current_date,
  p_date_to date default null,
  p_city_filter text default null,
  p_category_filter text default null,
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768
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
  where e.embedding_model = coalesce(nullif(p_embedding_model, ''), 'gemini-embedding-2')
    and e.embedding_dim = coalesce(p_embedding_dim, 768)
    and d.active
    and d.is_public
    and d.is_searchable
    and d.lifecycle_status = 'active'
    and d.availability_status not in ('cancelled', 'postponed')
    and (p_date_from is null or coalesce(d.end_date, d.start_date) >= p_date_from)
    and (p_date_to is null or d.start_date <= p_date_to)
    and (p_city_filter is null or d.city = p_city_filter)
    and (p_category_filter is null or d.category = p_category_filter)
  order by e.embedding <=> p_query_embedding
  limit least(greatest(coalesce(p_match_count, 24), 1), 60)
  offset greatest(coalesce(p_offset_count, 0), 0);
end;
$$;

revoke all on function public.search_events_by_embedding_v1(extensions.vector, integer, integer, date, date, text, text, text, integer) from public, anon, authenticated;
grant execute on function public.search_events_by_embedding_v1(extensions.vector, integer, integer, date, date, text, text, text, integer) to authenticated;

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
    and d.is_public
    and d.is_searchable
    and d.lifecycle_status = 'active'
    and d.availability_status not in ('cancelled', 'postponed')
    and (p_date_from is null or coalesce(d.end_date, d.start_date) >= p_date_from)
  order by coalesce(c.likes_count, 0) desc, d.start_date asc nulls last, d.event_id desc
  limit least(greatest(coalesce(p_match_count, 24), 1), 60)
  offset greatest(coalesce(p_offset_count, 0), 0);
end;
$$;

revoke all on function public.event_search_fallback_cards_v1(integer, integer, date) from public, anon, authenticated;
grant execute on function public.event_search_fallback_cards_v1(integer, integer, date) to authenticated;
