-- KenigEvents authorized search query-facet boost.
-- Weekday/time/admission words in an explicit user query should influence the
-- pgvector result order without turning the public page-view path into an
-- online search path. The RPC still returns trusted card snapshots only.

drop function if exists public.search_events_by_embedding_v1(
  extensions.vector,
  integer,
  integer,
  date,
  date,
  text,
  text,
  text,
  integer
);

create or replace function public.search_events_by_embedding_v1(
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
  p_admission_filter text default null
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
declare
  v_match_count integer := least(greatest(coalesce(p_match_count, 24), 1), 60);
  v_offset_count integer := greatest(coalesce(p_offset_count, 0), 0);
  v_candidate_count integer := least(greatest(v_offset_count + v_match_count * 6, 80), 240);
  v_time_of_day_filter text := nullif(p_time_of_day_filter, '');
  v_admission_filter text := nullif(p_admission_filter, '');
begin
  if (select auth.uid()) is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  if p_weekday_iso is not null and (p_weekday_iso < 1 or p_weekday_iso > 7) then
    raise exception 'weekday_iso must be between 1 and 7' using errcode = '22023';
  end if;

  if v_time_of_day_filter is not null and v_time_of_day_filter not in ('morning', 'day', 'evening', 'night') then
    raise exception 'unsupported time_of_day_filter' using errcode = '22023';
  end if;

  if v_admission_filter is not null and v_admission_filter not in ('free', 'registration_required', 'paid') then
    raise exception 'unsupported admission_filter' using errcode = '22023';
  end if;

  perform set_config('hnsw.ef_search', '120', true);

  return query
  with nearest as materialized (
    select
      d.event_id as event_id,
      d.title as title,
      d.category as category,
      d.tags as tags,
      d.city as city,
      d.start_date as start_date,
      d.weekday_iso as weekday_iso,
      d.time_of_day as time_of_day,
      d.admission_type as admission_type,
      d.ticket_kind as ticket_kind,
      d.is_free as is_free,
      d.card_snapshot as card_snapshot,
      (e.embedding <=> p_query_embedding)::double precision as distance
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
    limit v_candidate_count
  ),
  scored as (
    select
      n.*,
      (
        case when p_weekday_iso is not null and n.weekday_iso = p_weekday_iso then 0.08 else 0 end
        + case when v_time_of_day_filter is not null and n.time_of_day = v_time_of_day_filter then 0.04 else 0 end
        + case
            when v_admission_filter = 'free'
              and (n.is_free or n.admission_type = 'free' or n.ticket_kind = 'free')
              then 0.05
            when v_admission_filter = 'registration_required'
              and (n.admission_type = 'registration_required' or n.ticket_kind in ('registration', 'registration_required'))
              then 0.05
            when v_admission_filter = 'paid'
              and not n.is_free
              and (n.admission_type in ('paid', 'ticket', 'tickets') or n.ticket_kind in ('paid', 'ticket', 'tickets'))
              then 0.03
            else 0
          end
      )::double precision as query_facet_boost
    from nearest n
  )
  select
    s.event_id,
    s.distance,
    greatest(0, least(1, (1 - s.distance) + s.query_facet_boost))::double precision as similarity,
    s.title,
    s.category,
    s.tags,
    s.city,
    s.start_date,
    s.card_snapshot
  from scored s
  order by
    ((1 - s.distance) + s.query_facet_boost) desc,
    s.start_date asc nulls last,
    s.event_id
  limit v_match_count
  offset v_offset_count;
end;
$$;

revoke all on function public.search_events_by_embedding_v1(
  extensions.vector,
  integer,
  integer,
  date,
  date,
  text,
  text,
  text,
  integer,
  smallint,
  text,
  text
) from public, anon, authenticated;
grant execute on function public.search_events_by_embedding_v1(
  extensions.vector,
  integer,
  integer,
  date,
  date,
  text,
  text,
  text,
  integer,
  smallint,
  text,
  text
) to authenticated;
