-- KenigEvents two-document pgvector hardening.
--
-- Keep one embedding model/dimension, but separate document representations:
-- - search_v3: rich query document with calendar/admission/audience facets
-- - related_v1: cleaner event-to-event related document without calendar/price noise

alter table public.event_search_documents
  add column if not exists related_doc_version text not null default 'event-related-doc-v1',
  add column if not exists related_digest text,
  add column if not exists related_text_hash text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.event_search_documents'::regclass
      and conname = 'event_search_documents_related_digest_size_chk'
  ) then
    alter table public.event_search_documents
      add constraint event_search_documents_related_digest_size_chk
        check (related_digest is null or length(related_digest) <= 8000);
  end if;
end $$;

alter table public.event_embeddings
  add column if not exists embedding_doc_kind text not null default 'search_v3';

alter table public.event_embeddings
  drop constraint if exists event_embeddings_doc_kind_chk;

alter table public.event_embeddings
  add constraint event_embeddings_doc_kind_chk
    check (embedding_doc_kind in ('search_v3', 'related_v1'));

alter table public.event_embeddings
  drop constraint if exists event_embeddings_pkey;

alter table public.event_embeddings
  add constraint event_embeddings_pkey
    primary key (event_id, embedding_model, embedding_dim, embedding_doc_kind);

create index if not exists event_embeddings_search_v3_hnsw_idx
  on public.event_embeddings
  using hnsw (embedding extensions.vector_cosine_ops)
  with (m = 16, ef_construction = 128)
  where embedding_doc_kind = 'search_v3';

create index if not exists event_embeddings_related_v1_hnsw_idx
  on public.event_embeddings
  using hnsw (embedding extensions.vector_cosine_ops)
  with (m = 16, ef_construction = 128)
  where embedding_doc_kind = 'related_v1';

drop function if exists public.search_events_by_embedding_v1(
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
set search_path = public, extensions, pg_temp
as $$
declare
  v_match_count integer := least(greatest(coalesce(p_match_count, 24), 1), 60);
  v_offset_count integer := greatest(coalesce(p_offset_count, 0), 0);
  v_candidate_count integer := least(greatest(v_offset_count + v_match_count * 6, 80), 240);
  v_time_of_day_filter text := nullif(p_time_of_day_filter, '');
  v_admission_filter text := nullif(p_admission_filter, '');
  v_doc_kind text := coalesce(nullif(p_embedding_doc_kind, ''), 'search_v3');
begin
  if (select auth.uid()) is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  if v_doc_kind not in ('search_v3', 'related_v1') then
    raise exception 'unsupported embedding_doc_kind' using errcode = '22023';
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
      and e.embedding_doc_kind = v_doc_kind
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
  text,
  text
) to authenticated;

drop function if exists public.event_related_candidates_by_event_id_v1(bigint, text, integer, integer, date, date);

create or replace function public.event_related_candidates_by_event_id_v1(
  p_anchor_event_id bigint,
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768,
  p_match_count integer default 60,
  p_date_from date default current_date,
  p_date_to date default null,
  p_embedding_doc_kind text default 'related_v1'
)
returns table (
  anchor_event_id bigint,
  event_id bigint,
  distance double precision,
  vector_similarity double precision,
  title text,
  category text,
  tags text[],
  city text,
  start_date date,
  weekday_iso smallint,
  weekday_ru text,
  card_snapshot jsonb
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_anchor_embedding extensions.vector(768);
  v_anchor_start_date date;
  v_doc_kind text := coalesce(nullif(p_embedding_doc_kind, ''), 'related_v1');
begin
  if v_doc_kind not in ('search_v3', 'related_v1') then
    raise exception 'unsupported embedding_doc_kind' using errcode = '22023';
  end if;

  select e.embedding, d.start_date
    into v_anchor_embedding, v_anchor_start_date
  from public.event_embeddings e
  join public.event_search_documents d on d.event_id = e.event_id
  where e.event_id = p_anchor_event_id
    and e.embedding_model = coalesce(nullif(p_embedding_model, ''), 'gemini-embedding-2')
    and e.embedding_dim = coalesce(p_embedding_dim, 768)
    and e.embedding_doc_kind = v_doc_kind
    and d.active
    and d.lifecycle_status = 'active'
  limit 1;

  if v_anchor_embedding is null then
    raise exception 'anchor event embedding is missing' using errcode = 'P0001';
  end if;

  perform set_config('hnsw.ef_search', '120', true);

  return query
  select
    p_anchor_event_id as anchor_event_id,
    d.event_id,
    (e.embedding <=> v_anchor_embedding)::double precision as distance,
    (1 - (e.embedding <=> v_anchor_embedding))::double precision as vector_similarity,
    d.title,
    d.category,
    d.tags,
    d.city,
    d.start_date,
    d.weekday_iso,
    d.weekday_ru,
    d.card_snapshot
  from public.event_embeddings e
  join public.event_search_documents d on d.event_id = e.event_id
  where e.event_id <> p_anchor_event_id
    and e.embedding_model = coalesce(nullif(p_embedding_model, ''), 'gemini-embedding-2')
    and e.embedding_dim = coalesce(p_embedding_dim, 768)
    and e.embedding_doc_kind = v_doc_kind
    and d.active
    and d.lifecycle_status = 'active'
    and (p_date_from is null or coalesce(d.end_date, d.start_date) >= p_date_from)
    and (p_date_to is null or d.start_date <= p_date_to)
  order by e.embedding <=> v_anchor_embedding,
           abs(coalesce(d.start_date, v_anchor_start_date) - v_anchor_start_date),
           d.event_id
  limit least(greatest(coalesce(p_match_count, 60), 1), 120);
end;
$$;

revoke all on function public.event_related_candidates_by_event_id_v1(bigint, text, integer, integer, date, date, text) from public, anon, authenticated;
grant execute on function public.event_related_candidates_by_event_id_v1(bigint, text, integer, integer, date, date, text) to service_role;
