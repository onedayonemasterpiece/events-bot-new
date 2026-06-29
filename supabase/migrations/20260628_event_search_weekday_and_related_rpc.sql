-- KenigEvents pgvector related retrieval hardening.
-- Adds compact weekday facets to search docs and a backend-only related RPC for
-- static-site/Kaggle builders. Browser roles must not call this RPC directly.

alter table public.event_search_documents
  add column if not exists weekday_iso smallint check (weekday_iso between 1 and 7),
  add column if not exists weekday_ru text;

create index if not exists event_search_documents_weekday_idx
  on public.event_search_documents (weekday_iso)
  where active;

create or replace function public.event_related_candidates_by_event_id_v1(
  p_anchor_event_id bigint,
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768,
  p_match_count integer default 60,
  p_date_from date default current_date,
  p_date_to date default null
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
begin
  select e.embedding, d.start_date
    into v_anchor_embedding, v_anchor_start_date
  from public.event_embeddings e
  join public.event_search_documents d on d.event_id = e.event_id
  where e.event_id = p_anchor_event_id
    and e.embedding_model = coalesce(nullif(p_embedding_model, ''), 'gemini-embedding-2')
    and e.embedding_dim = coalesce(p_embedding_dim, 768)
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

revoke all on function public.event_related_candidates_by_event_id_v1(bigint, text, integer, integer, date, date) from public, anon, authenticated;
grant execute on function public.event_related_candidates_by_event_id_v1(bigint, text, integer, integer, date, date) to service_role;
