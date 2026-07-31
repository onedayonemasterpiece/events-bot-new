-- Compact, backend-only candidate retrieval for static related-page builds.
--
-- The legacy RPC intentionally remains available for its existing callers,
-- but it returns title/tags/card_snapshot fields that the static exporter never
-- consumes.  This dedicated projection keeps the HNSW ranking contract while
-- preventing those document-sized columns from becoming build egress.

create or replace function public.event_related_candidates_compact_by_event_id_v1(
  p_anchor_event_id bigint,
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768,
  p_match_count integer default 60,
  p_date_from date default current_date,
  p_date_to date default null,
  p_embedding_doc_kind text default 'related_v1'
)
returns table (
  event_id bigint,
  vector_similarity double precision
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
    d.event_id,
    (1 - (e.embedding <=> v_anchor_embedding))::double precision as vector_similarity
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

revoke all on function public.event_related_candidates_compact_by_event_id_v1(
  bigint, text, integer, integer, date, date, text
) from public, anon, authenticated;
grant execute on function public.event_related_candidates_compact_by_event_id_v1(
  bigint, text, integer, integer, date, date, text
) to service_role;
