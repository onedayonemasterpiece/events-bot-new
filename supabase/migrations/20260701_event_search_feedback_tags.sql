-- Capture authorized search result feedback and queue reusable search-tag candidates.
-- This migration intentionally keeps raw feedback private: browser calls the RPC,
-- public/anon roles never get table access.

create table if not exists public.event_search_feedback (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null,
  query_hash text not null,
  query_text text not null,
  verdict text not null check (verdict in ('matched', 'missed')),
  result_event_ids bigint[] not null default '{}'::bigint[],
  result_count integer not null default 0 check (result_count >= 0),
  status text not null default 'queued' check (status in ('queued', 'processing', 'processed', 'ignored', 'error')),
  created_at timestamptz not null default now(),
  processed_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  constraint event_search_feedback_query_text_size_chk check (length(query_text) between 3 and 160),
  constraint event_search_feedback_metadata_size_chk check (length(metadata::text) <= 4096)
);

create table if not exists public.event_search_tag_candidates (
  id uuid primary key default extensions.gen_random_uuid(),
  query_hash text not null unique,
  canonical_query text not null,
  slug text not null unique,
  status text not null default 'candidate' check (status in ('candidate', 'approved', 'published', 'rejected', 'merged')),
  source_feedback_count integer not null default 1 check (source_feedback_count >= 0),
  positive_feedback_count integer not null default 1 check (positive_feedback_count >= 0),
  negative_feedback_count integer not null default 0 check (negative_feedback_count >= 0),
  page_path text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint event_search_tag_candidates_query_size_chk check (length(canonical_query) between 3 and 160),
  constraint event_search_tag_candidates_slug_size_chk check (length(slug) between 8 and 80),
  constraint event_search_tag_candidates_metadata_size_chk check (length(metadata::text) <= 4096)
);

create index if not exists event_search_feedback_user_created_idx
  on public.event_search_feedback (user_id, created_at desc);
create index if not exists event_search_feedback_status_created_idx
  on public.event_search_feedback (status, created_at desc);
create index if not exists event_search_tag_candidates_status_updated_idx
  on public.event_search_tag_candidates (status, updated_at desc);

alter table public.event_search_feedback enable row level security;
alter table public.event_search_tag_candidates enable row level security;

revoke all on public.event_search_feedback from anon, authenticated;
revoke all on public.event_search_tag_candidates from anon, authenticated;
grant select, insert, update, delete on public.event_search_feedback to service_role;
grant select, insert, update, delete on public.event_search_tag_candidates to service_role;

drop policy if exists event_search_feedback_service_all on public.event_search_feedback;
create policy event_search_feedback_service_all
  on public.event_search_feedback
  for all
  to service_role
  using (true)
  with check (true);

drop policy if exists event_search_tag_candidates_service_all on public.event_search_tag_candidates;
create policy event_search_tag_candidates_service_all
  on public.event_search_tag_candidates
  for all
  to service_role
  using (true)
  with check (true);

create or replace function public.record_event_search_feedback_v1(
  p_query text,
  p_verdict text,
  p_result_event_ids bigint[] default '{}'::bigint[],
  p_result_count integer default 0,
  p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_query text := left(regexp_replace(trim(coalesce(p_query, '')), '[[:cntrl:]<>]+', ' ', 'g'), 160);
  v_verdict text := lower(trim(coalesce(p_verdict, '')));
  v_query_hash text;
  v_id uuid;
  v_ids bigint[] := coalesce(p_result_event_ids, '{}'::bigint[]);
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  v_query := regexp_replace(v_query, '\s+', ' ', 'g');
  if length(v_query) < 3 then
    raise exception 'query_too_short' using errcode = '22023';
  end if;

  if v_verdict not in ('matched', 'missed') then
    raise exception 'invalid_verdict' using errcode = '22023';
  end if;

  v_query_hash := encode(digest(lower(v_query), 'sha256'), 'hex');
  v_ids := (select coalesce(array_agg(distinct id), '{}'::bigint[]) from unnest(v_ids[1:40]) as id where id is not null and id > 0);

  insert into public.event_search_feedback(
    user_id,
    query_hash,
    query_text,
    verdict,
    result_event_ids,
    result_count,
    metadata
  ) values (
    v_user_id,
    v_query_hash,
    v_query,
    v_verdict,
    v_ids,
    least(greatest(coalesce(p_result_count, array_length(v_ids, 1), 0), 0), 500),
    coalesce(p_metadata, '{}'::jsonb)
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
      'search-' || left(v_query_hash, 12),
      'candidate',
      1,
      1,
      0,
      jsonb_build_object('source', 'event_search_feedback', 'needs_llm_canonicalization', true)
    )
    on conflict (query_hash) do update set
      source_feedback_count = public.event_search_tag_candidates.source_feedback_count + 1,
      positive_feedback_count = public.event_search_tag_candidates.positive_feedback_count + 1,
      updated_at = now(),
      metadata = public.event_search_tag_candidates.metadata || jsonb_build_object('last_feedback_id', v_id, 'needs_llm_canonicalization', true);
  else
    update public.event_search_tag_candidates
      set negative_feedback_count = negative_feedback_count + 1,
          source_feedback_count = source_feedback_count + 1,
          updated_at = now(),
          metadata = metadata || jsonb_build_object('last_negative_feedback_id', v_id)
      where query_hash = v_query_hash;
  end if;

  return v_id;
end;
$$;

revoke all on function public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb) from public, anon, authenticated;
grant execute on function public.record_event_search_feedback_v1(text, text, bigint[], integer, jsonb) to authenticated;
