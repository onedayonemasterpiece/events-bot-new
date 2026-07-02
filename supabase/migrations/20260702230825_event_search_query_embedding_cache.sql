-- Cache online query embeddings for repeated authorized static-site searches.
-- Stores only salted query hashes, never raw user query text.

create extension if not exists vector with schema extensions;

create table if not exists public.event_search_query_embeddings (
  query_hash text not null,
  embedding_model text not null,
  embedding_dim smallint not null default 768 check (embedding_dim = 768),
  embedding extensions.vector(768) not null,
  hit_count integer not null default 0 check (hit_count >= 0),
  created_at timestamptz not null default now(),
  last_used_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  primary key (query_hash, embedding_model, embedding_dim),
  constraint event_search_query_embeddings_hash_size_chk check (length(query_hash) between 32 and 128),
  constraint event_search_query_embeddings_model_size_chk check (length(embedding_model) between 3 and 120),
  constraint event_search_query_embeddings_metadata_size_chk check (length(metadata::text) <= 2048)
);

alter table public.event_search_query_embeddings enable row level security;

revoke all on public.event_search_query_embeddings from anon, authenticated;

grant select, insert, update, delete on public.event_search_query_embeddings to service_role;

create index if not exists event_search_query_embeddings_last_used_idx
  on public.event_search_query_embeddings (last_used_at desc);

create or replace function public.get_event_search_query_embedding_v1(
  p_query_hash text,
  p_embedding_model text,
  p_embedding_dim integer default 768
)
returns table (
  embedding extensions.vector(768),
  hit_count integer,
  last_used_at timestamptz
)
language plpgsql
security definer
set search_path = public, extensions
as $$
begin
  return query
  update public.event_search_query_embeddings q
     set hit_count = q.hit_count + 1,
         last_used_at = now()
   where q.query_hash = p_query_hash
     and q.embedding_model = p_embedding_model
     and q.embedding_dim = p_embedding_dim
  returning q.embedding, q.hit_count, q.last_used_at;
end;
$$;

create or replace function public.upsert_event_search_query_embedding_v1(
  p_query_hash text,
  p_embedding_model text,
  p_embedding_dim integer,
  p_embedding extensions.vector(768),
  p_metadata jsonb default '{}'::jsonb
)
returns void
language plpgsql
security definer
set search_path = public, extensions
as $$
begin
  insert into public.event_search_query_embeddings (
    query_hash,
    embedding_model,
    embedding_dim,
    embedding,
    hit_count,
    metadata,
    created_at,
    last_used_at
  ) values (
    p_query_hash,
    p_embedding_model,
    p_embedding_dim,
    p_embedding,
    0,
    coalesce(p_metadata, '{}'::jsonb),
    now(),
    now()
  )
  on conflict (query_hash, embedding_model, embedding_dim) do update set
    embedding = excluded.embedding,
    metadata = excluded.metadata,
    last_used_at = now();
end;
$$;

revoke all on function public.get_event_search_query_embedding_v1(text, text, integer) from public;
revoke all on function public.upsert_event_search_query_embedding_v1(text, text, integer, extensions.vector(768), jsonb) from public;
grant execute on function public.get_event_search_query_embedding_v1(text, text, integer) to service_role;
grant execute on function public.upsert_event_search_query_embedding_v1(text, text, integer, extensions.vector(768), jsonb) to service_role;
