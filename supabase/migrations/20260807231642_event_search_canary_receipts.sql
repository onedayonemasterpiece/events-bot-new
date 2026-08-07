-- Server-owned receipts for unattended Search canaries.  The browser only
-- receives a narrow owner-scoped projection through get_event_search_receipt_v1;
-- all mutation, revision and budget primitives remain service-role only.

create table public.event_search_canary_principals (
  user_id uuid primary key references auth.users(id) on delete cascade,
  enabled boolean not null default true,
  note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint event_search_canary_principals_note_size_chk
    check (length(coalesce(note, '')) <= 200)
);

create table public.event_search_canary_budget_policy (
  policy_id text primary key,
  daily_llm_attempt_limit integer not null check (daily_llm_attempt_limit between 0 and 100),
  policy_version text not null,
  enabled boolean not null default true,
  updated_at timestamptz not null default now(),
  constraint event_search_canary_budget_policy_id_chk
    check (policy_id = 'default'),
  constraint event_search_canary_budget_policy_version_size_chk
    check (length(policy_version) between 1 and 80)
);

insert into public.event_search_canary_budget_policy (
  policy_id,
  daily_llm_attempt_limit,
  policy_version
) values (
  'default',
  4,
  'event-search-canary-llm-budget-v1'
)
on conflict (policy_id) do nothing;

-- Product-user limits are deliberately not borrowed by unattended canaries.
-- LLM provider attempts are governed separately by the stricter atomic ledger
-- below, while this plan only bounds cold Search orchestration volume.
insert into public.search_quota_plans (
  plan_id,
  hourly_search_limit,
  daily_search_limit,
  monthly_search_limit,
  hourly_llm_rerank_limit,
  daily_llm_rerank_limit,
  monthly_llm_rerank_limit,
  enabled
) values (
  'search_canary', 16, 32, 320, 0, 0, 0, true
)
on conflict (plan_id) do update set
  hourly_search_limit = excluded.hourly_search_limit,
  daily_search_limit = excluded.daily_search_limit,
  monthly_search_limit = excluded.monthly_search_limit,
  hourly_llm_rerank_limit = excluded.hourly_llm_rerank_limit,
  daily_llm_rerank_limit = excluded.daily_llm_rerank_limit,
  monthly_llm_rerank_limit = excluded.monthly_llm_rerank_limit,
  enabled = true,
  updated_at = now();

create table public.event_search_canary_llm_budget_ledger (
  user_id uuid not null references auth.users(id) on delete cascade,
  budget_date date not null,
  attempts_used integer not null default 0 check (attempts_used >= 0),
  budget_limit integer not null check (budget_limit >= 0),
  policy_version text not null,
  first_reserved_at timestamptz not null default now(),
  last_reserved_at timestamptz not null default now(),
  primary key (user_id, budget_date),
  constraint event_search_canary_llm_ledger_policy_size_chk
    check (length(policy_version) between 1 and 80),
  constraint event_search_canary_llm_ledger_limit_chk
    check (attempts_used <= budget_limit)
);

create table public.event_search_canary_llm_budget_operation (
  user_id uuid not null references auth.users(id) on delete cascade,
  operation_id uuid not null,
  client_request_id uuid not null,
  budget_date date not null,
  attempts_reserved integer not null check (attempts_reserved between 1 and 10),
  created_at timestamptz not null default now(),
  primary key (user_id, operation_id)
);

create index event_search_canary_llm_operation_client_idx
  on public.event_search_canary_llm_budget_operation
  (user_id, client_request_id, created_at desc);
create index event_search_canary_llm_operation_created_idx
  on public.event_search_canary_llm_budget_operation (created_at);

create table public.event_search_canary_receipts (
  receipt_id uuid primary key default extensions.gen_random_uuid(),
  request_id uuid not null,
  client_request_id uuid not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  search_contract_version text not null,
  requested_execution_mode text not null,
  actual_execution_mode text not null,
  terminal_status text not null,
  catalog_revision text not null,
  corpus_revision text not null,
  search_document_revision text not null,
  embedding_policy_version text not null,
  llm_policy_version text not null,
  cache_policy_version text not null,
  embedding_provider_attempts integer not null default 0,
  llm_provider_attempts integer not null default 0,
  vector_rpc_attempts integer not null default 0,
  result_cache_read_attempts integer not null default 0,
  result_cache_hit_count integer not null default 0,
  result_cache_write_attempts integer not null default 0,
  query_embedding_cache_read_attempts integer not null default 0,
  query_embedding_cache_hit_count integer not null default 0,
  result_count integer not null default 0,
  response_event_ids bigint[] not null default '{}'::bigint[],
  served_list_id uuid,
  error_code text,
  created_at timestamptz not null default now(),
  terminal_at timestamptz not null default now(),
  unique (user_id, request_id),
  constraint event_search_canary_receipts_requested_mode_chk check (
    requested_execution_mode in (
      'cached_vector',
      'cold_vector',
      'cold_vector_llm',
      'degraded_vector_fallback'
    )
  ),
  constraint event_search_canary_receipts_actual_mode_chk check (
    actual_execution_mode in (
      'cached_vector',
      'cold_vector',
      'cold_vector_llm',
      'degraded_vector_fallback'
    )
  ),
  constraint event_search_canary_receipts_version_size_chk check (
    length(search_contract_version) between 1 and 80
    and length(catalog_revision) between 1 and 128
    and length(corpus_revision) between 1 and 128
    and length(search_document_revision) between 1 and 128
    and length(embedding_policy_version) between 1 and 128
    and length(llm_policy_version) between 1 and 128
    and length(cache_policy_version) between 1 and 128
  ),
  constraint event_search_canary_receipts_terminal_size_chk
    check (length(terminal_status) between 1 and 80),
  constraint event_search_canary_receipts_error_size_chk
    check (length(coalesce(error_code, '')) <= 120),
  constraint event_search_canary_receipts_counters_chk check (
    embedding_provider_attempts >= 0
    and llm_provider_attempts >= 0
    and vector_rpc_attempts >= 0
    and result_cache_read_attempts >= 0
    and result_cache_hit_count between 0 and result_cache_read_attempts
    and result_cache_write_attempts >= 0
    and query_embedding_cache_read_attempts >= 0
    and query_embedding_cache_hit_count between 0 and query_embedding_cache_read_attempts
    and result_count >= 0
    and cardinality(response_event_ids) <= 100
  )
);

-- Broker-compatible, PII-free issuance admission.  This is a claim ledger,
-- not a credential store: it intentionally has no email, action link, token,
-- cookie or serialized session columns.
create table public.static_site_auth_session_issue_claim (
  run_id text not null,
  run_attempt integer not null check (run_attempt between 1 and 1000),
  persona_id text not null,
  repository text not null,
  workflow_ref text not null,
  claimed_at timestamptz not null default now(),
  expires_at timestamptz not null default (now() + interval '20 minutes'),
  primary key (run_id, run_attempt, persona_id),
  constraint static_site_auth_session_claim_size_chk check (
    length(run_id) between 1 and 100
    and length(persona_id) between 1 and 80
    and length(repository) between 3 and 160
    and length(workflow_ref) between 3 and 240
  ),
  constraint static_site_auth_session_claim_expiry_chk
    check (expires_at > claimed_at and expires_at <= claimed_at + interval '1 hour')
);

create index static_site_auth_session_issue_claim_active_idx
  on public.static_site_auth_session_issue_claim (persona_id, expires_at);

create index event_search_canary_receipts_owner_created_idx
  on public.event_search_canary_receipts (user_id, created_at desc);
create index event_search_canary_receipts_created_idx
  on public.event_search_canary_receipts (created_at);

alter table public.event_search_canary_principals enable row level security;
alter table public.event_search_canary_budget_policy enable row level security;
alter table public.event_search_canary_llm_budget_ledger enable row level security;
alter table public.event_search_canary_llm_budget_operation enable row level security;
alter table public.event_search_canary_receipts enable row level security;
alter table public.static_site_auth_session_issue_claim enable row level security;

revoke all on table public.event_search_canary_principals from public, anon, authenticated;
revoke all on table public.event_search_canary_budget_policy from public, anon, authenticated;
revoke all on table public.event_search_canary_llm_budget_ledger from public, anon, authenticated;
revoke all on table public.event_search_canary_llm_budget_operation from public, anon, authenticated;
revoke all on table public.event_search_canary_receipts from public, anon, authenticated;
revoke all on table public.static_site_auth_session_issue_claim from public, anon, authenticated;

grant select, insert, update, delete on table public.event_search_canary_principals to service_role;
grant select, insert, update, delete on table public.event_search_canary_budget_policy to service_role;
grant select, insert, update, delete on table public.event_search_canary_llm_budget_ledger to service_role;
grant select, insert, delete on table public.event_search_canary_llm_budget_operation to service_role;
grant select, insert, update, delete on table public.event_search_canary_receipts to service_role;
grant select, insert, delete on table public.static_site_auth_session_issue_claim to service_role;

create policy "event search canary principals are service-only"
  on public.event_search_canary_principals for all to service_role
  using (true) with check (true);
create policy "event search canary budget policy is service-only"
  on public.event_search_canary_budget_policy for all to service_role
  using (true) with check (true);
create policy "event search canary llm ledger is service-only"
  on public.event_search_canary_llm_budget_ledger for all to service_role
  using (true) with check (true);
create policy "event search canary llm operations are service-only"
  on public.event_search_canary_llm_budget_operation for all to service_role
  using (true) with check (true);
create policy "event search canary receipts are service-only"
  on public.event_search_canary_receipts for all to service_role
  using (true) with check (true);
create policy "static site auth session issue claims are service-only"
  on public.static_site_auth_session_issue_claim for all to service_role
  using (true) with check (true);

create function public.claim_static_site_auth_session_issue_v1(
  p_run_id text,
  p_run_attempt integer,
  p_persona_id text,
  p_repository text,
  p_workflow_ref text,
  p_limit integer default 1
)
returns boolean
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_count integer;
  v_limit integer := least(greatest(coalesce(p_limit, 1), 1), 20);
begin
  if p_run_id is null or length(p_run_id) not between 1 and 100
    or p_run_attempt is null or p_run_attempt not between 1 and 1000
    or p_persona_id is null or length(p_persona_id) not between 1 and 80
    or p_repository is null or length(p_repository) not between 3 and 160
    or p_workflow_ref is null or length(p_workflow_ref) not between 3 and 240 then
    raise exception 'invalid_static_site_auth_session_issue_claim' using errcode = '22023';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(p_persona_id, 1904831207)
  );

  if exists (
    select 1 from public.static_site_auth_session_issue_claim c
    where c.run_id = p_run_id
      and c.run_attempt = p_run_attempt
      and c.persona_id = p_persona_id
  ) then
    return true;
  end if;

  delete from public.static_site_auth_session_issue_claim c
  where c.expires_at < pg_catalog.now() - interval '8 days';

  select pg_catalog.count(*)::integer into v_count
  from public.static_site_auth_session_issue_claim c
  where c.persona_id = p_persona_id
    and c.expires_at > pg_catalog.now();
  if v_count >= v_limit then
    return false;
  end if;

  insert into public.static_site_auth_session_issue_claim (
    run_id, run_attempt, persona_id, repository, workflow_ref
  ) values (
    p_run_id,
    p_run_attempt,
    p_persona_id,
    p_repository,
    p_workflow_ref
  );
  return true;
end;
$$;

create function public.is_event_search_canary_principal_internal_v1(
  p_user_id uuid
)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog
as $$
  select p_user_id is not null and exists (
    select 1
    from auth.users u
    where u.id = p_user_id
      and (
        pg_catalog.lower(coalesce(u.raw_app_meta_data ->> 'search_canary', 'false'))
          in ('1', 'true', 'yes', 'on')
        or pg_catalog.lower(coalesce(u.raw_app_meta_data ->> 'search_persona', ''))
          = 'canary'
        or pg_catalog.lower(coalesce(u.raw_app_meta_data ->> 'role', ''))
          = 'search_canary'
        or exists (
          select 1
          from public.event_search_canary_principals p
          where p.user_id = u.id and p.enabled
        )
      )
  );
$$;

create function public.get_event_search_revision_snapshot_internal_v1(
  p_embedding_model text default 'gemini-embedding-2',
  p_embedding_dim integer default 768,
  p_embedding_doc_kind text default 'search_v3'
)
returns table (
  catalog_revision text,
  corpus_revision text,
  search_document_revision text,
  document_count integer,
  embedding_count integer
)
language plpgsql
stable
security definer
set search_path = pg_catalog
as $$
declare
  v_catalog_revision text;
  v_corpus_revision text;
  v_search_document_revision text;
  v_document_count integer;
  v_embedding_count integer;
begin
  select
    case
      when pg_catalog.count(distinct nullif(d.metadata ->> 'catalog_revision', '')) = 1
        then pg_catalog.max(nullif(d.metadata ->> 'catalog_revision', ''))
      else null
    end,
    case
      when pg_catalog.count(distinct nullif(d.metadata ->> 'search_document_revision', '')) = 1
        then pg_catalog.max(nullif(d.metadata ->> 'search_document_revision', ''))
      when pg_catalog.count(distinct d.search_doc_version) = 1
        then pg_catalog.max(d.search_doc_version)
      else null
    end,
    pg_catalog.count(*)::integer
  into v_catalog_revision, v_search_document_revision, v_document_count
  from public.event_search_documents d
  where d.active and coalesce(d.is_public, true) and coalesce(d.is_searchable, true);

  if v_catalog_revision is null then
    select pg_catalog.encode(
      extensions.digest(
        coalesce(pg_catalog.string_agg(
          d.event_id::text || ':' || d.text_hash || ':' || d.card_snapshot_version,
          ',' order by d.event_id
        ), ''),
        'sha256'
      ),
      'hex'
    ) into v_catalog_revision
    from public.event_search_documents d
    where d.active and coalesce(d.is_public, true) and coalesce(d.is_searchable, true);
  end if;

  if v_search_document_revision is null then
    select pg_catalog.encode(
      extensions.digest(
        coalesce(pg_catalog.string_agg(
          d.event_id::text || ':' || d.text_hash || ':' || d.search_doc_version,
          ',' order by d.event_id
        ), ''),
        'sha256'
      ),
      'hex'
    ) into v_search_document_revision
    from public.event_search_documents d
    where d.active and coalesce(d.is_public, true) and coalesce(d.is_searchable, true);
  end if;

  select
    case
      when pg_catalog.count(distinct nullif(e.metadata ->> 'corpus_revision', '')) = 1
        then pg_catalog.max(nullif(e.metadata ->> 'corpus_revision', ''))
      else null
    end,
    pg_catalog.count(*)::integer
  into v_corpus_revision, v_embedding_count
  from public.event_embeddings e
  join public.event_search_documents d on d.event_id = e.event_id
  where d.active
    and coalesce(d.is_public, true)
    and coalesce(d.is_searchable, true)
    and e.embedding_model = p_embedding_model
    and e.embedding_dim = p_embedding_dim
    and e.embedding_doc_kind = p_embedding_doc_kind;

  if v_corpus_revision is null then
    select pg_catalog.encode(
      extensions.digest(
        coalesce(pg_catalog.string_agg(
          e.event_id::text || ':' || e.text_hash,
          ',' order by e.event_id
        ), ''),
        'sha256'
      ),
      'hex'
    ) into v_corpus_revision
    from public.event_embeddings e
    join public.event_search_documents d on d.event_id = e.event_id
    where d.active
      and coalesce(d.is_public, true)
      and coalesce(d.is_searchable, true)
      and e.embedding_model = p_embedding_model
      and e.embedding_dim = p_embedding_dim
      and e.embedding_doc_kind = p_embedding_doc_kind;
  end if;

  return query select
    coalesce(v_catalog_revision, 'empty'),
    coalesce(v_corpus_revision, 'empty'),
    coalesce(v_search_document_revision, 'empty'),
    coalesce(v_document_count, 0),
    coalesce(v_embedding_count, 0);
end;
$$;

create function public.reserve_event_search_canary_llm_budget_internal_v1(
  p_user_id uuid,
  p_operation_id uuid,
  p_client_request_id uuid,
  p_attempts integer default 1,
  p_now timestamptz default now()
)
returns table (
  budget_date date,
  budget_limit integer,
  attempts_used integer,
  attempts_remaining integer,
  policy_version text,
  reserved boolean
)
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_date date := (pg_catalog.date_trunc('day', p_now at time zone 'UTC'))::date;
  v_policy public.event_search_canary_budget_policy%rowtype;
  v_ledger public.event_search_canary_llm_budget_ledger%rowtype;
  v_operation public.event_search_canary_llm_budget_operation%rowtype;
begin
  if p_user_id is null or p_operation_id is null or p_client_request_id is null
     or p_attempts is null or p_attempts < 1 or p_attempts > 10 then
    raise exception 'invalid_canary_budget_reservation' using errcode = '22023';
  end if;
  if not public.is_event_search_canary_principal_internal_v1(p_user_id) then
    raise exception 'search_canary_persona_required' using errcode = '28000';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(p_user_id::text || ':' || v_date::text, 864204911)
  );

  select * into v_operation
  from public.event_search_canary_llm_budget_operation o
  where o.user_id = p_user_id and o.operation_id = p_operation_id;
  if found then
    select * into v_ledger
    from public.event_search_canary_llm_budget_ledger l
    where l.user_id = p_user_id and l.budget_date = v_operation.budget_date;
    return query select
      v_operation.budget_date,
      v_ledger.budget_limit,
      v_ledger.attempts_used,
      greatest(v_ledger.budget_limit - v_ledger.attempts_used, 0),
      v_ledger.policy_version,
      true;
    return;
  end if;

  select * into v_policy
  from public.event_search_canary_budget_policy p
  where p.policy_id = 'default' and p.enabled;
  if not found then
    raise exception 'search_canary_budget_policy_disabled' using errcode = '55000';
  end if;

  insert into public.event_search_canary_llm_budget_ledger (
    user_id, budget_date, attempts_used, budget_limit, policy_version
  ) values (
    p_user_id, v_date, 0, v_policy.daily_llm_attempt_limit, v_policy.policy_version
  )
  on conflict on constraint event_search_canary_llm_budget_ledger_pkey do update set
    budget_limit = least(
      public.event_search_canary_llm_budget_ledger.budget_limit,
      excluded.budget_limit
    ),
    policy_version = excluded.policy_version
  returning * into v_ledger;

  if v_ledger.attempts_used + p_attempts > v_ledger.budget_limit then
    raise exception 'search_canary_llm_daily_budget_exhausted' using errcode = '54000';
  end if;

  update public.event_search_canary_llm_budget_ledger l
  set attempts_used = l.attempts_used + p_attempts,
      last_reserved_at = pg_catalog.now(),
      policy_version = v_policy.policy_version
  where l.user_id = p_user_id and l.budget_date = v_date
  returning * into v_ledger;

  insert into public.event_search_canary_llm_budget_operation (
    user_id, operation_id, client_request_id, budget_date, attempts_reserved
  ) values (
    p_user_id, p_operation_id, p_client_request_id, v_date, p_attempts
  );

  delete from public.event_search_canary_llm_budget_operation o
  where o.user_id = p_user_id
    and o.created_at < pg_catalog.now() - interval '8 days';

  return query select
    v_date,
    v_ledger.budget_limit,
    v_ledger.attempts_used,
    greatest(v_ledger.budget_limit - v_ledger.attempts_used, 0),
    v_ledger.policy_version,
    true;
end;
$$;

create function public.record_event_search_canary_receipt_internal_v1(
  p_user_id uuid,
  p_request_id uuid,
  p_client_request_id uuid,
  p_search_contract_version text,
  p_requested_execution_mode text,
  p_actual_execution_mode text,
  p_terminal_status text,
  p_catalog_revision text,
  p_corpus_revision text,
  p_search_document_revision text,
  p_embedding_policy_version text,
  p_llm_policy_version text,
  p_cache_policy_version text,
  p_embedding_provider_attempts integer default 0,
  p_llm_provider_attempts integer default 0,
  p_vector_rpc_attempts integer default 0,
  p_result_cache_read_attempts integer default 0,
  p_result_cache_hit_count integer default 0,
  p_result_cache_write_attempts integer default 0,
  p_query_embedding_cache_read_attempts integer default 0,
  p_query_embedding_cache_hit_count integer default 0,
  p_result_count integer default 0,
  p_response_event_ids bigint[] default '{}'::bigint[],
  p_served_list_id uuid default null,
  p_error_code text default null
)
returns uuid
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_receipt_id uuid;
  v_ids bigint[];
begin
  if not public.is_event_search_canary_principal_internal_v1(p_user_id) then
    raise exception 'search_canary_persona_required' using errcode = '28000';
  end if;
  if p_request_id is null or p_client_request_id is null then
    raise exception 'search_canary_request_ids_required' using errcode = '22023';
  end if;

  v_ids := (
    select coalesce(pg_catalog.array_agg(distinct id order by id), '{}'::bigint[])
    from pg_catalog.unnest((coalesce(p_response_event_ids, '{}'::bigint[]))[1:100]) id
    where id is not null and id > 0
  );

  insert into public.event_search_canary_receipts (
    request_id,
    client_request_id,
    user_id,
    search_contract_version,
    requested_execution_mode,
    actual_execution_mode,
    terminal_status,
    catalog_revision,
    corpus_revision,
    search_document_revision,
    embedding_policy_version,
    llm_policy_version,
    cache_policy_version,
    embedding_provider_attempts,
    llm_provider_attempts,
    vector_rpc_attempts,
    result_cache_read_attempts,
    result_cache_hit_count,
    result_cache_write_attempts,
    query_embedding_cache_read_attempts,
    query_embedding_cache_hit_count,
    result_count,
    response_event_ids,
    served_list_id,
    error_code,
    terminal_at
  ) values (
    p_request_id,
    p_client_request_id,
    p_user_id,
    pg_catalog.left(coalesce(nullif(p_search_contract_version, ''), 'missing'), 80),
    p_requested_execution_mode,
    p_actual_execution_mode,
    pg_catalog.left(coalesce(nullif(p_terminal_status, ''), 'unknown'), 80),
    pg_catalog.left(coalesce(nullif(p_catalog_revision, ''), 'missing'), 128),
    pg_catalog.left(coalesce(nullif(p_corpus_revision, ''), 'missing'), 128),
    pg_catalog.left(coalesce(nullif(p_search_document_revision, ''), 'missing'), 128),
    pg_catalog.left(coalesce(nullif(p_embedding_policy_version, ''), 'missing'), 128),
    pg_catalog.left(coalesce(nullif(p_llm_policy_version, ''), 'missing'), 128),
    pg_catalog.left(coalesce(nullif(p_cache_policy_version, ''), 'missing'), 128),
    greatest(coalesce(p_embedding_provider_attempts, 0), 0),
    greatest(coalesce(p_llm_provider_attempts, 0), 0),
    greatest(coalesce(p_vector_rpc_attempts, 0), 0),
    greatest(coalesce(p_result_cache_read_attempts, 0), 0),
    greatest(coalesce(p_result_cache_hit_count, 0), 0),
    greatest(coalesce(p_result_cache_write_attempts, 0), 0),
    greatest(coalesce(p_query_embedding_cache_read_attempts, 0), 0),
    greatest(coalesce(p_query_embedding_cache_hit_count, 0), 0),
    greatest(coalesce(p_result_count, cardinality(v_ids), 0), 0),
    v_ids,
    p_served_list_id,
    nullif(pg_catalog.left(coalesce(p_error_code, ''), 120), ''),
    pg_catalog.now()
  )
  on conflict (user_id, request_id) do update set
    terminal_status = excluded.terminal_status,
    actual_execution_mode = excluded.actual_execution_mode,
    embedding_provider_attempts = excluded.embedding_provider_attempts,
    llm_provider_attempts = excluded.llm_provider_attempts,
    vector_rpc_attempts = excluded.vector_rpc_attempts,
    result_cache_read_attempts = excluded.result_cache_read_attempts,
    result_cache_hit_count = excluded.result_cache_hit_count,
    result_cache_write_attempts = excluded.result_cache_write_attempts,
    query_embedding_cache_read_attempts = excluded.query_embedding_cache_read_attempts,
    query_embedding_cache_hit_count = excluded.query_embedding_cache_hit_count,
    result_count = excluded.result_count,
    response_event_ids = excluded.response_event_ids,
    served_list_id = excluded.served_list_id,
    error_code = excluded.error_code,
    terminal_at = excluded.terminal_at
  returning receipt_id into v_receipt_id;

  return v_receipt_id;
end;
$$;

create function public.get_event_search_receipt_v1(
  p_request_id uuid
)
returns table (
  receipt_id uuid,
  request_id uuid,
  client_request_id uuid,
  search_contract_version text,
  requested_execution_mode text,
  actual_execution_mode text,
  terminal_status text,
  catalog_revision text,
  corpus_revision text,
  search_document_revision text,
  embedding_policy_version text,
  llm_policy_version text,
  cache_policy_version text,
  embedding_provider_attempts integer,
  llm_provider_attempts integer,
  vector_rpc_attempts integer,
  result_cache_read_attempts integer,
  result_cache_hit_count integer,
  result_cache_write_attempts integer,
  query_embedding_cache_read_attempts integer,
  query_embedding_cache_hit_count integer,
  result_count integer,
  response_event_ids bigint[],
  served_list_id uuid,
  error_code text,
  created_at timestamptz,
  terminal_at timestamptz
)
language sql
stable
security definer
set search_path = pg_catalog
as $$
  select
    r.receipt_id,
    r.request_id,
    r.client_request_id,
    r.search_contract_version,
    r.requested_execution_mode,
    r.actual_execution_mode,
    r.terminal_status,
    r.catalog_revision,
    r.corpus_revision,
    r.search_document_revision,
    r.embedding_policy_version,
    r.llm_policy_version,
    r.cache_policy_version,
    r.embedding_provider_attempts,
    r.llm_provider_attempts,
    r.vector_rpc_attempts,
    r.result_cache_read_attempts,
    r.result_cache_hit_count,
    r.result_cache_write_attempts,
    r.query_embedding_cache_read_attempts,
    r.query_embedding_cache_hit_count,
    r.result_count,
    r.response_event_ids,
    r.served_list_id,
    r.error_code,
    r.created_at,
    r.terminal_at
  from public.event_search_canary_receipts r
  where r.request_id = p_request_id
    and r.user_id = (select auth.uid());
$$;

revoke all on function public.is_event_search_canary_principal_internal_v1(uuid)
  from public, anon, authenticated;
revoke all on function public.get_event_search_revision_snapshot_internal_v1(text, integer, text)
  from public, anon, authenticated;
revoke all on function public.reserve_event_search_canary_llm_budget_internal_v1(uuid, uuid, uuid, integer, timestamptz)
  from public, anon, authenticated;
revoke all on function public.record_event_search_canary_receipt_internal_v1(
  uuid, uuid, uuid, text, text, text, text, text, text, text, text, text, text,
  integer, integer, integer, integer, integer, integer, integer, integer, integer,
  bigint[], uuid, text
) from public, anon, authenticated;
revoke all on function public.get_event_search_receipt_v1(uuid)
  from public, anon, authenticated;
revoke all on function public.claim_static_site_auth_session_issue_v1(text, integer, text, text, text, integer)
  from public, anon, authenticated;

grant execute on function public.is_event_search_canary_principal_internal_v1(uuid)
  to service_role;
grant execute on function public.get_event_search_revision_snapshot_internal_v1(text, integer, text)
  to service_role;
grant execute on function public.reserve_event_search_canary_llm_budget_internal_v1(uuid, uuid, uuid, integer, timestamptz)
  to service_role;
grant execute on function public.record_event_search_canary_receipt_internal_v1(
  uuid, uuid, uuid, text, text, text, text, text, text, text, text, text, text,
  integer, integer, integer, integer, integer, integer, integer, integer, integer,
  bigint[], uuid, text
) to service_role;
grant execute on function public.get_event_search_receipt_v1(uuid)
  to authenticated, service_role;
grant execute on function public.claim_static_site_auth_session_issue_v1(text, integer, text, text, text, integer)
  to service_role;

notify pgrst, 'reload schema';
