-- Transactional contract for event_search_canary_receipts.
begin;

insert into auth.users (
  id, email, email_confirmed_at, raw_app_meta_data, raw_user_meta_data
) values
  (
    '55555555-5555-4555-8555-555555555555',
    'search-canary@example.test',
    now(),
    '{"search_canary":true}'::jsonb,
    '{}'::jsonb
  ),
  (
    '66666666-6666-4666-8666-666666666666',
    'search-user-metadata-spoof@example.test',
    now(),
    '{}'::jsonb,
    '{"search_canary":true}'::jsonb
  ),
  (
    '77777777-7777-4777-8777-777777777777',
    'search-allowlisted@example.test',
    now(),
    '{}'::jsonb,
    '{}'::jsonb
  );

insert into public.event_search_canary_principals (user_id, note)
values ('77777777-7777-4777-8777-777777777777', 'transactional fixture');

do $$
begin
  if has_table_privilege(
    'authenticated',
    'public.event_search_canary_receipts',
    'select'
  ) then
    raise exception 'authenticated must not read receipt table directly';
  end if;
  if has_table_privilege(
    'authenticated',
    'public.event_search_canary_llm_budget_ledger',
    'select'
  ) then
    raise exception 'authenticated must not read canary budget ledger';
  end if;
  if has_function_privilege(
    'authenticated',
    'public.reserve_event_search_canary_llm_budget_internal_v1(uuid,uuid,uuid,integer,timestamptz)',
    'execute'
  ) then
    raise exception 'authenticated must not reserve canary budget';
  end if;
  if not has_function_privilege(
    'authenticated',
    'public.get_event_search_receipt_v1(uuid)',
    'execute'
  ) then
    raise exception 'owner-scoped receipt RPC is missing';
  end if;
  if has_function_privilege(
    'authenticated',
    'public.claim_static_site_auth_session_issue_v1(text,integer,text,text,text,integer)',
    'execute'
  ) then
    raise exception 'browser role must not claim broker issuance';
  end if;
end;
$$;

set local role service_role;

do $$
declare
  v_catalog text;
  v_corpus text;
  v_document text;
begin
  if not public.is_event_search_canary_principal_internal_v1(
    '55555555-5555-4555-8555-555555555555'
  ) then
    raise exception 'app_metadata canary persona was not accepted';
  end if;
  if public.is_event_search_canary_principal_internal_v1(
    '66666666-6666-4666-8666-666666666666'
  ) then
    raise exception 'user_metadata must never authorize canary persona';
  end if;
  if not public.is_event_search_canary_principal_internal_v1(
    '77777777-7777-4777-8777-777777777777'
  ) then
    raise exception 'service-managed canary allowlist was not accepted';
  end if;

  select catalog_revision, corpus_revision, search_document_revision
  into v_catalog, v_corpus, v_document
  from public.get_event_search_revision_snapshot_internal_v1(
    'gemini-embedding-2', 768, 'search_v3'
  );
  if coalesce(v_catalog, '') = '' or coalesce(v_corpus, '') = ''
     or coalesce(v_document, '') = '' then
    raise exception 'revision snapshot returned an empty contract';
  end if;
end;
$$;

select public.record_event_search_canary_receipt_internal_v1(
  '55555555-5555-4555-8555-555555555555',
  '10000000-0000-4000-8000-000000000001',
  '20000000-0000-4000-8000-000000000001',
  'event-search-contract-v2',
  'cold_vector',
  'cold_vector',
  'ok',
  repeat('a', 64),
  repeat('b', 64),
  repeat('c', 64),
  'embedding-policy-v1',
  'llm-policy-v1',
  'cache-policy-v1',
  1, 0, 1, 0, 0, 0, 0, 0, 2,
  array[7001, 7002],
  '30000000-0000-4000-8000-000000000001',
  null
);

select public.record_event_search_canary_receipt_internal_v1(
  '77777777-7777-4777-8777-777777777777',
  '10000000-0000-4000-8000-000000000002',
  '20000000-0000-4000-8000-000000000002',
  'event-search-contract-v2',
  'cached_vector',
  'cached_vector',
  'ok',
  repeat('a', 64),
  repeat('b', 64),
  repeat('c', 64),
  'embedding-policy-v1',
  'llm-policy-v1',
  'cache-policy-v1',
  0, 0, 0, 1, 1, 0, 0, 0, 1,
  array[7001],
  '30000000-0000-4000-8000-000000000002',
  null
);

-- The daily LLM attempt budget is atomic and each operation is idempotent.
update public.event_search_canary_budget_policy
set daily_llm_attempt_limit = 2
where policy_id = 'default';

select * from public.reserve_event_search_canary_llm_budget_internal_v1(
  '55555555-5555-4555-8555-555555555555',
  '40000000-0000-4000-8000-000000000001',
  '20000000-0000-4000-8000-000000000001',
  1,
  '2026-08-07 12:00:00+00'
);
select * from public.reserve_event_search_canary_llm_budget_internal_v1(
  '55555555-5555-4555-8555-555555555555',
  '40000000-0000-4000-8000-000000000001',
  '20000000-0000-4000-8000-000000000001',
  1,
  '2026-08-07 12:00:00+00'
);
select * from public.reserve_event_search_canary_llm_budget_internal_v1(
  '55555555-5555-4555-8555-555555555555',
  '40000000-0000-4000-8000-000000000002',
  '20000000-0000-4000-8000-000000000001',
  1,
  '2026-08-07 12:00:00+00'
);

do $$
declare
  v_attempts integer;
begin
  select attempts_used into v_attempts
  from public.event_search_canary_llm_budget_ledger
  where user_id = '55555555-5555-4555-8555-555555555555'
    and budget_date = '2026-08-07';
  if v_attempts <> 2 then
    raise exception 'canary budget reservation is not idempotent: %', v_attempts;
  end if;

  begin
    perform public.reserve_event_search_canary_llm_budget_internal_v1(
      '55555555-5555-4555-8555-555555555555',
      '40000000-0000-4000-8000-000000000003',
      '20000000-0000-4000-8000-000000000001',
      1,
      '2026-08-07 12:00:00+00'
    );
    raise exception 'daily LLM budget was not enforced';
  exception
    when sqlstate '54000' then null;
  end;
end;
$$;

-- Broker claims are idempotent and active-cap bounded without credential data.
do $$
begin
  if not public.claim_static_site_auth_session_issue_v1(
    'run-1', 1, 'search-browser', 'owner/repo', 'workflow@sha', 1
  ) then
    raise exception 'first broker claim was denied';
  end if;
  if not public.claim_static_site_auth_session_issue_v1(
    'run-1', 1, 'search-browser', 'owner/repo', 'workflow@sha', 1
  ) then
    raise exception 'idempotent broker claim was denied';
  end if;
  if public.claim_static_site_auth_session_issue_v1(
    'run-2', 1, 'search-browser', 'owner/repo', 'workflow@sha', 1
  ) then
    raise exception 'broker active claim limit was not enforced';
  end if;
end;
$$;

reset role;

-- Owner one can read only owner one's sanitized receipt.
set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '55555555-5555-4555-8555-555555555555',
  true
);

do $$
declare
  v_count integer;
  v_llm_attempts integer;
begin
  select count(*), max(llm_provider_attempts)
  into v_count, v_llm_attempts
  from public.get_event_search_receipt_v1(
    '10000000-0000-4000-8000-000000000001'
  );
  if v_count <> 1 or v_llm_attempts <> 0 then
    raise exception 'owner receipt/vector-only invariant failed: %, %', v_count, v_llm_attempts;
  end if;

  select count(*) into v_count
  from public.get_event_search_receipt_v1(
    '10000000-0000-4000-8000-000000000002'
  );
  if v_count <> 0 then
    raise exception 'receipt RPC crossed owner boundary';
  end if;
end;
$$;

reset role;
rollback;
