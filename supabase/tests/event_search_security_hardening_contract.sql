-- Transactional contract for 20260731174313_harden_event_search_internal_rpc.
-- Run after the event-search migrations; fixtures and quota use are rolled back.
begin;

insert into auth.users (id, email, email_confirmed_at)
values
  ('33333333-3333-4333-8333-333333333333', 'search-sec-one@example.test', now()),
  ('44444444-4444-4444-8444-444444444444', 'search-sec-two@example.test', now());

do $$
begin
  if has_function_privilege(
    'authenticated',
    'public.reserve_event_search_quota_v3(text,boolean,timestamptz)',
    'execute'
  ) then
    raise exception 'authenticated must not reserve provider quota directly';
  end if;
  if has_function_privilege(
    'authenticated',
    'public.search_events_by_embedding_v1(extensions.vector,integer,integer,date,date,text,text,text,integer,smallint,text,text,text)',
    'execute'
  ) then
    raise exception 'authenticated must not execute vector search directly';
  end if;
  if has_function_privilege(
    'authenticated',
    'public.reserve_event_search_quota_internal_v1(uuid,uuid,text,boolean,timestamptz)',
    'execute'
  ) then
    raise exception 'authenticated must not execute service-only wrapper';
  end if;
  if not has_function_privilege(
    'authenticated',
    'public.get_event_search_quota_v2(text,timestamptz)',
    'execute'
  ) then
    raise exception 'minimal authenticated quota status API is missing';
  end if;
  if not has_function_privilege(
    'authenticated',
    'public.record_event_search_feedback_v1(text,text,bigint[],integer,jsonb,uuid)',
    'execute'
  ) then
    raise exception 'hardened feedback API is missing';
  end if;
end;
$$;

set local role service_role;
select * from public.reserve_event_search_quota_internal_v1(
  '33333333-3333-4333-8333-333333333333',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  'registered',
  false,
  '2026-07-31 17:00:00+00'
);
select * from public.reserve_event_search_quota_internal_v1(
  '33333333-3333-4333-8333-333333333333',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  'registered',
  false,
  '2026-07-31 17:00:00+00'
);
reset role;

do $$
declare
  v_hour_count integer;
  v_operation_count integer;
begin
  select request_count into v_hour_count
  from public.user_search_quota_hourly_ledger
  where user_id = '33333333-3333-4333-8333-333333333333'
    and bucket_start_at = '2026-07-31 17:00:00+00';
  if v_hour_count <> 1 then
    raise exception 'duplicate operation consumed quota twice: %', v_hour_count;
  end if;

  select count(*) into v_operation_count
  from public.event_search_quota_operation
  where user_id = '33333333-3333-4333-8333-333333333333'
    and client_request_id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  if v_operation_count <> 1 then
    raise exception 'quota operation idempotency row count: %', v_operation_count;
  end if;
end;
$$;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '33333333-3333-4333-8333-333333333333',
  true
);
select public.record_event_search_feedback_v1(
  'джаз вечером',
  'matched',
  array[7001, 7001, 7002],
  3,
  '{"surface":"authorized_event_search","ignored":"not persisted"}'::jsonb,
  'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
);
select public.record_event_search_feedback_v1(
  'джаз вечером',
  'matched',
  array[7001, 7002],
  2,
  '{"surface":"authorized_event_search"}'::jsonb,
  'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
);
reset role;

do $$
declare
  v_count integer;
  v_metadata jsonb;
begin
  select count(*) into v_count
  from public.event_search_feedback
  where user_id = '33333333-3333-4333-8333-333333333333'
    and operation_id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  if v_count <> 1 then
    raise exception 'duplicate feedback operation was not idempotent: %', v_count;
  end if;
  select metadata into v_metadata
  from public.event_search_feedback
  where user_id = '33333333-3333-4333-8333-333333333333'
    and operation_id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  if v_metadata ? 'ignored' then
    raise exception 'arbitrary feedback metadata was persisted';
  end if;
end;
$$;

-- The same operation id is owner-scoped, not globally shared (BOLA guard).
set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '44444444-4444-4444-8444-444444444444',
  true
);
select public.record_event_search_feedback_v1(
  'джаз вечером',
  'missed',
  '{}'::bigint[],
  0,
  '{}'::jsonb,
  'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
);
reset role;

do $$
declare
  v_count integer;
begin
  select count(*) into v_count
  from public.event_search_feedback
  where operation_id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
  if v_count <> 2 then
    raise exception 'operation id incorrectly crossed owner boundary: %', v_count;
  end if;
end;
$$;

-- Fill the second owner's rolling hour to the cap and verify a new operation
-- is rejected without affecting the first owner's allowance.
insert into public.event_search_feedback (
  user_id,
  operation_id,
  query_hash,
  query_text,
  verdict,
  result_event_ids,
  result_count,
  metadata
)
select
  '44444444-4444-4444-8444-444444444444',
  extensions.gen_random_uuid(),
  repeat('c', 64),
  'проверка ограничения ' || n,
  'missed',
  '{}'::bigint[],
  0,
  '{}'::jsonb
from generate_series(1, 29) as n;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '44444444-4444-4444-8444-444444444444',
  true
);
do $$
begin
  begin
    perform public.record_event_search_feedback_v1(
      'ещё одна проверка',
      'missed',
      '{}'::bigint[],
      0,
      '{}'::jsonb,
      'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
    );
    raise exception 'feedback hourly cap was not enforced';
  exception
    when sqlstate '54000' then
      null;
  end;
end;
$$;
reset role;

rollback;
