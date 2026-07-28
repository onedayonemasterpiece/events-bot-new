-- Transactional RLS/RPC/view contract. Run only after
-- 20260727141820_durable_saved_events_v1.sql; all fixtures are rolled back.
begin;

insert into auth.users (id, email, email_confirmed_at)
values
  ('11111111-1111-4111-8111-111111111111', 'saved-one@example.test', now()),
  ('22222222-2222-4222-8222-222222222222', 'saved-two@example.test', now());

do $$
begin
  if has_table_privilege('anon', 'public.user_saved_event', 'select')
     or has_table_privilege('anon', 'public.my_saved_events_v1', 'select') then
    raise exception 'anon must not read saved-event state';
  end if;
  if has_function_privilege(
    'anon',
    'public.set_saved_event_state_v1(bigint,text,boolean)',
    'execute'
  ) then
    raise exception 'anon must not execute the saved-event mutation RPC';
  end if;
  if not has_function_privilege(
    'authenticated',
    'public.set_saved_event_state_v1(bigint,text,boolean)',
    'execute'
  ) then
    raise exception 'authenticated role cannot execute the saved-event RPC';
  end if;
end;
$$;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '11111111-1111-4111-8111-111111111111',
  true
);

select public.set_saved_event_state_v1(7001, 'calendar', true);
select public.set_saved_event_state_v1(7001, 'calendar', true);
select public.set_saved_event_state_v1(7001, 'favorite', true);
select public.set_saved_event_state_v1(7002, 'favorite', true);

do $$
declare
  v_count integer;
  v_first bigint;
begin
  select count(*) into v_count from public.my_saved_events_v1;
  if v_count <> 2 then
    raise exception 'saved-event dedupe failed: expected 2 rows, got %', v_count;
  end if;

  select event_id into v_first
  from public.my_saved_events_v1
  order by source_priority, sort_at desc, event_id;
  if v_first <> 7001 then
    raise exception 'calendar-first order failed: got %', v_first;
  end if;

  if not exists (
    select 1 from public.my_saved_events_v1
    where event_id = 7001 and calendar_saved and favorite_saved
  ) then
    raise exception 'calendar/favorite sources were not merged into one row';
  end if;
end;
$$;

select set_config(
  'request.jwt.claim.sub',
  '22222222-2222-4222-8222-222222222222',
  true
);

do $$
begin
  if exists (select 1 from public.my_saved_events_v1) then
    raise exception 'RLS leaked another user saved-event state';
  end if;
end;
$$;

select set_config(
  'request.jwt.claim.sub',
  '11111111-1111-4111-8111-111111111111',
  true
);
select public.set_saved_event_state_v1(7001, 'calendar', false);

do $$
begin
  if not exists (
    select 1 from public.my_saved_events_v1
    where event_id = 7001 and not calendar_saved and favorite_saved
  ) then
    raise exception 'source-specific removal deleted the remaining favorite';
  end if;
end;
$$;

select public.set_saved_event_state_v1(7001, 'favorite', false);

do $$
begin
  if exists (select 1 from public.my_saved_events_v1 where event_id = 7001) then
    raise exception 'inactive saved-event row was not deleted';
  end if;
end;
$$;

reset role;
rollback;
