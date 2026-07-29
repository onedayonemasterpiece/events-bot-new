-- Run after 20260729113221_focus_group_feedback_v1.sql. Everything rolls back.
begin;

insert into auth.users (id, email, email_confirmed_at)
values
  ('33333333-3333-4333-8333-333333333333', 'focus-one@example.test', now()),
  ('44444444-4444-4444-8444-444444444444', 'focus-two@example.test', now());

do $$
begin
  if has_table_privilege(
    'authenticated',
    'personalization.focus_group_feedback',
    'select'
  ) then
    raise exception 'authenticated users must not read raw feedback';
  end if;
  if has_function_privilege(
    'anon',
    'public.submit_focus_group_feedback_v1(text,text,text,smallint,text,text)',
    'execute'
  ) then
    raise exception 'anonymous users must not submit feedback';
  end if;
end;
$$;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '33333333-3333-4333-8333-333333333333',
  true
);

select public.submit_focus_group_feedback_v1(
  'page_score', 'home', '/', 9::smallint, null, null
);
select public.submit_focus_group_feedback_v1(
  'issue',
  'event_detail',
  '/sobytiya/example/',
  null,
  'Кнопка не открылась.',
  '33333333-3333-4333-8333-333333333333/report.webp'
);

reset role;

do $$
declare
  v_count integer;
begin
  select count(*) into v_count
  from personalization.focus_group_feedback
  where user_id = '33333333-3333-4333-8333-333333333333';
  if v_count <> 2 then
    raise exception 'expected two feedback rows, got %', v_count;
  end if;
end;
$$;

rollback;
