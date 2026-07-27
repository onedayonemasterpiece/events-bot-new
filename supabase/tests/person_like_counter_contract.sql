begin;

do $$
declare
  v_rls_tables integer;
  v_subjects integer;
begin
  select count(*)
    into v_rls_tables
    from pg_class
   where relnamespace = 'public'::regnamespace
     and relname in (
       'personalization_person_like_subject',
       'personalization_person_like_counter',
       'personalization_person_like_state'
     )
     and relrowsecurity;
  if v_rls_tables <> 3 then
    raise exception 'person-like tables must all have RLS enabled';
  end if;

  if has_table_privilege(
    'anon',
    'public.personalization_person_like_state',
    'select'
  ) or has_table_privilege(
    'authenticated',
    'public.personalization_person_like_state',
    'select'
  ) then
    raise exception 'raw per-user person-like state is exposed';
  end if;

  if not has_function_privilege(
    'anon',
    'public.get_person_like_snapshot_v1(text[])',
    'execute'
  ) or not has_function_privilege(
    'authenticated',
    'public.get_person_like_snapshot_v1(text[])',
    'execute'
  ) then
    raise exception 'snapshot RPC grants are incomplete';
  end if;

  if has_function_privilege(
    'anon',
    'public.set_person_like_v1(text,boolean)',
    'execute'
  ) or not has_function_privilege(
    'authenticated',
    'public.set_person_like_v1(text,boolean)',
    'execute'
  ) then
    raise exception 'write RPC must require an authenticated role';
  end if;

  select count(*)
    into v_subjects
    from public.personalization_person_like_subject
   where active;
  if v_subjects < 38 then
    raise exception 'expected at least 38 active KGD80 people, got %', v_subjects;
  end if;
end
$$;

rollback;
