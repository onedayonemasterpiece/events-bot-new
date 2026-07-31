-- The table-return column name `person_id` is also a PL/pgSQL variable.
-- Name the unique constraint explicitly so ON CONFLICT is unambiguous.

create or replace function public.set_person_like_v1(
  p_person_id text,
  p_liked boolean
)
returns table (
  person_id text,
  likes_count integer,
  liked boolean
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_person_id text := btrim(coalesce(p_person_id, ''));
begin
  if v_user_id is null then
    raise exception 'authentication_required'
      using errcode = '42501';
  end if;
  if p_liked is null then
    raise exception 'liked_state_required'
      using errcode = '22023';
  end if;
  if length(v_person_id) not between 3 and 128
     or v_person_id !~ '^[a-z0-9][a-z0-9:_-]+$'
     or not exists (
       select 1
         from public.personalization_person_like_subject subject
        where subject.person_id = v_person_id
          and subject.active
     )
  then
    raise exception 'unknown_person'
      using errcode = '22023';
  end if;

  if p_liked then
    insert into public.personalization_person_like_state(person_id, user_id)
    values (v_person_id, v_user_id)
    on conflict on constraint personalization_person_like_state_pkey do nothing;
  else
    delete from public.personalization_person_like_state
     where personalization_person_like_state.person_id = v_person_id
       and personalization_person_like_state.user_id = v_user_id;
  end if;

  return query
  select
    v_person_id,
    coalesce(counter.likes_count, 0)::integer,
    exists (
      select 1
        from public.personalization_person_like_state state
       where state.person_id = v_person_id
         and state.user_id = v_user_id
    )
    from public.personalization_person_like_subject subject
    left join public.personalization_person_like_counter counter
      on counter.person_id = subject.person_id
   where subject.person_id = v_person_id
     and subject.active;
end;
$$;
revoke all on function public.set_person_like_v1(text, boolean)
  from public, anon, authenticated;
grant execute on function public.set_person_like_v1(text, boolean)
  to authenticated;
