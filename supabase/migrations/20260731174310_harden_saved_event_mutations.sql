-- Security hardening for durable authenticated event saves.
-- Browser roles retain owner-scoped reads, but every mutation now goes through
-- one capped desired-state RPC. This migration is intentionally not applied by
-- the static-site build.

revoke insert, update, delete on table public.user_saved_event from authenticated;
grant select on table public.user_saved_event to authenticated;

create or replace function public.set_saved_event_state_v1(
  p_event_id bigint,
  p_source text,
  p_saved boolean default true
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_row public.user_saved_event%rowtype;
  v_active_count integer;
  v_now timestamptz := pg_catalog.now();
  v_max_active constant integer := 500;
begin
  if v_user_id is null then
    raise exception 'authentication_required' using errcode = '28000';
  end if;
  if p_event_id is null or p_event_id <= 0 then
    raise exception 'invalid_event_id' using errcode = '22023';
  end if;
  if p_source not in ('calendar', 'favorite') then
    raise exception 'invalid_save_source' using errcode = '22023';
  end if;
  if p_saved is null then
    raise exception 'invalid_saved_state' using errcode = '22023';
  end if;

  -- Serialize mutations for one owner. This makes the active-row cap exact
  -- even when several tabs save different events concurrently.
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(v_user_id::text, 736192847)
  );

  select * into v_row
  from public.user_saved_event
  where user_id = v_user_id
    and event_id = p_event_id
  for update;

  if p_saved then
    if not found then
      select pg_catalog.count(*)::integer into v_active_count
      from public.user_saved_event
      where user_id = v_user_id;

      if v_active_count >= v_max_active then
        raise exception 'saved_event_limit_exceeded' using errcode = '54000';
      end if;
    end if;

    insert into public.user_saved_event (
      user_id,
      event_id,
      calendar_saved,
      favorite_saved,
      calendar_added_at,
      favorite_added_at,
      created_at,
      updated_at
    )
    values (
      v_user_id,
      p_event_id,
      p_source = 'calendar',
      p_source = 'favorite',
      case when p_source = 'calendar' then v_now end,
      case when p_source = 'favorite' then v_now end,
      v_now,
      v_now
    )
    on conflict (user_id, event_id) do update
    set
      calendar_saved = public.user_saved_event.calendar_saved or excluded.calendar_saved,
      favorite_saved = public.user_saved_event.favorite_saved or excluded.favorite_saved,
      calendar_added_at = case
        when excluded.calendar_saved then coalesce(public.user_saved_event.calendar_added_at, excluded.calendar_added_at)
        else public.user_saved_event.calendar_added_at
      end,
      favorite_added_at = case
        when excluded.favorite_saved then coalesce(public.user_saved_event.favorite_added_at, excluded.favorite_added_at)
        else public.user_saved_event.favorite_added_at
      end,
      updated_at = v_now
    returning * into v_row;
  elsif v_row.user_id is not null and (
    (p_source = 'calendar' and not v_row.favorite_saved)
    or (p_source = 'favorite' and not v_row.calendar_saved)
  ) then
    delete from public.user_saved_event
    where user_id = v_user_id
      and event_id = p_event_id;
    v_row := null;
  elsif v_row.user_id is not null then
    update public.user_saved_event
    set
      calendar_saved = case when p_source = 'calendar' then false else calendar_saved end,
      favorite_saved = case when p_source = 'favorite' then false else favorite_saved end,
      calendar_added_at = case when p_source = 'calendar' then null else calendar_added_at end,
      favorite_added_at = case when p_source = 'favorite' then null else favorite_added_at end,
      updated_at = v_now
    where user_id = v_user_id
      and event_id = p_event_id
    returning * into v_row;
  end if;

  return pg_catalog.jsonb_build_object(
    'event_id', p_event_id,
    'saved', v_row.user_id is not null,
    'calendar_saved', coalesce(v_row.calendar_saved, false),
    'favorite_saved', coalesce(v_row.favorite_saved, false)
  );
end;
$$;

revoke all on function public.set_saved_event_state_v1(bigint, text, boolean)
  from public, anon, authenticated;
grant execute on function public.set_saved_event_state_v1(bigint, text, boolean)
  to authenticated;
grant execute on function public.set_saved_event_state_v1(bigint, text, boolean)
  to service_role;

notify pgrst, 'reload schema';
