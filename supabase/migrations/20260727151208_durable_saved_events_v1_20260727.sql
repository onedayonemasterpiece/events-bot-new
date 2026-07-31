-- Durable, user-owned event saves for the static site.
-- Canonical event facts remain in Fly SQLite/static manifests; this table owns
-- only the authenticated user's save state and the source of that save.

create table public.user_saved_event (
  user_id uuid not null references auth.users (id) on delete cascade,
  event_id bigint not null check (event_id > 0),
  calendar_saved boolean not null default false,
  favorite_saved boolean not null default false,
  calendar_added_at timestamptz,
  favorite_added_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, event_id),
  constraint user_saved_event_active_source_chk check (calendar_saved or favorite_saved),
  constraint user_saved_event_calendar_timestamp_chk check (
    calendar_saved = (calendar_added_at is not null)
  ),
  constraint user_saved_event_favorite_timestamp_chk check (
    favorite_saved = (favorite_added_at is not null)
  )
);

comment on table public.user_saved_event is
  'Durable authenticated event save state; event facts are joined from the static canonical catalog.';
comment on column public.user_saved_event.calendar_saved is
  'True after the user invokes the event calendar/save action; ICS delivery remains a separate side effect.';
comment on column public.user_saved_event.favorite_saved is
  'Favorite source synchronized from the canonical like/unlike action after its local state commit.';

alter table public.user_saved_event enable row level security;

revoke all on table public.user_saved_event from public, anon, authenticated;
grant select, insert, update, delete on table public.user_saved_event to authenticated;
grant select, insert, update, delete on table public.user_saved_event to service_role;

create policy "saved events are readable by their owner"
  on public.user_saved_event
  for select
  to authenticated
  using ((select auth.uid()) = user_id);

create policy "saved events are insertable by their owner"
  on public.user_saved_event
  for insert
  to authenticated
  with check ((select auth.uid()) = user_id);

create policy "saved events are updateable by their owner"
  on public.user_saved_event
  for update
  to authenticated
  using ((select auth.uid()) = user_id)
  with check ((select auth.uid()) = user_id);

create policy "saved events are removable by their owner"
  on public.user_saved_event
  for delete
  to authenticated
  using ((select auth.uid()) = user_id);

create index user_saved_event_calendar_order_idx
  on public.user_saved_event (user_id, calendar_added_at desc, event_id)
  where calendar_saved;

create index user_saved_event_favorite_order_idx
  on public.user_saved_event (user_id, favorite_added_at desc, event_id)
  where favorite_saved;

-- Postgres 15+ security_invoker is required here: the view must retain the
-- underlying table's owner-only RLS rather than run with its creator's rights.
create view public.my_saved_events_v1
with (security_invoker = true)
as
select
  event_id,
  calendar_saved,
  favorite_saved,
  calendar_added_at,
  favorite_added_at,
  case when calendar_saved then 0 else 1 end as source_priority,
  case
    when calendar_saved then calendar_added_at
    else favorite_added_at
  end as sort_at
from public.user_saved_event
where user_id = (select auth.uid())
  and (calendar_saved or favorite_saved);

revoke all on table public.my_saved_events_v1 from public, anon, authenticated;
grant select on table public.my_saved_events_v1 to authenticated;
grant select on table public.my_saved_events_v1 to service_role;

create or replace function public.set_saved_event_state_v1(
  p_event_id bigint,
  p_source text,
  p_saved boolean default true
)
returns jsonb
language plpgsql
security invoker
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_row public.user_saved_event%rowtype;
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

  if p_saved then
    insert into public.user_saved_event (
      user_id,
      event_id,
      calendar_saved,
      favorite_saved,
      calendar_added_at,
      favorite_added_at
    )
    values (
      v_user_id,
      p_event_id,
      p_source = 'calendar',
      p_source = 'favorite',
      case when p_source = 'calendar' then now() end,
      case when p_source = 'favorite' then now() end
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
      updated_at = now()
    returning * into v_row;
  else
    select * into v_row
    from public.user_saved_event
    where user_id = v_user_id
      and event_id = p_event_id
    for update;

    if found and (
      (p_source = 'calendar' and not v_row.favorite_saved)
      or (p_source = 'favorite' and not v_row.calendar_saved)
    ) then
      delete from public.user_saved_event
      where user_id = v_user_id
        and event_id = p_event_id;
      v_row := null;
    elsif found then
      update public.user_saved_event
      set
        calendar_saved = case when p_source = 'calendar' then false else calendar_saved end,
        favorite_saved = case when p_source = 'favorite' then false else favorite_saved end,
        calendar_added_at = case when p_source = 'calendar' then null else calendar_added_at end,
        favorite_added_at = case when p_source = 'favorite' then null else favorite_added_at end,
        updated_at = now()
      where user_id = v_user_id
        and event_id = p_event_id
      returning * into v_row;
    end if;
  end if;

  return jsonb_build_object(
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
;
