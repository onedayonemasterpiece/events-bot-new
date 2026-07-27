-- Compact PWA lifecycle analytics.
--
-- Storage is deliberately bounded:
--   * one mutable row per browser installation id (no raw event log);
--   * one aggregate row per Kaliningrad calendar day;
--   * installation state self-prunes after 180 inactive days;
--   * anonymous callers can create at most 1,000 new ids per day.
--
-- Canonical event/content data remains in Fly SQLite. These anonymous product
-- metrics belong to the personalization Supabase project.

create schema if not exists personalization;
revoke all on schema personalization from public, anon, authenticated;
grant usage on schema personalization to service_role;

create table if not exists personalization.pwa_installation_state (
  installation_id uuid primary key,
  install_confirmed boolean not null default false,
  installed_on date,
  first_standalone_on date,
  last_active_on date,
  last_session_id uuid,
  active_days integer not null default 0 check (active_days >= 0),
  d1_returned boolean not null default false,
  d7_returned boolean not null default false,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  constraint pwa_installation_confirmed_date_chk
    check (not install_confirmed or installed_on is not null)
);

alter table personalization.pwa_installation_state enable row level security;
revoke all on personalization.pwa_installation_state from public, anon, authenticated;
grant select, insert, update, delete on personalization.pwa_installation_state to service_role;

create index if not exists pwa_installation_state_last_active_idx
  on personalization.pwa_installation_state (
    coalesce(last_active_on, installed_on, (created_at at time zone 'Europe/Kaliningrad')::date)
  );

create table if not exists personalization.pwa_daily_metric (
  metric_date date primary key,
  confirmed_installs integer not null default 0 check (confirmed_installs >= 0),
  standalone_sessions integer not null default 0 check (standalone_sessions >= 0),
  active_installations integer not null default 0 check (active_installations >= 0),
  first_standalone_launches integer not null default 0 check (first_standalone_launches >= 0),
  cohort_d1_returns integer not null default 0 check (cohort_d1_returns >= 0),
  cohort_d7_returns integer not null default 0 check (cohort_d7_returns >= 0),
  accepted_state_creations integer not null default 0 check (
    accepted_state_creations between 0 and 1000
  ),
  updated_at timestamptz not null default statement_timestamp()
);

alter table personalization.pwa_daily_metric enable row level security;
revoke all on personalization.pwa_daily_metric from public, anon, authenticated;
grant select, insert, update, delete on personalization.pwa_daily_metric to service_role;

create table if not exists personalization.pwa_telemetry_maintenance (
  singleton boolean primary key default true check (singleton),
  last_pruned_on date not null default date '1970-01-01',
  updated_at timestamptz not null default statement_timestamp()
);

alter table personalization.pwa_telemetry_maintenance enable row level security;
revoke all on personalization.pwa_telemetry_maintenance from public, anon, authenticated;
grant select, insert, update, delete on personalization.pwa_telemetry_maintenance to service_role;

insert into personalization.pwa_telemetry_maintenance (singleton)
values (true)
on conflict (singleton) do nothing;

create or replace function public.record_pwa_lifecycle_v1(
  p_installation_id uuid,
  p_session_id uuid,
  p_event_kind text
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_today date := (statement_timestamp() at time zone 'Europe/Kaliningrad')::date;
  v_state personalization.pwa_installation_state%rowtype;
  v_new_state_count integer;
  v_prune_claimed boolean := false;
  v_first_standalone boolean := false;
begin
  if p_installation_id is null
     or p_session_id is null
     or p_event_kind not in ('install', 'standalone_open') then
    raise exception 'invalid_pwa_metric' using errcode = '22023';
  end if;

  insert into personalization.pwa_daily_metric (metric_date)
  values (v_today)
  on conflict (metric_date) do nothing;

  select *
    into v_state
    from personalization.pwa_installation_state
   where installation_id = p_installation_id
   for update;

  if not found then
    -- The daily row is also a very small abuse/storage circuit breaker.
    select accepted_state_creations
      into v_new_state_count
      from personalization.pwa_daily_metric
     where metric_date = v_today
     for update;

    if v_new_state_count >= 1000 then
      return false;
    end if;

    -- Re-check after acquiring the shared daily lock: another request for this
    -- id may have completed while this transaction was waiting.
    select *
      into v_state
      from personalization.pwa_installation_state
     where installation_id = p_installation_id
     for update;

    if not found then
      insert into personalization.pwa_installation_state (installation_id)
      values (p_installation_id)
      returning * into v_state;

      update personalization.pwa_daily_metric
         set accepted_state_creations = accepted_state_creations + 1,
             updated_at = statement_timestamp()
       where metric_date = v_today;
    end if;
  end if;

  if p_event_kind = 'install' and not v_state.install_confirmed then
    update personalization.pwa_installation_state
       set install_confirmed = true,
           installed_on = v_today,
           updated_at = statement_timestamp()
     where installation_id = p_installation_id;

    update personalization.pwa_daily_metric
       set confirmed_installs = confirmed_installs + 1,
           updated_at = statement_timestamp()
     where metric_date = v_today;
  elsif p_event_kind = 'standalone_open' then
    v_first_standalone := v_state.first_standalone_on is null;

    if v_state.last_session_id is distinct from p_session_id then
      update personalization.pwa_daily_metric
         set standalone_sessions = standalone_sessions + 1,
             updated_at = statement_timestamp()
       where metric_date = v_today;
    end if;

    if v_state.last_active_on is distinct from v_today then
      update personalization.pwa_daily_metric
         set active_installations = active_installations + 1,
             first_standalone_launches = first_standalone_launches
               + case when v_first_standalone then 1 else 0 end,
             updated_at = statement_timestamp()
       where metric_date = v_today;

      if v_state.install_confirmed
         and v_state.installed_on = v_today - 1
         and not v_state.d1_returned then
        insert into personalization.pwa_daily_metric (metric_date, cohort_d1_returns)
        values (v_state.installed_on, 1)
        on conflict (metric_date) do update
          set cohort_d1_returns = personalization.pwa_daily_metric.cohort_d1_returns + 1,
              updated_at = statement_timestamp();

        update personalization.pwa_installation_state
           set d1_returned = true
         where installation_id = p_installation_id;
      end if;

      if v_state.install_confirmed
         and v_state.installed_on = v_today - 7
         and not v_state.d7_returned then
        insert into personalization.pwa_daily_metric (metric_date, cohort_d7_returns)
        values (v_state.installed_on, 1)
        on conflict (metric_date) do update
          set cohort_d7_returns = personalization.pwa_daily_metric.cohort_d7_returns + 1,
              updated_at = statement_timestamp();

        update personalization.pwa_installation_state
           set d7_returned = true
         where installation_id = p_installation_id;
      end if;
    end if;

    update personalization.pwa_installation_state
       set first_standalone_on = coalesce(first_standalone_on, v_today),
           last_active_on = v_today,
           last_session_id = p_session_id,
           active_days = active_days
             + case when last_active_on is distinct from v_today then 1 else 0 end,
           updated_at = statement_timestamp()
     where installation_id = p_installation_id;
  end if;

  -- Traffic-triggered maintenance avoids a scheduler and still guarantees that
  -- stored installation ids do not grow forever. With no traffic there is no
  -- new storage pressure, so a delayed cleanup is harmless.
  update personalization.pwa_telemetry_maintenance
     set last_pruned_on = v_today,
         updated_at = statement_timestamp()
   where singleton
     and last_pruned_on < v_today
  returning true into v_prune_claimed;

  if coalesce(v_prune_claimed, false) then
    delete from personalization.pwa_installation_state
     where coalesce(
       last_active_on,
       installed_on,
       (created_at at time zone 'Europe/Kaliningrad')::date
     ) < v_today - 180;
  end if;

  return true;
end;
$$;

revoke execute on function public.record_pwa_lifecycle_v1(uuid, uuid, text)
  from public, anon, authenticated;
grant execute on function public.record_pwa_lifecycle_v1(uuid, uuid, text)
  to anon, authenticated, service_role;

comment on table personalization.pwa_installation_state is
  'One privacy-minimal mutable row per random browser PWA installation id; no event history.';
comment on table personalization.pwa_daily_metric is
  'Daily aggregate PWA installs, standalone sessions, active installations and exact D1/D7 cohort returns.';
comment on function public.record_pwa_lifecycle_v1(uuid, uuid, text) is
  'Anonymous compact PWA lifecycle ingest; fixed server date, bounded new ids, no raw request data stored.';
