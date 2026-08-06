-- Purpose-limited email registrations for the public 2026-09-01 site launch.
--
-- Privacy and abuse boundaries:
--   * the browser never receives direct table access;
--   * email is normalized and stored once in a dedicated table;
--   * the public RPC is idempotent for repeated/replayed requests;
--   * a honeypot absorbs basic automated form submissions;
--   * new unique rows are capped at 5,000 per Kaliningrad calendar day;
--   * no IP address, user agent or browsing history is stored;
--   * records carry a retention deadline after the one-time launch mailing.

create schema if not exists personalization;
revoke all on schema personalization from public, anon, authenticated;
grant usage on schema personalization to service_role;

create table if not exists personalization.prelaunch_launch_subscription (
  subscription_id bigint generated always as identity primary key,
  email text not null unique,
  launch_date date not null default date '2026-09-01'
    check (launch_date = date '2026-09-01'),
  source text not null default 'prelaunch_home'
    check (char_length(source) between 1 and 64),
  consent_version text not null
    check (consent_version = 'launch-2026-09-01-v1'),
  first_requested_at timestamptz not null default statement_timestamp(),
  last_requested_at timestamptz not null default statement_timestamp(),
  request_count integer not null default 1 check (request_count between 1 and 100),
  delivery_status text not null default 'pending'
    check (delivery_status in ('pending', 'sent', 'failed', 'suppressed')),
  delivery_attempt_count integer not null default 0
    check (delivery_attempt_count between 0 and 20),
  notification_sent_at timestamptz,
  last_delivery_error text,
  retention_due_at timestamptz not null default timestamptz '2026-12-01 00:00:00+02',
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  constraint prelaunch_launch_subscription_email_chk check (
    char_length(email) between 5 and 254
    and email = lower(btrim(email))
    and email ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  ),
  constraint prelaunch_launch_subscription_sent_chk check (
    (delivery_status = 'sent' and notification_sent_at is not null)
    or (delivery_status <> 'sent')
  )
);

alter table personalization.prelaunch_launch_subscription enable row level security;
revoke all on personalization.prelaunch_launch_subscription from public, anon, authenticated;
grant select, insert, update, delete on personalization.prelaunch_launch_subscription to service_role;

create index if not exists prelaunch_launch_subscription_delivery_idx
  on personalization.prelaunch_launch_subscription (delivery_status, last_requested_at);
create index if not exists prelaunch_launch_subscription_retention_idx
  on personalization.prelaunch_launch_subscription (retention_due_at);

create table if not exists personalization.prelaunch_signup_daily_guard (
  metric_date date primary key,
  accepted_new_rows integer not null default 0
    check (accepted_new_rows between 0 and 5000),
  updated_at timestamptz not null default statement_timestamp()
);

alter table personalization.prelaunch_signup_daily_guard enable row level security;
revoke all on personalization.prelaunch_signup_daily_guard from public, anon, authenticated;
grant select, insert, update, delete on personalization.prelaunch_signup_daily_guard to service_role;

create or replace function public.register_prelaunch_notification_v1(
  p_email text,
  p_source text default 'prelaunch_home',
  p_consent_version text default 'launch-2026-09-01-v1',
  p_website text default ''
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_email text := lower(btrim(coalesce(p_email, '')));
  v_source text := left(coalesce(nullif(btrim(p_source), ''), 'prelaunch_home'), 64);
  v_today date := (statement_timestamp() at time zone 'Europe/Kaliningrad')::date;
  v_accepted_new_rows integer;
begin
  -- Return the same public success shape for honeypot traffic without storing it.
  if char_length(btrim(coalesce(p_website, ''))) > 0 then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01'
    );
  end if;

  if char_length(v_email) not between 5 and 254
     or v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    raise exception 'invalid_prelaunch_email' using errcode = '22023';
  end if;
  if p_consent_version is distinct from 'launch-2026-09-01-v1' then
    raise exception 'invalid_prelaunch_consent' using errcode = '22023';
  end if;

  -- Fast idempotent path for ordinary repeats and transport replay.
  update personalization.prelaunch_launch_subscription
     set source = v_source,
         consent_version = p_consent_version,
         last_requested_at = statement_timestamp(),
         request_count = least(request_count + 1, 100),
         updated_at = statement_timestamp()
   where email = v_email;
  if found then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01'
    );
  end if;

  insert into personalization.prelaunch_signup_daily_guard (metric_date)
  values (v_today)
  on conflict (metric_date) do nothing;

  select accepted_new_rows
    into v_accepted_new_rows
    from personalization.prelaunch_signup_daily_guard
   where metric_date = v_today
   for update;

  -- Another request may have inserted the same normalized email while this
  -- transaction waited for the daily guard. Re-check under the shared lock.
  update personalization.prelaunch_launch_subscription
     set source = v_source,
         consent_version = p_consent_version,
         last_requested_at = statement_timestamp(),
         request_count = least(request_count + 1, 100),
         updated_at = statement_timestamp()
   where email = v_email;
  if found then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01'
    );
  end if;

  if v_accepted_new_rows >= 5000 then
    return jsonb_build_object(
      'accepted', false,
      'status', 'daily_capacity_reached',
      'launch_date', '2026-09-01'
    );
  end if;

  insert into personalization.prelaunch_launch_subscription (
    email,
    source,
    consent_version
  ) values (
    v_email,
    v_source,
    p_consent_version
  );

  update personalization.prelaunch_signup_daily_guard
     set accepted_new_rows = accepted_new_rows + 1,
         updated_at = statement_timestamp()
   where metric_date = v_today;

  return jsonb_build_object(
    'accepted', true,
    'status', 'registered',
    'launch_date', '2026-09-01'
  );
end;
$$;

revoke execute on function public.register_prelaunch_notification_v1(text, text, text, text)
  from public, anon, authenticated;
grant execute on function public.register_prelaunch_notification_v1(text, text, text, text)
  to anon, authenticated, service_role;

comment on table personalization.prelaunch_launch_subscription is
  'Purpose-limited normalized emails for one notification about the public launch on 2026-09-01; no browsing metadata.';
comment on table personalization.prelaunch_signup_daily_guard is
  'Kaliningrad-day cap for new unique public prelaunch email registrations.';
comment on function public.register_prelaunch_notification_v1(text, text, text, text) is
  'Idempotent anonymous registration for the one-time 2026-09-01 launch notification; direct table access remains closed.';
