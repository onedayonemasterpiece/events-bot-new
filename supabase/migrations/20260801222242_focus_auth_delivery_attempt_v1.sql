-- PII-free correlation ledger for focus-group Auth email delivery.
-- Plain email, OTP, token hash and browser/network identifiers are forbidden.

-- NotiSend's subscriber tariff is a shared ceiling of 200 unique recipients in
-- the provider billing period, not 200 messages or 200 lifetime users. The
-- provider-reported baseline is reconciled by an operator; new unique recipients
-- admitted after that snapshot are counted atomically by Supabase. A repeat send
-- to the same user in the same period consumes no additional slot.
alter table email_control.recommendation_capacity
  add column if not exists provider_period_key text;

alter table email_control.recommendation_capacity
  add column if not exists provider_period_ends_at timestamptz;

alter table email_control.recommendation_capacity
  add column if not exists provider_used_count integer not null default 0;

alter table email_control.recommendation_capacity
  add column if not exists provider_reconciled_at timestamptz;

alter table email_control.recommendation_capacity
  add constraint recommendation_capacity_provider_used_chk
  check (provider_used_count between 0 and capacity);

alter table email_control.recommendation_capacity
  add constraint recommendation_capacity_provider_period_chk
  check (
    (provider_period_key is null and provider_period_ends_at is null and provider_reconciled_at is null)
    or (
      length(provider_period_key) between 1 and 80
      and provider_period_ends_at is not null
      and provider_reconciled_at is not null
      and provider_reconciled_at < provider_period_ends_at
    )
  );

create table email_control.notisend_recipient_admission (
  -- Intentionally no FK/cascade: deleting a disposable Auth identity does not
  -- release a recipient already counted in the current provider period.
  period_key text not null,
  user_id uuid not null,
  first_source text not null,
  first_attempt_id uuid,
  admitted_at timestamptz not null default now(),
  included_in_provider_snapshot boolean not null default false,
  primary key (period_key, user_id),
  unique (period_key, first_attempt_id),
  constraint notisend_recipient_source_chk check (first_source in ('auth', 'recommendation')),
  constraint notisend_recipient_attempt_chk check (
    (first_source = 'auth' and first_attempt_id is not null)
    or first_source = 'recommendation'
  )
);

comment on table email_control.notisend_recipient_admission is
  'Private PII-free unique-recipient admission set per NotiSend billing period, shared by Auth and recommendation sends.';

alter table email_control.notisend_recipient_admission enable row level security;
revoke all on email_control.notisend_recipient_admission from public, anon, authenticated;
grant select, insert, update, delete on email_control.notisend_recipient_admission to service_role;

create or replace function email_control.reserve_notisend_recipient_v1(
  p_user_id uuid,
  p_source text,
  p_attempt_id uuid default null
)
returns table (
  admitted boolean,
  admitted_count integer,
  capacity integer
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_capacity integer;
  v_period_key text;
  v_period_ends_at timestamptz;
  v_provider_used integer;
  v_reconciled_at timestamptz;
  v_incremental_count integer;
begin
  if p_user_id is null
     or p_source not in ('auth', 'recommendation')
     or (p_source = 'auth' and p_attempt_id is null) then
    raise exception 'invalid NotiSend recipient admission' using errcode = '22023';
  end if;

  perform pg_advisory_xact_lock(
    pg_catalog.hashtextextended('kenigevents:notisend-recipient-capacity', 0)
  );
  select rc.capacity, rc.provider_period_key, rc.provider_period_ends_at,
         rc.provider_used_count, rc.provider_reconciled_at
    into v_capacity, v_period_key, v_period_ends_at,
         v_provider_used, v_reconciled_at
    from email_control.recommendation_capacity rc
   where rc.capacity_key = 'launch'
   for update;
  if v_capacity is null then
    raise exception 'NotiSend capacity is not configured' using errcode = '55000';
  end if;

  if v_reconciled_at is null
     or v_period_key is null
     or v_period_ends_at is null
     or v_period_ends_at <= now() then
    return query select false, coalesce(v_provider_used, 0), v_capacity;
    return;
  end if;

  select count(*)::integer
    into v_incremental_count
    from email_control.notisend_recipient_admission a
   where a.period_key = v_period_key
     and not a.included_in_provider_snapshot;
  if exists (
    select 1
      from email_control.notisend_recipient_admission a
     where a.period_key = v_period_key and a.user_id = p_user_id
  ) then
    return query select true, v_provider_used + v_incremental_count, v_capacity;
    return;
  end if;
  if v_provider_used + v_incremental_count >= v_capacity then
    return query select false, v_provider_used + v_incremental_count, v_capacity;
    return;
  end if;

  insert into email_control.notisend_recipient_admission (
    period_key, user_id, first_source, first_attempt_id
  ) values (
    v_period_key, p_user_id, p_source,
    case when p_source = 'auth' then p_attempt_id else null end
  );
  return query select true, v_provider_used + v_incremental_count + 1, v_capacity;
end;
$$;

create or replace function public.focus_auth_reconcile_notisend_capacity_v1(
  p_period_key text,
  p_period_ends_at timestamptz,
  p_provider_used_count integer
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_current_period text;
  v_current_period_ends_at timestamptz;
  v_current_provider_used integer;
  v_unabsorbed_admissions integer;
  v_result jsonb;
begin
  if nullif(trim(coalesce(p_period_key, '')), '') is null
     or length(trim(p_period_key)) > 80
     or p_period_ends_at is null
     or p_period_ends_at <= now()
     or p_provider_used_count is null
     or p_provider_used_count < 0 then
    raise exception 'invalid NotiSend provider reconciliation' using errcode = '22023';
  end if;
  perform pg_advisory_xact_lock(
    pg_catalog.hashtextextended('kenigevents:notisend-recipient-capacity', 0)
  );
  select rc.provider_period_key, rc.provider_period_ends_at, rc.provider_used_count
    into v_current_period, v_current_period_ends_at, v_current_provider_used
    from email_control.recommendation_capacity rc
   where rc.capacity_key = 'launch'
   for update;
  if v_current_period is distinct from trim(p_period_key)
     and v_current_period is not null
     and v_current_period_ends_at > now() then
    raise exception 'current NotiSend billing period is still active' using errcode = '55000';
  end if;
  if v_current_period is distinct from trim(p_period_key)
     and exists (
       select 1
         from email_control.notisend_recipient_admission a
        where a.period_key = trim(p_period_key)
     ) then
    raise exception 'NotiSend billing period key was already used' using errcode = '23505';
  end if;
  select count(*)::integer
    into v_unabsorbed_admissions
    from email_control.notisend_recipient_admission a
   where a.period_key = trim(p_period_key)
     and not a.included_in_provider_snapshot;
  if v_current_period = trim(p_period_key)
     and p_provider_used_count < v_current_provider_used + v_unabsorbed_admissions then
    raise exception 'NotiSend provider count is behind local admissions' using errcode = '22023';
  end if;
  -- The provider dashboard count is the new baseline. Existing local
  -- admissions in that same period are now represented by that snapshot;
  -- only later admissions are added on top of it.
  update email_control.notisend_recipient_admission
     set included_in_provider_snapshot = true
   where period_key = trim(p_period_key);
  update email_control.recommendation_capacity rc
     set provider_period_key = trim(p_period_key),
         provider_period_ends_at = p_period_ends_at,
         provider_used_count = p_provider_used_count,
         provider_reconciled_at = now(),
         updated_at = now()
   where rc.capacity_key = 'launch'
     and p_provider_used_count <= rc.capacity
  returning pg_catalog.jsonb_build_object(
    'period_key', provider_period_key,
    'period_ends_at', provider_period_ends_at,
    'provider_reported', provider_used_count,
    'capacity', capacity,
    'reconciled_at', provider_reconciled_at
  ) into v_result;
  if v_result is null then
    raise exception 'NotiSend capacity reconciliation rejected' using errcode = '22023';
  end if;
  return v_result;
end;
$$;

create or replace function public.focus_auth_reserve_notisend_recipient_v1(
  p_user_id uuid,
  p_attempt_id uuid
)
returns table (
  admitted boolean,
  admitted_count integer,
  capacity integer
)
language sql
security definer
set search_path = ''
as $$
  select r.admitted, r.admitted_count, r.capacity
    from email_control.reserve_notisend_recipient_v1(p_user_id, 'auth', p_attempt_id) r
$$;

revoke all on function email_control.reserve_notisend_recipient_v1(uuid, text, uuid)
  from public, anon, authenticated;
revoke all on function public.focus_auth_reserve_notisend_recipient_v1(uuid, uuid)
  from public, anon, authenticated;

create table personalization.focus_auth_delivery_attempt (
  attempt_id uuid primary key,
  user_id uuid,
  action_type text not null,
  send_ordinal integer not null,
  provider text,
  provider_outcome text not null default 'started',
  provider_message_id text,
  client_route text,
  client_outcome text,
  client_http_status integer,
  verification_route text,
  verification_outcome text,
  verification_http_status integer,
  verification_auth_result text,
  created_at timestamptz not null default now(),
  provider_finished_at timestamptz,
  client_reported_at timestamptz,
  verified_at timestamptz,
  constraint focus_auth_delivery_action_chk check (
    action_type in ('signup', 'magiclink', 'email', 'recovery', 'invite', 'email_change', 'reauthentication')
  ),
  constraint focus_auth_delivery_ordinal_chk check (send_ordinal between 1 and 1000),
  constraint focus_auth_delivery_provider_chk check (provider is null or provider in ('postbox', 'notisend')),
  constraint focus_auth_delivery_provider_outcome_chk check (
    provider_outcome in ('started', 'accepted', 'definitive_reject', 'ambiguous', 'configuration_error')
  ),
  constraint focus_auth_delivery_message_chk check (
    (provider_outcome = 'accepted' and provider_message_id is not null and length(provider_message_id) between 1 and 300)
    or (provider_outcome <> 'accepted' and provider_message_id is null)
  ),
  constraint focus_auth_delivery_client_route_chk check (client_route is null or client_route in ('direct', 'relay')),
  constraint focus_auth_delivery_verify_route_chk check (verification_route is null or verification_route in ('direct', 'relay')),
  constraint focus_auth_delivery_client_outcome_chk check (
    client_outcome is null or client_outcome in ('definitive', 'recovered', 'ambiguous', 'no_route', 'transport_failure')
  ),
  constraint focus_auth_delivery_verify_outcome_chk check (
    verification_outcome is null or verification_outcome in ('definitive', 'recovered', 'ambiguous', 'no_route', 'transport_failure')
  ),
  constraint focus_auth_delivery_verify_auth_result_chk check (
    verification_auth_result is null or verification_auth_result in ('verified', 'failed', 'ambiguous')
  ),
  constraint focus_auth_delivery_client_status_chk check (client_http_status is null or client_http_status between 100 and 599),
  constraint focus_auth_delivery_verify_status_chk check (verification_http_status is null or verification_http_status between 100 and 599)
);

comment on table personalization.focus_auth_delivery_attempt is
  'Private PII-free Auth email attempt/provider/transport ledger. Never store email, OTP, token hashes or IP/User-Agent.';

create index focus_auth_delivery_user_created_idx
  on personalization.focus_auth_delivery_attempt (user_id, created_at desc);

create index focus_auth_delivery_provider_created_idx
  on personalization.focus_auth_delivery_attempt (provider, provider_outcome, created_at desc);

alter table personalization.focus_auth_delivery_attempt enable row level security;
revoke all on personalization.focus_auth_delivery_attempt from public, anon, authenticated;
grant select, insert, update, delete on personalization.focus_auth_delivery_attempt to service_role;


create table personalization.focus_auth_method_attempt (
  attempt_id uuid primary key,
  auth_method text not null,
  outcome text not null default 'started',
  user_id uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint focus_auth_method_attempt_method_chk check (auth_method in ('email', 'custom:yandex')),
  constraint focus_auth_method_attempt_outcome_chk check (outcome in ('started', 'verified', 'failed', 'ambiguous'))
);

comment on table personalization.focus_auth_method_attempt is
  'Private PII-free actual login-method attempt ledger. Counts attempts without email, OTP, IP or User-Agent.';

create index focus_auth_method_attempt_created_idx
  on personalization.focus_auth_method_attempt (auth_method, outcome, created_at desc);

alter table personalization.focus_auth_method_attempt enable row level security;
revoke all on personalization.focus_auth_method_attempt from public, anon, authenticated;
grant select, insert, update, delete on personalization.focus_auth_method_attempt to service_role;

create or replace function public.focus_auth_begin_delivery_v1(
  p_attempt_id uuid,
  p_user_id uuid,
  p_action_type text,
  p_prefer_notisend boolean default false
)
returns table (
  send_ordinal integer,
  is_new boolean,
  previous_provider text,
  previous_outcome text,
  previous_message_id text,
  notisend_admitted boolean
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_ordinal integer;
begin
  if p_attempt_id is null or p_user_id is null then
    raise exception 'attempt and user required' using errcode = '22023';
  end if;
  if p_action_type not in ('signup', 'magiclink', 'email', 'recovery', 'invite', 'email_change', 'reauthentication') then
    raise exception 'unsupported action type' using errcode = '22023';
  end if;

  select a.send_ordinal, false, a.provider, a.provider_outcome, a.provider_message_id
    into send_ordinal, is_new, previous_provider, previous_outcome, previous_message_id
    from personalization.focus_auth_delivery_attempt a
   where a.attempt_id = p_attempt_id
     and a.user_id = p_user_id
     and a.action_type = p_action_type;
  if found then
    notisend_admitted := exists (
      select 1
        from email_control.notisend_recipient_admission n
        join email_control.recommendation_capacity rc
          on rc.capacity_key = 'launch' and rc.provider_period_key = n.period_key
       where n.user_id = p_user_id
    );
    return next;
    return;
  end if;

  perform pg_advisory_xact_lock(hashtextextended(p_user_id::text, 20260801));
  select coalesce(max(a.send_ordinal), 0) + 1
    into v_ordinal
    from personalization.focus_auth_delivery_attempt a
   where a.user_id = p_user_id;

  insert into personalization.focus_auth_delivery_attempt (
    attempt_id, user_id, action_type, send_ordinal
  ) values (
    p_attempt_id, p_user_id, p_action_type, v_ordinal
  );
  insert into personalization.focus_auth_method_attempt (
    attempt_id, auth_method, outcome
  ) values (
    p_attempt_id, 'email', 'started'
  ) on conflict (attempt_id) do nothing;

  send_ordinal := v_ordinal;
  is_new := true;
  previous_provider := null;
  previous_outcome := 'started';
  previous_message_id := null;
  notisend_admitted := false;
  if coalesce(p_prefer_notisend, false)
     or p_action_type <> 'signup'
     or v_ordinal > 1 then
    select r.admitted
      into notisend_admitted
      from email_control.reserve_notisend_recipient_v1(p_user_id, 'auth', p_attempt_id) r;
  end if;
  return next;
end;
$$;

create or replace function public.focus_auth_complete_delivery_v1(
  p_attempt_id uuid,
  p_provider text,
  p_outcome text,
  p_provider_message_id text default null
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_completed boolean;
begin
  if p_provider not in ('postbox', 'notisend') then
    raise exception 'unsupported provider' using errcode = '22023';
  end if;
  if p_outcome not in ('accepted', 'definitive_reject', 'ambiguous', 'configuration_error') then
    raise exception 'unsupported provider outcome' using errcode = '22023';
  end if;
  if (p_outcome = 'accepted') <> (nullif(trim(coalesce(p_provider_message_id, '')), '') is not null) then
    raise exception 'accepted outcome requires one provider receipt' using errcode = '23514';
  end if;

  update personalization.focus_auth_delivery_attempt
     set provider = p_provider,
         provider_outcome = p_outcome,
         provider_message_id = case when p_outcome = 'accepted' then left(trim(p_provider_message_id), 300) else null end,
         provider_finished_at = now()
   where attempt_id = p_attempt_id
     and provider_outcome = 'started';
  v_completed := found;
  if v_completed
     and p_provider = 'notisend'
     and p_outcome in ('definitive_reject', 'configuration_error') then
    delete from email_control.notisend_recipient_admission a
     where a.first_attempt_id = p_attempt_id
       and not a.included_in_provider_snapshot;
  end if;
  return v_completed;
end;
$$;

create or replace function public.focus_auth_get_delivery_receipt_v1(
  p_attempt_id uuid
)
returns table (
  delivery_state text,
  accepted boolean
)
language sql
security definer
set search_path = ''
stable
as $$
  select case
           when a.provider_outcome = 'accepted' then 'accepted'
           when a.provider_outcome in ('started', 'ambiguous') then 'pending_or_ambiguous'
           else 'rejected'
         end,
         a.provider_outcome = 'accepted'
    from personalization.focus_auth_delivery_attempt a
   where a.attempt_id = p_attempt_id
     and a.created_at >= now() - interval '30 minutes'
$$;

create or replace function public.focus_auth_record_client_outcome_v1(
  p_attempt_id uuid,
  p_route text,
  p_outcome text,
  p_http_status integer default null
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
begin
  if not ((p_route in ('direct', 'relay')) or (p_route is null and p_outcome = 'no_route'))
     or p_outcome not in ('definitive', 'recovered', 'ambiguous', 'no_route', 'transport_failure')
     or (p_http_status is not null and (p_http_status < 100 or p_http_status > 599)) then
    raise exception 'invalid client outcome' using errcode = '22023';
  end if;
  update personalization.focus_auth_delivery_attempt
     set client_route = p_route,
         client_outcome = p_outcome,
         client_http_status = p_http_status,
         client_reported_at = now()
   where attempt_id = p_attempt_id
     and created_at >= now() - interval '30 minutes'
     and client_reported_at is null;
  return found;
end;
$$;

create or replace function public.focus_auth_record_verification_v1(
  p_attempt_id uuid,
  p_route text,
  p_outcome text,
  p_http_status integer default null,
  p_verified boolean default null
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
begin
  if not ((p_route in ('direct', 'relay')) or (p_route is null and p_outcome = 'no_route'))
     or p_outcome not in ('definitive', 'recovered', 'ambiguous', 'no_route', 'transport_failure')
     or (p_http_status is not null and (p_http_status < 100 or p_http_status > 599)) then
    raise exception 'invalid verification outcome' using errcode = '22023';
  end if;
  if p_verified is true and not exists (
    select 1
      from personalization.focus_auth_delivery_attempt d
     where d.attempt_id = p_attempt_id
       and d.user_id = (select auth.uid())
       and d.created_at >= now() - interval '30 minutes'
  ) then
    raise exception 'verified outcome requires matching authenticated user' using errcode = '42501';
  end if;
  update personalization.focus_auth_delivery_attempt
     set verification_route = p_route,
         verification_outcome = p_outcome,
         verification_http_status = p_http_status,
         verification_auth_result = case
           when p_verified is true then 'verified'
           when p_verified is false then 'failed'
           else 'ambiguous'
         end,
         verified_at = now()
   where attempt_id = p_attempt_id
     and created_at >= now() - interval '30 minutes'
     and verified_at is null;
  if found then
    update personalization.focus_auth_method_attempt
       set outcome = case
         when p_verified is true then 'verified'
         when p_verified is false then 'failed'
         else 'ambiguous'
       end,
           user_id = case
             when p_verified is true then (
               select d.user_id from personalization.focus_auth_delivery_attempt d where d.attempt_id = p_attempt_id
             )
             else user_id
           end,
           updated_at = now()
     where attempt_id = p_attempt_id
       and auth_method = 'email';
  end if;
  return found;
end;
$$;

create or replace function public.focus_auth_record_method_attempt_v1(
  p_attempt_id uuid,
  p_auth_method text,
  p_outcome text
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
begin
  if p_attempt_id is null
     or p_auth_method <> 'custom:yandex'
     or p_outcome not in ('started', 'verified', 'failed', 'ambiguous')
     or (p_outcome = 'verified' and v_user_id is null) then
    raise exception 'invalid auth method attempt' using errcode = '22023';
  end if;
  insert into personalization.focus_auth_method_attempt (
    attempt_id, auth_method, outcome, user_id
  ) values (
    p_attempt_id, p_auth_method, p_outcome,
    case when p_outcome = 'verified' then v_user_id else null end
  )
  on conflict (attempt_id) do update set
    outcome = case
      when personalization.focus_auth_method_attempt.outcome = 'verified' then 'verified'
      else excluded.outcome
    end,
    user_id = coalesce(personalization.focus_auth_method_attempt.user_id, excluded.user_id),
    updated_at = now()
  where personalization.focus_auth_method_attempt.auth_method = excluded.auth_method
    and personalization.focus_auth_method_attempt.created_at >= now() - interval '30 minutes';
  return found;
end;
$$;

create or replace function public.focus_auth_operator_summary_v1(
  p_since timestamptz default (now() - interval '24 hours')
)
returns jsonb
language plpgsql
security definer
set search_path = ''
stable
as $$
declare
  v_since timestamptz := coalesce(p_since, now() - interval '24 hours');
  v_result jsonb;
begin
  if v_since < now() - interval '90 days' or v_since > now() then
    raise exception 'summary window must be within the last 90 days' using errcode = '22023';
  end if;

  select pg_catalog.jsonb_build_object(
    'schema', 'kenigevents.focus_auth_operator_summary.v1',
    'generated_at', now(),
    'since', v_since,
    'totals', pg_catalog.jsonb_build_object(
      'delivery_attempts', (
        select count(*) from personalization.focus_auth_delivery_attempt d where d.created_at >= v_since
      ),
      'unique_users', (
        select count(distinct d.user_id) from personalization.focus_auth_delivery_attempt d where d.created_at >= v_since
      ),
      'verified_methods', (
        select count(*) from personalization.focus_auth_method_attempt m
         where m.created_at >= v_since and m.outcome = 'verified'
      )
    ),
    'delivery_by_provider', coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'provider', q.provider,
        'outcome', q.provider_outcome,
        'attempts', q.attempts,
        'unique_users', q.unique_users
      ) order by q.provider, q.provider_outcome)
      from (
        select coalesce(d.provider, 'unselected') as provider,
               d.provider_outcome,
               count(*) as attempts,
               count(distinct d.user_id) as unique_users
          from personalization.focus_auth_delivery_attempt d
         where d.created_at >= v_since
         group by coalesce(d.provider, 'unselected'), d.provider_outcome
      ) q
    ), '[]'::jsonb),
    'login_method_outcomes', coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'method', q.auth_method,
        'outcome', q.outcome,
        'attempts', q.attempts
      ) order by q.auth_method, q.outcome)
      from (
        select m.auth_method, m.outcome, count(*) as attempts
          from personalization.focus_auth_method_attempt m
         where m.created_at >= v_since
         group by m.auth_method, m.outcome
      ) q
    ), '[]'::jsonb),
    'otp_issue_transport', coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'route', q.client_route,
        'outcome', q.client_outcome,
        'http_status', q.client_http_status,
        'attempts', q.attempts
      ) order by q.client_route, q.client_outcome, q.client_http_status)
      from (
        select coalesce(d.client_route, 'unreported') as client_route,
               coalesce(d.client_outcome, 'unreported') as client_outcome,
               d.client_http_status,
               count(*) as attempts
          from personalization.focus_auth_delivery_attempt d
         where d.created_at >= v_since
         group by coalesce(d.client_route, 'unreported'),
                  coalesce(d.client_outcome, 'unreported'), d.client_http_status
      ) q
    ), '[]'::jsonb),
    'otp_verify_transport', coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'route', q.verification_route,
        'outcome', q.verification_outcome,
        'auth_result', q.verification_auth_result,
        'http_status', q.verification_http_status,
        'attempts', q.attempts
      ) order by q.verification_route, q.verification_outcome, q.verification_auth_result, q.verification_http_status)
      from (
        select coalesce(d.verification_route, 'unreported') as verification_route,
               coalesce(d.verification_outcome, 'unreported') as verification_outcome,
               coalesce(d.verification_auth_result, 'unreported') as verification_auth_result,
               d.verification_http_status,
               count(*) as attempts
          from personalization.focus_auth_delivery_attempt d
         where d.created_at >= v_since
         group by coalesce(d.verification_route, 'unreported'),
                  coalesce(d.verification_outcome, 'unreported'),
                  coalesce(d.verification_auth_result, 'unreported'), d.verification_http_status
      ) q
    ), '[]'::jsonb),
    'notisend_capacity', (
      select pg_catalog.jsonb_build_object(
        'period_key', rc.provider_period_key,
        'period_ends_at', rc.provider_period_ends_at,
        'provider_reported', rc.provider_used_count,
        'admitted_after_reconcile', a.admitted_count,
        'occupied', rc.provider_used_count + a.admitted_count,
        'capacity', rc.capacity,
        'available', greatest(0, rc.capacity - rc.provider_used_count - a.admitted_count),
        'routing_ready', rc.provider_reconciled_at is not null
          and rc.provider_period_ends_at > now(),
        'reconciled_at', rc.provider_reconciled_at
      )
      from email_control.recommendation_capacity rc
      cross join lateral (
        select count(*)::integer as admitted_count
         from email_control.notisend_recipient_admission
         where period_key = rc.provider_period_key
           and not included_in_provider_snapshot
      ) a
      where rc.capacity_key = 'launch'
    )
  ) into v_result;
  return v_result;
end;
$$;

revoke all on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean) from public, anon, authenticated;
revoke all on function public.focus_auth_reconcile_notisend_capacity_v1(text, timestamptz, integer) from public, anon, authenticated;
revoke all on function public.focus_auth_complete_delivery_v1(uuid, text, text, text) from public, anon, authenticated;
revoke all on function public.focus_auth_get_delivery_receipt_v1(uuid) from public, anon, authenticated;
revoke all on function public.focus_auth_record_client_outcome_v1(uuid, text, text, integer) from public, anon, authenticated;
revoke all on function public.focus_auth_record_verification_v1(uuid, text, text, integer, boolean) from public, anon, authenticated;
revoke all on function public.focus_auth_record_method_attempt_v1(uuid, text, text) from public, anon, authenticated;
revoke all on function public.focus_auth_operator_summary_v1(timestamptz) from public, anon, authenticated;

grant execute on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean) to service_role;
grant execute on function public.focus_auth_reconcile_notisend_capacity_v1(text, timestamptz, integer) to service_role;
grant execute on function public.focus_auth_reserve_notisend_recipient_v1(uuid, uuid) to service_role;
grant execute on function public.focus_auth_complete_delivery_v1(uuid, text, text, text) to service_role;
grant execute on function public.focus_auth_get_delivery_receipt_v1(uuid) to anon, authenticated;
grant execute on function public.focus_auth_record_client_outcome_v1(uuid, text, text, integer) to anon, authenticated;
grant execute on function public.focus_auth_record_verification_v1(uuid, text, text, integer, boolean) to anon, authenticated;
grant execute on function public.focus_auth_record_method_attempt_v1(uuid, text, text) to anon, authenticated;
grant execute on function public.focus_auth_operator_summary_v1(timestamptz) to service_role;

notify pgrst, 'reload schema';
