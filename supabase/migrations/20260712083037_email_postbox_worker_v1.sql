-- Transactional-only Postbox worker boundary. The worker never claims the
-- NotiSend recommendation stream and cannot release a network-started claim
-- back to retryable state.

create or replace function public.email_claim_postbox_outbox_v2(
  p_worker_id text,
  p_limit integer default 10,
  p_lease_seconds integer default 120
)
returns table (
  outbox_id uuid,
  lease_token uuid,
  stream text,
  provider text,
  kind text,
  recipient_email text,
  payload_json jsonb,
  template_version text,
  dry_run boolean,
  attempt_number integer
)
language plpgsql
security definer
set search_path = ''
as $$
begin
  p_worker_id := trim(coalesce(p_worker_id, ''));
  if length(p_worker_id) not between 1 and 120
     or p_limit not between 1 and 25
     or p_lease_seconds not between 30 and 900 then
    raise exception 'invalid Postbox claim parameters' using errcode = '22023';
  end if;

  return query
  with eligible as (
    select o.id
      from email_control.email_outbox o
      join email_control.recipient_identity i on i.id = o.identity_id
      join auth.users u on u.id = o.user_id
      join email_control.runtime_switch g on g.switch_key = 'global'
      join email_control.runtime_switch s on s.switch_key = 'transactional'
     where o.stream = 'transactional'
       and o.provider = 'postbox'
       and o.status in ('ready', 'retryable')
       and o.next_attempt_at <= now()
       and u.email_confirmed_at is not null
       and lower(trim(u.email)) = i.normalized_email
       and (
         o.dry_run
         or (g.enabled and s.enabled and not g.dry_run_only and not s.dry_run_only)
       )
       and not exists (
         select 1
           from email_control.suppression x
          where x.email_hmac = i.email_hmac
            and x.active
            and x.scope in ('all', 'transactional')
       )
       and (
         o.kind = 'account_auth'
         or exists (
           select 1
             from email_control.purpose_consent c
            where c.user_id = o.user_id
              and c.purpose = 'transactional_event'
              and c.state = 'active'
         )
       )
     order by o.next_attempt_at, o.created_at
     for update of o skip locked
     limit p_limit
  ), claimed as (
    update email_control.email_outbox o
       set status = 'claimed',
           lease_owner = p_worker_id,
           lease_token = extensions.gen_random_uuid(),
           lease_expires_at = now() + make_interval(secs => p_lease_seconds),
           attempts = o.attempts + 1,
           network_started_at = null,
           updated_at = now()
      from eligible e
     where o.id = e.id
     returning o.*
  )
  select c.id, c.lease_token, c.stream, c.provider, c.kind,
         i.normalized_email, c.payload_json, c.template_version, c.dry_run, c.attempts
    from claimed c
    join email_control.recipient_identity i on i.id = c.identity_id;
end;
$$;

create or replace function public.email_fail_postbox_claim_before_network_v1(
  p_outbox_id uuid,
  p_lease_token uuid,
  p_error_class text,
  p_retryable boolean default false,
  p_retry_at timestamptz default null
)
returns text
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_status text;
begin
  p_error_class := trim(coalesce(p_error_class, ''));
  if length(p_error_class) not between 1 and 160 then
    raise exception 'invalid pre-network error class' using errcode = '22023';
  end if;
  v_status := case when coalesce(p_retryable, false) then 'retryable' else 'terminal_failed' end;

  update email_control.email_outbox
     set status = v_status,
         last_error_class = p_error_class,
         next_attempt_at = case
           when v_status = 'retryable' then coalesce(p_retry_at, now() + interval '5 minutes')
           else next_attempt_at
         end,
         lease_owner = null,
         lease_token = null,
         lease_expires_at = null,
         updated_at = now()
   where id = p_outbox_id
     and provider = 'postbox'
     and stream = 'transactional'
     and status = 'claimed'
     and lease_token = p_lease_token
     and lease_expires_at > now()
     and network_started_at is null;
  if not found then
    raise exception 'active pre-network Postbox claim required' using errcode = '40001';
  end if;
  return v_status;
end;
$$;

create or replace function public.email_recover_expired_postbox_claims_v2()
returns table (retryable_count integer, unknown_count integer)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_retryable integer;
  v_unknown integer;
begin
  with recovered as (
    update email_control.email_outbox
       set status = 'retryable',
           lease_owner = null,
           lease_token = null,
           lease_expires_at = null,
           next_attempt_at = now(),
           updated_at = now()
     where provider = 'postbox'
       and stream = 'transactional'
       and status = 'claimed'
       and lease_expires_at <= now()
       and network_started_at is null
     returning 1
  ) select count(*)::integer into v_retryable from recovered;

  with quarantined as (
    update email_control.email_outbox
       set status = 'unknown_delivery',
           lease_owner = null,
           lease_token = null,
           lease_expires_at = null,
           updated_at = now()
     where provider = 'postbox'
       and stream = 'transactional'
       and status = 'claimed'
       and lease_expires_at <= now()
       and network_started_at is not null
     returning 1
  ) select count(*)::integer into v_unknown from quarantined;

  return query select v_retryable, v_unknown;
end;
$$;

create or replace function public.email_postbox_health_v1()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  with outbox as (
    select *
      from email_control.email_outbox
     where provider = 'postbox' and stream = 'transactional'
  ), aggregate_state as (
    select
      count(*) filter (where status = 'ready')::integer as ready_count,
      count(*) filter (where status = 'retryable' and next_attempt_at <= now())::integer as retryable_due_count,
      count(*) filter (where status = 'claimed')::integer as claimed_count,
      count(*) filter (where status = 'claimed' and lease_expires_at <= now())::integer as expired_claim_count,
      count(*) filter (where status = 'submitted')::integer as submitted_count,
      count(*) filter (where status = 'submitted' and updated_at <= now() - interval '15 minutes')::integer as submitted_over_15m_count,
      count(*) filter (where status = 'submitted' and updated_at <= now() - interval '60 minutes')::integer as submitted_over_60m_count,
      count(*) filter (where status = 'unknown_delivery')::integer as unknown_delivery_count,
      count(*) filter (where status = 'terminal_failed' and updated_at >= now() - interval '24 hours')::integer as terminal_failed_24h_count,
      count(*) filter (where status = 'delivered' and updated_at >= now() - interval '24 hours')::integer as delivered_24h_count,
      coalesce(max(extract(epoch from (now() - created_at))) filter (where status in ('ready', 'retryable')), 0)::bigint as oldest_pending_seconds,
      coalesce(max(extract(epoch from (now() - updated_at))) filter (where status = 'submitted'), 0)::bigint as oldest_submitted_seconds
    from outbox
  ), event_state as (
    select
      count(*) filter (where created_at >= now() - interval '24 hours')::integer as provider_events_24h_count,
      max(created_at) as last_provider_event_at
    from email_control.provider_event
    where provider = 'postbox'
  )
  select jsonb_build_object(
    'ready_count', a.ready_count,
    'retryable_due_count', a.retryable_due_count,
    'claimed_count', a.claimed_count,
    'expired_claim_count', a.expired_claim_count,
    'submitted_count', a.submitted_count,
    'submitted_over_15m_count', a.submitted_over_15m_count,
    'submitted_over_60m_count', a.submitted_over_60m_count,
    'unknown_delivery_count', a.unknown_delivery_count,
    'terminal_failed_24h_count', a.terminal_failed_24h_count,
    'delivered_24h_count', a.delivered_24h_count,
    'oldest_pending_seconds', a.oldest_pending_seconds,
    'oldest_submitted_seconds', a.oldest_submitted_seconds,
    'provider_events_24h_count', e.provider_events_24h_count,
    'last_provider_event_at', e.last_provider_event_at,
    'observed_at', now()
  )
  from aggregate_state a cross join event_state e;
$$;

revoke execute on function public.email_claim_postbox_outbox_v2(text, integer, integer)
  from public, anon, authenticated;
revoke execute on function public.email_fail_postbox_claim_before_network_v1(uuid, uuid, text, boolean, timestamptz)
  from public, anon, authenticated;
revoke execute on function public.email_recover_expired_postbox_claims_v2()
  from public, anon, authenticated;
revoke execute on function public.email_postbox_health_v1()
  from public, anon, authenticated;

grant execute on function public.email_claim_postbox_outbox_v2(text, integer, integer)
  to service_role;
grant execute on function public.email_fail_postbox_claim_before_network_v1(uuid, uuid, text, boolean, timestamptz)
  to service_role;
grant execute on function public.email_recover_expired_postbox_claims_v2()
  to service_role;
grant execute on function public.email_postbox_health_v1()
  to service_role;
