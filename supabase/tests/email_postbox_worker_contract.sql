-- Rollback-only contract for the transactional Postbox worker RPCs.
begin;

do $$
declare
  v_user uuid := extensions.gen_random_uuid();
  v_hmac text := repeat('w', 43);
  v_outbox uuid;
  v_claim record;
  v_status text;
  v_health jsonb;
begin
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user, 'postbox-worker-contract@example.test', now());
  perform public.email_sync_verified_identity_v1(
    v_user, 'postbox-worker-contract@example.test', v_hmac, 1, now()
  );
  v_outbox := public.email_enqueue_transactional_v1(
    v_user, 'account_auth', null, 'postbox-worker-contract-1',
    'transactional-plain-v1',
    '{"subject":"Contract","text":"Contract body"}'::jsonb,
    repeat('a', 64), false
  );

  -- A non-dry-run claim is gated by both live switches.
  select * into v_claim
    from public.email_claim_postbox_outbox_v2('contract-worker', 1, 120);
  if v_claim.outbox_id is not null then
    raise exception 'Postbox claim bypassed disabled switches';
  end if;
  update email_control.runtime_switch
     set enabled = true, dry_run_only = false
   where switch_key in ('global', 'transactional');

  select * into v_claim
    from public.email_claim_postbox_outbox_v2('contract-worker', 1, 120);
  if v_claim.outbox_id <> v_outbox
     or v_claim.provider <> 'postbox'
     or v_claim.stream <> 'transactional'
     or v_claim.recipient_email <> 'postbox-worker-contract@example.test' then
    raise exception 'transactional Postbox claim mismatch';
  end if;

  v_status := public.email_fail_postbox_claim_before_network_v1(
    v_outbox, v_claim.lease_token, 'fixture_preflight', true,
    now() - interval '1 second'
  );
  if v_status <> 'retryable'
     or (select status from email_control.email_outbox where id = v_outbox) <> 'retryable'
     or exists (select 1 from email_control.send_attempt where outbox_id = v_outbox) then
    raise exception 'pre-network failure created network evidence or wrong status';
  end if;

  select * into v_claim
    from public.email_claim_postbox_outbox_v2('contract-worker', 1, 120);
  perform public.email_mark_network_started_v1(
    v_outbox, v_claim.lease_token, repeat('b', 64)
  );
  update email_control.email_outbox
     set lease_expires_at = now() - interval '1 second'
   where id = v_outbox;
  perform public.email_recover_expired_postbox_claims_v2();
  if (select status from email_control.email_outbox where id = v_outbox) <> 'unknown_delivery' then
    raise exception 'network-started expired claim was not quarantined';
  end if;

  v_health := public.email_postbox_health_v1();
  if (v_health->>'unknown_delivery_count')::integer < 1
     or not (v_health ? 'observed_at')
     or not (v_health ? 'provider_events_24h_count') then
    raise exception 'Postbox health projection missing required counters';
  end if;

  if has_function_privilege(
    'anon', 'public.email_claim_postbox_outbox_v2(text,integer,integer)', 'EXECUTE'
  ) or has_function_privilege(
    'authenticated', 'public.email_postbox_health_v1()', 'EXECUTE'
  ) then
    raise exception 'browser role can execute Postbox worker RPC';
  end if;
end;
$$;

rollback;
