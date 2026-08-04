-- Rollback-only contract for unified Postbox outbox/Auth correlation and health.
begin;

do $$
declare
  v_user uuid := extensions.gen_random_uuid();
  v_user_bounce uuid := extensions.gen_random_uuid();
  v_user_outbox uuid := extensions.gen_random_uuid();
  v_attempt uuid := extensions.gen_random_uuid();
  v_attempt_bounce uuid := extensions.gen_random_uuid();
  v_attempt_pending uuid := extensions.gen_random_uuid();
  v_hmac text := repeat('a', 43);
  v_hmac_bounce text := repeat('b', 43);
  v_hmac_outbox text := repeat('c', 43);
  v_legacy_hmac text := repeat('d', 43);
  v_outbox uuid;
  v_begin record;
  v_status text;
  v_health jsonb;
  v_event_at timestamptz := now();
begin
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user, 'postbox-auth-contract@example.test', now());

  select * into v_begin
    from public.focus_auth_begin_delivery_v1(
      v_attempt, v_user, 'signup', false
    );
  if not v_begin.is_new or v_begin.send_ordinal <> 1 then
    raise exception 'focus Auth attempt was not reserved';
  end if;
  if not public.focus_auth_complete_delivery_v1(
    v_attempt, 'postbox', 'accepted', 'postbox-auth-message-1'
  ) then
    raise exception 'focus Auth provider receipt was not persisted';
  end if;
  if not exists (
    select 1
      from email_control.postbox_message_correlation c
     where c.provider_message_id = 'postbox-auth-message-1'
       and c.source_kind = 'focus_auth'
       and c.auth_attempt_id = v_attempt
       and c.email_hmac is null
  ) then
    raise exception 'focus Auth receipt was not registered unbound';
  end if;
  if (
    select postbox_feedback_state
      from personalization.focus_auth_delivery_attempt
     where attempt_id = v_attempt
  ) <> 'submitted' then
    raise exception 'focus Auth submitted feedback state missing';
  end if;

  v_status := public.email_record_postbox_event_v2(
    'postbox-auth-delivery-1',
    'postbox-auth-message-1',
    'delivered',
    v_event_at,
    v_hmac,
    1,
    repeat('1', 64)
  );
  if v_status <> 'applied' then
    raise exception 'focus Auth delivery was not applied through v2 compatibility';
  end if;
  if not exists (
    select 1
      from email_control.postbox_message_correlation c
     where c.provider_message_id = 'postbox-auth-message-1'
       and c.email_hmac = v_hmac
       and c.hmac_key_version = 1
       and c.bound_at is not null
  ) then
    raise exception 'focus Auth HMAC was not bound by authenticated feedback';
  end if;
  if not exists (
    select 1
      from personalization.focus_auth_delivery_attempt a
     where a.attempt_id = v_attempt
       and a.postbox_feedback_state = 'delivered'
       and a.postbox_event_count = 1
       and a.postbox_last_event_type = 'delivered'
  ) then
    raise exception 'focus Auth delivery projection missing';
  end if;
  if public.email_record_postbox_event_v3(
    'postbox-auth-delivery-1',
    'postbox-auth-message-1',
    'delivered',
    v_event_at,
    v_hmac,
    1,
    repeat('1', 64)
  ) <> 'duplicate' then
    raise exception 'focus Auth exact duplicate was not idempotent';
  end if;

  begin
    perform public.email_record_postbox_event_v3(
      'postbox-auth-wrong-hmac',
      'postbox-auth-message-1',
      'hard_bounce',
      now(),
      repeat('x', 43),
      1,
      repeat('2', 64)
    );
    raise exception 'bound focus Auth receipt accepted a wrong HMAC';
  exception when check_violation then null;
  end;

  insert into auth.users (id, email, email_confirmed_at)
  values (v_user_bounce, 'postbox-auth-bounce@example.test', now());
  insert into personalization.focus_auth_delivery_attempt (
    attempt_id,
    user_id,
    action_type,
    send_ordinal,
    provider,
    provider_outcome,
    provider_message_id,
    provider_finished_at
  ) values (
    v_attempt_bounce,
    v_user_bounce,
    'signup',
    1,
    'postbox',
    'accepted',
    'postbox-auth-message-2',
    now()
  );
  if public.email_record_postbox_event_v3(
    'postbox-auth-bounce-1',
    'postbox-auth-message-2',
    'hard_bounce',
    now(),
    v_hmac_bounce,
    1,
    repeat('3', 64)
  ) <> 'applied' then
    raise exception 'focus Auth hard bounce was not applied';
  end if;
  if (
    select postbox_feedback_state
      from personalization.focus_auth_delivery_attempt
     where attempt_id = v_attempt_bounce
  ) <> 'terminal_failed' then
    raise exception 'focus Auth hard bounce did not become terminal';
  end if;
  if not exists (
    select 1
      from email_control.suppression s
     where s.email_hmac = v_hmac_bounce
       and s.scope = 'all'
       and s.provider = 'postbox'
       and s.reason = 'hard_bounce'
       and s.active
  ) then
    raise exception 'focus Auth hard-bounce suppression missing';
  end if;

  if not public.email_register_legacy_postbox_auth_v1(
    'postbox-legacy-message-1', repeat('4', 64), now() - interval '5 days'
  ) then
    raise exception 'legacy receipt registration did not insert';
  end if;
  if public.email_register_legacy_postbox_auth_v1(
    'postbox-legacy-message-1', repeat('4', 64), now() - interval '5 days'
  ) then
    raise exception 'legacy receipt registration was not idempotent';
  end if;
  if public.email_record_postbox_event_v3(
    'postbox-legacy-complaint-1',
    'postbox-legacy-message-1',
    'complaint',
    now(),
    v_legacy_hmac,
    1,
    repeat('5', 64)
  ) <> 'applied' then
    raise exception 'registered legacy complaint was not applied';
  end if;
  if not exists (
    select 1
      from email_control.suppression s
     where s.email_hmac = v_legacy_hmac
       and s.scope = 'all'
       and s.reason = 'complaint'
       and s.active
  ) then
    raise exception 'legacy complaint suppression missing';
  end if;

  insert into auth.users (id, email, email_confirmed_at)
  values (v_user_outbox, 'postbox-outbox-contract@example.test', now());
  perform public.email_sync_verified_identity_v1(
    v_user_outbox,
    'postbox-outbox-contract@example.test',
    v_hmac_outbox,
    1,
    now()
  );
  v_outbox := public.email_enqueue_transactional_v1(
    v_user_outbox,
    'account_auth',
    null,
    'postbox-unified-outbox-1',
    'transactional-plain-v1',
    '{"subject":"Contract","text":"Body"}'::jsonb,
    repeat('6', 64),
    false
  );
  update email_control.email_outbox
     set status = 'submitted',
         provider_message_id = 'postbox-outbox-message-1'
   where id = v_outbox;
  if not exists (
    select 1
      from email_control.postbox_message_correlation c
     where c.provider_message_id = 'postbox-outbox-message-1'
       and c.source_kind = 'transactional_outbox'
       and c.outbox_id = v_outbox
       and c.email_hmac = v_hmac_outbox
  ) then
    raise exception 'outbox receipt trigger did not register correlation';
  end if;
  if public.email_record_postbox_event_v3(
    'postbox-outbox-delivery-1',
    'postbox-outbox-message-1',
    'delivered',
    now(),
    v_hmac_outbox,
    1,
    repeat('7', 64)
  ) <> 'applied'
     or (
       select status from email_control.email_outbox where id = v_outbox
     ) <> 'delivered' then
    raise exception 'unified outbox delivery was not applied';
  end if;

  insert into personalization.focus_auth_delivery_attempt (
    attempt_id,
    user_id,
    action_type,
    send_ordinal,
    provider,
    provider_outcome,
    provider_message_id,
    provider_finished_at
  ) values (
    v_attempt_pending,
    v_user,
    'magiclink',
    2,
    'postbox',
    'accepted',
    'postbox-auth-pending-1',
    now() - interval '2 hours'
  );

  if public.email_record_postbox_event_v3(
    'postbox-unmatched-event-1',
    'postbox-unknown-message-1',
    'delivered',
    now(),
    v_hmac,
    1,
    repeat('8', 64)
  ) <> 'correlation_pending' then
    raise exception 'unknown receipt did not remain pending';
  end if;
  if exists (
    select 1
      from email_control.provider_event e
     where e.provider = 'postbox'
       and e.provider_event_key = 'postbox-unmatched-event-1'
  ) then
    raise exception 'unknown receipt inserted a provider-event tombstone';
  end if;

  v_health := public.email_postbox_health_v1();
  if (v_health->>'postbox_auth_submitted_count')::integer < 1
     or (v_health->>'postbox_correlation_unbound_count')::integer < 1
     or (v_health->>'postbox_legacy_correlation_count')::integer <> 1
     or (v_health->>'postbox_missing_correlation_count')::integer <> 0
     or (v_health->>'submitted_count')::integer < 1
     or (v_health->>'oldest_submitted_seconds')::integer < 3600 then
    raise exception 'unified Postbox health projection is incomplete';
  end if;

  if has_function_privilege(
    'anon',
    'public.email_record_postbox_event_v3(text,text,text,timestamptz,text,integer,text)',
    'EXECUTE'
  ) or has_function_privilege(
    'authenticated',
    'public.email_register_legacy_postbox_auth_v1(text,text,timestamptz)',
    'EXECUTE'
  ) then
    raise exception 'browser role can execute unified Postbox control RPCs';
  end if;
end;
$$;

rollback;
