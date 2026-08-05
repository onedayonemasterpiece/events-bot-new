-- Rollback-only contract for unified Postbox outbox/Auth correlation and health.
begin;

do $$
declare
  v_user uuid := extensions.gen_random_uuid();
  v_user_bounce uuid := extensions.gen_random_uuid();
  v_user_outbox uuid := extensions.gen_random_uuid();
  v_attempt uuid := extensions.gen_random_uuid();
  v_attempt_long uuid := extensions.gen_random_uuid();
  v_attempt_bounce uuid := extensions.gen_random_uuid();
  v_attempt_suppressed uuid := extensions.gen_random_uuid();
  v_change_1 uuid := extensions.gen_random_uuid();
  v_change_2 uuid := extensions.gen_random_uuid();
  v_blocked_1 uuid := extensions.gen_random_uuid();
  v_blocked_2 uuid := extensions.gen_random_uuid();
  v_hmac text := repeat('a', 43);
  v_hmac_bounce text := repeat('b', 43);
  v_hmac_outbox text := repeat('c', 43);
  v_legacy_hmac text := repeat('d', 43);
  v_change_hmac_1 text := repeat('e', 43);
  v_change_hmac_2 text := repeat('f', 43);
  v_long_message text := repeat('m', 512);
  v_long_event text := repeat('k', 300);
  v_outbox uuid;
  v_begin jsonb;
  v_status text;
  v_health jsonb;
  v_event_at timestamptz := date_trunc('second', now());
begin
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user, 'postbox-auth-contract@example.test', now());

  v_begin := public.focus_auth_begin_delivery_batch_v1(
    v_user,
    'signup',
    jsonb_build_array(jsonb_build_object(
      'attempt_id', v_attempt,
      'prefer_notisend', false,
      'email_hmac', v_hmac,
      'hmac_key_version', 1
    ))
  );
  if not (v_begin->>'admitted')::boolean
     or v_begin->>'admission_status' <> 'admitted'
     or not (v_begin->'results'->0->>'is_new')::boolean
     or (v_begin->'results'->0->>'send_ordinal')::integer <> 1 then
    raise exception 'focus Auth attempt was not admitted';
  end if;
  if not exists (
    select 1 from personalization.focus_auth_delivery_attempt a
     where a.attempt_id = v_attempt
       and a.recipient_hmac = v_hmac
       and a.recipient_hmac_key_version = 1
       and a.network_claimed_at is not null
  ) then
    raise exception 'focus Auth admission proof was not persisted';
  end if;
  if not public.focus_auth_complete_delivery_batch_v1(jsonb_build_array(
    jsonb_build_object(
      'attempt_id', v_attempt,
      'provider', 'postbox',
      'outcome', 'accepted',
      'provider_message_id', 'postbox-auth-message-1'
    )
  )) then
    raise exception 'focus Auth provider receipt was not persisted';
  end if;
  if not exists (
    select 1 from email_control.postbox_message_correlation c
     where c.provider_message_id = 'postbox-auth-message-1'
       and c.source_kind = 'focus_auth'
       and c.auth_attempt_id = v_attempt
       and c.email_hmac = v_hmac
       and c.hmac_key_version = 1
       and c.bound_at is not null
  ) then
    raise exception 'new focus Auth correlation was not immediately bound';
  end if;

  v_status := public.email_record_postbox_event_v2(
    'postbox-auth-delivery-1', 'postbox-auth-message-1', 'delivered',
    v_event_at, v_hmac, 1, repeat('1', 64)
  );
  if v_status <> 'applied' then
    raise exception 'focus Auth delivery was not applied through v2 compatibility';
  end if;
  if public.email_record_postbox_event_v3(
    'postbox-auth-delivery-1', 'postbox-auth-message-1', 'delivered',
    v_event_at, v_hmac, 1, repeat('1', 64)
  ) <> 'duplicate' then
    raise exception 'focus Auth exact duplicate was not idempotent';
  end if;
  begin
    perform public.email_record_postbox_event_v3(
      'postbox-auth-delivery-1', 'postbox-auth-message-1', 'delivered',
      v_event_at, v_hmac, 1, repeat('2', 64)
    );
    raise exception 'conflicting duplicate event was accepted';
  exception when check_violation then null;
  end;
  begin
    perform public.email_record_postbox_event_v3(
      'postbox-auth-wrong-hmac', 'postbox-auth-message-1', 'hard_bounce',
      now(), repeat('x', 43), 1, repeat('3', 64)
    );
    raise exception 'bound focus Auth receipt accepted a wrong HMAC';
  exception when check_violation then null;
  end;

  -- Maximum accepted receipt/event-key lengths remain correlatable.
  v_begin := public.focus_auth_begin_delivery_batch_v1(
    v_user, 'magiclink', jsonb_build_array(jsonb_build_object(
      'attempt_id', v_attempt_long, 'prefer_notisend', false,
      'email_hmac', v_hmac, 'hmac_key_version', 1
    ))
  );
  perform public.focus_auth_complete_delivery_batch_v1(jsonb_build_array(
    jsonb_build_object(
      'attempt_id', v_attempt_long, 'provider', 'postbox',
      'outcome', 'accepted', 'provider_message_id', v_long_message
    )
  ));
  if public.email_record_postbox_event_v3(
    v_long_event, v_long_message, 'open', now(), v_hmac, 1, repeat('4', 64)
  ) <> 'applied' then
    raise exception 'maximum Postbox identifiers were not accepted';
  end if;
  update personalization.focus_auth_delivery_attempt
     set postbox_event_count = 1000
   where attempt_id = v_attempt_long;
  v_status := public.email_record_postbox_event_v3(
    'postbox-auth-open-1001', v_long_message, 'open', now(),
    v_hmac, 1, repeat('5', 64)
  );
  if v_status <> 'applied' or (
    select postbox_event_count from personalization.focus_auth_delivery_attempt
     where attempt_id = v_attempt_long
  ) <> 1001 then
    raise exception 'Postbox event count retained the obsolete 1000-event cap: %, %',
      v_status, (select postbox_event_count from personalization.focus_auth_delivery_attempt
                  where attempt_id = v_attempt_long);
  end if;

  -- Pre-ledger accepted Auth remains one-time bindable by authenticated feedback.
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user_bounce, 'postbox-auth-bounce@example.test', now());
  insert into personalization.focus_auth_delivery_attempt (
    attempt_id, user_id, action_type, send_ordinal, provider,
    provider_outcome, provider_message_id, provider_finished_at
  ) values (
    v_attempt_bounce, v_user_bounce, 'signup', 1, 'postbox',
    'accepted', 'postbox-auth-message-2', now()
  );
  if public.email_record_postbox_event_v3(
    'postbox-auth-bounce-1', 'postbox-auth-message-2', 'hard_bounce',
    now(), v_hmac_bounce, 1, repeat('6', 64)
  ) <> 'applied' then
    raise exception 'focus Auth hard bounce was not applied';
  end if;
  if not exists (
    select 1 from email_control.suppression s
     where s.email_hmac = v_hmac_bounce and s.hmac_key_version = 1
       and s.scope = 'all' and s.reason = 'hard_bounce' and s.active
  ) then
    raise exception 'focus Auth hard-bounce suppression missing';
  end if;

  v_begin := public.focus_auth_begin_delivery_batch_v1(
    v_user_bounce, 'magiclink', jsonb_build_array(jsonb_build_object(
      'attempt_id', v_attempt_suppressed, 'prefer_notisend', true,
      'email_hmac', v_hmac_bounce, 'hmac_key_version', 1
    ))
  );
  if (v_begin->>'admitted')::boolean
     or v_begin->>'admission_status' <> 'recipient_suppressed'
     or exists (select 1 from personalization.focus_auth_delivery_attempt
                 where attempt_id = v_attempt_suppressed) then
    raise exception 'exact suppressed Auth identity reached reservation';
  end if;
  begin
    perform public.focus_auth_begin_delivery_batch_v1(
      v_user_bounce, 'magiclink', jsonb_build_array(jsonb_build_object(
        'attempt_id', extensions.gen_random_uuid(), 'prefer_notisend', true,
        'email_hmac', v_hmac_bounce, 'hmac_key_version', 2
      ))
    );
    raise exception 'HMAC version conflict failed open';
  exception when check_violation then null;
  end;

  -- Secure email change admits both exact identities atomically, independent of
  -- unrelated historical user identity; either exact suppression blocks both.
  v_begin := public.focus_auth_begin_delivery_batch_v1(
    v_user_bounce, 'email_change', jsonb_build_array(
      jsonb_build_object('attempt_id', v_change_1, 'prefer_notisend', false,
                         'email_hmac', v_change_hmac_1, 'hmac_key_version', 1),
      jsonb_build_object('attempt_id', v_change_2, 'prefer_notisend', false,
                         'email_hmac', v_change_hmac_2, 'hmac_key_version', 1)
    )
  );
  if not (v_begin->>'admitted')::boolean
     or jsonb_array_length(v_begin->'results') <> 2 then
    raise exception 'legitimate secure email change was not admitted';
  end if;
  insert into email_control.suppression (
    email_hmac, hmac_key_version, scope, provider, reason, provider_event_key
  ) values (
    repeat('g', 43), 1, 'transactional', 'postbox', 'unsubscribe',
    repeat('u', 300)
  );
  v_begin := public.focus_auth_begin_delivery_batch_v1(
    v_user_bounce, 'email_change', jsonb_build_array(
      jsonb_build_object('attempt_id', v_blocked_1, 'prefer_notisend', false,
                         'email_hmac', repeat('g', 43), 'hmac_key_version', 1),
      jsonb_build_object('attempt_id', v_blocked_2, 'prefer_notisend', false,
                         'email_hmac', repeat('h', 43), 'hmac_key_version', 1)
    )
  );
  if (v_begin->>'admitted')::boolean
     or exists (select 1 from personalization.focus_auth_delivery_attempt
                 where attempt_id in (v_blocked_1, v_blocked_2)) then
    raise exception 'suppressed email-change batch was partially reserved';
  end if;

  -- Legacy registration requires independent evidence and is exact-idempotent.
  if not public.email_register_legacy_postbox_auth_v1(
    'postbox-legacy-message-1', repeat('7', 64), now() - interval '5 days'
  ) then
    raise exception 'legacy receipt registration did not insert';
  end if;
  if public.email_register_legacy_postbox_auth_v1(
    'postbox-legacy-message-1', repeat('7', 64), now() - interval '5 days'
  ) then
    raise exception 'legacy receipt registration was not idempotent';
  end if;
  begin
    perform public.email_register_legacy_postbox_auth_v1(
      'postbox-legacy-message-1', repeat('8', 64), now() - interval '5 days'
    );
    raise exception 'conflicting legacy evidence was accepted';
  exception when check_violation then null;
  end;
  begin
    perform public.email_register_legacy_postbox_auth_v1(
      'postbox-auth-message-1', repeat('9', 64), now() - interval '5 days'
    );
    raise exception 'cross-source Auth/legacy MessageId collision was accepted';
  exception when check_violation then null;
  end;
  if public.email_record_postbox_event_v3(
    'postbox-legacy-complaint-1', 'postbox-legacy-message-1', 'complaint',
    now(), v_legacy_hmac, 1, repeat('a', 64)
  ) <> 'applied' then
    raise exception 'registered legacy complaint was not applied';
  end if;

  -- Transactional outbox correlation is immediate and its receipt identity is
  -- immutable. The global registry fails closed on cross-source MessageId reuse.
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user_outbox, 'postbox-outbox-contract@example.test', now());
  perform public.email_sync_verified_identity_v1(
    v_user_outbox, 'postbox-outbox-contract@example.test',
    v_hmac_outbox, 1, now()
  );
  v_outbox := public.email_enqueue_transactional_v1(
    v_user_outbox, 'account_auth', null, 'postbox-unified-outbox-1',
    'transactional-plain-v1', '{"subject":"Contract","text":"Body"}'::jsonb,
    repeat('b', 64), false
  );
  update email_control.email_outbox
     set status = 'submitted', provider_message_id = 'postbox-outbox-message-1'
   where id = v_outbox;
  if not exists (
    select 1 from email_control.postbox_message_correlation c
     where c.provider_message_id = 'postbox-outbox-message-1'
       and c.source_kind = 'transactional_outbox'
       and c.outbox_id = v_outbox and c.email_hmac = v_hmac_outbox
  ) then
    raise exception 'outbox receipt trigger did not register correlation';
  end if;
  begin
    update email_control.email_outbox
       set provider_message_id = 'postbox-auth-message-1'
     where id = v_outbox;
    raise exception 'cross-source outbox/Auth MessageId collision was accepted';
  exception when unique_violation then null;
  end;
  begin
    perform public.email_sync_verified_identity_v1(
      v_user_bounce, 'postbox-auth-bounce@example.test',
      v_hmac_bounce, 1, now()
    );
    update email_control.email_outbox
       set identity_id = (
         select i.id from email_control.recipient_identity i
          where i.user_id = v_user_bounce
       )
     where id = v_outbox;
    raise exception 'accepted outbox receipt identity was mutable';
  exception when check_violation then null;
  end;
  v_status := public.email_record_postbox_event_v3(
    'postbox-outbox-delivery-1', 'postbox-outbox-message-1', 'delivered',
    now(), v_hmac_outbox, 1, repeat('c', 64)
  );
  if v_status <> 'applied' or (
    select status from email_control.email_outbox where id = v_outbox
  ) <> 'delivered' then
    raise exception 'unified outbox delivery was not projected: %, %', v_status,
      (select status from email_control.email_outbox where id = v_outbox);
  end if;

  if public.email_record_postbox_event_v3(
    'postbox-unmatched-event-1', 'postbox-unknown-message-1', 'delivered',
    now(), v_hmac, 1, repeat('d', 64)
  ) <> 'correlation_pending' or exists (
    select 1 from email_control.provider_event e
     where e.provider = 'postbox'
       and e.provider_event_key = 'postbox-unmatched-event-1'
  ) then
    raise exception 'unknown receipt did not remain safely pending';
  end if;

  v_health := public.email_postbox_health_v1();
  if (v_health->>'postbox_auth_submitted_count')::bigint < 1
     or (v_health->>'postbox_correlation_unbound_count')::bigint <> 0
     or (v_health->>'postbox_legacy_correlation_count')::bigint <> 1
     or (v_health->>'postbox_missing_correlation_count')::bigint <> 0 then
    raise exception 'unified Postbox health projection is incomplete: %', v_health;
  end if;

  if has_function_privilege(
    'anon',
    'public.email_record_postbox_event_v3(text,text,text,timestamptz,text,integer,text)',
    'EXECUTE'
  ) or has_function_privilege(
    'authenticated',
    'public.email_register_legacy_postbox_auth_v1(text,text,timestamptz)',
    'EXECUTE'
  ) or has_function_privilege(
    'anon', 'public.focus_auth_begin_delivery_batch_v1(uuid,text,jsonb)', 'EXECUTE'
  ) or has_function_privilege(
    'authenticated', 'public.focus_auth_begin_delivery_batch_v1(uuid,text,jsonb)', 'EXECUTE'
  ) or has_function_privilege(
    'anon', 'public.email_classify_postbox_receipts_v1(text[])', 'EXECUTE'
  ) or has_function_privilege(
    'service_role',
    'public.focus_auth_begin_delivery_v1(uuid,uuid,text,boolean)', 'EXECUTE'
  ) then
    raise exception 'unsafe role can execute unified Postbox control RPCs';
  end if;
end;
$$;

rollback;
