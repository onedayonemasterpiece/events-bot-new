-- Transactional contract for email_record_postbox_event_v2. Rolls back fixtures.
begin;

do $$
declare
  v_user uuid := extensions.gen_random_uuid();
  v_hmac text := repeat('h', 43);
  v_outbox uuid;
  v_status text;
begin
  insert into auth.users (id, email, email_confirmed_at)
  values (v_user, 'postbox-contract@example.test', now());
  perform public.email_sync_verified_identity_v1(
    v_user, 'postbox-contract@example.test', v_hmac, 1, now()
  );
  v_outbox := public.email_enqueue_transactional_v1(
    v_user, 'account_auth', null, 'postbox-contract-first', 'fixture-v1',
    '{}'::jsonb, repeat('a', 64), false
  );
  update email_control.email_outbox
     set status = 'submitted', provider_message_id = 'postbox-contract-message-1'
   where id = v_outbox;

  v_status := public.email_record_postbox_event_v2(
    'postbox-event-delivered', 'postbox-contract-message-1', 'delivered',
    '2026-07-12T07:01:00Z', v_hmac, 1, repeat('b', 64)
  );
  if v_status <> 'applied' or (select status from email_control.email_outbox where id = v_outbox) <> 'delivered' then
    raise exception 'correlated delivery was not applied';
  end if;
  if public.email_record_postbox_event_v2(
    'postbox-event-delivered', 'postbox-contract-message-1', 'delivered',
    '2026-07-12T07:01:00Z', v_hmac, 1, repeat('b', 64)
  ) <> 'duplicate' then
    raise exception 'exact duplicate was not idempotent';
  end if;

  begin
    perform public.email_record_postbox_event_v2(
      'postbox-event-delivered', 'postbox-contract-message-1', 'delivered',
      '2026-07-12T07:01:00Z', v_hmac, 1, repeat('c', 64)
    );
    raise exception 'conflicting event id was accepted';
  exception when check_violation then null;
  end;

  if public.email_record_postbox_event_v2(
    'postbox-event-unmatched', 'unknown-provider-message', 'delivered', now(),
    v_hmac, 1, repeat('d', 64)
  ) <> 'correlation_pending' then
    raise exception 'unmatched event did not remain pending';
  end if;
  if exists (
    select 1 from email_control.provider_event
     where provider = 'postbox' and provider_event_key = 'postbox-event-unmatched'
  ) then
    raise exception 'unmatched event inserted a tombstone';
  end if;

  begin
    perform public.email_record_postbox_event_v2(
      'postbox-event-wrong-hmac', 'postbox-contract-message-1', 'hard_bounce', now(),
      repeat('x', 43), 1, repeat('e', 64)
    );
    raise exception 'wrong recipient HMAC was accepted';
  exception when check_violation then null;
  end;

  if public.email_record_postbox_event_v2(
    'postbox-event-hard-bounce', 'postbox-contract-message-1', 'hard_bounce',
    '2026-07-12T07:02:00Z', v_hmac, 1, repeat('f', 64)
  ) <> 'applied' then
    raise exception 'hard bounce was not applied';
  end if;
  if (select status from email_control.email_outbox where id = v_outbox) <> 'terminal_failed' then
    raise exception 'hard bounce did not terminate outbox';
  end if;
  if not exists (
    select 1 from email_control.suppression
     where email_hmac = v_hmac and scope = 'all' and provider = 'postbox'
       and reason = 'hard_bounce' and active
  ) then
    raise exception 'hard bounce suppression missing';
  end if;

  begin
    perform public.email_enqueue_transactional_v1(
      v_user, 'account_auth', null, 'postbox-contract-second', 'fixture-v1',
      '{}'::jsonb, repeat('a', 64), true
    );
    raise exception 'suppressed identity was enqueueable';
  exception when sqlstate '28000' then null;
  end;

  if has_function_privilege(
    'service_role',
    'public.email_record_provider_event_v1(text,text,text,text,timestamptz,text,text,boolean,boolean)',
    'EXECUTE'
  ) then
    raise exception 'unsafe provider event v1 remains executable by service_role';
  end if;
  if has_function_privilege(
    'anon',
    'public.email_record_postbox_event_v2(text,text,text,timestamptz,text,integer,text)',
    'EXECUTE'
  ) or has_function_privilege(
    'authenticated',
    'public.email_record_postbox_event_v2(text,text,text,timestamptz,text,integer,text)',
    'EXECUTE'
  ) then
    raise exception 'browser role can execute Postbox event v2';
  end if;
end;
$$;

rollback;
