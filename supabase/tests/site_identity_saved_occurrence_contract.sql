-- Run after 20260717170000_site_identity_saved_occurrence_v1.sql on local/staging.
-- Fixtures and all resulting outbox rows are rolled back.
begin;

do $$
declare
  v_user uuid:=extensions.gen_random_uuid();
  v_other uuid:=extensions.gen_random_uuid();
  v_device uuid:=extensions.gen_random_uuid();
  v_device_two uuid:=extensions.gen_random_uuid();
  v_request uuid:=extensions.gen_random_uuid();
  v_hmac text:=repeat('h',43);
  v_merge record;
  v_save record;
  v_reminder record;
  v_count integer;
  v_conflict boolean:=false;
begin
  if has_table_privilege('authenticated','saved_events.saved_occurrence','select')
     or has_table_privilege('anon','site_identity.device','select') then
    raise exception 'raw personalization tables leaked to browser roles';
  end if;
  if has_function_privilege('anon','public.personalization_save_occurrence_v1(bigint,text,timestamp with time zone,boolean)','execute') then
    raise exception 'anon role can call authenticated save RPC';
  end if;
  if not has_function_privilege('authenticated','public.personalization_saved_count_v1()','execute') then
    raise exception 'authenticated count API is not granted';
  end if;
  if has_function_privilege('authenticated','public.personalization_merge_device_v1(uuid,uuid,text,text,uuid)','execute') then
    raise exception 'authenticated role bypasses device-proof Edge control plane';
  end if;

  insert into auth.users(id,email,email_confirmed_at) values
   (v_user,'saved-fixture@example.test',now()),(v_other,'other-fixture@example.test',now());
  insert into auth.identities(provider_id,user_id,identity_data,provider)
  values(v_user::text,v_user,jsonb_build_object('sub',v_user::text,'email','saved-fixture@example.test'),'email'),
        (v_other::text,v_other,jsonb_build_object('sub',v_other::text,'email','other-fixture@example.test'),'email');
  perform public.email_sync_verified_identity_v1(v_user,'saved-fixture@example.test',v_hmac,1,now());
  perform set_config('request.jwt.claim.sub',v_user::text,true);
  perform public.email_set_purpose_consent_v1('transactional_event',true,'fixture-v1',extensions.gen_random_uuid());

  perform public.personalization_materialize_device_v1(v_device,repeat('a',64),'personalization-v1',
   '[{"event_id":101,"occurrence_key":"101@2026-08-20T16:00:00Z","occurrence_starts_at":"2026-08-20T16:00:00Z","saved":true}]'::jsonb);
  select * into v_merge from public.personalization_merge_device_v1(v_user,v_device,repeat('a',64),'personalization-v1',v_request);
  if v_merge.merge_status<>'merged' or v_merge.imported_saved_count<>1 then raise exception 'first merge failed: %',row_to_json(v_merge); end if;
  select * into v_merge from public.personalization_merge_device_v1(v_user,v_device,repeat('a',64),'personalization-v1',v_request);
  if v_merge.imported_saved_count<>1 then raise exception 'request replay was not idempotent'; end if;
  if (select count(*) from site_identity.profile where user_id=v_user)<>1
     or (select count(*) from saved_events.saved_occurrence where event_id=101)<>1 then
    raise exception 'merge duplicated profile or save';
  end if;

  perform public.personalization_materialize_device_v1(v_device_two,repeat('b',64),'personalization-v1',
   '[{"event_id":101,"occurrence_key":"101@2026-08-20T16:00:00Z","saved":true}]'::jsonb);
  perform public.personalization_merge_device_v1(v_user,v_device_two,repeat('b',64),'personalization-v1',extensions.gen_random_uuid());
  if (select count(*) from site_identity.profile where user_id=v_user)<>1
     or (select count(*) from saved_events.saved_occurrence where event_id=101)<>1 then
    raise exception 'cross-device merge duplicated profile or save';
  end if;

  begin
    perform public.personalization_merge_device_v1(v_other,v_device,repeat('a',64),'personalization-v1',extensions.gen_random_uuid());
  exception when unique_violation then v_conflict:=true; end;
  if not v_conflict then raise exception 'account-switch device conflict did not fail closed'; end if;

  perform set_config('request.jwt.claim.sub',v_user::text,true);
  select * into v_save from public.personalization_save_occurrence_v1(101,'101@2026-08-20T16:00:00Z','2026-08-20T16:00:00Z',true);
  select * into v_save from public.personalization_save_occurrence_v1(101,'101@2026-08-20T16:00:00Z','2026-08-20T16:00:00Z',true);
  if v_save.unique_saved_event_count<>1 or public.personalization_saved_count_v1()<>1 then raise exception 'repeat save/count failed'; end if;
  perform public.personalization_set_event_signal_v1(101,'101@2026-08-20T16:00:00Z','like',true);
  if (select count(*) from saved_events.event_signal where signal='like')<>1 then raise exception 'like was not separate'; end if;

  select * into v_reminder from public.personalization_set_reminder_v1(101,'101@2026-08-20T16:00:00Z',true,'reminder-v1',extensions.gen_random_uuid());
  if v_reminder.state<>'active' or v_reminder.masked_email<>'s***@example.test' then raise exception 'reminder consent/mask failed'; end if;
  if (select count(*) from saved_events.reminder_consent_event)<>1 then raise exception 'reminder consent evidence missing'; end if;
  if saved_events.schedule_d1_v1('2026-07-19 00:00:00+00','2026-07-17 18:00:00+00')<>'2026-07-18 06:00:00+00' then raise exception 'quiet-hours schedule failed'; end if;
  if saved_events.schedule_d1_v1('2026-07-17 15:00:00+00','2026-07-17 12:00:00+00')<>'2026-07-17 12:00:00+00' then raise exception 'catch-up schedule failed'; end if;
  update saved_events.reminder_subscription set scheduled_for=now()-interval '1 minute';
  v_count:=public.personalization_enqueue_due_reminders_v1(10,true);
  if v_count<>1 or public.personalization_enqueue_due_reminders_v1(10,true)<>0 then raise exception 'D-1 exactly-once enqueue failed'; end if;
  if (select count(*) from saved_events.reminder_delivery where kind='event_reminder_24h')<>1 then raise exception 'duplicate D-1 delivery guard failed'; end if;
  if exists(select 1 from email_control.email_outbox where kind='event_reminder_24h' and (payload_json->>'subject' is null or payload_json->>'text' is null or payload_json ? 'event_id')) then raise exception 'Postbox payload schema invalid'; end if;

  perform public.personalization_apply_occurrence_lifecycle_v1(101,'101@2026-08-20T16:00:00Z','cancelled','2026-08-20T16:00:00Z');
  if (select state from saved_events.reminder_subscription limit 1)<>'cancelled' then raise exception 'cancellation did not stop reminder'; end if;
  if (select count(*) from saved_events.reminder_delivery where kind='event_cancelled')<>1 then raise exception 'cancellation lifecycle mail missing'; end if;
  select * into v_save from public.personalization_save_occurrence_v1(101,'101@2026-08-20T16:00:00Z',null,false);
  if v_save.unique_saved_event_count<>0 or public.personalization_saved_count_v1()<>0 then raise exception 'undo failed'; end if;
end;
$$;
rollback;
