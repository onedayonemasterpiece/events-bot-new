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
  v_reminder_request uuid:=extensions.gen_random_uuid();
  v_hmac text:=repeat('h',43);
  v_merge record;
  v_save record;
  v_reminder record;
  v_count integer;
  v_conflict boolean:=false;
  v_anonymous_rejected boolean:=false;
  v_schedule timestamptz;
  v_subscription uuid;
begin
  if exists (
    select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname in ('site_identity','saved_events') and c.relkind='r'
      and (
        has_table_privilege('authenticated',c.oid,'select,insert,update,delete')
        or has_table_privilege('anon',c.oid,'select,insert,update,delete')
      )
  ) then
    raise exception 'raw personalization tables leaked to browser roles';
  end if;
  if exists (
    select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace,
      lateral aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
    where n.nspname in ('site_identity','saved_events') and c.relkind='r'
      and a.grantee=0 and a.privilege_type in ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
  ) then raise exception 'raw personalization table retains PUBLIC privileges'; end if;
  if exists (
    select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname in ('site_identity','saved_events') and c.relkind='r' and not c.relrowsecurity
  ) then raise exception 'personalization table without RLS'; end if;
  if exists (
    select 1
    from pg_constraint c join pg_namespace n on n.oid=c.connamespace
    cross join lateral unnest(c.conkey) k(attnum)
    where n.nspname in ('site_identity','saved_events') and c.contype='f'
      and not exists (
        select 1 from pg_index i
        where i.indrelid=c.conrelid and k.attnum=any(i.indkey) and i.indisvalid
      )
  ) then raise exception 'foreign-key column lacks supporting index'; end if;
  if exists (
    select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace,
      lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
    where p.prosecdef and a.grantee=0 and a.privilege_type='EXECUTE'
      and (
        (n.nspname='public' and p.proname like 'personalization\_%\_v1' escape '\')
        or (n.nspname='site_identity' and p.proname='ensure_profile_v1')
      )
  ) then raise exception 'SECURITY DEFINER function retains PUBLIC execute'; end if;
  if has_function_privilege('anon','public.personalization_save_occurrence_v1(bigint,text,timestamp with time zone,boolean)','execute') then
    raise exception 'anon role can call authenticated save RPC';
  end if;
  if not has_function_privilege('authenticated','public.personalization_saved_count_v1()','execute') then
    raise exception 'authenticated count API is not granted';
  end if;
  if has_function_privilege('authenticated','public.personalization_merge_device_v1(uuid,uuid,text,text,uuid)','execute') then
    raise exception 'authenticated role bypasses device-proof Edge control plane';
  end if;
  if exists (
    select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.proname in (
      'personalization_materialize_device_v1','personalization_merge_device_v1',
      'personalization_unlink_device_v1','personalization_mark_profile_deleting_v1',
      'personalization_apply_occurrence_lifecycle_v1','personalization_enqueue_due_reminders_v1',
      'personalization_retention_cleanup_v1'
    ) and (
      has_function_privilege('authenticated',p.oid,'execute')
      or has_function_privilege('anon',p.oid,'execute')
      or not has_function_privilege('service_role',p.oid,'execute')
    )
  ) then raise exception 'service-only RPC grant contract failed'; end if;

  insert into auth.users(id,email,email_confirmed_at) values
   (v_user,'saved-fixture@example.test',now()),(v_other,'other-fixture@example.test',now());
  insert into auth.identities(provider_id,user_id,identity_data,provider)
  values(v_user::text,v_user,jsonb_build_object('sub',v_user::text,'email','saved-fixture@example.test'),'email'),
        (v_other::text,v_other,jsonb_build_object('sub',v_other::text,'email','other-fixture@example.test'),'email');
  perform public.email_sync_verified_identity_v1(v_user,'saved-fixture@example.test',v_hmac,1,now());
  perform set_config('request.jwt.claim.sub',v_user::text,true);
  perform set_config('request.jwt.claims',jsonb_build_object('sub',v_user,'is_anonymous',false)::text,true);
  perform public.email_set_purpose_consent_v1('transactional_event',true,'fixture-v1',extensions.gen_random_uuid());

  perform set_config('request.jwt.claims',jsonb_build_object('sub',v_user,'is_anonymous',true)::text,true);
  begin
    perform public.personalization_save_occurrence_v1(999,'anonymous-denied',now()+interval '2 days',true);
  exception when invalid_authorization_specification then v_anonymous_rejected:=true; end;
  if not v_anonymous_rejected then raise exception 'anonymous Auth user reached account RPC'; end if;
  perform set_config('request.jwt.claims',jsonb_build_object('sub',v_user,'is_anonymous',false)::text,true);

  perform public.personalization_materialize_device_v1(v_device,repeat('a',64),'personalization-v1',
   '[{"event_id":101,"occurrence_key":"101@2026-08-20T16:00:00Z","occurrence_starts_at":"2026-08-20T16:00:00Z","saved":true}]'::jsonb);
  v_conflict:=false;
  begin
    perform public.personalization_materialize_device_v1(v_device,repeat('c',64),'personalization-v1','[]'::jsonb);
  exception when unique_violation then v_conflict:=true; end;
  if not v_conflict then raise exception 'device id accepted a replacement credential'; end if;
  select * into v_merge from public.personalization_merge_device_v1(v_user,v_device,repeat('a',64),'personalization-v1',v_request);
  if v_merge.merge_status<>'merged' or v_merge.imported_saved_count<>1 then raise exception 'first merge failed: %',row_to_json(v_merge); end if;
  select * into v_merge from public.personalization_merge_device_v1(v_user,v_device,repeat('a',64),'personalization-v1',v_request);
  if v_merge.imported_saved_count<>1 then raise exception 'request replay was not idempotent'; end if;
  v_conflict:=false;
  begin
    perform public.personalization_merge_device_v1(v_other,v_device,repeat('a',64),'personalization-v1',v_request);
  exception when unique_violation then v_conflict:=true; end;
  if not v_conflict then raise exception 'merge request id replay crossed accounts'; end if;
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

  v_conflict:=false;
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

  select * into v_reminder from public.personalization_set_reminder_v1(101,'101@2026-08-20T16:00:00Z',true,'reminder-v1',v_reminder_request);
  if v_reminder.state<>'active' or v_reminder.masked_email<>'s***@example.test' then raise exception 'reminder consent/mask failed'; end if;
  v_schedule:=v_reminder.scheduled_for;
  select * into v_reminder from public.personalization_set_reminder_v1(101,'101@2026-08-20T16:00:00Z',true,'reminder-v1',v_reminder_request);
  if v_reminder.scheduled_for<>v_schedule or (select count(*) from saved_events.reminder_consent_event)<>1 then raise exception 'reminder consent replay was not idempotent'; end if;
  if not exists(
    select 1 from saved_events.reminder_subscription
    where masked_email='s***@example.test' and email_verified_at is not null
  ) then raise exception 'masked verified email snapshot missing'; end if;
  if saved_events.schedule_d1_v1('2026-07-19 00:00:00+00','2026-07-17 18:00:00+00')<>'2026-07-18 06:00:00+00' then raise exception 'quiet-hours schedule failed'; end if;
  if saved_events.schedule_d1_v1('2026-07-18 12:00:00+00','2026-07-17 14:00:00+00')<>'2026-07-17 14:00:00+00' then raise exception 'bounded catch-up schedule failed'; end if;
  if saved_events.schedule_d1_v1('2026-07-18 15:00:00+00','2026-07-17 22:00:00+00') is not null then raise exception 'out-of-bound catch-up did not fail closed'; end if;
  if saved_events.schedule_d1_v1('2026-07-18 23:00:00+00','2026-07-18 00:00:00+00')<>'2026-07-18 06:00:00+00' then raise exception 'quiet-hours catch-up failed'; end if;

  insert into email_control.suppression(email_hmac,scope,provider,reason)
  values(v_hmac,'transactional','postbox','hard_bounce');
  update saved_events.reminder_subscription set scheduled_for=now()-interval '1 minute';
  v_count:=public.personalization_enqueue_due_reminders_v1(10,true);
  if v_count<>0 or exists(select 1 from saved_events.reminder_delivery where kind='event_reminder_24h')
     or not exists(select 1 from saved_events.reminder_subscription where state='paused') then
    raise exception 'Postbox suppression did not block and isolate reminder';
  end if;
  delete from email_control.suppression where email_hmac=v_hmac;
  update saved_events.reminder_subscription set state='active',scheduled_for=now()-interval '1 minute';
  v_count:=public.personalization_enqueue_due_reminders_v1(10,true);
  if v_count<>1 or public.personalization_enqueue_due_reminders_v1(10,true)<>0 then raise exception 'D-1 exactly-once enqueue failed'; end if;
  if (select count(*) from saved_events.reminder_delivery where kind='event_reminder_24h')<>1 then raise exception 'duplicate D-1 delivery guard failed'; end if;
  if exists(select 1 from email_control.email_outbox where kind='event_reminder_24h' and (payload_json->>'subject' is null or payload_json->>'text' is null or payload_json ? 'event_id')) then raise exception 'Postbox payload schema invalid'; end if;

  perform public.personalization_apply_occurrence_lifecycle_v1(101,'101@2026-08-20T16:00:00Z','cancelled','2026-08-20T16:00:00Z');
  if not exists(select 1 from saved_events.reminder_subscription where state='cancelled' and scheduled_for is null) then raise exception 'cancellation did not stop reminder'; end if;
  if not exists(select 1 from email_control.email_outbox where kind='event_reminder_24h' and status='skipped') then raise exception 'cancellation left pending D-1 sendable'; end if;
  if (select count(*) from saved_events.reminder_delivery where kind='event_cancelled')<>1 then raise exception 'cancellation lifecycle mail missing'; end if;
  if public.personalization_apply_occurrence_lifecycle_v1(101,'101@2026-08-20T16:00:00Z','cancelled','2026-08-20T16:00:00Z')<>0
     or (select count(*) from saved_events.reminder_delivery where kind='event_cancelled')<>1 then raise exception 'lifecycle replay was not idempotent'; end if;
  select * into v_save from public.personalization_save_occurrence_v1(101,'101@2026-08-20T16:00:00Z',null,false);
  if v_save.unique_saved_event_count<>0 or public.personalization_saved_count_v1()<>0 then raise exception 'undo failed'; end if;

  -- A reschedule recalculates an unsent/pending reminder. If the prior D-1 was
  -- already delivered, only the separately keyed lifecycle notice is created.
  select * into v_save from public.personalization_save_occurrence_v1(102,'102@revision',now()+interval '30 hours',true);
  select * into v_reminder from public.personalization_set_reminder_v1(102,'102@revision',true,'reminder-v1',extensions.gen_random_uuid());
  select r.id,r.scheduled_for into v_subscription,v_schedule from saved_events.reminder_subscription r
   join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id where s.event_id=102;
  perform public.personalization_apply_occurrence_lifecycle_v1(102,'102@revision','rescheduled',now()+interval '40 hours');
  if not exists(select 1 from saved_events.reminder_subscription where id=v_subscription and schedule_revision=2 and scheduled_for>v_schedule) then raise exception 'reschedule did not update unsent reminder'; end if;
  update saved_events.reminder_subscription set scheduled_for=now()-interval '1 minute' where id=v_subscription;
  if public.personalization_enqueue_due_reminders_v1(10,true)<>1 then raise exception 'rescheduled D-1 was not enqueued'; end if;
  perform public.personalization_apply_occurrence_lifecycle_v1(102,'102@revision','rescheduled',now()+interval '50 hours');
  if not exists(select 1 from saved_events.reminder_subscription where id=v_subscription and schedule_revision=3 and reminder_sent_at is null and scheduled_for>now())
     or not exists(select 1 from email_control.email_outbox o join saved_events.reminder_delivery d on d.outbox_id=o.id where d.reminder_subscription_id=v_subscription and d.kind='event_reminder_24h' and o.status='skipped') then
    raise exception 'pending old-time D-1 was not safely replaced';
  end if;
  update saved_events.reminder_subscription set scheduled_for=now()-interval '1 minute' where id=v_subscription;
  if public.personalization_enqueue_due_reminders_v1(10,true)<>1 or public.personalization_enqueue_due_reminders_v1(10,true)<>0 then raise exception 'replacement D-1 retry duplicated'; end if;
  update email_control.email_outbox o set status='delivered'
   from saved_events.reminder_delivery d where d.outbox_id=o.id and d.reminder_subscription_id=v_subscription and d.kind='event_reminder_24h' and o.status='ready';
  perform public.personalization_apply_occurrence_lifecycle_v1(102,'102@revision','rescheduled',now()+interval '60 hours');
  if (select count(*) from saved_events.reminder_delivery where reminder_subscription_id=v_subscription and kind='event_reminder_24h')<>2
     or not exists(select 1 from saved_events.reminder_subscription where id=v_subscription and reminder_sent_at is not null) then
    raise exception 'delivered D-1 was incorrectly reset after later reschedule';
  end if;

  select * into v_save from public.personalization_save_occurrence_v1(103,'103@completed',now()+interval '30 hours',true);
  select * into v_reminder from public.personalization_set_reminder_v1(103,'103@completed',true,'reminder-v1',extensions.gen_random_uuid());
  update saved_events.reminder_subscription r set scheduled_for=now()-interval '1 minute'
   from saved_events.saved_occurrence s where s.id=r.saved_occurrence_id and s.event_id=103;
  perform public.personalization_apply_occurrence_lifecycle_v1(103,'103@completed','completed',now()+interval '30 hours');
  if public.personalization_enqueue_due_reminders_v1(10,true)<>0
     or exists(select 1 from saved_events.reminder_delivery d join saved_events.reminder_subscription r on r.id=d.reminder_subscription_id join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id where s.event_id=103 and d.kind='event_reminder_24h') then
    raise exception 'completed occurrence produced D-1';
  end if;

  select * into v_save from public.personalization_save_occurrence_v1(104,'104@stale',now()+interval '30 hours',true);
  select * into v_reminder from public.personalization_set_reminder_v1(104,'104@stale',true,'reminder-v1',extensions.gen_random_uuid());
  update saved_events.reminder_subscription r set scheduled_for=now()-interval '7 hours'
   from saved_events.saved_occurrence s where s.id=r.saved_occurrence_id and s.event_id=104;
  v_count:=public.personalization_enqueue_due_reminders_v1(10,true);
  if v_count<>0
     or not exists(select 1 from saved_events.reminder_subscription r join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id where s.event_id=104 and r.state='expired') then
    raise exception 'scheduler catch-up exceeded six-hour bound: enqueued=%, state=%',v_count,
      (select r.state from saved_events.reminder_subscription r join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id where s.event_id=104);
  end if;
end;
$$;
rollback;
