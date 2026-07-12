-- Deterministic transactional contract smoke for a local/staging Supabase database.
-- Run only after the email_control_plane_v1 migration; this script rolls back all fixtures.

begin;

do $$
declare
  v_user_id uuid;
  v_first_user uuid;
  v_email text;
  v_hmac text;
  v_admitted integer := 0;
  v_capacity_rejections integer := 0;
  v_issue_two uuid;
  v_issue_three uuid;
  v_outbox uuid;
  v_suppressions integer;
  v_claim record;
begin
  if (select enabled or not dry_run_only from email_control.runtime_switch where switch_key = 'global') then
    raise exception 'global email switch must default disabled and dry-run-only';
  end if;
  if has_table_privilege('authenticated', 'email_control.email_outbox', 'select') then
    raise exception 'authenticated role must not have raw email_outbox SELECT';
  end if;

  for i in 1..201 loop
    v_user_id := extensions.gen_random_uuid();
    if v_first_user is null then v_first_user := v_user_id; end if;
    v_email := format('email-control-fixture-%s@example.test', i);
    v_hmac := encode(extensions.digest(v_email || ':fixture-key', 'sha256'), 'base64');
    insert into auth.users (id, email, email_confirmed_at) values (v_user_id, v_email, now());
    perform public.email_sync_verified_identity_v1(v_user_id, v_email, v_hmac, 1, now());
    perform set_config('request.jwt.claim.sub', v_user_id::text, true);
    begin
      perform public.email_set_purpose_consent_v1(
        'recommendation', true, 'fixture-terms-v1', extensions.gen_random_uuid()
      );
      v_admitted := v_admitted + 1;
    exception when sqlstate 'P0001' then
      if position('recommendation_capacity_full' in sqlerrm) = 0 then raise; end if;
      v_capacity_rejections := v_capacity_rejections + 1;
    end;
  end loop;

  if v_admitted <> 200 or v_capacity_rejections <> 1 then
    raise exception 'capacity contract failed: admitted %, rejected %', v_admitted, v_capacity_rejections;
  end if;
  if (select active_count from email_control.recommendation_capacity where capacity_key = 'launch') <> 200 then
    raise exception 'capacity ledger did not converge to 200';
  end if;

  v_issue_two := public.email_stage_recommendation_issue_v1(
    v_first_user, 'fixture-issue-two', repeat('a', 64), repeat('b', 64),
    'personal/fixture-two.html', 12,
    '[{"event_id":1,"email_position":1},{"event_id":2,"email_position":2}]'::jsonb
  );
  begin
    perform public.email_publish_recommendation_issue_v1(
      v_issue_two, repeat('c', 64), repeat('d', 43), 1, now(), now()
    );
    raise exception 'two-event issue was incorrectly published';
  exception when check_violation then
    null;
  end;

  v_issue_three := public.email_stage_recommendation_issue_v1(
    v_first_user, 'fixture-issue-three', repeat('a', 64), repeat('b', 64),
    'personal/fixture-three.html', 12,
    '[{"event_id":1,"email_position":1,"is_hero":true},{"event_id":2,"email_position":2},{"event_id":3,"email_position":3}]'::jsonb
  );
  if not public.email_publish_recommendation_issue_v1(
    v_issue_three, repeat('c', 64), repeat('d', 43), 1, now(), now()
  ) then
    raise exception 'valid three-event issue was not published';
  end if;
  v_outbox := public.email_enqueue_recommendation_v1(
    v_issue_three, 'fixture:recommendation:three', 'fixture-template-v1', '{}'::jsonb, repeat('e', 64), true
  );
  select * into v_claim from public.email_claim_outbox_v1('fixture-worker', 1, 120);
  if v_claim.outbox_id <> v_outbox or v_claim.provider <> 'notisend' or not v_claim.dry_run then
    raise exception 'recommendation provider/dry-run claim contract failed';
  end if;

  select count(*) into v_suppressions from email_control.suppression;
  perform public.email_record_provider_event_v1(
    'notisend', 'fixture-untrusted', '417', 'unsubscribe', now(), repeat('f', 43), repeat('0', 64), false, false
  );
  if (select count(*) from email_control.suppression) <> v_suppressions then
    raise exception 'untrusted NotiSend webhook changed suppression';
  end if;
  -- Provider-event V1 is retained only for migration compatibility. It accepts
  -- caller-controlled trust booleans and must not be used by a deployed service.
  -- Postbox state changes are covered by email_postbox_event_consumer_contract.sql;
  -- NotiSend needs a future provider-verified, message-correlated V2 contract.
end;
$$;

rollback;
