-- TR-EXP-10 transactional RPC/RLS/assignment contract. Rolls back fixtures.
begin;

-- Applying the migration never activates trusted ingest. This explicit fixture
-- switch is rolled back with the rest of the contract test.
update personalization.experiment_definition
set status = 'focus_group', updated_at = now()
where experiment_key = 'transport_timetable_layout' and experiment_version = 1;

insert into personalization.experiment_release_allowlist (
  experiment_key, experiment_version, release_id, config_hash, enabled, approved_at
) values (
  'transport_timetable_layout', 1, 'preview-secret-fixture',
  'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2',
  true, now()
);

do $$
declare
  v_subject uuid := '11111111-1111-4111-8111-111111111111';
  v_anon uuid := '22222222-2222-4222-8222-222222222222';
  v_session uuid := '33333333-3333-4333-8333-333333333333';
  v_base jsonb;
  v_count integer;
begin
  if has_table_privilege('anon', 'public.personalization_experiment_event', 'select')
     or has_table_privilege('anon', 'public.personalization_experiment_event', 'insert') then
    raise exception 'anon must not have direct experiment table access';
  end if;
  if not has_function_privilege('anon', 'public.ingest_transport_experiment_event_v1(jsonb)', 'execute') then
    raise exception 'anon must have only the bounded ingest RPC';
  end if;

  v_base := jsonb_build_object(
    'experiment_key', 'transport_timetable_layout',
    'experiment_version', 1,
    'experiment_subject_id', v_subject,
    'anon_id', v_anon,
    'session_id', v_session,
    'event_id', 4671,
    'assigned_variant', 'departure_board_v1',
    'rendered_variant', 'departure_board_v1',
    'assignment_bucket', 2892,
    'occurred_at', now(),
    'viewport_class', 'mobile',
    'release_id', 'preview-secret-fixture',
    'config_hash', 'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2',
    'transport_snapshot_hash', 'fixture:schedule:v1',
    'consent_version', 'fixture-consent-v1',
    'metadata', '{"trip_count":5}'::jsonb
  );

  perform public.ingest_transport_experiment_event_v1(
    v_base || jsonb_build_object('client_event_id', gen_random_uuid(), 'event_kind', 'valid_exposure')
  );
  -- A second exposure is idempotent through the partial unique index.
  perform public.ingest_transport_experiment_event_v1(
    v_base || jsonb_build_object('client_event_id', gen_random_uuid(), 'event_kind', 'valid_exposure')
  );
  select count(*) into v_count from public.personalization_experiment_event
  where experiment_subject_id = v_subject and event_id = 4671 and event_kind = 'valid_exposure';
  if v_count <> 1 then raise exception 'valid exposure was not idempotent: %', v_count; end if;

  perform public.ingest_transport_experiment_event_v1(
    v_base || jsonb_build_object(
      'client_event_id', gen_random_uuid(),
      'event_kind', 'walk_route_click',
      'metadata', '{"trip_id":null}'::jsonb
    )
  );

  begin
    perform public.ingest_transport_experiment_event_v1(
      v_base || jsonb_build_object(
        'client_event_id', gen_random_uuid(),
        'event_id', 4672,
        'event_kind', 'walk_route_click'
      )
    );
    raise exception 'action without exposure was accepted';
  exception when sqlstate '22023' then
    if position('action_without_exposure' in sqlerrm) = 0 then raise; end if;
  end;

  begin
    perform public.ingest_transport_experiment_event_v1(
      v_base || jsonb_build_object(
        'client_event_id', gen_random_uuid(),
        'assignment_bucket', 9000,
        'event_kind', 'valid_exposure'
      )
    );
    raise exception 'assignment mismatch was accepted';
  exception when sqlstate '22023' then
    if position('assignment_mismatch' in sqlerrm) = 0 then raise; end if;
  end;

  begin
    perform public.ingest_transport_experiment_event_v1(
      v_base || jsonb_build_object(
        'client_event_id', gen_random_uuid(),
        'release_id', 'unapproved-release',
        'event_kind', 'valid_exposure'
      )
    );
    raise exception 'unapproved release was accepted';
  exception when sqlstate '22023' then
    if position('release_not_allowed' in sqlerrm) = 0 then raise; end if;
  end;
end;
$$;

rollback;
