-- Compact, append-only A/B/C transport timetable evidence.
-- Canonical events/schedules/release jobs stay in Fly SQLite/static artifacts.

create schema if not exists personalization;
revoke all on schema personalization from public, anon, authenticated;
grant usage on schema personalization to service_role;

create table if not exists personalization.experiment_definition (
  experiment_key text not null,
  experiment_version integer not null,
  status text not null check (status in ('qa', 'focus_group', 'live', 'paused', 'closed')),
  config_hash text not null check (config_hash ~ '^sha256:[0-9a-f]{64}$'),
  allocation_algorithm text not null,
  variants jsonb not null,
  started_at timestamptz,
  closed_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (experiment_key, experiment_version),
  constraint experiment_definition_variants_chk check (
    jsonb_typeof(variants) = 'array'
    and jsonb_array_length(variants) between 2 and 8
    and length(variants::text) <= 2048
  )
);

revoke all on personalization.experiment_definition from public, anon, authenticated;
grant select, insert, update, delete on personalization.experiment_definition to service_role;

insert into personalization.experiment_definition (
  experiment_key, experiment_version, status, config_hash, allocation_algorithm, variants
) values (
  'transport_timetable_layout',
  1,
  'qa',
  'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2',
  'sha256-u32be-bucket-10000-v1',
  '[
    {"id":"departure_board_v1","from":0,"to":3332},
    {"id":"route_strips_v1","from":3333,"to":6665},
    {"id":"next_departure_queue_v1","from":6666,"to":9999}
  ]'::jsonb
)
on conflict (experiment_key, experiment_version) do nothing;

create table if not exists personalization.experiment_release_allowlist (
  experiment_key text not null,
  experiment_version integer not null,
  release_id text not null check (release_id ~ '^[a-zA-Z0-9._:/-]{1,160}$'),
  config_hash text not null check (config_hash ~ '^sha256:[0-9a-f]{64}$'),
  enabled boolean not null default false,
  approved_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key (experiment_key, experiment_version, release_id),
  foreign key (experiment_key, experiment_version)
    references personalization.experiment_definition (experiment_key, experiment_version)
    on delete cascade
);

revoke all on personalization.experiment_release_allowlist from public, anon, authenticated;
grant select, insert, update, delete on personalization.experiment_release_allowlist to service_role;

create table if not exists public.personalization_experiment_event (
  id uuid primary key default gen_random_uuid(),
  client_event_id uuid not null,
  experiment_key text not null,
  experiment_version integer not null,
  experiment_subject_id uuid not null,
  anon_id uuid not null,
  session_id uuid not null,
  event_id bigint not null check (event_id > 0),
  assigned_variant text not null,
  rendered_variant text not null,
  assignment_bucket smallint not null check (assignment_bucket between 0 and 9999),
  event_kind text not null check (event_kind in (
    'valid_exposure',
    'official_transfer_booking_click',
    'bus_origin_map_click',
    'walk_route_click',
    'car_route_click',
    'transport_calendar_add',
    'schedule_expand',
    'departure_select'
  )),
  occurred_at timestamptz not null,
  received_at timestamptz not null default now(),
  viewport_class text not null check (viewport_class in ('mobile', 'tablet', 'desktop')),
  release_id text not null check (release_id ~ '^[a-zA-Z0-9._:/-]{1,160}$'),
  config_hash text not null check (config_hash ~ '^sha256:[0-9a-f]{64}$'),
  transport_snapshot_hash text not null check (length(transport_snapshot_hash) between 1 and 128),
  consent_version text not null check (length(consent_version) between 1 and 80),
  actor_class text not null default 'unknown' check (actor_class in ('human_likely', 'unknown')),
  trust_state text not null default 'accepted' check (trust_state = 'accepted'),
  metadata jsonb not null default '{}'::jsonb,
  constraint personalization_experiment_variant_chk check (
    assigned_variant in ('departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1')
    and rendered_variant in ('departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1')
  ),
  constraint personalization_experiment_metadata_chk check (
    jsonb_typeof(metadata) = 'object' and length(metadata::text) <= 512
  ),
  unique (experiment_subject_id, client_event_id)
);

alter table public.personalization_experiment_event enable row level security;
revoke all on public.personalization_experiment_event from public, anon, authenticated;
grant select, insert, update, delete on public.personalization_experiment_event to service_role;

create unique index if not exists personalization_experiment_first_exposure_uidx
  on public.personalization_experiment_event (
    experiment_key, experiment_version, experiment_subject_id, event_id
  ) where event_kind = 'valid_exposure';
create index if not exists personalization_experiment_received_idx
  on public.personalization_experiment_event (received_at desc);
create index if not exists personalization_experiment_anon_received_idx
  on public.personalization_experiment_event (anon_id, received_at desc);
create index if not exists personalization_experiment_session_received_idx
  on public.personalization_experiment_event (session_id, received_at desc);
create index if not exists personalization_experiment_analysis_idx
  on public.personalization_experiment_event (
    experiment_key, experiment_version, assigned_variant, event_kind, received_at
  );

create or replace function public.ingest_transport_experiment_event_v1(p_payload jsonb)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_definition personalization.experiment_definition%rowtype;
  v_experiment_key text;
  v_experiment_version integer;
  v_subject uuid;
  v_anon uuid;
  v_session uuid;
  v_client_event uuid;
  v_event_id bigint;
  v_assigned text;
  v_rendered text;
  v_bucket integer;
  v_expected_bucket integer;
  v_expected_variant text;
  v_kind text;
  v_occurred timestamptz;
  v_viewport text;
  v_release text;
  v_config_hash text;
  v_snapshot_hash text;
  v_consent_version text;
  v_metadata jsonb;
  v_digest bytea;
  v_u32 bigint;
begin
  if p_payload is null or jsonb_typeof(p_payload) <> 'object' or octet_length(p_payload::text) > 4096 then
    raise exception 'invalid_payload' using errcode = '22023';
  end if;

  begin
    v_experiment_key := p_payload->>'experiment_key';
    v_experiment_version := (p_payload->>'experiment_version')::integer;
    v_subject := (p_payload->>'experiment_subject_id')::uuid;
    v_anon := (p_payload->>'anon_id')::uuid;
    v_session := (p_payload->>'session_id')::uuid;
    v_client_event := (p_payload->>'client_event_id')::uuid;
    v_event_id := (p_payload->>'event_id')::bigint;
    v_bucket := (p_payload->>'assignment_bucket')::integer;
    v_occurred := (p_payload->>'occurred_at')::timestamptz;
  exception when others then
    raise exception 'invalid_typed_fields' using errcode = '22023';
  end;

  v_assigned := p_payload->>'assigned_variant';
  v_rendered := p_payload->>'rendered_variant';
  v_kind := p_payload->>'event_kind';
  v_viewport := p_payload->>'viewport_class';
  v_release := p_payload->>'release_id';
  v_config_hash := p_payload->>'config_hash';
  v_snapshot_hash := p_payload->>'transport_snapshot_hash';
  v_consent_version := p_payload->>'consent_version';
  v_metadata := coalesce(p_payload->'metadata', '{}'::jsonb);

  select * into v_definition
  from personalization.experiment_definition
  where experiment_key = v_experiment_key and experiment_version = v_experiment_version;

  if not found or v_definition.status not in ('focus_group', 'live') then
    raise exception 'experiment_not_accepting_events' using errcode = '22023';
  end if;
  if v_config_hash is distinct from v_definition.config_hash
     or v_definition.allocation_algorithm <> 'sha256-u32be-bucket-10000-v1' then
    raise exception 'config_mismatch' using errcode = '22023';
  end if;
  if not exists (
    select 1 from personalization.experiment_release_allowlist
    where experiment_key = v_experiment_key
      and experiment_version = v_experiment_version
      and release_id = v_release
      and config_hash = v_config_hash
      and enabled
  ) then
    raise exception 'release_not_allowed' using errcode = '22023';
  end if;

  -- Same cross-runtime contract as site/src/lib/transportExperiment.ts.
  v_digest := extensions.digest(
    pg_catalog.convert_to(v_experiment_key || '|' || v_experiment_version::text || '|' || v_subject::text, 'UTF8'),
    'sha256'
  );
  v_u32 := get_byte(v_digest, 0)::bigint * 16777216
    + get_byte(v_digest, 1)::bigint * 65536
    + get_byte(v_digest, 2)::bigint * 256
    + get_byte(v_digest, 3)::bigint;
  v_expected_bucket := floor(v_u32::numeric * 10000 / 4294967296)::integer;
  v_expected_variant := case
    when v_expected_bucket <= 3332 then 'departure_board_v1'
    when v_expected_bucket <= 6665 then 'route_strips_v1'
    else 'next_departure_queue_v1'
  end;
  if v_bucket <> v_expected_bucket or v_assigned <> v_expected_variant or v_rendered <> v_assigned then
    raise exception 'assignment_mismatch' using errcode = '22023';
  end if;

  if v_event_id <= 0
     or v_kind not in (
       'valid_exposure', 'official_transfer_booking_click', 'bus_origin_map_click',
       'walk_route_click', 'car_route_click', 'transport_calendar_add',
       'schedule_expand', 'departure_select'
     )
     or v_viewport not in ('mobile', 'tablet', 'desktop')
     or v_release !~ '^[a-zA-Z0-9._:/-]{1,160}$'
     or length(coalesce(v_snapshot_hash, '')) not between 1 and 128
     or length(coalesce(v_consent_version, '')) not between 1 and 80
     or jsonb_typeof(v_metadata) <> 'object'
     or length(v_metadata::text) > 512
     or v_occurred < now() - interval '24 hours'
     or v_occurred > now() + interval '5 minutes' then
    raise exception 'invalid_event_contract' using errcode = '22023';
  end if;

  if (
    select count(*) from public.personalization_experiment_event
    where experiment_subject_id = v_subject and received_at > now() - interval '1 minute'
  ) >= 30 or (
    select count(*) from public.personalization_experiment_event
    where anon_id = v_anon and received_at > now() - interval '1 minute'
  ) >= 30 or (
    select count(*) from public.personalization_experiment_event
    where session_id = v_session and received_at > now() - interval '1 minute'
  ) >= 30 or (
    select count(*) from public.personalization_experiment_event
    where received_at > now() - interval '1 hour'
  ) >= 5000 then
    raise exception 'experiment_event_rate_limited' using errcode = '54000';
  end if;

  if v_kind <> 'valid_exposure' and not exists (
    select 1 from public.personalization_experiment_event
    where experiment_key = v_experiment_key
      and experiment_version = v_experiment_version
      and experiment_subject_id = v_subject
      and event_id = v_event_id
      and event_kind = 'valid_exposure'
  ) then
    raise exception 'action_without_exposure' using errcode = '22023';
  end if;

  insert into public.personalization_experiment_event (
    client_event_id, experiment_key, experiment_version,
    experiment_subject_id, anon_id, session_id, event_id,
    assigned_variant, rendered_variant, assignment_bucket, event_kind,
    occurred_at, viewport_class, release_id, config_hash,
    transport_snapshot_hash, consent_version, actor_class, trust_state, metadata
  ) values (
    v_client_event, v_experiment_key, v_experiment_version,
    v_subject, v_anon, v_session, v_event_id,
    v_assigned, v_rendered, v_bucket, v_kind,
    v_occurred, v_viewport, v_release, v_config_hash,
    v_snapshot_hash, v_consent_version, 'unknown', 'accepted', v_metadata
  )
  on conflict do nothing;

  return true;
end;
$$;

revoke all on function public.ingest_transport_experiment_event_v1(jsonb) from public, anon, authenticated;
grant execute on function public.ingest_transport_experiment_event_v1(jsonb) to anon;
grant execute on function public.ingest_transport_experiment_event_v1(jsonb) to service_role;
