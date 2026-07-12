-- KenigEvents email control-plane foundation.
-- Target: the separate personalization Supabase project, never the Fly SQLite core DB.
-- Production sends remain disabled by seeded runtime switches. This migration is additive,
-- but MUST NOT be applied until the documented live migration-history drift is reconciled.

create extension if not exists pgcrypto with schema extensions;

create schema if not exists email_control;
revoke all on schema email_control from public, anon, authenticated;
grant usage on schema email_control to service_role;

create table email_control.recipient_identity (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null unique references auth.users(id) on delete cascade,
  normalized_email text not null,
  email_hmac text not null unique,
  hmac_key_version smallint not null check (hmac_key_version > 0),
  verified_at timestamptz not null,
  auth_email_updated_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint recipient_identity_email_size_chk check (length(normalized_email) between 3 and 320),
  constraint recipient_identity_hmac_size_chk check (length(email_hmac) between 43 and 128)
);

create table email_control.purpose_consent (
  user_id uuid not null references auth.users(id) on delete cascade,
  identity_id uuid not null references email_control.recipient_identity(id) on delete cascade,
  purpose text not null check (purpose in ('transactional_event', 'recommendation')),
  state text not null check (state in ('active', 'paused', 'revoked')),
  terms_version text not null,
  granted_at timestamptz,
  paused_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (user_id, purpose),
  constraint purpose_consent_terms_size_chk check (length(terms_version) between 1 and 80),
  constraint purpose_consent_state_time_chk check (
    (state = 'active' and granted_at is not null and paused_at is null and revoked_at is null)
    or (state = 'paused' and paused_at is not null)
    or (state = 'revoked' and revoked_at is not null)
  )
);

create index purpose_consent_active_purpose_idx
  on email_control.purpose_consent (purpose, user_id)
  where state = 'active';

create table email_control.consent_event (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  identity_id uuid not null references email_control.recipient_identity(id) on delete cascade,
  purpose text not null check (purpose in ('transactional_event', 'recommendation')),
  previous_state text check (previous_state is null or previous_state in ('active', 'paused', 'revoked')),
  new_state text not null check (new_state in ('active', 'paused', 'revoked')),
  terms_version text not null,
  request_id uuid not null,
  source text not null default 'authenticated_rpc',
  evidence jsonb not null default '{}'::jsonb,
  occurred_at timestamptz not null default now(),
  constraint consent_event_user_purpose_request_uid unique (user_id, purpose, request_id),
  constraint consent_event_source_size_chk check (length(source) between 1 and 80),
  constraint consent_event_evidence_size_chk check (length(evidence::text) <= 4096)
);

create table email_control.recommendation_capacity (
  capacity_key text primary key,
  capacity integer not null check (capacity = 200),
  active_count integer not null default 0 check (active_count between 0 and capacity),
  updated_at timestamptz not null default now(),
  constraint recommendation_capacity_launch_key_chk check (capacity_key = 'launch')
);

insert into email_control.recommendation_capacity (capacity_key, capacity, active_count)
values ('launch', 200, 0)
on conflict (capacity_key) do nothing;

create table email_control.suppression (
  id uuid primary key default extensions.gen_random_uuid(),
  email_hmac text not null,
  scope text not null check (scope in ('all', 'transactional', 'recommendation')),
  provider text check (provider is null or provider in ('postbox', 'notisend')),
  reason text not null check (reason in ('hard_bounce', 'complaint', 'unsubscribe', 'manual', 'invalid_address')),
  active boolean not null default true,
  provider_event_key text,
  evidence jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  released_at timestamptz,
  constraint suppression_hmac_size_chk check (length(email_hmac) between 43 and 128),
  constraint suppression_event_key_size_chk check (provider_event_key is null or length(provider_event_key) <= 256),
  constraint suppression_evidence_size_chk check (length(evidence::text) <= 4096),
  constraint suppression_release_chk check ((active and released_at is null) or (not active and released_at is not null))
);

create unique index suppression_active_identity_reason_uidx
  on email_control.suppression (email_hmac, scope, coalesce(provider, ''), reason)
  where active;
create index suppression_active_lookup_idx
  on email_control.suppression (email_hmac, scope)
  where active;

create table email_control.runtime_switch (
  switch_key text primary key,
  enabled boolean not null default false,
  dry_run_only boolean not null default true,
  updated_at timestamptz not null default now(),
  updated_by text not null default 'migration',
  constraint runtime_switch_key_chk check (switch_key in ('global', 'transactional', 'recommendation')),
  constraint runtime_switch_actor_size_chk check (length(updated_by) between 1 and 120)
);

insert into email_control.runtime_switch (switch_key, enabled, dry_run_only)
values
  ('global', false, true),
  ('transactional', false, true),
  ('recommendation', false, true)
on conflict (switch_key) do nothing;

create table email_control.recommendation_issue (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  issue_key text not null unique,
  profile_revision text not null,
  event_snapshot_hash text not null,
  status text not null default 'staged' check (status in ('staged', 'published', 'enqueued', 'revoked', 'failed')),
  personal_page_path text not null,
  page_item_count integer not null check (page_item_count between 4 and 100),
  artifact_sha256 text,
  page_token_hmac text,
  token_key_version smallint check (token_key_version is null or token_key_version > 0),
  page_published_at timestamptz,
  page_validated_at timestamptz,
  expires_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint recommendation_issue_key_size_chk check (length(issue_key) between 8 and 200),
  constraint recommendation_issue_profile_revision_size_chk check (length(profile_revision) between 1 and 160),
  constraint recommendation_issue_snapshot_hash_size_chk check (length(event_snapshot_hash) between 32 and 128),
  constraint recommendation_issue_page_path_chk check (
    length(personal_page_path) between 1 and 1000
    and position('?' in personal_page_path) = 0
    and position('#' in personal_page_path) = 0
  ),
  constraint recommendation_issue_artifact_hash_chk check (artifact_sha256 is null or length(artifact_sha256) between 32 and 128),
  constraint recommendation_issue_token_hash_chk check (page_token_hmac is null or length(page_token_hmac) between 43 and 128),
  constraint recommendation_issue_publish_state_chk check (
    status = 'staged'
    or (
      artifact_sha256 is not null
      and page_token_hmac is not null
      and token_key_version is not null
      and page_published_at is not null
      and page_validated_at is not null
    )
  )
);

create table email_control.recommendation_issue_item (
  issue_id uuid not null references email_control.recommendation_issue(id) on delete cascade,
  event_id bigint not null check (event_id > 0),
  email_position smallint not null check (email_position between 1 and 20),
  is_hero boolean not null default false,
  created_at timestamptz not null default now(),
  primary key (issue_id, event_id),
  unique (issue_id, email_position)
);

create table email_control.email_outbox (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  identity_id uuid not null references email_control.recipient_identity(id) on delete restrict,
  stream text not null check (stream in ('transactional', 'recommendation')),
  provider text not null check (provider in ('postbox', 'notisend')),
  kind text not null,
  event_id bigint,
  recommendation_issue_id uuid references email_control.recommendation_issue(id) on delete restrict,
  idempotency_key text not null unique,
  template_version text not null,
  payload_json jsonb not null default '{}'::jsonb,
  payload_sha256 text not null,
  status text not null default 'ready' check (
    status in ('ready', 'claimed', 'submitted', 'delivered', 'retryable', 'suppressed', 'skipped', 'unknown_delivery', 'terminal_failed', 'dry_run_complete')
  ),
  dry_run boolean not null default true,
  attempts integer not null default 0 check (attempts between 0 and 20),
  next_attempt_at timestamptz not null default now(),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  network_started_at timestamptz,
  provider_message_id text,
  last_error_class text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint email_outbox_provider_route_chk check (
    (stream = 'transactional' and provider = 'postbox')
    or (stream = 'recommendation' and provider = 'notisend')
  ),
  constraint email_outbox_reference_chk check (
    (stream = 'recommendation' and recommendation_issue_id is not null and event_id is null and kind = 'recommendation_issue')
    or (stream = 'transactional' and recommendation_issue_id is null and kind in ('account_auth', 'calendar_confirmation', 'event_reminder_24h', 'event_rescheduled', 'event_cancelled'))
  ),
  constraint email_outbox_idempotency_size_chk check (length(idempotency_key) between 8 and 300),
  constraint email_outbox_template_size_chk check (length(template_version) between 1 and 120),
  constraint email_outbox_payload_size_chk check (length(payload_json::text) <= 50000),
  constraint email_outbox_payload_hash_chk check (length(payload_sha256) between 32 and 128),
  constraint email_outbox_lease_chk check (
    (status <> 'claimed')
    or (lease_owner is not null and lease_token is not null and lease_expires_at is not null)
  )
);

create unique index email_outbox_provider_message_uidx
  on email_control.email_outbox (provider, provider_message_id)
  where provider_message_id is not null;
create index email_outbox_ready_idx
  on email_control.email_outbox (next_attempt_at, created_at)
  where status in ('ready', 'retryable');
create index email_outbox_user_stream_idx
  on email_control.email_outbox (user_id, stream, created_at desc);

create table email_control.send_attempt (
  id uuid primary key default extensions.gen_random_uuid(),
  outbox_id uuid not null references email_control.email_outbox(id) on delete cascade,
  attempt_number integer not null check (attempt_number between 1 and 20),
  provider text not null check (provider in ('postbox', 'notisend')),
  request_sha256 text not null,
  started_at timestamptz not null default now(),
  network_started_at timestamptz,
  finished_at timestamptz,
  outcome text check (outcome is null or outcome in ('accepted', 'retryable', 'unknown', 'failed', 'dry_run')),
  provider_message_id text,
  response_code integer,
  error_class text,
  unique (outbox_id, attempt_number),
  constraint send_attempt_request_hash_chk check (length(request_sha256) between 32 and 128)
);

create table email_control.provider_event (
  id uuid primary key default extensions.gen_random_uuid(),
  provider text not null check (provider in ('postbox', 'notisend')),
  provider_event_key text not null,
  provider_message_id text,
  event_type text not null check (
    event_type in ('accepted', 'delivered', 'delivery_delay', 'soft_bounce', 'hard_bounce', 'complaint', 'unsubscribe', 'open', 'click', 'rendering_failure', 'skipped')
  ),
  event_at timestamptz not null,
  email_hmac text,
  payload_sha256 text not null,
  authenticated boolean not null default false,
  verified boolean not null default false,
  applied boolean not null default false,
  created_at timestamptz not null default now(),
  unique (provider, provider_event_key),
  constraint provider_event_key_size_chk check (length(provider_event_key) between 1 and 300),
  constraint provider_event_payload_hash_chk check (length(payload_sha256) between 32 and 128),
  constraint provider_event_trust_chk check (not verified or authenticated)
);

alter table email_control.recipient_identity enable row level security;
alter table email_control.purpose_consent enable row level security;
alter table email_control.consent_event enable row level security;
alter table email_control.recommendation_capacity enable row level security;
alter table email_control.suppression enable row level security;
alter table email_control.runtime_switch enable row level security;
alter table email_control.recommendation_issue enable row level security;
alter table email_control.recommendation_issue_item enable row level security;
alter table email_control.email_outbox enable row level security;
alter table email_control.send_attempt enable row level security;
alter table email_control.provider_event enable row level security;

revoke all on all tables in schema email_control from public, anon, authenticated, service_role;

create or replace function public.email_sync_verified_identity_v1(
  p_user_id uuid,
  p_normalized_email text,
  p_email_hmac text,
  p_hmac_key_version integer,
  p_verified_at timestamptz
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_auth_email text;
  v_confirmed_at timestamptz;
  v_identity_id uuid;
begin
  select lower(trim(u.email)), u.email_confirmed_at
    into v_auth_email, v_confirmed_at
    from auth.users u
   where u.id = p_user_id;

  if v_auth_email is null or v_confirmed_at is null then
    raise exception 'verified auth email required' using errcode = '28000';
  end if;
  if lower(trim(p_normalized_email)) <> v_auth_email then
    raise exception 'email does not match verified auth identity' using errcode = '22023';
  end if;
  if length(p_email_hmac) < 43 or p_hmac_key_version <= 0 then
    raise exception 'versioned keyed email HMAC required' using errcode = '22023';
  end if;

  insert into email_control.recipient_identity (
    user_id, normalized_email, email_hmac, hmac_key_version, verified_at, auth_email_updated_at, updated_at
  ) values (
    p_user_id, v_auth_email, p_email_hmac, p_hmac_key_version, least(p_verified_at, v_confirmed_at), now(), now()
  )
  on conflict (user_id) do update set
    normalized_email = excluded.normalized_email,
    email_hmac = excluded.email_hmac,
    hmac_key_version = excluded.hmac_key_version,
    verified_at = excluded.verified_at,
    auth_email_updated_at = excluded.auth_email_updated_at,
    updated_at = now()
  returning id into v_identity_id;

  return v_identity_id;
end;
$$;

create or replace function public.email_set_purpose_consent_v1(
  p_purpose text,
  p_enabled boolean,
  p_terms_version text,
  p_request_id uuid
)
returns table (purpose text, state text, active_recommendation_count integer, recommendation_capacity integer)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_identity_id uuid;
  v_previous_state text;
  v_new_state text;
  v_active_count integer;
  v_capacity integer;
  v_existing_request_state text;
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;
  if p_purpose not in ('transactional_event', 'recommendation') then
    raise exception 'unsupported email purpose' using errcode = '22023';
  end if;
  if p_request_id is null or length(trim(p_terms_version)) not between 1 and 80 then
    raise exception 'request id and terms version required' using errcode = '22023';
  end if;

  select i.id into v_identity_id
    from email_control.recipient_identity i
    join auth.users u on u.id = i.user_id
   where i.user_id = v_user_id
     and u.email_confirmed_at is not null
     and lower(trim(u.email)) = i.normalized_email;
  if v_identity_id is null then
    raise exception 'verified synchronized email identity required' using errcode = '28000';
  end if;

  select ce.new_state into v_existing_request_state
    from email_control.consent_event ce
   where ce.user_id = v_user_id and ce.purpose = p_purpose and ce.request_id = p_request_id;
  if v_existing_request_state is not null then
    if v_existing_request_state <> (case when p_enabled then 'active' else 'revoked' end) then
      raise exception 'request id was already used for a different consent transition' using errcode = '23505';
    end if;
    select rc.capacity, rc.active_count into v_capacity, v_active_count
      from email_control.recommendation_capacity rc where rc.capacity_key = 'launch';
    return query select p_purpose, v_existing_request_state, v_active_count, v_capacity;
    return;
  end if;

  select c.state into v_previous_state
    from email_control.purpose_consent c
   where c.user_id = v_user_id and c.purpose = p_purpose
   for update;

  select rc.capacity into v_capacity
    from email_control.recommendation_capacity rc
   where rc.capacity_key = 'launch'
   for update;

  if p_purpose = 'recommendation' and p_enabled and coalesce(v_previous_state, '') <> 'active' then
    select count(distinct c.user_id)::integer into v_active_count
      from email_control.purpose_consent c
     where c.purpose = 'recommendation' and c.state = 'active';
    if v_active_count >= v_capacity then
      raise exception 'recommendation_capacity_full' using errcode = 'P0001';
    end if;
  end if;

  v_new_state := case when p_enabled then 'active' else 'revoked' end;
  insert into email_control.purpose_consent (
    user_id, identity_id, purpose, state, terms_version, granted_at, paused_at, revoked_at, updated_at
  ) values (
    v_user_id, v_identity_id, p_purpose, v_new_state, trim(p_terms_version),
    case when p_enabled then now() end,
    null,
    case when not p_enabled then now() end,
    now()
  )
  on conflict on constraint purpose_consent_pkey do update set
    identity_id = excluded.identity_id,
    state = excluded.state,
    terms_version = excluded.terms_version,
    granted_at = case when excluded.state = 'active' then now() else email_control.purpose_consent.granted_at end,
    paused_at = null,
    revoked_at = case when excluded.state = 'revoked' then now() else null end,
    updated_at = now();

  insert into email_control.consent_event (
    user_id, identity_id, purpose, previous_state, new_state, terms_version, request_id
  ) values (
    v_user_id, v_identity_id, p_purpose, v_previous_state, v_new_state, trim(p_terms_version), p_request_id
  )
  on conflict on constraint consent_event_user_purpose_request_uid do nothing;

  select count(distinct c.user_id)::integer into v_active_count
    from email_control.purpose_consent c
   where c.purpose = 'recommendation' and c.state = 'active';
  update email_control.recommendation_capacity
     set active_count = v_active_count, updated_at = now()
   where capacity_key = 'launch';

  return query select p_purpose, v_new_state, v_active_count, v_capacity;
end;
$$;

create or replace function public.email_get_preferences_v1()
returns table (purpose text, state text, terms_version text, updated_at timestamptz)
language sql
security definer
set search_path = ''
as $$
  select c.purpose, c.state, c.terms_version, c.updated_at
    from email_control.purpose_consent c
   where c.user_id = (select auth.uid())
   order by c.purpose
$$;

create or replace function public.email_stage_recommendation_issue_v1(
  p_user_id uuid,
  p_issue_key text,
  p_profile_revision text,
  p_event_snapshot_hash text,
  p_personal_page_path text,
  p_page_item_count integer,
  p_email_items jsonb
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_issue_id uuid;
begin
  if jsonb_typeof(p_email_items) <> 'array' or jsonb_array_length(p_email_items) > 20 then
    raise exception 'email items must be a bounded array' using errcode = '22023';
  end if;
  if position('?' in p_personal_page_path) > 0 or position('#' in p_personal_page_path) > 0 then
    raise exception 'store artifact path only, never plaintext bearer token' using errcode = '22023';
  end if;

  insert into email_control.recommendation_issue (
    user_id, issue_key, profile_revision, event_snapshot_hash, personal_page_path, page_item_count
  ) values (
    p_user_id, p_issue_key, p_profile_revision, p_event_snapshot_hash, p_personal_page_path, p_page_item_count
  ) returning id into v_issue_id;

  insert into email_control.recommendation_issue_item (issue_id, event_id, email_position, is_hero)
  select
    v_issue_id,
    (item->>'event_id')::bigint,
    (item->>'email_position')::smallint,
    coalesce((item->>'is_hero')::boolean, false)
  from jsonb_array_elements(p_email_items) as item;

  return v_issue_id;
end;
$$;

create or replace function public.email_publish_recommendation_issue_v1(
  p_issue_id uuid,
  p_artifact_sha256 text,
  p_page_token_hmac text,
  p_token_key_version integer,
  p_published_at timestamptz,
  p_validated_at timestamptz
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_item_count integer;
  v_distinct_count integer;
  v_position_count integer;
  v_hero_count integer;
begin
  select
    count(*)::integer,
    count(distinct i.event_id)::integer,
    count(distinct i.email_position)::integer,
    count(*) filter (where i.is_hero)::integer
  into v_item_count, v_distinct_count, v_position_count, v_hero_count
  from email_control.recommendation_issue_item i
  where i.issue_id = p_issue_id;

  if v_item_count <> 3 or v_distinct_count <> 3 or v_position_count <> 3
     or exists (
       select 1 from email_control.recommendation_issue_item i
        where i.issue_id = p_issue_id and i.email_position not between 1 and 3
     )
     or v_hero_count > 1 then
    raise exception 'recommendation email must contain exactly three distinct events in positions 1..3' using errcode = '23514';
  end if;
  if p_published_at is null or p_validated_at is null or p_validated_at < p_published_at then
    raise exception 'published and validated personal page required' using errcode = '23514';
  end if;
  if length(p_artifact_sha256) < 32 or length(p_page_token_hmac) < 43 or p_token_key_version <= 0 then
    raise exception 'artifact and keyed token evidence required' using errcode = '22023';
  end if;

  update email_control.recommendation_issue
     set status = 'published',
         artifact_sha256 = p_artifact_sha256,
         page_token_hmac = p_page_token_hmac,
         token_key_version = p_token_key_version,
         page_published_at = p_published_at,
         page_validated_at = p_validated_at,
         updated_at = now()
   where id = p_issue_id and status = 'staged';
  return found;
end;
$$;

create or replace function public.email_enqueue_recommendation_v1(
  p_issue_id uuid,
  p_idempotency_key text,
  p_template_version text,
  p_payload_json jsonb,
  p_payload_sha256 text,
  p_dry_run boolean default true
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_issue email_control.recommendation_issue%rowtype;
  v_identity_id uuid;
  v_email_hmac text;
  v_active_count integer;
  v_capacity integer;
  v_outbox_id uuid;
begin
  select * into v_issue from email_control.recommendation_issue where id = p_issue_id for update;
  if v_issue.id is null or v_issue.status <> 'published'
     or v_issue.page_published_at is null or v_issue.page_validated_at is null then
    raise exception 'published and validated recommendation issue required' using errcode = '23514';
  end if;
  if (select count(*) from email_control.recommendation_issue_item i where i.issue_id = p_issue_id) <> 3 then
    raise exception 'exactly three recommendation events required' using errcode = '23514';
  end if;

  select c.identity_id, i.email_hmac into v_identity_id, v_email_hmac
    from email_control.purpose_consent c
    join email_control.recipient_identity i on i.id = c.identity_id
   where c.user_id = v_issue.user_id and c.purpose = 'recommendation' and c.state = 'active';
  if v_identity_id is null then
    raise exception 'active recommendation consent required' using errcode = '28000';
  end if;

  select rc.capacity into v_capacity
    from email_control.recommendation_capacity rc where rc.capacity_key = 'launch' for update;
  select count(distinct c.user_id)::integer into v_active_count
    from email_control.purpose_consent c
   where c.purpose = 'recommendation' and c.state = 'active';
  if v_active_count > v_capacity then
    raise exception 'recommendation_capacity_exceeded' using errcode = '23514';
  end if;
  if exists (
    select 1 from email_control.suppression s
     where s.email_hmac = v_email_hmac and s.active and s.scope in ('all', 'recommendation')
  ) then
    raise exception 'recipient suppressed' using errcode = '28000';
  end if;

  insert into email_control.email_outbox (
    user_id, identity_id, stream, provider, kind, recommendation_issue_id,
    idempotency_key, template_version, payload_json, payload_sha256, dry_run
  ) values (
    v_issue.user_id, v_identity_id, 'recommendation', 'notisend', 'recommendation_issue', p_issue_id,
    p_idempotency_key, p_template_version, coalesce(p_payload_json, '{}'::jsonb), p_payload_sha256, coalesce(p_dry_run, true)
  )
  on conflict (idempotency_key) do update set idempotency_key = excluded.idempotency_key
  returning id into v_outbox_id;

  update email_control.recommendation_issue set status = 'enqueued', updated_at = now() where id = p_issue_id;
  return v_outbox_id;
end;
$$;

create or replace function public.email_enqueue_transactional_v1(
  p_user_id uuid,
  p_kind text,
  p_event_id bigint,
  p_idempotency_key text,
  p_template_version text,
  p_payload_json jsonb,
  p_payload_sha256 text,
  p_dry_run boolean default true
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_identity_id uuid;
  v_email_hmac text;
  v_outbox_id uuid;
begin
  if p_kind not in ('account_auth', 'calendar_confirmation', 'event_reminder_24h', 'event_rescheduled', 'event_cancelled') then
    raise exception 'unsupported transactional kind' using errcode = '22023';
  end if;
  if p_kind <> 'account_auth' and p_event_id is null then
    raise exception 'event id required' using errcode = '22023';
  end if;

  select i.id, i.email_hmac into v_identity_id, v_email_hmac
    from email_control.recipient_identity i
   where i.user_id = p_user_id;
  if v_identity_id is null then
    raise exception 'verified synchronized email identity required' using errcode = '28000';
  end if;
  if p_kind <> 'account_auth' and not exists (
    select 1 from email_control.purpose_consent c
     where c.user_id = p_user_id and c.purpose = 'transactional_event' and c.state = 'active'
  ) then
    raise exception 'transactional event consent required' using errcode = '28000';
  end if;
  if exists (
    select 1 from email_control.suppression s
     where s.email_hmac = v_email_hmac and s.active and s.scope in ('all', 'transactional')
  ) then
    raise exception 'recipient suppressed' using errcode = '28000';
  end if;

  insert into email_control.email_outbox (
    user_id, identity_id, stream, provider, kind, event_id,
    idempotency_key, template_version, payload_json, payload_sha256, dry_run
  ) values (
    p_user_id, v_identity_id, 'transactional', 'postbox', p_kind, p_event_id,
    p_idempotency_key, p_template_version, coalesce(p_payload_json, '{}'::jsonb), p_payload_sha256, coalesce(p_dry_run, true)
  )
  on conflict (idempotency_key) do update set idempotency_key = excluded.idempotency_key
  returning id into v_outbox_id;
  return v_outbox_id;
end;
$$;

create or replace function public.email_claim_outbox_v1(
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
  if length(trim(p_worker_id)) not between 1 and 120 or p_limit not between 1 and 50 or p_lease_seconds not between 30 and 900 then
    raise exception 'invalid claim parameters' using errcode = '22023';
  end if;

  return query
  with eligible as (
    select o.id
      from email_control.email_outbox o
      join email_control.recipient_identity i on i.id = o.identity_id
      join auth.users u on u.id = o.user_id
      join email_control.runtime_switch g on g.switch_key = 'global'
      join email_control.runtime_switch s on s.switch_key = o.stream
     where o.status in ('ready', 'retryable')
       and o.next_attempt_at <= now()
       and u.email_confirmed_at is not null
       and lower(trim(u.email)) = i.normalized_email
       and (o.dry_run or (g.enabled and s.enabled and not g.dry_run_only and not s.dry_run_only))
       and not exists (
         select 1 from email_control.suppression x
          where x.email_hmac = i.email_hmac and x.active
            and (x.scope = 'all' or x.scope = o.stream)
       )
       and (
         (
           o.stream = 'transactional'
           and (
             o.kind = 'account_auth'
             or exists (
               select 1 from email_control.purpose_consent c
                where c.user_id = o.user_id and c.purpose = 'transactional_event' and c.state = 'active'
             )
           )
         )
         or (
           exists (
             select 1 from email_control.purpose_consent c
              where c.user_id = o.user_id and c.purpose = 'recommendation' and c.state = 'active'
           )
           and exists (
             select 1 from email_control.recommendation_issue ri
              where ri.id = o.recommendation_issue_id and ri.status = 'enqueued'
                and ri.page_published_at is not null and ri.page_validated_at is not null
                and (select count(*) from email_control.recommendation_issue_item x where x.issue_id = ri.id) = 3
           )
           and (
             select count(distinct c.user_id)
               from email_control.purpose_consent c
              where c.purpose = 'recommendation' and c.state = 'active'
           ) <= (
             select rc.capacity from email_control.recommendation_capacity rc where rc.capacity_key = 'launch'
           )
         )
       )
     order by o.next_attempt_at, o.created_at
     for update of o skip locked
     limit p_limit
  ), claimed as (
    update email_control.email_outbox o
       set status = 'claimed',
           lease_owner = trim(p_worker_id),
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

create or replace function public.email_mark_network_started_v1(
  p_outbox_id uuid,
  p_lease_token uuid,
  p_request_sha256 text
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_attempt_id uuid;
  v_attempt integer;
  v_provider text;
begin
  update email_control.email_outbox
     set network_started_at = now(), updated_at = now()
   where id = p_outbox_id and status = 'claimed' and lease_token = p_lease_token and lease_expires_at > now()
   returning attempts, provider into v_attempt, v_provider;
  if not found then
    raise exception 'active claim lease required' using errcode = '40001';
  end if;

  insert into email_control.send_attempt (
    outbox_id, attempt_number, provider, request_sha256, network_started_at
  ) values (
    p_outbox_id, v_attempt, v_provider, p_request_sha256, now()
  ) returning id into v_attempt_id;
  return v_attempt_id;
end;
$$;

create or replace function public.email_finish_attempt_v1(
  p_outbox_id uuid,
  p_lease_token uuid,
  p_outcome text,
  p_provider_message_id text default null,
  p_response_code integer default null,
  p_error_class text default null,
  p_retry_at timestamptz default null
)
returns text
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_attempt integer;
  v_dry_run boolean;
  v_network_started_at timestamptz;
  v_new_status text;
begin
  if p_outcome not in ('accepted', 'retryable', 'unknown', 'failed', 'dry_run') then
    raise exception 'unsupported attempt outcome' using errcode = '22023';
  end if;
  if p_outcome = 'accepted' and coalesce(trim(p_provider_message_id), '') = '' then
    raise exception 'real provider message id required for accepted send' using errcode = '23514';
  end if;

  select attempts, dry_run, network_started_at into v_attempt, v_dry_run, v_network_started_at
    from email_control.email_outbox
   where id = p_outbox_id and status = 'claimed' and lease_token = p_lease_token
     and lease_expires_at > now()
   for update;
  if not found then
    raise exception 'active claim lease required' using errcode = '40001';
  end if;
  if p_outcome = 'dry_run' and not v_dry_run then
    raise exception 'non-dry-run row cannot finish as dry run' using errcode = '23514';
  end if;
  if p_outcome <> 'dry_run' and v_network_started_at is null then
    raise exception 'network-start marker required before provider outcome' using errcode = '23514';
  end if;

  v_new_status := case p_outcome
    when 'accepted' then 'submitted'
    when 'retryable' then 'retryable'
    when 'unknown' then 'unknown_delivery'
    when 'failed' then 'terminal_failed'
    when 'dry_run' then 'dry_run_complete'
  end;

  if p_outcome <> 'dry_run' then
    update email_control.send_attempt
       set finished_at = now(), outcome = p_outcome, provider_message_id = p_provider_message_id,
           response_code = p_response_code, error_class = left(p_error_class, 160)
     where outbox_id = p_outbox_id and attempt_number = v_attempt;
    if not found then
      raise exception 'send attempt record required' using errcode = '23514';
    end if;
  end if;

  update email_control.email_outbox
     set status = v_new_status,
         provider_message_id = case when p_outcome = 'accepted' then p_provider_message_id else provider_message_id end,
         last_error_class = left(p_error_class, 160),
         next_attempt_at = case when p_outcome = 'retryable' then coalesce(p_retry_at, now() + interval '5 minutes') else next_attempt_at end,
         lease_owner = null, lease_token = null, lease_expires_at = null,
         updated_at = now()
   where id = p_outbox_id;

  return v_new_status;
end;
$$;

create or replace function public.email_recover_expired_claims_v1()
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
       set status = 'retryable', lease_owner = null, lease_token = null, lease_expires_at = null,
           next_attempt_at = now(), updated_at = now()
     where status = 'claimed' and lease_expires_at <= now() and network_started_at is null
     returning 1
  ) select count(*)::integer into v_retryable from recovered;

  with quarantined as (
    update email_control.email_outbox
       set status = 'unknown_delivery', lease_owner = null, lease_token = null, lease_expires_at = null,
           updated_at = now()
     where status = 'claimed' and lease_expires_at <= now() and network_started_at is not null
     returning 1
  ) select count(*)::integer into v_unknown from quarantined;

  return query select v_retryable, v_unknown;
end;
$$;

create or replace function public.email_record_provider_event_v1(
  p_provider text,
  p_provider_event_key text,
  p_provider_message_id text,
  p_event_type text,
  p_event_at timestamptz,
  p_email_hmac text,
  p_payload_sha256 text,
  p_authenticated boolean,
  p_verified boolean
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_inserted boolean;
  v_outbox email_control.email_outbox%rowtype;
  v_scope text;
begin
  if p_provider not in ('postbox', 'notisend') then
    raise exception 'unsupported provider' using errcode = '22023';
  end if;
  if p_verified and not p_authenticated then
    raise exception 'verified events must be authenticated' using errcode = '23514';
  end if;

  insert into email_control.provider_event (
    provider, provider_event_key, provider_message_id, event_type, event_at,
    email_hmac, payload_sha256, authenticated, verified
  ) values (
    p_provider, p_provider_event_key, p_provider_message_id, p_event_type, p_event_at,
    p_email_hmac, p_payload_sha256, p_authenticated, p_verified
  ) on conflict (provider, provider_event_key) do nothing;
  v_inserted := found;

  -- Unauthenticated NotiSend webhook bodies are evidence-only signals. They cannot
  -- change delivery state or suppress a recipient until an authenticated API poll verifies them.
  if not v_inserted or not p_authenticated or not p_verified then
    return v_inserted;
  end if;

  select * into v_outbox
    from email_control.email_outbox o
   where o.provider = p_provider and o.provider_message_id = p_provider_message_id
   for update;

  if v_outbox.id is not null then
    update email_control.email_outbox
       set status = case
         when p_event_type = 'delivered' then 'delivered'
         when p_event_type in ('hard_bounce', 'complaint') then 'terminal_failed'
         when p_event_type = 'skipped' then 'skipped'
         else status
       end,
       updated_at = now()
     where id = v_outbox.id;
  end if;

  if p_event_type in ('hard_bounce', 'complaint', 'unsubscribe') and p_email_hmac is not null then
    v_scope := case when p_event_type = 'unsubscribe' then 'recommendation' else 'all' end;
    insert into email_control.suppression (
      email_hmac, scope, provider, reason, provider_event_key, evidence
    ) values (
      p_email_hmac, v_scope, p_provider,
      case p_event_type when 'hard_bounce' then 'hard_bounce' when 'complaint' then 'complaint' else 'unsubscribe' end,
      p_provider_event_key,
      jsonb_build_object('source', 'verified_provider_event')
    ) on conflict do nothing;
  end if;

  update email_control.provider_event
     set applied = true
   where provider = p_provider and provider_event_key = p_provider_event_key;
  return true;
end;
$$;

revoke execute on function public.email_sync_verified_identity_v1(uuid, text, text, integer, timestamptz) from public, anon, authenticated;
revoke execute on function public.email_set_purpose_consent_v1(text, boolean, text, uuid) from public, anon;
revoke execute on function public.email_get_preferences_v1() from public, anon;
revoke execute on function public.email_stage_recommendation_issue_v1(uuid, text, text, text, text, integer, jsonb) from public, anon, authenticated;
revoke execute on function public.email_publish_recommendation_issue_v1(uuid, text, text, integer, timestamptz, timestamptz) from public, anon, authenticated;
revoke execute on function public.email_enqueue_recommendation_v1(uuid, text, text, jsonb, text, boolean) from public, anon, authenticated;
revoke execute on function public.email_enqueue_transactional_v1(uuid, text, bigint, text, text, jsonb, text, boolean) from public, anon, authenticated;
revoke execute on function public.email_claim_outbox_v1(text, integer, integer) from public, anon, authenticated;
revoke execute on function public.email_mark_network_started_v1(uuid, uuid, text) from public, anon, authenticated;
revoke execute on function public.email_finish_attempt_v1(uuid, uuid, text, text, integer, text, timestamptz) from public, anon, authenticated;
revoke execute on function public.email_recover_expired_claims_v1() from public, anon, authenticated;
revoke execute on function public.email_record_provider_event_v1(text, text, text, text, timestamptz, text, text, boolean, boolean) from public, anon, authenticated;

grant execute on function public.email_set_purpose_consent_v1(text, boolean, text, uuid) to authenticated;
grant execute on function public.email_get_preferences_v1() to authenticated;
grant execute on function public.email_sync_verified_identity_v1(uuid, text, text, integer, timestamptz) to service_role;
grant execute on function public.email_stage_recommendation_issue_v1(uuid, text, text, text, text, integer, jsonb) to service_role;
grant execute on function public.email_publish_recommendation_issue_v1(uuid, text, text, integer, timestamptz, timestamptz) to service_role;
grant execute on function public.email_enqueue_recommendation_v1(uuid, text, text, jsonb, text, boolean) to service_role;
grant execute on function public.email_enqueue_transactional_v1(uuid, text, bigint, text, text, jsonb, text, boolean) to service_role;
grant execute on function public.email_claim_outbox_v1(text, integer, integer) to service_role;
grant execute on function public.email_mark_network_started_v1(uuid, uuid, text) to service_role;
grant execute on function public.email_finish_attempt_v1(uuid, uuid, text, text, integer, text, timestamptz) to service_role;
grant execute on function public.email_recover_expired_claims_v1() to service_role;
grant execute on function public.email_record_provider_event_v1(text, text, text, text, timestamptz, text, text, boolean, boolean) to service_role;
