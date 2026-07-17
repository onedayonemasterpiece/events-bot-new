-- Site-wide identity, saved occurrence and D-1 reminder foundation.
-- Target: personalization Supabase/Postgres only. Canonical event facts stay in Fly SQLite.
-- Additive and fail-closed: browser roles receive RPC EXECUTE only, never table access.

create extension if not exists pgcrypto with schema extensions;

create schema if not exists site_identity;
create schema if not exists saved_events;
revoke all on schema site_identity, saved_events from public, anon, authenticated;
grant usage on schema site_identity, saved_events to service_role;

create table site_identity.profile (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null unique references auth.users(id) on delete cascade,
  status text not null default 'active' check (status in ('active','deleting','deleted')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  deleted_at timestamptz
);

create table site_identity.device (
  id uuid primary key,
  credential_hash bytea not null unique,
  linked_profile_id uuid references site_identity.profile(id) on delete set null,
  personalization_consent_version text,
  consented_at timestamptz,
  expires_at timestamptz not null default (now() + interval '180 days'),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  revoked_at timestamptz,
  constraint device_consent_chk check (
    (personalization_consent_version is null and consented_at is null)
    or (length(personalization_consent_version) between 1 and 80 and consented_at is not null)
  )
);
create index device_linked_profile_idx on site_identity.device(linked_profile_id)
  where linked_profile_id is not null;

create table site_identity.profile_identity_link (
  id uuid primary key default extensions.gen_random_uuid(),
  profile_id uuid not null references site_identity.profile(id) on delete cascade,
  auth_user_id uuid not null references auth.users(id) on delete cascade,
  provider text not null check (provider in ('email','custom:yandex','yandex','phone','unknown')),
  provider_subject text not null,
  linked_at timestamptz not null default now(),
  unlinked_at timestamptz,
  constraint profile_identity_subject_size_chk check (length(provider_subject) between 1 and 512)
);
create unique index profile_identity_active_subject_uidx
  on site_identity.profile_identity_link(provider, provider_subject) where unlinked_at is null;
create unique index profile_identity_active_user_provider_uidx
  on site_identity.profile_identity_link(auth_user_id, provider) where unlinked_at is null;
create index profile_identity_profile_idx on site_identity.profile_identity_link(profile_id);
create index profile_identity_auth_user_idx on site_identity.profile_identity_link(auth_user_id);

create table site_identity.merge_audit (
  request_id uuid primary key,
  source_device_id uuid not null,
  target_profile_id uuid not null references site_identity.profile(id) on delete cascade,
  status text not null check (status in ('merged','already_linked','conflict','rejected')),
  imported_saved_count integer not null default 0 check (imported_saved_count >= 0),
  conflict_policy text not null default 'authenticated_explicit_wins',
  consent_version text not null,
  occurred_at timestamptz not null default now(),
  constraint merge_consent_size_chk check (length(consent_version) between 1 and 80)
);
create index merge_audit_target_profile_idx on site_identity.merge_audit(target_profile_id);

create table site_identity.purge_request (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null,
  profile_id uuid not null,
  status text not null default 'pending' check (status in ('pending','projected','complete','failed')),
  requested_at timestamptz not null default now(),
  completed_at timestamptz
);

create table saved_events.anonymous_saved_occurrence (
  device_id uuid not null references site_identity.device(id) on delete cascade,
  event_id bigint not null check (event_id > 0),
  occurrence_key text not null,
  occurrence_starts_at timestamptz,
  saved_at timestamptz not null default now(),
  removed_at timestamptz,
  primary key (device_id,event_id,occurrence_key),
  constraint anon_occurrence_key_size_chk check (length(occurrence_key) between 1 and 160)
);

create table saved_events.saved_occurrence (
  id uuid primary key default extensions.gen_random_uuid(),
  profile_id uuid not null references site_identity.profile(id) on delete cascade,
  event_id bigint not null check (event_id > 0),
  occurrence_key text not null,
  occurrence_starts_at timestamptz,
  occurrence_validated_at timestamptz,
  lifecycle_status text not null default 'upcoming' check (lifecycle_status in ('upcoming','rescheduled','cancelled','completed')),
  lifecycle_revision integer not null default 1 check (lifecycle_revision > 0),
  saved_at timestamptz not null default now(),
  removed_at timestamptz,
  updated_at timestamptz not null default now(),
  unique (profile_id,event_id,occurrence_key),
  constraint saved_occurrence_key_size_chk check (length(occurrence_key) between 1 and 160)
);
create index saved_occurrence_profile_active_idx
  on saved_events.saved_occurrence(profile_id,saved_at desc) where removed_at is null;
create index saved_occurrence_event_active_idx
  on saved_events.saved_occurrence(event_id,occurrence_key) where removed_at is null;

create table saved_events.event_signal (
  profile_id uuid not null references site_identity.profile(id) on delete cascade,
  event_id bigint not null check (event_id > 0),
  occurrence_key text not null,
  signal text not null check (signal in ('like','not_interested')),
  active boolean not null default true,
  occurred_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (profile_id,event_id,occurrence_key,signal),
  constraint event_signal_occurrence_key_size_chk check (length(occurrence_key) between 1 and 160)
);

create table saved_events.reminder_subscription (
  id uuid primary key default extensions.gen_random_uuid(),
  saved_occurrence_id uuid not null unique references saved_events.saved_occurrence(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  state text not null default 'active' check (state in ('active','paused','revoked','expired','completed','cancelled')),
  terms_version text not null,
  schedule_revision integer not null default 1 check (schedule_revision > 0),
  scheduled_for timestamptz,
  reminder_sent_at timestamptz,
  masked_email text,
  email_verified_at timestamptz,
  granted_at timestamptz not null default now(),
  revoked_at timestamptz,
  updated_at timestamptz not null default now(),
  constraint reminder_terms_size_chk check (length(terms_version) between 1 and 80),
  constraint reminder_masked_email_chk check (
    (masked_email is null and email_verified_at is null)
    or (length(masked_email) between 5 and 324 and email_verified_at is not null)
  )
);
create index reminder_due_idx on saved_events.reminder_subscription(scheduled_for,id)
  where state='active' and reminder_sent_at is null;
create index reminder_subscription_user_idx on saved_events.reminder_subscription(user_id);

create table saved_events.reminder_consent_event (
  request_id uuid primary key,
  reminder_subscription_id uuid not null references saved_events.reminder_subscription(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  previous_state text,
  new_state text not null check (new_state in ('active','revoked')),
  terms_version text not null,
  occurred_at timestamptz not null default now(),
  constraint reminder_consent_terms_size_chk check (length(terms_version) between 1 and 80)
);
create index reminder_consent_subscription_idx on saved_events.reminder_consent_event(reminder_subscription_id);
create index reminder_consent_user_idx on saved_events.reminder_consent_event(user_id);

create table saved_events.reminder_delivery (
  id uuid primary key default extensions.gen_random_uuid(),
  reminder_subscription_id uuid not null references saved_events.reminder_subscription(id) on delete cascade,
  kind text not null check (kind in ('event_reminder_24h','event_rescheduled','event_cancelled')),
  schedule_revision integer not null check (schedule_revision > 0),
  outbox_id uuid references email_control.email_outbox(id) on delete set null,
  idempotency_key text not null unique,
  created_at timestamptz not null default now(),
  unique (reminder_subscription_id,kind,schedule_revision)
);
create index reminder_delivery_outbox_idx on saved_events.reminder_delivery(outbox_id)
  where outbox_id is not null;

alter table site_identity.profile enable row level security;
alter table site_identity.device enable row level security;
alter table site_identity.profile_identity_link enable row level security;
alter table site_identity.merge_audit enable row level security;
alter table site_identity.purge_request enable row level security;
alter table saved_events.anonymous_saved_occurrence enable row level security;
alter table saved_events.saved_occurrence enable row level security;
alter table saved_events.event_signal enable row level security;
alter table saved_events.reminder_subscription enable row level security;
alter table saved_events.reminder_consent_event enable row level security;
alter table saved_events.reminder_delivery enable row level security;

revoke all on all tables in schema site_identity, saved_events from public, anon, authenticated, service_role;
grant all on all tables in schema site_identity, saved_events to service_role;

-- Supabase anonymous Auth users also use the `authenticated` Postgres role. They
-- remain on the device-proof flow and must not gain account-owned RPC access.
create or replace function site_identity.current_user_v1()
returns uuid language plpgsql stable set search_path = '' as $$
declare v_user uuid := (select auth.uid());
begin
  if v_user is null
     or lower(coalesce((select auth.jwt() ->> 'is_anonymous'), 'false')) = 'true' then
    raise exception 'permanent authenticated user required' using errcode='28000';
  end if;
  return v_user;
end; $$;
revoke all on function site_identity.current_user_v1() from public, anon, authenticated;
grant execute on function site_identity.current_user_v1() to service_role;

create or replace function site_identity.ensure_profile_v1(p_user_id uuid)
returns uuid language plpgsql security definer set search_path = '' as $$
declare v_profile_id uuid;
begin
  if p_user_id is null or not exists (select 1 from auth.users u where u.id=p_user_id) then
    raise exception 'valid auth user required' using errcode='28000';
  end if;
  insert into site_identity.profile(user_id) values (p_user_id)
  on conflict(user_id) do update set updated_at=now()
  returning id into v_profile_id;
  return v_profile_id;
end; $$;
revoke all on function site_identity.ensure_profile_v1(uuid) from public, anon, authenticated;
grant execute on function site_identity.ensure_profile_v1(uuid) to service_role;

create or replace function public.personalization_save_occurrence_v1(
  p_event_id bigint, p_occurrence_key text, p_occurrence_starts_at timestamptz, p_saved boolean default true
) returns table(saved boolean, unique_saved_event_count bigint, lifecycle_status text)
language plpgsql security definer set search_path='' as $$
declare v_user uuid := site_identity.current_user_v1(); v_profile uuid; v_row saved_events.saved_occurrence%rowtype;
begin
  if p_saved is null or p_event_id <= 0 or length(trim(coalesce(p_occurrence_key,''))) not between 1 and 160 then
    raise exception 'valid occurrence required' using errcode='22023';
  end if;
  -- Browser/static-page timestamps are untrusted hints. Only the service lifecycle
  -- sync may populate the canonical time used for email scheduling.
  v_profile := site_identity.ensure_profile_v1(v_user);
  insert into saved_events.saved_occurrence(profile_id,event_id,occurrence_key,occurrence_starts_at,removed_at,updated_at)
  values(v_profile,p_event_id,trim(p_occurrence_key),null,case when p_saved then null else now() end,now())
  on conflict(profile_id,event_id,occurrence_key) do update set
    removed_at=excluded.removed_at, updated_at=now()
  returning * into v_row;
  if not p_saved then
    update saved_events.reminder_subscription set
      state='revoked',scheduled_for=null,masked_email=null,email_verified_at=null,
      revoked_at=now(),updated_at=now()
      where saved_occurrence_id=v_row.id and state in ('active','paused');
  end if;
  return query select p_saved,
    (select count(distinct s.event_id) from saved_events.saved_occurrence s where s.profile_id=v_profile and s.removed_at is null),
    v_row.lifecycle_status;
end; $$;

create or replace function public.personalization_saved_count_v1()
returns bigint language sql stable security definer set search_path='' as $$
  select count(distinct s.event_id)
  from saved_events.saved_occurrence s join site_identity.profile p on p.id=s.profile_id
  where p.user_id=site_identity.current_user_v1() and p.status='active' and s.removed_at is null;
$$;

create or replace function public.personalization_set_event_signal_v1(
  p_event_id bigint,p_occurrence_key text,p_signal text,p_active boolean
) returns boolean language plpgsql security definer set search_path='' as $$
declare v_user uuid := site_identity.current_user_v1(); v_profile uuid;
begin
 if p_active is null or p_event_id<=0 or p_signal not in ('like','not_interested') or length(trim(coalesce(p_occurrence_key,''))) not between 1 and 160 then
   raise exception 'invalid signal' using errcode='22023'; end if;
 v_profile:=site_identity.ensure_profile_v1(v_user);
 insert into saved_events.event_signal(profile_id,event_id,occurrence_key,signal,active)
 values(v_profile,p_event_id,trim(p_occurrence_key),p_signal,p_active)
 on conflict(profile_id,event_id,occurrence_key,signal) do update set active=excluded.active,updated_at=now();
 return p_active;
end; $$;

create or replace function saved_events.schedule_d1_v1(p_starts_at timestamptz,p_now timestamptz)
returns timestamptz language plpgsql immutable set search_path='' as $$
declare
 v_nominal timestamptz:=p_starts_at-interval '24 hours';
 v_due timestamptz;
 v_local timestamp;
begin
 if p_starts_at is null or p_starts_at<=p_now then return null; end if;
 -- A newly granted consent may catch up a missed D-1 only within six hours.
 -- Older reminders fail closed rather than becoming misleading near-event mail.
 if p_now-v_nominal>interval '6 hours' then return null; end if;
 v_due:=greatest(v_nominal,p_now);
 v_local:=v_due at time zone 'Europe/Kaliningrad';
 if extract(hour from v_local)>=22 then v_due:=((v_local::date+1)+time '08:00') at time zone 'Europe/Kaliningrad';
 elsif extract(hour from v_local)<8 then v_due:=(v_local::date+time '08:00') at time zone 'Europe/Kaliningrad'; end if;
 if v_due>=p_starts_at then return null; end if;
 return v_due;
end; $$;
revoke all on function saved_events.schedule_d1_v1(timestamptz,timestamptz) from public,anon,authenticated;
grant execute on function saved_events.schedule_d1_v1(timestamptz,timestamptz) to service_role;

create or replace function public.personalization_set_reminder_v1(
 p_event_id bigint,p_occurrence_key text,p_enabled boolean,p_terms_version text,p_request_id uuid
) returns table(state text,scheduled_for timestamptz,masked_email text)
language plpgsql security definer set search_path='' as $$
declare
 v_user uuid:=site_identity.current_user_v1();
 v_saved saved_events.saved_occurrence%rowtype;
 v_identity uuid;
 v_email text;
 v_verified_at timestamptz;
 v_masked text;
 v_state text;
 v_previous text;
 v_when timestamptz;
 v_subscription uuid;
 v_replay record;
begin
 if p_enabled is null or p_request_id is null or length(trim(coalesce(p_terms_version,''))) not between 1 and 80 then raise exception 'consent evidence required' using errcode='22023'; end if;
 select s.* into v_saved from saved_events.saved_occurrence s join site_identity.profile p on p.id=s.profile_id
 where p.user_id=v_user and s.event_id=p_event_id and s.occurrence_key=trim(p_occurrence_key) and s.removed_at is null for update;
 if v_saved.id is null then raise exception 'saved occurrence required' using errcode='P0002'; end if;
 v_state:=case when p_enabled then 'active' else 'revoked' end;

 select e.user_id,e.new_state,e.terms_version,r.saved_occurrence_id,
        r.id reminder_subscription_id,r.state reminder_state,r.scheduled_for,
        r.masked_email
 into v_replay
 from saved_events.reminder_consent_event e
 join saved_events.reminder_subscription r on r.id=e.reminder_subscription_id
 where e.request_id=p_request_id;
 if v_replay.user_id is not null then
  if v_replay.user_id<>v_user or v_replay.saved_occurrence_id<>v_saved.id
     or v_replay.new_state<>v_state or v_replay.terms_version<>trim(p_terms_version) then
   raise exception 'consent request replay conflict' using errcode='23505';
  end if;
  return query select v_replay.reminder_state,v_replay.scheduled_for,v_replay.masked_email;
  return;
 end if;

 if p_enabled then
  if v_saved.lifecycle_status not in ('upcoming','rescheduled') or v_saved.occurrence_validated_at is null then
   raise exception 'canonical upcoming occurrence required' using errcode='22023';
  end if;
  select i.id,i.normalized_email,i.verified_at into v_identity,v_email,v_verified_at
  from email_control.recipient_identity i where i.user_id=v_user;
  if v_identity is null or v_verified_at is null then raise exception 'verified email required' using errcode='28000'; end if;
  if not exists(
   select 1 from email_control.purpose_consent c
   where c.user_id=v_user and c.identity_id=v_identity
     and c.purpose='transactional_event' and c.state='active'
  ) then raise exception 'transactional consent required' using errcode='28000'; end if;
  v_when:=saved_events.schedule_d1_v1(v_saved.occurrence_starts_at,now());
  if v_when is null then raise exception 'D-1 reminder window unavailable' using errcode='22023'; end if;
  v_masked:=left(split_part(v_email,'@',1),1)||'***@'||split_part(v_email,'@',2);
 else
  v_when:=null;
  v_masked:=null;
  v_verified_at:=null;
 end if;

 select r.state into v_previous from saved_events.reminder_subscription r where r.saved_occurrence_id=v_saved.id;
 insert into saved_events.reminder_subscription(
  saved_occurrence_id,user_id,state,terms_version,scheduled_for,masked_email,email_verified_at,revoked_at
 ) values(
  v_saved.id,v_user,v_state,trim(p_terms_version),v_when,v_masked,v_verified_at,case when p_enabled then null else now() end
 )
 on conflict(saved_occurrence_id) do update set state=excluded.state,terms_version=excluded.terms_version,
 scheduled_for=excluded.scheduled_for,masked_email=excluded.masked_email,
 email_verified_at=excluded.email_verified_at,revoked_at=excluded.revoked_at,updated_at=now()
 returning reminder_subscription.id,reminder_subscription.state,reminder_subscription.scheduled_for into v_subscription,v_state,v_when;
 insert into saved_events.reminder_consent_event(request_id,reminder_subscription_id,user_id,previous_state,new_state,terms_version)
 values(p_request_id,v_subscription,v_user,v_previous,v_state,trim(p_terms_version)) on conflict(request_id) do nothing;
 return query select v_state,v_when,v_masked;
end; $$;

-- Service-only registration/materialization. The Edge Function hashes a random 256-bit device secret;
-- anonymous IDs alone are never accepted as ownership proof.
create or replace function public.personalization_materialize_device_v1(
 p_device_id uuid,p_credential_hash_hex text,p_consent_version text,p_saved jsonb default '[]'::jsonb
) returns uuid language plpgsql security definer set search_path='' as $$
declare v_item jsonb; v_materialized uuid;
begin
 if p_device_id is null or p_credential_hash_hex !~ '^[0-9a-f]{64}$' or length(trim(coalesce(p_consent_version,''))) not between 1 and 80
   or jsonb_typeof(coalesce(p_saved,'[]'::jsonb))<>'array' or jsonb_array_length(coalesce(p_saved,'[]'::jsonb))>500 then
   raise exception 'invalid device materialization' using errcode='22023'; end if;
 insert into site_identity.device(id,credential_hash,personalization_consent_version,consented_at,expires_at,last_seen_at)
 values(p_device_id,decode(p_credential_hash_hex,'hex'),trim(p_consent_version),now(),now()+interval '180 days',now())
 on conflict(id) do update set
  personalization_consent_version=excluded.personalization_consent_version,
  consented_at=excluded.consented_at,last_seen_at=excluded.last_seen_at,expires_at=excluded.expires_at
 where site_identity.device.credential_hash=excluded.credential_hash
 returning id into v_materialized;
 if v_materialized is null then raise exception 'device credential conflict' using errcode='23505'; end if;
 for v_item in select value from jsonb_array_elements(coalesce(p_saved,'[]'::jsonb)) loop
   if (v_item->>'event_id')~'^[1-9][0-9]{0,18}$' and length(trim(coalesce(v_item->>'occurrence_key',''))) between 1 and 160 then
    insert into saved_events.anonymous_saved_occurrence(device_id,event_id,occurrence_key,occurrence_starts_at,removed_at)
    values(p_device_id,(v_item->>'event_id')::bigint,trim(v_item->>'occurrence_key'),nullif(v_item->>'occurrence_starts_at','')::timestamptz,
      case when coalesce((v_item->>'saved')::boolean,true) then null else now() end)
    on conflict(device_id,event_id,occurrence_key) do update set removed_at=excluded.removed_at;
   end if;
 end loop;
 return p_device_id;
end; $$;

create or replace function public.personalization_merge_device_v1(
 p_user_id uuid,p_device_id uuid,p_credential_hash_hex text,p_consent_version text,p_request_id uuid
) returns table(profile_id uuid,merge_status text,imported_saved_count integer)
language plpgsql security definer set search_path='' as $$
declare v_profile uuid; v_device site_identity.device%rowtype; v_count integer:=0; v_existing site_identity.merge_audit%rowtype; v_existing_user uuid; v_ident record;
begin
 if p_user_id is null or p_request_id is null or p_credential_hash_hex !~ '^[0-9a-f]{64}$'
    or length(trim(coalesce(p_consent_version,''))) not between 1 and 80 then raise exception 'invalid merge request' using errcode='22023'; end if;
 select * into v_device from site_identity.device
 where id=p_device_id and credential_hash=decode(p_credential_hash_hex,'hex') for update;
 if v_device.id is null then raise exception 'device proof invalid' using errcode='28000'; end if;
 select * into v_existing from site_identity.merge_audit where request_id=p_request_id;
 if v_existing.request_id is not null then
  select p.user_id into v_existing_user from site_identity.profile p where p.id=v_existing.target_profile_id;
  if v_existing.source_device_id<>p_device_id or v_existing_user<>p_user_id
     or v_existing.consent_version<>trim(p_consent_version) then
   raise exception 'merge request replay conflict' using errcode='23505';
  end if;
  return query select v_existing.target_profile_id,v_existing.status,v_existing.imported_saved_count; return;
 end if;
 if v_device.revoked_at is not null or v_device.expires_at<=now() or v_device.consented_at is null
    or v_device.personalization_consent_version<>trim(p_consent_version) then raise exception 'device proof or consent invalid' using errcode='28000'; end if;
 v_profile:=site_identity.ensure_profile_v1(p_user_id);
 if v_device.linked_profile_id is not null and v_device.linked_profile_id<>v_profile then raise exception 'device already linked to another account' using errcode='23505'; end if;
 insert into saved_events.saved_occurrence(profile_id,event_id,occurrence_key,occurrence_starts_at,removed_at)
 select v_profile,a.event_id,a.occurrence_key,null,a.removed_at from saved_events.anonymous_saved_occurrence a where a.device_id=p_device_id
 on conflict on constraint saved_occurrence_profile_id_event_id_occurrence_key_key do nothing;
 get diagnostics v_count=row_count;
 update site_identity.device set linked_profile_id=v_profile,last_seen_at=now() where id=p_device_id;
 for v_ident in select i.provider,i.provider_id from auth.identities i where i.user_id=p_user_id loop
   insert into site_identity.profile_identity_link(profile_id,auth_user_id,provider,provider_subject)
   values(v_profile,p_user_id,case when v_ident.provider in ('email','custom:yandex','yandex','phone') then v_ident.provider else 'unknown' end,v_ident.provider_id)
   on conflict do nothing;
 end loop;
 insert into site_identity.merge_audit(request_id,source_device_id,target_profile_id,status,imported_saved_count,consent_version)
 values(p_request_id,p_device_id,v_profile,case when v_count=0 then 'already_linked' else 'merged' end,v_count,trim(p_consent_version));
 return query select v_profile,case when v_count=0 then 'already_linked' else 'merged' end,v_count;
end; $$;

create or replace function public.personalization_unlink_device_v1(p_user_id uuid,p_device_id uuid,p_credential_hash_hex text)
returns boolean language plpgsql security definer set search_path='' as $$
declare v_profile uuid;
begin
 if p_credential_hash_hex !~ '^[0-9a-f]{64}$' then raise exception 'device proof invalid' using errcode='22023'; end if;
 select id into v_profile from site_identity.profile where user_id=p_user_id;
 update site_identity.device set linked_profile_id=null,revoked_at=now()
 where id=p_device_id and linked_profile_id=v_profile and credential_hash=decode(p_credential_hash_hex,'hex');
 return found;
end; $$;

create or replace function public.personalization_mark_profile_deleting_v1(p_user_id uuid)
returns uuid language plpgsql security definer set search_path='' as $$
declare v_profile uuid;
begin
 update site_identity.profile set status='deleting',updated_at=now() where user_id=p_user_id returning id into v_profile;
 if v_profile is null then return null; end if;
 insert into site_identity.purge_request(user_id,profile_id) values(p_user_id,v_profile);
 update saved_events.reminder_subscription set state='revoked',revoked_at=now(),updated_at=now() where user_id=p_user_id and state in ('active','paused');
 return v_profile;
end; $$;

-- Service lifecycle sync from canonical Fly event facts. Browser cannot call this.
create or replace function public.personalization_apply_occurrence_lifecycle_v1(
 p_event_id bigint,p_occurrence_key text,p_lifecycle_status text,p_occurrence_starts_at timestamptz
) returns integer language plpgsql security definer set search_path='' as $$
declare
 v_changed integer;
 v_changed_ids uuid[];
 v_active_ids uuid[];
 v_reset_ids uuid[]:='{}'::uuid[];
 v_row record;
 v_kind text;
 v_key text;
 v_payload jsonb;
 v_hash text;
 v_outbox uuid;
begin
 if p_event_id<=0 or length(trim(coalesce(p_occurrence_key,''))) not between 1 and 160
    or p_lifecycle_status not in ('upcoming','rescheduled','cancelled','completed')
    or (p_lifecycle_status='rescheduled' and p_occurrence_starts_at is null) then
  raise exception 'invalid lifecycle' using errcode='22023';
 end if;
 with changed as (
  update saved_events.saved_occurrence s set
   lifecycle_status=p_lifecycle_status,
   occurrence_starts_at=coalesce(p_occurrence_starts_at,s.occurrence_starts_at),
   occurrence_validated_at=now(),
   lifecycle_revision=s.lifecycle_revision+1,
   updated_at=now()
  where s.event_id=p_event_id and s.occurrence_key=trim(p_occurrence_key)
    and s.removed_at is null
    and (
     s.lifecycle_status is distinct from p_lifecycle_status
     or (p_occurrence_starts_at is not null and s.occurrence_starts_at is distinct from p_occurrence_starts_at)
     or s.occurrence_validated_at is null
    )
  returning s.id
 ) select count(*)::integer,coalesce(array_agg(id),'{}'::uuid[])
   into v_changed,v_changed_ids from changed;
 if v_changed=0 then return 0; end if;

 select coalesce(array_agg(r.id),'{}'::uuid[]) into v_active_ids
 from saved_events.reminder_subscription r
 where r.saved_occurrence_id=any(v_changed_ids)
   and (r.state='active' or (p_lifecycle_status='rescheduled' and r.state='expired'));

 -- A pending D-1 contains the old occurrence time. Cancel it before a terminal
 -- transition, or replace it with the new revision after a reschedule. Claims
 -- that have recorded network start are never blindly retried or rewritten.
 if p_lifecycle_status in ('rescheduled','cancelled','completed') then
  with candidates as (
   select o.id,d.reminder_subscription_id
   from email_control.email_outbox o
   join saved_events.reminder_delivery d on d.outbox_id=o.id
   where d.reminder_subscription_id=any(v_active_ids)
     and d.kind='event_reminder_24h'
     and (o.status in ('ready','retryable') or (o.status='claimed' and o.network_started_at is null))
  ), skipped as (
   update email_control.email_outbox o set
    status='skipped',last_error_class='occurrence_'||p_lifecycle_status,
    lease_owner=null,lease_token=null,lease_expires_at=null,updated_at=now()
   from candidates c where o.id=c.id returning c.reminder_subscription_id
  ) select coalesce(array_agg(distinct reminder_subscription_id),'{}'::uuid[])
    into v_reset_ids from skipped;
 end if;

 update saved_events.reminder_subscription r set
 state=case
 when p_lifecycle_status='cancelled' then 'cancelled'
 when p_lifecycle_status='completed' then 'completed'
  when p_lifecycle_status='rescheduled'
       and (r.reminder_sent_at is null or r.id=any(v_reset_ids))
       and saved_events.schedule_d1_v1(p_occurrence_starts_at,now()) is null then 'expired'
  when p_lifecycle_status='rescheduled' and r.state='expired' then 'active'
  else r.state end,
 schedule_revision=case when p_lifecycle_status='rescheduled' then r.schedule_revision+1 else r.schedule_revision end,
 scheduled_for=case
  when p_lifecycle_status in ('cancelled','completed') then null
  when p_lifecycle_status='rescheduled' and (r.reminder_sent_at is null or r.id=any(v_reset_ids))
   then saved_events.schedule_d1_v1(p_occurrence_starts_at,now())
  else r.scheduled_for end,
 reminder_sent_at=case when p_lifecycle_status='rescheduled' and r.id=any(v_reset_ids) then null else r.reminder_sent_at end,
 updated_at=now()
 where r.id=any(v_active_ids);

 if p_lifecycle_status in ('rescheduled','cancelled') then
  v_kind:=case when p_lifecycle_status='cancelled' then 'event_cancelled' else 'event_rescheduled' end;
  for v_row in
   select r.id reminder_id,r.user_id,r.schedule_revision,s.id saved_id,s.lifecycle_revision
   from saved_events.reminder_subscription r join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id
   where r.id=any(v_active_ids) and s.id=any(v_changed_ids)
  loop
   v_key:='lifecycle:'||v_row.saved_id::text||':'||v_kind||':'||v_row.lifecycle_revision::text;
   v_payload:=jsonb_build_object(
    'subject',case when v_kind='event_cancelled' then 'Сохранённое событие отменено' else 'Сохранённое событие изменилось' end,
    'text',format('Статус события №%s изменён: %s. Проверьте актуальные сведения на KenigEvents.',p_event_id,p_lifecycle_status)
   );
   v_hash:=encode(extensions.digest(v_payload::text,'sha256'),'hex');
   begin
    v_outbox:=public.email_enqueue_transactional_v1(v_row.user_id,v_kind,p_event_id,v_key,'transactional-plain-v1',v_payload,v_hash,true);
    insert into saved_events.reminder_delivery(reminder_subscription_id,kind,schedule_revision,outbox_id,idempotency_key)
    values(v_row.reminder_id,v_kind,v_row.lifecycle_revision,v_outbox,v_key) on conflict do nothing;
   exception when sqlstate '28000' then null;
   end;
  end loop;
 end if;
 return v_changed;
end; $$;

-- Enqueues each due reminder once. Existing Postbox claim path rechecks verified identity,
-- transactional consent, suppressions, runtime switches, bounces and unsubscribes.
create or replace function public.personalization_enqueue_due_reminders_v1(p_limit integer default 100,p_dry_run boolean default true)
returns integer language plpgsql security definer set search_path='' as $$
declare v_row record; v_outbox uuid; v_count integer:=0; v_key text; v_payload jsonb; v_hash text;
begin
 if p_limit not between 1 and 500 then raise exception 'invalid limit' using errcode='22023'; end if;
 update saved_events.reminder_subscription r set
  state='expired',scheduled_for=null,updated_at=now()
 where r.state='active' and r.reminder_sent_at is null
   and r.scheduled_for<now()-interval '6 hours';
 for v_row in
  select r.id reminder_id,r.user_id,r.schedule_revision,s.id saved_id,s.event_id,s.occurrence_key,s.occurrence_starts_at
  from saved_events.reminder_subscription r join saved_events.saved_occurrence s on s.id=r.saved_occurrence_id
  where r.state='active' and r.reminder_sent_at is null and r.scheduled_for<=now() and s.removed_at is null and s.lifecycle_status in ('upcoming','rescheduled')
  order by r.scheduled_for for update of r skip locked limit p_limit
 loop
  v_key:='d1:'||v_row.saved_id::text||':'||v_row.schedule_revision::text;
  v_payload:=jsonb_build_object(
    'subject','Напоминание о сохранённом событии',
    'text',format('Событие №%s, сохранённое вами, начнётся %s. Проверьте актуальные дату, время и место на KenigEvents.',v_row.event_id,coalesce(v_row.occurrence_starts_at::text,'в указанную дату'))
  );
  v_hash:=encode(extensions.digest(v_payload::text,'sha256'),'hex');
  begin
   v_outbox:=public.email_enqueue_transactional_v1(v_row.user_id,'event_reminder_24h',v_row.event_id,v_key,'transactional-plain-v1',v_payload,v_hash,p_dry_run);
   insert into saved_events.reminder_delivery(reminder_subscription_id,kind,schedule_revision,outbox_id,idempotency_key)
   values(v_row.reminder_id,'event_reminder_24h',v_row.schedule_revision,v_outbox,v_key) on conflict do nothing;
   if found then update saved_events.reminder_subscription set reminder_sent_at=now(),updated_at=now() where id=v_row.reminder_id; v_count:=v_count+1; end if;
  exception when sqlstate '28000' then
   -- Missing/revoked consent and suppression evidence are owned by email_control.
   -- One ineligible recipient must not block the rest of the bounded batch.
   update saved_events.reminder_subscription set state='paused',scheduled_for=null,updated_at=now()
    where id=v_row.reminder_id;
  end;
 end loop;
 return v_count;
end; $$;

create or replace function public.personalization_retention_cleanup_v1(p_now timestamptz default now())
returns jsonb language plpgsql security definer set search_path='' as $$
declare v_devices integer; v_saves integer; v_signals integer; v_delivery integer; v_audit integer; v_purge integer;
begin
 delete from site_identity.device where expires_at<p_now; get diagnostics v_devices=row_count;
 delete from saved_events.saved_occurrence where removed_at<p_now-interval '30 days'; get diagnostics v_saves=row_count;
 delete from saved_events.event_signal where not active and updated_at<p_now-interval '30 days'; get diagnostics v_signals=row_count;
 delete from saved_events.reminder_delivery where created_at<p_now-interval '400 days'; get diagnostics v_delivery=row_count;
 delete from site_identity.merge_audit where occurred_at<p_now-interval '400 days'; get diagnostics v_audit=row_count;
 delete from site_identity.purge_request where status='complete' and completed_at<p_now-interval '90 days'; get diagnostics v_purge=row_count;
 return jsonb_build_object('devices',v_devices,'removed_saves',v_saves,'inactive_signals',v_signals,'reminder_delivery',v_delivery,'merge_audit',v_audit,'purge_requests',v_purge);
end; $$;

revoke all on function public.personalization_save_occurrence_v1(bigint,text,timestamptz,boolean) from public,anon,authenticated;
revoke all on function public.personalization_saved_count_v1() from public,anon,authenticated;
revoke all on function public.personalization_set_event_signal_v1(bigint,text,text,boolean) from public,anon,authenticated;
revoke all on function public.personalization_set_reminder_v1(bigint,text,boolean,text,uuid) from public,anon,authenticated;
grant execute on function public.personalization_save_occurrence_v1(bigint,text,timestamptz,boolean) to authenticated;
grant execute on function public.personalization_saved_count_v1() to authenticated;
grant execute on function public.personalization_set_event_signal_v1(bigint,text,text,boolean) to authenticated;
grant execute on function public.personalization_set_reminder_v1(bigint,text,boolean,text,uuid) to authenticated;

revoke all on function public.personalization_materialize_device_v1(uuid,text,text,jsonb) from public,anon,authenticated;
revoke all on function public.personalization_merge_device_v1(uuid,uuid,text,text,uuid) from public,anon,authenticated;
revoke all on function public.personalization_unlink_device_v1(uuid,uuid,text) from public,anon,authenticated;
revoke all on function public.personalization_mark_profile_deleting_v1(uuid) from public,anon,authenticated;
revoke all on function public.personalization_apply_occurrence_lifecycle_v1(bigint,text,text,timestamptz) from public,anon,authenticated;
revoke all on function public.personalization_enqueue_due_reminders_v1(integer,boolean) from public,anon,authenticated;
revoke all on function public.personalization_retention_cleanup_v1(timestamptz) from public,anon,authenticated;
grant execute on function public.personalization_materialize_device_v1(uuid,text,text,jsonb) to service_role;
grant execute on function public.personalization_merge_device_v1(uuid,uuid,text,text,uuid) to service_role;
grant execute on function public.personalization_unlink_device_v1(uuid,uuid,text) to service_role;
grant execute on function public.personalization_mark_profile_deleting_v1(uuid) to service_role;
grant execute on function public.personalization_apply_occurrence_lifecycle_v1(bigint,text,text,timestamptz) to service_role;
grant execute on function public.personalization_enqueue_due_reminders_v1(integer,boolean) to service_role;
grant execute on function public.personalization_retention_cleanup_v1(timestamptz) to service_role;
