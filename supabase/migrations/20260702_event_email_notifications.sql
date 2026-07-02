-- Event email notifications for authenticated static-site users.
-- Target: personalization Supabase/Postgres project.

create extension if not exists pgcrypto with schema extensions;

create table if not exists public.user_notification_profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  notification_email text,
  notification_email_hash text,
  email_verified boolean not null default false,
  email_source text not null default 'manual' check (email_source in ('yandex_oauth', 'manual')),
  notification_unsubscribed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  constraint user_notification_profiles_email_size_chk check (notification_email is null or length(notification_email) <= 320),
  constraint user_notification_profiles_metadata_size_chk check (length(metadata::text) <= 4096)
);

create table if not exists public.event_follows (
  id uuid primary key default extensions.gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  event_id bigint not null,
  notification_email text not null,
  notification_email_hash text not null,
  notification_consent_at timestamptz not null,
  calendar_added_at timestamptz not null default now(),
  unsubscribed_at timestamptz,
  event_url text not null,
  event_title text not null,
  starts_at timestamptz,
  start_date date,
  display_time text,
  venue_name text,
  location_address text,
  city text,
  source_url text,
  ticket_link text,
  lifecycle_status text not null default 'active',
  source_snapshot jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, event_id),
  constraint event_follows_email_size_chk check (length(notification_email) <= 320),
  constraint event_follows_title_size_chk check (length(event_title) <= 500),
  constraint event_follows_snapshot_size_chk check (length(source_snapshot::text) <= 20000)
);

create table if not exists public.email_outbox (
  id uuid primary key default extensions.gen_random_uuid(),
  kind text not null check (kind in ('calendar_confirmation', 'event_reminder_24h', 'event_rescheduled', 'event_cancelled')),
  event_id bigint not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  recipient_email text not null,
  recipient_email_hash text not null,
  payload_json jsonb not null,
  status text not null default 'pending' check (status in ('pending', 'sending', 'sent', 'failed', 'bounced', 'complained', 'suppressed', 'skipped')),
  attempts integer not null default 0 check (attempts >= 0),
  next_run_at timestamptz not null default now(),
  last_error text,
  provider_message_id text,
  idempotency_key text not null unique,
  dry_run boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint email_outbox_payload_size_chk check (length(payload_json::text) <= 30000)
);

create table if not exists public.email_delivery_events (
  id uuid primary key default extensions.gen_random_uuid(),
  outbox_id uuid references public.email_outbox(id) on delete set null,
  event_id bigint,
  user_id uuid,
  kind text not null,
  status text not null check (status in ('queued', 'sending', 'sent', 'failed', 'retry_scheduled', 'bounced', 'complained', 'suppressed', 'skipped', 'unsubscribed')),
  recipient_email_hash text,
  provider_message_id text,
  reason text,
  dry_run boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint email_delivery_events_metadata_size_chk check (length(metadata::text) <= 8192)
);

create table if not exists public.email_suppressions (
  id uuid primary key default extensions.gen_random_uuid(),
  recipient_email_hash text not null,
  reason text not null check (reason in ('bounce', 'complaint', 'unsubscribe', 'hard_failure')),
  event_id bigint,
  user_id uuid,
  active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (recipient_email_hash, reason)
);

create table if not exists public.email_rate_limit_ledger (
  bucket_key text primary key,
  bucket_kind text not null check (bucket_kind in ('sender_hour', 'sender_day', 'recipient_day', 'recipient_event_day', 'cancel_minute')),
  bucket_start timestamptz not null,
  count integer not null default 0 check (count >= 0),
  updated_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists event_follows_event_idx on public.event_follows(event_id) where unsubscribed_at is null;
create index if not exists event_follows_user_idx on public.event_follows(user_id, updated_at desc);
create index if not exists email_outbox_pending_idx on public.email_outbox(status, next_run_at, kind);
create index if not exists email_outbox_event_kind_idx on public.email_outbox(event_id, kind, status);
create index if not exists email_delivery_events_created_idx on public.email_delivery_events(created_at desc);
create index if not exists email_delivery_events_event_idx on public.email_delivery_events(event_id, kind, status);
create index if not exists email_suppressions_active_idx on public.email_suppressions(recipient_email_hash) where active;

alter table public.user_notification_profiles enable row level security;
alter table public.event_follows enable row level security;
alter table public.email_outbox enable row level security;
alter table public.email_delivery_events enable row level security;
alter table public.email_suppressions enable row level security;
alter table public.email_rate_limit_ledger enable row level security;

revoke all on public.user_notification_profiles from anon, authenticated;
revoke all on public.event_follows from anon, authenticated;
revoke all on public.email_outbox from anon, authenticated;
revoke all on public.email_delivery_events from anon, authenticated;
revoke all on public.email_suppressions from anon, authenticated;
revoke all on public.email_rate_limit_ledger from anon, authenticated;

grant select, insert, update on public.user_notification_profiles to authenticated;
grant select, insert, update on public.event_follows to authenticated;
grant all on public.user_notification_profiles to service_role;
grant all on public.event_follows to service_role;
grant all on public.email_outbox to service_role;
grant all on public.email_delivery_events to service_role;
grant all on public.email_suppressions to service_role;
grant all on public.email_rate_limit_ledger to service_role;

drop policy if exists user_notification_profiles_own_select on public.user_notification_profiles;
create policy user_notification_profiles_own_select on public.user_notification_profiles for select to authenticated using ((select auth.uid()) = user_id);
drop policy if exists user_notification_profiles_own_insert on public.user_notification_profiles;
create policy user_notification_profiles_own_insert on public.user_notification_profiles for insert to authenticated with check ((select auth.uid()) = user_id);
drop policy if exists user_notification_profiles_own_update on public.user_notification_profiles;
create policy user_notification_profiles_own_update on public.user_notification_profiles for update to authenticated using ((select auth.uid()) = user_id) with check ((select auth.uid()) = user_id);

drop policy if exists event_follows_own_select on public.event_follows;
create policy event_follows_own_select on public.event_follows for select to authenticated using ((select auth.uid()) = user_id);
drop policy if exists event_follows_own_insert on public.event_follows;
create policy event_follows_own_insert on public.event_follows for insert to authenticated with check ((select auth.uid()) = user_id);
drop policy if exists event_follows_own_update on public.event_follows;
create policy event_follows_own_update on public.event_follows for update to authenticated using ((select auth.uid()) = user_id) with check ((select auth.uid()) = user_id);

drop policy if exists email_outbox_service_all on public.email_outbox;
create policy email_outbox_service_all on public.email_outbox for all to service_role using (true) with check (true);
drop policy if exists email_delivery_events_service_all on public.email_delivery_events;
create policy email_delivery_events_service_all on public.email_delivery_events for all to service_role using (true) with check (true);
drop policy if exists email_suppressions_service_all on public.email_suppressions;
create policy email_suppressions_service_all on public.email_suppressions for all to service_role using (true) with check (true);
drop policy if exists email_rate_limit_ledger_service_all on public.email_rate_limit_ledger;
create policy email_rate_limit_ledger_service_all on public.email_rate_limit_ledger for all to service_role using (true) with check (true);
