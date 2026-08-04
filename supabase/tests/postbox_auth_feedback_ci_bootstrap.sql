-- Minimal Supabase-compatible boundary for the Postbox SQL contract in CI.
-- Production migrations remain authoritative; this file creates only the
-- schemas, roles and auth surface that those migrations expect.

\set ON_ERROR_STOP on

create role anon nologin;
create role authenticated nologin;
create role service_role nologin bypassrls;

create schema extensions;
create schema auth;
create schema personalization;

create table auth.users (
  id uuid primary key,
  email text,
  email_confirmed_at timestamptz
);

create or replace function auth.uid()
returns uuid
language sql
stable
as $$
  select null::uuid
$$;
