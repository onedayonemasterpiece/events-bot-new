-- Public launch-notification signup endpoint.
--
-- The underlying email ledger is closed to browser roles. Anonymous clients
-- can only call the narrow RPC at the end of this migration.

create table public.site_launch_subscriptions (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  source text not null default 'tile-mosaic-launch',
  page_path text not null default '/lab/launch/tile-mosaic/',
  locale text not null default 'ru',
  status text not null default 'subscribed',
  submission_count integer not null default 1,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  last_seen_at timestamptz not null default statement_timestamp(),
  constraint site_launch_subscriptions_email_key unique (email),
  constraint site_launch_subscriptions_email_chk check (
    email = lower(btrim(email))
    and char_length(email) between 3 and 254
    and email ~ '^[^[:space:]@]+@[^[:space:]@.]+(\.[^[:space:]@.]+)+$'
  ),
  constraint site_launch_subscriptions_source_chk check (
    source = lower(btrim(source))
    and char_length(source) between 1 and 64
    and source ~ '^[a-z0-9][a-z0-9._:-]{0,63}$'
  ),
  constraint site_launch_subscriptions_page_path_chk check (
    page_path = btrim(page_path)
    and char_length(page_path) between 1 and 500
    and page_path like '/%'
    and page_path !~ '[[:cntrl:]]'
  ),
  constraint site_launch_subscriptions_locale_chk check (
    locale = lower(btrim(locale))
    and char_length(locale) between 2 and 16
    and locale ~ '^[a-z]{2,3}(-[a-z0-9]{2,8})?$'
  ),
  constraint site_launch_subscriptions_status_chk check (
    status in ('subscribed', 'unsubscribed')
  ),
  constraint site_launch_subscriptions_submission_count_chk check (
    submission_count between 1 and 2147483647
  ),
  constraint site_launch_subscriptions_timestamps_chk check (
    updated_at >= created_at
    and last_seen_at >= created_at
  )
);

comment on table public.site_launch_subscriptions is
  'Private launch-notification email ledger for the static site; browser access is RPC-only.';

comment on column public.site_launch_subscriptions.submission_count is
  'Number of accepted submissions for the normalized email, saturated at the integer limit.';

alter table public.site_launch_subscriptions enable row level security;
revoke all on table public.site_launch_subscriptions from public, anon, authenticated;
grant select, insert, update, delete on table public.site_launch_subscriptions to service_role;

create or replace function public.subscribe_site_launch_v1(
  p_email text,
  p_source text default 'tile-mosaic-launch',
  p_page_path text default '/lab/launch/tile-mosaic/',
  p_locale text default 'ru'
)
returns table (
  accepted boolean,
  status text
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_email text := pg_catalog.lower(pg_catalog.btrim(p_email));
  v_source text := pg_catalog.lower(pg_catalog.btrim(p_source));
  v_page_path text := pg_catalog.btrim(p_page_path);
  v_locale text := pg_catalog.lower(pg_catalog.btrim(p_locale));
  v_now timestamptz := pg_catalog.statement_timestamp();
begin
  if p_email is null
     or pg_catalog.char_length(v_email) not between 3 and 254
     or v_email !~ '^[^[:space:]@]+@[^[:space:]@.]+(\.[^[:space:]@.]+)+$' then
    raise exception 'invalid_email' using errcode = '22023';
  end if;

  if p_source is null
     or pg_catalog.char_length(v_source) not between 1 and 64
     or v_source !~ '^[a-z0-9][a-z0-9._:-]{0,63}$' then
    raise exception 'invalid_source' using errcode = '22023';
  end if;

  if p_page_path is null
     or pg_catalog.char_length(v_page_path) not between 1 and 500
     or v_page_path not like '/%'
     or v_page_path ~ '[[:cntrl:]]' then
    raise exception 'invalid_page_path' using errcode = '22023';
  end if;

  if p_locale is null
     or pg_catalog.char_length(v_locale) not between 2 and 16
     or v_locale !~ '^[a-z]{2,3}(-[a-z0-9]{2,8})?$' then
    raise exception 'invalid_locale' using errcode = '22023';
  end if;

  insert into public.site_launch_subscriptions (
    email,
    source,
    page_path,
    locale,
    status,
    submission_count,
    created_at,
    updated_at,
    last_seen_at
  )
  values (
    v_email,
    v_source,
    v_page_path,
    v_locale,
    'subscribed',
    1,
    v_now,
    v_now,
    v_now
  )
  on conflict on constraint site_launch_subscriptions_email_key do update
  set
    source = excluded.source,
    page_path = excluded.page_path,
    locale = excluded.locale,
    status = 'subscribed',
    submission_count = case
      when public.site_launch_subscriptions.submission_count = 2147483647
        then 2147483647
      else public.site_launch_subscriptions.submission_count + 1
    end,
    updated_at = excluded.updated_at,
    last_seen_at = excluded.last_seen_at;

  -- Keep the anonymous response constant so callers cannot use the endpoint
  -- to learn whether an email was already present or how often it was used.
  return query select true, 'subscribed'::text;
end;
$$;

revoke all on function public.subscribe_site_launch_v1(text, text, text, text)
  from public, anon, authenticated, service_role;
grant execute on function public.subscribe_site_launch_v1(text, text, text, text)
  to anon;

notify pgrst, 'reload schema';
