-- Broaden the prelaunch consent from one launch message to launch information,
-- important service updates and useful editorial selections. Existing v1 rows
-- remain valid; every new browser registration uses the v2 consent contract.

create or replace function personalization.prelaunch_email_is_valid_v2(p_email text)
returns boolean
language plpgsql
immutable
strict
set search_path = ''
as $$
declare
  v_email text := p_email;
  v_local text;
  v_domain text;
  v_labels text[];
  v_label text;
  v_tld text;
begin
  if char_length(v_email) not between 5 and 254
     or v_email <> lower(btrim(v_email))
     or v_email ~ '[[:space:][:cntrl:]]'
     or position('<' in v_email) > 0
     or position('>' in v_email) > 0
     or position('"' in v_email) > 0
     or position(chr(92) in v_email) > 0
     or position('(' in v_email) > 0
     or position(')' in v_email) > 0
     or position('[' in v_email) > 0
     or position(']' in v_email) > 0
     or position('{' in v_email) > 0
     or position('}' in v_email) > 0
     or position(',' in v_email) > 0
     or position(';' in v_email) > 0
     or position(':' in v_email) > 0
     or char_length(v_email) - char_length(replace(v_email, '@', '')) <> 1 then
    return false;
  end if;

  v_local := split_part(v_email, '@', 1);
  v_domain := split_part(v_email, '@', 2);
  if char_length(v_local) not between 1 and 64
     or v_local !~ '^[a-z0-9.!#$%&''*+/=?^_`{|}~-]+$'
     or left(v_local, 1) = '.'
     or right(v_local, 1) = '.'
     or position('..' in v_local) > 0
     or char_length(v_domain) not between 3 and 253 then
    return false;
  end if;

  v_labels := string_to_array(v_domain, '.');
  if cardinality(v_labels) < 2 then
    return false;
  end if;

  foreach v_label in array v_labels loop
    if char_length(v_label) not between 1 and 63
       or v_label !~ '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$' then
      return false;
    end if;
  end loop;

  v_tld := v_labels[cardinality(v_labels)];
  return v_tld ~ '^[a-z]{2,63}$'
    or v_tld ~ '^xn--[a-z0-9-]{2,59}$';
end;
$$;

revoke execute on function personalization.prelaunch_email_is_valid_v2(text)
  from public, anon, authenticated;
grant execute on function personalization.prelaunch_email_is_valid_v2(text)
  to service_role;

alter table personalization.prelaunch_launch_subscription
  drop constraint if exists prelaunch_launch_subscription_consent_version_check;
alter table personalization.prelaunch_launch_subscription
  add constraint prelaunch_launch_subscription_consent_version_check check (
    consent_version in ('launch-2026-09-01-v1', 'prelaunch-updates-2026-v1')
  );

alter table personalization.prelaunch_launch_subscription
  drop constraint if exists prelaunch_launch_subscription_email_chk;
alter table personalization.prelaunch_launch_subscription
  add constraint prelaunch_launch_subscription_email_chk check (
    personalization.prelaunch_email_is_valid_v2(email)
  );

alter table personalization.prelaunch_launch_subscription
  alter column retention_due_at
  set default (statement_timestamp() + interval '24 months');

update personalization.prelaunch_launch_subscription
   set retention_due_at = greatest(
         retention_due_at,
         last_requested_at + interval '24 months'
       ),
       updated_at = statement_timestamp()
 where delivery_status <> 'suppressed';

create or replace function public.register_prelaunch_notification_v1(
  p_email text,
  p_source text default 'prelaunch_home',
  p_consent_version text default 'prelaunch-updates-2026-v1',
  p_website text default ''
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_email text := lower(btrim(coalesce(p_email, '')));
  v_source text := left(coalesce(nullif(btrim(p_source), ''), 'prelaunch_home'), 64);
  v_today date := (statement_timestamp() at time zone 'Europe/Kaliningrad')::date;
  v_accepted_new_rows integer;
begin
  -- Honeypot traffic receives the ordinary public shape but is never stored.
  if char_length(btrim(coalesce(p_website, ''))) > 0 then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01',
      'consent_version', 'prelaunch-updates-2026-v1'
    );
  end if;

  if not personalization.prelaunch_email_is_valid_v2(v_email) then
    raise exception 'invalid_prelaunch_email' using errcode = '22023';
  end if;
  if p_consent_version is distinct from 'prelaunch-updates-2026-v1' then
    raise exception 'invalid_prelaunch_consent' using errcode = '22023';
  end if;

  -- Replays through direct Supabase and the Yandex relay converge on one row.
  update personalization.prelaunch_launch_subscription
     set source = v_source,
         consent_version = p_consent_version,
         last_requested_at = statement_timestamp(),
         request_count = least(request_count + 1, 100),
         retention_due_at = statement_timestamp() + interval '24 months',
         updated_at = statement_timestamp()
   where email = v_email;
  if found then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01',
      'consent_version', p_consent_version
    );
  end if;

  insert into personalization.prelaunch_signup_daily_guard (metric_date)
  values (v_today)
  on conflict (metric_date) do nothing;

  select accepted_new_rows
    into v_accepted_new_rows
    from personalization.prelaunch_signup_daily_guard
   where metric_date = v_today
   for update;

  update personalization.prelaunch_launch_subscription
     set source = v_source,
         consent_version = p_consent_version,
         last_requested_at = statement_timestamp(),
         request_count = least(request_count + 1, 100),
         retention_due_at = statement_timestamp() + interval '24 months',
         updated_at = statement_timestamp()
   where email = v_email;
  if found then
    return jsonb_build_object(
      'accepted', true,
      'status', 'registered',
      'launch_date', '2026-09-01',
      'consent_version', p_consent_version
    );
  end if;

  if v_accepted_new_rows >= 5000 then
    return jsonb_build_object(
      'accepted', false,
      'status', 'daily_capacity_reached',
      'launch_date', '2026-09-01',
      'consent_version', p_consent_version
    );
  end if;

  insert into personalization.prelaunch_launch_subscription (
    email,
    source,
    consent_version,
    retention_due_at
  ) values (
    v_email,
    v_source,
    p_consent_version,
    statement_timestamp() + interval '24 months'
  );

  update personalization.prelaunch_signup_daily_guard
     set accepted_new_rows = accepted_new_rows + 1,
         updated_at = statement_timestamp()
   where metric_date = v_today;

  return jsonb_build_object(
    'accepted', true,
    'status', 'registered',
    'launch_date', '2026-09-01',
    'consent_version', p_consent_version
  );
end;
$$;

revoke execute on function public.register_prelaunch_notification_v1(text, text, text, text)
  from public, anon, authenticated;
grant execute on function public.register_prelaunch_notification_v1(text, text, text, text)
  to anon, authenticated, service_role;

comment on table personalization.prelaunch_launch_subscription is
  'Normalized email subscriptions for launch information, important service updates and useful editorial selections; no browsing metadata.';
comment on function public.register_prelaunch_notification_v1(text, text, text, text) is
  'Idempotent public subscription RPC. Direct and Yandex-relay replays converge on one normalized email row.';
