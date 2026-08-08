-- Make the public receipt distinguish a new registration from a repeat, while
-- retaining the existing normalized-email contract and closed table boundary.
-- Production verification before this migration confirmed the v1 UNIQUE(email)
-- constraint. ON CONFLICT below infers that existing contract; do not create a
-- redundant second unique index.

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
  v_inserted boolean := false;
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

  -- An ordinary repeat is accepted, counted and reported to the browser as a
  -- repeat rather than being indistinguishable from a first registration.
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
      'status', 'already_registered',
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

  -- Re-check after waiting for the guard. A same-email request that committed
  -- while this transaction waited remains a successful repeat even when the
  -- daily new-row capacity has just been reached.
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
      'status', 'already_registered',
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

  -- Do not rely on the daily guard as the deduplication primitive. The final
  -- insert is independently protected by the normalized-email unique index,
  -- so a competing writer outside this lock still cannot create a duplicate.
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
  )
  on conflict (email) do nothing
  returning true into v_inserted;

  if not coalesce(v_inserted, false) then
    -- A concurrent unique-key winner is a successful repeat. Update the same
    -- counters as the fast repeat path after the conflicting row is visible.
    update personalization.prelaunch_launch_subscription
       set source = v_source,
           consent_version = p_consent_version,
           last_requested_at = statement_timestamp(),
           request_count = least(request_count + 1, 100),
           retention_due_at = statement_timestamp() + interval '24 months',
           updated_at = statement_timestamp()
     where email = v_email;
    if not found then
      raise exception 'prelaunch_registration_conflict_without_row';
    end if;

    return jsonb_build_object(
      'accepted', true,
      'status', 'already_registered',
      'launch_date', '2026-09-01',
      'consent_version', p_consent_version
    );
  end if;

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

comment on function public.register_prelaunch_notification_v1(text, text, text, text) is
  'Race-safe idempotent public subscription RPC: registered is new, already_registered is a repeat; direct table access remains closed.';
