-- Keep the presentation cohort complete without classifying future public Auth
-- users as testers. The cutoff is fixed to the end of the presentation window.
-- Consent is never inferred by this backfill.

insert into personalization.focus_group_participant_contact (
  user_id,
  email,
  auth_provider,
  joined_at,
  email_confirmed_at,
  last_confirmed_at,
  source_route
)
select u.id,
       lower(trim(u.email)),
       case
         when exists (
           select 1
             from auth.identities yi
            where yi.user_id = u.id
              and yi.provider = 'custom:yandex'
         ) then 'custom:yandex'
         else 'email'
       end,
       coalesce(u.created_at, u.email_confirmed_at),
       u.email_confirmed_at,
       greatest(u.email_confirmed_at, coalesce(u.last_sign_in_at, u.email_confirmed_at)),
       '/focus-presentation-auth-backfill/'
  from auth.users u
 where u.email is not null
   and u.email_confirmed_at is not null
   and u.created_at < timestamptz '2026-08-01 00:00:00+00'
   and exists (
     select 1
       from auth.identities i
      where i.user_id = u.id
        and i.provider in ('email', 'custom:yandex')
   )
on conflict on constraint focus_group_participant_contact_pkey do update set
  email = excluded.email,
  auth_provider = excluded.auth_provider,
  email_confirmed_at = excluded.email_confirmed_at,
  last_confirmed_at = greatest(
    personalization.focus_group_participant_contact.last_confirmed_at,
    excluded.last_confirmed_at
  ),
  source_route = case
    when personalization.focus_group_participant_contact.source_route in (
      '/focus-feedback-backfill/',
      '/focus-presentation-auth-backfill/'
    ) then excluded.source_route
    else personalization.focus_group_participant_contact.source_route
  end,
  updated_at = now();

create or replace function public.register_focus_group_participant_v1(
  p_communication_opt_in boolean default false,
  p_source_route text default '/fokus-gruppa/priglashenie/'
)
returns table (
  user_id uuid,
  email text,
  auth_provider text,
  communication_opt_in boolean,
  joined_at timestamptz
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_email text;
  v_email_confirmed_at timestamptz;
  v_provider text;
  v_source_route text := left(coalesce(nullif(pg_catalog.btrim(p_source_route), ''), '/fokus-gruppa/priglashenie/'), 240);
  v_existing boolean;
  v_active_count integer;
begin
  if v_user_id is null then
    raise exception 'authentication required' using errcode = '28000';
  end if;

  select lower(pg_catalog.btrim(u.email)), u.email_confirmed_at
    into v_email, v_email_confirmed_at
    from auth.users u
   where u.id = v_user_id;

  if v_email is null or v_email_confirmed_at is null then
    raise exception 'verified email required' using errcode = '28000';
  end if;

  select case
           when pg_catalog.bool_or(i.provider = 'custom:yandex') then 'custom:yandex'
           else 'email'
         end
    into v_provider
    from auth.identities i
   where i.user_id = v_user_id
     and i.provider in ('email', 'custom:yandex');

  if v_provider is null then
    raise exception 'supported verified identity required' using errcode = '28000';
  end if;

  -- Serialize only the small focus-programme admission decision. Existing
  -- participants can always refresh their contact/consent without consuming a
  -- second place.
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('static-site-focus-group-2026:admission', 0)
  );
  select exists (
    select 1
      from personalization.focus_group_participant_contact c
     where c.user_id = v_user_id
  ) into v_existing;
  if not v_existing then
    select count(*)::integer
      into v_active_count
      from personalization.focus_group_participant_contact c
     where c.program_id = 'static-site-focus-group-2026'
       and c.status = 'active';
    if v_active_count >= 200 then
      raise exception 'focus group is full' using errcode = '54000';
    end if;
  end if;

  insert into personalization.focus_group_participant_contact (
    user_id,
    email,
    auth_provider,
    email_confirmed_at,
    last_confirmed_at,
    focus_updates_consent,
    product_updates_consent,
    friends_club_consent,
    consent_version,
    consent_updated_at,
    source_route,
    updated_at
  ) values (
    v_user_id,
    v_email,
    v_provider,
    v_email_confirmed_at,
    pg_catalog.now(),
    p_communication_opt_in,
    p_communication_opt_in,
    p_communication_opt_in,
    case when p_communication_opt_in then 'focus-contact-v1' else null end,
    case when p_communication_opt_in then pg_catalog.now() else null end,
    v_source_route,
    pg_catalog.now()
  )
  on conflict on constraint focus_group_participant_contact_pkey do update set
    email = excluded.email,
    auth_provider = excluded.auth_provider,
    status = 'active',
    email_confirmed_at = excluded.email_confirmed_at,
    last_confirmed_at = excluded.last_confirmed_at,
    focus_updates_consent = excluded.focus_updates_consent,
    product_updates_consent = excluded.product_updates_consent,
    friends_club_consent = excluded.friends_club_consent,
    consent_version = excluded.consent_version,
    consent_updated_at = excluded.consent_updated_at,
    source_route = excluded.source_route,
    updated_at = pg_catalog.now();

  return query
  select c.user_id,
         c.email,
         c.auth_provider,
         (
           c.focus_updates_consent
           and c.product_updates_consent
           and c.friends_club_consent
         ),
         c.joined_at
    from personalization.focus_group_participant_contact c
   where c.user_id = v_user_id;
end;
$$;

revoke all on function public.register_focus_group_participant_v1(boolean, text)
  from public, anon, authenticated;
grant execute on function public.register_focus_group_participant_v1(boolean, text)
  to authenticated, service_role;

notify pgrst, 'reload schema';
