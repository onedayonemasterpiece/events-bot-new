-- Durable focus-group participant contacts.
--
-- Authentication remains owned by Supabase Auth. This projection records only
-- people who complete the focus-group identity step, so operators can contact
-- the cohort without treating every historical Auth smoke user as a participant.

create table personalization.focus_group_participant_contact (
  user_id uuid primary key references auth.users (id) on delete cascade,
  program_id text not null default 'static-site-focus-group-2026',
  email text not null,
  auth_provider text not null,
  status text not null default 'active',
  joined_at timestamptz not null default now(),
  email_confirmed_at timestamptz not null,
  last_confirmed_at timestamptz not null default now(),
  focus_updates_consent boolean not null default false,
  product_updates_consent boolean not null default false,
  friends_club_consent boolean not null default false,
  consent_version text,
  consent_updated_at timestamptz,
  source_route text not null default '/fokus-gruppa/priglashenie/',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint focus_group_participant_program_chk check (
    program_id = 'static-site-focus-group-2026'
  ),
  constraint focus_group_participant_email_chk check (
    email = lower(trim(email)) and length(email) between 3 and 320 and position('@' in email) > 1
  ),
  constraint focus_group_participant_provider_chk check (
    auth_provider in ('email', 'custom:yandex')
  ),
  constraint focus_group_participant_status_chk check (
    status in ('active', 'completed', 'left')
  ),
  constraint focus_group_participant_source_chk check (
    length(source_route) between 1 and 240
  ),
  constraint focus_group_participant_consent_chk check (
    (
      focus_updates_consent = false
      and product_updates_consent = false
      and friends_club_consent = false
      and consent_version is null
      and consent_updated_at is null
    )
    or (
      focus_updates_consent = true
      and product_updates_consent = true
      and friends_club_consent = true
      and consent_version = 'focus-contact-v1'
      and consent_updated_at is not null
    )
  )
);

comment on table personalization.focus_group_participant_contact is
  'Private operator projection of verified focus-group contacts and explicit communication choices.';

create unique index focus_group_participant_email_uidx
  on personalization.focus_group_participant_contact (email);

create index focus_group_participant_mailing_idx
  on personalization.focus_group_participant_contact (
    status,
    focus_updates_consent,
    product_updates_consent,
    friends_club_consent,
    joined_at desc
  );

alter table personalization.focus_group_participant_contact enable row level security;
revoke all on personalization.focus_group_participant_contact from public, anon, authenticated;
grant select, insert, update, delete on personalization.focus_group_participant_contact to service_role;

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
  v_user_id uuid := auth.uid();
  v_email text;
  v_email_confirmed_at timestamptz;
  v_provider text;
  v_source_route text := left(coalesce(nullif(trim(p_source_route), ''), '/fokus-gruppa/priglashenie/'), 240);
begin
  if v_user_id is null then
    raise exception 'authentication required' using errcode = '28000';
  end if;

  select lower(trim(u.email)), u.email_confirmed_at
    into v_email, v_email_confirmed_at
    from auth.users u
   where u.id = v_user_id;

  if v_email is null or v_email_confirmed_at is null then
    raise exception 'verified email required' using errcode = '28000';
  end if;

  select case
           when bool_or(i.provider = 'custom:yandex') then 'custom:yandex'
           else 'email'
         end
    into v_provider
    from auth.identities i
   where i.user_id = v_user_id
     and i.provider in ('email', 'custom:yandex');

  if v_provider is null then
    raise exception 'supported verified identity required' using errcode = '28000';
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
    now(),
    p_communication_opt_in,
    p_communication_opt_in,
    p_communication_opt_in,
    case when p_communication_opt_in then 'focus-contact-v1' else null end,
    case when p_communication_opt_in then now() else null end,
    v_source_route,
    now()
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
    updated_at = now();

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

revoke all on function public.register_focus_group_participant_v1(boolean, text) from public;
revoke all on function public.register_focus_group_participant_v1(boolean, text) from anon;
grant execute on function public.register_focus_group_participant_v1(boolean, text) to authenticated;
grant execute on function public.register_focus_group_participant_v1(boolean, text) to service_role;

-- Recover only identities with durable focus feedback evidence. Do not infer
-- participation from historical Auth smoke users.
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
           select 1 from auth.identities yi
            where yi.user_id = u.id and yi.provider = 'custom:yandex'
         ) then 'custom:yandex'
         else 'email'
       end,
       min(f.created_at),
       u.email_confirmed_at,
       greatest(u.email_confirmed_at, max(f.created_at)),
       '/focus-feedback-backfill/'
  from auth.users u
  join personalization.focus_group_feedback f on f.user_id = u.id
 where u.email is not null
   and u.email_confirmed_at is not null
 group by u.id
on conflict on constraint focus_group_participant_contact_pkey do nothing;
