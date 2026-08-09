-- Bind Search health session claims to a closed platform vocabulary and return
-- typed admission outcomes. No plaintext email, token, OTP, action link,
-- cookie, or serialized session is persisted.
alter table public.static_site_auth_session_issue_claim
  add column if not exists platform text;

-- A recoverable credential is kept only as application-encrypted ciphertext
-- for the short lost-response window. It is atomically returned once and then
-- erased. No plaintext OTP/action link/session is stored in Postgres.
alter table public.static_site_auth_session_issue_claim
  add column if not exists credential_ciphertext text,
  add column if not exists credential_expires_at timestamptz;

update public.static_site_auth_session_issue_claim
set platform = case
  when persona_id = 'search-cached-android' then 'android'
  when persona_id = 'search-cached-ios' then 'ios'
  else 'browser'
end
where platform is null;

alter table public.static_site_auth_session_issue_claim
  alter column platform set not null;

alter table public.static_site_auth_session_issue_claim
  add constraint static_site_auth_session_claim_platform_chk
  check (platform in ('browser', 'android', 'ios'));

create or replace function public.claim_static_site_auth_session_issue_v2(
  p_run_id text,
  p_run_attempt integer,
  p_platform text,
  p_persona_id text,
  p_repository text,
  p_workflow_ref text,
  p_limit integer default 1
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_count integer;
  v_limit integer := least(greatest(coalesce(p_limit, 1), 1), 3);
  v_ciphertext text;
  v_credential_expires_at timestamptz;
begin
  if p_run_id is null or length(p_run_id) not between 1 and 100
    or p_run_attempt is null or p_run_attempt not between 1 and 1000
    or p_platform is null or p_platform not in ('browser', 'android', 'ios')
    or p_persona_id is null or length(p_persona_id) not between 1 and 80
    or p_repository is null or length(p_repository) not between 3 and 160
    or p_workflow_ref is null or length(p_workflow_ref) not between 3 and 240 then
    raise exception 'invalid_static_site_auth_session_issue_claim' using errcode = '22023';
  end if;

  -- A dedicated platform persona is the collision boundary.
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(p_persona_id, 1904831207)
  );

  update public.static_site_auth_session_issue_claim c
  set credential_ciphertext = null
  where c.credential_ciphertext is not null
    and c.credential_expires_at <= pg_catalog.now();

  select c.credential_ciphertext, c.credential_expires_at
  into v_ciphertext, v_credential_expires_at
    from public.static_site_auth_session_issue_claim c
    where c.run_id = p_run_id
      and c.run_attempt = p_run_attempt
      and c.platform = p_platform
      and c.persona_id = p_persona_id
      and c.repository = p_repository
      and c.workflow_ref = p_workflow_ref
    for update;
  if found then
    if v_ciphertext is not null
      and v_credential_expires_at > pg_catalog.now() then
      return pg_catalog.jsonb_build_object(
        'claim', 'replay', 'credential_ciphertext', v_ciphertext
      );
    end if;
    if v_credential_expires_at is not null then
      return pg_catalog.jsonb_build_object('claim', 'duplicate_consumed');
    end if;
    return pg_catalog.jsonb_build_object('claim', 'duplicate_inflight');
  end if;

  delete from public.static_site_auth_session_issue_claim c
  where c.expires_at < pg_catalog.now() - interval '8 days';

  select pg_catalog.count(*)::integer into v_count
  from public.static_site_auth_session_issue_claim c
  where c.persona_id = p_persona_id
    and c.expires_at > pg_catalog.now();
  if v_count >= v_limit then
    return pg_catalog.jsonb_build_object('claim', 'persona_busy');
  end if;

  insert into public.static_site_auth_session_issue_claim (
    run_id, run_attempt, platform, persona_id, repository, workflow_ref
  ) values (
    p_run_id, p_run_attempt, p_platform, p_persona_id, p_repository, p_workflow_ref
  );
  return pg_catalog.jsonb_build_object('claim', 'new');
end;
$$;

revoke all on function public.claim_static_site_auth_session_issue_v2(
  text, integer, text, text, text, text, integer
) from public, anon, authenticated;
grant execute on function public.claim_static_site_auth_session_issue_v2(
  text, integer, text, text, text, text, integer
) to service_role;

create or replace function public.complete_static_site_auth_session_issue_v2(
  p_run_id text,
  p_run_attempt integer,
  p_platform text,
  p_persona_id text,
  p_repository text,
  p_workflow_ref text,
  p_credential_ciphertext text
)
returns boolean
language plpgsql
security definer
set search_path = pg_catalog
as $$
declare
  v_updated integer;
begin
  if p_credential_ciphertext is null
    or length(p_credential_ciphertext) not between 80 and 8192 then
    raise exception 'invalid_static_site_auth_session_credential_ciphertext'
      using errcode = '22023';
  end if;
  update public.static_site_auth_session_issue_claim c
  set credential_ciphertext = p_credential_ciphertext,
      credential_expires_at = pg_catalog.now() + interval '2 minutes'
  where c.run_id = p_run_id
    and c.run_attempt = p_run_attempt
    and c.platform = p_platform
    and c.persona_id = p_persona_id
    and c.repository = p_repository
    and c.workflow_ref = p_workflow_ref
    and c.expires_at > pg_catalog.now()
    and c.credential_ciphertext is null;
  get diagnostics v_updated = row_count;
  return v_updated = 1;
end;
$$;

revoke all on function public.complete_static_site_auth_session_issue_v2(
  text, integer, text, text, text, text, text
) from public, anon, authenticated;
grant execute on function public.complete_static_site_auth_session_issue_v2(
  text, integer, text, text, text, text, text
) to service_role;
