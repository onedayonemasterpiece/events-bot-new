-- A completed one-time credential no longer keeps its persona admission lease
-- for the original 20-minute crash bound. GitHub workflow concurrency already
-- serializes production-health and legacy-debug use of each cached persona;
-- the only cross-process ambiguity that must remain blocked is the same
-- two-minute encrypted lost-response replay window.
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
      credential_expires_at = pg_catalog.now() + interval '2 minutes',
      expires_at = least(c.expires_at, pg_catalog.now() + interval '2 minutes')
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
