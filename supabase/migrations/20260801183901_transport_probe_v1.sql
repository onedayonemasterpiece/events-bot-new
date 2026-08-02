-- Capability probe used by the static browser transport. It has no side
-- effects and returns only the caller nonce plus a contract version.
create or replace function public.transport_probe_v1(p_nonce text)
returns jsonb
language plpgsql
stable
security invoker
set search_path = pg_catalog
as $$
begin
  if p_nonce is null
     or pg_catalog.char_length(p_nonce) < 16
     or pg_catalog.char_length(p_nonce) > 80
     or p_nonce !~ '^[A-Za-z0-9_-]+$' then
    raise exception 'invalid_transport_probe_nonce' using errcode = '22023';
  end if;

  return pg_catalog.jsonb_build_object(
    'nonce', p_nonce,
    'schema', 1
  );
end;
$$;

revoke all on function public.transport_probe_v1(text) from public;
grant execute on function public.transport_probe_v1(text) to anon, authenticated;

comment on function public.transport_probe_v1(text) is
  'Side-effect-free nonce echo for capability-aware browser transport checks.';
