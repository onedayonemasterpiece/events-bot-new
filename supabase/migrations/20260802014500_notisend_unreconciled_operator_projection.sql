-- Keep the original aggregate implementation private and project unknown
-- NotiSend capacity explicitly. PostgreSQL greatest(0, NULL) returns 0, so a
-- nullable provider counter alone is insufficient for honest operator output.

begin;

alter function public.focus_auth_operator_summary_v1(timestamptz)
  rename to focus_auth_operator_summary_base_v1;

revoke all on function public.focus_auth_operator_summary_base_v1(timestamptz)
  from public, anon, authenticated;
grant execute on function public.focus_auth_operator_summary_base_v1(timestamptz)
  to service_role;

create function public.focus_auth_operator_summary_v1(
  p_since timestamptz default (now() - interval '24 hours')
)
returns jsonb
language sql
security definer
set search_path = ''
stable
as $$
  select case
    when coalesce((q.report #>> '{notisend_capacity,routing_ready}')::boolean, false)
      then q.report
    else pg_catalog.jsonb_set(
      pg_catalog.jsonb_set(
        pg_catalog.jsonb_set(
          pg_catalog.jsonb_set(
            q.report,
            '{notisend_capacity,provider_reported}',
            'null'::jsonb,
            false
          ),
          '{notisend_capacity,admitted_after_reconcile}',
          'null'::jsonb,
          false
        ),
        '{notisend_capacity,occupied}',
        'null'::jsonb,
        false
      ),
      '{notisend_capacity,available}',
      'null'::jsonb,
      false
    )
  end
  from (
    select public.focus_auth_operator_summary_base_v1(p_since) as report
  ) q
$$;

revoke all on function public.focus_auth_operator_summary_v1(timestamptz)
  from public, anon, authenticated;
grant execute on function public.focus_auth_operator_summary_v1(timestamptz)
  to service_role;

commit;
