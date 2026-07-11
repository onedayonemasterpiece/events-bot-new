-- Let authorized search continue with pgvector results when the optional LLM
-- verifier quota is exhausted. Search quota remains authoritative; LLM quota is
-- a quality add-on, not a hard blocker for the whole search.

create or replace function public.reserve_event_search_quota_v2(
  p_plan_id text default 'registered',
  p_use_llm boolean default false,
  p_now timestamptz default now()
)
returns table (
  user_id uuid,
  plan_id text,
  day_remaining integer,
  month_remaining integer,
  llm_day_remaining integer,
  llm_month_remaining integer,
  llm_reserved boolean
)
language plpgsql
security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_plan public.search_quota_plans%rowtype;
  v_day_start date := (date_trunc('day', p_now at time zone 'UTC'))::date;
  v_month_start date := (date_trunc('month', p_now at time zone 'UTC'))::date;
  v_day public.user_search_quota_ledger%rowtype;
  v_month public.user_search_quota_ledger%rowtype;
  v_plan_id text := coalesce(nullif(p_plan_id, ''), 'registered');
  v_llm_reserved boolean := false;
begin
  if v_user_id is null then
    raise exception 'authenticated user required' using errcode = '28000';
  end if;

  select * into v_plan
  from public.search_quota_plans
  where search_quota_plans.plan_id = v_plan_id
    and enabled;

  if not found then
    raise exception 'search quota plan is disabled or missing' using errcode = 'P0001';
  end if;

  insert into public.user_search_quota_ledger(user_id, plan_id, bucket_kind, bucket_start)
  values (v_user_id, v_plan.plan_id, 'day', v_day_start)
  on conflict on constraint user_search_quota_ledger_pkey do nothing;

  insert into public.user_search_quota_ledger(user_id, plan_id, bucket_kind, bucket_start)
  values (v_user_id, v_plan.plan_id, 'month', v_month_start)
  on conflict on constraint user_search_quota_ledger_pkey do nothing;

  select * into v_day
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  for update;

  select * into v_month
  from public.user_search_quota_ledger
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'month'
    and bucket_start = v_month_start
  for update;

  if v_day.request_count >= v_plan.daily_search_limit or v_month.request_count >= v_plan.monthly_search_limit then
    raise exception 'search quota exceeded' using errcode = 'P0001';
  end if;

  v_llm_reserved := p_use_llm
    and v_day.llm_request_count < v_plan.daily_llm_rerank_limit
    and v_month.llm_request_count < v_plan.monthly_llm_rerank_limit;

  update public.user_search_quota_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when v_llm_reserved then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'day'
    and bucket_start = v_day_start
  returning * into v_day;

  update public.user_search_quota_ledger
  set request_count = request_count + 1,
      llm_request_count = llm_request_count + case when v_llm_reserved then 1 else 0 end,
      last_used_at = now(),
      plan_id = v_plan.plan_id
  where user_search_quota_ledger.user_id = v_user_id
    and bucket_kind = 'month'
    and bucket_start = v_month_start
  returning * into v_month;

  return query select
    v_user_id,
    v_plan.plan_id,
    greatest(v_plan.daily_search_limit - v_day.request_count, 0),
    greatest(v_plan.monthly_search_limit - v_month.request_count, 0),
    greatest(v_plan.daily_llm_rerank_limit - v_day.llm_request_count, 0),
    greatest(v_plan.monthly_llm_rerank_limit - v_month.llm_request_count, 0),
    v_llm_reserved;
end;
$$;

revoke all on function public.reserve_event_search_quota_v2(text, boolean, timestamptz) from public, anon, authenticated;
grant execute on function public.reserve_event_search_quota_v2(text, boolean, timestamptz) to authenticated;
