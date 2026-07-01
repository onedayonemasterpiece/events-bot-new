-- Expand authorized event-search daily quota after adding GOOGLE_API_KEY5.
-- 2026-07-01 facts:
-- - 5 Google AI key lanes passed live smoke for gemini-embedding-2,
--   gemini-3.1-flash-lite and gemma-4-26b-a4b-it.
-- - gemini-embedding-2: 1000 RPD per key lane => 5000 RPD total.
-- - fast verifier gemini-3.1-flash-lite: defensive 450 RPD per key lane
--   => 2250 RPD total, with 1000 RPD reserved for other services.
-- - overflow verifier gemma-4-26b-a4b-it: 1500 RPD per key lane
--   => 7500 RPD total, with 2500 RPD reserved for other services/static related.
-- - Effective verifier capacity after reserves is 1250 fast Lite RPD +
--   5000 Gemma overflow RPD = 6250 RPD; embedding remains the overall
--   online bottleneck at 4000 RPD after reserve.
-- With 47 registered users this yields floor(4000 / 47) = 85 verified
-- searches/day, capped to 80/day as a canary safety ceiling. The first
-- roughly 25 searches/user/day can be served by fast Lite if usage is uniform;
-- the rest is Gemma overflow.

create or replace function public.refresh_registered_search_quota_v1(
  p_embedding_rpd integer default 5000,
  p_llm_rpd integer default 9750,
  p_embedding_static_reserve integer default 1000,
  p_llm_static_reserve integer default 3500,
  p_min_daily_search integer default 10,
  p_max_daily_search integer default 80,
  p_min_daily_llm integer default 5,
  p_max_daily_llm integer default 80,
  p_month_multiplier integer default 10
)
returns table (
  auth_user_count integer,
  daily_search_limit integer,
  monthly_search_limit integer,
  daily_llm_rerank_limit integer,
  monthly_llm_rerank_limit integer
)
language plpgsql
security definer
set search_path = public, extensions, auth, pg_temp
as $$
declare
  v_user_count integer;
  v_daily_search integer;
  v_daily_llm integer;
begin
  select greatest(count(*)::integer, 1)
    into v_user_count
  from auth.users;

  v_daily_search := least(
    greatest(((greatest(p_embedding_rpd - p_embedding_static_reserve, 0)) / v_user_count)::integer, p_min_daily_search),
    p_max_daily_search
  );

  v_daily_llm := least(
    greatest(((greatest(p_llm_rpd - p_llm_static_reserve, 0)) / v_user_count)::integer, p_min_daily_llm),
    p_max_daily_llm
  );

  insert into public.search_quota_plans(
    plan_id,
    daily_search_limit,
    monthly_search_limit,
    daily_llm_rerank_limit,
    monthly_llm_rerank_limit,
    enabled
  ) values (
    'registered',
    v_daily_search,
    v_daily_search * p_month_multiplier,
    v_daily_llm,
    v_daily_llm * p_month_multiplier,
    true
  )
  on conflict (plan_id) do update set
    daily_search_limit = excluded.daily_search_limit,
    monthly_search_limit = excluded.monthly_search_limit,
    daily_llm_rerank_limit = excluded.daily_llm_rerank_limit,
    monthly_llm_rerank_limit = excluded.monthly_llm_rerank_limit,
    enabled = true,
    updated_at = now();

  return query select
    v_user_count,
    v_daily_search,
    v_daily_search * p_month_multiplier,
    v_daily_llm,
    v_daily_llm * p_month_multiplier;
end;
$$;

revoke all on function public.refresh_registered_search_quota_v1(integer, integer, integer, integer, integer, integer, integer, integer, integer) from public, anon, authenticated;
grant execute on function public.refresh_registered_search_quota_v1(integer, integer, integer, integer, integer, integer, integer, integer, integer) to service_role;

-- Apply the updated plan during migration. For 47 registered users this writes:
-- daily_search_limit=80, monthly_search_limit=800,
-- daily_llm_rerank_limit=80, monthly_llm_rerank_limit=800.
select * from public.refresh_registered_search_quota_v1();
