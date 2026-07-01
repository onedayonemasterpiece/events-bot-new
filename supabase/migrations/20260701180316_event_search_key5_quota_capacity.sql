-- Expand authorized event-search daily quota after adding GOOGLE_API_KEY5.
-- 2026-07-01 facts:
-- - all 5 Google AI key secrets passed live smoke for gemini-embedding-2,
--   gemini-3.1-flash-lite and gemma-4-26b-a4b-it, but four older lanes are
--   reserved for other production surfaces and must not be budgeted for normal
--   /poisk/ traffic.
-- - normal online search intentionally uses only the new non-reserved lane
--   GOOGLE_API_KEY5: gemini-embedding-2 has 1000 RPD on that lane, with
--   200 RPD kept as search/diagnostic buffer; gemini-3.1-flash-lite uses the
--   defensive repo cap 450 RPD, with 100 RPD kept as a search-lane buffer.
-- - GOOGLE_API_KEY4 (static related/vector sync), GOOGLE_API_KEY3 (Telegram
--   Monitoring), GOOGLE_API_KEY2 (guide monitoring) and GOOGLE_API_KEY
--   (Smart Update/shared bot traffic) are reserve/failover only.
-- - overflow verifier gemma-4-26b-a4b-it stays a resilience path only; it is not
--   counted into the normal fast-search quota because it is much slower than
--   Gemini Lite.
-- - Effective normal verifier capacity after the search-lane buffer is
--   350 fast Lite RPD; embedding has 800 RPD after the KEY5 query buffer.
-- Product "registered users" are counted by the live Yandex site identity
-- provider, not by all auth.users rows: on 2026-07-01 auth.users had 47 rows,
-- but only 1 custom:yandex identity; the other 46 were email/test/smoke users.
-- With the current 1 effective site user, floor(350 / 1) gives 350 fast
-- verified searches/day. Reserved/shared lanes are configured only as late
-- failover after the active search pool is exhausted or provider-degraded.

create or replace function public.refresh_registered_search_quota_v1(
  p_embedding_rpd integer default 1000,
  p_llm_rpd integer default 450,
  p_embedding_static_reserve integer default 200,
  p_llm_static_reserve integer default 100,
  p_min_daily_search integer default 10,
  p_max_daily_search integer default 350,
  p_min_daily_llm integer default 5,
  p_max_daily_llm integer default 350,
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
  v_embedding_daily integer;
  v_llm_daily_capacity integer;
  v_daily_search integer;
  v_daily_llm integer;
begin
  select greatest(count(distinct u.id)::integer, 1)
    into v_user_count
  from auth.users u
  join auth.identities i on i.user_id = u.id
  where i.provider = 'custom:yandex';

  v_embedding_daily := greatest(
    ((greatest(p_embedding_rpd - p_embedding_static_reserve, 0)) / v_user_count)::integer,
    p_min_daily_search
  );
  v_llm_daily_capacity := greatest(
    ((greatest(p_llm_rpd - p_llm_static_reserve, 0)) / v_user_count)::integer,
    p_min_daily_llm
  );

  v_daily_search := least(
    v_embedding_daily,
    v_llm_daily_capacity,
    p_max_daily_search
  );

  v_daily_llm := least(
    v_llm_daily_capacity,
    v_daily_search,
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

-- Apply the updated plan during migration. For the current 1 effective
-- custom:yandex site user this writes:
-- daily_search_limit=350, monthly_search_limit=3500,
-- daily_llm_rerank_limit=350, monthly_llm_rerank_limit=3500.
select * from public.refresh_registered_search_quota_v1();
