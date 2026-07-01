-- Expand authorized event-search daily quota after adding GOOGLE_API_KEY5.
-- 2026-07-01 facts:
-- - all 5 Google AI key secrets passed live smoke for gemini-embedding-2,
--   gemini-3.1-flash-lite and gemma-4-26b-a4b-it.
-- - Query embedding is a new online-search workload and uses all five lanes:
--   5 * 1000 RPD = 5000 gross, with 1000 RPD kept for static/vector backfills,
--   diagnostics and burst safety => 4000 online query embeddings/day.
-- - Normal online fast verifier pool follows the existing shared-key rotation
--   pattern and excludes only the guide fixed lane GOOGLE_API_KEY2:
--   active Lite lanes are GOOGLE_API_KEY5, GOOGLE_API_KEY4, GOOGLE_API_KEY3 and
--   GOOGLE_API_KEY. At the defensive 450 RPD/key this is 1800 gross Lite RPD.
-- - Keep 800 Lite RPD as cross-service buffer for Smart Update, Telegram
--   Monitoring/static/emergency overlap and provider variance, leaving 1000
--   normal fast Lite-verified searches/day for /poisk/.
-- - GOOGLE_API_KEY2 stays a fixed guide-monitoring reserve/failover lane for
--   LLM verification and is not counted into the normal search quota.
-- - overflow verifier gemma-4-26b-a4b-it stays a resilience path only; it is not
--   counted into the normal fast-search quota because it is much slower than
--   Gemini Lite.
-- Product "registered users" are counted by the live Yandex site identity
-- provider, not by all auth.users rows: on 2026-07-01 auth.users had 47 rows,
-- but only 1 custom:yandex identity; the other 46 were email/test/smoke users.
-- With the current 1 effective site user, floor(1000 / 1) gives 1000 fast
-- verified searches/day.

create or replace function public.refresh_registered_search_quota_v1(
  p_embedding_rpd integer default 5000,
  p_llm_rpd integer default 1800,
  p_embedding_static_reserve integer default 1000,
  p_llm_static_reserve integer default 800,
  p_min_daily_search integer default 10,
  p_max_daily_search integer default 1000,
  p_min_daily_llm integer default 5,
  p_max_daily_llm integer default 1000,
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
-- daily_search_limit=1000, monthly_search_limit=10000,
-- daily_llm_rerank_limit=1000, monthly_llm_rerank_limit=10000.
select * from public.refresh_registered_search_quota_v1();
