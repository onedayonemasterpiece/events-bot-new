-- Pin the lookup path for every SECURITY DEFINER-capable limiter RPC. This is
-- idempotent and also repairs databases bootstrapped before the definitions
-- gained an explicit search_path.

ALTER FUNCTION public.google_ai_limiter_capabilities()
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_reserve(uuid, integer, text, text, text, integer, uuid[])
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_mark_sent(uuid, integer)
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_finalize(uuid, integer, integer, integer, integer, integer, text, text, text, text)
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_sweep_stale(integer, integer)
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_finalize_interaction(uuid, integer, text, text, text, integer, integer, integer, integer, text, text, text)
    SET search_path TO public, pg_temp;

ALTER FUNCTION public.google_ai_record_interaction_semantic(uuid, integer, text, text)
    SET search_path TO public, pg_temp;
