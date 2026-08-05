-- Post-deploy cutover for the direct Focus Auth suppression gate.
-- Apply only after the Yandex Function version that calls
-- focus_auth_begin_delivery_batch_v1 is active and its signed smoke passes.
-- Rollback command (only together with rolling the Function back):
--   grant execute on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean) to service_role;

revoke execute on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean)
  from service_role;
revoke execute on function public.focus_auth_begin_delivery_v1(uuid, uuid, text, boolean)
  from public, anon, authenticated;

notify pgrst, 'reload schema';
