-- Authoritative Postbox events arrive only through the IAM-protected YDS consumer.
-- Correlate every state-changing event to the exact persisted provider MessageId
-- and derive the suppression identity from the database, never from plaintext mail.

create or replace function public.email_record_postbox_event_v2(
  p_provider_event_key text,
  p_provider_message_id text,
  p_event_type text,
  p_event_at timestamptz,
  p_recipient_hmac text,
  p_hmac_key_version integer,
  p_payload_sha256 text
)
returns text
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_outbox email_control.email_outbox%rowtype;
  v_identity email_control.recipient_identity%rowtype;
  v_existing email_control.provider_event%rowtype;
  v_scope text;
  v_reason text;
begin
  p_provider_event_key := trim(coalesce(p_provider_event_key, ''));
  p_provider_message_id := trim(coalesce(p_provider_message_id, ''));
  p_event_type := trim(coalesce(p_event_type, ''));
  p_recipient_hmac := trim(coalesce(p_recipient_hmac, ''));
  p_payload_sha256 := lower(trim(coalesce(p_payload_sha256, '')));

  if length(p_provider_event_key) not between 1 and 300
     or p_provider_event_key ~ '[[:cntrl:]]' then
    raise exception 'invalid provider event key' using errcode = '22023';
  end if;
  if length(p_provider_message_id) not between 1 and 512
     or p_provider_message_id ~ '[[:cntrl:]]' then
    raise exception 'invalid provider message id' using errcode = '22023';
  end if;
  if p_event_type not in (
    'accepted', 'delivered', 'delivery_delay', 'hard_bounce', 'complaint',
    'unsubscribe', 'open', 'click', 'rendering_failure'
  ) then
    raise exception 'unsupported Postbox event type' using errcode = '22023';
  end if;
  if p_event_at is null then
    raise exception 'provider event timestamp required' using errcode = '22023';
  end if;
  if length(p_recipient_hmac) not between 43 and 128
     or p_hmac_key_version is null or p_hmac_key_version < 1 then
    raise exception 'invalid recipient identity proof' using errcode = '22023';
  end if;
  if p_payload_sha256 !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid payload hash' using errcode = '22023';
  end if;

  select o.* into v_outbox
    from email_control.email_outbox o
   where o.provider = 'postbox'
     and o.provider_message_id = p_provider_message_id
   for update;
  if v_outbox.id is null then
    return 'correlation_pending';
  end if;

  select i.* into strict v_identity
    from email_control.recipient_identity i
   where i.id = v_outbox.identity_id;

  if v_outbox.stream <> 'transactional'
     or v_outbox.dry_run
     or v_outbox.status not in ('submitted', 'delivered', 'unknown_delivery', 'terminal_failed') then
    raise exception 'Postbox event outbox state invalid' using errcode = '23514';
  end if;
  if v_identity.email_hmac <> p_recipient_hmac
     or v_identity.hmac_key_version <> p_hmac_key_version then
    raise exception 'Postbox recipient correlation mismatch' using errcode = '23514';
  end if;

  insert into email_control.provider_event (
    provider, provider_event_key, provider_message_id, event_type, event_at,
    email_hmac, payload_sha256, authenticated, verified
  ) values (
    'postbox', p_provider_event_key, p_provider_message_id, p_event_type, p_event_at,
    v_identity.email_hmac, p_payload_sha256, true, true
  ) on conflict (provider, provider_event_key) do nothing;

  if not found then
    select e.* into strict v_existing
      from email_control.provider_event e
     where e.provider = 'postbox' and e.provider_event_key = p_provider_event_key
     for update;
    if v_existing.provider_message_id is distinct from p_provider_message_id
       or v_existing.event_type is distinct from p_event_type
       or v_existing.event_at is distinct from p_event_at
       or v_existing.email_hmac is distinct from v_identity.email_hmac
       or v_existing.payload_sha256 is distinct from p_payload_sha256
       or not v_existing.authenticated or not v_existing.verified then
      raise exception 'Postbox provider event conflict' using errcode = '23514';
    end if;
    if v_existing.applied then
      return 'duplicate';
    end if;
  end if;

  update email_control.email_outbox
     set status = case
       when p_event_type in ('hard_bounce', 'complaint', 'rendering_failure') then 'terminal_failed'
       when p_event_type = 'delivered' and status <> 'terminal_failed' then 'delivered'
       else status
     end,
     last_error_class = case
       when p_event_type in ('hard_bounce', 'complaint', 'rendering_failure')
         then left('postbox_' || p_event_type, 160)
       else last_error_class
     end,
     updated_at = now()
   where id = v_outbox.id;

  if p_event_type in ('hard_bounce', 'complaint', 'unsubscribe') then
    v_scope := case when p_event_type = 'unsubscribe' then 'transactional' else 'all' end;
    v_reason := case
      when p_event_type = 'hard_bounce' then 'hard_bounce'
      when p_event_type = 'complaint' then 'complaint'
      else 'unsubscribe'
    end;
    insert into email_control.suppression (
      email_hmac, scope, provider, reason, provider_event_key, evidence
    ) values (
      v_identity.email_hmac, v_scope, 'postbox', v_reason, p_provider_event_key,
      jsonb_build_object('source', 'authenticated_postbox_yds')
    ) on conflict do nothing;
  end if;

  update email_control.provider_event
     set applied = true
   where provider = 'postbox' and provider_event_key = p_provider_event_key;
  return 'applied';
end;
$$;

revoke execute on function public.email_record_postbox_event_v2(text, text, text, timestamptz, text, integer, text)
  from public, anon, authenticated;
grant execute on function public.email_record_postbox_event_v2(text, text, text, timestamptz, text, integer, text)
  to service_role;

-- V1 accepts caller-controlled identity/trust. No deployed component needs it;
-- keep the definition for migration compatibility but remove the privileged caller.
revoke execute on function public.email_record_provider_event_v1(text, text, text, text, timestamptz, text, text, boolean, boolean)
  from service_role;
