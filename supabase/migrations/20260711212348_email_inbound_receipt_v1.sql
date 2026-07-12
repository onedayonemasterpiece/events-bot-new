-- Idempotent, metadata-only receipt boundary for the Yandex Mail Trigger pipeline.
-- The retained SpaceWeb mailbox/private Object Storage envelope remains authoritative.

create table email_control.inbound_receipt (
  inbound_id text primary key,
  contract_schema text not null,
  mailbox text not null,
  received_at timestamptz not null,
  object_bucket text not null,
  object_key text not null,
  object_sha256 text not null,
  object_expires_at timestamptz not null,
  message_id_hmac text,
  sender_hmac text,
  body_bytes integer not null,
  body_sha256 text not null,
  body_media_type text not null,
  attachment_count integer not null,
  created_at timestamptz not null default now(),
  constraint inbound_receipt_id_chk check (inbound_id ~ '^[0-9a-f]{64}$'),
  constraint inbound_receipt_schema_chk check (contract_schema = 'kenigevents.email_inbound.adapter.v1'),
  constraint inbound_receipt_mailbox_chk check (mailbox = 'info@kenigevents.ru'),
  constraint inbound_receipt_bucket_size_chk check (length(object_bucket) between 3 and 63),
  constraint inbound_receipt_key_size_chk check (length(object_key) between 1 and 1023),
  constraint inbound_receipt_object_hash_chk check (object_sha256 ~ '^[0-9a-f]{64}$'),
  constraint inbound_receipt_message_hmac_chk check (message_id_hmac is null or message_id_hmac ~ '^[0-9a-f]{64}$'),
  constraint inbound_receipt_sender_hmac_chk check (sender_hmac is null or sender_hmac ~ '^[0-9a-f]{64}$'),
  constraint inbound_receipt_body_size_chk check (body_bytes between 0 and 3500000),
  constraint inbound_receipt_body_hash_chk check (body_sha256 ~ '^[0-9a-f]{64}$'),
  constraint inbound_receipt_media_size_chk check (length(body_media_type) between 1 and 255),
  constraint inbound_receipt_attachment_count_chk check (attachment_count between 0 and 100),
  constraint inbound_receipt_expiry_chk check (object_expires_at > received_at)
);

create index inbound_receipt_received_at_idx
  on email_control.inbound_receipt (received_at desc);

alter table email_control.inbound_receipt enable row level security;
revoke all on email_control.inbound_receipt from public, anon, authenticated, service_role;

create or replace function public.email_record_inbound_receipt_v1(
  p_inbound_id text,
  p_contract_schema text,
  p_mailbox text,
  p_received_at timestamptz,
  p_object_bucket text,
  p_object_key text,
  p_object_sha256 text,
  p_object_expires_at timestamptz,
  p_message_id_hmac text,
  p_sender_hmac text,
  p_body_bytes integer,
  p_body_sha256 text,
  p_body_media_type text,
  p_attachment_count integer
)
returns text
language plpgsql
security definer
set search_path = email_control, public, extensions, pg_temp
as $$
declare
  v_inserted text;
  v_existing email_control.inbound_receipt%rowtype;
begin
  insert into email_control.inbound_receipt (
    inbound_id, contract_schema, mailbox, received_at,
    object_bucket, object_key, object_sha256, object_expires_at,
    message_id_hmac, sender_hmac,
    body_bytes, body_sha256, body_media_type, attachment_count
  ) values (
    p_inbound_id, p_contract_schema, p_mailbox, p_received_at,
    p_object_bucket, p_object_key, p_object_sha256, p_object_expires_at,
    p_message_id_hmac, p_sender_hmac,
    p_body_bytes, p_body_sha256, p_body_media_type, p_attachment_count
  )
  on conflict (inbound_id) do nothing
  returning inbound_id into v_inserted;

  if v_inserted is not null then
    return 'accepted';
  end if;

  select * into strict v_existing
  from email_control.inbound_receipt
  where inbound_id = p_inbound_id;

  if v_existing.contract_schema is distinct from p_contract_schema
     or v_existing.mailbox is distinct from p_mailbox
     or v_existing.received_at is distinct from p_received_at
     or v_existing.object_bucket is distinct from p_object_bucket
     or v_existing.object_key is distinct from p_object_key
     or v_existing.object_sha256 is distinct from p_object_sha256
     or v_existing.object_expires_at is distinct from p_object_expires_at
     or v_existing.message_id_hmac is distinct from p_message_id_hmac
     or v_existing.sender_hmac is distinct from p_sender_hmac
     or v_existing.body_bytes is distinct from p_body_bytes
     or v_existing.body_sha256 is distinct from p_body_sha256
     or v_existing.body_media_type is distinct from p_body_media_type
     or v_existing.attachment_count is distinct from p_attachment_count then
    raise exception using errcode = 'P0001', message = 'inbound_receipt_collision';
  end if;

  return 'duplicate';
end;
$$;

revoke execute on function public.email_record_inbound_receipt_v1(
  text, text, text, timestamptz, text, text, text, timestamptz,
  text, text, integer, text, text, integer
) from public, anon, authenticated;
grant execute on function public.email_record_inbound_receipt_v1(
  text, text, text, timestamptz, text, text, text, timestamptz,
  text, text, integer, text, text, integer
) to service_role;
