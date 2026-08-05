-- Unify Postbox provider-message correlation across the transactional outbox and
-- the direct focus Auth Send Email Hook. The table remains PII-free: it stores
-- only provider receipts, internal source identifiers and versioned email HMACs.

begin;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists postbox_feedback_state text;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists postbox_feedback_state_at timestamptz;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists postbox_last_event_type text;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists postbox_last_event_at timestamptz;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists postbox_event_count bigint not null default 0;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists recipient_hmac text;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists recipient_hmac_key_version smallint;

alter table personalization.focus_auth_delivery_attempt
  add column if not exists network_claimed_at timestamptz;

alter table email_control.suppression
  add column if not exists hmac_key_version smallint;

alter table email_control.suppression
  add constraint suppression_hmac_key_version_chk
  check (hmac_key_version is null or hmac_key_version > 0);

create index suppression_active_versioned_lookup_idx
  on email_control.suppression (email_hmac, hmac_key_version, scope)
  where active;

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_postbox_feedback_state_chk
  check (
    postbox_feedback_state is null
    or postbox_feedback_state in (
      'submitted', 'accepted', 'delivery_delay', 'delivered', 'terminal_failed'
    )
  );

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_postbox_feedback_time_chk
  check (
    (postbox_feedback_state is null and postbox_feedback_state_at is null)
    or (postbox_feedback_state is not null and postbox_feedback_state_at is not null)
  );

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_postbox_last_event_type_chk
  check (
    postbox_last_event_type is null
    or postbox_last_event_type in (
      'accepted', 'delivered', 'delivery_delay', 'hard_bounce', 'complaint',
      'unsubscribe', 'open', 'click', 'rendering_failure'
    )
  );

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_postbox_event_count_chk
  check (postbox_event_count >= 0);

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_recipient_proof_chk
  check (
    (
      recipient_hmac is null
      and recipient_hmac_key_version is null
      and network_claimed_at is null
    )
    or (
      length(recipient_hmac) between 43 and 128
      and recipient_hmac_key_version > 0
      and network_claimed_at is not null
    )
  );

alter table personalization.focus_auth_delivery_attempt
  drop constraint focus_auth_delivery_message_chk;

alter table personalization.focus_auth_delivery_attempt
  add constraint focus_auth_delivery_message_chk
  check (
    (
      provider_outcome = 'accepted'
      and provider_message_id is not null
      and length(provider_message_id) between 1 and 512
      and provider_message_id !~ '[[:cntrl:]]'
    )
    or (provider_outcome <> 'accepted' and provider_message_id is null)
  );

alter table email_control.suppression
  drop constraint suppression_event_key_size_chk;

alter table email_control.suppression
  add constraint suppression_event_key_size_chk
  check (provider_event_key is null or length(provider_event_key) <= 300);

create unique index focus_auth_delivery_postbox_message_uidx
  on personalization.focus_auth_delivery_attempt (provider_message_id)
  where provider = 'postbox' and provider_message_id is not null;

create index focus_auth_delivery_recipient_proof_idx
  on personalization.focus_auth_delivery_attempt (
    recipient_hmac_key_version, recipient_hmac, created_at desc
  )
  where recipient_hmac is not null;

create table email_control.postbox_message_correlation (
  provider_message_id text primary key,
  source_kind text not null,
  outbox_id uuid references email_control.email_outbox(id) on delete restrict,
  auth_attempt_id uuid references personalization.focus_auth_delivery_attempt(attempt_id) on delete restrict,
  email_hmac text,
  hmac_key_version integer,
  bound_at timestamptz,
  legacy_evidence_sha256 text,
  legacy_sent_at timestamptz,
  last_event_at timestamptz,
  created_at timestamptz not null default now(),
  constraint postbox_message_id_size_chk check (
    length(provider_message_id) between 1 and 512
    and provider_message_id !~ '[[:cntrl:]]'
  ),
  constraint postbox_correlation_source_chk check (
    (
      source_kind = 'transactional_outbox'
      and outbox_id is not null
      and auth_attempt_id is null
      and legacy_evidence_sha256 is null
      and legacy_sent_at is null
    )
    or (
      source_kind = 'focus_auth'
      and outbox_id is null
      and auth_attempt_id is not null
      and legacy_evidence_sha256 is null
      and legacy_sent_at is null
    )
    or (
      source_kind = 'legacy_auth'
      and outbox_id is null
      and auth_attempt_id is null
      and legacy_evidence_sha256 is not null
      and legacy_evidence_sha256 ~ '^[0-9a-f]{64}$'
      and legacy_sent_at is not null
    )
  ),
  constraint postbox_correlation_hmac_chk check (
    (
      email_hmac is null
      and hmac_key_version is null
      and bound_at is null
      and source_kind in ('focus_auth', 'legacy_auth')
    )
    or (
      length(email_hmac) between 43 and 128
      and hmac_key_version is not null
      and hmac_key_version > 0
      and bound_at is not null
    )
  )
);

create unique index postbox_correlation_outbox_uidx
  on email_control.postbox_message_correlation (outbox_id)
  where outbox_id is not null;

create unique index postbox_correlation_auth_attempt_uidx
  on email_control.postbox_message_correlation (auth_attempt_id)
  where auth_attempt_id is not null;

create index postbox_correlation_source_created_idx
  on email_control.postbox_message_correlation (source_kind, created_at desc);

comment on table email_control.postbox_message_correlation is
  'PII-free provider MessageId registry for transactional outbox, focus Auth and audited legacy Auth Postbox sends.';

alter table email_control.postbox_message_correlation enable row level security;
revoke all on email_control.postbox_message_correlation from public, anon, authenticated;
revoke all on email_control.postbox_message_correlation from service_role;

-- Prevent an accepted outbox receipt from committing between the backfill
-- snapshot and trigger installation. The migration transaction holds this lock
-- through trigger creation.
lock table email_control.email_outbox in share row exclusive mode;
lock table personalization.focus_auth_delivery_attempt in share row exclusive mode;

-- Backfill the already-persisted transactional receipts with their DB-owned
-- verified recipient identity. Any receipt collision aborts the migration.
insert into email_control.postbox_message_correlation (
  provider_message_id,
  source_kind,
  outbox_id,
  email_hmac,
  hmac_key_version,
  bound_at,
  created_at
)
select
  o.provider_message_id,
  'transactional_outbox',
  o.id,
  i.email_hmac,
  i.hmac_key_version,
  coalesce(o.updated_at, now()),
  coalesce(o.created_at, now())
from email_control.email_outbox o
join email_control.recipient_identity i on i.id = o.identity_id
where o.provider = 'postbox'
  and o.provider_message_id is not null;

-- Auth attempts did not previously retain the email HMAC. Register the exact
-- provider receipt now and bind the HMAC only when the first authenticated YDS
-- event supplies the actual Postbox recipient proof.
update personalization.focus_auth_delivery_attempt
   set postbox_feedback_state = 'submitted',
       postbox_feedback_state_at = coalesce(provider_finished_at, created_at)
 where provider = 'postbox'
   and provider_outcome = 'accepted'
   and provider_message_id is not null
   and postbox_feedback_state is null;

insert into email_control.postbox_message_correlation (
  provider_message_id,
  source_kind,
  auth_attempt_id,
  created_at
)
select
  a.provider_message_id,
  'focus_auth',
  a.attempt_id,
  coalesce(a.provider_finished_at, a.created_at, now())
from personalization.focus_auth_delivery_attempt a
where a.provider = 'postbox'
  and a.provider_outcome = 'accepted'
  and a.provider_message_id is not null;

-- Existing suppressions predate an explicit HMAC-version column. Resolve the
-- version only where the current DB-owned identities/correlations agree. A null
-- legacy version remains fail-closed for an exact HMAC match at admission time.
with hmac_versions as (
  select i.email_hmac, i.hmac_key_version::integer
    from email_control.recipient_identity i
  union all
  select c.email_hmac, c.hmac_key_version
    from email_control.postbox_message_correlation c
   where c.email_hmac is not null
     and c.hmac_key_version is not null
), resolved as (
  select h.email_hmac, min(h.hmac_key_version)::smallint as hmac_key_version
    from hmac_versions h
   group by h.email_hmac
  having count(distinct h.hmac_key_version) = 1
)
update email_control.suppression s
   set hmac_key_version = r.hmac_key_version
  from resolved r
 where s.email_hmac = r.email_hmac
   and s.hmac_key_version is null;

create or replace function email_control.register_postbox_outbox_correlation_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_email_hmac text;
  v_hmac_key_version integer;
begin
  if new.provider <> 'postbox' or new.provider_message_id is null then
    return new;
  end if;
  if tg_op = 'UPDATE' and old.provider_message_id is not distinct from new.provider_message_id then
    if old.identity_id is distinct from new.identity_id then
      raise exception 'Postbox receipt identity is immutable' using errcode = '23514';
    end if;
    return new;
  end if;

  select i.email_hmac, i.hmac_key_version
    into strict v_email_hmac, v_hmac_key_version
    from email_control.recipient_identity i
   where i.id = new.identity_id;

  insert into email_control.postbox_message_correlation (
    provider_message_id,
    source_kind,
    outbox_id,
    email_hmac,
    hmac_key_version,
    bound_at,
    created_at
  ) values (
    new.provider_message_id,
    'transactional_outbox',
    new.id,
    v_email_hmac,
    v_hmac_key_version,
    now(),
    coalesce(new.created_at, now())
  );
  return new;
end;
$$;

create trigger email_outbox_register_postbox_correlation_v1
  after insert or update of provider_message_id, identity_id
  on email_control.email_outbox
  for each row
  execute function email_control.register_postbox_outbox_correlation_v1();

create or replace function email_control.initialize_postbox_auth_feedback_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if new.provider = 'postbox'
     and new.provider_outcome = 'accepted'
     and new.provider_message_id is not null
     and new.postbox_feedback_state is null then
    new.postbox_feedback_state := 'submitted';
    new.postbox_feedback_state_at := coalesce(new.provider_finished_at, now());
  end if;
  return new;
end;
$$;

create trigger focus_auth_initialize_postbox_feedback_v1
  before insert or update of provider, provider_outcome, provider_message_id
  on personalization.focus_auth_delivery_attempt
  for each row
  execute function email_control.initialize_postbox_auth_feedback_v1();

create or replace function email_control.register_postbox_auth_correlation_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if new.provider <> 'postbox'
     or new.provider_outcome <> 'accepted'
     or new.provider_message_id is null then
    return new;
  end if;

  if tg_op = 'UPDATE' and old.provider_message_id is not distinct from new.provider_message_id then
    return new;
  end if;

  insert into email_control.postbox_message_correlation (
    provider_message_id,
    source_kind,
    auth_attempt_id,
    email_hmac,
    hmac_key_version,
    bound_at,
    created_at
  ) values (
    new.provider_message_id,
    'focus_auth',
    new.attempt_id,
    new.recipient_hmac,
    new.recipient_hmac_key_version,
    case when new.recipient_hmac is not null then new.network_claimed_at end,
    coalesce(new.provider_finished_at, new.created_at, now())
  );
  return new;
end;
$$;

create trigger focus_auth_register_postbox_correlation_v1
  after insert or update of provider, provider_outcome, provider_message_id
  on personalization.focus_auth_delivery_attempt
  for each row
  execute function email_control.register_postbox_auth_correlation_v1();

-- Direct Auth owns plaintext only inside the verified Send Email Hook. The hook
-- computes the same versioned HMAC as the IAM-protected feedback consumer and
-- submits one or two PII-free delivery descriptors. Admission for the complete
-- set happens atomically before any attempt reservation or provider request,
-- including Supabase Secure Email Change's current/new-address pair.
create or replace function public.focus_auth_begin_delivery_batch_v1(
  p_user_id uuid,
  p_action_type text,
  p_deliveries jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_count integer;
  v_distinct_attempts integer;
  v_item jsonb;
  v_attempt_id uuid;
  v_email_hmac text;
  v_hmac_key_version integer;
  v_prefer_notisend boolean;
  v_ordinal integer;
  v_existing personalization.focus_auth_delivery_attempt%rowtype;
  v_notisend_admitted boolean;
  v_results jsonb := '[]'::jsonb;
begin
  if p_user_id is null then
    raise exception 'user required' using errcode = '22023';
  end if;
  if p_action_type not in (
    'signup', 'magiclink', 'email', 'recovery', 'invite',
    'email_change', 'reauthentication'
  ) then
    raise exception 'unsupported action type' using errcode = '22023';
  end if;
  if jsonb_typeof(p_deliveries) <> 'array' then
    raise exception 'delivery batch must be an array' using errcode = '22023';
  end if;
  v_count := jsonb_array_length(p_deliveries);
  if v_count not between 1 and 2 then
    raise exception 'delivery batch size invalid' using errcode = '22023';
  end if;
  select count(distinct d.value->>'attempt_id')
    into v_distinct_attempts
    from jsonb_array_elements(p_deliveries) d(value);
  if v_distinct_attempts <> v_count then
    raise exception 'delivery attempt ids must be unique' using errcode = '22023';
  end if;

  -- Serialize the complete batch for one Auth user and close the pre-existing
  -- concurrent duplicate-attempt insert race.
  perform pg_advisory_xact_lock(hashtextextended(p_user_id::text, 20260801));

  -- Validate every exact identity before taking its serialization lock.
  for v_item in select d.value from jsonb_array_elements(p_deliveries) d(value)
  loop
    if jsonb_typeof(v_item) <> 'object'
       or jsonb_typeof(v_item->'prefer_notisend') <> 'boolean'
       or jsonb_typeof(v_item->'hmac_key_version') <> 'number' then
      raise exception 'delivery descriptor invalid' using errcode = '22023';
    end if;
    begin
      v_attempt_id := (v_item->>'attempt_id')::uuid;
      v_hmac_key_version := (v_item->>'hmac_key_version')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'delivery descriptor invalid' using errcode = '22023';
    end;
    v_email_hmac := trim(coalesce(v_item->>'email_hmac', ''));
    if length(v_email_hmac) not between 43 and 128
       or v_hmac_key_version not between 1 and 32767 then
      raise exception 'invalid recipient identity proof' using errcode = '22023';
    end if;
  end loop;

  -- Suppression insertion uses the same HMAC-keyed advisory lock. The commit
  -- order therefore defines whether this network claim or the suppression wins.
  for v_email_hmac in
    select distinct trim(d.value->>'email_hmac')
      from jsonb_array_elements(p_deliveries) d(value)
     order by 1
  loop
    perform pg_advisory_xact_lock(hashtextextended(v_email_hmac, 20260804));
  end loop;

  -- Preflight every descriptor before making any reservation mutation.
  for v_item in select d.value from jsonb_array_elements(p_deliveries) d(value)
  loop
    v_attempt_id := (v_item->>'attempt_id')::uuid;
    v_email_hmac := trim(v_item->>'email_hmac');
    v_hmac_key_version := (v_item->>'hmac_key_version')::integer;
    select * into v_existing
      from personalization.focus_auth_delivery_attempt a
     where a.attempt_id = v_attempt_id
     for update;
    if v_existing.attempt_id is not null
       and (
         v_existing.user_id <> p_user_id
         or v_existing.action_type <> p_action_type
       ) then
      raise exception 'delivery attempt identity conflict' using errcode = '23514';
    end if;
    if v_existing.attempt_id is not null
       and v_existing.recipient_hmac is not null
       and (
         v_existing.recipient_hmac <> v_email_hmac
         or v_existing.recipient_hmac_key_version <> v_hmac_key_version
       ) then
      raise exception 'delivery attempt recipient proof conflict' using errcode = '23514';
    end if;
    if v_existing.attempt_id is not null
       and (
         v_existing.provider_outcome <> 'accepted'
         or v_existing.provider is null
         or v_existing.provider_message_id is null
       ) then
      return jsonb_build_object(
        'admitted', false,
        'admission_status', 'attempt_already_finalized',
        'results', '[]'::jsonb
      );
    end if;
    if v_existing.attempt_id is null and exists (
      select 1
        from email_control.suppression s
       where s.email_hmac = v_email_hmac
         and (
           s.hmac_key_version = v_hmac_key_version
           or s.hmac_key_version is null
         )
         and s.active
         and s.scope in ('all', 'transactional')
    ) then
      return jsonb_build_object(
        'admitted', false,
        'admission_status', 'recipient_suppressed',
        'results', '[]'::jsonb
      );
    end if;
    if v_existing.attempt_id is null and exists (
      select 1
        from email_control.suppression s
       where s.email_hmac = v_email_hmac
         and s.hmac_key_version is not null
         and s.hmac_key_version <> v_hmac_key_version
         and s.active
         and s.scope in ('all', 'transactional')
    ) then
      raise exception 'suppression HMAC version conflict' using errcode = '23514';
    end if;
  end loop;

  -- Only an entirely admitted batch can reserve attempts/capacity.
  for v_item in select d.value from jsonb_array_elements(p_deliveries) d(value)
  loop
    v_attempt_id := (v_item->>'attempt_id')::uuid;
    v_prefer_notisend := (v_item->>'prefer_notisend')::boolean;
    select * into v_existing
      from personalization.focus_auth_delivery_attempt a
     where a.attempt_id = v_attempt_id;
    if v_existing.attempt_id is not null then
      v_notisend_admitted := exists (
        select 1
          from email_control.notisend_recipient_admission n
          join email_control.recommendation_capacity rc
            on rc.capacity_key = 'launch'
           and rc.provider_period_key = n.period_key
         where n.user_id = p_user_id
      );
      v_results := v_results || jsonb_build_array(jsonb_build_object(
        'attempt_id', v_attempt_id,
        'send_ordinal', v_existing.send_ordinal,
        'is_new', false,
        'previous_provider', v_existing.provider,
        'previous_outcome', v_existing.provider_outcome,
        'previous_message_id', v_existing.provider_message_id,
        'notisend_admitted', v_notisend_admitted
      ));
      continue;
    end if;

    select coalesce(max(a.send_ordinal), 0) + 1
      into v_ordinal
      from personalization.focus_auth_delivery_attempt a
     where a.user_id = p_user_id;
    insert into personalization.focus_auth_delivery_attempt (
      attempt_id,
      user_id,
      action_type,
      send_ordinal,
      recipient_hmac,
      recipient_hmac_key_version,
      network_claimed_at
    ) values (
      v_attempt_id,
      p_user_id,
      p_action_type,
      v_ordinal,
      trim(v_item->>'email_hmac'),
      (v_item->>'hmac_key_version')::smallint,
      now()
    );
    insert into personalization.focus_auth_method_attempt (
      attempt_id, auth_method, outcome
    ) values (
      v_attempt_id, 'email', 'started'
    ) on conflict (attempt_id) do nothing;

    v_notisend_admitted := false;
    -- Email change may address two distinct mailboxes for one user. The current
    -- NotiSend admission ledger is user-keyed, so route that action to Postbox
    -- rather than under-counting provider recipients.
    if p_action_type <> 'email_change'
       and (
         coalesce(v_prefer_notisend, false)
         or p_action_type <> 'signup'
         or v_ordinal > 1
       ) then
      select r.admitted
        into v_notisend_admitted
        from email_control.reserve_notisend_recipient_v1(
          p_user_id, 'auth', v_attempt_id
        ) r;
    end if;
    v_results := v_results || jsonb_build_array(jsonb_build_object(
      'attempt_id', v_attempt_id,
      'send_ordinal', v_ordinal,
      'is_new', true,
      'previous_provider', null,
      'previous_outcome', 'started',
      'previous_message_id', null,
      'notisend_admitted', v_notisend_admitted
    ));
  end loop;

  return jsonb_build_object(
    'admitted', true,
    'admission_status', 'admitted',
    'results', v_results
  );
end;
$$;

-- Align the receipt boundary with Postbox/provider-event contracts. Never
-- truncate an accepted MessageId because that makes later feedback impossible
-- to correlate.
create or replace function public.focus_auth_complete_delivery_v1(
  p_attempt_id uuid,
  p_provider text,
  p_outcome text,
  p_provider_message_id text default null
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_completed boolean;
begin
  p_provider_message_id := nullif(trim(coalesce(p_provider_message_id, '')), '');
  if p_provider not in ('postbox', 'notisend') then
    raise exception 'unsupported provider' using errcode = '22023';
  end if;
  if p_outcome not in (
    'accepted', 'definitive_reject', 'ambiguous', 'configuration_error'
  ) then
    raise exception 'unsupported provider outcome' using errcode = '22023';
  end if;
  if (p_outcome = 'accepted') <> (p_provider_message_id is not null) then
    raise exception 'accepted outcome requires one provider receipt' using errcode = '23514';
  end if;
  if p_provider_message_id is not null
     and (
       length(p_provider_message_id) not between 1 and 512
       or p_provider_message_id ~ '[[:cntrl:]]'
     ) then
    raise exception 'provider receipt invalid' using errcode = '22023';
  end if;

  update personalization.focus_auth_delivery_attempt
     set provider = p_provider,
         provider_outcome = p_outcome,
         provider_message_id = case
           when p_outcome = 'accepted' then p_provider_message_id
           else null
         end,
         provider_finished_at = now()
   where attempt_id = p_attempt_id
     and provider_outcome = 'started';
  v_completed := found;
  if v_completed
     and p_provider = 'notisend'
     and p_outcome in ('definitive_reject', 'configuration_error') then
    delete from email_control.notisend_recipient_admission a
     where a.first_attempt_id = p_attempt_id
       and not a.included_in_provider_snapshot;
  end if;
  return v_completed;
end;
$$;

create or replace function public.focus_auth_complete_delivery_batch_v1(
  p_results jsonb
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_item jsonb;
  v_count integer;
begin
  if jsonb_typeof(p_results) <> 'array' then
    raise exception 'delivery result batch must be an array' using errcode = '22023';
  end if;
  v_count := jsonb_array_length(p_results);
  if v_count not between 1 and 2 then
    raise exception 'delivery result batch size invalid' using errcode = '22023';
  end if;
  for v_item in select r.value from jsonb_array_elements(p_results) r(value)
  loop
    if not public.focus_auth_complete_delivery_v1(
      (v_item->>'attempt_id')::uuid,
      v_item->>'provider',
      v_item->>'outcome',
      v_item->>'provider_message_id'
    ) then
      raise exception 'delivery batch completion conflict' using errcode = '23514';
    end if;
  end loop;
  return true;
end;
$$;

-- An operator may register a pre-ledger Hosted Auth receipt only after building a
-- sanitized, independently reviewable evidence manifest. The event itself never
-- auto-registers an unknown MessageId.
create or replace function public.email_register_legacy_postbox_auth_v1(
  p_provider_message_id text,
  p_evidence_sha256 text,
  p_sent_at timestamptz
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_existing email_control.postbox_message_correlation%rowtype;
begin
  p_provider_message_id := trim(coalesce(p_provider_message_id, ''));
  p_evidence_sha256 := lower(trim(coalesce(p_evidence_sha256, '')));
  if length(p_provider_message_id) not between 1 and 512
     or p_provider_message_id ~ '[[:cntrl:]]'
     or p_evidence_sha256 !~ '^[0-9a-f]{64}$'
     or p_sent_at is null
     or p_sent_at < now() - interval '180 days'
     or p_sent_at > now() + interval '5 minutes' then
    raise exception 'invalid legacy Postbox correlation evidence' using errcode = '22023';
  end if;

  insert into email_control.postbox_message_correlation (
    provider_message_id,
    source_kind,
    legacy_evidence_sha256,
    legacy_sent_at,
    created_at
  ) values (
    p_provider_message_id,
    'legacy_auth',
    p_evidence_sha256,
    p_sent_at,
    now()
  ) on conflict (provider_message_id) do nothing;
  if found then
    return true;
  end if;

  select * into strict v_existing
    from email_control.postbox_message_correlation c
   where c.provider_message_id = p_provider_message_id
   for update;
  if v_existing.source_kind <> 'legacy_auth'
     or v_existing.legacy_evidence_sha256 <> p_evidence_sha256
     or v_existing.legacy_sent_at <> p_sent_at then
    raise exception 'legacy Postbox correlation conflicts with existing receipt'
      using errcode = '23514';
  end if;
  return false;
end;
$$;

create or replace function public.email_record_postbox_event_v3(
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
  v_correlation email_control.postbox_message_correlation%rowtype;
  v_outbox email_control.email_outbox%rowtype;
  v_auth personalization.focus_auth_delivery_attempt%rowtype;
  v_existing email_control.provider_event%rowtype;
  v_scope text;
  v_reason text;
  v_next_state text;
  v_next_state_at timestamptz;
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
     or p_hmac_key_version is null
     or p_hmac_key_version < 1 then
    raise exception 'invalid recipient identity proof' using errcode = '22023';
  end if;
  if p_payload_sha256 !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid payload hash' using errcode = '22023';
  end if;

  select * into v_correlation
    from email_control.postbox_message_correlation c
   where c.provider_message_id = p_provider_message_id
   for update;
  if v_correlation.provider_message_id is null then
    return 'correlation_pending';
  end if;

  if v_correlation.email_hmac is null then
    if v_correlation.source_kind not in ('focus_auth', 'legacy_auth') then
      raise exception 'Postbox correlation identity missing' using errcode = '23514';
    end if;
    update email_control.postbox_message_correlation
       set email_hmac = p_recipient_hmac,
           hmac_key_version = p_hmac_key_version,
           bound_at = now()
     where provider_message_id = p_provider_message_id
       and email_hmac is null
    returning * into strict v_correlation;
  elsif v_correlation.email_hmac <> p_recipient_hmac
     or v_correlation.hmac_key_version <> p_hmac_key_version then
    raise exception 'Postbox recipient correlation mismatch' using errcode = '23514';
  end if;

  insert into email_control.provider_event (
    provider, provider_event_key, provider_message_id, event_type, event_at,
    email_hmac, payload_sha256, authenticated, verified
  ) values (
    'postbox', p_provider_event_key, p_provider_message_id, p_event_type, p_event_at,
    v_correlation.email_hmac, p_payload_sha256, true, true
  ) on conflict (provider, provider_event_key) do nothing;

  if not found then
    select e.* into strict v_existing
      from email_control.provider_event e
     where e.provider = 'postbox'
       and e.provider_event_key = p_provider_event_key
     for update;
    if v_existing.provider_message_id is distinct from p_provider_message_id
       or v_existing.event_type is distinct from p_event_type
       or v_existing.event_at is distinct from p_event_at
       or v_existing.email_hmac is distinct from v_correlation.email_hmac
       or v_existing.payload_sha256 is distinct from p_payload_sha256
       or not v_existing.authenticated
       or not v_existing.verified then
      raise exception 'Postbox provider event conflict' using errcode = '23514';
    end if;
    if v_existing.applied then
      return 'duplicate';
    end if;
  end if;

  if v_correlation.source_kind = 'transactional_outbox' then
    select o.* into strict v_outbox
      from email_control.email_outbox o
     where o.id = v_correlation.outbox_id
     for update;
    if v_outbox.provider <> 'postbox'
       or v_outbox.stream <> 'transactional'
       or v_outbox.provider_message_id <> p_provider_message_id
       or v_outbox.dry_run
       or v_outbox.status not in (
         'submitted', 'delivered', 'unknown_delivery', 'terminal_failed'
       ) then
      raise exception 'Postbox event outbox state invalid' using errcode = '23514';
    end if;

    update email_control.email_outbox
       set status = case
         when p_event_type in ('hard_bounce', 'complaint', 'rendering_failure')
           then 'terminal_failed'
         when p_event_type = 'delivered' and status <> 'terminal_failed'
           then 'delivered'
         else status
       end,
       last_error_class = case
         when p_event_type in ('hard_bounce', 'complaint', 'rendering_failure')
           then left('postbox_' || p_event_type, 160)
         else last_error_class
       end,
       updated_at = now()
     where id = v_outbox.id;
  elsif v_correlation.source_kind = 'focus_auth' then
    select a.* into strict v_auth
      from personalization.focus_auth_delivery_attempt a
     where a.attempt_id = v_correlation.auth_attempt_id
     for update;
    if v_auth.provider <> 'postbox'
       or v_auth.provider_outcome <> 'accepted'
       or v_auth.provider_message_id <> p_provider_message_id then
      raise exception 'Postbox Auth correlation state invalid' using errcode = '23514';
    end if;

    v_next_state := coalesce(v_auth.postbox_feedback_state, 'submitted');
    v_next_state_at := coalesce(
      v_auth.postbox_feedback_state_at,
      v_auth.provider_finished_at,
      v_auth.created_at,
      p_event_at
    );
    if p_event_type in ('hard_bounce', 'complaint', 'rendering_failure') then
      v_next_state := 'terminal_failed';
      v_next_state_at := greatest(v_next_state_at, p_event_at);
    elsif v_next_state <> 'terminal_failed' and p_event_type = 'delivered' then
      v_next_state := 'delivered';
      v_next_state_at := greatest(v_next_state_at, p_event_at);
    elsif v_next_state in ('submitted', 'accepted', 'delivery_delay')
       and p_event_type = 'delivery_delay'
       and p_event_at >= v_next_state_at then
      v_next_state := 'delivery_delay';
      v_next_state_at := p_event_at;
    elsif v_next_state = 'submitted'
       and p_event_type = 'accepted'
       and p_event_at >= v_next_state_at then
      v_next_state := 'accepted';
      v_next_state_at := p_event_at;
    end if;

    update personalization.focus_auth_delivery_attempt
       set postbox_feedback_state = v_next_state,
           postbox_feedback_state_at = v_next_state_at,
           postbox_last_event_type = case
             when postbox_last_event_at is null or p_event_at >= postbox_last_event_at
               then p_event_type
             else postbox_last_event_type
           end,
           postbox_last_event_at = case
             when postbox_last_event_at is null or p_event_at >= postbox_last_event_at
               then p_event_at
             else postbox_last_event_at
           end,
           postbox_event_count = postbox_event_count + 1
     where attempt_id = v_auth.attempt_id;
  elsif v_correlation.source_kind <> 'legacy_auth' then
    raise exception 'unsupported Postbox correlation source' using errcode = '23514';
  end if;

  if p_event_type in ('hard_bounce', 'complaint', 'unsubscribe') then
    perform pg_advisory_xact_lock(
      hashtextextended(v_correlation.email_hmac, 20260804)
    );
    v_scope := case when p_event_type = 'unsubscribe' then 'transactional' else 'all' end;
    v_reason := case
      when p_event_type = 'hard_bounce' then 'hard_bounce'
      when p_event_type = 'complaint' then 'complaint'
      else 'unsubscribe'
    end;
    insert into email_control.suppression (
      email_hmac, hmac_key_version, scope, provider, reason,
      provider_event_key, evidence
    ) values (
      v_correlation.email_hmac,
      v_correlation.hmac_key_version,
      v_scope,
      'postbox',
      v_reason,
      p_provider_event_key,
      jsonb_build_object(
        'source', 'authenticated_postbox_yds',
        'correlation_source', v_correlation.source_kind
      )
    ) on conflict do nothing;
  end if;

  update email_control.postbox_message_correlation
     set last_event_at = case
       when last_event_at is null or p_event_at >= last_event_at then p_event_at
       else last_event_at
     end
   where provider_message_id = p_provider_message_id;

  update email_control.provider_event
     set applied = true
   where provider = 'postbox'
     and provider_event_key = p_provider_event_key;
  return 'applied';
end;
$$;

-- Keep the deployed consumer compatible during migration/deploy ordering. New
-- code may call v3 explicitly; the old v2 name now has the unified semantics.
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
language sql
security definer
set search_path = ''
as $$
  select public.email_record_postbox_event_v3(
    p_provider_event_key,
    p_provider_message_id,
    p_event_type,
    p_event_at,
    p_recipient_hmac,
    p_hmac_key_version,
    p_payload_sha256
  )
$$;

create or replace function public.email_postbox_health_v2()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  with outbox as (
    select *
      from email_control.email_outbox
     where provider = 'postbox' and stream = 'transactional'
  ), outbox_state as (
    select
      count(*) filter (where status = 'ready')::integer as ready_count,
      count(*) filter (
        where status = 'retryable' and next_attempt_at <= now()
      )::integer as retryable_due_count,
      count(*) filter (where status = 'claimed')::integer as claimed_count,
      count(*) filter (
        where status = 'claimed' and lease_expires_at <= now()
      )::integer as expired_claim_count,
      count(*) filter (where status = 'submitted')::integer as submitted_count,
      count(*) filter (
        where status = 'submitted' and updated_at <= now() - interval '15 minutes'
      )::integer as submitted_over_15m_count,
      count(*) filter (
        where status = 'submitted' and updated_at <= now() - interval '60 minutes'
      )::integer as submitted_over_60m_count,
      count(*) filter (where status = 'unknown_delivery')::integer as unknown_delivery_count,
      count(*) filter (
        where status = 'terminal_failed' and updated_at >= now() - interval '24 hours'
      )::integer as terminal_failed_24h_count,
      count(*) filter (
        where status = 'delivered' and updated_at >= now() - interval '24 hours'
      )::integer as delivered_24h_count,
      coalesce(max(extract(epoch from (now() - created_at))) filter (
        where status in ('ready', 'retryable')
      ), 0)::bigint as oldest_pending_seconds,
      coalesce(max(extract(epoch from (now() - updated_at))) filter (
        where status = 'submitted'
      ), 0)::bigint as oldest_submitted_seconds
    from outbox
  ), auth_state as (
    select
      count(*) filter (
        where provider = 'postbox'
          and provider_outcome = 'accepted'
          and coalesce(postbox_feedback_state, 'submitted') in (
            'submitted', 'accepted', 'delivery_delay'
          )
      )::integer as submitted_count,
      count(*) filter (
        where provider = 'postbox'
          and provider_outcome = 'accepted'
          and coalesce(postbox_feedback_state, 'submitted') in (
            'submitted', 'accepted', 'delivery_delay'
          )
          and coalesce(postbox_feedback_state_at, provider_finished_at, created_at)
            <= now() - interval '15 minutes'
      )::integer as submitted_over_15m_count,
      count(*) filter (
        where provider = 'postbox'
          and provider_outcome = 'accepted'
          and coalesce(postbox_feedback_state, 'submitted') in (
            'submitted', 'accepted', 'delivery_delay'
          )
          and coalesce(postbox_feedback_state_at, provider_finished_at, created_at)
            <= now() - interval '60 minutes'
      )::integer as submitted_over_60m_count,
      count(*) filter (
        where provider = 'postbox' and provider_outcome = 'ambiguous'
      )::integer as unknown_delivery_count,
      count(*) filter (
        where provider = 'postbox'
          and postbox_feedback_state = 'terminal_failed'
          and postbox_feedback_state_at >= now() - interval '24 hours'
      )::integer as terminal_failed_24h_count,
      count(*) filter (
        where provider = 'postbox'
          and postbox_feedback_state = 'delivered'
          and postbox_feedback_state_at >= now() - interval '24 hours'
      )::integer as delivered_24h_count,
      coalesce(max(extract(epoch from (
        now() - coalesce(postbox_feedback_state_at, provider_finished_at, created_at)
      ))) filter (
        where provider = 'postbox'
          and provider_outcome = 'accepted'
          and coalesce(postbox_feedback_state, 'submitted') in (
            'submitted', 'accepted', 'delivery_delay'
          )
      ), 0)::bigint as oldest_submitted_seconds,
      coalesce(max(extract(epoch from (now() - created_at))) filter (
        where provider_outcome = 'started'
      ), 0)::bigint as oldest_pending_seconds
    from personalization.focus_auth_delivery_attempt
  ), event_state as (
    select
      count(*) filter (
        where created_at >= now() - interval '24 hours'
      )::integer as provider_events_24h_count,
      max(created_at) as last_provider_event_at
    from email_control.provider_event
    where provider = 'postbox'
  ), correlation_state as (
    select
      count(*)::integer as total_count,
      count(*) filter (where email_hmac is null)::integer as unbound_count,
      count(*) filter (where source_kind = 'legacy_auth')::integer as legacy_count
    from email_control.postbox_message_correlation
  ), missing_state as (
    select (
      select count(*)
        from email_control.email_outbox o
       where o.provider = 'postbox'
         and o.provider_message_id is not null
         and not exists (
           select 1
             from email_control.postbox_message_correlation c
            where c.provider_message_id = o.provider_message_id
              and c.source_kind = 'transactional_outbox'
              and c.outbox_id = o.id
         )
    ) + (
      select count(*)
        from personalization.focus_auth_delivery_attempt a
       where a.provider = 'postbox'
         and a.provider_outcome = 'accepted'
         and a.provider_message_id is not null
         and not exists (
           select 1
             from email_control.postbox_message_correlation c
            where c.provider_message_id = a.provider_message_id
              and c.source_kind = 'focus_auth'
              and c.auth_attempt_id = a.attempt_id
         )
    ) as missing_count
  )
  select jsonb_build_object(
    'ready_count', o.ready_count,
    'retryable_due_count', o.retryable_due_count,
    'claimed_count', o.claimed_count,
    'expired_claim_count', o.expired_claim_count,
    'submitted_count', o.submitted_count + a.submitted_count,
    'submitted_over_15m_count',
      o.submitted_over_15m_count + a.submitted_over_15m_count,
    'submitted_over_60m_count',
      o.submitted_over_60m_count + a.submitted_over_60m_count,
    'unknown_delivery_count', o.unknown_delivery_count + a.unknown_delivery_count,
    'terminal_failed_24h_count',
      o.terminal_failed_24h_count + a.terminal_failed_24h_count,
    'delivered_24h_count', o.delivered_24h_count + a.delivered_24h_count,
    'oldest_pending_seconds', greatest(
      o.oldest_pending_seconds, a.oldest_pending_seconds
    ),
    'oldest_submitted_seconds', greatest(
      o.oldest_submitted_seconds, a.oldest_submitted_seconds
    ),
    'provider_events_24h_count', e.provider_events_24h_count,
    'last_provider_event_at', e.last_provider_event_at,
    'postbox_auth_submitted_count', a.submitted_count,
    'postbox_auth_delivered_24h_count', a.delivered_24h_count,
    'postbox_auth_terminal_failed_24h_count', a.terminal_failed_24h_count,
    'postbox_correlation_total_count', c.total_count,
    'postbox_correlation_unbound_count', c.unbound_count,
    'postbox_legacy_correlation_count', c.legacy_count,
    'postbox_missing_correlation_count', m.missing_count,
    'observed_at', now()
  )
  from outbox_state o
  cross join auth_state a
  cross join event_state e
  cross join correlation_state c
  cross join missing_state m
$$;

create or replace function public.email_postbox_health_v1()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $$
  select public.email_postbox_health_v2()
$$;

-- Bounded operator inventory helper. Raw provider receipts are accepted only as
-- lookup inputs; the result exposes stable hashes and classifications, never the
-- receipt itself or a recipient identity.
create or replace function public.email_classify_postbox_receipts_v1(
  p_provider_message_ids text[]
)
returns table (
  message_sha256 text,
  source_classification text,
  correlation_status text
)
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_count integer;
begin
  v_count := cardinality(p_provider_message_ids);
  if v_count is null or v_count not between 1 and 500
     or exists (
       select 1 from unnest(p_provider_message_ids) u(message_id)
        where message_id is null
           or length(message_id) not between 1 and 512
           or message_id ~ '[[:cntrl:]]'
     ) then
    raise exception 'invalid Postbox inventory batch' using errcode = '22023';
  end if;
  return query
  select
    pg_catalog.encode(extensions.digest(u.message_id, 'sha256'), 'hex'),
    case
      when c.source_kind is not null then c.source_kind
      when o.id is not null then 'transactional_outbox'
      when a.attempt_id is not null then 'focus_auth'
      else 'unproven'
    end,
    case
      when c.provider_message_id is not null and c.email_hmac is not null then 'bound'
      when c.provider_message_id is not null then 'unbound'
      when o.id is not null or a.attempt_id is not null then 'missing_correlation'
      else 'correlation_pending'
    end
  from unnest(p_provider_message_ids) u(message_id)
  left join email_control.postbox_message_correlation c
    on c.provider_message_id = u.message_id
  left join email_control.email_outbox o
    on o.provider = 'postbox' and o.provider_message_id = u.message_id
  left join personalization.focus_auth_delivery_attempt a
    on a.provider = 'postbox' and a.provider_message_id = u.message_id;
end;
$$;

revoke execute on function public.email_register_legacy_postbox_auth_v1(text, text, timestamptz)
  from public, anon, authenticated;
revoke execute on function public.focus_auth_begin_delivery_batch_v1(uuid, text, jsonb)
  from public, anon, authenticated;
revoke execute on function public.focus_auth_complete_delivery_batch_v1(jsonb)
  from public, anon, authenticated;
revoke execute on function public.email_record_postbox_event_v3(text, text, text, timestamptz, text, integer, text)
  from public, anon, authenticated;
revoke execute on function public.email_record_postbox_event_v2(text, text, text, timestamptz, text, integer, text)
  from public, anon, authenticated;
revoke execute on function public.email_postbox_health_v2()
  from public, anon, authenticated;
revoke execute on function public.email_postbox_health_v1()
  from public, anon, authenticated;
revoke execute on function public.email_classify_postbox_receipts_v1(text[])
  from public, anon, authenticated;

grant execute on function public.email_register_legacy_postbox_auth_v1(text, text, timestamptz)
  to service_role;
grant execute on function public.focus_auth_begin_delivery_batch_v1(uuid, text, jsonb)
  to service_role;
grant execute on function public.focus_auth_complete_delivery_batch_v1(jsonb)
  to service_role;
grant execute on function public.email_record_postbox_event_v3(text, text, text, timestamptz, text, integer, text)
  to service_role;
grant execute on function public.email_record_postbox_event_v2(text, text, text, timestamptz, text, integer, text)
  to service_role;
grant execute on function public.email_postbox_health_v2()
  to service_role;
grant execute on function public.email_postbox_health_v1()
  to service_role;
grant execute on function public.email_classify_postbox_receipts_v1(text[])
  to service_role;

notify pgrst, 'reload schema';

commit;
