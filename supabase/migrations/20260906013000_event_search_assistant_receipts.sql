-- Additive private search receipts. No production rollout is implied by source.
-- Authenticated browser clients never acquire service_role or write these rows.
create table if not exists public.event_search_assistant_operations (
  id uuid primary key,
  owner_id uuid not null references auth.users(id),
  kind text not null check (kind in ('asr','interpret','search')),
  payload jsonb not null check (pg_column_size(payload) <= 65536),
  state text not null default 'accepted' check (state in ('accepted','processing','completed','failed','outcome_unknown')),
  claim_id uuid,
  dispatched boolean not null default false,
  outcome jsonb check (pg_column_size(outcome) <= 2097152),
  error_code text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists event_search_assistant_owner_history on public.event_search_assistant_operations(owner_id,created_at desc,id desc);
alter table public.event_search_assistant_operations enable row level security;
revoke all on public.event_search_assistant_operations from public, anon, authenticated;
grant select, insert, update, delete on public.event_search_assistant_operations to service_role;

create table if not exists public.event_search_assistant_audio_parts (
  operation_id uuid not null references public.event_search_assistant_operations(id),
  part_index integer not null check (part_index between 0 and 255),
  first_frame bigint not null check (first_frame >= 0),
  frame_count integer not null check (frame_count > 0),
  sample_rate integer not null check (sample_rate between 8000 and 96000),
  digest text not null check (digest ~ '^[a-f0-9]{64}$'),
  audio bytea not null check (octet_length(audio) <= 786432),
  primary key(operation_id,part_index)
);
alter table public.event_search_assistant_audio_parts enable row level security;
revoke all on public.event_search_assistant_audio_parts from public, anon, authenticated;
grant select, insert, update, delete on public.event_search_assistant_audio_parts to service_role;

create or replace function public.event_search_assistant_admit_v1(p_owner uuid,p_id uuid,p_kind text,p_payload jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog as $$
declare r public.event_search_assistant_operations;
begin
  if not exists(select 1 from auth.users where id=p_owner and not coalesce(is_anonymous,false)) then
    raise exception 'eligible_user_required' using errcode='28000';
  end if;
  insert into public.event_search_assistant_operations(id,owner_id,kind,payload) values(p_id,p_owner,p_kind,p_payload) on conflict(id) do nothing;
  select * into r from public.event_search_assistant_operations where id=p_id for update;
  if r.owner_id<>p_owner then raise exception 'operation_not_found' using errcode='42501'; end if;
  if r.kind<>p_kind or r.payload<>p_payload then raise exception 'payload_conflict' using errcode='23505'; end if;
  return to_jsonb(r)-'claim_id';
end $$;

create or replace function public.event_search_assistant_claim_v1(p_owner uuid,p_id uuid)
returns jsonb language plpgsql security definer set search_path=pg_catalog as $$
declare r public.event_search_assistant_operations;
begin
  select * into r from public.event_search_assistant_operations where id=p_id and owner_id=p_owner for update;
  if not found then raise exception 'operation_not_found' using errcode='42501'; end if;
  -- Crash before dispatch may be resumed; crash after dispatch is never replayed.
  if r.state='processing' and r.updated_at<now()-interval '5 minutes' then
    r.state:=case when r.dispatched then 'outcome_unknown' else 'accepted' end;
    update public.event_search_assistant_operations set state=r.state,claim_id=null,updated_at=now() where id=p_id;
  end if;
  if r.state<>'accepted' then return jsonb_build_object('claimed',false,'state',r.state); end if;
  update public.event_search_assistant_operations set state='processing',claim_id=gen_random_uuid(),updated_at=now()
    where id=p_id returning * into r;
  return jsonb_build_object('claimed',true,'claim_id',r.claim_id);
end $$;

create or replace function public.event_search_assistant_checkpoint_v1(p_owner uuid,p_id uuid,p_claim uuid,p_state text,p_outcome jsonb default null,p_error text default null)
returns boolean language plpgsql security definer set search_path=pg_catalog as $$
begin
  if p_state not in ('dispatched','completed','failed','outcome_unknown','accepted') or (p_error is not null and p_error !~ '^[a-z0-9_]{1,80}$') then
    raise exception 'invalid_checkpoint' using errcode='22023';
  end if;
  update public.event_search_assistant_operations set
    state=case when p_state='dispatched' then 'processing' else p_state end,
    dispatched=case when p_state='dispatched' then true else dispatched end,
    outcome=case when p_state='completed' then p_outcome else outcome end,
    error_code=p_error,updated_at=now()
  where id=p_id and owner_id=p_owner and claim_id=p_claim and state='processing'
    and (p_state<>'accepted' or not dispatched);
  if not found then raise exception 'checkpoint_conflict' using errcode='40001'; end if;
  return true;
end $$;

create or replace function public.event_search_assistant_audio_part_v1(p_owner uuid,p_id uuid,p_index integer,p_first bigint,p_frames integer,p_rate integer,p_digest text,p_audio text)
returns boolean language plpgsql security definer set search_path=pg_catalog as $$
declare op public.event_search_assistant_operations; part public.event_search_assistant_audio_parts; bytes bytea;
begin
  select * into op from public.event_search_assistant_operations where id=p_id and owner_id=p_owner for update;
  if not found or op.kind<>'asr' then raise exception 'operation_not_found' using errcode='42501'; end if;
  bytes:=decode(p_audio,'base64');
  select * into part from public.event_search_assistant_audio_parts where operation_id=p_id and part_index=p_index;
  if found then
    if part.audio<>bytes or part.digest<>p_digest or part.first_frame<>p_first or part.frame_count<>p_frames or part.sample_rate<>p_rate then
      raise exception 'audio_payload_conflict' using errcode='23505';
    end if;
    return true;
  end if;
  if op.state<>'accepted' or p_index>=(op.payload->>'partCount')::integer or p_rate<>(op.payload->>'sampleRate')::integer
     or p_first+p_frames>(op.payload->>'frames')::bigint then raise exception 'audio_manifest_conflict' using errcode='22023'; end if;
  insert into public.event_search_assistant_audio_parts values(p_id,p_index,p_first,p_frames,p_rate,p_digest,bytes);
  return true;
end $$;

revoke all on function public.event_search_assistant_admit_v1(uuid,uuid,text,jsonb) from public,anon,authenticated;
revoke all on function public.event_search_assistant_claim_v1(uuid,uuid) from public,anon,authenticated;
revoke all on function public.event_search_assistant_checkpoint_v1(uuid,uuid,uuid,text,jsonb,text) from public,anon,authenticated;
revoke all on function public.event_search_assistant_audio_part_v1(uuid,uuid,integer,bigint,integer,integer,text,text) from public,anon,authenticated;
grant execute on function public.event_search_assistant_admit_v1(uuid,uuid,text,jsonb) to service_role;
grant execute on function public.event_search_assistant_claim_v1(uuid,uuid) to service_role;
grant execute on function public.event_search_assistant_checkpoint_v1(uuid,uuid,uuid,text,jsonb,text) to service_role;
grant execute on function public.event_search_assistant_audio_part_v1(uuid,uuid,integer,bigint,integer,integer,text,text) to service_role;
