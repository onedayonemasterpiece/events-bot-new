-- Idempotent, bounded feedback writes for the resilient browser outbox.
-- The original v1 RPC stays available during rollout; new clients use v2.

alter table personalization.focus_group_feedback
  add column if not exists client_request_id uuid;

update personalization.focus_group_feedback
set client_request_id = id
where client_request_id is null;

alter table personalization.focus_group_feedback
  alter column client_request_id set default gen_random_uuid(),
  alter column client_request_id set not null;

create unique index if not exists focus_group_feedback_user_request_uidx
  on personalization.focus_group_feedback (user_id, client_request_id);

create or replace function public.submit_focus_group_feedback_v2(
  p_client_request_id uuid,
  p_kind text,
  p_page_family text,
  p_page_path text,
  p_score smallint default null,
  p_message text default null,
  p_attachment_path text default null
)
returns uuid
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_id uuid;
  v_message text := nullif(btrim(coalesce(p_message, '')), '');
  v_attachment text := nullif(btrim(coalesce(p_attachment_path, '')), '');
  v_recent_count integer;
begin
  if v_user_id is null then
    raise exception 'authentication_required' using errcode = '28000';
  end if;
  if p_client_request_id is null then
    raise exception 'client_request_id_required' using errcode = '22023';
  end if;

  select f.id into v_id
  from personalization.focus_group_feedback f
  where f.user_id = v_user_id
    and f.client_request_id = p_client_request_id;
  if v_id is not null then
    return v_id;
  end if;

  if p_kind not in ('page_score', 'issue') then
    raise exception 'invalid_feedback_kind' using errcode = '22023';
  end if;
  if p_page_family is null
     or char_length(p_page_family) not between 1 and 48
     or p_page_family !~ '^[a-z][a-z0-9_]*$' then
    raise exception 'invalid_page_family' using errcode = '22023';
  end if;
  if p_page_path is null
     or char_length(p_page_path) not between 1 and 500
     or p_page_path not like '/%' then
    raise exception 'invalid_page_path' using errcode = '22023';
  end if;
  if p_kind = 'page_score'
     and (
       p_score is null
       or p_score not between 0 and 10
       or v_message is not null
       or v_attachment is not null
     ) then
    raise exception 'invalid_page_score' using errcode = '22023';
  end if;
  if p_kind = 'issue'
     and (
       p_score is not null
       or v_message is null
       or char_length(v_message) > 2000
     ) then
    raise exception 'invalid_issue_report' using errcode = '22023';
  end if;
  if v_attachment is not null
     and (
       char_length(v_attachment) not between 38 and 300
       or split_part(v_attachment, '/', 1) <> v_user_id::text
     ) then
    raise exception 'invalid_attachment_path' using errcode = '22023';
  end if;

  -- Bound deliberate abuse even when a valid session is available. Replaying
  -- the same request id is free and returns above without consuming the cap.
  select count(*)::integer into v_recent_count
  from personalization.focus_group_feedback f
  where f.user_id = v_user_id
    and f.created_at >= statement_timestamp() - interval '24 hours';
  if v_recent_count >= 120 then
    raise exception 'feedback_daily_limit_exceeded' using errcode = '54000';
  end if;

  insert into personalization.focus_group_feedback (
    user_id,
    client_request_id,
    kind,
    page_family,
    page_path,
    score,
    message,
    attachment_path
  )
  values (
    v_user_id,
    p_client_request_id,
    p_kind,
    p_page_family,
    p_page_path,
    p_score,
    case when p_kind = 'issue' then v_message end,
    case when p_kind = 'issue' then v_attachment end
  )
  on conflict (user_id, client_request_id) do update
    set client_request_id = excluded.client_request_id
  returning id into v_id;

  return v_id;
end;
$$;

revoke all on function public.submit_focus_group_feedback_v2(
  uuid, text, text, text, smallint, text, text
) from public, anon, authenticated;
grant execute on function public.submit_focus_group_feedback_v2(
  uuid, text, text, text, smallint, text, text
) to authenticated;
grant execute on function public.submit_focus_group_feedback_v2(
  uuid, text, text, text, smallint, text, text
) to service_role;

notify pgrst, 'reload schema';
