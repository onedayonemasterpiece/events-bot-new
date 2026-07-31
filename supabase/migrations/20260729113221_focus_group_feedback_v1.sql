-- Authenticated focus-group feedback.
--
-- The browser can only call the narrow RPC below. Raw feedback stays in a
-- private schema and screenshots stay in an owner-scoped private bucket.

create schema if not exists personalization;
revoke all on schema personalization from public, anon, authenticated;
grant usage on schema personalization to service_role;

create table personalization.focus_group_feedback (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,
  kind text not null check (kind in ('page_score', 'issue')),
  page_family text not null check (
    char_length(page_family) between 1 and 48
    and page_family ~ '^[a-z][a-z0-9_]*$'
  ),
  page_path text not null check (
    char_length(page_path) between 1 and 500
    and page_path like '/%'
  ),
  score smallint,
  message text,
  attachment_path text,
  created_at timestamptz not null default statement_timestamp(),
  constraint focus_group_feedback_shape_chk check (
    (
      kind = 'page_score'
      and score between 0 and 10
      and message is null
      and attachment_path is null
    )
    or (
      kind = 'issue'
      and score is null
      and char_length(message) between 1 and 2000
      and (
        attachment_path is null
        or char_length(attachment_path) between 38 and 300
      )
    )
  )
);

comment on table personalization.focus_group_feedback is
  'Authenticated page ratings and problem reports from active focus-group participants.';

alter table personalization.focus_group_feedback enable row level security;
revoke all on personalization.focus_group_feedback from public, anon, authenticated;
grant select, insert, update, delete on personalization.focus_group_feedback to service_role;

create index focus_group_feedback_created_idx
  on personalization.focus_group_feedback (created_at desc);
create index focus_group_feedback_surface_idx
  on personalization.focus_group_feedback (page_family, created_at desc);

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'focus-feedback',
  'focus-feedback',
  false,
  5242880,
  array['image/png', 'image/jpeg', 'image/webp']::text[]
)
on conflict (id) do update
set
  public = false,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;

create policy "focus feedback screenshots are uploadable by their owner"
  on storage.objects
  for insert
  to authenticated
  with check (
    bucket_id = 'focus-feedback'
    and (storage.foldername(name))[1] = (select auth.uid())::text
  );

create policy "focus feedback screenshots are readable by their owner"
  on storage.objects
  for select
  to authenticated
  using (
    bucket_id = 'focus-feedback'
    and (storage.foldername(name))[1] = (select auth.uid())::text
  );

create policy "focus feedback screenshots are removable by their owner"
  on storage.objects
  for delete
  to authenticated
  using (
    bucket_id = 'focus-feedback'
    and (storage.foldername(name))[1] = (select auth.uid())::text
  );

create or replace function public.submit_focus_group_feedback_v1(
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
begin
  if v_user_id is null then
    raise exception 'authentication_required' using errcode = '28000';
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

  insert into personalization.focus_group_feedback (
    user_id,
    kind,
    page_family,
    page_path,
    score,
    message,
    attachment_path
  )
  values (
    v_user_id,
    p_kind,
    p_page_family,
    p_page_path,
    p_score,
    case when p_kind = 'issue' then v_message end,
    case when p_kind = 'issue' then v_attachment end
  )
  returning id into v_id;

  return v_id;
end;
$$;

revoke all on function public.submit_focus_group_feedback_v1(
  text, text, text, smallint, text, text
) from public, anon, authenticated;
grant execute on function public.submit_focus_group_feedback_v1(
  text, text, text, smallint, text, text
) to authenticated;
grant execute on function public.submit_focus_group_feedback_v1(
  text, text, text, smallint, text, text
) to service_role;

notify pgrst, 'reload schema';
