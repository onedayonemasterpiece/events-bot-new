create extension if not exists pgcrypto;

create table if not exists public.event_issue_reports (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  processed_at timestamptz,
  status text not null default 'submitted' check (status in ('submitted','processing','queued','done','failed','ignored')),
  event_id bigint not null,
  static_event_id bigint,
  event_slug text not null default '',
  event_title text not null default '',
  event_url text,
  source_url text,
  source_urls jsonb not null default '[]'::jsonb,
  telegraph_url text,
  event_date text,
  event_time text,
  venue_name text,
  address text,
  city text,
  report_text text not null,
  reported_by_user_id uuid not null,
  reporter_email text,
  reporter_provider text,
  reporter_metadata jsonb not null default '{}'::jsonb,
  artkodex_task_id text,
  artkodex_thread_url text,
  processing_error text
);

create index if not exists event_issue_reports_status_created_idx on public.event_issue_reports(status, created_at);
create index if not exists event_issue_reports_event_id_idx on public.event_issue_reports(event_id);

alter table public.event_issue_reports enable row level security;
revoke all on public.event_issue_reports from anon, authenticated;
grant select, insert, update on public.event_issue_reports to service_role;

create or replace function public.touch_event_issue_reports_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_event_issue_reports_updated_at on public.event_issue_reports;
create trigger trg_event_issue_reports_updated_at
before update on public.event_issue_reports
for each row execute function public.touch_event_issue_reports_updated_at();
