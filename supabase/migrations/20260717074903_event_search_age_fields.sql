-- Keep the personalization pgvector projection compatible with the canonical
-- event age-rating fields added to the static event export.

alter table public.event_search_documents
  add column if not exists age_restriction text,
  add column if not exists age_restriction_status text not null default 'unknown';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.event_search_documents'::regclass
      and conname = 'event_search_documents_age_restriction_check'
  ) then
    alter table public.event_search_documents
      add constraint event_search_documents_age_restriction_check
      check (age_restriction is null or age_restriction in ('0+', '6+', '12+', '16+', '18+'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.event_search_documents'::regclass
      and conname = 'event_search_documents_age_restriction_status_check'
  ) then
    alter table public.event_search_documents
      add constraint event_search_documents_age_restriction_status_check
      check (
        age_restriction_status in (
          'declared',
          'assessed',
          'conflict',
          'insufficient_evidence',
          'unknown',
          'budget_deferred'
        )
      );
  end if;
end
$$;

comment on column public.event_search_documents.age_restriction is
  'Declared public event age restriction projected from the canonical event store.';
comment on column public.event_search_documents.age_restriction_status is
  'Canonical age-rating decision status; unknown for legacy/search rows.';

notify pgrst, 'reload schema';
