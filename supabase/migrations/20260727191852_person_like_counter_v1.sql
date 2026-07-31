-- Cross-event, cross-device likes for verified people.
-- One compact state row per (user, person), one aggregate row per person,
-- and no append-only interaction log.

create table if not exists public.personalization_person_like_subject (
  person_id text primary key
    check (
      length(person_id) between 3 and 128
      and person_id ~ '^[a-z0-9][a-z0-9:_-]+$'
    ),
  active boolean not null default true,
  source_revision text,
  updated_at timestamptz not null default now()
);
create table if not exists public.personalization_person_like_counter (
  person_id text primary key
    references public.personalization_person_like_subject(person_id)
    on update cascade
    on delete cascade,
  likes_count integer not null default 0 check (likes_count >= 0),
  updated_at timestamptz not null default now()
);
create table if not exists public.personalization_person_like_state (
  person_id text not null
    references public.personalization_person_like_subject(person_id)
    on update cascade
    on delete cascade,
  user_id uuid not null
    references auth.users(id)
    on delete cascade,
  created_at timestamptz not null default now(),
  primary key (person_id, user_id)
);
create index if not exists personalization_person_like_state_user_idx
  on public.personalization_person_like_state(user_id, person_id);
alter table public.personalization_person_like_subject enable row level security;
alter table public.personalization_person_like_counter enable row level security;
alter table public.personalization_person_like_state enable row level security;
revoke all on public.personalization_person_like_subject from anon, authenticated;
revoke all on public.personalization_person_like_counter from anon, authenticated;
revoke all on public.personalization_person_like_state from anon, authenticated;
grant all on public.personalization_person_like_subject to service_role;
grant all on public.personalization_person_like_counter to service_role;
grant all on public.personalization_person_like_state to service_role;
create or replace function public.maintain_person_like_counter_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
begin
  if tg_op = 'INSERT' then
    insert into public.personalization_person_like_counter(
      person_id,
      likes_count,
      updated_at
    )
    values (new.person_id, 1, now())
    on conflict (person_id) do update
      set likes_count =
            public.personalization_person_like_counter.likes_count + 1,
          updated_at = now();
    return new;
  end if;

  update public.personalization_person_like_counter
     set likes_count = greatest(0, likes_count - 1),
         updated_at = now()
   where person_id = old.person_id;
  return old;
end;
$$;
revoke all on function public.maintain_person_like_counter_v1()
  from public, anon, authenticated;
drop trigger if exists personalization_person_like_state_counter_trg
  on public.personalization_person_like_state;
create trigger personalization_person_like_state_counter_trg
after insert or delete on public.personalization_person_like_state
for each row execute function public.maintain_person_like_counter_v1();
create or replace function public.get_person_like_snapshot_v1(
  p_person_ids text[]
)
returns table (
  person_id text,
  likes_count integer,
  liked boolean
)
language sql
stable
security definer
set search_path = ''
as $$
  with requested as (
    select distinct btrim(item.person_id) as person_id
      from unnest(coalesce(p_person_ids, array[]::text[]))
           with ordinality as item(person_id, position)
     where item.position <= 64
       and length(btrim(item.person_id)) between 3 and 128
       and btrim(item.person_id) ~ '^[a-z0-9][a-z0-9:_-]+$'
  )
  select
    requested.person_id,
    coalesce(counter.likes_count, 0)::integer as likes_count,
    (
      (select auth.uid()) is not null
      and exists (
        select 1
          from public.personalization_person_like_state state
         where state.person_id = requested.person_id
           and state.user_id = (select auth.uid())
      )
    ) as liked
    from requested
    join public.personalization_person_like_subject subject
      on subject.person_id = requested.person_id
     and subject.active
    left join public.personalization_person_like_counter counter
      on counter.person_id = requested.person_id;
$$;
revoke all on function public.get_person_like_snapshot_v1(text[])
  from public, anon, authenticated;
grant execute on function public.get_person_like_snapshot_v1(text[])
  to anon, authenticated;
create or replace function public.set_person_like_v1(
  p_person_id text,
  p_liked boolean
)
returns table (
  person_id text,
  likes_count integer,
  liked boolean
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_person_id text := btrim(coalesce(p_person_id, ''));
begin
  if v_user_id is null then
    raise exception 'authentication_required'
      using errcode = '42501';
  end if;
  if p_liked is null then
    raise exception 'liked_state_required'
      using errcode = '22023';
  end if;
  if length(v_person_id) not between 3 and 128
     or v_person_id !~ '^[a-z0-9][a-z0-9:_-]+$'
     or not exists (
       select 1
         from public.personalization_person_like_subject subject
        where subject.person_id = v_person_id
          and subject.active
     )
  then
    raise exception 'unknown_person'
      using errcode = '22023';
  end if;

  if p_liked then
    insert into public.personalization_person_like_state(person_id, user_id)
    values (v_person_id, v_user_id)
    on conflict (person_id, user_id) do nothing;
  else
    delete from public.personalization_person_like_state
     where personalization_person_like_state.person_id = v_person_id
       and personalization_person_like_state.user_id = v_user_id;
  end if;

  return query
  select
    v_person_id,
    coalesce(counter.likes_count, 0)::integer,
    exists (
      select 1
        from public.personalization_person_like_state state
       where state.person_id = v_person_id
         and state.user_id = v_user_id
    )
    from public.personalization_person_like_subject subject
    left join public.personalization_person_like_counter counter
      on counter.person_id = subject.person_id
   where subject.person_id = v_person_id
     and subject.active;
end;
$$;
revoke all on function public.set_person_like_v1(text, boolean)
  from public, anon, authenticated;
grant execute on function public.set_person_like_v1(text, boolean)
  to authenticated;
insert into public.personalization_person_like_subject(
  person_id,
  active,
  source_revision,
  updated_at
)
select
  seed.person_id,
  true,
  '1feacd2bca4f8226',
  now()
from jsonb_array_elements_text(
  '[
    "kgd80:aleksandr-nikolaevich-popadin",
    "kgd80:aleksey-sokolov",
    "kgd80:anastasiya-skrebtsova",
    "kgd80:andrey-aleksandrovich-anisimov",
    "kgd80:andrey-anatolevich-yartsev",
    "kgd80:andrey-boyko",
    "kgd80:andrey-viktorovich-levchenkov",
    "kgd80:artur-arturovich-sarnits",
    "kgd80:valeriya-yurevna-nadymova",
    "kgd80:vladimir-andreevich-chechko",
    "kgd80:vladimir-oleksin",
    "kgd80:gennadiy-viktorovich-kretinin",
    "kgd80:dmitriy-vladimirovich-mankevich",
    "kgd80:evgeniy-mosienko",
    "kgd80:evgeniya-nizhegorodtseva",
    "kgd80:ekaterina-mashinskaya",
    "kgd80:ekaterina-mihaylovna-ilyushkina",
    "kgd80:igor-selin",
    "kgd80:inga-dolotova",
    "kgd80:irina-sergeevna-litvinovich",
    "kgd80:larisa-aleksandrovna-bystrova",
    "kgd80:leonid-efremov",
    "kgd80:liliya-finkova",
    "kgd80:mihail-zhorzhevich-tsedrik",
    "kgd80:mihail-markovets",
    "kgd80:natalya-kazakova",
    "kgd80:natalya-konstantinovna-krimmel",
    "kgd80:natalya-mihaylovna-sitnikova",
    "kgd80:nikita-sergeevich-nikitin",
    "kgd80:nikolay-perkusov",
    "kgd80:olga-levkova",
    "kgd80:svetlana-gennadevna-sivkova",
    "kgd80:svetlana-kolbaneva",
    "kgd80:svetlana-sokolova",
    "kgd80:sergey-zhadobko",
    "kgd80:tatyana-konyuhova",
    "kgd80:tatyana-udovenko",
    "kgd80:shahnoza-muhitdinovna-usmanova"
  ]'::jsonb
) as seed(person_id)
on conflict (person_id) do update
  set active = excluded.active,
      source_revision = excluded.source_revision,
      updated_at = excluded.updated_at;
insert into public.personalization_person_like_counter(person_id)
select subject.person_id
  from public.personalization_person_like_subject subject
 where subject.active
on conflict (person_id) do nothing;
