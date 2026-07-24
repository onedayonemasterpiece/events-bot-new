# Связанные события: единый контракт

> **Статус:** каноника для переноса во все ветки, где показываются связи между
> событиями. Production source of truth — `origin/main`; лабораторные ветки
> ниже дают требования и визуальные образцы, но не становятся production
> автоматически.

Цель фичи — одна модель данных и одно предсказуемое поведение на странице
события, в mobile feed, на списочных страницах, в Telegram/Telegraph и в
будущих персональных поверхностях.

## Главное решение: четыре публичных связи + duplicate review

| Канонический тип | Что означает | Публичная подпись | Нельзя смешивать с |
| --- | --- | --- | --- |
| `same_occurrence_group` | та же программа/активность в другое время или дату | **Другие даты** / **Другое время** | похожими событиями и дедупликацией |
| `semantic_related` | действительно похожее, но самостоятельное событие | **Похожие события** | другими датами и широкой афишей |
| `broad_discovery` | ограниченное продолжение выбора и anti-bubble | **Ещё события** | утверждением «похожие» |
| `personalized_feed` | выдача изменена совместимым профилем пользователя | **Для вас** / **По вашим интересам** | анонимным статическим fallback |

`duplicate_same_slot` — отдельный непубличный outcome: два rows с одной
программой и одним slot должны идти в merge/review, а не показываться как
«другая дата». Он не входит в четыре публичных relation surfaces.

`festival`, площадка, организатор, источник, рубрика и поисковая выдача — это
**membership/filter context**, а не relation edge. Они могут быть признаками
ранжирования, но не дают права называть два события «похожими» или «другими
датами».

## Единая цепочка данных

```text
Fly SQLite Event (канонические факты и lifecycle)
  ├─ linked_event_ids -> same_occurrence_group
  ├─ static export -> активный публичный каталог
  ├─ offline retrieval + quality gates -> semantic_related
  ├─ bounded diverse tail -> broad_discovery
  └─ consented compatible profile -> personalized_feed rerank/filter
       -> static HTML/JSON/card projection
       -> один card renderer и один interaction controller на всех surfaces
```

- Fly SQLite остаётся источником истины для идентичности, дат, статусов и
  `linked_event_ids`.
- Supabase/pgvector может хранить поисковые документы, embeddings, компактную
  выдачу и telemetry, но не становится источником истины о lifecycle события.
- LLM/vector/provider не вызываются в page-view hot path. Страница всегда
  полезна как статический fallback.
- Любая ветка должна потреблять единый результат связи, а не заново решать
  regex/эвристиками, что является другой датой или похожим событием.

Историческое исключение в runtime lineage `origin/main@3d0af26c`: frontend
`getLinkedSessionIds()` объединяет explicit `other_date_ids` с inference по
normalized title/type/venue/city. В текущей интеграции это удалено: совпадение
текста больше не создаёт family; family требует взаимных explicit links, а
неполная coverage остаётся честным migration gap исходной базы.

Практический canary этой границы — `6318` (2 ноября) и `6586` (3 ноября).
Компоненты и formatter уже поддерживают требуемую одну карточку
`2, 3 ноября 19:00`, включая разные площадки, но production/export snapshot на
2026-07-21 содержит для обеих строк пустые `linked_event_ids`. Поэтому текущая
раздельная выдача — data coverage gap, а не повод вернуть frontend inference.
Для focused review допустим только явно зафиксированный взаимный fixture edge;
перед общим rollout требуется source-grounded взаимная canonical repair.
Legacy `recompute_linked_event_ids()` требует одинаковую площадку и может снять
такую межплощадочную ручную связь, поэтому durable repair должен получить
manual provenance/lock либо целевой `occurrence_group_id`, а не голый
незащищённый edge.

## Каноническая frontend-проекция

Contract/donor `feature/related-events-compact-unified-20260721` выборочно
интегрирован в три слоя:

1. `site/src/lib/eventOccurrences.ts` — pure resolver взаимных explicit links,
   lifecycle/graph issues, slot grouping, compact/rail formatting и card
   collapse (`none` / `per-date` / `per-family`).
2. `EventOccurrenceLabel.astro` — одна подпись для больших и rail cards;
   `EventOccurrenceNav.astro` — один selector для event detail.
3. `EventCard`, `EventListItem`, detail/CTA и hydrated discovery получают готовую
   projection, но не определяют identity и не собирают даты локально.
4. `sync_event_search_vectors_to_supabase.py` проецирует тот же
   reciprocal-explicit family id и exact compact/a11y labels в
   `card_snapshot`; Edge Function до LLM/final response сворачивает выдачу
   по этому family key. Так live authorized search остаётся
   `per-family`, а не только статический fallback.
   Материализованный DTO содержит точные `occurrence_member_ids`, compact label
   и полный `aria-label`; Edge Function сначала сворачивает полный ranked window,
   затем применяет logical pagination, повторяет collapse после LLM rerank и
   переносит тот же family seen-set в fallback. Первый/highest-ranked member
   остаётся representative; malformed one-way/dangling metadata fail closed.

Канонические примеры: `2, 9 ноября 19:00` и
`4 ноября 17:00, 19:00`. Rail показывает ту же projection в две строки и один
полный `aria-label`. Конкретная date-list сворачивает family только внутри даты;
entity/ranked surface оставляет одного representative на family. Полная матрица
и fail-closed правила — `REL-045`–`REL-048`.

## Поверхности

| Surface | Обязательное поведение |
| --- | --- |
| Статическая страница события | `Другие даты` рядом с датой/CTA; ниже контента конечный блок `Похожие события`; отдельный `Ещё события` допустим только как честно отделённый broad tail. |
| Mobile event detail | те же relation types и порядок; cards идут вертикально, без бесконечного скролла и без дублирующего второго блока. |
| Mobile feed / «Для меня» | использует тот же card/feedback/profile contract; скрывает siblings одной occurrence group после явного negative action, но не называет всю ленту «похожей». |
| `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`, категории | основной SEO-список не меняется; персональная часть — progressive enhancement, конечная и дедуплицированная с уже показанными событиями. |
| Telegraph/бот | компактная строка `Другие даты`; тот же `linked_event_ids`, сортировка и lifecycle-фильтры. |
| Галерея события | финальный CTA может вести на первый `semantic_related`, но не маскируется под фотографию и подписан `Смотреть похожее`. |

## Что является каноникой

1. [Единые требования и acceptance checklist](requirements.md) — документ,
   который нужно передавать во все implementation branches.
2. [Инвентаризация production, старых реализаций и лабораторий](inventory.md) —
   откуда взяты решения и какие ветки нельзя принимать за production.
3. [Визуальные паттерны и скриншоты](visual-patterns.md) — принятые механики,
   экспериментальные варианты и их provenance.
4. [Передача решения в рабочие ветки](handoff.md) — donor branch, короткий
   prompt, integration order и обязательные проверки.

Подробные subordinate contracts остаются на своих местах:

- event-detail ranking/personalization:
  `docs/features/unsigned-personalization/event-detail-related.md`;
- retrieval/cache/quality:
  `docs/features/unsigned-personalization/semantic-vector-retrieval.md`;
- listing continuation:
  `docs/features/static-site-pages/listing-personal-feed.md`;
- page/card/media behavior:
  `docs/features/static-site-pages/README.md`.

При расхождении терминов или поведения эти документы должны быть приведены к
этому umbrella contract; нельзя создавать ещё один параллельный «related»
словарь.

## Уже работающая legacy-механика `linked_event_ids`

Legacy-каноническая функция
`linked_events.recompute_linked_event_ids(db, event_id)`
связывает только однодневные события, требует точного `location_name` и строгого
совпадения title. Intended recompute invariant: связь симметрична, self-link
запрещён, все члены пересчитанной группы получают один состав siblings.
Это не гарантия качества всех legacy rows: production-derived snapshot содержит
one-way/dangling/same-slot defects, а некоторые early-return paths не очищают
старые edges.

Пересчёт вызывается после Smart Update create/merge и правок
`title/location_name/date/time`, в source parsing и в ручном UI. Изменённые
siblings получают `telegraph_build`. Same-day Telegram/VK публикация выбирает
детерминированный anchor (раннее время, затем меньший `event.id`) и показывает
все времена дня.

Это production foundation, но не целевая модель: стабильный target —
`occurrence_group_id` + relation kind/provenance/version/confidence/review/manual
lock, а `linked_event_ids` становится совместимым derived projection. До этой
миграции нельзя изобретать группу на frontend по title/type/venue.

Текущий foundation также не полный продуктовый контракт: статическая
страница должна ещё применять lifecycle-фильтры, переключать выбранную дату со
всеми зависимыми CTA/calendar/source данными и не отправлять siblings в блок
`semantic_related`.

### Cross-month compact labels

The formatter groups reciprocal occurrence dates by year and month instead of
repeating the month for every date. Thus a same-time family renders
`24, 25 июля, 27 сентября 19:00`; the two-line rail date is
`24, 25 июл, 27 сен`, with the full grouped schedule in `aria-label`. The
existing exact contracts `2, 9 ноября 19:00` and
`4 ноября 17:00, 19:00` remain unchanged. This formatting change does not relax
family identity: only reciprocal explicit `other_date_ids` may form a family.
