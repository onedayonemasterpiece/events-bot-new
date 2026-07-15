# Linked Events (Связанные события)

> Статус: **core relation + Telegraph «Другие даты» реализованы; static event-detail preview частичный; явное отображение альтернативных дат/времени внутри всех event cards отсутствует и является обязательным release blocker M6**.

Одна и та же активность (мастер‑класс, лекция, спектакль) часто публикуется как несколько отдельных событий по разным датам. Цель этой фичи: чтобы на странице одного события было понятно, что у него есть **другие доступные даты**.

В release terminology это «мультисобытие» или **occurrence group**: одна программа,
спектакль, экскурсия, концерт или иная активность имеет несколько подтверждённых
сеансов в разные даты и/или время. Это не:

- source post с несколькими разными программами;
- многодневное событие, выраженное одним диапазоном;
- похожие события F2;
- duplicate rows одного и того же реального сеанса — такие строки должны merge,
  а не маскироваться как «другая дата».

## Данные

- `Event.linked_event_ids: list[int]` — список `event_id` других “вхождений” того же события.
- Инвариант: связь **симметрична**. Если `A` ссылается на `B`, то `B` ссылается на `A`. `linked_event_ids` никогда не содержит `self`.

## Алгоритм связывания (deterministic)

Каноническая функция: `linked_events.recompute_linked_event_ids(db, event_id)`.

Правила (намеренно строгие, чтобы избежать ложных “Другие даты”):

- связываем только **однодневные** события: не `date` в формате диапазона `YYYY-MM-DD..YYYY-MM-DD` и без `end_date`;
- `location_name` должен совпадать **строго**;
- `title` должен совпадать по строгому matcher’у (нормализация + шумовые токены + `SequenceMatcher >= 0.90`);
- кандидаты берутся в окне вокруг даты события:
  - `LINKED_EVENTS_PAST_DAYS` (default `120`)
  - `LINKED_EVENTS_FUTURE_DAYS` (default `365`)
  - `LINKED_EVENTS_MAX_CANDIDATES` (default `800`)
  - `LINKED_EVENTS_MAX_GROUP_SIZE` (default `120`)

Результат применяется симметрично: всем событиям группы выставляется одинаковый список ссылок (минус self), а устаревшие ссылки удаляются.

## Где пересчитывается (потоки)

- Smart Update: после `create/merge`, а также при изменении `title/location_name/date/time`.
- Source parsing (`/parse` и парсеры сайтов): через `source_parsing.handlers.update_linked_events(...)`.
- Ручное редактирование через UI бота: при правках `title/location_name/date/time`.

Если пересчёт изменил `linked_event_ids` у других событий группы, они получают задачу `telegraph_build`, чтобы публичный инфоблок обновился.

## Публикация Telegram/VK

Если несколько связанных вхождений попадают на одну дату и отличаются только временем, наружные event-посты в Telegram и VK публикуются одним якорным постом. Якорь выбирается детерминированно: самое раннее время в этот день, затем меньший `event.id`. В строке времени якорного поста показываются все времена дня, например `14:00 и 16:00`.

Job, пришедший по любому sibling-событию same-day группы, публикует/переиспользует якорный пост и затем сохраняет ссылку на него в остальных sibling rows. Это не требует нового поля или ручного объединения событий; источник истины остаётся `linked_event_ids`.

## UI: “Другие даты” на Telegraph‑странице

При сборке Telegraph‑страницы события (`update_telegraph_event_page`) в `SourcePageEventSummary.other_dates` добавляются ближайшие связанные вхождения, а `_build_source_summary_block` рендерит строку:

`🗓 Другие даты: 12 марта 10:30 · 26 марта 10:30 · …`

Особенности:

- показывается до 6 дат (окно вокруг текущего события), остаток: `и ещё N`;
- отмена/перенос помечаются `❌` / `⏸`;
- для `event_type=выставка` и для событий‑диапазонов блок скрывается (там “много дат” уже выражено периодом).

## M6: обязательный static-site release contract

Первый публичный релиз должен сохранить уже существующую Telegraph-возможность и
сделать её видимой **не только после открытия detail page, но прямо в карточке**.
Если у текущего occurrence есть другие eligible сеансы той же программы, каждая
публичная карточка должна показать, что доступны другие дата и/или время.

### Current static gap

Core `Event.linked_event_ids` и симметричный recompute существуют, Telegraph
показывает `Другие даты`, а Astro detail имеет отдельный блок. Но production static
export сейчас hardcode-ит `other_date_ids: []`; `site/src/lib/events.ts` пытается
восстановить связи client/build-time эвристикой по title/type/venue/city, а shared
`EventCard`/`EventListItem` не выводят альтернативные slots пользователю. Это
нельзя считать надёжной поддержкой связанных событий.

### Data and Smart Update invariants

- источник истины V1 — канонический симметричный `Event.linked_event_ids`; static
  exporter переносит его как `other_date_ids` без потерь, self ids и dangling ids;
- одинаковый реальный slot `(program identity, date, time, place)` является
  duplicate/merge case, а разные подтверждённые date/time той же программы — link;
- deterministic title/location matching допустим как candidate retrieval, но
  неоднозначное semantic identity решение остаётся LLM-first в Smart Update и
  использует source/program/organizer/place evidence; uncertainty fails closed;
- связь с похожим названием, но другой программой, составом, фестивальным пунктом
  или возрастной версией не создаётся автоматически;
- create/update/merge/split/cancel/postpone и изменение title/place/date/time
  пересчитывают группу симметрично; изменение группы является effectful и попадает
  в общий F1 `+15 min` static rebuild, а не обновляет только Telegraph;
- ежедневный lifecycle build убирает прошедшие и недоступные alternatives без
  ожидания нового Smart Update.

### Card behavior

- основная дата/время карточки остаётся датой конкретного canonical occurrence;
- рядом видна компактная статическая строка/chips вида `Ещё: сегодня 18:00 ·
  20 июля 19:00` либо `Ещё 3 даты`; точный visual pattern фиксируется в F5 UI
  freeze, но сам факт наличия альтернатив нельзя скрыть до detail page;
- same-day siblings могут быть схлопнуты в одну карточку только если карточка
  показывает все доступные времена этого дня либо явный доступный `ещё N` control;
- different-day siblings показывают ближайшие future dates/times и общее число;
  полный список доступен в detail-блоке `Другие даты`;
- alternative slot ведёт на canonical URL соответствующего occurrence. Его ICS,
  favorite/calendar state, D-1 reminder, ticket/status and transport facts остаются
  occurrence-specific; выбор другой даты не должен сохранять исходный slot;
- cancelled/postponed/sold-out alternatives не выдаются за доступные: они либо
  исключены, либо явно маркированы по единой lifecycle policy;
- блок отделён от `Похожие события`, не влияет на recommendation count и не
  создаёт duplicate card impressions;
- no-JS HTML показывает минимум ближайшие alternatives и crawlable links; mobile
  не зависит от hover, keyboard/screen-reader получает понятные labels, card-wide
  navigation не создаёт nested interactive controls.

Обязательные surfaces: homepage, date/category/city/tag/popular listings, search,
personal feeds/pages, favorites, festival/transport listings, `EventCard`,
`EventListItem` и event detail. Email с event cards использует тот же projection,
если его layout показывает карточку события.

### Release acceptance

- [ ] `other_date_ids` в static artifact совпадает с eligible canonical
  `linked_event_ids` для 100% active/future events; heuristic inference не является
  source of truth и не создаёт false groups;
- [ ] graph audit: zero self/dangling/asymmetric links, zero same-slot duplicates,
  zero confirmed false «Другие даты» и zero linked occurrences inside F2 related;
- [ ] shared card formatter/component отображает alternatives на всех перечисленных
  surfaces при mobile/tablet/desktop и no-JS;
- [ ] Playwright fixtures покрывают same-day multiple times, different dates,
  mixed date+time, one/no alternative, past, cancelled, postponed, sold-out,
  duplicate same slot, false similar title, long group and timezone boundary;
- [ ] выбор alternative открывает правильный canonical URL и использует правильные
  occurrence-specific ICS/favorite/reminder/ticket/transport facts;
- [ ] `check:preview` падает при canonical→export loss, невидимой карточной метке,
  broken link, past unavailable slot или попадании sibling в `Похожие`;
- [ ] RC evidence содержит full-catalog parity ledger, representative screenshots
  всех card families и replay известных linked-event/duplicate incidents.
