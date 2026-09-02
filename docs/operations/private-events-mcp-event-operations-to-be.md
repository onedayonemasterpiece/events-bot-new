# EventsBot MCP: добавление событий, редактирование, промо и статусы публикаций — TO-BE

> **Статус:** owner-confirmed TO-BE, revision 2; runtime ещё не реализован.  
> **Дата решения:** 2 сентября 2026 года.  
> **Последнее уточнение владельца:** конфликтующий с фактами exact-текст запрещён; обязательны typed-операции переноса и отмены с обоснованием организатора.  
> **Канонический владелец:** `events-bot-new`.  
> **Источник исходного решения:** IdeaHub voice intake `voice-20260902-154844-651facb3`.  
> **Текущий AS-IS MCP-контракт:** [`private-events-mcp.md`](private-events-mcp.md).  
> **Release-blocking test matrix:** [`private-events-mcp-event-operations-scenarios.v1.yml`](../testing/private-events-mcp-event-operations-scenarios.v1.yml).  
> **Связанные реализованные контракты:** [Smart Update](../features/smart-event-update/README.md), [Promo Campaigns](../features/promo-campaigns/README.md), [Партнёрское промо](../features/promo-campaigns/partner-promo.md), [Event Media](../features/event-media/README.md).

Этот документ расширяет Private Events MCP предметными операциями над
каноническими событиями. Он не объявляет новые tools уже работающими. До
реализации, тестов, exact-main deploy и live readback текущим фактом остаётся
AS-IS-контракт из `private-events-mcp.md`.

## 1. Принятое архитектурное решение

### 1.1 Один EventsBot MCP, несколько защищённых проекций

Создаётся **не два независимых MCP-сервиса**, а один EventsBot MCP runtime внутри
существующего процесса `events-bot`:

- одна предметная реализация;
- одна каноническая event DB;
- один Smart Update;
- один `JobOutbox` и один набор workers;
- одна модель `promo_campaign / promo_target / promo_activity / promo_exposure`;
- один audit trail.

Над общим runtime существуют разные OAuth resource/audience и каталоги tools:

1. **owner/operator projection** — текущая ChatGPT/OpenCode поверхность с
   внутренними read-tools и разрешёнными owner mutations;
2. **partner projection** — отдельный audience `events-partner`, отдельный
   closed-allowlist tools и обязательные `principal + organization + tenant`
   bindings;
3. **Codex projection** — существующая read-only поверхность, без event/promo
   mutations.

Разные endpoint/resource projections нужны для token isolation, безопасного
`tools/list` и fail-closed tenant filtering. Они не создают второй backend,
вторую БД или альтернативный публикационный конвейер.

### 1.2 Общие commands, разные права

Owner и партнёр используют одинаковые application commands и одинаковые
предметные schemas там, где совпадает операция. Решение о выполнении принимает
policy layer по:

- audience и OAuth client;
- principal;
- organization/tenant binding;
- роли;
- владению событием/кампанией;
- состоянию объекта;
- entitlement/лимиту;
- требуемому editorial или spend approval.

Существующий `User.is_partner` остаётся допустимым UI-признаком Telegram-бота,
но не является достаточной security boundary для внешнего partner MCP.

## 2. Что уже существует и должно переиспользоваться

| Область | AS-IS | TO-BE изменение |
|---|---|---|
| Чтение события | `events_search`, `event_get`; Event 360 уже включает sources, publications и `publication_jobs` | Сохранить без дублирования |
| Чтение job | `fetch(id="job:<id>")` | Добавить предметный list/detail API и role-safe projection |
| Общая диагностика | `operations_snapshot` | Оставить owner-only; партнёру не раскрывать internal operations |
| Создание события | Telegram/manual intake вызывает `add_events_from_text` и Smart Update | Выставить тот же путь как typed MCP operation |
| Обработка события | Smart Update владеет identity, provenance, facts, merge/create и accepted `event_id` | Обход Smart Update запрещён |
| Обычные публикации | `schedule_event_update_tasks()` создаёт стандартный fan-out в `JobOutbox` | MCP не создаёт альтернативную очередь |
| Lifecycle из источников | `LifecycleActionType` уже содержит `CANCEL`, `POSTPONE`, `RESCHEDULE_DATE`, `RESCHEDULE_TIME`, `UPDATE_DETAILS`; VK intake умеет применять часть этих действий | Вынести мутацию в общий typed event-change service; MCP не вызывает VK-specific fuzzy matcher |
| Промо | MVP уже реализован через `promo_campaign`, `promo_target`, `promo_activity`, `promo_exposure` | Выставить capability/read/write projection поверх существующей модели |
| Партнёрское промо | Phase A уже использует общую promo-модель и owner checks | Добавить OAuth/tenant policy, не новую promo-модель |
| Social workspace | `social_action_*` работает с конкретными provider actions | Не использовать его для записи `Event` или создания promo campaign |

Существующий lifecycle код является полезной основой, но не готовым MCP write
API. История уже содержит серьёзные инциденты неправильного выбора события при
переносе/отмене. Поэтому MCP mutation всегда принимает точный `event_id`; title,
date, time и location используются как проверяемые anchors, но не выбирают
объект мутации через эвристический поиск.

## 3. Добавление события

### 3.1 Неподвижное правило

**Любое новое каноническое событие проходит полный Smart Update.**

Запрещены:

- прямой `INSERT Event` из MCP;
- отдельный упрощённый MCP parser/persist pipeline;
- создание публикационных jobs до accepted Smart Update outcome;
- выдача diagnostic/retry ID как готового `event_id`.

Accepted outcomes остаются каноническими outcomes Smart Update:

- `CREATED`;
- `MERGED`;
- `NOOP_EXACT_REPLAY`.

Только accepted outcome возвращает пригодный для следующих действий `event_id`.
`RETRY_SCHEDULED`, `FAILED_TECHNICAL` и product rejection не разрешают promo или
publication mutations.

### 3.2 MCP tools

#### `event_create_prepare`

Готовит одно добавление из сырого источника, но не создаёт `Event` и не запускает
provider publications.

Минимальный input:

```json
{
  "raw_text": "Полный исходный текст",
  "source": {
    "type": "owner_message | partner_message | source_url",
    "external_id": "устойчивый id исходника",
    "url": null
  },
  "text_policy": "smart_rewrite",
  "owner_comment": null,
  "idempotency_key": "client-generated-stable-key"
}
```

`text_policy`:

- `smart_rewrite` — default; публичные `description`, `short_description` и
  `search_digest` формируются существующим fact-first Smart Update;
- `preserve_original` — только по явному указанию; Smart Update всё равно
  полностью выполняет identity, dedup, provenance, facts и media gates, после
  чего исходный согласованный текст может стать атрибутированным manual
  public-description override **только после прохождения hard fact-consistency
  gate**. Он не заменяет `source_text` и не отменяет structured facts.

Prepare выполняет upstream source parsing один раз, возвращает замороженные
candidate previews, missing/ambiguous fields, read-only duplicate shortlist,
выбранную text policy и `preparation_ref + action_digest + expires_at`. Этот
результат затем используется commit без повторного свободного толкования
исходного текста.

Один source может содержать несколько событий. Prepare возвращает массив
candidates и требует явного выбора/подтверждения, если разбиение неоднозначно.

#### `event_create_commit`

Принимает только `preparation_ref`, `action_digest` и `idempotency_key`. Он:

1. проверяет неизменность frozen preparation и актуальные права;
2. создаёт durable event operation;
3. передаёт каждый подтверждённый candidate в существующий Smart Update;
4. сохраняет Event, EventSource, facts, identity decision и audit по обычному
   каноническому пути;
5. только для accepted events вызывает существующий
   `schedule_event_update_tasks()`;
6. возвращает `operation_ref`, состояние и уже известные accepted `event_ids`.

Commit может завершиться асинхронно. MCP timeout не означает неуспех и не даёт
права повторно создавать событие. Повтор с тем же idempotency key возвращает ту
же durable operation.

#### `event_operation_get`

Возвращает:

- `prepared | queued | processing | review_required | accepted | rejected | failed | reconciliation_pending | expired`;
- Smart Update outcome по каждому candidate;
- accepted `event_id`/`event_ids`;
- warnings и sanitized failure reason;
- число и статусы downstream jobs;
- timestamps и actor attribution.

Агент обязан дождаться accepted `event_id` через этот метод. До этого создание
promo campaign не начинается.

### 3.3 Owner и partner semantics

Owner может явно подтвердить прямой commit. Партнёр по умолчанию создаёт
атрибутированное предложение и получает `review_required`. Auto-approval
разрешается только отдельной policy/entitlement и никогда не выводится из одного
факта `is_partner=true`.

## 4. Hard fact-consistency gate для публичного текста

### 4.1 Конфликт — это запрет, а не предупреждение

`preserve_original`, `replace_exact` и exact public notice не могут быть
закоммичены, если текст противоречит принятым structured facts события.
Предупреждения недостаточно: система должна вернуть ошибку и не выдавать
коммитопригодную preparation.

Минимально защищаются:

- occurrence date и end date;
- start time и отдельно распознанные doors/gathering/opening times;
- venue name, address и city;
- lifecycle state;
- ticket/free/price/age facts, если текст делает явное утверждение о них;
- manual-locked и source-grounded facts.

Omission не является конфликтом: exact-текст может не повторять дату, время или
локацию и не должен из-за этого очищать поля. Проверяются только явные
утверждения.

### 4.2 Результаты проверки

Каждое явное утверждение получает один verdict:

```text
consistent
omitted
new_supported_fact
conflict
unresolved
```

- `consistent` и `omitted` разрешают продолжение;
- `new_supported_fact` сначала проходит Smart Update facts merge и повторную
  проверку против нового event revision;
- `conflict` блокирует операцию с `PUBLIC_TEXT_FACT_CONFLICT`;
- `unresolved` блокирует exact publication с
  `PUBLIC_TEXT_FACT_UNRESOLVED`.

Prepare возвращает field-level diff: какое утверждение найдено в тексте, какой
канонический факт ему противоречит и из какого source/fact lock этот факт
получен. Это объяснение не является кнопкой «всё равно сохранить».

### 4.3 Правильный путь при намеренном изменении факта

Если текст сообщает новую дату, время или площадку, сначала выполняется typed
reschedule/location change либо эти изменения включаются в один
`event_reschedule_prepare` вместе с replacement description. Exact-текст тогда
проверяется уже против **предлагаемого нового fact snapshot** и коммитится
атомарно с ним.

Generic `event_edit_commit` не может незаметно повысить текстовый edit до
изменения даты/времени/локации. При обнаружении такого расхождения он блокируется
и указывает требуемую typed operation.

У owner нет `force=true` для обхода этого gate. Ошибочный канонический факт
исправляется предметной операцией с provenance, а не публикацией текста, который
расходится с БД.

### 4.4 Generated text тоже fail-closed

`smart_rewrite` обычно строится из facts, но финальный generated текст всё равно
проверяется. Обнаруженный конфликт означает `GENERATED_TEXT_FACT_CONFLICT`:
текст не записывается и не публикуется, операция получает технический failure
или review state.

Commit повторно проверяет current event revision. Если facts изменились после
prepare, возвращается `STALE_EVENT_REVISION`; старый digest не применяется.

## 5. Редактирование существующего события

Редактирование не маскируется под новое добавление и не создаёт ещё одно Event.
Для обычных не-lifecycle изменений используется пара:

- `event_edit_prepare`;
- `event_edit_commit`.

Input содержит `event_id`, `edit_kind`, patch, reason, source/provenance,
idempotency key и при необходимости media refs.

### 5.1 Текст

`edit_kind="description"` поддерживает:

- `smart_rewrite` — добавить новый атрибутированный source/context и
  пересобрать текст по fact-first правилам;
- `replace_exact` — установить переданный owner-approved текст как manual
  description override после hard fact-consistency gate.

В обоих режимах новый текст проходит Smart Update facts extraction, чтобы новые
факты не остались только в prose. `replace_exact` запрещает незаметно
переписывать переданный текст моделью, но не запрещает извлечение и проверку
фактов.

Text-only edit не должен самопроизвольно менять изображения, campaign targets или
не относящиеся к запросу поля. Если текст явно расходится с identity/logistics
anchors, edit блокируется; он не превращается автоматически в другой тип
мутации.

Все версии текста сохраняют actor, reason, source, previous hash и resulting
hash. Raw source, generated description и manual public override — разные
данные.

### 5.2 Изображения

`edit_kind="media"` поддерживает явные операции:

- `add` — одно или несколько изображений;
- `replace` — одно, несколько или все изображения;
- `remove` — одно, несколько или все изображения;
- `reorder` — изменить порядок без повторного ingest неизменённых bytes.

Операция использует существующий Event Media gate. MCP не пишет URL напрямую в
`Event.photo_urls`, не обходит materialization, exact-pixel identity, pair review
или approval state. В viewer-facing projection попадают только approved media.

### 5.3 Другие не-lifecycle поля

Для title, ticket, festival и прочих typed fields применяется Smart Update
validation. Изменения, влияющие на identity или occurrence, не выполняются
локальным SQL patch. Date/time/location/lifecycle имеют отдельные операции ниже.

## 6. Перенос даты, времени, локации и отмена

### 6.1 Почему одна операция reschedule, а не три несвязанных commit

Организатор часто одновременно меняет дату, время и площадку. Три независимых
commit создали бы промежуточное противоречивое состояние и три публикационных
fan-out. Поэтому MCP выставляет одну пару:

- `event_reschedule_prepare`;
- `event_reschedule_commit`.

Она принимает любое непустое сочетание `date`, `time`, `location` и применяет
его атомарно. Агент может использовать её как функцию переноса только даты,
только времени, только локации или сразу нескольких параметров.

Отмена имеет отдельную семантику и отдельную пару:

- `event_cancel_prepare`;
- `event_cancel_commit`.

Обе пары используют общий `event_operation_get` для статуса/readback.

### 6.2 Обязательное обоснование организатора

Для reschedule и cancel обязательны:

- точный `event_id`;
- непустой `organizer_comment`;
- `source.type` и устойчивый `source.external_id`, optional source URL;
- actor/organization/tenant attribution;
- idempotency key.

`organizer_comment` хранится как внутреннее provenance/audit evidence по
умолчанию. Он **не публикуется автоматически**: там могут быть внутренние детали.
Viewer-facing причина задаётся отдельно через `public_notice`.

Минимальная форма rationale:

```json
{
  "organizer_comment": "Организатор сообщил о переносе из-за недоступности зала",
  "source": {
    "type": "partner_message",
    "external_id": "partner-message-1842",
    "url": null
  },
  "reason_code": "venue_unavailable"
}
```

`reason_code` помогает отчётам, но не заменяет свободный комментарий и source.
Реализация не должна вводить сложную универсальную taxonomy ради первого релиза.

Audit обязан долговечно сохранять before/after structured snapshot, actor,
comment, source, visibility policy, event revision, operation ref и terminal
outcome. Сначала переиспользуются существующие operation/EventSource/
EventSourceFact ledgers. Если они не могут выразить before/after и rationale без
неструктурированного JSON, допускается **одна** append-only таблица
`event_change_log`, а не новый workflow subsystem.

### 6.3 `event_reschedule_prepare`

Пример input:

```json
{
  "event_id": 123,
  "change": {
    "date": "2026-09-18",
    "time": "19:30",
    "location": {
      "name": "Новая площадка",
      "address": "Новый адрес",
      "city": "Калининград"
    }
  },
  "organizer_comment": "Организатор перенёс событие на другую площадку",
  "source": {
    "type": "owner_message",
    "external_id": "owner-request-20260902-1",
    "url": null
  },
  "description": {
    "policy": "none | smart_rewrite | replace_exact",
    "text": null
  },
  "idempotency_key": "event-123-reschedule-20260902"
}
```

Не менее одного поля внутри `change` должно присутствовать и реально отличаться.
Prepare:

1. загружает точный event и current revision/hash;
2. проверяет actor/tenant/ownership;
3. нормализует дату, время, venue/address/city через существующие reference rules;
4. запускает Smart Update fact/identity validation для proposed snapshot;
5. проверяет collision с другим occurrence/event;
6. при exact replacement description применяет hard fact-consistency gate к
   proposed, а не старому snapshot;
7. строит read-only reconciliation plan для обычных публикаций, promo и
   pending jobs;
8. возвращает old/new diff, affected surfaces/campaigns, approval state,
   `preparation_ref`, digest и expiry.

MCP не ищет target по приблизительному названию. Если агент получил только
текстовое название, он сначала использует `events_search`, показывает найденное
событие и передаёт точный ID.

Если proposed anchors совпадают с другим каноническим occurrence, prepare
возвращает `EVENT_IDENTITY_CONFLICT` и блокирует commit. Reschedule не должен
молча merge/delete два события; сначала требуется отдельное разрешение identity
конфликта.

### 6.4 `event_reschedule_commit`

Commit:

1. повторно проверяет event revision и права;
2. запускает shared typed event-change service с точным `target_event_id`;
3. применяет весь proposed fact snapshot атомарно;
4. сохраняет organizer rationale/provenance и before/after audit;
5. сохраняет тот же canonical event ID;
6. создаёт ровно один reconciliation fan-out новой revision через существующий
   `schedule_event_update_tasks()`/`JobOutbox`;
7. возвращает operation status и publication reconciliation refs.

Для inferred VK/TG lifecycle actions и MCP direct actions должна быть одна
application mutation. Текущую VK-specific DB mutation нужно вынести в shared
service; fuzzy source matching заканчивается после разрешения target ID и не
входит в commit MCP.

Stable event/calendar identity сохраняется: reschedule обновляет существующий
calendar item/UID, а не создаёт пользователю второй календарный объект.

### 6.5 `event_cancel_prepare`

Пример input:

```json
{
  "event_id": 123,
  "organizer_comment": "Организатор отменил событие из-за болезни участника",
  "source": {
    "type": "partner_message",
    "external_id": "partner-message-1843",
    "url": null
  },
  "public_notice": {
    "policy": "none | smart_rewrite | replace_exact",
    "text": null
  },
  "idempotency_key": "event-123-cancel-20260902"
}
```

Prepare проверяет точный target, текущее lifecycle state, права, rationale,
source, active publications, pending jobs, registrations/calendar effects и
связанные promo campaigns. Raw organizer comment остаётся internal. Exact public
reason проходит тот же fact-consistency gate; например, текст «событие
перенесено» не может сопровождать операцию `cancel`.

### 6.6 `event_cancel_commit`

Commit:

1. применяет typed lifecycle action к точному `event_id` через shared
   event-change/Smart Update boundary;
2. выставляет `lifecycle_status=cancelled`, не удаляя Event;
3. сохраняет rationale, source и before/after audit;
4. исключает событие из future active selectors, обычного нового fan-out и promo
   eligibility;
5. отменяет или supersede-ит ещё не выполненные jobs старой revision;
6. ставит reconciliation jobs, нужные для отображения «Отменено» на уже
   существующих managed pages/posts/calendar projections;
7. сохраняет публичные URL и историю exposures;
8. возвращает status/readback по каждой поверхности.

Отмена не публикует отдельный новый социальный пост автоматически. Если нужен
самостоятельный анонс об отмене, он должен быть явно показан в prepare и пройти
существующий social/publication action flow.

Повторная отмена идемпотентна: состояние не дублируется, jobs и публичные actions
не размножаются. Дополнительный новый источник может быть приложен как provenance
без повторного viewer-facing side effect.

Generic edit не восстанавливает отменённое событие. Реактивация/возврат после
отмены потребует отдельной typed operation и отдельной продуктовой фиксации.

### 6.7 Partner policy

В первой partner projection:

- партнёр может подготовить reschedule/cancel только своего tenant/event;
- direct public mutation по умолчанию запрещена;
- результат — `owner_review_required`;
- owner видит exact diff, comment, source и affected publications перед approve;
- попытка передать чужой event ID блокируется на read, prepare и commit.

Promo free-tier/auto-approval не распространяется автоматически на lifecycle
changes: перенос и отмена меняют уже данные, обещанные аудитории.

## 7. Публикационные очереди и фактические публикации

### 7.1 Не строить новую очередь

MCP использует существующий `JobOutbox`, existing workers, provider adapters и
publication/exposure receipts. Smart Update accepted event запускает стандартный
fan-out, предусмотренный текущей конфигурацией продукта. MCP не обещает
«опубликовать везде» и не создаёт скрытый второй distribution plan.

Promo activities — отдельный слой продвижения и не подменяют обычный Smart
Update fan-out.

### 7.2 Revision-aware reconciliation

После переноса, редактирования фактов или отмены одного статуса `published`
недостаточно: публикация может содержать старую дату. Каждая управляемая
проекция должна сравнивать target event revision/facts digest с фактически
применённой revision.

Если в текущем transport уже есть content hash/revision, он переиспользуется. В
противном случае добавляется минимальный event revision/digest binding, а не
новая очередь.

Pending job старой revision не имеет права опубликовать stale payload:

- на enqueue/coalesce он заменяется текущей revision;
- либо на claim завершается как `superseded`;
- текущая revision должна иметь один действующий job;
- ошибка между canonical commit и scheduling фиксируется как
  `reconciliation_pending` и восстанавливается durable recovery, а не выдаётся
  как откат уже принятого event change.

Reschedule не расширяет promo campaign period, exposure goal или spend
автоматически. Если новая дата выходит за окно кампании, owner получает явное
предупреждение/неeligibility. Допустимый deterministic clamp к более раннему
концу показывается в prepare; молчаливое продление запрещено.

Cancellation сохраняет исторические promo exposures, но не создаёт новые.

### 7.3 Новые read tools

#### `event_publication_status`

По `event_id` возвращает понятную строку состояния на каждую известную
поверхность:

- была ли поверхность запланирована;
- job/operation refs;
- `next_run_at`;
- попытки и sanitized last error;
- provider scheduled/live receipt;
- public URL, если он подтверждён;
- campaign/activity attribution, если публикация промо;
- target event revision и applied publication revision;
- `up_to_date | stale | update_queued | outcome_unknown`;
- последнее проверенное время.

Нормализованные состояния:

```text
not_planned
queued
running
scheduled
published
retry_wait
paused
superseded
failed
outcome_unknown
cancelled
```

`not_planned` принципиально отличается от `failed`, а `published/stale` — от
`published/up_to_date`.

#### `publication_queue_list`

Фильтры:

- `event_id`;
- `campaign_id`;
- `partner/organization` — owner only;
- surface/task;
- normalized status;
- event revision;
- due time range;
- bounded cursor/limit.

Owner видит всю очередь и внутренние diagnostic refs. Партнёр видит только
sanitized jobs и receipts событий/кампаний своего tenant; internal payload,
чужие jobs, secrets и `operations_snapshot` ему недоступны.

#### `publication_job_get`

Возвращает bounded detail одного job, включая dependency, attempts, next retry,
target/applied revision, supersession reason, outcome/readback и связанные
event/campaign refs. Существующий `fetch(id="job:<id>")` остаётся совместимым
owner evidence API; новый tool даёт предметную и partner-safe схему.

## 8. Промо-кампании

### 8.1 Существующая модель является источником истины

Не создаются новые упрощённые `campaign` или `campaign_event` таблицы вместо
реализованных:

- `promo_campaign`;
- `promo_target`;
- `promo_activity`;
- `promo_exposure`.

MCP переиспользует существующие domain services, включая
`create_partner_event_promo_campaign`, `add_partner_activity_to_campaign`,
`clamp_campaign_end_to_event` и фактические policy/resolver функции.

### 8.2 Отдельная операция после event creation

Создание события и создание промо-кампании — **две последовательные предметные
операции**:

```text
event_create_prepare
→ event_create_commit
→ event_operation_get до accepted event_id
→ promo_capabilities(event_id)
→ promo_campaign_create_prepare
→ promo_campaign_create_commit
→ promo_operation_get
```

Монолитный `create_event_and_campaign` в первый scope не вводится. Он скрывал бы
Smart Update retry/review states и зависимость promo target от accepted event ID.
Агент получает эту последовательность в MCP instructions и tool descriptions.

### 8.3 Promo tools

#### `promo_capabilities`

По actor и optional `event_id` возвращает уже реализованные surfaces/activity
profiles и решение policy для каждой возможности:

- `allowed`;
- `auto_approved`;
- `owner_review_required`;
- `denied`;
- применимые лимиты/остаток;
- disclosure/spend requirements;
- причины недоступности, включая missing source/media/lifecycle state.

#### `promo_campaigns_list` и `promo_campaign_get`

Owner читает все разрешённые кампании. Партнёр — только кампании своего tenant и
их sanitized stats/exposures.

#### `promo_campaign_create_prepare` / `promo_campaign_create_commit`

Создаёт campaign и target только для уже существующих accepted active
event/festival anchors. Preview показывает:

- target IDs;
- campaign period/status/priority;
- activity surfaces и profile keys;
- exposure goals/caps;
- disclosure;
- approval decision;
- любое действие, способное вызвать расход или внешнюю публикацию.

#### `promo_activity_add_prepare` / `promo_activity_add_commit`

Добавляет активность к существующей кампании через действующий domain service,
с повторной проверкой ownership, archive/lifecycle state, entitlement и limits.

#### `promo_operation_get`

Возвращает campaign/activity IDs, approval state, queue/exposure refs и
sanitized failure/readback.

### 8.4 Partner entitlement без преждевременного billing engine

Контракт предусматривает future basic/free tier: часть promo activities может
быть auto-approved в пределах количества/периода, остальные оформляются как
заявка владельцу. В первой реализации достаточно policy interface и audit
решения `auto_approved | owner_review_required | denied`.

Отдельный универсальный тарифный или платёжный движок не создаётся до
подтверждённых продуктовых правил.

## 9. Scopes и видимость tools

Целевые owner scopes:

```text
events:read
events:write
promo:read
promo:write
operations:read
```

Целевые partner scopes:

```text
partner:events:read
partner:events:propose
partner:promo:read
partner:promo:request
partner:publications:read
```

Scope задаёт только верхнюю границу. Каждый вызов дополнительно проверяет tenant,
organization, role, ownership, current event revision и object state. Partner
token не может быть принят owner resource и наоборот.

Одинаковый underlying ToolSpec/handler может иметь разные public descriptors и
output projections. Owner-only fields не должны сначала извлекаться и потом
маскироваться на клиенте — query/policy boundary обязана исключать их на сервере.

## 10. Idempotency, audit и неизвестный результат

Для каждого prepare/commit:

- preparation связана с actor/resource/tenant и имеет TTL;
- digest покрывает frozen candidates/patch, base event revision, text policy,
  targets и activities;
- idempotency key уникален в actor + operation kind scope;
- повтор того же commit возвращает прежний operation/result;
- тот же key с другим digest отклоняется;
- optimistic revision guard запрещает применять устаревший prepare;
- timeout после mutation boundary даёт `outcome_unknown`, а не автоматический
  повтор;
- status/readback является обязательной частью пользовательского результата;
- audit хранит actor, source/provenance, organizer comment, requested action,
  before/after, approval decision, event/campaign/job refs и terminal outcome без
  credentials/raw secrets.

## 11. Обязательные автотесты и release gate

Канонический перечень сценариев:
[`docs/testing/private-events-mcp-event-operations-scenarios.v1.yml`](../testing/private-events-mcp-event-operations-scenarios.v1.yml).
Каждый ID из YAML должен быть сопоставлен с автоматическим тестом. Наличие
несвязанных общих тестов не закрывает scenario ID.

### 11.1 Уровни

1. **Unit:** schemas, exact-text fact comparison, venue/time role normalization,
   policy и revision guards.
2. **Integration:** временная SQLite, реальные application services, Smart
   Update facade и `JobOutbox`, fake LLM/provider adapters.
3. **Protocol:** MCP tool discovery/calls, OAuth audience, scopes, tenant и
   idempotency.
4. **Worker:** claim/supersession/reconciliation с fake providers.
5. **Isolated live:** реальный MCP/auth/application/queue/worker path через
   `@eventsbotTestBot` и private test destinations.

### 11.2 Release-blocking группы

Обязательны, в частности:

- wrong date/time/location exact text блокируется без Event/Job mutations;
- omission не очищает facts;
- doors/opening/start roles не дают ложный конфликт;
- generated contradiction fail-closed;
- stale preparation не коммитится;
- date-only, time-only, location-only и combined reschedule;
- rationale/source обязательны;
- exact event ID и запрет fuzzy target mutation;
- collision с другим event не приводит к silent merge;
- regression двух прежних lifecycle incidents: unrelated same-location event
  остаётся неизменным;
- old jobs не публикуют stale revision;
- Telegraph/static/ICS/managed social reconciliation видима;
- calendar UID/event identity сохраняются;
- campaign не продлевается автоматически;
- cancel сохраняет Event, прекращает future publication/promo и обновляет
  существующие projections;
- organizer comment internal by default;
- partner change/cancel получает owner review и не видит чужой tenant;
- repeated commit/cancel/reschedule не создаёт дублей;
- DB-success/scheduling-failure восстанавливается как
  `reconciliation_pending`;
- `outcome_unknown` не повторяется автоматически;
- prepare/commit не вызывают Telegram/VK/Telegraph provider напрямую;
- test execution не может разрешить production destination IDs.

### 11.3 GitHub Actions

CI должен иметь отдельный обязательный job для domain/protocol/worker tests. Он
работает без production secrets и без внешних provider mutations. Простой
meta-check проверяет, что все scenario IDs из YAML зарегистрированы в test
suite.

Секрет-зависимый isolated live smoke запускается только вручную
(`workflow_dispatch`) или в доверенной release-среде. Он не запускается из fork
PR и server-side запрещает production destinations.

## 12. Порядок реализации

1. Провести fresh audit существующих operation/audit stores и lifecycle code.
2. Вынести mutation после target resolution из VK-specific функции в один shared
   typed event-change service; сохранить прежнее VK поведение через adapter.
3. Добавить exact-text fact-consistency validator на существующих facts/Smart
   Update extractors.
4. Добавить durable operation/revision/audit support; только при необходимости
   одну append-only `event_change_log`.
5. Добавить owner event create/edit/reschedule/cancel tools и write scopes.
6. Добавить revision-aware publication status/list/detail projection и stale-job
   supersession/recovery.
7. Выставить read/create/change tools существующей promo-модели.
8. Добавить отдельный partner resource/protocol поверх тех же handlers с
   tenant/policy projection.
9. Реализовать все scenario IDs, CI gate и isolated live acceptance.
10. Выполнить exact-main deploy, OAuth scope refresh, authenticated `tools/list`,
    mutation tests и remote readback.

До завершения шага 10 ChatGPT не должен утверждать, что event/promo/lifecycle
write через live MCP уже доступен.

## 13. Явные non-goals

В этот scope не входят:

- второй EventsBot service или отдельная partner DB;
- новый event parser вместо Smart Update;
- новая publication queue вместо `JobOutbox`;
- новая promo data model вместо существующей;
- скрытый combined event+promo transaction;
- fuzzy event matching как основание MCP mutation;
- owner force-bypass для конфликтующего публичного текста;
- автоматическая публикация внутреннего комментария организатора;
- прямой доступ партнёра к internal incidents/operations/social workspace;
- универсальный billing/tariff engine;
- автоматическая публикация на всех платформах без явного факта планирования и
  последующего readback.
