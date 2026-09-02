# EventsBot MCP: добавление событий, редактирование, промо и статусы публикаций — TO-BE

> **Статус:** owner-confirmed TO-BE, runtime ещё не реализован.  
> **Дата решения:** 2 сентября 2026 года.  
> **Канонический владелец:** `events-bot-new`.  
> **Источник решения:** IdeaHub voice intake `voice-20260902-154844-651facb3`.  
> **Текущий AS-IS MCP-контракт:** [`private-events-mcp.md`](private-events-mcp.md).  
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
| Промо | MVP уже реализован через `promo_campaign`, `promo_target`, `promo_activity`, `promo_exposure` | Выставить capability/read/write projection поверх существующей модели |
| Партнёрское промо | Phase A уже использует общую promo-модель и owner checks | Добавить OAuth/tenant policy, не новую promo-модель |
| Social workspace | `social_action_*` работает с конкретными provider actions | Не использовать его для записи `Event` или создания promo campaign |

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
  чего исходный согласованный текст сохраняется как атрибутированный manual
  public-description override. Он не заменяет `source_text` и не отменяет
  structured facts.

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

- `prepared | queued | processing | review_required | accepted | rejected | failed | expired`;
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

## 4. Редактирование существующего события

Редактирование не маскируется под новое добавление и не создаёт ещё одно Event.
Используется пара:

- `event_edit_prepare`;
- `event_edit_commit`.

Input содержит `event_id`, `edit_kind`, patch, reason, idempotency key и при
необходимости media refs.

### 4.1 Текст

`edit_kind="description"` поддерживает:

- `smart_rewrite` — добавить новый атрибутированный source/context и
  пересобрать текст по fact-first правилам;
- `replace_exact` — установить переданный owner-approved текст как manual
  description override.

В обоих режимах новый текст проходит Smart Update facts extraction, чтобы новые
факты не остались только в prose. `replace_exact` запрещает незаметно
переписывать переданный текст моделью, но не запрещает извлечение и проверку
фактов.

Text-only edit не должен самопроизвольно менять изображения, campaign targets или
не относящиеся к запросу поля. Если новый текст действительно меняет identity
anchors — title, occurrence/date/time, venue/city или source-native occurrence —
prepare повышает операцию до полного Smart Update event change и показывает это
до commit.

Все версии текста сохраняют actor, reason, source, previous hash и resulting
hash. Raw source и public override — разные данные.

### 4.2 Изображения

`edit_kind="media"` поддерживает явные операции:

- `add` — одно или несколько изображений;
- `replace` — одно, несколько или все изображения;
- `remove` — одно, несколько или все изображения;
- `reorder` — изменить порядок без повторного ingest неизменённых bytes.

Операция использует существующий Event Media gate. MCP не пишет URL напрямую в
`Event.photo_urls`, не обходит materialization, exact-pixel identity, pair review
или approval state. В viewer-facing projection попадают только approved media.

### 4.3 Остальные поля

Для title/date/time/location/ticket/festival/lifecycle применяется typed patch с
Smart Update validation. Изменения, влияющие на identity или occurrence,
обязательно проходят полный identity/merge guard; локальный SQL patch запрещён.

## 5. Публикационные очереди и фактические публикации

### 5.1 Не строить новую очередь

MCP использует существующий `JobOutbox`, existing workers, provider adapters и
publication/exposure receipts. Smart Update accepted event запускает стандартный
fan-out, предусмотренный текущей конфигурацией продукта. MCP не обещает
«опубликовать везде» и не создаёт скрытый второй distribution plan.

Promo activities — отдельный слой продвижения и не подменяют обычный Smart
Update fan-out.

### 5.2 Новые read tools

#### `event_publication_status`

По `event_id` возвращает одну понятную строку состояния на каждую известную
поверхность:

- была ли поверхность запланирована;
- job/operation refs;
- `next_run_at`;
- попытки и sanitized last error;
- provider scheduled/live receipt;
- public URL, если он подтверждён;
- campaign/activity attribution, если публикация промо;
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
failed
outcome_unknown
cancelled
```

`not_planned` принципиально отличается от `failed` и от отсутствия данных.

#### `publication_queue_list`

Фильтры:

- `event_id`;
- `campaign_id`;
- `partner/organization` — owner only;
- surface/task;
- normalized status;
- due time range;
- bounded cursor/limit.

Owner видит всю очередь и внутренние diagnostic refs. Партнёр видит только
sanitized jobs и receipts событий/кампаний своего tenant; internal payload,
чужие jobs, secrets и `operations_snapshot` ему недоступны.

#### `publication_job_get`

Возвращает bounded detail одного job, включая dependency, attempts, next retry,
outcome/readback и связанные event/campaign refs. Существующий
`fetch(id="job:<id>")` остаётся совместимым owner evidence API; новый tool даёт
предметную и partner-safe схему.

## 6. Промо-кампании

### 6.1 Существующая модель является источником истины

Не создаются новые упрощённые `campaign` или `campaign_event` таблицы вместо
реализованных:

- `promo_campaign`;
- `promo_target`;
- `promo_activity`;
- `promo_exposure`.

MCP переиспользует существующие domain services, включая
`create_partner_event_promo_campaign`, `add_partner_activity_to_campaign`,
`clamp_campaign_end_to_event` и фактические policy/resolver функции.

### 6.2 Отдельная операция после event creation

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

### 6.3 Promo tools

#### `promo_capabilities`

По actor и optional `event_id` возвращает уже реализованные surfaces/activity
profiles и решение policy для каждой возможности:

- `allowed`;
- `auto_approved`;
- `owner_review_required`;
- `denied`;
- применимые лимиты/остаток;
- disclosure/spend requirements;
- причины недоступности, включая missing source/media.

#### `promo_campaigns_list` и `promo_campaign_get`

Owner читает все разрешённые кампании. Партнёр — только кампании своего tenant и
их sanitized stats/exposures.

#### `promo_campaign_create_prepare` / `promo_campaign_create_commit`

Создаёт campaign и target только для уже существующих accepted event/festival
anchors. Preview показывает:

- target IDs;
- campaign period/status/priority;
- activity surfaces и profile keys;
- exposure goals/caps;
- disclosure;
- approval decision;
- любое действие, способное вызвать расход или внешнюю публикацию.

#### `promo_activity_add_prepare` / `promo_activity_add_commit`

Добавляет активность к существующей кампании через действующий domain service,
с повторной проверкой ownership, archive state, entitlement и limits.

#### `promo_operation_get`

Возвращает campaign/activity IDs, approval state, queue/exposure refs и
sanitized failure/readback.

### 6.4 Partner entitlement без преждевременного billing engine

Контракт предусматривает future basic/free tier: часть promo activities может
быть auto-approved в пределах количества/периода, остальные оформляются как
заявка владельцу. В первой реализации достаточно policy interface и audit
решения `auto_approved | owner_review_required | denied`.

Отдельный универсальный тарифный или платёжный движок не создаётся до
подтверждённых продуктовых правил.

## 7. Scopes и видимость tools

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
organization, role, ownership и object state. Partner token не может быть принят
owner resource и наоборот.

Одинаковый underlying ToolSpec/handler может иметь разные public descriptors и
output projections. Owner-only fields не должны сначала извлекаться и потом
маскироваться на клиенте — query/policy boundary обязана исключать их на сервере.

## 8. Idempotency, audit и неизвестный результат

Для каждого prepare/commit:

- preparation связана с actor/resource/tenant и имеет TTL;
- digest покрывает frozen candidates/patch, text policy, targets и activities;
- idempotency key уникален в actor + operation kind scope;
- повтор того же commit возвращает прежний operation/result;
- тот же key с другим digest отклоняется;
- timeout после mutation boundary даёт `outcome_unknown`, а не автоматический
  повтор;
- status/readback является обязательной частью пользовательского результата;
- audit хранит actor, source/provenance, requested action, approval decision,
  event/campaign/job refs и terminal outcome без credentials/raw secrets.

## 9. Обязательные тесты

### 9.1 GitHub Actions

Добавляется отдельный MCP domain smoke на временной SQLite и provider/LLM fakes,
но с реальными application services, Smart Update facade и `JobOutbox`:

1. `event_create_prepare` не создаёт Event и publication jobs;
2. commit обязательно вызывает Smart Update;
3. accepted create/merge возвращает event ID и создаёт стандартный fan-out;
4. retry/rejection не создаёт promo или downstream jobs;
5. `smart_rewrite` и явный `preserve_original` дают разные public text outcomes,
   но оба сохраняют facts/provenance;
6. exact text edit обновляет facts и не меняет unrelated media/campaign fields;
7. media add/replace/remove проходит Event Media gate;
8. повтор commit с тем же idempotency key не создаёт дублей;
9. event ID требуется перед promo create;
10. promo create/activity add использует существующие promo services;
11. queue/status readback различает `not_planned`, pending, scheduled, published,
    failed и `outcome_unknown`;
12. partner tenant не читает/не изменяет чужое событие или кампанию;
13. partner catalog не содержит owner operations/incidents;
14. ни один test не вызывает production Telegram/VK/Telegraph destination.

### 9.2 Live acceptance без конечных пользователей

Используется существующая test infrastructure (`@eventsbotTestBot`, test DB и
явно разрешённые private test destinations). Acceptance проходит реальный
MCP/auth/application/queue/worker path, но production destination IDs запрещены
на server-side resolver уровне.

Проверяемая цепочка:

```text
owner test token
→ event create prepare/commit
→ Smart Update accepted event_id
→ standard JobOutbox fan-out
→ worker в test environment
→ publication status/readback
→ promo campaign create
→ promo activity job/exposure
→ повторные calls без дублей
```

Отдельный manual/`workflow_dispatch` GitHub Actions job может выполнять этот
секрет-зависимый live smoke. Он не запускается на произвольном PR из fork и не
использует production provider targets.

## 10. Порядок реализации

1. Добавить durable domain-operation store либо минимально расширить уже
   подходящий action store; не вводить workflow engine.
2. Выделить shared application facade поверх upstream parse + Smart Update +
   standard scheduling без изменения Telegram intake semantics.
3. Добавить owner event create/edit tools и write scopes.
4. Добавить publication status/list/detail projection.
5. Выставить read/create/change tools существующей promo-модели.
6. Добавить отдельный partner resource/protocol поверх тех же handlers с
   tenant/policy projection.
7. Добавить CI smoke и isolated live acceptance.
8. Выполнить exact-main deploy, OAuth scope refresh, authenticated `tools/list`,
   mutation test и remote readback.

До завершения шага 8 ChatGPT не должен утверждать, что event/promo write через
live MCP уже доступен.

## 11. Явные non-goals

В этот scope не входят:

- второй EventsBot service или отдельная partner DB;
- новый event parser вместо Smart Update;
- новая publication queue вместо `JobOutbox`;
- новая promo data model вместо существующей;
- скрытый combined event+promo transaction;
- прямой доступ партнёра к internal incidents/operations/social workspace;
- универсальный billing/tariff engine;
- автоматическая публикация на всех платформах без явного факта планирования и
  последующего readback.
