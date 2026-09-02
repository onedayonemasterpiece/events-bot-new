# EventsBot MCP event operations: минимальный безопасный профиль реализации

> **Статус:** обязательный implementation gate.  
> **Дата аудита:** 2 сентября 2026 года.  
> **Проверенный baseline:** `45cf770cf743fde98c56b523af1ed913385be1a8`.  
> **Целевой продуктовый контракт:** [`private-events-mcp-event-operations-to-be.md`](private-events-mcp-event-operations-to-be.md).  
> **Полный acceptance registry:** [`../testing/private-events-mcp-event-operations-scenarios.v2.yml`](../testing/private-events-mcp-event-operations-scenarios.v2.yml).

Этот документ не отменяет TO-BE revision 3. Он ограничивает **порядок и способ
реализации**, чтобы целевой контракт не превратился в один большой рискованный
релиз. Для критической production-системы «вся архитектура описана» не означает
«всё нужно внедрить одновременно».

## 1. Вердикт аудита

Полный TO-BE остаётся продуктовой целью, но реализовывать Slices A–D одним
изменением нельзя. Безопасен только последовательный rollout с маленькими
обратимо совместимыми шагами:

1. read-only наблюдаемость существующей очереди;
2. owner-only создание события через уже работающий полный Smart Update;
3. owner-only promo поверх существующих promo services;
4. owner-only редактирование и lifecycle changes;
5. отдельные lifecycle notices, изображения и static-site history только после
   доказанной полноты publication receipts;
6. partner projection и управление партнёрами отдельным security-релизом.

Первый кодовый PR не должен менять существующие Telegram/VK/manual intake,
workers, provider adapters или публичные поверхности.

## 2. Что показал fresh-read кода

### 2.1 Уже есть необходимые основные контуры

Переиспользуются без альтернативной реализации:

- `add_events_from_text` и Smart Update для создания/merge;
- `SmartUpdateCandidateState` и attempts для terminal outcome/retry;
- `EventSource` и facts для provenance;
- `schedule_event_update_tasks()` и `JobOutbox` для обычного fan-out;
- `static_site_release.event_public_revision(event)` для детерминированного
  public snapshot SHA;
- существующие promo campaign/target/activity/exposure services;
- `PrivateEventsMCPServer`, `ToolSpec`, OAuth resources;
- `EventsEvidenceRepository`, read-only SQLite, redaction и query budgets;
- Event Media gate и image geometry.

Новый parser, scheduler, outbox, promo model или provider layer запрещён.

### 2.2 Текущий `JobOutbox` требует осторожности

`JobTask` и `JobStatus` являются SQLAlchemy Enum. Текущие статусы:

```text
pending | running | done | error | paused
```

Старый бинарник может не десериализовать строку с новым enum value. Поэтому в
первом и lifecycle-релизах запрещено записывать новые значения `JobTask` или
`JobStatus`.

Для различия `superseded`/`cancelled` безопаснее позднее добавить nullable
`terminal_reason TEXT` и нормализовать ответ MCP, сохраняя существующий terminal
`done` либо `paused`. Новые task kinds для отдельных lifecycle notices можно
вводить только отдельным совместимым rollout:

1. сначала код, который терпимо читает будущие task/status значения, но их не
   создаёт;
2. проверка rollback/startup на migrated DB;
3. только следующим exact-main release разрешается запись новых values.

До этого original reconciliation использует только существующие tasks.

### 2.3 `event_publication` пока не является полным ledger всех публикаций

Таблица существует и читается MCP, но найденные writers относятся прежде всего
к reconciliation/metrics отдельных VK-проекций. Не доказано, что каждый
canonical Telegram/VK/другой managed publish записывает авторитетные
`first_published_at` и applied event revision.

Следовательно:

- нельзя считать возраст публикации по `Event.added_at`;
- нельзя считать его по времени последнего readback;
- наличие URL не доказывает timestamp;
- правило отдельного notice `age > 24h` не включается автоматически, пока
  coverage receipts не измерена и не закрыта тестами;
- неизвестный возраст даёт `notice_review_required`.

Startup backfill публикационных timestamps запрещён: он может создать ложную
уверенность. Сначала добавляются future receipts, затем выполняется отдельный
read-only coverage audit; исторический backfill допускается только по
проверяемому provider evidence.

### 2.4 Static site сейчас экспортирует только active events

Текущий production-preview exporter фильтрует
`lifecycle_status='active'`. Поэтому обещание «cancelled/postponed detail page
остаётся доступной» требует отдельного site change и не должно незаметно входить
в первый MCP write release.

### 2.5 Existing OAuth/social action store нельзя использовать для Event writes

`OAuthStateStore` намеренно расположен в отдельной SQLite DB и хранит OAuth и
provider Social Workspace state. Event operation должна быть согласована с
канонической event DB. Распределённой транзакции между этими SQLite файлами нет.

Поэтому:

- authentication identity приходит из существующего OAuth context;
- event request/status/idempotency хранится в canonical event DB;
- `social_workspace_preparation/operation` не переиспользуются как Event ledger;
- provider `social_action_*` не используется для записи Event.

## 3. Бритва Оккама: минимальная модель

### 3.1 Одна новая таблица, не workflow engine

Для event-domain prepare/commit/status достаточно одной таблицы
`event_change_log`, используемой одновременно как operation ledger и история
изменения.

Не создаются отдельные таблицы preparation, operation, audit, workflow step и
outbox для одной операции.

Минимальные группы полей:

```text
identity:
  id, operation_ref, operation_kind, event_id
binding:
  actor_scope, tenant_id, idempotency_hash, action_digest
request:
  source locator, bounded frozen request, organizer comment
concurrency:
  base_event_revision, result_event_revision
history:
  before_json, after_json, changed_fields_json
state:
  prepared | processing | review_required | accepted | rejected |
  reconciliation_pending | failed | outcome_unknown | expired
result:
  error_code, created_at, committed_at, completed_at
```

Request/before/after/digest после записи неизменяемы. Status и result меняются
только монотонно условным `UPDATE ... WHERE status IN (...)`. Это сохраняет
историю, но не требует второго event-sourcing слоя и сложных DB triggers.

Для create `event_id` и `before_json` до Smart Update могут быть NULL. После
accepted outcome в той же operation фиксируется canonical `event_id` и
`after_json`.

### 3.2 Не добавлять `Event.revision INTEGER`

Используется существующий `event_public_revision(event)`. Он консервативно
инвалидирует preparation даже при изменении другого viewer-facing поля. Лишний
`STALE_EVENT_REVISION` безопаснее пропущенной гонки и не требует нового revision
counter с синхронизацией во всех старых путях.

### 3.3 Только additive SQLite migration

В первом schema release допустимы:

- новая таблица;
- nullable columns;
- неуникальные индексы;
- unique index только для новой таблицы и после теста collision semantics.

Запрещены:

- rebuild существующей `event` или `joboutbox`;
- изменение/удаление существующих колонок;
- массовый backfill в `Database.init()`;
- новые triggers на горячих production-таблицах;
- длительная транзакция вокруг LLM/network call;
- новые enum values, которые пишет новый бинарник и не понимает старый.

`Database.init()` проверяется на fresh DB, production-shaped old snapshot и
повторный init. После additive migration прежний baseline binary должен
по-прежнему стартовать и читать рабочие таблицы.

## 4. Минимальный rollout

### R0 — только очередь и status readback

Цель: дать владельцу требуемую наблюдаемость без единой мутации.

Добавляются read-only tools поверх существующего `EventsEvidenceRepository`:

```text
publication_queue_list
publication_job_get
```

При необходимости `event_publication_status` сначала возвращает только
доказанные данные и explicit `unknown`, без выведения факта публикации из URL.

Ограничения:

- scope: существующий `operations:read`;
- bounded cursor/limit;
- deterministic ordering;
- payload по умолчанию не возвращается;
- errors/result проходят существующую recursive redaction;
- никакой schema migration;
- никакой provider network;
- никакой новой auth projection.

R0 можно ревьюить и выпускать независимо. Он сразу отвечает на вопрос владельца
о состоянии очередей и практически не затрагивает production behavior.

### R1 — owner-only создание события

Добавляются только:

```text
event_create_prepare
event_create_commit
event_operation_get
```

Условия:

- полный существующий Smart Update, без прямого `INSERT Event`;
- стандартный существующий fan-out;
- partner, lifecycle, promo, badge и отдельные notices отсутствуют;
- существующие `/addevent`, VK/TG imports и Smart Update handlers не
  рефакторятся;
- одна additive `event_change_log` migration;
- master feature flag default OFF;
- disabled tools отсутствуют в `tools/list`;
- новые scopes не добавляются автоматически старым grants/tokens.

LLM/Smart Update никогда не вызываются внутри удерживаемой SQLite transaction.
Короткая транзакция резервирует idempotent operation, затем освобождается;
после Smart Update результат фиксируется второй короткой транзакцией. Crash
recovery сверяет operation с сохранённым Smart Update candidate key/outcome.

### R1b — owner-only promo

Только после accepted `event_id` выставляется узкая MCP-проекция существующих
promo services. Event create и campaign create остаются двумя операциями.

Не добавляются новые campaign tables, тарифный engine или partner entitlements.

### R2 — owner-only edit/reschedule/postpone/cancel

Добавляются exact-`event_id` операции и hard fact gate. На этом этапе:

- только owner;
- никакого fuzzy target mutation;
- current VK lifecycle path сначала остаётся неизменным;
- новый shared service покрывается characterization/integration tests;
- перевод VK/TG lifecycle callers на shared service выполняется отдельным PR
  после доказанной parity;
- original reconciliation использует существующий fan-out;
- отдельные social lifecycle notices и status-badge ещё выключены;
- `event_change_log` хранит before/after и internal organizer rationale.

Hard fact gate первой версии защищает только факты, для которых уже есть
стабильные typed fields и надёжная нормализация:

```text
date / end_date
start time
venue / address / city
lifecycle state
```

Он переиспользует один parse/Smart Update result и выполняет детерминированное
сравнение. Второй универсальный LLM fact-checker не создаётся. Price/ticket/age
claims добавляются позже только после отдельных evidence tests; неоднозначное
явное logistics-утверждение в exact mode блокируется как unresolved.

### R3 — lifecycle notices, badges и public history

Этот этап начинается только после четырёх закрытых prerequisites:

1. измерена и доказана coverage `event_publication` receipts;
2. writers сохраняют authoritative publication timestamp и applied revision;
3. JobTask/JobStatus имеют forward-compatible двухрелизный rollout;
4. static exporter умеет сохранять direct detail cancelled/postponed events без
   возврата их в active discovery.

Только затем включаются:

- правило `age > 24h`;
- отдельные TG/VK notices;
- deterministic derived image `ПЕРЕНОС`/`ОТМЕНЕНО`;
- public old → new history;
- новые task kinds.

До выполнения prerequisites автоматическая отдельная публикация запрещена.

### R4 — partner projection и управление партнёрами

Fresh IdeaHub voice `voice-20260902-173441-d86517ba` добавляет отдельные
требования: создание/приостановка партнёра, роли, права, выдача MCP access без
обязательного Telegram-доступа.

Это security-sensitive capability management, а не часть первого event-write
slice. Она требует отдельного owner-only admin contract и проверки lifecycle
credentials. Нельзя свести её к `User.is_partner=true` или втиснуть в event PR.

До R4:

- event writes доступны только owner resource;
- partner resource не объявляется готовым;
- партнёрские роли/токены не генерируются новым кодом;
- текущий TO-BE tenant boundary сохраняется как цель, но не имитируется
  статическими догадками.

## 5. Feature flags и rollback

Минимальный runtime control:

```text
PRIVATE_EVENTS_MCP_EVENT_OPERATIONS_ENABLED=0
PRIVATE_EVENTS_MCP_EVENT_OPERATIONS_ALLOWED=
```

`ALLOWED` — comma-separated allowlist (`create`, затем `edit,reschedule,...`).
При master OFF или отсутствии action tool не публикуется в каталоге.

Promo, notices и partner access включаются только отдельными поздними flags,
когда появляется соответствующий код. Не создаётся десяток пустых flags заранее.

Deploy sequence для каждого write-релиза:

1. additive schema + code, feature OFF;
2. startup/readiness/migration/read-only verification;
3. isolated test DB/private destination acceptance;
4. scope/token refresh;
5. узкое включение одной action;
6. наблюдение queue/status;
7. только затем следующая action.

Rollback не требует удаления новых таблиц/колонок. Flag OFF прекращает новые
операции; прежний binary должен продолжать работать с additive schema.

## 6. Обязательные safety tests до каждого этапа

Полный v2 registry является acceptance backlog всей целевой функции, а не
требованием внедрить все поверхности одним PR. Каждый release profile закрывает
свой subset; статус «полностью реализовано» запрещён, пока не закрыт весь
registry.

### Для R0

- query budget, cursor и deterministic ordering;
- redaction nested payload/errors;
- missing/legacy columns;
- concurrent worker read;
- zero DB writes и zero provider calls.

### Для R1

- existing `/addevent` и source imports не изменились;
- prepare не меняет Event/Source/JobOutbox;
- create обязательно проходит Smart Update;
- CREATED/MERGED/NOOP возвращают canonical ID;
- retry/rejection не создают downstream work;
- same key+digest converges; same key+other digest fails;
- crash после operation reserve до Smart Update восстанавливается;
- crash после accepted Smart Update до operation finalize не создаёт второй
  Event;
- scheduling failure даёт `reconciliation_pending` и repairable state;
- no direct provider calls.

### Для R2

- exact event ID only;
- same-venue unrelated event unchanged;
- old lifecycle incident replays;
- optimistic revision conflict;
- range-event policy;
- organizer comment remains private;
- conflicting exact text blocks the whole change;
- current VK/TG behavior parity before caller migration.

### Для любого schema release

- fresh SQLite;
- old production-shaped snapshot copy;
- repeated `Database.init()`;
- `PRAGMA quick_check`;
- startup time/disk delta;
- previous binary compatibility against migrated copy;
- no production data backfill during startup.

## 7. Явно запрещённые shortcut-решения

- один гигантский PR R0–R4;
- новый generic workflow engine;
- отдельная partner DB/event pipeline;
- cross-DB transaction между OAuth DB и event DB;
- рефакторинг Smart Update одновременно с первым MCP create;
- прямое изменение Event из MCP в обход Smart Update;
- новые enum task/status rows без forward-compatibility release;
- определение publication age по `Event.added_at` или URL;
- lifecycle notice при unknown publication age;
- изменение original poster bytes ради плашки;
- динамическое управление партнёрами внутри event-write PR;
- auto-enable scopes или tools после deploy;
- blind retry после timeout/outcome_unknown.

## 8. Решение о начале разработки

Первый допустимый кодовый шаг — **R0 read-only queue observability**. Он не
требует schema migration и не меняет поведение очереди.

После отдельного зелёного R0 можно начинать **R1 owner-only create**. Начинать с
таблиц для notices, badge renderer, partner roles или новых JobTask/JobStatus
нельзя: это расширяет blast radius до того, как доказана базовая owner operation.

Основная реализация выполняется в ChatGPT на одной integration branch малыми
проверяемыми commits. Codex получает только release integration после зелёных
этапов и не дописывает пропущенные крупные функции на deployment checkout.
