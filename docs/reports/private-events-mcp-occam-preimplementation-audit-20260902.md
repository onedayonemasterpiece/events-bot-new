# Предреализационный аудит минимальности: EventsBot MCP event operations

> Дата: 2 сентября 2026 года  
> Baseline: `45cf770cf743fde98c56b523af1ed913385be1a8`  
> Статус: **обязательный pre-implementation gate**  
> Runtime этим документом не изменён.  
> Канонический TO-BE: `docs/operations/private-events-mcp-event-operations-to-be.md`  
> Полный продуктовый аудит: `docs/reports/private-events-mcp-event-operations-to-be-audit-20260902.md`

## 1. Вердикт

Revision 3 достаточно полна как целевой продуктовый контракт, но **не должна
реализовываться одним большим изменением**. Для критической production-системы
это создало бы слишком широкий blast radius: одновременно затрагивались бы
Smart Update, SQLite schema, JobOutbox, lifecycle, provider publications,
статический сайт, media rendering, promo и OAuth.

Применяется бритва Оккама:

1. сначала только наблюдаемость и owner-only добавление события;
2. существующие production paths не переписываются;
3. новая схема не добавляется, пока без неё можно корректно получить продуктовый
   результат;
4. каждая следующая возможность вводится отдельным bounded gate после тестов
   предыдущей;
5. partner, promo, lifecycle notices и badge renderer не входят в первый релиз.

**К реализации можно приступать только с Minimal Slice 0/1 ниже.** Полный набор
revision 3 остаётся roadmap и release contract, а не размером первого PR.

## 2. Что уже является безопасной точкой переиспользования

Нужно использовать без параллельной реализации:

- существующий raw-text parsing и `add_events_from_text` mapping;
- полный Smart Update с candidate state/attempt ledger и accepted outcomes;
- `schedule_event_update_tasks()`;
- существующий `JobOutbox` и workers;
- `event_get`, `fetch(job:<id>)` и current evidence repository;
- `static_site_release.event_public_revision(event)`;
- существующий Private Events MCP runtime, OAuth и ToolSpec policy;
- существующую promo model/services, но только в позднем slice.

Новый MCP handler не имеет права самостоятельно создавать `Event`, копировать
Smart Update mapping или напрямую вызывать Telegram/VK/Telegraph.

## 3. Явно отклонённые ранние изменения

### 3.1 Не рефакторить текущий VK lifecycle path в первом PR

История уже содержит sev1-инциденты неверного выбора события при переносе и
отмене. Одновременное создание MCP lifecycle API и переподключение существующего
VK ingestion к новому service увеличивает риск регрессии.

Решение:

- первый owner create/read slice не касается `vk_auto_queue.py`;
- shared lifecycle service вводится позднее;
- существующий VK path переключается на него только отдельным change после
  parity/regression tests и shadow comparison;
- fuzzy matching никогда не входит в MCP commit boundary.

### 3.2 Не менять глобальные JobTask/JobStatus до появления реального worker

В первом slice не нужны новые tasks, `superseded`, `cancelled`,
`event_change_id`, `target_event_revision` или `publication_kind`.

Queue read нормализует существующие `pending/running/done/error/paused`, не меняя
claim semantics. Новые terminal reasons/identity fields добавляются только вместе
с lifecycle reconciliation worker и его тестами. До этого нельзя расширять enum
«на будущее».

### 3.3 Не создавать `event_change_log` ради обычного создания Event

Создание уже имеет Smart Update candidate state, attempts, EventSource и facts.
Новая append-only таблица действительно нужна для edit/reschedule/postpone/cancel
с before/after, но не для первого owner create.

Она добавляется только в lifecycle/edit slice вместе с:

- идемпотентной additive SQLite migration;
- повторным `Database.init()` test;
- production-shaped snapshot test;
- old-binary compatibility check;
- отсутствием startup backfill.

### 3.4 Не включать exact-text mode до hard-gate tests

Первый owner create поддерживает только default `smart_rewrite`.
`preserve_original` и `replace_exact` остаются закрыты feature policy, пока не
реализованы и не пройдены все `TXT-*` tests.

Это безопаснее, чем выставить exact mode и рассчитывать на будущую проверку
конфликтов.

### 3.5 Не вводить production hold/shadow state в основной outbox ради теста

Безопасный acceptance первого slice выполняется на временной SQLite и fake
providers. Для live acceptance используется test bot/private test destination.
Production queue semantics не меняются ради тестового сценария.

### 3.6 Не открывать partner resource до owner proof

Partner OAuth, tenant filtering, proposal/review и free-tier добавляются только
после доказанного owner path. `User.is_partner` не используется как внешняя
security boundary.

### 3.7 Не включать promo, lifecycle notice и badge в owner-create PR

- promo уже имеет собственные services и подключается отдельной проекцией после
  accepted `event_id`;
- правило `>24h` требует authoritative publication receipt и не должно
  вычисляться по `Event.added_at`;
- badge renderer требует отдельного deterministic render contract;
- ни одна из этих функций не нужна, чтобы безопасно добавить текущее событие и
  увидеть его jobs.

## 4. Minimal Slice 0 — read-only observability

Первое изменение не мутирует production data и не требует schema migration.

Добавить owner-only tools поверх существующего repository:

```text
publication_queue_list
publication_job_get
```

Опционально `event_publication_status`, если он может быть построен правдиво из
уже существующих rows без guessed timestamps/revisions.

Требования:

- bounded cursor/limit;
- deterministic ordering;
- filters по event/task/status/due range;
- sanitized error/result;
- никакого raw payload/secret dump;
- read-only ToolSpec;
- current Codex/partner catalog не расширяется автоматически.

Если статус невозможно доказать существующими данными, возвращается `unknown`, а
не новая фиктивная сущность или эвристика.

## 5. Minimal Slice 1 — owner-only create через существующий Smart Update

После зелёного Slice 0 добавить только:

```text
event_create_prepare
event_create_commit
event_operation_get
```

Ограничения первой версии:

- owner/operator resource only;
- default-off feature flag;
- только `smart_rewrite`;
- raw text + устойчивый source external ID/URL;
- один existing parsing/Smart Update mapping;
- accepted outcomes только `CREATED`, `MERGED`, `NOOP_EXACT_REPLAY`;
- standard `schedule_event_update_tasks()` только после accepted outcome;
- никаких promo/lifecycle/media-edit/provider calls;
- повторный source/idempotency identity не создаёт второй Event или jobs.

Prepare не меняет Event/EventSource/JobOutbox/Promo. Для durable preparation и
idempotency сначала переиспользуется уже имеющийся primitive Private Events MCP.
Нельзя создавать универсальный workflow engine. Если существующий primitive
жёстко связан с social provider payload и не может быть выделен малым безопасным
refactor, это отдельный stop gate: вводится одна узкая event-operation таблица,
а не общий orchestration subsystem.

### Source identity gate

Нельзя создавать поддельный публичный URL. Owner message получает серверный
внутренний canonical locator, основанный на principal + stable external ID и
неразглашаемом digest. Его формат должен быть принят Smart Update identity layer
явно и покрыт replay tests. Если текущий canonicalizer допускает только HTTP(S),
нужно малое typed расширение internal locator, а не `https://fake.local/...`.

## 6. Обязательные тесты первых двух slices

### Slice 0

1. queue list не меняет ни одной таблицы;
2. filters/pagination/order детерминированы;
3. secret-like payload/error data редактируются;
4. неизвестное состояние остаётся `unknown`;
5. read tools отсутствуют в запрещённых catalog projections.

### Slice 1

1. feature flag off сохраняет AS-IS `tools/list` и runtime behavior;
2. prepare не меняет Event/EventSource/JobOutbox/Promo counts;
3. commit обязательно проходит Smart Update;
4. `CREATED` создаёт один canonical Event и стандартный fan-out;
5. `MERGED` возвращает существующий canonical Event;
6. retry/rejection не создают downstream jobs;
7. одинаковый source/idempotency replay не создаёт дублей;
8. MCP и текущий manual intake дают эквивалентные EventCandidate/Smart Update
   inputs на одном fixture;
9. ни один test не вызывает Telegram/VK/Telegraph provider adapter;
10. current manual/VK/TG ingestion regression suites остаются зелёными.

## 7. Database safety gate для последующих slices

Любая будущая schema change допускается только как additive изменение через
`Database.init()`:

- никаких table rebuild для этой фичи;
- никаких обязательных колонок без безопасного constant default;
- никакого массового startup backfill;
- индексы проверяются на production-shaped snapshot по времени и размеру;
- `PRAGMA quick_check=ok` до и после;
- повторный init идемпотентен;
- старый binary может запуститься с добавленными nullable tables/columns;
- backup/rollback path зафиксирован до deploy.

Если условие не выполняется, изменение не внедряется в рамках текущего slice.

## 8. Stop conditions

Разработка останавливается и возвращается на аудит, если для очередного шага
потребовалось хотя бы одно из следующего:

- второй parser, scheduler, outbox, promo model или event store;
- изменение существующего provider behavior до parity tests;
- прямой provider call из MCP request/commit;
- fuzzy target selection внутри mutation;
- guessed publication timestamp или guessed event status;
- fake public source URL;
- table rebuild или тяжёлый startup backfill;
- owner `force` для конфликтующего public text;
- один PR, одновременно меняющий core, workers, site, promo и partner OAuth;
- неподтверждённое «успешно» без event/job/status readback.

## 9. Последовательность после первых двух slices

Только после доказанного owner create:

1. **Lifecycle core:** `event_change_log`, exact ID, reschedule/postpone/cancel,
   hard text gate; без отдельного social notice.
2. **Reconciliation:** revision-bound jobs и исправление existing managed
   projections.
3. **Notice + site + badge:** authoritative `>24h`, static old→new,
   deterministic derived asset.
4. **Promo projection:** только существующие promo services.
5. **Partner projection:** отдельный audience/catalog/tenant policy.
6. **Codex release slice:** независимый аудит, merge, deploy и isolated live
   acceptance готовой integration branch.

Каждый пункт может быть остановлен или отложен без нарушения уже работающих
предыдущих возможностей.

## 10. Итог

Полный revision 3 остаётся правильным целевым контрактом, но безопасная
реализация начинается существенно уже:

```text
read existing queue
→ owner create through existing Smart Update
→ verify event + standard jobs
```

Первый implementation PR не должен содержать lifecycle, notice renderer,
static-site UI, promo или partner OAuth. Это не сокращение требований, а
монотонный способ внедрить их без опасного одновременного изменения критической
системы.
