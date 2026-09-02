# EventsBot MCP: операции над событиями — TO-BE

> **Статус:** owner-confirmed TO-BE, revision 3, аудит завершён; готово к поэтапной реализации.  
> **Runtime status:** новые event/promo/lifecycle write tools ещё не реализованы и не опубликованы.  
> **Дата:** 2 сентября 2026 года.  
> **Канонический владелец:** `events-bot-new`.  
> **AS-IS MCP:** [`private-events-mcp.md`](private-events-mcp.md).  
> **Аудит:** [`private-events-mcp-event-operations-to-be-audit-20260902.md`](../reports/private-events-mcp-event-operations-to-be-audit-20260902.md).  
> **Release-blocking tests:** [`private-events-mcp-event-operations-scenarios.v2.yml`](../testing/private-events-mcp-event-operations-scenarios.v2.yml).  
> **Voice intake:** `voice-20260902-154844-651facb3`, `voice-20260902-163949-7ee6120d`, `voice-20260902-164447-cabb5893`.

Этот документ является целевым предметным контрактом. До реализации, тестов,
exact-main deploy и authenticated live readback действующим фактом остаётся
AS-IS-контракт Private Events MCP.

## 1. Архитектурное решение

### 1.1 Один MCP runtime, несколько защищённых проекций

Не создаются два независимых MCP-сервиса или два событийных конвейера.
Используется один EventsBot MCP runtime внутри существующего процесса
`events-bot`:

- одна каноническая SQLite event DB;
- один Smart Update;
- один `JobOutbox` и существующие workers;
- одна модель `promo_campaign / promo_target / promo_activity / promo_exposure`;
- общие application services;
- один audit trail.

Над runtime существуют отдельные OAuth resource/audience projections:

1. **owner/operator** — ChatGPT/OpenCode: internal reads и разрешённые owner
   mutations;
2. **partner** — audience `events-partner`: closed allowlist, обязательные
   `principal + organization + tenant` bindings и object-level policy;
3. **Codex** — существующая read-only projection без event/promo mutations.

Разные endpoint/resource projections обеспечивают token isolation, безопасный
`tools/list` и server-side tenant filtering. Они не создают второй backend,
вторую БД, scheduler или provider adapter.

### 1.2 Общие commands, разные права

Owner и партнёр используют одинаковые application commands и schemas там, где
совпадает предметное действие. Policy layer дополнительно проверяет:

- OAuth audience/client и scopes;
- principal, organization и tenant;
- роль;
- владение событием/кампанией;
- object state и current event revision;
- entitlement/лимит;
- editorial, lifecycle или spend approval.

`User.is_partner` может оставаться UI-признаком Telegram-бота, но не является
достаточной security boundary для внешнего partner MCP.

## 2. Что переиспользуется AS-IS

| Область | Существующая реализация | Решение |
|---|---|---|
| Event evidence | `events_search`, `event_get`, `fetch(event:/job:)` | Сохранить совместимость |
| MCP runtime | `PrivateEventsMCPServer`, OAuth resources и `ToolSpec` | Расширить существующий runtime |
| Создание/merge | `add_events_from_text` → Smart Update | Выставить shared facade, не копировать pipeline |
| Identity/facts/provenance | Smart Update candidate state, attempts, `EventSource`, facts | Обход запрещён |
| Event revision | `static_site_release.event_public_revision(event)` | Использовать как публичный revision SHA |
| Обычный fan-out | `schedule_event_update_tasks()` и `JobOutbox` | Не создавать новую очередь |
| Lifecycle parse | `CANCEL`, `POSTPONE`, `RESCHEDULE_DATE`, `RESCHEDULE_TIME`, `UPDATE_DETAILS` | Вынести mutation в общий typed service |
| Media | Event Media gate и image geometry | Переиспользовать для lifecycle assets |
| Promo | `promo_campaign`, targets, activities, exposures и partner promo services | Не создавать новую promo-модель |
| Provider work | существующие Telegram/VK/Telegraph/calendar workers/adapters | MCP commit напрямую provider не вызывает |

VK/TG source recognition может искать вероятный target, но shared mutation
всегда получает уже разрешённый точный `event_id`. Fuzzy matcher не входит в
commit boundary.

## 3. Минимальная модель данных

### 3.1 Event revision

Отдельный `Event.revision INTEGER` не вводится. `event_public_revision(event)`
уже вычисляет детерминированный SHA viewer-facing snapshot и включает текст,
дату, время, локацию, lifecycle, media и другие публичные поля.

Каждая preparation фиксирует `base_event_revision`. Каждый successful change
получает `result_event_revision`. Commit повторно сравнивает current revision и
отклоняет stale preparation с `STALE_EVENT_REVISION`.

### 3.2 `event_change_log` — обязательный append-only ledger

Текущая строка `Event` не хранит old values. Поэтому вводится одна минимальная
структурированная таблица `event_change_log`, обязательная для edit/reschedule/
postpone/cancel.

Минимальные поля:

```text
id
operation_ref
operation_kind
status
actor_kind / actor_id
organization / tenant_id
approval_state / reviewed_by / reviewed_at
idempotency_key / action_digest
source_type / source_external_id / source_url
organizer_comment_internal / reason_code
public_notice_policy / public_notice_text / public_notice_hash
base_event_revision / result_event_revision
before_json / after_json / changed_fields_json
notice_policy / notice_plan_json / rendered_assets_json
error_code
created_at / committed_at / completed_at
```

Ledger хранит before/after structured snapshot, а не только prose. Уникальность
idempotency задаётся в actor + operation kind scope. Секреты, provider tokens и
временные signed URLs не сохраняются.

### 3.3 `event_publication`

Существующая таблица расширяется либо получает строго эквивалентный companion
ledger. Предпочтительно расширение существующей таблицы:

```text
publication_kind       canonical_event | lifecycle_notice
event_change_id        nullable
first_published_at      authoritative provider/public receipt time
last_published_at
applied_event_revision
provider_operation_ref
outcome_state           scheduled | published | failed | outcome_unknown | ...
```

Правило отдельного notice не вычисляется по `Event.added_at`, URL или времени
последней проверки. Если authoritative publication time неизвестен, результат —
`notice_review_required`, а не догадка.

### 3.4 `JobOutbox`

Существующий outbox сохраняется. Добавляются nullable identity fields:

```text
event_change_id
target_event_revision
publication_kind
```

Не прятать эти значения только в JSON: они нужны для claim guard, фильтрации,
status/readback и индексов. `payload` остаётся bounded task-specific data.

`JobStatus` получает напрямую читаемые terminal states `superseded` и
`cancelled` либо эквивалентные отдельные состояния. Маскировать их под `done`
нельзя.

Новые задачи внутри существующего outbox:

```text
event_change_notice_render
tg_event_change_notice
vk_event_change_notice
```

Обычное reconciliation использует существующие tasks: `telegraph_build`,
`vk_sync`, `tg_event_publish`, `ics_publish`, `tg_ics_post`, page/static-site
jobs. MAX не объявляется поддержанным до отдельного adapter contract.

### 3.5 SQLite migration

Production SQLite эволюционирует идемпотентно через `db.py::Database.init()`:
`CREATE TABLE/INDEX IF NOT EXISTS` и `_add_column()`. `models.py` обновляется
синхронно. Отдельный неиспользуемый migration runtime не вводится.

## 4. Общий protocol операций

### 4.1 Prepare

Prepare:

- не меняет Event, promo, publications или jobs;
- проверяет права и видимость target;
- фиксирует source/provenance;
- нормализует proposed change;
- выполняет read-only identity/fact/conflict checks;
- строит exact reconciliation/notification plan;
- возвращает `preparation_ref`, `action_digest`, `base_event_revision`, expiry и
  `committable`.

### 4.2 Commit

Commit принимает только frozen preparation, digest и idempotency key:

- повторно проверяет actor/tenant/rights/revision;
- тот же key+digest возвращает прежнюю durable operation;
- тот же key с другим digest даёт `IDEMPOTENCY_CONFLICT`;
- напрямую не вызывает Telegram/VK/Telegraph;
- после canonical mutation создаёт revision-bound jobs;
- timeout после mutation boundary даёт `outcome_unknown` или
  `reconciliation_pending`, но не разрешает слепой повтор.

### 4.3 Status и review

`event_operation_get(operation_ref | idempotency_key)` возвращает:

```text
prepared
queued
processing
review_required
accepted
rejected
failed
reconciliation_pending
outcome_unknown
expired
```

Для partner proposal owner получает frozen diff через этот метод и принимает
решение идемпотентным `event_operation_decide(operation_ref, approve|reject,
owner_comment, idempotency_key)`. Approval не меняет frozen payload.

## 5. Создание события

### 5.1 Неподвижное правило

Любое новое каноническое событие проходит полный Smart Update. Запрещены:

- прямой `INSERT Event` из MCP;
- упрощённый MCP parser/persist pipeline;
- downstream jobs до accepted Smart Update outcome;
- diagnostic/retry ID как готовый `event_id`.

Accepted outcomes:

```text
CREATED
MERGED
NOOP_EXACT_REPLAY
```

`RETRY_SCHEDULED`, product rejection и `FAILED_TECHNICAL` не разрешают promo или
publication mutations.

### 5.2 Tools

```text
event_create_prepare
event_create_commit
event_operation_get
```

`event_create_prepare` принимает raw text, source locator, optional media refs,
text policy и idempotency key. Один source может дать несколько candidates;
неоднозначное разбиение требует явного frozen selection.

`event_create_commit` передаёт frozen candidates в существующий Smart Update.
Только accepted result возвращает `event_id` и запускает обычный
`schedule_event_update_tasks()`.

Создание promo начинается отдельной операцией только после accepted `event_id`.

## 6. Public text и обычное редактирование

### 6.1 Text policies

```text
smart_rewrite       default fact-first public text
preserve_original   exact source text as manual public override
replace_exact       exact replacement for existing event
```

`preserve_original` и `replace_exact` не обходят Smart Update: identity,
provenance, facts и media gates выполняются всегда.

### 6.2 Hard fact-consistency gate

Exact и generated text не могут быть сохранены, если явно противоречат accepted
structured facts. Проверяются как минимум:

- date/end date;
- start time отдельно от doors/gathering/opening time;
- venue/address/city;
- lifecycle state;
- ticket/free/price/age claims;
- manual-locked и source-grounded facts.

Field verdict:

```text
consistent
omitted
new_supported_fact
conflict
unresolved
```

- omission не очищает existing fact;
- `new_supported_fact` сначала проходит facts merge и повторную проверку;
- `conflict` → `PUBLIC_TEXT_FACT_CONFLICT`;
- `unresolved` → `PUBLIC_TEXT_FACT_UNRESOLVED`;
- generated conflict → `GENERATED_TEXT_FACT_CONFLICT`;
- `force=true` отсутствует даже у owner.

Если exact text сообщает новую дату/время/площадку, generic edit блокируется и
указывает на typed reschedule. Text может быть коммитнут атомарно внутри
reschedule только после проверки против proposed new snapshot.

### 6.3 Ordinary edit tools

```text
event_edit_prepare
event_edit_commit
```

`edit_kind`:

- `description`;
- `media`;
- другие non-lifecycle typed fields.

Text-only edit не меняет media, campaigns или несвязанные поля. Media operations:
`add`, `replace`, `remove`, `reorder`; они проходят Event Media gate и не пишут
URL напрямую в `Event.photo_urls`.

Date, end date, time, location и lifecycle не меняются через generic edit.

## 7. Lifecycle operations

### 7.1 Общие обязательные данные

Для reschedule/postpone/cancel обязательны:

- точный `event_id`;
- непустой `organizer_comment`;
- `source.type` и устойчивый `source.external_id`; optional source URL;
- actor/organization/tenant attribution;
- idempotency key.

`organizer_comment` — internal provenance/audit evidence и не публикуется
автоматически. Viewer-facing причина задаётся отдельно через `public_notice` и
проходит hard fact-consistency gate.

### 7.2 `event_reschedule_prepare/commit`

Одна атомарная операция принимает любое непустое сочетание:

```json
{
  "date": "2026-09-18",
  "end_date": "2026-09-18",
  "time": "19:30",
  "location": {
    "name": "Новая площадка",
    "address": "Новый адрес",
    "city": "Калининград"
  }
}
```

Она покрывает перенос только даты, времени, площадки или сразу нескольких
параметров. Три независимых commit не используются: промежуточное смешанное
состояние и многократный fan-out запрещены.

Prepare:

1. загружает exact event и revision;
2. нормализует fields через existing reference rules;
3. валидирует proposed snapshot через Smart Update/shared identity boundary;
4. проверяет collision с другим occurrence;
5. проверяет optional exact description против proposed snapshot;
6. строит original-reconciliation и lifecycle-notice plan;
7. показывает old → new diff.

При range-event изменение start date требует либо явный `end_date`, либо
`range_policy='preserve_duration'`, показанный в preview. Silent shift/keep
запрещён.

Все время нормализуется в `Europe/Kaliningrad`; audit хранит offset-aware
change/effective timestamps.

Commit сохраняет тот же Event ID и stable calendar identity, создаёт одну change
revision и один coalesced reconciliation fan-out. Если target уже `postponed`,
successful reschedule переводит его в `active`. Reschedule cancelled event
запрещён.

### 7.3 `event_postpone_prepare/commit`

Используется, когда организатор сообщил о переносе, но новая дата/время ещё не
известны.

Commit:

- выставляет `lifecycle_status='postponed'`;
- не придумывает новую дату;
- сохраняет old schedule в structured change ledger;
- старые date/time могут оставаться в Event как исторические технические поля,
  но public current schedule считается unknown;
- исключает событие из active discovery и promo eligibility;
- reconciliation помечает существующие surfaces как «перенесено, новая дата
  уточняется»;
- применяет тот же 24-hour notification policy.

Когда новая дата известна, используется `event_reschedule_*` с тем же event ID.

### 7.4 `event_cancel_prepare/commit`

Commit:

- выставляет `lifecycle_status='cancelled'`, не удаляя Event;
- сохраняет rationale, provenance и before/after;
- исключает event из future active selectors и promo eligibility;
- отменяет/supersede-ит pending jobs старой revision;
- ставит reconciliation для managed pages/posts/calendar projections;
- сохраняет public URLs и historical promo exposures;
- применяет 24-hour notification policy;
- не создаёт отдельный notice вне показанного plan.

Повторная отмена идемпотентна. Новый source может быть приложен как provenance
без второго viewer-facing effect.

Generic edit/reschedule не реактивирует cancelled Event. Отдельный restore не
входит в первую реализацию; до отдельного решения это owner-only recovery.

### 7.5 Partner policy

В первой partner projection lifecycle changes своих events создаются как
`owner_review_required`. Promo free-tier не даёт auto-approval для изменения уже
обещанных аудитории даты, времени, площадки или lifecycle.

## 8. Reconciliation и отдельные уведомления

### 8.1 Original managed projections

После edit/reschedule/postpone/cancel всегда строится reconciliation plan для
уже существующих managed projections. Новый lifecycle notice не заменяет
исправление исходной карточки/поста/ICS/static page.

Pending job старой revision:

- coalesce-ится в current revision; либо
- завершается `superseded` при claim.

Он не имеет права опубликовать stale payload. Если canonical change принят, но
scheduling упал, operation получает `reconciliation_pending`; recovery создаёт
jobs current revision идемпотентно.

### 8.2 Правило 24 часов

Для каждого previously published managed surface/target:

```text
first_published_at age > 24h   → отдельный lifecycle notice автоматически
age <= 24h                    → только reconcile/edit original
age unknown                   → notice_review_required, не публиковать автоматически
не было publication           → not_planned
```

Ровно 24 часа относится к `<= 24h`.

Prepare показывает по каждой surface:

- original publication URL и age evidence;
- будет ли original edit/reconcile;
- будет ли отдельный notice;
- почему surface не планируется;
- где нужен owner review.

Owner может выбрать `notification_policy=automatic|force|suppress`.
`force`/`suppress` требуют explicit preview; `suppress` — обязательный reason.
Partner использует automatic plan и не может обходить owner review.

Один notice создаётся на `(event_change_id, surface, target)`. Если новое
изменение появилось до provider mutation boundary, pending notice старой change
supersede-ится и latest snapshot получает новый job. Уже опубликованная история
не переписывается; следующее существенное изменение может дать новый notice.

`outcome_unknown` никогда не повторяется автоматически: сначала provider
readback/reconciliation.

### 8.3 Notice content

Public notice включает:

- label «Перенос»/«Отменено»;
- title;
- только реально изменённые old → new fields;
- optional проверенную public reason;
- stable event link;
- актуальный lifecycle state.

Internal organizer comment, tenant/actor IDs и private source metadata не
попадают в текст.

## 9. Визуальная плашка lifecycle status

Для social lifecycle notice создаётся deterministic derived asset:

```text
event_id + event_change_id + result_event_revision
+ source_pixel_sha256 + lifecycle label + renderer/template version
```

Правила:

- labels: `ПЕРЕНОС` и `ОТМЕНЕНО`;
- источник — approved Event Media;
- original EventPoster/photo_urls/bytes не изменяются;
- renderer использует existing image geometry и safe-region evidence;
- если overlay закроет важный текст/лицо/value region, используется безопасная
  боковая полоса или расширение canvas;
- asset immutable, content-addressed и записывается в
  `event_change_log.rendered_assets_json`;
- плашка не зависит от promo campaign или Afisha Engagement;
- при отсутствии безопасного renderable image text-only разрешается только
  provider policy; иначе job честно blocked/failed, без скрытого ухудшения.

Lifecycle badge не добавляется ко всем исходным media события. Это производный
артефакт конкретного уведомления об изменении.

## 10. Статический сайт

- stable event URL сохраняется;
- cancelled/postponed events исключаются из active discovery, но detail page
  остаётся доступна;
- detail page показывает заметный status banner;
- reschedule показывает только изменённые old → new date/end date/time/location;
- postpone показывает, что новая дата уточняется;
- cancel показывает отмену;
- optional public notice и effective timestamp видимы;
- internal organizer comment и private provenance не экспортируются;
- SEO `EventCancelled`/`EventPostponed` сохраняется;
- static projection получает минимальную allowlist `event_change_log`, а не всю
  operational таблицу;
- site tests проверяют mobile/desktop banner, accessibility name, stable URL и
  отсутствие internal comment.

## 11. Publication status и очереди через MCP

### 11.1 `event_publication_status(event_id)`

По каждой известной surface возвращает две оси:

```text
transport:
  not_planned | queued | running | scheduled | published | retry_wait |
  paused | failed | outcome_unknown | cancelled

revision:
  up_to_date | stale | update_queued | superseded | unknown
```

Поля: job/operation refs, timestamps, attempts, next retry, sanitized error,
public URL, campaign/change attribution, target/applied revision и last checked.

### 11.2 `publication_queue_list(...)`

Фильтры:

- event ID;
- event change ID;
- campaign ID;
- surface/task/status;
- target event revision;
- due range;
- organization/partner — owner only;
- bounded cursor/limit.

### 11.3 `publication_job_get(job_id)`

Возвращает bounded detail: dependency, attempts, next retry, target/applied
revision, supersession reason, provider receipt/readback и related refs.

Owner видит internal diagnostics. Partner видит только sanitized rows своего
tenанта. `operations_snapshot` и internal incidents partner resource не
получает.

## 12. Промо-кампании

Существующая модель является единственным источником истины. MCP переиспользует
фактические services, включая `create_partner_event_promo_campaign`,
`add_partner_activity_to_campaign`, `clamp_campaign_end_to_event` и resolver/
policy функции.

Создание события и promo — разные последовательные действия:

```text
event_create_prepare/commit
→ event_operation_get до accepted event_id
→ promo_capabilities(event_id)
→ promo_campaign_create_prepare/commit
→ promo_operation_get
```

Promo tools:

```text
promo_capabilities
promo_campaigns_list
promo_campaign_get
promo_campaign_create_prepare / commit
promo_activity_add_prepare / commit
promo_operation_get
```

Reschedule не продлевает campaign end, exposure goal, limits или spend
автоматически. Если новая дата выходит за campaign window, prepare показывает
ineligibility. Cancellation/postponement прекращают future exposure; historical
exposures сохраняются.

Partner basic/free tier выражается policy verdict
`auto_approved|owner_review_required|denied`. Универсальный billing engine не
вводится.

## 13. Scopes и безопасность

Owner scopes:

```text
events:read
events:write
promo:read
promo:write
operations:read
```

Partner scopes:

```text
partner:events:read
partner:events:propose
partner:promo:read
partner:promo:request
partner:publications:read
```

Scope — только верхняя граница. Backend дополнительно проверяет tenant,
organization, role, ownership, state и revision. Partner token не принимается
owner resource и наоборот. Server query boundary исключает чужие rows до
serialization; client-side redaction недостаточна.

## 14. Обязательные автотесты

Канонический registry —
[`private-events-mcp-event-operations-scenarios.v2.yml`](../testing/private-events-mcp-event-operations-scenarios.v2.yml).
Каждый scenario ID обязан быть связан с автоматическим тестом.

Уровни:

1. unit — schemas, fact consistency, temporal roles, aliases, revisions;
2. integration — temp SQLite, real application services, Smart Update facade и
   JobOutbox;
3. protocol — MCP tools, OAuth resources, scopes, tenant isolation,
   idempotency;
4. worker — claim/supersession/reconciliation, fake providers;
5. site/render — public change projection и lifecycle badge;
6. isolated live — real MCP/auth/queue/worker path через test bot/private test
   destinations.

Regression contracts обязательно включают:

- `INC-2026-05-07-vk-time-reschedule-wrong-match`;
- `INC-2026-08-24-vk-lifecycle-replay-stale-tg-repost`.

Ни один CI или live test не разрешает production destination IDs. Provider
network adapters не вызываются в unit/integration tests.

## 15. Порядок реализации

### Slice A — structured core

- `models.py`, `db.py`, SQLite migration;
- `event_change_log` и publication/revision bindings;
- shared typed event-change service;
- hard fact-consistency service;
- create/edit/reschedule/postpone/cancel domain tests.

### Slice B — owner MCP

- owner write scopes и schemas;
- prepare/commit/status/idempotency;
- publication status/list/detail;
- tests catalogue/scope/write denial;
- без partner resource и deploy.

### Slice C — reconciliation и viewer output

- revision-bound outbox and supersession;
- original projection reconciliation;
- 24-hour notice policy;
- lifecycle notice workers;
- deterministic badge renderer;
- static-site status/history;
- worker/site/render tests.

### Slice D — promo и partner projection

- existing promo services through MCP;
- separate partner OAuth resource/catalog;
- tenant/organization/role policy;
- owner review and own-status readback;
- protocol/security tests.

### Release slice — Codex

После готовой запушенной integration branch и зелёных тестов Codex выполняет
только independent audit, rebase, full relevant suite, merge/deploy, SQLite
readiness, OAuth scope refresh, authenticated `tools/list`, isolated live
acceptance и release evidence. Codex не должен допроектировать продукт или
реализовывать пропущенный крупный slice на deployment checkout.

## 16. Non-goals

- второй EventsBot service/DB/scheduler;
- новый event parser вместо Smart Update;
- новая publication queue;
- новая promo data model;
- fuzzy target selection внутри mutation commit;
- конфликтующий public text с owner force bypass;
- неявное `create_event_and_campaign`;
- автоматическая публикация на surface, где событие ранее не публиковалось;
- изменение original poster bytes ради lifecycle badge;
- универсальный tariff/billing engine;
- partner access к internal incidents/operations/social workspace;
- MAX support без отдельного проверенного adapter contract.

## 17. Readiness verdict

Revision 3 закрывает выявленные audit blockers: indefinite postpone, structured
change history, authoritative publication age/revision, 24-hour lifecycle
notice, static old → new history, deterministic status badge, date-range policy
и implementation slicing.

**К реализации Slice A можно приступать без дополнительного продуктового
проектирования.** Любое новое расхождение должно оформляться как конкретный
contract gap и owner decision, а не решаться созданием параллельного контура.
