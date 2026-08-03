# Персонализация KenigEvents: контракт реализации

> **Статус:** нормативная техническая детализация к [`personalization-to-be.md`](personalization-to-be.md).
> **Дата среза:** 2026-08-02.
> **Применение:** обязательно для новых runtime, storage, transport, database и E2E-изменений персонализации.
> **Текущее состояние:** локальный prototype; production durable loop отсутствует.
> **Юридическая зависимость:** remote-profile и server signal writes остаются выключенными до закрытия отдельного юридического/localization release-gate и синхронизации публичных документов.

## 1. Зачем нужен отдельный implementation contract

Целевой документ уже хорошо определяет продуктовую модель, сигналы, горизонты,
качество и release gates. Для реализации ему не хватало пяти вещей:

1. одного обязательного cross-page runtime и fail-closed surface registry;
2. физического, а не только логического контракта хранения;
3. точного wire protocol для действий, ACK, reconcile и projection refresh;
4. миграционного плана из фактического inline-прототипа;
5. поэтапного контроля PR, чтобы нельзя было объявить персонализацию готовой по
   одному красивому примеру выдачи.

Этот файл закрывает именно эти пробелы. Он не меняет продуктовую цель и не
назначает недоказанные model weights.

## 2. Итоговое архитектурное решение

Сквозная персонализация строится из четырёх независимых слоёв:

```text
static canonical page + public event feature manifest
                        │
                        ▼
shared browser runtime (surface policy + local scorer + safe presenter)
                        │
            compact local state / bounded outbox
                        │
                        ▼
approved same-origin personalization API
                        │
            primary current-state store + materializer
                        │
                        ▼
versioned compact profile projection (periodic ETag refresh)
```

Обязательные свойства:

- статическая страница остаётся полной и полезной без JavaScript и backend;
- один scorer, одна typed action model и одна storage implementation используются
  всеми page families;
- основной порядок явно календарной/хронологической выдачи не персонализируется;
  там применяется только exact hide;
- отдельная рекомендационная полка под календарным списком считается другой
  surface и может персонализироваться;
- обычный локальный rerank не делает сетевой запрос;
- remote profile не пересчитывается и не скачивается после каждого действия;
- browser не передаёт private profile в public Supabase RPC и не пишет в
  private tables;
- при неизвестной surface, несовместимой schema или ошибке runtime сохраняется
  исходный статический порядок;
- exact hide и reset имеют более высокий приоритет над telemetry, cache и model
  quality;
- transport outage никогда не блокирует CTA, календарь, переход по ссылке или
  общую статическую выдачу.

## 3. Что запрещено переносить из прототипа

Новые изменения не должны закреплять следующие переходные решения:

- дальнейшее расширение giant inline-script в `EventLayout.astro`;
- `consent_ok` как признак активации;
- отдельный dialog `Пока нет / ОК` перед like или `Не интересно`;
- создание долговременного `session_id` внутри localStorage-профиля;
- хранение одинакового event id одновременно в `not_interested_event_ids` и
  `hidden_event_ids`;
- превращение одного exact-hide в отрицательный semantic facet без отдельной
  typed-причины и подтверждённой повторяемости;
- два и более scorer/ranker с независимо меняющимися весами;
- browser → public Supabase RPC с полным локальным profile snapshot для durable
  персонализации;
- raw local feedback log как источник истины;
- reset, очищающий только браузер;
- автоматический replay неизвестно завершившейся записи без idempotency key;
- server row на каждый impression, open, scroll или dwell;
- новая page-specific персонализация без регистрации surface policy и route
  acceptance.

## 4. Один cross-page runtime

### 4.1. Точка подключения

Все публичные HTML page families подключают один компонент:

```text
site/src/components/personalization/PersonalizationRuntime.astro
```

Он монтируется через общий layout/shell, а не копируется по страницам.
Machine artifacts (`json`, `xml`, `ics`, media, service-worker assets), admin и
явно изолированные test artifacts исключаются из browser runtime.

Сборка обязана сформировать route inventory и доказать для каждого public HTML:

- runtime присутствует ровно один раз;
- указан `surface_id`;
- разрешённая policy найдена;
- либо route намеренно объявлена `static-only`.

### 4.2. Каноническая модульная граница

```text
site/src/lib/personalization/
  contract.ts             typed versions, action/surface enums
  storage.ts              browser envelope, migration, quota/eviction
  session.ts              sessionStorage + tab/session rotation
  explicit-state.ts       like/save/hide/undo/reset overlay
  profile-projection.ts   validation and compatibility
  scorer.ts               one pure deterministic scorer
  surface-policy.ts       eligibility/ranking policy registry
  presenter.ts            invisible-tail/frozen-prefix DOM application
  transport.ts            batch/outbox/ACK/reconcile/refresh
  controller.ts           orchestration only
  test-api.ts             sanitized test/staging snapshot
```

`PersonalizationRuntime.astro` только передаёт build/config/data attributes и
запускает `controller.ts`. Scoring, storage и transport не живут внутри Astro
markup.

### 4.3. Fail-closed правило

Если route не зарегистрирована, policy невалидна либо feature/taxonomy version
несовместима:

1. исходный статический порядок не меняется;
2. remote signals не отправляются;
3. exact hide применяется только при наличии валидного event id и совместимого
   explicit state;
4. runtime пишет санитарный diagnostic code в test API, но не показывает
   техническую ошибку обычному пользователю.

Нельзя использовать глобальный `default-personalize-everything`.

## 5. Surface policy и сквозное применение

### 5.1. Общая модель

Каждая surface описывает отдельно:

- eligibility;
- ranking strength;
- hard exclusions;
- frozen-prefix/presentation policy;
- допустимые signals;
- candidate/DOM limits;
- fallback;
- quality fixture key.

Пример registry находится в
[`collection-surfaces-v1.example.json`](collection-surfaces-v1.example.json).

### 5.2. Нормативная матрица v1

| Surface | Основной порядок | Персонализация | Exact hide | Особое правило |
|---|---|---:|---:|---|
| `calendar_primary` | строгая дата/время | нет | да | никакого rerank |
| `today_primary` / `tomorrow_primary` | дата/время | нет | да | chronological truth сохраняется |
| `weekend_primary` | день + время | нет | да | суббота/воскресенье не смешиваются |
| `calendar_personal_tail` | отдельная полка ниже списка | strong/medium | да | не меняет календарную часть |
| тематическая подборка | редакционный/тематический baseline | weak | да | только tie-break/tail rerank |
| `popular_primary` | popularity/freshness | tie-break only | да | популярность остаётся главным смыслом |
| `free_primary` | free eligibility | weak | да | платное никогда не спасается score |
| `children_primary` | child eligibility | weak | да | age/safety hard gate |
| `exhibitions_primary` | lifecycle/open-close logic | weak | да | lifecycle важнее profile |
| `search_results` | query relevance | tie-break only | да | query intent всегда главнее |
| `event_detail_related` | current-event similarity | anchor-first | да | profile не разрушает relatedness |
| `for_me` | eligibility + profile | strong | да | diversity/exploration обязательны |
| unknown surface | static baseline | нет | совместимый exact only | diagnostic + fail closed |

### 5.3. Что значит «работает на всех страницах»

Это не означает одинаковую сортировку всего сайта. Это означает:

- один explicit state и один reset действуют сквозным образом;
- один profile projection доступен всем зарегистрированным surfaces;
- все event-card renderers используют одинаковые event ids/actions;
- календарные primary lists уважают hide, но не теряют хронологию;
- тематические surfaces получают слабый локальный rerank;
- `Для меня` получает сильный ranker;
- search, popular и related сохраняют собственный главный смысл;
- unknown/new route не персонализируется, пока для неё не добавлены policy и
  acceptance tests.

## 6. Browser state: компактная и безопасная модель

### 6.1. Разделение хранилищ

| Storage | Что хранит | Чего не хранит |
|---|---|---|
| `localStorage` | один atomic current-state envelope, compact projection, authoritative local overlay, reset epoch | raw history, full manifests, session id, emails/tokens |
| `sessionStorage` | rotating session id, campaign/test context, visible/frozen ids, transient UI state | durable profile |
| IndexedDB | bounded unsent action outbox и quarantine | бесконечный event log |
| memory | full current page manifest, score plan, served summary до strong action | долговременные данные |

При отсутствии IndexedDB outbox использует один ограниченный localStorage
fallback. Это degraded mode, а не второй постоянный журнал.

### 6.2. Канонические keys

```text
ke_p13n_state_v1             localStorage, один current-state envelope
ke_p13n_outbox_fallback_v1   localStorage, только degraded fallback
ke_p13n_session_v1           sessionStorage
ke_p13n_route_health_v1      localStorage, короткоживущий transport hint
```

Legacy keys читаются только мигратором и после успешной атомарной записи нового
state удаляются. Production runtime не должен продолжать dual-write.

### 6.3. Atomic envelope

Контракт задан схемой
[`personalization-browser-state-v1.schema.json`](schemas/personalization-browser-state-v1.schema.json).

Обязательные поля:

- schema/version;
- activation state и reset epoch;
- monotonic local `write_seq`;
- совместимые model/feature/taxonomy versions;
- последняя server projection revision/ETag/refresh deadline;
- компактные exact states;
- immediate session overlay;
- last successful compaction/migration metadata.

Запись выполняется как `normalize → validate → stringify → size check → single
setItem`. Частично обновлённые наборы ключей запрещены.

Checksum может обнаруживать повреждение cache, но не является авторизацией.
Любой server mutation повторно валидирует ownership, sequence и payload.

### 6.4. Не дублировать exact state

Вместо отдельных массивов `not_interested` и `hidden` хранится один typed набор:

```text
[event_id, state_code, state_seq, expires_day]
```

Стартовые коды:

- `h` — exact hide;
- `l` — like;
- `s` — saved/favorite при включении общей модели;
- `u` — локальный undo-pending marker, не отправленный как отдельный durable
  интерес.

Один event имеет одно текущее состояние на тип действия. История живёт только в
bounded server ledger до materialization/retention cleanup.

### 6.5. Facets и веса

В browser projection используются:

- стабильные taxonomy ids, а не длинные произвольные строки;
- fixed-point integers, например `-1000..1000`, а не длинные JSON float;
- top-K sparse facets по каждому горизонту;
- отсутствие dense embedding;
- отсутствие raw evidence/history;
- отсутствие sensitive-topic facets.

Delta/base64-varint encoding event ids допускается только после benchmark,
если оно даёт не менее 30% выигрыша на реальном p95-профиле и проходит fuzz,
migration и debug tooling. Для небольших bounded массивов обычные JSON integers
предпочтительнее сложной компрессии.

## 7. Экологичный localStorage budget

### 7.1. Бюджеты

| Состояние | Бюджет |
|---|---:|
| steady-state target | `<= 24 KiB` |
| warning/forced compaction | `> 32 KiB` |
| hard write budget | `<= 48 KiB` |
| aggregate emergency ceiling вместе с другими KenigEvents keys | `64 KiB` |
| IndexedDB outbox | `<= 16 actions`, `<= 12 KiB`, TTL `24h`, attempts `<=5` |
| localStorage outbox fallback | `<= 8 actions`, `<= 8 KiB` |

Supabase Auth storage не читается, не переписывается и не входит в automatic
compaction KenigEvents.

### 7.2. Рекомендуемое распределение steady-state

| Часть | Target |
|---|---:|
| versions/activation/reset/meta | `<= 1 KiB` |
| profile projection | `<= 8 KiB` |
| exact explicit state | `<= 8 KiB` |
| immediate overlay | `<= 3 KiB` |
| refresh/transport hints | `<= 1 KiB` |
| reserve/migration | `<= 3 KiB` |

Это бюджеты, а не разрешение заполнять каждую часть до максимума.

### 7.3. Eviction priority

Удалять в таком порядке:

1. expired feed/search/image hints и legacy preview caches;
2. debug samples и уже ACKed/rejected outbox entries;
3. weak session overlay старых sessions;
4. stale non-authoritative projection, если server revision можно получить позже;
5. старые positive preferences после server-confirmed compaction.

Нельзя автоматически выбрасывать:

- reset epoch;
- pending exact hide/undo;
- unsent strong actions;
- последний совместимый authoritative explicit state;
- activation/document version marker, пока remote profile активен.

Если exact action не удалось надёжно записать локально, UI не говорит
«сохранено». Действие применяется в memory для текущего экрана, показывается
доступное предупреждение о временной невозможности сохранить и повторяется
только по bounded/idempotent contract.

### 7.4. Lifecycle cleanup

- события после завершения + safety window удаляются из local exact state после
  подтверждённой server compaction;
- full card manifests не сохраняются;
- per-preview keys запрещены;
- session id меняется при новой browser session и не переживает закрытие session;
- `storage`/`BroadcastChannel` синхронизируют только revision/delta, а не
  пересылают raw profile между tabs.

## 8. Compact physical database model

### 8.1. Principle

Supabase/Postgres либо иной утверждённый primary store хранит **текущее
состояние и короткий bounded ledger**, а не полный clickstream.

Рекомендуемая private schema:

| Relation | Cardinality | Назначение |
|---|---:|---|
| `p13n_subject` | 1 / active subject | внутренний `bigint` key, auth/anonymous ownership mapping, epoch/status |
| `p13n_activation` | 1 / activation epoch | минимальное доказательство старта/reset и версии документов |
| `p13n_current` | 1 / subject | compact explicit state, top facets, counters, latest sequence |
| `p13n_projection` | 1 current + максимум 1 previous / subject | browser-safe projection, revision, ETag, refresh policy |
| `p13n_action_recent` | bounded recent rows | idempotent strong actions до materialization/TTL |
| `p13n_identity_link` | bounded / subject | anonymous→auth merge audit/state |
| `p13n_materialization_queue` | максимум 1 pending / subject | coalesced due work, не одна job на action |

`served_list_summary` не получает строку на каждый page view. Он сохраняется
только:

- вместе с strong action, если нужен для проверки rank/context;
- либо в заранее ограниченной analytics sample;
- либо как compact session aggregate с TTL.

### 8.2. Физические правила

- внутренний subject key — `bigint`; UUID/auth ids находятся только в ownership
  mapping;
- browser не выбирает subject id самостоятельно;
- current JSONB/arrays не получают GIN index, если нет доказанного query;
- основной доступ — PK/BTREE по subject;
- recent action uniqueness — `(subject_id, client_event_id)`;
- materialization queue coalesces повторные действия в одну due row;
- explicit event ids хранятся как compact sorted arrays/tuples или отдельные
  rows только после реального benchmark обоих вариантов;
- canonical event descriptions/media/source text не копируются;
- weak raw telemetry не попадает в primary profile store;
- previous projection удаляется после compatibility/rollback window;
- table grants отсутствуют для browser roles; mutation/read идут через audited
  functions/API.

### 8.3. Storage budgets и capacity gate

До production migration должны быть измерены:

```text
measured_current_bytes_per_subject =
  (total relation + index bytes для subject/current/projection/activation)
  / fixture subjects

forecast_total =
  existing_database_bytes
  + active_subjects * measured_current_bytes_per_subject
  + retained_actions * measured_action_bytes
  + materialization/maintenance reserve
```

Engineering targets:

- `p13n_current` p95 row payload `<= 1.5 KiB`;
- browser projection payload p95 `<= 8 KiB`, preferred `<= 4 KiB`;
- current-state relations + indexes target `<= 3 KiB` на active subject;
- average retained strong actions после compaction `<= 24` на active subject;
- absolute action cap `256` на subject с forced materialization/compaction;
- никаких unbounded text/JSON keys;
- минимум 20% plan capacity остаётся свободным после 100k fixture forecast;
- disposable telemetry shed раньше current state, hide, favorites, reset и send
  controls.

Числа подтверждаются SQL fixture runs на `1k`, `10k`, `100k` subjects через
`pg_column_size`, `pg_total_relation_size`, index attribution и VACUUM-aware
report. Оценка «примерно столько-то байт» без фактической migration не закрывает
gate.

При прогнозе выше текущего plan ceiling выпуск блокируется: нельзя надеяться на
будущую очистку или TOAST как на стратегию ёмкости.

### 8.4. Retention

Стартовые operational bounds, требующие финальной legal/owner проверки:

- acknowledged recent strong action: `30d`, затем compact/delete;
- action-bound served evidence: `14d`;
- rejected/quarantined payload: `7d`, без secret/raw text;
- previous projection: `7d` либо одна previous revision;
- reset/delete tombstone: до завершения purge/replay protection;
- inactive profile compaction/anonymization после `365d` — открытое решение,
  которое нельзя молча включать.

## 9. Same-origin wire protocol

### 9.1. Endpoints

```text
POST /api/personalization/v1/actions:batch
GET  /api/personalization/v1/projection
POST /api/personalization/v1/reset
POST /api/personalization/v1/link          server-owned auth transition
GET  /api/personalization/v1/state         limited reconcile, не raw profile
```

Browser не отправляет Supabase service key и не вызывает private profile tables.
Durable endpoint определяет subject по защищённой same-origin credential/session,
а не по доверенному `anon_id` из JSON.

### 9.2. Batch shape

Normative JSON Schema:
[`personalization-action-batch-v1.schema.json`](schemas/personalization-action-batch-v1.schema.json).

Batch header содержит общие поля один раз:

- schema/build/model/taxonomy versions;
- stable `batch_id`;
- client instance и activation/reset epoch;
- `base_profile_revision`;
- sequence range;
- версии contract/privacy/rules только при activation/change;
- bounded actions.

Action не повторяет email, subject id, bearer token, full route, full title или
profile snapshot. Он содержит stable idempotency id, monotonic sequence, typed
action, target id, surface code, timestamp bucket и при необходимости
served-list hash/rank.

### 9.3. ACK shape

```json
{
  "schema_version": "personalization-ack-v1",
  "batch_id": "...",
  "accepted_through_seq": 42,
  "accepted_ids": ["..."],
  "rejected": [{"id":"...","code":"expired_target"}],
  "explicit_delta": {"revision": 18, "states": []},
  "profile_hint": {"revision": 9, "refresh_after":"2026-08-03T08:00:00Z"},
  "retry_after_ms": 0
}
```

Client удаляет outbox item только после валидного ACK либо reconcile, который
однозначно подтверждает его state.

### 9.4. HTTP semantics

| Result | Client behavior |
|---|---|
| `200`/`201` | validate ACK, commit explicit delta, remove accepted ids |
| `202` | считается accepted только если server **durably queued** batch и вернул receipt |
| `304` projection | сохранить revision/ETag, сдвинуть refresh deadline |
| `400`/`422` | permanent reject отдельных items; quarantine sanitized evidence |
| `401`/`403` | stop remote flush, keep bounded local state, start auth/credential recovery |
| `409` | sequence/revision reconcile; не blind replay |
| `413` | deterministic batch split; single oversized item reject |
| `429` | respect `Retry-After`, no alternate hammering |
| `5xx`/network before dispatch | bounded retry/backoff |
| timeout/connection loss after dispatch | ambiguous; reconcile by idempotency id |

## 10. Transport state machine

### 10.1. Policies

- `safe-read`: projection/state read; alternate healthy route допускается один
  раз;
- `idempotent-replay`: action batch/reset с stable idempotency key;
- `selected-once`: только операции, для которых нет idempotent server contract;
- `disposable`: weak analytics; может быть отброшена первой.

Personalization strong writes должны быть `idempotent-replay`, а не
`selected-once`.

### 10.2. Direct/relay reliability

Существующий resilient transport можно переиспользовать как infrastructure
pattern, но durable personalization API остаётся same-origin/private. Публичный
Supabase profile RPC не становится fallback.

Обязательная fault matrix:

| Состояние | Ожидаемый результат |
|---|---|
| direct Supabase path unavailable, Yandex relay path healthy | разрешённая операция проходит через relay один раз |
| Yandex relay unavailable, direct path healthy | проходит direct |
| оба network paths недоступны | strong action остаётся в bounded outbox, UI/static fallback работают |
| relay доступен, но primary datastore недоступен | только durable `202 receipt` либо transient failure; нельзя ложно ACK |
| headers получены, body оборван | ambiguous/reconcile |
| direct вернул definitive 4xx | alternate route не используется для обхода policy |
| route flaps между tabs | single-flight selection + shared cooldown |

Здесь различаются **network path outage** и **primary datastore outage**.
Yandex relay не доказывает доступность Supabase. Серверная очередь/YMQ может
давать durable `202`, но только после отдельного ownership, encryption,
retention и replay review; она не становится вторым profile SOR.

### 10.3. Flush cadence

Outbox flush запускается только когда есть pending strong actions:

- сразу после local durable write;
- при `online`;
- на следующей navigation/session;
- в bounded idle window;
- вручную в test/operator flow.

Обычный page view без pending state не создаёт personalization request.

`sendBeacon` не используется для authoritative strong actions: он не даёт
полноценного ACK/reconcile. `fetch(..., {keepalive:true})` допускается только
для малого same-origin idempotent batch и не заменяет следующий reconcile.

### 10.4. Backoff/circuit

- exponential backoff с jitter;
- server `Retry-After` главнее local schedule;
- circuit открывается отдельно по capability/route;
- half-open probe single-flight;
- attempts не сгорают при offline/foreign-channel condition;
- после TTL action либо безопасно compact/reject, либо требует operator review;
- exact state не исчезает из UI только потому, что transport exhausted.

## 11. Profile materialization и refresh

### 11.1. Immediate overlay

Like/hide/save меняют local explicit overlay сразу после успешной локальной
записи. Это даёт мгновенный UI и local ranking effect без ожидания materializer.

Overlay:

- типизирован;
- bounded;
- не строит long-term persona;
- не превращает single hide в semantic dislike;
- объединяется со stable projection детерминированно;
- очищается/сворачивается после authoritative server delta/revision.

### 11.2. Materializer

Materializer запускается:

- по расписанию;
- после threshold сильных действий;
- после link/reset/important explicit state change;
- не чаще установленного per-subject minimum interval, кроме correctness events.

Несколько pending действий coalesce в одну materialization job.

### 11.3. Projection refresh

- только для `active` personalization;
- после static paint и предпочтительно в idle;
- начальная гипотеза: не чаще `1/24h` при отсутствии change hint;
- `If-None-Match`/ETag;
- single-flight через Web Locks либо lease с expiry;
- валидируется schema/model/taxonomy и payload size;
- новая revision публикуется атомарно;
- invalid/stale/timeout сохраняет последнюю совместимую projection;
- link/reset может форсировать один refresh/reconcile.

Projection schema:
[`personalization-profile-projection-v1.schema.json`](schemas/personalization-profile-projection-v1.schema.json).

## 12. Exact hide, undo и hidden collection

### 12.1. UI state machine

```text
visible
  → pending-hide (card muted + plate + progress + Undo)
  → committed-hide (после undo window)
  → removed-with-anchor-preserved
  → visible-in-hidden-collection
```

Undo window — configurable hypothesis, стартово 5 секунд.

До завершения окна:

- event не отправляется как committed hide;
- card не выдёргивается из DOM;
- focus/scroll anchor сохраняется;
- повторное действие idempotent.

После commit exact hide применяется ко всем surfaces, включая calendar/search,
но прямой URL события остаётся доступен.

### 12.2. Semantic separation

Exact hide отвечает только на вопрос «не показывать этот event/family». Для
semantic negative preference нужна typed reason и/или повторяемый evidence.

Запрещено:

```text
one exact hide → negative facet category/genre
```

без отдельного правила model evidence. Sensitive events никогда не создают
semantic facet.

### 12.3. Recovery

Mobile menu содержит доступную collection `Помечены «не интересует»` с:

- bounded pagination/current active items;
- restore;
- source/date context;
- отсутствием SEO/index exposure персонального state;
- remote/local reconciliation.

## 13. Safe presenter и невидимая область

### 13.1. Frozen prefix

Перед DOM mutation runtime строит frozen set из карточек, которые:

- пересекаются с viewport + safety margin;
- содержат focus;
- были clicked/acted;
- находятся выше stable anchor;
- уже были объявлены screen reader interaction flow.

Переставляется только невидимый tail. При любом сомнении порядок не меняется.

### 13.2. Предпочтительный способ

Для конечной тематической подборки:

1. static HTML содержит baseline order;
2. runtime загружает local projection синхронно из cache;
3. до достижения блока пользователем рассчитывает rank plan;
4. применяет plan только к полностью невидимому tail;
5. сохраняет static fallback при JS/storage/schema error.

Для календарного primary list plan = identity order после exact exclusions.

### 13.3. DOM contract

Каждая персонализируемая collection объявляет:

```text
data-p13n-surface
 data-p13n-list
 data-event-id
 data-event-family-id (optional)
 data-static-rank
 data-feature-ref / bounded inline feature codes
```

Page adapter не имеет собственного scorer. Он только предоставляет кандидатов и
вызывает shared presenter.

## 14. Activation, identity, link, reset и delete

### 14.1. Activation

Первый допустимый functional action создаёт `activation epoch` и минимальное
versioned evidence. Нажатие notice `Понятно`, view, scroll, dwell, login или PWA
install не активируют personalization.

Legacy `consent_ok` мигрируется так:

- наличие старого profile само по себе не создаёт server activation;
- exact local states могут быть импортированы как device-local legacy state;
- remote sync начинается только после нового допустимого action и актуальных
  document versions;
- старый dialog удаляется, а не переименовывается.

### 14.2. Anonymous credential

- random secret генерируется browser/endpoint flow;
- server хранит keyed hash/proof, а не доверяет plain `anon_id`;
- rotation/link/reset создают новый epoch;
- browser payload не может назначить чужой subject;
- device fingerprint не используется как ownership proof.

### 14.3. Login/link

Link выполняется server-side после подтверждённой auth session:

- idempotent;
- explicit authenticated state выигрывает конфликт;
- exact hides объединяются union, кроме более нового explicit restore;
- raw browsing history не переносится;
- merge audit bounded;
- tabs получают новую revision/epoch;
- logout не удаляет durable account profile;
- unlink/reset/delete — разные операции.

### 14.4. Reset/delete

Reset:

1. немедленно создаёт local reset epoch;
2. очищает projection/overlay, но сохраняет pending reset receipt;
3. переводит UI в static mode;
4. idempotently отправляется same-origin API;
5. server invalidates old epoch/revisions и запускает purge;
6. ACK завершает local cleanup.

Delete account/data дополнительно следует account-level legal/identity contract.
Local reload без remote receipt не считается полным reset.

## 15. Signals и data minimization

### 15.1. Strong actions v1

- `like_set` / `like_unset`;
- `hide_commit` / `hide_restore`;
- `save_set` / `save_unset`, когда favorites contract объединён;
- `cta_ticket` / `cta_registration` только после navigation не блокируя её;
- `attendance_confirmed` и repeat attendance только через отдельный verified
  flow;
- `interest_profile_change`;
- `personal_mode_enabled`.

`share` — strong short-horizon evidence после успешного share/copy outcome, но не
первый activation по умолчанию.

### 15.2. Weak signals

Weak telemetry не входит в MVP durable loop. До отдельного gate запрещено
материализовать interests из page view, scroll, dwell или quick skip.

Позже weak signals:

- только после activation/нужного purpose basis;
- sampled/aggregated;
- YDB TTL либо вообще не собираются;
- campaign/easter-egg context отделён;
- не блокируют strong action path;
- не копируются в Supabase raw firehose.

### 15.3. Served evidence

Full served list существует в memory. При strong action отправляется только
минимум, необходимый для проверки:

- served hash/id;
- event id/rank;
- surface/policy/model version;
- bounded neighbouring/summary ids при доказанной необходимости.

Полный массив score components не хранится на каждый показ.

## 16. Security и privacy implementation gates

- private schema не exposed через Data API;
- browser roles не имеют direct table DML;
- same-origin API валидирует Origin/CSRF/session/device proof/schema/size/rate;
- idempotency enforcement server-side;
- no email/token/raw profile in logs, HTML, artifacts или YDB analytics;
- sensitive topics fail closed и не создают facets;
- test API отсутствует/санитизирован в production;
- projection не является authorization artifact;
- public event manifest не содержит private profile;
- localization/owner ADR закрыт до remote writes;
- юридический release-gate и версии публичных документов синхронизированы с
  фактическим data flow.

## 17. Observability без раздувания хранения

Минимальные aggregates:

- active/inactive/reset subjects;
- action batch accepted/rejected/deduped/ambiguous;
- outbox depth/age/expiry client samples;
- projection 200/304/invalid/stale;
- materialization queue age/coalescing;
- relation/index bytes and bytes/active subject;
- exact-hide resurrection invariant;
- surface fallback/unknown-policy count;
- rerank applied/skipped by frozen-prefix reason;
- cards-to-first-relevant, diversity, hide rate, CTA/save/attendance;
- direct/relay/no-route outcomes по operation class.

Raw per-user dashboards и raw profile dumps запрещены. Operator drill-down требует
sanitized subject correlation и ограниченный срок.

## 18. Feature flags и rollout

```text
PUBLIC_P13N_RUNTIME_MODE=
  off | characterize | local-shadow | local-on | sync-shadow | sync-on

P13N_SERVER_ACCEPT_WRITES=false|true
P13N_MATERIALIZER_ENABLED=false|true
P13N_PROJECTION_READ_ENABLED=false|true
P13N_SURFACE_POLICY_VERSION=collection-surfaces-v1
```

Semantics:

- `off`: static + legacy cleanup only;
- `characterize`: записывает только test diagnostics, DOM не меняет;
- `local-shadow`: считает plan, не применяет;
- `local-on`: local exact/weak rerank без server profile;
- `sync-shadow`: server loop работает на test cohort, DOM использует old/local;
- `sync-on`: production projection участвует в разрешённых surfaces.

Rollout: internal/test → focus cohort → 1% → 5% → 25% → 100% с явным hold и
rollback на каждом этапе. Assignment стабилен и не зависит от будущего
поведения.

Emergency rollback всегда может:

- остановить writes/materializer;
- оставить static page + exact local state;
- отключить rerank без удаления profile;
- отозвать несовместимую projection/model version.

## 19. Реализационные волны

### Wave 0 — characterization и extraction

- зафиксировать поведение current prototype тестами;
- создать shared modules без изменения видимого результата;
- удалить возможность добавлять новый personalization code в giant inline block;
- добавить route/surface inventory и fail-closed registry;
- создать legacy-key migration tests;
- никаких DB migrations и remote writes.

Полное задание: [`tasks/personalization-wave-0.md`](tasks/personalization-wave-0.md).

### Wave 1 — activation + compact local state

- убрать consent dialog/`consent_ok`;
- реализовать activation epoch;
- atomic browser envelope, session rotation, aggregate quota/eviction;
- typed exact state, 5s undo, hidden collection;
- BroadcastChannel/storage sync;
- local-only, server flags off.

### Wave 2 — canonical surface runtime

- один scorer/surface engine;
- calendar exact-only;
- weak thematic tail rerank;
- related/search/popular invariants;
- frozen prefix presenter;
- cross-route Playwright matrix.

### Wave 3 — same-origin durable transport

- IndexedDB outbox + fallback;
- action batch schema/ACK/reconcile;
- direct/relay fault matrix;
- server writes ещё могут идти в isolated staging schema;
- no materialized profile in production.

### Wave 4 — compact primary-store schema

- reviewed migrations/RLS/functions;
- 1k/10k/100k capacity fixtures;
- activation/current/recent/projection/link/reset;
- purge/retention jobs;
- longitudinal staging E2E.

### Wave 5 — materializer + projection

- scheduled/threshold coalescing;
- versioned projection/ETag refresh;
- immediate overlay reconciliation;
- quality replay/model registry;
- sync-shadow.

### Wave 6 — controlled product rollout

- `Для меня` strong;
- thematic weak;
- experiment/guardrails/rollback;
- focus-group + Android/iOS/browser reliability runs;
- final legal/localization/ops sign-off.

## 20. PR control rules

Каждый PR:

1. решает одну волну/подзадачу;
2. содержит characterization или contract test до изменения behavior;
3. перечисляет touched surfaces;
4. не смешивает model-weight tuning, transport и UI polish;
5. прикладывает before/after storage bytes;
6. прикладывает network trace с redaction;
7. обновляет `implementation-status.yml`;
8. не объявляет live evidence без реального run/artifact;
9. остаётся draft, пока hard gates не подтверждены;
10. не удаляет static fallback.

Нельзя закрыть этап по unit tests, которые regex-проверяют наличие функции в
`EventLayout.astro`. Для production path нужны importable pure-module tests,
real Astro build и browser E2E.

## 21. Обязательная test matrix

### 21.1. Unit/contract

- schema validation/migration/corruption;
- byte budgets/eviction priority;
- typed action and exact-state transitions;
- scorer invariants и deterministic tie breaks;
- unknown surface fail closed;
- batch split/ACK/reject/409/413/429;
- idempotent replay и ambiguous reconcile;
- sensitive/campaign exclusion.

### 21.2. Browser E2E

- public route inventory: один runtime/валидная policy;
- no JS/no storage/backend outage;
- visible/focused/acted cards never move;
- calendar order unchanged;
- thematic hidden tail reranks locally with zero network;
- hide → pending → undo/commit → all routes → hidden collection;
- tabs/reload/session rotation;
- local quota/full/corrupt state;
- reset local + remote receipt;
- anonymous→auth link;
- projection ETag 304/new/invalid/timeout;
- Android emulator, iOS simulator и desktop browser для critical/focus scenarios.

### 21.3. Transport fault matrix

- Supabase direct network unavailable / Yandex relay available;
- Yandex unavailable / direct available;
- both unavailable;
- datastore unavailable behind healthy relay;
- headers/body truncation;
- slow timeout before/after dispatch;
- stale route cache/circuit recovery;
- duplicate batch/reordered responses;
- offline queue expiry;
- no secret/raw profile in artifacts.

Deterministic emulated fault tests обязательны в PR CI. Защищённый live canary
проверяет реальную конфигурацию отдельно; отсутствие live secrets не превращает
mock PASS в доказательство production connectivity.

### 21.4. Capacity

- local state p50/p95/max bytes на realistic fixtures;
- IndexedDB fallback behavior;
- DB relation/index bytes 1k/10k/100k;
- compaction/retention under repeated actions;
- request count: ordinary local rerank = 0;
- materializer coalescing;
- projection 304 rate и refresh storm protection.

## 22. Hard NO-GO

- legacy consent dialog/`consent_ok` остаётся production activation path;
- private profile отправляется в public RPC;
- exact hide создаёт semantic negative facet из одного события;
- reset только local;
- неизвестная surface персонализируется;
- calendar primary order меняется;
- видимая/focused/acted card перемещается;
- strong action теряется или blind-replayed после ambiguous outcome;
- server ACK возвращён без durable commit/queue;
- per-impression Supabase rows;
- localStorage >64 KiB либо нет deterministic eviction;
- DB capacity model отсутствует/превышает ceiling;
- sensitive facet materialized;
- direct/relay tests выданы за live backend evidence;
- public release до legal/localization gate.

## 23. Решения, которые зафиксированы этим документом

1. **Cross-page означает общий runtime и state, а не одинаковый ranker.**
2. **Calendar primary lists — exact-hide-only.** Персональная полка под ними —
   отдельная surface.
3. **LocalStorage steady target 24 KiB; 64 KiB — аварийный aggregate ceiling, а
   не рабочая норма.**
4. **Один atomic state key; outbox — IndexedDB.**
5. **No raw history/full manifests/dense embeddings в browser profile.**
6. **No per-impression rows в primary DB.**
7. **Browser durable profile/actions идут через same-origin API.** Public
   Supabase RPC может обслуживать только обезличенный public/catalog read.
8. **Strong actions idempotent и reconcile-able.**
9. **Projection обновляется периодически, обычный rerank — zero-network.**
10. **Phase 0 сначала извлекает и характеризует текущий код; новые tables/model
    weights до этого не добавляются.**

## 24. Открытые owner/legal решения

Эти вопросы не блокируют Wave 0–2, но блокируют соответствующий production flow:

- inactive-profile compaction/anonymization после 365 дней;
- окончательная primary-store/localization схема;
- точный undo duration после mobile usability test;
- включение verified CTA/attendance evidence;
- допустимость и цель weak telemetry;
- final model weights/refresh threshold;
- server-side durable queue при временной недоступности primary store.

Все остальные базовые архитектурные решения для начала реализации определены.
