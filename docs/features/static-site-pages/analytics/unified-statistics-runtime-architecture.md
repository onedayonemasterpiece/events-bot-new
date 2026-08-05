# Единый runtime продуктовой статистики

> **Статус:** принятый TO-BE-контракт и начатый implementation foundation.  
> **Дата среза:** 2026-08-05.  
> **Родитель:** [Analytics](README.md).  
> **Метрики:** [product-measurement-extension.md](product-measurement-extension.md).  
> **Хранение:** [storage-retention-architecture.md](storage-retention-architecture.md).  
> **Машиночитаемые контракты:** [statistics-event-catalog.v1.yml](statistics-event-catalog.v1.yml), [statistics-batch-v1.schema.json](schemas/statistics-batch-v1.schema.json).

## 1. Решение

В сервисе вводится один унифицированный путь для продуктовой статистики:

```text
feature adapter
  -> versioned event catalog
  -> consent / privacy / test-traffic gate
  -> in-session accumulator
  -> compact batch
  -> bounded idempotent outbox
  -> resilient direct/relay transport
  -> first-party ingest
  -> schema validation + dedupe + quarantine
  -> compact online facts
  -> daily/monthly aggregates
  -> Parquet archive in Object Storage
  -> TTL / verified deletion
```

Ни одна feature не должна самостоятельно решать:

- как формируется actor/session identity;
- какие поля допустимы;
- требуется ли consent;
- как дедуплицировать событие;
- когда batching обязателен;
- какой direct/relay route выбрать;
- когда повторять отправку;
- куда писать в YDB/Supabase/Object Storage;
- сколько хранить actor/session detail;
- как исключить preview, автотесты и ботов.

Feature передаёт **семантический факт** в единый client. Владелец runtime отвечает
за доставку, компактность, приватность, версии и наблюдаемость.

## 2. Почему отдельный документ обязателен

Существующий аналитический контракт уже унифицирует определения метрик и
хранение, но не закрывает service-wide runtime migration. В коде есть хорошие
надёжные примитивы — `idempotentOutbox`, `resilientDataClient`, operation
catalog, bounded storage и direct/relay route selection — однако отдельные
features всё ещё могут иметь собственные payload, endpoint, dedupe и retry.

Это создаёт четыре риска:

1. одинаковое действие считается по-разному;
2. слабая browser-телеметрия получает надёжность и срок хранения сильного факта;
3. feature случайно пишет raw clickstream или чувствительные поля;
4. при отказе транспорта одни события теряются, другие создают retry storm.

Поэтому «единая статистика» означает не общий dashboard и не одну таблицу, а
**один контракт от UI до удаления данных**.

## 3. Три lane, которые нельзя смешивать

### 3.1 `product_fact`

Сильный продуктовый факт из authoritative receipt или подтверждённого state
transition:

- favorite/calendar save accepted;
- like current state accepted;
- reminder subscription accepted/cancelled;
- email/Push delivery receipt;
- feedback accepted;
- external navigation actually initiated;
- auth/linking command accepted.

Правила:

- источник — server/service receipt, не оптимистичный DOM click;
- обязателен стабильный `idempotency_key`;
- повторная доставка — no-op;
- может иметь более длинный operational TTL по domain policy;
- product state остаётся в своём SOR; analytics хранит только минимальный receipt.

### 3.2 `product_observation`

Слабое browser-наблюдение:

- карточка действительно была видна;
- достигнут semantic checkpoint описания;
- Hero Talk был виден/пройден/нажат;
- keyboard hint был виден;
- пользователь дошёл до CTA;
- достигнут максимум позиции в выдаче.

Правила:

- требует применимого analytics/personalization consent;
- не отправляется на каждый DOM/scroll tick;
- агрегируется в browser session;
- хранится как count/max/mask/bucket;
- actor/session detail живёт коротко;
- `sendBeacon` или unload не являются гарантией доставки.

### 3.3 `operational`

Техническое состояние без actor identity:

- ingest retry/failure class;
- outbox age/overflow;
- route selected/direct/relay;
- schema reject/quarantine;
- TTL/archive lag;
- rows/bytes/RU budget.

Правила:

- не содержит actor key, event copy, URL, query или PII;
- не используется как поведенческий профиль;
- имеет отдельные retention и alert policies;
- failure observability не должна рекурсивно создавать telemetry storm.

## 4. Канонический envelope

Минимальный logical envelope:

```text
schema_version
batch_id
created_at
lane
source
event_name
occurred_at / first_observed_at / last_observed_at
observation_count
idempotency_key?          # обязательно для product_fact
session_id?               # pseudonymous, bounded
actor_key?                # HMAC/installation/account projection, не raw ID
surface?
entity { kind, id }?
release {
  release_sha
  page_revision
  content_revision
  feature_version
}?
dimensions {}             # allowlisted low-cardinality scalars
counters {}               # bounded sums
maxima {}                 # bounded maximum/checkpoint
```

Запрещены:

- email, phone, OTP, JWT, access/refresh token;
- IP, полный user agent, fingerprint;
- raw URL/href/query;
- Search query text;
- полный Hero Talk или event description;
- DOM selector, coordinates, raw scroll offset;
- произвольный JSON;
- значение с неизвестной cardinality;
- browser-supplied `test_actor=true` как доверенный признак.

## 5. Компоненты

### 5.1 `UnifiedStatisticsClient`

Единственная browser/service facade:

```ts
record(input): Promise<RecordResult>
flush(): Promise<number>
```

Обязанности:

- валидировать lane/source/consent;
- принимать только catalogued `event_name` и поля;
- редактировать/отклонять чувствительные ключи;
- агрегировать weak observations;
- создавать compact batch;
- передавать batch в bounded outbox;
- не знать физическую таблицу YDB/Supabase.

Foundation находится в `site/src/lib/unifiedStatisticsClient.ts`. Он уже
проверяет consent, sensitive keys, authoritative receipt, batching, maxima,
bounded payload и retry-safe outbox behavior. Это не означает, что emitters и
backend ingest уже мигрированы.

### 5.2 Feature adapters

Тонкие adapter-функции переводят domain state в catalog event:

```text
recordCardVisible(...)
recordDescriptionCheckpoint(...)
recordCtaStage(...)
recordHeroTalkState(...)
recordKeyboardState(...)
recordAuthoritativeActionReceipt(...)
```

Adapter не вызывает `fetch`, Supabase SDK, YDB SDK, `sendBeacon` или
`localStorage` напрямую.

### 5.3 Event catalog

`statistics-event-catalog.v1.yml` — source of truth для:

- lane/source;
- consent class;
- aggregation mode;
- allowed dimensions/counters/maxima;
- retention class;
- whether authoritative receipt is required;
- whether event participates in capability maturity.

Unknown event или field fail closed. Catalog change требует schema/version
review, cardinality review и migration note.

### 5.4 Session accumulator

In-memory/session state хранит только:

- deduplicated exposure keys;
- bounded counters;
- maxima/checkpoints;
- compact masks;
- first/last timestamps.

Baseline:

```text
max aggregate keys/session = 64
batch target                <= 3.8 KiB while current outbox payload cap is 4 KiB
weak batches/session        <= 3 normal target
weak pending TTL            <= 7 days
```

При capacity pressure:

1. сначала flush compact aggregate;
2. затем drop low-value weak observations;
3. никогда не вытеснять strong product facts;
4. записать один bounded operational counter, а не сообщение на каждый drop.

### 5.5 Bounded idempotent outbox

Используется существующий `site/src/lib/idempotentOutbox.ts` или его
совместимый successor:

- IndexedDB primary, bounded localStorage fallback;
- payload/count/bytes/attempt/TTL caps;
- stable record id;
- retryable/terminal classification;
- corruption/quota fail-safe;
- cleanup не касается Supabase Auth storage.

Strong и weak channels разделены. Weak analytics не может занять весь budget,
нужный для user-control action.

### 5.6 Resilient transport adapter

Используется существующий direct/relay transport contract:

- route probe до cost-bearing/selected-once mutation;
- last-known-good capability route;
- bounded deadline/idle watchdog;
- idempotent replay только для replay-safe operation;
- ambiguous selected-once response не отправляется вторым маршрутом;
- 429/definitive response не маскируется retry;
- telemetry failure не quarantines здоровый data/auth route.

Feature code не выбирает replay policy. Для analytics operation catalog
определяет:

```text
weak compact batch     = idempotent replay, disposable under pressure
strong receipt batch   = idempotent by receipt key, never duplicated
```

`sendBeacon`/`fetch(..., {keepalive:true})` разрешены только как
opportunistic final flush поверх уже сохранённого batch. Они не заменяют outbox:
Beacon не даёт delivery receipt и не обеспечивает offline/background delivery.

### 5.7 First-party ingest

Целевой endpoint принимает только `statistics-batch-v1`:

1. authenticate/derive trusted environment and test markers;
2. limit body, event count and request rate;
3. validate JSON schema and catalog version;
4. reject PII/sensitive keys again server-side;
5. derive trusted actor bucket/HMAC where required;
6. dedupe strong facts and batch IDs;
7. upsert compact actor/session/day facts;
8. append only bounded quarantine metadata;
9. return terminal acknowledgement with accepted/deduped/rejected counts.

Browser никогда не пишет прямо в YDB.

### 5.8 Storage projector

Ingest пишет или ставит в transactional outbox минимальные normalized facts.
Projector:

- upsert-ит actor/session/day rows;
- обновляет daily aggregates;
- сохраняет current event engagement projection;
- экспортирует verified partitions в Parquet;
- отмечает archive manifest/hash;
- только после archive verification продвигает delete watermark.

## 6. Физические хранилища

### Online YDB

Используются компактные row tables для recent actor/session/day facts и
column-oriented/aggregate layout для аналитических срезов, если измерение
подтверждает выгоду. Partition key имеет достаточную cardinality; монотонный
 timestamp не становится единственным первым ключом.

Raw browser clickstream в YDB запрещён.

### Supabase/Postgres

Хранит Auth и authoritative current product state своего домена. Допустимы
minimal receipt/current counters, когда они нужны транзакции. Не становится
вторым analytics warehouse.

Целевой инвариант:

```text
Supabase raw browser analytics rows = 0
```

### Object Storage

После compaction хранит partitioned Parquet:

```text
analytics/v1/domain=<domain>/grain=<daily|monthly>/year=YYYY/month=MM/day=DD/
  part-*.parquet
  manifest.json
  schema.json
```

Lifecycle переводит старые объекты в более дешёвый класс или удаляет по
утверждённому сроку. Archive не содержит actor-level raw sequence после
соответствующего cutoff.

## 7. Retention и migration

Применяется [storage-retention-architecture.md](storage-retention-architecture.md):

- hot detail: обычно 14–35 дней;
- warm actor/session detail: 90 дней, 180 дней только для обоснованных доменов;
- daily aggregates: 25 месяцев для двух годовых циклов;
- после 25 месяцев — monthly anonymous aggregates;
- архив перед удалением только там, где он реально нужен;
- deletion watermark продвигается после hash/schema/count verification.

TTL в YDB считается асинхронным physical cleanup. Queries обязаны фильтровать
логически истёкшие строки по cutoff, а мониторинг отдельно показывает
`expired_eligible` и `physically_deleted`.

## 8. Конкретные продуктовые измерения

### Карточки

Weak:

- `card_visible` — один aggregate на session/surface/event/family;
- `card_opened` — observation of navigation;
- maxima position bucket;
- counters large/compact.

Strong:

- accepted save/calendar/like/share/CTA receipt, связанный с source card.

Отчёт показывает mean, median, p75, p90/p95 и distribution. Среднее без
распределения запрещено.

### Глубина описания

Хранятся semantic checkpoints, а не raw scroll:

```text
hero -> key_facts -> summary -> description_25/50/75/100
-> practical_info -> transport -> related -> page_end
```

`description_100` называется reach proxy, а не доказанным прочтением.

### CTA

Weak funnel:

```text
eligible -> visible -> click -> dispatch_started
```

Strong terminal facts:

```text
accepted/success/failure/cancel/undo/external_navigation_started
```

Покупка или регистрация не объявляются завершёнными без provider callback.

### Hero Talk

Раздельные denominators для `home_hero` и `page_end`. Хранятся chain/step/target
IDs, но не текст и raw URL. Assisted correlation не называется causal uplift.

### Освоение сервиса

`capability_maturity_tier` вычисляется из bounded capability masks и strong
facts за rolling window. Он не оценивает человека и не используется для доступа,
призов или чувствительных решений. Actor-level tier удаляется максимум через
90 дней; долгосрочно остаётся только distribution.

## 9. Миграция существующих emitters

### Фаза 0 — foundation

- [x] единый client/types;
- [x] consent/sensitive-field/size/idempotency tests;
- [x] event catalog и batch schema;
- [ ] CI на repository branch — временно не выполнен из-за GitHub Actions billing block.

### Фаза 1 — инвентарь и adapters

Первый обязательный инвентарь:

| Контур | Текущее состояние | Целевое действие |
|---|---|---|
| PWA install/open | уже использует resilient client + bounded outbox, но bespoke payload/RPC | перевести на catalog adapter после явного решения consent/lane; сохранить endpoint compatibility на переходе |
| focus feedback/NPS | authoritative feature SOR | analytics получает только accepted receipt/cohort, не текст/скриншот |
| favorite/calendar/like/share | несколько feature paths | terminal metric только из accepted current-state receipt |
| event CTA | DOM/external navigation | unified stage adapter; provider success только с callback |
| card visibility/list depth | частично/локально | один session accumulator, no raw tick |
| description depth | нет единого контракта | semantic checkpoint adapter |
| Hero Talk/page-end | feature docs есть | versioned chain/step/target adapter |
| keyboard | отдельная privacy-minimal telemetry | перенести event names/catalog, сохранить no-content invariant |
| Search | operational/product facts смешиваются | query text = 0; только outcome/latency/result buckets |
| personalization | profile/state и evaluation | SOR отдельно; analytics — versioned quality/coverage/feedback facts |
| email/Push | delivery receipts/monitoring | unified delivery fact adapter, без address/body |
| medallions/artifacts/volunteers | feature-specific adoption | unified eligible/visible/engaged/completed facts |

### Фаза 2 — ingest shadow

- новый endpoint принимает batches, но не влияет на ranking/UX;
- old/new pipelines работают с trusted test actors;
- daily numerator/denominator parity;
- duplicate/PII/cardinality/row-budget gates;
- exact SHA evidence.

### Фаза 3 — dual-write comparison

- production sample с consent;
- old and new metric reconciliation;
- no user-facing dependency;
- bounded duration и rollback.

### Фаза 4 — cutover

- feature code больше не вызывает legacy analytics endpoint;
- old tables/RPC read-only;
- historical migration только в compact aggregate, не raw copy;
- dashboard читает canonical aggregates.

### Фаза 5 — cleanup

- delete old raw rows после verified export/decision;
- remove duplicate code/SDK clients;
- close legacy schemas;
- update release checklist и ownership map.

## 10. Release gates

- [ ] 100% product emitters инвентаризированы; unknown direct telemetry call = failure.
- [ ] Feature code не вызывает raw Supabase/YDB/Beacon analytics transport.
- [ ] Event catalog/schema version pinned in client, ingest and aggregates.
- [ ] No-consent creates zero weak writes.
- [ ] Preview, bot and trusted E2E traffic excluded from product metrics.
- [ ] Strong facts reproduce authoritative receipts exactly once.
- [ ] Weak events stay within session/batch/outbox/TTL budgets.
- [ ] Direct/relay failure matrix proves no loss/duplicate/retry storm.
- [ ] PII and cardinality scanner passes client and server artifacts.
- [ ] Hot/warm/archive/delete cycle is demonstrated with counts and hashes.
- [ ] YDB rows/bytes/RU and Object Storage bytes stay within versioned forecast.
- [ ] Supabase raw analytics rows remain zero.
- [ ] Dashboard numerator/denominator parity and release/version segmentation pass.
- [ ] One exact-SHA canary covers card, description, CTA, Hero Talk and capability tier.

## 11. Короткий handoff кодовому агенту

```text
Переведи всю статистику сервиса на Unified Statistics Runtime по
`docs/features/static-site-pages/analytics/unified-statistics-runtime-architecture.md`.
Сначала мигрируй PWA, card/depth, CTA, Hero Talk и keyboard emitters; затем
реализуй first-party ingest, YDB compact facts/aggregates, Parquet archive/TTL и
удали legacy direct telemetry calls. Сохрани существующий resilient direct/relay
transport и idempotent outbox. Дай exact-SHA tests, migration inventory и live
canary evidence; не включай production writes без green gates.
```

## 12. Primary references

- OpenTelemetry Logs Data Model: https://opentelemetry.io/docs/specs/otel/logs/data-model/
- OpenTelemetry Metrics Data Model: https://opentelemetry.io/docs/specs/otel/metrics/data-model/
- OpenTelemetry Protocol: https://opentelemetry.io/docs/specs/otlp/
- W3C Beacon: https://www.w3.org/TR/beacon/
- YDB TTL: https://ydb.tech/docs/en/concepts/ttl
- YDB partitioning: https://ydb.tech/docs/en/concepts/datamodel/table
- Yandex Object Storage lifecycle: https://yandex.cloud/en/docs/storage/concepts/lifecycles
- Yandex Query over Object Storage/Parquet: https://yandex.cloud/en/docs/query/concepts/file-formats
