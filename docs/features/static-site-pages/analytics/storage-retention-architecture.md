# Экологичное хранение, устаревание и архив продуктовой статистики

> **Статус:** принятый TO-BE-контракт хранения; runtime и миграции ещё не реализованы.  
> **Дата среза:** 2026-08-05.  
> **Родительский документ:** [`README.md`](README.md).  
> **Область:** browser/product analytics, feature adoption, event/card/page facts, strong-action receipts, email delivery analytics, агрегаты, YDB, Supabase и Object Storage.  
> **Не является:** юридическим заключением, разрешением собирать данные без требуемого основания или обещанием нулевой стоимости без измерения фактического billing-account usage.

## 1. Цель

Хранить достаточно данных, чтобы понимать продукт и сравнивать релизы, но не
превращать YDB или Supabase в бесконечный clickstream warehouse.

Система должна одновременно обеспечивать:

1. продуктовые ответы с корректными denominators;
2. короткое окно диагностики конкретной сессии или релиза;
3. среднесрочные cohort и feature-аналитики;
4. долгосрочную сезонную динамику только в компактных агрегатах;
5. предсказуемый верхний предел строк, байтов, RU и egress;
6. воспроизводимый архив до удаления из оперативной БД;
7. удаление actor-level данных, когда они больше не нужны продуктовой задаче.

Ключевой принцип:

```text
не хранить каждый клик
-> агрегировать в браузере
-> хранить bounded actor/session/day facts
-> сворачивать в daily/monthly aggregates
-> экспортировать редкий исторический срез в Object Storage
-> удалять из online DB по проверенному cutoff
```

## 2. Владение хранилищами

### 2.1 Fly SQLite

Остаётся system of record для:

- канонических событий и источников;
- publication/build lifecycle;
- Smart Update и operational state;
- event-specific current control-plane данных.

Browser analytics туда не записывается. Смешивание пользовательского
clickstream с canonical event DB увеличило бы volume, lock contention и риск
операционных инцидентов.

### 2.2 Supabase

Остаётся владельцем Auth/session и тех user-linked current states, которыми он
владеет по актуальному архитектурному контракту.

В Supabase допускаются:

- authoritative strong action/current state;
- send-critical email receipts;
- purpose-specific consent/suppression;
- небольшой operational window, если он нужен транзакционному действию.

Запрещены:

- raw page views;
- scroll/click firehose;
- копия всех YDB analytics facts;
- бессрочная история всех состояний карточек;
- полнотекстовые Search queries в analytics;
- произвольные browser JSON rows.

`Supabase raw analytics rows = 0` — целевой инвариант.

### 2.3 Изолированный YDB analytics namespace

Целевой online analytics store:

- компактные actor-day и session summaries;
- page/event/card/feature actor-day facts;
- strong-action analytics receipts без повторного владения product state;
- delivery facts и daily aggregates;
- data-quality и resource-budget facts.

Analytics namespace отделён от:

- PII/identity vault;
- current personalization state;
- Region Talk;
- comment-feedback sidecar;
- других YDB workload, насколько это возможно через namespace/table/IAM и
  отдельные budget labels.

YDB analytics не используется как профиль пользователя и не участвует в
синхронном ranking/CTA path.

### 2.4 Object Storage

Хранит:

- проверенные Parquet-архивы;
- manifest и schema snapshot;
- анонимные daily/monthly exports;
- generated private/noindex reports;
- редкие cold evidence bundles.

Object Storage не хранит:

- JWT/OTP/email;
- raw free-form feedback;
- полные URL с bearer/query secrets;
- полный user agent/IP;
- неограниченный JSONL browser firehose.

### 2.5 Yandex Query / DataLens

- Yandex Query читает partitioned Parquet/JSON в Object Storage для редких
  исторических запросов.
- DataLens читает в первую очередь компактные YDB aggregate tables; historical
  charts могут идти через Yandex Query.
- DataLens cache — cache результатов запросов, а не архив и не system of record.

## 3. Слои данных

### 3.1 In-browser accumulator

Не является долговременным хранилищем.

Хранит только текущую сессию:

- deduplicated exposures;
- максимальную глубину;
- counts по card type и capability;
- bounded pending batches;
- strong-action IDs до authoritative acknowledgement.

Target:

```text
weak analytics batches <= 3 per session
batch target            < 8 KiB
batch hard limit        < 16 KiB
pending weak outbox     <= 7 days
pending strong actions  feature-specific, bounded separately
```

### 3.2 Hot online facts

Нужны для оперативного анализа и разборов свежих релизов.

```text
0–35 days
```

Содержат actor/session/day granularity, но только bounded facts.

### 3.3 Warm online facts

Нужны для D30, cohort, сезонного сравнения внутри квартала и проверки
персонализации.

```text
36–180 days
```

В warm остаются не все raw details, а только таблицы, для которых доказана
продуктовая необходимость.

### 3.4 Cold archive + aggregates

```text
181 days – 25 months
```

- daily anonymous aggregates остаются queryable;
- actor-level и session-level данные в основном удалены;
- при необходимости проверенный Parquet находится в Object Storage.

### 3.5 Historical monthly

```text
> 25 months
```

Сохраняются только маленькие анонимные monthly aggregates и release/event
milestones. Срок до пяти лет допустим, если строки не содержат стабильного actor
ID и объём остаётся пренебрежимо малым. Бессрочное хранение требует отдельного
решения, а не возникает по умолчанию.

## 4. Физическая модель компактных фактов

### 4.1 `analytics_actor_day`

Одна строка на `actor_key × local_day`:

```text
actor_bucket
actor_key_hmac
local_day
first_session_at
last_session_at
session_count
page_family_mask
capability_mask
value_reached
intent_action_count
app_mode_mask
device_class_mask
auth_state_class
max_capability_maturity_tier
expires_at
```

Не содержит список всех страниц/событий.

### 4.2 `analytics_session_summary`

Одна строка на session:

```text
session_id_hmac
actor_bucket
actor_key_hmac
started_at
ended_at
duration_bucket
page_count
unique_event_count
large_cards_exposed
compact_cards_exposed
large_cards_opened
compact_cards_opened
max_large_card_position_bucket
max_compact_card_position_bucket
max_event_description_checkpoint
intent_action_mask
intent_action_count
feature_use_mask
first_value_card_position_bucket
first_value_latency_bucket
release_sha_short
surface_entry_class
expires_at
```

Не хранит ordered click sequence. Для специальных экспериментов sequence
разрешается только в отдельной маленькой таблице с коротким TTL.

### 4.3 `analytics_page_actor_day`

Одна строка на `actor × day × page_family × device × app_mode × revision`:

```text
view_count_capped
engaged_view_count_capped
max_read_checkpoint
cta_visible_mask
cta_click_mask
hero_talk_state_mask
page_end_state_mask
```

Повторные reloads capped, чтобы один actor не раздувал счётчики.

### 4.4 `analytics_event_actor_day`

Одна строка на `actor × day × event_id × surface_family`:

```text
card_exposure_mask
card_density_mask
max_card_position_bucket
event_detail_opened
max_description_checkpoint
intent_action_mask
share_state_mask
cta_state_mask
```

Это позволяет считать просмотры и конверсии конкретного события без записи
каждого visibility callback.

### 4.5 `analytics_feature_actor_day`

Одна строка на `actor × day × feature × placement × version`:

```text
eligible
visible
engaged
completed
value_reached_after
state_mask
max_checkpoint
```

Подходит для keyboard, Hero Talk, page-end Talk, medallions, artifacts,
volunteers, Search, `/dlya-menya/`, onboarding и collections.

### 4.6 `analytics_action_fact`

Только сильное или audit-worthy действие:

```text
action_id
actor_key_hmac
occurred_at
action_kind
target_kind
target_id
surface
stage
result_class
release_sha_short
expires_at
```

Строка появляется из authoritative command/receipt, а не из оптимистичного
DOM click. Для внешней ссылки terminal state обычно заканчивается на
`external_navigation_started`, если нет provider callback.

### 4.7 `analytics_delivery_fact`

Для писем/Push/feedback transport:

```text
delivery_id
purpose
provider_class
stage
occurred_at
result_class
route_class
latency_bucket
expires_at
```

PII, subject/body и provider payload не входят.

### 4.8 Агрегаты

```text
analytics_daily_metric
analytics_monthly_metric
analytics_data_quality_day
analytics_resource_budget_day
```

Ключи только из allowlisted low-cardinality dimensions. Любая новая dimension
должна пройти cardinality review.

## 5. Primary key и partition safety

Рекомендуемая форма ключа для actor-oriented таблиц:

```text
PRIMARY KEY (actor_bucket, actor_key_hmac, local_day, dimension...)
```

`actor_bucket` — стабильный небольшой hash bucket, который распределяет writes.
Для daily aggregate:

```text
PRIMARY KEY (metric_day, metric_name, dimension_hash)
```

Запрещены:

- монотонный timestamp как единственный первый key component для high-write
  таблицы;
- raw URL/query/text в key;
- nullable key components;
- unbounded JSON properties;
- отдельная строка на каждый scroll/IntersectionObserver tick.

Каждая таблица получает:

- documented maximum rows per actor/day;
- target average row bytes;
- hard payload cap;
- TTL column `expires_at`;
- expected monthly rows/bytes/RU for launch, 1k, 10k и 100k active actors.

## 6. Политика устаревания по доменам

| Домен | Hot detail | Warm detail | Online aggregate | Cold/archive |
|---|---:|---:|---:|---:|
| transport/debug/quarantine | 14 дней | нет | 90 дней daily errors | incident-only evidence |
| page/event actor-day | 35 дней | до 90 дней при открытом анализе | 25 месяцев daily | Parquet optional |
| session summaries | 35 дней | 90 дней | 25 месяцев daily | monthly after 25 months |
| card type/depth | 35 дней | 90 дней | 25 месяцев daily | monthly historical |
| description read depth | 35 дней | 90 дней | 25 месяцев daily | monthly historical |
| keyboard/Hero/feature facts | 35 дней | 90 дней | 25 месяцев daily | experiment snapshot |
| Search audit | 14 дней diagnostic | 30 дней bounded facts | 25 месяцев daily | no raw query text |
| personalization evaluation | 90 дней | 180 дней | 25 месяцев model/day | frozen benchmark artifacts |
| strong product action facts | 90 дней | до 180 дней | 25 месяцев daily | purpose/legal dependent |
| event exact facts | до event end + 90 дней | при споре/incident | event daily/monthly | event aggregate only |
| email/Push delivery facts | 30 дней operational | до 180 дней deliverability | 25 месяцев daily | provider/audit policy |
| focus page score/NPS | feature SOR, не analytics TTL | по research policy | anonymized cohorts | separate feedback archive |
| experiments | experiment + 30 дней | максимум 180 дней | immutable result summary | design/evidence repository |
| capability maturity actor-level | 35 дней | максимум 90 дней | 25 месяцев tier distribution | no actor-level archive |

### 6.1 Почему 35 дней

Покрывает:

- D1/D7/D30;
- полный календарный месяц плюс задержку обработки;
- сравнение релиза до/после;
- диагностику недавнего пользовательского пути.

### 6.2 Почему 90 дней

Покрывает квартальный сезон, stabilisation и проверку, что feature adoption не
является однодневным эффектом. Более долгий actor/session detail обычно не нужен.

### 6.3 Почему 180 дней

Используется только для сильных действий, model evaluation и deliverability,
где полугодовой горизонт реально помогает. Это exception, а не default.

### 6.4 Почему 25 месяцев для daily aggregates

Позволяет сравнить:

- текущий месяц с прошлым;
- сезон год-к-году;
- два полных годовых цикла с небольшим запасом.

После этого daily granularity редко оправдана; данные сворачиваются в monthly.

## 7. Событийное старение

Для event-specific facts используется не только `occurred_at`, но и lifecycle
самого события.

```text
exact event actor facts expire at:
max(occurred_at + domain_ttl, event_end + 90 days)
```

Исключения:

- open incident;
- active experiment;
- legal/audit hold;
- explicit editorial post-analysis.

После expiry остаются:

- daily total views;
- engaged views;
- intent actions;
- CTA conversion by type;
- card density/surface breakdown;
- release/content revision class.

Actor-event связь удаляется.

## 8. Двухфазная архивация

Удаление из YDB разрешено только после проверенного архива, если политика домена
вообще требует архив. Для disposable data архив не обязателен.

```text
PLAN
-> EXPORT
-> MANIFEST
-> READBACK VERIFY
-> MARK ARCHIVED
-> TTL / DELETE APPLY
-> POST-DELETE VERIFY
```

### 8.1 PLAN

План содержит:

```text
archive_run_id
source table
cutoff
expected row count
min/max occurred_at
schema version
partition list
reason
retention policy version
repo SHA
```

### 8.2 EXPORT

Рекомендуемый путь:

```text
analytics/v1/
  domain=<domain>/
  year=YYYY/
  month=MM/
  part-0000.parquet
  manifest.json
```

Parquet предпочтителен для больших табличных агрегатов. JSON допустим для
маленьких manifest/control records.

### 8.3 MANIFEST

Обязательные поля:

```text
schema_version
archive_run_id
source_table
source_query_hash
cutoff
row_count
min_occurred_at
max_occurred_at
uncompressed_bytes
object_bytes
object_sha256[]
schema_sha256
created_at
repo_sha
```

### 8.4 READBACK VERIFY

Проверяются:

- все object hashes;
- row count;
- min/max timestamps;
- nullability и enum domain;
- sampled aggregate parity;
- отсутствие запрещённых полей;
- запрос файла через Yandex Query или bounded DuckDB verifier.

### 8.5 DELETE APPLY

YDB TTL не считается доказательством архива и не гарантирует удаление ровно в
момент expiry. Все online queries обязаны фильтровать logically expired rows по
`expires_at`, пока background removal ещё не завершён.

### 8.6 POST-DELETE VERIFY

- rows before cutoff logically absent;
- table size/row estimate снизились ожидаемо;
- daily aggregates сохранены;
- manifest доступен;
- rerun является no-op.

## 9. Object Storage lifecycle

Базовая политика:

```text
0–180 days      STANDARD
181–730 days    COLD for archive partitions that are rarely read
>730 days       monthly aggregate only or explicit delete
```

`ICE` не используется автоматически для маленьких часто пересобираемых файлов:
у него есть минимальный срок хранения и цена раннего удаления. Переход между
storage classes также является тарифицируемой операцией.

Lifecycle rules:

- применяются по префиксу `analytics/v1/domain=...`;
- не удаляют current manifest до удаления всех data parts;
- очищают incomplete multipart uploads;
- versioning/non-current policy задаётся отдельно;
- учитывают, что lifecycle выполняется асинхронно и не является точным scheduler.

## 10. Предсказуемые ограничения роста

### 10.1 Row budgets

Launch target на одну обычную сессию:

```text
session_summary              1 row
actor_day                    <= 1 upsert/day
page_actor_day               <= 12 rows/day
feature_actor_day            <= 12 rows/day
unique event_actor_day       <= 30 rows/day
strong action facts          <= actual accepted commands
weak raw event rows          0
```

Hard guard:

```text
analytics fact rows <= 64 per actor/day before strong action facts
```

При превышении:

- новые low-value dimensions схлопываются;
- event details сохраняют только top/first/last/value-reached subset;
- disposable facts отбрасываются;
- strong product action не блокируется.

### 10.2 Resource bands

| Band | Условие | Действие |
|---|---|---|
| Green | <50% внутреннего monthly budget | обычный режим |
| Yellow | 50–70% | уменьшить sampling/debug, проверить cardinality |
| Orange | 70–85% | отключить optional cohorts и cold backfills |
| Red | 85–95% | принимать только strong receipts + core summaries |
| Critical | >=95% или прогноз превышения free tier | analytics kill switch; продукт продолжает работать |

Internal targets ниже provider free tier:

```text
YDB analytics storage        < 250 MB
YDB analytics RU             < 250 000 / month
API Gateway analytics calls  < 50 000 / month
weak batches                 <= 3 / session
Supabase raw analytics       0 rows
```

Эти targets учитывают, что provider free tier общий для billing account и уже
используется другими контурами.

### 10.3 Cardinality registry

Каждая dimension получает:

```text
name
type
enum or bounded hash
max expected values/month
owner
retention
dashboard need
```

Запрещены как dimensions:

- full URL;
- free-form Search text;
- title/description;
- arbitrary error message;
- UA string;
- email/account name;
- campaign text;
- unbounded CSS selector.

## 11. Aggregation schedule

### Hourly

Только технические health/resource counters, если нужны alerts.

### Daily

- actor/session/page/event/feature metrics;
- CTA and delivery funnels;
- data-quality checks;
- budget forecast;
- previous-day immutable aggregate revision.

### Monthly

- monthly aggregate from finalized daily rows;
- archive plan for expired hot/warm facts;
- Parquet export/readback;
- storage and RU forecast at launch/1k/10k/100k actors;
- orphan partitions and manifest audit.

### Quarterly

- review retention usefulness;
- delete unused dimensions;
- verify dashboards still answer real decisions;
- compare archive cost against value;
- reapprove exceptions above 90/180 days.

## 12. Data quality gates

Daily job fails/warns on:

- missing release/content/page revisions;
- duplicate session/action facts;
- actor/day row budget breach;
- unknown enum or high-cardinality dimension;
- test/preview pollution;
- aggregate denominator < numerator;
- archive manifest mismatch;
- logically expired rows included in dashboards;
- YDB size/RU forecast over band;
- Supabase analytics relation growth above zero/new allowlist.

## 13. Privacy and consent

Retention is maximum, not entitlement to collect.

- Analytics consent/legal basis is separate from personalization activation,
  Auth, feedback, email and marketing.
- Before permitted analytics, weak events are dropped; product remains usable.
- Strong product command may exist for functional necessity, but its analytics
  projection is minimized and purpose-limited.
- Actor IDs are keyed/HMAC pseudonyms with key version; no plain email or
  reversible browser identifier enters analytics.
- Export removes actor IDs unless a documented cold analysis absolutely needs a
  short-lived pseudonymous cohort.
- Delete/reset/account-purge emits purge work for eligible actor-level facts;
  irreversibly anonymous aggregates may remain under policy.

## 14. Required implementation artifacts

Before production ingest:

1. physical schemas and YQL migrations;
2. row/byte/RU capacity model;
3. browser accumulator and payload schemas;
4. same-origin ingest + direct/bridge transport tests;
5. dimension registry;
6. TTL definitions;
7. daily aggregate job;
8. archive plan/export/verify/apply CLI;
9. Object Storage lifecycle configuration;
10. budget dashboard and kill switch;
11. redaction/PII regression tests;
12. synthetic launch/1k/10k/100k capacity tests;
13. one terminal cold-archive rehearsal with exact manifest and no production
    deletion.

## 15. Официальные технические основания

- YDB TTL and eviction: <https://ydb.tech/docs/en/concepts/ttl>
- YDB primary key guidance: <https://ydb.tech/docs/en/dev/primary-key/row-oriented>
- Yandex Serverless free tier: <https://yandex.cloud/en/docs/billing/concepts/serverless-free-tier>
- Object Storage lifecycle: <https://yandex.cloud/en/docs/storage/concepts/lifecycles>
- Object Storage classes: <https://yandex.cloud/en/docs/storage/concepts/storage-class>
- Yandex Query overview: <https://yandex.cloud/en/docs/query/concepts/>
- Yandex Query partitioning: <https://yandex.cloud/en/docs/query/concepts/partitioning>
- DataLens pricing: <https://yandex.cloud/en/docs/datalens/pricing>
- Supabase database size: <https://supabase.com/docs/guides/platform/database-size>
