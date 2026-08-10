# Product Atlas: сквозная продуктовая доска в Penpot

> **Статус:** скорректированное архитектурное решение для практического пилота.  
> **Дата:** 7 августа 2026 года.  
> **Канонический владелец продуктового смысла:** `events-bot-new`.  
> **Не является:** автоматической системой продуктовой аналитики, новым backlog-сервисом или live-dashboard.

## 1. Решение

Для «Полюбить Калининград Анонсы» создаётся отдельная **Product Atlas board** в Penpot и отдельный Product Atlas plugin.

Доска показывает одну связанную модель:

```text
user need
→ Job / Job Story
→ user outcome
→ journey
→ capability
→ User Stories / operator jobs / technical enablers
→ acceptance scenarios
→ implementation / release evidence
→ production health / incidents
→ наблюдения и результаты отдельных анализов
→ owner outcome
→ decision
```

Она не заменяет release-checklist, статистические БД, дизайн-систему или feature-документы. Она связывает их через стабильные IDs и ссылки.

## 2. Где находятся источники истины

### 2.1 Product model и решения — `events-bot-new`

Здесь хранятся:

- user needs, Jobs, Job Stories и user outcomes;
- owner goals, owner outcomes и operator jobs;
- будущие partner needs, Jobs и outcomes после отдельного исследования;
- journeys, capabilities, User Stories и enablers;
- acceptance scenarios, release/checklist links и incidents;
- принятые decisions;
- результаты отдельных продуктовых анализов, выполненных человеком или ChatGPT.

Penpot является визуальной проекцией и review-средой. Комментарий или вручную нарисованная карточка в Penpot не становится продуктовым требованием автоматически.

### 2.2 Статистика — проектируемый DB/runtime-контур

Статистика не хранится в `common-analytics` и не должна переноситься в GitHub как основной data store.

Канонический общий контракт в `main`:

```text
docs/features/static-site-pages/analytics/README.md
```

На дату этого решения детальные runtime/migration contracts находятся в открытом PR `#337`:

```text
docs/features/static-site-pages/analytics/unified-statistics-runtime-architecture.md
docs/features/static-site-pages/analytics/statistics-migration-inventory.v1.yml
docs/features/static-site-pages/analytics/statistics-event-catalog.v1.yml
```

Целевой путь статистики:

```text
browser / authoritative receipt
→ unified client and catalog
→ first-party ingest
→ compact YDB facts and aggregates
→ verified Parquet archive
→ TTL / verified deletion
```

Product Atlas не читает production БД автоматически на первом этапе.

### 2.3 Продуктовая аналитика — отдельные запросы и analysis records

Пока не вводится автоматическая сквозная продуктовая аналитика.

Рабочий процесс:

```text
вопрос владельца
→ запрос к данным / документам / production evidence
→ анализ в ChatGPT
→ проверяемый Markdown analysis record
→ связанные findings и decisions
→ обновление Product Atlas
```

Analysis record хранит не сырые данные, а:

- вопрос и границы анализа;
- дату и data cutoff;
- использованные источники, query/export IDs и release identity;
- метод;
- выводы;
- неопределённость и ограничения;
- связанные Jobs, outcomes, capabilities и scenarios;
- варианты решений и принятое решение.

Каноническое место: [`analysis/README.md`](analysis/README.md).

#### 2.3.1 Action-map projection

Канонический feature contract — [First-party карта действий](../features/static-site-pages/first-party-action-map.md).
Она не создаёт отдельный analytics view или новую страницу Atlas. Plugin
принимает только immutable reviewed
`ProductAnalyticsEvidencePackage`, связанный с конкретным versioned analysis
record из [`analysis/`](analysis/README.md). Package сохраняет hashes/receipts
campaign manifest, schema, aggregate/export, release archive и artifacts;
изменение evidence создаёт новую immutable revision, а не переписывает прежнюю.

Используются существующие страницы:

- **50 — UI and design evidence:** reviewed page/component maps, scope,
  denominator/coverage, release/page/layout/model/component identities,
  limitations и ссылка на package/analysis record;
- **40 — Findings, incidents and decisions:** только accepted finding,
  competing explanations, options, explicit owner decision и follow-up
  measurement.

Страница `45 — Product analytics evidence` **не создаётся**: она дублировала бы
analysis record и pages 40/50. Product Atlas сохраняет deep links на
соответствующие Resource Graph evidence IDs, но evidence само по себе не меняет
Component Contract или design-system catalog.

Ingest запускается только явной командой **`Обновить Product Atlas`**. Plugin не
имеет live connection к production DB/YDB, не читает raw metrics или raw
action-map stream, не делает background refresh и не интерпретирует hotspot.
Hotspot/overlay не создаёт finding, `ProblemBubble`, UI gap, design change или
profile signal автоматически: обязательна цепочка
`MeasurementQuestion → evidence → finding → decision → follow-up` в concrete
analysis record.

### 2.4 `common-analytics`

`common-analytics` не является источником статистики, Job health или Product Atlas evidence.

Он может хранить исследовательские копии и свои существующие on-demand catalog artifacts, но Product Atlas не зависит от него.

### 2.5 Дизайн-система

Канонический runtime UI остаётся в `events-bot-new`. `lovekgd-design-system` владеет Penpot delivery/review tooling и визуальными contracts.

Product Atlas связывается с:

- design-system component / pattern / archetype IDs;
- actual/baseline/diff evidence;
- coverage и fragmentation;
- candidate visual decisions.

Неполный component inventory не блокирует доску. Он становится видимым gap:

```text
not_modeled
missing_component
partial_component_coverage
fragmented_implementation
missing_visual_evidence
```

## 3. Интересанты

Product Atlas поддерживает три разные lane.

### Пользователь

```text
need → Job → journey → user outcome
```

### Владелец / оператор

```text
owner goal → owner outcome → operator job → decision
```

### Партнёр

```text
partner need → partner Job → partner outcome
```

Partner lane заранее не заполняется выдуманными Jobs. До исследования используются `not_modeled` и `unknown`.

Общие capabilities показывают, где интересы:

- совпадают;
- усиливают друг друга;
- конфликтуют;
- ограничены guardrails.

## 4. Пять согласованных views

### 4.1 Product Outcome Spine

Показывает:

```text
need → Job → user outcome → owner outcome
```

Рядом располагаются capabilities, guardrails, analysis findings и decisions.

### 4.2 Job & Journey Map

Показывает альтернативные journeys, шаги, recovery paths и затронутые scenarios.

### 4.3 Capability Delivery Map

Показывает связь:

```text
capability
→ stories / operator jobs / enablers
→ acceptance
→ implementation
→ release
```

### 4.4 Coverage & Readiness Matrix

Показывает независимые признаки по контекстам:

```text
implemented
tested
released
live_verified
observed_with_sufficient_data
```

Контексты могут включать device/app mode, auth, network, accessibility, account/data state и recovery.

### 4.5 Evidence, Findings & Decisions

Показывает:

- release/test/incident evidence;
- вручную добавленные analysis findings;
- metric snapshots только когда они получены конкретным проверяемым анализом;
- owner decisions;
- открытые вопросы.

Для action-map evidence это означает split без дублирования: reviewed maps и
provenance находятся на page 50, а принятый finding/decision — на page 40. До
review или при `insufficient-data` overlay остаётся evidence и не становится
problem/UI gap.

На первом этапе это не auto-refresh dashboard.

## 5. Многоосевая готовность

Один `done` запрещён.

Capability может одновременно иметь:

```yaml
definition: decided
delivery: implemented
verification: candidate_pass
deployment: production
runtime_health: broken
runtime_context: mobile + weak_network
analysis_state: insufficient_data
user_outcome: not_confirmed
owner_outcome: unknown
```

Прошлое implementation/release evidence не стирается текущим incident.

`unknown` не означает success.

## 6. Product Problem Radar

На странице `00 — Executive / Problem Radar` выводятся до семи главных проблем.

На первом этапе problems формируются из известных источников:

1. release/checklist blocker;
2. acceptance gap;
3. production incident;
4. conflict или stale requirement;
5. missing component / UI coverage / design drift;
6. accepted analysis finding;
7. owner decision required.

DB-метрика может стать источником проблемы только после отдельного анализа и сохранения analysis record.

Problem record:

```yaml
id:
problem_type:
severity: S | M | L
statement:
affected_job_ids: []
affected_capability_ids: []
context:
source_refs: []
analysis_record_ids: []
incident_id:
owner:
decision_due_at:
```

Размер bubble дискретный `S/M/L`. Это не непрозрачный автоматический score.

Клик ведёт:

```text
problem
→ affected Job
→ context / journey step
→ capability
→ source evidence / analysis / incident
→ options and decision
```

## 7. Отдельный Penpot-файл и отдельный plugin

Product Atlas размещается в отдельном Penpot-файле.

Страницы:

```text
00 — Executive / Problem Radar
10 — Stakeholders, Jobs and outcomes
20 — Journeys and capabilities
30 — Delivery, coverage and readiness
40 — Findings, incidents and decisions
50 — UI and design evidence
80 — Candidate decisions
89 — Decision archive
99 — Technical diagnostics
```

Allowlist намеренно не содержит page 45. Action-map evidence использует pages
50 и 40 по контракту §2.3.1.

Для него создаётся отдельный plugin и отдельный manifest.

### Почему plugin отдельный

Design-system Resource Graph и Product Atlas имеют:

- разные catalogs;
- разные managed namespaces;
- разные pages;
- разные update cadence;
- разные сущности и feedback semantics.

Единый пользовательский plugin создавал бы риск запустить неправильный import в неправильном файле.

Допускается переиспользовать внутренний renderer core, но не manifest, catalog kind или managed namespace.

### Обязательная защита от неправильного файла

Product Atlas plugin:

- принимает только `catalog_kind=product-atlas`;
- пишет namespace `lovekgd.productatlas.*`;
- требует file marker `file_kind=product-atlas`;
- отказывается работать при обнаружении Resource Graph namespace или design-system marker;
- создаёт и изменяет только allowlisted Product Atlas pages;
- никогда не импортирует design-system resource catalog.

Design-system plugin симметрично должен отказываться работать при Product Atlas marker. Это отдельный acceptance gate перед реальным использованием двух plugins.

## 8. Визуальная связь с дизайн-системой

Product Atlas использует те же semantic foundations:

- typography;
- spacing;
- semantic colors;
- shape/elevation;
- focus и keyboard rules;
- reduced motion;
- accessible status grammar.

Но internal visualization components имеют отдельный namespace:

```text
Visualization/ProductModel/*
Internal/ProductAtlas/*
```

Минимальный набор:

- `ProductEntityCard`;
- `StakeholderLane`;
- `JobNode`;
- `OutcomeNode`;
- `CapabilityNode`;
- `StatusFacetStrip`;
- `ProblemBubble`;
- `CoverageCell`;
- `AnalysisFindingCard`;
- `IncidentMarker`;
- `DecisionCallout`;
- `EvidenceLink`;
- `Legend`.

Если какого-либо компонента ещё нет, board показывает gap и temporary primitive, а не скрывает неполноту системы.

## 9. Комплексная обратная связь

На Product Atlas комментарии ставятся к разным управляемым сущностям: Jobs, outcomes, journeys, capabilities, coverage cells, problems, findings и UI evidence.

Plugin собирает **один системный prompt** из всех выбранных или всех незакрытых комментариев.

Для каждого комментария сохраняются:

- Penpot thread ID;
- page и managed element ID;
- entity type и stable product ID;
- stakeholder lane;
- связанные Job, journey, capability и scenario;
- current statuses;
- source/evidence refs;
- Product Atlas catalog revision;
- точный текст комментария.

Prompt требует от ChatGPT:

1. рассмотреть комментарии как связанную систему, а не отдельные UI-правки;
2. объединить дубли и выявить сквозные темы;
3. отделить product problem от предложенного решения;
4. указать затронутые user/owner/partner outcomes;
5. проверить противоречия между комментариями;
6. предложить варианты решения;
7. разложить последствия по слоям:
   - product intent;
   - UX/journey;
   - UI/design system;
   - implementation/enablers;
   - acceptance/testing;
   - statistics/measurement;
   - documentation;
8. отдельно сформулировать owner decisions;
9. не менять production и не закрывать комментарии автоматически.

Результат review после проверки сохраняется как analysis/decision record в `events-bot-new` и только затем меняет каноническую модель.

## 10. Update cycle

```text
product model / evidence / analysis record changes
→ Product Atlas catalog generation
→ plugin preflight and guarded update
→ comments across the board
→ one systemic ChatGPT prompt
→ reviewed analysis and decision record
→ candidate product/design/implementation changes
→ acceptance and release evidence
→ next Product Atlas snapshot
```

Каждый переход `analysis record changes → Product Atlas catalog generation`
является on-demand операцией по явной команде **`Обновить Product Atlas`**.
Отсутствуют live DB connection, raw-metric polling и background refresh; новый
snapshot обязан сохранить package hash и Resource Graph deep links.

## 11. Пилот

Первая итерация ограничена:

- одним Job: найти подходящее событие и получить достаточно информации для решения;
- двумя journeys: каталог и Search;
- 5–8 capabilities;
- четырьмя context scenarios;
- одним реальным incident или confirmed gap;
- одним вручную подготовленным analysis record;
- одним Product Problem Radar;
- одним комплексным comment-to-prompt cycle.

Пилот не требует автоматического доступа к production DB.

Критерий успеха: владелец за 10–15 минут без устных пояснений находит главный Job, top problem, affected context, evidence, связанные комментарии и требуемое решение.

## 12. Отложенные, но не забытые работы

После пилота:

1. восстановить полный реестр Jobs и stories;
2. связать его с release-checklist, acceptance и incidents;
3. дополнить unified statistics contracts недостающими measurement questions;
4. по запросу выполнять продуктовые анализы над DB/exports и сохранять analysis records;
5. расширить board на фокус-группу и партнёрский контур;
6. только после накопления практики решать, нужна ли автоматическая product-analytics projection.
