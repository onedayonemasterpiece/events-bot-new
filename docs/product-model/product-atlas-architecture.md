# Product Atlas: сквозная визуализация продукта, готовности и фактической ценности

> **Статус:** рекомендуемая архитектура для owner review.  
> **Дата:** 7 августа 2026 года.  
> **Канонический владелец смысла:** `events-bot-new`.  
> **Связанные документы:** [методология продуктовой модели](README.md), [исследования визуализации](../research/product-visualization/README.md), [аналитика](../features/static-site-pages/analytics/README.md), [дизайн-система](../features/static-site-pages/design-system/README.md).

## 1. Решение

Для «Полюбить Калининград Анонсы» рекомендуется гибридный **Product Atlas**:

```text
одна машиночитаемая продуктовая модель
  → живая web-проекция Product Console
  → пространственная Penpot-проекция Product Atlas
```

Модель хранит устойчивый продуктовый backbone:

```text
user need
→ Job
→ user outcome
→ journey
→ capability
```

и связывает его с изменяемыми слоями:

```text
capability
→ User Story / operator job / technical enabler
→ acceptance scenario
→ implementation / release evidence
→ runtime observation / incident
→ metric / SLI / SLO
→ owner outcome
→ decision
```

Penpot и web не являются конкурирующими источниками. Они показывают разные проекции одной модели и одного evidence snapshot.

## 2. Почему выбран гибрид

### Penpot-only не подходит

Penpot хорошо поддерживает пространственный обзор, дизайн-контекст, нативные комментарии и совместный review. Но большой борд не должен вручную имитировать live-dashboard, хранить частые metric updates или становиться вторым backlog.

### Web-only недостаточен

Web-проекция лучше подходит для фильтров, актуальных метрик, таблиц, accessibility, mobile review и PDF. Но она хуже передаёт целостную пространственную картину, связи с визуальными компонентами и коллективный review на большом борде.

### Гибрид использует сильные стороны обоих

- **Product Console:** актуальные данные, фильтры, coverage matrices, trends, decision queue;
- **Penpot Product Atlas:** стабильная пространственная карта, дизайн-контекст, owner review, комментарии и candidate alternatives;
- **Git:** канонические определения, IDs, связи, decisions и evidence references.

## 3. Владение по репозиториям

### `events-bot-new`

Владеет:

- user needs, Jobs и Job Stories;
- user outcomes;
- owner goals, owner outcomes и operator jobs;
- будущими partner needs, Jobs и outcomes после их исследования;
- journeys, capabilities, stories и enablers;
- acceptance scenarios, release checklist и incidents;
- decisions и traceability.

Raw telemetry не получает изменчивые `story_id` как основную семантику.

### `common-analytics`

Владеет вычисляемой evidence-проекцией:

- job attempts и terminal states;
- metric snapshots;
- SLI/SLO и error-budget state;
- context coverage и data-quality state;
- freshness, sample size и confidence;
- агрегированными problem signals для Product Radar.

Он не переопределяет Jobs, outcomes или product intent.

### `lovekgd-design-system`

Владеет:

- foundations и semantic design tokens;
- visual status grammar;
- внутренними Product Atlas components;
- Penpot catalog/renderer/plugin contracts;
- visual evidence и comment-to-prompt transport.

Он не становится вторым реестром продукта.

## 4. Интересанты

Product Atlas сразу поддерживает три отдельные lane:

1. **Пользователь:** needs, Jobs, journeys и user outcomes.
2. **Владелец / оператор:** owner outcomes, operator jobs, guardrails и decisions.
3. **Партнёр:** future-ready lane для partner Jobs и outcomes.

Партнёрская lane не заполняется предположениями. До исследования она явно имеет состояние `not_modeled` или `unknown`.

Общие capabilities связывают несколько lane и показывают:

- взаимное усиление интересов;
- конфликты целей;
- guardrails;
- незакрытые stakeholder gaps.

## 5. Основные представления

Product Atlas использует пять согласованных views.

### 5.1 Product Outcome Spine

Показывает:

```text
need → Job → user outcome → owner outcome
```

Рядом видны capabilities, guardrails, metric contracts и confidence.

### 5.2 Job & Journey Map

Показывает альтернативные journeys, шаги, recovery paths и текущую работоспособность в выбранном контексте.

### 5.3 Capability Delivery Map

Показывает capabilities, User Stories, operator jobs, enablers, acceptance, implementation и release milestones.

### 5.4 Coverage & Operational Health Matrix

Показывает независимое состояние по контекстам:

```text
device / app mode
× authentication
× network
× accessibility
× account/data state
× recovery path
```

`unknown` не считается PASS.

### 5.5 Evidence, Metrics & Decisions

Показывает metric definitions, current values, target, sample, freshness, SLI/SLO, incidents и owner decisions.

## 6. Главный экран и Product Problem Radar

Верхняя полоса каждого overview содержит **Problem Radar**. Это не вручную расставленные стикеры, а вычисляемые product-problem records.

Источники проблем:

1. critical runtime `broken` или `degraded`;
2. P0/P1 release blocker;
3. critical `unknown`, `insufficient_data` или `stale_evidence`;
4. user outcome ниже принятого порога;
5. owner/partner `decision_required`;
6. design-system fragmentation или drift.

Минимальные поля проблемы:

```yaml
id:
problem_type:
severity: S | M | L
affected_job_ids: []
affected_capability_ids: []
context:
first_seen_at:
last_seen_at:
evidence_refs: []
incident_id:
owner:
decision_due_at:
```

Размер пузыря дискретный `S/M/L`, а не псевдоточная площадь. На первом экране показываются не более семи главных проблем; остальные сворачиваются в счётчик.

Типы визуально различаются текстом, формой, pattern и цветом:

- product gap;
- coverage gap;
- runtime incident;
- evidence gap;
- decision gap;
- design drift.

Клик ведёт по цепочке:

```text
problem
→ affected Job
→ context scenario
→ capability
→ evidence / incident
→ варианты решения
```

## 7. Многоосевая готовность

Единый `done` запрещён. Capability может одновременно иметь:

```yaml
delivery:
  implemented: true
  verified: true
  released: true
runtime:
  status: broken
  context: mobile + weak_network
  incident_id: INC-...
adoption:
  status: observed
user_outcome:
  status: not_confirmed
owner_outcome:
  status: insufficient_data
```

Минимальные независимые фасеты:

- definition / decision / design;
- implementation;
- verification;
- release / exposure;
- runtime health;
- adoption;
- task completion;
- user outcome;
- owner outcome;
- evidence freshness/confidence;
- causal confidence.

## 8. Penpot boundary

Product Atlas создаётся в **отдельном Penpot-файле**, а не внутри Resource Graph дизайн-системы.

Причины:

- другая частота обновления;
- другой набор сущностей и владельцев;
- более высокий объём dynamic evidence;
- отдельный review lifecycle;
- необходимость сохранять design-system library чистой.

Product Atlas использует общие Penpot libraries и foundations LoveKGD, но имеет собственные named pages:

```text
00 — Executive / Problem Radar
10 — Stakeholders, Jobs and outcomes
20 — Journeys and capabilities
30 — Delivery, coverage and readiness
40 — Metrics, incidents and decisions
50 — UI and design evidence
80 — Candidate decisions
89 — Decision archive
99 — Technical diagnostics
```

## 9. Интеграция с дизайн-системой

Создаётся внутреннее расширение с отдельным namespace, например:

```text
Visualization/ProductModel/*
Internal/ProductAtlas/*
```

Переиспользуются:

- `--ke-*` color, typography, spacing, shape, elevation и interaction tokens;
- accessibility rules;
- status semantics;
- focus, keyboard и reduced-motion behavior.

Добавляются семантические компоненты:

- `ProductEntityCard`;
- `StakeholderLane`;
- `JobNode`;
- `OutcomeNode`;
- `CapabilityNode`;
- `StatusFacetStrip`;
- `ProblemBubble`;
- `CoverageCell`;
- `MetricEvidenceCard`;
- `IncidentMarker`;
- `DecisionCallout`;
- `EvidenceLink`;
- `Legend` и `FilterContext`.

Внутренние atlas-компоненты не становятся публичными UI primitives сайта.

## 10. Расширение Penpot plugin

Новый режим не переписывает существующий Resource Graph renderer с нуля. Он переиспользует доказанные механизмы:

- one catalog / exact identity;
- schema and hash validation;
- managed plugin metadata;
- idempotent whole-system reconciliation;
- checkpoints, resume и fail-closed semantics;
- preservation of foreign objects and comments;
- native comments → deterministic prompt.

Добавляются:

- Product Atlas catalog schema;
- stable placement rules for five views;
- in-place updates of metric/status child shapes;
- Product Radar derivation;
- cross-links to Product Console, incidents, release evidence и design-system archetypes.

Частые metric updates изменяют managed child values **in place**, чтобы не терять spatial position и Penpot comment attachment. Structural schema changes создают versioned/archive snapshot.

Каждый snapshot и managed object содержит:

```text
product_model_sha
analytics_snapshot_sha
checklist_sha
incident_revision
release_identity
design_token_version
renderer_version
```

Penpot MCP может использоваться как вспомогательный read/inspect и prototyping-инструмент. Детерминированный sync выполняет самописный plugin, потому что он проверяет catalog, hashes, object identity и idempotency.

## 11. Operating cycle

```text
product model change
→ validation
→ Product Console generation
→ Penpot Product Atlas snapshot
→ owner/design review and comments
→ deterministic implementation prompt
→ candidate implementation
→ acceptance and release
→ production evidence
→ Product Radar and outcome update
→ decision: retain / iterate / narrow / rollback / stop
```

Penpot comment не меняет production автоматически. Он создаёт трассируемое предложение или candidate task. Только accepted code, tests и release evidence меняют actual state.

## 12. Пилот

Первая итерация ограничивается:

- одним Job: «найти подходящее событие и получить достаточно информации для решения»;
- двумя journeys: обычный каталог и Search;
- 5–8 capabilities;
- четырьмя контекстами: desktop normal, mobile weak network, anonymous→auth, keyboard/screen reader;
- 3–5 метриками;
- одним реальным или учебным incident;
- одним Product Radar.

Пилот генерирует:

1. статический Markdown snapshot;
2. одну web coverage/readiness page;
3. один Penpot Product Atlas snapshot.

Критерий успеха: за 10–15 минут владелец без устных пояснений находит главный Job, top problem, affected context, evidence и необходимое решение.

## 13. Отложенные, но не забытые шаги

После owner review этой архитектуры и согласования с общей дизайн-системой:

1. создать пилотный machine-readable registry;
2. восстановить первый набор Jobs и stories;
3. связать их с release-checklist, acceptance и incidents;
4. определить metric contracts и недостающие events;
5. сгенерировать `JOB-HEALTH`, `USER-STORIES`, `OWNER-OUTCOMES` и `FOCUS-GROUP-STORIES` views;
6. затем расширить модель на весь статический сайт и партнёрский контур.
