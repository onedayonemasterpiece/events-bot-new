# Глубокое исследование: визуализация продуктового видения, готовности и фактической ценности

Проведи практическое исследование лучших способов визуально представлять сложное продуктовое видение так, чтобы небольшая команда и владелец продукта могли за несколько минут понять:

- какую проблему и Job решает продукт;
- какие пользовательские и владельческие outcomes ожидаются;
- через какие journeys и capabilities они достигаются;
- какие User Stories и enablers это реализуют;
- что только запланировано, что реализовано, протестировано и выпущено;
- какие сценарии сейчас работают или сломаны на production;
- что реально используется;
- достигается ли пользовательский outcome и ценность владельцу;
- где отсутствуют данные или причинные доказательства;
- какие решения и риски требуют внимания.

Не анализируй конкретный репозиторий и не проектируй ещё один backlog-сервис. Нужна воспроизводимая информационная и визуальная методология, которую позднее можно реализовать из единой машиночитаемой продуктовой модели.

## Контекст

Есть публичный цифровой продукт и небольшая продуктовая команда. Требования, исследования, acceptance-сценарии, release-checklist, статистика, SLI/SLO и production-инциденты уже существуют, но показывают продукт с разных сторон.

Нужна визуальная система, которая:

- не создаёт второй расходящийся источник истины;
- генерирует несколько согласованных представлений из одного data model;
- работает в GitHub/Markdown как долговременная документация;
- при необходимости имеет более удобное web-представление;
- пригодна для desktop, ограниченного mobile review, печати/PDF и screenshot evidence;
- позднее использует общие design tokens, компоненты, иконографику и accessibility-правила продуктовой дизайн-системы;
- не превращается в enterprise-космолёт.

## Методологическая модель, которую нужно визуализировать

### 1. Основная цепочка

```text
свидетельства
→ user need
→ Job и user outcome
→ journey
→ capability сервиса
→ User Stories и technical enablers
→ acceptance rules и scenarios
→ implementation / release
→ production events, metrics и SLI/SLO
→ owner outcome
→ decision
```

### 2. Сущности

- `user_need` — проблема, ограничение или желаемое изменение положения пользователя;
- `job` — устойчивый прогресс, которого пользователь добивается в определённом контексте;
- `user_outcome` — наблюдаемое полезное изменение для пользователя;
- `journey` — один возможный путь выполнения Job через каналы и состояния;
- `capability` — устойчивая способность сервиса поддерживать часть поведения;
- `user_story` — небольшой вертикальный срез capability для поставки;
- `scenario` — контекст: устройство, канал, auth, сеть, accessibility, состояние данных, recovery;
- `operator_job` — задача владельца, редактора или оператора;
- `technical_enabler` — техническая или организационная способность;
- `owner_outcome` — ценность, миссия, удержание, охват, стоимость, риск или скорость решений для владельца;
- `guardrail` — ограничение качества, безопасности, приватности, доступности, diversity или бюджета;
- `metric`, `SLI`, `SLO` — измерительные контракты;
- `incident` — production-воздействие;
- `decision` — решение, принятое на основании evidence.

Job, capability, outcome и stable domain events сравнительно устойчивы. User Story является изменяемым planning artifact и не должна быть корневой единицей визуальной модели.

### 3. Двойной результат

Для значимой capability показываются отдельно:

```text
user outcome
+
owner outcome
+
guardrails
```

Например, высокий engagement не доказывает, что пользователь решил задачу, а рост business metric не должен скрывать ухудшение доступности, качества или доверия.

### 4. Независимые оси evidence

Один статус `done` запрещён. Для Job/capability/story различаются:

```text
definition / decision / design
delivery
verification
deployment / exposure
runtime health
adoption
task completion
user outcome
owner outcome
observability confidence
causal confidence
```

Возможна ситуация:

```text
реализовано
→ протестировано
→ выпущено
→ сейчас сломано
```

При этом прошлое delivery evidence сохраняется, а runtime health становится `broken` и связывается с incident.

### 5. Job attempts

Job, выполняемый всем сервисом, измеряется попытками:

```text
eligible
→ started
→ in_progress
→ completed_strict
→ completed_acceptable
→ rejected_domain
→ cancelled_by_user
→ failed_technical
→ expired_or_unknown

failed_technical
→ recovery_started
→ recovered
→ in_progress
```

Нужно визуально различать task completion, technical failure, domain rejection, user cancellation, recovery и unknown.

### 6. Неполное покрытие

Job имеет конечный набор обязательных scenario cells:

```text
Job
× journey variant
× device/client/app mode
× authentication state
× network state
× accessibility need
× locale/region
× account/data state
× recovery path
```

Для каждой ячейки отдельно видны:

```text
implemented
tested
released
live verified
observed with sufficient data
```

Состояния:

```text
unsupported
implemented_unverified
tested_not_released
released_unhealthy
released_unobserved
degraded_recoverable
healthy
unknown
```

Критический P0-сценарий не должен скрываться высоким средним coverage.

### 7. Разделение source-of-truth контуров

- product model — needs, Jobs, outcomes, journeys, capabilities, stories;
- release-checklist — deliverables и gates конкретного релиза;
- acceptance inventory — правила и сценарии поведения;
- analytics — stable events, metric contracts, SLI/SLO;
- incidents — production impact и recovery;
- feature documents — конкретное поведение и правила;
- evidence — tests, candidates, production receipts и research.

Визуализация должна соединять эти контуры ссылками, но не копировать их содержимое в независимые вручную обновляемые диаграммы.

## Главные вопросы исследования

### 1. Какие визуальные модели лучше подходят разным вопросам?

Сопоставь и критически оцени минимум:

- product vision boards;
- outcome maps и benefits maps;
- impact mapping;
- Opportunity Solution Trees;
- User Story Mapping;
- customer journey maps;
- service blueprints;
- capability maps;
- strategy/goal trees;
- value stream maps;
- dependency graphs;
- traceability matrices;
- coverage heatmaps;
- funnel и state-transition views;
- SLI/SLO и error-budget visualizations;
- release-readiness scorecards;
- incident impact overlays;
- evidence maps и decision logs.

Для каждого подхода укажи:

- какой вопрос он хорошо решает;
- какой слой модели показывает;
- что скрывает;
- при каком масштабе перестаёт читаться;
- насколько пригоден для регулярного обновления;
- можно ли генерировать его из structured data;
- насколько он доступен и понятен неэкспертному владельцу продукта.

Не выбирай одну диаграмму для всего.

### 2. Какой минимальный согласованный набор views нужен?

Предложи не более 5–7 основных представлений. В качестве гипотезы проверь:

1. **Product vision / outcomes view** — user needs, Jobs, user outcomes, owner outcomes и guardrails.
2. **Job and journey view** — backbone пользовательского пути, альтернативные journeys и service responsibility.
3. **Capability / story map** — capabilities, vertical slices, enablers и release slices.
4. **Service blueprint** — customer actions, frontstage, backstage, support systems и operator jobs.
5. **Coverage and health matrix** — context scenarios, delivery/test/release/live/observed state.
6. **Evidence and release view** — exact release, tests, production evidence, incidents и stale/unknown.
7. **Owner outcome scorecard** — user outcome, owner outcome, guardrails, confidence и decision.

Сократи или измени набор, если исследование показывает более устойчивую композицию.

Для каждого view определи:

- главный пользовательский вопрос;
- primary entities;
- обязательные поля;
- допустимую плотность;
- порядок чтения;
- переходы в другие views;
- статическое Markdown/SVG-представление;
- интерактивное web-представление;
- mobile fallback;
- print/PDF fallback.

### 3. Как избежать перегрузки и «спагетти»?

Исследуй практики:

- overview first, zoom/filter, details on demand;
- progressive disclosure;
- focus + context;
- small multiples;
- hierarchical edge bundling или альтернативы графам;
- ограничение числа уровней и связей на одном экране;
- semantic zoom;
- lanes и swimlanes;
- stable backbone;
- drill-down до evidence вместо постоянного показа всех ссылок;
- фильтры по Job, release, scenario, owner и risk;
- отдельные views для target и actual.

Дай конкретные числовые или операционные ограничения там, где они обоснованы: например, сколько карточек, колонок, уровней, status-маркеров и cross-links допустимо до перехода к drill-down.

### 4. Как визуально кодировать состояния без ложной простоты?

Нужно различать как минимум:

```text
planned
implemented
verified
released
healthy
broken
degraded_recoverable
unobserved
unknown
insufficient_sample
stale_evidence
owner_decision_required
superseded
```

Исследуй:

- цвет;
- форму;
- pattern/texture;
- иконку;
- текстовый label;
- положение на оси;
- opacity;
- confidence marker;
- freshness marker.

Определи:

- почему одного traffic-light цвета недостаточно;
- как не смешивать readiness, health и outcome;
- как показывать `unknown` отдельно от `not started` и `healthy`;
- как показывать stale evidence;
- как показывать критический floor rule;
- как сделать систему понятной при color-vision deficiencies, high contrast и screen reader.

Результатом должен стать **semantic status grammar**, которую позднее можно выразить design tokens.

### 5. Как показать target, actual и history?

Предложи способ различать:

- целевую продуктовую модель;
- текущий delivery state;
- exposure конкретного release;
- фактический production health;
- observed usage и outcomes;
- изменение во времени;
- открытые и закрытые incidents;
- решения и superseded hypotheses.

Исследуй, когда полезны:

- наложение target/actual;
- side-by-side;
- temporal snapshots;
- sparklines;
- release bands;
- evidence freshness badges;
- immutable decision snapshots.

### 6. Как связать визуализацию с аналитикой?

Определи, как визуально пройти путь:

```text
Job / outcome
→ measurement question
→ metric / SLI / SLO
→ stable events/facts
→ current value
→ confidence / sample / freshness
→ decision
```

Dashboard не должен выдавать usage за completion, completion за user outcome, а before/after correlation — за causality.

Предложи понятные visual levels evidence:

```text
specification
implementation
acceptance
production health
observational outcome
causal evidence
```

### 7. Как встроить визуальную модель в дизайн-систему?

Дизайн-система продукта разрабатывается параллельно. Исследование должно дать interface contract, а не самостоятельный конкурирующий стиль.

Определи необходимые будущие компоненты и tokens:

- entity cards;
- nodes и relationships;
- status badges;
- evidence chips;
- metric cards;
- timeline/release bands;
- lanes;
- coverage cells;
- incident markers;
- decision callouts;
- legends;
- filters;
- empty/unknown/stale states.

Определи, что должно быть общим с продуктовой дизайн-системой:

- typography;
- spacing/grid;
- semantic colors;
- icons;
- elevation/borders;
- focus and keyboard behavior;
- responsive breakpoints;
- reduced motion;
- accessible descriptions;
- screenshot/reference governance.

Не выбирай конкретный фирменный цвет или декоративный стиль. Нужна информационная архитектура и component semantics, которые затем получат общий визуальный язык бренда.

### 8. Как поддерживать один source of truth?

Исследуй архитектуру:

```text
machine-readable registry
+ release checklist
+ scenario inventory
+ metric catalog
+ incident/evidence indexes
→ generated visual views
```

Определи:

- что редактируется вручную;
- что вычисляется;
- что является immutable snapshot;
- как проверять unknown refs и orphan entities;
- как обновлять views без ручного рисования;
- какие форматы подходят: YAML/TOML/JSON, Markdown, Mermaid, Graphviz, SVG, HTML;
- ограничения Mermaid и других text-to-diagram подходов;
- когда нужен custom renderer;
- как сохранять deep links и source citations;
- как GitHub Project может быть только синхронизированным представлением, а не вторым реестром.

## Практический пример

Покажи одну и ту же предметную область во всех рекомендуемых views.

Используй нейтральный пример сервиса событий:

```text
Job:
найти подходящее событие и получить достаточно информации для решения.

Journeys:
обычный каталог, поиск, персональная выдача, подборка, похожие события.

Capabilities:
актуальный каталог, discovery, event detail, save/calendar/share,
registration/ticket, route/transport, authentication recovery.

User outcome:
пользователь нашёл пригодный вариант и принял решение.

Owner outcomes:
повторное использование, полезное внимание к событиям,
снижение пустых сессий и стоимости одного полезного результата.

Guardrails:
accessibility, diversity, privacy, performance, reliability, resource budget.
```

Добавь минимум четыре context-сценария:

- desktop, нормальная сеть;
- mobile, слабая сеть;
- anonymous → auth handoff;
- screen reader или keyboard-only.

Покажи случай:

```text
capability реализована и прошла candidate-тест,
выпущена на production,
но mobile weak-network scenario сейчас нарушает SLO,
а accessibility scenario имеет недостаточно production data.
```

Из примера должно быть понятно, какие views помогают:

- увидеть продуктовый смысл;
- локализовать сломанный контекст;
- перейти к incident/evidence;
- понять влияние на user и owner outcomes;
- принять решение.

## Источники

Используй преимущественно первичные и авторитетные источники по:

- User Story Mapping и product discovery;
- Opportunity Solution Trees;
- service design и service blueprinting;
- systems engineering traceability;
- product operations и outcome management;
- information visualization и visual analytics;
- cognitive load, overview/zoom/details-on-demand;
- dashboard and status visualization;
- SRE observability, SLI/SLO и incidents;
- WCAG и inclusive data visualization;
- design systems и semantic design tokens.

Явно отделяй:

- общепринятые практики;
- выводы конкретных авторов;
- собственный синтез для этой задачи.

## Требуемый результат

Верни один цельный Markdown-отчёт объёмом не более 8 000 слов.

Структура:

1. Краткий вывод и рекомендуемая visual architecture.
2. Сравнение подходов и диаграмм.
3. Минимальный набор согласованных views.
4. Semantic status grammar.
5. Progressive disclosure и навигация между views.
6. Target / actual / history / confidence model.
7. Связь с metrics, SLI/SLO, incidents и decisions.
8. Accessibility и responsive/print правила.
9. Контракт интеграции с дизайн-системой.
10. Machine-readable input и generated-output architecture.
11. Сквозной практический пример.
12. Антипаттерны.
13. Пилотный план внедрения на одну итерацию.

Для каждого рекомендуемого view добавь компактную спецификацию:

```text
Purpose
Primary question
Entities
Visual structure
Status encoding
Drill-down
Static format
Interactive format
Mobile/print fallback
Accessibility
Failure modes
```

В конце дай:

- одну сводную таблицу `entity → primary view → secondary view`;
- одну таблицу `state → semantic meaning → visual channels → accessible label`;
- рекомендуемый набор reusable visualization components;
- критерии, по которым можно проверить прототип на владельце продукта за 10–15 минут.

## Антикритерии

Исследование неудовлетворительно, если оно:

- предлагает одну гигантскую схему;
- перечисляет инструменты без информационной модели;
- смешивает план, release, runtime health и outcome одним цветом;
- считает отсутствие данных успехом;
- предлагает вручную поддерживать несколько расходящихся досок;
- не различает user и owner outcomes;
- не показывает incident и recovery;
- не учитывает accessibility;
- создаёт визуальный стиль отдельно от будущей дизайн-системы;
- превращает решение в тяжёлую enterprise-платформу до проверки пилота.
