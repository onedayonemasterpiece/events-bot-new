# Визуализация продуктового видения и фактической готовности

## Рекомендуемая визуальная архитектура

Рекомендуется не единая «карта продукта», а **система из пяти согласованных проекций одной машиночитаемой модели**. Каждая проекция отвечает на отдельный класс вопросов, но использует одинаковые идентификаторы сущностей, статусы, фильтры, контексты и ссылки на evidence.

У системы должен быть **устойчивый backbone**:

```
text
```

**Копировать**

```
user need
→ Job
→ user outcome
→ journey
→ capability

```

И более изменчивые слои:

```
text
```

**Копировать**

```
capability
→ User Story / technical enabler
→ acceptance scenario
→ implementation evidence
→ release evidence
→ runtime observation / incident
→ metric evidence
→ owner outcome
→ decision

```

Job, outcome, journey и capability целесообразно считать долговечными узлами модели. User Stories, тесты, релизы и решения должны ссылаться на них, а не заменять их. Это соответствует природе story mapping: пользовательские действия образуют backbone, а поставляемые элементы и release slices располагаются под ним; impact mapping, в свою очередь, связывает цели, изменения поведения и deliverables, делая видимой причинную гипотезу. 

Ключевое архитектурное правило — **не вычислять один агрегированный ****`done`**. Вместо этого у сущности одновременно существуют независимые фасеты:

```
yaml
```

**Копировать**

```
delivery:
  defined: true
  implemented: true
  verified: true
  released: true

runtime:
  status: broken
  context: mobile + weak_network
  incident: INC-42

adoption:
  status: insufficient_data

user_outcome:
  status: unknown

owner_outcome:
  status: insufficient_data

```

Таким образом, `implemented`, `verified` и `released` являются историческим evidence, а `broken` — текущим runtime-состоянием. Инцидент не отменяет факт поставки, а объясняет актуальное нарушение. Такая модель согласуется с SRE-практикой: SLI измеряет наблюдаемую характеристику сервиса, SLO задаёт целевой уровень, а incident и postmortem сохраняют воздействие, временную линию, причины и последующие действия. 

Все views должны явно разделять:

| **ИзмерениеTargetActual** |                                  |                                               |
| ------------------------- | -------------------------------- | --------------------------------------------- |
| Продуктовый смысл         | ожидаемый user/owner outcome     | подтверждённый или неподтверждённый результат |
| Scope                     | planned capabilities и stories   | реализованные и выпущенные элементы           |
| Coverage                  | требуемые contexts и scenarios   | реально проверенное покрытие                  |
| Reliability               | SLO и guardrails                 | текущие SLI, incidents и error-budget state   |
| Adoption                  | ожидаемое использование          | наблюдаемое использование                     |
| Evidence                  | требуемый измерительный контракт | свежесть, выборка, пробелы и provenance       |

Эта архитектура не является backlog-сервисом. Это **read model** продуктовой системы: редактируется модель и evidence, а визуальные представления генерируются как projections.

## Сравнение применимых подходов

Ни один из существующих методов не покрывает всю цепочку. Наиболее практична композиция их сильных сторон.

| **ПодходКакой вопрос решаетПоказывает хорошоЧто скрываетПоддержка из structured data** |                                                                    |                                                                        |                                                                 |                                                                                          |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| **User Story Mapping**                                                                 | Как пользователь проходит работу и какими slices её поставлять?    | Последовательность действий, backbone, stories, release slices         | Причинную связь с outcome, runtime health, контекстное покрытие | **Высокая**, если backbone и stories имеют стабильные ID                                 |
| **Outcome / Impact Mapping**                                                           | Зачем делать deliverable и какое изменение должно привести к цели? | Цель, actors, impacts, assumptions, связь результата с поставкой       | Подробный journey, тестовое покрытие, production-состояние      | **Высокая** для графа связей                                                             |
| **Journey Map**                                                                        | Что делает и переживает пользователь на пути к Job?                | Фазы, действия, touchpoints, препятствия, альтернативные пути          | Backstage, технические зависимости, readiness                   | **Средняя–высокая**, если эмоции и исследования не представлены только свободным текстом |
| **Service Blueprint**                                                                  | Что внутри сервиса обеспечивает пользовательский путь?             | Frontstage, backstage, support processes, зависимости                  | Стратегические outcomes, release evidence и adoption            | **Средняя**, поскольку детализация процессов быстро разрастается                         |
| **Capability Map**                                                                     | Какими устойчивыми способностями должен обладать сервис?           | Стабильную структуру продукта, gaps, ownership, зависимости            | Временную последовательность journey и мелкие slices            | **Очень высокая**                                                                        |
| **Coverage Heatmap**                                                                   | Где capability или journey проверены в важных контекстах?          | Пропуски по device, auth, network, accessibility, data state, recovery | Причину дефекта и пользовательскую значимость без drill-down    | **Очень высокая**, если contexts нормализованы                                           |
| **Release / Readiness Scorecard**                                                      | Можно ли принимать go/no-go решение?                               | Checklist, blockers, sign-off, completeness, quality gates             | Продуктовый смысл и post-release деградацию                     | **Очень высокая**                                                                        |
| **SLI/SLO и incident visualization**                                                   | Работает ли сервис сейчас так, как его воспринимают пользователи?  | Reliability, latency, correctness, error budget, incidents, recovery   | Планируемые capabilities и более широкие продуктовые outcomes   | **Очень высокая** при интеграции с telemetry                                             |

Story map хорошо показывает действия и release slices, но сам по себе не доказывает, что пользователь достиг результата. Impact map делает видимой цепочку «цель → изменение поведения → deliverable», но обычно не отражает подробную последовательность пути. 

Journey map рассматривает опыт с точки зрения пользователя, а service blueprint — деятельность организации, frontstage, backstage и support processes, создающие этот опыт. Поэтому blueprint следует использовать как детализацию выбранного journey, а не как главный обзор всего продукта. 

Capability map наиболее пригодна в роли стабильной структуры между journeys и изменчивыми stories. Профессиональные reference models business architecture обычно связывают capability maps с value streams, stakeholders и information maps, то есть рассматривают capability как устойчивую архитектурную единицу, а не sprint-item. 

Readiness scorecard полезен как gate: production-readiness reviews обычно проверяют функциональные и нефункциональные требования, тестирование, deployment, support и риски, после чего поддерживают go/no-go решение. Однако после выпуска checklist должен уступить место фактическим runtime-сигналам. 

SLI/SLO-представление должно измерять путь так, как его воспринимает пользователь, а alerts — реагировать на существенное нарушение SLO, а не просто на отдельные внутренние показатели. Error-budget policy может напрямую инициировать решение об ограничении релизов и приоритизации reliability work. 

## Минимальный набор views

**View: Product Outcome Spine**

**Purpose:** объяснить, зачем существует продукт и как поставка должна привести к пользовательскому и владельческому результату.
**Primary question:** «Какую задачу решаем, какой observable outcome ожидаем и на каких предположениях основана связь с owner outcome?»
**Main entities:** user need, Job, user outcome, owner outcome, guardrail, capability, hypothesis, decision.
**Visual structure:** компактный направленный граф `need → Job → user outcome → owner outcome`; capabilities подключаются как средства, guardrails — как поперечные ограничения. Рядом с каждой outcome показываются metric contract и уровень evidence.
**Status encoding:** outcomes получают не delivery-статус, а `confirmed`, `not_confirmed`, `unknown`, `insufficient_data`, `stale_evidence`; capabilities отображают отдельные delivery/runtime badges.
**Drill-down:** Job → journeys; outcome → metric definitions, observations и decisions; capability → delivery map.
**Static form:** Markdown-таблица связей плюс небольшой Mermaid-граф.
**Interactive form:** collapsible graph с фильтром по Job, outcome, guardrail и времени.
**Accessibility:** граф сопровождается линейным текстовым описанием всех связей; порядок DOM повторяет причинную цепочку.

Метрики должны выводиться из purpose и гипотез сервиса, а не подбираться после поставки. GOV.UK Service Manual рекомендует начинать с user need и intended benefit, формулировать гипотезы, затем определять источники данных и сочетать количественные метрики с user research. 

**View: Job and Journey Map**

**Purpose:** показать альтернативные пути выполнения Job, их шаги, recovery и фактическую работоспособность.
**Primary question:** «Какими путями пользователь может завершить Job, и какие шаги работают в конкретном контексте?»
**Main entities:** Job, journey, journey step, touchpoint, capability, acceptance scenario, context, incident.
**Visual structure:** small multiples: отдельная горизонтальная lane для каталога, поиска, персональной выдачи и подборки. Общие шаги выровнены по смысловым фазам, а не принудительно объединены. Под основной веткой располагаются recovery paths.
**Status encoding:** над шагом — delivery milestones; внутри шага — текущий runtime badge; под шагом — completion/usage; incident marker присоединён к затронутому контексту.
**Drill-down:** шаг → capability → acceptance scenarios → evidence → incident.
**Static form:** таблица journeys × steps и отдельный список известных нарушений.
**Interactive form:** фильтры device, authentication, network, accessibility, account/data state и release; переключатель target/actual.
**Accessibility:** каждая lane имеет заголовок и текстовый summary; визуальные стрелки дублируются ordered structure.

**View: Capability Delivery Map**

**Purpose:** соединить стабильные capabilities с поставляемыми stories, enablers, тестами и релизами.
**Primary question:** «Какие части продукта обеспечивают journey и насколько далеко каждая прошла по delivery pipeline?»
**Main entities:** capability, User Story, technical enabler, acceptance scenario, implementation record, release.
**Visual structure:** capability является родительской карточкой; под ней — stories и enablers. Этапы `defined → implemented → verified → released` представлены фиксированными колонками, а не единым badge. Target scope и actual scope показываются отдельными секциями.
**Status encoding:** milestone присутствует только при наличии evidence; текущая поломка отображается отдельным runtime marker, не снимающим milestone.
**Drill-down:** capability → story/enabler → pull request/build/test run/release artifact; обратная навигация к journeys и outcomes.
**Static form:** Markdown-таблица capabilities с вложенными ссылками; release slices — отдельными группами.
**Interactive form:** treegrid с раскрытием children, фильтром release и сортировкой по gap/risk.
**Accessibility:** нативная HTML-таблица предпочтительнее ARIA-grid; treegrid применяется только при реальной интерактивной иерархии и реализует полную keyboard navigation. WAI рекомендует native table для статических данных, а grid/treegrid — для интерактивной табличной навигации. 

**View: Coverage and Operational Readiness Matrix**

**Purpose:** показать readiness, контекстное покрытие и production health без смешивания этих понятий.
**Primary question:** «Что поставлено, где проверено и в каких production-контекстах сейчас работает?»
**Main entities:** journey step или capability, context dimension, acceptance scenario, release, SLI/SLO, incident.
**Visual structure:** строки — capabilities или критические journey steps; группы колонок: delivery evidence, release, runtime, contexts. Контексты представлены small-multiple matrices, а не одной усреднённой ячейкой.
**Status encoding:** каждая ячейка содержит текстовый token и символ; пустая ячейка запрещена. Неизвестное состояние выводится как `unknown`, недостаточная выборка — `insufficient_data`.
**Drill-down:** coverage cell → scenarios и test runs; runtime cell → SLI и incident; release marker → artifact и change record.
**Static form:** печатная scorecard-таблица с legend и списком blockers.
**Interactive form:** sticky headers, сохранённые фильтры, режим «только gaps», сравнение candidate/released/current production.
**Accessibility:** summary перед матрицей, row/column headers, caption, сокращённый mobile-режим по одной capability; pattern и текст сохраняются при печати.

**View: Evidence, Metrics and Decisions**

**Purpose:** отделить наблюдения от выводов и показать, какие решения требуют владельца.
**Primary question:** «Используется ли продукт, достигается ли outcome, надёжны ли данные и какое решение следует принять?»
**Main entities:** metric, SLI, SLO, evidence record, incident, decision, risk, owner outcome.
**Visual structure:** metric cards группируются по user outcome, owner outcome и guardrails. Рядом располагаются incident timeline и decision log. Каждая карточка показывает definition, current value, target, window, segmentation, sample size, freshness и provenance.
**Status encoding:** `healthy/degraded/broken` применяется только к operational contract; `confirmed/not_confirmed` — к outcome; `insufficient_data/stale_evidence` — к качеству evidence; `decision_required` — к governance.
**Drill-down:** metric → query/dashboard/data source; incident → timeline/postmortem/actions; decision → evidence snapshot и superseding decision.
**Static form:** Markdown decision register и snapshot ключевых метрик.
**Interactive form:** trends, segment filters, annotations релизов и incidents, сравнение до/после.
**Accessibility:** числа и тенденции выражаются текстом; chart имеет accessible table и long description. W3C требует для сложных графиков и диаграмм краткое и развёрнутое текстовое представление существенной информации. 

## Semantic status grammar

Статусная система должна быть **многоосевой**. Нельзя выбирать один статус из общего enum для всей сущности: `released` и `broken` могут быть истинны одновременно.

| **TokenТекст и основной символФорма, pattern и положениеСемантика** |                         |                                                 |                                                                  |
| ------------------------------------------------------------------- | ----------------------- | ----------------------------------------------- | ---------------------------------------------------------------- |
| `planned`                                                           | `PLANNED` · ◌           | Пунктирная рамка; target-область                | Одобрено как намерение, evidence реализации отсутствует          |
| `implemented`                                                       | `IMPLEMENTED` · ■       | Квадрат в delivery-колонке                      | Существует implementation evidence                               |
| `verified`                                                          | `VERIFIED` · ◆✓         | Ромб в verification-колонке                     | Acceptance evidence соответствует заданному environment          |
| `released`                                                          | `RELEASED` · ▲          | Треугольник/маркер релиза                       | Artifact доступен целевой аудитории или environment              |
| `healthy`                                                           | `HEALTHY` · ●✓          | Сплошной круг; runtime-область                  | SLI удовлетворяет operational contract                           |
| `degraded`                                                          | `DEGRADED` · △!         | Треугольник с диагональной штриховкой           | Путь работает частично или близок к breach                       |
| `broken`                                                            | `BROKEN` · ⛔×           | Восьмиугольник/толстая рамка                    | Путь не выполняет определённый критический результат             |
| `unknown`                                                           | `UNKNOWN` · ?           | Полая форма с точечным контуром                 | Текущее состояние не наблюдалось или не классифицировано         |
| `insufficient_data`                                                 | `INSUFFICIENT DATA` · ∅ | Точечная заливка                                | Измерение существует, но не выполнены sample/coverage criteria   |
| `stale_evidence`                                                    | `STALE` · ◷             | Диагональная штриховка и timestamp              | Последнее evidence старше freshness contract                     |
| `decision_required`                                                 | `DECISION REQUIRED` · ! | Callout в отдельной decision-колонке            | Нарушен порог, есть конфликт evidence или требуется owner action |
| `superseded`                                                        | `SUPERSEDED` · ↪        | Двойной контур, muted pattern, ссылка на замену | Запись исторически действительна, но заменена новой              |
| `not_applicable`                                                    | `N/A` · —               | Горизонтальная черта                            | Контекст явно исключён моделью; не равен unknown                 |

Цвет может усиливать различие, но никогда не несёт значение самостоятельно. WCAG требует, чтобы цвет не был единственным способом передачи информации, а границы, focus indicators и другие визуальные части интерфейса имели достаточный non-text contrast. 

Дополнительные правила:

1. Полный текстовый token всегда доступен визуально либо через accessible name.
2. Положение имеет стабильную семантику: delivery слева направо, runtime после release, outcome evidence отдельно.
3. Patterns различают degraded, unknown и stale в grayscale/PDF.
4. Иконки не используются без текста или legend.
5. Screen reader получает фразу вида: «Event detail; released 14 July; production broken for mobile on weak network; incident INC-42; screen-reader coverage insufficient».
6. В high-contrast mode сохраняются border width, shape, text и системные forced colors.
7. Focus проходит по сущностям и действиям, а не по каждой декоративной ячейке.

## Навигация и progressive disclosure

Основной паттерн — **overview first, zoom and filter, then details on demand**. Оригинальная taxonomy Шнайдермана также включает relate, history и extract, что особенно полезно здесь: пользователю нужно видеть связи, историю evidence и экспортировать отфильтрованный snapshot. 

Первый экран должен отвечать только на пять вопросов: какой Job приоритетен; какой outcome ожидается; какие journeys его обеспечивают; где имеются broken/degraded/unknown; какие decisions required. Stories, отдельные тесты и metric queries раскрываются позднее.

Рекомендуемый путь навигации:

```
text
```

**Копировать**

```
Product Outcome Spine
→ Job
→ Journey lane
→ Capability
→ Story / enabler / acceptance scenario
→ implementation or release evidence
→ metric or SLI
→ incident
→ decision

```

Каждая сущность должна иметь canonical URL и backlinks. Переход от incident к затронутому journey столь же важен, как переход от journey к incident.

Фильтры должны быть общими для всех views: Job, journey, capability, release, environment, device, app mode, authentication, network, accessibility, data/account state, recovery path, evidence freshness и time window. Смена view не должна сбрасывать filter context.

Для различения target и actual предпочтительны либо параллельные панели, либо переключатель с явной подписью. Нельзя накладывать target и actual друг на друга только различием оттенка. Для нескольких journeys лучше использовать small multiples с одинаковыми фазами и шкалами: это упрощает сравнение и предотвращает превращение карты в сеть пересекающихся линий.

Статическая версия должна начинаться с текстового summary, затем показывать ограниченную таблицу, а подробности помещать ниже или в collapsible sections. GitHub поддерживает Mermaid и `<details>`, но сложные Mermaid-диаграммы могут становиться трудными для чтения и рендеринга; диаграммы также следует объяснять текстом. 

## Связь с source of truth

Рекомендуемый pipeline:

```
text
```

**Копировать**

```
versioned product model
+ acceptance scenarios
+ release and build evidence
+ metric catalog
+ telemetry snapshots
+ incident records
+ decision records
→ validation and derivation
→ generated Markdown, SVG and web views

```

**Вручную редактируются:** определения Jobs и outcomes; journeys и steps; capabilities и зависимости; stories и enablers; guardrails; acceptance scenarios; metric/SLI/SLO contracts; ожидаемые contexts; risk statements; decisions и rationale.

**Автоматически импортируются:** pull requests и commits; build artifacts; test results; deployment/release events; feature-flag exposure; telemetry aggregates; SLO observations; incidents и postmortem metadata.

**Вычисляются:** delivery milestones; gaps между target и actual; context coverage; `unknown`; `insufficient_data`; `stale_evidence`; current runtime status; затронутые journeys; readiness blockers; outcome confidence; список `decision_required`.

Минимальная запись evidence должна содержать:

```
yaml
```

**Копировать**

```
id:
subject_id:
evidence_type:
observed_at:
environment:
release_id:
contexts:
result:
source:
fresh_until:
sample_size:
incident_id:

```

У всех сущностей требуются стабильный `id`, `type`, `title`, `owner`, timestamps и typed relations. Status не должен вручную копироваться в несколько файлов: он выводится из evidence и правил.

Markdown/GitHub подходят для Product Outcome Spine в табличной форме, capability index, release snapshot, decision register, incident summaries и простых Mermaid-графов. GitHub нативно визуализирует Mermaid-файлы и fenced Mermaid diagrams. 

SVG или web-renderer нужны для coverage matrices, journey small multiples, плотных capability maps и наложения incidents/metrics. SVG должен иметь title/description и сопровождаться HTML-таблицей. Интерактивный renderer нужен только для фильтрации, сохранения context, drill-down, trend charts и comparison; редактирование модели может оставаться в YAML/JSON/Markdown и существующих инженерных системах.

Чтобы избежать расходящихся досок:

- generated views помечаются `GENERATED — DO NOT EDIT`;
- workshop-доски считаются временными;
- результат workshop переносится в модель через reviewable change;
- внешние systems of record сохраняют собственные артефакты, а модель хранит ссылки и нормализованные evidence records;
- CI проверяет orphan links, циклы, отсутствующие owners, невалидные status combinations и просроченное evidence.

## Контракт интеграции с дизайн-системой

Методология определяет семантику компонентов, но не их цвет, типографику, spacing или визуальный бренд.

| **КомпонентСемантический контракт** |                                                                                             |
| ----------------------------------- | ------------------------------------------------------------------------------------------- |
| **Entity card**                     | `entity_type`, stable ID, title, owner, short purpose, relations, facets, updated time      |
| **Status badge**                    | status token, текстовая label, facet, observed time, context, evidence link                 |
| **Metric card**                     | definition, outcome relation, value, target, window, segment, sample, freshness, provenance |
| **Coverage cell**                   | subject, context tuple, expected scenarios, observed scenarios, result, gaps                |
| **Journey lane**                    | Job, journey ID, ordered steps, entry/exit, completion definition, recovery paths           |
| **Release marker**                  | release ID, environment, timestamp, audience/exposure, artifact, rollback reference         |
| **Incident marker**                 | incident ID, severity, active/resolved, affected contexts, impact, timeline link            |
| **Evidence link**                   | evidence type, immutable source reference, timestamp, environment, freshness                |
| **Decision callout**                | decision status, trigger, alternatives, owner, deadline, evidence snapshot                  |
| **Legend**                          | все status tokens, shapes, patterns, facet definitions и data-quality semantics             |
| **Filters**                         | общая модель выбранного Job, contexts, release, environment и time window                   |

От общей дизайн-системы наследуются typography tokens, semantic colors, borders, patterns, spacing, icons, focus treatment, breakpoints и motion preferences. Для обмена design tokens между инструментами уже существует vendor-neutral формат Design Tokens Community Group; методология может использовать такие tokens, не определяя собственную палитру. 

Responsive behavior должен менять композицию, но не смысл. На узком экране capability matrix превращается в последовательность карточек с одинаковым порядком фасетов. Hover никогда не является единственным способом открытия evidence. Любое действие доступно с клавиатуры; сортировка и фильтрация объявляются assistive technology; сложные grids следуют WAI-ARIA Authoring Practices. 

## Практический пример, пилот и антипаттерны

Для сервиса событий Product Outcome Spine показывает:

```
text
```

**Копировать**

```
Need:
понять, стоит ли посещать событие

Job:
найти подходящее событие и получить достаточно информации для решения

User outcome:
пользователь нашёл пригодный вариант и принял решение

Owner outcomes:
повторное использование
снижение пустых сессий

```

К Job подключены journeys: каталог, поиск, персональная выдача и подборка. К ним подключены capabilities: актуальный каталог, event detail, save/calendar/share, registration, route и authentication recovery. Guardrails — accessibility, diversity, privacy, performance и reliability.

Рассматриваемая capability — `event-detail`.

| **ФасетФактическое состояние** |                                                                       |
| ------------------------------ | --------------------------------------------------------------------- |
| Defined                        | Acceptance scenarios и contexts определены                            |
| Implemented                    | Implementation evidence присутствует                                  |
| Verified                       | Протестировано на release candidate                                   |
| Released                       | Выпущено в production                                                 |
| Runtime                        | `broken` для `mobile + weak_network`                                  |
| Incident                       | `INC-42`, активен или mitigated                                       |
| Screen reader                  | `insufficient_data`                                                   |
| Usage                          | Наблюдается, но сегмент mobile weak-network выделен отдельно          |
| User outcome                   | Для затронутого сегмента `not_confirmed` или `unknown`, но не success |
| Owner outcome                  | `insufficient_data`, пока нет достаточного окна после исправления     |
| Governance                     | `decision_required`: hotfix, rollback или ограничение тяжёлого media  |

**Product Outcome Spine** не меняет capability на «не реализована». Он показывает, что связь `event-detail → informed decision` имеет актуальный риск, а owner outcome ещё не подтверждён.

**Job and Journey Map** показывает сломанный шаг `открыть detail` в journeys каталога и поиска только при фильтре `mobile + weak_network`. Recovery path может вести к облегчённой карточке или повторной попытке. Для screen reader отображается `insufficient_data`, а не healthy.

**Capability Delivery Map** сохраняет четыре milestones: defined, implemented, verified и released. После release находится отдельный runtime marker `BROKEN`, связанный с `INC-42`. Candidate test не объявляется ложным: его environment и contexts остаются видимыми.

**Coverage and Operational Readiness Matrix** выглядит концептуально так:

| **ContextImplementedVerifiedReleasedRuntimeOutcome evidence** |     |          |          |                     |                       |
| ------------------------------------------------------------- | --- | -------- | -------- | ------------------- | --------------------- |
| Desktop, normal network                                       | Yes | Verified | Released | Healthy             | Observed              |
| Mobile, normal network                                        | Yes | Verified | Released | Healthy             | Partial               |
| Mobile, weak network                                          | Yes | Partial  | Released | **Broken — INC-42** | Not confirmed         |
| Screen reader                                                 | Yes | Limited  | Released | **Unknown**         | **Insufficient data** |

**Evidence, Metrics and Decisions View** показывает latency/error-rate SLI для event detail, completion from listing to decision proxy, incident impact, sample size по accessibility и decision callout. SLO должен иметь target и measurement window, а incident — сохранять воздействие и remediation actions. 

Пилот на одну итерацию следует ограничить одним Job, двумя journeys, пятью–восемью capabilities, одним текущим release, несколькими acceptance scenarios, тремя–пятью метриками и хотя бы одним реальным или учебным incident.

В начале итерации команда определяет stable IDs, связи, contexts и минимальный YAML/JSON schema. Затем импортирует существующие release/test/incident references, реализует генерацию Markdown для всех пяти views и только одну интерактивную страницу — coverage/readiness matrix. В конце команда проводит пятнадцатиминутный review с product owner и инженером, проверяя, можно ли без устных пояснений найти broken journey, неизвестное покрытие, evidence и необходимое решение.

Критерии успеха пилота: владелец за несколько минут находит главный Job, outcome gap и decision required; команда не вводит статусы вручную в нескольких местах; release и runtime не противоречат друг другу; `unknown` возникает автоматически при отсутствии evidence; incident виден из journey и capability; статический PDF остаётся понятным без цвета.

Основные антипаттерны:

| **АнтипаттернПочему опасенЗамена** |                                                     |                                                |
| ---------------------------------- | --------------------------------------------------- | ---------------------------------------------- |
| Одна гигантская диаграмма          | Смешивает разные вопросы и масштабы                 | Пять согласованных projections                 |
| Универсальный `done`               | Скрывает release, runtime и outcome gaps            | Независимые facets                             |
| Пустая ячейка как success          | Превращает отсутствие наблюдения в уверенность      | Явный `unknown`                                |
| Процент readiness без состава      | Маскирует критический blocker средним значением     | Rule-based blockers и раскрываемые evidence    |
| Heatmap только по цвету            | Непонятна в high contrast и печати                  | Text, shape, pattern и position                |
| Story как корень модели            | История переписывается и ломает трассировку         | Job/journey/capability backbone                |
| Incident только в ops-dashboard    | Product owner не видит нарушенный outcome           | Связь incident → context → journey → outcome   |
| Смешивание candidate и production  | Успешный тест выдаётся за текущую работоспособность | Environment и observed-at для каждого evidence |
| Несколько ручных досок             | Статусы и scope расходятся                          | Generated read models                          |
| Ранняя enterprise-платформа        | Автоматизирует непроверенную онтологию              | Пилот в Git/Markdown и лёгком renderer         |

Итоговая методология строится не вокруг отчётности о выполненной работе, а вокруг трассируемой цепочки: **зачем продукт нужен, каким путём создаётся результат, что было поставлено, что работает сейчас, что измерено, чего мы не знаем и какое решение следует принять**.