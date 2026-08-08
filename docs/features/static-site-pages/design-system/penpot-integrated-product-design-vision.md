# Интегрированное видение Penpot: Product Atlas, дизайн-система, UI gaps и визуальная проверка

> **Статус:** проект целевого видения и пилотного acceptance contract; не является принятым ADR до практической проверки.  
> **Дата:** 8 августа 2026 года.  
> **Канонический продуктовый и runtime-репозиторий:** `onedayonemasterpiece/events-bot-new`.  
> **Penpot:** два параллельных, но сходящихся решения — Product Atlas и Design System.  
> **MCP:** подключение проверено как доступный рабочий путь; в этом документе требуется проверить его безопасность, parity с plugin path и реальную экономию действий оператора.  
> **Назначение документа:** одно самодостаточное окно контекста для реализации и тестирования полного процесса.

---

## 1. Короткое решение

LoveKGD использует **два Penpot-плагина и два смысловых контура**, а внутри Design System — два типа файлов:

```text
1. Product Atlas plugin
   └── Product Atlas file

2. LoveKGD Design System plugin
   ├── Resource Graph file
   └── UI Exploration file
```

Дополнительно кодовый агент может работать с `UI Exploration` через официальный Penpot MCP:

```text
LoveKGD plugin
= детерминированное обновление, проверка, adoption, history, freeze и promotion

Penpot MCP + code agent
= быстрая live-работа над следующей итерацией в защищённой рабочей зоне
```

Основной сквозной поток:

```text
Product Atlas
product problem / Job / journey / capability / outcome / UI gap
        │
        │ stable IDs + product context
        ▼
Design System · UI Exploration
visual analysis → alternatives → components/patterns/compositions
        │
        ├── plugin-only iteration
        └── MCP-assisted live iteration
        │
        ▼
Selected for build
approved design reference + implementation package
        │
        ▼
GitHub implementation + browser tests
        │
        ▼
Resource Graph
accepted components / patterns / archetypes / runtime evidence
        │
        ▼
Product Atlas
decision, coverage, release/evidence and resulting product state
```

Главное правило:

> **Product Atlas отвечает, зачем и для какого результата требуется изменение. UI Exploration отвечает, какие визуальные решения возможны. Resource Graph отвечает, какой системный дизайн-контракт созрел и что реально отрисовал браузер.**

---

## 2. Цели

Система должна позволять одному оператору:

1. увидеть продуктовый gap и его связь с `Job`, `journey`, `capability`, `outcome` и acceptance scenarios;
2. перейти в связанную visual exploration page без создания второго backlog;
3. увидеть актуальный runtime и текущий archetype;
4. проанализировать продуктовую ценность существующих областей страницы;
5. получить несколько параллельных решений на уровнях:
   - page composition;
   - blocks / product patterns;
   - components / variants;
   - states / responsive / interaction;
6. использовать AI-generated UI images как источник как композиционных, так и компонентных идей;
7. собрать комментарии в один крупный iteration package, а не в множество микрозадач;
8. быстро дорабатывать вариант через MCP либо воспроизводимо через plugin path;
9. зафиксировать выбранный дизайн как эталон до реализации;
10. реализовать его кодовым агентом и сравнить browser actual с design reference;
11. после приёмки обновить компоненты, patterns и page archetypes в Resource Graph;
12. видеть календарную историю `before → after` по компонентам, patterns, compositions и archetypes;
13. выполнять всё с минимальным количеством операционных действий.

---

## 3. Не-цели

Система не должна становиться:

- вторым backlog или issue tracker;
- отдельным enterprise workflow engine;
- системой, где один комментарий автоматически превращается в одну задачу;
- автоматическим механизмом публикации любого эксперимента в Resource Graph;
- источником production CSS из Penpot без проверки;
- способом считать красивый AI-image готовым интерфейсным контрактом;
- третьим независимым продуктовым контуром;
- обязательной причиной создавать новый компонент для каждого UI gap;
- заменой функциональных, accessibility и interaction tests одним screenshot diff.

---

## 4. Источники истины и визуальные проекции

### 4.1 GitHub / `events-bot-new`

Хранит канонические:

- product entities и stable IDs;
- decisions и analysis records;
- UI gap identity и связи;
- implementation code;
- component/pattern/archetype contracts;
- fixtures и scenarios;
- acceptance tests;
- release identity;
- visual-reference manifests;
- runtime actual / diff / receipt identity.

### 4.2 Product Atlas file

Показывает:

- user need;
- Job / Job Story;
- user outcome;
- journey;
- capability;
- User Stories / enablers;
- acceptance scenarios;
- product gaps, включая UI gaps;
- implementation/release/runtime state;
- findings и decisions;
- связи с design-system evidence.

Он хранит ссылку на визуальное исследование, но не весь brainstorm.

### 4.3 Resource Graph file

Показывает созревшее состояние дизайн-системы:

- foundations;
- colors, typography, icons;
- accepted components и variants;
- product patterns;
- accepted page archetypes;
- consumer coverage;
- fragmentation;
- runtime `actual / approved baseline / diff`;
- product-value overlays для текущих archetypes;
- promotion review и archive.

### 4.4 UI Exploration file

Показывает незавершённое и исследуемое:

- current runtime и current archetype как baseline;
- product context;
- value-map existing UI;
- текстовые направления;
- AI references и sketches;
- local component candidates;
- block/pattern candidates;
- page composition alternatives;
- archetype impact hypotheses;
- whole iterations;
- selected / parked / rejected alternatives;
- runtime closure после реализации.

### 4.5 `lovekgd-design-system`

Хранит Penpot delivery/review tooling, manifests, catalogs, schemas, agent skills и plugin/MCP contracts. Он не должен становиться второй runtime UI library, расходящейся с `events-bot-new`.

---

## 5. Связь продукта и дизайн-системы

### 5.1 Минимум: product linkage на уровне archetype

Каждый принятый page archetype должен иметь явные ссылки как минимум на:

```yaml
product_links:
  job_ids: []
  outcome_ids: []
  journey_ids: []
  capability_ids: []
  acceptance_scenario_ids: []
```

Пример:

```yaml
archetype_id: archetype.event-detail
product_links:
  job_ids:
    - job.discover-event
  outcome_ids:
    - outcome.user.informed-choice
  journey_ids:
    - journey.catalog
    - journey.search
  capability_ids:
    - capability.event-detail
    - capability.intent-actions
  acceptance_scenario_ids:
    - scenario.event-detail.open
    - scenario.event-detail.registration-open
```

В Resource Graph рядом с archetype должны быть видны:

- поддерживаемый Job;
- затронутый journey step;
- capabilities;
- intended outcome;
- существенные scenarios;
- coverage/evidence status;
- ссылка в Product Atlas.

### 5.2 Patterns и archetype regions

Product pattern или archetype region может иметь прямую более узкую связь:

```yaml
region_id: region.event-detail.primary-actions
supports:
  capability_ids:
    - capability.intent-actions
  outcome_ids:
    - outcome.user.informed-choice
```

### 5.3 Компонент master не обязан иметь один глобальный Job

Generic component вроде `Button/Primary` не должен искусственно привязываться к одному Job. Его продуктовый смысл зависит от consumer context.

Правило:

```text
component master
→ техническая и семантическая роль

component instance / pattern / archetype region
→ product value и product entity links
```

Прямой product link на component master допустим только для product-specialized компонента с устойчивым смыслом, например `EventRegistrationStatus`.

### 5.4 Связь UI gap

UI gap должен содержать:

```yaml
ui_gap_id: UI-GAP-...
product_problem_id: ...
affected_job_ids: []
affected_outcome_ids: []
affected_journey_ids: []
affected_capability_ids: []
affected_archetype_ids: []
affected_region_ids: []
current_evidence_refs: []
exploration_ref: ...
decision_state: ...
```

Product Atlas показывает этот record и deep link на соответствующую page в UI Exploration.

---

## 6. Page composition и page archetype

### 6.1 Различие

```text
page composition
= конкретная исследуемая сборка страницы или состояния

page archetype
= принятый переиспользуемый контракт семейства страниц
```

Composition включает:

- порядок regions;
- blocks и patterns;
- component instances;
- визуальную иерархию;
- responsive layout;
- существенные состояния.

Archetype включает:

- route/page family;
- required и optional regions;
- допустимые patterns;
- variants;
- responsive rules;
- state rules;
- runtime consumers;
- revision identity;
- product links;
- acceptance и evidence.

### 6.2 Пять возможных результатов выбранной composition

```text
existing_archetype_no_change
existing_archetype_revision
existing_archetype_variant
new_archetype_candidate
route_local_composition
```

Выбранный макет не обновляет Resource Graph автоматически. Archetype revision становится accepted только после implementation + runtime review + owner acceptance.

### 6.3 Что показывать в UI Exploration

В каждой whole iteration нужны два поля:

```yaml
integrated_composition:
  id: composition.event-detail.v5
  viewports: [...]

archetype_impact_hypothesis:
  kind: existing_archetype_revision
  target_archetype_id: archetype.event-detail
  summary: add optional registration region
```

На ранней стадии допустимо:

```yaml
archetype_impact_hypothesis:
  kind: unknown
```

---

## 7. Топология UI Exploration

Рекомендуемый отдельный Penpot-файл внутри Design System solution:

```text
00 — Index / active gaps
05 — Recent changes
10 — test gap flow
11+ — реальные UI gap pages
65 — Product value map index
70 — Shared candidate index
80 — Selected for build
89 — Archive
99 — Diagnostics
```

По умолчанию:

> **Одна активная содержательная UI gap-задача = одна Penpot page.**

Отдельный файл на gap нужен только для очень большого, долгого или access-isolated исследования.

---

## 8. Тестовая page `test gap flow`

Первая пилотная page должна называться точно:

```text
10 — test gap flow
```

Она заполняется синтетическими данными и предназначена не для продуктового решения, а для проверки информационной архитектуры и операционного flow.

### 8.1 Тестовый gap

```yaml
ui_gap_id: UI-GAP-TEST-001
title: Полка событий при прокрутке
problem: Пользователь не всегда понимает, что в горизонтальной полке есть продолжение
user_outcome: Быстро увидеть дополнительные варианты без визуального шума
product_links:
  job_ids:
    - job.discover-event
  journey_ids:
    - journey.catalog
  capability_ids:
    - capability.discovery
  outcome_ids:
    - outcome.user.informed-choice
archetype_links:
  - archetype.home
  - archetype.listing
contexts:
  - mobile-390x844
  - desktop-1280x800
```

### 8.2 Горизонталь — whole iterations

```text
CURRENT / BASELINE
→ ITERATION A
→ ITERATION B
→ SHORTLIST
→ SELECTED FOR BUILD
→ RUNTIME REVIEW
```

Каждая колонка — coherent snapshot, а не набор разрозненных fixes.

### 8.3 Вертикаль — параллельные tracks

```text
01 Product context and criteria
02 Current value map
03 Page composition / archetype impact
04 Blocks / product patterns
05 Components / variants
06 Interaction / states / responsive
07 AI images / references / extracted claims
08 Product and technical evaluation
09 Decision / evidence
```

### 8.4 Синтетические объекты

Baseline:

```text
Archetype/Home r3
Pattern/HorizontalShelf P1
Component/EventCard/Compact C1
no explicit continuation control
```

Iteration A:

```text
Composition A1: stronger peek of next card
Pattern P2: fade + partial next card
Component C2: compact media and stronger title rhythm
Control S1: small next affordance
```

Iteration B:

```text
Composition B1: sticky shelf heading
Pattern P3: desktop arrows + mobile peek
Component C3: edge treatment inspired by AI reference
Control S2: keyboard-visible navigation
```

Shortlist:

```text
A2 = conservative geometry
B2 = stronger navigation affordance
```

Selected:

```yaml
selected_composition_id: composition.home.shelf.v4
selected_pattern_id: pattern.horizontal-shelf.p2
selected_component_candidates:
  - candidate.event-card.compact.c2
  - candidate.shelf-next-control.s1
archetype_impact:
  kind: existing_archetype_no_change
  target_archetype_id: archetype.home
```

Runtime Review initially shows placeholders, later:

```text
approved design reference
browser actual mobile
browser actual desktop
diff
functional/keyboard result
owner decision
```

### 8.5 Цель тестовой page

За 5–10 минут без устного объяснения оператор должен понять:

- какой product problem исследуется;
- какие product entities затронуты;
- какой current archetype и runtime являются baseline;
- что менялось на уровне composition, pattern и components;
- какие альтернативы сравниваются;
- почему выбран shortlist;
- меняется ли archetype;
- что требуется реализовать и проверить;
- где history и evidence.

---

## 9. Визуальная карта продуктовой ценности существующего UI

### 9.1 Идея

Над current runtime screenshot или native archetype composition создаётся отдельный annotation layer со stroke-прямоугольниками:

```text
зелёный stroke
жёлтый stroke
красный stroke
```

Они выделяют regions или UI elements и показывают не эстетическую оценку, а состояние продуктовой обоснованности и иерархии.

### 9.2 Семантика цветов

#### Зелёный

```text
value_state: supported
```

Используется, когда:

- есть ясная связь с Job/outcome/capability;
- роль элемента понятна;
- расположение и визуальный приоритет соответствуют ценности;
- нет существенного конфликта с более важным действием;
- есть достаточное evidence либо сильный принятый design rationale.

#### Жёлтый

```text
value_state: uncertain
```

Используется, когда:

- продуктовая ценность вероятна, но не подтверждена;
- элемент частично дублирует другой;
- placement или hierarchy вызывает сомнение;
- value link известен, но текущая форма плохо его реализует;
- данных недостаточно;
- элемент является кандидатом на объединение, перемещение или упрощение.

`unknown` относится к жёлтому, а не автоматически к красному.

#### Красный

```text
value_state: challenged
```

Используется, когда:

- не найдено внятной product-value связи;
- элемент мешает более важному Job/action;
- нарушает иерархию;
- создаёт ложный приоритет;
- дублирует функцию без дополнительной ценности;
- является кандидатом на исключение, скрытие, объединение или радикальное перепроектирование.

Красный **не означает автоматическое удаление**. Он означает `decision required`.

### 9.3 Карточка overlay

```yaml
value_overlay_id: value-map.event-detail.primary-actions
archetype_id: archetype.event-detail
region_id: region.event-detail.primary-actions
element_ref: ...
value_state: supported | uncertain | challenged
supports:
  job_ids: []
  outcome_ids: []
  journey_ids: []
  capability_ids: []
value_claim: ...
evidence_state: sufficient | insufficient | unknown
hierarchy_assessment: correct | weak | conflicting
recommendation: keep | improve | move | merge | test-removal | remove-candidate
ui_gap_ids: []
```

### 9.4 Где это должно жить

**Полная визуальная карта живёт в Design System solution, а не в Product Atlas.**

Причина:

- она накладывается на actual page/archetype;
- использует component, pattern, region и evidence identity;
- относится к визуальной иерархии и UI composition;
- может непосредственно породить UI gaps и candidate changes.

Основное представление:

```text
Resource Graph
65 — Product value maps
```

или overlay mode непосредственно рядом с archetype на `60 — Page archetypes`.

В UI Exploration current-context section импортируется snapshot соответствующей value map для конкретного gap.

Product Atlas хранит агрегированную продуктовую сторону:

```yaml
archetype_value_summary:
  archetype_id: archetype.event-detail
  supported_count: 8
  uncertain_count: 3
  challenged_count: 1
  ui_gap_ids:
    - UI-GAP-...
  value_map_ref: ...
```

Таким образом:

```text
Product Atlas
→ владеет product meaning, проблемой и решением

Design System
→ показывает визуальное соответствие UI product value
```

### 9.5 Ограничения

Value map не должна:

- превращаться в автоматический score интерфейса;
- считать все декоративные элементы бесполезными;
- считать отсутствие аналитических данных отсутствием ценности;
- заменять accessibility review;
- автоматически удалять красные элементы;
- требовать прямой Job-link для generic component master.

---

## 10. Visual brainstorm и experimental components

AI images могут породить идеи на разных уровнях:

```text
page composition
block / product pattern
component anatomy
component visual treatment
state presentation
responsive behavior
interaction affordance
```

Правило:

```text
AI image
→ visual seed
→ extracted claims
→ native LoveKGD candidate
→ integrated page alternative
→ review/testing
```

AI image не является автоматически design reference.

Experimental component lifecycle:

```text
visual seed
→ local candidate in gap
→ used in integrated alternative
→ selected for build/trial
→ implemented and runtime-reviewed
→ accepted local / promotion-ready / rejected / parked
→ optional Resource Graph promotion
```

Candidate первоначально живёт рядом с gap. Shared candidate index — автоматическая сводка, а не обязательное второе место ручной поддержки.

---

## 11. Page `05 — Recent changes`

Страница автоматически показывает material changes:

```text
Сегодня
→ группировка по prompt / iteration package

Вчера и раньше
→ группировка по календарным датам
```

Пример:

```text
Сегодня · 15:42 · ITER-018 · MCP-LOCAL · UI-GAP-TEST-001
Component/EventCard/Compact      C1 → C2
Pattern/HorizontalShelf          P1 → P2
Composition/Home                 V3 → V4
Archetype/Home                   no change

7 августа 2026
Component/Button/Primary         visual-v2 → visual-v3
Archetype/EventDetail            r3 → r4-candidate
```

History строится из change manifests. Оператор ничего не журналирует вручную.

Не показываются как visual changes:

- metadata-only refresh;
- повторное чтение catalog;
- перемещение board без изменения содержания;
- comment-only changes;
- screenshot-renderer noise без подтверждённого design delta.

---

## 12. Plugin-only flow

Этот путь должен быть полностью работоспособным без MCP.

```text
1. Product Atlas → open linked UI gap
2. Design System plugin → Обновить UI gap
3. Operator reviews whole page and comments
4. Design System plugin → Собрать следующую итерацию
5. One agent run creates artifacts + change manifest in Git
6. Design System plugin → Обновить UI gap
7. New whole iteration appears
8. Operator repeats or uses Зафиксировать для сборки
9. Implementation + browser tests
10. Plugin imports runtime evidence and closes/pivots decision
```

### 12.1 Максимум видимых операций UI Exploration mode

1. `Обновить UI gap`
2. `Собрать следующую итерацию`
3. `Зафиксировать для сборки`

Не нужны отдельные действия для:

- создания page;
- импорта каждого компонента;
- регистрации каждой ветки;
- обновления history;
- отправки каждого комментария;
- ручного обновления Product Atlas;
- экспорта каждого файла.

---

## 13. MCP-assisted flow

MCP используется для быстрой отладки и live-итераций.

```text
1. Plugin creates/refreshes safe working root
2. Operator opens exact gap-page
3. Code agent connects to Penpot MCP
4. Agent performs read-only preflight
5. Agent verifies file/page/gap markers and write scope
6. Agent creates or modifies Next Iteration directly in Penpot
7. Operator sees result immediately
8. Several prompts may refine the same working iteration
9. Agent returns MCP session receipt + change manifest
10. Plugin validates and adopts MCP output
11. Зафиксировать для сборки freezes canonical reference
```

### 13.1 Protected zones

```text
PROTECTED
├── Product Atlas context
├── Current runtime / baseline
├── Current accepted archetype
├── Prior iterations
├── Selected for build
├── Runtime evidence
├── Recent changes
└── file/page markers

MCP-WRITABLE
└── Next iteration / working root
    ├── component candidates
    ├── patterns/blocks
    ├── composition alternatives
    ├── states/responsive experiments
    ├── references
    └── temporary annotations
```

MCP не может самостоятельно:

- менять Product Atlas;
- менять Resource Graph current resources;
- изменять protected areas;
- утверждать design reference;
- продвигать candidate;
- писать accepted history;
- удалять прошлые iterations.

### 13.2 Plugin adoption

MCP output остаётся draft, пока plugin не проверит:

- file kind;
- page ID;
- `ui_gap_id`;
- base revision;
- allowed working root;
- protected hashes;
- stable IDs;
- отсутствие shared-library mutation;
- exports;
- change manifest;
- receipt;
- archetype impact;
- required states/viewports.

### 13.3 Local и remote MCP

Для code↔design loop предпочтителен local MCP, если нужен asset round-trip:

```text
Penpot reference export
→ code change
→ Playwright screenshot
→ import actual beside reference
→ compare and iterate
```

Remote MCP может оставаться более простым вариантом canvas manipulation, но не должен считаться эквивалентным local file round-trip без отдельной проверки.

---

## 14. Batch feedback

Нормальная модель:

```text
comments across composition, patterns, components and value map
→ themes and conflicts
→ product + technical analysis
→ one iteration brief
→ one coherent next iteration
```

Запрещённый default:

```text
comment 1 → task 1
comment 2 → task 2
comment 3 → agent call 3
```

Один iteration package может одновременно изменить:

- несколько компонентов;
- один pattern;
- одну composition;
- responsive rule;
- archetype impact hypothesis;
- тестовые scenarios.

Это остаётся одним product/design decision package.

---

## 15. `Selected for build` и design reference

При фиксации plugin создаёт один bundle:

```text
approved-design-reference/
├── reference.svg
├── reference.png
├── penpot.css
├── structure.json
├── scenario.json
├── assets.json
├── change-manifest.json
├── archetype-impact.json
├── approval.json
└── manifest.json
```

Он включает:

- selected composition;
- component/pattern candidate IDs;
- current Resource Graph revision;
- product links;
- value-map decisions;
- viewports;
- states;
- fixtures/content constraints;
- parked/rejected alternatives;
- unresolved assumptions;
- archetype impact;
- approval identity и hashes.

Generated CSS является evidence/hint, а не автоматически принимаемым production code.

---

## 16. Browser implementation и visual tests

До реализации:

```text
Penpot selected state
= approved design reference
```

После реализации:

```text
browser actual
→ geometry comparison
→ perceptual comparison
→ semantic/structural checks
→ functional/accessibility checks
```

### 16.1 Три уровня проверки

#### Geometry

- coordinates;
- width/height;
- spacing;
- alignment;
- line counts;
- region placement.

#### Perceptual

- composition;
- color;
- imagery;
- shadows;
- visual hierarchy;
- contrast distribution.

#### Semantic and functional

- correct component/variant;
- correct tokens;
- correct DOM semantics;
- keyboard/focus;
- loading/error/empty states;
- responsive conditions.

После runtime acceptance появляется:

```text
accepted-runtime-baseline
```

Он не заменяет исходный Penpot design reference.

---

## 17. Тестирование отображения дизайн-системы

### 17.1 Resource Graph display

Должно быть доказано:

- native colors и typography;
- native icons;
- component masters;
- variants;
- product patterns;
- page archetypes assembled from instances;
- product links на уровне archetype;
- product-value maps;
- actual/baseline/diff;
- currentness dimensions;
- coverage/fragmentation;
- comments and prompt routing.

### 17.2 Product linkage display

Для каждого пилотного archetype оператор должен пройти:

```text
Archetype
→ Job
→ Journey
→ Capability
→ Outcome
→ Acceptance scenario
→ Runtime evidence
```

И обратно:

```text
Product Atlas UI gap
→ UI Exploration page
→ selected composition
→ archetype impact
→ implementation/evidence
→ accepted Resource Graph item
```

### 17.3 Value map display

Проверяется:

- overlay aligns with actual UI;
- green/yellow/red semantics ясны;
- каждый overlay имеет rationale;
- product IDs корректны;
- `unknown` не классифицируется автоматически как red;
- red не вызывает auto-delete;
- summary виден в Product Atlas;
- detailed overlay остаётся в Design System.

---

## 18. Acceptance test matrix

### A. Product Atlas linkage

```text
A01 UI gap has stable ID
A02 Job/journey/capability/outcome links render
A03 Design evidence link opens exact archetype/gap context
A04 Decision summary returns after selection/runtime review
A05 Product Atlas does not import speculative component catalog
```

### B. Resource Graph

```text
B01 current components/variants/patterns/archetypes render
B02 archetype product links render
B03 product-value map renders over exact archetype/runtime
B04 actual/baseline/diff links work
B05 candidate objects are separated from accepted current
B06 wrong file/catalog fails closed
```

### C. `test gap flow`

```text
C01 page is named exactly “10 — test gap flow”
C02 synthetic Product Atlas context is visible
C03 current runtime and archetype baseline are visible
C04 current value map is visible
C05 horizontal whole-iteration model is readable
C06 composition/pattern/component tracks are distinct
C07 AI references show extracted claims, not target-design status
C08 shortlist and selected state are explicit
C09 archetype impact is explicit
C10 parked/rejected area is present but lightweight
C11 runtime review placeholder/evidence is present
C12 operator can explain flow without external documentation
```

### D. Plugin-only path

```text
D01 one update creates/refreshes the full gap page
D02 second identical update is noop
D03 comments/manual references survive update
D04 one feedback batch produces one iteration package
D05 one update imports the entire next iteration
D06 Recent changes updates automatically
D07 freeze exports deterministic reference bundle
D08 stale/hash mismatch preserves current state
```

### E. MCP path

```text
E01 read-only preflight succeeds
E02 exact file/page/gap markers are verified
E03 only working root changes
E04 protected roots remain byte/hash equivalent
E05 code agent creates component + pattern + composition coherently
E06 existing components/tokens are reused where required
E07 wrong focused page blocks writes
E08 stale base blocks adoption
E09 disconnect can resume/park/discard safely
E10 session receipt and change manifest are produced
E11 plugin adopts valid MCP output without duplicates
E12 invalid MCP output remains draft and cannot freeze
```

### F. Plugin/MCP parity

```text
F01 both paths use the same stable IDs
F02 both paths produce the same change manifest schema
F03 both paths produce equivalent design-reference bundle semantics
F04 origin channel is recorded
F05 Recent changes does not expose raw unadopted MCP operations as accepted
F06 plugin-only remains full fallback
```

### G. Runtime visual closure

```text
G01 reference exports successfully
G02 deterministic fixture renders
G03 mobile actual captured
G04 desktop actual captured
G05 diff generated
G06 geometry assertions run
G07 visual/perceptual comparison runs
G08 component/variant/token checks run
G09 keyboard/focus tests run where relevant
G10 acceptance updates Resource Graph and Product Atlas links
```

### H. One-operator efficiency

```text
H01 no manual page creation for normal gap
H02 no manual object registration
H03 no manual history logging
H04 no comment-to-issue fan-out
H05 plugin-only iteration requires at most two normal update/review actions plus one agent run
H06 finalization requires one freeze action
H07 MCP-assisted iteration requires one live session and one final adoption/freeze action
H08 operator can review last change set from Recent changes without hunting across pages
```

---

## 19. Pilot sequence

### Phase 0 — static model

- create UI Exploration file;
- create pages;
- populate `10 — test gap flow` with synthetic objects;
- create one synthetic value map;
- verify readability manually.

### Phase 1 — plugin-only

- generate the same page through LoveKGD plugin;
- update twice and prove idempotency;
- add comments;
- build one feedback batch;
- import a second synthetic iteration;
- freeze a design-reference bundle.

### Phase 2 — MCP-assisted

- plugin creates safe working root;
- code agent connects through tested MCP;
- modifies one component, one pattern and one composition;
- produces receipt;
- plugin adopts it;
- verify protected zones and parity.

### Phase 3 — code/runtime

- use selected synthetic reference as implementation target in a test/lab route;
- render via Playwright;
- compare actual/reference;
- import evidence back;
- mark archetype outcome.

### Phase 4 — first real gap

Use a real case such as:

- shelf while scrolling;
- experimental navigation/menu;
- desktop treatment of mobile bottom navigation.

Only after this phase should the proposed contract become ADR.

---

## 20. Success criteria

Пилот успешен, если:

1. один оператор понимает `test gap flow` за 5–10 минут;
2. product linkage виден на archetype и gap page;
3. value map помогает выявить минимум один justified keep, один uncertain и один decision-required element;
4. несколько comments превращаются в один coherent next iteration;
5. plugin-only path полностью работает;
6. MCP реально сокращает feedback loop;
7. MCP не изменяет protected zones;
8. plugin может усыновить MCP output;
9. selected design становится проверяемым browser reference;
10. accepted результат корректно классифицируется как component/pattern/archetype/route-local change;
11. Resource Graph не засоряется всеми экспериментами;
12. Product Atlas получает компактный decision/evidence summary;
13. оператор не ведёт вручную второй backlog, changelog или registry.

---

## 21. Ключевые решения, которые документ уже предлагает

```text
Product Atlas и Design System остаются двумя отдельными Penpot plugins.

UI Exploration — отдельный file kind внутри Design System solution.

Одна содержательная UI gap-задача по умолчанию занимает одну page.

Первая тестовая page называется “10 — test gap flow”.

Product entities видимы в дизайн-системе минимум на уровне archetypes.

Product value component master не выдумывается глобально; value привязывается к instance/pattern/region/archetype context.

Визуальный green/yellow/red value overlay живёт в Design System; Product Atlas получает summary и links.

Composition и archetype различаются.

MCP — быстрый working path, plugin — canonical adoption/freeze path.

Один prompt должен производить одну крупную итерацию, а не множество микрозадач.

History создаётся автоматически из change manifests.

Penpot design reference появляется до кода; browser runtime baseline — после принятой реализации.
```

---

## 22. Открытые вопросы пилота

- Нужна ли отдельная page `65 — Product value maps` или overlay лучше держать рядом с каждым archetype?
- Какой размер gap package остаётся когнитивно целостным?
- Какой порог альтернатив оптимален для разных типов gaps?
- Какие MCP APIs и receipts доступны в реально проверенной конфигурации?
- Можно ли надёжно читать native Penpot comments через MCP, или comments остаются только plugin responsibility?
- Как лучше представлять connection lines между Product Atlas entities и Resource Graph archetypes без визуального шума?
- Какие visual similarity thresholds разумны для Penpot reference versus Chromium actual?
- Когда local candidate должен появляться в shared candidate index?
- Нужно ли сохранять value-map snapshots по каждой accepted archetype revision?
- Какое окно подробной истории оставить в Penpot до сворачивания старых изменений?

Открытые вопросы не должны блокировать Phase 0 и Phase 1.

---

## 23. Связанные контракты и research background

Внутри `events-bot-new`:

- [`docs/product-model/product-atlas-architecture.md`](../../../product-model/product-atlas-architecture.md)
- [`penpot-resource-graph-004.md`](penpot-resource-graph-004.md)
- [`penpot-review-flow.md`](penpot-review-flow.md)
- [`architecture-decision-2026-08-07.md`](architecture-decision-2026-08-07.md)

В `lovekgd-design-system`:

- `docs/resource-graph-004.md`
- `contracts/resource-graph-004.plugin.json`
- `prototypes/penpot-product-atlas-001`

Research synthesis и уточнения находятся в draft PR:

- `onedayonemasterpiece/events-bot-new#354`
- `onedayonemasterpiece/common-analytics#6`

Исследовательская база включает evidence о parallel prototyping, multiple-alternative review, experimental/stable component lifecycle, local/core systems, AI design fixation, design rationale и visual regression. Два исходных deep-research результата сохранены и проиндексированы в соответствующих research PR.

---

## 24. Handoff для следующего окна

Следующий агент должен:

1. считать этот документ основной постановкой;
2. проверить его против текущего `main` обоих репозиториев;
3. не создавать третий LoveKGD plugin;
4. спроектировать/реализовать UI Exploration mode в Design System plugin;
5. создать синтетическую page `10 — test gap flow`;
6. добавить product linkage на archetypes;
7. реализовать либо смоделировать green/yellow/red product-value overlay;
8. поддержать plugin-only и MCP-assisted paths;
9. провести acceptance matrix по разделу 18;
10. вернуть exact Penpot file/manifest identity, screenshots/evidence, run IDs и список фактически пройденных/непройденных gates;
11. не считать research proposal принятым ADR до практического пилота.
