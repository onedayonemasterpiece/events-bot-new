# Product Atlas: Git SoT и MCP-проекция в Penpot

> **Статус:** принятое архитектурное решение v2.  
> **Дата:** 25 августа 2026 года.  
> **Product authority:** `onedayonemasterpiece/events-bot-new`.  
> **UI authority:** `onedayonemasterpiece/lovekgd-design-system`.

## Решение

Product Atlas строится из одной versioned Git-модели:

```text
user need
→ Job / Job Story
→ user outcome
→ journey / recovery path
→ capability
→ User Story / operator job / technical enabler
→ acceptance
→ implementation / release / runtime evidence
→ stable events / measurement questions
→ problems / UI gaps / findings
→ owner outcome / decision
```

Каноническая machine-readable модель находится в:

```text
docs/product-model/atlas/v1/
```

Git-модель допускает human-readable views и отдельную пространственную проекцию в Product Atlas Penpot-файле. Penpot-проекция создаётся только явной scoped-задачей через Penpot MCP после entry gate и обязательного read-back.

**Product Atlas plugin, plugin manifest, plugin namespace, installation URL и background sync больше не являются частью действующей архитектуры.** Историческая реализация остаётся только в Git history.

## Authority

### `events-bot-new`

Владеет user needs, Jobs, outcomes, journeys, capabilities, stories, operator jobs, enablers, guardrails, acceptance, stable events, measurement questions, product problems, UI gaps, findings, decisions и всеми delivery/runtime/outcome facets.

### `lovekgd-design-system`

Владеет foundations, components, patterns, archetypes, ProductScreenStates, Astro/SoT/Penpot bindings и visual/runtime conformance. Репозиторий хранит только typed foreign keys на product IDs и exact UI context, но не копирует определения Jobs, outcomes или decisions.

### `common-analytics`

Может быть methodology/research evidence, но не является каноническим product/runtime input.

## Stakeholder lanes

- `user`;
- `owner_operator`;
- `future_partner` — строго `not_modeled` до отдельного исследования.

Наличие partner-facing route или UI не доказывает partner need, Job или outcome.

## Entity и status contract

Каждая сущность имеет stable ID, kind, title, precise definition, lane, status, confidence, source refs, typed relations, unresolved conflicts, supersession history и независимые facets.

Допустимые semantic statuses:

```text
accepted
source_proven
hypothesis
partial
unresolved
not_modeled
superseded
not_applicable
```

Один status `done` запрещён. Независимо ведутся:

```text
definition
delivery
verification
deployment
runtime_health
evidence
user_outcome
owner_outcome
```

Route, UI, код, тест или deployment не доказывает user/owner outcome.

## User Story boundary

User Story описывает полезный vertical slice с actor/context, observable result, Job/capability/journey/outcome linkage, acceptance, measurement question и evidence. Кнопка, страница, API, таблица, миграция или внутренняя техническая работа оформляется как UI implementation, operator job, technical enabler, guardrail или release deliverable.

## Source lock

Product Atlas v1 фиксирует:

- product `main@821e816b2c8317b1cc5e4b85c5ece72aa27a5c44`;
- corrected UI SoT PR `lovekgd-design-system#50@9b8043f3bdb86fab4eee00bf94b0f10d4f029c50`;
- corrected manifest SHA-256 `ac2cb64bbccb113dd7c81cdb8caec953d3d5e2f56ea10a1f54914d7a0ed46819`;
- planning PR `#39@a2991f8b7cc516d7e80f95057d7b9e21ec81097f`;
- exact file/blob or aggregate-manifest identities.

Позднее решение supersedes ранний source только в затронутой части. Research остаётся evidence, а не автоматически принятой product truth.

## UI linkage

Exact linkage:

```text
product entity
↔ route / route pattern
↔ corrected semantic archetype
↔ semantic region
↔ pattern
↔ configured component instance or runtime boundary
↔ ProductScreenState
↔ acceptance scenario
↔ measurement question
```

Corrected route registry остаётся authority полного inventory: 29 production route patterns, 29 source pages, 32 generated routes и 17 archetypes. Product Atlas не создаёт второй расходящийся route registry.

Generic component master не обязан иметь один Job. Product meaning может принадлежать configured instance, pattern, archetype region или ProductScreenState.

До публичного MCP read-back native binding имеет только `binding_pending`. Координаты, display text и предполагаемые IDs не являются binding evidence.

## Product Atlas Penpot file

Product Atlas использует отдельный Penpot-файл и не размещает product dashboards внутри design-system Resource Graph. Возможная page topology является projection concern, а не Git authority. До MCP read-back file/page/object IDs не записываются.

## MCP materialization

```text
locked reviewed Git SoT
→ validate sources and relations
→ read exact Product Atlas target through MCP
→ bounded dry-run plan
→ scoped MCP mutation
→ exact read-back
→ versioned Git receipt
→ owner review
```

Обязательные правила:

1. Перед каждой mutation проверять exact current file/page.
2. Fail closed при wrong target, source drift или orphan entity.
3. Не выполнять background refresh или mutation при открытии файла.
4. Не читать production DB или raw analytics.
5. Сохранять comments и unrelated objects.
6. Ограничивать изменение выбранной page/zone.
7. Считать delivery доказанным только после read-back.
8. Каждая новая revision требует новой явной MCP-задачи.

## Evidence boundary

Product Atlas принимает reviewed versioned evidence из `events-bot-new`:

```text
MeasurementQuestion
→ evidence
→ finding
→ decision
→ follow-up
```

Raw telemetry, dashboard или hotspot не создаёт автоматически finding, UI gap или decision. Action-map допускается только как reviewed aggregate package; он не изменяет profile/ranking и не переносится в Penpot raw stream.

## Readiness

Currentness показывается отдельно по product model, source lock, acceptance/release evidence, runtime incidents, UI SoT, Penpot bindings и owner review. Generic `CURRENT`/`done` запрещён.

Git-only Product Atlas допустим при stable route/archetype/region IDs, определённом product authority, честном `not_modeled`/`unresolved` и fail-closed relation validation.

Penpot MCP materialization требует accepted Git revision, безопасного target, стабильных UI IDs, reviewed mutation plan и read-back procedure. Git SoT может развиваться до финальной визуальной унификации; Penpot projection не должна выдумывать временные bindings.

## Validation

```bash
python scripts/validate_product_atlas_v1.py
pytest -q tests/test_product_atlas_v1.py
```

Validator проверяет source locks, statuses, lanes, facets, relation closure, User Story boundary, 17 archetypes, acceptance/measurement links, partner `not_modeled`, `binding_pending`, отсутствие `done` и fabricated UUID.

## Supersession

Это решение supersedes активные части документов от 7 августа 2026 года, которые требовали Product Atlas plugin или plugin delivery. Исторические commits и receipts остаются доступны в Git history, но не являются действующей инструкцией.