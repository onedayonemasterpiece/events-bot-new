# UI Exploration: два канала интеграции — LoveKGD plugin и Penpot MCP

> **Статус:** архитектура пилота и acceptance contract; не принятый implementation ADR.  
> **Дата:** 8 августа 2026 года.  
> **Контур:** Design System solution → `Resource Graph` + `UI Exploration`.  
> **Цель:** обеспечить два взаимозаменяемых способа работы с одной UI gap-page: детерминированный plugin path и быстрый agent-driven MCP path.  
> **Главное ограничение:** у одного оператора должно быть минимум действий; MCP не становится новым источником истины или третьим LoveKGD-плагином.

## 1. Короткое решение

LoveKGD должен поддерживать **два канала работы с одной и той же UI Exploration model**:

```text
A. LoveKGD Design System plugin
   deterministic batch / sync / review / freeze

B. Official Penpot MCP + LoveKGD agent skill
   live inspect / create / modify / code↔design debugging
```

Они не являются двумя независимыми дизайн-системами.

```text
shared UI gap contract
+ shared stable IDs
+ shared iteration/change manifests
+ shared protected zones
+ shared final design-reference bundle
```

Разница только в способе создания следующей working iteration:

```text
plugin path:
Git/agent artifacts → plugin reconciliation → Penpot

MCP path:
code agent → live Penpot working zone → adoption by plugin
```

Каноническая граница:

> **MCP создаёт и отлаживает working design. LoveKGD plugin проверяет, усыновляет, фиксирует и публикует результат.**

MCP не может самостоятельно:

- изменить принятый Resource Graph;
- пометить design reference как approved;
- повысить component candidate;
- переписать `Current`, прошлые итерации или `Recent changes`;
- заменить plugin currentness/recovery/hash contract;
- менять Product Atlas.

## 2. Что подтверждают текущие источники

### 2.1 Официальный Penpot MCP

Официальный MCP умеет читать и менять структуру открытого Penpot-файла: components, styles, tokens, pages и layers. Среди официально заявленных сценариев — создание экранов из существующей дизайн-системы, генерация variants, аудит системы, извлечение layout metadata, design-to-code, обновление frontend styles и code-to-design.

Текущий MCP работает через три части:

```text
MCP client / code agent
→ MCP server
→ MCP connector/plugin inside the open Penpot file
```

Критические runtime-свойства:

- MCP действует на **currently focused page**;
- MCP активен только в одной выбранной browser tab;
- agent может выполнять write operations: создавать, перемещать, переименовывать, удалять и restyle объекты;
- официальный guidance рекомендует начинать read-only, описывать plan до изменений и делать небольшие обратимые шаги;
- remote MCP не имеет privileged local filesystem access;
- local MCP предоставляет более полный asset round-trip, включая `import_image` и полноценный `export_shape` в локальный контур.

Официальные источники:

- <https://help.penpot.app/mcp/>
- <https://github.com/penpot/penpot/tree/develop/mcp>
- <https://github.com/penpot/penpot-ai-kit>

### 2.2 Официальный Penpot AI Kit как полезный precedent

Penpot AI Kit поверх MCP использует:

- agent instructions и reusable skills;
- режимы `suggest / review / autofix`;
- approval checkpoints;
- preview/export перед существенными изменениями;
- предпочтение существующих tokens/components;
- golden evals;
- отдельные workflows для `brief-to-screen`, `code-to-penpot-sync` и `design-to-code-review`.

LoveKGD не должен копировать kit целиком. Но он подтверждает правильность идеи:

```text
generic MCP
+ project-specific agent rules
+ protected write scope
+ checkpoint/preview
+ evals
```

Без LoveKGD-specific skill обычный MCP-клиент слишком свободен для управляемого Resource Graph/UI Exploration процесса.

### 2.3 Граница из глубокого исследования

Evidence-led отчёт рекомендует MCP для candidate prototyping и manipulation design structure, но не как канонический sync mechanism. Второй отчёт предлагает MCP намного агрессивнее, включая прямую корректировку design tokens и code generation; эти идеи полезны как hypotheses, но часть утверждений должна быть проверена техническим пилотом.

Следовательно, пилот должен доказать не только «MCP умеет рисовать», но и:

- корректно ли он соблюдает LoveKGD file/page boundaries;
- можно ли воспроизводимо получить change receipt;
- может ли plugin безопасно усыновить MCP output;
- действительно ли уменьшается число действий оператора;
- остаётся ли plugin-only path полноценным fallback.

## 3. Роли двух каналов

| Область | LoveKGD plugin | Penpot MCP + agent |
|---|---|---|
| Основная роль | control plane и deterministic transport | live working plane |
| Granularity | цельный change/iteration package | небольшие live mutations внутри одной session |
| Источник | immutable catalog / Git artifacts | focused Penpot page + repository context |
| Оптимальный use case | sync, currentness, review, freeze, history, evidence, promotion | быстрый visual iteration, component/layout debugging, code↔design loop |
| Mutation scope | allowlisted managed objects/files | только designated working zone одной gap-page |
| Determinism | высокий: IDs, hashes, idempotency, fail-closed | ниже: зависит от model/tool execution |
| Audit | встроенный catalog/change manifest | обязательный MCP session receipt, затем plugin adoption |
| Работа без live agent | да | нет |
| Требует active Penpot tab/page | plugin session | всегда; focused page является MCP context |
| Публикация в Resource Graph | после explicit acceptance | запрещена напрямую |
| Approved design reference | фиксирует plugin | может подготовить, но не утверждает |
| Fallback | основной надёжный путь | при сбое возвращается к plugin path |

## 4. Общая модель данных

Оба канала обязаны использовать один контракт.

### 4.1 Stable identity

```yaml
file_kind: ui-exploration
ui_gap_id: UI-GAP-022
page_id: penpot-page-id
iteration_id: ITER-004
change_set_id: CHANGE-004
base_resource_graph_revision: rg-004a.3
base_catalog_sha256: ...
working_root_id: managed.ui-gap-022.iter-004.working
```

### 4.2 Object identity

Каждый material object получает стабильный ID:

```text
component_candidate
pattern_candidate
composition_candidate
page_variant
archetype_revision_candidate
reference_asset
runtime_actual
```

Пример:

```yaml
object_id: candidate.event-card.compact.c2
object_kind: component_candidate
ui_gap_id: UI-GAP-022
parent_object_id: candidate.event-card.compact.c1
origin_channel: mcp
```

### 4.3 Общий change manifest

Независимо от канала финальная итерация должна вернуть:

```json
{
  "schema_version": "lovekgd-ui-exploration-change-set-v1",
  "change_set_id": "CHANGE-004",
  "ui_gap_id": "UI-GAP-022",
  "iteration_id": "ITER-004",
  "origin": {
    "channel": "plugin-or-mcp",
    "session_id": "optional-mcp-session-id",
    "prompt_id": "prompt-004",
    "agent_client": "optional",
    "model": "optional"
  },
  "base": {
    "resource_graph_revision": "rg-004a.3",
    "catalog_sha256": "...",
    "page_marker_sha256": "..."
  },
  "changes": [],
  "exports": {},
  "archetype_impact": {},
  "validation": {},
  "receipt_sha256": "..."
}
```

`origin.channel` допускает:

```text
plugin
mcp-local
mcp-remote
```

Всё остальное имеет одинаковую семантику.

## 5. Protected zones и allowed MCP scope

UI Exploration gap-page должна содержать управляемые зоны:

```text
PROTECTED
├── Context / Product Atlas link
├── Current runtime / baseline
├── Prior accepted iterations
├── Selected for build
├── Runtime evidence
└── page/file markers

MCP-WRITABLE
└── Next iteration / working root
    ├── component candidates
    ├── pattern/block candidates
    ├── composition alternatives
    ├── behavior/state experiments
    ├── imported references
    └── temporary annotations
```

Plugin при обычном `Обновить UI gap` пишет в page marker:

```yaml
mcp_policy:
  enabled: true
  allowed_write_root_id: managed.ui-gap-022.next-working
  protected_root_ids:
    - managed.ui-gap-022.context
    - managed.ui-gap-022.current
    - managed.ui-gap-022.selected
    - managed.ui-gap-022.runtime
  expected_ui_gap_id: UI-GAP-022
  expected_page_id: ...
  base_revision: ...
```

MCP agent обязан читать marker перед каждым write batch.

Если focused page изменилась, `ui_gap_id` не совпадает или marker устарел, агент завершает операцию без mutation.

## 6. Plugin-only path

Этот путь остаётся обязательным и полностью работоспособным без MCP.

```text
1. Product Atlas → ссылка на UI gap
2. Оператор открывает UI Exploration page
3. Design System plugin: «Обновить UI gap»
4. Оператор смотрит текущую итерацию и оставляет comments
5. Plugin: «Собрать следующую итерацию»
6. Один prompt / agent run создаёт artifacts + change manifest в Git
7. Plugin: «Обновить UI gap»
8. Оператор получает новую цельную iteration
9. Plugin: «Зафиксировать для сборки»
10. Implementation + runtime evidence
```

### 6.1 Сильные стороны

- воспроизводимость;
- idempotent update;
- строгая provenance;
- безопасное массовое обновление;
- сохранение comments и review snapshots;
- устойчивость к смене модели/agent client;
- возможность повторить update из одного catalog;
- пригодность для Resource Graph promotion и evidence.

### 6.2 Ограничение

Между агентским изменением и визуальным просмотром есть Git/artifact/plugin round-trip. Для нескольких мелких геометрических корректировок это медленнее живой MCP-сессии.

## 7. MCP-assisted path

MCP path не заменяет plugin flow. Он сокращает внутреннюю отладочную петлю между initial update и final freeze.

```text
1. Plugin «Обновить UI gap» создаёт/освежает safe working root
2. Оператор открывает точную gap-page и активирует Penpot MCP
3. Code agent выполняет read-only preflight
4. Agent показывает plan и target write scope
5. Agent создаёт новую working iteration, не трогая previous/current
6. Оператор смотрит изменения сразу в Penpot
7. Последующие prompts меняют эту же working iteration
8. Agent генерирует MCP session receipt + change manifest
9. Plugin проверяет и усыновляет MCP output
10. «Зафиксировать для сборки» создаёт canonical design-reference bundle
```

### 7.1 Что MCP делает хорошо

- быстро создаёт несколько variants из существующих компонентов;
- корректирует flex/grid, spacing, hierarchy и responsive composition;
- создаёт локальные component candidates;
- переносит идеи из code/runtime в Penpot;
- извлекает structure/tokens/layout для code agent;
- показывает intermediate visual result без отдельного plugin import;
- позволяет кодовому агенту одновременно видеть repository implementation и design structure.

### 7.2 Что MCP не делает в v1

- не читает Product Atlas напрямую как источник требований без подготовленного context block;
- не полагается на native Penpot comments как гарантированно доступный MCP API;
- не обновляет `05 — Recent changes` напрямую;
- не закрывает comments;
- не меняет shared Resource Graph resources;
- не фиксирует approval;
- не создаёт GitHub microtasks;
- не удаляет старые iterations.

Native comments продолжает агрегировать LoveKGD plugin. MCP получает уже собранный iteration brief либо explicit operator prompt.

## 8. Быстрый code↔design debugging loop

Главный выигрыш MCP появляется, когда один code agent имеет одновременно:

```text
repository checkout
+ Playwright/browser preview
+ Penpot MCP
+ focused UI gap page
```

Рекомендуемый loop:

```text
selected/working Penpot composition
→ MCP inspect: components, tokens, geometry
→ agent changes frontend code
→ Playwright renders actual
→ agent compares actual with design reference
→ local MCP imports actual screenshot beside working reference
→ agent corrects code or working design
→ repeat inside one session
→ plugin freeze/adoption
```

### 8.1 Почему local MCP предпочтителен для этого loop

Official local MCP предоставляет local filesystem-oriented capabilities, включая полноценный `export_shape` и `import_image`. Это позволяет одному агенту:

- экспортировать reference PNG/SVG;
- читать локальный build;
- создавать Playwright screenshots;
- импортировать actual обратно в Penpot;
- сохранять receipt рядом с Git changes.

Remote MCP проще подключить, но:

- не читает локальные пути;
- не импортирует изображения с local path;
- экспорт ограничен по сравнению с local mode.

Поэтому rollout:

```text
primary code-agent pilot: local MCP
fallback/smoke: remote MCP
```

Remote MCP остаётся полезным для live canvas editing без local asset round-trip.

## 9. LoveKGD MCP agent skill

Нельзя полагаться на произвольный natural-language prompt. Нужен versioned project skill, хранящийся в `lovekgd-design-system` и подключаемый к code agent.

Предлагаемая структура после принятия ADR:

```text
agent-skills/penpot-ui-exploration/
├── SKILL.md
├── policies/
│   ├── protected-zones.json
│   ├── modes.json
│   └── receipt.schema.json
├── workflows/
│   ├── inspect-gap.md
│   ├── build-next-iteration.md
│   ├── code-design-debug-loop.md
│   └── resume-session.md
└── evals/
    ├── read-only-preflight.json
    ├── wrong-page-refusal.json
    ├── protected-zone-refusal.json
    └── receipt-parity.json
```

### 9.1 Обязательные правила skill

1. **Read before write.**
2. Проверять `file_kind`, `page_id`, `ui_gap_id`, base revision и `allowed_write_root_id`.
3. Перед mutation показывать compact plan.
4. Создавать новую iteration append-only.
5. Не изменять protected roots.
6. Использовать current tokens/components прежде hardcoded values.
7. Не promote local candidate.
8. Делать preview/checkpoint после material batch.
9. Возвращать machine-readable receipt.
10. При потере connection оставлять recoverable session marker.
11. Перед каждым следующим write повторно проверять focused page marker.
12. Не считать визуальное сходство функциональной/accessibility приёмкой.

### 9.2 Режимы

```text
inspect
  read-only; анализ и план

working
  запись только в MCP working root

code-design-debug
  working root + local exports/imports + repo changes

adoption-ready
  no further mutation; receipt complete; waiting for plugin
```

MCP agent не получает режим `promote`.

## 10. Session and concurrency model

Официальный MCP следует focused page и active tab. Поэтому одновременная запись plugin и MCP недопустима.

### 10.1 One-writer rule

```text
at any moment:
LoveKGD plugin writer XOR MCP writer
```

Plugin может быть открыт для read-only status, но update/freeze не запускается во время active MCP session.

### 10.2 MCP session marker

```yaml
mcp_session:
  session_id: MCP-20260808-004
  status: active
  ui_gap_id: UI-GAP-022
  page_id: ...
  working_root_id: ...
  base_revision: ...
  started_at: ...
  last_checkpoint_at: ...
```

Завершение:

```text
active
→ adoption_ready
→ adopted
```

или:

```text
active
→ interrupted
→ resumed | discarded | parked
```

### 10.3 Stale-base rule

Если plugin обновил base Resource Graph/catalog после начала MCP session:

```text
session base != current base
→ no automatic adoption
→ agent/plugin produce rebase plan
```

Старый working design сохраняется; он не исчезает.

## 11. Plugin adoption of MCP output

MCP session считается только draft до plugin validation.

Plugin проверяет:

- correct file kind/page/gap;
- only allowed root changed;
- protected roots unchanged by hash;
- stable IDs unique;
- base revision current;
- no shared-library mutation;
- required reference exports exist;
- receipt and change manifest complete;
- archetype impact classification present;
- no missing material state/viewports declared by selected variant.

После проверки plugin:

```text
MCP working iteration
→ adopted iteration snapshot
→ Recent changes projection
→ selectable for build
```

Если проверка не прошла:

```text
working iteration remains visible
+ adoption report explains violations
+ current/selected state unchanged
```

## 12. `05 — Recent changes` для обоих каналов

History page показывает единый timeline, но указывает origin:

```text
15:42 · ITER-018 · MCP-LOCAL · prompt-018
Component/EventCard/Compact    C1 → C2
Pattern/HorizontalShelf        P1 → P2
Composition/Home               V3 → V4
Archetype/Home                 no change

12:10 · ITER-017 · PLUGIN · catalog 92ab…
Component/Button/Primary       r2 → r3
```

Правила:

- raw MCP mutations до adoption не попадают в canonical history;
- внутри gap-page может отображаться temporary session delta;
- после adoption plugin строит normal before/after cards;
- today grouping остаётся по prompt/change set/session;
- older grouping остаётся календарным;
- origin channel является provenance, а не отдельным workflow.

## 13. Operator workflow comparison

### 13.1 Plugin-only normal iteration

```text
review + comments
→ «Собрать следующую итерацию»
→ один agent run
→ «Обновить UI gap»
```

Финал:

```text
«Зафиксировать для сборки»
```

### 13.2 MCP-assisted iteration

После one-time setup MCP client:

```text
открыть gap-page + connect MCP
→ natural-language iteration with code agent
→ immediate Penpot review
→ plugin «Зафиксировать для сборки»
```

При нескольких корректировках MCP экономит повторные:

```text
export prompt
→ external artifact generation
→ commit
→ plugin update
```

Но подключение MCP само по себе является session overhead. Поэтому MCP не обязан использоваться для маленького однократного gap, который plugin-only закрывает одним batch.

## 14. Когда выбирать какой канал

### Plugin path по умолчанию

- full Resource Graph refresh;
- large generated update;
- комментарии → systemic prompt;
- изменение уже принято и нужна воспроизводимость;
- работа идёт без live Penpot session;
- импорт runtime actual/baseline/diff;
- history/currentness/recovery;
- promotion и final freeze.

### MCP path

- требуется быстро попробовать 2–4 визуальные версии;
- нужно несколько раз поправить геометрию/иерархию;
- code agent одновременно меняет frontend;
- требуется code-to-Penpot или Penpot-to-code inspection;
- нужно быстро проверить новый component/pattern in situ;
- нужно импортировать локальные screenshots в live loop;
- вариант ещё experimental и обратим.

### Не использовать MCP

- для массового обновления всего Resource Graph;
- для автоматического promotion;
- для unattended long-running mutation;
- когда невозможно удерживать правильную focused page/tab;
- когда нет свежего safe working root;
- когда base revision неизвестна;
- когда нужен только read-only report, доступный из Git/catalog.

## 15. Acceptance test matrix

Пилот должен тестировать не только визуальную успешность, но и границы.

### 15.1 Shared fixture

Создать synthetic UI gap:

```text
UI-GAP-MCP-001
Current archetype: Archetype/EventDetail r3
Existing component: Button/Primary
Local candidate: RegistrationPanel/C1
Protected current/selected/runtime zones
Empty MCP working root
```

Один и тот же logical brief выполняется plugin path и MCP path.

### 15.2 Plugin tests

| ID | Test | Expected |
|---|---|---|
| P01 | create/update gap from catalog | exact page/IDs created |
| P02 | second identical update | noop |
| P03 | comments/manual references present | preserved |
| P04 | import one whole iteration package | one coherent new column |
| P05 | update Recent changes | before/after + prompt/change set |
| P06 | freeze selected composition | deterministic design-reference bundle |
| P07 | wrong file kind | fail closed |
| P08 | stale/hash mismatch | current state unchanged |

### 15.3 MCP tests

| ID | Test | Expected |
|---|---|---|
| M01 | read-only overview | correct file/page/gap/resources |
| M02 | create one next iteration | only allowed root changed |
| M03 | use existing component/tokens | no detached hardcoded substitute without explicit reason |
| M04 | create local component candidate | remains gap-local |
| M05 | alter composition + candidate in one prompt | coherent iteration, not detached fixes |
| M06 | focused page switched before write | agent refuses mutation |
| M07 | attempt protected-zone edit | refusal; protected hashes unchanged |
| M08 | disconnect mid-session | prior iterations intact; session resumable/discardable |
| M09 | plugin updates base during session | adoption refused as stale |
| M10 | generate receipt | complete machine-readable manifest |
| M11 | local export/import actual screenshot | successful code↔design round-trip |
| M12 | remote mode local-path import attempt | explicit unsupported result; no silent failure |

### 15.4 Adoption/parity tests

| ID | Test | Expected |
|---|---|---|
| A01 | plugin adopts valid MCP session | one adopted iteration, no duplicate objects |
| A02 | invalid receipt | adoption blocked |
| A03 | protected hash changed | adoption blocked; current unchanged |
| A04 | plugin vs MCP logical result | same bundle schema and stable IDs |
| A05 | history projection | correct origin channel and before/after |
| A06 | final archetype impact | same classification semantics |
| A07 | fallback | MCP output can be exported to Git and re-imported plugin-only |

### 15.5 Product scenarios

Проверить на трёх реальных типах gaps:

1. experimental menu — MCP быстро создаёт/отменяет варианты;
2. scrolling shelf — MCP параллельно меняет pattern, component и composition;
3. desktop bottom navigation — MCP помогает отладить responsive rule без обязательного нового component.

## 16. Success metrics

Пилот собирает:

```text
operator actions per accepted iteration
manual copy/paste operations
prompt-to-first-visible-change latency
number of plugin syncs per iteration
number of untracked mutations
wrong-page/protected-zone incidents
recovery success after interruption
MCP session adoption success rate
plugin/MCP bundle parity
visual/functional/a11y test result
```

Acceptance gates:

```text
0 protected-zone mutations
0 silent wrong-page writes
0 untracked accepted changes
100% accepted MCP sessions have valid receipts
100% final references pass plugin validation
plugin-only path remains complete fallback
```

MCP считается полезным fast path, только если он действительно сокращает действия/итерации без снижения traceability.

## 17. Failure and fallback semantics

Любой MCP failure приводит не к потере работы, а к одному из исходов:

```text
resume MCP session
park working iteration
export change package to Git
continue through plugin-only path
```

MCP failure не должен:

- повреждать current/selected state;
- блокировать plugin update;
- удалять previous iteration;
- менять Product Atlas decision;
- создавать partial Resource Graph promotion.

Plugin остаётся recovery authority.

## 18. Связь с composition/archetype lifecycle

Оба канала создают composition candidates, но только plugin freeze/adoption фиксирует один из outcomes:

```text
existing_archetype_no_change
existing_archetype_revision
existing_archetype_variant
new_archetype_candidate
route_local_composition
```

MCP может предложить classification, но не утвердить её.

После implementation/runtime review plugin обновляет Resource Graph согласно принятому outcome.

## 19. План реализации после принятия пилота

### Phase 0 — capability probe

- подтвердить Penpot version;
- проверить official remote/local MCP availability;
- проверить coexistence/sequential use с LoveKGD plugin;
- зафиксировать actual tool set;
- отдельно проверить comment access, не предполагая его наличие.

### Phase 1 — LoveKGD MCP skill

- read-only preflight;
- file/page marker validation;
- allowed working root;
- plan/preview/checkpoints;
- receipt generation;
- wrong-page/protected-zone evals.

### Phase 2 — plugin adoption

- MCP session marker recognition;
- protected hash verification;
- change manifest validation;
- adopted iteration snapshot;
- Recent changes origin channel.

### Phase 3 — local code↔design loop

- export selected reference;
- run local preview/Playwright;
- import actual;
- side-by-side/diff evidence;
- one code agent session.

### Phase 4 — three-case pilot

- menu;
- scrolling shelf;
- desktop navigation.

### Phase 5 — ADR

Только после evidence пилота решить:

- является ли local MCP рекомендуемым default для active debugging;
- нужен ли remote MCP как supported fallback;
- какие operations можно считать safe auto-fix;
- стоит ли расширять MCP write scope;
- как version/pin Penpot AI Kit или собственный LoveKGD skill.

## 20. Итог

```text
Plugin
= надёжный batch transport, validation, adoption, history, approval, promotion

MCP
= быстрый live editor и code↔design debugger внутри одной safe working zone
```

Они сходятся через один manifest/receipt contract.

Оператор не выбирает между двумя несовместимыми процессами. Он выбирает скорость внутренней итерации:

```text
нужен один batch → plugin-only
нужна живая многократная отладка → MCP
```

Финал всегда один:

```text
plugin validation
→ selected design reference
→ implementation/runtime evidence
→ accepted component/pattern/archetype update where applicable
```
