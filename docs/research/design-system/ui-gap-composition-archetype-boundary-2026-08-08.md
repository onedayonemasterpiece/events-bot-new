# UI Exploration: граница между page composition и page archetype

> **Статус:** уточнение к исследовательскому синтезу и synthetic example; proposed pilot contract, не принятый ADR.  
> **Дата:** 8 августа 2026 года.  
> **Контур:** Design System plugin → `UI Exploration` и `Resource Graph`.  
> **Причина уточнения:** исходные документы показывали параллельный track `page composition`, но не объясняли, когда его результат становится обновлением page archetype.

## 1. Короткий ответ

**Page composition и page archetype — связанные, но не одинаковые сущности.**

```text
page composition
= конкретная сборка страницы или состояния в исследуемом варианте

page archetype
= устойчивый переиспользуемый контракт строения семейства страниц
```

`Composition` существует уже на стадии визуального поиска. Она может быть одноразовой, ошибочной, локальной или промежуточной.

`Archetype` появляется только тогда, когда структура признана устойчивой и должна определять семейство реальных страниц: их регионы, слоты, допустимые patterns, responsive rules, состояния и runtime consumers.

Поэтому выбранная composition **часто** приводит к обновлению существующего archetype или созданию нового, но это не должно происходить автоматически для каждого UI gap.

## 2. Точное различие

### 2.1 Page composition

Page composition описывает, как в конкретном исследуемом решении собраны:

- регионы страницы;
- blocks и product patterns;
- экземпляры компонентов;
- порядок и визуальная иерархия;
- размеры, плотность и отношения между областями;
- responsive-поведение;
- состояния и переходы, существенные для общей сборки.

Примеры:

- полка перенесена выше вводного блока;
- sticky header полки накладывается на соседний регион;
- registration card добавлена между facts и related events;
- bottom navigation на desktop заменена компактной floating navigation;
- новый hero собран из существующих компонентов в другом порядке.

Composition может относиться:

- к одной конкретной странице;
- к одному viewport/state;
- к одному альтернативному варианту;
- к будущей структуре, которой ещё нет в runtime.

Она является рабочей design hypothesis.

### 2.2 Page archetype

Page archetype — это уже системный контракт семейства страниц. Он должен отвечать как минимум на вопросы:

- какое семейство routes/pages он описывает;
- какие обязательные и optional regions существуют;
- какие patterns допустимы в каждом регионе;
- какие данные и состояния влияют на composition;
- какие responsive rules являются частью контракта;
- какие варианты archetype существуют и чем они управляются;
- какие runtime consumers используют archetype;
- какая revision принята и каким evidence подтверждена.

Пример:

```text
Archetype/EventDetail
  required:
    Hero
    EventFacts
    PrimaryActions
  optional:
    Registration
    VolunteerCallout
    RelatedShelf
  responsive:
    mobile actions → bottom action region
    desktop actions → hero side region
```

Archetype не обязан быть одним фиксированным скриншотом. Это переиспользуемая модель composition с правилами и вариантами.

## 3. Нормальный жизненный цикл

```text
current archetype + runtime evidence
        ↓
composition alternatives in UI Exploration
        ↓
selected composition
        ↓
classification of archetype impact
        ↓
implementation
        ↓
runtime review
        ↓
accepted archetype revision / variant / new archetype
or accepted route-local composition
        ↓
Resource Graph update where applicable
```

Ключевая граница:

> **Selected composition ещё не является обновлённым archetype.**

Она становится archetype revision только после того, как:

1. определено, что изменение действительно относится к семейству страниц, а не к одному экрану;
2. описаны regions, slots, rules и affected consumers;
3. реализация существует или принята к реализации;
4. runtime review подтвердил, что контракт работает в существенных состояниях и viewport’ах;
5. решение явно принято владельцем продукта.

## 4. Пять возможных результатов composition track

Каждая выбранная composition должна завершаться одним из пяти исходов.

### A. `existing_archetype_no_change`

Композиционная оболочка остаётся прежней. Меняются только:

- component anatomy;
- visual treatment;
- component state;
- внутренний pattern;
- content rule внутри уже существующего slot.

Пример: EventCard стала компактнее внутри уже существующей `RelatedShelf`; структура Event Detail не изменилась.

### B. `existing_archetype_revision`

Меняются устойчивые правила существующего семейства страниц:

- добавляется или перемещается регион;
- меняется обязательность region;
- меняется общий порядок blocks;
- меняется responsive-placement rule;
- изменяется контракт, затрагивающий несколько runtime consumers.

Пример: у всех Event Detail появляется optional `Registration` region между `PrimaryActions` и `EventFacts`.

### C. `existing_archetype_variant`

Базовый archetype остаётся, но появляется управляемый вариант для определённого контекста.

Пример:

```text
Archetype/EventDetail
variant.registration = none | open | closed | registered
```

Variant оправдан, когда структура повторяема и определяется ясным состоянием/типом страницы, а не случайной ручной компоновкой.

### D. `new_archetype_candidate`

Возникает новое повторяемое семейство страниц с самостоятельной структурой и назначением.

Пример:

- Partner Registration Page;
- Festival Hub;
- Focus Group Review Page;
- Search Results with conversational refinement.

До реализации это остаётся `archetype candidate`. После runtime acceptance оно может стать новым принятым archetype в Resource Graph.

### E. `route_local_composition`

Композиция нужна одной особой странице и не должна искусственно обобщаться.

Пример: уникальная юбилейная лендинговая страница или разовая prelaunch-заглушка.

Она может быть принята и реализована, но не обязана становиться общим archetype дизайн-системы.

## 5. Ответ на вопрос «результатом будет обновление archetype?»

### Обычно — да, если UI gap касается структуры существующего семейства страниц

Например:

- изменение общей композиции Event Detail;
- новая navigation region на всех listing pages;
- перестройка home/catalog archetype;
- новое responsive rule, применяемое ко всем consumers archetype.

В таком случае flow должен завершиться:

```text
selected composition
→ proposed archetype revision
→ implementation
→ runtime evidence
→ accepted archetype revision in Resource Graph
```

### Но не всегда

UI gap может закончиться:

- только новой версией компонента;
- новым product pattern внутри существующего slot;
- route-local composition;
- отклонением гипотезы;
- изменением consumer condition без изменения anatomy archetype;
- новым archetype candidate вместо обновления старого.

Поэтому документ и plugin не должны заранее обозначать любую composition как archetype revision.

## 6. Как это должно выглядеть на gap-page

Вертикальный track следует переименовать:

```text
было:
Page / journey composition

должно быть:
Page composition / archetype impact
```

В каждой целой итерации показываются два разных объекта:

1. **Integrated page composition** — что именно видит оператор;
2. **Archetype impact hypothesis** — какой системный результат предполагается.

Пример:

```text
ITERATION B

Integrated composition:
  Event Detail V4 · mobile + desktop

Archetype impact hypothesis:
  existing_archetype_revision
  target: Archetype/EventDetail
  change: add optional Registration region
```

До shortlist поле impact может быть неопределённым:

```text
archetype_impact: unknown
```

Это нормально. Процесс не должен заставлять классифицировать уровень решения до того, как собран целый вариант.

## 7. Что фиксируется в `Selected for build`

Design-reference bundle должен явно содержать classification:

```yaml
composition_outcome:
  kind: existing_archetype_revision
  target_archetype_id: archetype.event-detail
  base_revision: r3
  proposed_revision: r4-candidate
  affected_consumers:
    - route-family:event-detail
  regions_added:
    - registration
  regions_removed: []
  order_changes:
    - registration after primary-actions
  responsive_rules:
    - registration is inline on desktop
    - registration follows facts on mobile
  unresolved_assumptions:
    - whether closed registration keeps the same region
```

Допустимые значения `kind`:

```text
existing_archetype_no_change
existing_archetype_revision
existing_archetype_variant
new_archetype_candidate
route_local_composition
```

Это не отдельная операция оператора. Plugin должен предложить classification автоматически на основе выбранного варианта и связи с текущим Resource Graph. Оператор подтверждает или исправляет её внутри уже существующего действия `Зафиксировать для сборки`.

## 8. Promotion в Resource Graph

### До реализации

В UI Exploration существуют:

- composition candidate;
- possible archetype impact;
- page/state design reference.

Они не изменяют current archetype в Resource Graph.

### После реализации и runtime review

В зависимости от classification:

- `no_change` → обновляются components/patterns/evidence, archetype revision не меняется;
- `revision` → Resource Graph получает новую revision существующего archetype;
- `variant` → Resource Graph получает новый archetype variant и его consumers;
- `new_archetype_candidate` → после acceptance создаётся новый archetype;
- `route_local` → Resource Graph может хранить runtime evidence и route link, но не обязан создавать общий archetype.

Таким образом, Resource Graph остаётся представлением созревшей системы, а UI Exploration — местом, где composition может свободно меняться до принятия.

## 9. История изменений должна показывать archetypes отдельно

На `05 — Recent changes` нельзя оставлять только абстрактное `PageVariant/V4`.

Нужно различать:

```text
composition_candidate
page_variant
archetype_revision_candidate
accepted_archetype_revision
```

Пример сегодняшнего change set:

```text
15:42 · ITER-018 · UI-GAP-022

Component/EventCard/Compact        C1 → C2
Pattern/HorizontalShelf            P1 → P2
Composition/Home/V4                V3 → V4
Archetype/Home                     no change
```

Другой пример:

```text
18:20 · ITER-019 · UI-GAP-031

Composition/EventDetail/V5         V4 → V5
Archetype/EventDetail              r3 → r4-candidate
Region/Registration                new optional region
```

После runtime acceptance:

```text
Archetype/EventDetail              r4-candidate → r4 accepted
runtime: mobile + desktop PASS
```

Это позволяет увидеть не только, что экран визуально изменился, но и было ли изменение локальной пробой или системной ревизией archetype.

## 10. Проверка на трёх типовых кейсах

### Полка при прокрутке

Возможны разные исходы:

- меняются card/control и `HorizontalShelf` pattern, но slot уже существует → archetype без изменений;
- sticky shelf становится новой общей region на Home/Listing → revision существующего archetype;
- появляется специализированная shelf-layout family → archetype variant или новый archetype candidate.

То есть сама работа над полкой не означает автоматического обновления archetype.

### Дублирование mobile bottom navigation на desktop

Наиболее вероятный outcome:

```text
existing_archetype_revision
или
existing_archetype_variant
```

Меняется responsive consumer/region rule. Новый компонент может вообще не появиться.

Если эксперимент отклонён, current archetype остаётся без изменений, а composition переносится в parked/rejected.

### Новая страница регистрации партнёра

Если это одна уникальная страница:

```text
route_local_composition
```

Если это повторяемое семейство registration pages:

```text
new_archetype_candidate
→ implementation
→ accepted Archetype/PartnerRegistration
```

## 11. Связь с Product Atlas

Product Atlas не хранит подробную composition, но после решения получает:

```yaml
ui_gap_id: UI-GAP-031
selected_composition_id: composition.event-detail.v5
composition_outcome: existing_archetype_revision
affected_archetype_ids:
  - archetype.event-detail
implementation_ref: ...
runtime_evidence_ref: ...
```

Так Product Atlas показывает системное следствие решения, не становясь хранилищем макетов.

## 12. Исправленная терминологическая цепочка

```text
UI gap
→ composition alternatives
→ integrated page variant
→ selected composition
→ archetype impact classification
→ implementation
→ runtime review
→ accepted archetype revision / variant / new archetype
  OR accepted route-local composition
```

Главное правило:

> **Composition — материал исследования. Archetype — принятый системный контракт.**

Они могут визуально выглядеть одинаково в финальной колонке, но имеют разный статус, область применения и lifecycle.
