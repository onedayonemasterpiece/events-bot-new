# Синтетический пример UI Exploration и истории изменений компонентов

> **Статус:** дополнение к исследовательскому синтезу; proposed pilot contract, не принятый ADR.  
> **Дата:** 8 августа 2026 года.  
> **Контур:** Design System plugin → отдельный Penpot-файл `UI Exploration`.  
> **Связь:** UI gaps и продуктовый контекст приходят из Product Atlas по стабильному `ui_gap_id`.

## 1. Зачем нужен визуальный пример

Абстрактное описание `one page per gap` недостаточно, чтобы проверить удобство модели. До реализации плагина полезно увидеть на синтетических данных сразу два представления:

1. **gap-page как визуальную матрицу**, где параллельно развиваются page composition, blocks/patterns и component candidates;
2. **календарную историю изменений компонентов**, где видно, что изменилось сегодня, каким prompt/iteration package это было сделано и как выглядело `before → after`.

Пример не фиксирует окончательный визуальный стиль Penpot-файла. Он проверяет информационную архитектуру, плотность, порядок чтения и требуемое число действий оператора.

## 2. Новая страница: `05 — Recent changes`

В UI Exploration file рекомендуется добавить автоматически формируемую страницу:

```text
00 — Index / active gaps
05 — Recent changes
10+ — одна page на активный UI gap
70 — Shared candidate index
80 — Selected for build
89 — Archive
99 — Diagnostics
```

`05 — Recent changes` — не второй Git history и не ручной changelog. Это визуальная проекция iteration/change manifests.

Она должна отвечать на четыре вопроса:

1. что визуально изменилось;
2. когда это произошло;
3. в рамках какого gap и какого общего prompt;
4. принято ли изменение, остаётся ли experimental или уже отклонено.

### Почему страница полезна

Для одного оператора она становится начальной поверхностью свежего review:

```text
открыть Recent changes
→ увидеть все изменения последнего agent run
→ открыть затронутый gap только при необходимости
→ оставить системные комментарии
→ собрать следующую итерацию одним prompt
```

Она особенно полезна, когда один пакет одновременно меняет несколько связанных объектов:

- компонент;
- блок/pattern;
- композицию страницы;
- responsive-state;
- integrated page variant.

Без агрегированного представления оператору пришлось бы вручную искать изменения по нескольким gap-pages.

## 3. Календарная группировка

### Сегодня

Сегодня изменения группируются прежде всего **по iteration package / prompt**, потому что оператор помнит недавние запуски и оценивает их как единое решение.

```text
Сегодня

23:42 · ITER-004 · GAP-022 · «Пересобрать полку и управление прокруткой»
  EventCard/Compact       before | after
  ShelfNextControl/C1     before | after
  ShelfPattern/P2         before | after
  PageVariant/V4          before | after

20:15 · ITER-003 · GAP-023 · «Проверить desktop navigation»
  DesktopQuickNav/B2      before | after
  Home/Desktop/P3         before | after
```

### Вчера и недавнее прошлое

Вчера и более ранние изменения группируются по календарной дате. Prompt остаётся ссылкой/provenance, но перестаёт быть главным визуальным заголовком.

```text
7 августа 2026
  Button/Primary        visual-v2 → visual-v3
  BottomNav             mobile-only → responsive-experiment

6 августа 2026
  EventCard/Default     spacing-v1 → spacing-v2
```

### Старое

После настраиваемого периода, например 14 или 30 дней, подробные карточки можно сворачивать до дневной/недельной группы. История не удаляется из Git; визуальная страница остаётся быстрой.

## 4. Карточка одного изменения

Минимальная карточка:

```text
┌──────────────────────────────────────────────────────────────────┐
│ EventCard / Compact                     EXPERIMENTAL · GAP-022    │
│                                                                  │
│  BEFORE                     →          AFTER                     │
│  [thumbnail]                           [thumbnail]                │
│                                                                  │
│  Изменено: media ratio, title rhythm, edge treatment             │
│  Причина: следующая карточка должна лучше читаться как продолжение│
│  ITER-004 · prompt-004 · 23:42 · open gap                        │
└──────────────────────────────────────────────────────────────────┘
```

Обязательный минимум данных:

- stable component/candidate/pattern ID;
- `before_ref` и `after_ref`;
- `ui_gap_id`;
- iteration/prompt/change-set ID;
- время;
- короткий смысловой summary;
- состояние: `experimental`, `selected`, `accepted`, `parked`, `rejected`, `promoted`;
- ссылка на gap-page;
- при наличии — Git SHA, PR, runtime evidence.

Не показываются как отдельные визуальные изменения:

- metadata-only refresh;
- перенос board без изменения содержимого;
- перечитывание каталога;
- изменение комментариев;
- шум screenshot renderer без подтверждённого design delta.

## 5. Источник данных: change manifest, а не ручной ввод

Оператор не должен самостоятельно заполнять историю. Каждый крупный agent iteration package возвращает компактный manifest:

```json
{
  "schema_version": "lovekgd-ui-exploration-change-set-v1",
  "change_set_id": "ITER-004",
  "occurred_at": "2026-08-08T23:42:00+02:00",
  "ui_gap_id": "UI-GAP-022",
  "prompt_id": "prompt-004",
  "summary": "Пересобрать полку и управление прокруткой",
  "changes": [
    {
      "object_id": "candidate.event-card.compact.c2",
      "object_kind": "component_candidate",
      "before_ref": "iteration-b/event-card-compact-c1",
      "after_ref": "iteration-c/event-card-compact-c2",
      "change_types": ["geometry", "typography", "visual_treatment"],
      "summary": "Уменьшена высота media, усилен title rhythm и добавлен edge treatment",
      "state": "experimental"
    },
    {
      "object_id": "candidate.shelf-next-control.c1",
      "object_kind": "component_candidate",
      "before_ref": null,
      "after_ref": "iteration-c/shelf-next-control-c1",
      "change_types": ["new_object", "interaction_state"],
      "summary": "Добавлен компактный контрол продолжения полки",
      "state": "experimental"
    },
    {
      "object_id": "pattern.horizontal-shelf.p2",
      "object_kind": "pattern_candidate",
      "before_ref": "iteration-b/horizontal-shelf-p1",
      "after_ref": "iteration-c/horizontal-shelf-p2",
      "change_types": ["composition", "responsive_behavior"],
      "summary": "Изменены peek, edge fade и desktop controls",
      "state": "selected"
    }
  ]
}
```

Плагин строит `Recent changes` из этих данных при обычной операции `Обновить UI gap` или при общем обновлении Exploration file. Отдельной кнопки «обновить историю» не требуется.

## 6. Синтетическая gap-page

Пример использует вымышленный, но реалистичный gap:

```yaml
ui_gap_id: UI-GAP-022
title: Полка при прокрутке
problem: Пользователь не всегда понимает, что в горизонтальной полке есть продолжение
user_outcome: Быстро увидеть дополнительные события без визуального шума
contexts:
  - mobile-390x844
  - desktop-1280x800
constraints:
  - сохранить текущую EventCard как baseline
  - не полагаться только на hover
  - keyboard controls должны оставаться доступными
```

### Горизонталь — цельные итерации

```text
CURRENT
→ ITERATION A
→ ITERATION B
→ SHORTLIST
→ SELECTED FOR BUILD
→ RUNTIME REVIEW
```

Каждая колонка является snapshot целой системы, а не набором несвязанных задач.

### Вертикаль — параллельные треки

```text
01 Product context and criteria
02 Page composition
03 Blocks / product patterns
04 Component candidates
05 Interaction / states / responsive
06 AI images / references / extracted claims
07 Product and technical evaluation
```

### Синтетическое наполнение

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ UI-GAP-022 · Полка при прокрутке                                                                            │
├──────────────────────┬──────────────────┬──────────────────┬──────────────────┬──────────────────┬─────────────┤
│ CURRENT              │ ITERATION A      │ ITERATION B      │ SHORTLIST        │ SELECTED         │ RUNTIME     │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ Product context      │ Criteria A       │ Criteria B       │ Decision notes   │ Accepted intent  │ Findings    │
│ baseline + evidence  │ 3 directions     │ comments merged  │ A2 vs B3         │ measurable claim │ metrics     │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ Page composition     │ Layout A1        │ Layout A2        │ A2               │ Page V4          │ actual      │
│ current home         │ Layout B1        │ Layout B2        │ B2               │ mobile+desktop   │ diff        │
│                      │ Layout C1        │                  │                  │                  │             │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ Blocks / patterns    │ Shelf P1         │ Shelf P2         │ P2               │ Shelf P2         │ pass/fail   │
│ current shelf        │ Sticky P1        │ Sticky parked    │                  │                  │             │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ Components           │ Card C1          │ Card C2          │ Card C2          │ Card C2          │ actual      │
│ EventCard baseline   │ Control C1       │ Control C2       │ Control C2       │ Control C2       │ states      │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ AI / references      │ image 01         │ extracted claim  │ —                │ design reference │ —           │
│                      │ image 02         │ edge treatment   │                  │                  │             │
├──────────────────────┼──────────────────┼──────────────────┼──────────────────┼──────────────────┼─────────────┤
│ Evaluation           │ review cut 1     │ review cut 2     │ product analysis │ build assumptions│ runtime     │
│                      │ 11 comments      │ 7 comments       │ technical check  │ test scenarios   │ decision    │
└──────────────────────┴──────────────────┴──────────────────┴──────────────────┴──────────────────┴─────────────┘
```

## 7. Связь gap-page и Recent changes

Gap-page отвечает:

```text
как развивалось решение одной проблемы
```

`Recent changes` отвечает:

```text
что изменилось во всём UI Exploration после последних agent runs
```

Один объект может присутствовать в двух представлениях без дублирования истины:

- на gap-page — как часть контекста и ветвления;
- на Recent changes — как автоматически сформированная before/after-проекция;
- в Git — как точный change manifest;
- после promotion — как принятый объект Resource Graph;
- после реализации — как browser actual/baseline/diff.

## 8. Что происходит после одного общего prompt

Пример действия оператора:

```text
1. Оператор смотрит ITERATION B.
2. Оставляет комментарии на Page B2, Shelf P2, Card C2 и Control C2.
3. Нажимает «Собрать следующую итерацию».
4. Один prompt требует согласованно изменить все четыре объекта.
5. Агент возвращает ITERATION C + change manifest.
6. Оператор нажимает «Обновить UI gap».
7. Плагин:
   - добавляет ITERATION C на gap-page;
   - обновляет Recent changes;
   - сохраняет старые итерации;
   - не создаёт задачи на каждый comment.
```

Число обязательных действий оператора не увеличивается из-за истории компонентов.

## 9. Ограничения, чтобы history page не стала космолётом

1. **Только автоматически обнаруженные material changes.**
2. **Никакого ручного журналирования.**
3. **Сегодня — по prompt/change set; прошлое — по датам.**
4. **Before/after + одна смысловая строка**, а не полный diff каждого свойства.
5. **Детальная история ограничена временным окном**; старое сворачивается.
6. **Git остаётся source of truth**; Penpot — удобная визуальная проекция.
7. **History не является approval.** Experimental изменение остаётся experimental.
8. **История не создаёт backlog.** У карточек нет priority, due date или assignee.

## 10. Место в двухплагинной архитектуре

```text
Product Atlas plugin
  UI gap / product context / decision link

Design System plugin
  Resource Graph file
  UI Exploration file
    00 Index
    05 Recent changes
    gap pages
    candidate index
    selected / archive
```

Отдельный третий plugin не требуется. `Recent changes` является представлением внутри UI Exploration mode того же Design System plugin.

## 11. Что проверить на синтетическом прототипе

До кодирования полного workflow достаточно визуально проверить:

- считывается ли gap-page слева направо;
- понятны ли параллельные tracks без устного объяснения;
- можно ли за минуту увидеть изменения последнего prompt;
- не перегружена ли before/after-карточка;
- полезно ли деление `сегодня по prompts / прошлое по датам`;
- нужен ли отдельный cross-gap candidate index после появления Recent changes;
- сколько старых change cards можно оставить до деградации производительности;
- достаточно ли трёх операций Design System plugin для всего цикла.

Следующий технический шаг после согласования визуального примера — описать machine-readable schemas для `gap package`, `iteration bundle` и `change set`, затем собрать минимальный Penpot prototype на синтетическом каталоге.