# Keyboard Navigation V8: onboarding, Hero-talk и артефакты

> **Статус:** companion к
> [V8 product model](keyboard-event-navigation-v8-product-model.md). После owner
> acceptance положения этого файла должны быть внесены в stacked onboarding PR
> #288 и Hero-talk PR #291, а не оставаться отдельной параллельной каноникой.

## 1. Capability

```yaml
capability_id: desktop_keyboard_power_navigation
surface: event_detail_desktop
minimum_viewport: 1024
primary_outcome: keyboard_route_started
mastery_outcome: keyboard_cross_page_route_completed
help_command: '?'
```

Capability не является обязательным условием event value, входа, коллекции,
клуба или розыгрыша. Это ускоритель для power users.

## 2. Contextual onboarding arc

```text
keycaps рядом с частыми действиями
→ первый keyboard intent
→ one-time popover: arrows + Enter + ?
→ contextual help
→ reading/card/gallery route
→ cross-page Enter + Back
→ mastery suppression
```

Popover copy:

> **Можно быстрее**  
> Стрелки ведут по событию и похожим карточкам. Enter открывает выбранное
> событие. `?` покажет все клавиши.

Не использовать как единственный текст `«Нажмите ↓, чтобы прочитать событие»`:
значение `↓` контекстно, а пользователь должен сразу узнать точку помощи и
cross-page действие `Enter`.

## 3. Hero-talk chains

### `home_hero / feature_discovery`

Eligibility: несколько event-page visits, keyboard fact отсутствует, cooldown
соблюдён.

```text
События можно смотреть и без мыши.
→ На странице события нажмите ? — покажем все клавиши.
```

### `event_page_end / feature_discovery`

До первого использования:

```text
Эту страницу можно пройти клавишами.
→ Стрелки ведут по разделам и похожим событиям.
→ ? покажет команды.
```

После первого route:

```text
Вы уже начали пользоваться клавиатурой.
→ Enter откроет выбранное событие.
→ Back вернёт к той же карточке.
```

После mastery:

```text
Клавиатурный маршрут освоен.
→ Подсказки на кнопках можно вернуть через ?.
```

### Отдельный pointer chain

```text
Двойной щелчок по карточке — нравится.
```

Не объединять pointer и keyboard обучение в одну длинную сцену.

## 4. Help как постоянный реестр

Clickable question control и физическая `?` открывают один dialog. Он показывает
все commands и фактический current state:

- available;
- disabled;
- disabled reason;
- current owner;
- page-family capability;
- `Esc` close/return.

Help остаётся доступным после suppression keycaps/popover и не считается
mastery сам по себе.

## 5. Артефакт из первой коллекции

Не создавать сущность `keyboard artifact`. Использовать существующий collectible
`migratory-bird-ring` — **«Кольцо перелётной птицы»** — из fixed collection
`signs-of-kaliningrad-001 / «Знаки Янтарного края»`.

Recommended placement bundle:

```yaml
artifact_id: migratory-bird-ring
collection_id: signs-of-kaliningrad-001
mode: keyboard_only_optional_bonus
primary_anchor: event_related_to_continuation_bridge
split_fallback: event_after_last_related_row
editorial_fallback: event_after_last_related_row
screen_reader_path: keyboard_graph_same_anchor
pointer_path: none
threshold_required: false
```

Find route:

```text
последняя строка похожих событий
→ ArrowDown
→ отдельный artifact node
→ Enter / Space
→ find receipt + история + коллекция
→ ArrowDown в continuation
```

Почему этот placement предпочтителен:

- одинаков в Editorial и Split;
- не меняет оптимизированный визуальный порядок cards;
- не выглядит как event card;
- требует реального использования arrow graph;
- стабилен при rerender и reload;
- не зависит от multi-image gallery;
- threshold первой коллекции остаётся достижим без keyboard-only find.

## 6. Alternative placements to prototype

1. gallery extra stop после полного keyboard traversal;
2. reading margin stop между description и practical;
3. marker после cross-page Enter → Back;
4. clue внутри help, но не сам find;
5. hero wrap stop для multi-image events;
6. page-end completion marker.

Reject для первой коллекции: секретная комбинация стрелок по grid. Она
необъяснима, плохо тестируется и превращает discovery в кодовый замок.

## 7. State separation

```text
keyboard capability competency
keyboard help/popover state
keyboard daily usage facts
artifact assignment/find
collection progress
personalization taste
```

- keyboard usage не становится taste signal;
- artifact find не становится keyboard mastery автоматически;
- help exposure не равен artifact hint/find;
- keyboard-only find не меняет raffle odds;
- no raw key stream.

## 8. Required changes after acceptance

- добавить capability и chains в `docs/features/static-site-onboarding/README.md`;
- добавить `feature_discovery` и artifact bridge в
  `docs/features/hero-talk/README.md`;
- добавить scenario packs в Hero-talk testing companion;
- обновить artifact placement bundle/collection contract;
- сохранить first collection membership и existing first onboarding specimen
  `amber-cosmonaut` без переназначения.
