# Адаптивная навигация публичного статического сайта

> **Статус:** принято продуктовое направление; точная геометрия и финальный
> visual sign-off выполняются в release UI track.

## Решение

Mobile и desktop сохраняют одну информационную архитектуру, но используют
разную disclosure mechanics:

- **compact/mobile:** фирменная бирка может открывать navigation sheet;
- **desktop/large:** основные destinations постоянно видимы в горизонтальной
  шапке; бирка — brand treatment, а не второй конкурирующий drawer;
- **tablet/medium:** режим выбирается по реально доступной ширине, не по user
  agent.

Инварианты на всех generated HTML pages:

1. одинаковые destinations, labels и относительный порядок;
2. одинаковая current-location semantics;
3. одинаковые Search, identity/account и `Избранное` semantics;
4. логичный keyboard order, accessible names и focus recovery;
5. static/no-JS доступ к основным маршрутам;
6. не существует двух конкурирующих primary navigation одновременно.

## Desktop decision gate

Immutable preview содержит минимум три сопоставимых варианта с одним контентом:

- A — обычная горизонтальная шапка;
- B — неглубокая hybrid-шапка с интегрированной фирменной биркой — baseline;
- C — выраженная mobile-like бирка как control candidate.

Владелец принимает один exact variant. До sign-off никакой lab не объявляется
каноническим UI.

## Acceptance matrix

Проверяются `320/375/390`, `768`, `1366/1440` CSS px:

- desktop destinations доступны без лишнего раскрытия;
- mobile sheet открывается/закрывается, `Esc` и outside click работают, focus
  возвращается к trigger;
- header не закрывает hero/title/focus ring и не создаёт horizontal overflow;
- browser zoom и reduced motion сохраняют доступность;
- account/search/favorites state не расходится между страницами;
- route matrix проходит no-JS и cross-device task rehearsal;
- визуальные варианты не меняют смысл или порядок навигации.

Device share может определить canary emphasis, но не позволяет удалить desktop
или mobile из release surface.
