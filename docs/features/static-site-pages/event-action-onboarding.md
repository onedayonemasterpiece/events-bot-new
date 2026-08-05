# Контекстный онбординг действий на странице события

> **Статус:** принятый TO-BE продуктовый контракт. Exact visual variant и
> production enablement требуют отдельного release gate.
> **Связанные контракты:**
> [`event-reminders-calendar-strategy.md`](event-reminders-calendar-strategy.md),
> [`calendar-reminder-strategy.md`](calendar-reminder-strategy.md),
> [`analytics/unified-statistics-runtime-architecture.md`](analytics/unified-statistics-runtime-architecture.md).

## Решение

Онбординг объясняет конкретную пользу рядом с действием, а не запускает общий
first-launch tour. На странице одновременно видна не более чем одна подсказка.

Priority:

1. `Добавить в календарь`, если действие ещё не освоено;
2. `Нравится`, если календарь уже понятен;
3. `Поделиться`, если первые два действия освоены;
4. никакой подсказки, если она не нужна.

Подсказка появляется только после устойчивой visibility action row и не
перекрывает CTA, auth/modal/gallery или keyboard focus.

## Границы состояний

- calendar save и like — независимые состояния;
- calendar/favorite states входят в `Избранное`, но один event учитывается в
  union count один раз;
- hidden/not-interest recovery не переносится в профиль и не смешивается с
  calendar/favorite onboarding;
- `.ics` download/open остаётся доступным без backend success;
- email reminder требует explicit verified identity и consent;
- optimistic click не считается accepted save или reminder.

## Честные partial outcomes

Один user action может иметь несколько независимых результатов:

```text
ICS opened
site calendar state accepted
email follow accepted
```

UI сообщает только подтверждённое. Нельзя писать «письма включены», если есть
лишь local click, pending outbox или dry-run.

Примеры:

- только ICS: `Файл календаря открыт. После входа можно сохранить событие в «Избранном» и включить напоминания.`
- save accepted, email off: `Сохранено в «Избранном». Письма не включены.`
- save + follow accepted: `Сохранено. Напоминания включены.`

## Suppression policy

- successful accepted action навсегда suppresses соответствующее обучение;
- dismiss запускает bounded cooldown;
- не более двух unsolicited impressions на action;
- не более одной unsolicited hint на meaningful session;
- local fallback хранит только compact suppression/use state;
- auth merge сохраняет strongest `used/dismissed` evidence и не создаёт
  duplicate state.

## Accessibility

- target минимум `44×44px`;
- persistent hint использует `role="note"` и `aria-describedby`;
- focus не переносится в hint автоматически;
- dismiss имеет отдельное accessible name;
- action row не меняет координаты при появлении hint;
- reduced motion и no-JS сохраняют основной action.

## Statistics

Все измерения идут через Unified Statistics Runtime:

Weak, consent-gated:

```text
hint_eligible
hint_visible
hint_dismissed
cta_visible
cta_click
```

Strong, только из authoritative receipt:

```text
calendar_save_accepted
calendar_save_undone
reminder_follow_accepted
reminder_follow_cancelled
```

Не хранить email, raw URL, hint copy или DOM selector.

## Acceptance

- [ ] Подсказка не появляется до устойчивой visibility и не блокирует действие.
- [ ] Не более одной подсказки на session/page.
- [ ] `.ics` работает при отказе backend.
- [ ] Partial outcome copy соответствует terminal receipts.
- [ ] Calendar/favorite/like/reminder не склеиваются в один state.
- [ ] Cross-tab/auth merge не создаёт повторных hints или duplicate actions.
