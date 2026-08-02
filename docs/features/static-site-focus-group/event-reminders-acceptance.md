# Acceptance фокус-группы: Favorites, reminder Push и promo Web Push

> **Статус:** cross-feature product/test companion; runtime не реализуется этим документом.
> **Focus source:** [`README.md`](README.md) и [`product-prototype.md`](product-prototype.md).
> **Event-delivery strategy:** [`../static-site-pages/event-reminders-calendar-strategy.md`](../static-site-pages/event-reminders-calendar-strategy.md).
> **Saved-state contract:** [`../event-favorites-calendar/README.md`](../event-favorites-calendar/README.md).
> **Promo contract:** [`../promo-campaigns/README.md`](../promo-campaigns/README.md).

## 1. Назначение

Фокус-группа — первая ограниченная acceptance lane для сценария «не пропустить»,
но не отдельный backend и не автоматическое разрешение production rollout.
Нужно проверить понятность двух saved-state действий, системный permission UX,
реальную доставку utility Push и отдельный promotional purpose.

Исследование не требует положительной оценки, Notification permission grant или
promo opt-in. Эти действия не дают преимущество в prize programme.

## 2. Целевая поверхность «Избранного»

```text
Мой календарь
Дата
HH:MM–HH:MM | Мероприятие | локация

Понравилось
[крупная карточка]
[крупная карточка]
```

- верхний compact agenda использует только `calendar_saved`;
- нижние крупные cards используют только `favorite_saved`;
- same event может находиться один раз в каждой зоне, если оба состояния true;
- внутри зоны duplicate запрещён;
- снятие одного состояния не снимает другое;
- Push off не удаляет saved state.

## 3. Три независимых purpose

| Purpose | Триггер | Consent/state | Что нельзя наследовать |
|---|---|---|---|
| Utility reminder | пользователь calendar-saves event | reminder preferences + Notification permission | не создаёт promo consent |
| Promo campaign Web Push | active `promo_activity.surface=web_push` | отдельный `promo_push` opt-in | не использует reminder opt-in |
| Focus research communication | участие/weekly research consent | focus-specific consent | не создаёт recommendation/marketing consent |

Transport/subscription может быть общим, но producers, outbox kinds,
idempotency, caps, telemetry и kill switches раздельные.

## 4. Orientation mission

После здорового focus onboarding/PWA continuity:

1. resolver выбирает актуальное timed событие exact candidate build;
2. участник ставит like;
3. участник добавляет событие в календарь;
4. открывает «Избранное» и проверяет обе зоны;
5. повторяет действия и не получает duplicate;
6. снимает одно состояние и убеждается, что второе осталось;
7. открывает reminder pre-prompt и выбирает grant либо deny;
8. при grant меняет один reminder type и получает compressed-clock utility
   reminder canary;
9. отдельный экран предлагает optional promo Push canary без prechecked consent;
10. при opt-in active editorial campaign отправляет один Push;
11. участник оставляет обычный usefulness/problem feedback.

Deny/skip — валидный terminal outcome, а не незавершённая миссия.

## 5. Актуальные события

Hardcoded event URL запрещён. Используется общий current-event resolver. Один
актуальный event может намеренно получить оба saved-state флага и появиться в
двух зонах; этого достаточно для cross-zone contract. Frozen fixtures покрывают
несколько дат, distinct events, перенос, отмену и empty states.

До первого Push side effect event можно re-resolve один раз. После side effect
смена event запрещена.

## 6. Promo campaign canary

Первая итерация:

- одна active editorial campaign/activity;
- один current grounded event target;
- только active focus members с explicit `promo_push` opt-in;
- одна отправка на участника в canary sequence;
- видимый editorial/promo label;
- pause до claim отменяет campaign job;
- resume отправляет ровно один Push;
- archive прекращает новые jobs;
- utility reminders продолжают работать независимо.

Commercial/partner campaign, broad targeting и постоянные frequency caps не
принимаются этим canary.

## 7. Evidence

Обязательный sanitized record:

- exact repo/build/candidate identity;
- selected event URL/UID hash/ICS hash;
- calendar and like state transitions;
- element count per zone and card/row role;
- Notification permission outcome без давления на grant;
- reminder preference and outbox kinds;
- promo consent purpose, campaign/activity hash and lifecycle;
- native notification display/click where implemented;
- ordinary focus feedback receipt;
- confirmation that no step changed prize scoring.

## 8. Метрики и интерпретация

Полезны:

- доля участников, правильно объяснивших разницу like/calendar save;
- успешность нахождения compact agenda и large liked cards;
- duplicate/confusion/error rate;
- permission prompt abandonment, grant и deny как разные outcomes;
- reminder preference success и click-through;
- promo opt-in/open только с явным denominator;
- qualitative feedback и repair time.

Grant rate, положительный feedback и promo opt-in не являются самоцелью. Малый
cohort даёт directional evidence, а не доказательство статистического uplift.

## 9. Release и rollback

Focus PASS не включает public feature автоматически. Для общего rollout всё ещё
нужны Stage 14 gates, Android/iOS L2 и соответствующие L3 canaries.

Independent rollback:

- Favorites остаётся доступным при отключённом Push;
- utility reminder producer выключается отдельно;
- promo activity/campaign выключается отдельно;
- focus feedback и programme lifecycle не зависят от обоих Push channels;
- ICS остаётся fallback.
