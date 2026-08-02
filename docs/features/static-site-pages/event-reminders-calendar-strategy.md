# Напоминания о событиях и календарная доставка

> **Дата решения:** 2026-08-02  
> **Статус:** source parity с пользовательскими требованиями подтверждён; product scope и test design зафиксированы; runtime Push, calendar-email producer и Android connector ещё не реализованы  
> **Неизменяемый источник:** [`schedule-user-requirements.md`](schedule-user-requirements.md)  
> **Общий release truth:** [`release-plan.md`](release-plan.md), Stage 14  
> **Release evidence:** [`release-autotest-gates.md`](release-autotest-gates.md) и [`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md)

## 1. Source parity и продуктовая цель

Исходные требования определяют одну пользовательскую задачу: человек не должен
забыть о выбранном мероприятии или опоздать на него. Они также прямо требуют:

1. компактный календарный список сохранённых событий сверху «Избранного», после которого идут крупные карточки лайков;
2. Push за сутки;
3. ближний Push за 1 час для события в текущем городе либо за 3 часа для другого города;
4. возможность выключать уведомления целиком и отдельные типы;
5. отсутствие notification storm;
6. сохранение ICS download;
7. календарное приглашение по email;
8. системную форму календаря через отдельное APK или упакованный сайт.

Настоящий документ не изменяет и не «улучшает» эти формулировки. При расхождении
приоритет всегда имеет `schedule-user-requirements.md`.

Задача формулируется как **«не пропустить сохранённое событие»**, а не как
обязательная запись в произвольный внешний календарь. Календарь, email и APK —
независимые delivery channels вокруг одной saved-event сущности.

## 2. Зафиксированная матрица решений

| Канал | Решение | Роль |
|---|---|---|
| Двухзонное «Избранное»: компактный календарь сверху, крупные карточки лайков ниже | `GO_TO_IMPLEMENTATION` | календарный и визуальный saved-state сценарии остаются раздельными |
| Web Push T−24h | `GO_TO_IMPLEMENTATION` | основное раннее напоминание |
| Web Push T−1h current city / T−3h other city | `GO_TO_IMPLEMENTATION_WITH_OWNER_DECISION` | ближнее напоминание; нужен server-owned источник текущего города |
| Push при отмене, переносе и существенном изменении места | `GO_TO_IMPLEMENTATION` | lifecycle-уведомления |
| Управление Push и отдельными типами | `GO_TO_IMPLEMENTATION` | обязательное требование, включая anti-storm |
| Web Push как `promo_activity` кампании | `GO_TO_FOCUS_CANARY` | отдельный promotional purpose/consent; не наследует reminder opt-in |
| Существующий ICS download | `KEEP` | честный ручной fallback |
| Calendar invitation через Yandex Cloud Postbox | `GO_TO_RESEARCH` | основной email-кандидат |
| Calendar invitation через NotiSend | `COMPARISON_ONLY` | не production-route и не fallback без отдельного решения |
| Тонкий Android connector APK | `GO_TO_PROTOTYPE` | системная форма календаря через `ACTION_INSERT` |
| Google Calendar OAuth/API | `OUT_OF_SCOPE` | не проектировать в этом track |
| Google Calendar web template | `NO_GO_PRODUCT_UX` | не использовать как основной мобильный CTA |
| Подписные календари | `OUT_OF_SCOPE` | не проектировать |
| Browser `intent:` | `NO_GO` | лаборатория не доказала переносимый contract |
| ICS через Web Share | `NO_GO_CALENDAR_INSERTION` | Share не равен открытию calendar editor |
| Фоновая загрузка и последующее открытие ICS из PWA | `NO_GO` | PWA не получает системный URI скачанного файла |

## 3. Пользовательский сценарий

Основная CTA на карточке/странице:

```text
Не пропустить
```

После сохранения:

```text
Событие сохранено

Напоминания
✓ За сутки
✓ За 1 час, если событие в текущем городе
  или за 3 часа, если оно в другом городе
✓ При переносе или отмене

[Настроить уведомления]

Другие способы
├── Скачать ICS
├── Отправить приглашение на email        [после research]
└── Открыть через Calendar Connector      [Android, после prototype]
```

Сохранение события, Notification permission, reminder preferences, email
invitation и ICS download — независимые состояния. Одно действие не является
согласием на другое.

Нельзя показывать «Добавлено в календарь», если система знает только о:

- скачивании ICS;
- принятии email провайдером;
- открытии письма;
- запуске Android Activity;
- открытии системной формы события.

## 4. Двухзонное «Избранное»: календарь сверху, лайки ниже

Страница «Избранное» состоит из двух последовательных блоков, а не из одного
calendar-first merge.

### 4.1. Верхний блок — `Мой календарь`

Только события с `calendar_saved=true` показываются компактно:

```text
Дата
HH:MM–HH:MM | Мероприятие | локация
```

Обязательные свойства:

- future/current события группируются по локальной дате;
- даты и строки сортируются хронологически;
- повторный calendar save идемпотентен;
- перенос перемещает строку;
- отмена видима как lifecycle state;
- неизвестное окончание или локация не выдумываются;
- выключение Push не удаляет строку.

### 4.2. Нижний блок — `Понравилось`

После завершения компактного календаря отображаются крупные обычные карточки
событий с `favorite_saved=true`. Они сохраняют визуальный формат карточек сайта,
а не превращаются в ещё один компактный список.

Сигналы независимы. Если пользователь и добавил событие в календарь, и поставил
лайк, событие может появиться один раз в каждом блоке. Это намеренное
cross-surface представление двух действий. Дубликаты запрещены **внутри**
каждого блока, но не между блоками. Снятие calendar save не снимает лайк и
наоборот.

Точная дополнительная month-grid остаётся UI implementation decision. Release
gate проверяет порядок блоков, компактный agenda contract, крупные liked cards
и независимость состояний.

## 5. Push reminders

### 5.1. Временные kinds

```text
event_reminder_24h
event_reminder_1h_current_city
event_reminder_3h_other_city
```

Для одной event revision создаётся максимум два временных напоминания:

```text
T−24h
+
ровно один ближний kind:
  T−1h_current_city
  или
  T−3h_other_city
```

`T−1h_current_city` и `T−3h_other_city` взаимоисключающие.

| Момент сохранения | Текущий город | Другой город |
|---|---|---|
| раньше T−24h | T−24h + T−1h | T−24h + T−3h |
| после T−24h, но раньше ближнего offset | только T−1h | только T−3h |
| позднее ближнего offset | backdated job не создаётся | backdated job не создаётся |
| после начала события | jobs не создаются | jobs не создаются |

Расчёт выполняется по абсолютному `starts_at`; пользовательское отображение —
`Europe/Kaliningrad` либо другой явно принятый timezone contract. Напоминание
нельзя планировать только по текстовой дате.

### 5.2. Открытый вопрос: текущий город

Исходные требования задают разное время для текущего и другого города, но не
определяют источник текущего города пользователя.

До implementation M1 требуется owner/data decision. Возможные источники должны
быть рассмотрены отдельно:

- явно выбранный город в пользовательском профиле;
- существующая server-owned настройка региона;
- другой устойчивый owner-scoped атрибут.

Browser geolocation, User-Agent, IP-эвристика и caller-supplied `current_city`
не становятся источником истины автоматически. Если city relation неизвестен,
система может безопасно поставить T−24h, но ближний kind должен оставаться
fail-closed до отдельного утверждённого fallback.

### 5.3. Permission и preferences UX

1. Пользователь сохраняет событие или нажимает «Не пропустить».
2. UI объясняет T−24h и один city-relative ближний reminder.
3. Notification permission запрашивается только после отдельного tap.
4. `denied` не отменяет saved event, календарный вид и ICS.
5. Пользователь может выключить:
   - все Push;
   - T−24h;
   - ближний reminder;
   - конкретное событие.
6. Выключенный тип не создаёт новый outbox job и инвалидирует pending job согласно принятой политике.
7. Повторное сохранение не сбрасывает preferences.

Поведение lifecycle-типов при частичном отключении — отдельное owner decision,
которое должно быть отражено в UI и тестах до реализации.

### 5.4. Lifecycle kinds

```text
event_rescheduled
event_cancelled
event_location_changed
```

При новой event revision:

- pending reminders старой revision инвалидируются;
- временные reminders пересчитываются от нового времени;
- city classification перечитывается из server-owned данных;
- существенное изменение создаёт ровно одно lifecycle-уведомление соответствующего kind;
- отменённое событие не получает обычные countdown reminders;
- повторный Smart Update с той же revision/kind — no-op.

### 5.5. Anti-storm

Требование запрещает notification storm, приводящий к когнитивным ошибкам и
потере фокуса.

Минимальные гарантии:

- максимум два time-based jobs на event revision;
- local/other-city near kinds не могут существовать одновременно;
- повторный scheduler tick, retry или Smart Update не создаёт новый visible Push;
- один lifecycle kind отправляется не более одного раза на revision;
- отключённый тип не ставится повторно;
- ambiguous provider result не повторяется вслепую;
- multi-device отправка различает одно логическое уведомление и доставки по subscriptions.

Глобальный дневной cap, объединение нескольких событий в digest и политика при
нескольких мероприятиях в одно время не определены исходными требованиями и
остаются owner decisions. Документ не придумывает численный предел.

### 5.6. Ownership и idempotency

Не создаётся второй saved-event truth. Используется существующий durable
`user_saved_event` contract.

Планируемые runtime-сущности:

```text
push_subscription
├── owner identity
├── endpoint + encryption keys
├── device/install identity
├── enabled/revoked
└── last provider state

event_reminder_preference
├── owner identity
├── event id
├── enabled types
├── city decision reference
├── event revision
└── enabled

event_reminder_outbox
├── idempotency key
├── event/user/subscription
├── notification kind
├── scheduled_at
├── event revision
├── claim/attempt state
└── provider result
```

Минимальный idempotency key:

```text
owner + event + revision + notification kind + subscription
```

Push endpoint и encryption keys — transport credentials. Они не попадают в
статический HTML, YDB, публичные artifacts и обычную telemetry.

### 5.7. Delivery semantics

Web Push — best effort, не точный будильник. Provider acceptance не доказывает
фактический показ или просмотр уведомления.

Обязательное:

- claim/lease перед сетевой отправкой;
- bounded retry только для однозначно retryable результата;
- `404/410` отключает истёкшую subscription;
- notification click открывает canonical event route;
- payload не содержит лишние персональные данные;
- мониторинг различает `scheduled`, `provider_accepted`, `permanent_failure`, `opened`.

### 5.8. Web Push как activity промо-кампании

Utility reminders из этого раздела не являются рекламной кампанией. Отдельно
общая модель `promo_campaign / promo_target / promo_activity` может получить
planned surface:

```text
promo_activity.surface = web_push
```

Она может продвигать grounded future event/festival через тот же Web Push
transport, но обязана иметь отдельные product boundaries:

- отдельный consent purpose `promo_push`; reminder permission/opt-in его не
  создаёт;
- отдельный producer/outbox namespace и idempotency key с campaign/activity;
- active campaign, eligible target, send window, caps и disclosure;
- pause/archive кампании инвалидирует только pending promo jobs;
- reminder preferences и saved-event jobs не меняются;
- subscription endpoint/keys не попадают в `promo_exposure`;
- provider acceptance не считается просмотром.

Первая проверка планируется только как bounded editorial canary внутри
фокус-группы: одна явно выбранная активная кампания, отдельный opt-in и не более
одной отправки участнику в canary sequence. Общий public rollout и постоянные
frequency caps требуют отдельного owner decision.

Канонический campaign contract:
[`../promo-campaigns/README.md`](../promo-campaigns/README.md).

## 6. ICS download

Существующий ICS download остаётся доступным как ручной fallback.

Обязательный contract:

- стабильный `UID`;
- корректные `DTSTAMP`, `DTSTART`, `DTEND`;
- title, description, location и canonical event URL;
- UTC/timezone без сдвига;
- CRLF, escaping и line folding;
- понятное имя файла;
- повторная загрузка со страницы события и из saved events;
- видимый feedback «Файл календаря скачан»;
- download/click не считается внешним calendar save.

Не развивать:

- скрытую фоновую загрузку;
- автоматическое открытие Download Manager;
- попытку восстановить `content://` URI;
- ICS Web Share как основной calendar path.

## 7. Calendar invitation через Postbox

### 7.1. Почему Postbox — основной кандидат

Текущий `email_control/providers/postbox.py` уже формирует MIME bytes,
кодирует их Base64 и отправляет через Postbox Raw API (`Content.Raw.Data`).
Поэтому provider-level вопрос «можно ли передать raw MIME» практически закрыт.

Открытые вопросы относятся к нашему builder и mail clients:

- inline `text/calendar`, attachment или оба;
- `METHOD:REQUEST`, update и `CANCEL`;
- стабильный UID и монотонный SEQUENCE;
- duplicate behavior;
- Gmail, Apple Mail, Outlook;
- Inbox/Spam placement;
- timezone и fields после импорта.

Текущий worker допускает только `transactional-plain-v1` с `subject/text/html`.
Нужен новый строгий server-owned template, например:

```text
transactional-calendar-request-v1
```

Browser передаёт только event/save action. Producer повторно читает канонический
event snapshot и самостоятельно формирует MIME, sender headers, `ORGANIZER`,
`ATTENDEE`, `UID` и `SEQUENCE`.

### 7.2. Research variants

```text
A. multipart/alternative
   ├── text/plain
   ├── text/html
   └── text/calendar; method=REQUEST

B. multipart/mixed
   ├── multipart/alternative с text/calendar
   └── event.ics attachment

C. METHOD:PUBLISH attachment
   └── negative/control variant
```

`method` в MIME и `METHOD` в VCALENDAR обязаны совпадать.

Lifecycle:

- initial: `METHOD:REQUEST`, `SEQUENCE:0`;
- update: тот же `UID`, `SEQUENCE+1`;
- cancel: тот же `UID`, `METHOD:CANCEL`;
- каждый этап имеет отдельный idempotency key;
- ambiguous send не повторяется автоматически.

### 7.3. NotiSend boundary

NotiSend остаётся comparison-only. Действующая архитектура направляет event
transactional mail в Postbox. NotiSend не становится hidden fallback, не
использует recommendation consent и не выбирается после ambiguous Postbox send
без отдельного owner/architecture decision.

## 8. Тонкий Android Calendar Connector

Connector — отдельное небольшое Android-приложение; сайт/PWA остаётся основной
точкой входа.

```text
PWA / browser
→ https://calendar.kenigevents.ru/add/<opaque-token>
→ verified Android App Link
├── connector установлен
│   └── CalendarConnectorActivity
│       └── ACTION_INSERT + CalendarContract.Events.CONTENT_URI
│           └── системная форма календаря
└── connector отсутствует
    └── web fallback
        └── ICS download + install guidance
```

MVP не запрашивает:

```text
READ_CALENDAR
WRITE_CALENDAR
storage/files
contacts
location
notifications
```

Пользователь видит системную форму, выбирает календарь и нажимает Save.
Connector сообщает «Форма календаря открыта», а не «Событие добавлено».

Обязательные свойства:

- постоянный package name;
- постоянный release signing key;
- `android:autoVerify="true"`;
- `assetlinks.json` с exact certificate fingerprint;
- release build, не debug APK;
- bounded first-party payload по opaque token;
- запрет arbitrary host/redirect и `file://`/`content://` deep links;
- сначала Google Play Internal/Closed Testing;
- direct signed APK — только bounded pilot.

## 9. Тестирование и актуальные события

Канонический test design:
[`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md).

Production-like сценарии не используют hardcoded event URL. Resolver:

1. проверяет `preview-build.json` и full repository SHA;
2. обходит current listing routes того же build;
3. выбирает complete timed future event;
4. получает adjacent ICS;
5. сохраняет immutable `selected-event.json`;
6. revalidates событие до первого side effect.

После первого Push/email side effect сценарий не может тихо переключиться на
другое событие.

### 9.1. Фокус-группа как первая acceptance lane

Продуктовая проверка выполняется не на отдельной лабораторной странице, а в
существующем focus cohort после его onboarding/PWA gates:

1. участник лайкает актуальное событие;
2. добавляет актуальное событие в календарь;
3. открывает «Избранное» и видит компактный календарь сверху и крупные liked
   cards ниже;
4. по отдельному tap включает utility reminders и меняет один preference;
5. получает compressed-clock reminder canary;
6. отдельно, без prechecked consent, может включить один promo Web Push canary;
7. оставляет usefulness/problem feedback обычным focus feedback path.

Отказ в Notification permission или promo opt-in не является провалом миссии и
не влияет на prize/progress scoring. Focus evidence помогает принять UX и
reliability, но само по себе не включает public Push или promo activity.

Companion contract:
[`../static-site-focus-group/event-reminders-acceptance.md`](../static-site-focus-group/event-reminders-acceptance.md).

## 10. Порядок реализации

### M0 — документы и test scaffold

- source requirements byte-identical;
- strategy, routes и Stage 14 общего release plan;
- scenario registry;
- dynamic current-event resolver;
- GitHub Actions browser/Android/iOS environment scaffold;
- никаких production provider/native side effects.

### M1a — двухзонное «Избранное»

- верхний compact date/time/event/location agenda только для `calendar_saved`;
- нижние крупные event cards только для `favorite_saved`;
- same event may appear once per zone when both states are true;
- no duplicate inside either zone;
- independent toggle, lifecycle and typed empty/fallback states;
- current-event integration + frozen grouping/card fixtures.

### M1b — utility reminder Push

- owner decision по источнику текущего города;
- subscription/preferences UI;
- T−24h + взаимоисключающий T−1h-current-city/T−3h-other-city scheduler;
- anti-storm и idempotent outbox;
- lifecycle invalidation;
- Android/iOS L2 и bounded L3 acceptance;
- independent feature flags и rollback.

### M1c — promo campaign Web Push focus canary

- planned `promo_activity.surface=web_push`;
- separate `promo_push` consent and campaign/activity idempotency;
- one bounded editorial campaign in the active focus cohort;
- campaign pause/archive and disclosure assertions;
- proof that reminder opt-in does not authorize promo delivery;
- independent kill switch; failure does not block M1a/M1b.

### M2 — Postbox research

- deterministic MIME builder;
- protected REQUEST/update/CANCEL roundtrip;
- Gmail/Apple Mail/Outlook matrix;
- отдельный release decision.

### M3 — Android connector POC

- Android project и stable signing identity;
- verified App Links;
- `ACTION_INSERT`;
- Internal Testing;
- emulator + real-device/OEM evidence;
- отдельное решение о публичном распространении.

Fail M1c, M2 или M3 не блокирует M1a/M1b и не удаляет ICS fallback.

## 11. NO-GO и rollback

NO-GO:

- source requirements изменены этим track;
- календарный вид отсутствует либо выдумывает время/локацию;
- current city принимается от browser как authoritative факт;
- одновременно создаются T−1h и T−3h near jobs;
- выключенный reminder type продолжает создавать jobs;
- duplicate/notification storm возможен при retry, scheduler tick или Smart Update;
- event facts принимаются от browser вместо server-side snapshot;
- permission запрашивается без user gesture;
- reminder consent или Notification permission использованы как promo consent;
- paused/archived promo campaign продолжает создавать promo Push jobs;
- focus mission требует grant/положительный feedback либо влияет на prize scoring;
- Postbox provider acceptance выдан за calendar-client recognition;
- connector просит calendar write permission либо не прошёл App Link/native editor acceptance;
- ICS download назван внешним calendar save;
- Android/iOS environment smoke назван product PASS.

Rollback независим по channel:

- utility Push producer/sender выключается, pending reminder jobs инвалидируются;
- promo Web Push activity выключается отдельно, campaign jobs инвалидируются без изменения saved reminders;
- email experiment выключается без ambiguous resend;
- connector CTA возвращается к web fallback;
- saved event, простой календарный вид и ICS остаются доступны.

## 12. Открытые owner decisions

Перед M1a/M1b:

1. Какая server-owned сущность определяет текущий город пользователя?
2. Что делать, когда текущий город неизвестен?
3. Нужен ли immediate reminder при сохранении позднее ближнего offset?
4. Пользователь меняет offsets или только включает/выключает типы?
5. Как обрабатывать событие без точного времени или достоверного города?
6. Нужен ли отдельный Push при изменении только площадки?
7. Каков глобальный anti-storm cap и нужно ли объединять несколько близких событий?

Перед M1c:

8. Как называется отдельный consent purpose для promo Web Push в UI?
9. Какой первый editorial campaign и audience selector допускаются в focus canary?
10. Каковы public per-user/day и per-campaign caps после canary?
11. Как маркируются editorial и partner/commercial promo Push?

Перед M2:

12. Какой controlled sender используется для invitation canary?
13. Какие Gmail/iCloud/Outlook mailboxes входят в blocking matrix?
14. Initial-only допустим или update/cancel обязательны для MVP?

Перед M3:

15. Какой host выделяется под App Links?
16. Как называется connector?
17. Pilot только через Play Internal Testing или также direct signed APK?

## 13. Связанные документы

- [`../event-favorites-calendar/README.md`](../event-favorites-calendar/README.md)
- [`../promo-campaigns/README.md`](../promo-campaigns/README.md)
- [`../static-site-focus-group/README.md`](../static-site-focus-group/README.md)
- [`../static-site-focus-group/event-reminders-acceptance.md`](../static-site-focus-group/event-reminders-acceptance.md)
- [`release-plan.md`](release-plan.md)
- [`release-autotest-gates.md`](release-autotest-gates.md)
- [`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md)
