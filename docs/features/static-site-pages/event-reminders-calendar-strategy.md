# Напоминания о событиях и календарная доставка

> **Дата решения:** 2026-08-02  
> **Статус:** product scope и test design зафиксированы; runtime Push, calendar-email producer и Android connector ещё не реализованы  
> **Исходные требования:** [`schedule-user-requirements.md`](schedule-user-requirements.md) — неизменяемый источник пользовательских требований  
> **Release truth:** [`release-plan.md`](release-plan.md)  
> **Тестирование:** [`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md)

## 1. Решение

Задача формулируется как **«не пропустить сохранённое событие»**, а не как
обязательная запись в сторонний календарь.

Принятый scope:

| Канал | Решение | Роль |
|---|---|---|
| Web Push за сутки и за час | `GO_TO_IMPLEMENTATION` | основной пользовательский путь |
| Push при отмене, переносе и существенном изменении места | `GO_TO_IMPLEMENTATION` | lifecycle-уведомления |
| Существующий ICS download | `KEEP` | ручной universal fallback |
| Calendar invitation через Yandex Cloud Postbox | `GO_TO_RESEARCH` | основной email-кандидат |
| Calendar invitation через NotiSend | `COMPARISON_ONLY` | не production-route без отдельного решения |
| Тонкий Android connector APK | `GO_TO_PROTOTYPE` | системная форма календаря через `ACTION_INSERT` |
| Google Calendar OAuth/API | `OUT_OF_SCOPE` | не проектировать в текущем track |
| Google Calendar web template | `NO_GO_PRODUCT_UX` | не использовать как основной мобильный CTA |
| Подписные календари | `OUT_OF_SCOPE` | не проектировать |
| Browser `intent:` | `NO_GO` | лаборатория не доказала portable contract |
| ICS через Web Share | `NO_GO_CALENDAR_INSERTION` | Share не равен открытию calendar editor |
| Фоновая загрузка и последующее открытие ICS из PWA | `NO_GO` | у PWA нет системного URI скачанного файла |

`docs/features/static-site-pages/schedule-user-requirements.md` не изменяется
этим документом. Перед implementation merge выполняется byte-level source parity:
производная стратегия не может ослабить или переопределить исходные требования.

## 2. Пользовательский сценарий

Основная CTA:

```text
Не пропустить
```

После действия:

```text
Напоминания включены
✓ За сутки
✓ За час
✓ При переносе или отмене

Другие способы
├── Скачать ICS
├── Отправить приглашение на email        [после research]
└── Открыть через Calendar Connector      [Android, после prototype]
```

Сохранение события, Push permission, email invitation и ICS download — четыре
независимых состояния. Нельзя автоматически считать одно согласием на другое.

Нельзя показывать «Добавлено в календарь», если система знает только о:

- скачивании ICS;
- принятии письма провайдером;
- открытии письма;
- запуске Android Calendar Activity;
- открытии системной формы события.

## 3. Push reminders

### 3.1. Базовые offsets

```text
T−24h
T−1h
```

| Когда пользователь сохраняет событие | План |
|---|---|
| раньше T−24h | T−24h и T−1h |
| между T−24h и T−1h | только T−1h |
| позднее T−1h | не создавать backdated jobs; immediate reminder остаётся owner decision |
| после начала события | reminder jobs не создаются |

Расчёт выполняется по абсолютному `starts_at`. Отображение —
`Europe/Kaliningrad`. Нельзя планировать напоминание только по календарной дате.

### 3.2. Permission UX

1. Пользователь сохраняет событие или нажимает «Не пропустить».
2. UI объясняет два времени напоминания.
3. Notification permission запрашивается только после отдельного tap.
4. `denied` не отменяет сохранение события и не блокирует ICS.
5. Настройки позволяют отключить Push для одного события и для устройства.

### 3.3. Lifecycle

Минимальные kinds:

```text
event_reminder_24h
event_reminder_1h
event_rescheduled
event_cancelled
event_location_changed
```

При новой event revision:

- pending reminders старой revision инвалидируются;
- offsets пересчитываются от нового времени;
- существенное изменение создаёт ровно одно lifecycle-уведомление;
- отменённое событие не получает обычные reminders;
- повторный Smart Update с той же revision/kind не создаёт duplicate.

### 3.4. Ownership и idempotency

Не создавать второй saved-event truth. Связь пользователя с событием должна
использовать существующий durable saved-event contract.

Необходимые runtime-сущности:

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
├── offsets
├── event revision
└── enabled

event_reminder_outbox
├── idempotency key
├── event/user/subscription
├── kind
├── scheduled_at
├── event revision
├── claim/attempt state
└── provider result
```

Минимальный idempotency key:

```text
user + event + revision + kind + subscription
```

Push endpoint и encryption keys считаются transport credentials. Они не попадают
в статический HTML, YDB, публичные artifacts и обычную telemetry.

### 3.5. Delivery semantics

Web Push — best effort, не точный будильник. Provider acceptance не доказывает
фактический показ или просмотр уведомления.

Обязательные свойства:

- claim/lease до сетевой отправки;
- bounded retry только для однозначно retryable результата;
- revoked/expired subscription отключается;
- notification click открывает canonical event route;
- payload не содержит лишних персональных данных;
- мониторинг различает `scheduled`, `provider_accepted`, `permanent_failure`,
  `opened`, но не смешивает эти состояния.

## 4. ICS download

Существующий ICS download сохраняется без изменения продуктовой роли.

Обязательный contract:

- стабильный `UID`;
- корректные `DTSTAMP`, `DTSTART`, `DTEND`;
- title, description, location и canonical event URL;
- UTC/`Europe/Kaliningrad` без временного сдвига;
- CRLF, escaping и line folding;
- понятное имя файла;
- возможность повторной загрузки;
- видимый feedback «Файл календаря скачан»;
- download/click не считается внешним calendar save.

Не развивать:

- скрытую фоновую загрузку;
- автоматическое открытие Download Manager;
- попытку восстановить `content://` URI;
- ICS Web Share как основной calendar path.

## 5. Почему Postbox — основной email-кандидат

Yandex Cloud Postbox поддерживает raw MIME через `Content.Raw.Data`. Текущий
`email_control/providers/postbox.py` уже формирует MIME-сообщение, кодирует его
в Base64 и отправляет через Postbox Raw API. Поэтому provider-level вопрос
«можно ли передать произвольную MIME-структуру» для Postbox в основном закрыт.

Открыты другие вопросы:

- как Gmail, Apple Mail и Outlook интерпретируют конкретный iTIP/MIME;
- нужен ли inline `text/calendar`, attachment или оба;
- корректно ли работают `REQUEST`, update и `CANCEL`;
- возникает ли duplicate;
- как письмо попадает в Inbox/Spam;
- какие sender/consent/product формулировки принимаются.

Текущий worker принимает только `transactional-plain-v1` с ключами
`subject`, `text`, `html`. Calendar invitation требует отдельного строгого
шаблона, например:

```text
transactional-calendar-request-v1
```

Нельзя добавлять произвольный raw MIME из browser payload. Server-side producer
получает event ID, перечитывает канонический event snapshot и сам строит MIME.

### 5.1. Предлагаемая MIME-структура

```text
multipart/mixed
├── multipart/alternative
│   ├── text/plain
│   ├── text/html
│   └── text/calendar; method=REQUEST; charset=UTF-8
└── event.ics; Content-Disposition: attachment
```

Research сравнивает также inline-only вариант. `METHOD` внутри VCALENDAR должен
совпадать с MIME `method`.

Минимальный payload:

```text
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//KenigEvents//Calendar Invitation//RU
CALSCALE:GREGORIAN
METHOD:REQUEST
BEGIN:VEVENT
UID:<stable-per-user-event-uid>
SEQUENCE:0
DTSTAMP:<utc>
DTSTART:<utc-or-explicit-tz>
DTEND:<utc-or-explicit-tz>
SUMMARY:<title>
DESCRIPTION:<description + canonical URL>
LOCATION:<location>
ORGANIZER:mailto:<controlled-sender>
ATTENDEE;RSVP=TRUE:mailto:<controlled-recipient>
STATUS:CONFIRMED
END:VEVENT
END:VCALENDAR
```

### 5.2. Update и cancel

- initial: `METHOD:REQUEST`, `SEQUENCE:0`;
- update: тот же `UID`, `SEQUENCE:1+`;
- cancel: тот же `UID`, `METHOD:CANCEL`;
- ambiguous send никогда не повторяется автоматически;
- initial/update/cancel имеют отдельные idempotency keys.

### 5.3. NotiSend

NotiSend остаётся сравнительным исследованием только по явному owner decision.
Действующая email architecture направляет event transactional mail в Postbox;
NotiSend не становится скрытым fallback и не выбирается после ambiguous Postbox
send.

## 6. Тонкий Android Calendar Connector

Connector — отдельное небольшое Android-приложение. Сайт/PWA остаётся основной
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

Пользователь видит системную форму, выбирает календарь и сам нажимает Save.
Connector сообщает «Форма календаря открыта», а не «Событие добавлено».

### 6.1. App identity

- постоянный package name;
- постоянный release signing key;
- `android:autoVerify="true"`;
- `assetlinks.json` с exact certificate fingerprint;
- release build, не debug APK;
- сначала Google Play Internal/Closed Testing;
- direct signed APK — только bounded pilot.

### 6.2. Event payload

App Link не принимает произвольный JSON в query string. Connector получает
bounded first-party payload по opaque token.

```json
{
  "schema": "kenigevents-calendar-connector-v1",
  "event_id": 123,
  "revision": 7,
  "title": "…",
  "starts_at": "…",
  "ends_at": "…",
  "all_day": false,
  "location": "…",
  "description": "…",
  "canonical_url": "…",
  "expires_at": "…"
}
```

Запрещено:

- arbitrary host/redirect;
- `file://`/`content://` из deep link;
- прямая запись без системной формы;
- чтение пользовательского календаря;
- логирование полного event description как analytics.

## 7. Порядок реализации

### M0 — документы и test scaffold

- source requirements остаются byte-identical;
- product strategy;
- scenario registry;
- dynamic current-event resolver;
- GitHub Actions scaffold;
- никаких provider/native/runtime side effects.

### M1 — Push

- data model и idempotent outbox;
- subscription UI;
- T−24/T−1 scheduler;
- lifecycle invalidation;
- Android L2 и real-device L3 acceptance;
- iOS Home Screen PWA acceptance;
- independent feature flag и rollback.

### M2 — Postbox calendar invitation research

- MIME builder contracts;
- Postbox Raw acceptance;
- controlled mailbox roundtrip;
- Gmail/Apple Mail/Outlook matrix;
- update/cancel;
- отдельный release decision.

### M3 — Android connector POC

- Android project;
- verified App Links;
- `ACTION_INSERT`;
- Internal Testing;
- emulator + real-device evidence;
- отдельное решение о публичном распространении.

Fail M2 или M3 не блокирует M1 и не удаляет ICS fallback.

## 8. NO-GO и rollback

NO-GO:

- source requirements недоступны или parity не подтверждена;
- event facts принимаются от browser вместо server-side canonical snapshot;
- duplicate Push возможен при retry/Smart Update;
- permission запрашивается без user gesture;
- Postbox client compatibility объявлена без received MIME/client evidence;
- NotiSend включён как скрытый fallback;
- connector запрашивает calendar write permission;
- App Link не verified;
- debug signer используется как release;
- desktop mobile viewport выдан за Android/iOS acceptance.

Rollback:

```text
Push failure
→ disable producer/sender
→ invalidate pending jobs
→ saved events и ICS остаются

Email research failure
→ disable invitation experiment
→ не повторять ambiguous sends
→ Push + ICS остаются

Connector failure
→ web App Link fallback остаётся
→ install CTA отключается
→ canonical event routes не меняются
```

## 9. Открытые owner decisions

Перед M1:

1. Нужен ли immediate reminder при сохранении менее чем за час?
2. Offsets фиксированы или пользователь может их менять?
3. Изменение только площадки создаёт отдельный Push?
4. Как обрабатывается событие без точного времени?

Перед M2:

5. Какой controlled sender используется для invitation canary?
6. Какие Gmail/iCloud/Outlook mailboxes входят в blocking matrix?
7. Initial-only допустим или update/cancel обязательны для MVP?

Перед M3:

8. Какой host выделяется под App Links?
9. Как называется connector?
10. Pilot только через Play Internal Testing или также direct signed APK?

## 10. Связанные документы

- [`schedule-user-requirements.md`](schedule-user-requirements.md)
- [`pwa-capabilities-lab.md`](pwa-capabilities-lab.md)
- [`release-plan.md`](release-plan.md)
- [`release-autotest-gates.md`](release-autotest-gates.md)
- [`../event-favorites-calendar/README.md`](../event-favorites-calendar/README.md)
- [`../event-email-notifications/README.md`](../event-email-notifications/README.md)
- [`../../operations/email-delivery.md`](../../operations/email-delivery.md)
- [`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md)
