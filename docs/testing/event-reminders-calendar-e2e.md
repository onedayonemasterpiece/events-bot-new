# E2E-сценарии напоминаний и календарной доставки

> **Статус:** test design + executable scaffold; product side effects пока не реализованы  
> **Product strategy:** [`../features/static-site-pages/event-reminders-calendar-strategy.md`](../features/static-site-pages/event-reminders-calendar-strategy.md)  
> **Исходные требования:** [`../features/static-site-pages/schedule-user-requirements.md`](../features/static-site-pages/schedule-user-requirements.md) — не изменяются  
> **Общая стратегия:** [`../operations/static-site-autotest-strategy.md`](../operations/static-site-autotest-strategy.md)  
> **Scenario registry:** [`static-site-autotest-scenarios.v1.yml`](static-site-autotest-scenarios.v1.yml)  
> **Workflow scaffold:** [`.github/workflows/event-reminders-calendar-e2e.yml`](../../.github/workflows/event-reminders-calendar-e2e.yml)

## 1. Решение по слоям

Используются существующие уровни доказательства:

- **L0:** canonical event/ICS/MIME/outbox contracts без браузера;
- **L1:** Chromium journey на exact deployed HTTPS target;
- **L2 Android:** Android Emulator + Chrome + UiAutomator2/native UI;
- **L2 iOS:** iOS Simulator + Mobile Safari + XCUITest/native UI;
- **L3:** физические устройства/device farm для Push background/OEM и реальных
  Gmail/Apple Mail/Calendar integration canaries.

Mobile viewport в desktop Chromium не заменяет Android/iOS. Full event catalog
не запускается на эмуляторах. Один run выбирает ровно одно актуальное событие и
передаёт его всем downstream jobs как immutable test input.

## 2. Актуальное событие вместо hardcoded URL

### 2.1. Resolver contract

`site/e2e/event-reminders/resolve-current-event.mjs` получает:

```text
E2E_TARGET_URL
E2E_EXPECTED_REPO_SHA
E2E_MIN_LEAD_MINUTES       default 90
E2E_MAX_LEAD_DAYS          default 30
E2E_SELECTED_EVENT_PATH
```

Алгоритм:

1. Разрешить exact preview base и получить `preview-build.json`.
2. Сверить полный `repo_sha` с workflow input.
3. Обойти в фиксированном порядке текущие listing routes:
   `zavtra`, `segodnya`, `vyhodnye`, `populyarnoe`, root.
4. Извлечь только same-origin event links внутри того же preview prefix.
5. Для каждой страницы запросить соседний `event.ics`.
6. Развернуть folded ICS lines и извлечь первый `VEVENT`.
7. Отфильтровать:
   - started/past;
   - `STATUS:CANCELLED`;
   - all-day для первого test slice;
   - неизвестный datetime format;
   - отсутствие `UID`, `SUMMARY`, `LOCATION`, `DTSTART`, `DTEND`;
   - lead меньше configured minimum или больше configured maximum.
8. Выбрать детерминированно самое раннее подходящее событие; при равном времени
   использовать event URL.
9. Сохранить SHA-256 ICS и selection evidence.

`selected-event.json` содержит:

```json
{
  "schema_version": 1,
  "selected_at": "…",
  "selection_reason": "earliest_complete_timed_current_event",
  "source_listing_url": "…",
  "event_url": "…",
  "ics_url": "…",
  "ics_sha256": "…",
  "uid": "…",
  "summary": "…",
  "location": "…",
  "starts_at": "…",
  "ends_at": "…",
  "expected_repo_sha": "…",
  "observed_repo_sha": "…",
  "preview_build_id": "…",
  "revalidate_before": "…"
}
```

### 2.2. Revalidation и устаревание

Перед первым side effect каждый protected scenario повторно получает event page
и ICS и сверяет:

- event ещё не начался и не отменён;
- UID и event URL прежние;
- ICS hash/revision соответствует политике сценария;
- deployed repo SHA не изменился.

Если событие устарело **до первого side effect**, resolver может быть запущен
ровно один раз повторно. После отправки Push/email переключение на другой event
запрещено: run завершается `BLOCKED_EVENT_CHANGED_AFTER_SIDE_EFFECT`.

Expired editorial event не удаляет deterministic unit coverage. Формат ICS,
MIME, scheduler и lifecycle дополнительно проверяются frozen fixtures; live
resolver нужен для production-like route/delivery journey.

## 3. Реестр сценариев

| Scenario ID | Статус сейчас | Layers/platforms | Side effects | Blocking policy |
|---|---|---|---|---|
| `event.current_event.selection` | scaffold implemented | L0/L1 browser | none | blocking для остальных сценариев |
| `event.ics.download_contract` | partial existing | L0/L1 browser | download only | release при изменении ICS |
| `event.reminder.push_subscription` | planned | L1/L2 Android/iOS | test subscription | при реализации Push contract |
| `event.reminder.push_delivery` | planned | L1/L2 + L3 canary | protected Push | release blocking после реализации |
| `event.reminder.lifecycle` | planned | L0/L1/L2 sample | isolated revision overlay + Push | release blocking после реализации |
| `event.calendar_email.postbox_mime` | planned | L0 | none | PR blocking при MIME builder change |
| `event.calendar_email.postbox_roundtrip` | planned | server + mailbox | protected one-message sequence | protected release/research gate |
| `event.calendar_email.client_action` | planned | L2/L3 | controlled invitation | research acceptance, not routine PR |
| `event.calendar_connector.android` | planned | L0/L1/L2 Android + L3 | system calendar editor | connector release blocking |
| `event.current_event.mobile_environment` | scaffold implemented | L2 Android/iOS | none | environment diagnostic only |

`planned` не превращается в PASS. Workflow skeleton может доказать resolver и
mobile environment readiness, но должен писать `NOT_IMPLEMENTED` для product
assertions, которых ещё нет.

## 4. `event.current_event.selection`

### Assertions

- exact preview metadata доступна;
- expected и observed full SHA совпали;
- source listing и event link same-origin и внутри одного prefix;
- event page и ICS возвращают `200`;
- start/end валидны и событие не началось;
- UID, summary, location непусты;
- ICS hash сохранён;
- выбор детерминирован;
- в evidence нет bearer query/hash и персональных данных.

### Failure domains

```text
BLOCKED_TARGET_METADATA
FAIL_REPO_SHA_MISMATCH
BLOCKED_NO_CURRENT_EVENT
FAIL_EVENT_ICS_INVALID
FAIL_CROSS_ORIGIN_EVENT
FAIL_EVENT_ALREADY_STARTED
```

## 5. `event.ics.download_contract`

### L0

- CRLF и unfolded semantic fields;
- stable UID для event/revision policy;
- DTSTART/DTEND и timezone;
- canonical URL;
- escaping/folding;
- no cancelled event presented as active;
- content type и filename.

### L1

1. Открыть выбранную event page.
2. Найти calendar/ICS action.
3. Нажать ordinary pointer gesture.
4. Доказать один download с `.ics` и ожидаемым content.
5. Проверить видимый feedback «Файл календаря скачан».
6. Не принимать download за внешний calendar save.

Android/iOS system import не входит в этот сценарий: лаборатория уже доказала,
что браузерный download/share не является переносимым calendar insertion path.

## 6. Push-сценарии

## 6.1. `event.reminder.push_subscription`

1. Открыть выбранное актуальное событие.
2. Сохранить событие test identity.
3. Проверить, что permission ещё не запрошен автоматически.
4. Нажать «Включить напоминания».
5. Принять permission в native UI.
6. Получить одну subscription и server-side binding к test identity/device.
7. Повторное действие не создаёт вторую активную subscription.
8. Revoke/deny даёт честное состояние и не удаляет saved event/ICS.

Android/iOS permission dialog проверяется в native context. DOM-флаг недостаточен.

## 6.2. `event.reminder.push_delivery`

CI не ждёт 24 часа. Семантические kinds остаются production:

```text
event_reminder_24h
event_reminder_1h
```

Но защищённый E2E namespace для фиксированной test identity преобразует их в
короткие deadlines, например:

```text
24h kind → +90 seconds
1h kind  → +30 seconds
```

Это server-side test clock/offset override, а не browser-supplied event time.
Browser передаёт только selected event ID/revision; producer перечитывает
canonical event snapshot.

Assertions:

- созданы ровно два outbox jobs с разными kinds;
- порядок scheduled_at корректен;
- один claim/provider send на kind;
- Android/iOS получает два видимых notifications;
- title/event identity соответствуют selected snapshot;
- click открывает exact event URL;
- app closed/background path проверен;
- повторный scheduler tick не создаёт duplicate;
- expired/revoked subscription завершается typed permanent failure.

### L3 boundary

Эмулятор проверяет permission, UI, click-through и deterministic provider
contract. Финальная background reliability требует physical canary:

- Android OEM/battery state;
- iPhone real APNs/Web Push delivery;
- device locked/background/cold state.

## 6.3. `event.reminder.lifecycle`

Не менять реальное editorial событие. Test control plane создаёт из выбранного
snapshot изолированные revisions:

```text
revision N     original
revision N+1   start + 30 min
revision N+2   cancelled
```

Assertions:

- pending old-revision reminders инвалидированы;
- новая revision создаёт новые offsets;
- ровно один `event_rescheduled`;
- cancellation удаляет ordinary pending reminders;
- ровно один `event_cancelled`;
- same revision replay — no-op;
- notification click открывает event lifecycle page/state;
- test overlay удаляется после run.

## 7. Postbox calendar email

## 7.1. `event.calendar_email.postbox_mime`

Postbox уже принимает raw MIME через `Content.Raw.Data`; тестируем не простую
возможность raw transport, а правильность нашего builder.

Frozen fixture + selected live event:

- `multipart/alternative` или reviewed `multipart/mixed` structure;
- `text/plain` и `text/html` существуют;
- `text/calendar; method=REQUEST` существует;
- MIME method совпадает с VCALENDAR `METHOD`;
- UID стабилен;
- `SEQUENCE` монотонен;
- organizer/attendee server-owned;
- timezone/title/location/canonical URL соответствуют event snapshot;
- CRLF/folding;
- Base64 Postbox body декодируется обратно в byte-identical MIME;
- browser payload не может передать raw MIME/header override;
- prohibited headers (`From`, `Return-Path`, `Message-ID`) server-owned.

Variants для research:

```text
A: inline text/calendar inside multipart/alternative
B: inline calendar + event.ics attachment
C: ordinary METHOD:PUBLISH attachment (negative/control)
```

## 7.2. `event.calendar_email.postbox_roundtrip`

Protected Environment, sequential, controlled mailboxes.

1. Resolver выбирает актуальное событие.
2. Создать один server-side calendar invitation request.
3. Доказать ровно один outbox row/claim/network start.
4. Получить реальный Postbox `MessageId`.
5. Получить authenticated Send/Delivery feedback по существующему contract.
6. Получить письмо через controlled mailbox adapter после checkpoint.
7. Сохранить только redacted MIME structure/field hashes.
8. Проверить REQUEST fields.
9. Отправить update с тем же UID и `SEQUENCE+1`.
10. Отправить CANCEL с тем же UID.
11. Каждый этап имеет отдельный stable idempotency key.
12. Ambiguous delivery не повторяется автоматически.

Один recipient и один UID sequence выполняются последовательно. Параллельные
mail jobs запрещены, пока каждой платформе не выделен независимый mailbox.

## 7.3. `event.calendar_email.client_action`

Матрица:

| Client | Layer | Acceptance |
|---|---|---|
| Gmail Android + Google Calendar | L3 primary | invitation card/action, fields, add/update/cancel/no duplicate |
| Apple Mail + Calendar | L2 diagnostic, L3 final | event recognized, system editor/calendar fields, update/cancel |
| Outlook mobile/web | L2/L3 or protected browser | meeting request, fields, update/cancel |
| Gmail web | L1 | MIME recognition/control baseline |

Ограничения runners:

- OTP Android image `google_apis` пригоден для Chrome/Appium, но не является
  гарантированным Gmail/Google Calendar account environment;
- настоящий Gmail→Google Calendar acceptance выполняется на managed Play image
  с подготовленным test account либо на физическом device farm;
- iOS Simulator может проверить системный UI и web app, но final mail/push
  behavior остаётся L3, если simulator не воспроизводит provider delivery.

Evidence не содержит raw recipient, full message body, auth token или calendar
account. Для client matrix допустимы screenshots только после masking.

## 8. `event.calendar_connector.android`

### L0

- stable package/signing identity contract;
- exact `assetlinks.json` package + certificate fingerprint;
- autoVerify App Link host/path;
- no `READ_CALENDAR`/`WRITE_CALENDAR`/storage permissions;
- payload schema/expiry/size/redirect allowlist;
- canonical event facts server-side.

### L1 fallback

- connector absent: HTTPS route returns valid web fallback;
- ICS action остаётся доступным;
- не возникает redirect loop;
- PWA state сохраняется.

### L2 Android

1. Установить signed connector test APK.
2. Открыть selected event в Chrome и затем в installed PWA.
3. Нажать connector CTA.
4. Проверить verified App Link открыл package, а не chooser/browser.
5. Connector fetches exact selected event payload.
6. Запускается `ACTION_INSERT` с `CalendarContract.Events.CONTENT_URI`.
7. Native editor содержит title/start/end/location/description/link.
8. Save и Cancel не приводят к crash.
9. Connector не сообщает «saved» только по Activity launch.
10. Malformed/expired token fail closed.

Финальный OEM handler matrix остаётся L3.

## 9. GitHub Actions scaffold

Workflow имеет inputs:

```text
target_url
expected_repo_sha
platform: resolver | browser | android | ios | all
```

Jobs:

1. `resolve-current-event` — blocking L0/L1 input selection;
2. `browser-contract` — revalidation и ICS route smoke;
3. `android-current-event-smoke` — API 35 Pixel 7 Chrome environment;
4. `ios-current-event-smoke` — macOS 15, Xcode 16.4, iPhone 16/iOS 18.5;
5. product Push/email/connector jobs добавляются только после runtime contracts.

Scaffold mobile jobs используют exact environment choices, отлаженные в
`feature/mobile-otp-autotest-20260802`, но не копируют OTP/mail side effects.
После merge общей Appium platform layer следует переиспользовать её adapters,
a не поддерживать второй набор capabilities.

Scaffold выдаёт только:

```text
PASS                 resolver/browser/current-route smoke
NOT_IMPLEMENTED      Push/email-client/connector product journey
BLOCKED               emulator/runtime/target problem
```

Environment smoke не считается Push/calendar PASS.

## 10. Evidence

Один package format:

```text
evidence/
├── selected-event.json
├── qa-summary.json
├── run.json
├── scenarios.jsonl
├── junit.xml
├── device.json
├── screenshots/
├── native-ui/
├── network.sanitized.jsonl
├── mail-mime.sanitized.json
└── redaction-audit.json
```

Обязательные metadata:

- full repo SHA;
- preview build ID;
- selected event URL/ICS hash/UID hash;
- platform/OS/browser/device;
- scenario ID;
- target and selection reason;
- side-effect checkpoint;
- terminal status/failure domain;
- redaction result.

## 11. Release policy

NO-GO после реализации соответствующего channel, если:

- current event selection не воспроизводится;
- target SHA mismatch;
- browser supplies authoritative event facts;
- Push duplicate или stale-revision delivery;
- Postbox MIME/client behavior заявлено без roundtrip evidence;
- Gmail/iOS behavior выдано за проверенное только по desktop browser;
- App Link/Calendar editor не проверены в native UI;
- required L2/L3 result `BLOCKED`/`NOT_IMPLEMENTED`;
- evidence не прошло redaction.
