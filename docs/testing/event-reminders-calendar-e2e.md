# E2E-сценарии напоминаний и календарной доставки

> **Статус:** source parity с `schedule-user-requirements.md` подтверждён; test design + executable environment scaffold; product side effects пока не реализованы  
> **Product strategy:** [`../features/static-site-pages/event-reminders-calendar-strategy.md`](../features/static-site-pages/event-reminders-calendar-strategy.md)  
> **Исходные требования:** [`../features/static-site-pages/schedule-user-requirements.md`](../features/static-site-pages/schedule-user-requirements.md) — не изменяются  
> **Общий release plan:** [`../features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md), Stage 14  
> **Общая тестовая стратегия:** [`../operations/static-site-autotest-strategy.md`](../operations/static-site-autotest-strategy.md)  
> **Scenario registry:** [`static-site-autotest-scenarios.v1.yml`](static-site-autotest-scenarios.v1.yml)  
> **Workflow scaffold:** [`.github/workflows/event-reminders-calendar-e2e.yml`](../../.github/workflows/event-reminders-calendar-e2e.yml)

## 1. Слои доказательства

- **L0:** event/ICS/MIME/outbox/preferences/calendar-view contracts без браузера;
- **L1:** Chromium journey на exact deployed HTTPS target;
- **L2 Android:** Android Emulator + Chrome/Appium UiAutomator2 + native UI;
- **L2 iOS:** iOS Simulator + Mobile Safari/Appium XCUITest + native UI;
- **L3:** физические устройства/device farm для background Push, OEM/calendar apps и реальных mail clients.

Mobile viewport в desktop Chromium не заменяет Android/iOS. Playwright WebKit
не заменяет Mobile Safari/system UI. На эмуляторах не обходится полный каталог:
один run выбирает одно актуальное событие и передаёт его всем downstream jobs.

## 2. Актуальное событие вместо hardcoded URL

### 2.1. Resolver

`site/e2e/event-reminders/resolve-current-event.mjs` получает:

```text
E2E_TARGET_URL
E2E_EXPECTED_REPO_SHA
E2E_MIN_LEAD_MINUTES       default 90
E2E_MAX_LEAD_DAYS          default 30
E2E_SELECTED_EVENT_PATH
```

Алгоритм:

1. Определить exact preview base и получить `preview-build.json`.
2. Сверить полный `repo_sha`.
3. Обойти `zavtra`, `segodnya`, `vyhodnye`, `populyarnoe`, root.
4. Извлечь только same-origin event links внутри того же prefix.
5. Для каждой страницы получить adjacent `event.ics`.
6. Развернуть folded ICS lines и извлечь первый `VEVENT`.
7. Отбросить started/past, `STATUS:CANCELLED`, all-day первого slice,
   неизвестный datetime и записи без UID/SUMMARY/LOCATION/DTSTART/DTEND.
8. Применить lead-time window.
9. Детерминированно выбрать earliest complete timed current event.
10. Сохранить ICS hash и build identity.

`selected-event.json`:

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

Protected backend дополнительно разрешает selected URL/UID в canonical
`event_id`, revision и `event_city`. Browser location string не является
источником city classification.

### 2.2. Revalidation

Перед первым side effect повторно проверяются page и ICS:

- событие ещё не началось и не отменено;
- URL/UID прежние;
- event revision/ICS hash допустимы для сценария;
- deployed repo SHA прежний.

До первого side effect разрешён один повторный resolver run. После первой
Push/email отправки переключение на другое событие запрещено:

```text
BLOCKED_EVENT_CHANGED_AFTER_SIDE_EFFECT
```

Frozen fixtures сохраняют deterministic coverage, когда редакционные события
устаревают. Live resolver нужен для production-like integration journey.

## 3. Реестр сценариев

| Scenario ID | Статус | Layers/platforms | Side effects |
|---|---|---|---|
| `event.current_event.selection` | scaffold implemented | L0/L1 browser | none |
| `event.current_event.mobile_environment` | scaffold implemented | L2 Android/iOS | none |
| `event.saved_calendar_view` | planned | L0/L1 browser | idempotent saved-event fixture |
| `event.ics.download_contract` | partial existing | L0/L1 browser | download only |
| `event.reminder.push_subscription` | planned | L1/L2 Android/iOS | test subscription |
| `event.reminder.preferences` | planned | L0/L1/L2 | preference updates |
| `event.reminder.push_delivery` | planned | L1/L2 + L3 | protected Push |
| `event.reminder.lifecycle` | planned | L0/L1/L2 | revision overlay + Push |
| `event.calendar_email.postbox_mime` | planned | L0 | none |
| `event.calendar_email.postbox_roundtrip` | planned | server/mailbox | protected REQUEST/update/CANCEL |
| `event.calendar_email.client_action` | planned | L1/L2/L3 | controlled invitation |
| `event.calendar_connector.android` | planned | L0/L1/L2/L3 Android | system editor |

`planned` не превращается в PASS. Environment scaffold обязан возвращать
`NOT_IMPLEMENTED` для отсутствующих product assertions.

## 4. `event.current_event.selection`

Assertions:

- exact build metadata и full SHA;
- current listing/event/ICS в одном origin/prefix;
- HTTP 200;
- future start/end;
- непустые UID/summary/location;
- deterministic selection;
- sanitized evidence.

Failure domains:

```text
BLOCKED_TARGET_METADATA
FAIL_REPO_SHA_MISMATCH
BLOCKED_NO_CURRENT_EVENT
FAIL_EVENT_ICS_INVALID
FAIL_CROSS_ORIGIN_EVENT
FAIL_EVENT_ALREADY_STARTED
```

## 5. `event.saved_calendar_view`

### Dynamic integration

1. Resolver выбирает актуальное событие.
2. Сценарий сохраняет его idempotent test identity.
3. Открывает календарный вид «Избранного».
4. Проверяет правильную date group.
5. Проверяет строку:

```text
HH:MM–HH:MM | Мероприятие | локация
```

6. Повторный save не создаёт duplicate.
7. Отключение Push не удаляет строку.

### Frozen contracts

- несколько дат;
- несколько событий в один день;
- хронологическая сортировка;
- favorite + calendar source без duplicate;
- unknown end/location typed fallback;
- reschedule перемещает строку;
- cancellation видима;
- честный empty state.

Live current event обеспечивает актуальный integration input, а сложные
группировки не зависят от редакционного каталога конкретного дня.

## 6. `event.ics.download_contract`

### L0

- CRLF и unfolded semantic fields;
- stable UID;
- DTSTART/DTEND/timezone;
- canonical URL;
- escaping/folding;
- active/cancelled semantics;
- content type/filename.

### L1

1. Открыть selected event.
2. Нажать ordinary ICS CTA.
3. Получить ровно один `.ics` download.
4. Сверить semantic content с snapshot.
5. Проверить visible feedback.
6. Не считать download внешним calendar save.

System import в Android/iOS не входит: browser download/share не является
переносимым insertion contract.

## 7. Push scenarios

### 7.1. `event.reminder.push_subscription`

1. Открыть selected event.
2. Сохранить test identity.
3. Доказать отсутствие автоматического permission prompt.
4. Нажать «Включить напоминания».
5. Принять native permission dialog.
6. Получить одну device-bound subscription.
7. Повторное действие не создаёт вторую active subscription.
8. Deny/revoke не удаляет saved event, calendar view и ICS.

Native dialog обязателен; DOM-флаг недостаточен.

### 7.2. `event.reminder.preferences`

Проверяются независимые controls:

- all Push on/off;
- T−24h on/off;
- city-relative near reminder on/off;
- настройка одного event не меняет другое;
- disabled type не создаёт pending job;
- повторный save не сбрасывает preferences;
- all-off сохраняет saved event/calendar view/ICS.

Поведение lifecycle kinds и смены текущего города остаётся owner decision до
implementation и не подменяется незафиксированным default.

### 7.3. `event.reminder.push_delivery`

Production kinds:

```text
event_reminder_24h
event_reminder_1h_current_city
event_reminder_3h_other_city
```

Один protected run выполняет две изолированные profile branches на одном event:

```text
A. current city == event city
   → T−24h + T−1h_current_city
   → T−3h_other_city отсутствует

B. current city != event city
   → T−24h + T−3h_other_city
   → T−1h_current_city отсутствует
```

CI не ждёт реальные часы. Server-side E2E namespace отображает semantic kinds
на короткие deadlines, например:

```text
T−24h → +90 s
T−3h  → +45 s
T−1h  → +30 s
```

Browser не задаёт event time, revision или city classification.

Assertions:

- ровно два jobs на profile branch, не три;
- near kinds взаимоисключающие;
- unknown city даёт typed fail-closed state;
- correct scheduling order;
- one claim/provider send per kind;
- два visible notifications;
- title/event identity совпадают;
- click открывает exact event URL;
- closed/background path;
- scheduler replay не создаёт duplicate;
- expired subscription даёт permanent typed failure.

L2 проверяет permission/UI/click-through. Финальная background reliability — L3:
Android OEM/battery и real iPhone locked/background/cold canary.

### 7.4. `event.reminder.lifecycle`

Editorial event не мутируется. Из snapshot создаются isolated revisions:

```text
N     original
N+1   start + 30 min
N+2   cancelled
```

Assertions:

- old pending jobs invalidated;
- N+1 создаёт T−24h + ровно один city-relative near kind;
- один `event_rescheduled`;
- N+2 удаляет ordinary pending reminders;
- один `event_cancelled`;
- same revision replay — no-op;
- click открывает lifecycle state;
- overlay очищается.

## 8. Postbox calendar email

### 8.1. `event.calendar_email.postbox_mime`

Postbox уже принимает Raw MIME; тестируется наш strict builder.

Frozen fixture + selected current event:

- reviewed multipart structure;
- text/plain + text/html;
- `text/calendar; method=REQUEST`;
- MIME method == VCALENDAR METHOD;
- stable UID;
- monotonic SEQUENCE;
- server-owned organizer/attendee;
- correct timezone/title/location/URL;
- CRLF/folding;
- Base64 Postbox body roundtrip;
- browser не может передать raw MIME/header override;
- From/Return-Path/Message-ID server-owned.

Research variants:

```text
A inline text/calendar
B inline + event.ics attachment
C METHOD:PUBLISH attachment control
```

### 8.2. `event.calendar_email.postbox_roundtrip`

Protected Environment, sequential mailbox/UID sequence:

1. Resolve/revalidate current event.
2. Create one server-side invitation request.
3. Prove one outbox row/claim/network start.
4. Receive real Postbox `MessageId`.
5. Receive authenticated Send/Delivery feedback.
6. Read controlled mailbox after checkpoint.
7. Store only redacted MIME structure/hashes.
8. Validate REQUEST.
9. Send update with same UID and SEQUENCE+1.
10. Send CANCEL with same UID.
11. Separate idempotency key per stage.
12. Never auto-retry ambiguous delivery.

Parallel mail jobs запрещены, пока платформы не имеют независимые mailboxes.

### 8.3. `event.calendar_email.client_action`

| Client | Layer | Acceptance |
|---|---|---|
| Gmail web | L1 | MIME recognition baseline |
| Gmail Android + Google Calendar | L3 primary | action, fields, add/update/cancel/no duplicate |
| Apple Mail + Calendar | L2 diagnostic, L3 final | recognition, fields, update/cancel |
| Outlook web/mobile | L1/L2/L3 | meeting request lifecycle |

`google_apis` Android image пригоден для Chrome/Appium, но не доказывает Gmail +
Google Calendar account environment. Final Gmail acceptance требует managed Play
image с test account либо device farm. iOS Simulator — diagnostic; final mail and
background Push may require physical iPhone.

Evidence исключает raw recipient/body/token/account. Screenshots — после masking.

## 9. `event.calendar_connector.android`

### L0

- stable package/signing identity;
- exact assetlinks package/fingerprint;
- autoVerify host/path;
- no READ/WRITE_CALENDAR/storage permissions;
- payload schema/expiry/size/redirect allowlist;
- canonical server-side event facts.

### L1 fallback

- connector absent → valid HTTPS fallback;
- ICS доступен;
- no redirect loop;
- PWA state preserved.

### L2 Android

1. Install signed test APK.
2. Open selected event in Chrome and installed PWA.
3. Tap connector CTA.
4. Verified App Link opens package, not browser/chooser.
5. Connector fetches exact selected event payload.
6. Launch `ACTION_INSERT` with `CalendarContract.Events.CONTENT_URI`.
7. Native editor contains title/start/end/location/description/link.
8. Save and Cancel do not crash.
9. Activity launch is not reported as confirmed save.
10. Malformed/expired token fails closed.

Final OEM handler matrix remains L3.

## 10. GitHub Actions scaffold

Workflow inputs:

```text
target_url
expected_repo_sha
platform: resolver | browser | android | ios | all
```

Current jobs:

1. `resolve_current_event`;
2. `browser_contract`;
3. `android_current_event_smoke` — API 35, Pixel 7, Chrome;
4. `ios_current_event_smoke` — macOS 15, Xcode 16.4, iPhone 16/iOS 18.5;
5. `summary`.

Environment choices reuse the OTP mobile work. After shared Appium adapters are
merged, this suite must reuse them instead of maintaining divergent capabilities.

Current truthful outcomes:

```text
PASS             resolver/browser/current-route smoke
NOT_IMPLEMENTED  Push/email-client/connector product journey
BLOCKED          emulator/runtime/target problem
```

Environment boot/open is not Push/calendar PASS.

## 11. Evidence

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

Metadata:

- full repo SHA/build ID;
- selected event URL, ICS hash, UID hash;
- platform/OS/browser/device;
- scenario ID;
- city branch and server-owned city decision reference where relevant;
- side-effect checkpoint;
- terminal status/failure domain;
- redaction result.

## 12. Release policy

NO-GO после реализации соответствующего channel, если:

- hardcoded/expired URL вместо current-event resolver;
- target SHA mismatch;
- selected event изменился после first side effect;
- browser supplies authoritative event time/revision/current city;
- T−1h and T−3h near jobs coexist;
- saved calendar view violates date/time/event/location contract;
- disabled reminder type still creates job;
- duplicate/stale-revision Push;
- Postbox acceptance declared client recognition;
- Gmail/iOS claimed only from desktop browser;
- App Link/editor not asserted in native UI;
- required L2/L3 is BLOCKED/NOT_IMPLEMENTED;
- environment smoke called product PASS;
- evidence failed redaction.
