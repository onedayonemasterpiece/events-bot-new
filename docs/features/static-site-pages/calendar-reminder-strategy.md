# Стратегия «Не пропустить»: push, ICS, email-приглашение и Android connector

> **Дата решения:** 2026-08-02  
> **Статус:** `GO_TO_RESEARCH_AND_PROTOTYPE`; production email-invite и Android APK остаются `NO-GO` до закрытия своих acceptance gates  
> **Scope:** статический сайт/PWA, сохранённые события, Web Push, существующий ICS export, исследование NotiSend iMIP и тонкий Android calendar connector  
> **Не входит:** Google Calendar OAuth/API, подписные календари, Google Calendar web template как основной UX, iOS native application, TWA/Capacitor rollout

## 1. Источник требований и граница этого документа

Неизменяемый источник пользовательских требований:

`docs/features/static-site-pages/schedule-user-requirements.md`

Этот файл не должен изменяться в рамках календарного track. Настоящий документ:

- переводит исходные пользовательские требования и зафиксированные владельцем решения в product/architecture/research contract;
- не заменяет и не исправляет исходный файл;
- при расхождении уступает исходным пользовательским требованиям;
- должен пройти read-only сверку с исходным файлом перед merge и перед реализацией.

На момент создания этой ветки исходный файл ещё не был доступен в GitHub-срезе. Поэтому ниже зафиксированы только решения, явно подтверждённые владельцем в задаче:

1. Web Push — принимается как основной канал напоминаний.
2. Google Calendar через OAuth — не входит в план.
3. Календарное приглашение через NotiSend — только исследование и лабораторная проверка.
4. Подписные календари — не входят в план.
5. Существующее скачивание ICS сохраняется как ручной fallback.
6. Для Android исследуется тонкое подписанное приложение-коннектор.

## 2. Продуктовый результат

Главное обещание пользователю — **не пропустить событие**, а не гарантировать запись в произвольный внешний календарь.

Целевой пользовательский путь:

```text
Не пропустить
├── сохранить событие
├── включить push за сутки и за час
├── получать push при переносе или отмене
└── при необходимости выбрать календарный канал
    ├── скачать ICS
    ├── отправить календарное приглашение на email — после успешного исследования
    └── открыть системную форму календаря через Android connector — после установки APK
```

Это снимает календарь с роли единственной точки отказа. Пользователь получает полезный результат уже после сохранения и включения push, даже если его календарь не принимает ICS.

## 3. Зафиксированная матрица решений

| Канал | Решение | Роль |
|---|---|---|
| Web Push | `GO_TO_IMPLEMENTATION` | основной способ не пропустить событие |
| ICS download | `KEEP` | универсальный ручной fallback без обещания автоматического импорта |
| NotiSend calendar invitation | `GO_TO_LAB` | кроссплатформенный email fallback, если MIME и клиенты пройдут проверку |
| Android thin connector APK | `GO_TO_PROTOTYPE` | нативное открытие системной формы календаря для установивших connector |
| Google Calendar OAuth/API | `OUT_OF_SCOPE` | не реализовывать в этом track |
| Google Calendar web template | `REJECTED_AS_PRIMARY` | мобильный UX не принят владельцем; не показывать основной кнопкой |
| Subscription/webcal calendar | `OUT_OF_SCOPE` | не реализовывать |
| Browser `intent:` | `REJECTED` | лабораторная проверка не дала рабочего Android contract |
| ICS через Web Share | `REJECTED_AS_CALENDAR_PATH` | Share Sheet не показал календарь; можно применять только как обычный file share вне основного UX |
| Фоновое скачивание и последующее открытие ICS из PWA | `REJECTED` | PWA не получает системный URI скачанного файла и не может надёжно открыть его в другом приложении |

## 4. Что доказала PWA-лаборатория

Ручная проверка установленной PWA дала следующие product facts:

- установка PWA возможна, но install/reinstall flow на Android требует отдельного системного acceptance;
- локальные уведомления и push-сценарий сработали;
- ICS Web Share не предложил пригодного сценария добавления в календарь;
- browser `intent:` не открыл системную форму календаря;
- ICS download на Android может пройти почти незаметно, после чего пользователь теряет файл в Downloads.

Эти факты запрещают возвращать Web Share или browser intent в основной календарный CTA без нового независимого evidence.

## 5. Общая архитектура

```text
Fly SQLite — canonical event facts/revision/lifecycle
             │
             ▼
Supabase — owner-scoped saved event, push subscription,
           reminder schedule/delivery, consent and idempotency
             │
             ├── Web Push sender
             ├── ICS static/export artifact
             ├── isolated NotiSend invitation lab
             └── signed Android connector payload endpoint
```

Основные инварианты:

- браузер не может передавать произвольные title/date/location как доверенные server facts;
- server-side producer повторно читает актуальную event revision из Fly SQLite;
- один user + event имеет одно durable saved-event состояние;
- сохранение события не означает email consent;
- скачивание ICS не доказывает, что пользователь импортировал событие;
- push, email invitation и connector — независимые delivery channels одной saved-event сущности;
- отмена/перенос события инвалидируют старые scheduled reminders.

Существующий контракт saved events остаётся каноническим:
[`Event favorites and calendar`](../event-favorites-calendar/README.md).

## 6. Основной track: Web Push

### 6.1. Продуктовый контракт

Основная CTA:

```text
Не пропустить
```

После действия:

```text
Событие сохранено
Напоминания:
✓ за сутки
✓ за час
✓ при переносе или отмене
```

Notification permission запрашивается только после явного пользовательского действия и понятного pre-prompt. Автоматический запрос при первом открытии страницы запрещён.

### 6.2. Расписание

- Если событие сохранено раньше `T−24h`, поставить напоминания `T−24h` и `T−1h`.
- Если сохранено между `T−24h` и `T−1h`, поставить только `T−1h`.
- Если сохранено позже `T−1h`, не отправлять искусственное немедленное countdown-уведомление; показать только in-app confirmation. Lifecycle alerts остаются активными.
- При отмене отправить один приоритетный push и отменить все будущие reminders.
- При изменении даты/времени отправить один push с прежним и новым временем, затем пересчитать reminders.
- При существенном изменении места отправить lifecycle push, если пользователь всё ещё сохраняет событие.

Web Push является best-effort delivery, а не точным будильником. UI не обещает доставку в конкретную секунду.

### 6.3. Идемпотентность

Логический idempotency key:

```text
(user_id, event_id, event_revision, notification_kind, scheduled_at)
```

Обязательные свойства:

- повторное сохранение не создаёт второй schedule;
- новый event revision отменяет pending rows старой revision;
- unsave удаляет будущие countdown reminders;
- ambiguous sender result не запускает слепой повтор без provider/browser-safe политики;
- push click открывает canonical event URL и не создаёт новое сохранение.

### 6.4. Subscription lifecycle

Нужно поддержать:

- одну или несколько device subscriptions на owner identity;
- ротацию/истечение endpoint;
- terminal removal после provider `404/410`;
- logout/account purge;
- отключение только reminders без удаления favorite;
- отдельное отключение lifecycle alerts, если продукт это разрешит;
- PII-free operational counters.

VAPID private key и sender credentials находятся только на сервере.

### 6.5. Push acceptance

До production enablement обязательны:

- Android Chrome installed PWA: subscribe, background delivery, click-through;
- iOS Home Screen PWA: subscribe, delivery, click-through;
- `default`, `granted`, `denied` permission states;
- T−24/T−1 scheduling against `Europe/Kaliningrad`;
- late-save matrix;
- cancel/reschedule invalidation;
- duplicate and multi-device tests;
- expired subscription cleanup;
- delivery when event page is closed;
- sanitized evidence без subscription endpoint/keys.

## 7. ICS download — сохраняемый fallback

Существующий ICS export остаётся доступным.

### 7.1. Обязательный UX

До скачивания показывать честный текст:

> Скачается файл календаря. На Android он может попасть в «Загрузки» без заметного окна. Откройте файл из уведомления загрузки или из приложения «Файлы».

После запуска скачивания показывать in-app state:

```text
Файл календаря отправлен в загрузки
[Скачать ещё раз]
[Открыть инструкцию]
```

Нельзя показывать `Добавлено в календарь` или считать download успешным calendar save.

### 7.2. Технический контракт

- stable UID;
- корректная `Europe/Kaliningrad` timezone/UTC projection;
- CRLF, escaping и line folding;
- понятное имя файла с bounded slug/date;
- повторное скачивание из saved-events page;
- корректные `Content-Type` и `Content-Disposition`;
- lifecycle metadata для отменённого/перенесённого события согласно принятому ICS contract.

### 7.3. Что запрещено

- фоново скачивать ICS без отдельного действия пользователя;
- считать Cache Storage или IndexedDB системной папкой Downloads;
- обещать программное открытие скачанного файла;
- использовать Web Share как основной календарный путь;
- измерять download как подтверждённый импорт.

## 8. Исследование: календарное приглашение через NotiSend

### 8.1. Исследовательский вопрос

> Может ли существующий NotiSend SMTP-контур без потери MIME доставить настоящее iMIP/iTIP calendar invitation, которое Gmail/Apple Mail/Outlook показывают как понятное действие Add/Accept, а не как потерянное вложение?

Публичные материалы NotiSend подтверждают SMTP/API transactional delivery и attachments, но не гарантируют сохранение произвольной `text/calendar; method=REQUEST` MIME-части. Поэтому production решение нельзя принимать без raw-message evidence.

### 8.2. Граница с действующей email-архитектурой

Канонический event-lifecycle email сейчас закреплён за Yandex Cloud Postbox, а NotiSend имеет ограниченные отдельные роли. Исследование:

- не меняет production provider routing;
- не включает NotiSend как скрытый fallback;
- не использует recommendation consent;
- не импортирует обычных пользователей в marketing/contact list;
- выполняется только на controlled recipients;
- после успеха требует отдельного owner decision и изменения:
  - [`Transactional event email`](../event-email-notifications/README.md);
  - [`Email delivery operations`](../../operations/email-delivery.md).

### 8.3. Тестовые MIME-варианты

#### N1 — attachment baseline

```text
multipart/mixed
├── text/plain
├── text/html
└── event.ics; METHOD:PUBLISH
```

Цель: зафиксировать текущий attachment UX, не считать его целевым invitation flow.

#### N2 — inline calendar request

```text
multipart/alternative
├── text/plain
├── text/html
└── text/calendar; charset=UTF-8; method=REQUEST
```

ICS:

```text
METHOD:REQUEST
UID:<stable-user-event-uid>
SEQUENCE:0
ORGANIZER;CN=KenigEvents:mailto:<verified-sender>
ATTENDEE;RSVP=FALSE:mailto:<controlled-recipient>
STATUS:CONFIRMED
```

#### N3 — request + attachment compatibility

```text
multipart/mixed
└── multipart/alternative
    ├── text/plain
    ├── text/html
    └── text/calendar; method=REQUEST
└── event.ics
```

#### N4 — update

- тот же `UID`;
- повышенный `SEQUENCE`;
- изменённые start/end/location;
- `METHOD:REQUEST`.

#### N5 — cancel

- тот же `UID`;
- повышенный `SEQUENCE`;
- `METHOD:CANCEL`;
- `STATUS:CANCELLED`.

Отдельно сравнить `RSVP=FALSE` и `RSVP=TRUE`, чтобы выбрать UX без лишних ответов пользователю и без ложного представления KenigEvents как официального организатора мероприятия.

### 8.4. Клиентская матрица

Минимум:

| Клиент | Платформа |
|---|---|
| Gmail | Android |
| Gmail | web |
| Apple Mail + Calendar | iOS |
| Outlook | Android или iOS |
| Outlook | web |
| Яндекс Почта | Android/web |
| Mail.ru | Android/web |

Для каждого варианта фиксировать:

- видит ли клиент calendar invitation;
- есть ли явная Add/Accept action;
- открывается ли системный/встроенный календарный UX;
- создаётся ли запись после действия;
- правильны ли title/date/timezone/location/description;
- не появляется ли duplicate;
- обновляет ли N4 существующую запись;
- отменяет/маркирует ли N5;
- показывает ли клиент только обычный attachment;
- raw MIME после доставки;
- DKIM/SPF/DMARC и From/Organizer alignment.

### 8.5. NotiSend provider checks

- отправка именно через SMTP, чтобы сформировать полный MIME;
- provider acceptance/message id;
- сохраняется ли `Content-Type: text/calendar`;
- сохраняется ли `method=REQUEST`;
- не удаляется ли `ORGANIZER`/`ATTENDEE`;
- не перепаковывается ли `multipart/alternative`;
- создаёт ли transactional SMTP контакт и расходует ли лимит subscriber plan;
- разрешено ли произвольное calendar MIME официальной политикой провайдера;
- ответ поддержки NotiSend сохранён как evidence.

Для получения входящего raw source и redacted evidence следует переиспользовать существующий `.codex/skills/kenigevents-email-roundtrip/` там, где он подходит.

### 8.6. Success criteria

Исследование может получить `GO_TO_IMPLEMENTATION`, только если одновременно:

1. NotiSend сохраняет calendar MIME и метод без смыслового изменения.
2. Gmail Android и Apple Mail iOS дают явное календарное действие, а не только файл.
3. Минимум один Outlook-клиент проходит request/update/cancel.
4. Timezone и event fields не искажаются.
5. Повторная доставка с тем же UID/SEQUENCE не создаёт очевидных дублей.
6. Нет скрытого marketing consent/contact-list side effect.
7. Известна безопасная provider retry/idempotency политика.
8. Отправитель и текст честно объясняют, что это напоминание KenigEvents, а не официальное приглашение организатора события.

### 8.7. NO-GO criteria

- NotiSend удаляет или преобразует `text/calendar`;
- основные мобильные клиенты показывают только неприметное вложение;
- update/cancel создают дубли;
- SMTP требует marketing subscription;
- невозможно получить/проверить raw MIME;
- результат зависит от ручной настройки, неприемлемой для обычного пользователя;
- нет контролируемого поведения после ambiguous send.

### 8.8. Research deliverable

Создать отдельный отчёт:

`docs/reports/calendar-invitation-notisend-lab-YYYY-MM-DD.md`

В нём должны быть exact MIME fixtures/hashes, provider message ids без адресов, raw-source conclusions, client matrix, screenshots и итог `GO | NARROW | NO-GO`.

## 9. Прототип: тонкий Android calendar connector

### 9.1. Продуктовая роль

Connector устанавливают только пользователи, которым нужен нативный системный календарь. PWA и сайт остаются основной актуальной версией продукта.

Connector не должен становиться вторым интерфейсом афиши. Его минимальная самостоятельная ценность:

- открыть системную форму добавления события;
- показать, что connector официально связан с KenigEvents;
- показать версию, privacy note и ссылку «Открыть афишу»;
- дать понятный fallback, если календарного обработчика нет.

### 9.2. Целевой flow

```text
PWA / browser
  → https://calendar.kenigevents.ru/add/<signed-token>
      ├── APK не установлен → HTTPS fallback page
      │   ├── включить push
      │   ├── скачать ICS
      │   └── установить connector
      └── APK установлен + App Link verified
          → CalendarConnectorActivity
          → resolve/verify canonical event payload
          → Intent.ACTION_INSERT + CalendarContract.Events.CONTENT_URI
          → системная форма календаря
```

Для открытия формы не нужен ICS и не нужен `FileProvider`. Connector должен использовать Android `ACTION_INSERT` напрямую. Пользователь сам выбирает календарь и подтверждает Save; `WRITE_CALENDAR` не запрашивается.

### 9.3. Package и подпись

Предварительный package name:

```text
ru.kenigevents.calendarconnector
```

Требования:

- один стабильный release signing key на весь lifecycle;
- release keystore никогда не хранится в Git;
- SHA-256 signing fingerprint публикуется в `assetlinks.json`;
- приложение собирается reproducibly из pinned Gradle/SDK dependencies;
- debug и release package/signing не смешиваются;
- package регистрируется/верифицируется по актуальным требованиям Android Developer Verification;
- update APK обязан быть подписан тем же ключом.

### 9.4. Verified App Links

На connector host публикуется:

```text
https://calendar.kenigevents.ru/.well-known/assetlinks.json
```

Он должен:

- отвечать `200` без redirect;
- иметь `application/json`;
- содержать exact package name;
- содержать SHA-256 fingerprint release certificate;
- проходить Android domain verification.

Приложение принимает только allowlisted HTTPS path `/add/*` своего домена. Custom scheme не является основным входом.

### 9.5. Signed payload

URL не должен содержать произвольные event fields или PII. Token минимум связывает:

```text
event_id
event_revision
expires_at
nonce_or_scope
signature
```

Activity получает canonical payload только с allowlisted endpoint и проверяет:

- HTTPS host;
- token signature/expiry;
- schema version;
- event id/revision;
- start/end validity;
- bounded text lengths;
- timezone;
- canonical event URL.

Connector не принимает arbitrary callback URL или caller-provided calendar intent fields.

### 9.6. Calendar intent payload

Передавать:

- `CalendarContract.EXTRA_EVENT_BEGIN_TIME`;
- `CalendarContract.EXTRA_EVENT_END_TIME`;
- `CalendarContract.Events.TITLE`;
- `CalendarContract.Events.DESCRIPTION`;
- `CalendarContract.Events.EVENT_LOCATION`;
- optional all-day flag только при canonical all-day event;
- canonical event URL внутри description отдельной строкой.

До запуска проверить `resolveActivity`; при отсутствии handler показать экран с ICS/PWA fallback.

### 9.7. Распространение

Исследовать два режима:

1. **Controlled direct APK** с собственного HTTPS-сайта — для владельца/команды и малого пилота; требуется системное разрешение на установку из источника.
2. **Google Play Internal/Closed Testing** — предпочтительный acceptance/pilot путь без unknown-source friction.

Public Play/RuStore rollout не входит в первый prototype decision.

### 9.8. Android acceptance matrix

Минимум:

- Pixel/Google Calendar;
- Samsung + системный/Google Calendar;
- Xiaomi/MIUI при наличии устройства;
- Android 10/12/14/15 или ближайшая доступная стратифицированная выборка;
- ссылка из Chrome tab;
- ссылка из установленной PWA;
- установленный connector;
- connector отсутствует;
- calendar handler отсутствует/disabled;
- offline/error response.

Проверки:

1. Verified App Link открывает connector без browser chooser.
2. Без APK открывается HTTPS fallback, а не dead link.
3. System calendar form содержит exact canonical fields.
4. Отмена формы не создаёт событие.
5. Save создаёт событие в выбранном пользователем календаре.
6. Connector не запрашивает read/write calendar permission.
7. Не возникает двойного запуска при repeated tap.
8. Истёкший/подделанный token отклоняется.
9. Release APK устанавливается и обновляется тем же signing key.
10. Приложение имеет понятное имя/иконку и видно как официальный KenigEvents connector.

### 9.9. Ограничения, которые нужно показать честно

- Connector открывает форму, но не получает надёжного provider event id после сохранения.
- Он не может гарантировать последующее update/delete созданной записи.
- Переносы и отмены продолжают доставляться через push.
- Пользователь должен отдельно установить APK.
- Direct website install имеет sideload friction.
- iOS в этот prototype не входит.

### 9.10. Prototype deliverable

Создать отдельный Android project и отчёт:

```text
android/calendar-connector/
docs/reports/android-calendar-connector-poc-YYYY-MM-DD.md
```

Отчёт должен содержать package/signing fingerprint без секретов, exact App Link host/path, device matrix, screenshots/video, launcher/app identity, form fields и итог `GO | NARROW | NO-GO`.

## 10. Что сейчас не проектируем

- Google OAuth consent/token storage/Calendar API;
- Google Calendar web template как primary CTA;
- Apple/iCloud subscription calendar;
- Outlook connector;
- iOS native connector;
- TWA или Capacitor application shell;
- прямую запись в Android Calendar Provider;
- calendar read permission;
- background ICS auto-open;
- автоматическое утверждение, что календарь сохранён после download или form open.

Если Android connector pilot покажет устойчивый спрос, решение «тонкий connector или полноценное Android application shell» принимается отдельным product track.

## 11. Данные и API-контракты

### 11.1. Saved event

Переиспользовать существующий `public.user_saved_event` и его owner-scoped mutation boundary. Не создавать параллельную сущность только для push.

Логически нужны состояния:

```text
saved
reminders_enabled
calendar_saved_intent
favorite_saved
```

Названия физических колонок/таблиц определяются после data audit; этот документ не разрешает браузеру напрямую писать внутренние delivery tables.

### 11.2. Push projection

Нужны server-owned:

- subscription identity/device projection;
- reminder schedule;
- send attempt/result;
- event revision binding;
- cancellation/reschedule invalidation;
- idempotency key;
- PII-free health view.

### 11.3. Email invitation projection

Только после `GO_TO_IMPLEMENTATION`:

- stable iCalendar UID;
- current SEQUENCE/event revision;
- invitation status;
- provider message id/attempt state;
- update/cancel state;
- explicit user trigger/consent evidence;
- suppression and account purge behavior.

### 11.4. Connector endpoint

Предварительные публичные contracts:

```text
GET /add/<signed-token>               # App Link + fallback page
GET /v1/events/<signed-token>.json    # bounded canonical payload
GET /.well-known/assetlinks.json
```

Endpoint не является general event API и не возвращает персональные данные.

## 12. Этапы реализации

### Stage A — документация и audit

- сверить этот документ с immutable source requirements;
- провести current saved-event/push/email data audit;
- определить server owner push sender/scheduler;
- подготовить threat model connector token/App Links/signing;
- не менять production UI.

### Stage B — production Web Push

- subscription UI и owner-scoped storage;
- T−24/T−1 scheduler;
- cancel/reschedule invalidation;
- notification click routing;
- Android+iOS device acceptance;
- feature flag и bounded canary;
- rollout только после terminal evidence.

### Stage C — NotiSend invitation lab

- exact MIME fixtures N1–N5;
- controlled SMTP sends;
- raw-source verification;
- client matrix;
- support question;
- итог `GO | NARROW | NO-GO`;
- никаких production invitations до отдельного owner decision.

### Stage D — Android connector prototype

- signed minimal project;
- verified App Links;
- canonical payload endpoint;
- `ACTION_INSERT` form;
- direct APK и Play Internal/Closed comparison;
- Android device matrix;
- итог `GO | NARROW | NO-GO`.

### Stage E — product decision

На основании Stage B–D решить:

- push-only + ICS;
- push + accepted email invitation;
- push + Android connector pilot;
- public Android distribution;
- отдельный future native-app track.

## 13. Release gates

### 13.1. Push GO

- exact source SHA/build id;
- server-side canonical event revalidation;
- T−24/T−1 matrix;
- cancel/reschedule behavior;
- idempotency/multi-device/expired endpoint;
- Android and iOS installed-PWA evidence;
- rollback switch;
- no secrets/endpoints in artifacts.

### 13.2. NotiSend research GO

- exact MIME fixtures/hashes;
- provider/raw-source proof;
- Gmail Android and Apple Mail iOS explicit calendar action;
- update/cancel evidence;
- no marketing consent/contact side effect;
- provider routing decision recorded in canonical email docs.

### 13.3. Android connector prototype GO

- stable package/signing key contract;
- verified App Links;
- `ACTION_INSERT` exact-field matrix;
- no calendar permission;
- installed/absent fallback behavior;
- direct APK and/or Play test evidence;
- real Android device evidence;
- no arbitrary intent/payload injection.

### 13.4. Global NO-GO

- UI говорит «Добавлено», имея только download/share/form-open evidence;
- push scheduling опирается на browser-supplied event facts;
- NotiSend silently replaces Postbox production route;
- email invitation превращает пользователя в marketing subscriber;
- APK подписан debug/временным ключом;
- App Link не verified;
- calendar permission запрашивается без необходимости;
- новый provider/native track блокирует существующий ICS fallback;
- source requirements изменены в этом track.

## 14. Rollback

- Push: выключить server scheduling flag, сохранить saved-event state и очистить/инвалидировать pending reminders; существующий сайт/ICS продолжают работать.
- NotiSend lab: остановить controlled sends; production routing не меняется, поэтому application rollback отсутствует.
- Android connector: убрать install CTA/App Link promotion, оставить HTTPS fallback; уже установленный APK должен безопасно открыть сайт или объяснить прекращение поддержки.
- ICS: не удалять при сбое новых каналов.

## 15. Document routing

| Назначение | Документ |
|---|---|
| Неизменяемые пользовательские требования | `docs/features/static-site-pages/schedule-user-requirements.md` |
| Настоящая стратегия/research contract | `docs/features/static-site-pages/calendar-reminder-strategy.md` |
| Saved event и ICS | [`Event favorites and calendar`](../event-favorites-calendar/README.md) |
| Transactional event email | [`Transactional event email`](../event-email-notifications/README.md) |
| Provider/DNS/delivery operations | [`Email delivery`](../../operations/email-delivery.md) |
| Static-site production release | [`Release plan`](release-plan.md) |

Канонический route key этого документа:

```yaml
features.static_site_pages.calendar_reminder_strategy
```

## 16. Основные внешние источники для research/implementation

- Android calendar intents: https://developer.android.com/guide/components/intents-common#Calendar
- Android App Links: https://developer.android.com/training/app-links
- Digital Asset Links: https://developer.android.com/training/app-links/configure-assetlinks
- Android alternative distribution: https://developer.android.com/distribute/marketing-tools/alternative-distribution
- NotiSend transactional email/SMTP: https://notisend.ru/transactional-emails/
- NotiSend developer documentation: https://notisend.ru/docs/
- iCalendar: https://www.rfc-editor.org/rfc/rfc5545
- iTIP: https://www.rfc-editor.org/rfc/rfc5546
- iMIP: https://www.rfc-editor.org/rfc/rfc6047
