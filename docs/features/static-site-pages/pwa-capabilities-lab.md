# PWA-возможности для анонсов: календарь, уведомления, офлайн и системный Share

> **Дата решения:** 2026-08-02  
> **Статус:** минимальная лабораторная страница реализована; ручная Android-матрица ещё не пройдена
> **Решение:** `GO_TO_LAB`, `NO-GO` для production rollout до ручной проверки на реальном Android  
> **Ветка:** `feature/static-site-pwa-capabilities-lab-20260802`  
> **Scope:** изолированная noindex-страница Astro; без изменения production-навигации, root service worker, Auth, Supabase и публикации canonical root

## 1. Итоговое решение

Установленная через Chrome PWA остаётся веб-приложением. Она не получает Android-разрешения
`WRITE_CALENDAR` и не может универсально вызвать системную функцию «добавить событие» так,
как это делает нативное приложение.

Для текущего сайта принимается следующий набор:

| Возможность | Статус | Что проверяем |
|---|---|---|
| Google Calendar template URL | `GO` | открытие заполненной формы без скачивания файла |
| Скачать `.ics` | `GO` | универсальный fallback |
| Передать `.ics` через системное меню Share | `GO_TO_LAB` | какие календари Android принимают файл без отдельного шага скачивания |
| Android `intent:` из браузера | `LAB_ONLY` | фактическое поведение Chrome/прошивки; не считать переносимым контрактом |
| Локальное уведомление через service worker | `GO_TO_LAB` | запрос разрешения и показ уведомления на Android |
| Настоящая Web Push-подписка | `PARTIAL_LAB` | подписка возможна; доставка требует VAPID и sender/backend |
| Кэш 30 карточек «Для меня» | `GO` | мгновенный показ last-known данных и обновление после открытия |
| Периодическое фоновое обновление | `BEST_EFFORT_ONLY` | браузер сам решает, когда запускать Periodic Background Sync |
| Системный Share с plain text/URL/files | `GO` | реальное поведение Telegram/VK/WhatsApp/почты и других targets |
| Универсальный HTML-текст в системном Share | `NO` | Web Share не содержит поля HTML |
| Rich HTML через Clipboard | `GO_TO_LAB` | копирование `text/html` + `text/plain` и ручная вставка в target |
| Нативный `ACTION_INSERT` календаря | `WRAPPER_ONLY` | отдельный Android APK/AAB из того же frontend-кода |

Лаборатория нужна не для доказательства уже известных ограничений спецификаций, а для
получения фактической матрицы поведения конкретных Android-устройств и приложений-получателей.

## 2. Календарь: что может браузерная PWA

### 2.1. Поддерживаемые web-варианты

1. **Google Calendar template URL.** Открывает заполненную форму Google Calendar.
   Пользователь подтверждает сохранение. Это не системный Android API и не работает с любым
   календарём, зато не требует OAuth и не создаёт скачанный файл.
2. **ICS download.** Самый переносимый fallback. Пользователь скачивает файл и выбирает
   приложение, которое умеет его импортировать.
3. **ICS через Web Share.** PWA создаёт `File` в памяти и передаёт его в
   `navigator.share({ files })`. Отдельного шага скачивания может не быть, но появление
   календаря в системном меню и качество импорта зависят от установленного приложения.
4. **Google Calendar API.** Позволяет создавать, обновлять и удалять события после OAuth.
   Это интеграция с Google-аккаунтом, а не доступ к локальному системному календарю.
   В лабораторию не входит.

### 2.2. Почему `intent:` оставляем только экспериментом

Chrome for Android поддерживает URL вида `intent:` и разрешает переход к Android Activity
после явного действия пользователя. Однако вызываемая Activity должна быть доступна браузеру
как `BROWSABLE`. Стандартный календарный `ACTION_INSERT` документирован как native intent и
не является гарантированным browser-deep-link contract.

Поэтому кнопка `Android Intent — эксперимент` допустима только при следующих условиях:

- отображается как экспериментальная и не становится основной CTA;
- запускается только после click/tap;
- содержит `browser_fallback_url` на Google Calendar или возврат к ICS;
- ошибка/отсутствие обработчика явно пишется в журнал;
- успешность на одном телефоне не считается доказательством совместимости.

### 2.3. Требования к тестовому ICS

Для простоты лаборатория генерирует UTC timestamps и корректно экранирует значения.
Минимальный файл должен содержать:

```text
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//KenigEvents//PWA Capabilities Lab//RU
CALSCALE:GREGORIAN
METHOD:PUBLISH
BEGIN:VEVENT
UID:<stable-random-id>@kenigevents.ru
DTSTAMP:<UTC>
DTSTART:<UTC>
DTEND:<UTC>
SUMMARY:<escaped title>
DESCRIPTION:<escaped description>
LOCATION:<escaped location>
URL:<absolute event URL>
END:VEVENT
END:VCALENDAR
```

Использовать `CRLF`, экранировать `\\`, `,`, `;` и переводы строк, а длинные строки
fold-ить по RFC 5545. Для лаборатории достаточно одного события.

## 3. Нативная оболочка: как это устроено

### 3.1. Главное ограничение

PWA, установленная кнопкой Chrome, может быть представлена Android как WebAPK, но это не APK,
которым управляет команда проекта. В такой установке нельзя дописать Kotlin-класс или
нативный Calendar plugin.

Совместить PWA и native-возможности можно **на уровне одного продукта и кодовой базы**, но
не внутри одной browser-installation:

```text
общий Astro/frontend-код
├── web deployment → сайт + PWA из Chrome
└── Android project → APK/AAB с native bridge
```

Пользователь, установивший сайт из Chrome, получает только web-возможности. Пользователь,
установивший APK/AAB, получает native bridge. UI может быть одинаковым и выбирать adapter по
feature detection.

### 3.2. Два практических варианта

#### Вариант A — Capacitor

Capacitor упаковывает тот же web UI в Android WebView/runtime и предоставляет plugin bridge.
Для календаря создаётся маленький Android plugin:

```text
JavaScript CalendarAdapter.addEvent(event)
        ↓
Capacitor plugin (Kotlin)
        ↓
Intent(Intent.ACTION_INSERT, CalendarContract.Events.CONTENT_URI)
        ↓
системная форма календаря
```

Для `ACTION_INSERT` не требуется прямое разрешение записи в календарь: пользователь видит
форму и подтверждает действие. Это предпочтительный первый native-прототип. Прямая запись
через `CalendarContract` без формы требует calendar permissions и не нужна для текущего кейса.

Плюсы: простой JS↔native bridge, плагины, одна UI-кодовая база. Минусы: отдельные Android
build/sign/release steps, WebView-поведение нужно тестировать отдельно от Chrome PWA.

#### Вариант B — Trusted Web Activity / Bubblewrap

TWA показывает проверенный HTTPS-сайт полноэкранно через браузерный runtime. Bubblewrap
создаёт отдельный Android-проект, APK/AAB, подпись и Digital Asset Links. Native-функции
добавляются отдельным Android-кодом и связываются с web-контентом через TWA postMessage или
специальный deep-link/Activity adapter.

Плюсы: web-контент остаётся основным и обновляется с сервера. Минусы: bridge сложнее, чем в
Capacitor; это всё равно отдельный Android artifact и отдельная доставка пользователю.

### 3.3. Рекомендуемая архитектура адаптера

```ts
export interface CalendarAdapter {
  kind: 'web' | 'native';
  addEvent(event: LabEvent): Promise<CalendarResult>;
}
```

- web adapter: Google Calendar URL → ICS Share → ICS download fallback;
- native adapter: bridge → Android `ACTION_INSERT`;
- одна и та же кнопка может показывать лучший доступный метод;
- native bridge никогда не считается доступным только по Android User-Agent: нужна проверка
  конкретного объекта/plugin API.

Native-wrapper implementation в текущую лабораторию не входит. Документ и page должны лишь
оставить disabled probe-кнопку `Native bridge не обнаружен` и описать ожидаемый payload.

## 4. Офлайн: можно ли заранее держать 30 карточек «Для меня»

Да. Для персонального feed рекомендуется app-managed stale-while-revalidate:

```text
открытие PWA
├── сразу прочитать последние 30 DTO из IndexedDB
├── отрисовать их без ожидания сети
└── параллельно запросить свежие 30
    ├── обновить экран
    ├── заменить DTO в IndexedDB
    └── прогреть изображения в Cache Storage
```

Это даёт мгновенное повторное открытие после хотя бы одной успешной загрузки. Первая загрузка
нового пользователя всё равно требует сети, если данные не были ранее получены push/sync.

### 4.1. Что хранить

- в IndexedDB: компактные versioned DTO карточек, порядок, `generated_at`, identity/profile key;
- в Cache Storage: изображения и другие same-origin/static assets;
- не складывать персональный API response в общий cache без явной user/version segmentation;
- очищать персональные данные при logout/reset identity;
- ограничить количество, возраст и объём; 30 карточек — разумный bounded cache, но изображения
  должны иметь отдельную eviction policy.

### 4.2. Что значит «периодически»

Надёжно обновлять можно:

- при каждом открытии/возврате приложения;
- после события `online`;
- после изменения профиля/избранного;
- во время обрабатываемого push-события, если push infrastructure уже существует.

Periodic Background Sync можно зарегистрировать как дополнительную оптимизацию, но браузер
сам определяет расписание на основе engagement, сети, батареи и других факторов. Это не cron и
не гарантия «каждые 30 минут». Поэтому product contract должен обещать **мгновенный last-known
feed**, а не гарантированно свежий feed до открытия.

## 5. Уведомления и настоящий Web Push

### 5.1. Разрешение

На странице нельзя автоматически вызывать permission prompt при загрузке. Кнопка пользователя
должна:

1. показать текущее `Notification.permission` (`default`, `granted`, `denied`);
2. по tap вызвать `Notification.requestPermission()`;
3. зарегистрировать lab-scoped service worker;
4. при `granted` вызвать `registration.showNotification(...)`.

На мобильном notification следует показывать через service worker. Это проверит реальное
Android-разрешение и отображение уведомления, но это ещё **не remote push**.

### 5.2. Два теста на лабораторной странице

- `Разрешить и показать уведомление` — локальный вызов `showNotification`;
- `Симулировать push payload` — page отправляет payload в lab service worker через
  `postMessage`, worker показывает уведомление. В UI явно писать `simulation`, а не `push sent`.

### 5.3. Настоящий push

Для remote Web Push нужны:

- active service worker;
- `PushManager.subscribe({ userVisibleOnly: true, applicationServerKey })`;
- VAPID public key в клиенте;
- сохранение `PushSubscription` на backend;
- sender, который подписывает запрос VAPID private key и отправляет его на subscription endpoint.

Статическая страница сама не должна хранить private key. Поэтому лаборатория может добавить
кнопку `Создать PushSubscription` только когда задан
`PUBLIC_PWA_LAB_VAPID_PUBLIC_KEY`, показать sanitized subscription JSON и дать скопировать его.
Полноценную доставку без backend не заявлять.

## 6. Системное меню Share и форматирование

Web Share принимает только:

```ts
{
  title?: string;
  text?: string;
  url?: string;
  files?: File[];
}
```

Универсального `html`/`richText` поля нет. Нельзя гарантировать ссылку вида
`<a href="…">Название события</a>` вместо plain URL. Приложение-получатель само решает:
использовать ли `title`, сохранить ли text рядом с image/file и как показать URL.

Следовательно:

- **полноценный форматированный пост через generic Share — нет**;
- дополнительное исследование спецификации не требуется;
- нужен только device/target compatibility test, потому что деградация по приложениям разная.

Практические альтернативы:

1. `ClipboardItem` одновременно с `text/html` и `text/plain`, затем пользователь вручную
   вставляет пост. Target может сохранить rich formatting, а может взять только plain fallback.
2. Рендер готового поста в PNG/WebP и share изображения; визуальное оформление сохранится,
   caption останется plain text.
3. Отдельные API/боты конкретных платформ для настоящего форматированного поста.
4. Native Android `ACTION_SEND` может передать `EXTRA_HTML_TEXT` вместе с `EXTRA_TEXT`, но
   приложение-получатель всё равно вправе проигнорировать HTML. Native wrapper не делает
   rich share универсальным.

## 7. Лабораторная страница

### 7.1. Маршрут и изоляция

Предлагаемые файлы:

```text
site/src/pages/lab/pwa-capabilities/index.astro
site/src/pages/lab/pwa-capabilities/sw.js.ts
site/src/lib/pwaCapabilitiesLab.ts              # optional, если логика станет неудобной inline
site/tests/pwa-capabilities-lab.test.mjs         # минимальный source/behavior contract
```

Требования:

- маршрут: `/lab/pwa-capabilities/`;
- `noindex, nofollow, noarchive`;
- не добавлять в global nav, sitemap, home и production CTA;
- service worker script находится внутри lab path и регистрируется со scope
  `/lab/pwa-capabilities/`;
- не менять поведение существующего [`pwa-sw.js.ts`](../../../site/src/pages/pwa-sw.js.ts),
  identity существующего [`manifest.webmanifest.ts`](../../../site/src/pages/manifest.webmanifest.ts)
  и install flow;
- страница должна работать на HTTPS и `localhost`; на небезопасном origin показывать понятную
  диагностику вместо исключений.

### 7.2. Тестовое событие

Небольшая форма:

- название;
- начало и конец;
- location;
- description;
- absolute URL;
- timezone, по умолчанию `Europe/Kaliningrad`.

Начальные значения должны быть в будущем относительно открытия страницы, например start через
два часа и duration 90 минут, чтобы лаборатория не протухала.

### 7.3. Блок Calendar

Кнопки:

1. `Открыть Google Calendar`;
2. `Скачать ICS`;
3. `Поделиться ICS` — сначала `navigator.canShare({ files: [icsFile] })`, затем Share,
   иначе понятный fallback на download;
4. `Android Intent — эксперимент` — только на Android, с browser fallback;
5. `Native ACTION_INSERT` — disabled, пока bridge adapter не обнаружен.

После каждого действия показать method, timestamp, success/cancel/error и короткую
интерпретацию результата.

### 7.4. Блок Notifications / Push

Показать:

- secure context;
- текущий Notification permission;
- service worker availability и active scope;
- PushManager availability;
- кнопку local notification;
- кнопку simulated payload;
- optional subscription button при наличии VAPID public key;
- кнопку unregister/clear lab worker для повторных тестов.

`denied` не считать технической ошибкой: показать, что повторный prompt из страницы уже не
появится и разрешение меняется в настройках сайта/Android.

### 7.5. Блок Offline 30

Лаборатория не должна обращаться к production personal API. Использовать 30 локально
сгенерированных demo-card DTO.

Кнопки:

- `Сохранить 30 карточек`;
- `Прочитать из IndexedDB`;
- `Прогреть изображения` — использовать существующий same-origin PWA icon/fixtures;
- `Зарегистрировать periodic sync`, если API доступен;
- `Очистить lab data и cache`.

Показывать cached count, bytes/approximate size, last updated, periodic sync support и результат
последней операции. После reload карточки должны читаться из IndexedDB без сети.

### 7.6. Блок Share

Кнопки:

1. `Поделиться текстом и ссылкой`;
2. `Поделиться картинкой + текстом + ссылкой`;
3. `Скопировать rich post` — `ClipboardItem` с `text/html` и `text/plain`;
4. `Скопировать plain fallback`;
5. optional `Поделиться HTML как файлом`, с явной подписью, что это attachment, а не body.

Рядом оставить ручную таблицу результатов:

| Target | text | URL | image/file | formatting | заметка |
|---|---:|---:|---:|---:|---|
| Telegram |  |  |  |  |  |
| VK |  |  |  |  |  |
| WhatsApp |  |  |  |  |  |
| Email |  |  |  |  |  |
| Calendar apps |  |  |  |  |  |

### 7.7. Capability diagnostics

В верхней части показать read-only таблицу:

- `isSecureContext`;
- Android / display-mode standalone;
- `navigator.share`;
- `navigator.canShare` и file share;
- Service Worker;
- Notifications;
- PushManager;
- IndexedDB / CacheStorage;
- Periodic Background Sync;
- Clipboard + `ClipboardItem`;
- native bridge probe.

Все ошибки должны попадать в один видимый журнал, а не только в console.

## 8. Acceptance для Codex

Минимальный результат считается готовым, когда:

1. Astro build проходит.
2. Страница открывается по `/lab/pwa-capabilities/` и не попадает в навигацию/sitemap.
3. Unsupported APIs дают понятный disabled/fallback state без uncaught errors.
4. Google Calendar URL содержит актуальные значения формы.
5. ICS скачивается и может быть передан через Share там, где поддерживается file share.
6. Permission запрашивается только после tap; при `granted` виден local notification.
7. Simulated payload явно не называется настоящим remote push.
8. 30 demo cards сохраняются, читаются после reload и очищаются отдельной кнопкой.
9. Plain share и rich clipboard можно проверить независимо.
10. Root manifest и root service worker не изменены по поведению.
11. Добавлен один минимальный test/contract и записаны команды проверки.

### 8.1. Реализация 2026-08-02

Лаборатория реализована изолированно:

- `site/src/pages/lab/pwa-capabilities/index.astro` — noindex UI без ссылки из
  production-навигации;
- `site/src/lib/pwaCapabilitiesLab.js` — calendar/ICS, notification/push probes,
  IndexedDB/Cache Storage, Web Share/Clipboard, diagnostics и единый журнал;
- `site/src/pages/lab/pwa-capabilities/sw.js.ts` — отдельный worker со scope
  `/lab/pwa-capabilities/`;
- `site/tests/pwa-capabilities-lab.test.mjs` — минимальный contract для ICS,
  Google Calendar URL, 30 demo-card DTO, noindex и lab-scoped worker.

Проверки из `site/`:

```bash
npm run test:pwa-capabilities-lab
npm run build
```

Обе команды прошли 2026-08-02. Build содержит
`/lab/pwa-capabilities/index.html` и `/lab/pwa-capabilities/sw.js`. Root manifest,
root service worker, sitemap, global navigation, Auth/Supabase и release gates не менялись.
Ручная проверка Android/target-приложений остаётся отдельным acceptance-шагом из раздела 9.

## 9. Ручная Android-матрица

Проверить минимум:

- Chrome tab и installed standalone PWA;
- permission states `default`, `granted`, `denied`;
- online, airplane mode и offline reload;
- Google Calendar и доступный системный/сторонний календарь;
- Share targets Telegram, VK, WhatsApp и email, если установлены;
- повторная установка/очистка lab worker;
- один современный Android и, по возможности, второй телефон/прошивку.

Результаты фиксировать фактами: Android/Chrome/app version, режим browser/standalone,
нажатая кнопка, появившийся target и фактический импорт/форматирование.

## 10. Вне scope лаборатории

- Google Calendar OAuth/API;
- production Web Push sender и хранение subscriptions;
- production personal-feed endpoint;
- native APK/AAB и Play Store release;
- прямой `CalendarContract` write permission;
- точные локальные будильники;
- аналитика и production UX;
- изменение текущего static-site release decision.

## 11. Короткий handoff-промпт

```text
Работай в ветке feature/static-site-pwa-capabilities-lab-20260802.
Реализуй минимальную noindex Astro-страницу /lab/pwa-capabilities/ строго по
`docs/features/static-site-pages/pwa-capabilities-lab.md`. Не меняй root manifest,
root service worker, global nav, sitemap, Auth/Supabase и production release gates.
Используй lab-scoped service worker. Реализуй calendar buttons, permission + local
notification, simulated push payload, optional PushManager subscription, offline cache
30 demo cards, plain/file Share и rich Clipboard с понятными fallbacks. Добавь один
минимальный test/contract, запусти релевантный site build/test и commit/push в эту ветку.
```

## 12. Основные источники

- Android Calendar intents and provider: https://developer.android.com/identity/providers/calendar-provider
- Android common intents: https://developer.android.com/guide/components/intents-common
- Chrome Android intents: https://developer.chrome.com/docs/android/intents
- Google Calendar event template: https://developers.google.com/workspace/calendar/api/concepts/inviting-attendees-to-events
- Web Share API: https://w3c.github.io/web-share/
- Web Share files: https://developer.mozilla.org/docs/Web/API/Navigator/share
- Notifications from a service worker: https://developer.mozilla.org/docs/Web/API/ServiceWorkerRegistration/showNotification
- Push API: https://developer.mozilla.org/docs/Web/API/Push_API
- Cache Storage: https://developer.mozilla.org/docs/Web/API/CacheStorage
- IndexedDB: https://developer.mozilla.org/docs/Web/API/IndexedDB_API
- Periodic Background Sync: https://developer.mozilla.org/docs/Web/API/Web_Periodic_Background_Synchronization_API
- Clipboard rich formats: https://www.w3.org/TR/clipboard-apis/
- Capacitor: https://capacitorjs.com/docs
- Trusted Web Activity: https://developer.chrome.com/docs/android/trusted-web-activity
- TWA postMessage: https://developer.chrome.com/docs/android/trusted-web-activity/receive-payments-play-billing#communicate-with-the-twa
