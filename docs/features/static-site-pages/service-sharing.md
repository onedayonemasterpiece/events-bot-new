# Карточка «Поделиться сервисом»

> Статус: **F18 — обязательный блокер первого публичного релиза; mobile contract готов, desktop Windows/macOS rich-clipboard mode требует аналитики и тестирования, реализация и release evidence отсутствуют**.

## Назначение и граница фичи

Пользователь должен уметь не только поделиться конкретным событием, но и коротко рассказать знакомому о самом KenigEvents. Это отдельная фича от [шеринга события](event-sharing.md):

- `event sharing` передаёт конкретное событие, его изображение и canonical event URL;
- `service sharing` передаёт одну централизованно подготовленную карточку о сервисе и ссылку на `https://kenigevents.ru/`;
- карточка сервиса не становится изображением события, не попадает в event gallery, hero, `Event.image[]` или карточку «Как добраться».

Функция входит в общий shell всех публичных статических HTML-страниц и является release blocker, а не пострелизным экспериментом.

## Точки входа и поведение по устройствам

Один общий компонент и один payload обслуживают две обязательные точки входа:

1. в раскрытом мобильном меню под бренд-биркой — действие **«Рассказать о сервисе»**;
2. в общем footer — действие **«Поделиться сервисом»**.

Это две placements **одной адаптивной F18 action**, а не отдельные mobile/desktop фичи: общий компонент, service URL, card manifest, copy/claim gates, accessible semantics и analytics family сохраняются на всех устройствах, а breakpoint меняет только транспорт доставки. Mobile использует system share; desktop — clipboard. Нельзя реализовать независимо редактируемые mobile/desktop payloads, показывать оба транспорта одновременно или закрывать desktop-исследование без проверки mobile-части того же RC.

Поведение адаптируется, но смысл и ссылка остаются одинаковыми:

- **mobile (`<768px`)**: вызывается native Web Share с заранее подготовленным файлом карточки, коротким plain-text сообщением и URL сервиса;
- если mobile browser/target не принимает файл, используется native share только текста+URL, затем clipboard fallback; обычная навигация никогда не блокируется;
- **desktop (`>=768px`)**: native share намеренно не вызывается; безопасный release baseline называется **«Скопировать ссылку»**, копирует короткий текст+URL и показывает доступное подтверждение;
- desktop-кандидат **«Скопировать карточку»** может записывать один `ClipboardItem` с настоящим `image/png`, `text/html` и `text/plain`, но не становится release contract до фактической [Windows/macOS clipboard matrix](service-sharing-desktop-clipboard-research.md): target app само выбирает representation, поэтому нельзя обещать единое сообщение «изображение + подпись + ссылка»;
- если Clipboard API недоступен, показывается выделяемая ссылка с обычным `<a>` fallback;
- breakpoint, accessible name, focus/keyboard semantics и analytics reason должны быть едиными для обеих точек входа, без двух расходящихся реализаций.

Mobile-first поведение следует повторно использовать из уже оттестированного Pharmastaff organization-card share flow. На момент фиксации требований точный Pharmastaff repository/branch/SHA не найден ни в этом репозитории, ни в доступном списке репозиториев организации. До реализации нужно приложить точную ссылку/SHA и зафиксировать проверенный контракт: предварительная доступность файла, сохранение transient user activation, `navigator.canShare`, cancel/error handling и fallback. Без этой привязки нельзя заявлять parity с Pharmastaff.

## Рекомендуемая карточка V1

Карточка должна читаться за 2–3 секунды: один заголовок, одна доказательная строка, не более трёх коротких выгод и один CTA. Стартовая иерархия:

```text
[KenigEvents lettering / brand mark]

Найдите своё событие быстрее
{events_floor}+ актуальных событий · {city_count} городов области

Умный поиск · Подборки для вас · В календарь и напоминание

Смотреть события → kenigevents.ru
```

Точный текст и визуал проходят owner sign-off вместе с frozen UI. Для первой версии приоритетны:

1. региональный охват и объём актуального каталога;
2. умный поиск и персонализация как способ быстрее найти подходящее;
3. сохранение в календарь и подтверждённое D-1 напоминание;
4. явный CTA и короткий домен.

Дополнительные правдивые тезисы для последующих A/B/редакционных вариантов, но не для перегрузки стартовой карточки:

- «Калининград и города области — в одной афише»;
- «Сегодня, завтра и на выходных»;
- «Похожие события и новые идеи, а не только знакомые интересы»;
- «Актуальные даты, места и расписание обновляются автоматически» — только после закрытия F1/F13;
- «Как добраться в города области» — только после production acceptance F11.

## Политика продуктовых утверждений

Числа и сравнения на публичной карточке являются release data, а не рекламным предположением:

- `eligible_event_count` — число distinct canonical current/future events, реально включённых в production static catalog; cancelled, inactive, `silent`, review-only, duplicate identities и отдельные occurrence-дубли исключаются;
- `city_count` — число distinct normalized cities области с хотя бы одним eligible event; unknown/prose locations не считаются;
- на карточке показывается консервативный `events_floor` (например, нижняя ступень по 25/50), а не округление вверх; повышение ступени требует safety margin, падение ниже обещанного значения вызывает немедленный новый asset или evergreen fallback;
- формула, snapshot time, catalog hash и список городов сохраняются в manifest/evidence, но не перегружают лицевую сторону.

Фразы **«самая большая база» / «крупнейшая база»** допускаются только после датированного воспроизводимого сравнения с определённым набором региональных альтернатив и одинаковой методикой подсчёта. Без такого evidence используется проверяемая формулировка **«{N}+ актуальных событий по всей области»**.

Фраза **«быстрее, чем в других местах»** также требует сравнительного продуктового исследования. До него используется **«Найдите своё событие быстрее»**. Более сильное персонализационное обещание допускается только после прохождения F6 E2E gate (`cards_to_first_relevant <= 20`) на зрелых golden personas и наличия релевантного предложения в каталоге.

Тезис про D-1 напоминание публикуется только когда F8/F12 закрыты end-to-end; calendar save сам по себе не выдаётся за email consent.

## Централизованный prerender, а не генерация при клике

Картинка никогда не создаётся в браузере или отдельным backend-запросом на каждое нажатие. Целевой pipeline:

1. production static export считает metrics из того же accepted catalog snapshot, который публикуется на сайт;
2. формируется versioned `service-share-card` manifest: schema/copy/template versions, `catalog_hash`, metrics, `measured_at`, rendered asset URL, byte size и SHA-256;
3. deterministic SVG-template рендерится централизованно в лёгкий `1080×1350` WebP; SVG остаётся source/template, а public file и CTA проходят visual/text checks;
   тот же visual payload при необходимости детерминированно экспортирует настоящий PNG только для desktop clipboard compatibility, без повторной генерации композиции в browser;
4. render запускается только когда изменился нормализованный payload: display bucket, city count, утверждённый текст/бренд/CTA или template version; одинаковый payload переиспользует существующий asset;
5. content-addressed WebP и manifest публикуются в CDN до HTML promotion, затем current pointer переключается атомарно вместе с release manifest;
6. если metrics/source/render validation не прошли, build использует заранее проверенную evergreen-карточку без числового обещания либо link-only fallback — но не старое заведомо ложное число.

Требования к asset:

- WebP-first, `1080×1350`, целевой бюджет `<=350 KiB`; без случайных PNG/JPEG runtime assets;
- desktop clipboard PNG — отдельное typed representation с тем же `visual_payload_hash`, настоящей PNG signature и собственными size/latency gates; он не используется как обычный page/LCP asset и допускается только после Windows/macOS matrix;
- `https://static.kenigevents.ru/...` с immutable cache key, корректными `Content-Type`, CORS и checksum;
- CTA, logo/lettering, текст и домен остаются читаемыми после уменьшения до ширины 360 px;
- карточка не должна ухудшать LCP: загрузка/подготовка выполняется после critical content и с учётом проверенного Pharmastaff user-activation pattern;
- никаких user/session/email/profile/query данных в файле, manifest или URL.

## Визуальная эволюция

### Release V1

Использовать существующий [`site/public/brand-mark.svg`](../../../site/public/brand-mark.svg), утверждённый lettering/типографику, простой фирменный фон, один сильный proof-number block и CTA. Новый генеративный фон для закрытия релиза не требуется.

### Future V2 — кубы с афишами

Идея множества кубов, на гранях которых находятся афиши, зафиксирована как будущий визуальный вариант, но не как зависимость V1. В истории репозитория найден ранний `3d_intro`/bento prototype на commit [`16650190`](https://github.com/onedayonemasterpiece/events-bot-new/tree/1665019087511b44c5d759b898eb9fb3adfff17f/3d_intro): `bento_scene.py` строил 6×6 сетку кубов и предусматривал poster textures, `generate_intro.py` — zoom к выбранной poster cube. Он не дошёл до production CherryFlash и не должен вливаться целиком.

Для V2 переносится только концепция: статичный чистый keyframe/bento из кубов, source-safe афиши на гранях, logo и CTA. Новый renderer реализуется отдельно, соблюдает OCR/no-crop, image-dedup, права на media, WebP budget и visual QA.

## Минимальная аналитика

Сохраняются только компактные aggregate-friendly действия:

- `service_share_opened` / `service_share_invoked` с `surface=mobile_menu|footer`;
- `service_link_copied` для desktop/fallback;
- `service_copy_attempted` / `service_copy_result` для desktop rich-clipboard candidate различают только `clipboard_card|text_link` и честный API/fallback result; clipboard contents и фактическую вставку/отправку не читают и не имитируют;
- `share_file_unsupported`, `share_cancelled`, `share_error` как bounded reason counters.

Web Share не позволяет честно утверждать фактическую отправку в target app, поэтому UI/метрики не называют `navigator.share()` «успешной доставкой». Ни одно событие не создаёт unbounded raw telemetry row без принятого compact-ingest contract.

## Release acceptance

- [ ] один общий компонент присутствует на 100% публичных static HTML page families;
- [ ] mobile Playwright на `390×844`: действие видно под раскрытой биркой и в footer; оба используют один current manifest/URL;
- [ ] при `canShare(files)=true` в `navigator.share` передаются WebP file, короткий text и canonical service URL; payload не содержит текущий event/personal secret URL;
- [ ] file-unsupported, API rejection/cancel, offline/CDN failure и Clipboard denial дают проверяемый fallback без broken UI;
- [ ] desktop `1366/1440`: обе точки не вызывают native share; D0 «Скопировать ссылку» всегда копирует текст+service URL либо показывает selectable fallback и объявляет результат через `aria-live`;
- [ ] до решения о «Скопировать карточку» выполнена полная [Windows/macOS desktop clipboard research matrix](service-sharing-desktop-clipboard-research.md): Edge/Chrome/Firefox на Windows, Safari/Chrome/Firefox на macOS, controlled targets и реальные Telegram/VK/MAX/rich/plain applications;
- [ ] D1 `text/html`-first и D2 `image/png`-first проверены как один `ClipboardItem` с тремя representations; ни один target-specific результат не обобщается без evidence, `empty_paste` после success запрещён;
- [ ] owner по matrix выбирает D0, D1, D2 либо две явные desktop actions и фиксирует label/success/fallback; до этого основной контракт остаётся «Скопировать ссылку»;
- [ ] real Android/iOS checks подтверждают Telegram, VK и MAX для file-share, text+URL fallback и возврата в браузер;
- [ ] rendered card проходит screenshot/OCR/readability QA на исходном размере и thumbnail width 360 px; CTA и домен видимы, текст не обрезан;
- [ ] metrics manifest совпадает с accepted catalog hash; displayed floor никогда не превышает eligible count; city count воспроизводим;
- [ ] content-addressed WebP возвращает `200 image/webp`, укладывается в byte budget и доступен через CDN; старый asset можно откатить вместе с release manifest;
- [ ] superlative/comparative/reminder claims не публикуются без соответствующего evidence gate;
- [ ] точный Pharmastaff reference/SHA и перенесённые browser-behavior checks приложены к implementation PR;
- [ ] owner подписывает точный copy/template/asset SHA и mobile/desktop screenshots в составе immutable UI release preview.

F18 считается закрытой только когда код достижим из `origin/main`, public CDN asset и manifest опубликованы, mobile и Windows/macOS desktop проверки приложены к тому же RC SHA, desktop mode выбран по matrix, а обе обязательные точки входа работают на всех page families.
